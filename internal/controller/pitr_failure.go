package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// pitrFailureReason returns the first refusal token in message (§7.1), or
// "". Every PITR step writes its token as the first word of its termination
// message; the controller may prefix it ("PITRCoverageInvalid: " when it
// refuses a verdict), so the earliest whole-word token wins.
func pitrFailureReason(message string) string {
	for _, field := range strings.FieldsFunc(message, func(r rune) bool {
		return strings.ContainsRune(" \t\n\r:,;()\"{}=", r)
	}) {
		if t := pitrarchive.Token(strings.TrimSuffix(field, ".")); t.Known() {
			return string(t)
		}
	}
	return ""
}

// wrapTokenFirst prefixes err with context while keeping a leading refusal
// token first, so status messages always start "<token>: " (§7.1).
func wrapTokenFirst(context string, err error) error {
	if tok, ok := pitrarchive.ParseToken(err.Error()); ok {
		rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(err.Error()), string(tok)))
		rest = strings.TrimSpace(strings.TrimPrefix(rest, ":"))
		return fmt.Errorf("%s: %s: %s", tok, context, rest)
	}
	return fmt.Errorf("%s: %w", context, err)
}

// latestJobPod returns the newest pod the Job owns, or nil when none is left.
// Only an API error is transient; a missing pod is a final observation.
func latestJobPod(ctx context.Context, c client.Client, job *batchv1.Job) (*corev1.Pod, error) {
	pods, err := restoreJobPods(ctx, c, job)
	if err != nil {
		return nil, err
	}
	var chosen *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if podBelongsToJob(p, job) && (chosen == nil || p.CreationTimestamp.After(chosen.CreationTimestamp.Time)) {
			chosen = p
		}
	}
	return chosen, nil
}

// pitrJobFailure returns the termination message of the earliest failing
// container: init containers run in order, so a later container's message
// never describes an earlier failure. Containers that exited zero are
// skipped, so a successful step's last line is never reported as the cause.
func pitrJobFailure(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			term := status.State.Terminated
			if term == nil {
				term = status.LastTerminationState.Terminated
			}
			if term == nil || term.ExitCode == 0 {
				continue
			}
			if message := strings.TrimSpace(term.Message); message != "" {
				return message
			}
			return fmt.Sprintf("container %s exited %d (%s)", status.Name, term.ExitCode, term.Reason)
		}
	}
	return ""
}

func jobHasPITR(job *batchv1.Job) bool {
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name == restorePITRInitContainerName {
			return true
		}
	}
	return false
}

// Verdict sentinels written by restore.py (design WS-D §4.13). The main
// container's termination message holds the verdict line, then the RESULT
// line, then optionally the on-history line; see _pitr_verdict_message.
const (
	pitrVerdictSentinel   = "BLOODRAVEN_PITR_VERDICT"
	pitrResultSentinel    = "BLOODRAVEN_PITR_RESULT"
	pitrOnHistorySentinel = "BLOODRAVEN_PITR_ON_HISTORY_EXCLUSIONS"
	pitrMaxStatusGTIDs    = 2048
	pitrMaxReplaySites    = 16
)

// pitrGTIDSetChars is the alphabet of a GTID set as the server prints it
// (newlines removed): UUIDs, tags, intervals and separators.
var pitrGTIDSetChars = regexp.MustCompile(`^[0-9A-Za-z:,_-]*$`)

var (
	pitrSiteName = regexp.MustCompile(`^[a-z0-9-]+$`)
	pitrSHA256   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// pitrRenderedRequest is the replay the controller rendered into the Job,
// read back from the Job template so a verdict is checked against what the
// Job was asked to do, not against the current spec.
type pitrRenderedRequest struct {
	Mode              string // timestamp | latest
	Stop              string // canonical µs, timestamp mode only
	AllowPartial      bool
	Destination       string
	EncryptionMode    string
	ExpectedSourceUID string
	Verification      bool
}

func containerEnv(containers []corev1.Container, name string) map[string]string {
	env := map[string]string{}
	for _, c := range containers {
		if c.Name != name {
			continue
		}
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
	}
	return env
}

func renderedPITRRequest(job *batchv1.Job) pitrRenderedRequest {
	spec := job.Spec.Template.Spec
	init := containerEnv(spec.InitContainers, restorePITRInitContainerName)
	main := map[string]string{}
	if len(spec.Containers) > 0 {
		main = containerEnv(spec.Containers[:1], spec.Containers[0].Name)
	}
	return pitrRenderedRequest{
		Mode:              init["BLOODRAVEN_PITR_MODE"],
		Stop:              init["BLOODRAVEN_PITR_STOP_DATETIME"],
		AllowPartial:      init["BLOODRAVEN_PITR_ALLOW_PARTIAL"] == "true",
		Destination:       init["BLOODRAVEN_PITR_DESTINATION"],
		EncryptionMode:    init["BLOODRAVEN_PITR_ENCRYPTION_MODE"],
		ExpectedSourceUID: main["BLOODRAVEN_PITR_EXPECTED_SOURCE_UID"],
		Verification:      main["BLOODRAVEN_VERIFY_PITR_MODE"] != "" && main["BLOODRAVEN_VERIFY_PITR_MODE"] != "none",
	}
}

// pitrJobVerdict accepts a verdict only from the main container of a
// Succeeded pod owned by the Job, terminated with exit code 0, and only when
// it is consistent with the replay the Job template rendered. It returns the
// certified PointInTimeResult.
func pitrJobVerdict(pod *corev1.Pod, job *batchv1.Job) (*v1alpha1.PointInTimeResult, error) {
	if pod == nil {
		return nil, fmt.Errorf("the Job's pod is gone, so its PITR verdict cannot be read")
	}
	if pod.Status.Phase != corev1.PodSucceeded {
		return nil, fmt.Errorf("pod %s is %s, not Succeeded", pod.Name, pod.Status.Phase)
	}
	spec := job.Spec.Template.Spec
	if len(spec.Containers) == 0 {
		return nil, fmt.Errorf("the Job has no main container")
	}
	main := spec.Containers[0]
	var message string
	found := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != main.Name {
			continue
		}
		term := status.State.Terminated
		if term == nil || term.ExitCode != 0 {
			return nil, fmt.Errorf("container %s did not terminate successfully", main.Name)
		}
		message, found = strings.TrimSpace(term.Message), true
	}
	if !found {
		return nil, fmt.Errorf("container %s has no terminated status", main.Name)
	}
	return parsePITRVerdict(message, renderedPITRRequest(job))
}

// parsePITRVerdict parses restore.py's termination message against the
// rendered request.
func parsePITRVerdict(message string, req pitrRenderedRequest) (*v1alpha1.PointInTimeResult, error) {
	lines := strings.Split(strings.ReplaceAll(message, "\r\n", "\n"), "\n")
	tokens, ok := parseSentinelTokens(lines[0], pitrVerdictSentinel)
	if !ok || tokens["v"] != "2" {
		return nil, fmt.Errorf("the main container wrote no version 2 PITR verdict")
	}
	value := func(key string) (string, error) {
		raw, present := tokens[key]
		if !present {
			return "", fmt.Errorf("PITR verdict lacks %s", key)
		}
		decoded, valid := decodeRestoreToken(raw)
		if !valid {
			return "", fmt.Errorf("PITR verdict has an invalid %s", key)
		}
		return decoded, nil
	}
	count := func(key string) (int64, error) {
		raw, err := value(key)
		if err != nil {
			return 0, err
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("PITR verdict has an invalid %s", key)
		}
		return n, nil
	}
	fields := map[string]string{}
	for _, key := range []string{"mode", "stop", "outcome", "archive", "encryptionMode", "sourceUID", "sourceNamespace", "sourceName", "lastFile", "newest", "executedSha256", "sites"} {
		v, err := value(key)
		if err != nil {
			return nil, err
		}
		fields[key] = v
	}
	counts := map[string]int64{}
	for _, key := range []string{"segments", "files", "transactions", "divergent", "onHistoryExcluded"} {
		n, err := count(key)
		if err != nil {
			return nil, err
		}
		counts[key] = n
	}

	res := &v1alpha1.PointInTimeResult{}
	// Mode and stop must be the rendered ones.
	if fields["mode"] != req.Mode {
		return nil, fmt.Errorf("PITR verdict mode %q differs from the rendered mode %q", fields["mode"], req.Mode)
	}
	switch req.Mode {
	case "timestamp":
		res.Mode = v1alpha1.PointInTimeModeTimestamp
		if fields["stop"] != req.Stop {
			return nil, fmt.Errorf("PITR verdict stop %q differs from the rendered stop %q", fields["stop"], req.Stop)
		}
		stop, err := backupmeta.ParseMicroTime(fields["stop"])
		if err != nil {
			return nil, fmt.Errorf("PITR verdict stop: %w", err)
		}
		mt := metav1.NewMicroTime(stop.Time)
		res.StopTime = &mt
	case "latest":
		res.Mode = v1alpha1.PointInTimeModeLatest
		if fields["stop"] != "" {
			return nil, fmt.Errorf("latest PITR verdict carries a stop %q", fields["stop"])
		}
	default:
		return nil, fmt.Errorf("the Job rendered no PITR mode")
	}

	res.Outcome = v1alpha1.PointInTimeOutcome(fields["outcome"])
	switch {
	case req.Mode == "latest" && res.Outcome != v1alpha1.PointInTimeArchiveHead,
		req.Mode == "timestamp" && res.Outcome != v1alpha1.PointInTimeTargetCovered && res.Outcome != v1alpha1.PointInTimePartial:
		return nil, fmt.Errorf("PITR verdict outcome %q is not valid in %s mode", res.Outcome, req.Mode)
	case res.Outcome == v1alpha1.PointInTimePartial && (!req.AllowPartial || req.Verification):
		return nil, fmt.Errorf("PITR verdict reports a partial outcome that was not authorized")
	}

	// Archive identity.
	if !pitrarchive.ValidArchiveID(fields["archive"]) {
		return nil, fmt.Errorf("PITR verdict archive %q is malformed", fields["archive"])
	}
	mode := v1alpha1.PITRArchiveEncryptionMode(fields["encryptionMode"])
	if mode != v1alpha1.PITRArchiveEncryptionNone && mode != v1alpha1.PITRArchiveEncryptionPassphrase {
		return nil, fmt.Errorf("PITR verdict encryption mode %q is invalid", mode)
	}
	if req.EncryptionMode != "" && string(mode) != req.EncryptionMode {
		return nil, fmt.Errorf("PITR verdict encryption mode %s differs from the rendered %s", mode, req.EncryptionMode)
	}
	if fields["sourceUID"] == "" || fields["sourceNamespace"] == "" || fields["sourceName"] == "" {
		return nil, fmt.Errorf("PITR verdict names no complete archive source")
	}
	if req.ExpectedSourceUID != "" && fields["sourceUID"] != req.ExpectedSourceUID {
		return nil, fmt.Errorf("PITR verdict source uid %s differs from the backup's source group %s", fields["sourceUID"], req.ExpectedSourceUID)
	}
	archive := &v1alpha1.PITRArchiveIdentity{
		ArchiveID:      fields["archive"],
		Destination:    req.Destination,
		Source:         v1alpha1.PITRArchiveSource{Namespace: fields["sourceNamespace"], Name: fields["sourceName"], UID: types.UID(fields["sourceUID"])},
		EncryptionMode: mode,
	}
	if mode == v1alpha1.PITRArchiveEncryptionPassphrase {
		archive.Algorithm = v1alpha1.EncryptionAlgorithmAES256GCM
	}
	res.Archive = archive

	// Replay shape.
	segments, files, transactions := counts["segments"], counts["files"], counts["transactions"]
	if segments > math.MaxInt32 || files > math.MaxInt32 {
		return nil, fmt.Errorf("PITR verdict counts overflow")
	}
	switch {
	case segments == 0 && (files != 0 || transactions != 0 || fields["lastFile"] != ""):
		return nil, fmt.Errorf("PITR verdict replays files without segments")
	case segments > 0 && (files == 0 || files > segments || transactions < segments):
		return nil, fmt.Errorf("PITR verdict segment, file and transaction counts disagree")
	case segments == 0 && res.Outcome != v1alpha1.PointInTimeTargetCovered:
		return nil, fmt.Errorf("%s PITR verdict replayed no archived transaction", res.Outcome)
	}
	res.SegmentCount = int32(segments)
	res.ReplayedBinlogCount = int32(files)
	res.ReplayedTransactionCount = transactions
	if fields["lastFile"] != "" {
		site, name, ok := strings.Cut(fields["lastFile"], "/")
		if !ok || !pitrSiteName.MatchString(site) || len(site) > 63 || name == "" || len(name) > 255 || strings.Contains(name, "/") {
			return nil, fmt.Errorf("PITR verdict lastFile %q is not <site>/<name>", fields["lastFile"])
		}
		res.LastReplayedBinlog = &v1alpha1.ReplayedBinlogRef{Site: site, Name: name}
	}
	if fields["newest"] != "" {
		newest, err := backupmeta.ParseMicroTime(fields["newest"])
		if err != nil {
			return nil, fmt.Errorf("PITR verdict newest: %w", err)
		}
		mt := metav1.NewMicroTime(newest.Time)
		res.NewestReplayedCommitTime = &mt
	}
	if fields["sites"] != "" {
		for _, site := range strings.Split(fields["sites"], ",") {
			if strings.HasPrefix(site, "+") {
				continue // restore.py's "+N more" marker
			}
			if !pitrSiteName.MatchString(site) || len(site) > 63 {
				return nil, fmt.Errorf("PITR verdict site %q is invalid", site)
			}
			if len(res.ReplaySites) < pitrMaxReplaySites {
				res.ReplaySites = append(res.ReplaySites, site)
			}
		}
	}
	res.ExcludedDivergentTransactionCount = counts["divergent"]
	res.OnHistoryExclusionCount = counts["onHistoryExcluded"]

	// S and its digest.
	if !pitrSHA256.MatchString(fields["executedSha256"]) {
		return nil, fmt.Errorf("PITR verdict executedSha256 is malformed")
	}
	res.GtidExecutedSHA256 = fields["executedSha256"]
	if len(lines) < 2 {
		return nil, fmt.Errorf("PITR verdict has no %s line", pitrResultSentinel)
	}
	result, ok := parseSentinelTokens(lines[1], pitrResultSentinel)
	if !ok || result["v"] != "1" {
		return nil, fmt.Errorf("PITR verdict has no version 1 %s line", pitrResultSentinel)
	}
	if executed, present := result["executed"]; present {
		if !pitrGTIDSetChars.MatchString(executed) || len(executed) > pitrMaxStatusGTIDs {
			return nil, fmt.Errorf("PITR result executed set is malformed")
		}
		sum := sha256.Sum256([]byte(executed))
		if hex.EncodeToString(sum[:]) != res.GtidExecutedSHA256 {
			return nil, fmt.Errorf("PITR result executed set does not match the verdict's executedSha256")
		}
		res.GtidExecuted = executed
	} else if result["executedSha256"] != res.GtidExecutedSHA256 {
		return nil, fmt.Errorf("PITR result carries neither the executed set nor the verdict's digest")
	}
	for _, line := range lines[2:] {
		onHistory, ok := parseSentinelTokens(line, pitrOnHistorySentinel)
		if !ok {
			continue
		}
		set := onHistory["gtids"]
		if res.OnHistoryExclusionCount == 0 || !pitrGTIDSetChars.MatchString(set) || len(set) > pitrMaxStatusGTIDs {
			return nil, fmt.Errorf("PITR on-history exclusions line is malformed")
		}
		res.OnHistoryExclusions = set
	}
	return res, nil
}

// parseSentinelTokens parses "<sentinel> k=v k=v ..." with unique keys.
func parseSentinelTokens(line, sentinel string) (map[string]string, bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != sentinel {
		return nil, false
	}
	tokens := make(map[string]string, len(fields)-1)
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "" {
			return nil, false
		}
		if _, duplicate := tokens[key]; duplicate {
			return nil, false
		}
		tokens[key] = value
	}
	return tokens, true
}
