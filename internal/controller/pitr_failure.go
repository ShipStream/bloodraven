package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/internal/backupmeta"
)

func pitrFailureReason(message string) string {
	first := len(message)
	primary := ""
	for _, reason := range []string{"PITRBackupBoundUnknown", "PITRCrossSiteOrder", "PITRArchiveSiteOmitted", "PITRTargetNotCovered", "PITRArchiveListFailed", "PITRNoArchivedReplay", "PITRCoverageInvalid"} {
		if index := strings.Index(message, reason); index >= 0 && index < first {
			first = index
			primary = reason
		}
	}
	return primary
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

// pitrVerdict is restore.py's version 1 success record. It is the only
// certificate for a PITR Job and carries no GTID sets, so it fits the
// kubelet's 4096-byte termination message.
type pitrVerdict struct {
	Mode                string
	Stop                string
	TargetCovered       bool
	Partial             bool
	CrossSiteUnverified bool
	NewestEventTime     string
	Files               int32
	LastFile            string
	Sites               []string
}

const pitrVerdictSentinel = "BLOODRAVEN_PITR_VERDICT"

// pitrJobVerdict accepts a verdict only from the main container of a
// Succeeded pod owned by the Job, terminated with exit code 0, and only when
// it matches the rendered mode, stop and override policy.
func pitrJobVerdict(pod *corev1.Pod, job *batchv1.Job) (pitrVerdict, error) {
	var verdict pitrVerdict
	if pod == nil {
		return verdict, fmt.Errorf("the Job's pod is gone, so its PITR verdict cannot be read")
	}
	if pod.Status.Phase != corev1.PodSucceeded {
		return verdict, fmt.Errorf("pod %s is %s, not Succeeded", pod.Name, pod.Status.Phase)
	}
	spec := job.Spec.Template.Spec
	if len(spec.Containers) == 0 {
		return verdict, fmt.Errorf("the Job has no main container")
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
			return verdict, fmt.Errorf("container %s did not terminate successfully", main.Name)
		}
		message, found = strings.TrimSpace(term.Message), true
	}
	if !found {
		return verdict, fmt.Errorf("container %s has no terminated status", main.Name)
	}
	tokens, ok := parsePITRVerdictTokens(message)
	if !ok || tokens["v"] != "1" {
		return verdict, fmt.Errorf("container %s wrote no version 1 PITR verdict", main.Name)
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
	flag := func(key string) (bool, error) {
		raw, err := value(key)
		if err != nil {
			return false, err
		}
		if raw != "true" && raw != "false" {
			return false, fmt.Errorf("PITR verdict has an invalid %s", key)
		}
		return raw == "true", nil
	}
	var err error
	if verdict.Mode, err = value("mode"); err != nil {
		return verdict, err
	}
	if verdict.Stop, err = value("stop"); err != nil {
		return verdict, err
	}
	if verdict.TargetCovered, err = flag("targetCovered"); err != nil {
		return verdict, err
	}
	if verdict.Partial, err = flag("partial"); err != nil {
		return verdict, err
	}
	if verdict.CrossSiteUnverified, err = flag("crossSiteUnverified"); err != nil {
		return verdict, err
	}
	if verdict.NewestEventTime, err = value("newest"); err != nil {
		return verdict, err
	}
	if verdict.LastFile, err = value("lastFile"); err != nil {
		return verdict, err
	}
	files, err := value("files")
	if err != nil {
		return verdict, err
	}
	count, err := strconv.ParseInt(files, 10, 32)
	if err != nil || count < 0 {
		return verdict, fmt.Errorf("PITR verdict has an invalid files count")
	}
	verdict.Files = int32(count)
	sites, err := value("sites")
	if err != nil {
		return verdict, err
	}
	if sites != "" {
		verdict.Sites = strings.Split(sites, ",")
	}

	env := map[string]string{"BLOODRAVEN_PITR_MODE": "timestamp"}
	for _, init := range spec.InitContainers {
		if init.Name != restorePITRInitContainerName {
			continue
		}
		for _, e := range init.Env {
			if e.Value != "" || e.Name != "BLOODRAVEN_PITR_MODE" {
				env[e.Name] = e.Value
			}
		}
	}
	if verdict.Mode != env["BLOODRAVEN_PITR_MODE"] {
		return verdict, fmt.Errorf("PITR verdict mode %q differs from the rendered mode %q", verdict.Mode, env["BLOODRAVEN_PITR_MODE"])
	}
	stop, err := backupmeta.ParsePITRStopTime(env["BLOODRAVEN_PITR_STOP_DATETIME"])
	if err != nil {
		return verdict, fmt.Errorf("rendered PITR stop time: %w", err)
	}
	if want := pitrStopSecond(stop); verdict.Stop != want {
		return verdict, fmt.Errorf("PITR verdict stop %q differs from the rendered stop %q", verdict.Stop, want)
	}
	allowPartial := env["BLOODRAVEN_PITR_ALLOW_PARTIAL"] == "true"
	allowCross := env["BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE"] == "true"
	switch {
	case verdict.Partial && (!allowPartial || verdict.Mode == "latest"):
		return verdict, fmt.Errorf("PITR verdict reports a partial outcome that was not authorized")
	case verdict.CrossSiteUnverified && !allowCross:
		return verdict, fmt.Errorf("PITR verdict reports unverified cross-site replay that was not authorized")
	case verdict.Mode != "latest" && !verdict.TargetCovered && !verdict.Partial:
		return verdict, fmt.Errorf("PITR verdict reports neither coverage nor an authorized partial outcome")
	case verdict.Mode == "latest" && verdict.Files == 0:
		return verdict, fmt.Errorf("latest PITR verdict replayed no archived binlog")
	}
	return verdict, nil
}

func parsePITRVerdictTokens(message string) (map[string]string, bool) {
	fields := strings.Fields(message)
	if len(fields) == 0 || fields[0] != pitrVerdictSentinel {
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

// pitrStopSecond mirrors restore.py: a fractional stop rounds up to the next
// second because binlog event times have one-second resolution.
func pitrStopSecond(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() != 0 {
		t = t.Truncate(time.Second).Add(time.Second)
	}
	return t.Format("2006-01-02 15:04:05")
}
