package controller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

const (
	pitrArchiveUnreachablePolls = 3
	// pitrArchiveUnreachableSpan is how long a site that has answered in
	// this operator process may miss polls before SidecarUnreachable.
	pitrArchiveUnreachableSpan = 60 * time.Second
	// pitrArchiveFirstAnswerGrace is the same bound for a site that has
	// not answered yet in this process: a new group's pods, a bootstrap,
	// or a pod moving to another node routinely take minutes. A crash
	// looping sidecar still surfaces once it has passed.
	pitrArchiveFirstAnswerGrace = 5 * time.Minute
	// pitrArchiveErrorPolls is the poll-count debounce for an error from
	// a sidecar that reports neither consecutiveFailedScans nor
	// lastScanAt.
	pitrArchiveErrorPolls = 2
	// pitrArchiveDisabledPolls is how many consecutive enabled=false
	// answers outside a pending rollout make ArchiverDisabled.
	pitrArchiveDisabledPolls = 2
	// pitrArchiveErrorStuckAfter bounds how long the same unconfirmed
	// error may stay visible before it is ArchiverError anyway: a scan
	// that never completes does not advance the scan counters.
	pitrArchiveErrorStuckAfter = 10 * time.Minute
	pitrArchiveRecoveryRounds  = 2
	// pitrArchiveRolloutBound caps how long a disabled sidecar can count
	// as awaiting its ordered update. A rollout that has not delivered
	// archiving by then (blocked on an unhealthy standby, say) is
	// ArchiverDisabled.
	pitrArchiveRolloutBound = 30 * time.Minute
	// pitrArchiveMessageWriteInterval rate-limits status writes whose
	// only change is message text.
	pitrArchiveMessageWriteInterval = 5 * time.Minute

	pitrArchiveAwaitingFirstPollText  = "Waiting for the first archiver status poll from every site"
	pitrArchiveArchivingText          = "Every site reports a running archiver with a ready store and no error"
	pitrArchiveDisabledOnSidecarText  = "sidecar reports PITR archiving disabled although spec.backup.pitr.enabled is true; check that spec.backup.pitr.profileName names an existing profile"
	pitrArchiveAwaitingRolloutText    = "ordered update pending; the pod does not run the PITR configuration yet"
	pitrArchiveRolloutStalledText     = "ordered update still pending after 30m; the pod does not run the PITR configuration (check status.updatePhase and \"ordered update: deferring\" operator logs)"
	pitrArchiveUnreachableDetailTempl = "archiver status unreachable since %s: %s"
	pitrArchiveSidecarOutdatedText    = "sidecar archives without an archive identity (it predates archive identity), so its manifests are unusable for restores; set spec.sidecar.image to this operator's release (or clear it) before archiving to a new prefix"
)

// pitrArchiveFalseReasons is the reason precedence when sites disagree:
// what a live sidecar reports explicitly outranks what the operator
// infers, and the most specific report comes first. The archive refusals
// (D-27) rank after ArchiveStoreInitFailed. The message still lists every
// affected site.
var pitrArchiveFalseReasons = func() []string {
	out := []string{v1alpha1.ReasonArchiveStoreInitFailed}
	for _, r := range pitrarchive.ArchiveReadyReasons {
		out = append(out, string(r))
	}
	return append(out, v1alpha1.ReasonArchiverError, v1alpha1.ReasonSidecarOutdated, v1alpha1.ReasonArchiverDisabled, v1alpha1.ReasonSidecarUnreachable)
}()

// pitrArchiveRefusal returns the D-27 refusal reason a sidecar reports for
// its archive, or "". The history-integrity marker wins, whether persisted
// in the site's manifest or still pending (detected before the manifest
// exists, held only for the pod's lifetime); then the
// archive block's refusal; then a refusal token leading lastError (a
// sidecar whose store could not open the archive).
func pitrArchiveRefusal(s *internalmysql.ArchiverStatus) pitrarchive.RefusalReason {
	if s == nil {
		return ""
	}
	if s.HistoryIntegrity != nil {
		return pitrarchive.ReasonArchiveUnsafeHistory
	}
	if a := s.Archive; a != nil && a.State == pitrarchive.StateRefused {
		for _, r := range pitrarchive.ArchiveReadyReasons {
			if a.Reason == r {
				return r
			}
		}
	}
	if tok, ok := pitrarchive.ParseToken(s.LastError); ok {
		if r, ok := tok.Reason(); ok {
			return r
		}
	}
	return ""
}

// pitrArchiveObservation derives status.pitr.archive from the sidecars'
// archive reports. Only reports for the destination the group's writers
// render count (rendered, Δ14: a held render keeps the group on its
// previous archive, and the observation follows it rather than the spec):
// a pod still on an older render reports another destination, or none. A
// refusal on any counted site wins; Verified needs every site to report
// Verified with one archive ID. ObservedTime moves only when the
// observation changes, so an unchanged archive does not churn status.
func pitrArchiveObservation(rendered pitrRenderInputs, sites []string, snaps, lastGood map[string]*internalmysql.ArchiverStatus, prev *v1alpha1.PITRArchiveObservation, now time.Time) *v1alpha1.PITRArchiveObservation {
	if !rendered.Enabled {
		return nil
	}
	dest := rendered.Destination
	out := &v1alpha1.PITRArchiveObservation{
		Destination:    dest,
		EncryptionMode: v1alpha1.EncryptionMode(rendered.EncryptionMode),
		State:          v1alpha1.PITRArchiveAwaitingDescriptor,
	}
	var (
		refusal   pitrarchive.RefusalReason
		refused   bool
		verified  int
		ids, keys = map[string]bool{}, map[string]bool{}
	)
	for _, site := range sites {
		s := snaps[site]
		if s == nil {
			s = lastGood[site]
		}
		if s == nil || s.Archive == nil || s.Archive.Destination != dest {
			continue
		}
		a := s.Archive
		if r := pitrArchiveRefusal(s); r != "" || a.State == pitrarchive.StateRefused {
			refused = true
			if r != "" && (refusal == "" || slices.Index(pitrarchive.ArchiveReadyReasons, r) < slices.Index(pitrarchive.ArchiveReadyReasons, refusal)) {
				refusal = r
			}
		}
		if a.State == pitrarchive.StateVerified && a.ArchiveID != "" {
			verified++
			ids[a.ArchiveID] = true
			if a.EncryptionMode != "" {
				out.EncryptionMode = v1alpha1.EncryptionMode(a.EncryptionMode)
			}
		}
		keys[a.KeyRevision] = true
	}
	if len(ids) == 1 {
		for id := range ids {
			out.ArchiveID = id
		}
	}
	if len(keys) == 1 {
		for k := range keys {
			out.KeyRevision = k
		}
	}
	switch {
	case refused:
		out.State = v1alpha1.PITRArchiveRefused
		out.Reason = v1alpha1.PITRArchiveRefusalReason(refusal)
	case len(ids) > 1:
		// Two verified archives at one destination cannot both be this
		// group's; nothing is certified.
		out.State, out.ArchiveID = v1alpha1.PITRArchiveRefused, ""
	case verified == len(sites) && verified > 0:
		out.State = v1alpha1.PITRArchiveVerified
	}
	if prev != nil && prev.ObservedTime != nil {
		same := *prev
		same.ObservedTime = nil
		if same == *out {
			out.ObservedTime = prev.ObservedTime
			return out
		}
	}
	t := metav1.NewMicroTime(now.UTC().Truncate(time.Microsecond))
	out.ObservedTime = &t
	return out
}

// archiverPollResult is one site's outcome in one poll round. status is
// nil exactly when err is set. awaitingRollout is set when the site's
// ordered update is still pending.
type archiverPollResult struct {
	site            string
	status          *internalmysql.ArchiverStatus
	err             error
	awaitingRollout bool
}

// pitrArchiveVerdict is the PITRArchiveReady condition the poller's
// observations support.
type pitrArchiveVerdict struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

// pitrSiteHealth classifies one site after its latest poll.
type pitrSiteHealth int

const (
	pitrSiteClean pitrSiteHealth = iota
	// pitrSitePending is an observation that establishes nothing yet: a
	// first missed poll, a first failed scan, a store still initializing.
	pitrSitePending
	// pitrSiteRollout is a site reporting archiving disabled while its
	// ordered update is still pending.
	pitrSiteRollout
	pitrSiteBad
)

type pitrSiteHistory struct {
	// answered records that the site's sidecar has answered at least
	// once in this operator process.
	answered    bool
	misses      int
	firstMissAt time.Time

	// The current run of polls reporting a ready store with lastError.
	errorPolls  int
	errorSince  time.Time
	errorScanAt time.Time
	// disabledPolls counts consecutive enabled=false answers outside a
	// pending rollout.
	disabledPolls int
	// rolloutSince is the first enabled=false answer while the site's
	// rollout was pending, in the current run of such answers.
	rolloutSince time.Time
	// outdatedPolls and outdatedRolloutSince are the same two counters
	// for an enabled archiver without an archive identity block.
	outdatedPolls        int
	outdatedRolloutSince time.Time

	health pitrSiteHealth
	reason string // set when health == pitrSiteBad
	detail string
}

func (h *pitrSiteHistory) resetError() {
	h.errorPolls, h.errorSince, h.errorScanAt = 0, time.Time{}, time.Time{}
}

// pitrArchiveTracker turns completed poll rounds into the
// PITRArchiveReady verdict. It counts poll rounds and archiver scans,
// never reconciles or status writes, so the debounce means what it says.
// The archiverPoller guards it with its mutex.
//
// Rules:
//   - Any site with storeReady=false and an error is False at once; it is
//     sticky on the sidecar, so it cannot flap. enabled=false is False on
//     the second consecutive poll, unless the PITR spec renders an
//     archiver and the site's ordered update is still pending (or its
//     drift is not computed yet), which is Unknown/AwaitingRollout for
//     at most pitrArchiveRolloutBound.
//   - A ready store with a lastError is False once two consecutive
//     archiver scans failed (consecutiveFailedScans >= 2, or a later
//     lastScanAt still erroring from an older sidecar, or two polls from a
//     sidecar that reports neither). The same error unconfirmed for
//     pitrArchiveErrorStuckAfter (at least three scan intervals) counts
//     too.
//   - A failed poll is False/SidecarUnreachable after
//     pitrArchiveUnreachablePolls consecutive misses spanning at least
//     pitrArchiveUnreachableSpan, or pitrArchiveFirstAnswerGrace for a
//     site that has not answered in this process.
//   - Anything short of those keeps the established verdict and resets
//     recovery progress: a single miss never clears a known error and
//     never turns False into True.
//   - False returns to True after pitrArchiveRecoveryRounds consecutive
//     clean rounds; Unknown needs one.
type pitrArchiveTracker struct {
	sites       map[string]*pitrSiteHistory
	verdict     pitrArchiveVerdict
	cleanRounds int
	// anchored is set once the verdict has been reconciled with a
	// condition read from the API server (see anchor).
	anchored bool
	// fromSeed marks a False verdict that came from a persisted
	// condition rather than this tracker's own observations; ownVerdict
	// is what those observations supported before it was adopted.
	fromSeed   bool
	ownVerdict pitrArchiveVerdict
	// scanInterval is the sidecar archiver's scan interval.
	scanInterval time.Duration
}

// newPITRArchiveTracker starts at Unknown. A persisted False is carried
// over, so a restart neither hides a known failure nor re-fires its
// Event, and recovery still needs two clean rounds. A persisted True is
// not: this process has not observed it. The tracker is not anchored.
func newPITRArchiveTracker(persisted *metav1.Condition) *pitrArchiveTracker {
	t := &pitrArchiveTracker{scanInterval: defaultPITRArchivePollInterval}
	t.reset()
	if persisted != nil && persisted.Status == metav1.ConditionFalse {
		t.ownVerdict = t.verdict
		t.verdict = pitrArchiveVerdict{Status: metav1.ConditionFalse, Reason: persisted.Reason, Message: persisted.Message}
		t.fromSeed = true
	}
	return t
}

func (t *pitrArchiveTracker) reset() {
	t.sites = map[string]*pitrSiteHistory{}
	t.cleanRounds = 0
	t.verdict = pitrArchiveVerdict{Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonAwaitingFirstPoll, Message: pitrArchiveAwaitingFirstPollText}
	t.fromSeed, t.ownVerdict = false, t.verdict
}

func (t *pitrArchiveTracker) current() pitrArchiveVerdict { return t.verdict }

// anchor reconciles the verdict, once, with the condition persisted on
// the group, read uncached (seedPITRArchiveTracker or
// anchorPITRArchiveTracker). In both directions:
//   - a persisted False is adopted unless this tracker has observed its
//     own False, so the next write cannot replace it with Unknown, nor
//     with True after a single clean round;
//   - a False this tracker only inherited (a seed) is dropped, back to
//     what its own observations support, when the persisted condition is
//     no longer False, so a recovered failure is not written back.
//
// Own observations and clean-round progress are kept either way.
func (t *pitrArchiveTracker) anchor(persisted *metav1.Condition) {
	if t.anchored {
		return
	}
	t.anchored = true
	switch {
	case persisted != nil && persisted.Status == metav1.ConditionFalse:
		if t.verdict.Status != metav1.ConditionFalse {
			t.ownVerdict = t.verdict
			t.verdict = pitrArchiveVerdict{Status: metav1.ConditionFalse, Reason: persisted.Reason, Message: persisted.Message}
			t.fromSeed = true
		}
	case t.fromSeed:
		t.verdict = t.ownVerdict
		t.fromSeed = false
	}
}

func (t *pitrArchiveTracker) errorStuckAfter() time.Duration {
	return max(pitrArchiveErrorStuckAfter, 3*t.scanInterval)
}

// observe records one completed poll round taken at now.
func (t *pitrArchiveTracker) observe(now time.Time, round []archiverPollResult) {
	if len(round) == 0 {
		return
	}
	pending := false
	var bad, rollout []*pitrSiteHistory
	var badSites, rolloutSites []string
	for _, r := range round {
		h := t.sites[r.site]
		if h == nil {
			h = &pitrSiteHistory{}
			t.sites[r.site] = h
		}
		h.classify(now, r, t.errorStuckAfter())
		switch h.health {
		case pitrSitePending:
			pending = true
		case pitrSiteRollout:
			rollout = append(rollout, h)
			rolloutSites = append(rolloutSites, r.site)
		case pitrSiteBad:
			bad = append(bad, h)
			badSites = append(badSites, r.site)
		}
	}

	if len(bad) > 0 {
		t.cleanRounds = 0
		t.fromSeed = false
		t.verdict = pitrArchiveVerdict{
			Status:  metav1.ConditionFalse,
			Reason:  pitrArchiveTopReason(bad),
			Message: pitrArchiveSiteMessage(badSites, bad),
		}
		return
	}
	if len(rollout) > 0 {
		t.cleanRounds = 0
		// A known failure stays False until it is seen to recover.
		if t.verdict.Status != metav1.ConditionFalse {
			t.fromSeed = false
			t.verdict = pitrArchiveVerdict{
				Status:  metav1.ConditionUnknown,
				Reason:  v1alpha1.ReasonAwaitingRollout,
				Message: pitrArchiveSiteMessage(rolloutSites, rollout),
			}
		}
		return
	}
	if pending {
		t.cleanRounds = 0
		return
	}
	t.cleanRounds++
	if t.verdict.Status == metav1.ConditionFalse && t.cleanRounds < pitrArchiveRecoveryRounds {
		if t.fromSeed {
			// Unknown would have certified True on this round.
			t.ownVerdict = pitrArchiveVerdict{Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonArchiving, Message: pitrArchiveArchivingText}
		}
		return
	}
	t.fromSeed = false
	t.verdict = pitrArchiveVerdict{Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonArchiving, Message: pitrArchiveArchivingText}
}

func (h *pitrSiteHistory) classify(now time.Time, r archiverPollResult, errorStuckAfter time.Duration) {
	h.reason, h.detail = "", ""
	if r.err != nil || r.status == nil {
		if h.misses == 0 {
			h.firstMissAt = now
		}
		h.misses++
		h.resetError()
		h.disabledPolls = 0
		errText := "no response"
		if r.err != nil {
			errText = r.err.Error()
		}
		h.detail = fmt.Sprintf(pitrArchiveUnreachableDetailTempl, h.firstMissAt.UTC().Format(time.RFC3339), errText)
		span := pitrArchiveUnreachableSpan
		if !h.answered {
			span = pitrArchiveFirstAnswerGrace
		}
		if h.misses >= pitrArchiveUnreachablePolls && now.Sub(h.firstMissAt) >= span {
			h.health, h.reason = pitrSiteBad, v1alpha1.ReasonSidecarUnreachable
		} else {
			h.health = pitrSitePending
		}
		return
	}
	h.misses = 0
	h.answered = true
	s := r.status
	if s.Enabled {
		h.rolloutSince = time.Time{}
	}
	if !s.Enabled || s.Archive != nil {
		h.outdatedPolls, h.outdatedRolloutSince = 0, time.Time{}
	}
	switch {
	case !s.Enabled:
		h.resetError()
		if r.awaitingRollout {
			h.disabledPolls = 0
			if h.rolloutSince.IsZero() {
				h.rolloutSince = now
			}
			if now.Sub(h.rolloutSince) < pitrArchiveRolloutBound {
				h.health, h.detail = pitrSiteRollout, pitrArchiveAwaitingRolloutText
				return
			}
			h.health, h.reason, h.detail = pitrSiteBad, v1alpha1.ReasonArchiverDisabled, pitrArchiveRolloutStalledText
			return
		}
		h.rolloutSince = time.Time{}
		// Two polls, so a spec change the runner has not turned into a
		// pending rollout yet (it checks drift every 30s) is not reported
		// as ArchiverDisabled first.
		h.disabledPolls++
		h.detail = pitrArchiveDisabledOnSidecarText
		if h.disabledPolls < pitrArchiveDisabledPolls {
			h.health = pitrSitePending
			return
		}
		h.health, h.reason = pitrSiteBad, v1alpha1.ReasonArchiverDisabled
	case pitrArchiveRefusal(s) != "":
		// An archive refusal is an explicit verdict, reported at once,
		// ahead of the store state it usually comes with: a refused
		// descriptor or a token-led lastError holds until the archive is
		// replaced, and a history marker is either persisted in the
		// site's manifest (permanent for the prefix) or pending (detected
		// before the manifest exists and held for the pod's lifetime: a
		// restart loses it, and it is detected again only while the
		// setting is still in effect).
		h.resetError()
		h.disabledPolls = 0
		h.health, h.reason = pitrSiteBad, string(pitrArchiveRefusal(s))
		h.detail = s.LastError
		if h.detail == "" && s.HistoryIntegrity != nil {
			h.detail = fmt.Sprintf("history integrity marker %s: %s", s.HistoryIntegrity.Violation, s.HistoryIntegrity.Detail)
		}
		if h.detail == "" {
			h.detail = "archive refused: " + h.reason
		}
	case s.StoreReady != nil && !*s.StoreReady:
		h.resetError()
		h.disabledPolls = 0
		if s.LastError == "" {
			// The sidecar's first construction attempt is still running.
			h.health = pitrSitePending
			return
		}
		h.health, h.reason, h.detail = pitrSiteBad, v1alpha1.ReasonArchiveStoreInitFailed, s.LastError
	case s.Archive == nil:
		// F003: an enabled archiver that reports no archive identity
		// block predates archive identity (an older spec.sidecar.image pin).
		// It writes manifests without an archive ID that no restore of
		// this release accepts, so it is not archiving for PITR however
		// healthy it looks. Handled like a disabled archiver: Unknown
		// while its ordered update is pending (for at most
		// pitrArchiveRolloutBound), else False on the second poll.
		h.resetError()
		h.disabledPolls = 0
		if r.awaitingRollout {
			h.outdatedPolls = 0
			if h.outdatedRolloutSince.IsZero() {
				h.outdatedRolloutSince = now
			}
			if now.Sub(h.outdatedRolloutSince) < pitrArchiveRolloutBound {
				h.health, h.detail = pitrSiteRollout, pitrArchiveAwaitingRolloutText
				return
			}
			h.health, h.reason, h.detail = pitrSiteBad, v1alpha1.ReasonSidecarOutdated, pitrArchiveSidecarOutdatedText
			return
		}
		h.outdatedRolloutSince = time.Time{}
		h.outdatedPolls++
		h.detail = pitrArchiveSidecarOutdatedText
		if h.outdatedPolls < pitrArchiveDisabledPolls {
			h.health = pitrSitePending
			return
		}
		h.health, h.reason = pitrSiteBad, v1alpha1.ReasonSidecarOutdated
	case s.LastError != "":
		h.disabledPolls = 0
		if h.errorPolls == 0 {
			h.errorSince, h.errorScanAt = now, s.LastScanAt
		}
		h.errorPolls++
		h.detail = s.LastError
		var confirmed bool
		switch {
		case s.ConsecutiveFailedScans != nil:
			confirmed = *s.ConsecutiveFailedScans >= 2
		case !s.LastScanAt.IsZero():
			// Older sidecar: a later scan that still reports an error.
			confirmed = s.LastScanAt.After(h.errorScanAt)
		default:
			confirmed = h.errorPolls >= pitrArchiveErrorPolls
		}
		if !confirmed && h.errorPolls >= 2 && now.Sub(h.errorSince) >= errorStuckAfter {
			confirmed = true
		}
		if confirmed {
			h.health, h.reason = pitrSiteBad, v1alpha1.ReasonArchiverError
		} else {
			h.health = pitrSitePending
		}
	default:
		h.resetError()
		h.disabledPolls = 0
		h.health = pitrSiteClean
	}
}

func pitrArchiveTopReason(bad []*pitrSiteHistory) string {
	for _, reason := range pitrArchiveFalseReasons {
		for _, h := range bad {
			if h.reason == reason {
				return reason
			}
		}
	}
	return bad[0].reason
}

// pitrArchiveSiteMessage is the sorted "site: detail" list joined with
// "; ", the construction status.pitr.message uses.
func pitrArchiveSiteMessage(sites []string, hist []*pitrSiteHistory) string {
	idx := make([]int, len(sites))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return sites[idx[a]] < sites[idx[b]] })
	parts := make([]string, 0, len(sites))
	for _, i := range idx {
		parts = append(parts, fmt.Sprintf("%s: %s", sites[i], hist[i].detail))
	}
	return strings.Join(parts, "; ")
}

func pitrEnabled(fg *v1alpha1.MysqlFailoverGroup) bool {
	return fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.Enabled
}

// pitrArchiveScanInterval is the sidecar archiver's scan interval for fg.
func pitrArchiveScanInterval(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.ArchivePollInterval != nil &&
		fg.Spec.Backup.PITR.ArchivePollInterval.Duration > 0 {
		return fg.Spec.Backup.PITR.ArchivePollInterval.Duration
	}
	return defaultPITRArchivePollInterval
}

// pitrArchiveHandoff is what a stopped poller passes to its replacement.
type pitrArchiveHandoff struct {
	uid      types.UID
	sites    []string
	tracker  *pitrArchiveTracker
	lastGood map[string]*internalmysql.ArchiverStatus
}

// seedPITRArchiveTracker returns the starting state for a new poller of
// fg's sites. A replacement for a stopped poller of the same group and
// sites continues that poller's history, which is at least as new as
// anything it persisted. Otherwise it starts from the condition read
// from the API server now, after any previous manager has stopped,
// rather than from the runner's earlier, possibly cached, List.
//
// readErr is set when the uncached read failed (or found another
// object); the returned tracker is then unanchored.
func (r *TopologyManagerRunner) seedPITRArchiveTracker(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, sites []string, prev *managedTopology) (tracker *pitrArchiveTracker, lastGood map[string]*internalmysql.ArchiverStatus, readErr error) {
	if prev != nil && prev.archiver != nil && archiverStopped(prev) {
		h := prev.archiver.handoff()
		if h.tracker != nil && h.uid != "" && h.uid == fg.UID && slices.Equal(h.sites, sortedCopy(sites)) {
			// The tracker keeps its anchored state: an ownership change
			// is not an authoritative read. An unanchored predecessor's
			// observations carry over and anchorPITRArchiveTracker still
			// does the read.
			return h.tracker, h.lastGood, nil
		}
	}
	reader := r.pitrArchiveReader()
	if reader == nil {
		return newPITRArchiveTracker(nil), nil, errors.New("no API reader")
	}
	cur, err := readPITRArchiveGroup(ctx, reader, nn, fg.UID)
	if err != nil {
		// No authoritative read. The listed copy may be stale in either
		// direction, so start at Unknown and unanchored: the writers leave
		// the persisted condition alone until the poller's writer manages
		// an uncached read (anchorPITRArchiveTracker).
		return newPITRArchiveTracker(nil), nil, err
	}
	t := newPITRArchiveTracker(pitrArchiveCondition(&cur.Status))
	t.anchored = true
	return t, nil, nil
}

// readPITRArchiveGroup reads nn and checks it is the object with uid.
func readPITRArchiveGroup(ctx context.Context, reader client.Reader, nn types.NamespacedName, uid types.UID) (*v1alpha1.MysqlFailoverGroup, error) {
	var cur v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, nn, &cur); err != nil {
		return nil, err
	}
	if uid != "" && cur.UID != uid {
		return nil, fmt.Errorf("group UID is %s, this poller's group was %s", cur.UID, uid)
	}
	return &cur, nil
}

// archiverStopped reports whether mt's poller goroutine has returned.
func archiverStopped(mt *managedTopology) bool {
	if mt.archiverDone == nil {
		return false
	}
	select {
	case <-mt.archiverDone:
		return true
	default:
		return false
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// applyPITRArchiveStatus sets status.pitr and the PITRArchiveReady
// condition on fg from the archiver poller's current state. Both status
// writers call it on the object they fetched and are about to write, so
// neither replays a stale copy of the other's result. Without PITR
// enabled both are removed; without a running poller both are left as
// they are.
func (r *TopologyManagerRunner) applyPITRArchiveStatus(ctx context.Context, nn types.NamespacedName, fg *v1alpha1.MysqlFailoverGroup) {
	if !pitrEnabled(fg) {
		fg.Status.PITR = nil
		removeCondition(&fg.Status.Conditions, v1alpha1.ConditionPITRArchiveReady)
		return
	}
	rendered := r.renderedPITR(ctx, fg)
	r.populatePITRStatusFor(nn, fg, rendered)
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok || mt.archiver == nil {
		return
	}
	mt.archiver.observeSpec(fg, pitrRendersArchiver(rendered))
	v, anchored := mt.archiver.archiveCondition()
	if !anchored {
		return
	}
	// A spec.mysqlConf option that would set the permanent history marker
	// is refused at render (PITRMysqlConfAccepted, N6), so this condition
	// reports only what the sidecars observe.
	setCondition(&fg.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionPITRArchiveReady,
		Status:             v.Status,
		ObservedGeneration: fg.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             v.Reason,
		Message:            v.Message,
	})
}

// pitrArchiveCondition returns a copy of the persisted condition, or nil.
func pitrArchiveCondition(status *v1alpha1.MysqlFailoverGroupStatus) *metav1.Condition {
	c := apimeta.FindStatusCondition(status.Conditions, v1alpha1.ConditionPITRArchiveReady)
	if c == nil {
		return nil
	}
	return c.DeepCopy()
}

// pitrArchiveMessageNoise matches per-request identifiers that AWS SDK
// errors embed, which change on every attempt of the same failure.
var pitrArchiveMessageNoise = regexp.MustCompile(`(RequestID|HostID|request id|host id): [^,;]*`)

func pitrArchiveNormalizeMessage(msg string) string {
	return pitrArchiveMessageNoise.ReplaceAllString(msg, "")
}

// pitrArchiveMessageSites returns which of sites a "site: detail; ..."
// message names.
func pitrArchiveMessageSites(msg string, sites []string) []string {
	var out []string
	for _, s := range sites {
		if strings.HasPrefix(msg, s+": ") || strings.Contains(msg, "; "+s+": ") {
			out = append(out, s)
		}
	}
	return out
}

// pitrArchiveWriteNeeded compares what the poller's writer is
// responsible for: the condition, and status.pitr's presence, identity
// and message. immediate covers status, reason, generation, presence,
// archive identity and the set of affected sites; messageOnly is a change confined
// to message text (after removing per-request identifiers), which the
// writer rate-limits. Coverage-only changes ride along with the next
// write.
func pitrArchiveWriteNeeded(before, after *v1alpha1.MysqlFailoverGroupStatus, sites []string) (immediate, messageOnly bool) {
	b, a := pitrArchiveCondition(before), pitrArchiveCondition(after)
	if (b == nil) != (a == nil) {
		return true, false
	}
	if (before.PITR == nil) != (after.PITR == nil) {
		return true, false
	}
	var bMsg, aMsg, bPITRMsg, aPITRMsg string
	if a != nil {
		if a.Status != b.Status || a.Reason != b.Reason || a.ObservedGeneration != b.ObservedGeneration {
			return true, false
		}
		bMsg, aMsg = b.Message, a.Message
	}
	if after.PITR != nil {
		// The archive identity is written when it changes, not with the
		// next coverage write.
		if a, b := after.PITR.Archive, before.PITR.Archive; (a == nil) != (b == nil) ||
			(a != nil && (a.Destination != b.Destination || a.ArchiveID != b.ArchiveID || a.State != b.State || a.Reason != b.Reason ||
				a.KeyRevision != b.KeyRevision || a.EncryptionMode != b.EncryptionMode)) {
			return true, false
		}
		bPITRMsg, aPITRMsg = before.PITR.Message, after.PITR.Message
	}
	if !slices.Equal(pitrArchiveMessageSites(bMsg, sites), pitrArchiveMessageSites(aMsg, sites)) ||
		!slices.Equal(pitrArchiveMessageSites(bPITRMsg, sites), pitrArchiveMessageSites(aPITRMsg, sites)) {
		return true, false
	}
	messageOnly = pitrArchiveNormalizeMessage(bMsg) != pitrArchiveNormalizeMessage(aMsg) ||
		pitrArchiveNormalizeMessage(bPITRMsg) != pitrArchiveNormalizeMessage(aPITRMsg)
	return false, messageOnly
}

// updatePITRArchiveStatus is the poller's status writer, run after every
// completed poll round. The topology writer only runs on topology
// changes, which an archive failure on a healthy cluster never causes.
func (r *TopologyManagerRunner) updatePITRArchiveStatus(ctx context.Context, nn types.NamespacedName) error {
	r.mu.RLock()
	mt := r.managers[nn]
	r.mu.RUnlock()
	var p *archiverPoller
	if mt != nil {
		p = mt.archiver
	}
	now := time.Now()
	if p != nil {
		now = p.now()
		if !p.isAnchored() {
			r.anchorPITRArchiveTracker(ctx, nn, p)
		}
	}
	var (
		prev    *metav1.Condition
		written *v1alpha1.MysqlFailoverGroup
	)
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		written = nil
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		if !pitrEnabled(&fresh) {
			r.resetPITRArchiveTracker(nn)
		}
		before := fresh.Status.DeepCopy()
		prev = pitrArchiveCondition(before)
		r.applyPITRArchiveStatus(ctx, nn, &fresh)
		sites := make([]string, len(fresh.Spec.Sites))
		for i, s := range fresh.Spec.Sites {
			sites[i] = s.Name
		}
		immediate, messageOnly := pitrArchiveWriteNeeded(before, &fresh.Status, sites)
		if !immediate && (!messageOnly || (p != nil && now.Sub(p.lastStatusWrite()) < pitrArchiveMessageWriteInterval)) {
			return nil
		}
		if err := r.client.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		written = &fresh
		return nil
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		r.logger.Warn("update PITR archive status", "fg", nn, "error", err)
		return err
	}
	if written != nil {
		if p != nil {
			p.setLastStatusWrite(now)
		}
		r.emitPITRArchiveEvents(written, prev)
	}
	return nil
}

// renderedPITR is the PITR render fg's writers run (renderedPITRInputs),
// from the site Deployments in the runner's cached client. Without a client,
// or when the list fails, it falls back to the render the group would
// create now: the observation is a label for sidecar reports, never an
// authority.
func (r *TopologyManagerRunner) renderedPITR(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) pitrRenderInputs {
	if r.client == nil {
		return renderedPITRInputs(fg, nil)
	}
	deploys, err := listPITRSiteDeployments(ctx, r.client, fg)
	if err != nil {
		r.logger.Debug("list site deployments for the PITR render failed; using the desired render", "fg", fg.Name, "error", err)
		return renderedPITRInputs(fg, nil)
	}
	return renderedPITRInputs(fg, deploys)
}

// anchorPITRArchiveTracker anchors an unanchored tracker on the group's
// condition read through the uncached reader. Until it succeeds the
// writers do not touch the condition, so neither a stale cached object
// nor a stale listed copy can decide it.
//
// The first failure of an episode logs a WARN and the success that ends
// it an INFO, so a sustained failure logs once. Runs on the poller's
// goroutine only.
func (r *TopologyManagerRunner) anchorPITRArchiveTracker(ctx context.Context, nn types.NamespacedName, p *archiverPoller) {
	reader := r.pitrArchiveReader()
	if reader == nil {
		return
	}
	cur, err := readPITRArchiveGroup(ctx, reader, nn, p.uid)
	if err != nil {
		r.notePITRArchiveReadFailed(nn, p, err)
		return
	}
	p.anchor(pitrArchiveCondition(&cur.Status))
	if p.anchorReadFailing {
		p.anchorReadFailing = false
		r.logger.Info("PITR archive condition read recovered", "fg", nn)
	}
}

// notePITRArchiveReadFailed logs the first failed uncached read of an
// episode.
func (r *TopologyManagerRunner) notePITRArchiveReadFailed(nn types.NamespacedName, p *archiverPoller, err error) {
	if p.anchorReadFailing {
		return
	}
	p.anchorReadFailing = true
	r.logger.Warn("PITR archive condition read failed; leaving the condition as persisted", "fg", nn, "error", err)
}

// pitrArchiveReader is the uncached reader when there is one.
func (r *TopologyManagerRunner) pitrArchiveReader() client.Reader {
	if r.apiReader != nil {
		return r.apiReader
	}
	if r.client != nil {
		return r.client
	}
	return nil
}

func (r *TopologyManagerRunner) resetPITRArchiveTracker(nn types.NamespacedName) {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if ok && mt.archiver != nil {
		mt.archiver.resetArchiveTracker()
	}
}

// emitPITRArchiveEvents fires the PITRArchiveReady edge Events. prev is
// the condition persisted before the write that produced fg, so an
// operator restart or a second writer cannot re-fire an edge. Call only
// after that write succeeded.
func (r *TopologyManagerRunner) emitPITRArchiveEvents(fg *v1alpha1.MysqlFailoverGroup, prev *metav1.Condition) {
	if r.recorder == nil {
		return
	}
	cur := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionPITRArchiveReady)
	if cur == nil {
		return
	}
	if cur.Status == metav1.ConditionFalse && cur.Reason == string(pitrarchive.ReasonArchiveUnsafeHistory) &&
		(prev == nil || prev.Status != metav1.ConditionFalse || prev.Reason != cur.Reason) {
		r.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveUnsafeHistoryDetected,
			"PITR archive history is unsafe: %s", cur.Message)
	}
	switch {
	case cur.Status == metav1.ConditionFalse && (prev == nil || prev.Status != metav1.ConditionFalse):
		r.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveNotReady,
			"PITRArchiveReady is False (%s): %s", cur.Reason, cur.Message)
	case cur.Status == metav1.ConditionTrue && prev != nil && prev.Status == metav1.ConditionFalse:
		r.recorder.Event(fg, corev1.EventTypeNormal, v1alpha1.EventPITRArchiveRecovered,
			"PITRArchiveReady is True again: every site reports a running archiver with a ready store and no error")
	}
}
