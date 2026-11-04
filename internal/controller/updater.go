package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// UpdatePhase tracks the current phase of an ordered update.
type UpdatePhase string

const (
	UpdatePhaseNone             UpdatePhase = ""
	UpdatePhaseUpdateReplica    UpdatePhase = "UpdateReplica"
	UpdatePhaseWaitReplica      UpdatePhase = "WaitReplica"
	UpdatePhaseFailover         UpdatePhase = "Failover"
	UpdatePhaseUpdateOldPrimary UpdatePhase = "UpdateOldPrimary"
	UpdatePhaseWaitOldPrimary   UpdatePhase = "WaitOldPrimary"
	UpdatePhaseComplete         UpdatePhase = "Complete"
)

// UpdateController manages ordered rolling updates for a MysqlFailoverGroup.
// It ensures updates are applied replica-first, then failover, then old primary,
// to avoid simultaneous downtime of both DCs.
type UpdateController struct {
	failover *FailoverController
	logger   *slog.Logger

	// tickInterval controls how often waitForReplicaReady / waitForSiteReady poll.
	// Exposed for tests; production code uses 5s.
	tickInterval time.Duration

	// failFastDuration is how long waitForReplicaReady tolerates a writable standby
	// with no replication source before aborting. Derived at call time from
	// tickInterval so the threshold scales with the polling cadence.
	failFastDuration time.Duration

	// primaryLossWindow is how long the active primary may stay unreachable
	// during a follower wait before the ordered update aborts.
	primaryLossWindow time.Duration

	// rolledSiteTimeout bounds each wait for a site ExecuteTargets rolled:
	// its MySQL to replicate and its new pod to be Ready. Exposed for
	// tests; production uses 5 minutes.
	rolledSiteTimeout time.Duration

	// handoff bounds the maintenance handoff of the active site. Zero
	// values use the ExecuteHandoff defaults; tests shorten them.
	handoff HandoffOptions

	mu       sync.Mutex
	phase    UpdatePhase
	updating bool
}

// UpdateTarget describes one site in an N-site ordered rollout.
type UpdateTarget struct {
	Name           string
	Host           string
	Checker        mysql.Checker
	Promotable     bool
	Drifted        bool
	ExpectedSource string
	ReplUser       string
	ReplPassword   string
	UseSSL         bool
	// RotationBlocked is true when the site is mid-keyring-rotation.
	// It may still receive a Deployment update as a follower, but it
	// must not be chosen as the handoff target.
	RotationBlocked bool
	// ReclonePending is true when an in-place restore has not recloned
	// the site yet (it holds pre-restore data). Like RotationBlocked, it
	// excludes the site from the handoff.
	ReclonePending bool
	// DivergenceBlocked is true when the site holds transactions the
	// primary lacks (status.sites[].divergentGtid). Promoting it would make
	// that history authoritative, so it is not a handoff target until it
	// is recloned or the primary contains the set. DivergentTransactions
	// and DivergentGtid describe the recorded set for the refusal message.
	DivergenceBlocked     bool
	DivergentTransactions int64
	DivergentGtid         string
	// PodReady reports whether the site's pod for its current pod
	// template is Ready. The MySQL probes cannot tell: they reach MySQL
	// through the -internal Service, which publishes not-ready addresses,
	// so a pod whose sidecar crash-loops still answers them. Nil turns
	// the pod-readiness gate off for this site; production always sets
	// it together with ApplyUpdate.
	PodReady func(context.Context) (PodReadiness, error)
}

// podReadinessProbeTimeout bounds one pod-readiness probe.
const podReadinessProbeTimeout = 5 * time.Second

// PodNotReadyError reports that the ordered update stopped on a site whose
// pod is not Ready. It rolls no further site and hands nothing off; the
// remaining drift stays queued and the update retries later.
type PodNotReadyError struct {
	Site   string
	Pod    string
	Reason string
	// Rolled is true when this update rolled the site and its new pod did
	// not become Ready afterwards. The site's Deployment carries the new
	// pod template either way, so the site has no spec drift left. False
	// when the site already ran the new template and the update refused
	// to take another site out of service.
	Rolled bool
	// Waited is how long the update waited for the pod; zero when it
	// checked once.
	Waited time.Duration
	// Next names the step the update did not take.
	Next string
	// Err is the underlying failure, if any (a rollout that did not
	// complete, a probe error).
	Err error
}

func (e *PodNotReadyError) Error() string {
	pod := "pod"
	if e.Pod != "" {
		pod = "pod " + e.Pod
	}
	var b strings.Builder
	switch {
	case e.Rolled && e.Waited > 0:
		fmt.Fprintf(&b, "site %s was updated but its %s did not become Ready within %s", e.Site, pod, e.Waited)
	case e.Rolled:
		fmt.Fprintf(&b, "site %s was updated but its %s is not Ready", e.Site, pod)
	default:
		fmt.Fprintf(&b, "site %s already runs the new pod template but its %s is not Ready", e.Site, pod)
	}
	if e.Reason != "" {
		fmt.Fprintf(&b, " (%s)", e.Reason)
	}
	if e.Next != "" {
		fmt.Fprintf(&b, "; not %s", e.Next)
	}
	return b.String()
}

func (e *PodNotReadyError) Unwrap() error { return e.Err }

// NewUpdateController creates a new UpdateController.
func NewUpdateController(failover *FailoverController, logger *slog.Logger) *UpdateController {
	return &UpdateController{
		failover:          failover,
		logger:            logger,
		tickInterval:      5 * time.Second,
		failFastDuration:  30 * time.Second,
		primaryLossWindow: defaultActivePrimaryLossWindow,
		rolledSiteTimeout: 5 * time.Minute,
	}
}

// IsUpdating returns true if an ordered update is in progress.
func (u *UpdateController) IsUpdating() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.updating
}

// Phase returns the current update phase.
func (u *UpdateController) Phase() UpdatePhase {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.phase
}

// Execute performs an ordered update. It should be called when a spec drift is detected.
// activeSiteName and standbySiteName identify which site is currently active/standby.
// standbyChecker and activeChecker are the MySQL connections for each site.
// applyUpdate is a callback that applies the spec change to a specific site's deployment.
//
// Execute is the original two-site path. The operator runs ExecuteTargets;
// only tests call Execute. It has none of ExecuteTargets' later gates (the
// pod-readiness gate, direct-source checks, the old-active GTID check), so
// new callers must use ExecuteTargets.
func (u *UpdateController) Execute(ctx context.Context,
	activeSiteName, standbySiteName string,
	standbyChecker, activeChecker mysql.Checker,
	applyUpdate func(ctx context.Context, siteName string) error) error {

	// Precondition: refuse to start if the standby is not actually replicating.
	// Runs before u.updating=true so an aborted attempt leaves no lock state behind.
	// A probe error is tolerated — the standby may be briefly unreachable; let the
	// rest of the flow discover that rather than adding a new transient-error path.
	// Each probe is bounded independently so a hung MySQL cannot stall the reconciler.
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if ro, err := standbyChecker.CheckReadOnly(probeCtx); err == nil {
		if !ro {
			cancel()
			return fmt.Errorf("precondition: standby %s is writable; refusing to start ordered update", standbySiteName)
		}
		if rs, err := standbyChecker.ShowReplicaStatus(probeCtx); err == nil {
			if rs == nil || !rs.IORunning || !rs.SQLRunning || rs.SourceHost == "" {
				cancel()
				return fmt.Errorf("precondition: standby %s is not replicating", standbySiteName)
			}
		}
	}
	cancel()

	u.mu.Lock()
	if u.updating {
		u.mu.Unlock()
		return fmt.Errorf("update already in progress")
	}
	u.updating = true
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		u.updating = false
		u.phase = UpdatePhaseNone
		u.mu.Unlock()
	}()

	// Phase 1: Update standby site
	u.setPhase(UpdatePhaseUpdateReplica)
	u.logger.Info("ordered update: updating standby", "site", standbySiteName)
	if err := applyUpdate(ctx, standbySiteName); err != nil {
		return fmt.Errorf("update standby deployment: %w", err)
	}

	// Phase 2: Wait for standby to be ready and replication caught up
	u.setPhase(UpdatePhaseWaitReplica)
	u.logger.Info("ordered update: waiting for standby to be ready", "site", standbySiteName)
	if err := u.waitForReplicaReady(ctx, standbyChecker, 5*time.Minute); err != nil {
		return fmt.Errorf("wait for standby ready: %w", err)
	}

	// Phase 3: Hand off to the (now-updated) standby
	u.setPhase(UpdatePhaseFailover)
	u.logger.Info("ordered update: failing over to updated standby", "site", standbySiteName)
	if _, err := u.failover.ExecuteHandoff(ctx, standbyChecker, activeChecker, standbySiteName, u.handoffOptions()); err != nil {
		return fmt.Errorf("failover during update: %w", err)
	}

	// Phase 4: Update the old active (now a standby)
	u.setPhase(UpdatePhaseUpdateOldPrimary)
	u.logger.Info("ordered update: updating old active", "site", activeSiteName)
	if err := applyUpdate(ctx, activeSiteName); err != nil {
		return fmt.Errorf("update old active deployment: %w", err)
	}

	// Phase 5: Wait for old active to be ready
	u.setPhase(UpdatePhaseWaitOldPrimary)
	u.logger.Info("ordered update: waiting for old active to be ready", "site", activeSiteName)
	if err := u.waitForSiteReady(ctx, activeChecker, 5*time.Minute); err != nil {
		u.logger.Warn("old active not ready after update, continuing", "site", activeSiteName, "error", err)
	}

	u.setPhase(UpdatePhaseComplete)
	u.logger.Info("ordered update complete")
	return nil
}

// ExecuteTargets updates every drifted follower sequentially, then updates the
// active site only after handing authority to a healthy promotable standby.
// The returned names completed successfully and may be removed from drift.
//
// A rolled site counts as done only once its MySQL replicates directly from
// the primary and its new pod is Ready. Before it takes any site out of
// service (rolling a follower, handing off the active site) the update also
// requires every site that already runs the new pod template to have a
// Ready pod, so a template that breaks pods stops at the first site it
// broke, in this run or an earlier one. Such a stop returns an error that
// wraps a *PodNotReadyError.
func (u *UpdateController) ExecuteTargets(ctx context.Context, active UpdateTarget, followers []UpdateTarget,
	applyUpdate func(context.Context, string) error, onPromoted func(string, string)) (processed []string, err error) {
	u.mu.Lock()
	if u.updating {
		u.mu.Unlock()
		return nil, fmt.Errorf("update already in progress")
	}
	u.updating = true
	u.mu.Unlock()
	defer func() {
		u.mu.Lock()
		u.updating = false
		u.phase = UpdatePhaseNone
		u.mu.Unlock()
	}()

	onTemplate := sitesOnTemplate(active, followers)
	for _, follower := range followers {
		if !follower.Drifted {
			continue
		}
		if err := u.requireDirectReplica(ctx, follower); err != nil {
			u.logger.Warn("ordered update: retaining unsafe follower drift", "site", follower.Name, "error", err)
			continue
		}
		if err := u.requirePodsReady(ctx, onTemplate, "updating follower "+follower.Name); err != nil {
			return processed, err
		}
		u.setPhase(UpdatePhaseUpdateReplica)
		u.logger.Info("ordered update: updating follower", "site", follower.Name)
		if err := applyUpdate(ctx, follower.Name); err != nil {
			return processed, fmt.Errorf("update follower %s: %w", follower.Name, u.rolloutError(ctx, follower, err))
		}
		u.setPhase(UpdatePhaseWaitReplica)
		if err := u.waitForRolledSite(ctx, follower, follower.ExpectedSource, active.Checker, u.rolledSiteTimeout); err != nil {
			return processed, fmt.Errorf("wait for follower %s: %w", follower.Name, err)
		}
		processed = append(processed, follower.Name)
		onTemplate = append(onTemplate, follower)
	}

	if !active.Drifted {
		u.setPhase(UpdatePhaseComplete)
		return processed, nil
	}

	// The handoff is where the active site leaves service, and the
	// standby it picks must itself be Ready: it runs the new template,
	// either from the start or because it was just rolled.
	if err := u.requirePodsReady(ctx, onTemplate, "handing off the active site "+active.Name); err != nil {
		return processed, err
	}

	processedFollowers := make(map[string]struct{}, len(processed))
	for _, name := range processed {
		processedFollowers[name] = struct{}{}
	}
	var handoff *UpdateTarget
	var rotatingStandby, pendingStandby, divergentStandby []string
	for i := range followers {
		if !followers[i].Promotable {
			continue
		}
		if followers[i].RotationBlocked {
			rotatingStandby = append(rotatingStandby, followers[i].Name)
			continue
		}
		if followers[i].ReclonePending {
			pendingStandby = append(pendingStandby, followers[i].Name)
			continue
		}
		if followers[i].DivergenceBlocked {
			divergentStandby = append(divergentStandby, divergentStandbyMessage(followers[i]))
			continue
		}
		if followers[i].Drifted {
			if _, updated := processedFollowers[followers[i].Name]; !updated {
				continue
			}
		}
		if err := u.requireDirectReplica(ctx, followers[i]); err == nil {
			handoff = &followers[i]
			break
		}
	}
	if handoff == nil {
		if len(rotatingStandby) > 0 {
			return processed, fmt.Errorf("no healthy promotable standby available for active-site update: %s mid-keyring-rotation (UnsealReason=Rotation); finish the rotation before this site can be promoted", strings.Join(rotatingStandby, ", "))
		}
		if len(pendingStandby) > 0 {
			return processed, fmt.Errorf("no healthy promotable standby available for active-site update: %s pending post-restore reclone; wait for status.restoreReclones to drain", strings.Join(pendingStandby, ", "))
		}
		if len(divergentStandby) > 0 {
			return processed, fmt.Errorf("no healthy promotable standby available for active-site update: %s", strings.Join(divergentStandby, "; "))
		}
		return processed, fmt.Errorf("no healthy promotable standby available for active-site update")
	}
	if handoff.Host == "" {
		return processed, fmt.Errorf("promotable standby %s has no replication host", handoff.Name)
	}

	u.setPhase(UpdatePhaseFailover)
	// The active site is healthy, so this is a maintenance handoff, not an
	// emergency: promote only a standby that provably holds every
	// transaction the fenced source committed. On failure the source has its
	// writes back and the active drift stays queued for a later attempt.
	opts := u.handoffOptions()
	opts.RestoreReplication = mysql.ReplicationSourceOpts{
		Host: handoff.ExpectedSource, User: active.ReplUser, Password: active.ReplPassword, UseSSL: active.UseSSL,
	}
	promotionGTID, err := u.failover.ExecuteHandoff(ctx, handoff.Checker, active.Checker, handoff.Name, opts)
	if err != nil {
		return processed, fmt.Errorf("failover during update: %w", err)
	}
	// ExecuteHandoff has already moved MySQL authority. Record that change
	// before the confirmation probe so a transient probe failure cannot leave
	// controller status pointing at the old primary.
	if onPromoted != nil {
		onPromoted(handoff.Name, promotionGTID)
	}
	readOnly, err := handoff.Checker.CheckReadOnly(ctx)
	if err != nil || readOnly {
		return processed, fmt.Errorf("updated standby %s was not confirmed writable after handoff", handoff.Name)
	}
	u.setPhase(UpdatePhaseUpdateOldPrimary)
	if err := applyUpdate(ctx, active.Name); err != nil {
		return processed, fmt.Errorf("update old active %s: %w", active.Name, u.rolloutError(ctx, active, err))
	}
	if err := u.requireOldActiveContained(ctx, active, *handoff); err != nil {
		return processed, err
	}
	if err := u.failover.RecoverOldPrimary(ctx, active.Checker, handoff.Host, active.ReplUser, active.ReplPassword, active.UseSSL); err != nil {
		return processed, fmt.Errorf("configure old active %s as replica of %s: %w", active.Name, handoff.Name, err)
	}
	u.setPhase(UpdatePhaseWaitOldPrimary)
	// The new primary is the source now. If it dies while the old active
	// catches up, end the update so emergency failover is not held off for
	// the full wait.
	if err := u.waitForRolledSite(ctx, active, handoff.Host, handoff.Checker, u.rolledSiteTimeout); err != nil {
		return processed, fmt.Errorf("wait for old active %s to replicate from %s: %w", active.Name, handoff.Name, err)
	}
	processed = append(processed, active.Name)
	u.setPhase(UpdatePhaseComplete)
	return processed, nil
}

// requireOldActiveContained refuses to repoint the old active at the new
// primary unless the new primary has executed every transaction the old
// active holds. ExecuteHandoff already proved that on the fenced set before
// promoting, so this is defense in depth for the step issue #202 went wrong
// at: CHANGE REPLICATION SOURCE on a site with transactions its new source
// lacks leaves them as errant GTIDs on a replica whose threads look
// healthy. The replica GTID audit reports such a replica, but only once the
// update has ended (it does not run during one), and until then the site
// counts as a clean replica. Left fenced with no replication channel, the
// site goes to old-primary recovery instead, which compares the sets and
// reports any divergence (status.sites[].divergentGtid). A failed fence or
// a GTID set that cannot be read or parsed also refuses.
//
// The old active is fenced again before its set is read. Its pod was just
// replaced and mysqld boots with read_only only, so without the fence a
// privileged write could land after the read and before RecoverOldPrimary
// fences it, pass the comparison, and be left as an errant GTID. The fence
// also keeps a refused site fenced.
func (u *UpdateController) requireOldActiveContained(ctx context.Context, active, handoff UpdateTarget) error {
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		return active.Checker.SetSuperReadOnly(probeCtx, true)
	}); err != nil {
		return fmt.Errorf("fence old active %s before repointing it at %s: %w", active.Name, handoff.Name, err)
	}
	oldRaw, oldSet, err := readGTIDSetRaw(ctx, active.Checker)
	if err != nil {
		return fmt.Errorf("read old active %s GTID set before repointing it at %s: %w", active.Name, handoff.Name, err)
	}
	newRaw, newSet, err := readGTIDSetRaw(ctx, handoff.Checker)
	if err != nil {
		return fmt.Errorf("read new primary %s GTID set before repointing %s: %w", handoff.Name, active.Name, err)
	}
	divergent := oldSet.Subtract(newSet)
	if divergent.IsEmpty() {
		return nil
	}
	err = fmt.Errorf("old active %s has %d transactions new primary %s lacks (%s); left fenced for old-primary recovery to report",
		active.Name, divergent.TransactionCount(), handoff.Name, truncateGtidHint(divergent.String()))
	u.logger.Error("ordered update: old active has transactions the new primary lacks; not repointing it",
		"site", active.Name, "newPrimary", handoff.Name,
		"divergentTransactions", divergent.TransactionCount(),
		"divergentGtid", truncateGtidSet(divergent.String()),
		"oldPrimaryGtid", oldRaw,
		"newPrimaryGtid", newRaw,
		"error", err)
	return err
}

// divergentStandbyMessage explains why a standby with recorded divergence
// cannot take the active site's role.
func divergentStandbyMessage(t UpdateTarget) string {
	return fmt.Sprintf("%s is divergent (%d transactions the primary lacks); reclone or reconcile before the active site can be updated",
		t.Name, t.DivergentTransactions)
}

// handoffOptions returns the handoff budgets, watching the source with
// the same loss window as the follower waits.
func (u *UpdateController) handoffOptions() HandoffOptions {
	opts := u.handoff
	if opts.SourceLossWindow <= 0 {
		opts.SourceLossWindow = u.primaryLossWindow
	}
	return opts
}

func (u *UpdateController) requireDirectReplica(ctx context.Context, target UpdateTarget) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ro, err := target.Checker.CheckReadOnly(probeCtx)
	if err != nil {
		return fmt.Errorf("precondition: follower %s probe failed: %w", target.Name, err)
	}
	if !ro {
		return fmt.Errorf("precondition: follower %s is writable", target.Name)
	}
	rs, err := target.Checker.ShowReplicaStatus(probeCtx)
	if err != nil || rs == nil || !rs.IORunning || !rs.SQLRunning || canonicalSourceHost(rs.SourceHost) != canonicalSourceHost(target.ExpectedSource) {
		return fmt.Errorf("precondition: follower %s is not directly replicating from the active primary", target.Name)
	}
	return nil
}

// errActivePrimaryLost is returned by waitForReplicaReadyWatching when the
// replication source stopped answering while a rolled site was waiting to
// catch up. The ordered update must end so cross-site failover, which is
// suppressed for as long as an update runs, can act on the lost primary.
var errActivePrimaryLost = errors.New("active primary became unreachable during ordered update")

// defaultActivePrimaryLossWindow is how long the active primary must stay
// unreachable, across consecutive ticks, before an in-flight follower wait
// gives up. It only has to ride out a probe blip: an aborted update is
// retried from the remaining spec drift, whereas a held update blocks
// failover for up to the follower wait timeout.
const defaultActivePrimaryLossWindow = 10 * time.Second

func (u *UpdateController) waitForReplicaReadyExpected(ctx context.Context, checker mysql.Checker, expected string, timeout time.Duration) error {
	return u.waitForReplicaReadyWatching(ctx, checker, expected, nil, timeout)
}

// waitForReplicaReadyWatching is waitForReplicaReadyExpected plus a probe of
// the active primary the follower is expected to replicate from. A follower
// that is waiting on a dead source can never become ready, and while this
// wait runs the topology manager suppresses cross-site actions to avoid
// failing over because the rolled standby looks unreachable. Without the
// probe a primary that dies mid-update is not failed over (or, for a
// rotation-blocked standby, not even refused) until the 5 minute timeout.
// A nil primary disables the probe.
func (u *UpdateController) waitForReplicaReadyWatching(ctx context.Context, checker mysql.Checker, expected string, primary mysql.Checker, timeout time.Duration) error {
	return u.waitForRolledSite(ctx, UpdateTarget{Checker: checker}, expected, primary, timeout)
}

// waitForRolledSite is waitForReplicaReadyWatching for a site the update
// just rolled, plus its pod when target.PodReady is set: the site is ready
// once its MySQL is a read-only direct replica of expected and its new pod
// has been Ready on two consecutive probes, one tick apart, with no
// container restart in between. One Ready sample is not enough: a
// crash-looping container passes its readiness probe between crashes.
//
// The primary probe runs for the whole wait, so a primary that dies while
// a rolled pod is still starting ends the update as promptly as one that
// dies while MySQL catches up. A pod that is not Ready when the wait times
// out returns a *PodNotReadyError, whatever MySQL reported.
func (u *UpdateController) waitForRolledSite(ctx context.Context, target UpdateTarget, expected string, primary mysql.Checker, timeout time.Duration) error {
	// N consecutive failed probes, one per tick, span (N-1) ticks between
	// the first and last: the first failure opens the window rather than
	// consuming part of it.
	lossThreshold := 2
	if u.tickInterval > 0 {
		if n := int((u.primaryLossWindow+u.tickInterval-1)/u.tickInterval) + 1; n > lossThreshold {
			lossThreshold = n
		}
	}
	lost := 0
	var pod podReadyStreak
	deadline := time.Now().Add(timeout)
	for {
		replicating := directReplicaReady(ctx, target.Checker, expected)
		podReady := true
		if target.PodReady != nil {
			pod.observe(u.probePod(ctx, target))
			podReady = pod.stable()
		}
		if replicating && podReady {
			return nil
		}
		if primary != nil {
			perr := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				_, err := primary.CheckReadOnly(probeCtx)
				return err
			})
			switch {
			case perr == nil:
				lost = 0
			case ctx.Err() != nil:
				// A cancelled update (shutdown, lost leadership) is not
				// evidence about the primary; do not report it as lost.
				return ctx.Err()
			default:
				lost++
				if lost >= lossThreshold {
					return fmt.Errorf("%w: %v", errActivePrimaryLost, perr)
				}
			}
		}
		if time.Now().After(deadline) {
			if !podReady {
				return &PodNotReadyError{Site: target.Name, Pod: pod.last.Pod, Reason: pod.reason(),
					Rolled: true, Waited: timeout, Err: pod.err}
			}
			return fmt.Errorf("timeout waiting for direct replication")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(u.tickInterval):
		}
	}
}

// directReplicaReady reports whether checker is a read-only replica whose
// threads run against expected. It re-fences a follower whose pod
// replacement left it writable and restarts a stopped channel that already
// points at expected.
func directReplicaReady(ctx context.Context, checker mysql.Checker, expected string) bool {
	readOnly, roleErr := checker.CheckReadOnly(ctx)
	if roleErr == nil && !readOnly {
		// MySQL does not persist SET GLOBAL read_only across a pod
		// replacement. Re-fence the updated follower before restarting
		// its retained replication configuration.
		if err := checker.SetSuperReadOnly(ctx, true); err == nil {
			if err := checker.SetReadOnly(ctx, true); err == nil {
				readOnly = true
			}
		}
	}
	rs, err := checker.ShowReplicaStatus(ctx)
	if roleErr != nil || !readOnly || err != nil || rs == nil {
		return false
	}
	if canonicalSourceHost(rs.SourceHost) == canonicalSourceHost(expected) && (!rs.IORunning || !rs.SQLRunning) {
		_ = checker.StartReplica(ctx)
	}
	return rs.IORunning && rs.SQLRunning && canonicalSourceHost(rs.SourceHost) == canonicalSourceHost(expected)
}

// podReadyStreak counts consecutive Ready observations of one container
// run: the same pod with the same restart count.
type podReadyStreak struct {
	last  PodReadiness
	err   error
	ready int
	// restarted records that the pod's restart count moved between the
	// last two observations.
	restarted bool
}

func (s *podReadyStreak) observe(pr PodReadiness, err error) {
	if err != nil {
		s.last, s.err, s.ready, s.restarted = PodReadiness{}, err, 0, false
		return
	}
	s.restarted = s.last.Pod != "" && pr.Pod == s.last.Pod && pr.Restarts != s.last.Restarts
	switch {
	case !pr.Ready:
		s.ready = 0
	case s.ready > 0 && pr.Pod == s.last.Pod && !s.restarted:
		s.ready++
	default:
		s.ready = 1
	}
	s.last, s.err = pr, nil
}

func (s *podReadyStreak) stable() bool { return s.ready >= 2 }

func (s *podReadyStreak) reason() string {
	switch {
	case s.err != nil:
		return "pod readiness unknown: " + s.err.Error()
	case s.last.Ready && s.restarted:
		return "a container restarted between readiness probes"
	case s.last.Ready:
		return "Ready for less than one probe interval"
	}
	return s.last.Reason
}

func (u *UpdateController) probePod(ctx context.Context, target UpdateTarget) (PodReadiness, error) {
	probeCtx, cancel := context.WithTimeout(ctx, podReadinessProbeTimeout)
	defer cancel()
	return target.PodReady(probeCtx)
}

// sitesOnTemplate returns the sites that already run the new pod template
// when an update starts: those without spec drift.
func sitesOnTemplate(active UpdateTarget, followers []UpdateTarget) []UpdateTarget {
	var sites []UpdateTarget
	if !active.Drifted {
		sites = append(sites, active)
	}
	for _, f := range followers {
		if !f.Drifted {
			sites = append(sites, f)
		}
	}
	return sites
}

// requirePodsReady refuses to take another site out of service while a site
// that already runs the new pod template has a pod that is not Ready. A
// template that breaks pods (a sidecar that cannot start, say) leaves MySQL
// answering through the -internal Service, so the MySQL checks alone would
// walk it across the whole group: an earlier attempt's rolled site has no
// drift left and would even be chosen for the handoff. next names the
// refused step for the stall message. A probe error refuses too: the
// update does not roll on an unknown.
func (u *UpdateController) requirePodsReady(ctx context.Context, sites []UpdateTarget, next string) error {
	for _, s := range sites {
		if s.PodReady == nil {
			continue
		}
		pr, err := u.probePod(ctx, s)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return &PodNotReadyError{Site: s.Name, Reason: "pod readiness unknown: " + err.Error(), Next: next, Err: err}
		}
		if !pr.Ready {
			return &PodNotReadyError{Site: s.Name, Pod: pr.Pod, Reason: pr.Reason, Next: next}
		}
	}
	return nil
}

// requireUpdatedPodsReady is the start-of-update form of requirePodsReady:
// the topology manager runs it before it claims the update window, so a
// stalled update that has nothing it may roll does not go through lease
// admission on every retry.
func (u *UpdateController) requireUpdatedPodsReady(ctx context.Context, active UpdateTarget, followers []UpdateTarget) error {
	return u.requirePodsReady(ctx, sitesOnTemplate(active, followers), "rolling another site")
}

// rolloutError turns a rollout that did not complete into a pod-readiness
// stall when the site's pod is what is missing. Other apply failures, and
// any failure once ctx is done, are returned unchanged.
func (u *UpdateController) rolloutError(ctx context.Context, target UpdateTarget, err error) error {
	var incomplete *RolloutIncompleteError
	if target.PodReady == nil || ctx.Err() != nil || !errors.As(err, &incomplete) {
		return err
	}
	pr, perr := u.probePod(ctx, target)
	if perr != nil || pr.Ready {
		return err
	}
	return &PodNotReadyError{Site: target.Name, Pod: pr.Pod, Reason: pr.Reason, Rolled: true, Err: err}
}

func (u *UpdateController) setPhase(p UpdatePhase) {
	u.mu.Lock()
	u.phase = p
	u.mu.Unlock()
}

// waitForReplicaReady waits for the replica to have replication running and caught up.
// It aborts early (~30s) when the standby is observed writable with no replication source,
// since that condition will never satisfy the happy path and holding updating=true blocks
// cross-site split-brain recovery.
func (u *UpdateController) waitForReplicaReady(ctx context.Context, checker mysql.Checker, timeout time.Duration) error {
	// Derive the fail-fast tick threshold from duration ÷ tickInterval so the
	// behaviour is consistent regardless of how tests override the tick cadence.
	failFastThreshold := 1
	if u.tickInterval > 0 {
		failFastThreshold = int((u.failFastDuration + u.tickInterval - 1) / u.tickInterval)
		if failFastThreshold < 1 {
			failFastThreshold = 1
		}
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(u.tickInterval)
	defer ticker.Stop()

	// writableObservations counts ticks where we confirmed the standby is
	// writable. It is deliberately not a strict "streak": probe errors leave
	// it alone rather than resetting, so a stale connection pool whose dial
	// errors alternate with successful "writable with no source" reads
	// cannot pin the counter below the fail-fast threshold and mask a
	// genuinely broken standby until the outer 5-minute deadline.
	writableObservations := 0

	for {
		// Check if MySQL is reachable
		ro, err := checker.CheckReadOnly(ctx)
		if err == nil {
			if !ro {
				// Writable standby counts regardless of whether
				// ShowReplicaStatus succeeds: cross-site recovery is
				// suppressed while the update runs, so nothing will start
				// the replica for us once the node is writable. The data
				// directory often retains master.info across a pod rollout,
				// leaving SourceHost populated with threads stopped — that
				// shape matters just as much as "no source at all" and
				// must also drive the fail-fast abort. Skipping the
				// replication probe here also avoids letting an
				// intermittent ShowReplicaStatus error swallow the !ro
				// observation and pin the counter below the threshold.
				writableObservations++
				if writableObservations >= failFastThreshold {
					return fmt.Errorf("standby is writable but replication is not running; aborting ordered update")
				}
			} else {
				// Read-only: check replication progress. A successful probe
				// that shows threads still starting up is real progress, so
				// reset the counter. A ShowReplicaStatus error with ro=true
				// is a probe blip — leave the counter alone and retry.
				rs, rsErr := checker.ShowReplicaStatus(ctx)
				if rsErr == nil {
					if rs != nil && rs.IORunning && rs.SQLRunning &&
						rs.SecondsBehindSource != nil && *rs.SecondsBehindSource < 5 {
						return nil
					}
					if rs != nil && rs.SourceHost != "" && (!rs.IORunning || !rs.SQLRunning) {
						// A standby pod restart preserves replication metadata but
						// --skip-replica-start intentionally leaves the IO/SQL
						// threads stopped. Ordered updates own this restart window,
						// so restart the existing channel before continuing to wait.
						if err := checker.StartReplica(ctx); err != nil {
							u.logger.Warn("ordered update: failed to start standby replica", "error", err)
						}
					}
					writableObservations = 0
				}
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for replica to be ready")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// waitForSiteReady waits for a site to be reachable.
func (u *UpdateController) waitForSiteReady(ctx context.Context, checker mysql.Checker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(u.tickInterval)
	defer ticker.Stop()

	for {
		_, err := checker.CheckReadOnly(ctx)
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for site to be ready")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
