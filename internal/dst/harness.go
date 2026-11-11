package dst

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const simPollInterval = time.Second

// TrialResult is everything a trial produced.
type TrialResult struct {
	Trial      Trial
	Violations []Violation
	Signature  string
	BaselineOK bool

	PromotionPolls []int
	RestartPolls   []int
	// LostStatePolls are restarts that came up without the durable
	// anti-flap record.
	LostStatePolls []int
	FinalStatus    controller.StatusResponse
	FinalSnapshot  *controller.TopologySnapshot
	// FinalFailoverRecord is the out-of-band anti-flap store's last
	// accepted write.
	FinalFailoverRecord *controller.FailoverRecord
	FinalTruth          []SiteTruth
	// SelfFenced are the sites whose sidecar monitors ended the trial
	// believing they had self-fenced.
	SelfFenced []string
	Events     []Event // populated only in capture mode
	// Planned is the planned-failover actors' report; nil when the trial's
	// actors are disabled.
	Planned *PlannedResult
}

// Failed reports whether any invariant was violated.
func (r TrialResult) Failed() bool { return len(r.Violations) > 0 }

type trialRunner struct {
	trial   Trial
	cluster *Cluster
	dns     *simDNS
	tainter *simTainter
	clk     *clock.FakeClock
	hub     *platform.Hub
	logger  *slog.Logger
	cfg     controller.TopologyConfig
	tm      *controller.TopologyManager

	// persisted simulates the CR status subresource: the last snapshot whose
	// write was not denied. Operator restarts rehydrate from it exactly the
	// way runner.go rehydrates from CR status.
	persisted *controller.TopologySnapshot

	// persistedFailover simulates the out-of-band anti-flap store (the
	// failover group's object annotations in production): the last record
	// whose write was not denied. Its outage knob is separate from the
	// status one, so a trial can deny either path independently.
	persistedFailover *controller.FailoverRecord
	// stateDeniedSincePromotion tracks whether the out-of-band store has an
	// unhealed rejection. Cleared by any accepted write, so it means
	// exactly "the durable record may be behind the last promotion".
	stateDeniedSincePromotion bool
	// statusDroppedPromotion is the status-path analog: it latches when the
	// process that performed the last promotion lands a successful CR
	// status write whose record still predates that promotion — the status
	// path was available, and the operator dropped the record from it. A
	// stale rehydrate is inherent only when NEITHER durable path could have
	// preserved the record; this latch is what catches "status could have".
	statusDroppedPromotion bool
	// promotionArms counts armPromotion calls. A status write is in flight
	// across a promotion's record when the count moves between the write's
	// start (the manager's snapshot) and its commit.
	promotionArms int
	// promotionRestarts is len(restartPolls) at the last promotion. A
	// stale-looking status write from a LATER process generation is
	// legitimate (the replacement never knew the record), so the latch
	// above only arms while the promoting process is still the one running.
	promotionRestarts int
	// replacedBeforeRecord latches when a manager replacement (trials with
	// actors) lands after the last promotion and before any status write
	// carried it. Before DST-1 was fixed the replacement restored from a
	// status that never saw the record; the latch now only tags a
	// regression of that fix (README, Known finding classes).
	replacedBeforeRecord bool
	// retiredRecordPolls are the polls at which an ordered update recorded
	// its handoff on a retired manager; the record is carried into its
	// replacement since DST-1 was fixed, and the tag marks a regression.
	retiredRecordPolls map[int]bool

	// sidecars are the per-site fencing actors (sidecar.go). Each wraps a
	// real sidecar.FencingMonitor.
	sidecars []*simSidecar
	// sidecarTick rotates deterministic sidecar execution order so peer-relay
	// races are sampled from every site first, rather than permanently
	// privileging declaration order.
	sidecarTick int

	promotionPolls []int
	// promotionSpans is the fake-clock window of each promotion's poll in
	// promotionPolls. A poll is one second of fake time unless a slow
	// FLUSH BINARY LOGS took part of the operator's rotation bound inside
	// it, so the cooldown is judged on these, not on poll numbers.
	promotionSpans []clockSpan
	// pollStartAt is the fake clock at the start of the current poll.
	pollStartAt time.Time
	// promotionActors is the actor of each promotion in promotionPolls
	// (trials with actors only).
	promotionActors []Actor
	// promotionRuns is the planned run of each pf promotion in
	// promotionPolls ("" for the other actors).
	promotionRuns []string
	restartPolls  []int
	violations    []Violation
	kindsSeen     map[string]struct{}
	reasonsSeen   map[string]struct{}

	// currentTarget is the failover target the running operator process
	// knows in memory; it reverts to the persisted value on restart.
	currentTarget string
	// lastPromotionAt is the fake-clock instant of the most recent promotion
	// the harness observed, and lostStatePolls the polls at which a restart
	// rehydrated an anti-flap record older than it. Together they separate
	// an inherent cooldown reset (every durable path was down) from a
	// regression (a durable path was up and the operator did not use it).
	lastPromotionAt time.Time
	lostStatePolls  []int

	// rehydrated is the anti-flap record the current operator process
	// started from — what buildManager merged out of the two durable paths.
	rehydrated controller.FailoverRecord

	// operatorDeadUntil is the first poll at which a process killed
	// mid-Execute is replaced; -1 when the operator is alive.
	// pendingCrashDown carries the armed crash's down window until the
	// death actually lands.
	operatorDeadUntil int

	// recloneWait is the pre-reclone archive-tail wait in progress
	// (OpRecloneWait), nil when none; recloneTold is the manager that was
	// last given its request.
	recloneWait      *simRecloneWait
	recloneTold      *controller.TopologyManager
	pendingCrashDown int

	dualStreak  int
	dualFlagged bool
	// flushGrants is FlushAfterWriteGrant's state across polls
	// (invariants.go).
	flushGrants flushGrants

	// pl is the planned-failover actor machinery (planned_harness.go); nil
	// when the trial's actors are disabled, which leaves every code path
	// below exactly as it was before the actors existed.
	pl *plannedHarness
}

// simFailoverStore is the out-of-band anti-flap store: a
// controller.FailoverStateRecorder whose availability is modeled
// independently of simulated CR /status writes.
//
// Called only from the operator's poll goroutine (the promotion paths and
// the per-poll retry), which is the harness goroutine itself, so the
// unsynchronized runner fields it touches are safe. The one production
// caller that is NOT on that goroutine — the ordered-update handoff — cannot
// run here: the trial wires a nil UpdateController.
type simFailoverStore struct{ r *trialRunner }

func (s *simFailoverStore) RecordFailoverState(ctx context.Context, rec controller.FailoverRecord) error {
	if s.r.cluster.OperatorDead() {
		return errOperatorDead
	}
	if s.r.cluster.StateDenied() {
		s.r.cluster.NoteFailoverStateWrite(rec.LastFailoverTarget, true)
		s.r.stateDeniedSincePromotion = true
		return errors.New("mysqlfailovergroups patch forbidden (sim: outage)")
	}
	if s.r.pl != nil {
		// The production recorder over the simulated object: an uncached
		// read, then a merge patch conditioned on its resourceVersion, a
		// no-op when the stored record is not superseded.
		if err := s.r.pl.recordOOB(ctx, rec); err != nil {
			// The path is unavailable (a read outage fails the recorder's
			// uncached read; a conflict storm, an outage): the record may be
			// behind the last promotion, exactly as for a denied write.
			s.r.cluster.NoteFailoverStateWrite(rec.LastFailoverTarget, true)
			s.r.stateDeniedSincePromotion = true
			return err
		}
		if stored, ok := s.r.pl.storedOOB(); ok {
			s.r.persistedFailover = &stored
		}
		s.r.stateDeniedSincePromotion = false
		s.r.cluster.NoteFailoverStateWrite(rec.LastFailoverTarget, false)
		return nil
	}
	annotations, err := controller.FailoverRecordAnnotations(rec)
	if err != nil {
		return err
	}
	cp, err := controller.FailoverRecordFromAnnotations(annotations)
	if err != nil {
		return err
	}
	s.r.persistedFailover = &cp
	s.r.stateDeniedSincePromotion = false
	s.r.cluster.NoteFailoverStateWrite(rec.LastFailoverTarget, false)
	return nil
}

// RunTrial executes one trial. When capture is true the full event log is
// retained and handler (if non-nil) receives operator logs.
func RunTrial(trial Trial, capture bool, handler slog.Handler) (result TrialResult) {
	if handler == nil {
		handler = slog.DiscardHandler
	}
	r := &trialRunner{
		trial:             trial,
		logger:            slog.New(handler),
		kindsSeen:         make(map[string]struct{}),
		reasonsSeen:       make(map[string]struct{}),
		operatorDeadUntil: -1,
	}

	defer func() {
		if p := recover(); p != nil {
			r.violations = append(r.violations, Violation{
				Invariant: "Panic",
				Poll:      -1,
				Detail:    fmt.Sprintf("%v\n%s", p, debug.Stack()),
			})
		}
		result = r.finish()
	}()

	r.setup(capture)
	r.run()
	return result // named result is computed by the deferred finish
}

func (r *trialRunner) setup(capture bool) {
	t := r.trial
	specs := make([]SiteSpec, len(t.SiteNames))
	siteCfgs := make([]controller.SiteTopologyConfig, len(t.SiteNames))
	for i, name := range t.SiteNames {
		specs[i] = SiteSpec{
			Name:           name,
			Host:           name + ".mysql.sim",
			LoadBalancerIP: fmt.Sprintf("10.0.0.%d", i+1),
			UUID:           fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
		}
		siteCfgs[i] = controller.SiteTopologyConfig{
			Name:           name,
			Zone:           "zone-" + name,
			LoadBalancerIP: specs[i].LoadBalancerIP,
			Role:           t.Roles[i],
			TaintSelector:  "sim/site=" + name,
			Host:           specs[i].Host,
		}
	}

	r.cluster = NewCluster(specs, 100)
	r.cluster.SetCapture(capture)
	r.dns = newSimDNS(r.cluster)
	r.tainter = newSimTainter(r.cluster)
	r.clk = clock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	r.cluster.SetClockAdvancer(r.clk.Advance)
	r.hub = platform.NewHub(r.logger)

	r.cfg = controller.TopologyConfig{
		Namespace:         "sim",
		Name:              "sim",
		Sites:             siteCfgs,
		PollInterval:      int64(simPollInterval),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  int64(time.Duration(t.CooldownSec) * time.Second),
		SitePriorities:    t.Priorities,
	}

	if t.SeedHistory {
		// A prior process failed over to the first site long ago. Both
		// durable paths carry it, which is the steady state after a
		// promotion that neither path rejected.
		stamp := r.clk.Now().Add(-2 * time.Hour)
		r.persisted = &controller.TopologySnapshot{
			LastFailover:       stamp,
			LastFailoverTarget: t.SiteNames[0],
		}
		r.persistedFailover = &controller.FailoverRecord{
			LastFailover:       stamp,
			LastFailoverTarget: t.SiteNames[0],
		}
	}

	if t.Actors.Enabled {
		r.pl = newPlannedHarness(r)
	}
	r.buildSidecars()
	r.buildManager()
}

// buildManager constructs a fresh TopologyManager (initial start or an
// operator restart) and rehydrates it from the simulated CR status, mirroring
// runner.go's startup sequence. With actors the start can fail (its
// uncached read of the group failed); it reports whether a manager started.
func (r *trialRunner) buildManager() bool {
	if r.pl != nil {
		return r.pl.startManager()
	}
	r.tm = r.newManager(nil)
	return true
}

// newManager constructs and rehydrates a TopologyManager without making it
// the trial's current one. view is the start's uncached read of the group
// (trials with actors; nil when that read failed and the start proceeds
// without it).
func (r *trialRunner) newManager(view *v1alpha1.MysqlFailoverGroup) *controller.TopologyManager {
	checkers := make([]mysql.Checker, len(r.trial.SiteNames))
	for i, name := range r.trial.SiteNames {
		checkers[i] = r.cluster.NewChecker(name)
	}
	// The trial's clock bounds the failover controller's best-effort binary
	// log rotations, so a slow FLUSH BINARY LOGS is abandoned in fake time.
	fc := controller.NewFailoverControllerWithClock(r.logger, r.clk)
	// The bootstrap controller is real and non-nil, exactly as the runner
	// wires it whenever replication credentials exist — a nil controller
	// with credentials set is a production-impossible combination that
	// would skip the bootstrap-precedence guards in the split-brain paths.
	// CLONE itself stays unreachable in trials: the model never produces an
	// empty site, so no clone goroutine can start and determinism holds.
	// simChecker's clone methods error loudly if that assumption breaks.
	bootstrap := controller.NewBootstrapController(r.logger)
	tm := controller.NewTopologyManagerWithClock(
		r.cfg, checkers, fc, nil, bootstrap,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "sim-pw"},
		r.tainter, r.hub, r.dns, r.logger, r.clk,
	)
	tm.SetSleepForTest(func(time.Duration) {})
	// PITR archiving is "enabled" only while a reclone wait is scripted
	// (OpRecloneWait); otherwise the source answers disabled and a reclone
	// would skip the wait, as on a group without PITR. Production runs
	// each step of the wait (the sidecar status request and the rotation)
	// on a goroutine of its own; here a step runs inline, inside the Poll
	// that started it, so the model sees one actor at a time and a trial
	// replays exactly.
	tm.ArchiveTail = simArchiveTail{r: r}
	tm.SetRecloneTailStepRunnerForSim(func(step func()) { step() })
	tm.ArchiveTailNotArchivedCallback = func(site, reason string) {
		// A wait that reaches its bound goes on to CLONE, which the model
		// does not have: the harness withdraws every request before that.
		r.violations = append(r.violations, Violation{
			Invariant: "HarnessRecloneWaitExpired",
			Poll:      r.cluster.Poll(),
			Detail:    fmt.Sprintf("the pre-reclone wait on %s reached its bound (%s); CLONE is not modelled", site, reason),
		})
	}

	if r.pl != nil {
		// Trials with actors run the production restore (runner.go
		// restoreFailoverState and restorePlannedFailoverState) from an
		// uncached read of the simulated object.
		r.rehydrated = r.pl.restoreManager(tm, &simFailoverStore{r: r}, view)
		r.restoreSites(tm)
		r.wireStatus(tm)
		return tm
	}
	tm.SetFailoverStateRecorder(&simFailoverStore{r: r})

	// Rehydration (runner.go): the anti-flap record comes from whichever
	// durable path is newer — the CR status copy or the out-of-band store —
	// and per-site source convergence and recovery state come from CR status
	// (every site can carry its own recovery marker).
	statusRecord := controller.FailoverRecord{}
	if p := r.persisted; p != nil {
		statusRecord = controller.FailoverRecord{
			LastFailover:       p.LastFailover,
			LastFailoverTarget: p.LastFailoverTarget,
		}
	}
	oobRecord := controller.FailoverRecord{}
	if f := r.persistedFailover; f != nil {
		oobRecord = *f
	}
	rehydrated := controller.NewerFailoverRecord(statusRecord, oobRecord)
	if rehydrated.LastFailoverTarget != "" {
		tm.SetLastFailoverTarget(rehydrated.LastFailoverTarget)
	}
	if !rehydrated.LastFailover.IsZero() {
		tm.SetLastFailover(rehydrated.LastFailover)
	}
	r.rehydrated = rehydrated

	r.restoreSites(tm)
	r.wireStatus(tm)
	return tm
}

// restoreSites restores per-site source convergence and recovery state from
// the simulated CR status (every site can carry its own recovery marker).
func (r *trialRunner) restoreSites(tm *controller.TopologyManager) {
	if p := r.persisted; p != nil {
		for _, site := range p.Sites {
			if site.SourceConvergenceState != "" || site.SourceHost != "" {
				tm.SetSourceConvergence(site.Name, site.SourceHost, site.SourceConvergenceState, site.SourceConvergenceReason)
			}
			switch site.RecoveryState {
			case "RecoveryInProgress":
				tm.SetRecoveryInProgress(site.Name)
			case "RecoveryBlocked":
				tm.SetRecoveryBlockedFrom(site.Name, site.DivergentGtid, site.DivergentTxnCount, site.DivergenceDetectedBy, site.DivergenceComparedTo)
			}
		}
	}
}

// wireStatus sets the manager's StatusCallback: the simulated CR status
// subresource.
func (r *trialRunner) wireStatus(tm *controller.TopologyManager) {
	tm.StatusCallback = func(snap controller.TopologySnapshot) {
		if r.pl != nil && tm != r.tm {
			// As the runner's callback: a replaced manager's status
			// writes are dropped (runner.go startManager).
			return
		}
		if r.cluster.OperatorDead() {
			// A process that no longer exists does not land status writes,
			// however far the dying Poll gets through its own bookkeeping.
			tm.MarkStatusWriteResult(errOperatorDead)
			return
		}
		if r.cluster.StatusDenied() {
			tm.MarkStatusWriteResult(errors.New("mysqlfailovergroups/status write forbidden (sim: outage)"))
			return
		}
		// The snapshot was taken before this callback: the promotions armed
		// so far are the ones it can carry. With actors the write below
		// yields between its read and its commit, and a promotion recorded
		// at that yield (an ordered update's handoff: another goroutine in
		// production) is not in it.
		armsAtSnapshot := r.promotionArms
		if r.pl != nil {
			// The snapshot lands on the simulated object, contending on its
			// resourceVersion with the planned run's writes; the store also
			// carries the planned run's own status outage.
			if err := r.pl.store.MergeSnapshot(snap); err != nil {
				tm.MarkStatusWriteResult(err)
				return
			}
		}
		cp := snap
		r.persisted = &cp
		if snap.DegradedReason != "" {
			r.reasonsSeen[snap.DegradedReason] = struct{}{}
		}
		// A successful status write by the promoting process that still
		// predates its own promotion means the operator dropped the record
		// from a durable path that was accepting writes. Only a snapshot
		// taken after the promotion was recorded can have dropped it: a
		// write already in flight when the record was made commits without
		// it by construction, and the manager's next write (judged here
		// like any other) carries it.
		if !r.lastPromotionAt.IsZero() && cp.LastFailover.Before(r.lastPromotionAt) &&
			len(r.restartPolls) == r.promotionRestarts && armsAtSnapshot == r.promotionArms {
			r.statusDroppedPromotion = true
		}
		if !cp.LastFailover.Before(r.lastPromotionAt) && !r.statusDroppedPromotion {
			r.replacedBeforeRecord = false
		}
		tm.MarkStatusWriteResult(nil)
	}
}

func (r *trialRunner) run() {
	ctx := context.Background()
	ops := r.trial.ActiveOps()
	next := 0

	for p := 0; p < r.trial.Polls; p++ {
		// Open the poll's event window BEFORE applying fault ops so injected
		// faults are stamped with the poll they take effect in — the repro
		// log then matches the schedule's @pNNN annotations exactly.
		r.cluster.BeginPoll(p)
		r.pollStartAt = r.clk.Now()
		if r.pl != nil {
			r.pl.beginPoll(p)
		}

		// Apply scheduled fault ops due at this poll (never during warmup by
		// construction; the shrinker only removes ops).
		for next < len(ops) && ops[next].At <= p {
			r.applyOp(ops[next], p)
			next++
		}
		if r.pl != nil {
			r.pl.applyDueOps(p)
		}
		if p == r.trial.HealAt {
			r.healAll()
		}
		r.driveRecloneWait(p)

		// A process killed mid-Execute is replaced at the start of the poll
		// its down window expires in — before that poll's sidecar ticks, so
		// the sidecars see the operator return exactly when it returns.
		if r.operatorDeadUntil >= 0 && p >= r.operatorDeadUntil {
			r.restartOperator(p)
		}

		r.cluster.Tick()

		if !r.trial.SidecarAfterPoll {
			r.tickSidecars(ctx)
		}
		if !r.cluster.OperatorDead() {
			if r.pl != nil {
				r.pl.pollActors(ctx)
			} else {
				r.tm.Poll(ctx)
			}
		}
		if r.cluster.OperatorDead() && r.operatorDeadUntil < 0 {
			// Died during this poll. The replacement starts Down polls
			// later; until then no Poll runs at all, so the sidecars are
			// the only actors — which is the interaction this fault exists
			// to reach.
			r.operatorDeadUntil = p + 1 + r.pendingCrashDown
			r.pendingCrashDown = 0
		}
		if r.trial.SidecarAfterPoll {
			r.tickSidecars(ctx)
		}

		if r.pl != nil {
			r.pl.endPoll(p)
		}
		st := r.tm.Status()
		events := r.cluster.PollEvents()
		r.checkPoll(p, st, events)
		r.clk.Advance(simPollInterval)

		if p == r.trial.WarmupPolls-1 {
			r.checkBaseline(p, st)
		}
	}

	// A trial that ends with the operator still dead would judge its end
	// state against a cluster nobody is driving. The heal window is
	// generated well before the last poll and Down is bounded, so this is a
	// belt-and-braces guard rather than a live path.
	if r.cluster.OperatorDead() {
		r.violations = append(r.violations, Violation{
			Invariant: "HarnessOperatorNeverReturned",
			Poll:      r.trial.Polls - 1,
			Detail:    "trial ended with the operator process still dead",
		})
	}
}

// clockSpan is the fake-clock window of one poll.
type clockSpan struct{ start, end time.Time }

// simRecloneWait is one scripted pre-reclone wait (OpRecloneWait).
type simRecloneWait struct {
	site    string
	until   int // the poll the request is withdrawn at
	sidecar int // what the site's sidecar answers (Op.K)
}

// driveRecloneWait keeps the scripted reclone request on the current
// manager, as the reclone annotation would after a restart or a manager
// replacement, and withdraws it when its window ends or the faults heal.
func (r *trialRunner) driveRecloneWait(p int) {
	w := r.recloneWait
	if w == nil {
		return
	}
	if p >= w.until || p >= r.trial.HealAt {
		if r.recloneTold != nil {
			r.recloneTold.CancelPendingRecloneForSim(w.site)
		}
		if r.tm != nil && r.tm != r.recloneTold {
			r.tm.CancelPendingRecloneForSim(w.site)
		}
		r.recloneWait, r.recloneTold = nil, nil
		return
	}
	if r.tm != nil && r.tm != r.recloneTold {
		if r.recloneTold != nil {
			r.recloneTold.CancelPendingRecloneForSim(w.site)
		}
		r.tm.SetRecloneSite(w.site)
		r.recloneTold = r.tm
	}
}

// simArchiveTail is the simulated PITR status source of the pre-reclone
// wait (controller.ArchiveTailSource).
type simArchiveTail struct{ r *trialRunner }

// ArchivingEnabled: PITR archiving is "on" while a reclone wait is
// scripted.
func (s simArchiveTail) ArchivingEnabled() bool { return s.r.recloneWait != nil }

// TailStatus is the simulated sidecar's /archiver/status for the
// pre-reclone wait. It never reports the tail archived (the wait must not
// reach CLONE); what it does report is the trial's choice.
func (s simArchiveTail) TailStatus(_ context.Context, site string) (*mysql.ArchiverStatus, error) {
	r := s.r
	w := r.recloneWait
	if w == nil || w.site != site {
		// Only the scripted site's sidecar is modelled.
		return nil, fmt.Errorf("sim: no sidecar status modelled for %s", site)
	}
	if r.cluster.OperatorDead() {
		return nil, errOperatorDead
	}
	var truth SiteTruth
	for _, t := range r.cluster.Truth() {
		if t.Name == site {
			truth = t
		}
	}
	if truth.Crashed || w.sidecar == 0 {
		return nil, fmt.Errorf("dial tcp %s:8080: connect: connection refused", site)
	}
	tail := &mysql.ArchiverTailStatus{State: mysql.ArchiverTailPending, PendingFiles: 1,
		ActiveFile: simBinlogFile(truth.BinlogSeq)}
	switch w.sidecar {
	case 2:
		tail.State, tail.Reason = mysql.ArchiverTailBlocked, "sim: the archive store is unreachable"
	case 3:
		// The sidecar has not scanned since the trial began.
		tail.State, tail.PendingFiles, tail.ActiveFile = mysql.ArchiverTailDrained, 0, simBinlogFile(0)
	}
	return &mysql.ArchiverStatus{Enabled: true, Tail: tail}, nil
}

// restartOperator brings up a replacement process after any kind of death —
// a scheduled restart or a mid-Execute kill — and checks what the
// replacement managed to rehydrate.
func (r *trialRunner) restartOperator(poll int) {
	r.operatorDeadUntil = -1
	r.cluster.ReviveOperator()
	r.cluster.NoteOperatorRestart()
	r.restartPolls = append(r.restartPolls, poll)
	if r.pl != nil {
		r.pl.restartProcess()
	}
	if !r.buildManager() {
		// The start failed; the restart's rehydration is judged when a
		// later start succeeds (plannedHarness.retryStart).
		r.pl.restartCheckPending = true
		return
	}
	r.checkRehydratedAntiFlap(poll)
}

// replacedBeforeRecordMarker tags an AntiFlapStateLost whose record was
// lost to a manager replacement: made on a retired manager, or made before
// the replacement and not yet carried by any status write
// (fingerprint AntiFlapStateLost(replacement): the fixed DST-1, now a
// regression; README, Known finding classes).
const replacedBeforeRecordMarker = "(the record was made on, or not yet written to status by, a replaced manager)"

// checkRehydratedAntiFlap compares what the new process rehydrated against
// the promotion the harness actually observed.
//
// Losing the record is inherent ONLY while neither durable path could have
// preserved it: the out-of-band store has an unhealed rejection AND the
// status path never landed a post-promotion copy either. The operator
// writes the out-of-band record at promotion time and retries it every
// poll, so a healthy store means a current record; and a status write that
// the promoting process landed successfully WITHOUT the record is the
// operator dropping it, not the path failing. Either way the loss is the
// CooldownViolated(restart) class this store closes.
//
// Note the status side needs no "denied" flag of its own: if a successful
// post-promotion status write had carried the record, the rehydrate (the
// newer of the two paths) would not be stale in the first place. The only
// status-path regression a stale rehydrate can hide is the drop latched by
// statusDroppedPromotion.
func (r *trialRunner) checkRehydratedAntiFlap(poll int) {
	if r.lastPromotionAt.IsZero() || !r.rehydrated.LastFailover.Before(r.lastPromotionAt) {
		return
	}
	r.lostStatePolls = append(r.lostStatePolls, poll)
	if r.stateDeniedSincePromotion && !r.statusDroppedPromotion {
		return
	}
	why := "the out-of-band store was accepting writes"
	if r.stateDeniedSincePromotion {
		why = "the promoting process landed a CR status write that dropped the record"
		if r.replacedBeforeRecord {
			why += " " + replacedBeforeRecordMarker
		}
	}
	r.violations = append(r.violations, Violation{
		Invariant: "AntiFlapStateLost",
		Poll:      poll,
		Detail: fmt.Sprintf("restart rehydrated lastFailover=%v (target %q) but a promotion landed at %v and %s",
			r.rehydrated.LastFailover, r.rehydrated.LastFailoverTarget, r.lastPromotionAt, why),
	})
}

// applyOp applies one schedule op to the model (or restarts the operator).
func (r *trialRunner) applyOp(op Op, poll int) {
	switch op.Kind {
	case OpCrash:
		r.cluster.Crash(op.Site)
	case OpRecover:
		r.cluster.Recover(op.Site, op.Fenced, op.BootWritable)
	case OpPartOpSite:
		r.cluster.SetOperatorLink(op.Site, true)
	case OpHealOpSite:
		r.cluster.SetOperatorLink(op.Site, false)
	case OpPartPair:
		r.cluster.SetPairLink(op.Site, op.Peer, true)
	case OpHealPair:
		r.cluster.SetPairLink(op.Site, op.Peer, false)
	case OpPartOneWay:
		r.cluster.SetDirLink(op.Site, op.Peer, true)
	case OpHealOneWay:
		r.cluster.SetDirLink(op.Site, op.Peer, false)
	case OpRestartOperator:
		r.restartOperator(poll)
	case OpCrashOperatorMid:
		// Arming only schedules the death; it lands inside this poll's
		// mutation sequence, wherever the countdown runs out. If the
		// operator issues fewer mutations than the countdown, the arm
		// carries into later polls — which is the point: the crash lands
		// where the operator happens to be, not where the schedule is.
		if r.cluster.ArmOperatorCrash(op.N, op.PreApply) {
			r.pendingCrashDown = op.Down
		}
	case OpAmbiguousMuts:
		r.cluster.AddAmbiguousMutations(op.Site, op.N)
	case OpFailMuts:
		r.cluster.AddFailMutations(op.Site, op.N)
	case OpRecloneWait:
		r.recloneWait = &simRecloneWait{site: op.Site, until: poll + op.N, sidecar: op.K}
		r.recloneTold = nil
	case OpFailFlush:
		r.cluster.AddFlushFailures(op.Site, op.N)
	case OpSlowFlush:
		r.cluster.AddSlowFlushes(op.Site, op.N, op.K == 1)
	case OpStallApply:
		r.cluster.SetApplyStalled(op.Site, true)
	case OpHealApply:
		r.cluster.SetApplyStalled(op.Site, false)
	case OpStallFetch:
		r.cluster.SetFetchStalled(op.Site, true)
	case OpHealFetch:
		r.cluster.SetFetchStalled(op.Site, false)
	case OpDrainStall:
		r.cluster.SetDrainStalled(op.Site, true)
	case OpHealDrain:
		r.cluster.SetDrainStalled(op.Site, false)
	case OpRogueFence:
		r.cluster.RogueFence(op.Site)
	case OpRogueWritable:
		r.cluster.RogueWritable(op.Site)
	case OpStatusOutage:
		r.cluster.SetStatusDenied(true)
	case OpStatusHeal:
		r.cluster.SetStatusDenied(false)
	case OpStateOutage:
		r.cluster.SetStateDenied(true)
	case OpStateHeal:
		r.cluster.SetStateDenied(false)
	case OpDNSOutage:
		r.cluster.SetDNSDenied(true)
	case OpDNSHeal:
		r.cluster.SetDNSDenied(false)
	default:
		panic(fmt.Sprintf("dst: applyOp unhandled kind %q", op.Kind))
	}
}

// healAll clears every outstanding fault: crashed sites boot (with their
// crash op's boot mode), links heal, stalls and outages end, and pending
// injected call errors are discarded.
func (r *trialRunner) healAll() {
	boot := make(map[string]Op)
	for _, op := range r.trial.ActiveOps() {
		if op.Kind == OpCrash {
			boot[op.Site] = op
		}
	}
	for _, s := range r.trial.SiteNames {
		r.cluster.Recover(s, boot[s].Fenced, boot[s].BootWritable)
		r.cluster.SetOperatorLink(s, false)
		r.cluster.SetApplyStalled(s, false)
		r.cluster.SetFetchStalled(s, false)
		r.cluster.SetDrainStalled(s, false)
		for _, o := range r.trial.SiteNames {
			if s < o {
				r.cluster.SetPairLink(s, o, false)
			}
		}
	}
	r.cluster.SetDNSDenied(false)
	r.cluster.SetStatusDenied(false)
	r.cluster.SetStateDenied(false)
	r.cluster.ClearInjectedCallFaults()
	if r.pl != nil {
		r.pl.healAll()
	}
	// Disarm a crash that has not fired yet. An armed countdown surviving
	// into the settle window would kill the operator during the very polls
	// the end-state invariants assume a quiesced, actively-managed cluster.
	r.cluster.DisarmOperatorCrash()
	r.pendingCrashDown = 0
}

// checkBaseline asserts the warmup converged: the first site is the observed
// active primary and every other core site is read-only. A broken baseline is
// a harness defect, not an operator bug.
func (r *trialRunner) checkBaseline(poll int, st controller.StatusResponse) {
	ok := st.ActiveSite == r.trial.SiteNames[0]
	for _, s := range st.Sites {
		if s.Name == r.trial.SiteNames[0] {
			if s.State != v1alpha1.SiteStateWritable {
				ok = false
			}
		} else if s.State != v1alpha1.SiteStateReadOnly {
			ok = false
		}
	}
	if !ok {
		r.violations = append(r.violations, Violation{
			Invariant: "BaselineBroken",
			Poll:      poll,
			Detail:    fmt.Sprintf("post-warmup status: %+v", st),
		})
	}
}

func (r *trialRunner) finish() TrialResult {
	var violations []Violation
	if r.cluster != nil {
		violations = append(violations, r.cluster.Violations()...)
	}
	violations = append(violations, r.violations...)
	violations = append(violations, r.checkEnd()...)
	if r.pl != nil {
		violations = append(violations, r.pl.finish()...)
	}

	res := TrialResult{
		Trial:               r.trial,
		Violations:          violations,
		BaselineOK:          true,
		PromotionPolls:      r.promotionPolls,
		RestartPolls:        r.restartPolls,
		LostStatePolls:      r.lostStatePolls,
		FinalSnapshot:       r.persisted,
		FinalFailoverRecord: r.persistedFailover,
	}
	if r.sidecars != nil {
		res.SelfFenced = r.selfFencedSites()
	}
	if r.tm != nil {
		res.FinalStatus = r.tm.Status()
	}
	for _, v := range violations {
		if v.Invariant == "BaselineBroken" {
			res.BaselineOK = false
		}
	}
	if r.cluster != nil {
		res.FinalTruth = r.cluster.Truth()
		res.Events = r.cluster.Events()
	}
	if r.pl != nil {
		res.Planned = r.pl.result()
	}
	res.Signature = r.signature(res)
	return res
}

// signature summarizes the behaviors this trial exercised. The campaign
// stops when new trials stop producing new signatures.
func (r *trialRunner) signature(res TrialResult) string {
	var b []byte
	appendSorted := func(set map[string]struct{}) {
		keys := make([]string, 0, len(set))
		for k := range set {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b = append(b, k...)
			b = append(b, ',')
		}
		b = append(b, ';')
	}

	// The signature captures observed BEHAVIOR only — which operator actions
	// ran (with outcomes), which topology reasons appeared, how the trial
	// ended. Input shape (site count, roles, fault-op kinds) is deliberately
	// excluded: including it multiplies the signature space by the input
	// space and the saturation rule can never go dry.
	appendSorted(r.kindsSeen)
	appendSorted(r.reasonsSeen)

	promoBucket := len(res.PromotionPolls)
	if promoBucket > 2 {
		promoBucket = 3
	}
	b = append(b, fmt.Sprintf("promos=%d;restarts=%v;", promoBucket, len(res.RestartPolls) > 0)...)

	invs := make(map[string]struct{})
	for _, v := range res.Violations {
		invs[v.Invariant] = struct{}{}
	}
	appendSorted(invs)

	endActive := ""
	if res.FinalSnapshot != nil {
		endActive = string(res.FinalSnapshot.RecoveryState)
	}
	b = append(b, fmt.Sprintf("endRecovery=%s;endActive=%v;", endActive, res.FinalStatus.ActiveSite != "")...)
	if r.pl != nil {
		b = append(b, r.pl.signature()...)
	}
	return string(b)
}

// coreSite reports whether the named site participates in failover-group
// readiness (everything except explicit read-only readers).
func (r *trialRunner) coreSite(name string) bool {
	for i, n := range r.trial.SiteNames {
		if n == name {
			return r.trial.Roles[i] != state.SiteRoleReadOnly
		}
	}
	return false
}
