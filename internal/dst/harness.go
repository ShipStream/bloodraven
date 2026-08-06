package dst

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"time"

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
	// promotionRestarts is len(restartPolls) at the last promotion. A
	// stale-looking status write from a LATER process generation is
	// legitimate (the replacement never knew the record), so the latch
	// above only arms while the promoting process is still the one running.
	promotionRestarts int

	// sidecars are the per-site fencing actors (sidecar.go). Each wraps a
	// real sidecar.FencingMonitor.
	sidecars []*simSidecar
	// sidecarTick rotates deterministic sidecar execution order so peer-relay
	// races are sampled from every site first, rather than permanently
	// privileging declaration order.
	sidecarTick int

	promotionPolls []int
	restartPolls   []int
	violations     []Violation
	kindsSeen      map[string]struct{}
	reasonsSeen    map[string]struct{}

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
	pendingCrashDown  int

	dualStreak  int
	dualFlagged bool
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

func (s *simFailoverStore) RecordFailoverState(_ context.Context, rec controller.FailoverRecord) error {
	if s.r.cluster.OperatorDead() {
		return errOperatorDead
	}
	if s.r.cluster.StateDenied() {
		s.r.cluster.NoteFailoverStateWrite(rec.LastFailoverTarget, true)
		s.r.stateDeniedSincePromotion = true
		return errors.New("mysqlfailovergroups patch forbidden (sim: outage)")
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
			Name: name,
			Host: name + ".mysql.sim",
			LBIP: fmt.Sprintf("10.0.0.%d", i+1),
			UUID: fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1),
		}
		siteCfgs[i] = controller.SiteTopologyConfig{
			Name:          name,
			Zone:          "zone-" + name,
			LBIP:          specs[i].LBIP,
			Role:          t.Roles[i],
			TaintSelector: "sim/site=" + name,
			Host:          specs[i].Host,
		}
	}

	r.cluster = NewCluster(specs, 100)
	r.cluster.SetCapture(capture)
	r.dns = newSimDNS(r.cluster)
	r.tainter = newSimTainter(r.cluster)
	r.clk = clock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
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

	r.buildSidecars()
	r.buildManager()
}

// buildManager constructs a fresh TopologyManager (initial start or an
// operator restart) and rehydrates it from the simulated CR status, mirroring
// runner.go's startup sequence.
func (r *trialRunner) buildManager() {
	checkers := make([]mysql.Checker, len(r.trial.SiteNames))
	for i, name := range r.trial.SiteNames {
		checkers[i] = r.cluster.NewChecker(name)
	}
	fc := controller.NewFailoverController(r.logger)
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

	if p := r.persisted; p != nil {
		for _, site := range p.Sites {
			if site.SourceConvergenceState != "" || site.SourceHost != "" {
				tm.SetSourceConvergence(site.Name, site.SourceHost, site.SourceConvergenceState, site.SourceConvergenceReason)
			}
			switch site.RecoveryState {
			case "RecoveryInProgress":
				tm.SetRecoveryInProgress(site.Name)
			case "RecoveryBlocked":
				tm.SetRecoveryBlocked(site.Name, site.DivergentGtid, site.DivergentTxnCount)
			}
		}
	}

	tm.StatusCallback = func(snap controller.TopologySnapshot) {
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
		cp := snap
		r.persisted = &cp
		if snap.DegradedReason != "" {
			r.reasonsSeen[snap.DegradedReason] = struct{}{}
		}
		// A successful status write by the promoting process that still
		// predates its own promotion means the operator dropped the record
		// from a durable path that was accepting writes.
		if !r.lastPromotionAt.IsZero() && cp.LastFailover.Before(r.lastPromotionAt) &&
			len(r.restartPolls) == r.promotionRestarts {
			r.statusDroppedPromotion = true
		}
		tm.MarkStatusWriteResult(nil)
	}

	r.tm = tm
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

		// Apply scheduled fault ops due at this poll (never during warmup by
		// construction; the shrinker only removes ops).
		for next < len(ops) && ops[next].At <= p {
			r.applyOp(ops[next], p)
			next++
		}
		if p == r.trial.HealAt {
			r.healAll()
		}

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
			r.tm.Poll(ctx)
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

// restartOperator brings up a replacement process after any kind of death —
// a scheduled restart or a mid-Execute kill — and checks what the
// replacement managed to rehydrate.
func (r *trialRunner) restartOperator(poll int) {
	r.operatorDeadUntil = -1
	r.cluster.ReviveOperator()
	r.cluster.NoteOperatorRestart()
	r.restartPolls = append(r.restartPolls, poll)
	r.buildManager()
	r.checkRehydratedAntiFlap(poll)
}

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
		r.cluster.Recover(op.Site, op.Fenced)
	case OpPartOpSite:
		r.cluster.SetOperatorLink(op.Site, true)
	case OpHealOpSite:
		r.cluster.SetOperatorLink(op.Site, false)
	case OpPartPair:
		r.cluster.SetPairLink(op.Site, op.Peer, true)
	case OpHealPair:
		r.cluster.SetPairLink(op.Site, op.Peer, false)
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
	}
}

// healAll clears every outstanding fault: crashed sites boot (with their
// crash op's boot mode), links heal, stalls and outages end, and pending
// injected call errors are discarded.
func (r *trialRunner) healAll() {
	fenced := make(map[string]bool)
	for _, op := range r.trial.ActiveOps() {
		if op.Kind == OpCrash {
			fenced[op.Site] = op.Fenced
		}
	}
	for _, s := range r.trial.SiteNames {
		r.cluster.Recover(s, fenced[s])
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
			if s.State != "writable" {
				ok = false
			}
		} else if s.State != "read-only" {
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
		endActive = res.FinalSnapshot.RecoveryState
	}
	b = append(b, fmt.Sprintf("endRecovery=%s;endActive=%v;", endActive, res.FinalStatus.ActiveSite != "")...)
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
