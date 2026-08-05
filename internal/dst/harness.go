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
	FinalStatus    controller.StatusResponse
	FinalSnapshot  *controller.TopologySnapshot
	FinalTruth     []SiteTruth
	Events         []Event // populated only in capture mode
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

	promotionPolls []int
	restartPolls   []int
	violations     []Violation
	kindsSeen      map[string]struct{}
	reasonsSeen    map[string]struct{}

	// currentTarget is the failover target the running operator process
	// knows in memory; it reverts to the persisted CR value on restart.
	currentTarget string

	dualStreak  int
	dualFlagged bool
}

// RunTrial executes one trial. When capture is true the full event log is
// retained and handler (if non-nil) receives operator logs.
func RunTrial(trial Trial, capture bool, handler slog.Handler) (result TrialResult) {
	if handler == nil {
		handler = slog.DiscardHandler
	}
	r := &trialRunner{
		trial:       trial,
		logger:      slog.New(handler),
		kindsSeen:   make(map[string]struct{}),
		reasonsSeen: make(map[string]struct{}),
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
	r.tainter = newSimTainter()
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
		// A prior process failed over to the first site long ago; the CR
		// carries the durable record.
		r.persisted = &controller.TopologySnapshot{
			LastFailover:       r.clk.Now().Add(-2 * time.Hour),
			LastFailoverTarget: t.SiteNames[0],
		}
	}

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
	// The bootstrap controller stays nil (CLONE is not modeled); replication
	// credentials are still configured so recovery and source convergence —
	// which reuse them — run exactly as in production.
	tm := controller.NewTopologyManagerWithClock(
		r.cfg, checkers, fc, nil, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "sim-pw"},
		r.tainter, r.hub, r.dns, r.logger, r.clk,
	)
	tm.SetSleepForTest(func(time.Duration) {})

	// Rehydration from CR status (runner.go): lastFailoverTarget,
	// lastFailover, per-site source convergence, and the single pending
	// recovery marker.
	if p := r.persisted; p != nil {
		if p.LastFailoverTarget != "" {
			tm.SetLastFailoverTarget(p.LastFailoverTarget)
		}
		if !p.LastFailover.IsZero() {
			tm.SetLastFailover(p.LastFailover)
		}
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
		if r.cluster.StatusDenied() {
			tm.MarkStatusWriteResult(errors.New("mysqlfailovergroups/status write forbidden (sim: outage)"))
			return
		}
		cp := snap
		r.persisted = &cp
		if snap.DegradedReason != "" {
			r.reasonsSeen[snap.DegradedReason] = struct{}{}
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
		// Apply scheduled fault ops due at this poll (never during warmup by
		// construction; the shrinker only removes ops).
		for next < len(ops) && ops[next].At <= p {
			r.applyOp(ops[next], p)
			next++
		}
		if p == r.trial.HealAt {
			r.healAll()
		}

		r.cluster.BeginPoll(p)
		r.cluster.Tick()
		r.tm.Poll(ctx)

		st := r.tm.Status()
		events := r.cluster.PollEvents()
		r.checkPoll(p, st, events)
		r.clk.Advance(simPollInterval)

		if p == r.trial.WarmupPolls-1 {
			r.checkBaseline(p, st)
		}
	}
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
		r.cluster.NoteOperatorRestart()
		r.restartPolls = append(r.restartPolls, poll)
		r.buildManager()
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
	r.cluster.ClearInjectedCallFaults()
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
		Trial:          r.trial,
		Violations:     violations,
		BaselineOK:     true,
		PromotionPolls: r.promotionPolls,
		RestartPolls:   r.restartPolls,
		FinalSnapshot:  r.persisted,
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
