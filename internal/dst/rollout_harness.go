package dst

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// The paced-rollout simulation (README "Paced rollouts").
//
// Real code: the RolloutPacer (planRollout and its conditional writes), the
// group reconciler with the gate on the Deployment write path
// (gateDeploymentRollout, admitRolloutWrite), the runner's drift check
// (rolloutHoldsSite), the runner's status write, one TopologyManager per
// group with the production FailoverController and UpdateController (the
// ordered updater, its handoff included), and DeploymentUnstableReason on
// the status those managers write.
//
// Modelled: the API server (controller-runtime's fake client behind an
// interceptor that kills the operator process and observes every write),
// the Deployment controller and the kubelet (simPod: a pod that is Ready at
// once, later, or not until it is fixed), each group's MySQL (Cluster), and
// an operator release (rolloutSim.release).

// rolloutStep is the fake time of one step: the pacer's interval and the
// groups' poll interval.
const rolloutStep = 5 * time.Second

// rolloutNamespace is a group's namespace. Each group has its own, as
// tenants do, which also keeps every list the operator makes down to one
// group's objects.
func rolloutNamespace(group string) string { return "fleet-" + group }

// rolloutQuietSteps is how many steps a trial runs on after the fleet has
// converged past the heal: long enough to see a pass of every loop write
// something it should not, without paying for the rest of the settle
// window.
const rolloutQuietSteps = 6

// errAPIOperatorDead is what a dead process's API calls see.
var errAPIOperatorDead = errors.New("dst: operator process died (API call dropped)")

// RolloutResult is what a rollout trial's fleet did.
type RolloutResult struct {
	// Releases is how many operator releases landed.
	Releases int
	// Restarts counts operator processes started after the first; of
	// those, MidRollRestarts started while a group's roll had started and
	// not completed, and WriteCrashes followed a death at an API write.
	Restarts, MidRollRestarts, WriteCrashes int
	// CrashKinds are the kinds of API write a process died at.
	CrashKinds []string
	// Transitions counts the changes of status.rollout by kind (admit,
	// start, stall:<reason>, retarget, complete, defer, withdraw, ...).
	Transitions map[string]int
	// TemplateWrites is how many pod templates the operator changed.
	TemplateWrites int
	// Handoffs is how many ordered updates handed the active site over.
	Handoffs int
	// Completed is how many groups ended with a Complete record.
	Completed int
	// NeedsHuman names the groups that ended with no primary and no
	// authority on record (rolloutSim.needsHuman).
	NeedsHuman []string
	// Log is the step log (capture mode only).
	Log []string
}

// simPod is the pod of one site Deployment.
type simPod struct {
	ready bool
	// readyAt is the step a pod that is not Ready becomes Ready at; -1
	// when only a fix (OpPodReady, the heal) makes it Ready.
	readyAt int
	// armed: the next roll leaves the pod not Ready, for delay steps or,
	// with delay 0, until it is fixed.
	armed bool
	delay int
}

type rolloutGroupSim struct {
	spec RolloutGroup
	key  types.NamespacedName
	cfg  controller.TopologyConfig

	cluster    *Cluster
	dns        *simDNS
	tainter    *simTainter
	identities simSiteIdentityStore
	pods       map[string]*simPod

	tm    *controller.TopologyManager
	apply func(ctx context.Context, site string) error

	// dirty: the group or one of its Deployments was written since its
	// last reconcile by something other than that reconcile. That is the
	// watch event that wakes the next one.
	dirty bool
	// rec is the group's stored status.rollout, kept current by the
	// interceptor so that the invariants need not read it back.
	rec *v1alpha1.RolloutStatus

	// Invariant bookkeeping (rollout_invariants.go).
	admitAnno, releaseAnno bool
	annoUntil              map[string]int
	// manual: the admission in flight was made by the admit annotation,
	// or the release-slot annotation was set during it.
	manual bool
	// licensed: the roll of the admission in flight has started, or it
	// completed in this step; the ordered updater may touch MySQL.
	licensedStep int
	// writes counts the template writes per site since the last release.
	writes map[string]int
	// completedEpoch is the release the group last completed a roll of.
	completedEpoch int
	// everPending: a release made this group's workloads pending.
	everPending bool
}

// rolloutProc is one operator process: everything it holds in memory.
type rolloutProc struct {
	ctx    context.Context
	cancel context.CancelFunc
	rec    *controller.MysqlFailoverGroupReconciler
	runner *controller.TopologyManagerRunner
	pacer  *controller.RolloutPacer
}

type rolloutSim struct {
	trial   Trial
	plan    RolloutPlan
	capture bool
	logger  *slog.Logger
	clk     *clock.FakeClock
	hub     *platform.Hub
	scheme  *runtime.Scheme

	// under is the simulated API server; api is the operator's client to
	// it, which dies with the process and observes its writes.
	under client.WithWatch
	api   client.WithWatch
	// reads serves both from decoded copies (rollout_api.go); every write
	// to under is followed by reads.wrote.
	reads *readCache

	groups  []*rolloutGroupSim
	byName  map[string]*rolloutGroupSim
	deploys map[string]deployRef

	// identity is the running renderer's name; a release renames every
	// stored trace of it instead of changing it (release).
	identity string
	epoch    int
	badBuild bool

	// pollGate is held while a manager's Poll runs: a stall recheck that
	// Poll starts on a goroutine waits at its probe until Poll returned.
	pollGate sync.RWMutex

	// mu guards everything below: the interceptor and the pod probe run
	// on the ordered update's goroutine.
	mu          sync.Mutex
	proc        *rolloutProc
	dead        bool
	deadUntil   int
	countdown   int
	preApply    bool
	crashDown   int
	step        int
	violations  []Violation
	res         RolloutResult
	seen        map[string]struct{}
	deathInStep bool
	// lastState is the last fleet line of the step log (capture mode).
	lastState string
}

type deployRef struct {
	g    *rolloutGroupSim
	site string
}

// quietControllerRuntime gives controller-runtime's package logger a sink
// once: the reconciler logs through it, and without one the library prints
// a warning with a stack trace 30 seconds into the process.
var quietControllerRuntime sync.Once

// runRolloutTrial executes one rollout trial.
func runRolloutTrial(trial Trial, capture bool, handler slog.Handler) (result TrialResult) {
	quietControllerRuntime.Do(func() { ctrllog.SetLogger(logr.Discard()) })
	if handler == nil {
		handler = slog.DiscardHandler
	}
	s := &rolloutSim{
		trial: trial, plan: trial.Rollout, capture: capture, logger: slog.New(handler),
		byName: map[string]*rolloutGroupSim{}, deploys: map[string]deployRef{},
		countdown: -1, deadUntil: -1, seen: map[string]struct{}{},
	}
	s.res.Transitions = map[string]int{}
	defer func() {
		if p := recover(); p != nil {
			s.violate("Panic", fmt.Sprintf("%v\n%s", p, debug.Stack()))
		}
		result = s.finish()
	}()
	s.setup()
	s.run()
	return result
}

func (s *rolloutSim) logf(format string, args ...any) {
	if s.capture {
		s.res.Log = append(s.res.Log, fmt.Sprintf("p%03d ", s.step)+fmt.Sprintf(format, args...))
	}
}

// violate records a violation. Callers hold s.mu or run on the harness
// goroutine with no operator goroutine alive.
func (s *rolloutSim) violate(invariant, detail string) {
	s.violations = append(s.violations, Violation{Invariant: invariant, Poll: s.step, Detail: detail})
	s.logf("VIOLATION %s: %s", invariant, detail)
}

func (s *rolloutSim) setup() {
	s.clk = clock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	s.hub = platform.NewHub(s.logger)
	s.scheme = runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s.scheme); err != nil {
		panic(err)
	}
	if err := v1alpha1.AddToScheme(s.scheme); err != nil {
		panic(err)
	}
	var objs []client.Object
	for gi, spec := range s.plan.Groups {
		objs = append(objs, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-credentials", Namespace: rolloutNamespace(spec.Name)},
			Data: map[string][]byte{
				"username": []byte("bloodraven"), "password": []byte("password"), "MYSQL_ROOT_PASSWORD": []byte("root-password"),
			},
		})
		fg := &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: rolloutNamespace(spec.Name), UID: types.UID("uid-" + spec.Name), Generation: 1},
			Spec: v1alpha1.MysqlFailoverGroupSpec{
				Image:        "mysql:9.7",
				Credentials:  v1alpha1.CredentialsSpec{OperatorSecretName: "mysql-credentials"},
				Sidecar:      v1alpha1.SidecarSpec{Image: "bloodraven-sidecar:sim"},
				PollInterval: &metav1.Duration{Duration: rolloutStep},
			},
		}
		if spec.Recreate {
			fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
		}
		for si, site := range spec.Sites {
			fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{
				Name: site, ServerID: int64(100*(gi+1) + si), Zone: spec.Name + "-" + site,
				LoadBalancerIP:    fmt.Sprintf("10.%d.0.%d", gi+1, si+1),
				TaintNodeSelector: map[string]string{"sim/group": spec.Name, "sim/site": site},
				Storage:           v1alpha1.StorageSpec{Size: "1Gi"},
			})
		}
		cfg, err := controller.CRConfigToTopologyConfig(fg)
		if err != nil {
			panic(err)
		}
		g := &rolloutGroupSim{
			spec: spec, key: client.ObjectKeyFromObject(fg), cfg: cfg,
			pods: map[string]*simPod{}, annoUntil: map[string]int{}, writes: map[string]int{}, licensedStep: -1, completedEpoch: -1,
		}
		specs := make([]SiteSpec, len(cfg.Sites))
		for i, sc := range cfg.Sites {
			specs[i] = SiteSpec{Name: sc.Name, Host: sc.Host, LoadBalancerIP: sc.LoadBalancerIP,
				UUID: fmt.Sprintf("00000000-0000-0000-%04d-%012d", gi+1, i+1)}
			s.deploys["mysql-"+spec.Name+"-"+sc.Name] = deployRef{g: g, site: sc.Name}
		}
		g.cluster = NewCluster(specs, 100)
		g.cluster.SetCapture(false)
		g.cluster.SetClockAdvancer(func(time.Duration) {})
		g.dns = newSimDNS(g.cluster)
		g.tainter = newSimTainter(g.cluster)
		s.groups = append(s.groups, g)
		s.byName[spec.Name] = g
		objs = append(objs, fg)
	}
	s.under = fake.NewClientBuilder().WithScheme(s.scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}, &appsv1.Deployment{}).WithObjects(objs...).Build()
	s.reads = newReadCache(s.under)
	s.api = interceptor.NewClient(s.under, s.interceptorFuncs())

	// The earlier release: it created the workloads, and it is the one
	// whose name the first release takes away from them.
	ctx := context.Background()
	old := &controller.MysqlFailoverGroupReconciler{Client: s.api, APIReader: s.api, Scheme: s.scheme, Recorder: &record.FakeRecorder{}, Now: s.clk.Now}
	for _, g := range s.groups {
		if _, err := old.Reconcile(ctx, ctrl.Request{NamespacedName: g.key}); err != nil {
			panic(fmt.Sprintf("dst: rollout setup: reconcile %s: %v", g.key, err))
		}
		for _, site := range g.spec.Sites {
			d, err := s.deployment(g, site)
			if err != nil {
				panic(fmt.Sprintf("dst: rollout setup: %v", err))
			}
			if s.identity == "" {
				s.identity = d.Annotations[v1alpha1.RenderedByAnnotation]
			}
			if s.identity == "" || d.Annotations[v1alpha1.RenderedByAnnotation] != s.identity {
				panic("dst: rollout setup: a created Deployment carries no renderer")
			}
		}
	}
	s.startProcess()
}

func (s *rolloutSim) deployment(g *rolloutGroupSim, site string) (*appsv1.Deployment, error) {
	var d appsv1.Deployment
	err := s.reads.Get(context.Background(), types.NamespacedName{Namespace: g.key.Namespace, Name: "mysql-" + g.spec.Name + "-" + site}, &d)
	return &d, err
}

func (s *rolloutSim) group(g *rolloutGroupSim) *v1alpha1.MysqlFailoverGroup {
	var fg v1alpha1.MysqlFailoverGroup
	if err := s.reads.Get(context.Background(), g.key, &fg); err != nil {
		panic(fmt.Sprintf("dst: rollout: get %s: %v", g.key, err))
	}
	return &fg
}

// ---------------------------------------------------------------------------
// The operator process
// ---------------------------------------------------------------------------

// startProcess starts an operator process over the API objects and the
// sites as they are: a new reconciler, runner, pacer and topology managers.
// Nothing of the previous process is carried over.
func (s *rolloutSim) startProcess() {
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &record.FakeRecorder{} // a nil channel: events are dropped
	rec := &controller.MysqlFailoverGroupReconciler{
		Client: s.api, APIReader: s.api, Scheme: s.scheme, Recorder: recorder, RolloutPacing: true, Now: s.clk.Now,
	}
	// A pod that is not Ready when its Deployment is written stays so for
	// the rest of the step: one read decides, and the wait ends.
	rec.SetRolloutWaitForSim(time.Millisecond, time.Hour)
	runner := controller.NewTopologyManagerRunner(s.api, nil, s.hub, recorder, s.logger)
	runner.SetRolloutPacing(true)
	soak := time.Duration(-1)
	if s.plan.SoakSteps > 0 {
		soak = time.Duration(s.plan.SoakSteps) * rolloutStep
	}
	pacer := controller.NewRolloutPacer(s.api, s.api, recorder, s.logger, rec, nil, controller.RolloutPacerConfig{
		Soak:       soak,
		StartGrace: time.Duration(s.plan.StartGraceSteps) * rolloutStep,
		StallAfter: time.Duration(s.plan.StallAfterSteps) * rolloutStep,
	})
	pacer.SetClock(s.clk.Now)
	proc := &rolloutProc{ctx: ctx, cancel: cancel, rec: rec, runner: runner, pacer: pacer}

	s.mu.Lock()
	s.proc, s.dead, s.deadUntil = proc, false, -1
	s.mu.Unlock()

	for _, g := range s.groups {
		g := g
		g.cluster.ReviveOperator()
		checkers := make([]mysql.Checker, len(g.cfg.Sites))
		for i, sc := range g.cfg.Sites {
			checkers[i] = g.cluster.NewChecker(sc.Name)
		}
		fc := controller.NewFailoverControllerWithClock(s.logger, s.clk)
		uc := controller.NewUpdateController(fc, s.logger)
		uc.SetTickIntervalForTest(time.Millisecond)
		uc.SetRolledSiteTimeoutForTest(3 * time.Millisecond)
		tm := controller.NewTopologyManagerWithClock(g.cfg, checkers, fc, uc, controller.NewBootstrapController(s.logger),
			controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "sim-pw"}, g.tainter, s.hub, g.dns, s.logger, s.clk)
		tm.SetSleepForTest(func(time.Duration) {})
		tm.SetSiteIdentityStore(&g.identities)
		_ = tm.LoadSiteIdentities(ctx)
		tm.SetRecloneRunnerForSim(g.cluster.Reclone)
		tm.SetRecloneTailStepRunnerForSim(func(step func()) { step() })
		fg := s.group(g)
		// As the runner's startManager: the failover record and the observed
		// authority come back from the group's status and annotations.
		tm.RestoreFailoverStateForSim(fg, controller.NewAnnotationFailoverStateRecorderWithClock(s.api, s.api, g.key, s.clk))
		tm.SetAppliedGeneration(fg.Generation)
		tm.PodReady = func(_ context.Context, site string) (controller.PodReadiness, error) { return s.probePod(g, site) }
		tm.StatusCallback = func(snap controller.TopologySnapshot) {
			tm.MarkStatusWriteResult(runner.WriteStatusForTest(ctx, g.key, snap))
		}
		g.apply = func(ctx context.Context, site string) error {
			s.noteApply(g, site)
			return rec.ReconcileSiteDeployment(ctx, g.key, site)
		}
		g.tm = tm
		g.dirty = true
	}
}

// killProcess ends the operator process: its context is cancelled and every
// later call it makes, to the API server or to MySQL, fails. Callers hold
// s.mu.
func (s *rolloutSim) killProcessLocked(during string) {
	if s.dead {
		return
	}
	s.dead = true
	s.deathInStep = true
	for _, g := range s.groups {
		g.cluster.KillOperator(during)
	}
	if s.proc != nil {
		s.proc.cancel()
	}
}

func (s *rolloutSim) isDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

// restart replaces the operator process between passes.
func (s *rolloutSim) restart(why string) {
	s.mu.Lock()
	s.killProcessLocked(why)
	s.mu.Unlock()
	s.noteRestart(why)
	s.startProcess()
}

func (s *rolloutSim) noteRestart(why string) {
	s.res.Restarts++
	mid := false
	for _, g := range s.groups {
		if g.rec.Started() {
			mid = true
		}
	}
	if mid {
		s.res.MidRollRestarts++
		s.seen["restartMidRoll"] = struct{}{}
	}
	s.logf("operator restart (%s) midRoll=%v", why, mid)
}

// ---------------------------------------------------------------------------
// The step loop
// ---------------------------------------------------------------------------

func (s *rolloutSim) run() {
	ops := s.plan.ActiveOps()
	next, quiet := 0, 0
	for p := 0; p < s.trial.Polls; p++ {
		s.mu.Lock()
		s.step = p
		s.deathInStep = false
		s.mu.Unlock()
		for _, g := range s.groups {
			g.cluster.BeginPoll(p)
		}
		for next < len(ops) && ops[next].At <= p {
			s.applyOp(ops[next], p)
			next++
		}
		if p == s.trial.HealAt {
			s.healAll(p)
		}
		s.expireAnnotations(p)
		s.advancePods(p)
		if s.isDead() && s.deadUntil >= 0 && p >= s.deadUntil {
			s.noteRestart("after a death")
			s.startProcess()
		}
		for _, g := range s.groups {
			g.cluster.Tick()
		}
		if !s.isDead() {
			if !s.plan.PacerAfterGroups {
				s.pacerPass()
			}
			for _, g := range s.groups {
				s.groupPass(g)
			}
			if s.plan.PacerAfterGroups {
				s.pacerPass()
			}
		}
		s.mu.Lock()
		if s.dead && s.deadUntil < 0 {
			// Died during this step: the replacement starts crashDown
			// steps later.
			s.deadUntil = p + 1 + s.crashDown
			s.crashDown = 0
		}
		s.mu.Unlock()
		s.checkStep(p)
		s.clk.Advance(rolloutStep)
		if p > s.trial.HealAt && next == len(ops) && s.converged() {
			quiet++
			if quiet >= rolloutQuietSteps {
				break
			}
		} else {
			quiet = 0
		}
	}
	if s.isDead() {
		s.violate("HarnessOperatorNeverReturned", "trial ended with the operator process still dead")
	}
}

func (s *rolloutSim) pacerPass() {
	if s.isDead() {
		return
	}
	proc := s.proc
	if err := proc.pacer.Tick(proc.ctx); err != nil && !s.isDead() {
		s.violate("HarnessPacerError", err.Error())
	}
}

// groupPass is one round of a group's loops: the reconcile its last change
// woke, the manager's poll, the runner's drift check and the ordered
// update attempt the poll would start.
func (s *rolloutSim) groupPass(g *rolloutGroupSim) {
	if s.isDead() {
		return
	}
	proc := s.proc
	if s.takeDirty(g) {
		if _, err := proc.rec.Reconcile(proc.ctx, ctrl.Request{NamespacedName: g.key}); err != nil && !s.isDead() {
			s.violate("HarnessReconcileError", fmt.Sprintf("%s: %v", g.spec.Name, err))
		}
		// Its own writes do not wake it again.
		s.takeDirty(g)
	}
	if s.isDead() {
		return
	}
	// Poll with no ApplyUpdate starts no update attempt, so nothing of the
	// attempt runs beside the rest of the Poll.
	s.pollGate.Lock()
	g.tm.Poll(proc.ctx)
	s.pollGate.Unlock()
	g.tm.WaitOrderedUpdateIdleForSim()
	if s.isDead() {
		return
	}
	var fg v1alpha1.MysqlFailoverGroup
	if err := s.api.Get(proc.ctx, g.key, &fg); err != nil {
		return
	}
	proc.runner.CheckSpecDriftForSim(proc.ctx, &fg, g.tm)
	s.checkHeldDrift(g, &fg)
	g.tm.ApplyUpdate = g.apply
	g.tm.CheckUpdateForSim(WithActor(proc.ctx, ActorUpdate))
	g.tm.WaitOrderedUpdateIdleForSim()
	g.tm.ApplyUpdate = nil
}

// converged reports whether the fleet is where checkEnd wants it: the
// operator runs, and every group has one primary and a completed roll of
// the release in Ready pods.
func (s *rolloutSim) converged() bool {
	if s.isDead() {
		return false
	}
	for _, g := range s.groups {
		if !g.everPending {
			continue
		}
		rec := g.rec
		if rec == nil || rec.CompletedTime == nil || rec.Renderer != s.identity || g.writableSites() != 1 || s.notRolled(g) != "" {
			return false
		}
	}
	return true
}

// writableSites counts the group's sites that take writes.
func (g *rolloutGroupSim) writableSites() int {
	n := 0
	for _, t := range g.cluster.Truth() {
		if !t.Crashed && !t.ReadOnly && !t.SuperReadOnly {
			n++
		}
	}
	return n
}

// takeDirty reports whether g was written since it was last asked, and
// forgets it.
func (s *rolloutSim) takeDirty(g *rolloutGroupSim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	was := g.dirty
	g.dirty = false
	return was
}

// ---------------------------------------------------------------------------
// Ops
// ---------------------------------------------------------------------------

func (s *rolloutSim) applyOp(op Op, p int) {
	s.logf("op %s", op)
	g := s.byName[op.Group]
	switch op.Kind {
	case OpRelease:
		s.release(p, op.N)
	case OpRestartOperator:
		if !s.isDead() {
			s.restart("restart")
		}
	case OpCrashOperatorWrite:
		s.mu.Lock()
		if !s.dead && s.countdown < 0 {
			s.countdown, s.preApply, s.crashDown = op.N, op.PreApply, op.Down
		}
		s.mu.Unlock()
	case OpBadBuild:
		s.badBuild = true
	case OpPodNotReady:
		s.mu.Lock()
		pod := g.pods[op.Site]
		pod.armed, pod.delay = true, op.N
		s.mu.Unlock()
	case OpPodReady:
		s.fixPod(g, op.Site)
	case OpSiteDown:
		for _, t := range g.cluster.Truth() {
			if t.Name == op.Site && t.ReadOnly {
				g.cluster.Crash(op.Site)
			}
		}
	case OpSiteUp:
		g.cluster.Recover(op.Site, false, false)
	case OpRolloutAdmit:
		s.setAnnotation(g, v1alpha1.RolloutAdmitAnnotation, true)
		g.annoUntil[v1alpha1.RolloutAdmitAnnotation] = p + op.N
	case OpRolloutReleaseSlot:
		s.setAnnotation(g, v1alpha1.RolloutReleaseSlotAnnotation, true)
		g.annoUntil[v1alpha1.RolloutReleaseSlotAnnotation] = p + op.N
	default:
		panic(fmt.Sprintf("dst: rollout applyOp unhandled kind %q", op.Kind))
	}
}

// release is an operator release. The running renderer's name is fixed by
// the binary under test, so instead of changing it the model renames every
// stored trace of it: the Deployments it wrote carry the name and the spec
// hash of an earlier release, and so do the rollout records written for
// it. The renderer compares names and hashes for equality only, so this is
// the state a new release finds. The upgrade restarts the operator.
func (s *rolloutSim) release(p, down int) {
	ctx := context.Background()
	s.epoch++
	s.res.Releases++
	earlier := fmt.Sprintf("release-%d", s.epoch-1)
	for _, g := range s.groups {
		g.writes = map[string]int{}
		for _, site := range g.spec.Sites {
			d, err := s.deployment(g, site)
			if err != nil {
				panic(err)
			}
			g.everPending = true
			if d.Annotations[v1alpha1.RenderedByAnnotation] != s.identity {
				continue
			}
			d.Annotations[v1alpha1.RenderedByAnnotation] = earlier
			d.Annotations[v1alpha1.SpecHashAnnotation] = earlier + "-hash"
			if d.Spec.Template.Annotations == nil {
				d.Spec.Template.Annotations = map[string]string{}
			}
			d.Spec.Template.Annotations[v1alpha1.SpecHashAnnotation] = earlier + "-hash"
			err = s.under.Update(ctx, d)
			s.reads.wrote(d.Namespace)
			if err != nil {
				panic(err)
			}
			s.writeDeploymentStatus(g, site)
		}
		fg := s.group(g)
		if r := fg.Status.Rollout; r != nil && r.Renderer == s.identity {
			r.Renderer = earlier
			err := s.under.Status().Update(ctx, fg)
			s.reads.wrote(fg.Namespace)
			if err != nil {
				panic(err)
			}
			g.rec = r.DeepCopy()
		}
		g.dirty = true
	}
	s.mu.Lock()
	wasDead := s.dead
	s.killProcessLocked("release")
	if !wasDead || s.deadUntil < 0 || s.deadUntil > p+down {
		s.deadUntil = p + down
	}
	s.mu.Unlock()
}

func (s *rolloutSim) setAnnotation(g *rolloutGroupSim, key string, on bool) {
	fg := s.group(g)
	if fg.Annotations == nil {
		fg.Annotations = map[string]string{}
	}
	if on {
		fg.Annotations[key] = "true"
	} else {
		delete(fg.Annotations, key)
	}
	err := s.under.Update(context.Background(), fg)
	s.reads.wrote(fg.Namespace)
	if err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g.dirty = true
	switch key {
	case v1alpha1.RolloutAdmitAnnotation:
		g.admitAnno = on
	case v1alpha1.RolloutReleaseSlotAnnotation:
		g.releaseAnno = on
		if on && fg.Status.Rollout.InFlight() {
			g.manual = true
		}
	}
}

func (s *rolloutSim) expireAnnotations(p int) {
	for _, g := range s.groups {
		for key, until := range g.annoUntil {
			if p >= until {
				s.setAnnotation(g, key, false)
				delete(g.annoUntil, key)
			}
		}
	}
}

// healAll ends every fault: the build is fixed and every pod is Ready,
// every site's MySQL runs, the annotations are gone, no death is armed and
// a dead operator starts in this step.
func (s *rolloutSim) healAll(p int) {
	s.badBuild = false
	for _, g := range s.groups {
		for _, site := range g.spec.Sites {
			s.fixPod(g, site)
			g.cluster.Recover(site, false, false)
		}
		for key := range g.annoUntil {
			g.annoUntil[key] = p
		}
	}
	s.mu.Lock()
	s.countdown, s.crashDown = -1, 0
	if s.dead {
		s.deadUntil = p
	}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Pods
// ---------------------------------------------------------------------------

// fixPod makes a site's pod Ready and disarms it.
func (s *rolloutSim) fixPod(g *rolloutGroupSim, site string) {
	s.mu.Lock()
	pod := g.pods[site]
	pod.armed, pod.delay = false, 0
	was := pod.ready
	pod.ready, pod.readyAt = true, -1
	s.mu.Unlock()
	if !was {
		s.writeDeploymentStatus(g, site)
	}
}

// advancePods makes the pods Ready whose time has come.
func (s *rolloutSim) advancePods(p int) {
	for _, g := range s.groups {
		for _, site := range g.spec.Sites {
			s.mu.Lock()
			pod := g.pods[site]
			due := pod != nil && !pod.ready && pod.readyAt >= 0 && p >= pod.readyAt
			if due {
				pod.ready, pod.readyAt = true, -1
			}
			s.mu.Unlock()
			if due {
				s.writeDeploymentStatus(g, site)
			}
		}
	}
}

// writeDeploymentStatus is the Deployment controller: the status says the
// rollout finished exactly when the site's pod is Ready.
func (s *rolloutSim) writeDeploymentStatus(g *rolloutGroupSim, site string) {
	d, err := s.deployment(g, site)
	if err != nil {
		return
	}
	available := int32(0)
	if pod := g.pods[site]; pod != nil && pod.ready {
		available = 1
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1,
		ReadyReplicas: available, AvailableReplicas: available}
	if available == 0 {
		d.Status.UnavailableReplicas = 1
	}
	err = s.under.Status().Update(context.Background(), d)
	s.reads.wrote(d.Namespace)
	if err != nil {
		panic(fmt.Sprintf("dst: rollout: deployment status %s: %v", d.Name, err))
	}
	g.dirty = true
}

// probePod is the runner's pod-readiness probe (TopologyManager.PodReady).
func (s *rolloutSim) probePod(g *rolloutGroupSim, site string) (controller.PodReadiness, error) {
	s.pollGate.RLock()
	//nolint:staticcheck // SA2001: the gate is only waited for
	s.pollGate.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return controller.PodReadiness{}, errAPIOperatorDead
	}
	name := "mysql-" + g.spec.Name + "-" + site + "-0"
	if pod := g.pods[site]; pod != nil && !pod.ready {
		return controller.PodReadiness{Pod: name, Reason: "sidecar not ready: crash looping, last exit code 1 (Error)"}, nil
	}
	return controller.PodReadiness{Ready: true, Pod: name}, nil
}

// ---------------------------------------------------------------------------
// The API server
// ---------------------------------------------------------------------------

func (s *rolloutSim) interceptorFuncs() interceptor.Funcs {
	alive := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.dead {
			return errAPIOperatorDead
		}
		return nil
	}
	return interceptor.Funcs{
		Get: func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := alive(); err != nil {
				return err
			}
			return s.reads.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := alive(); err != nil {
				return err
			}
			return s.reads.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			o := &client.CreateOptions{}
			o.ApplyOptions(opts)
			return s.write("create", obj, len(o.DryRun) > 0, func() error { return c.Create(ctx, obj, opts...) })
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			o := &client.UpdateOptions{}
			o.ApplyOptions(opts)
			return s.write("update", obj, len(o.DryRun) > 0, func() error { return c.Update(ctx, obj, opts...) })
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			o := &client.PatchOptions{}
			o.ApplyOptions(opts)
			return s.write("patch", obj, len(o.DryRun) > 0, func() error { return c.Patch(ctx, obj, patch, opts...) })
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			o := &client.DeleteOptions{}
			o.ApplyOptions(opts)
			return s.write("delete", obj, len(o.DryRun) > 0, func() error { return c.Delete(ctx, obj, opts...) })
		},
		DeleteAllOf: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteAllOfOption) error {
			return s.write("delete", obj, false, func() error { return c.DeleteAllOf(ctx, obj, opts...) })
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			return s.write(sub, obj, false, func() error { return c.SubResource(sub).Update(ctx, obj, opts...) })
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return s.write(sub, obj, false, func() error { return c.SubResource(sub).Patch(ctx, obj, patch, opts...) })
		},
	}
}

// write is every write the operator sends: dropped when the process is
// dead, the fatal one when the armed countdown runs out, and observed when
// it commits.
func (s *rolloutSim) write(verb string, obj client.Object, dryRun bool, apply func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return errAPIOperatorDead
	}
	if dryRun {
		return apply()
	}
	what := fmt.Sprintf("%s %T", verb, obj)
	what = strings.NewReplacer("*v1alpha1.", "", "*v1.", "").Replace(what)
	fatal := false
	// The reconciler sends deletes of objects that are long gone on every
	// pass; a death there is a death before the next write that matters,
	// so a delete does not use up the countdown.
	if s.countdown >= 0 && verb != "delete" {
		if s.countdown == 0 {
			fatal = true
			s.countdown = -1
		} else {
			s.countdown--
		}
	}
	if fatal && s.preApply {
		s.die(what, false)
		return errAPIOperatorDead
	}
	before := s.before(obj)
	starts := s.res.Transitions["start"]
	err := apply()
	s.reads.wrote(obj.GetNamespace())
	if err == nil {
		s.observe(obj, before)
	}
	if fatal {
		if s.res.Transitions["start"] > starts {
			// The write that latched the start of a roll: the process is
			// gone before the Deployment write the latch was taken for.
			what = "the start latch"
		}
		s.die(what, err == nil)
		return errAPIOperatorDead
	}
	return err
}

// die kills the process at an API write. Callers hold s.mu.
func (s *rolloutSim) die(what string, applied bool) {
	s.res.WriteCrashes++
	s.res.CrashKinds = append(s.res.CrashKinds, what)
	s.seen["diedAt:"+what] = struct{}{}
	s.logf("operator died at %q applied=%v", what, applied)
	s.killProcessLocked("api write: " + what)
}

// before reads the stored object a write is about to replace.
func (s *rolloutSim) before(obj client.Object) client.Object {
	if _, ok := obj.(*appsv1.Deployment); !ok {
		return nil
	}
	old := &appsv1.Deployment{}
	if err := s.reads.Get(context.Background(), client.ObjectKeyFromObject(obj), old); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		panic(fmt.Sprintf("dst: rollout: read before write: %v", err))
	}
	return old
}

// observe sees a committed write of the operator. Callers hold s.mu.
func (s *rolloutSim) observe(obj client.Object, before client.Object) {
	switch obj.(type) {
	case *appsv1.Deployment:
		ref, ok := s.deploys[obj.GetName()]
		if !ok || obj.GetNamespace() != ref.g.key.Namespace {
			return
		}
		var now appsv1.Deployment
		if err := s.reads.Get(context.Background(), client.ObjectKeyFromObject(obj), &now); err != nil {
			return // deleted
		}
		old, _ := before.(*appsv1.Deployment)
		ref.g.dirty = true
		s.observeDeployment(ref.g, ref.site, old, &now)
	case *v1alpha1.MysqlFailoverGroup:
		g := s.byName[obj.GetName()]
		if g == nil {
			return
		}
		var now v1alpha1.MysqlFailoverGroup
		if err := s.reads.Get(context.Background(), g.key, &now); err != nil {
			return
		}
		g.dirty = true
		was := g.rec
		g.rec = now.Status.Rollout.DeepCopy()
		s.observeRecord(g, was, g.rec)
	}
}

// observeDeployment is the Deployment controller and the kubelet for a
// write of a site Deployment: a new pod for a new Deployment, a replaced
// pod (and a restarted MySQL) for a changed pod template.
func (s *rolloutSim) observeDeployment(g *rolloutGroupSim, site string, old, now *appsv1.Deployment) {
	if old == nil {
		g.pods[site] = &simPod{ready: true, readyAt: -1}
		s.writeDeploymentStatus(g, site)
		return
	}
	if equality.Semantic.DeepEqual(old.Spec.Template, now.Spec.Template) {
		s.checkRestamp(g, site, old, now)
		return
	}
	s.checkTemplateWrite(g, site, old, now)
	s.res.TemplateWrites++
	pod := g.pods[site]
	pod.ready, pod.readyAt = true, -1
	switch {
	case pod.armed && pod.delay > 0:
		pod.ready, pod.readyAt = false, s.step+pod.delay
	case pod.armed || s.badBuild:
		pod.ready = false
	}
	pod.armed, pod.delay = false, 0
	s.logf("template of %s/%s written; pod ready=%v readyAt=%d", g.spec.Name, site, pod.ready, pod.readyAt)
	// The new pod's mysqld: a restart on the same data directory, which
	// boots read_only without super_read_only. A pod that is not Ready has
	// a MySQL that answers all the same (issue #209).
	g.cluster.Crash(site)
	g.cluster.Recover(site, false, false)
	s.writeDeploymentStatus(g, site)
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

func (s *rolloutSim) finish() TrialResult {
	var violations []Violation
	for _, g := range s.groups {
		if g.cluster != nil {
			for _, v := range g.cluster.Violations() {
				v.Detail = g.spec.Name + ": " + v.Detail
				violations = append(violations, v)
			}
		}
	}
	violations = append(violations, s.violations...)
	if s.under != nil && !hasInvariant(s.violations, "Panic") {
		violations = append(violations, s.checkEnd()...)
	}
	res := s.res
	sort.Strings(res.CrashKinds)
	out := TrialResult{Trial: s.trial, Violations: violations, BaselineOK: true, Rollout: &res}
	out.Signature = s.signature(violations)
	return out
}

func hasInvariant(vs []Violation, name string) bool {
	for _, v := range vs {
		if v.Invariant == name {
			return true
		}
	}
	return false
}

// signature summarizes the behaviors the trial exercised: which kinds of
// record transition happened, where the operator died, whether a restart
// or a second release landed mid-roll, how it ended and what was violated.
// It is coarse on purpose (no counts, no input shape), so that the
// campaign's saturation rule can go dry.
func (s *rolloutSim) signature(violations []Violation) string {
	keys := make([]string, 0, len(s.seen)+len(s.res.Transitions))
	for k := range s.seen {
		keys = append(keys, k)
	}
	for k := range s.res.Transitions {
		switch k {
		case "admit", "start", "complete", "requeue":
			// Every trial with a release has them.
		default:
			keys = append(keys, k)
		}
	}
	invs := map[string]struct{}{}
	for _, v := range violations {
		invs[v.Invariant] = struct{}{}
	}
	for k := range invs {
		keys = append(keys, "violated:"+k)
	}
	sort.Strings(keys)
	return "rollout;" + strings.Join(keys, ",") + fmt.Sprintf(";allCompleted=%v", s.res.Completed == len(s.groups))
}
