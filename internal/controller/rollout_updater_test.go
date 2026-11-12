package controller

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
	"github.com/shipstream/bloodraven/internal/testutil"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// rolloutFleetGroup is one failover group of the paced-rollout fleet test:
// its object in the API, and a topology manager with the real ordered
// updater whose Deployment writes go through the real reconciler.
type rolloutFleetGroup struct {
	fg *v1alpha1.MysqlFailoverGroup
	h  *stallHarness
}

// rolloutFleet is an operator process over a fleet: reconciler, runner
// (drift check) and pacer. restart() replaces all of them, and the
// topology managers, with new ones over the same API objects.
type rolloutFleetHarness struct {
	t      *testing.T
	c      client.Client
	r      *MysqlFailoverGroupReconciler
	runner *TopologyManagerRunner
	pacer  *RolloutPacer
	now    time.Time
	groups []*rolloutFleetGroup
	// brokenAfterRoll names the "<group>/<site>" pods that never become
	// Ready on the new template: the bad build.
	mu              sync.Mutex
	brokenAfterRoll map[string]bool
}

func rolloutFleetFG(name string) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Name, fg.UID = name, types.UID("uid-"+name)
	fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name = "primary", "follower-a"
	return fg
}

func newRolloutFleetHarness(t *testing.T, names ...string) *rolloutFleetHarness {
	t.Helper()
	var objs []client.Object
	f := &rolloutFleetHarness{t: t, now: rolloutT0, brokenAfterRoll: map[string]bool{}}
	for _, name := range names {
		fg := rolloutFleetFG(name)
		f.groups = append(f.groups, &rolloutFleetGroup{fg: fg})
		objs = append(objs, fg)
	}
	// The earlier release: it created the workloads, unpaced.
	old, c := newReconciler(objs...)
	f.c = c
	for _, g := range f.groups {
		rolloutReconcile(t, old, g.fg)
		ageDeployments(t, c, g.fg, oldReleaseHash)
	}
	f.restart()
	return f
}

// restart is an operator restart: everything held in memory is new.
func (f *rolloutFleetHarness) restart() {
	f.t.Helper()
	scheme := testScheme()
	f.r = &MysqlFailoverGroupReconciler{Client: f.c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000),
		RolloutPacing: true, rolloutPollInterval: time.Millisecond, rolloutWaitTimeout: 50 * time.Millisecond}
	f.runner = rolloutRunner(f.c)
	f.pacer = NewRolloutPacer(f.c, f.c, record.NewFakeRecorder(1000), testutil.TestLogger(), f.r,
		func(*v1alpha1.MysqlFailoverGroup) string { return "" }, RolloutPacerConfig{Soak: -1})
	f.pacer.SetClock(func() time.Time { return f.now })
	for _, g := range f.groups {
		g := g
		nn := client.ObjectKeyFromObject(g.fg)
		h := newStallHarness(f.t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate})
		g.h = h
		for site, pod := range h.pods {
			f.mu.Lock()
			pod.broken = f.brokenAfterRoll[g.fg.Name+"/"+site] && siteDeployment(f.t, f.c, g.fg, site).Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash
			f.mu.Unlock()
		}
		h.tm.ApplyUpdate = func(ctx context.Context, site string) error {
			h.mu.Lock()
			h.applied = append(h.applied, site)
			h.mu.Unlock()
			if err := f.r.ReconcileSiteDeployment(ctx, nn, site); err != nil {
				return err
			}
			f.mu.Lock()
			broken := f.brokenAfterRoll[g.fg.Name+"/"+site]
			f.mu.Unlock()
			if broken {
				h.pods[site].breakIt()
			}
			return nil
		}
		// The runner's status write, reduced to the condition the pacer reads.
		h.tm.StatusCallback = func(s TopologySnapshot) {
			h.mu.Lock()
			h.snaps = append(h.snaps, s)
			h.mu.Unlock()
			fresh := freshGroup(f.t, f.c, g.fg)
			cond := metav1.Condition{Type: v1alpha1.ConditionUpdating, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNotUpdating}
			if s.UpdateStalled != "" {
				cond = metav1.Condition{Type: v1alpha1.ConditionUpdating, Status: metav1.ConditionTrue,
					Reason: v1alpha1.ReasonOrderedUpdateStalled, Message: orderedUpdateStalledMessage(s.UpdateStalled)}
			}
			apimeta.SetStatusCondition(&fresh.Status.Conditions, cond)
			if err := f.c.Status().Update(context.Background(), fresh); err != nil {
				f.t.Errorf("write Updating condition of %s: %v", g.fg.Name, err)
			}
		}
	}
}

func (f *rolloutFleetHarness) group(name string) *rolloutFleetGroup {
	for _, g := range f.groups {
		if g.fg.Name == name {
			return g
		}
	}
	f.t.Fatalf("no group %s", name)
	return nil
}

// pass is one round of the operator's loops: the pacer, then each group's
// drift check and update attempt, then the pacer again on what they wrote.
func (f *rolloutFleetHarness) pass() {
	f.t.Helper()
	if err := f.pacer.Tick(context.Background()); err != nil {
		f.t.Fatalf("pacer: %v", err)
	}
	for _, g := range f.groups {
		f.runner.checkSpecDrift(context.Background(), freshGroup(f.t, f.c, g.fg), g.h.tm)
		g.h.tm.checkUpdate(context.Background())
		g.h.waitIdle(f.t)
		g.h.observed()
	}
	if err := f.pacer.Tick(context.Background()); err != nil {
		f.t.Fatalf("pacer: %v", err)
	}
}

// later moves both clocks past the updater's stall back-off.
func (f *rolloutFleetHarness) later() {
	f.now = f.now.Add(orderedUpdateStallRetry)
	for _, g := range f.groups {
		g.h.clk.Advance(orderedUpdateStallRetry)
	}
}

func (f *rolloutFleetHarness) record(name string) *v1alpha1.RolloutStatus {
	f.t.Helper()
	return freshGroup(f.t, f.c, f.group(name).fg).Status.Rollout
}

// requireUntouched fails unless every workload of the group is exactly as
// the earlier release left it and its MySQL was never touched.
func (f *rolloutFleetHarness) requireUntouched(name, when string) {
	f.t.Helper()
	g := f.group(name)
	for _, site := range g.fg.Spec.Sites {
		d := siteDeployment(f.t, f.c, g.fg, site.Name)
		if d.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash || d.Annotations[v1alpha1.RenderedByAnnotation] != "" ||
			d.Spec.Template.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash {
			f.t.Fatalf("%s: %s/%s was rolled outside its slot: %v", when, name, site.Name, d.Annotations)
		}
	}
	if got := g.h.appliedSites(); got != "" {
		f.t.Fatalf("%s: the updater of %s applied %q", when, name, got)
	}
	primary, follower := g.h.tm.sites[0].mysql.(*mockMySQL), g.h.tm.sites[1].mysql.(*mockMySQL)
	if primary.superReadOnly || primary.readOnly || follower.promoted {
		f.t.Fatalf("%s: MySQL authority of %s was touched", when, name)
	}
}

// R-4, R-5, R-7 with the real ordered updater and the real Deployment write
// path: a release whose pods never become Ready stops at the first group.
//
// The replica of lion is rolled and its pod stays not Ready. The updater
// stops there (it does not hand the primary over or roll it), the stall is
// latched in status.rollout, and tiger keeps every pod it had: through
// retries, through an operator restart, and through a second operator
// upgrade mid-rollout. Once lion's pod is Ready its roll finishes and tiger
// is rolled next.
func TestPacedRollout_BadBuildStopsAtTheFirstGroup(t *testing.T) {
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	f := newRolloutFleetHarness(t, "lion", "tiger")
	f.brokenAfterRoll["lion/follower-a"] = true

	f.pass()
	lion := f.group("lion")
	if got := lion.h.appliedSites(); got != "follower-a" {
		t.Fatalf("lion applied %q, want only its replica", got)
	}
	if rec := f.record("lion"); rec.Phase != v1alpha1.RolloutPhaseStalled || rec.Reason != rolloutReasonUpdateStalled || !rec.Started() ||
		!strings.Contains(rec.Message, "follower-a") {
		t.Fatalf("lion record = %+v, want its roll started and stalled on follower-a", rec)
	}
	if rec := f.record("tiger"); rec == nil || rec.Phase != v1alpha1.RolloutPhasePending || rec.Reason != rolloutReasonHalted {
		t.Fatalf("tiger record = %+v, want Pending/RolloutHalted", rec)
	}
	f.requireUntouched("tiger", "after lion stalled")
	if d := siteDeployment(t, f.c, lion.fg, "primary"); d.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash {
		t.Fatal("lion's primary was rolled although its replica is not Ready")
	}
	if p := lion.h.tm.sites[0].mysql.(*mockMySQL); p.readOnly || p.superReadOnly {
		t.Fatal("lion's primary was fenced although its replica is not Ready")
	}
	stalledAt := f.record("lion").StalledTime.DeepCopy()

	// Retries change nothing.
	for i := 0; i < 3; i++ {
		f.later()
		f.pass()
	}
	f.requireUntouched("tiger", "after retries")
	if rec := f.record("lion"); rec.Phase != v1alpha1.RolloutPhaseStalled || !rec.StalledTime.Equal(stalledAt) {
		t.Fatalf("lion record after retries = %+v", rec)
	}
	if got := lion.h.appliedSites(); got != "follower-a" {
		t.Fatalf("lion applied %q after retries", got)
	}

	// An operator restart: the admission, the latch and the halt are read
	// back from the API.
	f.restart()
	lion = f.group("lion")
	f.later()
	f.pass()
	f.requireUntouched("tiger", "after an operator restart")
	if rec := f.record("lion"); rec.Phase != v1alpha1.RolloutPhaseStalled || !rec.StalledTime.Equal(stalledAt) || !rec.Started() {
		t.Fatalf("lion record after a restart = %+v, want the same stall", rec)
	}
	if got := lion.h.appliedSites(); got != "" {
		t.Fatalf("after a restart lion's updater applied %q; its replica is still not Ready", got)
	}
	if rec := f.record("tiger"); rec.Phase != v1alpha1.RolloutPhasePending || rec.InFlight() {
		t.Fatalf("tiger record after a restart = %+v", rec)
	}

	// A second operator upgrade mid-rollout (another default sidecar image,
	// so another renderer and another hash): lion keeps the slot.
	SetSidecarImageDefault("registry.example/bloodraven-sidecar:second-upgrade")
	f.restart()
	lion = f.group("lion")
	f.later()
	f.pass()
	f.requireUntouched("tiger", "after a second upgrade")
	rec := f.record("lion")
	if rec.Renderer != rendererIdentity() || !rec.Started() || rec.StalledTime == nil || !rec.StalledTime.Equal(stalledAt) {
		t.Fatalf("lion record after a second upgrade = %+v, want the same admission for the new renderer", rec)
	}
	if rec := f.record("tiger"); rec.Renderer != rendererIdentity() || rec.Phase != v1alpha1.RolloutPhasePending || rec.Reason != rolloutReasonHalted {
		t.Fatalf("tiger record after a second upgrade = %+v", rec)
	}
	if got := lion.h.appliedSites(); slices.Contains(strings.Split(got, ","), "primary") {
		t.Fatalf("lion applied %q: its primary was rolled while its replica is not Ready", got)
	}

	// The pod is fixed: lion finishes, then tiger is admitted and rolled.
	f.mu.Lock()
	delete(f.brokenAfterRoll, "lion/follower-a")
	f.mu.Unlock()
	p := lion.h.pods["follower-a"]
	p.mu.Lock()
	p.broken = false
	p.mu.Unlock()
	for i := 0; i < 6 && f.record("tiger").Phase != v1alpha1.RolloutPhaseComplete; i++ {
		f.later()
		f.pass()
	}
	for _, name := range []string{"lion", "tiger"} {
		if rec := f.record(name); rec.Phase != v1alpha1.RolloutPhaseComplete || rec.StalledTime != nil {
			t.Fatalf("%s record = %+v, want Complete", name, rec)
		}
		for _, site := range f.group(name).fg.Spec.Sites {
			d := siteDeployment(t, f.c, f.group(name).fg, site.Name)
			if d.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() || d.Annotations[v1alpha1.SpecHashAnnotation] == oldReleaseHash {
				t.Fatalf("%s/%s was not rolled: %v", name, site.Name, d.Annotations)
			}
		}
	}
	lionDone, tigerAdmitted := f.record("lion").CompletedTime, f.record("tiger").AdmittedTime
	if tigerAdmitted.Time.Before(lionDone.Time) {
		t.Fatalf("tiger was admitted at %s, before lion completed at %s", tigerAdmitted, lionDone)
	}
}
