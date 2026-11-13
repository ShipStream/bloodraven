package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// rolloutSim is a group's Dragonfly sites for the rollout tests: their
// roles follow the commands sent to them, as a real pair's do. A
// REPLTAKEOVER that succeeds makes the target a master under a new
// replication id and ends the old master's process (it stops answering).
type rolloutSim struct {
	mu    sync.Mutex
	sites map[string]*rolloutSimSite
	// takeoverErr fails every REPLTAKEOVER.
	takeoverErr error
	takeovers   int
	noOnes      int
}

type rolloutSimSite struct {
	// down: the site does not answer a dial.
	down     bool
	role     string
	replID   string
	link     string
	fullSync bool
	flows    []int64
}

const rolloutNewImage = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"

func rolloutSimSiteOf(addr string) string {
	site := strings.TrimPrefix(addr, "lion-dragonfly-")
	return site[:strings.Index(site, ".")]
}

func (s *rolloutSim) connect(_ context.Context, addr, _ string) (DragonflyConnection, error) {
	name := rolloutSimSiteOf(addr)
	s.mu.Lock()
	defer s.mu.Unlock()
	site, ok := s.sites[name]
	if !ok || site.down {
		return nil, errors.New("connection refused")
	}
	return &rolloutSimConn{fakeDragonflyConn: &fakeDragonflyConn{}, sim: s, name: name}, nil
}

// master makes site the master, at flows.
func (s *rolloutSim) master(site, replID string, flows ...int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sites[site] = &rolloutSimSite{role: "master", replID: replID, flows: flows}
}

// replica makes site a replica of master, linked and at flows.
func (s *rolloutSim) replica(site, master string, flows ...int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sites[site] = &rolloutSimSite{role: "slave", replID: s.sites[master].replID, link: "up", flows: flows}
}

// restarted makes site what a recreated pod is before the manager attaches
// it: an empty standalone master under its own replication id.
func (s *rolloutSim) restarted(site string) {
	s.master(site, "fresh-"+site, 0, 0)
}

func (s *rolloutSim) set(site string, mutate func(*rolloutSimSite)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutate(s.sites[site])
}

func (s *rolloutSim) counts() (takeovers, noOnes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.takeovers, s.noOnes
}

type rolloutSimConn struct {
	*fakeDragonflyConn
	sim  *rolloutSim
	name string
}

func (c *rolloutSimConn) InfoReplication(context.Context) (dragonfly.ReplicationInfo, error) {
	c.sim.mu.Lock()
	defer c.sim.mu.Unlock()
	site := c.sim.sites[c.name]
	info := dragonfly.ReplicationInfo{Role: site.role, MasterReplID: site.replID}
	if site.role == "slave" {
		info.MasterLinkStatus, info.MasterSyncInProgress = site.link, site.fullSync
	}
	return info, nil
}

func (c *rolloutSimConn) ReplicationPosition(context.Context) (dragonfly.Position, error) {
	c.sim.mu.Lock()
	defer c.sim.mu.Unlock()
	site := c.sim.sites[c.name]
	return dragonfly.Position{Role: site.role, ReplID: site.replID, Flows: append([]int64(nil), site.flows...)}, nil
}

func (c *rolloutSimConn) ReplTakeover(context.Context, time.Duration) error {
	c.sim.mu.Lock()
	defer c.sim.mu.Unlock()
	c.sim.takeovers++
	if c.sim.takeoverErr != nil {
		return c.sim.takeoverErr
	}
	me := c.sim.sites[c.name]
	for _, other := range c.sim.sites {
		if other.role == "master" && other.replID == me.replID {
			other.down = true
		}
	}
	me.role, me.replID, me.link = "master", "takeover-"+c.name, ""
	return nil
}

func (c *rolloutSimConn) ReplicaOfNoOne(context.Context) error {
	c.sim.mu.Lock()
	defer c.sim.mu.Unlock()
	c.sim.noOnes++
	return nil
}

// rolloutFixture is a group of sites whose Dragonfly StatefulSets exist,
// are rolled out and in sync (the first site is the master), and whose
// spec then gets a new image.
type rolloutFixture struct {
	t   *testing.T
	r   *MysqlFailoverGroupReconciler
	c   client.Client
	fg  *v1alpha1.MysqlFailoverGroup
	sim *rolloutSim
	now time.Time
	old string
}

func newRolloutFixture(t *testing.T, siteNames ...string) *rolloutFixture {
	t.Helper()
	fg := fgWithDragonfly()
	for _, name := range siteNames[2:] {
		extra := *fg.Spec.Sites[1].DeepCopy()
		extra.Name, extra.Zone, extra.ServerID = name, "lion-"+name, extra.ServerID+int64(len(fg.Spec.Sites))
		fg.Spec.Sites = append(fg.Spec.Sites, extra)
	}
	fg.Status.ActiveSite = siteNames[0]
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: siteNames[0], Phase: v1alpha1.DragonflyPhaseReady}
	r, c := newReconciler(fg)
	r.Recorder = record.NewFakeRecorder(1024)
	f := &rolloutFixture{t: t, r: r, c: c, sim: &rolloutSim{sites: map[string]*rolloutSimSite{}}, now: time.Now(), old: fg.Spec.Dragonfly.Image}
	r.Now = func() time.Time { return f.now }
	r.dragonflyConnector = f.sim.connect
	f.sim.master(siteNames[0], "a", 24, 28)
	for _, name := range siteNames[1:] {
		f.sim.replica(name, siteNames[0], 24, 28)
	}
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	for _, name := range siteNames {
		if err := c.Create(ctx, dragonflyPodForTest(fg, name, fg.Spec.Dragonfly.Image, true)); err != nil {
			t.Fatalf("create %s pod: %v", name, err)
		}
		markDragonflyStatefulSetRolledOutForTest(t, c, name)
	}
	f.fg = freshGroup(t, c, fg)
	return f
}

// newImage stores the template change the rollout is about.
func (f *rolloutFixture) newImage() {
	f.t.Helper()
	f.fg = freshGroup(f.t, f.c, f.fg)
	f.fg.Spec.Dragonfly.Image = rolloutNewImage
	if err := f.c.Update(context.Background(), f.fg); err != nil {
		f.t.Fatalf("update spec.dragonfly.image: %v", err)
	}
}

// pass runs one reconcile of the group's Dragonfly resources.
func (f *rolloutFixture) pass() time.Duration {
	f.t.Helper()
	f.fg = freshGroup(f.t, f.c, f.fg)
	requeue, err := f.r.reconcileDragonflyResources(context.Background(), f.fg)
	if err != nil {
		f.t.Fatalf("reconcileDragonflyResources: %v", err)
	}
	return requeue
}

func (f *rolloutFixture) sts(site string) *appsv1.StatefulSet {
	f.t.Helper()
	var sts appsv1.StatefulSet
	key := types.NamespacedName{Namespace: f.fg.Namespace, Name: dragonflyStatefulSetName(f.fg.Name, site)}
	if err := f.c.Get(context.Background(), key, &sts); err != nil {
		f.t.Fatalf("get StatefulSet %s: %v", site, err)
	}
	return &sts
}

func (f *rolloutFixture) image(site string) string {
	f.t.Helper()
	return f.sts(site).Spec.Template.Spec.Containers[0].Image
}

func (f *rolloutFixture) inFlight(site string) bool {
	f.t.Helper()
	return f.sts(site).Annotations[v1alpha1.DragonflyRolloutInFlightAnnotation] != ""
}

// podNotReady makes site's StatefulSet look as it does while its pod is
// being replaced.
func (f *rolloutFixture) podNotReady(site string) {
	f.t.Helper()
	sts := f.sts(site)
	sts.Status.ReadyReplicas = 0
	if err := f.c.Status().Update(context.Background(), sts); err != nil {
		f.t.Fatalf("update StatefulSet %s status: %v", site, err)
	}
}

// wantImages checks which sites run the new template: rolled lists them,
// every other site must still be on the old one.
func (f *rolloutFixture) wantImages(when string, rolled ...string) {
	f.t.Helper()
	want := map[string]bool{}
	for _, site := range rolled {
		want[site] = true
	}
	for _, site := range f.fg.Spec.Sites {
		got := f.image(site.Name)
		if want[site.Name] && got != rolloutNewImage {
			f.t.Fatalf("%s: site %s image = %q, want the new template", when, site.Name, got)
		}
		if !want[site.Name] && got != f.old {
			f.t.Fatalf("%s: site %s image = %q, want it untouched (%q)", when, site.Name, got, f.old)
		}
	}
}

func (f *rolloutFixture) events() []string {
	return drainEventsCh(f.r.Recorder.(*record.FakeRecorder).Events, 1024, 20*time.Millisecond)
}

func rolloutStallEvents(events []string, state string) int {
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, v1alpha1.EventDragonflyRolloutStalled) && strings.Contains(ev, "("+state+")") {
			n++
		}
	}
	return n
}

// passBackoff lets the rollout's takeover backoff pass.
func (f *rolloutFixture) passBackoff() {
	f.r.dragonflyRolloutMu.Lock()
	defer f.r.dragonflyRolloutMu.Unlock()
	for key, st := range f.r.dragonflyRolloutBackoff {
		st.lastFailure = time.Now().Add(-time.Hour)
		f.r.dragonflyRolloutBackoff[key] = st
	}
}

// Fail closed on master authority (#217): while the operator cannot tell
// which site is the Dragonfly master, or status and the sites disagree, no
// StatefulSet template is changed, the replica's included. The wait is
// reported once, after two minutes, and the rollout goes on when the
// authority is back.
func TestDragonflyRollout_UnknownMasterChangesNoTemplate(t *testing.T) {
	for _, tc := range []struct {
		name string
		// break makes the master unknown; mend puts it back.
		breakIt func(f *rolloutFixture)
		mend    func(f *rolloutFixture)
		detail  string
	}{
		{
			name: "status names no master",
			breakIt: func(f *rolloutFixture) {
				fg := freshGroup(f.t, f.c, f.fg)
				fg.Status.ActiveSite, fg.Status.Dragonfly = "", nil
				if err := f.c.Status().Update(context.Background(), fg); err != nil {
					f.t.Fatal(err)
				}
			},
			mend: func(f *rolloutFixture) {
				fg := freshGroup(f.t, f.c, f.fg)
				fg.Status.ActiveSite = "dc1"
				fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
				if err := f.c.Status().Update(context.Background(), fg); err != nil {
					f.t.Fatal(err)
				}
			},
			detail: "status names no Dragonfly master",
		},
		{
			// Status is behind a role change: it would have the rollout
			// restart dc2, which holds the master, as "the replica".
			name: "status is stale: the site it names answers as a replica",
			breakIt: func(f *rolloutFixture) {
				f.sim.master("dc2", "b", 24, 28)
				f.sim.replica("dc1", "dc2", 24, 28)
			},
			mend: func(f *rolloutFixture) {
				f.sim.master("dc1", "a", 24, 28)
				f.sim.replica("dc2", "dc1", 24, 28)
			},
			detail: `it answers with role "slave"`,
		},
		{
			name:    "two sites answer as a master",
			breakIt: func(f *rolloutFixture) { f.sim.restarted("dc2") },
			mend:    func(f *rolloutFixture) { f.sim.replica("dc2", "dc1", 24, 28) },
			detail:  `"dc2" answers as a master too`,
		},
		{
			name:    "the site status names cannot be read",
			breakIt: func(f *rolloutFixture) { f.sim.set("dc1", func(s *rolloutSimSite) { s.down = true }) },
			mend:    func(f *rolloutFixture) { f.sim.set("dc1", func(s *rolloutSimSite) { s.down = false }) },
			detail:  "it could not be read",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRolloutFixture(t, "dc1", "dc2")
			tc.breakIt(f)
			f.newImage()
			for i := 0; i < 3; i++ {
				if requeue := f.pass(); requeue == 0 {
					t.Fatalf("pass %d: requeue = 0, want the rollout to come back", i)
				}
				f.wantImages("master unknown")
			}
			if f.inFlight("dc1") || f.inFlight("dc2") {
				t.Fatal("a site was marked in flight although nothing was written")
			}
			if n := rolloutStallEvents(f.events(), dragonflyRolloutMasterUnknown); n != 0 {
				t.Fatalf("DragonflyRolloutStalled emitted %d times before the stall time", n)
			}
			f.now = f.now.Add(dragonflyRolloutStallAfter + time.Second)
			f.pass()
			f.pass()
			events := f.events()
			if n := rolloutStallEvents(events, dragonflyRolloutMasterUnknown); n != 1 {
				t.Fatalf("DragonflyRolloutStalled (MasterUnknown) emitted %d times, want once: %v", n, events)
			}
			if !eventsContain(events, tc.detail) {
				t.Fatalf("the stall Event does not say why (%q): %v", tc.detail, events)
			}
			f.wantImages("after the stall was reported")
			if takeovers, noOnes := f.sim.counts(); takeovers != 0 || noOnes != 0 {
				t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want nothing sent", takeovers, noOnes)
			}

			tc.mend(f)
			f.pass()
			f.wantImages("authority back", "dc2")
			if !f.inFlight("dc2") {
				t.Fatal("the replica's StatefulSet was written without the in-flight mark")
			}
		})
	}
}

// Failed takeovers never lead to a write of the active site's template
// (#217): the rollout retries with its backoff and reports the stall, and
// no REPLICAOF NO ONE is sent. The template is written only by the takeover
// that succeeds, in the same pass, so the pod comes back once.
func TestDragonflyRollout_FailedTakeoversNeverRestartTheMaster(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2")
	f.newImage()
	f.pass()
	f.wantImages("replica first", "dc2")
	f.sim.takeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}

	for attempt := 1; attempt <= 8; attempt++ {
		if requeue := f.pass(); requeue == 0 {
			t.Fatalf("attempt %d: requeue = 0, want the rollout to come back", attempt)
		}
		if takeovers, noOnes := f.sim.counts(); takeovers != attempt || noOnes != 0 {
			t.Fatalf("attempt %d: REPLTAKEOVER %d, REPLICAOF NO ONE %d", attempt, takeovers, noOnes)
		}
		f.wantImages("after a failed takeover", "dc2")
		if got := rolloutActiveSite(t, f.c, f.fg); got != "dc1" {
			t.Fatalf("attempt %d: dragonfly activeSite = %q, want dc1", attempt, got)
		}
		// Within the backoff nothing is sent.
		f.pass()
		if takeovers, _ := f.sim.counts(); takeovers != attempt {
			t.Fatalf("attempt %d: a takeover was sent within the backoff", attempt)
		}
		f.passBackoff()
	}
	if f.inFlight("dc1") {
		t.Fatal("the active site was marked in flight although its template was never written")
	}
	f.now = f.now.Add(dragonflyRolloutStallAfter + time.Second)
	f.pass()
	f.passBackoff()
	events := f.events()
	if n := rolloutStallEvents(events, dragonflyRolloutTakeoverFailed); n != 1 {
		t.Fatalf("DragonflyRolloutStalled (TakeoverFailed) emitted %d times, want once: %v", n, events)
	}
	f.wantImages("after the stall was reported", "dc2")

	f.sim.takeoverErr = nil
	f.pass()
	if got := rolloutActiveSite(t, f.c, f.fg); got != "dc2" {
		t.Fatalf("dragonfly activeSite = %q, want dc2 after the takeover", got)
	}
	f.wantImages("after the takeover", "dc1", "dc2")
	if !f.inFlight("dc1") {
		t.Fatal("the former master's StatefulSet was written without the in-flight mark")
	}
	if _, noOnes := f.sim.counts(); noOnes != 0 {
		t.Fatalf("REPLICAOF NO ONE sent %d times by a rollout", noOnes)
	}
}

// A group whose replica never becomes promotable keeps its master's pod:
// there is no last resort. The stall is reported, and the template is
// applied only when an administrator names the site.
func TestDragonflyRollout_NoPromotableReplicaWaitsForTheAdministrator(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2")
	f.newImage()
	f.pass()
	f.wantImages("replica first", "dc2")
	// The replica comes back and never reaches the master.
	f.sim.set("dc2", func(s *rolloutSimSite) { s.flows = []int64{1, 1} })
	for i := 0; i < 5; i++ {
		if requeue := f.pass(); requeue == 0 {
			t.Fatalf("pass %d: requeue = 0", i)
		}
	}
	f.now = f.now.Add(dragonflyRolloutStallAfter + time.Second)
	f.pass()
	f.wantImages("replica behind", "dc2")
	if takeovers, noOnes := f.sim.counts(); takeovers != 0 || noOnes != 0 {
		t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want nothing sent to a replica that is behind", takeovers, noOnes)
	}
	if events := f.events(); rolloutStallEvents(events, dragonflyRolloutSiteInFlight) != 1 {
		t.Fatalf("want one DragonflyRolloutStalled (SiteInFlight): %v", events)
	}

	// An annotation that names another site changes nothing for dc1.
	annotate := func(site string) {
		fg := freshGroup(t, f.c, f.fg)
		if fg.Annotations == nil {
			fg.Annotations = map[string]string{}
		}
		fg.Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation] = site
		if err := f.c.Update(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	annotate("dc2")
	f.pass()
	f.wantImages("annotation names the replica", "dc2")

	annotate("dc1")
	f.pass()
	f.wantImages("annotation names the master", "dc1", "dc2")
	if !f.inFlight("dc1") {
		t.Fatal("the in-place restart did not mark the site in flight")
	}
	if got := freshGroup(t, f.c, f.fg).Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation]; got != "" {
		t.Fatalf("the annotation is still set (%q) after it was used", got)
	}
	if events := f.events(); !eventsContain(events, v1alpha1.EventDragonflyRestartedInPlace) {
		t.Fatalf("no DragonflyRestartedInPlace Event: %v", events)
	}
	if takeovers, noOnes := f.sim.counts(); takeovers != 0 || noOnes != 0 {
		t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; the in-place restart sends neither", takeovers, noOnes)
	}
}

// An annotation with nothing to apply to does not stay armed for the next
// rollout.
func TestDragonflyRollout_RestartInPlaceAnnotationIsRemovedWhenNothingIsPending(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2")
	fg := freshGroup(t, f.c, f.fg)
	fg.Annotations = map[string]string{v1alpha1.DragonflyRestartInPlaceAnnotation: "dc1"}
	if err := f.c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if requeue := f.pass(); requeue != 0 {
		t.Fatalf("requeue = %s with nothing to roll", requeue)
	}
	if got := freshGroup(t, f.c, f.fg).Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation]; got != "" {
		t.Fatalf("the annotation stayed (%q) with no template pending", got)
	}
	f.wantImages("nothing pending")
	if events := f.events(); eventsContain(events, v1alpha1.EventDragonflyRestartedInPlace) {
		t.Fatalf("a restart was reported although nothing was restarted: %v", events)
	}
}

// One site at a time (#217), with three sites: each restarted site has to
// be Ready and proven in sync with the master before the next template is
// written. A replica that is still an unattached master, in its full sync,
// or behind holds the others. The former master's template is written by
// the takeover, and the rollout ends when it is back in sync too.
func TestDragonflyRollout_OneSiteAtATime(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2", "dc3")
	f.newImage()

	f.pass()
	f.wantImages("first write", "dc2")
	if !f.inFlight("dc2") {
		t.Fatal("dc2 was written without the in-flight mark")
	}

	// dc2's pod is being replaced: it does not answer.
	f.podNotReady("dc2")
	f.sim.set("dc2", func(s *rolloutSimSite) { s.down = true })
	f.pass()
	f.wantImages("dc2 down", "dc2")

	// It is back, empty and not attached yet: two sites answer as a master.
	f.sim.restarted("dc2")
	markDragonflyStatefulSetRolledOutForTest(t, f.c, "dc2")
	f.pass()
	f.wantImages("dc2 unattached", "dc2")

	// Attached, in its full sync.
	f.sim.replica("dc2", "dc1", 0, 0)
	f.sim.set("dc2", func(s *rolloutSimSite) { s.fullSync = true })
	f.pass()
	f.wantImages("dc2 in full sync", "dc2")

	// Full sync done, still behind on one flow.
	f.sim.set("dc2", func(s *rolloutSimSite) { s.fullSync, s.flows = false, []int64{24, 20} })
	f.pass()
	f.wantImages("dc2 behind", "dc2")
	if !f.inFlight("dc2") {
		t.Fatal("dc2's in-flight mark was removed while it was behind the master")
	}

	// Caught up: the mark goes and the next replica is written.
	f.sim.set("dc2", func(s *rolloutSimSite) { s.flows = []int64{24, 28} })
	f.pass()
	f.wantImages("dc2 in sync", "dc2", "dc3")
	if f.inFlight("dc2") || !f.inFlight("dc3") {
		t.Fatalf("in flight: dc2=%v dc3=%v, want only dc3", f.inFlight("dc2"), f.inFlight("dc3"))
	}

	// dc3 in its full sync holds the master's site: no takeover yet, also
	// not to dc2, which is in sync.
	f.sim.set("dc3", func(s *rolloutSimSite) { s.fullSync = true })
	f.pass()
	f.pass()
	f.wantImages("dc3 in full sync", "dc2", "dc3")
	if takeovers, _ := f.sim.counts(); takeovers != 0 {
		t.Fatalf("REPLTAKEOVER sent %d times while a restarted replica was not back in sync", takeovers)
	}

	// dc3 in sync: takeover to the first replica in sync, and the former
	// master's template in the same pass.
	f.sim.set("dc3", func(s *rolloutSimSite) { s.fullSync = false })
	f.pass()
	if got := rolloutActiveSite(t, f.c, f.fg); got != "dc2" {
		t.Fatalf("dragonfly activeSite = %q, want dc2", got)
	}
	f.wantImages("after the takeover", "dc1", "dc2", "dc3")
	if f.inFlight("dc3") || !f.inFlight("dc1") {
		t.Fatalf("in flight: dc3=%v dc1=%v, want only dc1", f.inFlight("dc3"), f.inFlight("dc1"))
	}
	if takeovers, noOnes := f.sim.counts(); takeovers != 1 || noOnes != 0 {
		t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want one takeover", takeovers, noOnes)
	}

	// The former master comes back and attaches to the new one.
	f.podNotReady("dc1")
	if requeue := f.pass(); requeue == 0 {
		t.Fatal("requeue = 0 while the former master is not back")
	}
	f.sim.restarted("dc1")
	markDragonflyStatefulSetRolledOutForTest(t, f.c, "dc1")
	if requeue := f.pass(); requeue == 0 || !f.inFlight("dc1") {
		t.Fatalf("requeue = %s, in flight = %v while the former master is unattached", requeue, f.inFlight("dc1"))
	}
	f.sim.replica("dc1", "dc2", 24, 28)
	f.sim.replica("dc3", "dc2", 24, 28)
	f.pass()
	if f.inFlight("dc1") {
		t.Fatal("dc1's in-flight mark stayed after it was back in sync")
	}
	if requeue := f.pass(); requeue != 0 {
		t.Fatalf("requeue = %s after the rollout finished", requeue)
	}
	if events := f.events(); eventsContain(events, v1alpha1.EventDragonflyRolloutStalled) || eventsContain(events, v1alpha1.EventDragonflySessionsLost) {
		t.Fatalf("a rollout that never waited two minutes reported a stall or lost sessions: %v", events)
	}
}

// ageDragonflyStatefulSets makes the group's Dragonfly StatefulSets look as
// an earlier operator release left them: no renderer annotation.
func ageDragonflyStatefulSets(f *rolloutFixture) {
	f.t.Helper()
	for _, site := range f.fg.Spec.Sites {
		sts := f.sts(site.Name)
		delete(sts.Annotations, v1alpha1.RenderedByAnnotation)
		if err := f.c.Update(context.Background(), sts); err != nil {
			f.t.Fatalf("age StatefulSet %s: %v", sts.Name, err)
		}
	}
}

// The paced rollout's gate (proposals/23-paced-rollouts.md, 2.6) comes
// before the serial rollout: a StatefulSet another renderer wrote, whose
// template differs, is not touched and no takeover is sent until
// status.rollout admits the group. Then the strict rollout runs, and its
// first write latches the start of the roll.
func TestDragonflyRollout_PacedGateHoldsAnUnadmittedGroup(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2")
	f.r.RolloutPacing = true
	ageDragonflyStatefulSets(f)
	f.newImage()

	for i := 0; i < 3; i++ {
		f.pass()
		f.wantImages("not admitted")
	}
	for _, site := range []string{"dc1", "dc2"} {
		if sts := f.sts(site); sts.Annotations[v1alpha1.RenderedByAnnotation] != "" || f.inFlight(site) {
			t.Fatalf("held StatefulSet %s was annotated: %v", site, sts.Annotations)
		}
	}
	if takeovers, noOnes := f.sim.counts(); takeovers != 0 || noOnes != 0 {
		t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d for a group that is not admitted", takeovers, noOnes)
	}
	if got := freshGroup(t, f.c, f.fg).Status.Rollout; got != nil && got.StartedTime != nil {
		t.Fatalf("the roll was latched as started for a group that is not admitted: %+v", got)
	}

	admitGroup(t, f.c, f.fg)
	f.pass()
	f.wantImages("admitted: replica first", "dc2")
	if got := f.sts("dc2").Annotations[v1alpha1.RenderedByAnnotation]; got != rendererIdentity() {
		t.Fatalf("rolled StatefulSet's renderer = %q, want %q", got, rendererIdentity())
	}
	if got := freshGroup(t, f.c, f.fg).Status.Rollout; got == nil || got.StartedTime == nil {
		t.Fatalf("the first write did not latch the start of the roll: %+v", got)
	}
	f.pass()
	f.wantImages("admitted: takeover and former master", "dc1", "dc2")
	if got := rolloutActiveSite(t, f.c, f.fg); got != "dc2" {
		t.Fatalf("dragonfly activeSite = %q, want dc2", got)
	}
}

// A StatefulSet another renderer wrote whose template the running render
// would not change is restamped: the annotation alone, no template write,
// no in-flight mark, and no admission needed.
func TestDragonflyRollout_PacedGateRestampsAnEqualTemplate(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2")
	f.r.RolloutPacing = true
	ageDragonflyStatefulSets(f)
	before := map[string]string{}
	for _, site := range []string{"dc1", "dc2"} {
		before[site] = f.sts(site).ResourceVersion
	}
	if requeue := f.pass(); requeue != 0 {
		t.Fatalf("requeue = %s for a restamp", requeue)
	}
	f.wantImages("restamp")
	for _, site := range []string{"dc1", "dc2"} {
		sts := f.sts(site)
		if sts.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
			t.Fatalf("StatefulSet %s was not restamped: %v", site, sts.Annotations)
		}
		if f.inFlight(site) {
			t.Fatalf("StatefulSet %s was marked in flight by a restamp", site)
		}
		if sts.ResourceVersion == before[site] {
			t.Fatalf("StatefulSet %s was not patched", site)
		}
	}
	if got := freshGroup(t, f.c, f.fg).Status.Rollout; got != nil {
		t.Fatalf("a restamp wrote a rollout record: %+v", got)
	}
}

// With pacing off (rollout.pacing=false) a StatefulSet another renderer
// wrote needs no admission. The serial rollout is as strict as ever.
func TestDragonflyRollout_PacingOffRollsWithoutAdmission(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2")
	ageDragonflyStatefulSets(f)
	f.newImage()
	f.pass()
	f.wantImages("pacing off", "dc2")
}

func TestDragonflyRolloutAuthority(t *testing.T) {
	f := newRolloutFixture(t, "dc1", "dc2", "dc3")
	ctx := context.Background()
	if got := f.r.dragonflyRolloutAuthority(ctx, freshGroup(t, f.c, f.fg)); got.Master != "dc1" {
		t.Fatalf("authority = %+v, want dc1", got)
	}
	// A site that does not answer is not a second master.
	f.sim.set("dc3", func(s *rolloutSimSite) { s.down = true })
	if got := f.r.dragonflyRolloutAuthority(ctx, freshGroup(t, f.c, f.fg)); got.Master != "dc1" {
		t.Fatalf("authority with a replica down = %+v, want dc1", got)
	}
	// A Dragonfly-only promotion moved the master away from the MySQL
	// active site: status.dragonfly.activeSite is what counts.
	fg := freshGroup(t, f.c, f.fg)
	fg.Status.Dragonfly.ActiveSite = "dc2"
	if err := f.c.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if got := f.r.dragonflyRolloutAuthority(ctx, freshGroup(t, f.c, f.fg)); got.Master != "" {
		t.Fatalf("authority = %+v with status naming a replica, want unknown", got)
	}
	f.sim.master("dc2", "b", 1, 1)
	f.sim.replica("dc1", "dc2", 1, 1)
	if got := f.r.dragonflyRolloutAuthority(ctx, freshGroup(t, f.c, f.fg)); got.Master != "dc2" {
		t.Fatalf("authority = %+v, want dc2", got)
	}
	fg = freshGroup(t, f.c, f.fg)
	fg.Status.Dragonfly.ActiveSite = "elsewhere"
	if err := f.c.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if got := f.r.dragonflyRolloutAuthority(ctx, freshGroup(t, f.c, f.fg)); got.Master != "" || !strings.Contains(got.Detail, "not a site of the group") {
		t.Fatalf("authority = %+v with status naming an unknown site", got)
	}
}
