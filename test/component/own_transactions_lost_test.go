package component

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// "Found, not fixed" 1 of PR #280, end to end through the real
// TopologyManager: dc1 is the primary and issues 1-100 under its
// server_uuid; dc2 has all of them. dc1 dies, dc2 is promoted and writes.
// dc1 comes back on a volume that holds only 1-90.
const (
	ownLossUUIDdc1   = "aaaaaaaa-1111-11f1-8000-000000000001"
	ownLossUUIDdc2   = "bbbbbbbb-2222-11f1-8000-000000000002"
	ownLossUUIDclone = "cccccccc-3333-11f1-8000-000000000003"

	ownLossBefore  = ownLossUUIDdc1 + ":1-100"
	ownLossPrimary = ownLossUUIDdc1 + ":1-100," + ownLossUUIDdc2 + ":1-20"
	ownLossReturn  = ownLossUUIDdc1 + ":1-90"
	ownLossLost    = ownLossUUIDdc1 + ":91-100"
)

type ownLossFixture struct {
	t        *testing.T
	tm       *controller.TopologyManager
	clk      *clock.FakeClock
	dc1, dc2 *mockMySQL
	logs     *syncBuffer
	events   *record.FakeRecorder
	c        client.Client
	nn       client.ObjectKey
}

func newOwnLossFixture(t *testing.T) *ownLossFixture {
	t.Helper()
	f := &ownLossFixture{t: t, logs: &syncBuffer{}}
	logger := slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	dc1Host := "mysql-lion-dc1-internal.default.svc.cluster.local"
	dc2Host := "mysql-lion-dc2-internal.default.svc.cluster.local"
	lag := int64(0)
	f.dc1 = &mockMySQL{readOnly: false, gtidExecuted: ownLossBefore, clearChannelOnReset: true}
	f.dc2 = &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: ownLossBefore, clearChannelOnReset: true,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: dc1Host, SecondsBehindSource: &lag}}
	sites := []controller.SiteTopologyConfig{
		{Name: "dc1", Zone: "lion-dc1", LoadBalancerIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: dc1Host},
		{Name: "dc2", Zone: "lion-dc2", LoadBalancerIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: dc2Host},
	}
	f.clk = clock.NewFakeClock(time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC))
	f.tm = controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{Name: "lion", Namespace: "default", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
		[]mysql.Checker{withServerUUID(f.dc1, ownLossUUIDdc1), withServerUUID(f.dc2, ownLossUUIDdc2)},
		controller.NewFailoverController(logger), nil, controller.NewBootstrapController(logger),
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger, f.clk)

	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: "default"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "dc1", ServerID: 101}, {Name: "dc2", ServerID: 102}}},
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	f.c = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fg).WithObjects(fg).Build()
	f.events = record.NewFakeRecorder(200)
	runner := controller.NewTopologyManagerRunner(f.c, nil, nil, f.events, logger)
	f.nn = client.ObjectKeyFromObject(fg)
	runner.SetManagerForTest(f.nn, f.tm)
	f.tm.StatusCallback = func(snap controller.TopologySnapshot) {
		f.tm.MarkStatusWriteResult(runner.WriteStatusForTest(context.Background(), f.nn, snap))
	}
	return f
}

func (f *ownLossFixture) poll(n int) {
	for i := 0; i < n; i++ {
		f.clk.Advance(time.Second)
		f.tm.Poll(context.Background())
		f.waitBootstrap()
	}
}

// waitBootstrap waits out a clone the last poll started: it runs on a
// goroutine of its own.
func (f *ownLossFixture) waitBootstrap() {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		phase := f.tm.BootstrapPhase()
		if phase == controller.BootstrapPhaseNone || phase == controller.BootstrapPhaseDone || phase == controller.BootstrapPhaseFailed {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("bootstrap did not end: phase %s", phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *ownLossFixture) statusSite(name string) v1alpha1.SiteStatus {
	f.t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := f.c.Get(context.Background(), f.nn, &got); err != nil {
		f.t.Fatal(err)
	}
	for _, s := range got.Status.Sites {
		if s.Name == name {
			return s
		}
	}
	return v1alpha1.SiteStatus{}
}

func (f *ownLossFixture) condition(name string) metav1.Condition {
	f.t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := f.c.Get(context.Background(), f.nn, &got); err != nil {
		f.t.Fatal(err)
	}
	for _, cond := range got.Status.Conditions {
		if cond.Type == name {
			return cond
		}
	}
	return metav1.Condition{}
}

func (f *ownLossFixture) drainEvents() string {
	var out []string
	for {
		select {
		case e := <-f.events.Events:
			out = append(out, e)
		default:
			return strings.Join(out, "\n")
		}
	}
}

// failoverAndReturn runs the history up to the return of dc1: dc1 dies,
// dc2 is promoted and writes, dc1 comes back read-only on the older
// volume with no replication channel.
func (f *ownLossFixture) failoverAndReturn() {
	f.t.Helper()
	f.poll(3)
	if got := f.tm.Status().ActiveSite; got != "dc1" {
		f.t.Fatalf("setup: active site = %q, want dc1", got)
	}
	f.dc1.setError(errors.New("dial tcp: connection refused"))
	f.poll(6)
	if f.dc2.isReadOnly() {
		f.t.Fatal("setup: dc2 was not promoted after dc1 died")
	}
	f.dc2.setGtidExecuted(ownLossPrimary)
	f.poll(3)
	if got := f.tm.Status().ActiveSite; got != "dc2" {
		f.t.Fatalf("setup: active site = %q after the failover, want dc2", got)
	}
	lost := ownLossReturn
	f.dc1.replacePod(podReplacement{gtidExecuted: &lost, noChannel: true})
}

// armClone makes CLONE INSTANCE on dc1 have a clone's effect: dc1 holds
// the donor's set under a new server_uuid and has no channel. It records
// whether replication had been configured on dc1 before the clone.
func (f *ownLossFixture) armClone(replicationBeforeClone *bool) {
	f.dc1.mu.Lock()
	f.dc1.onClone = func() {
		f.dc2.mu.Lock()
		donorSet := f.dc2.gtidExecuted
		f.dc2.mu.Unlock()
		f.dc1.mu.Lock()
		*replicationBeforeClone = f.dc1.changeSourceCalls > 0 || f.dc1.replicaStarted
		f.dc1.gtidExecuted = donorSet
		f.dc1.serverUUID = ownLossUUIDclone
		f.dc1.replicaStatus = nil
		f.dc1.mu.Unlock()
	}
	f.dc1.mu.Unlock()
}

// The lossy old primary is never rejoined by replication: the operator
// reports it, reclones it through the reclone path, and the site ends as a
// replica of dc2 under a new server_uuid with the report gone.
func TestOwnTransactionsLost_ReturningOldPrimaryIsRecloned(t *testing.T) {
	f := newOwnLossFixture(t)
	f.failoverAndReturn()
	replicationBeforeClone := false
	f.armClone(&replicationBeforeClone)
	f.drainEvents()

	f.poll(8)

	f.dc1.mu.Lock()
	cloned, changes, started, uuid, set := f.dc1.cloneInstanceCalled, f.dc1.changeSourceCalls, f.dc1.replicaStarted, f.dc1.serverUUID, f.dc1.gtidExecuted
	f.dc1.mu.Unlock()
	if !cloned {
		t.Fatalf("dc1 was not recloned; logs:\n%s", f.logs.String())
	}
	if replicationBeforeClone {
		t.Fatal("replication was configured on dc1 before it was recloned: its receiver drops the transactions it lost")
	}
	if changes == 0 || !started {
		t.Fatalf("replication was not configured after the reclone (CHANGE=%d started=%v)", changes, started)
	}
	if uuid != ownLossUUIDclone || set != ownLossPrimary {
		t.Fatalf("dc1 after the reclone: server_uuid %s, set %s; want the new identity and the primary's set", uuid, set)
	}
	if got := f.tm.Status().Sites[0]; got.RecoveryState != "" || got.LostGtid != "" {
		t.Fatalf("dc1 status after the reclone = %+v, want no report", got)
	}
	if got := f.statusSite("dc1"); got.LostGtid != "" || got.RecoveryState != "" {
		t.Fatalf("persisted dc1 status after the reclone = %+v, want no report", got)
	}
	logs := f.logs.String()
	for _, want := range []string{
		`msg="own transactions lost" site=dc1 lostTransactions=10 lostGtid=` + ownLossLost,
		`msg="reclone requested for a site that lacks transactions it issued itself" site=dc1 donor=dc2`,
		`msg="own transactions lost cleared: the site was recloned" site=dc1`,
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("log does not contain %q", want)
		}
	}
	if strings.Contains(logs, "auto-recovering old primary as replica") {
		t.Error("the operator took the rejoin-by-replication path")
	}
	evs := f.drainEvents()
	if !strings.Contains(evs, "Warning OwnTransactionsLost dc1 lacks 10 transactions it issued itself that dc2 holds") {
		t.Errorf("events = %q, want the OwnTransactionsLost Warning", evs)
	}
	if !strings.Contains(evs, "Normal OwnTransactionsLostResolved") {
		t.Errorf("events = %q, want OwnTransactionsLostResolved", evs)
	}
}

// When the automatic reclone cannot run (here the donor refuses the clone)
// the site stays what the report says: fenced, without replication, not a
// failover candidate, with the reason and the remedy in status. It clears
// by itself once a clone succeeds.
func TestOwnTransactionsLost_SiteStaysOutUntilAReclone(t *testing.T) {
	f := newOwnLossFixture(t)
	f.failoverAndReturn()
	f.dc1.mu.Lock()
	f.dc1.cloneInstanceErr = errors.New("clone instance: Error 3862 (HY000): Clone Donor Error: 1045 : Access denied")
	f.dc1.mu.Unlock()
	f.drainEvents()

	f.poll(8)

	f.dc1.mu.Lock()
	cloned, changes, started, fenced := f.dc1.cloneInstanceCalled, f.dc1.changeSourceCalls, f.dc1.replicaStarted, f.dc1.superReadOnly
	f.dc1.mu.Unlock()
	if !cloned {
		t.Fatal("no clone was attempted")
	}
	if changes != 0 || started {
		t.Fatalf("replication was configured on dc1 although its clone failed (CHANGE=%d started=%v)", changes, started)
	}
	if !fenced {
		t.Fatal("dc1 is not fenced")
	}
	site := f.statusSite("dc1")
	if site.RecoveryState != v1alpha1.RecoveryStateRecloneRequired || site.LostGtid != ownLossLost ||
		site.LostTransactionCount == nil || *site.LostTransactionCount != 10 {
		t.Fatalf("persisted dc1 status = %+v, want RecloneRequired with %s (10)", site, ownLossLost)
	}
	for _, name := range []string{"RecoveryPending", "Degraded"} {
		c := f.condition(name)
		// BootstrapFailed outranks the report on Degraded; RecoveryPending
		// names it.
		if name == "RecoveryPending" && (c.Status != metav1.ConditionTrue || c.Reason != "OwnTransactionsLost" ||
			!strings.Contains(c.Message, "kubectl bloodraven reclone lion dc1") || !strings.Contains(c.Message, "did not complete")) {
			t.Errorf("%s = %+v, want True/OwnTransactionsLost with the remedy and the retry", name, c)
		}
		if name == "Degraded" && c.Status != metav1.ConditionTrue {
			t.Errorf("%s = %+v, want True", name, c)
		}
	}

	// The primary dies while dc1 still lacks its transactions: dc1 is the
	// only other candidate and must not be promoted.
	f.dc2.setError(errors.New("dial tcp: connection refused"))
	f.poll(8)
	if !f.dc1.isReadOnly() {
		t.Fatalf("dc1 was promoted although it lacks %s: it would issue those GTIDs again", ownLossLost)
	}
	if evs := f.drainEvents(); !strings.Contains(evs, "Warning PromotionRefusedOwnTransactionsLost") {
		t.Errorf("events = %q, want PromotionRefusedOwnTransactionsLost", evs)
	}

	// The primary returns as the primary and the clone works again: after
	// the retry delay the operator reclones dc1 without being asked.
	f.dc2.respawn(false)
	f.dc1.mu.Lock()
	f.dc1.cloneInstanceErr = nil
	f.dc1.mu.Unlock()
	replicationBeforeClone := false
	f.armClone(&replicationBeforeClone)
	f.poll(4)
	f.clk.Advance(6 * time.Minute)
	f.poll(8)
	if got := f.statusSite("dc1"); got.LostGtid != "" || got.RecoveryState != "" {
		t.Fatalf("persisted dc1 status after the successful reclone = %+v, want no report; logs:\n%s", got, f.logs.String())
	}
	f.dc1.mu.Lock()
	changes, started = f.dc1.changeSourceCalls, f.dc1.replicaStarted
	f.dc1.mu.Unlock()
	if replicationBeforeClone || changes == 0 || !started {
		t.Fatalf("after the successful reclone: replication before the clone %v, CHANGE=%d started=%v", replicationBeforeClone, changes, started)
	}
}

// A report survives an operator restart: the new manager restores it from
// status, keeps the site out of replication and promotion, and finishes
// the reclone.
func TestOwnTransactionsLost_ReportSurvivesAnOperatorRestart(t *testing.T) {
	f := newOwnLossFixture(t)
	f.failoverAndReturn()
	f.dc1.mu.Lock()
	f.dc1.cloneInstanceErr = errors.New("clone instance: Error 3862 (HY000): Clone Donor Error")
	f.dc1.mu.Unlock()
	f.poll(8)
	persisted := f.statusSite("dc1")
	if persisted.LostGtid != ownLossLost {
		t.Fatalf("persisted dc1 status = %+v, want the lost set", persisted)
	}

	// A new operator process: nothing in memory, the report from status.
	g := newOwnLossFixture(t)
	g.dc1, g.dc2 = f.dc1, f.dc2
	logger := slog.New(slog.NewTextHandler(g.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	sites := []controller.SiteTopologyConfig{
		{Name: "dc1", Zone: "lion-dc1", LoadBalancerIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: "mysql-lion-dc1-internal.default.svc.cluster.local"},
		{Name: "dc2", Zone: "lion-dc2", LoadBalancerIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: "mysql-lion-dc2-internal.default.svc.cluster.local"},
	}
	tm := controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{Name: "lion", Namespace: "default", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
		[]mysql.Checker{mockWithUUID{f.dc1}, mockWithUUID{f.dc2}},
		controller.NewFailoverController(logger), nil, controller.NewBootstrapController(logger),
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger, f.clk)
	var lostCount int64
	if persisted.LostTransactionCount != nil {
		lostCount = *persisted.LostTransactionCount
	}
	tm.SetOwnTransactionsLost("dc1", persisted.LostGtid, lostCount)
	tm.SetLastFailoverTarget("dc2")

	// Before its first comparison the restored report already keeps the
	// site's replication from being started.
	if st := tm.Status().Sites[0]; st.RecoveryState != v1alpha1.RecoveryStateRecloneRequired || st.LostGtid != ownLossLost {
		t.Fatalf("restored dc1 status = %+v, want RecloneRequired with the lost set", st)
	}

	g.tm = tm
	f.dc1.mu.Lock()
	f.dc1.cloneInstanceErr = nil
	f.dc1.mu.Unlock()
	replicationBeforeClone := false
	f.armClone(&replicationBeforeClone)
	g.poll(8)
	f.dc1.mu.Lock()
	uuid, changes := f.dc1.serverUUID, f.dc1.changeSourceCalls
	f.dc1.mu.Unlock()
	if replicationBeforeClone {
		t.Fatal("the restarted operator configured replication on dc1 before recloning it")
	}
	if uuid != ownLossUUIDclone || changes == 0 {
		t.Fatalf("after the restart dc1 has server_uuid %s and %d CHANGE REPLICATION SOURCE; want it recloned and replicating; logs:\n%s",
			uuid, changes, g.logs.String())
	}
	if st := tm.Status().Sites[0]; st.LostGtid != "" {
		t.Fatalf("dc1 status after the reclone = %+v, want no report", st)
	}
}
