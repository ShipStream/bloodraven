package component

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
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

// Issue #202's state after a v1.5.0 ordered update: dc1 was promoted
// without d1240b6c…:1741933-1742153, and dc2, demoted, kept them while
// replicating every later dc1 transaction.
const (
	div202Primary = "d07c0c83-71ca-11f0-9e33-c80aa9429562:1-240010,d1240b6c-71ca-11f0-9e33-c80aa9429563:1-1741932"
	div202Replica = "d07c0c83-71ca-11f0-9e33-c80aa9429562:1-240010,d1240b6c-71ca-11f0-9e33-c80aa9429563:1-1742153"
	div202Errant  = "d1240b6c-71ca-11f0-9e33-c80aa9429563:1741933-1742153"
)

// syncBuffer is a goroutine-safe log sink: the ordered update logs from its
// own goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The new operator starts on a group already damaged by #202. It must:
// report the healthy, replicating dc2 as divergent (Degraded=True,
// DivergentTransactions, the exact set); refuse to hand the active site to
// dc2 in the ordered update the upgrade itself triggers, without fencing
// dc1; and finish the update once the primary contains the set.
func TestDivergentReplica_Issue202UpgradeStallsUntilReconciled(t *testing.T) {
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	dc1Host := "mysql-lion-dc1-internal.default.svc.cluster.local"
	dc2Host := "mysql-lion-dc2-internal.default.svc.cluster.local"
	lag := int64(0)
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: div202Primary}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: div202Replica,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: dc1Host, SecondsBehindSource: &lag}}
	sites := []controller.SiteTopologyConfig{
		{Name: "dc1", Zone: "lion-dc1", LBIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: dc1Host},
		{Name: "dc2", Zone: "lion-dc2", LBIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: dc2Host},
	}
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 5, 30, 0, 0, time.UTC))
	fc := controller.NewFailoverController(logger)
	updater := controller.NewUpdateController(fc, logger)
	tm := controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{Name: "lion", Namespace: "default", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
		[]mysql.Checker{dc1, dc2}, fc, updater, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger, clk)
	var mu sync.Mutex
	var applied []string
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		mu.Lock()
		applied = append(applied, site)
		mu.Unlock()
		return nil
	}
	appliedSites := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), applied...)
	}

	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: "default"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "dc1"}, {Name: "dc2"}}},
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fg).WithObjects(fg).Build()
	runner := controller.NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(100), logger)
	nn := client.ObjectKeyFromObject(fg)
	runner.SetManagerForTest(nn, tm)
	tm.StatusCallback = func(snap controller.TopologySnapshot) {
		tm.MarkStatusWriteResult(runner.WriteStatusForTest(context.Background(), nn, snap))
	}
	degraded := func() metav1.Condition {
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &got); err != nil {
			t.Fatal(err)
		}
		for _, cond := range got.Status.Conditions {
			if cond.Type == "Degraded" {
				return cond
			}
		}
		return metav1.Condition{}
	}
	poll := func(n int) {
		for i := 0; i < n; i++ {
			clk.Advance(time.Second)
			tm.Poll(context.Background())
		}
	}
	waitIdle := func() {
		deadline := time.Now().Add(10 * time.Second)
		for updater.IsUpdating() {
			if time.Now().After(deadline) {
				t.Fatal("ordered update did not finish")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// 1. Detection on the healthy, replicating replica.
	poll(3)
	st := tm.Status()
	if st.ActiveSite != "dc1" {
		t.Fatalf("setup: active site = %q, want dc1", st.ActiveSite)
	}
	if st.Sites[1].RecoveryState != "RecoveryBlocked" || st.Sites[1].DivergentGtid != div202Errant ||
		st.Sites[1].DivergentTransactionCount == nil || *st.Sites[1].DivergentTransactionCount != 221 {
		t.Fatalf("dc2 status = %+v, want RecoveryBlocked with %s (221)", st.Sites[1], div202Errant)
	}
	if d := degraded(); d.Status != metav1.ConditionTrue || d.Reason != "DivergentTransactions" || !strings.Contains(d.Message, "dc2") {
		t.Fatalf("Degraded = %+v, want True/DivergentTransactions naming dc2", d)
	}

	// 2. The upgrade's ordered update (both sites drifted) stalls loudly:
	// the follower is rolled, but the active site is not handed to dc2 and
	// dc1 is never fenced.
	tm.SetSpecDriftSites([]string{"dc1", "dc2"})
	deadline := time.Now().Add(10 * time.Second)
	for !slices.Contains(appliedSites(), "dc2") {
		if time.Now().After(deadline) {
			t.Fatalf("drifted follower never updated; applied=%v", appliedSites())
		}
		poll(1)
		time.Sleep(5 * time.Millisecond)
	}
	waitIdle()
	poll(3)
	waitIdle()
	if got := appliedSites(); slices.Contains(got, "dc1") {
		t.Fatalf("active site was updated through a divergent standby: applied=%v", got)
	}
	if dc1.isSuperReadOnly() || dc1.isReadOnly() || !dc2.isReadOnly() {
		t.Fatalf("roles changed: dc1 ro=%v sro=%v, dc2 ro=%v", dc1.isReadOnly(), dc1.isSuperReadOnly(), dc2.isReadOnly())
	}
	if out := logs.String(); !strings.Contains(out, "dc2 is divergent (221 transactions the primary lacks); reclone or reconcile before the active site can be updated") {
		t.Fatalf("stall not reported with the distinct message; logs:\n%s", out)
	}

	// 3. The administrator accepts the transactions: the primary is made to
	// contain them (empty transactions or a replay). The next audit clears
	// the block and the update completes.
	dc1.setGtidExecuted(div202Replica)
	clk.Advance(31 * time.Second)
	// The audit stands aside while the failed update's goroutine is still
	// releasing its topology claim, so give it a few polls.
	deadline = time.Now().Add(10 * time.Second)
	for tm.Status().Sites[1].RecoveryState != "" {
		if time.Now().After(deadline) {
			t.Fatalf("dc2 still blocked after the primary contained the set: %+v", tm.Status().Sites[1])
		}
		poll(1)
		time.Sleep(5 * time.Millisecond)
	}
	waitIdle()
	if d := degraded(); d.Reason == "DivergentTransactions" {
		t.Fatalf("Degraded still DivergentTransactions after the proof: %+v", d)
	}
	deadline = time.Now().Add(10 * time.Second)
	for !slices.Contains(appliedSites(), "dc1") {
		if time.Now().After(deadline) {
			t.Fatalf("active site never updated after the divergence was resolved; applied=%v", appliedSites())
		}
		poll(1)
		time.Sleep(5 * time.Millisecond)
	}
	waitIdle()
	if dc2.isReadOnly() {
		t.Error("dc2 was not promoted by the completed update")
	}
}
