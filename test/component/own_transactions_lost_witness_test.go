package component

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// Pinned from the DST quick sweep (seed 66, PromotedWithoutOwnTransactions):
// the primary dies, the remaining candidate has come back on an older data
// directory, and a disaster-recovery site that still answers holds the
// transactions that candidate issued and lost. There is no primary to
// compare the candidate with, but the disaster-recovery site is the same
// evidence: the candidate must not be promoted.
func TestOwnTransactionsLost_WitnessKeepsARegressedCandidateFromPromotion(t *testing.T) {
	// dc2 was the primary once and issued 1-100; dc1 is the primary now.
	const full = ownLossUUIDdc2 + ":1-100," + ownLossUUIDdc1 + ":1-20"
	const regressed = ownLossUUIDdc2 + ":1-90," + ownLossUUIDdc1 + ":1-20"

	run := func(t *testing.T, dc2AfterRestart string) (dc2 *mockMySQL, tm *controller.TopologyManager, logs *syncBuffer) {
		t.Helper()
		logs = &syncBuffer{}
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
		host := func(site string) string { return "mysql-lion-" + site + "-internal.default.svc.cluster.local" }
		lag := int64(0)
		replica := func() *mysql.ReplicaStatus {
			return &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: host("dc1"), SecondsBehindSource: &lag}
		}
		dc1 := &mockMySQL{readOnly: false, gtidExecuted: full}
		dc2 = &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: full, replicaStatus: replica()}
		dc3 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: full, replicaStatus: replica()}
		sites := []controller.SiteTopologyConfig{
			{Name: "dc1", Zone: "lion-dc1", LoadBalancerIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: host("dc1")},
			{Name: "dc2", Zone: "lion-dc2", LoadBalancerIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: host("dc2")},
			{Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "10.0.0.3", Role: state.SiteRoleDROnly, Host: host("dc3")},
		}
		clk := clock.NewFakeClock(time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC))
		tm = controller.NewTopologyManagerWithClock(
			controller.TopologyConfig{Name: "lion", Namespace: "default", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
			[]mysql.Checker{withServerUUID(dc1, ownLossUUIDdc1), withServerUUID(dc2, ownLossUUIDdc2), withServerUUID(dc3, ownLossUUIDclone)},
			controller.NewFailoverController(logger), nil, nil,
			controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
			newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger, clk)
		poll := func(n int) {
			for i := 0; i < n; i++ {
				clk.Advance(time.Second)
				tm.Poll(context.Background())
			}
		}
		poll(3)
		if got := tm.Status().ActiveSite; got != "dc1" {
			t.Fatalf("setup: active site = %q, want dc1", got)
		}
		// The primary dies, and in the same moment dc2's pod restarts.
		dc1.setError(errors.New("dial tcp: connection refused"))
		// A dead server answers no GTID read either; without this the
		// mock's primary would itself be the witness.
		dc1.failNextCalls("GetGtidExecuted", 1<<20)
		dc2.replacePod(podReplacement{gtidExecuted: &dc2AfterRestart})
		poll(10)
		return dc2, tm, logs
	}

	t.Run("a candidate back on an older volume is refused", func(t *testing.T) {
		dc2, tm, logs := run(t, regressed)
		if !dc2.isReadOnly() {
			t.Fatalf("dc2 was promoted although dc3 holds %s:91-100, which dc2 issued and lacks; logs:\n%s", ownLossUUIDdc2, logs.String())
		}
		st := tm.Status()
		if st.Sites[1].RecoveryState != v1alpha1.RecoveryStateRecloneRequired || st.Sites[1].LostGtid != ownLossUUIDdc2+":91-100" {
			t.Fatalf("dc2 status = %+v, want RecloneRequired with the lost set", st.Sites[1])
		}
		if out := logs.String(); !strings.Contains(out, "failover picker: refusing candidate that lacks transactions it issued itself") ||
			!strings.Contains(out, "comparedTo=dc3") {
			t.Fatalf("the refusal was not logged against dc3; logs:\n%s", out)
		}
	})
	t.Run("the same candidate with its data is promoted", func(t *testing.T) {
		dc2, tm, logs := run(t, full)
		if dc2.isReadOnly() {
			t.Fatalf("dc2 was not promoted although it lacks nothing; logs:\n%s", logs.String())
		}
		if st := tm.Status(); st.Sites[1].LostGtid != "" {
			t.Fatalf("dc2 status = %+v, want no report", st.Sites[1])
		}
	})
}
