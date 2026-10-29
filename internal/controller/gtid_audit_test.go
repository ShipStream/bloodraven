package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// Issue #202's sets after the lossy ordered update: dc1 was promoted
// without d1240b6c…:1741933-1742153, which stayed on the demoted dc2 while
// dc2 replicated every later dc1 transaction.
const (
	issue202PrimaryUUID = "d07c0c83-71ca-11f0-9e33-c80aa9429562"
	issue202ReplicaUUID = "d1240b6c-71ca-11f0-9e33-c80aa9429563"
	issue202PrimaryGTID = issue202PrimaryUUID + ":1-240010," + issue202ReplicaUUID + ":1-1741932"
	issue202ReplicaGTID = issue202PrimaryUUID + ":1-240010," + issue202ReplicaUUID + ":1-1742153"
	issue202ErrantGTID  = issue202ReplicaUUID + ":1741933-1742153"
)

// auditServer is a mockMySQL that also answers the audit's combined
// server_uuid + gtid_executed read. afterRead runs after each snapshot
// read with the 1-based read count, so a test can move time on between
// the audit's reads.
type auditServer struct {
	*mockMySQL
	smu       sync.Mutex
	uuid      string
	snapErr   error
	snapReads int
	afterRead func(n int)
}

func (s *auditServer) ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error) {
	s.smu.Lock()
	s.snapReads++
	n, uuid, err, hook := s.snapReads, s.uuid, s.snapErr, s.afterRead
	s.smu.Unlock()
	if err != nil {
		return mysql.GTIDSnapshot{}, err
	}
	s.mockMySQL.mu.Lock()
	gtid := s.mockMySQL.gtidExecuted
	s.mockMySQL.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return mysql.GTIDSnapshot{ServerUUID: uuid, GtidExecuted: gtid}, nil
}

func (s *auditServer) setUUID(uuid string) {
	s.smu.Lock()
	defer s.smu.Unlock()
	s.uuid = uuid
}

func (s *auditServer) setSnapErr(err error) {
	s.smu.Lock()
	defer s.smu.Unlock()
	s.snapErr = err
}

func (s *auditServer) reads() int {
	s.smu.Lock()
	defer s.smu.Unlock()
	return s.snapReads
}

const auditPrimaryHost = "mysql-primary-internal.ns.svc.cluster.local"

func auditPrimary(uuid, gtid string) *auditServer {
	return &auditServer{mockMySQL: &mockMySQL{readOnly: false, gtidExecuted: gtid}, uuid: uuid}
}

// auditReplica is a read-only follower with healthy threads replicating
// directly from the primary.
func auditReplica(uuid, gtid string) *auditServer {
	lag := int64(0)
	return &auditServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: gtid,
		replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: auditPrimaryHost,
			SecondsBehindSource: &lag, ExecutedGtidSet: gtid}}, uuid: uuid}
}

// newAuditManager builds a manager over primary, follower-a and
// (optionally) follower-b on a fake clock.
func newAuditManager(t *testing.T, clk *clock.FakeClock, roles []state.SiteRole, servers ...*auditServer) *TopologyManager {
	t.Helper()
	names := []string{"primary", "follower-a", "follower-b"}
	sites := make([]SiteTopologyConfig, len(servers))
	checkers := make([]mysql.Checker, len(servers))
	for i, s := range servers {
		sites[i] = SiteTopologyConfig{Name: names[i], Role: roles[i], Host: "mysql-" + names[i] + "-internal.ns.svc.cluster.local"}
		checkers[i] = s
	}
	tm := NewTopologyManagerWithClock(TopologyConfig{Name: "fg", Sites: sites, ReadOnlyMaxLagSeconds: 30}, checkers,
		NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"}, newMockTainter(), platform.NewHub(testLogger()), nil, testLogger(), clk)
	tm.failoverCooldown = 0
	return tm
}

func twoSiteAudit(t *testing.T, primary, replica *auditServer) (*TopologyManager, *clock.FakeClock) {
	t.Helper()
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 5, 4, 27, 0, time.UTC))
	tm := newAuditManager(t, clk, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate}, primary, replica)
	return tm, clk
}

func recoveryOf(tm *TopologyManager, site string) siteRecovery {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if rec := tm.recovery[site]; rec != nil {
		return *rec
	}
	return siteRecovery{}
}

func divergentGauge(site string) float64 {
	return promtestutil.ToFloat64(metrics.DivergentTransactions.WithLabelValues(site))
}

// statusRunner wires tm's status callback to a runner writing into a fake
// API server. It returns a function reading the written conditions and the
// runner's event recorder.
func statusRunner(t *testing.T, tm *TopologyManager) (func() map[string]metav1.Condition, *record.FakeRecorder) {
	t.Helper()
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	recorder := record.NewFakeRecorder(100)
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), recorder: recorder, managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	runner.SetManagerForTest(nn, tm)
	tm.StatusCallback = func(snap TopologySnapshot) {
		err := runner.updateCRStatus(context.Background(), nn, snap)
		tm.MarkStatusWriteResult(err)
	}
	return func() map[string]metav1.Condition {
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &got); err != nil {
			t.Fatal(err)
		}
		out := map[string]metav1.Condition{}
		for _, cond := range got.Status.Conditions {
			out[cond.Type] = cond
		}
		return out
	}, recorder
}

// #202: a replica holding transactions the primary lacks, with healthy IO
// and SQL threads, is reported divergent with the exact set, the gauge and
// Degraded=DivergentTransactions. Ready stays True: the group serves.
func TestGTIDAudit_Issue202HealthyReplicaIsBlocked(t *testing.T) {
	primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
	replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
	tm, _ := twoSiteAudit(t, primary, replica)
	conditions, events := statusRunner(t, tm)

	pollN(tm, 2)

	rec := recoveryOf(tm, "follower-a")
	if rec.state != recoveryStateBlocked || rec.divergentGtid != issue202ErrantGTID || rec.divergentCount != 221 {
		t.Fatalf("recovery = %+v, want RecoveryBlocked with %s (221)", rec, issue202ErrantGTID)
	}
	if rec.detectedBy != divergenceDetectedByAudit || rec.comparedTo != "primary" {
		t.Errorf("provenance detectedBy=%q comparedTo=%q, want ReplicaAudit against primary", rec.detectedBy, rec.comparedTo)
	}
	if got := divergentGauge("follower-a"); got != 221 {
		t.Errorf("bloodraven_divergent_transactions{follower-a} = %v, want 221", got)
	}
	st := tm.Status()
	if st.Sites[1].DivergentGtid != issue202ErrantGTID || st.Sites[1].RecoveryState != recoveryStateBlocked {
		t.Errorf("status site = %+v, want the divergent set", st.Sites[1])
	}
	conds := conditions()
	if d := conds["Degraded"]; d.Status != metav1.ConditionTrue || d.Reason != "DivergentTransactions" ||
		!strings.Contains(d.Message, "follower-a") || !strings.Contains(d.Message, "221") {
		t.Errorf("Degraded = %+v, want True/DivergentTransactions naming follower-a and 221", d)
	}
	if r := conds["Ready"]; r.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %+v, want True: divergence must not become an outage", r)
	}
	if replica.stopReplicaCalls != 0 || replica.resetReplicaCalls != 0 || replica.changeSourceCalls != 0 {
		t.Errorf("audit mutated replication: stop=%d reset=%d change=%d", replica.stopReplicaCalls, replica.resetReplicaCalls, replica.changeSourceCalls)
	}
	var warnings []string
	for len(events.Events) > 0 {
		warnings = append(warnings, <-events.Events)
	}
	found := false
	for _, e := range warnings {
		if strings.Contains(e, "DataLossDetected") {
			t.Errorf("replica audit emitted the old-primary DataLossDetected Event: %q", e)
		}
		if strings.HasPrefix(e, "Warning DivergentReplicaDetected") && strings.Contains(e, "221 transactions on follower-a") {
			found = true
		}
	}
	if !found {
		t.Errorf("no DivergentReplicaDetected Warning Event; events %q", warnings)
	}
}

// The follower is read first. A transaction the primary commits and the
// follower applies between the two reads is in both sets by the time the
// primary is read, so it is not reported. Reading the primary first would
// report it.
func TestGTIDAudit_TransactionAppliedBetweenReadsIsNotDivergent(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := auditPrimary(uuid, uuid+":1-100")
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	next := 100
	var mu sync.Mutex
	advance := func(int) {
		mu.Lock()
		defer mu.Unlock()
		next++
		set := uuid + ":1-" + strconv.Itoa(next)
		primary.setGtidExecuted(set)
		replica.setGtidExecuted(set)
	}
	primary.afterRead, replica.afterRead = advance, advance
	tm, _ := twoSiteAudit(t, primary, replica)

	pollN(tm, 2)

	if replica.reads() == 0 {
		t.Fatal("follower was never audited")
	}
	if rec := recoveryOf(tm, "follower-a"); rec.state == recoveryStateBlocked {
		t.Fatalf("follower reported divergent (%s) for a transaction it applied between the reads", rec.divergentGtid)
	}
}

// A result is discarded when the authority or either server changed while
// the audit was reading: the sets no longer describe one primary and one
// follower.
func TestGTIDAudit_DiscardsResultWhenAuthorityOrIdentityChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(tm *TopologyManager, primary, replica *auditServer)
	}{
		{"primary server replaced", func(_ *TopologyManager, primary, _ *auditServer) {
			primary.afterRead = func(n int) {
				if n == 1 {
					primary.setUUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
				}
			}
		}},
		{"follower server replaced", func(_ *TopologyManager, _, replica *auditServer) {
			replica.afterRead = func(n int) {
				if n == 1 {
					replica.setUUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
				}
			}
		}},
		{"authority changed", func(tm *TopologyManager, _, replica *auditServer) {
			replica.afterRead = func(n int) {
				if n == 1 {
					tm.mu.Lock()
					tm.authorityEpoch++
					tm.mu.Unlock()
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
			replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
			tm, _ := twoSiteAudit(t, primary, replica)
			tc.mutate(tm, primary, replica)

			pollN(tm, 1)

			if replica.reads() == 0 {
				t.Fatal("follower was never audited")
			}
			if rec := recoveryOf(tm, "follower-a"); rec.state == recoveryStateBlocked {
				t.Fatalf("published a result read across a change: %+v", rec)
			}
		})
	}
}

// A probe or parse failure proves nothing: an existing block stays, and no
// block is invented.
func TestGTIDAudit_ProbeFailureIsUnverified(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blocked bool
		inject  func(replica *auditServer)
	}{
		{"read error keeps block", true, func(r *auditServer) { r.setSnapErr(errors.New("i/o timeout")) }},
		{"unparseable set keeps block", true, func(r *auditServer) { r.setGtidExecuted("garbage") }},
		{"read error records nothing", false, func(r *auditServer) { r.setSnapErr(errors.New("i/o timeout")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
			replica := auditReplica(issue202ReplicaUUID, issue202PrimaryGTID)
			tm, _ := twoSiteAudit(t, primary, replica)
			if tc.blocked {
				tm.SetRecoveryBlocked("follower-a", issue202ErrantGTID, 221)
			}
			tc.inject(replica)

			pollN(tm, 2)

			rec := recoveryOf(tm, "follower-a")
			if tc.blocked && (rec.state != recoveryStateBlocked || rec.divergentGtid != issue202ErrantGTID) {
				t.Fatalf("unverified audit changed the block: %+v", rec)
			}
			if !tc.blocked && rec.state != "" {
				t.Fatalf("unverified audit recorded %+v", rec)
			}
		})
	}
}

// Healthy threads and a converged source are not a containment proof. The
// block survives them, including clearHealthyRecoverySites, until a fresh
// audit proves the follower holds nothing the primary lacks.
func TestGTIDAudit_BlockSurvivesHealthyReplication(t *testing.T) {
	primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
	replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
	tm, clk := twoSiteAudit(t, primary, replica)
	pollN(tm, 2)
	if rec := recoveryOf(tm, "follower-a"); rec.state != recoveryStateBlocked {
		t.Fatalf("setup: recovery = %+v, want blocked", rec)
	}

	for i := 0; i < 5; i++ {
		clk.Advance(time.Second)
		pollN(tm, 1)
	}
	tm.clearHealthyRecoverySites(context.Background(), []*mysql.ReplicaStatus{nil, replica.replicaStatusVal})

	if rec := recoveryOf(tm, "follower-a"); rec.state != recoveryStateBlocked || rec.divergentCount != 221 {
		t.Fatalf("block cleared without a containment proof: %+v", rec)
	}
}

// Making the primary contain the set (replaying it, or injecting empty
// transactions) resolves the divergence; the next audit proves it.
func TestGTIDAudit_ClearsWhenPrimaryGainsContainment(t *testing.T) {
	primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
	replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
	tm, clk := twoSiteAudit(t, primary, replica)
	conditions, _ := statusRunner(t, tm)
	pollN(tm, 2)

	primary.setGtidExecuted(issue202ReplicaGTID)
	clk.Advance(recoveryRetryDelay + time.Second)
	pollN(tm, 1)

	if rec := recoveryOf(tm, "follower-a"); rec.state != "" {
		t.Fatalf("block not cleared after the primary gained the set: %+v", rec)
	}
	if got := divergentGauge("follower-a"); got != 0 {
		t.Errorf("gauge = %v, want 0", got)
	}
	if d := conditions()["Degraded"]; d.Reason == "DivergentTransactions" {
		t.Errorf("Degraded still DivergentTransactions after the proof: %+v", d)
	}
}

// A reclone replaces the follower's data with the primary's; the audit
// after the clone proves containment and clears the block.
func TestGTIDAudit_ClearsAfterReclone(t *testing.T) {
	primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
	replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
	tm, clk := twoSiteAudit(t, primary, replica)
	pollN(tm, 2)

	replica.setGtidExecuted(issue202PrimaryGTID)
	clk.Advance(recoveryRetryDelay + time.Second)
	pollN(tm, 1)

	if rec := recoveryOf(tm, "follower-a"); rec.state != "" {
		t.Fatalf("block not cleared after reclone: %+v", rec)
	}
}

// After an operator restart the block is restored from status and stays
// until a fresh audit decides; a still-divergent follower keeps it.
func TestGTIDAudit_RehydratedBlockIsReverified(t *testing.T) {
	for _, tc := range []struct {
		name        string
		replicaGTID string
		wantBlocked bool
	}{
		{"still divergent", issue202ReplicaGTID, true},
		{"resolved while the operator was down", issue202PrimaryGTID, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
			replica := auditReplica(issue202ReplicaUUID, tc.replicaGTID)
			tm, _ := twoSiteAudit(t, primary, replica)
			tm.SetRecoveryBlocked("follower-a", issue202ErrantGTID, 221)

			pollN(tm, 2)

			rec := recoveryOf(tm, "follower-a")
			if tc.wantBlocked && (rec.state != recoveryStateBlocked || rec.divergentGtid != issue202ErrantGTID) {
				t.Fatalf("recovery = %+v, want the restored block kept", rec)
			}
			if !tc.wantBlocked && rec.state != "" {
				t.Fatalf("recovery = %+v, want the block cleared by the fresh audit", rec)
			}
		})
	}
}

// Non-promotable readers are audited too: a reader holding extra history
// is reported even though it can never be promoted.
func TestGTIDAudit_AuditsReaderSites(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := auditPrimary(uuid, uuid+":1-100")
	candidate := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	reader := auditReplica("cccccccc-cccc-cccc-cccc-cccccccccccc", uuid+":1-100,cccccccc-cccc-cccc-cccc-cccccccccccc:1-2")
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	tm := newAuditManager(t, clk, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly},
		primary, candidate, reader)

	pollN(tm, 2)

	if rec := recoveryOf(tm, "follower-b"); rec.state != recoveryStateBlocked || rec.divergentCount != 2 {
		t.Fatalf("reader recovery = %+v, want blocked with 2 transactions", rec)
	}
	if rec := recoveryOf(tm, "follower-a"); rec.state != "" {
		t.Errorf("clean candidate recovery = %+v, want none", rec)
	}
}

// The audit runs on the recovery retry cadence, not every poll, with a
// forced pass when the manager starts.
func TestGTIDAudit_IsRateLimited(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := auditPrimary(uuid, uuid+":1-100")
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	tm, clk := twoSiteAudit(t, primary, replica)

	pollN(tm, 1)
	first := replica.reads()
	if first == 0 {
		t.Fatal("no forced audit on the first poll with a confirmed primary")
	}
	for i := 0; i < 5; i++ {
		clk.Advance(time.Second)
		pollN(tm, 1)
	}
	if got := replica.reads(); got != first {
		t.Fatalf("follower re-audited within the interval: %d reads, want %d", got, first)
	}
	clk.Advance(recoveryRetryDelay)
	pollN(tm, 1)
	if got := replica.reads(); got <= first {
		t.Fatal("follower not re-audited after the interval")
	}
}

// Degraded is last-writer-wins across the per-site replication reasons.
// A divergence block must not be hidden by a lagging or broken replica.
func TestUpdateCRStatus_DivergenceWinsOverReplicationReasons(t *testing.T) {
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	lag := int64(900)
	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly,
				ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged,
				Replication:   &mysql.ReplicaStatus{IORunning: false, SQLRunning: true, SourceHost: "dc1", SecondsBehindSource: &lag},
				RecoveryState: recoveryStateBlocked, DivergentGtid: issue202ErrantGTID, DivergentTxnCount: 221},
		},
		ActiveSite:     "dc1",
		DegradedReason: "Healthy",
	}); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatal(err)
	}
	for _, cond := range got.Status.Conditions {
		if cond.Type == "Degraded" {
			if cond.Status != metav1.ConditionTrue || cond.Reason != "DivergentTransactions" {
				t.Fatalf("Degraded = %+v, want True/DivergentTransactions", cond)
			}
			return
		}
	}
	t.Fatal("no Degraded condition")
}

// A block restored from status for a site that is now the confirmed
// primary is not compared with itself (which would always look clean): it
// becomes the durable divergent-promotion note.
func TestGTIDAudit_RestoredBlockOnPrimaryBecomesPromotionNote(t *testing.T) {
	primary := auditPrimary(issue202ReplicaUUID, issue202ReplicaGTID)
	replica := auditReplica(issue202PrimaryUUID, issue202ReplicaGTID)
	tm, _ := twoSiteAudit(t, primary, replica)
	tm.SetLastFailoverTarget("primary")
	tm.SetRecoveryBlockedFrom("primary", issue202ErrantGTID, 221, divergenceDetectedByAudit, "follower-a")

	pollN(tm, 2)

	if rec := recoveryOf(tm, "primary"); rec.state != "" {
		t.Fatalf("block kept on the primary: %+v", rec)
	}
	note := tm.buildSnapshot(nil).DivergentPromotion
	if note == nil || note.Site != "primary" || note.DivergentTransactions != 221 || note.ComparedTo != "follower-a" {
		t.Fatalf("DivergentPromotion = %+v, want primary with 221 transactions against follower-a", note)
	}
}

// Provenance is persisted and restored with the block, so a restarted
// operator reports the same comparison it made before.
func TestUpdateCRStatus_PersistsDivergenceProvenance(t *testing.T) {
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true,
				SourceConvergenceState: sourceConvergenceConverged,
				RecoveryState:          recoveryStateBlocked, DivergentGtid: issue202ErrantGTID, DivergentTxnCount: 221,
				DivergenceDetectedBy: divergenceDetectedByAudit, DivergenceComparedTo: "dc1"},
		},
		ActiveSite: "dc1", DegradedReason: "Healthy",
	}); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatal(err)
	}
	s := got.Status.Sites[1]
	if s.DivergenceDetectedBy != divergenceDetectedByAudit || s.DivergenceComparedTo != "dc1" || s.DivergentGtid != issue202ErrantGTID {
		t.Fatalf("site status = %+v, want the audit provenance against dc1", s)
	}
}

// The production checker reads identity and set together; without it the
// audit could not tell a replaced server from a changed set.
func TestGTIDAudit_ProductionCheckerReadsIdentityWithSet(t *testing.T) {
	replica := testutil.NewFakeReplica(testutil.FakeReplicaState{
		ServerUUID: issue202ReplicaUUID, ExecutedGtidSet: issue202ReplicaGTID, ReadOnly: true,
	})
	reader, ok := replica.Checker().(gtidSnapshotReader)
	if !ok {
		t.Fatal("production mysql.Checker does not implement gtidSnapshotReader")
	}
	snap, err := reader.ReadGTIDSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.ServerUUID != issue202ReplicaUUID || snap.GtidExecuted != issue202ReplicaGTID {
		t.Fatalf("snapshot = %+v", snap)
	}
}

// slowPrimary answers CheckReadOnly only after delay, and its GTID
// snapshot never answers before its context ends: a primary that is
// slow to confirm and then stalls.
type slowPrimary struct {
	*auditServer
	delay time.Duration
	slow  atomic.Bool
}

func (p *slowPrimary) CheckReadOnly(ctx context.Context) (bool, error) {
	if p.slow.Load() {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return p.auditServer.CheckReadOnly(ctx)
}

func (p *slowPrimary) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	if p.slow.Load() {
		<-ctx.Done()
		return mysql.GTIDSnapshot{}, ctx.Err()
	}
	return p.auditServer.ReadGTIDSnapshot(ctx)
}

// The audit runs on the poll goroutine, so whatever it waits on delays the
// next poll's failure detection. Confirming the primary and every read
// share one budget: a primary that confirms slowly and then stalls costs
// at most that budget, not a fresh one after the confirmation.
func TestGTIDAudit_SlowPrimaryCostsAtMostOneBudget(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := &slowPrimary{auditServer: auditPrimary(uuid, uuid+":1-100"), delay: 3 * time.Second}
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	names := []string{"primary", "follower-a"}
	sites := make([]SiteTopologyConfig, 2)
	for i, n := range names {
		sites[i] = SiteTopologyConfig{Name: n, Role: state.SiteRolePrimaryCandidate, Host: "mysql-" + n + "-internal.ns.svc.cluster.local"}
	}
	tm := NewTopologyManagerWithClock(TopologyConfig{Name: "fg", Sites: sites}, []mysql.Checker{primary, replica},
		NewFailoverController(testLogger()), nil, nil, BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), nil, testLogger(), clk)
	pollN(tm, 2)
	clk.Advance(gtidAuditInterval + time.Second)
	primary.slow.Store(true)

	start := time.Now()
	tm.auditFollowerDivergence(context.Background(), []*mysql.ReplicaStatus{nil, replica.replicaStatusVal}, nil)
	elapsed := time.Since(start)

	if elapsed > gtidAuditBudget+time.Second {
		t.Fatalf("audit took %s, want at most its %s budget", elapsed, gtidAuditBudget)
	}
	if rec := recoveryOf(tm, "follower-a"); rec.state != "" {
		t.Fatalf("an unverified audit recorded %+v", rec)
	}
}

// Nothing due means nothing probed: the audit adds no primary round trip
// to the polls between its passes.
func TestGTIDAudit_ProbesNothingWhenNoFollowerIsDue(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := auditPrimary(uuid, uuid+":1-100")
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	tm, clk := twoSiteAudit(t, primary, replica)
	repl := []*mysql.ReplicaStatus{nil, replica.replicaStatusVal}
	pollN(tm, 2)
	clk.Advance(time.Second)

	primary.mu.Lock()
	confirmsBefore := primary.checkReadOnlyCalls
	primary.mu.Unlock()
	readsBefore := primary.reads()
	tm.auditFollowerDivergence(context.Background(), repl, nil)

	primary.mu.Lock()
	confirms := primary.checkReadOnlyCalls - confirmsBefore
	primary.mu.Unlock()
	if confirms != 0 || primary.reads() != readsBefore {
		t.Fatalf("audit with no follower due probed the primary: %d confirmations, %d snapshot reads", confirms, primary.reads()-readsBefore)
	}
}

// The primary replaced under the same site name (a restore or a new pod
// with a new server_uuid) invalidates every earlier comparison. The
// followers' receivers report the new Source_UUID as soon as they
// reconnect, so the audit sees the replacement without probing the
// primary on every poll, and runs at once instead of waiting out the
// interval.
func TestGTIDAudit_SameSitePrimaryReplacementForcesAudit(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	const replacement = "cccccccc-cccc-cccc-cccc-cccccccccccc"
	primary := auditPrimary(uuid, uuid+":1-100")
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	replica.replicaStatusVal.SourceUUID = uuid
	tm, clk := twoSiteAudit(t, primary, replica)
	pollN(tm, 2)
	before := replica.reads()

	primary.setUUID(replacement)
	replica.mockMySQL.mu.Lock()
	replica.replicaStatusVal.SourceUUID = replacement
	replica.mockMySQL.mu.Unlock()
	clk.Advance(time.Second)
	pollN(tm, 1)

	if replica.reads() == before {
		t.Fatal("follower not re-audited after the primary was replaced under the same name")
	}
}

// While authority is moving (an ordered update or planned failover runs,
// a topology change or promotion is pending) a comparison against the
// active site does not describe a settled primary and follower: a read of
// the new primary just after a handoff, then of the fenced old one, would
// report the new primary's writes as divergence. The audit stays out.
func TestGTIDAudit_SuppressedWhileAuthorityMoves(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(tm *TopologyManager)
	}{
		{"ordered update running", func(tm *TopologyManager) { tm.deploymentUpdateRunning = true }},
		{"topology change in flight", func(tm *TopologyManager) { tm.deploymentChanging = 1 }},
		{"planned failover active", func(tm *TopologyManager) { tm.plannedFailoverActive = true }},
		{"promotion pending", func(tm *TopologyManager) { tm.promotedSite, tm.promotedAt = "follower-a", tm.clock.Now() }},
		{"in-place restore", func(tm *TopologyManager) { tm.topologyFrozen = true }},
		{"initial restore", func(tm *TopologyManager) { tm.autoBootstrapSuppressed = true }},
		{"bootstrap", func(tm *TopologyManager) { tm.bootstrapPhase = BootstrapPhaseCloning }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The follower is the new primary's peer whose set looks
			// "extra" against the fenced old source.
			primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
			replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
			tm, _ := twoSiteAudit(t, primary, replica)
			pollN(tm, 1)
			tm.mu.Lock()
			tm.recovery = map[string]*siteRecovery{}
			tm.gtidAudit = map[string]*gtidAuditSchedule{}
			tm.gtidAuditPrimary = ""
			tc.set(tm)
			tm.mu.Unlock()
			before := replica.reads()

			tm.auditFollowerDivergence(context.Background(), []*mysql.ReplicaStatus{nil, replica.replicaStatusVal}, nil)

			if replica.reads() != before {
				t.Errorf("audit read the follower while suppressed")
			}
			if rec := recoveryOf(tm, "follower-a"); rec.state != "" {
				t.Errorf("audit recorded %+v while suppressed", rec)
			}
			if tm.pendingDivergentPromotion != nil {
				t.Errorf("promotion note recorded while suppressed")
			}
		})
	}
}

// Changed evidence is announced even when its size is unchanged: one
// extra transaction reconciled on the primary while the follower gained a
// different one is a new set the Event stream must carry.
func TestFailoverEvents_ChangedDivergentSetWithSameCount(t *testing.T) {
	primary := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
	replica := auditReplica(issue202ReplicaUUID, issue202ReplicaGTID)
	tm, clk := twoSiteAudit(t, primary, replica)
	_, events := statusRunner(t, tm)
	pollN(tm, 2)
	for len(events.Events) > 0 {
		<-events.Events
	}

	primary.setGtidExecuted(issue202PrimaryUUID + ":1-240010," + issue202ReplicaUUID + ":1-1741933")
	replica.setGtidExecuted(issue202PrimaryUUID + ":1-240010," + issue202ReplicaUUID + ":1-1742154")
	clk.Advance(gtidAuditInterval + time.Second)
	pollN(tm, 1)

	if rec := recoveryOf(tm, "follower-a"); rec.divergentCount != 221 || rec.divergentGtid != issue202ReplicaUUID+":1741934-1742154" {
		t.Fatalf("setup: recovery = %+v, want the shifted 221-transaction set", rec)
	}
	found := false
	for len(events.Events) > 0 {
		if strings.Contains(<-events.Events, "DivergentReplicaDetected") {
			found = true
		}
	}
	if !found {
		t.Fatal("no DivergentReplicaDetected Event for a changed set of the same size")
	}
}

// A handoff refused before fencing because the candidate holds extra
// transactions is evidence the audit has not seen yet. The audit runs on
// the next poll instead of after its interval, so the update stops
// retrying a doomed handoff within one poll.
func TestGTIDAudit_HandoffRefusalWakesAudit(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := auditPrimary(uuid, uuid+":1-100")
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	names := []string{"primary", "follower-a"}
	sites := make([]SiteTopologyConfig, 2)
	for i, n := range names {
		sites[i] = SiteTopologyConfig{Name: n, Role: state.SiteRolePrimaryCandidate, Host: "mysql-" + n + "-internal.ns.svc.cluster.local"}
	}
	fc := NewFailoverController(testLogger())
	updater := NewUpdateController(fc, testLogger())
	updater.tickInterval = time.Millisecond
	updater.handoff = HandoffOptions{PreFenceCatchUp: 50 * time.Millisecond, CatchUp: 50 * time.Millisecond, PollInterval: time.Millisecond}
	tm := NewTopologyManagerWithClock(TopologyConfig{Name: "fg", Sites: sites}, []mysql.Checker{primary, replica},
		fc, updater, nil, BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(testLogger()), nil, testLogger(), clk)
	tm.ApplyUpdate = func(context.Context, string) error { return nil }
	pollN(tm, 2)

	replica.setGtidExecuted(uuid + ":1-100," + issue202ReplicaUUID + ":1-3")
	tm.SetSpecDriftSites([]string{"primary"})
	clk.Advance(time.Second)
	pollN(tm, 1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		tm.mu.RLock()
		busy := tm.deploymentUpdatePending || tm.deploymentUpdateRunning
		tm.mu.RUnlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ordered update did not end")
		}
		time.Sleep(time.Millisecond)
	}
	if primary.superReadOnly {
		t.Fatal("source fenced for a divergent candidate")
	}

	clk.Advance(time.Second)
	pollN(tm, 1)

	if rec := recoveryOf(tm, "follower-a"); rec.state != recoveryStateBlocked || rec.divergentCount != 3 {
		t.Fatalf("recovery = %+v, want the refused candidate audited and blocked on the next poll", rec)
	}
}

// stallingSnapshotPrimary answers liveness normally but, once stalled,
// never answers the audit's snapshot read before its context ends.
type stallingSnapshotPrimary struct {
	*auditServer
	stalled atomic.Bool
}

func (p *stallingSnapshotPrimary) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	if p.stalled.Load() {
		<-ctx.Done()
		return mysql.GTIDSnapshot{}, ctx.Err()
	}
	return p.auditServer.ReadGTIDSnapshot(ctx)
}

// An ordered update that fails every poll claims the mutation window (and
// moves authorityEpoch) every poll without changing authority. That must
// not force an audit pass each time, nor erase the backoff an unverified
// pass left: otherwise a slow primary costs the whole audit budget on
// every poll while the rollout keeps failing.
func TestGTIDAudit_FailingUpdateDoesNotForceAuditEveryPoll(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := &stallingSnapshotPrimary{auditServer: auditPrimary(uuid, uuid+":1-100")}
	replica := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	names := []string{"primary", "follower-a"}
	sites := make([]SiteTopologyConfig, 2)
	for i, n := range names {
		sites[i] = SiteTopologyConfig{Name: n, Role: state.SiteRolePrimaryCandidate, Host: "mysql-" + n + "-internal.ns.svc.cluster.local"}
	}
	fc := NewFailoverController(testLogger())
	updater := NewUpdateController(fc, testLogger())
	updater.tickInterval = time.Millisecond
	tm := NewTopologyManagerWithClock(TopologyConfig{Name: "fg", Sites: sites}, []mysql.Checker{primary, replica},
		fc, updater, nil, BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(testLogger()), nil, testLogger(), clk)
	tm.ApplyUpdate = func(context.Context, string) error { return errors.New("admission webhook refuses deployment") }
	pollN(tm, 2)
	if replica.reads() == 0 {
		t.Fatal("setup: no initial audit")
	}

	primary.stalled.Store(true)
	tm.SetSpecDriftSites([]string{"follower-a"})
	waitUpdate := func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			tm.mu.RLock()
			busy := tm.deploymentUpdatePending || tm.deploymentUpdateRunning
			tm.mu.RUnlock()
			if !busy {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("ordered update did not end")
			}
			time.Sleep(time.Millisecond)
		}
	}
	readsBefore := replica.reads()
	tm.mu.RLock()
	epochBefore := tm.authorityEpoch
	tm.mu.RUnlock()
	var slowest time.Duration
	for i := 0; i < 4; i++ {
		clk.Advance(time.Second)
		start := time.Now()
		tm.Poll(context.Background())
		slowest = max(slowest, time.Since(start))
		waitUpdate()
	}
	tm.mu.RLock()
	epochAfter := tm.authorityEpoch
	tm.mu.RUnlock()
	if epochAfter == epochBefore {
		t.Fatal("setup: the failing update never claimed the mutation window")
	}
	if got := replica.reads() - readsBefore; got != 0 {
		t.Errorf("follower re-audited %d times by failed update attempts within the interval", got)
	}
	if slowest > 2*time.Second {
		t.Errorf("slowest poll took %s: failed update attempts made the stalled audit run every poll", slowest)
	}
}

// A primary that confirms but stalls on its GTID read makes the whole pass
// unverified: every due follower waits out its interval, so the stall
// costs one budget per interval, not one per follower on successive polls.
func TestGTIDAudit_PrimaryReadStallCostsOneBudgetPerInterval(t *testing.T) {
	const uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := &stallingSnapshotPrimary{auditServer: auditPrimary(uuid, uuid+":1-100")}
	followerA := auditReplica("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", uuid+":1-100")
	followerB := auditReplica("cccccccc-cccc-cccc-cccc-cccccccccccc", uuid+":1-100")
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	names := []string{"primary", "follower-a", "follower-b"}
	sites := make([]SiteTopologyConfig, 3)
	for i, n := range names {
		sites[i] = SiteTopologyConfig{Name: n, Role: state.SiteRolePrimaryCandidate, Host: "mysql-" + n + "-internal.ns.svc.cluster.local"}
	}
	followerA.replicaStatusVal.SourceHost = sites[0].Host
	followerB.replicaStatusVal.SourceHost = sites[0].Host
	tm := NewTopologyManagerWithClock(TopologyConfig{Name: "fg", Sites: sites}, []mysql.Checker{primary, followerA, followerB},
		NewFailoverController(testLogger()), nil, nil, BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), nil, testLogger(), clk)
	pollN(tm, 2)
	if followerA.reads() == 0 || followerB.reads() == 0 {
		t.Fatal("setup: no initial audit of both followers")
	}

	primary.stalled.Store(true)
	clk.Advance(gtidAuditInterval + time.Second)
	slow := 0
	for i := 0; i < 4; i++ {
		start := time.Now()
		tm.Poll(context.Background())
		if time.Since(start) > 2*time.Second {
			slow++
		}
		clk.Advance(time.Second)
	}
	if slow > 1 {
		t.Fatalf("%d slow polls in one interval, want at most one: each due follower paid the budget separately", slow)
	}
}
