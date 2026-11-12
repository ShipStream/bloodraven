package controller

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// A manager the runner starts restores a site's report from status, so the
// site stays out of replication and promotion across an operator restart,
// and has the durable store of the sites' server_uuids.
func TestStartManager_RestoresOwnTransactionsLostAndWiresTheHistory(t *testing.T) {
	r, c, fg, _ := lifecycleRunner(t)
	lost := int64(10)
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1"},
		{Name: "dc2", RecoveryState: v1alpha1.RecoveryStateRecloneRequired, LostGtid: lossLostGTID, LostTransactionCount: &lost, LostComparedTo: "dc1"},
	}
	t.Cleanup(func() { metrics.OwnTransactionsLost.WithLabelValues("dc2").Set(0) })
	if err := r.startManager(context.Background(), fg, mustTopologyConfig(fg), nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	nn := FailoverGroupNamespacedName(fg)
	mt := registeredManager(r, nn)
	if mt == nil {
		t.Fatal("no manager registered")
	}
	mt.tm.mu.RLock()
	rec := mt.tm.ownLoss["dc2"]
	other := mt.tm.ownLoss["dc1"]
	mt.tm.mu.RUnlock()
	if rec == nil || rec.lostGtid != lossLostGTID || rec.lostCount != 10 {
		t.Fatalf("restored report = %+v, want %s (10)", rec, lossLostGTID)
	}
	if rec.comparedTo != "dc1" || rec.witness || rec.detectedBy != "" {
		t.Fatalf("restored report = %+v, want the site it was compared with (dc1) and no kind until it is compared again", rec)
	}
	if other != nil {
		t.Fatalf("dc1 got a report: %+v", other)
	}
	if err := mt.tm.withReplicaStartGate("dc2", func() error { return nil }); err == nil {
		t.Fatal("the restored report does not keep the site's replication from being started")
	}

	mt.tm.siteIdentity.mu.Lock()
	store, loaded := mt.tm.siteIdentity.store, mt.tm.siteIdentity.loaded
	mt.tm.siteIdentity.mu.Unlock()
	if store != nil && !loaded {
		t.Fatal("the history was not loaded when the manager started: a manager that starts into an outage would choose a promotion target without it")
	}
	if store == nil {
		t.Fatal("the runner's manager has no SiteIdentityStore: a site back on a volume from before a reclone would be compared under its current server_uuid only")
	}
	if err := store.Add(context.Background(), "dc2", lossUUIDOld); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Annotations[v1alpha1.ServerUUIDHistoryAnnotation], lossUUIDOld) {
		t.Fatalf("annotations = %v, want the history on the manager's group", got.Annotations)
	}
}

// A planned failover is not admitted onto a site that lacks transactions
// it issued itself.
func TestPlannedClaim_RefusesATargetThatLacksItsOwnTransactions(t *testing.T) {
	f := newOpsFixture(t, "")
	req := PlannedClaimRequest{RunID: "run-9", Target: "dc2"}

	f.tm.mu.Lock()
	_, _, decided := f.tm.plannedClaimRefusalLocked(req)
	f.tm.mu.Unlock()
	if decided {
		t.Fatal("setup: the claim is refused without a report")
	}

	f.tm.SetOwnTransactionsLost("dc2", opsUUIDB+":5-7", 3)
	t.Cleanup(func() { metrics.OwnTransactionsLost.WithLabelValues("dc2").Set(0) })
	f.tm.mu.Lock()
	d, _, decided := f.tm.plannedClaimRefusalLocked(req)
	f.tm.mu.Unlock()
	if !decided || d.Kind == ClaimGranted || d.RejectReason != v1alpha1.PlannedFailoverReasonTargetUnhealthy ||
		!strings.Contains(d.Message, "lacks 3 transactions it issued itself") {
		t.Fatalf("decision = %+v (decided %v), want a TargetUnhealthy rejection naming the lost transactions", d, decided)
	}
}

// A status written against a CRD from before lostGtid stores a site's
// own-loss report as RecoveryBlocked without any set. A manager that starts
// on it restores the report all the same, without its set: the site stays
// out of promotion, and the report ends only by the rule of every report.
func TestStartManager_RestoresAnOwnLossReportDegradedForAnOlderCRD(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1"},
		{Name: "dc2", RecoveryState: v1alpha1.RecoveryStateBlocked},
	}
	if !ownLossReportDegradedForOlderCRD(fg.Status.Sites[1]) {
		t.Fatal("the degraded shape is not recognised")
	}
	// What a real divergence block looks like, and a site without a report.
	div := int64(3)
	for _, s := range []v1alpha1.SiteStatus{
		{Name: "dc2", RecoveryState: v1alpha1.RecoveryStateBlocked, DivergentGtid: lossUUIDOld + ":101-103", DivergentTransactionCount: &div},
		{Name: "dc2", RecoveryState: v1alpha1.RecoveryStateInProgress},
		{Name: "dc2"},
	} {
		if ownLossReportDegradedForOlderCRD(s) {
			t.Fatalf("%+v was taken for a degraded own-loss report", s)
		}
	}
	if err := r.startManager(context.Background(), fg, mustTopologyConfig(fg), nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, FailoverGroupNamespacedName(fg))
	if mt == nil {
		t.Fatal("no manager registered")
	}
	mt.tm.mu.RLock()
	rec := mt.tm.ownLoss["dc2"]
	mt.tm.mu.RUnlock()
	if rec == nil || rec.lostGtid != "" || rec.witness || rec.detectedBy != "" {
		t.Fatalf("restored report = %+v, want one without a set and without a kind", rec)
	}
	if err := mt.tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("handoff to dc2 is not refused after a restart on a status an older CRD stored")
	}
	if err := mt.tm.withReplicaStartGate("dc2", func() error { return nil }); err == nil {
		t.Fatal("the restored report does not keep the site's replication from being started")
	}
}
