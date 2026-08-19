package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// These tests cover the two self-heal fixes that back playground chaos
// scenarios 32 (MFG status-write denial) and 38 (DNSEndpoint-write denial)
// during an emergency promotion. The operator used to write both DNS and CR
// status only as side-effects of a promotion/transition; if that write was
// RBAC-denied, nothing re-attempted it on a later healthy poll.
//
// Status now arms a per-poll retry that self-heals once permissions return.
// DNS goes further: it is reconciled level-triggered against the CURRENT
// active site on every poll, so the heal survives an operator restart and can
// never replay a promotion target that has since been superseded. Neither
// re-runs promotion or mutates MySQL.

// TestUpdateCRStatus_DeniedWriteReturnsErrorThenHeals proves updateCRStatus
// now signals a denied /status write to its caller (non-nil error) and that a
// subsequent permitted write persists the desired status — the mechanism the
// runner's StatusCallback uses to arm/disarm the per-poll retry (scenario 32).
func TestUpdateCRStatus_DeniedWriteReturnsErrorThenHeals(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	deny := true
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, underlying client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if deny {
					return apierrors.NewForbidden(
						schema.GroupResource{Group: "shipstream.io", Resource: "mysqlfailovergroups"},
						fg.Name, errors.New("mysqlfailovergroups/status update is forbidden"))
				}
				return underlying.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", State: state.StateWritable},
			{Name: "dc2", State: state.StateReadOnly},
		},
		ActiveSite:         "dc1",
		LastFailoverTarget: "dc1",
	}

	// Denied write → non-nil error (arms the retry). Status must NOT persist.
	if err := runner.updateCRStatus(context.Background(), nn, snap); err == nil {
		t.Fatal("updateCRStatus must return an error when the /status write is denied")
	}
	var denied v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &denied); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if denied.Status.ActiveSite == "dc1" {
		t.Fatal("status must not persist while the write is denied")
	}

	// Permissions return → nil, and the desired status now persists.
	deny = false
	if err := runner.updateCRStatus(context.Background(), nn, snap); err != nil {
		t.Fatalf("updateCRStatus must succeed once the write is permitted, got %v", err)
	}
	var healed v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &healed); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if healed.Status.ActiveSite != "dc1" {
		t.Errorf("status did not heal: ActiveSite=%q want dc1", healed.Status.ActiveSite)
	}
	if healed.Status.LastFailoverTarget != "dc1" {
		t.Errorf("lastFailoverTarget did not heal: got %q want dc1", healed.Status.LastFailoverTarget)
	}
}

// TestPoll_StatusRetryReFiresCallbackUntilCleared proves the Poll gate:
// an armed status-write retry re-fires StatusCallback on a poll with no fresh
// state transition, and a cleared retry stops the extra firing (scenario 32).
func TestPoll_StatusRetryReFiresCallbackUntilCleared(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	var calls int
	tm.StatusCallback = func(snap TopologySnapshot) { calls++ }

	// Drain to a steady state where an extra poll fires no callback.
	drainSteady := func() {
		for i := 0; i < 40; i++ {
			before := calls
			pollN(tm, 1)
			if calls == before {
				return
			}
		}
		t.Fatalf("topology never reached a steady no-callback state (calls=%d)", calls)
	}
	drainSteady()

	// Baseline: a steady poll fires nothing.
	before := calls
	pollN(tm, 1)
	if calls != before {
		t.Fatalf("steady poll fired %d callbacks; want 0", calls-before)
	}

	// Arm the status-write retry (a prior /status write was denied).
	tm.MarkStatusWriteResult(errors.New("mysqlfailovergroups/status is forbidden"))
	before = calls
	pollN(tm, 1)
	if calls == before {
		t.Fatal("armed status-write retry must re-fire StatusCallback on a no-transition poll")
	}

	// Disarm (write succeeded): steady polls fire nothing again.
	tm.MarkStatusWriteResult(nil)
	drainSteady()
	before = calls
	pollN(tm, 1)
	if calls != before {
		t.Fatalf("after retry cleared, steady poll fired %d callbacks; want 0", calls-before)
	}
}

func TestPoll_StatusRetryPreservesRejectedSnapshot(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	for i := 0; i < 40; i++ {
		pollN(tm, 1)
	}
	want := TopologySnapshot{ActiveSite: "dc1", Alert: "full rejected alert", DegradedReason: "Degraded"}
	tm.mu.Lock()
	tm.statusRetrySnapshot = &want
	tm.statusWriteFailed = true
	tm.mu.Unlock()
	var got TopologySnapshot
	tm.StatusCallback = func(snapshot TopologySnapshot) { got = snapshot }
	pollN(tm, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retry snapshot = %#v, want exact rejected snapshot %#v", got, want)
	}
}

// fakeDNSEndpoint is a DNSUpdater that also implements
// platform.DNSRecordReader — the shape of the real DNSEndpoint updater, which
// can read its record back from the API server. `record` is what external-dns
// would see; denied makes the apply fail the way an RBAC-forbidden write does;
// readErr makes the record unreadable (get denied too).
type fakeDNSEndpoint struct {
	mu      sync.Mutex
	record  string
	writes  int
	denied  error
	readErr error
}

var _ platform.DNSRecordReader = (*fakeDNSEndpoint)(nil)

func (f *fakeDNSEndpoint) UpdateDNSRecord(_ context.Context, ip string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.denied != nil {
		return f.denied
	}
	f.record = ip
	f.writes++
	return nil
}

func (f *fakeDNSEndpoint) CurrentDNSRecord(_ context.Context) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return "", false, f.readErr
	}
	if f.record == "" {
		return "", false, nil
	}
	return f.record, true, nil
}

func (f *fakeDNSEndpoint) setDenied(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.denied = err
}

func (f *fakeDNSEndpoint) setRecord(ip string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record = ip
}

func (f *fakeDNSEndpoint) current() (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.record, f.writes
}

// fakeDNSSpec is a DNSUpdater that implements DNSSpecController so
// reconcileDNS can see hostname/TTL divergence independently of the
// target IP. Used to pin issue #166: a rename must rewrite the record
// without incrementing bloodraven_dns_flips_total.
type fakeDNSSpec struct {
	mu         sync.Mutex
	hostname   string
	ttl        int64
	generation int64
	recordHost string
	recordTTL  int64
	record     string
	writes     int
	denied     error
	readErr    error
}

var (
	_ platform.DNSUpdater        = (*fakeDNSSpec)(nil)
	_ platform.DNSSpecController = (*fakeDNSSpec)(nil)
)

func (f *fakeDNSSpec) SetRecordSpec(hostname string, ttl int64, generation int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if generation < f.generation {
		return
	}
	f.generation = generation
	f.hostname = hostname
	f.ttl = ttl
}

func (f *fakeDNSSpec) RecordSpec() (string, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hostname, f.ttl
}

func (f *fakeDNSSpec) SpecNeedsApply() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recordHost != f.hostname || f.recordTTL != f.ttl
}

func (f *fakeDNSSpec) CurrentDNSEndpoint(_ context.Context) (string, string, int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return "", "", 0, false, f.readErr
	}
	if f.record == "" && f.recordHost == "" {
		return "", "", 0, false, nil
	}
	return f.recordHost, f.record, f.recordTTL, true, nil
}

func (f *fakeDNSSpec) UpdateDNSRecord(_ context.Context, ip string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.denied != nil {
		return f.denied
	}
	f.record = ip
	f.recordHost = f.hostname
	f.recordTTL = f.ttl
	f.writes++
	return nil
}

func (f *fakeDNSSpec) snapshot() (host, ip string, ttl int64, writes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recordHost, f.record, f.recordTTL, f.writes
}

func (f *fakeDNSSpec) setReadErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readErr = err
}

// newDNSHealTM builds a TopologyManager over the two-site test topology
// (dc1=1.1.1.1, dc2=2.2.2.2) with a caller-supplied DNS updater.
func newDNSHealTM(dns platform.DNSUpdater, site0, site1 *mockMySQL) *TopologyManager {
	tm := NewTopologyManager(testTopologyConfig(), []mysql.Checker{site0, site1},
		NewFailoverController(testLogger()), nil, nil, BootstrapConfig{},
		newMockTainter(), platform.NewHub(testLogger()), dns, testLogger())
	tm.failoverCooldown = 0
	return tm
}

// dnsFlips reads bloodraven_dns_flips_total{site}. The counter is a package
// global shared across tests, so assertions must be on deltas.
func dnsFlips(site string) float64 {
	return testutil.ToFloat64(metrics.DNSFlipCount.WithLabelValues(site))
}

// TestReconcileDNS_HealsStaleRecordAfterOperatorRestart is the restart-safety
// property: dc2 is the primary (a failover happened and its DNS flip was
// denied), but the live record still points at the old primary. A FRESH
// TopologyManager — the operator restarted, so nothing about the failed flip
// survives in memory — must still repair DNS, because the desired target is
// re-derived from live topology and the live record, not from a memoized
// retry. Exactly one write; the converged record is not re-written afterwards.
func TestReconcileDNS_HealsStaleRecordAfterOperatorRestart(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}  // old primary, demoted
	site1 := &mockMySQL{readOnly: false} // promoted primary
	dns := &fakeDNSEndpoint{record: "1.1.1.1"}
	tm := newDNSHealTM(dns, site0, site1)

	before := dnsFlips("dc2")
	pollN(tm, 2)

	record, writes := dns.current()
	if record != "2.2.2.2" {
		t.Fatalf("DNS record=%q; a restarted operator must repair it to the current primary dc2 (2.2.2.2)", record)
	}
	if writes != 1 {
		t.Fatalf("DNS writes=%d, want exactly 1 (the reconcile must be idempotent)", writes)
	}
	if delta := dnsFlips("dc2") - before; delta != 1 {
		t.Errorf("bloodraven_dns_flips_total{dc2} delta=%g, want 1", delta)
	}

	// Converged: further polls must leave the record alone.
	pollN(tm, 3)
	if _, writes := dns.current(); writes != 1 {
		t.Errorf("converged record re-written: writes=%d, want 1", writes)
	}
	if delta := dnsFlips("dc2") - before; delta != 1 {
		t.Errorf("bloodraven_dns_flips_total{dc2} delta=%g after converged polls, want 1", delta)
	}
}

// TestReconcileDNS_MatchingRecordIsNeverRewritten proves the reconcile does
// not touch a record that already points at the active site — no write, no
// flip metric (the counter must track real flips, not poll cycles).
func TestReconcileDNS_MatchingRecordIsNeverRewritten(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	dns := &fakeDNSEndpoint{record: "1.1.1.1"} // already the active site
	tm := newDNSHealTM(dns, site0, site1)

	before := dnsFlips("dc1")
	pollN(tm, 5)

	record, writes := dns.current()
	if record != "1.1.1.1" || writes != 0 {
		t.Errorf("record=%q writes=%d; a converged record must never be re-applied", record, writes)
	}
	if delta := dnsFlips("dc1") - before; delta != 0 {
		t.Errorf("bloodraven_dns_flips_total{dc1} delta=%g, want 0 (no flip happened)", delta)
	}
}

// TestReconcileDNS_NeverReplaysSupersededTarget is the stale-target property:
// an emergency promotion to dc2 has its DNS flip denied, and before DNS writes
// are permitted again the primary moves back to dc1. The heal must publish the
// CURRENT primary (dc1), never the superseded dc2 target — a retry that
// replayed the memoized target would point DNS at a read-only site. The flip
// metric must likewise only count the site actually applied.
func TestReconcileDNS_NeverReplaysSupersededTarget(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	dns := &fakeDNSEndpoint{record: "1.1.1.1"}
	tm := newDNSHealTM(dns, site0, site1)
	pollN(tm, 2) // steady: dc1 primary

	// DNS writes are denied, then dc1 dies → emergency promotion to dc2 whose
	// DNS flip is rejected.
	dns.setDenied(errors.New("dnsendpoints.externaldns.k8s.io is forbidden"))
	site0.setError(errors.New("connection refused"))
	pollN(tm, 3)

	tm.mu.RLock()
	target := tm.lastFailoverTarget
	tm.mu.RUnlock()
	if target != "dc2" {
		t.Fatalf("lastFailoverTarget=%q want dc2 (MySQL promotion must be durable through a denied DNS flip)", target)
	}
	if record, _ := dns.current(); record != "1.1.1.1" {
		t.Fatalf("denied DNS write must not land, record=%q", record)
	}

	// While DNS is still denied, the primary moves back to dc1: it returns as a
	// replica and is then promoted by a planned switchover, which supersedes
	// the emergency dc2 target (its own DNS flip is denied too).
	site0.setReadOnly(true) // dc1 back online, replicating
	pollN(tm, 2)
	if _, err := tm.PlannedPromote(context.Background(), "dc1", "dc2"); err == nil {
		t.Fatal("planned promote should report the denied DNS flip")
	}
	site1.setReadOnly(true) // dc2 demoted by the switchover
	pollN(tm, 2)

	// Someone has left the record on a target that matches neither site, so the
	// heal has to actively write — and it must write dc1, not the superseded dc2.
	dns.setRecord("9.9.9.9")
	beforeDC1, beforeDC2 := dnsFlips("dc1"), dnsFlips("dc2")
	dns.setDenied(nil)
	pollN(tm, 1)

	record, _ := dns.current()
	if record == "2.2.2.2" {
		t.Fatal("DNS heal replayed the superseded promotion target dc2 — it would point writes at a read-only site")
	}
	if record != "1.1.1.1" {
		t.Fatalf("DNS record=%q, want the current primary dc1 (1.1.1.1)", record)
	}
	if delta := dnsFlips("dc2") - beforeDC2; delta != 0 {
		t.Errorf("bloodraven_dns_flips_total{dc2} delta=%g, want 0 — the metric must only count the target actually applied", delta)
	}
	if delta := dnsFlips("dc1") - beforeDC1; delta != 1 {
		t.Errorf("bloodraven_dns_flips_total{dc1} delta=%g, want 1", delta)
	}
}

// TestReconcileDNS_DeniedPromotionFlipHealsWithoutRefailover is scenario 38:
// the promotion-time DNS flip is RBAC-denied, MySQL promotion stands, and once
// the write is permitted the record catches up to the promoted site on an
// ordinary poll — with no second failover.
func TestReconcileDNS_DeniedPromotionFlipHealsWithoutRefailover(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	dns := &fakeDNSEndpoint{record: "1.1.1.1"}
	tm := newDNSHealTM(dns, site0, site1)
	var callbackCalls int
	tm.EmergencyFailoverCallback = func(context.Context, string, string) { callbackCalls++ }
	pollN(tm, 2)

	beforeFailovers := testutil.ToFloat64(metrics.FailoversTotal.WithLabelValues("dc2"))
	dns.setDenied(errors.New("dnsendpoints.externaldns.k8s.io is forbidden"))
	site0.setError(errors.New("connection refused"))
	pollN(tm, 3)

	site1.mu.Lock()
	promotedRO := site1.readOnly
	site1.mu.Unlock()
	if promotedRO {
		t.Fatal("dc2 must be promoted (writable) despite the denied DNS flip")
	}
	if callbackCalls != 1 {
		t.Fatalf("EmergencyFailoverCallback calls=%d, want 1 despite DNS failure", callbackCalls)
	}
	if record, _ := dns.current(); record != "1.1.1.1" {
		t.Fatalf("DNS must not have advanced while denied, record=%q", record)
	}

	// Permissions return: an ordinary poll (no fresh state transition) heals.
	dns.setDenied(nil)
	pollN(tm, 1)

	record, writes := dns.current()
	if record != "2.2.2.2" {
		t.Errorf("DNS should heal to the promoted site lbIP 2.2.2.2, got %q", record)
	}
	if writes != 1 {
		t.Errorf("DNS writes=%d, want 1", writes)
	}
	tm.mu.RLock()
	target := tm.lastFailoverTarget
	tm.mu.RUnlock()
	if target != "dc2" {
		t.Errorf("lastFailoverTarget=%q — the DNS heal must not re-run promotion", target)
	}
	if delta := testutil.ToFloat64(metrics.FailoversTotal.WithLabelValues("dc2")) - beforeFailovers; delta != 1 {
		t.Errorf("bloodraven_failovers_total{dc2} delta=%g, want exactly 1 (no second failover)", delta)
	}
}

// TestReconcileDNS_HealsWithoutRecordReadCapability covers a DNSUpdater that
// cannot read its record back (no platform.DNSRecordReader). The reconcile
// then falls back to this process's own knowledge: a write that was rejected
// is retried on the next poll against the CURRENT primary — and a converged
// cluster is still never written speculatively.
func TestReconcileDNS_HealsWithoutRecordReadCapability(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1) // mockDNS: write-only
	pollN(tm, 2)

	// Nothing is known to be stale, so a healthy cluster is never written.
	if got := dns.getLastIP(); got != "" {
		t.Fatalf("write-only updater must not be written speculatively, got %q", got)
	}

	dns.mu.Lock()
	dns.err = errors.New("dnsendpoints.externaldns.k8s.io is forbidden")
	dns.mu.Unlock()
	site0.setError(errors.New("connection refused"))
	pollN(tm, 3)
	if got := dns.getLastIP(); got != "" {
		t.Fatalf("DNS must not have applied while denied, got %q", got)
	}

	dns.mu.Lock()
	dns.err = nil
	dns.mu.Unlock()
	pollN(tm, 1)

	if got := dns.getLastIP(); got != "2.2.2.2" {
		t.Errorf("denied flip should heal to the promoted site lbIP 2.2.2.2, got %q", got)
	}
	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()
	if calls != 1 {
		t.Errorf("DNS applied %d times, want 1 (idempotent once converged)", calls)
	}
}

// TestReconcileDNS_HostnameChangeRewritesWithoutFlipMetric is issue #166:
// changing spec.dns.hostname must rewrite dnsName even when the target IP
// still matches the active site, and must not count as a DNS flip.
func TestReconcileDNS_HostnameChangeRewritesWithoutFlipMetric(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	dns := &fakeDNSSpec{
		hostname:   "old.example.com",
		ttl:        60,
		generation: 1,
		recordHost: "old.example.com",
		recordTTL:  60,
		record:     "1.1.1.1",
	}
	tm := newDNSHealTM(dns, site0, site1)

	before := dnsFlips("dc1")
	pollN(tm, 2)
	if _, _, _, writes := dns.snapshot(); writes != 0 {
		t.Fatalf("converged record rewritten before rename: writes=%d", writes)
	}

	tm.SetDNSRecordSpec("new.example.com", 60, 2)
	pollN(tm, 1)

	host, ip, ttl, writes := dns.snapshot()
	if host != "new.example.com" || ip != "1.1.1.1" || ttl != 60 {
		t.Fatalf("after rename: host=%q ip=%q ttl=%d, want (new.example.com, 1.1.1.1, 60)", host, ip, ttl)
	}
	if writes != 1 {
		t.Fatalf("rename writes=%d, want 1", writes)
	}
	if delta := dnsFlips("dc1") - before; delta != 0 {
		t.Errorf("bloodraven_dns_flips_total{dc1} delta=%g, want 0 (hostname-only rewrite is not an IP flip)", delta)
	}

	pollN(tm, 3)
	if _, _, _, writes := dns.snapshot(); writes != 1 {
		t.Errorf("converged renamed record re-written: writes=%d, want 1", writes)
	}
}

func TestReconcileDNS_TTLChangeRewritesWithoutFlipMetric(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	dns := &fakeDNSSpec{
		hostname:   "lion.az.example.com",
		ttl:        60,
		generation: 1,
		recordHost: "lion.az.example.com",
		recordTTL:  60,
		record:     "1.1.1.1",
	}
	tm := newDNSHealTM(dns, site0, site1)
	before := dnsFlips("dc1")
	pollN(tm, 1)

	tm.SetDNSRecordSpec("lion.az.example.com", 15, 2)
	pollN(tm, 1)

	host, ip, ttl, writes := dns.snapshot()
	if host != "lion.az.example.com" || ip != "1.1.1.1" || ttl != 15 {
		t.Fatalf("after TTL edit: host=%q ip=%q ttl=%d", host, ip, ttl)
	}
	if writes != 1 {
		t.Fatalf("TTL rewrite writes=%d, want 1", writes)
	}
	if delta := dnsFlips("dc1") - before; delta != 0 {
		t.Errorf("bloodraven_dns_flips_total{dc1} delta=%g, want 0", delta)
	}
}

func TestReconcileDNS_SpecControllerStillCountsTargetFlip(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: false}
	dns := &fakeDNSSpec{
		hostname:   "lion.az.example.com",
		ttl:        60,
		generation: 1,
		recordHost: "lion.az.example.com",
		recordTTL:  60,
		record:     "1.1.1.1", // stale: still points at dc1
	}
	tm := newDNSHealTM(dns, site0, site1)
	before := dnsFlips("dc2")
	pollN(tm, 2)

	_, ip, _, writes := dns.snapshot()
	if ip != "2.2.2.2" || writes != 1 {
		t.Fatalf("target heal: ip=%q writes=%d, want (2.2.2.2, 1)", ip, writes)
	}
	if delta := dnsFlips("dc2") - before; delta != 1 {
		t.Errorf("bloodraven_dns_flips_total{dc2} delta=%g, want 1", delta)
	}
}

func TestReconcileDNS_SpecChangeAppliesWhenRecordUnreadable(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	dns := &fakeDNSSpec{
		hostname:   "old.example.com",
		ttl:        60,
		generation: 1,
		recordHost: "old.example.com",
		recordTTL:  60,
		record:     "1.1.1.1",
	}
	tm := newDNSHealTM(dns, site0, site1)
	pollN(tm, 2)
	if _, _, _, writes := dns.snapshot(); writes != 0 {
		t.Fatalf("converged record rewritten before rename: writes=%d", writes)
	}

	dns.setReadErr(errors.New("get dnsendpoints is forbidden"))
	tm.SetDNSRecordSpec("new.example.com", 60, 2)
	before := dnsFlips("dc1")
	pollN(tm, 1)

	host, ip, _, writes := dns.snapshot()
	if host != "new.example.com" || ip != "1.1.1.1" || writes != 1 {
		t.Fatalf("unreadable live record must still apply a rename: host=%q ip=%q writes=%d", host, ip, writes)
	}
	if delta := dnsFlips("dc1") - before; delta != 0 {
		t.Errorf("bloodraven_dns_flips_total{dc1} delta=%g, want 0", delta)
	}
}
