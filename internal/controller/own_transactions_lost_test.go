package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// The history of "Found, not fixed" 1 of PR #280. dc2 was the primary
// under lossUUIDOld and issued 1-100; dc1 received all of them, was
// promoted and issued 1-20 of its own. dc2 then came back on a volume
// that holds only 1-90.
const (
	lossUUIDNew = "11111111-0000-11f1-8000-0000000000a1" // dc1, the primary
	lossUUIDOld = "22222222-0000-11f1-8000-0000000000b2" // dc2, the returning site
	lossUUIDRe  = "33333333-0000-11f1-8000-0000000000c3" // dc2 after a reclone

	lossPrimaryGTID = lossUUIDNew + ":1-20," + lossUUIDOld + ":1-100"
	lossReturnGTID  = lossUUIDOld + ":1-90"
	lossLostGTID    = lossUUIDOld + ":91-100"
)

// lossServer is a mockMySQL that reports its server_uuid with its GTID set
// (gtidSnapshotReader, as the production checker does) and whose CLONE
// INSTANCE has a clone's effect: the recipient holds the donor's set
// under a new server_uuid.
type lossServer struct {
	*mockMySQL
	lmu        sync.Mutex
	uuid       string
	donor      *lossServer
	cloneUUID  string
	snapErr    error
	cloneFails error
}

func (s *lossServer) ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error) {
	s.lmu.Lock()
	uuid, err := s.uuid, s.snapErr
	s.lmu.Unlock()
	if err != nil {
		return mysql.GTIDSnapshot{}, err
	}
	s.mockMySQL.mu.Lock()
	defer s.mockMySQL.mu.Unlock()
	return mysql.GTIDSnapshot{ServerUUID: uuid, GtidExecuted: s.mockMySQL.gtidExecuted}, nil
}

func (s *lossServer) CloneInstance(ctx context.Context, user, host, password string, ssl bool, timeout int) error {
	_ = s.mockMySQL.CloneInstance(ctx, user, host, password, ssl, timeout)
	s.lmu.Lock()
	fails := s.cloneFails
	s.lmu.Unlock()
	if fails != nil {
		return fails
	}
	s.donor.mockMySQL.mu.Lock()
	donorSet := s.donor.mockMySQL.gtidExecuted
	s.donor.mockMySQL.mu.Unlock()
	s.mockMySQL.mu.Lock()
	s.mockMySQL.gtidExecuted = donorSet
	s.mockMySQL.replicaStatusVal = nil
	s.mockMySQL.mu.Unlock()
	s.lmu.Lock()
	s.uuid = s.cloneUUID
	s.lmu.Unlock()
	return nil
}

func (s *lossServer) serverUUID() string {
	s.lmu.Lock()
	defer s.lmu.Unlock()
	return s.uuid
}

func (s *lossServer) counts() (changeSource, startReplica, clones int, fenced bool) {
	s.mockMySQL.mu.Lock()
	defer s.mockMySQL.mu.Unlock()
	return s.mockMySQL.changeSourceCalls, s.mockMySQL.startReplicaCalls, s.mockMySQL.cloneCalls, s.mockMySQL.superReadOnly
}

type lossFixture struct {
	tm      *TopologyManager
	clk     *clock.FakeClock
	primary *lossServer
	site    *lossServer
	logs    *bytes.Buffer
}

// newLossFixture is dc1 (the primary) and dc2 (the returning old primary,
// read-only, with no replication channel) on a fake clock. withBootstrap
// gives the manager the clone machinery.
func newLossFixture(t *testing.T, primaryGTID, siteGTID string, withBootstrap bool) *lossFixture {
	t.Helper()
	f := &lossFixture{logs: &bytes.Buffer{}}
	f.primary = &lossServer{mockMySQL: &mockMySQL{readOnly: false, gtidExecuted: primaryGTID}, uuid: lossUUIDNew}
	f.site = &lossServer{mockMySQL: &mockMySQL{readOnly: true, gtidExecuted: siteGTID}, uuid: lossUUIDOld,
		donor: f.primary, cloneUUID: lossUUIDRe}
	f.clk = clock.NewFakeClock(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	cfg := testTopologyConfig()
	var bc *BootstrapController
	if withBootstrap {
		bc = NewBootstrapController(testLogger())
	}
	f.tm = NewTopologyManagerWithClock(cfg, []mysql.Checker{f.primary, f.site}, NewFailoverController(testLogger()), nil, bc,
		BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), f.clk)
	f.tm.failoverCooldown = 0
	f.tm.logger = slog.New(slog.NewJSONHandler(f.logs, nil))
	t.Cleanup(func() {
		metrics.OwnTransactionsLost.WithLabelValues("dc2").Set(0)
		metrics.DivergentTransactions.WithLabelValues("dc2").Set(0)
	})
	return f
}

func (f *lossFixture) loss() *ownLossRecord {
	f.tm.mu.RLock()
	defer f.tm.mu.RUnlock()
	if rec := f.tm.ownLoss["dc2"]; rec != nil {
		cp := *rec
		return &cp
	}
	return nil
}

func (f *lossFixture) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(f.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// waitBootstrapEnd waits for the bootstrap goroutine a poll started.
func (f *lossFixture) waitBootstrapEnd(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		f.tm.mu.RLock()
		idle := bootstrapIdlePhase(f.tm.bootstrapPhase) && f.tm.bootstrapAdmitting == ""
		f.tm.mu.RUnlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("bootstrap did not end: phase %s", f.tm.BootstrapPhase())
}

func ownLostGauge(site string) float64 {
	return promtestutil.ToFloat64(metrics.OwnTransactionsLost.WithLabelValues(site))
}

func TestOwnLostTransactions(t *testing.T) {
	const a, b, c = "aaaaaaaa-0000-11f1-8000-000000000001", "bbbbbbbb-0000-11f1-8000-000000000002", "cccccccc-0000-11f1-8000-000000000003"
	tests := []struct {
		name          string
		primary, site string
		uuids         []string
		want          string
	}{
		{"a lost tail of its own", a + ":1-100," + b + ":1-20", a + ":1-90", []string{a}, a + ":91-100"},
		{"a hole in the middle", a + ":1-100", a + ":1-40:51-100", []string{a}, a + ":41-50"},
		{"nothing lost: only the primary's own era is missing", a + ":1-100," + b + ":1-20", a + ":1-100", []string{a}, ""},
		{"lag under another site's uuid is not a loss", a + ":1-100," + b + ":1-20," + c + ":1-9", a + ":1-100," + b + ":1-5", []string{a}, ""},
		{"lost under an earlier server_uuid", a + ":1-100," + c + ":1-50", a + ":1-100", []string{a, c}, c + ":1-50"},
		{"the earlier server_uuid is not known: not attributed", a + ":1-100," + c + ":1-50", a + ":1-100", []string{a}, ""},
		{"tagged transactions of its own", a + ":1-10," + a + ":blue:1-5", a + ":1-10", []string{a}, a + ":blue:1-5"},
		{"the case of the recorded uuid does not matter", a + ":1-10", a + ":1-5", []string{strings.ToUpper(a)}, a + ":6-10"},
		{"no identity, no verdict", a + ":1-100", a + ":1-90", nil, ""},
		{"a site holding extra transactions lost nothing by that", a + ":1-100", a + ":1-120", []string{a}, ""},
		{"extra and lost at once", a + ":1-100", a + ":1-90:101-120", []string{a}, a + ":91-100"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := mysql.ParseGTIDSet(tt.primary)
			if err != nil {
				t.Fatal(err)
			}
			s, err := mysql.ParseGTIDSet(tt.site)
			if err != nil {
				t.Fatal(err)
			}
			want, err := mysql.ParseGTIDSet(tt.want)
			if err != nil {
				t.Fatal(err)
			}
			if got := ownLostTransactions(p, s, tt.uuids); got.String() != want.String() {
				t.Fatalf("ownLostTransactions = %q, want %q", got.String(), want.String())
			}
		})
	}
}

// The defect: a returning old primary whose set is a subset of the new
// primary's, but which lacks transactions it issued itself, was rejoined by
// replication. It must not be: no CHANGE REPLICATION SOURCE, no START
// REPLICA, the site stays fenced, and the loss is reported.
func TestOwnLoss_ReturningOldPrimaryIsNotRejoined(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	pollN(f.tm, 6)

	changes, starts, clones, fenced := f.site.counts()
	if changes != 0 || starts != 0 {
		t.Fatalf("replication was configured on the site (CHANGE=%d START=%d): its receiver drops the lost transactions", changes, starts)
	}
	if clones != 0 {
		t.Fatalf("a manager without clone machinery cloned (%d)", clones)
	}
	if !fenced {
		t.Fatal("the site is not fenced")
	}
	rec := f.loss()
	if rec == nil || rec.lostGtid != lossLostGTID || rec.lostCount != 10 || rec.comparedTo != "dc1" || rec.detectedBy != divergenceDetectedByRecovery {
		t.Fatalf("own-loss record = %+v, want %s (10) against dc1 by OldPrimaryRecovery", rec, lossLostGTID)
	}
	if got := recoveryOf(f.tm, "dc2").state; got != "" {
		t.Fatalf("recovery state = %q, want none (not RecoveryInProgress: nothing is rejoined)", got)
	}
	st := f.tm.Status()
	if st.Sites[1].RecoveryState != v1alpha1.RecoveryStateRecloneRequired || st.Sites[1].LostGtid != lossLostGTID ||
		st.Sites[1].LostTransactionCount == nil || *st.Sites[1].LostTransactionCount != 10 {
		t.Fatalf("status site = %+v, want RecloneRequired with the lost set", st.Sites[1])
	}
	if got := ownLostGauge("dc2"); got != 10 {
		t.Fatalf("bloodraven_own_transactions_lost{dc2} = %v, want 10", got)
	}
	warns := f.records(t, "own transactions lost")
	if len(warns) != 1 || warns[0]["level"] != "WARN" || warns[0]["site"] != "dc2" || warns[0]["lostGtid"] != lossLostGTID ||
		warns[0]["primary"] != "dc1" || warns[0]["serverUUIDs"] != lossUUIDOld || warns[0]["detectedBy"] != "OldPrimaryRecovery" ||
		!strings.Contains(warns[0]["remedy"].(string), "kubectl bloodraven reclone lion dc2") {
		t.Fatalf("log records = %v, want one WARN naming the site, the set, the primary and the remedy", warns)
	}
	if got := f.loss().recloneWaiting; !strings.Contains(got, "cannot clone") {
		t.Fatalf("recloneWaiting = %q, want the reason no automatic reclone runs", got)
	}
}

// The same site with its whole own history is still rejoined: the check
// must not turn every old primary into a reclone.
func TestOwnLoss_CompleteOldPrimaryIsStillRejoined(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
	pollN(f.tm, 6)

	changes, starts, clones, _ := f.site.counts()
	if changes == 0 || starts == 0 {
		t.Fatalf("a complete old primary was not rejoined (CHANGE=%d START=%d)", changes, starts)
	}
	if clones != 0 {
		t.Fatalf("a complete old primary was cloned (%d)", clones)
	}
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record = %+v, want none", rec)
	}
}

// With the clone machinery the operator reclones the site through the
// reclone path: the site ends as a copy of the primary under a new
// server_uuid with replication configured, and the report is gone.
func TestOwnLoss_ReturningOldPrimaryIsRecloned(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	consumed := 0
	f.tm.RecloneCompleteCallback = func(context.Context, string, RecloneOutcome) error {
		consumed++
		return nil
	}
	conditions, events := statusRunner(t, f.tm)
	for i := 0; i < 10; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
	}

	changes, starts, clones, _ := f.site.counts()
	if clones != 1 {
		t.Fatalf("clones = %d, want exactly one", clones)
	}
	if changes == 0 || starts == 0 {
		t.Fatalf("replication was not configured after the reclone (CHANGE=%d START=%d)", changes, starts)
	}
	if got := f.site.serverUUID(); got != lossUUIDRe {
		t.Fatalf("server_uuid after the reclone = %s, want the new one", got)
	}
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record after the reclone = %+v, want none", rec)
	}
	if got := ownLostGauge("dc2"); got != 0 {
		t.Fatalf("bloodraven_own_transactions_lost{dc2} = %v after the reclone, want 0", got)
	}
	if consumed != 0 {
		t.Fatalf("the reclone annotation callback ran %d times for the operator's own request; it has no annotation to consume", consumed)
	}
	if got := len(f.records(t, "reclone requested for a site that lacks transactions it issued itself")); got != 1 {
		t.Fatalf("reclone-requested log lines = %d, want 1", got)
	}
	if got := len(f.records(t, "own transactions lost cleared: the site was recloned")); got != 1 {
		t.Fatalf("cleared log lines = %d, want 1", got)
	}
	evs := strings.Join(drainEvents(events), "\n")
	if !strings.Contains(evs, "Warning OwnTransactionsLost dc2 lacks 10 transactions it issued itself") ||
		!strings.Contains(evs, "kubectl bloodraven reclone lion dc2") {
		t.Errorf("events = %q, want the OwnTransactionsLost Warning with the remedy", evs)
	}
	if !strings.Contains(evs, "Normal OwnTransactionsLostResolved") {
		t.Errorf("events = %q, want OwnTransactionsLostResolved after the reclone", evs)
	}
	if d := conditions()["RecoveryPending"]; d.Reason == v1alpha1.ReasonOwnTransactionsLost {
		t.Errorf("RecoveryPending = %+v after the reclone, want the report gone", d)
	}
}

// The status a detection writes: recoveryState RecloneRequired with the
// set, RecoveryPending and Degraded with the reason, and the Event.
func TestOwnLoss_StatusConditionsAndEvent(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	conditions, events := statusRunner(t, f.tm)
	pollN(f.tm, 6)

	conds := conditions()
	for _, name := range []string{"RecoveryPending", "Degraded"} {
		c := conds[name]
		if c.Status != metav1.ConditionTrue || c.Reason != "OwnTransactionsLost" || !strings.Contains(c.Message, "dc2 lacks 10 transactions it issued itself") ||
			!strings.Contains(c.Message, "kubectl bloodraven reclone lion dc2") || !strings.Contains(c.Message, "automatic reclone waiting") {
			t.Errorf("%s = %+v, want True/OwnTransactionsLost naming dc2, the count, the remedy and why no reclone runs", name, c)
		}
	}
	evs := drainEvents(events)
	n := 0
	for _, e := range evs {
		if strings.HasPrefix(e, "Warning OwnTransactionsLost ") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("OwnTransactionsLost events = %d (%q), want one: the report is announced once", n, evs)
	}
}

// A site that both holds transactions the primary lacks and lacks some of
// its own is a divergence: an administrator decides, nothing is cloned
// over it. The loss is reported beside the block.
func TestOwnLoss_DivergenceWinsAndIsNotClonedOver(t *testing.T) {
	// 101-105 never reached dc1; 91-100 were lost on dc2.
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-90:101-105", true)
	for i := 0; i < 8; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
	}
	changes, starts, clones, _ := f.site.counts()
	if clones != 0 {
		t.Fatalf("a divergent site was cloned over (%d clones)", clones)
	}
	if changes != 0 || starts != 0 {
		t.Fatalf("replication was configured on a divergent site (CHANGE=%d START=%d)", changes, starts)
	}
	rec := recoveryOf(f.tm, "dc2")
	if rec.state != v1alpha1.RecoveryStateBlocked || rec.divergentGtid != lossUUIDOld+":101-105" {
		t.Fatalf("recovery = %+v, want RecoveryBlocked with the extra transactions", rec)
	}
	loss := f.loss()
	if loss == nil || loss.lostGtid != lossLostGTID {
		t.Fatalf("own-loss record = %+v, want the lost set reported beside the block", loss)
	}
	if !strings.Contains(loss.recloneWaiting, "administrator") {
		t.Fatalf("recloneWaiting = %q, want it to say an administrator decides", loss.recloneWaiting)
	}
	st := f.tm.Status()
	if st.Sites[1].RecoveryState != v1alpha1.RecoveryStateBlocked || st.Sites[1].LostGtid != lossLostGTID {
		t.Fatalf("status site = %+v, want RecoveryBlocked carrying lostGtid", st.Sites[1])
	}
}

// A clone that fails leaves the site out of replication, and the operator
// does not ask again before ownLossRecloneRetryDelay.
func TestOwnLoss_FailedRecloneIsRetriedAfterTheDelayOnly(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	f.site.cloneFails = errors.New("clone instance: Error 3862 (HY000): Clone Donor Error")
	for i := 0; i < 10; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
	}
	changes, starts, clones, _ := f.site.counts()
	if clones != 1 {
		t.Fatalf("clone attempts = %d, want one: no retry at poll frequency", clones)
	}
	if changes != 0 || starts != 0 {
		t.Fatalf("replication was configured after a failed clone (CHANGE=%d START=%d)", changes, starts)
	}
	rec := f.loss()
	if rec == nil || !strings.Contains(rec.recloneWaiting, "did not complete") {
		t.Fatalf("own-loss record = %+v, want it kept with the retry time", rec)
	}

	f.site.lmu.Lock()
	f.site.cloneFails = nil
	f.site.lmu.Unlock()
	f.clk.Advance(ownLossRecloneRetryDelay - time.Second)
	for i := 0; i < 3; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
	}
	if _, _, clones, _ = f.site.counts(); clones != 1 {
		t.Fatalf("clone attempts = %d before the retry delay, want still one", clones)
	}
	f.clk.Advance(2 * time.Second)
	for i := 0; i < 4; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
	}
	if _, _, clones, _ = f.site.counts(); clones != 2 {
		t.Fatalf("clone attempts = %d after the retry delay, want two", clones)
	}
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record after the successful retry = %+v, want none", rec)
	}
}

// What refuses an automatic reclone, and that the site is in none of them
// replicated.
func TestOwnLoss_AutomaticRecloneGates(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(f *lossFixture)
		waiting string
	}{
		{"an in-place restore froze the topology", func(f *lossFixture) { f.tm.topologyFrozen = true }, "in-place restore"},
		{"the site awaits its post-restore reclone", func(f *lossFixture) { f.tm.restoreReclonePending = []string{"dc2"} }, "in-place restore"},
		{"the site's restore queue clones nothing now", func(f *lossFixture) { f.tm.restoreRecloneBlocked = []string{"dc2"} }, "in-place restore"},
		{"another reclone is pending", func(f *lossFixture) { f.tm.reclonePendingSite = "dc9" }, "another reclone is pending"},
		{"no replication credentials", func(f *lossFixture) { f.tm.bootstrapCfg.ReplUser = "" }, "cannot clone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Classify the sites with a whole dc2, then record the loss and
			// the gate by hand: detection itself stands down for a frozen
			// topology or missing credentials.
			f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
			pollN(f.tm, 2)
			f.tm.mu.Lock()
			f.tm.reclonePendingSite, f.tm.reclonePendingOwner = "", ""
			f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1", detectedBy: divergenceDetectedByRecovery}}
			tt.arrange(f)
			f.tm.mu.Unlock()
			f.tm.requestOwnLossReclone()

			f.tm.mu.RLock()
			pending, owner := f.tm.reclonePendingSite, f.tm.reclonePendingOwner
			waiting := f.tm.ownLoss["dc2"].recloneWaiting
			f.tm.mu.RUnlock()
			if pending == "dc2" || owner == recloneOwnerOwnLoss {
				t.Fatalf("a reclone was requested (pending=%q owner=%q)", pending, owner)
			}
			if !strings.Contains(waiting, tt.waiting) {
				t.Fatalf("recloneWaiting = %q, want it to mention %q", waiting, tt.waiting)
			}
			if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); !errors.Is(err, errOwnTransactionsLost) && !errors.Is(err, errRestorePeerHeld) {
				t.Fatalf("replication start gate = %v, want a refusal", err)
			}
		})
	}
}

// Every operator path that starts replication on a site goes through the
// gate, and the gate refuses a site with a report.
func TestOwnLoss_ReplicationStartGateRefuses(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	ran := false
	if err := f.tm.withReplicaStartGate("dc2", func() error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("gate without a report: err=%v ran=%v, want the start admitted", err, ran)
	}
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	ran = false
	err := f.tm.withReplicaStartGate("dc2", func() error { ran = true; return nil })
	if !errors.Is(err, errOwnTransactionsLost) || ran {
		t.Fatalf("gate with a report: err=%v ran=%v, want errOwnTransactionsLost and no start", err, ran)
	}
}

// A report restored from status keeps the site out of promotion at once,
// and the first comparison that finds the site whole clears it.
func TestOwnLoss_RestoredReportHoldsUntilACleanComparison(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", false)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	if !f.tm.ownLossSite("dc2") {
		t.Fatal("the restored report does not hold")
	}
	blocked := false
	for _, obs := range f.tm.observations() {
		if obs.Name == "dc2" && obs.PromotionBlocked && obs.PromotionBlockedReason == state.PromotionBlockedOwnTransactionsLost {
			blocked = true
		}
	}
	if !blocked {
		t.Fatal("the restored report does not block promotion")
	}
	pollN(f.tm, 6)
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record = %+v after a clean comparison, want none", rec)
	}
	if got := len(f.records(t, "own transactions lost cleared: the site holds every transaction it issued")); got != 1 {
		t.Fatalf("cleared log lines = %d, want 1", got)
	}
	changes, starts, _, _ := f.site.counts()
	if changes == 0 || starts == 0 {
		t.Fatalf("the site was not rejoined after the clean comparison (CHANGE=%d START=%d)", changes, starts)
	}
}

// A restored report whose loss still stands is confirmed without a second
// WARN (the Event stream announced it before the restart).
func TestOwnLoss_RestoredReportIsConfirmedQuietly(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	pollN(f.tm, 6)
	rec := f.loss()
	if rec == nil || rec.detectedBy != divergenceDetectedByRecovery || rec.comparedTo != "dc1" {
		t.Fatalf("own-loss record = %+v, want it confirmed by OldPrimaryRecovery against dc1", rec)
	}
	if got := len(f.records(t, "own transactions lost")); got != 0 {
		t.Fatalf("WARN lines = %d, want none for an unchanged restored report", got)
	}
}

// A site whose identity cannot be read is not rejoined on a guess: the
// comparison backs off and nothing is configured.
func TestOwnLoss_UnreadableIdentityDoesNotRejoin(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	f.site.snapErr = errors.New("connection refused")
	pollN(f.tm, 6)
	changes, starts, _, _ := f.site.counts()
	if changes != 0 || starts != 0 {
		t.Fatalf("replication was configured although the site's identity could not be read (CHANGE=%d START=%d)", changes, starts)
	}
}

// Promotion: a site with a report is no candidate for the operator.
func TestOwnLoss_SiteIsNotPromoted(t *testing.T) {
	t.Run("emergency failover", func(t *testing.T) {
		f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
		_, events := statusRunner(t, f.tm)
		pollN(f.tm, 6)
		if f.loss() == nil {
			t.Fatal("no report before the primary died")
		}
		// The primary dies. dc2 is the only other candidate.
		f.primary.mockMySQL.mu.Lock()
		f.primary.mockMySQL.err = errors.New("connection refused")
		f.primary.mockMySQL.mu.Unlock()
		pollN(f.tm, 12)

		f.site.mockMySQL.mu.Lock()
		promoted, readOnly := f.site.mockMySQL.promoted, f.site.mockMySQL.readOnly
		f.site.mockMySQL.mu.Unlock()
		if promoted || !readOnly {
			t.Fatalf("the site was promoted (promoted=%v readOnly=%v): it would issue %s again", promoted, readOnly, lossLostGTID)
		}
		snap := f.tm.buildSnapshot(nil)
		if len(snap.OwnLossPromotionRefused) != 1 || snap.OwnLossPromotionRefused[0] != "dc2" {
			t.Fatalf("OwnLossPromotionRefused = %v, want [dc2]", snap.OwnLossPromotionRefused)
		}
		if !strings.Contains(snap.Alert, "refusing to promote dc2 (lacks transactions it issued itself") {
			t.Fatalf("alert = %q, want the refusal and its reason", snap.Alert)
		}
		evs := strings.Join(drainEvents(events), "\n")
		if !strings.Contains(evs, "Warning PromotionRefusedOwnTransactionsLost") {
			t.Errorf("events = %q, want PromotionRefusedOwnTransactionsLost", evs)
		}
		if strings.Contains(evs, "NoPrimaryDetected") {
			t.Errorf("events = %q: the generic NoPrimaryDetected hides the specific refusal", evs)
		}
	})
	t.Run("ordered-update handoff", func(t *testing.T) {
		f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
		f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
		err := f.tm.liveHandoffRefusal("dc2")()
		if err == nil || !strings.Contains(err.Error(), "lacks transactions it issued itself") {
			t.Fatalf("handoff refusal = %v, want the own-loss refusal", err)
		}
		if err := f.tm.liveHandoffRefusal("dc1")(); err != nil {
			t.Fatalf("handoff refusal of a clean site = %v", err)
		}
	})
	t.Run("primary re-assert", func(t *testing.T) {
		// Both sites read-only, dc2 the recorded target, and dc2 carries a
		// report: the wedge-healing path must not lift it.
		f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
		f.primary.mockMySQL.readOnly = true
		f.primary.mockMySQL.superReadOnly = true
		pollN(f.tm, 3)
		f.tm.mu.Lock()
		f.tm.lastFailoverTarget = "dc2"
		f.tm.mu.Unlock()
		f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
		if f.tm.checkPrimaryReassert(context.Background()) {
			t.Fatal("re-assert was attempted on a site that lacks its own transactions")
		}
		f.site.mockMySQL.mu.Lock()
		readOnly := f.site.mockMySQL.readOnly
		f.site.mockMySQL.mu.Unlock()
		if !readOnly {
			t.Fatal("the site was made writable")
		}
		if got := f.tm.buildSnapshot(nil).OwnLossPromotionRefused; len(got) != 1 || got[0] != "dc2" {
			t.Fatalf("OwnLossPromotionRefused = %v, want [dc2]", got)
		}
	})
}

// A site with a report that an administrator made the primary: the report
// is dropped loudly, and the Event says the GTIDs may be issued again.
func TestOwnLoss_SiteMadePrimaryByHandIsRecorded(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	_, events := statusRunner(t, f.tm)
	pollN(f.tm, 6)
	if f.loss() == nil {
		t.Fatal("no report")
	}
	// The administrator fences dc1 and opens dc2.
	f.primary.mockMySQL.mu.Lock()
	f.primary.mockMySQL.readOnly, f.primary.mockMySQL.superReadOnly = true, true
	f.primary.mockMySQL.mu.Unlock()
	f.site.mockMySQL.mu.Lock()
	f.site.mockMySQL.readOnly, f.site.mockMySQL.superReadOnly = false, false
	f.site.mockMySQL.mu.Unlock()
	pollN(f.tm, 6)

	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record = %+v on the primary, want it dropped", rec)
	}
	errs := f.records(t, "site that lacks transactions it issued itself is now the primary; unless its server_uuid was changed it issues their GTIDs again")
	if len(errs) != 1 || errs[0]["level"] != "ERROR" || errs[0]["site"] != "dc2" {
		t.Fatalf("log records = %v, want one ERROR naming dc2", errs)
	}
	if evs := strings.Join(drainEvents(events), "\n"); !strings.Contains(evs, "Warning OwnTransactionsLostSitePromoted dc2 is the primary") {
		t.Errorf("events = %q, want OwnTransactionsLostSitePromoted", evs)
	}
}

// The replica GTID audit finds a site whose channel already runs (started
// by hand, or by an operator that predates the check). Lag does not look
// like a loss.
func TestOwnLoss_AuditFindsAReplicatingSite(t *testing.T) {
	t.Run("a replicating former primary that lacks its own tail", func(t *testing.T) {
		primary := auditPrimary(lossUUIDNew, lossPrimaryGTID)
		replica := auditReplica(lossUUIDOld, lossUUIDNew+":1-20,"+lossUUIDOld+":1-90")
		tm, _ := twoSiteAudit(t, primary, replica)
		t.Cleanup(func() { metrics.OwnTransactionsLost.WithLabelValues("follower-a").Set(0) })
		pollN(tm, 3)
		tm.mu.RLock()
		rec := tm.ownLoss["follower-a"]
		tm.mu.RUnlock()
		if rec == nil || rec.lostGtid != lossLostGTID || rec.detectedBy != divergenceDetectedByAudit || rec.comparedTo != "primary" {
			t.Fatalf("own-loss record = %+v, want %s by ReplicaAudit against primary", rec, lossLostGTID)
		}
		if st := tm.Status(); st.Sites[1].RecoveryState != v1alpha1.RecoveryStateRecloneRequired {
			t.Fatalf("recoveryState = %q, want RecloneRequired", st.Sites[1].RecoveryState)
		}
		if got := recoveryOf(tm, "follower-a").state; got == v1alpha1.RecoveryStateBlocked {
			t.Fatal("a site that only lacks transactions was reported divergent")
		}
	})
	t.Run("a lagging replica is not reported", func(t *testing.T) {
		primary := auditPrimary(lossUUIDNew, lossPrimaryGTID)
		// Behind on the primary's own transactions only.
		replica := auditReplica(lossUUIDOld, lossUUIDNew+":1-3,"+lossUUIDOld+":1-100")
		tm, _ := twoSiteAudit(t, primary, replica)
		pollN(tm, 3)
		tm.mu.RLock()
		rec := tm.ownLoss["follower-a"]
		tm.mu.RUnlock()
		if rec != nil {
			t.Fatalf("own-loss record = %+v for a lagging replica, want none", rec)
		}
	})
	t.Run("a report is cleared when the site holds its own transactions again", func(t *testing.T) {
		primary := auditPrimary(lossUUIDNew, lossPrimaryGTID)
		replica := auditReplica(lossUUIDRe, lossPrimaryGTID) // recloned by hand
		tm, _ := twoSiteAudit(t, primary, replica)
		tm.SetOwnTransactionsLost("follower-a", lossLostGTID, 10)
		t.Cleanup(func() { metrics.OwnTransactionsLost.WithLabelValues("follower-a").Set(0) })
		pollN(tm, 3)
		if tm.ownLossSite("follower-a") {
			t.Fatal("the report survived a comparison that found nothing lost")
		}
	})
}

// memSiteIdentityStore is an in-memory SiteIdentityStore.
type memSiteIdentityStore struct {
	mu      sync.Mutex
	uuids   map[string][]string
	loadErr error
	addErr  error
	loads   int
	adds    int
}

func (s *memSiteIdentityStore) Load(context.Context) (map[string][]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	out := map[string][]string{}
	for k, v := range s.uuids {
		out[k] = append([]string(nil), v...)
	}
	return out, nil
}

func (s *memSiteIdentityStore) Add(_ context.Context, site, uuid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adds++
	if s.addErr != nil {
		return s.addErr
	}
	if s.uuids == nil {
		s.uuids = map[string][]string{}
	}
	for _, u := range s.uuids[site] {
		if u == uuid {
			return nil
		}
	}
	s.uuids[site] = append(s.uuids[site], uuid)
	return nil
}

func (s *memSiteIdentityStore) get(site string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.uuids[site]...)
}

// The receiver filters on server_id, which a site keeps across reclones.
// A site whose volume came back from before a reclone runs under its
// earlier server_uuid and lacks everything it issued under the later one;
// only the recorded history attributes those transactions to it.
func TestOwnLoss_VolumeFromBeforeARecloneIsFoundThroughTheHistory(t *testing.T) {
	// dc2 ran under lossUUIDOld (1-100), was recloned to lossUUIDRe and
	// issued 1-50 as the primary. dc1 holds everything. dc2 is back on
	// the volume from before the reclone: lossUUIDOld, nothing of
	// lossUUIDRe.
	primaryGTID := lossUUIDNew + ":1-20," + lossUUIDOld + ":1-100," + lossUUIDRe + ":1-50"
	siteGTID := lossUUIDOld + ":1-100"

	t.Run("with the recorded history", func(t *testing.T) {
		f := newLossFixture(t, primaryGTID, siteGTID, false)
		store := &memSiteIdentityStore{uuids: map[string][]string{"dc2": {lossUUIDOld, lossUUIDRe}}}
		f.tm.SetSiteIdentityStore(store)
		pollN(f.tm, 6)
		rec := f.loss()
		if rec == nil || rec.lostGtid != lossUUIDRe+":1-50" {
			t.Fatalf("own-loss record = %+v, want %s:1-50", rec, lossUUIDRe)
		}
		if changes, starts, _, _ := f.site.counts(); changes != 0 || starts != 0 {
			t.Fatalf("replication was configured (CHANGE=%d START=%d)", changes, starts)
		}
	})
	t.Run("without it the current server_uuid alone finds nothing", func(t *testing.T) {
		// The control: this is the hole the history closes.
		f := newLossFixture(t, primaryGTID, siteGTID, false)
		pollN(f.tm, 6)
		if rec := f.loss(); rec != nil {
			t.Fatalf("own-loss record = %+v without a history; the control no longer shows what the history adds", rec)
		}
	})
}

// The history is written when a server_uuid is first seen, survives a
// failed write, and is not written again for a known uuid.
func TestSiteIdentity_RecordedOnceAndRetried(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", false)
	store := &memSiteIdentityStore{addErr: errors.New("apiserver unavailable")}
	f.tm.SetSiteIdentityStore(store)
	ctx := context.Background()

	got := f.tm.noteSiteServerUUID(ctx, "dc2", strings.ToUpper(lossUUIDOld))
	if len(got) != 1 || got[0] != lossUUIDOld {
		t.Fatalf("known uuids = %v, want the normalised uuid although the write failed", got)
	}
	if len(store.get("dc2")) != 0 {
		t.Fatal("the failing store holds a uuid")
	}
	// Rate-limited: no write per call while the store fails.
	adds := store.adds
	f.tm.noteSiteServerUUID(ctx, "dc2", lossUUIDOld)
	if store.adds != adds {
		t.Fatalf("store writes = %d, want %d: a failed write is not retried on every call", store.adds, adds)
	}
	store.mu.Lock()
	store.addErr = nil
	store.mu.Unlock()
	f.clk.Advance(siteIdentityRetryInterval + time.Second)
	f.tm.noteSiteServerUUID(ctx, "dc2", lossUUIDOld)
	if got := store.get("dc2"); len(got) != 1 || got[0] != lossUUIDOld {
		t.Fatalf("stored uuids = %v, want the uuid written after the retry interval", got)
	}
	adds = store.adds
	f.tm.noteSiteServerUUID(ctx, "dc2", lossUUIDOld)
	if store.adds != adds {
		t.Fatalf("a known uuid was written again (%d writes, want %d)", store.adds, adds)
	}
	// A second identity is appended, oldest first.
	if got := f.tm.noteSiteServerUUID(ctx, "dc2", lossUUIDRe); len(got) != 2 || got[0] != lossUUIDOld || got[1] != lossUUIDRe {
		t.Fatalf("known uuids = %v, want [old, new]", got)
	}
	if got := store.get("dc2"); len(got) != 2 {
		t.Fatalf("stored uuids = %v, want both", got)
	}
}

// A history that cannot be loaded does not stop the comparison under the
// current server_uuid, and is loaded later.
func TestSiteIdentity_LoadFailureFallsBackToTheCurrentUUID(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	store := &memSiteIdentityStore{loadErr: errors.New("apiserver unavailable"),
		uuids: map[string][]string{"dc2": {lossUUIDRe}}}
	f.tm.SetSiteIdentityStore(store)
	pollN(f.tm, 6)
	if rec := f.loss(); rec == nil || rec.lostGtid != lossLostGTID {
		t.Fatalf("own-loss record = %+v, want the loss under the current server_uuid", rec)
	}
	store.mu.Lock()
	store.loadErr = nil
	store.mu.Unlock()
	f.clk.Advance(siteIdentityRetryInterval + time.Second)
	got := f.tm.noteSiteServerUUID(context.Background(), "dc2", lossUUIDOld)
	if len(got) != 2 || got[0] != lossUUIDRe || got[1] != lossUUIDOld {
		t.Fatalf("known uuids = %v, want the stored history first, then the one seen", got)
	}
}

func TestGroupSiteIdentityStore(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg).Build()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	store := &groupSiteIdentityStore{client: c, nn: nn, logger: testLogger()}
	site := fg.Spec.Sites[0].Name

	if got, err := store.Load(ctx); err != nil || len(got) != 0 {
		t.Fatalf("Load on a group without the annotation = %v, %v", got, err)
	}
	if err := store.Add(ctx, site, strings.ToUpper(lossUUIDOld)); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, site, lossUUIDOld); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, site, lossUUIDRe); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx)
	if err != nil || len(got[site]) != 2 || got[site][0] != lossUUIDOld || got[site][1] != lossUUIDRe {
		t.Fatalf("Load = %v, %v, want [old, new] once each, lower case", got, err)
	}
	// A site the spec does not have is not recorded, and is dropped.
	if err := store.Add(ctx, "gone", lossUUIDNew); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Load(ctx); len(got["gone"]) != 0 {
		t.Fatalf("a site outside the spec was recorded: %v", got)
	}
	// The limit keeps the newest.
	for i := 0; i < serverUUIDHistoryLimit+3; i++ {
		u := "44444444-0000-11f1-8000-0000000000" + string(rune('a'+i)) + "0"
		if err := store.Add(ctx, site, u); err != nil {
			t.Fatal(err)
		}
	}
	got, _ = store.Load(ctx)
	if len(got[site]) != serverUUIDHistoryLimit {
		t.Fatalf("history length = %d, want the limit %d", len(got[site]), serverUUIDHistoryLimit)
	}
	if last := got[site][len(got[site])-1]; !strings.HasSuffix(last, "k0") {
		t.Fatalf("newest entry = %s, want the last one added", last)
	}
	// An undecodable value reads as empty and is replaced by the next write.
	var cur v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	cur.Annotations[v1alpha1.ServerUUIDHistoryAnnotation] = "{not json"
	if err := c.Update(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx); err != nil || len(got) != 0 {
		t.Fatalf("Load of an undecodable value = %v, %v, want empty", got, err)
	}
	if err := store.Add(ctx, site, lossUUIDOld); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Load(ctx); len(got[site]) != 1 {
		t.Fatalf("Load after the rewrite = %v", got)
	}
	// A group that is gone is no error.
	gone := &groupSiteIdentityStore{client: c, nn: types.NamespacedName{Name: "nope", Namespace: fg.Namespace}}
	if err := gone.Add(ctx, site, lossUUIDOld); err != nil {
		t.Fatalf("Add on a missing group = %v", err)
	}
}

// A skipped clone leaves the recipient's data in place and only sets up
// replication, which a recipient that lacks its own transactions must not
// get.
func TestCanSkipClone_NotForARecipientThatLacksItsOwnTransactions(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	if f.tm.canSkipClone(context.Background(), f.primary, f.site, "dc2") {
		t.Fatal("canSkipClone = true for a recipient that lacks transactions it issued")
	}
	if got := len(f.records(t, "clone not skipped: the recipient lacks transactions it issued itself")); got != 1 {
		t.Fatalf("log lines = %d, want 1", got)
	}
	whole := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
	if !whole.tm.canSkipClone(context.Background(), whole.primary, whole.site, "dc2") {
		t.Fatal("canSkipClone = false for a recipient that is a plain subset")
	}
}

// An administrator's (or a restore queue's) request for the site the
// operator is about to reclone takes the request over, so its annotation
// is consumed; a clone already running is not adopted.
func TestOwnLoss_RecloneRequestIsTakenOverByAnExplicitOne(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	f.tm.mu.Lock()
	f.tm.reclonePendingSite, f.tm.reclonePendingOwner = "dc2", recloneOwnerOwnLoss
	f.tm.mu.Unlock()
	if !f.tm.SetRecloneSiteFor("dc2", "") {
		t.Fatal("an administrator's request for the same site was not accepted")
	}
	f.tm.mu.RLock()
	owner := f.tm.reclonePendingOwner
	f.tm.mu.RUnlock()
	if owner != "" {
		t.Fatalf("owner = %q, want the administrator's", owner)
	}

	f.tm.mu.Lock()
	f.tm.reclonePendingOwner = recloneOwnerOwnLoss
	f.tm.bootstrapRecipient, f.tm.bootstrapPhase = "dc2", BootstrapPhaseCloning
	f.tm.mu.Unlock()
	if f.tm.SetRecloneSiteFor("dc2", "") {
		t.Fatal("a clone already running was adopted by a later request")
	}
}

// Source convergence restarts a follower's stopped channel. A follower
// that restarted on an older volume and lacks its own transactions is
// compared before the start, and not started.
func TestOwnLoss_ConvergenceDoesNotRestartTheChannel(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDNew+":1-5,"+lossUUIDOld+":1-90", false)
	// A channel pointing at the primary, threads stopped
	// (--skip-replica-start after a restart).
	f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	pollN(f.tm, 6)

	changes, starts, _, _ := f.site.counts()
	if starts != 0 || changes != 0 {
		t.Fatalf("the channel was restarted (START=%d CHANGE=%d)", starts, changes)
	}
	rec := f.loss()
	if rec == nil || rec.lostGtid != lossLostGTID {
		t.Fatalf("own-loss record = %+v, want %s", rec, lossLostGTID)
	}
	st := f.tm.Status()
	if st.Sites[1].SourceConvergenceState != sourceConvergenceBlocked || st.Sites[1].SourceConvergenceReason != v1alpha1.SourceReasonOwnTransactionsLost {
		t.Fatalf("source convergence = %s/%s, want Blocked/OwnTransactionsLost", st.Sites[1].SourceConvergenceState, st.Sites[1].SourceConvergenceReason)
	}
}

// The control for the test above: the same follower with its own history
// complete is restarted.
func TestOwnLoss_ConvergenceRestartsACompleteFollower(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDNew+":1-5,"+lossUUIDOld+":1-100", false)
	f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	pollN(f.tm, 6)
	if _, starts, _, _ := f.site.counts(); starts == 0 {
		t.Fatal("a complete follower's channel was not restarted")
	}
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record = %+v, want none", rec)
	}
}
