package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// Tests of the durable record of a clone recipient's server_uuid (R4 B1):
// a clone that finishes after the manager that issued it is gone is still
// checked, exactly once, and the recipient's Clone keyring hold is kept
// until the clone's restart is confirmed.

// memCloneIdentityStore is a CloneIdentityStore that outlives a manager,
// as the group's metadata does.
type memCloneIdentityStore struct {
	mu      sync.Mutex
	records map[string]CloneIdentity
	// loadErr, recordErr and resolveErr fail the next calls while set.
	loadErr, recordErr, resolveErr error
	// onRecord runs inside Record, before the record is stored.
	onRecord             func()
	recordCalls, resolve int
}

func newMemCloneIdentityStore() *memCloneIdentityStore {
	return &memCloneIdentityStore{records: map[string]CloneIdentity{}}
}

func (s *memCloneIdentityStore) Load(context.Context) (map[string]CloneIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	out := map[string]CloneIdentity{}
	for site, rec := range s.records {
		out[site] = rec
	}
	return out, nil
}

func (s *memCloneIdentityStore) Record(_ context.Context, site string, rec CloneIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordCalls++
	if s.recordErr != nil {
		return s.recordErr
	}
	if s.onRecord != nil {
		s.onRecord()
	}
	s.records[site] = rec
	return nil
}

func (s *memCloneIdentityStore) Resolve(_ context.Context, site string, rec CloneIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolve++
	if s.resolveErr != nil {
		return s.resolveErr
	}
	if cur, ok := s.records[site]; ok && cur.equal(rec) {
		delete(s.records, site)
	}
	return nil
}

func (s *memCloneIdentityStore) get(site string) (CloneIdentity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[site]
	return rec, ok
}

func (s *memCloneIdentityStore) set(mutate func(s *memCloneIdentityStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutate(s)
}

func cloneBegin(s string) *string { return &s }

// stoppingReplica is a recipient whose manager stops while it waits for
// the restart that follows CLONE INSTANCE: the first probe after the clone
// cancels the manager's context and fails.
type stoppingReplica struct {
	*identityMockMySQL
	stop func()
}

func (r *stoppingReplica) CheckReadOnly(ctx context.Context) (bool, error) {
	r.mockMySQL.mu.Lock()
	cloned := r.mockMySQL.cloneCalls > 0
	r.mockMySQL.mu.Unlock()
	if cloned && r.stop != nil {
		r.stop()
		return false, errors.New("connection refused")
	}
	return r.identityMockMySQL.CheckReadOnly(ctx)
}

// cloneStatusReplica is a recipient whose clone_status reads as given:
// an error, or no row at all (a CLONE INSTANCE that was refused leaves
// none behind).
type cloneStatusReplica struct {
	*identityMockMySQL
	err error
}

func (r *cloneStatusReplica) CloneStatus(context.Context) (*mysql.CloneStatus, error) {
	return nil, r.err
}

// withStore wires store into the fixture's manager and makes the manager's
// own view of the recipient (the poll loop's) the identity-reporting mock.
func (f *cloneIdentityFixture) withStore(store CloneIdentityStore) *cloneIdentityFixture {
	f.tm.SetCloneIdentityStore(store)
	f.tm.sites[1].mysql = f.replica
	return f
}

// successor is a new manager on the same servers and the same store: the
// operator restarted.
func (f *cloneIdentityFixture) successor(store CloneIdentityStore) *cloneIdentityFixture {
	next := &cloneIdentityFixture{logs: &bytes.Buffer{}, primary: f.primary, replica: f.replica}
	next.tm, _, _ = newTestTopologyManagerWithBootstrap(next.primary, next.replica.mockMySQL)
	next.tm.logger = slog.New(slog.NewJSONHandler(next.logs, nil))
	next.tm.CloneServerUUIDNotRotatedCallback = func(site, serverUUID string) {
		next.reported = append(next.reported, site+" "+serverUUID)
	}
	return next.withStore(store)
}

func (f *cloneIdentityFixture) cloneCalls() int {
	f.replica.mockMySQL.mu.Lock()
	defer f.replica.mockMySQL.mu.Unlock()
	return f.replica.mockMySQL.cloneCalls
}

// interruptedClone runs a clone whose manager stops during the wait for the
// recipient's restart, and returns the fixture and the store the manager
// left behind. The recipient reports before, then after, as server_uuid.
func interruptedClone(t *testing.T, before, after string) (*cloneIdentityFixture, *memCloneIdentityStore) {
	t.Helper()
	store := newMemCloneIdentityStore()
	f := newCloneIdentityFixture(before, after).withStore(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := f.tm.runBootstrap(ctx, f.primary, &stoppingReplica{identityMockMySQL: f.replica, stop: cancel}, "mysql-dc1", "dc2", false)
	if err == nil || !strings.Contains(err.Error(), "wait for replica restart") {
		t.Fatalf("runBootstrap = %v, want the manager to stop while waiting for the restart", err)
	}
	if f.cloneCalls() != 1 {
		t.Fatalf("clone calls = %d, want 1", f.cloneCalls())
	}
	rec, ok := store.get("dc2")
	if !ok || rec.ServerUUID != before || rec.PreviousCloneBegin == nil || *rec.PreviousCloneBegin != "" {
		t.Fatalf("record left behind = %+v (%v), want the server_uuid before the clone and the clone_status row before it", rec, ok)
	}
	if len(f.reported)+len(f.records(t, msgCloneUUIDKept))+len(f.records(t, msgCloneUUIDRotated))+len(f.records(t, msgCloneUUIDNotVerified)) != 0 {
		t.Fatalf("a verdict was given before the clone's restart:\n%s", f.logs)
	}
	return f, store
}

// The record is on the group before CLONE INSTANCE is issued, and gone
// once the manager that ran the clone has checked it.
func TestCloneIdentity_RecordedBeforeTheCloneAndRemovedAfterTheCheck(t *testing.T) {
	store := newMemCloneIdentityStore()
	f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID1).withStore(store)
	clonesAtRecord := -1
	store.onRecord = func() { clonesAtRecord = f.cloneCalls() }
	if err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", false); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	if clonesAtRecord != 0 {
		t.Fatalf("clone calls when the record was written = %d, want 0 (recorded before CLONE INSTANCE)", clonesAtRecord)
	}
	if rec, ok := store.get("dc2"); ok {
		t.Fatalf("record kept after the check: %+v", rec)
	}
	if _, pending := f.tm.pendingCloneIdentity("dc2"); pending {
		t.Fatal("record still pending in the manager after the check")
	}
	if recs := f.records(t, msgCloneUUIDRotated); len(recs) != 1 || len(f.reported) != 0 {
		t.Fatalf("rotated records = %v, reported = %v", recs, f.reported)
	}
}

// A record that cannot be made durable stops the clone before it starts:
// a clone that outlived this manager would never be checked.
func TestCloneIdentity_NoCloneWithoutADurableRecord(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(s *memCloneIdentityStore)
	}{
		{name: "the record cannot be written", fail: func(s *memCloneIdentityStore) { s.recordErr = errors.New("api unavailable") }},
		{name: "the stored records cannot be read", fail: func(s *memCloneIdentityStore) { s.loadErr = errors.New("api unavailable") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemCloneIdentityStore()
			tc.fail(store)
			f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID1).withStore(store)
			err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", false)
			if err == nil || !strings.Contains(err.Error(), "clone not started") || !strings.Contains(err.Error(), "api unavailable") {
				t.Fatalf("runBootstrap = %v, want the clone refused for the missing record", err)
			}
			if f.cloneCalls() != 0 {
				t.Fatalf("CLONE INSTANCE ran %d times without a durable record", f.cloneCalls())
			}
		})
	}
}

// The case of R4 B1. The manager stops between CLONE INSTANCE and the
// restart; the recipient comes back with the server_uuid it had (the start
// that installed the clone could not rotate it). The next manager finds
// the donor's data in place and skips the clone, and that path now makes
// the comparison: one WARN, one report, the record removed, no second
// clone, and nothing more on a later bootstrap.
func TestCloneIdentity_SkippedCloneIsCheckedAfterAManagerRestart(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID0)
	next := first.successor(store)
	if err := next.tm.runBootstrap(context.Background(), next.primary, next.replica, "mysql-dc1", "dc2", true); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	if next.cloneCalls() != 1 {
		t.Fatalf("clone calls = %d, want the finished clone to be skipped, not repeated", next.cloneCalls())
	}
	if len(next.reported) != 1 || next.reported[0] != "dc2 "+cloneTestUUID0 {
		t.Fatalf("reported = %v, want exactly [dc2 %s]", next.reported, cloneTestUUID0)
	}
	if recs := next.records(t, msgCloneUUIDKept); len(recs) != 1 || recs[0]["level"] != "WARN" || recs[0]["site"] != "dc2" || recs[0]["serverUuid"] != cloneTestUUID0 {
		t.Fatalf("records %q = %v, want one WARN", msgCloneUUIDKept, recs)
	}
	if !next.replicationConfigured() {
		t.Fatal("replication was not configured")
	}
	if rec, ok := store.get("dc2"); ok {
		t.Fatalf("record kept after its verdict: %+v", rec)
	}

	// Exactly once: another bootstrap, another poll look and another
	// manager find nothing to report.
	if err := next.tm.runBootstrap(context.Background(), next.primary, next.replica, "mysql-dc1", "dc2", true); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	next.tm.settlePendingCloneIdentities(context.Background())
	third := next.successor(store)
	third.tm.settlePendingCloneIdentities(context.Background())
	if err := third.tm.runBootstrap(context.Background(), third.primary, third.replica, "mysql-dc1", "dc2", true); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	if len(next.reported) != 1 || len(third.reported) != 0 || len(third.records(t, msgCloneUUIDKept)) != 0 {
		t.Fatalf("reported again: %v, %v", next.reported, third.reported)
	}
}

// The same clone, rotated: the skip path logs the rotation and reports
// nothing.
func TestCloneIdentity_SkippedCloneThatRotatedIsNotReported(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID1)
	next := first.successor(store)
	if err := next.tm.runBootstrap(context.Background(), next.primary, next.replica, "mysql-dc1", "dc2", true); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	recs := next.records(t, msgCloneUUIDRotated)
	if len(next.reported) != 0 || len(recs) != 1 || recs[0]["previousServerUuid"] != cloneTestUUID0 || recs[0]["serverUuid"] != cloneTestUUID1 {
		t.Fatalf("reported = %v, rotated records = %v", next.reported, recs)
	}
	if _, ok := store.get("dc2"); ok {
		t.Fatal("record kept after its verdict")
	}
}

// A site whose replication something other than a bootstrap set up is
// never bootstrapped again; the poll loop gives its clone the verdict.
func TestCloneIdentity_PollLoopChecksACloneNoBootstrapSettles(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID0)
	next := first.successor(store)
	next.tm.autoBootstrapSuppressed = true
	pollN(next.tm, 1)
	if next.cloneCalls() != 1 {
		t.Fatalf("premise: the poll should not have cloned again (clone calls = %d)", next.cloneCalls())
	}
	if len(next.reported) != 1 || next.reported[0] != "dc2 "+cloneTestUUID0 {
		t.Fatalf("reported = %v, want exactly [dc2 %s]", next.reported, cloneTestUUID0)
	}
	if _, ok := store.get("dc2"); ok {
		t.Fatal("record kept after its verdict")
	}
	next.tm.cloneIdentity.mu.Lock()
	next.tm.cloneIdentity.nextLook = time.Time{}
	next.tm.cloneIdentity.mu.Unlock()
	pollN(next.tm, 2)
	if len(next.reported) != 1 {
		t.Fatalf("reported again: %v", next.reported)
	}
}

// The poll loop's looks at a record that waits are rate-limited.
func TestCloneIdentity_PollLoopLooksAreRateLimited(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID0)
	m := first.replica.mockMySQL
	m.mu.Lock()
	m.cloneStatusVal = &mysql.CloneStatus{State: "In Progress", BeginTime: "clone-1"}
	m.mu.Unlock()
	next := first.successor(store)
	ctx := context.Background()
	next.tm.settlePendingCloneIdentities(ctx)
	// The clone finishes; a look right away is not due yet.
	m.mu.Lock()
	m.cloneStatusVal = nil
	m.mu.Unlock()
	next.tm.settlePendingCloneIdentities(ctx)
	if len(next.reported) != 0 {
		t.Fatalf("looked again within %s: %v", cloneIdentityRetryInterval, next.reported)
	}
	next.tm.cloneIdentity.mu.Lock()
	next.tm.cloneIdentity.nextLook = next.tm.clock.Now()
	next.tm.cloneIdentity.mu.Unlock()
	next.tm.settlePendingCloneIdentities(ctx)
	if len(next.reported) != 1 {
		t.Fatalf("reported = %v, want the verdict once the next look is due", next.reported)
	}
}

// What a later look makes of a record, from the recipient's clone_status.
func TestCloneIdentity_SettleDecidesFromCloneStatus(t *testing.T) {
	const recorded = "2026-11-10 10:00:00.000"
	for _, tc := range []struct {
		name string
		// rec is the stored record; status the recipient's clone_status row
		// (nil: none); uuid its server_uuid now.
		rec       CloneIdentity
		status    *mysql.CloneStatus
		statusErr error
		uuid      string
		uuidErr   error
		// wantPending: the record waits for another look.
		wantPending   bool
		wantRestarted bool
		wantReported  bool
		wantMsg       string
		wantReason    string
	}{
		{
			name:   "completed clone, server_uuid kept",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID0,
			wantReported: true, wantMsg: msgCloneUUIDKept,
		},
		{
			name:   "completed clone, server_uuid rotated",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID1,
			wantMsg: msgCloneUUIDRotated,
		},
		{
			name:   "first clone of the recipient, server_uuid kept",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin("")},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID0,
			wantReported: true, wantMsg: msgCloneUUIDKept,
		},
		{
			// CLONE was refused, or the manager stopped before issuing it:
			// the recorded row is still the newest. No clone, no verdict.
			name:   "the clone did not begin",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: recorded}, uuid: cloneTestUUID0,
			wantMsg: msgCloneUUIDNotVerified, wantReason: "did not begin",
		},
		{
			name: "the clone did not begin, no clone_status row", uuid: cloneTestUUID0,
			rec:     CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin("")},
			wantMsg: msgCloneUUIDNotVerified, wantReason: "did not begin",
		},
		{
			// A failed clone keeps its server_uuid by design.
			name:   "the clone failed",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			status: &mysql.CloneStatus{State: mysql.CloneStateFailed, ErrorNo: 1317, BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID0,
			wantMsg: msgCloneUUIDNotVerified, wantReason: "failed",
		},
		{
			name:   "the clone is still running",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			status: &mysql.CloneStatus{State: "In Progress", BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID0,
			wantPending: true,
		},
		{
			name:      "the recipient does not answer",
			rec:       CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			statusErr: errors.New("connection refused"), uuid: cloneTestUUID0,
			wantPending: true,
		},
		{
			// The clone is installed (the restart happened); only the
			// identity read is missing. No verdict yet, and no guess.
			name:   "completed clone, server_uuid unreadable",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin(recorded)},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "2026-11-10 11:00:00.000"}, uuidErr: errors.New("connection refused"),
			wantPending: true, wantRestarted: true, wantMsg: msgCloneUUIDNotVerified, wantReason: "after the clone",
		},
		{
			// The row before the clone was unreadable: a Completed row may
			// be an earlier clone's, so an unchanged server_uuid is not
			// reported as kept.
			name:   "clone_status before the clone unknown, server_uuid unchanged",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID0,
			wantMsg: msgCloneUUIDNotVerified, wantReason: "cannot be told from an earlier one",
		},
		{
			name:   "clone_status before the clone unknown, server_uuid changed",
			rec:    CloneIdentity{ServerUUID: cloneTestUUID0},
			status: &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "2026-11-10 11:00:00.000"}, uuid: cloneTestUUID1,
			wantMsg: msgCloneUUIDRotated,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemCloneIdentityStore()
			store.records["dc2"] = tc.rec
			f := newCloneIdentityFixture(tc.uuid, tc.uuid).withStore(store)
			f.replica.beforeErr, f.replica.afterErr = tc.uuidErr, tc.uuidErr
			m := f.replica.mockMySQL
			m.mu.Lock()
			if tc.status != nil {
				m.cloneCalls, m.cloneStatusVal = 1, tc.status
			}
			m.mu.Unlock()
			var recipient mysql.Checker = f.replica
			if tc.statusErr != nil {
				recipient = &cloneStatusReplica{identityMockMySQL: f.replica, err: tc.statusErr}
			}

			f.tm.settleCloneIdentity(context.Background(), recipient, "dc2")

			p, pending := f.tm.pendingCloneIdentity("dc2")
			_, stored := store.get("dc2")
			if pending != tc.wantPending || stored != tc.wantPending {
				t.Fatalf("pending = %v, stored = %v, want %v", pending, stored, tc.wantPending)
			}
			if pending && (p.decided || p.restarted != tc.wantRestarted) {
				t.Fatalf("pending record = %+v, want restarted = %v and no outcome", p, tc.wantRestarted)
			}
			if got := len(f.reported) == 1; got != tc.wantReported || len(f.reported) > 1 {
				t.Fatalf("reported = %v, want reported = %v", f.reported, tc.wantReported)
			}
			total := 0
			for _, msg := range []string{msgCloneUUIDKept, msgCloneUUIDRotated, msgCloneUUIDNotVerified} {
				recs := f.records(t, msg)
				total += len(recs)
				if msg != tc.wantMsg {
					if len(recs) != 0 {
						t.Fatalf("unexpected %q: %v", msg, recs)
					}
					continue
				}
				if len(recs) != 1 || recs[0]["site"] != "dc2" {
					t.Fatalf("records %q = %v, want one for dc2", msg, recs)
				}
				if reason, _ := recs[0]["reason"].(string); !strings.Contains(reason, tc.wantReason) {
					t.Fatalf("reason = %q, want it to contain %q", reason, tc.wantReason)
				}
			}
			if tc.wantMsg == "" && total != 0 {
				t.Fatalf("a record that waits was logged:\n%s", f.logs)
			}
		})
	}
}

// A record gets one verdict from a manager even when its removal from the
// group fails: the removal is retried, the report is not repeated.
func TestCloneIdentity_FailedRemovalDoesNotRepeatTheReport(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID0)
	next := first.successor(store)
	store.set(func(s *memCloneIdentityStore) { s.resolveErr = errors.New("api unavailable") })
	ctx := context.Background()
	next.tm.settleCloneIdentity(ctx, next.replica, "dc2")
	if len(next.reported) != 1 {
		t.Fatalf("reported = %v, want one report", next.reported)
	}
	if _, ok := store.get("dc2"); !ok {
		t.Fatal("premise: the removal should have failed")
	}
	for range 3 {
		next.tm.settleCloneIdentity(ctx, next.replica, "dc2")
	}
	if err := next.tm.runBootstrap(ctx, next.primary, next.replica, "mysql-dc1", "dc2", true); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	if len(next.reported) != 1 || len(next.records(t, msgCloneUUIDKept)) != 1 {
		t.Fatalf("reported again while the removal failed: %v", next.reported)
	}
	store.set(func(s *memCloneIdentityStore) { s.resolveErr = nil })
	next.tm.settleCloneIdentity(ctx, next.replica, "dc2")
	if _, ok := store.get("dc2"); ok {
		t.Fatal("record not removed once the group could be written")
	}
	if _, pending := next.tm.pendingCloneIdentity("dc2"); pending || len(next.reported) != 1 {
		t.Fatalf("pending = %v, reported = %v", pending, next.reported)
	}
}

// A clone issued while an earlier one into the same recipient has no
// outcome yet keeps the earlier record: replacing it would record the
// running clone's own row as "the row before the clone", and its
// completion would then read as "the clone did not begin".
func TestCloneIdentity_EarlierRecordIsKeptWhileItsCloneHasNoOutcome(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID0)
	before, _ := store.get("dc2")
	// The first clone is still copying when the next manager tries again.
	m := first.replica.mockMySQL
	m.mu.Lock()
	m.cloneStatusVal = &mysql.CloneStatus{State: "In Progress", BeginTime: "clone-1"}
	m.gtidExecuted = ""
	m.cloneErr = errors.New("clone instance: Error 3634 (HY000): Too many concurrent clone operations")
	m.mu.Unlock()
	next := first.successor(store)
	if err := next.tm.runBootstrap(context.Background(), next.primary, next.replica, "mysql-dc1", "dc2", true); err == nil {
		t.Fatal("runBootstrap: want the refused second clone to fail")
	}
	if rec, ok := store.get("dc2"); !ok || !rec.equal(before) {
		t.Fatalf("record = %+v (%v), want the first clone's record %+v kept", rec, ok, before)
	}
	// The first clone finishes without rotating: it is reported.
	m.mu.Lock()
	m.cloneStatusVal = &mysql.CloneStatus{State: mysql.CloneStateCompleted, BeginTime: "clone-1"}
	m.gtidExecuted = cloneTestHistory
	m.mu.Unlock()
	next.tm.settleCloneIdentity(context.Background(), next.replica, "dc2")
	if len(next.reported) != 1 {
		t.Fatalf("reported = %v, want the first clone reported\n%s", next.reported, next.logs)
	}
}

// --- The Clone keyring hold ----------------------------------------------

// recloneThroughGate requests a reclone of dc2 and waits for the bootstrap
// goroutine to end.
func recloneThroughGate(t *testing.T, ctx context.Context, tm *TopologyManager) {
	t.Helper()
	var consumed atomic.Int32
	tm.RecloneCompleteCallback = func(context.Context, string, RecloneOutcome) error {
		consumed.Add(1)
		return nil
	}
	tm.SetRecloneSite("dc2")
	if !tm.checkReclone(ctx) {
		t.Fatal("reclone did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if consumed.Load() > 0 && !tm.isBootstrapping() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("bootstrap did not end: phase %s", tm.BootstrapPhase())
}

// holdFixture is a manager with a ready keyring gate and a store, whose
// recipient (dc2) reports its server_uuid to the poll loop as well.
func holdFixture(t *testing.T, before, after string) (*cloneIdentityFixture, *memCloneIdentityStore, *testKeyringGate) {
	t.Helper()
	store := newMemCloneIdentityStore()
	f := newCloneIdentityFixture(before, after)
	f.primary.readOnly = false
	f.tm.autoBootstrapSuppressed = true
	pollN(f.tm, 2)
	f.withStore(store)
	gate := &testKeyringGate{ready: true}
	f.tm.SetKeyringGate(gate)
	return f, store, gate
}

// A bootstrap that ends while its clone may still be waiting for the
// restart keeps the recipient's Clone keyring hold: sealing the site now
// would make the start that installs the clone keep the old server_uuid
// (or kill a clone still copying). The hold is released once the clone has
// its outcome.
func TestCloneHold_KeptUntilTheClonesRestartIsConfirmed(t *testing.T) {
	f, store, gate := holdFixture(t, cloneTestUUID0, cloneTestUUID1)
	// The manager stops during the restart wait, and the recipient is not
	// back yet when the bootstrap ends.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tm.sites[1].mysql = &stoppingReplica{identityMockMySQL: f.replica, stop: func() {
		m := f.replica.mockMySQL
		m.mu.Lock()
		m.cloneStatusVal = &mysql.CloneStatus{State: "In Progress", BeginTime: "clone-1"}
		m.mu.Unlock()
		cancel()
	}}
	recloneThroughGate(t, ctx, f.tm)
	if f.tm.BootstrapPhase() != BootstrapPhaseFailed || f.cloneCalls() != 1 {
		t.Fatalf("phase = %s, clone calls = %d, want a clone whose bootstrap failed", f.tm.BootstrapPhase(), f.cloneCalls())
	}
	if n := gate.notifyCalls.Load(); n != 0 {
		t.Fatalf("NotifyCloneComplete calls = %d: the Clone hold was released before the clone's restart was confirmed", n)
	}
	if _, ok := store.get("dc2"); !ok {
		t.Fatal("the record of the unconfirmed clone is gone")
	}
	if recs := f.records(t, "clone restart not confirmed; keeping the recipient's keyring hold"); len(recs) != 1 || recs[0]["recipient"] != "dc2" {
		t.Fatalf("keep-hold records = %v, want one for dc2", recs)
	}

	// Looks while the clone has no outcome release nothing.
	bg := context.Background()
	f.tm.sites[1].mysql = f.replica
	f.tm.settlePendingCloneIdentities(bg)
	f.tm.releaseCloneHold(bg)
	if n := gate.notifyCalls.Load(); n != 0 {
		t.Fatalf("NotifyCloneComplete calls = %d while the clone is still in progress", n)
	}

	// The recipient restarted and installed the clone: verdict, then the
	// release, once.
	m := f.replica.mockMySQL
	m.mu.Lock()
	m.cloneStatusVal = nil
	m.mu.Unlock()
	f.tm.cloneIdentity.mu.Lock()
	f.tm.cloneIdentity.nextLook = time.Time{}
	f.tm.cloneIdentity.mu.Unlock()
	f.tm.settlePendingCloneIdentities(bg)
	f.tm.releaseCloneHold(bg)
	f.tm.releaseCloneHold(bg)
	if n := gate.notifyCalls.Load(); n != 1 {
		t.Fatalf("NotifyCloneComplete calls = %d, want 1 once the clone had its outcome", n)
	}
	if _, ok := store.get("dc2"); ok || len(f.records(t, msgCloneUUIDRotated)) != 1 || len(f.reported) != 0 {
		t.Fatalf("record kept = %v, rotated records = %v, reported = %v", ok, f.records(t, msgCloneUUIDRotated), f.reported)
	}
}

// The next manager releases the hold its predecessor kept, once the clone
// has its outcome, and reports a recipient that kept its server_uuid.
func TestCloneHold_ReleasedByTheNextManager(t *testing.T) {
	first, store := interruptedClone(t, cloneTestUUID0, cloneTestUUID0)
	next := first.successor(store)
	gate := &testKeyringGate{ready: true}
	next.tm.SetKeyringGate(gate)
	bg := context.Background()
	next.tm.settlePendingCloneIdentities(bg)
	next.tm.releaseCloneHold(bg)
	if n := gate.notifyCalls.Load(); n != 1 || len(next.reported) != 1 {
		t.Fatalf("NotifyCloneComplete calls = %d, reported = %v, want one release and one report", n, next.reported)
	}
}

// A bootstrap whose clone never began, or failed, releases the hold when
// it ends, as before: nothing is staged for a restart.
func TestCloneHold_ReleasedWhenNoCloneIsStaged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(f *cloneIdentityFixture)
	}{
		{name: "CLONE refused before it began", break_: func(f *cloneIdentityFixture) {
			f.replica.mockMySQL.cloneErr = errors.New("clone instance: Error 1045 (28000): Access denied")
			f.tm.sites[1].mysql = &cloneStatusReplica{identityMockMySQL: f.replica}
		}},
		{name: "clone failed on the recipient", break_: func(f *cloneIdentityFixture) {
			f.replica.mockMySQL.cloneErr = errors.New("clone instance: invalid connection")
			f.replica.mockMySQL.cloneStatusVal = &mysql.CloneStatus{State: mysql.CloneStateFailed, ErrorNo: 1317, ErrorMessage: "Query execution was interrupted", BeginTime: "t1"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, store, gate := holdFixture(t, cloneTestUUID0, cloneTestUUID0)
			f.replica.mockMySQL.mu.Lock()
			tc.break_(f)
			f.replica.mockMySQL.mu.Unlock()
			recloneThroughGate(t, context.Background(), f.tm)
			if f.tm.BootstrapPhase() != BootstrapPhaseFailed {
				t.Fatalf("phase = %s, want Failed", f.tm.BootstrapPhase())
			}
			if n := gate.notifyCalls.Load(); n != 1 {
				t.Fatalf("NotifyCloneComplete calls = %d, want the hold released at the end of the bootstrap", n)
			}
			if _, ok := store.get("dc2"); ok || len(f.reported) != 0 {
				t.Fatalf("record kept = %v, reported = %v, want the record dropped with no verdict", ok, f.reported)
			}
		})
	}
}

// A successful bootstrap releases the hold when it ends, and leaves no
// record.
func TestCloneHold_ReleasedAfterASuccessfulBootstrap(t *testing.T) {
	f, store, gate := holdFixture(t, cloneTestUUID0, cloneTestUUID1)
	recloneThroughGate(t, context.Background(), f.tm)
	if f.tm.BootstrapPhase() != BootstrapPhaseDone {
		t.Fatalf("phase = %s, want Done", f.tm.BootstrapPhase())
	}
	if n := gate.notifyCalls.Load(); n != 1 {
		t.Fatalf("NotifyCloneComplete calls = %d, want 1", n)
	}
	if _, ok := store.get("dc2"); ok {
		t.Fatal("record kept after a checked clone")
	}
}

// --- The store on the group ------------------------------------------------

func TestGroupCloneIdentityStore(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "lion",
		Annotations: map[string]string{"example.com/keep": "me"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fg).Build()
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	store := (&TopologyManagerRunner{logger: testLogger(), client: c}).cloneIdentityStore(nn)
	annotation := func() (string, bool) {
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		if got.Annotations["example.com/keep"] != "me" {
			t.Fatalf("another annotation was lost: %v", got.Annotations)
		}
		v, ok := got.Annotations[v1alpha1.CloneServerUUIDAnnotation]
		return v, ok
	}

	if recs, err := store.Load(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("Load on a group without records = %v, %v", recs, err)
	}
	dc2 := CloneIdentity{ServerUUID: cloneTestUUID0, PreviousCloneBegin: cloneBegin("2026-11-10 10:00:00.000")}
	dc3 := CloneIdentity{ServerUUID: cloneTestUUID1}
	if err := store.Record(ctx, "dc2", dc2); err != nil {
		t.Fatal(err)
	}
	if err := store.Record(ctx, "dc3", dc3); err != nil {
		t.Fatal(err)
	}
	// The documented value (well_known.go).
	if v, _ := annotation(); v != `{"dc2":{"serverUUID":"`+cloneTestUUID0+`","previousCloneBegin":"2026-11-10 10:00:00.000"},"dc3":{"serverUUID":"`+cloneTestUUID1+`"}}` {
		t.Fatalf("annotation = %s", v)
	}
	// Another manager's store reads them back.
	recs, err := (&TopologyManagerRunner{logger: testLogger(), client: c, apiReader: c}).cloneIdentityStore(nn).Load(ctx)
	if err != nil || len(recs) != 2 || !recs["dc2"].equal(dc2) || !recs["dc3"].equal(dc3) {
		t.Fatalf("Load = %+v, %v", recs, err)
	}

	// Resolve removes only the record it was given: one that was replaced
	// since belongs to a newer clone.
	replaced := CloneIdentity{ServerUUID: cloneTestUUID1, PreviousCloneBegin: cloneBegin("2026-11-10 11:00:00.000")}
	if err := store.Record(ctx, "dc2", replaced); err != nil {
		t.Fatal(err)
	}
	if err := store.Resolve(ctx, "dc2", dc2); err != nil {
		t.Fatal(err)
	}
	if recs, _ = store.Load(ctx); !recs["dc2"].equal(replaced) {
		t.Fatalf("Resolve removed a newer record: %+v", recs)
	}
	if err := store.Resolve(ctx, "dc2", replaced); err != nil {
		t.Fatal(err)
	}
	if err := store.Resolve(ctx, "dc3", dc3); err != nil {
		t.Fatal(err)
	}
	if v, ok := annotation(); ok {
		t.Fatalf("annotation kept after its last record: %s", v)
	}

	// A value edited by hand is read as empty, and replaced by the next
	// record.
	var edited v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &edited); err != nil {
		t.Fatal(err)
	}
	edited.Annotations[v1alpha1.CloneServerUUIDAnnotation] = "{not json"
	if err := c.Update(ctx, &edited); err != nil {
		t.Fatal(err)
	}
	if recs, err := store.Load(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("Load of an undecodable value = %v, %v", recs, err)
	}
	if err := store.Record(ctx, "dc2", dc2); err != nil {
		t.Fatal(err)
	}
	if recs, _ = store.Load(ctx); len(recs) != 1 || !recs["dc2"].equal(dc2) {
		t.Fatalf("Load after re-recording = %+v", recs)
	}

	// A group that is gone: nothing to load, nothing left to remove, and
	// no record can be made (so no clone starts).
	if err := c.Delete(ctx, &edited); err != nil {
		t.Fatal(err)
	}
	if recs, err := store.Load(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("Load of a deleted group = %v, %v", recs, err)
	}
	if err := store.Resolve(ctx, "dc2", dc2); err != nil {
		t.Fatalf("Resolve on a deleted group: %v", err)
	}
	if err := store.Record(ctx, "dc2", dc2); err == nil {
		t.Fatal("Record on a deleted group reported success")
	}
}

// The manager the runner starts records on its group.
func TestStartManager_WiresCloneIdentityStore(t *testing.T) {
	r, c, fg, _ := lifecycleRunner(t)
	if err := r.startManager(context.Background(), fg, mustTopologyConfig(fg), nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	nn := FailoverGroupNamespacedName(fg)
	mt := registeredManager(r, nn)
	if mt == nil {
		t.Fatal("no manager registered")
	}
	mt.tm.cloneIdentity.mu.Lock()
	store := mt.tm.cloneIdentity.store
	mt.tm.cloneIdentity.mu.Unlock()
	if store == nil {
		t.Fatal("the runner's manager has no CloneIdentityStore: a clone that outlives it would never be checked")
	}
	if err := store.Record(context.Background(), "dc2", CloneIdentity{ServerUUID: cloneTestUUID0}); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Annotations[v1alpha1.CloneServerUUIDAnnotation], cloneTestUUID0) {
		t.Fatalf("annotations = %v, want the record on the manager's group", got.Annotations)
	}
}

// --- The probe after the clone (R4 B6) -----------------------------------

// flakyIdentityReplica fails the first failures identity reads after the
// clone and answers from then on.
type flakyIdentityReplica struct {
	*identityMockMySQL
	failures int
	reads    int
}

func (r *flakyIdentityReplica) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	r.mockMySQL.mu.Lock()
	cloned := r.mockMySQL.cloneCalls > 0
	r.mockMySQL.mu.Unlock()
	if cloned {
		r.reads++
		if r.reads <= r.failures {
			return mysql.GTIDSnapshot{}, errors.New("invalid connection")
		}
	}
	return r.identityMockMySQL.ReadGTIDSnapshot(ctx)
}

// The recipient's server_uuid is read up to three times after the clone
// (the number the log schema documents): two dropped pooled connections
// still end in a verdict, three in "not verified".
func TestCloneServerUUID_ThreeProbeAttemptsAfterTheClone(t *testing.T) {
	if cloneServerUUIDProbeAttempts != 3 {
		t.Fatalf("cloneServerUUIDProbeAttempts = %d, want 3 (documented in the log schema)", cloneServerUUIDProbeAttempts)
	}
	for _, tc := range []struct {
		failures    int
		wantVerdict bool
		wantReads   int
	}{
		{failures: 0, wantVerdict: true, wantReads: 1},
		{failures: 2, wantVerdict: true, wantReads: 3},
		{failures: 3, wantVerdict: false, wantReads: 3},
	} {
		f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID0)
		replica := &flakyIdentityReplica{identityMockMySQL: f.replica, failures: tc.failures}
		if err := f.tm.runBootstrap(context.Background(), f.primary, replica, "mysql-dc1", "dc2", false); err != nil {
			t.Fatalf("failures=%d: runBootstrap: %v", tc.failures, err)
		}
		if got := len(f.reported) == 1; got != tc.wantVerdict || replica.reads != tc.wantReads {
			t.Fatalf("failures=%d: reported = %v after %d reads, want verdict = %v after %d reads",
				tc.failures, f.reported, replica.reads, tc.wantVerdict, tc.wantReads)
		}
		if notVerified := len(f.records(t, msgCloneUUIDNotVerified)) == 1; notVerified == tc.wantVerdict {
			t.Fatalf("failures=%d: not-verified logged = %v, want %v", tc.failures, notVerified, !tc.wantVerdict)
		}
	}
}
