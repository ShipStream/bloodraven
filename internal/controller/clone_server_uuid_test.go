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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
)

const (
	cloneTestHistory = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-500"
	cloneTestUUID0   = "aaaaaaaa-0000-11f1-8000-000000000001"
	cloneTestUUID1   = "bbbbbbbb-0000-11f1-8000-000000000002"
)

// identityMockMySQL is a mockMySQL that also reports @@server_uuid, as the
// production checker does (gtidSnapshotReader). It reports before until
// CLONE INSTANCE ran and after from then on: a recipient whose launcher
// rotated its identity has two different values, one whose launcher did
// not has the same value twice.
type identityMockMySQL struct {
	*mockMySQL
	before, after string
	// beforeErr and afterErr fail the read before and after the clone.
	beforeErr, afterErr error

	idMu       sync.Mutex
	afterReads int
}

func (m *identityMockMySQL) ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error) {
	m.mockMySQL.mu.Lock()
	cloned := m.mockMySQL.cloneCalls > 0
	gtid := m.mockMySQL.gtidExecuted
	m.mockMySQL.mu.Unlock()
	if !cloned {
		return mysql.GTIDSnapshot{ServerUUID: m.before, GtidExecuted: gtid}, m.beforeErr
	}
	m.idMu.Lock()
	m.afterReads++
	m.idMu.Unlock()
	return mysql.GTIDSnapshot{ServerUUID: m.after, GtidExecuted: gtid}, m.afterErr
}

type cloneIdentityFixture struct {
	tm       *TopologyManager
	primary  *mockMySQL
	replica  *identityMockMySQL
	logs     *bytes.Buffer
	reported []string
}

// newCloneIdentityFixture is a donor with history and a recipient whose
// clone ends the way it does in a container (error 3707, then a Completed
// clone_status row) and reports before, then after, as its server_uuid.
func newCloneIdentityFixture(before, after string) *cloneIdentityFixture {
	f := &cloneIdentityFixture{logs: &bytes.Buffer{}}
	f.primary = &mockMySQL{readOnly: false, gtidExecuted: cloneTestHistory}
	f.replica = &identityMockMySQL{before: before, after: after, mockMySQL: &mockMySQL{readOnly: true, gtidExecuted: cloneTestHistory,
		cloneErr: errors.New("clone instance: Error 3707 (HY000): Restart server failed (mysqld is not managed by supervisor process)."),
	}}
	f.tm, _, _ = newTestTopologyManagerWithBootstrap(f.primary, f.replica.mockMySQL)
	f.tm.logger = slog.New(slog.NewJSONHandler(f.logs, nil))
	f.tm.CloneServerUUIDNotRotatedCallback = func(site, serverUUID string) {
		f.reported = append(f.reported, site+" "+serverUUID)
	}
	return f
}

// records returns the log records with message msg.
func (f *cloneIdentityFixture) records(t *testing.T, msg string) []map[string]any {
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

// replicationConfigured reports whether the bootstrap went on to configure
// replication on the recipient.
func (f *cloneIdentityFixture) replicationConfigured() bool {
	f.replica.mockMySQL.mu.Lock()
	defer f.replica.mockMySQL.mu.Unlock()
	return f.replica.mockMySQL.changeSourceCalls > 0 && f.replica.mockMySQL.startReplicaCalls > 0
}

const (
	msgCloneUUIDRotated     = "clone recipient server_uuid rotated"
	msgCloneUUIDKept        = "clone recipient kept its server_uuid"
	msgCloneUUIDNotVerified = "clone recipient server_uuid rotation not verified"
)

// A recipient whose launcher rotated its identity: nothing is reported, and
// the log records both values (documented in the log schema).
func TestRunBootstrap_CloneRecipientServerUUIDRotated(t *testing.T) {
	f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID1)
	if err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", false); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	if len(f.reported) != 0 {
		t.Fatalf("a rotated recipient was reported: %v", f.reported)
	}
	recs := f.records(t, msgCloneUUIDRotated)
	if len(recs) != 1 || recs[0]["level"] != "INFO" || recs[0]["site"] != "dc2" ||
		recs[0]["previousServerUuid"] != cloneTestUUID0 || recs[0]["serverUuid"] != cloneTestUUID1 {
		t.Fatalf("records %q = %v, want one INFO with site, previousServerUuid and serverUuid", msgCloneUUIDRotated, recs)
	}
	if kept := f.records(t, msgCloneUUIDKept); len(kept) != 0 {
		t.Fatalf("unexpected %q: %v", msgCloneUUIDKept, kept)
	}
	if !f.replicationConfigured() {
		t.Fatal("replication was not configured after the clone")
	}
}

// A recipient that came back with its old server_uuid (no launcher in the
// pod, or one that could not remove auto.cnf) is reported once, and the
// bootstrap still completes: the report must not cost the site its
// replication.
func TestRunBootstrap_CloneRecipientKeptServerUUIDIsReported(t *testing.T) {
	f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID0)
	if err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", false); err != nil {
		t.Fatalf("runBootstrap failed on an unrotated server_uuid; it must only report it: %v", err)
	}
	if len(f.reported) != 1 || f.reported[0] != "dc2 "+cloneTestUUID0 {
		t.Fatalf("reported = %v, want exactly [dc2 %s]", f.reported, cloneTestUUID0)
	}
	recs := f.records(t, msgCloneUUIDKept)
	if len(recs) != 1 || recs[0]["level"] != "WARN" || recs[0]["site"] != "dc2" || recs[0]["serverUuid"] != cloneTestUUID0 {
		t.Fatalf("records %q = %v, want one WARN with site and serverUuid", msgCloneUUIDKept, recs)
	}
	if rotated := f.records(t, msgCloneUUIDRotated); len(rotated) != 0 {
		t.Fatalf("unexpected %q: %v", msgCloneUUIDRotated, rotated)
	}
	if !f.replicationConfigured() {
		t.Fatal("replication was not configured on a recipient that kept its server_uuid")
	}
	// The recipient is fenced the way every bootstrap leaves it, nothing
	// more: no state marks it ineligible for promotion.
	if site := f.tm.getSite("dc2"); site == nil || !site.isPromotable() {
		t.Fatalf("the recipient is no longer a promotion candidate after the report")
	}
}

// Without a nil check the report of an unrotated recipient would panic in
// a manager that has no callback wired (tests, the DST harness).
func TestRunBootstrap_CloneRecipientKeptServerUUIDWithoutCallback(t *testing.T) {
	f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID0)
	f.tm.CloneServerUUIDNotRotatedCallback = nil
	if err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", false); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}
	if recs := f.records(t, msgCloneUUIDKept); len(recs) != 1 {
		t.Fatalf("records %q = %v, want one", msgCloneUUIDKept, recs)
	}
}

// The check never guesses. When either read is missing it says the
// rotation was not verified, reports nothing, and the bootstrap completes.
func TestRunBootstrap_CloneServerUUIDNotVerified(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(f *cloneIdentityFixture)
		replica   func(f *cloneIdentityFixture) mysql.Checker
		wantLevel string
		wantWhy   string
		// wantAfterReads is how often the identity was read after the clone.
		wantAfterReads int
	}{
		{
			name:      "identity unreadable before the clone",
			mutate:    func(f *cloneIdentityFixture) { f.replica.beforeErr = errors.New("connection refused") },
			wantLevel: "INFO", wantWhy: "before the clone is unknown",
		},
		{
			name:      "identity unreadable after the clone",
			mutate:    func(f *cloneIdentityFixture) { f.replica.afterErr = errors.New("connection refused") },
			wantLevel: "WARN", wantWhy: "after the clone: connection refused",
			wantAfterReads: cloneServerUUIDProbeAttempts,
		},
		{
			name:      "empty identity after the clone",
			mutate:    func(f *cloneIdentityFixture) { f.replica.after = "" },
			wantLevel: "WARN", wantWhy: "empty @@server_uuid",
			wantAfterReads: cloneServerUUIDProbeAttempts,
		},
		{
			name:      "a checker that does not read the identity",
			replica:   func(f *cloneIdentityFixture) mysql.Checker { return f.replica.mockMySQL },
			wantLevel: "INFO", wantWhy: "before the clone is unknown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID0)
			if tc.mutate != nil {
				tc.mutate(f)
			}
			var replica mysql.Checker = f.replica
			if tc.replica != nil {
				replica = tc.replica(f)
			}
			if err := f.tm.runBootstrap(context.Background(), f.primary, replica, "mysql-dc1", "dc2", false); err != nil {
				t.Fatalf("runBootstrap: %v", err)
			}
			if len(f.reported) != 0 {
				t.Fatalf("reported %v without both identities", f.reported)
			}
			recs := f.records(t, msgCloneUUIDNotVerified)
			if len(recs) != 1 || recs[0]["level"] != tc.wantLevel || recs[0]["site"] != "dc2" {
				t.Fatalf("records %q = %v, want one %s for dc2", msgCloneUUIDNotVerified, recs, tc.wantLevel)
			}
			if reason, _ := recs[0]["reason"].(string); !strings.Contains(reason, tc.wantWhy) {
				t.Fatalf("reason = %q, want it to contain %q", reason, tc.wantWhy)
			}
			if len(f.records(t, msgCloneUUIDKept))+len(f.records(t, msgCloneUUIDRotated)) != 0 {
				t.Fatalf("a verdict was logged without both identities:\n%s", f.logs)
			}
			f.replica.idMu.Lock()
			reads := f.replica.afterReads
			f.replica.idMu.Unlock()
			if reads != tc.wantAfterReads {
				t.Fatalf("identity reads after the clone = %d, want %d", reads, tc.wantAfterReads)
			}
			if !f.replicationConfigured() {
				t.Fatal("replication was not configured")
			}
		})
	}
}

// A clone that failed, and a bootstrap that skipped the clone, have no
// rotation to check: no verdict, no report.
func TestRunBootstrap_NoServerUUIDVerdictWithoutACompletedClone(t *testing.T) {
	t.Run("failed clone", func(t *testing.T) {
		f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID0)
		f.replica.mockMySQL.cloneErr = errors.New("clone instance: invalid connection")
		f.replica.mockMySQL.cloneStatusVal = &mysql.CloneStatus{State: mysql.CloneStateFailed, ErrorNo: 1317, ErrorMessage: "Query execution was interrupted", BeginTime: "t1"}
		if err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", false); err == nil {
			t.Fatal("runBootstrap reported success for a failed clone")
		}
		if len(f.reported) != 0 || len(f.records(t, msgCloneUUIDKept))+len(f.records(t, msgCloneUUIDRotated))+len(f.records(t, msgCloneUUIDNotVerified)) != 0 {
			t.Fatalf("a failed clone got a server_uuid verdict: reported %v\n%s", f.reported, f.logs)
		}
	})
	t.Run("clone skipped", func(t *testing.T) {
		f := newCloneIdentityFixture(cloneTestUUID0, cloneTestUUID0)
		if err := f.tm.runBootstrap(context.Background(), f.primary, f.replica, "mysql-dc1", "dc2", true); err != nil {
			t.Fatalf("runBootstrap: %v", err)
		}
		f.replica.mockMySQL.mu.Lock()
		clones := f.replica.mockMySQL.cloneCalls
		f.replica.mockMySQL.mu.Unlock()
		if clones != 0 {
			t.Fatalf("premise: the clone should have been skipped, ran %d", clones)
		}
		if len(f.reported) != 0 || len(f.records(t, msgCloneUUIDKept))+len(f.records(t, msgCloneUUIDRotated))+len(f.records(t, msgCloneUUIDNotVerified)) != 0 {
			t.Fatalf("a skipped clone got a server_uuid verdict: reported %v\n%s", f.reported, f.logs)
		}
	})
}

// The runner turns the report into a Warning Event on the group.
func TestEmitCloneServerUUIDNotRotated(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "lion"}}
	rec := record.NewFakeRecorder(4)
	r := &TopologyManagerRunner{
		logger:   testLogger(),
		client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(fg).Build(),
		recorder: rec,
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	r.emitCloneServerUUIDNotRotated(context.Background(), nn, "dc2", cloneTestUUID0)
	select {
	case ev := <-rec.Events:
		for _, want := range []string{"Warning " + v1alpha1.EventCloneServerUUIDNotRotated, "site dc2", cloneTestUUID0,
			mysqlLauncherLogPrefix, "replicates normally", v1alpha1.EventPITRArchiveReissueRiskDetected, "Reclone the site"} {
			if !strings.Contains(ev, want) {
				t.Fatalf("event %q lacks %q", ev, want)
			}
		}
	default:
		t.Fatal("no Event recorded")
	}
	// A group that is gone records nothing and does not panic; neither
	// does a runner without a recorder.
	r.emitCloneServerUUIDNotRotated(context.Background(), types.NamespacedName{Namespace: "default", Name: "gone"}, "dc2", cloneTestUUID0)
	(&TopologyManagerRunner{logger: testLogger()}).emitCloneServerUUIDNotRotated(context.Background(), nn, "dc2", cloneTestUUID0)
	select {
	case ev := <-rec.Events:
		t.Fatalf("an Event was recorded for a missing group: %s", ev)
	default:
	}
}

// The manager the runner starts is wired to it: a recipient that kept its
// server_uuid becomes a Warning Event on the manager's group, without a
// test (or the DST harness) having to set the callback.
func TestStartManager_WiresCloneServerUUIDNotRotated(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	rec := record.NewFakeRecorder(4)
	r.recorder = rec
	if err := r.startManager(context.Background(), fg, mustTopologyConfig(fg), nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, FailoverGroupNamespacedName(fg))
	if mt == nil {
		t.Fatal("no manager registered")
	}
	if mt.tm.CloneServerUUIDNotRotatedCallback == nil {
		t.Fatal("the runner's manager has no CloneServerUUIDNotRotatedCallback: an unrotated clone would only be logged")
	}
	mt.tm.CloneServerUUIDNotRotatedCallback("dc2", cloneTestUUID0)
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Warning "+v1alpha1.EventCloneServerUUIDNotRotated) || !strings.Contains(ev, "site dc2") || !strings.Contains(ev, cloneTestUUID0) {
			t.Fatalf("event = %q", ev)
		}
	default:
		t.Fatal("no Event recorded")
	}
}
