package component

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// Emergency failover with the old primary dead and a delayed applier on the
// candidate: the candidate's receiver reports a connect error while 61
// received transactions are still unapplied. Reproduces the live finding
// where the IO error aborted the drain and RESET REPLICA ALL deleted the
// backlog (61 acknowledged writes later reported as divergent on the old
// primary). The candidate is the real mysql.Checker on a fake driver, so
// SHOW REPLICA STATUS parsing and the drain loop are production code.
func TestFailover_DrainAppliesBacklogDespiteIOErrorFromDeadPrimary(t *testing.T) {
	const (
		retrieved = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-603"
		executed  = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-542"
	)
	candidate := testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IOConnecting: true, SQLRunning: true,
		LastIOErrno: 2003,
		LastIOError: "Error reconnecting to source 'replicator@mysql-dc1:3306' - retry-time: 60 retries: 1 message: Can't connect to MySQL server on 'mysql-dc1:3306' (111)",
		SourceHost:  "mysql-dc1", ReadOnly: true, SuperReadOnly: true,
		RetrievedGtidSet: retrieved, ExecutedGtidSet: executed,
	})
	// The delayed applier catches up on the third poll (~1.5s in).
	candidate.OnPoll(func(poll int, s *testutil.FakeReplicaState) {
		if poll >= 3 {
			s.ExecutedGtidSet = s.RetrievedGtidSet
		}
	})
	checker := candidate.Checker()
	defer checker.Close()

	// The old primary is gone: its fence fails, as it does against a dead
	// host.
	oldPrimary := &mockMySQL{readOnly: false, gtidExecuted: retrieved}
	oldPrimary.failNextSuperRO(1)

	var logs bytes.Buffer
	fc := controller.NewFailoverController(slog.New(slog.NewTextHandler(&logs, nil)))

	promotionGtid, err := fc.Execute(context.Background(), checker, oldPrimary, "dc2")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	st := candidate.State()
	if st.Discarded != "" {
		t.Errorf("RESET REPLICA ALL discarded received transactions %s; the drain must apply them first", st.Discarded)
	}
	if promotionGtid != retrieved {
		t.Errorf("promotion GTID = %q, want %q (every received transaction)", promotionGtid, retrieved)
	}
	if st.ReadOnly || st.SuperReadOnly {
		t.Errorf("candidate not promoted: read_only=%v super_read_only=%v", st.ReadOnly, st.SuperReadOnly)
	}
	if !strings.Contains(logs.String(), `msg="relay log drain complete"`) {
		t.Errorf("drain did not complete cleanly; logs:\n%s", logs.String())
	}
}

// An applier error still aborts the drain, and promotion proceeds as before:
// the backlog cannot be applied without intervention.
func TestFailover_DrainAbortsOnApplierError(t *testing.T) {
	candidate := testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IOConnecting: true,
		LastIOErrno: 2003, LastIOError: "Can't connect",
		LastSQLErrno: 1062, LastSQLError: "Could not execute Write_rows event: Duplicate entry",
		ReadOnly:         true,
		RetrievedGtidSet: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-20",
		ExecutedGtidSet:  "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10",
	})
	checker := candidate.Checker()
	defer checker.Close()

	var logs bytes.Buffer
	fc := controller.NewFailoverController(slog.New(slog.NewTextHandler(&logs, nil)))
	if _, err := fc.Execute(context.Background(), checker, nil, "dc2"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := candidate.Polls(); got != 1 {
		t.Errorf("SHOW REPLICA STATUS polls = %d, want 1 (abort on first applier error)", got)
	}
	if !strings.Contains(logs.String(), "relay log drain aborted: SQL thread error: 1062") {
		t.Errorf("want the applier error in the drain warning; logs:\n%s", logs.String())
	}
	if candidate.State().ReadOnly {
		t.Error("candidate should still be promoted after an aborted drain")
	}
}
