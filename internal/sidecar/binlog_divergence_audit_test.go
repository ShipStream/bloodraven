package sidecar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// The history audit (auditArchive): what it reports as an archiver error
// and what it only records.

const auditOtherUUID = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"

// auditRow is an archived row holding gtids on top of previous.
func auditRow(name, id, previous, gtids string) ManifestEntry {
	id = strings.Repeat(id, 64)
	return ManifestEntry{Name: name, ID: id, PreviousGTIDs: previous, GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: gtids, ContentID: id}}
}

func auditArchiver(t *testing.T, state roleChecker, m *Manifest) (*BinlogArchiver, ArchiveStore) {
	t.Helper()
	store := newMemStore()
	if err := seedManifest(context.Background(), store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	return newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.New(slog.NewTextHandler(io.Discard, nil))), store
}

// Divergence is evidence for the planner. It is an archiver error only when
// this server could issue the missing GTIDs again: when they were issued
// under the server_uuid it has now.
func TestDivergenceIsAnErrorOnlyWhenReissuable(t *testing.T) {
	sid := coverageTestSID
	for _, tc := range []struct {
		name       string
		uuid       string
		readOnly   bool
		executed   string
		rows       []ManifestEntry
		divergent  string // MissingGTIDs of mysql-bin.000002, "" for none
		reissuable string
		wantError  bool
	}{
		{name: "replica recloned with a new server_uuid", uuid: auditOtherUUID, readOnly: true, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":11"},
		{name: "primary with a new server_uuid", uuid: auditOtherUUID, executed: sid + ":1-10," + auditOtherUUID + ":1-4",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":11"},
		{name: "replica that kept its server_uuid", uuid: sid, readOnly: true, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":11", reissuable: sid + ":11", wantError: true},
		{name: "primary that kept its server_uuid", uuid: sid, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":11", reissuable: sid + ":11", wantError: true},
		{name: "server_uuid in another case", uuid: strings.ToUpper(sid), readOnly: true, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":11", reissuable: sid + ":11", wantError: true},
		{name: "tagged GTIDs of its own server_uuid", uuid: sid, readOnly: true, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":blue:1-2")}, divergent: sid + ":blue:1-2", reissuable: sid + ":blue:1-2", wantError: true},
		{name: "only another source's transactions missing", uuid: sid, readOnly: true, executed: sid + ":1-11",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11,"+auditOtherUUID+":1-3")}, divergent: auditOtherUUID + ":1-3"},
		{name: "own and another source's transactions missing", uuid: sid, readOnly: true, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11,"+auditOtherUUID+":1-3")}, divergent: sid + ":11," + auditOtherUUID + ":1-3", reissuable: sid + ":11", wantError: true},
		{name: "unknown server_uuid", uuid: "", readOnly: true, executed: sid + ":1-10",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":11", reissuable: sid + ":11", wantError: true},
		{name: "a predecessor missing", uuid: auditOtherUUID, readOnly: true, executed: sid + ":1-8," + sid + ":11",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}, divergent: sid + ":9-10"},
		{name: "nothing missing", uuid: sid, readOnly: true, executed: sid + ":1-11",
			rows: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			state := &bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: tc.readOnly}, uuid: tc.uuid, executed: tc.executed}, ready: true}
			a, store := auditArchiver(t, state, &Manifest{Site: "iad", Files: tc.rows})
			for scan := 0; scan < 3; scan++ {
				a.maintainArchive(ctx)
				st := a.Snapshot(ctx)
				m, err := loadManifest(ctx, store, "binlogs", "iad")
				if err != nil {
					t.Fatal(err)
				}
				got := ""
				if len(m.Divergent) > 0 {
					got = m.Divergent[0].MissingGTIDs
				}
				wantRows := int64(0)
				if tc.divergent != "" {
					wantRows = 1
				}
				if got != mustGTIDs(t, tc.divergent) || len(m.Divergent) != int(wantRows) || st.DivergentRows != wantRows {
					t.Fatalf("scan %d: divergence evidence %+v (divergentRows %d), want %q", scan, m.Divergent, st.DivergentRows, tc.divergent)
				}
				if st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != mustGTIDs(t, tc.reissuable) {
					t.Fatalf("scan %d: reissuableGtids %v, want %q", scan, st.ReissuableGTIDs, tc.reissuable)
				}
				if tc.wantError {
					if !strings.Contains(st.LastError, "reuses their GTIDs") || !strings.Contains(st.LastError, mustGTIDs(t, tc.reissuable)) || st.UploadFailures != int64(scan+1) {
						t.Fatalf("scan %d: lastError %q, uploadFailures %d", scan, st.LastError, st.UploadFailures)
					}
				} else if st.LastError != "" || st.UploadFailures != 0 {
					t.Fatalf("scan %d: divergence that cannot be reissued is reported as an error: lastError %q, uploadFailures %d", scan, st.LastError, st.UploadFailures)
				}
			}
		})
	}
}

// mustGTIDs is text in the canonical spelling the audit writes.
func mustGTIDs(t *testing.T, text string) string {
	t.Helper()
	if text == "" {
		return ""
	}
	set, err := internalmysql.ParseGTIDSet(text)
	if err != nil {
		t.Fatal(err)
	}
	return set.String()
}

// A quarantine moves metadata only: the server can reissue a quarantined
// row's lost transactions like an active row's, with no Divergent[] entry.
func TestQuarantinedRowIsStillReissuable(t *testing.T) {
	ctx := context.Background()
	sid := coverageTestSID
	row := auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")
	for _, tc := range []struct {
		name, uuid, executed string
		evidence             *ArchiveDivergence
		reissuable           string
	}{
		{"own server_uuid, transactions missing", sid, sid + ":1-10", nil, sid + ":11"},
		{"own server_uuid, numbers back, loss recorded at quarantine", sid, sid + ":1-11", &ArchiveDivergence{Name: row.Name, ID: row.ID, MissingGTIDs: sid + ":11"}, sid + ":11"},
		{"own server_uuid, nothing missing", sid, sid + ":1-11", nil, ""},
		{"new server_uuid, transactions missing", auditOtherUUID, sid + ":1-10", &ArchiveDivergence{Name: row.Name, ID: row.ID, MissingGTIDs: sid + ":11"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: tc.uuid, executed: tc.executed}, ready: true}
			m := &Manifest{Site: "iad", Quarantined: []ArchiveQuarantine{{Entry: row, Divergent: tc.evidence, Reason: "abandoned tail", ResumeGTIDs: sid + ":1-10"}}}
			a, _ := auditArchiver(t, state, m)
			a.maintainArchive(ctx)
			st := a.Snapshot(ctx)
			if st.DivergentRows != 0 || st.QuarantinedRows != 1 || st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != tc.reissuable {
				t.Fatalf("status: divergentRows %d, quarantinedRows %d, reissuableGtids %v, want %q", st.DivergentRows, st.QuarantinedRows, st.ReissuableGTIDs, tc.reissuable)
			}
			if (st.LastError != "") != (tc.reissuable != "") || (tc.reissuable != "" && !strings.Contains(st.LastError, "reuses their GTIDs")) {
				t.Fatalf("lastError %q with reissuable %q", st.LastError, tc.reissuable)
			}
		})
	}
}

// A loss the server could reissue is added to the quarantine's own evidence,
// so the numbers' return (the reissue, still in the active binlog) does not
// end the verdict for a quarantined row any more than for an active one.
func TestQuarantinedRowLossSurvivesTheNumbersReturn(t *testing.T) {
	ctx := context.Background()
	sid := coverageTestSID
	row := auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11,"+auditOtherUUID+":1-3")
	state := &bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: sid, executed: sid + ":1-10"}, ready: true}
	a, store := auditArchiver(t, state, &Manifest{Site: "iad", Quarantined: []ArchiveQuarantine{{Entry: row, Reason: "object lost", ResumeGTIDs: sid + ":1-10"}}})
	evidence := func() *ArchiveDivergence {
		t.Helper()
		m, err := loadManifest(ctx, store, "binlogs", "iad")
		if err != nil || len(m.Quarantined) != 1 || len(m.Divergent) != 0 {
			t.Fatalf("manifest: %+v, %v", m, err)
		}
		return m.Quarantined[0].Divergent
	}
	a.maintainArchive(ctx)
	// Only what this server could reissue is added: the other source's
	// missing transactions are not its to issue.
	if got := evidence(); got == nil || got.Name != row.Name || got.ID != row.ID || got.MissingGTIDs != sid+":11" {
		t.Fatalf("quarantine evidence = %+v", got)
	}
	state.executed = sid + ":1-11," + auditOtherUUID + ":1-3"
	for i := 0; i < 2; i++ {
		a.maintainArchive(ctx)
		if st := a.Snapshot(ctx); st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != sid+":11" || !strings.Contains(st.LastError, "reuses their GTIDs") {
			t.Fatalf("after the numbers returned: reissuableGtids %v, lastError %q", st.ReissuableGTIDs, st.LastError)
		}
	}
	// Undoing the quarantine hands the evidence to the active row.
	if err := testQuarantine(ctx, store, "binlogs", "iad", row.Name, row.ID, "", "", true); err != nil {
		t.Fatal(err)
	}
	a.maintainArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Quarantined) != 0 || len(m.Divergent) != 1 || m.Divergent[0].MissingGTIDs != sid+":11" {
		t.Fatalf("after the undo: %+v, %v", m, err)
	}
	if st := a.Snapshot(ctx); *st.ReissuableGTIDs != sid+":11" {
		t.Fatalf("after the undo: reissuableGtids %v", st.ReissuableGTIDs)
	}
	// A server that issued none of the missing transactions writes no
	// evidence at all.
	state.uuid = "cccccccc-bbbb-cccc-dddd-eeeeeeeeeeee"
	state.executed = sid + ":1-10"
	b, other := auditArchiver(t, state, &Manifest{Site: "iad", Quarantined: []ArchiveQuarantine{{Entry: row, Reason: "object lost", ResumeGTIDs: sid + ":1-10"}}})
	b.maintainArchive(ctx)
	if m, _ := loadManifest(ctx, other, "binlogs", "iad"); m.Quarantined[0].Divergent != nil {
		t.Fatalf("evidence recorded for a server that cannot reissue: %+v", m.Quarantined[0].Divergent)
	}
}

type failingAuditState struct {
	stubRoleChecker
	err error
}

func (s *failingAuditState) BinlogState(context.Context) (string, string, error) {
	return "", "", s.err
}

// What the audit keeps fail-closed besides reissuable divergence: a server
// whose state cannot be read, and the reuse guard, which outranks every
// divergence verdict.
func TestAuditFailClosedCases(t *testing.T) {
	ctx := context.Background()
	sid := coverageTestSID
	row := auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")

	t.Run("unreadable server state", func(t *testing.T) {
		state := &failingAuditState{stubRoleChecker: stubRoleChecker{readOnly: true}, err: errors.New("connection refused")}
		a, _ := auditArchiver(t, state, &Manifest{Site: "iad", Files: []ManifestEntry{row}})
		a.maintainArchive(ctx)
		if st := a.Snapshot(ctx); !strings.Contains(st.LastError, "connection refused") || st.UploadFailures != 1 || st.ReissuableGTIDs != nil {
			t.Fatalf("status = %+v", st)
		}
	})

	t.Run("reuse guard", func(t *testing.T) {
		state := &bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: auditOtherUUID, executed: sid + ":1-10"}, ready: true}
		reason := "verified GTID namespace reuse within " + sid + ":11 (original commit identities differ); use a new manifest prefix and full backup"
		a, store := auditArchiver(t, state, &Manifest{Site: "iad", Files: []ManifestEntry{row}, Epoch: &ArchiveEpoch{ResetReason: reason}})
		a.maintainArchive(ctx)
		if st := a.Snapshot(ctx); st.LastError != "site iad: "+reason || st.UploadFailures != 1 {
			t.Fatalf("status = %+v", st)
		}
		// The guard stands in front of the divergence audit: nothing else
		// is written to a manifest no restore accepts.
		if m, _ := loadManifest(ctx, store, "binlogs", "iad"); len(m.Divergent) != 0 {
			t.Fatalf("divergence recorded behind the reuse guard: %+v", m.Divergent)
		}
	})

	t.Run("no verdict without server history", func(t *testing.T) {
		for _, state := range []roleChecker{
			&bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: sid, executed: ""}, ready: true},
			&bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: sid, executed: sid + ":1-3"}, ready: false},
			&stubRoleChecker{readOnly: true},
		} {
			a, store := auditArchiver(t, state, &Manifest{Site: "iad", Files: []ManifestEntry{row}})
			a.maintainArchive(ctx)
			st := a.Snapshot(ctx)
			m, _ := loadManifest(ctx, store, "binlogs", "iad")
			if st.ReissuableGTIDs != nil || st.LastError != "" || len(m.Divergent) != 0 {
				t.Fatalf("%T: reissuableGtids %v, lastError %q, divergent %+v", state, st.ReissuableGTIDs, st.LastError, m.Divergent)
			}
		}
	})
}

// A verdict from before the server lost its history does not outlive it:
// while the audit cannot evaluate the server, the status reports no verdict.
func TestReissuableVerdictIsDroppedWhileTheServerHasNoHistory(t *testing.T) {
	ctx := context.Background()
	sid := coverageTestSID
	state := &bootstrapAuditState{historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: sid, executed: sid + ":1-10"}, ready: true}
	a, _ := auditArchiver(t, state, &Manifest{Site: "iad", Files: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}})
	a.maintainArchive(ctx)
	if st := a.Snapshot(ctx); st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != sid+":11" || st.LastError == "" {
		t.Fatalf("status = %+v", st)
	}
	// The site is recloned: a clone in progress, then a new server_uuid.
	state.ready = false
	a.maintainArchive(ctx)
	if st := a.Snapshot(ctx); st.ReissuableGTIDs != nil || st.LastError != "" {
		t.Fatalf("during the clone: reissuableGtids %v, lastError %q", st.ReissuableGTIDs, st.LastError)
	}
	state.ready, state.uuid = true, auditOtherUUID
	a.maintainArchive(ctx)
	if st := a.Snapshot(ctx); st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != "" || st.LastError != "" || st.DivergentRows != 1 {
		t.Fatalf("after the clone: %+v", st)
	}
}

// probingAuditState reads the archiver's status from inside the audit's
// server-state query: after the scan proper, before the audit's verdict.
type probingAuditState struct {
	bootstrapAuditState
	probe func()
}

func (s *probingAuditState) BinlogState(ctx context.Context) (string, string, error) {
	if s.probe != nil {
		s.probe()
	}
	return s.bootstrapAuditState.BinlogState(ctx)
}

// A scan that ends clean does not clear a standing audit error for the
// maintenance pass to set again: a status poll that lands in between would
// see a clean archiver, and the operator counts clean polls as a recovery.
func TestStandingAuditErrorSurvivesTheScan(t *testing.T) {
	sid := coverageTestSID
	for _, readOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "writable", true: "read-only"}[readOnly], func(t *testing.T) {
			ctx := context.Background()
			state := &probingAuditState{bootstrapAuditState: bootstrapAuditState{
				historyStateStub: historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: readOnly}, uuid: sid, executed: sid + ":1-10"}, ready: true}}
			store := newMemStore()
			m := &Manifest{Site: "iad", Files: []ManifestEntry{auditRow("mysql-bin.000002", "a", sid+":1-10", sid+":11")}}
			if err := seedManifest(ctx, store, "binlogs", m); err != nil {
				t.Fatal(err)
			}
			cfg := &PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}
			a := newTestArchiver(cfg, "iad", state, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
			var between []string
			state.probe = func() { between = append(between, a.Snapshot(ctx).LastError) }
			for i := 0; i < 3; i++ {
				a.scanAndArchive(ctx)
				if st := a.Snapshot(ctx); !strings.Contains(st.LastError, "reuses their GTIDs") || st.ConsecutiveFailedScans != int64(i+1) {
					t.Fatalf("scan %d: lastError %q, failedScans %d", i, st.LastError, st.ConsecutiveFailedScans)
				}
			}
			if len(between) != 3 || between[0] != "" {
				t.Fatalf("status between scan and audit: %q", between)
			}
			for i, got := range between[1:] {
				if !strings.Contains(got, "reuses their GTIDs") {
					t.Fatalf("scan %d cleared the standing audit error before the audit ran: %q", i+1, got)
				}
			}
			// Recloned with a new server_uuid: the audit clears its error.
			state.uuid = auditOtherUUID
			a.scanAndArchive(ctx)
			if st := a.Snapshot(ctx); st.LastError != "" || st.ConsecutiveFailedScans != 0 || st.DivergentRows != 1 {
				t.Fatalf("after the reclone: %+v", st)
			}
			a.scanAndArchive(ctx)
			if got := between[len(between)-1]; got != "" {
				t.Fatalf("a resolved audit error came back: %q", got)
			}
		})
	}
}
