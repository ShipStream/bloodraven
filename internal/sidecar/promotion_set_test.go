package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// The promotion set and the confirmation of a promotion record (issue
// #269). The server under test has server_uuid psOwn and server_id 102;
// psPeer is the source it replicated from.
const (
	psOwn  = hintSIDB
	psPeer = hintSIDA
)

// psLog collects the archiver's log lines.
type psLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *psLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *psLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// psFixture is an archiver over a binlog directory whose files have the
// given Previous_gtids (the last one is the active file), a read-only
// server and a clock the test moves.
type psFixture struct {
	t     *testing.T
	ctx   context.Context
	cfg   *PITRConfig
	store *faultStore
	role  *epSlowIntegrity
	a     *BinlogArchiver
	clock time.Time
	log   *psLog
}

func psWriteBinlogs(t *testing.T, dir string, previous ...string) {
	t.Helper()
	var index []string
	for i, prev := range previous {
		name := fmt.Sprintf("mysql-bin.%06d", i+1)
		index = append(index, name)
		data := binlogtest.File{ServerID: 102, Previous: prev}.MustBytes()
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte(strings.Join(index, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// newPSFixture starts read-only with gtid_executed readOnly and runs the
// first scan, which opens the session and observes the server read-only.
func newPSFixture(t *testing.T, readOnly string, previous ...string) *psFixture {
	t.Helper()
	dir := t.TempDir()
	psWriteBinlogs(t, dir, previous...)
	f := &psFixture{t: t, ctx: context.Background(), store: &faultStore{ArchiveStore: newMemStore()}, log: &psLog{},
		clock: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)}
	f.cfg = &PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour}
	f.role = &epSlowIntegrity{stubRoleChecker: &stubRoleChecker{readOnly: true, serverUUID: psOwn, serverID: 102, gtidExecuted: readOnly}, clock: &f.clock}
	f.start()
	f.a.scanAndArchive(f.ctx)
	if st := f.a.Snapshot(f.ctx); st.LastError != "" {
		t.Fatalf("read-only scan: %+v", st)
	}
	return f
}

// start builds the archiver (a sidecar start).
func (f *psFixture) start() {
	f.a = newTestArchiver(f.cfg, "iad", f.role, f.store, slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug})))
	f.a.now = func() time.Time { return f.clock }
}

func (f *psFixture) set(readOnly bool, gtids string) {
	f.role.mu.Lock()
	f.role.readOnly, f.role.gtidExecuted = readOnly, gtids
	f.role.mu.Unlock()
}

// tick advances the clock and runs one role probe and record tick.
func (f *psFixture) tick(d time.Duration) {
	f.clock = f.clock.Add(d)
	f.a.roleTick(f.ctx)
}

func (f *psFixture) records() []PromotionRecord {
	f.t.Helper()
	m, err := loadManifest(f.ctx, f.store, "binlogs", "iad")
	if err != nil {
		f.t.Fatal(err)
	}
	return m.Promotions
}

// promote makes the server writable with gtids and records the episode.
func (f *psFixture) promote(gtids string) PromotionRecord {
	f.t.Helper()
	before := len(f.records())
	f.set(false, gtids)
	f.tick(5 * time.Second)
	recs := f.records()
	if len(recs) != before+1 {
		f.t.Fatalf("promotion wrote %d records, want 1 (log: %s)", len(recs)-before, f.log)
	}
	return recs[len(recs)-1]
}

// psSet is the canonical spelling of a GTID set, as the sidecar records a
// promotion set.
func psSet(t *testing.T, s string) string {
	t.Helper()
	set, err := internalmysql.ParseGTIDSet(s)
	if err != nil {
		t.Fatal(err)
	}
	return set.String()
}

// wantNoSet asserts the record carries no promotion set and that the
// sidecar logged why.
func (f *psFixture) wantNoSet(rec PromotionRecord, reason string) {
	f.t.Helper()
	if rec.PromotionGTIDExecuted != "" {
		f.t.Fatalf("record carries the promotion set %q, want none", rec.PromotionGTIDExecuted)
	}
	if log := f.log.String(); !strings.Contains(log, `msg="promotion set not recorded"`) || !strings.Contains(log, reason) {
		f.t.Fatalf("log lacks the reason %q:\n%s", reason, log)
	}
}

// The operator's promotion-time FLUSH BINARY LOGS was acknowledged: the
// active file's Previous_gtids are the promotion GTID set, which is more
// than the last read-only observation held, and the record's observation
// already holds the episode's first own transaction.
func TestPromotionSetFromAcknowledgedRotation(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	rec := f.promote(psPeer + ":1-7," + psOwn + ":1")
	want := PromotionRecord{ServerUUID: psOwn, ServerID: 102, GTIDExecuted: psPeer + ":1-7," + psOwn + ":1", ObservedAt: f.clock,
		ProbeInterval: pitrarchive.Duration(5 * time.Second), ReadOnlyGTIDExecuted: psPeer + ":1-5", PromotionGTIDExecuted: psSet(t, psPeer+":1-7")}
	if rec != want {
		t.Fatalf("record = %+v, want %+v", rec, want)
	}
	if log := f.log.String(); !strings.Contains(log, `msg="promotion recorded"`) || !strings.Contains(log, "promotionGtidExecuted="+psSet(t, psPeer+":1-7")) {
		t.Fatalf("log: %s", log)
	}
	// The record gated the upload of the sealed file as before.
	f.a.scanAndArchive(f.ctx)
	if st := f.a.Snapshot(f.ctx); st.FilesArchived != 1 || st.LastError != "" {
		t.Fatalf("uploads: %+v", st)
	}
}

// Which boundary the derivation takes, and what it leaves out.
func TestPromotionSetBoundary(t *testing.T) {
	const gro = psPeer + ":1-5"
	tests := []struct {
		name     string
		readOnly string
		previous []string
		recorded string
		want     string
	}{
		// No rotation at all: the only file predates everything.
		{"no rotation", gro, []string{""}, psPeer + ":1-7," + psOwn + ":1", gro},
		// The rotation was not acknowledged: an older boundary, a smaller set.
		{"older boundary", gro, []string{"", psPeer + ":1-3"}, psPeer + ":1-7," + psOwn + ":1", gro},
		{"older boundary beyond the read-only observation", gro, []string{"", psPeer + ":1-6"}, psPeer + ":1-7," + psOwn + ":1", psPeer + ":1-6"},
		// The rotation landed after the episode's first own transaction
		// (it timed out and completed after writes were enabled): that
		// file is no boundary, the one before it is.
		{"own transaction before the newest boundary", gro, []string{"", psPeer + ":1-6", psPeer + ":1-7," + psOwn + ":1"}, psPeer + ":1-7," + psOwn + ":1-2", psPeer + ":1-6"},
		// Own transactions of earlier eras are in the read-only observation.
		{"own transactions of an earlier era", gro + "," + psOwn + ":1-3", []string{"", psPeer + ":1-7," + psOwn + ":1-3"}, psPeer + ":1-7," + psOwn + ":1-4", psPeer + ":1-7," + psOwn + ":1-3"},
		// A file opened after the record's observation (a writable server
		// that still replicates) holds more than the record: not this
		// record's boundary.
		{"boundary beyond the record", gro, []string{"", psPeer + ":1-6", psPeer + ":1-9"}, psPeer + ":1-7", psPeer + ":1-6"},
		// Every local file was opened after the episode's first own
		// transaction (the boundary was purged): the read-only set alone.
		{"boundary purged", gro, []string{psPeer + ":1-7," + psOwn + ":1", psPeer + ":1-7," + psOwn + ":1-2"}, psPeer + ":1-7," + psOwn + ":1-3", gro},
		// A transaction the server committed under another source's GTID
		// before a boundary is in the set: every witness is under the
		// server's own uuid, and that boundary precedes all of those.
		{"foreign-uuid transaction of the episode before the boundary", gro, []string{"", psPeer + ":1-7," + epSIDB + ":9"}, psPeer + ":1-7," + epSIDB + ":9," + psOwn + ":1", psPeer + ":1-7," + epSIDB + ":9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPSFixture(t, tt.readOnly, tt.previous...)
			rec := f.promote(tt.recorded)
			if rec.PromotionGTIDExecuted != psSet(t, tt.want) {
				t.Fatalf("promotion set %q, want %q (log: %s)", rec.PromotionGTIDExecuted, psSet(t, tt.want), f.log)
			}
			if rec.ReadOnlyGTIDExecuted != tt.readOnly {
				t.Fatalf("readOnlyGtidExecuted %q", rec.ReadOnlyGTIDExecuted)
			}
		})
	}
}

// Without a read-only observation of the same incarnation directly before
// the episode there is no promotion set; the record is written all the same.
func TestPromotionSetNeedsReadOnlyObservation(t *testing.T) {
	t.Run("sidecar start while writable", func(t *testing.T) {
		f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
		f.promote(psPeer + ":1-7")
		f.clock = f.clock.Add(3 * time.Second)
		f.start() // the sidecar restarts in the middle of the episode
		f.a.scanAndArchive(f.ctx)
		recs := f.records()
		if len(recs) != 2 {
			t.Fatalf("%d records, want the restart's second one", len(recs))
		}
		f.wantNoSet(recs[1], "did not observe this server read-only directly before the episode")
	})
	t.Run("failed probe in between", func(t *testing.T) {
		f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
		f.role.mu.Lock()
		f.role.isReadOnlyErr = errors.New("connection refused")
		f.role.mu.Unlock()
		f.tick(5 * time.Second)
		f.role.mu.Lock()
		f.role.isReadOnlyErr = nil
		f.role.mu.Unlock()
		f.wantNoSet(f.promote(psPeer+":1-7"), "did not observe this server read-only directly before the episode")
	})
	t.Run("another server_uuid", func(t *testing.T) {
		f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
		f.role.mu.Lock()
		f.role.serverUUID = "cccccccc-0000-0000-0000-00000000000c"
		f.role.mu.Unlock()
		f.wantNoSet(f.promote(psPeer+":1-7"), "did not observe this server read-only directly before the episode")
	})
	// A reclone or reset between the read-only observation and the
	// episode: the same identity, a gtid_executed that lost transactions.
	t.Run("history replaced before the episode", func(t *testing.T) {
		f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-3")
		rec := f.promote(psPeer + ":1-3")
		f.wantNoSet(rec, "the server changed incarnation between the sidecar's last read-only observation and the episode")
		if rec.ReadOnlyGTIDExecuted != psPeer+":1-5" {
			t.Fatalf("readOnlyGtidExecuted %q is kept for diagnosis", rec.ReadOnlyGTIDExecuted)
		}
	})
}

// A header that cannot be read, or a read that exceeds its bound, costs the
// promotion set and nothing else: the record is written on time.
func TestPromotionSetReadFailures(t *testing.T) {
	const gro, recorded = psPeer + ":1-5", psPeer + ":1-7"
	t.Run("missing file", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		if err := os.Remove(filepath.Join(f.cfg.BinlogDir, "mysql-bin.000002")); err != nil {
			t.Fatal(err)
		}
		f.wantNoSet(f.promote(recorded), "read the header of mysql-bin.000002")
	})
	t.Run("encrypted file", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		data, err := os.ReadFile(filepath.Join(f.cfg.BinlogDir, "mysql-bin.000002"))
		if err != nil {
			t.Fatal(err)
		}
		data[0] = 0xfd // the magic of an encrypted binlog
		for i := 4; i < len(data); i++ {
			data[i] ^= 0x5a
		}
		if err := os.WriteFile(filepath.Join(f.cfg.BinlogDir, "mysql-bin.000002"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		f.wantNoSet(f.promote(recorded), "read the header of mysql-bin.000002")
	})
	t.Run("truncated header", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		if err := os.Truncate(filepath.Join(f.cfg.BinlogDir, "mysql-bin.000002"), 40); err != nil {
			t.Fatal(err)
		}
		f.wantNoSet(f.promote(recorded), "read the header of mysql-bin.000002")
	})
	// An unreadable newer file ends the derivation even though an older
	// one would qualify: the newer one might be the boundary.
	t.Run("read error", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		f.a.promotionSets.header = func(string) (binlogHeader, error) { return binlogHeader{}, errors.New("input/output error") }
		f.wantNoSet(f.promote(recorded), "input/output error")
	})
	t.Run("slow volume", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		f.a.promotionSets.bound = 50 * time.Millisecond
		f.a.promotionSets.header = func(path string) (binlogHeader, error) {
			<-release
			return localBinlogHeader(path)
		}
		start := time.Now()
		rec := f.promote(recorded)
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("the record waited %s for the binlog headers", took)
		}
		f.wantNoSet(rec, "reading the local binlog headers took longer than 50ms")
		// The episode is recorded and uploads.
		f.a.scanAndArchive(f.ctx)
		if st := f.a.Snapshot(f.ctx); st.FilesArchived != 1 || st.LastError != "" {
			t.Fatalf("uploads: %+v", st)
		}
	})
	t.Run("too many files since the promotion", func(t *testing.T) {
		previous := []string{""}
		for i := 0; i < promotionSetMaxFiles; i++ {
			previous = append(previous, fmt.Sprintf("%s:1-7,%s:1-%d", psPeer, psOwn, i+1))
		}
		f := newPSFixture(t, gro, previous...)
		f.wantNoSet(f.promote(fmt.Sprintf("%s:1-7,%s:1-%d", psPeer, psOwn, promotionSetMaxFiles+1)),
			fmt.Sprintf("none of the %d newest binlog files was opened before the episode's first transaction", promotionSetMaxFiles))
	})
}

// The read-only observation is of one incarnation; the server then loses
// transactions and becomes writable (review r5). The probe that opens the
// episode is slower than maxRoleProbeSpread, so it cannot open the record,
// but it shows the change, which is written as a reset record and
// forgotten. Before the next probe the server commits a transaction of its
// own and then applies the lost ones again, so that probe's gtid_executed
// contains the read-only observation's and no reset is pending. The record
// it opens must not take that observation for its promotion set: this
// incarnation executed PEER:4-5 after OWN:1.
func TestPromotionSetAcrossRecordedIncarnationChange(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-3")
	f.role.probe = 2 * time.Second
	f.set(false, psPeer+":1-3")
	f.tick(5 * time.Second)
	recs := f.records()
	if len(recs) != 1 || recs[0].Kind != pitrarchive.RecordReset || recs[0].GTIDExecuted != psPeer+":1-3" {
		t.Fatalf("after the slow probe: %+v, want the reset record alone", recs)
	}
	f.role.probe = 0
	f.set(false, psPeer+":1-5,"+psOwn+":1")
	f.tick(5 * time.Second)
	recs = f.records()
	if len(recs) != 2 || recs[1].Kind != pitrarchive.RecordPromotion || recs[1].GTIDExecuted != psPeer+":1-5,"+psOwn+":1" {
		t.Fatalf("after the prompt probe: %+v, want the episode's promotion record", recs)
	}
	f.wantNoSet(recs[1], "the server changed incarnation between the sidecar's last read-only observation and the episode")
	// A record without a promotion set gets no confirmation either.
	f.tick(10 * time.Second)
	f.tick(5 * time.Second)
	if got := f.records()[1]; !got.ConfirmedAt.IsZero() || got.ConfirmedGTIDAdded != "" {
		t.Fatalf("the record without a promotion set was confirmed: %+v", got)
	}
}

// The server must be the record's incarnation after the last file read: a
// reset or a reclone while the headers are read leaves files of another
// incarnation under the same names.
func TestPromotionSetIncarnationAcrossDerivation(t *testing.T) {
	const gro, recorded = psPeer + ":1-5", psPeer + ":1-7"
	during := func(f *psFixture, change func()) {
		f.a.promotionSets.header = func(path string) (binlogHeader, error) {
			h, err := localBinlogHeader(path)
			change()
			return h, err
		}
	}
	t.Run("history replaced", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		during(f, func() { f.set(false, psPeer+":1-2") })
		rec := f.promote(recorded)
		f.wantNoSet(rec, "the server left the episode or changed incarnation while the binlog headers were read")
		if rec.GTIDExecuted != recorded {
			t.Fatalf("the record holds its own observation: %+v", rec)
		}
	})
	t.Run("server_uuid changed", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		during(f, func() {
			f.role.mu.Lock()
			f.role.serverUUID = "cccccccc-0000-0000-0000-00000000000c"
			f.role.mu.Unlock()
		})
		f.wantNoSet(f.promote(recorded), "the server left the episode or changed incarnation while the binlog headers were read")
	})
	t.Run("turned read-only", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		during(f, func() { f.set(true, recorded) })
		f.wantNoSet(f.promote(recorded), "the server left the episode or changed incarnation while the binlog headers were read")
	})
	t.Run("probe fails", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-7")
		during(f, func() {
			f.role.mu.Lock()
			f.role.isReadOnlyErr = errors.New("server has gone away")
			f.role.mu.Unlock()
		})
		f.wantNoSet(f.promote(recorded), "the role probe after the binlog headers were read failed")
	})
	// The index restarts while its file is read (RESET BINARY LOGS AND
	// GTIDS, a clone): the file read may be the old or the new one.
	t.Run("binlog index restarted", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-6", psPeer+":1-7")
		during(f, func() { psWriteBinlogs(t, f.cfg.BinlogDir, "") })
		f.wantNoSet(f.promote(recorded), "the binlog index changed while mysql-bin.000003 was read")
	})
	// A rotation and a purge during the read keep the boundary's run.
	t.Run("rotation and purge during the read", func(t *testing.T) {
		f := newPSFixture(t, gro, "", psPeer+":1-6", psPeer+":1-7")
		during(f, func() {
			index := "mysql-bin.000002\nmysql-bin.000003\nmysql-bin.000004\n"
			if err := os.WriteFile(filepath.Join(f.cfg.BinlogDir, "mysql-bin.index"), []byte(index), 0o600); err != nil {
				t.Error(err)
			}
		})
		if rec := f.promote(recorded); rec.PromotionGTIDExecuted != psSet(t, psPeer+":1-7") {
			t.Fatalf("promotion set %q (log: %s)", rec.PromotionGTIDExecuted, f.log)
		}
	})
}

// A transaction under the server's own uuid with a tag is the server's own
// too (the test binlog builder writes no tagged Previous_gtids, so the
// headers are given directly).
func TestPromotionBoundaryTaggedOwnTransaction(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000001\nmysql-bin.000002\nmysql-bin.000003\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	parse := func(s string) internalmysql.GTIDSet {
		set, err := internalmysql.ParseGTIDSet(s)
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	headers := map[string]string{
		"mysql-bin.000001": "",
		"mysql-bin.000002": psPeer + ":1-6," + psPeer + ":batch:1-2",
		"mysql-bin.000003": psPeer + ":1-7," + psPeer + ":batch:1-2," + psOwn + ":batch:1",
	}
	read := func(path string) (binlogHeader, error) {
		return binlogHeader{previous: headers[filepath.Base(path)]}, nil
	}
	gro := parse(psPeer + ":1-5")
	recorded := parse(psPeer + ":1-7," + psPeer + ":batch:1-2," + psOwn + ":batch:1-2")
	got, err := promotionBoundary(dir, "mysql-bin.index", psOwn, gro, recorded, read, time.Now().Add(time.Minute))
	if err != nil || got.String() != parse(headers["mysql-bin.000002"]).String() {
		t.Fatalf("boundary %q %v, want the file before the tagged own transaction", got, err)
	}
	// A tagged transaction of the read-only set is no transaction of the episode.
	gro = parse(psPeer + ":1-5," + psOwn + ":batch:1")
	got, err = promotionBoundary(dir, "mysql-bin.index", psOwn, gro, recorded, read, time.Now().Add(time.Minute))
	if err != nil || got.String() != parse(headers["mysql-bin.000003"]).String() {
		t.Fatalf("boundary %q %v, want the newest file", got, err)
	}
	// A deadline that has passed reads nothing.
	if _, err := promotionBoundary(dir, "mysql-bin.index", psOwn, gro, recorded, read, time.Now().Add(-time.Second)); err == nil {
		t.Fatal("a passed deadline returned a boundary")
	}
}

func TestIndexStillHolds(t *testing.T) {
	tests := []struct {
		index, run string
		want       bool
	}{
		{"1 2 3", "2 3", true},
		{"1 2 3", "3", true},
		{"2 3 4", "2 3", true},  // a rotation appended
		{"3 4", "3", true},      // a purge removed the front
		{"1 2 3", "2 4", false}, // not the same run
		{"1", "2 3", false},     // restarted
		{"1 2", "2 3", false},   // restarted and shorter
		{"", "1", false},
	}
	for _, tt := range tests {
		if got := indexStillHolds(strings.Fields(tt.index), strings.Fields(tt.run)); got != tt.want {
			t.Errorf("indexStillHolds(%q, %q) = %v", tt.index, tt.run, got)
		}
	}
}

// The confirmation is the first observation read at least the confirm
// delay after the record's, in the same uninterrupted episode, and is
// saved once.
func TestPromotionRecordConfirmation(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	rec := f.promote(psPeer + ":1-7")
	observed := f.clock
	f.a.scanAndArchive(f.ctx) // the episode's upload does not wait for a confirmation
	if st := f.a.Snapshot(f.ctx); st.FilesArchived != 1 {
		t.Fatalf("uploads: %+v", st)
	}

	f.set(false, psPeer+":1-7,"+psOwn+":1-3")
	f.tick(5 * time.Second)
	if got := f.records()[0]; got != rec {
		t.Fatalf("confirmed %s after the record: %+v", f.clock.Sub(observed), got)
	}
	// A probe that returns after the delay but started before it does not
	// confirm: its read may be earlier than observedAt + 2δ.
	f.role.probe = time.Second
	f.set(false, psPeer+":1-7,"+psOwn+":1-4")
	f.tick(4500 * time.Millisecond) // returns at +10.5 s, started at +9.5 s
	if got := f.records()[0]; !got.ConfirmedAt.IsZero() {
		t.Fatalf("a probe that started before the delay confirmed: %+v", got)
	}
	f.role.probe = 0
	f.set(false, psPeer+":1-7,"+psOwn+":1-6")
	f.tick(4500 * time.Millisecond)
	want := rec
	want.ConfirmedAt, want.ConfirmedGTIDAdded = f.clock, psOwn+":1-6"
	got := f.records()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("records = %+v, want %+v", got, want)
	}
	if log := f.log.String(); !strings.Contains(log, `msg="promotion record confirmed"`) || !strings.Contains(log, "confirmedGtidAdded="+psOwn+":1-6") {
		t.Fatalf("log: %s", log)
	}
	// It is written once: later observations change nothing, and the
	// uploaded row is still listed.
	f.set(false, psPeer+":1-7,"+psOwn+":1-9")
	f.tick(5 * time.Second)
	f.a.scanAndArchive(f.ctx)
	m, err := loadManifest(f.ctx, f.store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Promotions) != 1 || m.Promotions[0] != want || len(m.Files) != 1 {
		t.Fatalf("manifest after later ticks: %+v files %d", m.Promotions, len(m.Files))
	}
	if n := strings.Count(f.log.String(), `msg="promotion record confirmed"`); n != 1 {
		t.Fatalf("confirmed %d times", n)
	}
}

// An idle episode confirms with an empty addition; confirmedAt carries
// the confirmation.
func TestPromotionRecordConfirmationWithoutWrites(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	f.promote(psPeer + ":1-7")
	f.tick(10 * time.Second)
	got := f.records()[0]
	if !got.ConfirmedAt.Equal(f.clock) || got.ConfirmedGTIDAdded != "" {
		t.Fatalf("record = %+v", got)
	}
	raw, _, err := f.store.Get(f.ctx, "binlogs/manifest-iad.json")
	if err != nil || !strings.Contains(string(raw), `"confirmedAt"`) || strings.Contains(string(raw), `"confirmedGtidAdded"`) {
		t.Fatalf("manifest: %v\n%s", err, raw)
	}
}

// Anything but a writable observation of the same incarnation between the
// record and the confirming observation leaves the record unconfirmed.
func TestPromotionRecordConfirmationNeedsAnUninterruptedEpisode(t *testing.T) {
	const recorded = psPeer + ":1-7"
	tests := []struct {
		name      string
		interrupt func(f *psFixture)
		restore   func(f *psFixture)
		// set: the episode after the interruption has a promotion set (it
		// followed a read-only observation).
		set bool
	}{
		{"failed probe", func(f *psFixture) {
			f.role.mu.Lock()
			f.role.isReadOnlyErr = errors.New("connection refused")
			f.role.mu.Unlock()
		}, func(f *psFixture) {
			f.role.mu.Lock()
			f.role.isReadOnlyErr = nil
			f.role.mu.Unlock()
		}, false},
		{"read-only probe", func(f *psFixture) { f.set(true, recorded) }, func(f *psFixture) { f.set(false, recorded) }, true},
		{"history replaced", func(f *psFixture) { f.set(false, psPeer+":1-3") }, func(*psFixture) {}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
			f.promote(recorded)
			tt.interrupt(f)
			f.tick(5 * time.Second)
			tt.restore(f)
			f.tick(5 * time.Second)
			f.tick(10 * time.Second) // the new episode's own delay
			f.tick(5 * time.Second)
			recs := f.records()
			promotions := recs[:0:0]
			for _, r := range recs {
				if r.Kind == pitrarchive.RecordPromotion {
					promotions = append(promotions, r)
				}
			}
			if len(promotions) != 2 {
				t.Fatalf("%d promotion records, want 2: %+v", len(promotions), recs)
			}
			if !promotions[0].ConfirmedAt.IsZero() {
				t.Fatalf("the interrupted episode's record was confirmed: %+v", promotions[0])
			}
			// The episode after the interruption is confirmed on its own,
			// when it has a promotion set: a confirmation is evidence only
			// beside one, so a record without one gets none.
			if (promotions[1].PromotionGTIDExecuted != "") != tt.set || promotions[1].ConfirmedAt.IsZero() == tt.set {
				t.Fatalf("the new episode's record: %+v", promotions[1])
			}
		})
	}
}

// A sidecar restart forgets the episode: the old record stays unconfirmed,
// and so does the restart's record, which has no promotion set.
func TestPromotionRecordConfirmationAcrossRestart(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	first := f.promote(psPeer + ":1-7")
	f.clock = f.clock.Add(3 * time.Second)
	f.start()
	f.a.scanAndArchive(f.ctx)
	f.tick(10 * time.Second)
	recs := f.records()
	if len(recs) != 2 || recs[0] != first {
		t.Fatalf("records = %+v", recs)
	}
	if recs[1].PromotionGTIDExecuted != "" || !recs[1].ConfirmedAt.IsZero() || recs[1].ConfirmedGTIDAdded != "" {
		t.Fatalf("the restart's record = %+v", recs[1])
	}
}

// A confirmation whose save fails is retried with the same observation.
func TestPromotionRecordConfirmationRetriesTheSave(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	rec := f.promote(psPeer + ":1-7")
	f.store.dropManifests = true
	f.set(false, psPeer+":1-7,"+psOwn+":1")
	f.tick(10 * time.Second)
	confirmedAt := f.clock
	if got := f.records()[0]; got != rec {
		t.Fatalf("record after a dropped save: %+v", got)
	}
	if st := f.a.Snapshot(f.ctx); st.LastError != "" || !st.PromotionRecordedAt.Equal(rec.ObservedAt) {
		t.Fatalf("a failed confirmation is no archiver error and leaves the episode recorded: %+v", st)
	}
	f.store.dropManifests = false
	f.set(false, psPeer+":1-7,"+psOwn+":1-5")
	f.tick(5 * time.Second)
	want := rec
	want.ConfirmedAt, want.ConfirmedGTIDAdded = confirmedAt, psOwn+":1"
	if got := f.records()[0]; got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

// Nothing is written for an episode the timeline has ended, whether the
// scan goroutine learns of the end before it looks (the tick) or while it
// holds a snapshot from before the end (the manifest save refuses).
func TestPromotionRecordConfirmationOfAnEndedEpisode(t *testing.T) {
	demoted := func(t *testing.T) (*psFixture, PromotionRecord, roleSnapshot) {
		f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
		rec := f.promote(psPeer + ":1-7")
		// The sampler sees the confirming observation, and then the
		// demotion, before the scan goroutine runs.
		f.clock = f.clock.Add(10 * time.Second)
		if _, _, err := f.a.sampleRole(f.ctx); err != nil {
			t.Fatal(err)
		}
		before := f.a.roles.snapshot()
		if before.confirm == nil {
			t.Fatal("the timeline holds no confirming observation")
		}
		f.set(true, psPeer+":1-7")
		f.clock = f.clock.Add(time.Second)
		if _, _, err := f.a.sampleRole(f.ctx); err != nil {
			t.Fatal(err)
		}
		return f, rec, before
	}
	t.Run("seen by the tick", func(t *testing.T) {
		f, rec, _ := demoted(t)
		f.a.recordTick(f.ctx)
		f.a.scanAndArchive(f.ctx)
		if got := f.records(); len(got) != 1 || got[0] != rec {
			t.Fatalf("records = %+v", got)
		}
	})
	t.Run("after the snapshot was taken", func(t *testing.T) {
		f, rec, before := demoted(t)
		f.a.confirmEpisode(f.ctx, before)
		if got := f.records(); len(got) != 1 || got[0] != rec {
			t.Fatalf("records = %+v", got)
		}
		if log := f.log.String(); !strings.Contains(log, `msg="promotion record not confirmed yet"`) || !strings.Contains(log, "the writable episode ended") {
			t.Fatalf("log: %s", log)
		}
	})
}

// The record's times are wall-clock times. When the wall clock stepped
// back between the record and its confirming observation (the timeline
// orders them by the monotonic clock), no confirmation is written: one
// dated before its delay would read as damaged evidence and cost the
// record its promotion set too.
func TestPromotionRecordConfirmationAfterAClockStep(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	rec := f.promote(psPeer + ":1-7")
	f.tick(10 * time.Second)
	snap := f.a.roles.snapshot()
	if snap.confirm == nil || !f.records()[0].ConfirmedAt.Equal(f.clock) {
		t.Fatalf("the fixture's record was not confirmed: %+v", f.records())
	}
	// The same episode again, with the confirming observation dated 9 s
	// after the record on the wall clock.
	f = newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	rec = f.promote(psPeer + ":1-7")
	snap = f.a.roles.snapshot()
	stepped := *snap.first
	stepped.ObservedAt = rec.ObservedAt.Add(9 * time.Second)
	snap.confirm = &stepped
	f.a.confirmEpisode(f.ctx, snap)
	if got := f.records(); len(got) != 1 || got[0] != rec || !f.a.episode.confirmed {
		t.Fatalf("records = %+v, confirmed %v", got, f.a.episode.confirmed)
	}
}

// A save of a manifest copy loaded before the confirmation keeps the
// confirmation and the promotion set (the session's guard).
func TestManifestGuardKeepsRecordEvidence(t *testing.T) {
	f := newPSFixture(t, psPeer+":1-5", "", psPeer+":1-7")
	rec := f.promote(psPeer + ":1-7")
	stale, err := f.a.loadManifest(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A copy as an older writer would hold it: without the new fields.
	stale.Promotions[0].PromotionGTIDExecuted = ""
	f.tick(10 * time.Second)
	want := f.records()[0]
	if want.ConfirmedAt.IsZero() || want.PromotionGTIDExecuted != rec.PromotionGTIDExecuted {
		t.Fatalf("confirmed record = %+v", want)
	}
	if err := f.a.saveManifest(f.ctx, stale); err != nil {
		t.Fatal(err)
	}
	if got := f.records(); len(got) != 1 || got[0] != want {
		t.Fatalf("after a stale save: %+v, want %+v", got, want)
	}
	if stale.Promotions[0] != want {
		t.Fatalf("the saved copy was not brought up to date: %+v", stale.Promotions[0])
	}

	// The guard alone: evidence is copied onto a record that lacks it,
	// never over evidence the record has, and the caller's slice is not
	// modified.
	g := manifestGuard{promotions: []PromotionRecord{want}}
	bare := want
	bare.PromotionGTIDExecuted, bare.ConfirmedAt, bare.ConfirmedGTIDAdded = "", time.Time{}, ""
	base := []PromotionRecord{bare}
	if got := g.withKnownEvidence(base); got[0] != want || base[0] != bare {
		t.Fatalf("withKnownEvidence = %+v (base %+v)", got, base)
	}
	other := want
	other.PromotionGTIDExecuted, other.ConfirmedGTIDAdded = psPeer+":1", psOwn+":7"
	if got := g.withKnownEvidence([]PromotionRecord{other}); got[0] != other {
		t.Fatalf("withKnownEvidence replaced evidence: %+v", got)
	}
}

// Older and newer manifests: a record without the new fields decodes and
// encodes without them, and an unknown field of a newer writer is ignored.
func TestPromotionRecordFieldsAreOptional(t *testing.T) {
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	old := PromotionRecord{ServerUUID: psOwn, ServerID: 102, GTIDExecuted: psPeer + ":1-7", ObservedAt: at,
		ProbeInterval: pitrarchive.Duration(5 * time.Second), ReadOnlyGTIDExecuted: psPeer + ":1-5"}
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"promotionGtidExecuted", "confirmedAt", "confirmedGtidAdded"} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("a record without %s encodes it: %s", key, raw)
		}
	}
	var back PromotionRecord
	if err := json.Unmarshal(raw, &back); err != nil || back != old {
		t.Fatalf("round trip: %+v %v", back, err)
	}
	future := strings.Replace(string(raw), `{`, `{"laterEvidence":{"x":1},`, 1)
	if err := json.Unmarshal([]byte(future), &back); err != nil || back != old {
		t.Fatalf("a record with an unknown field: %+v %v", back, err)
	}
	full := old
	full.PromotionGTIDExecuted, full.ConfirmedAt, full.ConfirmedGTIDAdded = psPeer+":1-7", at.Add(10*time.Second), psOwn+":1"
	raw, err = json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &back); err != nil || back != full {
		t.Fatalf("round trip with evidence: %+v %v", back, err)
	}
}
