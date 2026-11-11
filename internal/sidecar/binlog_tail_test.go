package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// The read-only tail drain (#264): a site whose server turned read-only
// still archives the sealed files of its last writable era, and no pure
// replica copy.

// tailSim is one site's mysqld as the archiver sees it: the role probe and
// a data directory with a binlog index, sealed files and the active one.
// A sealed file is written once, so its identity is stable across scans.
type tailSim struct {
	t        *testing.T
	dir      string
	role     *stubRoleChecker
	serverID uint32
	executed internalmysql.GTIDSet
	// logged are the GTIDs in this incarnation's binlogs, which is what a
	// Previous_gtids header holds: a clone's GTIDs are not among them.
	logged internalmysql.GTIDSet
	names  []string // the index; the last entry is active
	active binlogtest.File
}

func newTailSim(t *testing.T, uuid string, serverID uint32, readOnly bool) *tailSim {
	t.Helper()
	s := &tailSim{t: t, dir: t.TempDir(), serverID: serverID, executed: internalmysql.GTIDSet{},
		role: &stubRoleChecker{readOnly: readOnly, serverUUID: uuid, serverID: serverID}}
	s.start()
	return s
}

// start opens the first binlog of an incarnation.
func (s *tailSim) start() {
	s.logged = internalmysql.GTIDSet{}
	s.names = []string{"mysql-bin.000001"}
	s.active = binlogtest.File{ServerID: s.serverID}
	s.write()
}

func (s *tailSim) write() {
	s.t.Helper()
	if err := os.WriteFile(filepath.Join(s.dir, s.names[len(s.names)-1]), s.active.MustBytes(), 0o600); err != nil {
		s.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.dir, "mysql-bin.index"), []byte(strings.Join(s.names, "\n")+"\n"), 0o600); err != nil {
		s.t.Fatal(err)
	}
}

// apply logs transactions in the active binlog: commits of this server, or
// the copies a replica applies.
func (s *tailSim) apply(txns ...binlogtest.Txn) {
	s.t.Helper()
	for _, txn := range txns {
		set, err := internalmysql.ParseGTIDSet(fmt.Sprintf("%s:%d", txn.SID, txn.GNO))
		if err != nil {
			s.t.Fatal(err)
		}
		s.executed = UnionBinlogGTIDs(s.executed, set)
		s.logged = UnionBinlogGTIDs(s.logged, set)
	}
	s.active.Txns = append(s.active.Txns, txns...)
	s.role.mu.Lock()
	s.role.gtidExecuted = s.executed.String()
	s.role.mu.Unlock()
	s.write()
}

// rotate seals the active binlog (FLUSH BINARY LOGS) and returns its name.
func (s *tailSim) rotate() string {
	s.t.Helper()
	sealed := s.names[len(s.names)-1]
	var number int
	if _, err := fmt.Sscanf(sealed, "mysql-bin.%d", &number); err != nil {
		s.t.Fatal(err)
	}
	s.names = append(s.names, fmt.Sprintf("mysql-bin.%06d", number+1))
	s.active = binlogtest.File{ServerID: s.serverID, Previous: s.logged.String()}
	s.write()
	return sealed
}

func (s *tailSim) setReadOnly(readOnly bool) {
	s.role.mu.Lock()
	s.role.readOnly = readOnly
	s.role.mu.Unlock()
}

// reclone replaces the data directory with a clone holding executed: every
// binlog is gone, the numbering restarts and, as on MySQL, the first file's
// Previous_gtids is empty.
func (s *tailSim) reclone(executed string) {
	s.t.Helper()
	for _, name := range s.names {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
			s.t.Fatal(err)
		}
	}
	set, err := internalmysql.ParseGTIDSet(executed)
	if err != nil {
		s.t.Fatal(err)
	}
	s.executed = set
	s.role.mu.Lock()
	s.role.gtidExecuted = set.String()
	s.role.mu.Unlock()
	s.start()
}

func (s *tailSim) config() *PITRConfig {
	return &PITRConfig{BinlogDir: s.dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour}
}

// tailStore is a memStore that counts this site's manifest reads, records
// its binlog uploads, and can fail or observe them.
type tailStore struct {
	*memStore
	mu           sync.Mutex
	manifestGets int
	uploads      []string // binlog names, in upload order
	failUploads  func(name string) bool
	onUpload     func(name string)
}

func newTailStore() *tailStore { return &tailStore{memStore: newMemStore()} }

func (s *tailStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if key == manifestKey("binlogs", "iad") {
		s.mu.Lock()
		s.manifestGets++
		s.mu.Unlock()
	}
	return s.memStore.Get(ctx, key)
}

func (s *tailStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if rest, ok := strings.CutPrefix(key, "binlogs/iad/"); ok {
		name := rest[:len("mysql-bin.000000")]
		s.mu.Lock()
		fail, hook := s.failUploads, s.onUpload
		s.mu.Unlock()
		if hook != nil {
			hook(name)
		}
		if fail != nil && fail(name) {
			return fmt.Errorf("upload of %s refused by the test", name)
		}
		s.mu.Lock()
		s.uploads = append(s.uploads, name)
		s.mu.Unlock()
	}
	return s.memStore.Put(ctx, key, r, size)
}

func (s *tailStore) failing(fail func(name string) bool) {
	s.mu.Lock()
	s.failUploads = fail
	s.mu.Unlock()
}

func (s *tailStore) uploaded() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.uploads, ",")
}

func (s *tailStore) manifestReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifestGets
}

func tailArchiver(sim *tailSim, store ArchiveStore) *BinlogArchiver {
	return newTestArchiver(sim.config(), "iad", sim.role, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// tailRows are the names of the site's manifest rows, sorted as stored.
func tailRows(t *testing.T, store ArchiveStore) string {
	t.Helper()
	m, err := loadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, row := range m.Files {
		names = append(names, row.Name)
	}
	return strings.Join(names, ",")
}

// assertTail checks the tail block and the counters a site's health is
// judged by.
func assertTail(t *testing.T, a *BinlogArchiver, state, active string, pending int64) Status {
	t.Helper()
	st := a.Snapshot(context.Background())
	if st.Tail == nil || st.Tail.State != state || st.Tail.ActiveFile != active || st.Tail.PendingFiles != pending {
		t.Fatalf("tail = %+v, want %s with active %s and %d pending (lastError %q)", st.Tail, state, active, pending, st.LastError)
	}
	return st
}

// assertQuiet checks a status reports what a standby with nothing to
// archive always reported: no backlog, no error, no failure counted.
func assertQuiet(t *testing.T, st Status) {
	t.Helper()
	if st.BacklogFiles != 0 || st.LastError != "" || st.UploadFailures != 0 || st.ConsecutiveFailedScans != 0 {
		t.Fatalf("status = backlog %d, lastError %q, uploadFailures %d, failedScans %d; want a quiet standby",
			st.BacklogFiles, st.LastError, st.UploadFailures, st.ConsecutiveFailedScans)
	}
}

const (
	tailA = hintSIDA // the site under test, server_id 101
	tailB = hintSIDB // its peer, server_id 102
)

// writableEra starts a writable site whose first binlog (A:1-3) is sealed
// and archived, with A:4-5 in the active one.
func writableEra(t *testing.T, store ArchiveStore) (*tailSim, *BinlogArchiver) {
	t.Helper()
	sim := newTailSim(t, tailA, 101, false)
	a := tailArchiver(sim, store)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	a.scanAndArchive(context.Background())
	if got := tailRows(t, store); got != "mysql-bin.000001" {
		t.Fatalf("the writable era archived %q", got)
	}
	sim.apply(originTxns(tailA, 101, 4, 5)...)
	return sim, a
}

// A planned demotion with the fence's FLUSH: the file it sealed is the
// writable-era tail and is archived while read-only; the replica copies
// sealed after it are not. A second scan does no work.
func TestTailDrainPlannedDemotionWithFlush(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)

	sim.setReadOnly(true)
	sim.rotate() // the fence's flush seals 000002
	sim.apply(originTxns(tailB, 102, 1, 2)...)
	sim.rotate() // 000003: replica copies only
	sim.apply(originTxns(tailB, 102, 3, 3)...)
	a.scanAndArchive(ctx)

	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("uploads = %q, want the tail 000002 and not the replica copy 000003", got)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0)
	assertQuiet(t, st)
	if st.Tail.NewestSealedFile != "mysql-bin.000003" || st.FilesArchived != 2 || st.LastUploadAt.IsZero() || st.Primary {
		t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Promotions) != 1 || len(m.Files) != 2 || m.Files[1].GTIDCoverage == nil || m.Files[1].GTIDCoverage.GTIDs != tailA+":4-5" {
		t.Fatalf("manifest = %+v", m)
	}

	// Nothing is examined or read from the store twice.
	headers, walks, reads := a.tail.headerReads, a.tail.walks, store.manifestReads()
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("the second scan uploaded: %q", got)
	}
	if a.tail.headerReads != headers || a.tail.walks != walks {
		t.Fatalf("the second scan read %d headers and walked %d files again", a.tail.headerReads-headers, a.tail.walks-walks)
	}
	if quiet := standbyManifestReads(t); store.manifestReads()-reads != quiet {
		t.Fatalf("the second scan read the manifest %d times; a standby with no sealed file reads it %d times", store.manifestReads()-reads, quiet)
	}
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0))

	// More replica copies seal: still nothing to archive.
	sim.rotate()
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("a replica copy was uploaded: %q", got)
	}
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000005", 0))
}

// standbyManifestReads is how often one steady scan of a read-only site
// with no sealed binlog reads its manifest (the history-integrity check and
// the maintenance pass do): the baseline a drain with nothing to do must
// not exceed.
func standbyManifestReads(t *testing.T) int {
	t.Helper()
	store := newTailStore()
	a := tailArchiver(newTailSim(t, tailB, 102, true), store)
	a.scanAndArchive(context.Background())
	before := store.manifestReads()
	a.scanAndArchive(context.Background())
	return store.manifestReads() - before
}

// No flush at the demotion (an emergency failover the old primary came back
// from): its last writable-era transactions stay in the active file, which
// then takes replica copies. Nothing is uploaded until it seals; then the
// mixed file is uploaded whole.
func TestTailDrainMixedFileSealsLater(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)

	sim.setReadOnly(true)
	sim.apply(originTxns(tailB, 102, 1, 2)...)
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001" {
		t.Fatalf("uploads before the mixed file sealed: %q", got)
	}
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000002", 0))

	sim.rotate()
	sim.apply(originTxns(tailB, 102, 3, 3)...)
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("uploads = %q, want the mixed file", got)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Files[1].GTIDCoverage.GTIDs; got != tailA+":4-5,"+tailB+":1-2" {
		t.Fatalf("the mixed file's row covers %q", got)
	}
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0))
}

// A site demoted again before its promotion-time uploads finished holds
// [R0, T1] unarchived: R0 the replica-era file the promotion sealed (no
// origin transaction), T1 the writable-era tail. Both are archived: R0 was
// sealed before T1's first transaction, so the writable era was entitled to
// upload it, and stopping at R0 would lose T1. The replica copies after T1
// are not.
func TestTailDrainReplicaEraFileBeforeTheTail(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	a := tailArchiver(sim, store)
	sim.apply(originTxns(tailB, 102, 1, 5)...)
	a.scanAndArchive(ctx)

	// Promotion: the candidate's flush seals R0, then it is writable. Its
	// uploads fail for the whole short era.
	sim.rotate()
	sim.setReadOnly(false)
	store.failing(func(string) bool { return true })
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.PromotionRecordedAt.IsZero() || !strings.Contains(st.LastError, "mysql-bin.000001") {
		t.Fatalf("the writable era: %+v", st)
	}
	sim.apply(originTxns(tailA, 101, 1, 2)...)

	// Demotion with the fence's flush, then replica copies.
	sim.setReadOnly(true)
	sim.rotate()
	sim.apply(originTxns(tailB, 102, 6, 6)...)
	sim.rotate()
	store.failing(nil)
	a.scanAndArchive(ctx)

	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("uploads = %q, want R0 (000001) and T1 (000002) and not 000003", got)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0)
	if st.BacklogFiles != 0 || st.LastError != "" || st.FilesArchived != 2 {
		t.Fatalf("status = %+v", st)
	}
}

// A reclone in the middle of a drain: the index restarts at 000001 and
// gtid_executed no longer contains the old one. The reset record is
// written, nothing of the new incarnation is uploaded while read-only, and
// its 000001 is not taken for the old incarnation's archived 000001.
func TestTailDrainRecloneMidDrain(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.setReadOnly(true)
	sim.rotate()
	store.failing(func(string) bool { return true })
	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 1)
	if st.BacklogFiles != 1 || !strings.Contains(st.LastError, "mysql-bin.000002") || st.Tail.Reason != st.LastError {
		t.Fatalf("a failing drain: %+v (tail %+v)", st, st.Tail)
	}

	// Recloned from a donor without A:4-5. The new incarnation logs replica
	// copies and, in 000001, a transaction of its own (errant): a file the
	// drain would upload if its records allowed it.
	sim.reclone(tailA + ":1-3," + tailB + ":1-2")
	sim.apply(originTxns(tailB, 102, 3, 3)...)
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.rotate()
	sim.apply(originTxns(tailB, 102, 4, 4)...)
	sim.rotate()
	store.failing(nil)
	a.scanAndArchive(ctx)

	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(m.Promotions); n != 2 || m.Promotions[1].Kind != pitrarchive.RecordReset {
		t.Fatalf("records = %+v, want the promotion record and a reset record", m.Promotions)
	}
	if got := store.uploaded(); got != "mysql-bin.000001" {
		t.Fatalf("uploads = %q: the new incarnation uploaded while read-only", got)
	}
	if len(m.Files) != 1 || m.Files[0].GTIDCoverage.GTIDs != tailA+":1-3" {
		t.Fatalf("rows = %+v, want only the old incarnation's 000001", m.Files)
	}
	st = assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 1)
	assertQuiet(t, Status{BacklogFiles: st.BacklogFiles, LastError: st.LastError})
	if !strings.Contains(st.Tail.Reason, "is not a promotion record") {
		t.Fatalf("reason = %q", st.Tail.Reason)
	}

	// Promoted, the site archives everything under its new record.
	sim.setReadOnly(false)
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("uploads after the promotion = %q", got)
	}
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
}

// A sidecar restart in the middle of a drain: the new process finds the
// rest of the tail from the files and the manifest alone, and uploads
// nothing twice.
func TestTailDrainSidecarRestart(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.rotate() // 000002 sealed while writable, not scanned yet
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.setReadOnly(true)
	sim.rotate() // the fence's flush seals 000003
	sim.apply(originTxns(tailB, 102, 1, 1)...)
	sim.rotate() // 000004: a replica copy

	store.failing(func(name string) bool { return name == "mysql-bin.000003" })
	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000005", 1)
	if st.BacklogFiles != 1 || st.LastError == "" || store.uploaded() != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("the interrupted drain: %+v, uploads %q", st, store.uploaded())
	}

	store.failing(nil)
	restarted := tailArchiver(sim, store)
	if restarted.Snapshot(ctx).Tail != nil {
		t.Fatal("a new process reports a tail before its first scan")
	}
	restarted.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("uploads = %q", got)
	}
	st = assertTail(t, restarted, internalmysql.ArchiverTailDrained, "mysql-bin.000005", 0)
	assertQuiet(t, st)
	if got := tailRows(t, store); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" || st.FilesArchived != 1 {
		t.Fatalf("rows = %q, filesArchived %d", got, st.FilesArchived)
	}
	if m, _ := loadManifest(ctx, store, "binlogs", "iad"); len(m.Promotions) != 1 {
		t.Fatalf("a read-only restart wrote records: %+v", m.Promotions)
	}

	// A further restart with nothing left: one look at the manifest, then
	// none beyond a standby's.
	again := tailArchiver(sim, store)
	again.scanAndArchive(ctx)
	reads := store.manifestReads()
	again.scanAndArchive(ctx)
	if got, quiet := store.manifestReads()-reads, standbyManifestReads(t); got != quiet {
		t.Fatalf("a drained site reads the manifest %d times per scan, a standby %d", got, quiet)
	}
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("a restart uploaded again: %q", got)
	}
	assertQuiet(t, assertTail(t, again, internalmysql.ArchiverTailDrained, "mysql-bin.000005", 0))
}

// A long-time replica holds only replica copies. It uploads nothing, reads
// file headers once and no file body, reads the manifest no more often than
// a standby without binlogs, and reports a drained tail and no error.
func TestTailDrainLongTimeReplica(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	a := tailArchiver(sim, store)
	for gno := int64(1); gno <= 12; gno++ {
		sim.apply(originTxns(tailB, 102, gno, gno)...)
		sim.rotate()
	}
	a.scanAndArchive(ctx)
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000013", 0))
	if a.tail.walks != 0 || a.tail.headerReads != 13 {
		t.Fatalf("examined %d headers and %d bodies; want each header once and no body", a.tail.headerReads, a.tail.walks)
	}

	reads := store.manifestReads()
	a.scanAndArchive(ctx)
	if got, quiet := store.manifestReads()-reads, standbyManifestReads(t); got != quiet {
		t.Fatalf("the replica reads the manifest %d times per scan, a standby without binlogs %d", got, quiet)
	}
	if a.tail.walks != 0 || a.tail.headerReads != 13 {
		t.Fatalf("a steady scan examined files again: %d headers, %d bodies", a.tail.headerReads, a.tail.walks)
	}
	// One more rotation costs two headers: the new sealed file's and the
	// new active one's.
	sim.apply(originTxns(tailB, 102, 13, 13)...)
	sim.rotate()
	a.scanAndArchive(ctx)
	if a.tail.walks != 0 || a.tail.headerReads != 15 {
		t.Fatalf("after a rotation: %d headers, %d bodies", a.tail.headerReads, a.tail.walks)
	}
	if got := store.uploaded(); got != "" || tailRows(t, store) != "" {
		t.Fatalf("a replica uploaded %q", got)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000014", 0)
	assertQuiet(t, st)
	if st.FilesArchived != 0 || st.Tail.NewestSealedFile != "mysql-bin.000013" || st.Tail.Reason != "" {
		t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
	}
}

// readOnlyTail is a read-only site with one sealed, unarchived file that
// holds transactions the server originated, over a store whose manifest
// holds records.
func readOnlyTail(t *testing.T, store ArchiveStore, records ...PromotionRecord) *tailSim {
	t.Helper()
	sim := newTailSim(t, tailA, 101, true)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	if len(records) > 0 {
		if err := seedManifest(context.Background(), store, "binlogs", &Manifest{Site: "iad", Promotions: records}); err != nil {
			t.Fatal(err)
		}
	}
	return sim
}

// Each gate of a read-only upload refuses with a blocked tail and a
// reason, uploads nothing, and (the server and the archive being healthy)
// reports no archiver error.
func TestTailDrainGatesRefuse(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	promotion := PromotionRecord{ServerUUID: tailA, ServerID: 101, GTIDExecuted: "", ObservedAt: at, ProbeInterval: pitrarchive.Duration(5 * time.Second)}
	reset := PromotionRecord{Kind: pitrarchive.RecordReset, ServerUUID: tailA, ServerID: 101, GTIDExecuted: tailA + ":1-2", ObservedAt: at.Add(time.Hour), ProbeInterval: pitrarchive.Duration(5 * time.Second)}
	other := PromotionRecord{ServerUUID: tailB, ServerID: 102, GTIDExecuted: "", ObservedAt: at, ProbeInterval: pitrarchive.Duration(5 * time.Second)}

	for _, tc := range []struct {
		name    string
		records []PromotionRecord
		reason  string
		// resets is how many reset records the scan itself writes: the
		// start-up check turns a record of another incarnation into one.
		resets int
	}{
		{"no record", nil, "holds no promotion record", 0},
		{"the latest record is a reset", []PromotionRecord{promotion, reset}, "is not a promotion record", 0},
		{"a record of another server", []PromotionRecord{other}, "is not a promotion record", 1},
		{"a record the server no longer contains", []PromotionRecord{{ServerUUID: tailA, ServerID: 101, GTIDExecuted: tailA + ":1-9", ObservedAt: at}}, "is not a promotion record", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTailStore()
			sim := readOnlyTail(t, store, tc.records...)
			a := tailArchiver(sim, store)
			for range 2 {
				a.scanAndArchive(ctx)
				st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 1)
				assertQuiet(t, st)
				if !strings.Contains(st.Tail.Reason, tc.reason) {
					t.Fatalf("reason = %q, want %q", st.Tail.Reason, tc.reason)
				}
			}
			if got := store.uploaded(); got != "" {
				t.Fatalf("a refused drain uploaded %q", got)
			}
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			if got := len(resetRecords(m.Promotions)) - len(resetRecords(tc.records)); got != tc.resets || len(m.Files) != 0 {
				t.Fatalf("manifest = %+v, want %d new reset records and no row", m, tc.resets)
			}
		})
	}

	// With the record of this very incarnation the same tail is archived.
	t.Run("the record of this incarnation allows it", func(t *testing.T) {
		store := newTailStore()
		a := tailArchiver(readOnlyTail(t, store, promotion), store)
		a.scanAndArchive(ctx)
		assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000002", 0))
		if got := store.uploaded(); got != "mysql-bin.000001" {
			t.Fatalf("uploads = %q", got)
		}
	})

	t.Run("history integrity marker", func(t *testing.T) {
		store := newTailStore()
		sim := readOnlyTail(t, store, promotion)
		sim.role.violation, sim.role.detail = pitrarchive.ViolationReplicationFilter, "performance_schema.replication_applier_filters has 1 rows"
		a := tailArchiver(sim, store)
		a.scanAndArchive(ctx)
		st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 0)
		if !strings.HasPrefix(st.Tail.Reason, string(pitrarchive.TokenArchiveUnsafeHistory)) || st.Tail.Reason != st.LastError || store.uploaded() != "" {
			t.Fatalf("status = %+v (tail %+v), uploads %q", st, st.Tail, store.uploaded())
		}
		// The marker is permanent: a clean server uploads nothing either.
		sim.role.violation, sim.role.detail = "", ""
		a.scanAndArchive(ctx)
		assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 0)
		if got := store.uploaded(); got != "" {
			t.Fatalf("a marked site uploaded %q", got)
		}
	})

	t.Run("archive session not verified", func(t *testing.T) {
		// No descriptor yet: a read-only site only waits for the writer's.
		store := newTailStore()
		sim := readOnlyTail(t, store)
		a := NewBinlogArchiver(withTestArchive(sim.config(), store), "iad", sim.role, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
		a.scanAndArchive(ctx)
		st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 0)
		assertQuiet(t, st)
		if !strings.Contains(st.Tail.Reason, "archive session is not verified") || len(store.objects) != 0 {
			t.Fatalf("reason = %q, objects %d", st.Tail.Reason, len(store.objects))
		}
	})

	t.Run("no server identity", func(t *testing.T) {
		// A role checker that reports read-only but no identity cannot
		// tell the tail from replica copies.
		store := newTailStore()
		sim := readOnlyTail(t, store, promotion)
		a := newTestArchiver(sim.config(), "iad", roleOnly{sim.role}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
		a.scanAndArchive(ctx)
		st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 0)
		if st.Tail.Reason == "" || store.uploaded() != "" || st.BacklogFiles != 0 {
			t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
		}
	})
}

// roleOnly hides the role probe's identity: IsReadOnly and the
// history-integrity probe only.
type roleOnly struct{ s *stubRoleChecker }

func (r roleOnly) IsReadOnly(ctx context.Context) (bool, error) { return r.s.IsReadOnly(ctx) }
func (r roleOnly) HistoryIntegrityViolation(ctx context.Context) (pitrarchive.HistoryViolation, string, error) {
	return r.s.HistoryIntegrityViolation(ctx)
}

func TestTailRecordGate(t *testing.T) {
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	obs := RoleObservation{ReadOnly: true, ServerUUID: tailA, ServerID: 101, GTIDExecuted: tailA + ":1-9"}
	record := func(change func(*PromotionRecord)) PromotionRecord {
		r := PromotionRecord{ServerUUID: tailA, ServerID: 101, GTIDExecuted: tailA + ":1-5", ObservedAt: at}
		if change != nil {
			change(&r)
		}
		return r
	}
	older := record(func(r *PromotionRecord) { r.ObservedAt = at.Add(-time.Hour) })
	for _, tc := range []struct {
		name    string
		records []PromotionRecord
		refusal string
	}{
		{"this incarnation's promotion record", []PromotionRecord{record(nil)}, ""},
		{"after an older reset record", []PromotionRecord{record(func(r *PromotionRecord) { r.Kind, r.ObservedAt = pitrarchive.RecordReset, at.Add(-time.Hour) }), record(nil)}, ""},
		{"no record", nil, "holds no promotion record"},
		{"the latest record is a reset", []PromotionRecord{older, record(func(r *PromotionRecord) { r.Kind = pitrarchive.RecordReset })}, "is not a promotion record"},
		{"a record kind this release does not know", []PromotionRecord{record(func(r *PromotionRecord) { r.Kind = "Later" })}, "is not a promotion record"},
		{"another server_uuid", []PromotionRecord{older, record(func(r *PromotionRecord) { r.ServerUUID = tailB })}, "not of this server"},
		{"another server_id", []PromotionRecord{record(func(r *PromotionRecord) { r.ServerID = 7 })}, "not of this server"},
		{"a gtid_executed the server no longer contains", []PromotionRecord{record(func(r *PromotionRecord) { r.GTIDExecuted = tailA + ":1-12" })}, "another incarnation"},
		{"an unparsable gtid_executed", []PromotionRecord{record(func(r *PromotionRecord) { r.GTIDExecuted = "?" })}, "another incarnation"},
	} {
		err := tailRecordGate(&Manifest{Promotions: tc.records}, obs)
		switch {
		case tc.refusal == "" && err != nil:
			t.Errorf("%s: refused: %v", tc.name, err)
		case tc.refusal != "" && (err == nil || !strings.Contains(err.Error(), tc.refusal)):
			t.Errorf("%s: %v, want a refusal with %q", tc.name, err, tc.refusal)
		}
	}
}

// The role sampler sees the server writable again while a drain uploads:
// the drain's manifest save is refused, so no row is published under no
// episode. The next scan, of a writable site, records the promotion and
// publishes the row.
func TestTailDrainRefusesSaveWhenRoleChanges(t *testing.T) {
	ctx := context.Background()
	for _, change := range []struct {
		name  string
		apply func(sim *tailSim)
	}{
		{"writable again", func(sim *tailSim) { sim.setReadOnly(false) }},
		{"the role probe fails", func(sim *tailSim) {
			sim.role.mu.Lock()
			sim.role.isReadOnlyErr = fmt.Errorf("connection refused")
			sim.role.mu.Unlock()
		}},
		{"another incarnation", func(sim *tailSim) {
			sim.role.mu.Lock()
			sim.role.gtidExecuted = tailA + ":1-3"
			sim.role.mu.Unlock()
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			store := newTailStore()
			sim, a := writableEra(t, store)
			sim.setReadOnly(true)
			sim.rotate()
			// The sampler probes while the tail's object is being written.
			store.onUpload = func(name string) {
				if name == "mysql-bin.000002" {
					change.apply(sim)
					_, _, _ = a.sampleRole(ctx)
				}
			}
			a.scanAndArchive(ctx)
			if got := tailRows(t, store); got != "mysql-bin.000001" {
				t.Fatalf("rows = %q: the drain published under a changed role", got)
			}
			st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 1)
			if !strings.Contains(st.LastError, errTailEnded.Error()) || st.Tail.Reason != st.LastError {
				t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
			}
		})
	}

	// Writable again: the next scan publishes the row under the new
	// episode's record, without a second upload of the object.
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.setReadOnly(true)
	sim.rotate()
	store.onUpload = func(name string) {
		if name == "mysql-bin.000002" {
			sim.setReadOnly(false)
			_, _, _ = a.sampleRole(ctx)
		}
	}
	a.scanAndArchive(ctx)
	store.onUpload = nil
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Promotions) != 2 || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("records %+v, rows %q", m.Promotions, tailRows(t, store))
	}
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("uploads = %q", got)
	}
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
}

// A sealed binlog replaced between the drain's index read and the upload
// (a reclone restarts the numbering) is not uploaded under the old name.
func TestTailDrainRefusesAReplacedFile(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.rotate() // 000002, unarchived
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.setReadOnly(true)
	sim.rotate() // 000003, unarchived
	// While 000002 is uploaded, 000003 is replaced by other bytes.
	replacement := binlogtest.File{ServerID: 101, Previous: tailA + ":1-5", Txns: originTxns(tailA, 101, 6, 7)}.MustBytes()
	store.onUpload = func(name string) {
		if name == "mysql-bin.000002" {
			path := filepath.Join(sim.dir, "mysql-bin.000003")
			if err := os.Remove(path); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(path, replacement, 0o600); err != nil {
				t.Error(err)
			}
		}
	}
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("uploads = %q: the replaced 000003 was uploaded", got)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000004", 1)
	if !strings.Contains(st.LastError, "was replaced after its index was read") {
		t.Fatalf("lastError = %q", st.LastError)
	}
}

// The tail block: absent until a scan read the index; state and file names
// from one scan; a writable site has to archive every sealed file.
func TestArchiverTailStatus(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, false)
	a := tailArchiver(sim, store)
	if st := a.Snapshot(ctx); st.Tail != nil {
		t.Fatalf("tail before any scan: %+v", st.Tail)
	}

	// A scan that fails before the index and cannot read it reports none.
	index := filepath.Join(sim.dir, "mysql-bin.index")
	if err := os.Rename(index, index+".away"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(index, 0o700); err != nil {
		t.Fatal(err)
	}
	sim.role.isReadOnlyErr = fmt.Errorf("mysql down")
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.Tail != nil || !strings.Contains(st.LastError, "role check") {
		t.Fatalf("tail without an index read: %+v (lastError %q)", st.Tail, st.LastError)
	}
	if err := os.Remove(index); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(index+".away", index); err != nil {
		t.Fatal(err)
	}

	// The same failure with a readable index: blocked, with the names.
	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000001", 0)
	if st.Tail.NewestSealedFile != "" || !strings.Contains(st.Tail.Reason, "role check: ") {
		t.Fatalf("tail = %+v", st.Tail)
	}

	// Writable, one active file: drained.
	sim.role.isReadOnlyErr = nil
	a.scanAndArchive(ctx)
	st = assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000001", 0)
	if st.Tail.NewestSealedFile != "" || st.Tail.Reason != "" {
		t.Fatalf("tail = %+v", st.Tail)
	}

	// Sealed files whose uploads fail: blocked by the file's error.
	sim.apply(originTxns(tailA, 101, 1, 2)...)
	sim.rotate()
	sim.apply(originTxns(tailA, 101, 3, 3)...)
	sim.rotate()
	store.failing(func(string) bool { return true })
	a.scanAndArchive(ctx)
	st = assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 2)
	if st.Tail.NewestSealedFile != "mysql-bin.000002" || st.BacklogFiles != 2 || st.Tail.Reason != st.LastError || st.LastError == "" {
		t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
	}

	// Uploaded: drained.
	store.failing(nil)
	a.scanAndArchive(ctx)
	st = assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
	if st.Tail.NewestSealedFile != "mysql-bin.000002" || st.BacklogFiles != 0 || st.LastError != "" {
		t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
	}

	// What the operator waits for around its FLUSH BINARY LOGS: a scan
	// that saw the rotation (another active file) and ended drained. A
	// scan that fails after the rotation shows the new active file, but
	// never as drained.
	sim.setReadOnly(true)
	a.scanAndArchive(ctx)
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
	sim.apply(originTxns(tailA, 101, 4, 4)...) // committed before the fence
	sim.rotate()
	sim.role.isReadOnlyErr = fmt.Errorf("mysql down")
	a.scanAndArchive(ctx)
	assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000004", 0)
	sim.role.isReadOnlyErr = nil
	a.scanAndArchive(ctx)
	st = assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0)
	if st.Tail.NewestSealedFile != "mysql-bin.000003" || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("tail = %+v, rows %q", st.Tail, tailRows(t, store))
	}

	// A scan that cannot read the index is blocked and names no file: the
	// names of an earlier index read do not belong to this scan's state.
	if err := os.Rename(index, index+".away"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(index, 0o700); err != nil {
		t.Fatal(err)
	}
	a.scanAndArchive(ctx)
	st = assertTail(t, a, internalmysql.ArchiverTailBlocked, "", 0)
	assertQuiet(t, Status{BacklogFiles: st.BacklogFiles, LastError: st.LastError})
	if !strings.Contains(st.Tail.Reason, "read index") || st.Tail.NewestSealedFile != "" {
		t.Fatalf("tail = %+v", st.Tail)
	}
}

// A writable site after a sidecar restart re-verifies its archived files a
// few per scan; until then they count as its backlog and the tail is
// pending.
func TestArchiverTailPendingWhileWritableBacklog(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, false)
	a := tailArchiver(sim, store)
	for gno := int64(1); gno <= 12; gno++ {
		sim.apply(originTxns(tailA, 101, gno, gno)...)
		sim.rotate()
	}
	a.scanAndArchive(ctx)
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000013", 0)

	restarted := tailArchiver(sim, store)
	restarted.scanAndArchive(ctx)
	st := restarted.Snapshot(ctx)
	if st.Tail == nil || st.Tail.State != internalmysql.ArchiverTailPending || st.Tail.PendingFiles == 0 ||
		st.Tail.PendingFiles != st.BacklogFiles || st.LastError != "" || st.Tail.Reason != "" {
		t.Fatalf("status = %+v (tail %+v)", st, st.Tail)
	}
	for range 4 {
		restarted.scanAndArchive(ctx)
	}
	assertTail(t, restarted, internalmysql.ArchiverTailDrained, "mysql-bin.000013", 0)
}

// The files a read-only site cannot examine block the tail, without making
// the replica look failed.
func TestTailDrainUnreadableFilesBlockQuietly(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	a := tailArchiver(sim, store)
	sim.apply(originTxns(tailB, 102, 1, 1)...)
	sealed := sim.rotate()
	// MySQL encrypts the file (binlog_encryption): another magic and no
	// event the sidecar can read.
	path := filepath.Join(sim.dir, sealed)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 0xfd
	for i := 4; i < len(data); i++ {
		data[i] ^= 0x5a
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 0)
	assertQuiet(t, st)
	if !strings.Contains(st.Tail.Reason, sealed) || store.uploaded() != "" {
		t.Fatalf("tail = %+v, uploads %q", st.Tail, store.uploaded())
	}
}

// A file whose transactions under this server's UUID all came from another
// server_id (the header filter passes it) is walked and found to hold no
// origin transaction: it is not uploaded.
func TestTailDrainWalksOnlySuspectFiles(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	a := tailArchiver(sim, store)
	sim.apply(originTxns(tailB, 102, 1, 2)...)
	sim.rotate()
	sim.apply(originTxns(tailA, 7, 1, 2)...) // this server's UUID under another server_id
	sim.rotate()
	a.scanAndArchive(ctx)
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0))
	if a.tail.walks != 1 || store.uploaded() != "" {
		t.Fatalf("walked %d files, uploads %q; want one walk and no upload", a.tail.walks, store.uploaded())
	}
	a.scanAndArchive(ctx)
	if a.tail.walks != 1 {
		t.Fatalf("the verdict was not kept: %d walks", a.tail.walks)
	}
}

// A drain hands scanFiles a prefix of the index. What the scan remembers
// about the sealed files after it (their identities, that they were seen
// sealed) stays, so the next scan of the whole index, after a promotion,
// does not take them for newly sealed and identify each again.
func TestTailDrainKeepsScanMemoryBeyondThePrefix(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, false)
	a := tailArchiver(sim, store)
	sim.apply(originTxns(tailA, 101, 1, 1)...)
	sim.rotate() // 000001 holds the era's only transaction; its upload fails
	sim.rotate() // 000002 and 000003 are empty and archived
	sim.rotate()
	store.failing(func(name string) bool { return name == "mysql-bin.000001" })
	a.scanAndArchive(ctx)
	if got := tailRows(t, store); got != "mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("the writable era archived %q", got)
	}

	sim.setReadOnly(true)
	store.failing(nil)
	a.scanAndArchive(ctx)
	if got := tailRows(t, store); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("rows = %q", got)
	}
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0)
	for _, name := range []string{"mysql-bin.000002", "mysql-bin.000003"} {
		if _, ok := a.localIDs[name]; !ok || a.sealedSnapshot[name] == nil {
			t.Fatalf("the drain of the prefix through 000001 forgot %s", name)
		}
	}
}

// A sealed file with the name, size and Previous_gtids of an archived row
// but other bytes (a name reused after a reset or a restored data
// directory) is not taken for archived: the file that holds the tail's
// last transactions is confirmed by content identity, and uploaded.
func TestTailDrainConfirmsTheTailByContent(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	info, err := os.Stat(filepath.Join(sim.dir, "mysql-bin.000001"))
	if err != nil {
		t.Fatal(err)
	}
	other := strings.Repeat("0123456789abcdef", 4)
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	if err := seedManifest(ctx, store, "binlogs", &Manifest{Site: "iad",
		Promotions: []PromotionRecord{{ServerUUID: tailA, ServerID: 101, ObservedAt: at, ProbeInterval: pitrarchive.Duration(5 * time.Second)}},
		Files: []ManifestEntry{{Name: "mysql-bin.000001", ID: other, Size: info.Size(), ArchivedAt: at,
			GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: tailB + ":1", ContentID: other}}},
	}); err != nil {
		t.Fatal(err)
	}
	a := tailArchiver(sim, store)
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001" {
		t.Fatalf("uploads = %q: the tail passed for the archived row of its name", got)
	}
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000002", 0))
	if got := tailRows(t, store); got != "mysql-bin.000001,mysql-bin.000001" {
		t.Fatalf("rows = %q", got)
	}
}

// The origin walk of a large file is bounded per scan: a scan that runs
// out of its budget reports the tail pending, uploads nothing, and the next
// scans continue the same walk to its verdict.
func TestTailDrainResumesABoundedWalk(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, false)
	a := tailArchiver(sim, store)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	a.scanAndArchive(ctx)
	// The next file cannot be ruled out by its header (it logs this
	// server's UUID), and its only origin transaction lies beyond two read
	// windows of replica copies.
	sim.apply(originTxns(tailB, 102, 1, 900)...)
	sim.apply(originTxns(tailA, 101, 4, 4)...)
	sim.setReadOnly(true)
	sim.rotate()
	a.tail.budget = func() *originBudget { return &originBudget{bytes: 1, deadline: time.Now().Add(time.Minute)} }

	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailPending, "mysql-bin.000003", 0)
	assertQuiet(t, Status{BacklogFiles: st.BacklogFiles, LastError: st.LastError})
	if got := store.uploaded(); got != "mysql-bin.000001" {
		t.Fatalf("uploads before the verdict: %q", got)
	}
	scans := 1
	for a.Snapshot(ctx).Tail.State == internalmysql.ArchiverTailPending {
		if scans++; scans > 20 {
			t.Fatal("the walk never finished")
		}
		a.scanAndArchive(ctx)
	}
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002" || a.tail.walks != 1 || scans < 3 {
		t.Fatalf("uploads %q after %d scans and %d walks; want the mixed file after several scans of one walk", got, scans, a.tail.walks)
	}
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
}

func TestHasSourceUUID(t *testing.T) {
	for _, tc := range []struct {
		set  string
		want bool
	}{
		{"", false},
		{tailB + ":1-9", false},
		{tailA + ":4-5", true},
		{tailB + ":1-9," + tailA + ":7", true},
		// A tagged source identifier of this server (AUTOMATIC:tag).
		{tailB + ":1-9," + tailA + ":fixture:1", true},
		{tailB + ":1-9:fixture:1-3", false},
		{strings.ToUpper(tailA) + ":1", true},
	} {
		set, err := internalmysql.ParseGTIDSet(tc.set)
		if err != nil {
			t.Fatal(err)
		}
		if got := hasSourceUUID(set, tailA); got != tc.want {
			t.Errorf("hasSourceUUID(%q) = %v, want %v", tc.set, got, tc.want)
		}
	}
}

// What a drain remembers is bound to the files, not to their names: after
// a reclone restarts the numbering, a file named like the settled prefix's
// last one does not end the search. Here the donor held everything the
// site's latest record does, so no incarnation reset is observed, and a
// transaction the new incarnation commits itself (errant, on a read-only
// server) is archived under that record, as the site's next promotion
// would archive it.
func TestTailDrainSettledPrefixIsBoundToTheFile(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.setReadOnly(true)
	sim.rotate()
	a.scanAndArchive(ctx)
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)

	sim.reclone(tailA + ":1-5," + tailB + ":1-2")
	sim.apply(originTxns(tailB, 102, 3, 3)...)
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.rotate()
	sim.apply(originTxns(tailB, 102, 4, 4)...)
	sim.rotate()
	a.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000001" {
		t.Fatalf("uploads = %q, want the new incarnation's 000001 and not its 000002", got)
	}
	assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
	if m, _ := loadManifest(ctx, store, "binlogs", "iad"); len(m.Promotions) != 1 || len(m.Files) != 3 {
		t.Fatalf("manifest = %+v", m)
	}
}

// The drain's log lines are in the log schema: an INFO per scan that
// uploaded tail files, and a WARN per standing refusal, logged once.
func TestTailDrainLogs(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	records := func() []map[string]any {
		t.Helper()
		var out []map[string]any
		for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal(line, &rec); err != nil {
				t.Fatalf("log line %q: %v", line, err)
			}
			if msg, _ := rec["msg"].(string); strings.Contains(msg, "binlog tail") {
				out = append(out, rec)
			}
		}
		return out
	}
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	// Two tail files, uploaded in one scan.
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, false)
	a := newTestArchiver(sim.config(), "iad", sim.role, store, logger)
	a.scanAndArchive(ctx)
	sim.apply(originTxns(tailA, 101, 1, 1)...)
	sim.rotate()
	sim.apply(originTxns(tailA, 101, 2, 2)...)
	sim.setReadOnly(true)
	sim.rotate()
	a.scanAndArchive(ctx)
	a.scanAndArchive(ctx)
	got := records()
	if len(got) != 1 || got[0]["msg"] != "archived binlog tail while read-only" || got[0]["level"] != "INFO" ||
		got[0]["count"] != float64(2) || got[0]["through"] != "mysql-bin.000002" || got[0]["site"] != "iad" {
		t.Fatalf("logs = %v", got)
	}

	// A refused drain: one WARN, however many scans repeat the refusal.
	logs.Reset()
	store = newTailStore()
	b := newTestArchiver(readOnlyTail(t, store).config(), "iad", &stubRoleChecker{readOnly: true, serverUUID: tailA, serverID: 101, gtidExecuted: tailA + ":1-3"}, store, logger)
	for range 3 {
		b.scanAndArchive(ctx)
	}
	got = records()
	if len(got) != 1 || got[0]["msg"] != "binlog tail not archived while read-only" || got[0]["level"] != "WARN" ||
		got[0]["pendingFiles"] != float64(1) || !strings.Contains(fmt.Sprint(got[0]["reason"]), "holds no promotion record") {
		t.Fatalf("logs = %v", got)
	}
}

// A writable scan skips a file whose upload fails and goes on to the next,
// so the newest tail file can be archived while an older one is not. The
// drain does not stop at the archived one: every sealed file up to the tail
// has to be in the manifest.
func TestTailDrainFillsAHoleBelowAnArchivedTail(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.rotate() // 000002: its upload fails
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.rotate() // 000003: archived
	store.failing(func(name string) bool { return name == "mysql-bin.000002" })
	a.scanAndArchive(ctx)
	if got := tailRows(t, store); got != "mysql-bin.000001,mysql-bin.000003" {
		t.Fatalf("the writable era archived %q", got)
	}
	store.failing(nil)
	sim.setReadOnly(true)

	// A new process: nothing but the files and the manifest.
	restarted := tailArchiver(sim, store)
	restarted.scanAndArchive(ctx)
	if got := tailRows(t, store); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("rows = %q, want the hole 000002 filled", got)
	}
	assertQuiet(t, assertTail(t, restarted, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0))
}

// auditedRole adds the server state the divergence audit reads
// (binlogStateReader) to the role stub.
type auditedRole struct{ *stubRoleChecker }

func (r auditedRole) BinlogState(context.Context) (string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.serverUUID, r.gtidExecuted, nil
}

// tailC is the server_uuid a clone recipient starts with (#265).
const tailC = "cccccccc-0000-0000-0000-00000000000c"

// setServerUUID gives the simulated server a new server_uuid, as a clone
// does.
func (s *tailSim) setServerUUID(uuid string) {
	s.role.mu.Lock()
	s.role.serverUUID = uuid
	s.role.mu.Unlock()
}

// lostTailSite is a former primary whose tail A:4-5 never reached the new
// primary: the tail is archived read-only before the reclone (the planner
// needs it as evidence), and the site is then recloned from a donor that
// holds A:1-3 and B:1-2. With rotate the clone gives the server a new
// server_uuid (#265); without, it keeps the one that issued the lost
// transactions (a clone by an earlier release). retained is what the
// operator's retention cutoff answers.
func lostTailSite(t *testing.T, rotate bool, retained string) (*tailSim, *BinlogArchiver, *tailStore) {
	t.Helper()
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, false)
	a := newTestArchiver(sim.config(), "iad", auditedRole{sim.role}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if retained != "" {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(pitrCutoffResponse{Coverage: PITRGTIDCoverageProtocol, CutoffTime: "2026-04-17T00:00:00Z", RetainedGTIDs: []string{retained}})
		}))
		t.Cleanup(server.Close)
		a.SetRetentionClient(strings.TrimPrefix(server.URL, "http://"), "ns", "fg", testRetentionArchiveID, time.Hour)
		// The first sweep waits for the reclone.
		a.lastRetentionRun = time.Now()
	}
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	a.scanAndArchive(ctx)
	sim.apply(originTxns(tailA, 101, 4, 5)...) // never replicated
	sim.setReadOnly(true)
	sim.rotate() // the flush before the reclone
	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
	if st.LastError != "" || st.DivergentRows != 0 || st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != "" || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("before the reclone: %+v, rows %q", st, tailRows(t, store))
	}
	assertQuiet(t, st)
	sim.reclone(tailA + ":1-3," + tailB + ":1-2")
	if rotate {
		sim.setServerUUID(tailC)
	}
	return sim, a, store
}

// A tail with transactions that never reached the new primary is archived
// before the reclone. After the reclone the server no longer holds them,
// and it has a new server_uuid, so it cannot issue them again. The site's
// audit records its own archived row as divergent, which the planner reads,
// and that is all: no archiver error, no failure counted, and the site
// keeps uploading and retiring.
func TestTailDrainLostTailIsRecordedAfterReclone(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	sim, a, store := lostTailSite(t, true, tailA+":1-3,"+tailB+":1-2")
	a.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if st.DivergentRows != 1 || st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != "" {
		t.Fatalf("after the reclone: divergentRows %d, reissuableGtids %v", st.DivergentRows, st.ReissuableGTIDs)
	}
	assertQuiet(t, st)
	m, _ := loadManifest(ctx, store, "binlogs", "iad")
	if len(resetRecords(m.Promotions)) != 1 || len(m.Divergent) != 1 || m.Divergent[0].Name != "mysql-bin.000002" || m.Divergent[0].MissingGTIDs != tailA+":4-5" {
		t.Fatalf("manifest = %+v", m)
	}
	if got := strings.Count(logs.String(), `"msg":"archive divergence evidence recorded"`); got != 1 ||
		!strings.Contains(logs.String(), `"rows":1,"missingGtids":"`+tailA+`:4-5","reissuableGtids":""`) {
		t.Fatalf("divergence log records: %d in\n%s", got, logs.String())
	}

	// Later scans change nothing and log nothing more.
	a.scanAndArchive(ctx)
	if got := strings.Count(logs.String(), `"msg":"archive divergence evidence recorded"`); got != 1 {
		t.Fatalf("the unchanged evidence was logged %d times", got)
	}
	assertQuiet(t, a.Snapshot(ctx))

	// The site is promoted again and archives its new era.
	sim.setReadOnly(false)
	sim.apply(originTxns(tailC, 101, 1, 2)...)
	sim.rotate()
	a.scanAndArchive(ctx)
	st = a.Snapshot(ctx)
	if st.LastError != "" || st.BacklogFiles != 0 || st.DivergentRows != 1 || *st.ReissuableGTIDs != "" || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("after the failback: %+v, rows %q", st, tailRows(t, store))
	}
	if m, _ := loadManifest(ctx, store, "binlogs", "iad"); m.Epoch != nil {
		t.Fatalf("a server with a new server_uuid set a reuse guard: %+v", m.Epoch)
	}

	// Retention retires the row every retained backup covers, with the
	// divergent row beside it. No backup holds the lost transactions, so
	// that row stays.
	a.lastRetentionRun = time.Time{}
	a.scanAndArchive(ctx)
	m, _ = loadManifest(ctx, store, "binlogs", "iad")
	if len(m.Pruned) != 1 || m.Pruned[0].Name != "mysql-bin.000001" || !m.Pruned[0].Deleted || len(m.Divergent) != 1 ||
		tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("retention with a divergent row: pruned %+v, divergent %+v, rows %q", m.Pruned, m.Divergent, tailRows(t, store))
	}
	if st := a.Snapshot(ctx); st.LastError != "" || st.UploadFailures != 0 {
		t.Fatalf("after retention: %+v", st)
	}
}

// The same reclone by a release that kept the recipient's server_uuid: the
// server lost transactions it issued under the server_uuid it still has, so
// it issues those GTIDs again as soon as it writes. That is an archiver
// error before the promotion, the reissue is evidence the audit never
// drops, and once the reissued transactions are archived the manifest
// carries the reuse guard.
func TestTailDrainLostTailIsReissuableWhenTheServerKeepsItsUUID(t *testing.T) {
	ctx := context.Background()
	sim, a, store := lostTailSite(t, false, "")
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	lost := tailA + ":4-5"
	if st.DivergentRows != 1 || st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != lost || st.UploadFailures != 1 {
		t.Fatalf("after the reclone: divergentRows %d, reissuableGtids %v, uploadFailures %d", st.DivergentRows, st.ReissuableGTIDs, st.UploadFailures)
	}
	for _, want := range []string{"server_uuid " + tailA, "archived transactions " + lost, "row mysql-bin.000002", "reuses their GTIDs", "PITRArchiveHistoryReset", "Reclone this site before it is promoted", "new archive prefix"} {
		if !strings.Contains(st.LastError, want) {
			t.Fatalf("lastError %q lacks %q", st.LastError, want)
		}
	}
	if _, ok := pitrarchive.ParseToken(st.LastError); ok {
		t.Fatalf("lastError %q starts with a refusal token: the archive is not refused yet", st.LastError)
	}
	if strings.Contains(st.LastError, "pitr-quarantine") {
		t.Fatalf("lastError %q asks for a quarantine, which does not remove the risk", st.LastError)
	}

	// Promoted, it reissues A:4-5. While they are only in the active
	// binlog the server holds those numbers again; the evidence stays.
	sim.setReadOnly(false)
	reissued := originTxns(tailA, 101, 4, 6)
	for i := range reissued {
		reissued[i].Commit = reissued[i].Commit.Add(time.Hour)
	}
	sim.apply(reissued...)
	a.scanAndArchive(ctx)
	st = a.Snapshot(ctx)
	if st.DivergentRows != 1 || *st.ReissuableGTIDs != lost || !strings.Contains(st.LastError, "reuses their GTIDs") {
		t.Fatalf("with the reissue in the active binlog: %+v", st)
	}
	if m, _ := loadManifest(ctx, store, "binlogs", "iad"); m.Epoch != nil {
		t.Fatalf("reuse guard before the reissue is archived: %+v", m.Epoch)
	}

	// Sealed and uploaded, the reissue is proven from the archived bytes.
	sim.rotate()
	a.scanAndArchive(ctx)
	m, _ := loadManifest(ctx, store, "binlogs", "iad")
	if m.Epoch == nil || !strings.Contains(m.Epoch.ResetReason, "verified GTID namespace reuse within "+lost) || len(m.Files) != 3 {
		t.Fatalf("after the reissue was archived: epoch %+v, %d rows", m.Epoch, len(m.Files))
	}
	if st := a.Snapshot(ctx); !strings.Contains(st.LastError, "verified GTID namespace reuse") {
		t.Fatalf("lastError %q, want the reuse guard", st.LastError)
	}
}
