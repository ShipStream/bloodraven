package component

// Archive writers across failover and failback (#206, §3.5-§3.7, §9.2),
// without the planner: real BinlogArchivers for two sites share one PVC
// archive, each over a simulated mysqld (role, GTID state, binlog files on
// disk). The tests assert what the planner later consumes: one descriptor,
// the promotion records of every writable episode, and the file layout
// (mixed files, nothing rotated at demotion, the reclone tail gap).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

const (
	writerUUIDA = "aaaaaaaa-0000-4000-8000-00000000000a"
	writerUUIDB = "bbbbbbbb-0000-4000-8000-00000000000b"
	writerGroup = "00000000-0000-4000-8000-000000000206"
)

var writerClock = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

// simServer is one site's mysqld as the archiver sees it: role, identity,
// gtid_executed, the D-34 settings and the binlog files in its datadir.
type simServer struct {
	t   *testing.T
	dir string

	mu        sync.Mutex
	uuid      string
	serverID  uint32
	readOnly  bool
	executed  internalmysql.GTIDSet
	violation pitrarchive.HistoryViolation
	detail    string
	files     []binlogtest.File // the last one is active
	first     int               // number of files[0]
}

func newSimServer(t *testing.T, uuid string, serverID uint32, readOnly bool) *simServer {
	s := &simServer{t: t, dir: t.TempDir(), uuid: uuid, serverID: serverID, readOnly: readOnly, executed: internalmysql.GTIDSet{}, first: 1}
	s.files = []binlogtest.File{{ServerID: serverID, Created: writerClock}}
	s.writeLocked()
	return s
}

func (s *simServer) IsReadOnly(context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readOnly, nil
}

func (s *simServer) ProbeRole(context.Context) (sidecar.RoleObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sidecar.RoleObservation{ReadOnly: s.readOnly, ServerUUID: s.uuid, ServerID: s.serverID, GTIDExecuted: s.executed.String()}, nil
}

func (s *simServer) HistoryIntegrityViolation(context.Context) (pitrarchive.HistoryViolation, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.violation, s.detail, nil
}

func (s *simServer) setReadOnly(ro bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOnly = ro
}

func (s *simServer) setViolation(v pitrarchive.HistoryViolation, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.violation, s.detail = v, detail
}

// apply writes transactions to the active binlog and gtid_executed: a
// commit on the origin, or the relayed copy on a replica.
func (s *simServer) apply(txns ...binlogtest.Txn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := &s.files[len(s.files)-1]
	for _, txn := range txns {
		active.Txns = append(active.Txns, txn)
		set, err := internalmysql.ParseGTIDSet(fmt.Sprintf("%s:%d", txn.SID, txn.GNO))
		if err != nil {
			s.t.Fatal(err)
		}
		s.executed = sidecar.UnionBinlogGTIDs(s.executed, set)
	}
	s.writeLocked()
}

// rotate seals the active binlog (FLUSH BINARY LOGS).
func (s *simServer) rotate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append(s.files, binlogtest.File{ServerID: s.serverID, Created: writerClock, Previous: s.executed.String()})
	s.writeLocked()
}

// reclone replaces the datadir with a clone of donor: every local binlog is
// gone, gtid_executed is the donor's, and server_uuid is kept (F25).
func (s *simServer) reclone(donor *simServer) {
	donor.mu.Lock()
	executed := sidecar.UnionBinlogGTIDs(donor.executed)
	donor.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, _ := os.ReadDir(s.dir)
	for _, e := range entries {
		_ = os.Remove(filepath.Join(s.dir, e.Name()))
	}
	s.executed = executed
	s.files = []binlogtest.File{{ServerID: s.serverID, Created: writerClock, Previous: executed.String()}}
	s.first = 1
	s.writeLocked()
}

func (s *simServer) writeLocked() {
	var index []string
	for i, f := range s.files {
		name := fmt.Sprintf("mysql-bin.%06d", s.first+i)
		index = append(index, name)
		writeAtomic(s.t, filepath.Join(s.dir, name), f.MustBytes())
	}
	writeAtomic(s.t, filepath.Join(s.dir, "mysql-bin.index"), []byte(strings.Join(index, "\n")+"\n"))
}

func writeAtomic(t *testing.T, path string, data []byte) {
	tmp := path + ".writing"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// txns are n transactions of origin uuid from gno, one second apart.
func txns(uuid string, serverID uint32, from, to int64, start time.Time) []binlogtest.Txn {
	var out []binlogtest.Txn
	for gno := from; gno <= to; gno++ {
		out = append(out, binlogtest.Txn{SID: uuid, GNO: gno, ServerID: serverID, Commit: start.Add(time.Duration(gno) * time.Second)})
	}
	return out
}

// writerArchive is one shared archive and its sites.
type writerArchive struct {
	t     *testing.T
	ctx   context.Context
	root  string
	store sidecar.ArchiveStore
	dest  pitrarchive.Destination
	wg    sync.WaitGroup
}

func newWriterArchive(t *testing.T) *writerArchive {
	ctx, cancel := context.WithCancel(context.Background())
	root := t.TempDir()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: "binlogs",
		PVC: &sidecar.PITRPVCConfig{MountPath: root, MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := pitrarchive.NewPVCDestination("ns", "archive", "")
	if err != nil {
		t.Fatal(err)
	}
	w := &writerArchive{t: t, ctx: ctx, root: root, store: store, dest: dest}
	t.Cleanup(func() { cancel(); w.wg.Wait() })
	return w
}

func (w *writerArchive) identity(uid string, dest pitrarchive.Destination) sidecar.ArchiveIdentity {
	return sidecar.ArchiveIdentity{
		Source:      pitrarchive.Source{Namespace: "ns", Name: "orders", UID: types.UID(uid)},
		Destination: dest,
		Encryption:  pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone},
	}
}

// start runs a real archiver for site over srv until the test ends.
func (w *writerArchive) start(site string, srv *simServer, store sidecar.ArchiveStore, identity sidecar.ArchiveIdentity) *sidecar.BinlogArchiver {
	cfg := &sidecar.PITRConfig{
		BinlogDir: srv.dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs",
		PollInterval: 10 * time.Millisecond, RoleProbeInterval: 10 * time.Millisecond, Archive: identity,
	}
	a := sidecar.NewBinlogArchiver(cfg, site, srv, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.wg.Add(1)
	go func() { defer w.wg.Done(); a.Run(w.ctx) }()
	return a
}

func (w *writerArchive) manifest(site string) *sidecar.Manifest {
	m, err := sidecar.LoadManifest(context.Background(), w.store, "binlogs", site)
	if err != nil {
		w.t.Fatalf("load manifest %s: %v", site, err)
	}
	return m
}

func (w *writerArchive) eventually(what string, cond func() bool) {
	w.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			w.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (w *writerArchive) waitRecords(site string, n int) {
	w.t.Helper()
	w.eventually(fmt.Sprintf("%d promotion records for %s", n, site), func() bool { return len(w.manifest(site).Promotions) >= n })
}

// waitDemoted waits for a site's archiver to observe read-only, which ends
// its writable episode.
func (w *writerArchive) waitDemoted(a *sidecar.BinlogArchiver) {
	w.t.Helper()
	w.eventually("demotion observed", func() bool { return a.Snapshot(context.Background()).PromotionRecordedAt.IsZero() })
}

func (w *writerArchive) descriptor() *pitrarchive.Descriptor {
	w.t.Helper()
	b, ok, err := w.store.Get(context.Background(), pitrarchive.DescriptorKey("binlogs"))
	if err != nil || !ok {
		w.t.Fatalf("descriptor: %v %v", ok, err)
	}
	d, err := pitrarchive.ParseDescriptor(b)
	if err != nil {
		w.t.Fatal(err)
	}
	return d
}

// assertOneDescriptor checks the prefix holds one descriptor, no probe
// leftovers, and that every manifest carries its archive ID.
func (w *writerArchive) assertOneDescriptor() *pitrarchive.Descriptor {
	w.t.Helper()
	keys, err := w.store.List(context.Background(), "binlogs/")
	if err != nil {
		w.t.Fatal(err)
	}
	descriptors := 0
	for _, key := range keys {
		if strings.HasSuffix(key, "/archive.json") {
			descriptors++
		}
		if strings.Contains(key, pitrarchive.CASProbeKeyPrefix) {
			w.t.Fatalf("probe key %s left behind", key)
		}
	}
	if descriptors != 1 {
		w.t.Fatalf("%d descriptors under the prefix: %v", descriptors, keys)
	}
	d := w.descriptor()
	for _, key := range keys {
		if site := sidecar.SiteFromManifestKey(key); site != "" {
			if m := w.manifest(site); m.ArchiveID != d.ArchiveID {
				w.t.Fatalf("manifest %s has archive %q, descriptor %q", site, m.ArchiveID, d.ArchiveID)
			}
		}
	}
	return d
}

func gtidSet(t *testing.T, s string) string {
	t.Helper()
	set, err := internalmysql.ParseGTIDSet(s)
	if err != nil {
		t.Fatal(err)
	}
	return set.String()
}

func assertRecord(t *testing.T, got sidecar.PromotionRecord, uuid string, serverID uint32, executed string) {
	t.Helper()
	if got.ServerUUID != uuid || got.ServerID != serverID || got.GTIDExecuted != gtidSet(t, executed) || got.ObservedAt.IsZero() ||
		got.ObservedAt.Location() != time.UTC || got.ObservedAt.Nanosecond()%1000 != 0 {
		t.Fatalf("record = %+v, want %s/%d with %q at a µs UTC instant", got, uuid, serverID, executed)
	}
}

// Example 3 layout: A→B→A without reclone, a short B era with no B rotation.
// Each site keeps one mixed file; only a's flush after failback uploads it.
func TestArchiveWritersFailbackWithoutReclone(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	b := w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := txns(writerUUIDA, 101, 1, 10, writerClock)
	srvA.apply(era1...)
	srvB.apply(era1...)

	// Planned failover to b: no rotation at demotion.
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	era2 := txns(writerUUIDB, 102, 1, 3, writerClock.Add(time.Minute))
	srvB.apply(era2...)
	srvA.apply(era2...)

	// Failback to a, then a's flush seals its mixed file.
	srvB.setReadOnly(true)
	w.waitDemoted(b)
	srvA.setReadOnly(false)
	w.waitRecords("a", 2)
	era3 := txns(writerUUIDA, 101, 11, 15, writerClock.Add(2*time.Minute))
	srvA.apply(era3...)
	srvA.rotate()
	w.eventually("a's mixed file archived", func() bool { return len(w.manifest("a").Files) == 1 })

	d := w.assertOneDescriptor()
	if d.Source.UID != writerGroup || !pitrarchive.IsDerivedArchiveID(d.ArchiveID, writerGroup, w.dest) || d.CreatedBy != "a" {
		t.Fatalf("descriptor = %+v", d)
	}
	ma, mb := w.manifest("a"), w.manifest("b")
	if len(ma.Promotions) != 2 || len(mb.Promotions) != 1 {
		t.Fatalf("records: a %+v, b %+v", ma.Promotions, mb.Promotions)
	}
	assertRecord(t, ma.Promotions[0], writerUUIDA, 101, "")
	assertRecord(t, mb.Promotions[0], writerUUIDB, 102, writerUUIDA+":1-10")
	assertRecord(t, ma.Promotions[1], writerUUIDA, 101, writerUUIDA+":1-10,"+writerUUIDB+":1-3")
	if !mb.Promotions[0].ObservedAt.After(ma.Promotions[0].ObservedAt) || !ma.Promotions[1].ObservedAt.After(mb.Promotions[0].ObservedAt) {
		t.Fatal("records are not in episode order")
	}
	if len(mb.Files) != 0 {
		t.Fatalf("b's era never rotated, yet b archived %+v", mb.Files)
	}
	row := ma.Files[0]
	c := row.GTIDCoverage
	if row.Name != "mysql-bin.000001" || row.PreviousGTIDs != "" || c == nil || !c.Complete ||
		c.GTIDs != gtidSet(t, writerUUIDA+":1-15,"+writerUUIDB+":1-3") || c.EmptyGTIDs != "" {
		t.Fatalf("mixed file row = %+v (coverage %+v)", row, c)
	}
	if c.OriginalCommitMin != era1[0].Commit.UnixMicro() || c.OriginalCommitMax != era3[len(era3)-1].Commit.UnixMicro() {
		t.Fatalf("commit hints %d..%d", c.OriginalCommitMin, c.OriginalCommitMax)
	}
	key, err := sidecar.ArchiveEntryKey("binlogs", "a", row)
	if err != nil || !strings.HasPrefix(key, "binlogs/a/mysql-bin.000001.") {
		t.Fatalf("object key %q: %v", key, err)
	}
	if _, ok, err := w.store.Get(context.Background(), key); err != nil || !ok {
		t.Fatalf("archived object missing: %v %v", ok, err)
	}
	for _, s := range []*sidecar.BinlogArchiver{a, b} {
		st := s.Snapshot(context.Background())
		if st.Archive == nil || st.Archive.State != pitrarchive.StateVerified || st.Archive.ArchiveID != d.ArchiveID {
			t.Fatalf("%s archive status %+v", st.Site, st.Archive)
		}
	}
}

// Example 4 layout: a is recloned from b during b's era. Its unarchived
// mixed file is gone (the tail gap, FU-1); after failback a's renumbered
// binlog starts from the clone's state.
func TestArchiveWritersFailbackWithReclone(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	b := w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := txns(writerUUIDA, 101, 1, 10, writerClock)
	srvA.apply(era1...)
	srvB.apply(era1...)
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	srvB.apply(txns(writerUUIDB, 102, 1, 3, writerClock.Add(time.Minute))...)

	srvA.reclone(srvB)
	srvB.setReadOnly(true)
	w.waitDemoted(b)
	srvA.setReadOnly(false)
	w.waitRecords("a", 2)
	srvA.apply(txns(writerUUIDA, 101, 11, 15, writerClock.Add(2*time.Minute))...)
	srvA.rotate()
	w.eventually("a's post-clone file archived", func() bool { return len(w.manifest("a").Files) == 1 })

	w.assertOneDescriptor()
	ma, mb := w.manifest("a"), w.manifest("b")
	assertRecord(t, ma.Promotions[1], writerUUIDA, 101, writerUUIDA+":1-10,"+writerUUIDB+":1-3")
	if len(mb.Files) != 0 || len(mb.Promotions) != 1 {
		t.Fatalf("b manifest %+v", mb)
	}
	row := ma.Files[0]
	if row.Name != "mysql-bin.000001" || row.PreviousGTIDs != gtidSet(t, writerUUIDA+":1-10,"+writerUUIDB+":1-3") ||
		row.GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":11-15") {
		t.Fatalf("post-clone row = %+v (coverage %+v)", row, row.GTIDCoverage)
	}
	// No archived bytes hold A:1-10 or B:1-3: the planner reports the gap.
	if ma.Epoch != nil && ma.Epoch.ResetReason != "" {
		t.Fatalf("a reclone without reissue set a reset guard: %+v", ma.Epoch)
	}
}

// Example 5 layout: a lossy failover. a archived A:1-11 and committed A:12;
// b was promoted holding only A:1-10. b's record proves what it lacked.
func TestArchiveWritersLossyFailover(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	w.start("a", srvA, w.store, id)
	w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	srvA.apply(txns(writerUUIDA, 101, 1, 11, writerClock)...)
	srvB.apply(txns(writerUUIDA, 101, 1, 10, writerClock)...)
	srvA.rotate()
	w.eventually("a's file archived", func() bool { return len(w.manifest("a").Files) == 1 })
	srvA.apply(txns(writerUUIDA, 101, 12, 12, writerClock)...)

	// a dies; b is promoted without A:11-12.
	srvA.setReadOnly(true)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	srvB.apply(txns(writerUUIDB, 102, 1, 3, writerClock.Add(time.Minute))...)
	srvB.rotate()
	w.eventually("b's file archived", func() bool { return len(w.manifest("b").Files) == 1 })

	w.assertOneDescriptor()
	ma, mb := w.manifest("a"), w.manifest("b")
	assertRecord(t, mb.Promotions[0], writerUUIDB, 102, writerUUIDA+":1-10")
	if got := ma.Files[0].GTIDCoverage.GTIDs; got != gtidSet(t, writerUUIDA+":1-11") {
		t.Fatalf("a archived %s", got)
	}
	if got := mb.Files[0].GTIDCoverage.GTIDs; got != gtidSet(t, writerUUIDA+":1-10,"+writerUUIDB+":1-3") {
		t.Fatalf("b archived %s", got)
	}
	// A:11 is archived but outside b's record: the loss evidence D-37 uses.
	recorded, _ := internalmysql.ParseGTIDSet(mb.Promotions[0].GTIDExecuted)
	lost, _ := internalmysql.ParseGTIDSet(writerUUIDA + ":11")
	if recorded.Contains(lost) {
		t.Fatal("b's record holds a transaction b never had")
	}
}

// Writers that do not own the descriptor refuse and write nothing:
// another group (Foreign) and the same group at a copied destination (Copy).
func TestArchiveWritersForeignAndCopyRefuse(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	w.start("a", srvA, w.store, w.identity(writerGroup, w.dest))
	w.waitRecords("a", 1)
	before := w.manifest("a")

	foreignSrv := newSimServer(t, writerUUIDB, 102, false)
	foreign := w.start("a", foreignSrv, w.store, w.identity("00000000-0000-4000-8000-0000000000ff", w.dest))
	w.eventually("foreign writer refused", func() bool {
		st := foreign.Snapshot(context.Background())
		return st.Archive != nil && st.Archive.State == pitrarchive.StateRefused
	})
	if st := foreign.Snapshot(context.Background()); st.Archive.Reason != pitrarchive.ReasonArchiveForeign ||
		!strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveForeign)+": ") {
		t.Fatalf("foreign writer = %+v (%+v)", st, st.Archive)
	}

	// A copy of the archive at another claim: same group, different destination.
	copyRoot := t.TempDir()
	if err := os.CopyFS(copyRoot, os.DirFS(w.root)); err != nil {
		t.Fatal(err)
	}
	copyStore, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: "binlogs",
		PVC: &sidecar.PITRPVCConfig{MountPath: copyRoot, MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	copyDest, err := pitrarchive.NewPVCDestination("ns", "archive-copy", "")
	if err != nil {
		t.Fatal(err)
	}
	copySrv := newSimServer(t, writerUUIDA, 101, false)
	copyWriter := w.start("a", copySrv, copyStore, w.identity(writerGroup, copyDest))
	w.eventually("copy writer refused", func() bool {
		st := copyWriter.Snapshot(context.Background())
		return st.Archive != nil && st.Archive.State == pitrarchive.StateRefused
	})
	if st := copyWriter.Snapshot(context.Background()); st.Archive.Reason != pitrarchive.ReasonArchiveCopy {
		t.Fatalf("copy writer = %+v", st.Archive)
	}
	copied, err := sidecar.LoadManifest(context.Background(), copyStore, "binlogs", "a")
	if err != nil || len(copied.Promotions) != len(before.Promotions) {
		t.Fatalf("a refused copy writer changed the copy's manifest: %+v %v", copied, err)
	}
	if after := w.manifest("a"); len(after.Promotions) != 1 {
		t.Fatalf("the foreign writer changed the manifest: %+v", after.Promotions)
	}
}

// Two sites that both believe they are writable on an empty prefix
// establish one descriptor; both verify it.
func TestArchiveWritersConcurrentFirstWriters(t *testing.T) {
	w := newWriterArchive(t)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", newSimServer(t, writerUUIDA, 101, false), w.store, id)
	b := w.start("b", newSimServer(t, writerUUIDB, 102, false), w.store, id)
	w.waitRecords("a", 1)
	w.waitRecords("b", 1)
	d := w.assertOneDescriptor()
	for _, s := range []*sidecar.BinlogArchiver{a, b} {
		if st := s.Snapshot(context.Background()); st.Archive.State != pitrarchive.StateVerified || st.Archive.ArchiveID != d.ArchiveID {
			t.Fatalf("%s: %+v", st.Site, st.Archive)
		}
	}
}

// A replica with a filter marks its own manifest; the primary's archive
// continues and the primary's manifest is untouched.
func TestArchiveWritersReplicaViolationSetsItsMarker(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	w.start("a", srvA, w.store, id)
	b := w.start("b", srvB, w.store, id)
	w.waitRecords("a", 1)
	srvB.setViolation(pitrarchive.ViolationReplicationFilter, "performance_schema.replication_applier_filters has 1 rows")
	w.eventually("b's marker", func() bool { return w.manifest("b").HistoryIntegrity != nil })
	mb := w.manifest("b")
	if mb.HistoryIntegrity.Violation != pitrarchive.ViolationReplicationFilter || len(mb.Promotions) != 0 {
		t.Fatalf("b manifest = %+v", mb)
	}
	if st := b.Snapshot(context.Background()); st.HistoryIntegrity == nil ||
		!strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveUnsafeHistory)+": b ReplicationFilter") {
		t.Fatalf("b status = %+v", st)
	}
	srvA.apply(txns(writerUUIDA, 101, 1, 3, writerClock)...)
	srvA.rotate()
	w.eventually("a keeps archiving", func() bool { return len(w.manifest("a").Files) == 1 })
	if w.manifest("a").HistoryIntegrity != nil {
		t.Fatal("b's violation marked a")
	}
}
