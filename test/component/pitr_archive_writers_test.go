package component

// Archive writers across failover and failback (#206, §3.5-§3.7, §9.2):
// real BinlogArchivers for two sites share one PVC archive, each over a
// simulated mysqld (role, GTID state, binlog files on disk). The tests
// assert what the planner consumes (one descriptor, the promotion records
// of every writable episode, the file layout: mixed files, nothing rotated
// at demotion, the reclone tail gap), and for the failback, reclone and
// lossy layouts plan restores from the writers' own manifests and bytes
// (review F097), so archiver output and planner assumptions cannot drift
// apart behind hand-built planner fixtures.

import (
	"bytes"
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

	"github.com/shipstream/bloodraven/internal/backupmeta"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
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
	// written counts the sealed files already on disk: like mysqld, the
	// simulation writes a sealed file once and never touches it again.
	written int
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
	s.applyLocked(txns)
	s.writeLocked()
}

// rotate seals the active binlog (FLUSH BINARY LOGS).
func (s *simServer) rotate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotateLocked()
	s.writeLocked()
}

func (s *simServer) rotateLocked() {
	s.files = append(s.files, binlogtest.File{ServerID: s.serverID, Created: writerClock, Previous: s.executed.String()})
}

func (s *simServer) applyLocked(txns []binlogtest.Txn) {
	active := &s.files[len(s.files)-1]
	for _, txn := range txns {
		active.Txns = append(active.Txns, txn)
		set, err := internalmysql.ParseGTIDSet(fmt.Sprintf("%s:%d", txn.SID, txn.GNO))
		if err != nil {
			s.t.Fatal(err)
		}
		s.executed = sidecar.UnionBinlogGTIDs(s.executed, set)
	}
}

// promote is a promotion as the server executes it, in one step that no
// role probe can split: the last copies of its relay log (drained), the
// operator's FLUSH BINARY LOGS when rotate (#229), read_only=OFF, and then
// whatever the server executes before its sidecar's next probe: early
// returns those transactions (its first own commits, copies a still-running
// applier adds), stamped on the wall clock while the server is locked, so
// they commit after every read-only probe and before the first writable
// one. With rotate the new file's Previous_gtids are the promotion GTID
// set. The promotion record of the episode then already holds early: the
// lag window (#269).
func (s *simServer) promote(rotate bool, drained []binlogtest.Txn, early func() []binlogtest.Txn) []binlogtest.Txn {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyLocked(drained)
	if rotate {
		s.rotateLocked()
	}
	s.readOnly = false
	var txns []binlogtest.Txn
	if early != nil {
		txns = early()
		s.applyLocked(txns)
	}
	s.writeLocked()
	return txns
}

// nowTxns are origin uuid's transactions from..to committed now, 10 µs
// apart; it returns once the last has committed. Call it where the commits
// happen (simServer.promote's early).
func nowTxns(uuid string, serverID uint32, from, to int64) []binlogtest.Txn {
	start := time.Now().UTC().Truncate(time.Microsecond)
	var out []binlogtest.Txn
	for gno := from; gno <= to; gno++ {
		out = append(out, binlogtest.Txn{SID: uuid, GNO: gno, ServerID: serverID, Commit: start.Add(time.Duration(gno-from) * 10 * time.Microsecond)})
	}
	time.Sleep(time.Until(out[len(out)-1].Commit.Add(time.Microsecond)))
	return out
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
	s.first, s.written = 1, 0
	s.writeLocked()
}

func (s *simServer) writeLocked() {
	var index []string
	for i, f := range s.files {
		name := fmt.Sprintf("mysql-bin.%06d", s.first+i)
		index = append(index, name)
		if i >= s.written {
			writeAtomic(s.t, filepath.Join(s.dir, name), f.MustBytes())
		}
	}
	s.written = len(s.files) - 1
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

// liveTxns are origin uuid's transactions from..to on the wall clock, 1 ms
// apart from 2 ms after the call, which returns once the last has
// committed. The archivers stamp promotion records with the wall clock, so
// a test that plans needs its commits on the same timeline: after the
// record that precedes them and before the next.
func liveTxns(uuid string, serverID uint32, from, to int64) []binlogtest.Txn {
	start := time.Now().UTC().Truncate(time.Microsecond).Add(2 * time.Millisecond)
	var out []binlogtest.Txn
	for gno := from; gno <= to; gno++ {
		out = append(out, binlogtest.Txn{SID: uuid, GNO: gno, ServerID: serverID, Commit: start.Add(time.Duration(gno-from) * time.Millisecond)})
	}
	time.Sleep(time.Until(out[len(out)-1].Commit.Add(time.Millisecond)))
	return out
}

// before is the target 1 µs before txn committed: txn is its witness.
func before(txn binlogtest.Txn) time.Time { return txn.Commit.Add(-time.Microsecond) }

// writerDelta is δ for the plans of these tests: every simulated server
// shares the test's clock.
const writerDelta = time.Millisecond

// writerDump is dump.json of a consistent dump of site's server holding
// executed, finished now. It returns once the wall clock is past both
// dump-end bounds the planner enforces (ceil(End) and serverClock.end +
// 1 s), so a later commit is an admissible target.
func writerDump(t *testing.T, site string, serverID uint32, executed string) backupmeta.DumpMeta {
	t.Helper()
	end := time.Now().UTC().Truncate(time.Microsecond)
	meta := backupmeta.NewMicroTime(end)
	d := backupmeta.DumpMeta{Version: 1, GTIDExecuted: planSet(t, executed).String(), End: meta,
		ServerClock: backupmeta.ServerClock{Start: meta, End: meta}, Stamp: backupmeta.Stamp{Site: site, ServerID: serverID, CreatedAt: meta}}
	time.Sleep(time.Until(end.Add(time.Second + 10*time.Millisecond)))
	return d
}

func planSet(t *testing.T, s string) pitrplan.Set {
	t.Helper()
	set, err := pitrplan.ParseSet(s)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// plan runs the planner over the archive as pitr-download would: every
// site's manifest through PlanSite, every row read from the store,
// indexed and authenticated against its content ID. T zero is latest mode.
func (w *writerArchive) plan(dump backupmeta.DumpMeta, T time.Time, exclude string) (*pitrplan.Plan, error) {
	w.t.Helper()
	return w.planEdited(dump, T, exclude, nil)
}

// planEdited is plan over the archive after edit changed what the planner
// reads from the manifests (nil: nothing).
func (w *writerArchive) planEdited(dump backupmeta.DumpMeta, T time.Time, exclude string, edit func(*pitrplan.Archive)) (*pitrplan.Plan, error) {
	w.t.Helper()
	keys, err := w.store.List(context.Background(), "binlogs/")
	if err != nil {
		w.t.Fatal(err)
	}
	var arch pitrplan.Archive
	for _, key := range keys {
		if site := sidecar.SiteFromManifestKey(key); site != "" {
			sm, err := sidecar.PlanSite(w.manifest(site))
			if err != nil {
				w.t.Fatal(err)
			}
			arch.Sites = append(arch.Sites, sm)
		}
	}
	if edit != nil {
		edit(&arch)
	}
	budget := pitrplan.NewBudget(64 << 20)
	fetch := pitrplan.FetchFunc(func(ctx context.Context, ref pitrplan.RowRef) (*pitrplan.FileIndex, error) {
		key, err := sidecar.ArchiveEntryKey("binlogs", ref.Site, sidecar.ManifestEntry{Name: ref.Name, ID: ref.ID})
		if err != nil {
			return nil, err
		}
		b, ok, err := w.store.Get(ctx, key)
		if err != nil || !ok {
			return nil, fmt.Errorf("get %s: %v %v", key, ok, err)
		}
		idx, err := sidecar.InspectTransactions(bytes.NewReader(b), sidecar.TxIndexOptions{Label: ref.Site + "/" + ref.Name, Budget: budget})
		if err != nil {
			return nil, err
		}
		if !sidecar.MatchesBinlogDigest(w.store, ref.ID, idx.Digest) {
			return nil, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: "digest mismatch"}
		}
		return idx, nil
	})
	mode := pitrplan.ModeTimestamp
	if T.IsZero() {
		mode = pitrplan.ModeLatest
	}
	return pitrplan.Run(context.Background(), pitrplan.Request{Mode: mode, Stop: T, Exclude: planSet(w.t, exclude), Delta: writerDelta,
		Dump: dump, Budget: budget}, arch, fetch)
}

// assertPlan checks a successful plan's lineage Λ, its witness and how Λ
// was established, and that its segments replay exactly Λ ∖ B ∖ X.
func assertPlan(t *testing.T, what string, plan *pitrplan.Plan, err error, lineage, witness string, truth pitrplan.GroundTruth) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if want := planSet(t, lineage); !plan.Lineage.Equal(want) {
		t.Fatalf("%s: lineage %s, want %s", what, plan.Lineage, want)
	}
	if witness != "" && (plan.Witness == nil || plan.Witness.GTID.String() != witness || plan.Witness.GroundTruth != truth) {
		t.Fatalf("%s: witness %+v, want %s (%s)", what, plan.Witness, witness, truth)
	}
	replayed := pitrplan.Set{}
	for _, seg := range plan.Segments {
		if seg.File < 0 || seg.File >= len(plan.Files) || seg.GTIDs.Overlaps(replayed) {
			t.Fatalf("%s: segment %+v of %d files", what, seg, len(plan.Files))
		}
		replayed = replayed.Union(seg.GTIDs)
	}
	if want := plan.Lineage.Subtract(plan.Baseline).Subtract(plan.Excluded); !replayed.Equal(want) || !plan.Replay.Equal(want) {
		t.Fatalf("%s: segments replay %s (plan %s), want %s", what, replayed, plan.Replay, want)
	}
}

// assertRefusal checks err is a planner refusal with token.
func assertRefusal(t *testing.T, what string, err error, token pitrarchive.Token) *pitrplan.Refusal {
	t.Helper()
	ref, ok := pitrplan.RefusalOf(err)
	if !ok || ref.Token != token {
		t.Fatalf("%s: %v, want %s", what, err, token)
	}
	return ref
}

// writerArchive is one shared archive and its sites.
type writerArchive struct {
	t     *testing.T
	ctx   context.Context
	root  string
	store sidecar.ArchiveStore
	dest  pitrarchive.Destination
	wg    sync.WaitGroup
	// confirmDelay, when set, is the archivers' promotion-record
	// confirmation delay (default: the production 2δ, 10 s).
	confirmDelay time.Duration
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
		PromotionConfirmDelay: w.confirmDelay,
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

// waitTailDrained waits for a site's archiver to report what the operator
// waits for after its FLUSH BINARY LOGS: a scan that saw the rotation
// (active is the new active binlog) and found nothing left to archive.
func (w *writerArchive) waitTailDrained(a *sidecar.BinlogArchiver, active string) sidecar.Status {
	w.t.Helper()
	var st sidecar.Status
	w.eventually("a drained tail with active binlog "+active, func() bool {
		st = a.Snapshot(context.Background())
		return st.Tail != nil && st.Tail.ActiveFile == active && st.Tail.State == internalmysql.ArchiverTailDrained
	})
	if st.Tail.PendingFiles != 0 || st.BacklogFiles != 0 || st.LastError != "" {
		w.t.Fatalf("drained tail of %s: %+v (tail %+v)", st.Site, st, st.Tail)
	}
	return st
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
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	dump := writerDump(t, "a", 101, writerUUIDA+":1-5")
	era1b := liveTxns(writerUUIDA, 101, 6, 10)
	srvA.apply(era1b...)
	srvB.apply(era1b...)
	era1 = append(era1, era1b...)

	// Planned failover to b: no rotation at demotion.
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	era2 := liveTxns(writerUUIDB, 102, 1, 3)
	srvB.apply(era2...)
	srvA.apply(era2...)

	// Failback to a, then a's flush seals its mixed file.
	srvB.setReadOnly(true)
	w.waitDemoted(b)
	srvA.setReadOnly(false)
	w.waitRecords("a", 2)
	era3 := liveTxns(writerUUIDA, 101, 11, 15)
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

	// Example 3 (i)-(iii), planned from the writers' manifests and bytes.
	A, B := writerUUIDA, writerUUIDB
	plan, err := w.plan(dump, before(era1[7]), "")
	assertPlan(t, "(i) inside a's first era", plan, err, A+":1-7", A+":8", pitrplan.GroundTruthOrigin)
	plan, err = w.plan(dump, before(era2[1]), "")
	assertPlan(t, "(ii) inside b's era, a replica copy only", plan, err, A+":1-10,"+B+":1", B+":2", pitrplan.GroundTruthAnchored)
	if got := plan.Witness.AnchorRecords; len(got) != 1 || got[0] != "b#0" {
		t.Fatalf("(ii) anchored by %v, want b's record b#0", got)
	}
	plan, err = w.plan(dump, before(era3[1]), "")
	assertPlan(t, "(iii) after the failback", plan, err, A+":1-11,"+B+":1-3", A+":12", pitrplan.GroundTruthOrigin)
}

// Example 4 layout (variant 4g): a1 is archived, then a is recloned from b
// during b's era, which never rotated. a's unarchived mixed file is gone
// (the tail gap, FU-1); after failback a's renumbered binlog a′1 reuses
// a1's name and starts from the clone's state.
func TestArchiveWritersFailbackWithReclone(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	b := w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	early := writerDump(t, "a", 101, writerUUIDA+":1-5")
	srvA.rotate()
	w.eventually("a1 archived", func() bool { return len(w.manifest("a").Files) == 1 })
	era1 = liveTxns(writerUUIDA, 101, 6, 10)
	srvA.apply(era1...)
	srvB.apply(era1...)
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	srvB.apply(liveTxns(writerUUIDB, 102, 1, 3)...)

	srvA.reclone(srvB)
	srvB.setReadOnly(true)
	w.waitDemoted(b)
	srvA.setReadOnly(false)
	w.waitRecords("a", 2)
	late := writerDump(t, "a", 101, writerUUIDA+":1-10,"+writerUUIDB+":1-3")
	era3 := liveTxns(writerUUIDA, 101, 11, 15)
	srvA.apply(era3...)
	srvA.rotate()
	w.eventually("a's post-clone file archived", func() bool { return len(w.manifest("a").Files) == 2 })

	w.assertOneDescriptor()
	ma, mb := w.manifest("a"), w.manifest("b")
	assertRecord(t, ma.Promotions[1], writerUUIDA, 101, writerUUIDA+":1-10,"+writerUUIDB+":1-3")
	if len(mb.Files) != 0 || len(mb.Promotions) != 1 {
		t.Fatalf("b manifest %+v", mb)
	}
	// a′1 reuses a1's binlog name with a new ID.
	a1, row := ma.Files[0], ma.Files[1]
	if a1.PreviousGTIDs != "" {
		a1, row = row, a1
	}
	if a1.Name != "mysql-bin.000001" || a1.GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":1-5") || a1.ID == row.ID {
		t.Fatalf("a1 row = %+v (coverage %+v)", a1, a1.GTIDCoverage)
	}
	if row.Name != "mysql-bin.000001" || row.PreviousGTIDs != gtidSet(t, writerUUIDA+":1-10,"+writerUUIDB+":1-3") ||
		row.GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":11-15") {
		t.Fatalf("post-clone row = %+v (coverage %+v)", row, row.GTIDCoverage)
	}
	if ma.Epoch != nil && ma.Epoch.ResetReason != "" {
		t.Fatalf("a reclone without reissue set a reset guard: %+v", ma.Epoch)
	}

	// Variant 4g, planned from the writers' manifests and bytes: no archived
	// bytes hold A:6-10 or B:1-3, so a backup from before them refuses
	// PITRArchiveGap; a backup taken after the reclone covers a's new era.
	A, B := writerUUIDA, writerUUIDB
	_, err := w.plan(early, before(era3[1]), "")
	ref := assertRefusal(t, "early backup across the unarchived tail", err, pitrarchive.TokenArchiveGap)
	if !strings.Contains(ref.Message, A+":6-10") || !strings.Contains(ref.Message, B+":1-3") {
		t.Fatalf("gap refusal does not name the unarchived tail: %s", ref.Message)
	}
	plan, err := w.plan(late, before(era3[1]), "")
	assertPlan(t, "backup after the reclone", plan, err, A+":1-11,"+B+":1-3", A+":12", pitrplan.GroundTruthOrigin)
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
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	beta1 := writerDump(t, "a", 101, writerUUIDA+":1-5")
	era1 = append(era1, liveTxns(writerUUIDA, 101, 6, 11)...)
	srvA.apply(era1[5:]...)
	srvB.apply(era1[5:10]...)
	srvA.rotate()
	w.eventually("a's file archived", func() bool { return len(w.manifest("a").Files) == 1 })
	srvA.apply(liveTxns(writerUUIDA, 101, 12, 12)...)

	// a dies; b is promoted without A:11-12.
	srvA.setReadOnly(true)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)

	// Latest mode before b archives anything, Example 8 (c): a's file is
	// the only head, and b's real record is the loss evidence for A:11
	// (review F228): refused until X holds exactly the lost A:11.
	A, B := writerUUIDA, writerUUIDB
	plan, err := w.plan(beta1, time.Time{}, "")
	assertRefusal(t, "latest, a's head only", err, pitrarchive.TokenArchiveDivergentHistory)
	if plan == nil || !plan.Lost.Equal(planSet(t, A+":11")) || !plan.Divergent.Equal(planSet(t, A+":11")) {
		t.Fatalf("latest loss refusal reports %+v, want lost and divergent %s:11", plan, A)
	}
	plan, err = w.plan(beta1, time.Time{}, A+":11")
	assertPlan(t, "latest, a's head excluding the lost A:11", plan, err, A+":1-11", "", "")
	if plan.Outcome != pitrplan.OutcomeArchiveHead || !plan.Lost.Equal(planSet(t, A+":11")) {
		t.Fatalf("latest, a's head: outcome %s, lost %s", plan.Outcome, plan.Lost)
	}

	era2 := liveTxns(writerUUIDB, 102, 1, 1)
	srvB.apply(era2...)
	beta2 := writerDump(t, "b", 102, writerUUIDA+":1-10,"+writerUUIDB+":1")
	era2 = append(era2, liveTxns(writerUUIDB, 102, 2, 3)...)
	srvB.apply(era2[1:]...)
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

	// Example 5 with both backups, planned from the writers' manifests and
	// bytes. A:11 is archived (a origin) and lost at b's promotion.
	plan, err = w.plan(beta1, before(era1[10]), "")
	assertPlan(t, "(ii) β1 inside the later-lost tail", plan, err, A+":1-10", A+":11", pitrplan.GroundTruthOrigin)
	plan, err = w.plan(beta1, before(era2[1]), "")
	assertRefusal(t, "(iv) β1 in b's era", err, pitrarchive.TokenArchiveDivergentHistory)
	if plan == nil || !plan.Divergent.Equal(planSet(t, A+":11")) {
		t.Fatalf("(iv) divergent set %+v, want %s:11", plan, A)
	}
	plan, err = w.plan(beta1, before(era2[1]), A+":11")
	assertPlan(t, "(iv) β1 in b's era, excluding the lost A:11", plan, err, A+":1-10,"+B+":1", B+":2", pitrplan.GroundTruthOrigin)
	plan, err = w.plan(beta2, before(era2[2]), "")
	assertPlan(t, "(v) β2 after the loss needs no exclusion", plan, err, A+":1-10,"+B+":1-2", B+":3", pitrplan.GroundTruthOrigin)
	if !plan.Divergent.IsEmpty() {
		t.Fatalf("(v) divergent %s", plan.Divergent)
	}
	_, err = w.plan(beta2, before(era1[4]), "")
	assertRefusal(t, "(vi) β2 before its dump", err, pitrarchive.TokenBackupAfterTarget)

	// Latest mode (Example 8 (d)), the loss evidence from b's real record
	// (review F228): refused until X holds exactly the lost A:11, then b's
	// head with A:11 reported as lost.
	// Latest mode, Example 8 (d): heads a's file and b's file.
	_, err = w.plan(beta1, time.Time{}, "")
	assertRefusal(t, "latest with both heads archived", err, pitrarchive.TokenArchiveSplitBrain)
	plan, err = w.plan(beta1, time.Time{}, A+":11")
	assertPlan(t, "latest excluding the lost A:11", plan, err, A+":1-10,"+B+":1-3", "", "")
	if !plan.Lost.IsEmpty() {
		t.Fatalf("latest on b's head reports loss evidence %s", plan.Lost)
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
	// The scan persists the marker in the manifest and sets LastError only
	// once it returns, so the status can trail the manifest by one step.
	w.eventually("b's status reports the violation", func() bool {
		st := b.Snapshot(context.Background())
		return st.HistoryIntegrity != nil &&
			strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveUnsafeHistory)+": b ReplicationFilter")
	})
	srvA.apply(txns(writerUUIDA, 101, 1, 3, writerClock)...)
	srvA.rotate()
	w.eventually("a keeps archiving", func() bool { return len(w.manifest("a").Files) == 1 })
	if w.manifest("a").HistoryIntegrity != nil {
		t.Fatal("b's violation marked a")
	}
}

// startStoppable runs an archiver like start and returns a function that
// stops it (a sidecar container restart) and waits for it to return.
func (w *writerArchive) startStoppable(site string, srv *simServer, identity sidecar.ArchiveIdentity) func() {
	ctx, cancel := context.WithCancel(w.ctx)
	cfg := &sidecar.PITRConfig{
		BinlogDir: srv.dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs",
		PollInterval: 10 * time.Millisecond, RoleProbeInterval: 10 * time.Millisecond, Archive: identity,
	}
	a := sidecar.NewBinlogArchiver(cfg, site, srv, w.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	w.wg.Add(1)
	go func() { defer w.wg.Done(); defer close(done); a.Run(ctx) }()
	return func() { cancel(); <-done }
}

func resetRecordsOf(m *sidecar.Manifest) []sidecar.PromotionRecord {
	var out []sidecar.PromotionRecord
	for _, r := range m.Promotions {
		if r.Kind == pitrarchive.RecordReset {
			out = append(out, r)
		}
	}
	return out
}

// N3 revised: a lossy failover, then the old primary is recloned from the
// new one while its sidecar runs. Its history loses the unreplicated
// A:9-10 it had, so its sidecar records the reset (read-only), which
// fences a's earlier records off for anything committed after it.
func TestArchiveWritersRecloneRecordsReset(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	w.start("b", srvB, w.store, id)
	w.waitRecords("a", 1)
	srvA.apply(txns(writerUUIDA, 101, 1, 10, writerClock)...)
	srvB.apply(txns(writerUUIDA, 101, 1, 8, writerClock)...)

	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	if got := resetRecordsOf(w.manifest("a")); len(got) != 0 {
		t.Fatalf("a demotion wrote reset records %+v", got)
	}
	srvA.reclone(srvB)
	w.eventually("a's reset record", func() bool { return len(resetRecordsOf(w.manifest("a"))) == 1 })
	ma := w.manifest("a")
	r := resetRecordsOf(ma)[0]
	if r.ServerUUID != writerUUIDA || r.ServerID != 101 || r.GTIDExecuted != gtidSet(t, writerUUIDA+":1-8") ||
		!r.ObservedAt.After(ma.Promotions[0].ObservedAt) || time.Duration(r.ProbeInterval) < 10*time.Millisecond {
		t.Fatalf("reset record = %+v", r)
	}
	if got := resetRecordsOf(w.manifest("b")); len(got) != 0 {
		t.Fatalf("b wrote reset records %+v", got)
	}
}

// N3 revised: a sidecar restart in the middle of a's era writes a new
// promotion record of the same incarnation with the configured probe
// interval, and no reset record. A reclone from a donor that lacks part
// of a's latest record while the sidecar is down is found at start-up:
// the reset record's window reaches back to that record.
func TestArchiveWritersSidecarRestart(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	id := w.identity(writerGroup, w.dest)
	stop := w.startStoppable("a", srvA, id)
	w.waitRecords("a", 1)
	srvA.apply(txns(writerUUIDA, 101, 1, 5, writerClock)...)
	stop()

	stop = w.startStoppable("a", srvA, id)
	w.waitRecords("a", 2)
	ma := w.manifest("a")
	r := ma.Promotions[1]
	if r.Kind != pitrarchive.RecordPromotion || r.GTIDExecuted != gtidSet(t, writerUUIDA+":1-5") ||
		time.Duration(r.ProbeInterval) != 10*time.Millisecond || len(resetRecordsOf(ma)) != 0 {
		t.Fatalf("restart records = %+v", ma.Promotions)
	}
	stop()

	// While the sidecar is down, a is demoted and recloned from a donor
	// that has only A:1-3.
	donor := newSimServer(t, writerUUIDB, 102, true)
	donor.apply(txns(writerUUIDA, 101, 1, 3, writerClock)...)
	srvA.setReadOnly(true)
	srvA.reclone(donor)
	time.Sleep(20 * time.Millisecond)
	w.startStoppable("a", srvA, id)
	w.eventually("a's start-up reset record", func() bool { return len(resetRecordsOf(w.manifest("a"))) == 1 })
	ma = w.manifest("a")
	reset := resetRecordsOf(ma)[0]
	if reset.GTIDExecuted != gtidSet(t, writerUUIDA+":1-3") ||
		time.Duration(reset.ProbeInterval) < reset.ObservedAt.Sub(r.ObservedAt) {
		t.Fatalf("start-up reset record = %+v, latest record before it %+v", reset, r)
	}
}

// #264, a planned handoff: the operator fences a and flushes its binary
// log, which seals a's writable-era tail. a is read-only from then on, and
// its sidecar still archives the tail. Targets inside the tail plan from
// a's own bytes (before, with b's era unrotated, they refused
// PITRArchiveGap). The replica copies a logs afterwards are never uploaded.
func TestArchiveWritersPlannedHandoffArchivesTail(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	dump := writerDump(t, "a", 101, writerUUIDA+":1-5")
	srvA.rotate()
	w.eventually("a1 archived", func() bool { return len(w.manifest("a").Files) == 1 })
	tail := liveTxns(writerUUIDA, 101, 6, 10)
	srvA.apply(tail...)
	srvB.apply(tail...)

	// The handoff: a is fenced, then flushed; b is promoted. The flush
	// comes once a's sidecar has seen the fence, so no scan of a's
	// writable episode can still upload the file it seals.
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvA.rotate()
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	era2 := liveTxns(writerUUIDB, 102, 1, 3)
	srvB.apply(era2...)
	srvA.apply(era2...)

	st := w.waitTailDrained(a, "mysql-bin.000003")
	if st.Primary || st.Tail.NewestSealedFile != "mysql-bin.000002" {
		t.Fatalf("a's status %+v (tail %+v)", st, st.Tail)
	}
	ma := w.manifest("a")
	if len(ma.Files) != 2 || len(ma.Promotions) != 1 {
		t.Fatalf("a manifest: %d rows, records %+v", len(ma.Files), ma.Promotions)
	}
	row := ma.Files[1]
	if row.Name != "mysql-bin.000002" || row.PreviousGTIDs != gtidSet(t, writerUUIDA+":1-5") ||
		row.GTIDCoverage == nil || !row.GTIDCoverage.Complete || row.GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":6-10") {
		t.Fatalf("a's tail row = %+v (coverage %+v)", row, row.GTIDCoverage)
	}
	if mb := w.manifest("b"); len(mb.Files) != 0 {
		t.Fatalf("b's era never rotated, yet b archived %+v", mb.Files)
	}

	// Targets inside the tail, planned from the writers' manifests and bytes.
	A, B := writerUUIDA, writerUUIDB
	plan, err := w.plan(dump, before(tail[2]), "")
	assertPlan(t, "inside a's tail", plan, err, A+":1-7", A+":8", pitrplan.GroundTruthOrigin)
	if plan.Witness.Row.Site != "a" || plan.Witness.Row.Name != "mysql-bin.000002" {
		t.Fatalf("witness row %+v, want a's tail file", plan.Witness.Row)
	}
	plan, err = w.plan(dump, before(tail[4]), "")
	assertPlan(t, "the tail's last transaction", plan, err, A+":1-9", A+":10", pitrplan.GroundTruthOrigin)

	// a's next file holds only replica copies of b's era: sealed, it is
	// not uploaded.
	srvA.rotate()
	w.waitTailDrained(a, "mysql-bin.000004")
	if got := len(w.manifest("a").Files); got != 2 {
		t.Fatalf("a uploaded a replica copy: %d rows", got)
	}

	// b's era, once b seals its file.
	srvB.rotate()
	w.eventually("b's file archived", func() bool { return len(w.manifest("b").Files) == 1 })
	plan, err = w.plan(dump, before(era2[1]), "")
	assertPlan(t, "inside b's era", plan, err, A+":1-10,"+B+":1", B+":2", pitrplan.GroundTruthOrigin)
	w.assertOneDescriptor()
}

// #264, the reclone of TestArchiveWritersFailbackWithReclone with the
// operator's flush before the clone: a's unarchived file (its tail A:6-10,
// then replica copies of b's era) is sealed and archived whole before the
// wipe. The backup from before the tail now reaches a's new era, where it
// refused PITRArchiveGap.
func TestArchiveWritersRecloneArchivesTail(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	b := w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	early := writerDump(t, "a", 101, writerUUIDA+":1-5")
	srvA.rotate()
	w.eventually("a1 archived", func() bool { return len(w.manifest("a").Files) == 1 })
	tail := liveTxns(writerUUIDA, 101, 6, 10)
	srvA.apply(tail...)
	srvB.apply(tail...)
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	era2 := liveTxns(writerUUIDB, 102, 1, 3)
	srvB.apply(era2...)
	srvA.apply(era2...)

	// Before the reclone the operator flushes a and waits for its sidecar.
	srvA.rotate()
	w.waitTailDrained(a, "mysql-bin.000003")
	ma := w.manifest("a")
	if len(ma.Files) != 2 || ma.Files[1].GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":6-10,"+writerUUIDB+":1-3") {
		t.Fatalf("a's tail before the wipe: %+v", ma.Files)
	}

	srvA.reclone(srvB)
	srvB.setReadOnly(true)
	w.waitDemoted(b)
	srvA.setReadOnly(false)
	w.waitRecords("a", 2)
	era3 := liveTxns(writerUUIDA, 101, 11, 15)
	srvA.apply(era3...)
	srvA.rotate()
	w.eventually("a's post-clone file archived", func() bool { return len(w.manifest("a").Files) == 3 })
	if mb := w.manifest("b"); len(mb.Files) != 0 {
		t.Fatalf("b archived %+v", mb.Files)
	}

	A, B := writerUUIDA, writerUUIDB
	plan, err := w.plan(early, before(era3[1]), "")
	assertPlan(t, "early backup across the archived tail", plan, err, A+":1-11,"+B+":1-3", A+":12", pitrplan.GroundTruthOrigin)
	plan, err = w.plan(early, before(tail[2]), "")
	assertPlan(t, "inside the tail", plan, err, A+":1-7", A+":8", pitrplan.GroundTruthOrigin)
	// b's era is archived only as the replica copies in a's tail file.
	plan, err = w.plan(early, before(era2[1]), "")
	assertPlan(t, "inside b's era", plan, err, A+":1-10,"+B+":1", B+":2", pitrplan.GroundTruthAnchored)
}

// #264, a reclone after a lossy failover: a committed A:9-10 that never
// reached b. a comes back fenced, and the operator's flush before the
// reclone seals its tail, which a's sidecar archives with the lost
// transactions in it. The planner reports them as divergent history that a
// restore of b's era must exclude; it is not a gap, and they are not lost
// without a trace.
func TestArchiveWritersRecloneArchivesLostTail(t *testing.T) {
	w := newWriterArchive(t)
	srvA := newSimServer(t, writerUUIDA, 101, false)
	srvB := newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	early := writerDump(t, "a", 101, writerUUIDA+":1-5")
	srvA.rotate()
	w.eventually("a1 archived", func() bool { return len(w.manifest("a").Files) == 1 })
	tail := liveTxns(writerUUIDA, 101, 6, 10)
	srvA.apply(tail...)
	srvB.apply(tail[:3]...)

	// a fails; b is promoted without A:9-10. a comes back read-only.
	srvA.setReadOnly(true)
	w.waitDemoted(a)
	srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	era2 := liveTxns(writerUUIDB, 102, 1, 3)
	srvB.apply(era2...)

	srvA.rotate()
	w.waitTailDrained(a, "mysql-bin.000003")
	ma := w.manifest("a")
	if len(ma.Files) != 2 || ma.Files[1].GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":6-10") {
		t.Fatalf("a's tail before the wipe: %+v", ma.Files)
	}
	srvA.reclone(srvB)
	w.eventually("a's reset record", func() bool { return len(resetRecordsOf(w.manifest("a"))) == 1 })
	srvB.rotate()
	w.eventually("b's file archived", func() bool { return len(w.manifest("b").Files) == 1 })
	if got := len(w.manifest("a").Files); got != 2 {
		t.Fatalf("the recloned a uploaded while read-only: %d rows", got)
	}

	A, B := writerUUIDA, writerUUIDB
	lost := A + ":9-10"
	plan, err := w.plan(early, before(era2[1]), "")
	assertRefusal(t, "b's era across the lost tail", err, pitrarchive.TokenArchiveDivergentHistory)
	if plan == nil || !plan.Divergent.Equal(planSet(t, lost)) {
		t.Fatalf("divergent set %+v, want %s", plan, lost)
	}
	plan, err = w.plan(early, before(era2[1]), lost)
	assertPlan(t, "b's era, excluding the lost tail", plan, err, A+":1-8,"+B+":1", B+":2", pitrplan.GroundTruthOrigin)
	// A target inside the lost tail restores what a had then.
	plan, err = w.plan(early, before(tail[4]), "")
	assertPlan(t, "inside the lost tail", plan, err, A+":1-9", A+":10", pitrplan.GroundTruthOrigin)
	// Latest mode: two heads until the lost transactions are excluded.
	_, err = w.plan(early, time.Time{}, "")
	assertRefusal(t, "latest with a's lost tail archived", err, pitrarchive.TokenArchiveSplitBrain)
	plan, err = w.plan(early, time.Time{}, lost)
	assertPlan(t, "latest excluding the lost tail", plan, err, A+":1-8,"+B+":1-3", "", "")
}

// writerUUIDC is the server_uuid a clone gives its recipient (#265).
const writerUUIDC = "cccccccc-0000-4000-8000-00000000000c"

// auditedServer is a simServer whose sidecar also runs the history audit,
// as every real server's does: it answers the audit's server-state query.
type auditedServer struct{ *simServer }

func (s auditedServer) BinlogState(context.Context) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uuid, s.executed.String(), nil
}

// startAudited is start for a server whose sidecar audits its manifest
// against the server's state.
func (w *writerArchive) startAudited(site string, srv *simServer, identity sidecar.ArchiveIdentity) *sidecar.BinlogArchiver {
	cfg := &sidecar.PITRConfig{
		BinlogDir: srv.dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs",
		PollInterval: 10 * time.Millisecond, RoleProbeInterval: 10 * time.Millisecond, Archive: identity,
	}
	a := sidecar.NewBinlogArchiver(cfg, site, auditedServer{srv}, w.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.wg.Add(1)
	go func() { defer w.wg.Done(); a.Run(w.ctx) }()
	return a
}

// recloneAs is reclone by an operator that rotates the recipient's
// server_uuid: the clone starts as a new server.
func (s *simServer) recloneAs(donor *simServer, uuid string) {
	s.reclone(donor)
	s.mu.Lock()
	s.uuid = uuid
	s.mu.Unlock()
}

// lossyRecloneLayout runs the layout of #271's audit: a archives A:1-5, then
// commits A:6-10 of which only A:6-8 reach b. a fails, b is promoted and
// commits B:1-3 (archived). a comes back read-only; the operator's flush
// seals its tail, which its sidecar archives with the lost A:9-10 in it.
// The caller then reclones a from b and fails back.
type lossyRecloneLayout struct {
	w          *writerArchive
	srvA, srvB *simServer
	a, b       *sidecar.BinlogArchiver
	early      backupmeta.DumpMeta
	tail, era2 []binlogtest.Txn
}

func newLossyRecloneLayout(t *testing.T) *lossyRecloneLayout {
	l := &lossyRecloneLayout{w: newWriterArchive(t)}
	w := l.w
	l.srvA = newSimServer(t, writerUUIDA, 101, false)
	l.srvB = newSimServer(t, writerUUIDB, 102, true)
	id := w.identity(writerGroup, w.dest)
	l.a = w.startAudited("a", l.srvA, id)
	l.b = w.startAudited("b", l.srvB, id)

	w.waitRecords("a", 1)
	era1 := liveTxns(writerUUIDA, 101, 1, 5)
	l.srvA.apply(era1...)
	l.srvB.apply(era1...)
	l.early = writerDump(t, "a", 101, writerUUIDA+":1-5")
	l.srvA.rotate()
	w.eventually("a1 archived", func() bool { return len(w.manifest("a").Files) == 1 })
	l.tail = liveTxns(writerUUIDA, 101, 6, 10)
	l.srvA.apply(l.tail...)
	l.srvB.apply(l.tail[:3]...)

	// a fails; b is promoted without A:9-10 and archives its era.
	l.srvA.setReadOnly(true)
	w.waitDemoted(l.a)
	l.srvB.setReadOnly(false)
	w.waitRecords("b", 1)
	l.era2 = liveTxns(writerUUIDB, 102, 1, 3)
	l.srvB.apply(l.era2...)
	l.srvB.rotate()
	w.eventually("b's file archived", func() bool { return len(w.manifest("b").Files) == 1 })

	// a comes back fenced. It still holds everything it archived, so its
	// audit has nothing to record yet.
	l.srvA.rotate()
	st := w.waitTailDrained(l.a, "mysql-bin.000003")
	if st.DivergentRows != 0 || st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != "" {
		t.Fatalf("a before its reclone: divergentRows %d, reissuableGtids %v", st.DivergentRows, st.ReissuableGTIDs)
	}
	if ma := w.manifest("a"); len(ma.Files) != 2 || ma.Files[1].GTIDCoverage.GTIDs != gtidSet(t, writerUUIDA+":6-10") {
		t.Fatalf("a's tail before the wipe: %+v", ma.Files)
	}
	return l
}

// failback demotes b and promotes the recloned a, whose manifest then holds
// its first promotion record, the reclone's reset record and this one.
func (l *lossyRecloneLayout) failback() {
	l.srvB.setReadOnly(true)
	l.w.waitDemoted(l.b)
	l.srvA.setReadOnly(false)
	l.w.waitRecords("a", 3)
}

// #271, #265: after a lossy failover the old primary's tail is archived and
// the site is recloned as a server with a new server_uuid. Its audit
// records the lost transactions as divergence evidence and reports no
// error; it cannot reissue them, so after the failback it archives again
// and the archive stays usable: a restore across the lost tail refuses
// PITRArchiveDivergentHistory naming exactly the lost set, and plans once
// that set is excluded.
func TestArchiveWritersLossyRecloneWithNewServerUUIDStaysUsable(t *testing.T) {
	l := newLossyRecloneLayout(t)
	w, A, B, C := l.w, writerUUIDA, writerUUIDB, writerUUIDC
	lost := A + ":9-10"

	l.srvA.recloneAs(l.srvB, C)
	w.eventually("a's reset record", func() bool { return len(resetRecordsOf(w.manifest("a"))) == 1 })
	var st sidecar.Status
	w.eventually("a's divergence evidence", func() bool {
		st = l.a.Snapshot(context.Background())
		return st.DivergentRows == 1
	})
	ma := w.manifest("a")
	if len(ma.Divergent) != 1 || ma.Divergent[0].Name != "mysql-bin.000002" || ma.Divergent[0].ID != ma.Files[1].ID || ma.Divergent[0].MissingGTIDs != gtidSet(t, lost) {
		t.Fatalf("a's divergence evidence: %+v (rows %+v)", ma.Divergent, ma.Files)
	}
	// Evidence, not a failure: the status a site's health is judged by is
	// clean on every scan from here on.
	clean := func(what string) {
		t.Helper()
		for i := 0; i < 5; i++ {
			st := l.a.Snapshot(context.Background())
			if st.LastError != "" || st.UploadFailures != 0 || st.ConsecutiveFailedScans != 0 || st.DivergentRows != 1 ||
				st.ReissuableGTIDs == nil || *st.ReissuableGTIDs != "" {
				t.Fatalf("%s: lastError %q, uploadFailures %d, failedScans %d, divergentRows %d, reissuableGtids %v",
					what, st.LastError, st.UploadFailures, st.ConsecutiveFailedScans, st.DivergentRows, st.ReissuableGTIDs)
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	clean("recloned a, read-only")

	l.failback()
	assertRecord(t, w.manifest("a").Promotions[2], C, 101, A+":1-8,"+B+":1-3")
	era3 := liveTxns(C, 101, 1, 3)
	l.srvA.apply(era3...)
	l.srvB.apply(era3...)
	l.srvA.rotate()
	w.eventually("a's new era archived", func() bool { return len(w.manifest("a").Files) == 3 })
	clean("a after the failback")
	for _, site := range []string{"a", "b"} {
		if m := w.manifest(site); m.Epoch != nil {
			t.Fatalf("site %s set a reuse guard: %+v", site, m.Epoch)
		}
	}
	if st := l.b.Snapshot(context.Background()); st.LastError != "" || st.DivergentRows != 0 || *st.ReissuableGTIDs != "" {
		t.Fatalf("b: %+v", st)
	}
	w.assertOneDescriptor()

	// The old backup reaches a's new era across the lost tail only with
	// the lost set excluded, and the refusal names exactly that set.
	plan, err := w.plan(l.early, before(era3[1]), "")
	assertRefusal(t, "a's new era across the lost tail", err, pitrarchive.TokenArchiveDivergentHistory)
	if plan == nil || !plan.Divergent.Equal(planSet(t, lost)) {
		t.Fatalf("divergent set %+v, want %s", plan, lost)
	}
	plan, err = w.plan(l.early, before(era3[1]), lost)
	assertPlan(t, "a's new era, excluding the lost tail", plan, err, A+":1-8,"+B+":1-3,"+C+":1", C+":2", pitrplan.GroundTruthOrigin)
	plan, err = w.plan(l.early, before(l.era2[1]), "")
	assertRefusal(t, "b's era across the lost tail", err, pitrarchive.TokenArchiveDivergentHistory)
	if plan == nil || !plan.Divergent.Equal(planSet(t, lost)) {
		t.Fatalf("divergent set %+v, want %s", plan, lost)
	}
	plan, err = w.plan(l.early, before(l.era2[1]), lost)
	assertPlan(t, "b's era, excluding the lost tail", plan, err, A+":1-8,"+B+":1", B+":2", pitrplan.GroundTruthOrigin)
	// A target inside the lost tail restores what a had then.
	plan, err = w.plan(l.early, before(l.tail[4]), "")
	assertPlan(t, "inside the lost tail", plan, err, A+":1-9", A+":10", pitrplan.GroundTruthOrigin)
	// Latest mode: two heads until the lost transactions are excluded; the
	// refusal names them as one of the branches.
	_, err = w.plan(l.early, time.Time{}, "")
	if ref := assertRefusal(t, "latest with a's lost tail archived", err, pitrarchive.TokenArchiveSplitBrain); !strings.Contains(ref.Message, "a/mysql-bin.000002 holds "+gtidSet(t, lost)) {
		t.Fatalf("split-brain refusal does not name the lost tail: %s", ref.Message)
	}
	plan, err = w.plan(l.early, time.Time{}, lost)
	assertPlan(t, "latest excluding the lost tail", plan, err, A+":1-8,"+B+":1-3,"+C+":1-3", "", "")
	// A backup taken after the reclone needs no exclusion.
	late := writerDump(t, "a", 101, A+":1-8,"+B+":1-3,"+C+":1-3")
	era4 := liveTxns(C, 101, 4, 6)
	l.srvA.apply(era4...)
	l.srvA.rotate()
	w.eventually("a's second new file archived", func() bool { return len(w.manifest("a").Files) == 4 })
	plan, err = w.plan(late, before(era4[1]), "")
	assertPlan(t, "backup after the reclone", plan, err, A+":1-8,"+B+":1-3,"+C+":1-4", C+":5", pitrplan.GroundTruthOrigin)
	if !plan.Divergent.IsEmpty() {
		t.Fatalf("backup after the reclone: divergent %s", plan.Divergent)
	}
	clean("a at the end")
}

// The same layout with a reclone that keeps the recipient's server_uuid (a
// clone by a release before #265, or a data directory restored without a
// clone). The server lost transactions it issued under the server_uuid it
// still has: its sidecar reports that as an archiver error before the
// failback, and after it the server reissues A:9-10 for other transactions.
// Once they are archived, a's manifest carries the reuse guard that every
// reader refuses (PITRArchiveHistoryReset): the archive fails closed.
func TestArchiveWritersLossyRecloneWithSameServerUUIDFailsClosed(t *testing.T) {
	l := newLossyRecloneLayout(t)
	w, A, B := l.w, writerUUIDA, writerUUIDB
	lost := gtidSet(t, A+":9-10")

	l.srvA.reclone(l.srvB)
	w.eventually("a's reset record", func() bool { return len(resetRecordsOf(w.manifest("a"))) == 1 })
	var st sidecar.Status
	w.eventually("a's reissuable divergence", func() bool {
		st = l.a.Snapshot(context.Background())
		return st.ReissuableGTIDs != nil && *st.ReissuableGTIDs == lost && st.LastError != ""
	})
	if st.DivergentRows != 1 || !strings.Contains(st.LastError, "server_uuid "+A) || !strings.Contains(st.LastError, "archived transactions "+lost) ||
		!strings.Contains(st.LastError, "reuses their GTIDs") || !strings.Contains(st.LastError, "Reclone this site before it is promoted") {
		t.Fatalf("recloned a, read-only: divergentRows %d, lastError %q", st.DivergentRows, st.LastError)
	}
	// The planner is unaffected so far: the lost set is excluded as before.
	plan, err := w.plan(l.early, before(l.era2[1]), "")
	assertRefusal(t, "b's era across the lost tail", err, pitrarchive.TokenArchiveDivergentHistory)
	if plan == nil || !plan.Divergent.Equal(planSet(t, lost)) {
		t.Fatalf("divergent set %+v, want %s", plan, lost)
	}
	plan, err = w.plan(l.early, before(l.era2[1]), lost)
	assertPlan(t, "b's era, excluding the lost tail", plan, err, A+":1-8,"+B+":1", B+":2", pitrplan.GroundTruthOrigin)

	// Fail back: a issues A:9-12, reusing the lost A:9-10.
	l.failback()
	assertRecord(t, w.manifest("a").Promotions[2], A, 101, A+":1-8,"+B+":1-3")
	reissued := liveTxns(A, 101, 9, 12)
	l.srvA.apply(reissued...)
	l.srvB.apply(reissued...)
	// In the active binlog only: the server holds those numbers again, and
	// the audit keeps the loss on record rather than taking the archive
	// for whole.
	w.eventually("the evidence survives the numeric return", func() bool {
		st = l.a.Snapshot(context.Background())
		return st.Primary && st.ReissuableGTIDs != nil && *st.ReissuableGTIDs == lost
	})
	if ma := w.manifest("a"); len(ma.Divergent) != 1 || ma.Divergent[0].MissingGTIDs != lost || ma.Epoch != nil || st.LastError == "" {
		t.Fatalf("a with the reissue in its active binlog: divergent %+v, epoch %+v, lastError %q", ma.Divergent, ma.Epoch, st.LastError)
	}

	// Sealed and archived, the reissue is proven from the archived bytes.
	l.srvA.rotate()
	w.eventually("a's reuse guard", func() bool {
		m := w.manifest("a")
		return m.Epoch != nil && m.Epoch.ResetReason != ""
	})
	ma := w.manifest("a")
	if !strings.Contains(ma.Epoch.ResetReason, "verified GTID namespace reuse within "+lost) || len(ma.Epoch.Reuse) != 1 || len(ma.Files) != 3 {
		t.Fatalf("a's reuse guard: %+v, %d rows", ma.Epoch, len(ma.Files))
	}
	w.eventually("a reports the reuse guard", func() bool {
		return strings.Contains(l.a.Snapshot(context.Background()).LastError, "verified GTID namespace reuse")
	})
	if mb := w.manifest("b"); mb.Epoch != nil {
		t.Fatalf("b set a reuse guard: %+v", mb.Epoch)
	}
}

// #269, the promotion-record lag window. b is promoted while its relay log
// still holds A:7-8, commits B:1-2 before its sidecar's next role probe (so
// its promotion record already holds them, with A:7-8) and dies before it
// seals a file of its era: the era is archived only as replica copies in
// a's mixed file. A target whose witness is B:1 or B:2 has no record
// provably before it. With the operator's promotion-time rotation the
// record's promotion set is the promotion GTID set A:1-8, read from the
// file the rotation opened, and it anchors both witnesses by containment;
// before #269 they refused PITRStopAmbiguous. Without the rotation the
// promotion set is only what the sidecar last saw read-only, which lacks
// A:7-8, and the targets still refuse, with the safe target.
func TestArchiveWritersPromotionLagWindow(t *testing.T) {
	A, B := writerUUIDA, writerUUIDB
	for _, rotate := range []bool{true, false} {
		t.Run(fmt.Sprintf("rotation=%v", rotate), func(t *testing.T) {
			t.Parallel()
			w := newWriterArchive(t)
			srvA := newSimServer(t, A, 101, false)
			srvB := newSimServer(t, B, 102, true)
			id := w.identity(writerGroup, w.dest)
			a := w.start("a", srvA, w.store, id)
			b := w.start("b", srvB, w.store, id)

			w.waitRecords("a", 1)
			era1 := liveTxns(A, 101, 1, 5)
			srvA.apply(era1...)
			srvB.apply(era1...)
			dump := writerDump(t, "a", 101, A+":1-5")
			tail := liveTxns(A, 101, 6, 8)
			srvA.apply(tail...)
			srvB.apply(tail[0]) // A:7-8 stay in b's relay log
			// b's sidecar observes it read-only with A:1-6 at most.
			time.Sleep(100 * time.Millisecond)

			srvA.setReadOnly(true)
			w.waitDemoted(a)
			lag := srvB.promote(rotate, tail[1:], func() []binlogtest.Txn { return nowTxns(B, 102, 1, 2) })
			w.waitRecords("b", 1)
			srvA.apply(lag...)
			era2 := liveTxns(B, 102, 3, 5)
			srvB.apply(era2...)
			srvA.apply(era2...)

			rec := w.manifest("b").Promotions[0]
			assertRecord(t, rec, B, 102, A+":1-8,"+B+":1-2")
			promotionSet := planSet(t, rec.PromotionGTIDExecuted)
			if rotate {
				if !promotionSet.Equal(planSet(t, A+":1-8")) {
					t.Fatalf("promotion set %q, want the promotion GTID set %s:1-8", rec.PromotionGTIDExecuted, A)
				}
			} else if rec.PromotionGTIDExecuted == "" || !planSet(t, A+":1-6").Contains(promotionSet) {
				t.Fatalf("promotion set %q, want what b held read-only (within %s:1-6)", rec.PromotionGTIDExecuted, A)
			}

			// b dies before it seals its era; a takes over again and its
			// flush seals the mixed file.
			srvB.setReadOnly(true)
			w.waitDemoted(b)
			srvA.setReadOnly(false)
			w.waitRecords("a", 2)
			era3 := liveTxns(A, 101, 9, 10)
			srvA.apply(era3...)
			srvA.rotate()
			w.eventually("a's mixed file archived", func() bool { return len(w.manifest("a").Files) == 1 })
			for _, row := range w.manifest("b").Files {
				if c := row.GTIDCoverage; c == nil || planSet(t, c.GTIDs).Overlaps(planSet(t, B+":1-5")) {
					t.Fatalf("b archived a file of its own era: %+v", row)
				}
			}

			// Without the #269 evidence (a record of an older sidecar) both
			// targets refuse, whatever the rotation did.
			strip := func(arch *pitrplan.Archive) {
				for i := range arch.Sites {
					for j := range arch.Sites[i].Promotions {
						r := &arch.Sites[i].Promotions[j]
						r.PromotionGTIDExecuted, r.ConfirmedAt, r.ConfirmedGTIDAdded = "", time.Time{}, ""
					}
				}
			}
			for i, want := range []string{A + ":1-8", A + ":1-8," + B + ":1"} {
				witness := fmt.Sprintf("%s:%d", B, i+1)
				_, err := w.planEdited(dump, before(lag[i]), "", strip)
				ref := assertRefusal(t, witness+" without a promotion set", err, pitrarchive.TokenStopAmbiguous)
				if !strings.Contains(ref.Message, "in the window") || !strings.Contains(ref.Message, "it carries no promotion set (promotionGtidExecuted)") {
					t.Fatalf("%s without a promotion set: %s", witness, ref.Message)
				}
				plan, err := w.plan(dump, before(lag[i]), "")
				if !rotate {
					ref := assertRefusal(t, witness+" without the rotation", err, pitrarchive.TokenStopAmbiguous)
					for _, sub := range []string{"or choose a target after", "the promotion record b#0 proves by containment only its promotion set (promotionGtidExecuted), which does not hold " + A + ":7",
						"the promotion-time binlog rotation was not acknowledged"} {
						if !strings.Contains(ref.Message, sub) {
							t.Fatalf("%s without the rotation: %q lacks %q", witness, ref.Message, sub)
						}
					}
					continue
				}
				assertPlan(t, witness+" in the lag window", plan, err, want, witness, pitrplan.GroundTruthAnchored)
				if got := plan.Witness.AnchorRecords; len(got) != 1 || got[0] != "b#0" || plan.Witness.AnchorOriginRow != "" {
					t.Fatalf("%s anchored by %v / %q, want b's record b#0 alone", witness, got, plan.Witness.AnchorOriginRow)
				}
			}
			// After the record and its margin the time rule anchors, with or
			// without the rotation, as before.
			plan, err := w.plan(dump, before(era2[1]), "")
			assertPlan(t, "after the lag window", plan, err, A+":1-8,"+B+":1-3", B+":4", pitrplan.GroundTruthAnchored)
			plan, err = w.plan(dump, time.Time{}, "")
			assertPlan(t, "latest", plan, err, A+":1-10,"+B+":1-5", "", "")
			w.assertOneDescriptor()
		})
	}
}

// #269, a promoted server whose applier is still running. b is promoted
// with the rotation, commits B:1, and only then applies A:8, the last
// transaction of a's era, from its relay log; all of it before its sidecar's
// next probe, so its promotion record holds A:1-8 and B:1. On a, A:8
// precedes B:1. b never seals its era, so B:1 is archived only as a's copy,
// whose predecessors are A:1-8, while b committed B:1 on A:1-7. The record's
// promotion set is A:1-7 (the file the rotation opened), not what the record
// holds: it does not explain A:8, and the target refuses PITRStopAmbiguous
// instead of certifying a history b never had. A planner that anchored with
// the whole record would certify A:1-8.
func TestArchiveWritersPromotionLagWindowLateApplier(t *testing.T) {
	A, B := writerUUIDA, writerUUIDB
	w := newWriterArchive(t)
	srvA := newSimServer(t, A, 101, false)
	srvB := newSimServer(t, B, 102, true)
	id := w.identity(writerGroup, w.dest)
	a := w.start("a", srvA, w.store, id)
	b := w.start("b", srvB, w.store, id)

	w.waitRecords("a", 1)
	era1 := liveTxns(A, 101, 1, 5)
	srvA.apply(era1...)
	srvB.apply(era1...)
	dump := writerDump(t, "a", 101, A+":1-5")
	tail := liveTxns(A, 101, 6, 8)
	srvA.apply(tail...)
	srvB.apply(tail[:2]...)
	time.Sleep(100 * time.Millisecond)

	srvA.setReadOnly(true)
	w.waitDemoted(a)
	early := srvB.promote(true, nil, func() []binlogtest.Txn { return append(nowTxns(B, 102, 1, 1), tail[2]) })
	w.waitRecords("b", 1)
	rec := w.manifest("b").Promotions[0]
	assertRecord(t, rec, B, 102, A+":1-8,"+B+":1")
	if !planSet(t, rec.PromotionGTIDExecuted).Equal(planSet(t, A+":1-7")) {
		t.Fatalf("promotion set %q, want %s:1-7: what b had executed before its first own transaction", rec.PromotionGTIDExecuted, A)
	}
	srvA.apply(early[0])
	era2 := liveTxns(B, 102, 2, 4)
	srvB.apply(era2...)
	srvA.apply(era2...)

	srvB.setReadOnly(true)
	w.waitDemoted(b)
	srvA.setReadOnly(false)
	w.waitRecords("a", 2)
	srvA.apply(liveTxns(A, 101, 9, 10)...)
	srvA.rotate()
	w.eventually("a's mixed file archived", func() bool { return len(w.manifest("a").Files) == 1 })

	_, err := w.plan(dump, before(early[0]), "")
	ref := assertRefusal(t, "B:1, committed before b applied A:8", err, pitrarchive.TokenStopAmbiguous)
	for _, sub := range []string{"predecessors include " + A + ":8", "the promotion record b#0 proves by containment only its promotion set (promotionGtidExecuted), which does not hold " + A + ":8"} {
		if !strings.Contains(ref.Message, sub) {
			t.Fatalf("refusal %q lacks %q", ref.Message, sub)
		}
	}
	// After the record and its margin b had A:8, and the record says so.
	plan, err := w.plan(dump, before(era2[1]), "")
	assertPlan(t, "after the lag window", plan, err, A+":1-8,"+B+":1-2", B+":3", pitrplan.GroundTruthAnchored)
}
