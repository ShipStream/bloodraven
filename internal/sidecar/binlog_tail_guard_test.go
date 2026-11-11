package sidecar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// The tail drain's reporting and its guards, one at a time (the second
// review of #264): what a failing upload reports while it is retried, what
// holds a file as archived, which file an upload is bound to, and the
// conditions that cover for each other in the scenario tests.

// manifestHookStore runs a hook before each read of the site's manifest.
type manifestHookStore struct {
	*tailStore
	onGet func()
}

func (s *manifestHookStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if key == manifestKey("binlogs", "iad") && s.onGet != nil {
		s.onGet()
	}
	return s.tailStore.Get(ctx, key)
}

// hideIndex makes the binlog index unreadable and returns what restores it.
func hideIndex(t *testing.T, sim *tailSim) func() {
	t.Helper()
	index := filepath.Join(sim.dir, "mysql-bin.index")
	if err := os.Rename(index, index+".away"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(index, 0o700); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if err := os.Remove(index); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(index+".away", index); err != nil {
			t.Fatal(err)
		}
	}
}

// spentBudget is an examination budget with nothing left.
func spentBudget() *originBudget { return &originBudget{deadline: time.Now().Add(time.Minute)} }

// A failing tail upload is reported like a writable site's: from the scan
// that failed until a scan settles it, with no gap while the next scan
// retries. The operator polls between scans; a poll that saw no error
// during a retry counted as a recovery, PITRArchiveReady stayed True and
// the backlog alert never fired, while the tail was never archived (review
// F1).
func TestTailDrainUploadErrorStaysReportedDuringTheRetry(t *testing.T) {
	ctx := context.Background()
	for _, readOnly := range []bool{true, false} {
		t.Run(map[bool]string{true: "read-only", false: "writable"}[readOnly], func(t *testing.T) {
			store := newTailStore()
			sim, a := writableEra(t, store)
			sim.setReadOnly(readOnly)
			sim.rotate() // seals 000002
			store.failing(func(name string) bool { return name == "mysql-bin.000002" })
			a.scanAndArchive(ctx)
			failed := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 1)
			if !strings.Contains(failed.LastError, "archive mysql-bin.000002") || failed.BacklogFiles != 1 ||
				failed.ConsecutiveFailedScans != 1 || failed.UploadFailures != 1 || failed.Tail.Reason != failed.LastError {
				t.Fatalf("the failed scan: %+v (tail %+v)", failed, failed.Tail)
			}

			// What a status poll sees while the retry's store call runs.
			var during []Status
			store.mu.Lock()
			store.onUpload = func(string) { during = append(during, a.Snapshot(ctx)) }
			store.mu.Unlock()
			for retry := int64(1); retry <= 2; retry++ {
				a.scanAndArchive(ctx)
				if int64(len(during)) != retry {
					t.Fatalf("retry %d ran %d store calls", retry, len(during))
				}
				mid := during[retry-1]
				if mid.LastError != failed.LastError || mid.BacklogFiles != 1 || mid.ConsecutiveFailedScans != retry ||
					mid.Tail == nil || mid.Tail.State != internalmysql.ArchiverTailBlocked {
					t.Fatalf("during retry %d: lastError %q, backlog %d, failedScans %d, tail %+v; want the failure still reported",
						retry, mid.LastError, mid.BacklogFiles, mid.ConsecutiveFailedScans, mid.Tail)
				}
				after := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 1)
				if after.LastError != failed.LastError || after.BacklogFiles != 1 || after.ConsecutiveFailedScans != retry+1 || after.UploadFailures != retry+1 {
					t.Fatalf("after retry %d: %+v", retry, after)
				}
			}

			// The store recovers: the scan that uploads the file ends the report.
			store.failing(nil)
			store.mu.Lock()
			store.onUpload = nil
			store.mu.Unlock()
			a.scanAndArchive(ctx)
			st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
			if st.LastError != "" || st.BacklogFiles != 0 || st.ConsecutiveFailedScans != 0 || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
				t.Fatalf("after the upload: %+v, rows %q", st, tailRows(t, store))
			}
		})
	}
}

// A read-only scan that cannot decide whether there is a tail (it could not
// read the index or a file, or its examination is unfinished) reports
// nothing of its own. It does not end an upload failure either: the file
// that failed is still not archived, so the failure stays reported until a
// scan settles it, and every such scan counts as failed.
func TestTailDrainUndecidedScanKeepsTheUploadFailure(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		state string
		// undecide makes the next scan unable to decide; it returns what
		// lets the scans decide again.
		undecide func(t *testing.T, sim *tailSim, a *BinlogArchiver, store *manifestHookStore) func()
		reason   string
	}{
		{"the index cannot be read", internalmysql.ArchiverTailBlocked, func(t *testing.T, sim *tailSim, a *BinlogArchiver, _ *manifestHookStore) func() {
			return hideIndex(t, sim)
		}, "read index"},
		{"the examination is unfinished", internalmysql.ArchiverTailPending, func(t *testing.T, sim *tailSim, a *BinlogArchiver, _ *manifestHookStore) func() {
			sim.rotate() // a newly sealed file the scan has no budget to examine
			a.tail.budget = spentBudget
			return func() { a.tail.budget = nil }
		}, ""},
		{"the role moved on before the drain", internalmysql.ArchiverTailBlocked, func(t *testing.T, sim *tailSim, a *BinlogArchiver, store *manifestHookStore) func() {
			// The sampler sees the server writable right after the scan's
			// own probe (the scan's first manifest read comes after it);
			// the server is read-only again for the next scan's probe.
			store.onGet = func() {
				if a.roles.snapshot().state == roleReadOnly {
					sim.setReadOnly(false)
					_, _, _ = a.sampleRole(context.Background())
					sim.setReadOnly(true)
				}
			}
			return func() { store.onGet = nil }
		}, "stopped being read-only during this scan"},
		{"the role moved on during the drain", internalmysql.ArchiverTailBlocked, func(t *testing.T, sim *tailSim, a *BinlogArchiver, _ *manifestHookStore) func() {
			// The same after the drain took the observation it is decided
			// on (the budget is drawn right after).
			a.tail.budget = func() *originBudget {
				sim.setReadOnly(false)
				_, _, _ = a.sampleRole(context.Background())
				sim.setReadOnly(true)
				return newOriginBudget()
			}
			return func() { a.tail.budget = nil }
		}, errTailEnded.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &manifestHookStore{tailStore: newTailStore()}
			sim, a := writableEra(t, store)
			sim.setReadOnly(true)
			sim.rotate() // seals 000002, the tail
			store.failing(func(name string) bool { return name == "mysql-bin.000002" })
			a.scanAndArchive(ctx)
			failed := a.Snapshot(ctx)
			if !strings.Contains(failed.LastError, "archive mysql-bin.000002") || failed.BacklogFiles != 1 {
				t.Fatalf("the failed scan: %+v", failed)
			}

			decide := tc.undecide(t, sim, a, store)
			for scan := int64(2); scan <= 3; scan++ {
				a.scanAndArchive(ctx)
				st := a.Snapshot(ctx)
				if st.LastError != failed.LastError || st.BacklogFiles != 1 || st.ConsecutiveFailedScans != scan {
					t.Fatalf("scan %d: lastError %q, backlog %d, failedScans %d; want the upload failure still reported",
						scan, st.LastError, st.BacklogFiles, st.ConsecutiveFailedScans)
				}
				// The scan itself failed no upload and reports its own reason
				// in the tail block only.
				if st.UploadFailures != 1 || st.Tail == nil || st.Tail.State != tc.state || !strings.Contains(st.Tail.Reason, tc.reason) ||
					(tc.reason != "" && st.Tail.Reason == st.LastError) {
					t.Fatalf("scan %d: uploadFailures %d, tail %+v", scan, st.UploadFailures, st.Tail)
				}
			}
			if got := store.uploaded(); got != "mysql-bin.000001" {
				t.Fatalf("uploads = %q", got)
			}

			decide()
			store.failing(nil)
			a.scanAndArchive(ctx)
			st := a.Snapshot(ctx)
			if st.LastError != "" || st.BacklogFiles != 0 || st.ConsecutiveFailedScans != 0 || st.Tail.State != internalmysql.ArchiverTailDrained ||
				tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
				t.Fatalf("after the upload: %+v (tail %+v), rows %q", st, st.Tail, tailRows(t, store))
			}
		})
	}

	// What stays is the upload failure, not whatever error a scan in
	// between left: a failed role probe is over once a probe succeeds.
	t.Run("an error of another scan does not take its place", func(t *testing.T) {
		store := newTailStore()
		sim, a := writableEra(t, store)
		sim.setReadOnly(true)
		sim.rotate()
		store.failing(func(name string) bool { return name == "mysql-bin.000002" })
		a.scanAndArchive(ctx)
		failed := a.Snapshot(ctx)

		sim.role.mu.Lock()
		sim.role.isReadOnlyErr = fmt.Errorf("mysql down")
		sim.role.mu.Unlock()
		a.scanAndArchive(ctx)
		if st := a.Snapshot(ctx); !strings.Contains(st.LastError, "role check") {
			t.Fatalf("lastError %q, want the role check's", st.LastError)
		}
		sim.role.mu.Lock()
		sim.role.isReadOnlyErr = nil
		sim.role.mu.Unlock()
		restore := hideIndex(t, sim)
		a.scanAndArchive(ctx)
		if st := a.Snapshot(ctx); st.LastError != failed.LastError || st.BacklogFiles != 1 {
			t.Fatalf("lastError %q, backlog %d; want the upload failure", st.LastError, st.BacklogFiles)
		}
		restore()
	})

	// A failure that a scan settled is over: here the site was recloned, so
	// its records no longer allow an upload (a refusal, by design no
	// error), and a later scan that cannot decide has nothing to report.
	t.Run("a settled failure does not return", func(t *testing.T) {
		store := newTailStore()
		sim, a := writableEra(t, store)
		sim.setReadOnly(true)
		sim.rotate()
		store.failing(func(name string) bool { return name == "mysql-bin.000002" })
		a.scanAndArchive(ctx)
		if st := a.Snapshot(ctx); !strings.Contains(st.LastError, "archive mysql-bin.000002") {
			t.Fatalf("the failed scan: %+v", st)
		}
		sim.reclone(tailA + ":1-3," + tailB + ":1-2")
		sim.apply(originTxns(tailA, 101, 6, 6)...)
		sim.rotate()
		a.scanAndArchive(ctx)
		st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 1)
		if st.LastError != "" || st.BacklogFiles != 0 || !strings.Contains(st.Tail.Reason, "is not a promotion record") {
			t.Fatalf("after the reclone: %+v (tail %+v)", st, st.Tail)
		}
		restore := hideIndex(t, sim)
		a.scanAndArchive(ctx)
		if st := a.Snapshot(ctx); st.LastError != "" || st.BacklogFiles != 0 || st.ConsecutiveFailedScans != 0 {
			t.Fatalf("an undecided scan after the refusal: lastError %q, backlog %d, failedScans %d", st.LastError, st.BacklogFiles, st.ConsecutiveFailedScans)
		}
		restore()
	})

	// Without an upload failure such a scan clears what earlier scans
	// reported, as a standby always did.
	t.Run("nothing stands without an upload failure", func(t *testing.T) {
		store := newTailStore()
		sim, a := writableEra(t, store)
		sim.setReadOnly(true)
		sim.rotate()
		sim.role.mu.Lock()
		sim.role.isReadOnlyErr = fmt.Errorf("mysql down")
		sim.role.mu.Unlock()
		a.scanAndArchive(ctx)
		if st := a.Snapshot(ctx); !strings.Contains(st.LastError, "role check") || st.ConsecutiveFailedScans != 1 {
			t.Fatalf("the failed probe: %+v", st)
		}
		sim.role.mu.Lock()
		sim.role.isReadOnlyErr = nil
		sim.role.mu.Unlock()
		restore := hideIndex(t, sim)
		a.scanAndArchive(ctx)
		st := a.Snapshot(ctx)
		if st.LastError != "" || st.BacklogFiles != 0 || st.ConsecutiveFailedScans != 0 || st.Tail.State != internalmysql.ArchiverTailBlocked {
			t.Fatalf("after the probe recovered: %+v (tail %+v)", st, st.Tail)
		}
		restore()
	})

	// A writable site's failing upload is the same failure once the site
	// is demoted: the first read-only scans report it until one decides.
	t.Run("a failure of the writable era", func(t *testing.T) {
		store := newTailStore()
		sim, a := writableEra(t, store)
		sim.rotate() // seals 000002 while writable
		store.failing(func(name string) bool { return name == "mysql-bin.000002" })
		a.scanAndArchive(ctx)
		failed := a.Snapshot(ctx)
		if !strings.Contains(failed.LastError, "archive mysql-bin.000002") || failed.BacklogFiles != 1 {
			t.Fatalf("the writable scan: %+v", failed)
		}
		sim.setReadOnly(true)
		sim.rotate() // the fence's flush seals an empty 000003
		a.tail.budget = spentBudget
		a.scanAndArchive(ctx)
		st := assertTail(t, a, internalmysql.ArchiverTailPending, "mysql-bin.000004", 0)
		if st.LastError != failed.LastError || st.BacklogFiles != 1 || st.ConsecutiveFailedScans != 2 {
			t.Fatalf("the first read-only scan: %+v", st)
		}
		a.tail.budget = nil
		store.failing(nil)
		a.scanAndArchive(ctx)
		st = assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0)
		if st.LastError != "" || st.BacklogFiles != 0 || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
			t.Fatalf("after the drain: %+v, rows %q", st, tailRows(t, store))
		}
	})
}

// retireRow moves the site's row of name to a retirement record, as a
// retention sweep does while the file is still local.
func retireRow(t *testing.T, a *BinlogArchiver, name string) {
	t.Helper()
	ctx := context.Background()
	m, err := a.loadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kept := m.Files[:0]
	found := false
	for _, row := range m.Files {
		if row.Name != name {
			kept = append(kept, row)
			continue
		}
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		m.Pruned = append(m.Pruned, PrunedEntry{Name: row.Name, ID: row.ID, Key: key, PreviousGTIDs: row.PreviousGTIDs, FirstEventTime: row.FirstEventTime, Deleted: true})
		found = true
	}
	if !found {
		t.Fatalf("no row %s to retire", name)
	}
	m.Files = kept
	if err := a.saveManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
}

// recloneWithAHole is a site that archived mysql-bin.000001, was recloned
// (the numbering restarts, with a new server_uuid) and promoted, and was
// demoted again with its new 000002 archived and its new 000001 not (the
// upload failed once). The new 000001 holds two transactions and the old
// one three, so the files differ in size; both have empty Previous_gtids.
// retire: retention retired the old 000001's row while the file was local.
func recloneWithAHole(t *testing.T, retire bool) (*tailSim, *BinlogArchiver, *tailStore) {
	t.Helper()
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store) // 000001 (A:1-3) archived, A:4-5 active
	if retire {
		retireRow(t, a, "mysql-bin.000001")
	}
	sim.setReadOnly(true)
	a.scanAndArchive(ctx)
	sim.reclone(tailA + ":1-5")
	sim.setServerUUID(tailC)
	a.scanAndArchive(ctx)
	sim.setReadOnly(false)
	sim.apply(originTxns(tailC, 101, 1, 2)...)
	sim.rotate() // the new 000001: C:1-2
	sim.apply(originTxns(tailC, 101, 3, 3)...)
	sim.rotate() // the new 000002: C:3
	store.failing(func(name string) bool { return name == "mysql-bin.000001" })
	a.scanAndArchive(ctx)
	store.failing(nil)
	want := "mysql-bin.000001,mysql-bin.000002"
	if retire {
		want = "mysql-bin.000002"
	}
	if got := tailRows(t, store); got != want || !strings.Contains(a.Snapshot(ctx).LastError, "archive mysql-bin.000001") {
		t.Fatalf("the new writable era: rows %q, lastError %q", got, a.Snapshot(ctx).LastError)
	}
	sim.setReadOnly(true) // demoted before the retry
	return sim, a, store
}

// A retirement record keeps a file's name and Previous_gtids, and
// compaction keeps the record for as long as a local file of that name
// exists. After a reclone the first file of the new incarnation has the old
// first file's name and, like it, empty Previous_gtids. The old record must
// not hold the new file: it is confirmed by content, the hole below the
// archived tail is filled, and the tail is not reported drained before
// (review F3).
func TestTailDrainRetirementRecordOfAnEarlierIncarnation(t *testing.T) {
	ctx := context.Background()
	for _, process := range []string{"the same process", "a new process"} {
		t.Run(process, func(t *testing.T) {
			sim, a, store := recloneWithAHole(t, true)
			if process == "a new process" {
				a = tailArchiver(sim, store)
			}
			a.scanAndArchive(ctx)
			if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000001" {
				t.Fatalf("uploads = %q, want the new incarnation's 000001 uploaded by the drain", got)
			}
			st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
			if st.LastError != "" || st.BacklogFiles != 0 || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
				t.Fatalf("after the drain: %+v, rows %q", st, tailRows(t, store))
			}
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			if got := m.Files[0].GTIDCoverage.GTIDs; got != tailC+":1-2" || len(m.Pruned) != 1 {
				t.Fatalf("the new 000001's row covers %q, retirement records %+v", got, m.Pruned)
			}
		})
	}
}

// A retired file that is still local is the retirement record's own file:
// it is not uploaded again, and it is hashed at most once per process, not
// at all when the scan identified it already, and within the scan's budget.
func TestTailDrainRetirementRecordHoldsItsOwnFile(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.rotate() // 000002: A:4-5
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.rotate() // 000003: A:6, the tail
	a.scanAndArchive(ctx)
	retireRow(t, a, "mysql-bin.000001")
	retireRow(t, a, "mysql-bin.000002")
	sim.setReadOnly(true)
	uploads := store.uploaded()

	check := func(a *BinlogArchiver, hashes int) {
		t.Helper()
		assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0))
		if got := store.uploaded(); got != uploads || a.tail.hashes != hashes {
			t.Fatalf("uploads %q (were %q), %d files hashed, want %d", got, uploads, a.tail.hashes, hashes)
		}
	}
	// The process that archived the files knows their identities: nothing
	// is hashed, and nothing waits for the budget a hash would need.
	a.tail.budget = func() *originBudget { return &originBudget{bytes: 1, deadline: time.Now().Add(time.Minute)} }
	a.scanAndArchive(ctx)
	check(a, 0)

	// A new process hashes each once: the tail's last file and the two a
	// retirement record holds.
	b := tailArchiver(sim, store)
	b.scanAndArchive(ctx)
	check(b, 3)
	b.scanAndArchive(ctx)
	check(b, 3)

	// Within the budget: one retired file per scan here, and the tail is
	// pending, with nothing reported, until the last is confirmed.
	c := tailArchiver(sim, store)
	c.tail.budget = func() *originBudget { return &originBudget{bytes: 1, deadline: time.Now().Add(time.Minute)} }
	c.scanAndArchive(ctx)
	assertQuiet(t, assertTail(t, c, internalmysql.ArchiverTailPending, "mysql-bin.000004", 0))
	if c.tail.hashes != 2 {
		t.Fatalf("the first scan hashed %d files, want the tail's last file and one retired file", c.tail.hashes)
	}
	c.scanAndArchive(ctx)
	check(c, 3)
}

// The upload of an unarchived tail does not wait for the retired files
// below it to be hashed within the budget: the upload pass identifies every
// file without a live row itself, in the same scan, and what it reconciled
// counts as archived.
func TestTailDrainUploadDoesNotWaitForRetiredFiles(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)
	sim.rotate() // 000002: A:4-5
	a.scanAndArchive(ctx)
	retireRow(t, a, "mysql-bin.000001")
	retireRow(t, a, "mysql-bin.000002")
	sim.apply(originTxns(tailA, 101, 6, 6)...)
	sim.setReadOnly(true)
	sim.rotate() // 000003: A:6, the unarchived tail

	// A new process whose budget allows one hash of a retired file per
	// scan: the tail is uploaded by its first scan all the same.
	b := tailArchiver(sim, store)
	b.tail.budget = func() *originBudget { return &originBudget{bytes: 1, deadline: time.Now().Add(time.Minute)} }
	b.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("uploads = %q, want the tail uploaded and no retired file again", got)
	}
	st := assertTail(t, b, internalmysql.ArchiverTailDrained, "mysql-bin.000004", 0)
	assertQuiet(t, st)
	if b.tail.hashes != 0 || tailRows(t, store) != "mysql-bin.000003" {
		t.Fatalf("the drain hashed %d files itself, rows %q", b.tail.hashes, tailRows(t, store))
	}
	// The files the upload pass reconciled are settled: nothing is hashed
	// or read from the store again.
	reads := store.manifestReads()
	b.scanAndArchive(ctx)
	if got, quiet := store.manifestReads()-reads, standbyManifestReads(t); got != quiet || b.tail.hashes != 0 {
		t.Fatalf("the next scan read the manifest %d times (a standby %d) and hashed %d files", got, quiet, b.tail.hashes)
	}
}

// A file left to the upload pass counts as archived only when that pass
// reconciled it. Here the pass does not reach it (a row of another size has
// its name, so it waits for the scan's history lane, which takes the newest
// files first): the tail stays pending, the next scan compares the file by
// content itself, and the new incarnation's first file is uploaded instead
// of passing for the old one's retired file.
func TestTailDrainUnconfirmedFileStaysPending(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store) // the old 000001 (A:1-3) is archived
	// A retirement record of an even earlier file of that name.
	m, err := a.loadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	other := strings.Repeat("0123456789abcdef", 4)
	key, err := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000001", ID: other})
	if err != nil {
		t.Fatal(err)
	}
	m.Pruned = append(m.Pruned, PrunedEntry{Name: "mysql-bin.000001", ID: other, Key: key, Deleted: true})
	if err := a.saveManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	sim.setReadOnly(true)
	a.scanAndArchive(ctx)
	sim.reclone(tailA + ":1-5")
	sim.setServerUUID(tailC)
	a.scanAndArchive(ctx)

	// The new incarnation's writable era: 000001 (two transactions, so
	// another size than the old one) fails to upload, 000002 to 000009 are
	// archived, and 000010 is sealed by the fence.
	sim.setReadOnly(false)
	sim.apply(originTxns(tailC, 101, 1, 2)...)
	sim.rotate()
	for gno := int64(3); gno <= 10; gno++ {
		sim.apply(originTxns(tailC, 101, gno, gno)...)
		sim.rotate()
	}
	store.failing(func(name string) bool { return name == "mysql-bin.000001" })
	a.scanAndArchive(ctx)
	store.failing(nil)
	sim.apply(originTxns(tailC, 101, 11, 11)...)
	sim.setReadOnly(true)
	sim.rotate()
	uploads := store.uploaded()

	b := tailArchiver(sim, store)
	b.scanAndArchive(ctx)
	if got := store.uploaded(); got != uploads+",mysql-bin.000010" {
		t.Fatalf("uploads = %q, want the tail 000010 after %q", got, uploads)
	}
	st := assertTail(t, b, internalmysql.ArchiverTailPending, "mysql-bin.000011", 1)
	if st.BacklogFiles != 1 || st.LastError != "" {
		t.Fatalf("with 000001 not reconciled: %+v", st)
	}
	b.scanAndArchive(ctx)
	if got := store.uploaded(); got != uploads+",mysql-bin.000010,mysql-bin.000001" {
		t.Fatalf("uploads = %q, want the new incarnation's 000001 last", got)
	}
	assertQuiet(t, assertTail(t, b, internalmysql.ArchiverTailDrained, "mysql-bin.000011", 0))
}

// A retirement record with other Previous_gtids is not the file's: the file
// is unarchived without reading it, and counts as pending.
func TestTailDrainRetirementRecordOfOtherPreviousGTIDs(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	sim.apply(originTxns(tailA, 101, 4, 5)...)
	sim.rotate()
	other := strings.Repeat("0123456789abcdef", 4)
	key, err := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000001", ID: other})
	if err != nil {
		t.Fatal(err)
	}
	// No promotion record: the record gate refuses, and the tail block
	// counts what the drain found unarchived.
	if err := seedManifest(ctx, store, "binlogs", &Manifest{Site: "iad",
		Pruned: []PrunedEntry{{Name: "mysql-bin.000001", ID: other, Key: key, PreviousGTIDs: tailB + ":1-9", Deleted: true}}}); err != nil {
		t.Fatal(err)
	}
	a := tailArchiver(sim, store)
	a.scanAndArchive(ctx)
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 2)
	assertQuiet(t, st)
	if a.tail.hashes != 0 || !strings.Contains(st.Tail.Reason, "holds no promotion record") {
		t.Fatalf("%d files hashed, reason %q", a.tail.hashes, st.Tail.Reason)
	}
}

// A file whose content is not the matching record's is hashed once, not on
// every scan, while a gate keeps it from being uploaded.
func TestTailDrainHashesARefusedFileOnce(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	other := strings.Repeat("0123456789abcdef", 4)
	key, err := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000001", ID: other})
	if err != nil {
		t.Fatal(err)
	}
	// A retirement record with the file's name and (empty) Previous_gtids
	// and another content, and no promotion record.
	if err := seedManifest(ctx, store, "binlogs", &Manifest{Site: "iad",
		Pruned: []PrunedEntry{{Name: "mysql-bin.000001", ID: other, Key: key, Deleted: true}}}); err != nil {
		t.Fatal(err)
	}
	a := tailArchiver(sim, store)
	for range 3 {
		a.scanAndArchive(ctx)
		assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000002", 1))
	}
	if a.tail.hashes != 1 || store.uploaded() != "" {
		t.Fatalf("%d hashes in three scans, uploads %q", a.tail.hashes, store.uploaded())
	}
}

// A live row holds a file only with the file's size as well as its name and
// Previous_gtids: after a reclone the new incarnation's first file is not
// the archived first file of the old one.
func TestTailDrainRowOfAnotherSizeDoesNotHoldAFile(t *testing.T) {
	ctx := context.Background()
	sim, a, store := recloneWithAHole(t, false)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(sim.dir, "mysql-bin.000001"))
	if err != nil {
		t.Fatal(err)
	}
	if old := m.Files[0]; old.Name != "mysql-bin.000001" || old.PreviousGTIDs != "" || old.Size == info.Size() {
		t.Fatalf("the old incarnation's row %+v must differ from the new file (%d bytes) in size only", old, info.Size())
	}
	// A new process: nothing but the manifest and the files.
	b := tailArchiver(sim, store)
	b.scanAndArchive(ctx)
	if got := store.uploaded(); got != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000001" {
		t.Fatalf("uploads = %q, want the new incarnation's 000001", got)
	}
	st := assertTail(t, b, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
	if st.LastError != "" || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("after the drain: %+v, rows %q", st, tailRows(t, store))
	}
	_ = a
}

// A quarantined row holds its file like a live one (a quarantine moves
// metadata only): the file is not unarchived, so no gate is asked and the
// tail is drained, here on a site whose records would refuse an upload.
func TestTailDrainQuarantinedRowHoldsItsFile(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	sim.apply(originTxns(tailA, 101, 1, 3)...)
	sim.rotate()
	sim.apply(originTxns(tailA, 101, 4, 5)...)
	sim.rotate()
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	var quarantined []ArchiveQuarantine
	for name, previous := range map[string]string{"mysql-bin.000001": "", "mysql-bin.000002": tailA + ":1-3"} {
		path := filepath.Join(sim.dir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := fileContentID(path)
		if err != nil {
			t.Fatal(err)
		}
		id := store.BinlogContentID(digest)
		quarantined = append(quarantined, ArchiveQuarantine{Reason: "test", ResumeGTIDs: tailA + ":1-5", At: at,
			Entry: ManifestEntry{Name: name, ID: id, Size: info.Size(), PreviousGTIDs: previous, ArchivedAt: at,
				GTIDCoverage: &BinlogGTIDCoverage{Complete: true, ContentID: id}}})
	}
	if err := seedManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Quarantined: quarantined}); err != nil {
		t.Fatal(err)
	}
	a := tailArchiver(sim, store)
	for range 2 {
		a.scanAndArchive(ctx)
		assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0))
	}
	if got := store.uploaded(); got != "" {
		t.Fatalf("uploads = %q", got)
	}
}

// The upload is bound to the file the drain examined, from its first stat
// of the scan on. A file replaced after its examination (here while the
// drain reads the manifest) is not uploaded as examined: the scan ends
// undecided and the next one examines the new file (review F4).
func TestTailDrainRefusesAFileReplacedAfterItsExamination(t *testing.T) {
	ctx := context.Background()
	store := &manifestHookStore{tailStore: newTailStore()}
	sim, a := writableEra(t, store)
	sim.setReadOnly(true)
	sim.rotate() // 000002 = the tail (A:4-5)
	// A pure replica copy under the tail's name, of another size.
	replacement := binlogtest.File{ServerID: 101, Previous: tailA + ":1-3", Txns: originTxns(tailB, 102, 1, 3)}.MustBytes()
	replaced := false
	store.onGet = func() {
		// The drain's manifest read follows its search for the tail.
		if f := a.tail.files["mysql-bin.000002"]; !replaced && f != nil && f.origin == originFound {
			replaced = true
			path := filepath.Join(sim.dir, "mysql-bin.000002")
			if err := os.Remove(path); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(path, replacement, 0o600); err != nil {
				t.Error(err)
			}
		}
	}
	a.scanAndArchive(ctx)
	if !replaced {
		t.Fatal("the file was not replaced during the drain")
	}
	if got := store.uploaded(); got != "mysql-bin.000001" {
		t.Fatalf("uploads = %q: a file the drain never examined was uploaded", got)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 0)
	assertQuiet(t, st)
	if !strings.Contains(st.Tail.Reason, "mysql-bin.000002") || !strings.Contains(st.Tail.Reason, errTailFileChanged.Error()) {
		t.Fatalf("reason = %q", st.Tail.Reason)
	}

	// The next scan examines the file that is there now: replica copies,
	// which are not uploaded.
	a.scanAndArchive(ctx)
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0))
	if got := store.uploaded(); got != "mysql-bin.000001" || tailRows(t, store) != "mysql-bin.000001" {
		t.Fatalf("uploads = %q, rows %q", got, tailRows(t, store))
	}
}

// The header filter trusts the next file's Previous_gtids only when they
// continue this file's. A next header that does not contain this file's is
// no proof of what the file logged, so the file is walked: here it holds a
// transaction the server originated, which the difference of the two
// headers does not show.
func TestTailDrainWalksAFileItsSuccessorDoesNotContinue(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim := newTailSim(t, tailA, 101, true)
	write := func(name string, file binlogtest.File) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(sim.dir, name), file.MustBytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("mysql-bin.000001", binlogtest.File{ServerID: 101, Previous: tailB + ":1-5", Txns: originTxns(tailA, 101, 1, 1)})
	write("mysql-bin.000002", binlogtest.File{ServerID: 101, Previous: tailB + ":1-3"})
	if err := os.WriteFile(filepath.Join(sim.dir, "mysql-bin.index"), []byte("mysql-bin.000001\nmysql-bin.000002\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sim.role.mu.Lock()
	sim.role.gtidExecuted = tailA + ":1," + tailB + ":1-5"
	sim.role.mu.Unlock()
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	if err := seedManifest(ctx, store, "binlogs", &Manifest{Site: "iad",
		Promotions: []PromotionRecord{{ServerUUID: tailA, ServerID: 101, ObservedAt: at, ProbeInterval: pitrarchive.Duration(5 * time.Second)}}}); err != nil {
		t.Fatal(err)
	}
	a := tailArchiver(sim, store)
	a.scanAndArchive(ctx)
	if a.tail.walks != 1 || store.uploaded() != "mysql-bin.000001" {
		t.Fatalf("%d files walked, uploads %q; want the file walked and uploaded", a.tail.walks, store.uploaded())
	}
	assertQuiet(t, assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000002", 0))
}

// tailCurrent's conditions, each alone. In the scenario tests an incarnation
// change is caught twice (the pending reset and the comparison with the
// drain's observation), so either could be removed unnoticed.
func TestTailCurrent(t *testing.T) {
	obs := RoleObservation{ReadOnly: true, ServerUUID: tailA, ServerID: 101, GTIDExecuted: tailA + ":1-5"}
	with := func(change func(*RoleObservation)) *RoleObservation {
		o := obs
		change(&o)
		return &o
	}
	more := with(func(o *RoleObservation) { o.GTIDExecuted = tailA + ":1-7" })
	for _, tc := range []struct {
		name     string
		timeline *roleTimeline
		ended    bool
	}{
		{"the read-only server the drain was decided on", &roleTimeline{state: roleReadOnly, readOnly: &obs}, false},
		{"the same server, with more executed", &roleTimeline{state: roleReadOnly, readOnly: more}, false},
		{"writable", &roleTimeline{state: roleWritable, readOnly: &obs}, true},
		{"a failed probe", &roleTimeline{state: roleUnknown, readOnly: &obs}, true},
		{"no read-only observation", &roleTimeline{state: roleReadOnly}, true},
		{"an observation without the server identity", &roleTimeline{state: roleReadOnly, readOnly: &RoleObservation{ReadOnly: true}}, true},
		// The server lost history and caught up again: its latest
		// observation contains the drain's, and only the unwritten reset
		// says it is another incarnation.
		{"an incarnation reset not yet recorded", &roleTimeline{state: roleReadOnly, readOnly: more,
			resets: []resetCandidate{{obs: *with(func(o *RoleObservation) { o.GTIDExecuted = tailA + ":1-3" })}}}, true},
		// No reset is pending (it was recorded): only the comparison with
		// the drain's observation says so.
		{"less executed than the drain saw", &roleTimeline{state: roleReadOnly, readOnly: with(func(o *RoleObservation) { o.GTIDExecuted = tailA + ":1-3" })}, true},
		{"another server_uuid", &roleTimeline{state: roleReadOnly, readOnly: with(func(o *RoleObservation) { o.ServerUUID = tailC })}, true},
		{"another server_id", &roleTimeline{state: roleReadOnly, readOnly: with(func(o *RoleObservation) { o.ServerID = 7 })}, true},
	} {
		if err := tc.timeline.tailCurrent(obs); (err != nil) != tc.ended || (err != nil && err != errTailEnded) {
			t.Errorf("%s: %v, want ended = %v", tc.name, err, tc.ended)
		}
	}

	// An observation without the server identity ends the drain whatever
	// it is compared with.
	unidentified := RoleObservation{ReadOnly: true}
	if err := (&roleTimeline{state: roleReadOnly, readOnly: &unidentified}).tailCurrent(unidentified); err != errTailEnded {
		t.Errorf("an unidentified observation on both sides: %v", err)
	}

	// The same through the role sampler: read-only, then a probe that
	// shows less (a reset candidate), then more than the drain saw.
	var timeline roleTimeline
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	for i, executed := range []string{tailA + ":1-5", tailA + ":1-3", tailA + ":1-7"} {
		o := obs
		o.GTIDExecuted, o.ObservedAt = executed, at.Add(time.Duration(i)*time.Second)
		timeline.observe(o, true, nil)
	}
	if err := timeline.tailCurrent(obs); err != errTailEnded {
		t.Fatalf("after a reset the sampler saw: %v", err)
	}
}

// Once the role sampler has seen the server writable, failed or of another
// incarnation, the drain uploads nothing: without the check before the
// upload pass the object would be written first and only its manifest save
// refused, as an upload failure.
func TestTailDrainUploadsNothingOnceTheRoleMovedOn(t *testing.T) {
	ctx := context.Background()
	store := &manifestHookStore{tailStore: newTailStore()}
	sim, a := writableEra(t, store)
	sim.setReadOnly(true)
	sim.rotate() // 000002 = the tail
	moved := false
	store.onGet = func() {
		// The drain's manifest read: after it took the read-only
		// observation it is decided on, before its upload pass.
		if f := a.tail.files["mysql-bin.000002"]; !moved && f != nil && f.origin == originFound {
			moved = true
			sim.setReadOnly(false)
			_, _, _ = a.sampleRole(ctx)
		}
	}
	objects := len(store.objects)
	a.scanAndArchive(ctx)
	if !moved {
		t.Fatal("the role did not change during the drain")
	}
	if got := store.uploaded(); got != "mysql-bin.000001" || len(store.objects) != objects {
		t.Fatalf("uploads = %q, %d objects (were %d): the drain wrote an object for a server that is no longer read-only", got, len(store.objects), objects)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailBlocked, "mysql-bin.000003", 1)
	assertQuiet(t, st)
	if st.Tail.Reason != errTailEnded.Error() {
		t.Fatalf("reason = %q", st.Tail.Reason)
	}

	// The next scan is a writable site's: it records the promotion and
	// uploads the file.
	store.onGet = nil
	a.scanAndArchive(ctx)
	if got := tailRows(t, store); got != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("rows = %q", got)
	}
}

// The tail block's file names are those of the index read its state was
// decided on, also when the index rotated while the scan worked: the scan
// does not read the index again. Drained is therefore a statement about
// that index, and a reader that waits for a rotation of its own (the
// operator before a reclone) has to see ActiveFile at or after the file
// that rotation opened. Here a scan that read the index before the fence
// publishes Drained for the file that was active then, although that file,
// sealed since, holds a transaction that is not archived yet (review F2).
func TestArchiverTailNamesAreOfTheScansIndexRead(t *testing.T) {
	ctx := context.Background()
	store := newTailStore()
	sim, a := writableEra(t, store)            // 000001 archived; 000002 active with A:4-5
	sim.rotate()                               // a rotation while writable: 000003 active
	sim.apply(originTxns(tailA, 101, 6, 6)...) // an origin transaction in 000003
	var afterFlush string
	store.mu.Lock()
	store.onUpload = func(name string) { // the scan is uploading 000002
		if name == "mysql-bin.000002" && afterFlush == "" {
			sim.setReadOnly(true) // the fence
			sim.rotate()          // the fence's FLUSH seals 000003: 000004 active
			sim.rotate()          // the operator's pre-reclone FLUSH: 000005 active
			afterFlush = sim.names[len(sim.names)-1]
		}
	}
	store.mu.Unlock()
	a.scanAndArchive(ctx)
	if afterFlush != "mysql-bin.000005" {
		t.Fatalf("the rotations during the scan opened %q", afterFlush)
	}
	st := assertTail(t, a, internalmysql.ArchiverTailDrained, "mysql-bin.000003", 0)
	if st.Tail.NewestSealedFile != "mysql-bin.000002" || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002" {
		t.Fatalf("tail %+v, rows %q", st.Tail, tailRows(t, store))
	}
	// Index order is name order: the block is of an index before the
	// operator's rotation, which is how the operator tells.
	if st.Tail.ActiveFile >= afterFlush {
		t.Fatalf("activeFile %s is not before the file the operator's FLUSH opened (%s)", st.Tail.ActiveFile, afterFlush)
	}

	// The scan that reads the rotated index archives the sealed 000003 and
	// reports Drained for that index.
	a.scanAndArchive(ctx)
	st = assertTail(t, a, internalmysql.ArchiverTailDrained, afterFlush, 0)
	if st.Tail.NewestSealedFile != "mysql-bin.000004" || tailRows(t, store) != "mysql-bin.000001,mysql-bin.000002,mysql-bin.000003" {
		t.Fatalf("tail %+v, rows %q", st.Tail, tailRows(t, store))
	}
}
