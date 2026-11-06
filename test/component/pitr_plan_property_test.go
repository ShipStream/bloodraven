package component

// Planner property sweep (#206, design §9.1 property_test, §9.2): seeded
// random failover and failback histories run through real BinlogArchivers
// (two sites over simulated mysqlds sharing one PVC archive); the planner
// then plans restores from the archive the archivers produced. A reference
// model replays every server's commit log:
//
//   - when the planner certifies a timestamp restore, its witness is the
//     model's earliest archived commit at or after T outside B ∪ X, Λ is that
//     transaction's origin's executed set just before it, the segments
//     applied to B yield exactly Λ ∖ X, and D ⊆ X;
//   - when it certifies latest mode, Λ is a state some server actually held,
//     and it never includes a transaction lost at a failover unless excluded;
//   - when it refuses, the token is one the trial's faults explain, and a
//     trial without a lossy failover never refuses with an integrity or
//     ordering token.
//
// PITR_PLAN_SEED=<n> replays one trial.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

type propCommit struct {
	g   pitrplan.GTID
	oct int64
	srv uint32
}

// propModel is the reference: every server's executed sequence, in order.
type propModel struct {
	log    map[string][]propCommit // site → commits it executed, in order
	oct    map[pitrplan.GTID]int64
	origin map[pitrplan.GTID]string // site that committed it
	last   time.Time
}

func (m *propModel) tick() time.Time {
	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.After(m.last) {
		now = m.last.Add(time.Microsecond)
	}
	m.last = now
	return now
}

// executedBefore is site's executed set just before it executed g.
func (m *propModel) executedBefore(site string, g pitrplan.GTID) (pitrplan.Set, bool) {
	s := pitrplan.Set{}
	for _, c := range m.log[site] {
		if c.g == g {
			return s, true
		}
		s = s.Union(pitrplan.SetOf(c.g))
	}
	return nil, false
}

// heldState reports whether some server held exactly set at some point.
func (m *propModel) heldState(set pitrplan.Set) bool {
	for _, log := range m.log {
		s := pitrplan.Set{}
		if s.Equal(set) {
			return true
		}
		for _, c := range log {
			s = s.Union(pitrplan.SetOf(c.g))
			if s.Equal(set) {
				return true
			}
		}
	}
	return false
}

type propTrial struct {
	t       *testing.T
	w       *writerArchive
	srv     map[string]*simServer
	arch    map[string]*sidecar.BinlogArchiver
	model   *propModel
	uuid    map[string]string
	id      map[string]uint32
	queue   map[string][]binlogtest.Txn // replica apply lag
	dead    pitrplan.Set
	lossy   bool
	backup  backupmeta.DumpMeta
	targets []time.Time
}

func (p *propTrial) commit(primary string) {
	gno := int64(1)
	for _, c := range p.model.log[primary] {
		if c.g.SID == p.uuid[primary] && c.g.GNO >= gno {
			gno = c.g.GNO + 1
		}
	}
	at := p.model.tick()
	txn := binlogtest.Txn{SID: p.uuid[primary], GNO: gno, Commit: at, ServerID: p.id[primary]}
	g := pitrplan.GTID{SID: txn.SID, GNO: gno}
	p.srv[primary].apply(txn)
	p.model.log[primary] = append(p.model.log[primary], propCommit{g, at.UnixMicro(), txn.ServerID})
	p.model.oct[g] = at.UnixMicro()
	p.model.origin[g] = primary
	for site := range p.srv {
		if site != primary && p.queue[site] != nil {
			p.queue[site] = append(p.queue[site], txn)
		}
	}
}

// drain applies a replica's queued copies (all, or all but the last keep).
func (p *propTrial) drain(site string, drop int) {
	q := p.queue[site]
	keep := len(q) - drop
	for _, txn := range q[:keep] {
		p.srv[site].apply(txn)
		g := pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}
		p.model.log[site] = append(p.model.log[site], propCommit{g, txn.Commit.UnixMicro(), txn.ServerID})
	}
	for _, txn := range q[keep:] {
		p.dead = p.dead.Union(pitrplan.SetOf(pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}))
	}
	p.queue[site] = q[:0]
}

func (p *propTrial) sealed(site string) int { return len(p.srv[site].files) - 1 }

func (p *propTrial) waitUploaded(site string) {
	p.w.eventually("uploads of "+site, func() bool { return len(p.w.manifest(site).Files) >= p.sealed(site) })
}

func runPropTrial(t *testing.T, seed int64) (planned, refused int) {
	r := rand.New(rand.NewSource(seed))
	p := &propTrial{t: t, w: newWriterArchive(t), srv: map[string]*simServer{}, arch: map[string]*sidecar.BinlogArchiver{},
		model: &propModel{log: map[string][]propCommit{}, oct: map[pitrplan.GTID]int64{}, origin: map[pitrplan.GTID]string{}},
		uuid:  map[string]string{"a": writerUUIDA, "b": writerUUIDB}, id: map[string]uint32{"a": 101, "b": 102},
		queue: map[string][]binlogtest.Txn{"b": {}}, dead: pitrplan.Set{}}
	p.srv["a"] = newSimServer(t, writerUUIDA, 101, false)
	p.srv["b"] = newSimServer(t, writerUUIDB, 102, true)
	ident := p.w.identity(writerGroup, p.w.dest)
	for _, s := range []string{"a", "b"} {
		p.arch[s] = p.w.start(s, p.srv[s], p.w.store, ident)
	}
	p.w.waitRecords("a", 1)
	primary, replica := "a", "b"
	records := map[string]int{"a": 1, "b": 0}
	eras := 2 + r.Intn(3)
	backupAt := 2 + r.Intn(3)
	backupSite := []string{"a", "b"}[r.Intn(2)]
	commits := 0
	for era := 0; era < eras; era++ {
		n := 3 + r.Intn(5)
		for i := 0; i < n; i++ {
			p.commit(primary)
			commits++
			if r.Intn(5) == 0 {
				p.srv[primary].rotate()
			}
			if r.Intn(4) == 0 && p.queue[replica] != nil {
				p.drain(replica, 0)
			}
			if era == 0 && commits == backupAt {
				// A consistent dump of backupSite: its executed set now.
				if backupSite == replica {
					p.drain(replica, 0)
				}
				b := pitrplan.Set{}
				var last int64
				for _, c := range p.model.log[backupSite] {
					b = b.Union(pitrplan.SetOf(c.g))
					last = max(last, c.oct)
				}
				end := time.UnixMicro(last + 1).UTC()
				meta := backupmeta.NewMicroTime(end)
				p.backup = backupmeta.DumpMeta{Version: 1, GTIDExecuted: b.String(), End: meta, ServerClock: backupmeta.ServerClock{Start: meta, End: meta},
					Stamp: backupmeta.Stamp{Site: backupSite, ServerID: p.id[backupSite], CreatedAt: meta}}
				// Targets after the dump end, rounded up to the second.
				time.Sleep(time.Until(end.Truncate(time.Second).Add(time.Second + 10*time.Millisecond)))
			}
			if commits > backupAt && r.Intn(2) == 0 {
				p.targets = append(p.targets, time.UnixMicro(p.model.oct[p.model.log[primary][len(p.model.log[primary])-1].g]-1).UTC())
			}
		}
		if era == eras-1 || p.lossy {
			continue
		}
		// Fail over to the replica: planned (caught up) or lossy.
		p.waitUploaded(primary)
		drop := 0
		if r.Intn(3) == 0 && len(p.queue[replica]) > 0 {
			drop = 1 + r.Intn(len(p.queue[replica]))
			p.lossy = true
			if r.Intn(2) == 0 {
				// The lost tail was sealed and archived before the
				// primary died: D and loss evidence see it.
				p.srv[primary].rotate()
				p.waitUploaded(primary)
			}
		}
		p.drain(replica, drop)
		p.srv[primary].setReadOnly(true)
		p.w.waitDemoted(p.arch[primary])
		p.srv[replica].setReadOnly(false)
		records[replica]++
		p.w.waitRecords(replica, records[replica])
		p.waitUploaded(replica)
		if p.lossy {
			// The old primary is down for good: no further copies.
			p.queue[primary] = nil
		} else {
			p.queue[primary] = []binlogtest.Txn{}
		}
		p.queue[replica] = nil
		primary, replica = replica, primary
	}
	if r.Intn(3) != 0 {
		p.srv[primary].rotate()
	}
	p.waitUploaded(primary)
	if p.backup.Version == 0 {
		return 0, 0
	}
	p.targets = append(p.targets, time.Now().UTC().Add(time.Hour))

	var arch pitrplan.Archive
	for _, s := range []string{"a", "b"} {
		sm, err := sidecar.PlanSite(p.w.manifest(s))
		if err != nil {
			t.Fatal(err)
		}
		arch.Sites = append(arch.Sites, sm)
	}
	archived := pitrplan.Set{}
	for _, sm := range arch.Sites {
		for _, row := range sm.Rows {
			archived = archived.Union(row.Hints.GTIDs)
		}
	}
	B := pitrplan.MustParseSet(p.backup.GTIDExecuted)
	t.Logf("seed %d: %d eras, backup on %s, lossy=%v (lost %s), archived %s", seed, eras, backupSite, p.lossy, p.dead, archived)
	excludes := []pitrplan.Set{{}}
	if !p.dead.IsEmpty() && !p.dead.Overlaps(B) {
		excludes = append(excludes, p.dead)
	}
	for _, X := range excludes {
		for _, T := range append(p.targets, time.Time{}) {
			mode := pitrplan.ModeTimestamp
			if T.IsZero() {
				mode = pitrplan.ModeLatest
			}
			budget := pitrplan.NewBudget(64 << 20)
			plan, err := pitrplan.Run(context.Background(), pitrplan.Request{Mode: mode, Stop: T, Exclude: X, Delta: time.Microsecond, Dump: p.backup, Budget: budget},
				arch, propFetcher(p.w, budget))
			what := fmt.Sprintf("seed %d %s T=%s X=%s lossy=%v", seed, mode, T.Format(time.RFC3339Nano), X, p.lossy)
			if err != nil {
				refused++
				ref, ok := pitrplan.RefusalOf(err)
				if !ok {
					t.Fatalf("%s: non-refusal error %v", what, err)
				}
				allowed := map[pitrarchive.Token]bool{pitrarchive.TokenTargetNotCovered: true, pitrarchive.TokenBackupAfterTarget: true,
					pitrarchive.TokenArchiveGap: true, pitrarchive.TokenNoArchivedReplay: true}
				if p.lossy {
					for _, tok := range []pitrarchive.Token{pitrarchive.TokenArchiveDivergentHistory, pitrarchive.TokenArchiveSplitBrain,
						pitrarchive.TokenStopAmbiguous, pitrarchive.TokenBackupNotOnLineage} {
						allowed[tok] = true
					}
				}
				if !allowed[ref.Token] {
					t.Fatalf("%s: refusal not explained by the trial's faults: %v", what, err)
				}
				t.Logf("%s: %s", what, ref.Token)
				continue
			}
			planned++
			// Segments applied to B yield Λ ∖ X, each starting at the state.
			state := B
			for _, s := range plan.Segments {
				if !s.StartPredecessors.Subtract(X).Equal(state) {
					t.Fatalf("%s: segment starts at %s, state %s", what, s.StartPredecessors, state)
				}
				state = state.Union(s.GTIDs)
			}
			if !state.Equal(plan.Lineage.Subtract(X)) || !X.Contains(plan.Divergent) {
				t.Fatalf("%s: final %s, Λ∖X %s, D %s", what, state, plan.Lineage.Subtract(X), plan.Divergent)
			}
			if mode == pitrplan.ModeLatest {
				if !p.model.heldState(plan.Lineage) || plan.Lineage.Subtract(X).Overlaps(p.dead) {
					t.Fatalf("%s: latest Λ %s is not a server state, or holds lost %s", what, plan.Lineage, p.dead)
				}
				continue
			}
			// The model's witness: earliest archived commit ≥ T outside B ∪ X.
			var w pitrplan.GTID
			best := int64(-1)
			for _, g := range archived.Subtract(B).Subtract(X).GTIDs() {
				if o := p.model.oct[g]; o >= T.UnixMicro() && (best < 0 || o < best) {
					w, best = g, o
				}
			}
			if plan.Witness == nil || plan.Witness.GTID != w {
				t.Fatalf("%s: witness %+v, the model says %s", what, plan.Witness, w)
			}
			lambda, ok := p.model.executedBefore(p.model.origin[w], w)
			if !ok || !plan.Lineage.Subtract(X).Equal(lambda.Subtract(X)) {
				t.Fatalf("%s: Λ %s, %s's origin had %s", what, plan.Lineage, w, lambda)
			}
		}
	}
	return planned, refused
}

// propFetcher reads rows from the archive store, authenticating them like
// pitr-download does.
func propFetcher(w *writerArchive, budget *pitrplan.Budget) pitrplan.Fetcher {
	return pitrplan.FetchFunc(func(ctx context.Context, ref pitrplan.RowRef) (*pitrplan.FileIndex, error) {
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
}

func TestPITRPlanProperty(t *testing.T) {
	seeds := []int64{}
	if v := os.Getenv("PITR_PLAN_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, n)
	} else {
		for s := int64(1); s <= 10; s++ {
			seeds = append(seeds, s)
		}
	}
	planned, refused := 0, 0
	for _, seed := range seeds {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			pl, rf := runPropTrial(t, seed)
			planned += pl
			refused += rf
		})
	}
	t.Logf("%d certified plans, %d refusals over %d trials", planned, refused, len(seeds))
	if len(seeds) > 1 && planned < len(seeds) {
		t.Errorf("the sweep certified only %d plans; the generator is not exercising the planner", planned)
	}
}
