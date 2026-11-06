package component

// Planner property sweep (#206, design §9.1 property_test, §9.2): seeded
// random failover and failback histories run through real BinlogArchivers
// (two sites over simulated mysqlds sharing one PVC archive); the planner
// then plans restores from the archive the archivers produced. A reference
// model replays every server's commit log, and the oracle decides what the
// planner may answer before it looks at the answer:
//
//   - when the planner certifies a timestamp restore, its witness is the
//     model's earliest archived commit at or after T outside B ∪ X, Λ is that
//     transaction's origin's executed set just before it, D is the model's
//     {g archived : maxOct(B) < oct(g) < T, g ∉ Λ ∪ B} and D ⊆ X, and each
//     segment, re-derived from the indexed bytes of the file it names,
//     applied to B yields exactly Λ ∖ X;
//   - when it certifies latest mode, Λ is a state some server actually held,
//     it never includes a transaction lost at a failover unless excluded,
//     and the model's loss evidence is excluded and reported;
//   - when it refuses, the model justifies the token: TargetNotCovered only
//     without a model witness, DivergentHistory only when the model's D (or
//     loss evidence) is not excluded, StopAmbiguous only for a witness
//     archived only as a replica copy that no promotion record or origin
//     transaction anchors, ArchiveGap only when the model's history is not
//     fully archived from the backup's position, and so on.
//
// PITR_PLAN_SEED=<n> replays one trial.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// propProbeSlack bounds how long after a role change the archivers (role
// probe every 10 ms) observe it; the anchor model widens its windows by it.
const propProbeSlack = 100 * time.Millisecond

type propCommit struct {
	g   pitrplan.GTID
	oct int64
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
	w       *writerArchive
	srv     map[string]*simServer
	arch    map[string]*sidecar.BinlogArchiver
	model   *propModel
	uuid    map[string]string
	id      map[string]uint32
	records map[string]int
	queue   map[string][]binlogtest.Txn // replica apply lag
	dead    pitrplan.Set
	lossy   bool
	backup  backupmeta.DumpMeta
	targets []time.Time
}

// newPropTrial starts a writable a and a replicating b, each with a real
// archiver, and waits for a's first promotion record.
func newPropTrial(t *testing.T) *propTrial {
	p := &propTrial{w: newWriterArchive(t), srv: map[string]*simServer{}, arch: map[string]*sidecar.BinlogArchiver{},
		model: &propModel{log: map[string][]propCommit{}, oct: map[pitrplan.GTID]int64{}, origin: map[pitrplan.GTID]string{}},
		uuid:  map[string]string{"a": writerUUIDA, "b": writerUUIDB}, id: map[string]uint32{"a": 101, "b": 102},
		records: map[string]int{"a": 1, "b": 0}, queue: map[string][]binlogtest.Txn{"b": {}}, dead: pitrplan.Set{}}
	p.srv["a"] = newSimServer(t, writerUUIDA, 101, false)
	p.srv["b"] = newSimServer(t, writerUUIDB, 102, true)
	ident := p.w.identity(writerGroup, p.w.dest)
	for _, s := range []string{"a", "b"} {
		p.arch[s] = p.w.start(s, p.srv[s], p.w.store, ident)
	}
	p.w.waitRecords("a", 1)
	return p
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
	p.model.log[primary] = append(p.model.log[primary], propCommit{g, at.UnixMicro()})
	p.model.oct[g] = at.UnixMicro()
	p.model.origin[g] = primary
	for site := range p.srv {
		if site != primary && p.queue[site] != nil {
			p.queue[site] = append(p.queue[site], txn)
		}
	}
}

// lastCommit is the newest commit of primary.
func (p *propTrial) lastCommit(primary string) pitrplan.GTID {
	return p.model.log[primary][len(p.model.log[primary])-1].g
}

// target adds a target 1 µs before primary's newest commit, so that
// commit is the model's witness.
func (p *propTrial) target(primary string) time.Time {
	t := time.UnixMicro(p.model.oct[p.lastCommit(primary)] - 1).UTC()
	p.targets = append(p.targets, t)
	return t
}

// drain applies a replica's queued copies except the last drop, which are
// lost with the old primary.
func (p *propTrial) drain(site string, drop int) {
	q := p.queue[site]
	keep := len(q) - drop
	for _, txn := range q[:keep] {
		p.srv[site].apply(txn)
		g := pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}
		p.model.log[site] = append(p.model.log[site], propCommit{g, txn.Commit.UnixMicro()})
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

// takeBackup records a consistent dump of site (its executed set now),
// then waits until every later commit is past both dump-end bounds the
// planner enforces (ceil(End) and serverClock.end + 1 s), so a later
// target never justifies PITRBackupAfterTarget.
func (p *propTrial) takeBackup(site string) {
	if p.queue[site] != nil {
		p.drain(site, 0)
	}
	b := pitrplan.Set{}
	var last int64
	for _, c := range p.model.log[site] {
		b = b.Union(pitrplan.SetOf(c.g))
		last = max(last, c.oct)
	}
	end := time.UnixMicro(last + 1).UTC()
	meta := backupmeta.NewMicroTime(end)
	p.backup = backupmeta.DumpMeta{Version: 1, GTIDExecuted: b.String(), End: meta, ServerClock: backupmeta.ServerClock{Start: meta, End: meta},
		Stamp: backupmeta.Stamp{Site: site, ServerID: p.id[site], CreatedAt: meta}}
	time.Sleep(time.Until(end.Add(time.Second + 10*time.Millisecond)))
}

// failover moves the primary role to replica. drop > 0 loses the last drop
// queued copies with the old primary (lossy); archiveTail first seals and
// uploads the old primary's tail, so the lost transactions are archived.
// It returns the new primary's promotion record time.
func (p *propTrial) failover(primary, replica string, drop int, archiveTail bool) time.Time {
	p.waitUploaded(primary)
	if drop > 0 {
		p.lossy = true
		if archiveTail {
			p.srv[primary].rotate()
			p.waitUploaded(primary)
		}
	}
	p.drain(replica, drop)
	p.srv[primary].setReadOnly(true)
	p.w.waitDemoted(p.arch[primary])
	p.srv[replica].setReadOnly(false)
	p.records[replica]++
	p.w.waitRecords(replica, p.records[replica])
	p.waitUploaded(replica)
	if p.lossy {
		// The old primary is down for good: no further copies.
		p.queue[primary] = nil
	} else {
		p.queue[primary] = []binlogtest.Txn{}
	}
	p.queue[replica] = nil
	recs := p.w.manifest(replica).Promotions
	return recs[len(recs)-1].ObservedAt
}

// sleepPast waits until the wall clock (every simulated server's) is
// strictly past at + d.
func sleepPast(at time.Time, d time.Duration) {
	time.Sleep(time.Until(at.Add(d + 200*time.Millisecond)))
}

func runPropTrial(t *testing.T, seed int64) (planned, refused int) {
	r := rand.New(rand.NewSource(seed))
	p := newPropTrial(t)
	primary, replica := "a", "b"
	eras := 2 + r.Intn(3)
	n0 := 3 + r.Intn(5)
	// The backup is taken inside era 0, so every trial plans.
	backupAt := 2 + r.Intn(n0-1)
	backupSite := []string{"a", "b"}[r.Intn(2)]
	commits := 0
	for era := 0; era < eras; era++ {
		n := n0
		if era > 0 {
			n = 3 + r.Intn(5)
		}
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
				p.takeBackup(backupSite)
			}
			if commits > backupAt && r.Intn(2) == 0 {
				p.target(primary)
			}
		}
		if era == eras-1 || p.lossy {
			continue
		}
		// Fail over to the replica: planned (caught up) or lossy.
		drop := 0
		archiveTail := false
		if r.Intn(3) == 0 && len(p.queue[replica]) > 0 {
			drop = 1 + r.Intn(len(p.queue[replica]))
			archiveTail = r.Intn(2) == 0
		}
		p.failover(primary, replica, drop, archiveTail)
		primary, replica = replica, primary
	}
	if r.Intn(3) != 0 {
		p.srv[primary].rotate()
	}
	p.waitUploaded(primary)
	if p.backup.Version == 0 {
		t.Fatalf("seed %d: the generator took no backup (backupAt %d, era 0 has %d commits)", seed, backupAt, n0)
	}
	p.targets = append(p.targets, time.Now().UTC().Add(time.Hour))
	o := newPropOracle(t, p, time.Microsecond)
	t.Logf("seed %d: %d eras, backup on %s, lossy=%v (lost %s), archived %s", seed, eras, backupSite, p.lossy, p.dead, o.archived)
	res := o.run(fmt.Sprintf("seed %d", seed))
	for _, r := range res {
		if r.err != nil {
			refused++
		} else {
			planned++
		}
	}
	if planned+refused == 0 {
		t.Fatalf("seed %d: the oracle checked nothing", seed)
	}
	return planned, refused
}

// propOracle holds the reference model's view of one finished trial.
type propOracle struct {
	t        *testing.T
	p        *propTrial
	delta    time.Duration
	arch     pitrplan.Archive
	rows     map[pitrplan.RowRef]pitrplan.Row
	archived pitrplan.Set
	B        pitrplan.Set
	maxOctB  int64

	mu      sync.Mutex
	fetched map[pitrplan.RowRef]*pitrplan.FileIndex
}

type propResult struct {
	mode pitrplan.Mode
	T    time.Time
	X    pitrplan.Set
	plan *pitrplan.Plan
	err  error
}

func newPropOracle(t *testing.T, p *propTrial, delta time.Duration) *propOracle {
	o := &propOracle{t: t, p: p, delta: delta, rows: map[pitrplan.RowRef]pitrplan.Row{}, archived: pitrplan.Set{},
		fetched: map[pitrplan.RowRef]*pitrplan.FileIndex{}}
	for _, s := range []string{"a", "b"} {
		sm, err := sidecar.PlanSite(p.w.manifest(s))
		if err != nil {
			t.Fatal(err)
		}
		o.arch.Sites = append(o.arch.Sites, sm)
		for _, row := range sm.Rows {
			o.rows[pitrplan.RowRef{Site: row.Site, Name: row.Name, ID: row.ID, State: row.State}] = row
			o.archived = o.archived.Union(row.Hints.GTIDs)
		}
	}
	o.B = pitrplan.MustParseSet(p.backup.GTIDExecuted)
	for _, g := range o.B.GTIDs() {
		o.maxOctB = max(o.maxOctB, p.model.oct[g])
	}
	return o
}

// fetcher reads rows from the archive store, authenticating them like
// pitr-download does, and keeps each index so segments can be checked
// against the bytes of the file they name.
func (o *propOracle) fetcher(budget *pitrplan.Budget) pitrplan.Fetcher {
	w := o.p.w
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
		o.mu.Lock()
		o.fetched[ref] = idx
		o.mu.Unlock()
		return idx, nil
	})
}

// witness is the model's earliest archived commit at or after T outside
// B ∪ X.
func (o *propOracle) witness(T time.Time, X pitrplan.Set) (pitrplan.GTID, bool) {
	var w pitrplan.GTID
	best := int64(-1)
	for _, g := range o.archived.Subtract(o.B).Subtract(X).GTIDs() {
		if oc := o.p.model.oct[g]; oc >= T.UnixMicro() && (best < 0 || oc < best) {
			w, best = g, oc
		}
	}
	return w, best >= 0
}

// refD is the model's D: archived transactions committed in (maxOct(B), T)
// outside Λ ∪ B.
func (o *propOracle) refD(T time.Time, lambda pitrplan.Set) pitrplan.Set {
	d := pitrplan.Set{}
	known := lambda.Union(o.B)
	for _, g := range o.archived.GTIDs() {
		if oc := o.p.model.oct[g]; oc > o.maxOctB && oc < T.UnixMicro() && !known.Has(g) {
			d = d.Union(pitrplan.SetOf(g))
		}
	}
	return d
}

// originCopy reports whether w is archived by its origin site's server.
func (o *propOracle) originCopy(w pitrplan.GTID) bool {
	site := o.p.model.origin[w]
	for _, row := range o.rows {
		if row.Site == site && row.Hints.GTIDs.Has(w) {
			return true
		}
	}
	return false
}

func (o *propOracle) promotions(site string) []pitrarchive.PromotionRecord {
	for _, sm := range o.arch.Sites {
		if sm.Site == site {
			return sm.Promotions
		}
	}
	return nil
}

// anchorable is the model of the replica-copy anchor rule (§4.4 as
// revised): a promotion record R of w's origin site, or an archived origin
// transaction q of that site, anchors w when it is more than δ older than
// w, no record of that site was observed after it and up to δ (plus the
// probe interval) after w (a later episode or incarnation), and no record
// of the other site observed within δ of that span lacks w. The model is
// generous to refusals: it requires δ for origin anchors too.
func (o *propOracle) anchorable(w pitrplan.GTID) bool {
	site := o.p.model.origin[w]
	octW := o.p.model.oct[w]
	d := o.delta.Microseconds()
	slack := propProbeSlack.Microseconds()
	ok := func(at int64) bool {
		if at+d >= octW {
			return false
		}
		for _, sm := range o.arch.Sites {
			for _, rec := range sm.Promotions {
				obs := rec.ObservedAt.UnixMicro()
				if sm.Site == site {
					if obs > at && obs <= octW+slack+d {
						return false
					}
					continue
				}
				if obs >= at-d && obs <= octW+d && !pitrplan.MustParseSet(rec.GTIDExecuted).Has(w) {
					return false
				}
			}
		}
		return true
	}
	for _, rec := range o.promotions(site) {
		if ok(rec.ObservedAt.UnixMicro()) {
			return true
		}
	}
	for _, row := range o.rows {
		if row.Site != site {
			continue
		}
		for _, q := range row.Hints.GTIDs.GTIDs() {
			if o.p.model.origin[q] == site && ok(o.p.model.oct[q]) {
				return true
			}
		}
	}
	return false
}

// bPositioned reports whether some archived file reaches exactly B at a
// position (its start, or after one of its transactions in its server's
// order): the search needs one to start replay from.
func (o *propOracle) bPositioned() bool {
	for _, row := range o.rows {
		state := row.Hints.Previous
		if state.Equal(o.B) {
			return true
		}
		for _, c := range o.p.model.log[row.Site] {
			if !row.Hints.GTIDs.Has(c.g) {
				continue
			}
			state = state.Union(pitrplan.SetOf(c.g))
			if state.Equal(o.B) {
				return true
			}
		}
	}
	return false
}

// modelLost is latest mode's loss evidence candidates (§4.8 G2 as ruled
// N1/N2): an archived transaction committed more than δ before a promotion
// record of another site was observed, missing from that record and not
// excluded. The planner counts those of them in the head's Λ.
func (o *propOracle) modelLost(X pitrplan.Set) pitrplan.Set {
	lost := pitrplan.Set{}
	d := o.delta.Microseconds()
	for _, row := range o.rows {
		for _, g := range row.Hints.GTIDs.GTIDs() {
			for _, sm := range o.arch.Sites {
				if sm.Site == row.Site {
					continue
				}
				for _, rec := range sm.Promotions {
					if o.p.model.oct[g] < rec.ObservedAt.UnixMicro()-d && !pitrplan.MustParseSet(rec.GTIDExecuted).Has(g) {
						lost = lost.Union(pitrplan.SetOf(g))
					}
				}
			}
		}
	}
	return lost.Subtract(X)
}

// run plans every target (and latest mode) with X = ∅ and, when the loss
// is excludable, X = the lost transactions, checking each answer against
// the model.
func (o *propOracle) run(label string) []propResult {
	t, p := o.t, o.p
	excludes := []pitrplan.Set{{}}
	if !p.dead.IsEmpty() && !p.dead.Overlaps(o.B) {
		excludes = append(excludes, p.dead)
	}
	var out []propResult
	for _, X := range excludes {
		for _, T := range append(append([]time.Time{}, p.targets...), time.Time{}) {
			mode := pitrplan.ModeTimestamp
			if T.IsZero() {
				mode = pitrplan.ModeLatest
			}
			budget := pitrplan.NewBudget(64 << 20)
			plan, err := pitrplan.Run(context.Background(), pitrplan.Request{Mode: mode, Stop: T, Exclude: X, Delta: o.delta, Dump: p.backup, Budget: budget},
				o.arch, o.fetcher(budget))
			what := fmt.Sprintf("%s δ=%s %s T=%s X=%s lossy=%v", label, o.delta, mode, T.Format(time.RFC3339Nano), X, p.lossy)
			if mode == pitrplan.ModeLatest {
				o.checkLatest(what, X, plan, err)
			} else {
				o.checkTimestamp(what, T, X, plan, err)
			}
			if err != nil {
				ref, _ := pitrplan.RefusalOf(err)
				t.Logf("%s: %s", what, ref.Token)
			}
			out = append(out, propResult{mode, T, X, plan, err})
		}
	}
	return out
}

// refusal fails the test unless err is a refusal whose token justified
// allows.
func (o *propOracle) refusal(what string, err error, justified map[pitrarchive.Token]bool) {
	ref, ok := pitrplan.RefusalOf(err)
	if !ok {
		o.t.Fatalf("%s: non-refusal error %v", what, err)
	}
	if !justified[ref.Token] {
		o.t.Fatalf("%s: the model does not justify the refusal (justified: %v): %v", what, justified, err)
	}
}

func (o *propOracle) checkTimestamp(what string, T time.Time, X pitrplan.Set, plan *pitrplan.Plan, err error) {
	t, m := o.t, o.p.model
	justified := map[pitrarchive.Token]bool{}
	if T.UnixMicro() <= o.maxOctB || T.Before(o.p.backup.ServerClock.End.Add(time.Second)) {
		justified[pitrarchive.TokenBackupAfterTarget] = true
	}
	w, found := o.witness(T, X)
	if !found {
		justified[pitrarchive.TokenTargetNotCovered] = true
		if err == nil {
			t.Fatalf("%s: certified %+v, but the model has no witness", what, plan.Witness)
		}
		o.refusal(what, err, justified)
		return
	}
	lambda, ok := m.executedBefore(m.origin[w], w)
	if !ok {
		t.Fatalf("%s: model witness %s is not in its origin's log", what, w)
	}
	refD := o.refD(T, lambda)
	if !o.originCopy(w) && !o.anchorable(w) {
		justified[pitrarchive.TokenStopAmbiguous] = true
	}
	if !lambda.Contains(o.B) {
		justified[pitrarchive.TokenBackupNotOnLineage] = true
	}
	if !X.Contains(refD) {
		justified[pitrarchive.TokenArchiveDivergentHistory] = true
	} else if !o.bPositioned() || !o.archived.Union(o.B).Contains(lambda.Subtract(X)) {
		// The gap search runs after the D window, so it cannot answer for
		// an unexcluded D.
		justified[pitrarchive.TokenArchiveGap] = true
	}
	if err != nil {
		o.refusal(what, err, justified)
		return
	}
	if plan.Witness == nil || plan.Witness.GTID != w {
		t.Fatalf("%s: witness %+v, the model says %s", what, plan.Witness, w)
	}
	if !plan.Lineage.Subtract(X).Equal(lambda.Subtract(X)) {
		t.Fatalf("%s: Λ %s, %s's origin had %s", what, plan.Lineage, w, lambda)
	}
	if !X.Contains(refD) || !refD.Equal(plan.Divergent) {
		t.Fatalf("%s: model D %s (must be ⊆ X), planner D %s", what, refD, plan.Divergent)
	}
	o.checkSegments(what, X, plan)
}

func (o *propOracle) checkLatest(what string, X pitrplan.Set, plan *pitrplan.Plan, err error) {
	t, p := o.t, o.p
	justified := map[pitrarchive.Token]bool{}
	lostArchived := p.dead.Intersect(o.archived).Subtract(X)
	lost := o.modelLost(X)
	if !lostArchived.IsEmpty() || !lost.IsEmpty() {
		justified[pitrarchive.TokenArchiveDivergentHistory] = true
		justified[pitrarchive.TokenArchiveSplitBrain] = true
	}
	if p.dead.Overlaps(o.B) {
		justified[pitrarchive.TokenBackupNotOnLineage] = true
	}
	replay := false
	for _, row := range o.rows {
		end := row.Hints.Previous.Union(row.Hints.GTIDs)
		if end.Contains(o.B) && !row.Hints.GTIDs.Subtract(o.B).Subtract(X).IsEmpty() {
			replay = true
		}
		if !o.archived.Union(o.B).Contains(end.Subtract(X)) {
			justified[pitrarchive.TokenArchiveGap] = true
		}
	}
	if !replay {
		justified[pitrarchive.TokenNoArchivedReplay] = true
	}
	if !o.bPositioned() {
		justified[pitrarchive.TokenArchiveGap] = true
	}
	if err != nil {
		o.refusal(what, err, justified)
		return
	}
	if !p.model.heldState(plan.Lineage) || plan.Lineage.Subtract(X).Overlaps(p.dead) {
		t.Fatalf("%s: latest Λ %s is not a server state, or holds lost %s", what, plan.Lineage, p.dead)
	}
	// Loss evidence is about the head's own transactions: a record
	// proves lost the members of Λ it lacks.
	if l := lost.Intersect(plan.Lineage); !l.IsEmpty() {
		t.Fatalf("%s: certified Λ %s although the model's loss evidence %s is not excluded", what, plan.Lineage, l)
	}
	if want := o.modelLost(pitrplan.Set{}).Intersect(plan.Lineage); !plan.Lost.Equal(want) || !p.dead.Contains(plan.Lost) {
		t.Fatalf("%s: reported loss evidence %s; the model's is %s, the lost transactions %s", what, plan.Lost, want, p.dead)
	}
	o.checkSegments(what, X, plan)
}

// checkSegments re-derives every segment from the indexed bytes of the file
// it names (F232): its start predecessors are that file's Previous_gtids ∪
// baseGtids ∪ the transactions before Start, it applies exactly the
// unexcluded transactions of [Start, End), and applied to B in order the
// segments yield Λ ∖ X.
func (o *propOracle) checkSegments(what string, X pitrplan.Set, plan *pitrplan.Plan) {
	t := o.t
	state := o.B
	for k, s := range plan.Segments {
		if s.File < 0 || s.File >= len(plan.Files) {
			t.Fatalf("%s: segment %d names file %d of %d", what, k, s.File, len(plan.Files))
		}
		pf := plan.Files[s.File]
		o.mu.Lock()
		idx := o.fetched[pf.Row]
		o.mu.Unlock()
		row, listed := o.rows[pf.Row]
		if idx == nil || !listed {
			t.Fatalf("%s: segment %d names %+v, which was not fetched (%v) or is not listed (%v)", what, k, pf.Row, idx != nil, listed)
		}
		if pf.Digest != idx.Digest || s.Start < 0 || s.End > len(idx.Txns) || s.Start > s.End {
			t.Fatalf("%s: segment %d [%d,%d) of %s (digest %s) does not fit the fetched file (%d transactions, digest %s)",
				what, k, s.Start, s.End, pf.Row.Name, pf.Digest, len(idx.Txns), idx.Digest)
		}
		pred := idx.Previous.Union(row.Base)
		for i := 0; i < s.Start; i++ {
			pred = pred.Union(pitrplan.SetOf(idx.GTID(i)))
		}
		if !pred.Equal(s.StartPredecessors) || !pred.Subtract(X).Equal(state) {
			t.Fatalf("%s: segment %d starts at %s (file says %s), state %s", what, k, s.StartPredecessors, pred, state)
		}
		applied := pitrplan.Set{}
		var n, first, last int64
		for i := s.Start; i < s.End; i++ {
			g := idx.GTID(i)
			if X.Has(g) {
				continue
			}
			applied = applied.Union(pitrplan.SetOf(g))
			n++
			if first == 0 {
				first = idx.Txns[i].OCT
			}
			last = idx.Txns[i].OCT
		}
		if !applied.Equal(s.GTIDs) || n != s.Transactions || first != s.FirstCommit || last != s.LastCommit || applied.Overlaps(state) {
			t.Fatalf("%s: segment %d declares %s (%d, %d..%d); its file range holds %s (%d, %d..%d), state %s",
				what, k, s.GTIDs, s.Transactions, s.FirstCommit, s.LastCommit, applied, n, first, last, state)
		}
		state = state.Union(applied)
	}
	if !state.Equal(plan.Lineage.Subtract(X)) {
		t.Fatalf("%s: final %s, Λ∖X %s", what, state, plan.Lineage.Subtract(X))
	}
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

// TestPITRPlanPropertyProductionMargin plans with the production clock-skew
// margin (F231) over a planned failover to b and failback to a in which b
// never seals a binlog, so b's era is archived only as replica copies in
// a's file and every target inside it needs b's promotion record as
// anchor. Spaced, b commits more than δ after its record and those targets
// are certified; unspaced, b commits within δ and they refuse
// PITRStopAmbiguous. The model oracle checks every other answer.
func TestPITRPlanPropertyProductionMargin(t *testing.T) {
	delta := pitrplan.DefaultClockSkewMargin
	for _, spaced := range []bool{true, false} {
		t.Run(fmt.Sprintf("spaced=%v", spaced), func(t *testing.T) {
			t.Parallel()
			p := newPropTrial(t)
			first := p.w.manifest("a").Promotions[0].ObservedAt
			for range 3 {
				p.commit("a")
			}
			p.takeBackup("a")
			for range 3 {
				p.commit("a")
				p.target("a")
			}
			if spaced {
				// b's record must not fall within δ of a's.
				sleepPast(first, delta)
			}
			recB := p.failover("a", "b", 0, false)
			if spaced {
				sleepPast(recB, delta)
			}
			var eraB []time.Time
			for range 3 {
				p.commit("b")
				eraB = append(eraB, p.target("b"))
			}
			p.failover("b", "a", 0, false)
			for range 2 {
				p.commit("a")
				p.target("a")
			}
			p.srv["a"].rotate()
			p.waitUploaded("a")
			o := newPropOracle(t, p, delta)
			for _, T := range eraB {
				w, _ := o.witness(T, pitrplan.Set{})
				if o.originCopy(w) {
					t.Fatalf("the fixture archived b's commit %s on b", w)
				}
			}
			res := o.run(fmt.Sprintf("spaced=%v", spaced))
			for _, r := range res {
				inB := false
				for _, T := range eraB {
					inB = inB || r.T.Equal(T)
				}
				switch {
				case r.mode == pitrplan.ModeLatest && r.err != nil:
					t.Errorf("latest mode refused: %v", r.err)
				case inB && spaced && r.err != nil:
					t.Errorf("T=%s inside b's era, more than δ after b's record: %v", r.T.Format(time.RFC3339Nano), r.err)
				case inB && !spaced:
					if ref, ok := pitrplan.RefusalOf(r.err); !ok || ref.Token != pitrarchive.TokenStopAmbiguous {
						t.Errorf("T=%s inside b's era, within δ of b's record: want PITRStopAmbiguous, got %v", r.T.Format(time.RFC3339Nano), r.err)
					}
				}
			}
		})
	}
}
