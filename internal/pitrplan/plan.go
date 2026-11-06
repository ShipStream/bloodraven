package pitrplan

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Plan is the planner's result (§4.12). On refusal Run returns the plan
// filled as far as planning got, with no segments, so pitr-download can
// still write coverage.json.
type Plan struct {
	Mode         Mode
	Outcome      Outcome
	Stop         time.Time
	AllowPartial bool

	Baseline            Set // B
	Excluded            Set // X
	Lineage             Set // Λ
	Replay              Set // P
	PostStop            Set // Q
	Divergent           Set // D (⊆ X on success)
	OnHistoryExclusions Set // D-48
	Lost                Set // latest-mode loss evidence ⊆ X

	// BackupNewestCommit is maxOct(B) in µs; 0 means −∞ (no archived
	// member of B). BackupFrontierProven is false when B-frontier bytes are
	// missing (D-52).
	BackupNewestCommit   int64
	BackupFrontierProven bool

	Witness *Witness
	Head    *RowRef

	Files    []PlanFile
	Segments []Segment

	// Downloaded counts rows fetched (each at most once).
	Downloaded          int
	IndexedTransactions int64
	Sites               []string

	newestReplayed, replayedCount int64
}

// NewestReplayedCommit is the newest commit time replayed (µs; 0 if none).
func (p *Plan) NewestReplayedCommit() int64 { return p.newestReplayed }

// ReplayedTransactions is the number of transactions the segments apply.
func (p *Plan) ReplayedTransactions() int64 { return p.replayedCount }

// Witness is w and how Λ was established (§4.4).
type Witness struct {
	GTID        GTID
	Row         RowRef
	CommitTime  int64
	GroundTruth GroundTruth
	// AnchorRecords are "<site>#<index>" into that site's promotions.
	AnchorRecords []string
	// AnchorOriginRow is "<site>/<name>/<id>" of the origin row q, or "".
	AnchorOriginRow string
}

// PlanFile is one file to stage.
type PlanFile struct {
	Row      RowRef
	Digest   string
	Size     int64
	Previous Set // prev(f): PREVIOUS_GTIDS ∪ baseGtids
	GTIDs    Set
}

// StagedPath is the file's path relative to the staging directory.
func (f PlanFile) StagedPath() string {
	name := f.Row.Name
	if f.Row.ID != "" {
		name += "." + f.Row.ID
	}
	return f.Row.Site + "/" + name
}

// Segment is `mysqlbinlog --include-gtids=<GTIDs>` over Files[File], over
// transactions [Start, End) of that file.
type Segment struct {
	File              int
	Start, End        int
	GTIDs             Set
	StartPredecessors Set
	Transactions      int64
	FirstCommit       int64
	LastCommit        int64
}

// file is a downloaded row.
type file struct {
	row    *Row
	idx    *FileIndex
	prev   Set
	exec   Set
	marked bool
	seq    int
	// posW caches witnessPos.
	posW     int
	posWDone bool
}

func (f *file) n() int            { return len(f.idx.Txns) }
func (f *file) gtid(i int) GTID   { return f.idx.GTID(i) }
func (f *file) txn(i int) Txn     { return f.idx.Txns[i] }
func (f *file) origin(i int) bool { return f.idx.Txns[i].Srv == f.idx.WriterServerID }

type occRef struct {
	f *file
	i int32
	n uint16
}

// unreadableError marks a row whose bytes could not be read.
type unreadableError struct {
	row *Row
	err error
}

func (e *unreadableError) Error() string {
	return fmt.Sprintf("%s (id %s): %v", e.row.Label(), e.row.ID, e.err)
}
func (e *unreadableError) Unwrap() error { return e.err }

func isUnreadable(err error) bool {
	var u *unreadableError
	return errors.As(err, &u)
}

type promoRef struct {
	site  string
	index int
	rec   pitrarchive.PromotionRecord
	set   Set
}

type planner struct {
	ctx     context.Context
	req     Request
	fetcher Fetcher
	budget  *Budget

	rows   []*Row
	marked map[string]*pitrarchive.HistoryIntegrity
	promos []promoRef
	sites  []string

	B, X  Set
	T     int64 // µs; 0 in latest mode
	delta int64 // µs

	files    map[*Row]*file
	failed   map[*Row]error
	order    []*file
	occ      map[GTID]*occRef
	mismatch *Refusal

	// used are the rows whose bytes the plan relies on (closure U).
	used map[*Row]bool

	maxOctB      int64 // math.MinInt64 = −∞
	maxOctBGTID  GTID
	maxOctBFile  *file
	frontierMiss Set

	w      *Witness
	wFile  *file
	wPos   int
	lambda Set

	plan *Plan
}

func newPlanner(ctx context.Context, req Request, archive Archive, fetcher Fetcher) (*planner, error) {
	if req.Budget == nil {
		req.Budget = NewBudget(DefaultPlanMemoryBytes)
	}
	if req.Delta == 0 {
		req.Delta = DefaultClockSkewMargin
	}
	p := &planner{ctx: ctx, req: req, fetcher: fetcher, budget: req.Budget,
		marked: map[string]*pitrarchive.HistoryIntegrity{}, files: map[*Row]*file{}, failed: map[*Row]error{},
		occ: map[GTID]*occRef{}, used: map[*Row]bool{}, maxOctB: math.MinInt64, delta: req.Delta.Microseconds()}
	p.X = req.Exclude
	if p.X == nil {
		p.X = Set{}
	}
	b, err := ParseSet(req.Dump.GTIDExecuted)
	if err != nil {
		return nil, refuse(pitrarchive.TokenBackupInconsistent, "dump.json gtidExecuted %q is not a GTID set: %v", req.Dump.GTIDExecuted, err)
	}
	p.B = b
	if !req.Stop.IsZero() {
		p.T = req.Stop.UnixMicro()
	}
	for si := range archive.Sites {
		sm := &archive.Sites[si]
		p.sites = append(p.sites, sm.Site)
		if sm.Marked != nil {
			p.marked[sm.Site] = sm.Marked
		}
		for ri := range sm.Rows {
			r := sm.Rows[ri]
			r.Site = sm.Site
			p.rows = append(p.rows, &r)
		}
		for i, rec := range sm.Promotions {
			set, err := ParseSet(rec.GTIDExecuted)
			if err != nil {
				return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "site %s promotion record %d has gtidExecuted %q: %v", sm.Site, i, rec.GTIDExecuted, err)
			}
			if err := p.budget.chargeSet(set, "promotion records"); err != nil {
				return nil, err
			}
			p.promos = append(p.promos, promoRef{site: sm.Site, index: i, rec: rec, set: set})
		}
	}
	sort.Strings(p.sites)
	p.sortRows(p.rows)
	p.plan = &Plan{Mode: req.Mode, Stop: req.Stop, AllowPartial: req.AllowPartial, Baseline: p.B, Excluded: p.X,
		Lineage: Set{}, Replay: Set{}, PostStop: Set{}, Divergent: Set{}, OnHistoryExclusions: Set{}, Lost: Set{}}
	return p, nil
}

// rowLess is the file order (site, |hprev|, name, id) of §4.6.
func rowLess(a, b *Row) bool {
	if a.Site != b.Site {
		return a.Site < b.Site
	}
	if ca, cb := a.Hints.Previous.Count(), b.Hints.Previous.Count(); ca != cb {
		return ca < cb
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	if a.ID != b.ID {
		return a.ID < b.ID
	}
	return a.State < b.State
}

func (p *planner) sortRows(rows []*Row) {
	sort.SliceStable(rows, func(i, j int) bool { return rowLess(rows[i], rows[j]) })
}

func (p *planner) isMarked(site string) bool { return p.marked[site] != nil }

// fetch downloads and indexes a row at most once.
func (p *planner) fetch(r *Row) (*file, error) {
	if f := p.files[r]; f != nil {
		return f, nil
	}
	if err := p.failed[r]; err != nil {
		return nil, err
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	// The up-front estimate from hinted counts refuses before any bytes
	// are read (D-42).
	if r.Hints.Known {
		est := r.Hints.GTIDs.Count() * (TxnBytes + occurrenceBytes)
		if err := p.budget.Charge(est, "estimated index of "+r.Label()); err != nil {
			return nil, err
		}
		p.budget.Release(est)
	}
	idx, err := p.fetcher.Fetch(p.ctx, r.ref())
	p.plan.Downloaded++
	if err != nil {
		if _, ok := RefusalOf(err); ok {
			return nil, err
		}
		if p.ctx.Err() != nil {
			return nil, p.ctx.Err()
		}
		u := &unreadableError{row: r, err: err}
		p.failed[r] = u
		return nil, u
	}
	f, err := p.ingest(r, idx)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (p *planner) ingest(r *Row, idx *FileIndex) (*file, error) {
	prev := idx.Previous.Union(r.Base)
	f := &file{row: r, idx: idx, prev: prev, exec: prev.Union(idx.GTIDs), marked: p.isMarked(r.Site), seq: len(p.order)}
	if r.Base.Overlaps(idx.GTIDs) {
		return nil, refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s: %s are in the row's baseGtids and among its transactions; the GTIDs were reissued", r.Label(), r.Base.Intersect(idx.GTIDs))
	}
	if r.Hints.Known {
		var bad []string
		if !r.Hints.Previous.Equal(prev) {
			bad = append(bad, fmt.Sprintf("previous GTIDs %q, bytes %q", r.Hints.Previous, prev))
		}
		if !r.Hints.GTIDs.Equal(idx.GTIDs) {
			bad = append(bad, fmt.Sprintf("GTIDs %q, bytes %q", r.Hints.GTIDs, idx.GTIDs))
		}
		if !r.Hints.Empty.Equal(idx.EmptyGTIDs) {
			bad = append(bad, fmt.Sprintf("empty GTIDs %q, bytes %q", r.Hints.Empty, idx.EmptyGTIDs))
		}
		if r.Hints.timesKnown() && (r.Hints.Min != idx.CommitMin || r.Hints.Max != idx.CommitMax) {
			bad = append(bad, fmt.Sprintf("commit times [%d,%d], bytes [%d,%d]", r.Hints.Min, r.Hints.Max, idx.CommitMin, idx.CommitMax))
		}
		if len(bad) > 0 {
			return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "%s (id %s): the manifest hints disagree with the verified bytes: %v", r.Label(), r.ID, bad)
		}
	}
	if err := p.budget.Charge(int64(len(idx.Txns))*occurrenceBytes, "occurrence registry"); err != nil {
		return nil, err
	}
	if err := p.budget.chargeSet(f.exec, "file sets"); err != nil {
		return nil, err
	}
	for i, t := range idx.Txns {
		g := f.gtid(i)
		o := p.occ[g]
		if o == nil {
			p.occ[g] = &occRef{f: f, i: int32(i), n: 1}
			continue
		}
		if o.n < math.MaxUint16 {
			o.n++
		}
		if p.mismatch == nil {
			first := o.f.txn(int(o.i))
			var why []string
			if first.OCT != t.OCT {
				why = append(why, fmt.Sprintf("commit time %s vs %s", fmtMicros(first.OCT), fmtMicros(t.OCT)))
			}
			if first.Srv != t.Srv {
				why = append(why, fmt.Sprintf("origin server %d vs %d", first.Srv, t.Srv))
			}
			if first.Empty() != t.Empty() {
				why = append(why, "one copy is empty")
			}
			if len(why) > 0 {
				p.mismatch = refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s differs between %s and %s (%s); the GTID was reissued, or one site's history was rewritten",
					g, o.f.row.Label(), r.Label(), joinComma(why))
			}
		}
	}
	p.files[r] = f
	p.order = append(p.order, f)
	p.plan.IndexedTransactions += int64(len(idx.Txns))
	return f, nil
}

// pred materializes pred(f, i).
func (p *planner) pred(f *file, i int) (Set, error) {
	s := f.prev.Clone()
	for j := 0; j < i; j++ {
		s.add(f.gtid(j))
	}
	if err := p.budget.chargeSet(s, "predecessor sets"); err != nil {
		return nil, err
	}
	return s, nil
}

// position returns the index of g in f, or -1.
func position(f *file, g GTID) int {
	if !f.idx.GTIDs.Has(g) {
		return -1
	}
	for i := range f.idx.Txns {
		if t := f.idx.Txns[i]; t.GNO == g.GNO && f.idx.SIDs[t.SID] == g.SID {
			return i
		}
	}
	return -1
}

type occurrence struct {
	f *file
	i int
}

// occurrences downloads every active row that may hold g (marked ones
// included) and returns g's occurrences in file order. An unreadable row
// that may hold g refuses: the copies cannot be compared.
func (p *planner) occurrences(g GTID, purpose string) ([]occurrence, error) {
	one := SetOf(g)
	for _, r := range p.rows {
		if r.State != RowActive || !r.Hints.mayMeet(one) {
			continue
		}
		if _, err := p.fetch(r); err != nil {
			if isUnreadable(err) {
				return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold %s (%s); quarantine the row with bloodraven pitr-quarantine if its object is lost", err, g, purpose)
			}
			return nil, err
		}
	}
	var out []occurrence
	for _, r := range p.rows {
		f := p.files[r]
		if f == nil || r.State != RowActive {
			continue
		}
		if i := position(f, g); i >= 0 {
			out = append(out, occurrence{f, i})
		}
	}
	return out, nil
}

func fmtMicros(us int64) string {
	if us == 0 || us == math.MinInt64 {
		return "-∞"
	}
	return time.UnixMicro(us).UTC().Format("2006-01-02T15:04:05.000000Z")
}

func joinComma(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}

// Run plans a replay. It returns the plan, and a *Refusal (or a context or
// fetch error) when the restore must not proceed; the plan is non-nil
// whenever planning started, so coverage.json can record the refusal.
func Run(ctx context.Context, req Request, archive Archive, fetcher Fetcher) (*Plan, error) {
	p, err := newPlanner(ctx, req, archive, fetcher)
	if err != nil {
		return &Plan{Mode: req.Mode, Stop: req.Stop, AllowPartial: req.AllowPartial, Excluded: req.Exclude}, err
	}
	err = p.run()
	if err != nil {
		p.plan.Files, p.plan.Segments, p.plan.Sites = nil, nil, nil
	}
	return p.plan, err
}

func (p *planner) run() error {
	switch p.req.Mode {
	case ModeTimestamp:
		if p.T == 0 {
			return refuse(pitrarchive.TokenTargetInvalid, "timestamp mode needs a target")
		}
	case ModeLatest:
		if p.T != 0 || p.req.AllowPartial {
			return refuse(pitrarchive.TokenTargetInvalid, "latest mode takes no target and no allowPartial")
		}
	default:
		return refuse(pitrarchive.TokenTargetInvalid, "unknown mode %q", p.req.Mode)
	}
	if err := p.checkDump(); err != nil {
		return err
	}
	if inter := p.X.Intersect(p.B); !inter.IsEmpty() {
		return refuse(pitrarchive.TokenExcludeIntersectsBackup, "excludeGtids holds %s, which the backup already contains; a dump cannot unapply transactions; remove them from excludeGtids, or use a backup taken before them", inter.bounded(1024))
	}
	if err := p.frontier(); err != nil {
		return err
	}
	if p.req.Mode == ModeLatest {
		return p.latest(OutcomeArchiveHead)
	}
	found, err := p.witness()
	if err != nil {
		return err
	}
	if !found {
		if !p.req.AllowPartial {
			return refuse(pitrarchive.TokenTargetNotCovered, "the archive holds no transaction at or after %s; wait for archiving (the active binlog is archived after it rotates, e.g. FLUSH BINARY LOGS on the primary), or set allowPartial", fmtMicros(p.T))
		}
		return p.latest(OutcomePartial)
	}
	return p.timestamp()
}

func (p *planner) checkDump() error {
	d := p.req.Dump
	if d.End.IsZero() || d.ServerClock.End.IsZero() {
		return refuse(pitrarchive.TokenBackupIncomplete, "dump.json records no dump end (@.done.json end %q, serverClock.end %q); the dump did not finish", d.End, d.ServerClock.End)
	}
	if d.GTIDExecutedInconsistent {
		return refuse(pitrarchive.TokenBackupInconsistent, "the dump's gtidExecuted is marked inconsistent")
	}
	return nil
}

// timestamp continues timestamp mode once w and Λ are known (§4.5–§4.6).
func (p *planner) timestamp() error {
	pl := p.plan
	pl.Lineage = p.lambda
	if err := p.lineageL1(); err != nil {
		return err
	}
	if err := p.lineageL2(); err != nil {
		return err
	}
	pl.Replay = p.lambda.Subtract(p.B).Subtract(p.X)
	if err := p.fetchReplayRows(); err != nil {
		return err
	}
	if err := p.lineageL3(); err != nil {
		return err
	}
	if err := p.dWindow(); err != nil {
		return err
	}
	return p.searchAndFinish(OutcomeTargetCovered)
}

// searchAndFinish runs §4.11 steps 10–12 and fills the plan.
func (p *planner) searchAndFinish(outcome Outcome) error {
	pl := p.plan
	goal := p.lambda.Subtract(p.X)
	res, searchErr := p.search(goal)
	// Closure: U is the plan's segment files, or the whole search set when
	// no plan exists, plus the witness, anchor and frontier rows (§4.9).
	if res != nil {
		if searchErr == nil {
			for _, s := range res.segments {
				p.used[res.files[s.file].row] = true
			}
		} else {
			for _, f := range res.files {
				p.used[f.row] = true
			}
		}
	}
	if err := p.identity(); err != nil {
		return err
	}
	if p.mismatch != nil {
		return p.mismatch
	}
	if err := p.orderCheck(); err != nil {
		return err
	}
	if searchErr != nil {
		return searchErr
	}
	if err := p.checkL5(res); err != nil {
		return err
	}
	pl.Outcome = outcome
	p.fillSegments(res)
	for _, rule := range p.req.Rules {
		if err := rule(pl); err != nil {
			pl.Outcome = ""
			return err
		}
	}
	return nil
}
