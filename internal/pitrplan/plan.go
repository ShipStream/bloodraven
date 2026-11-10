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
	Divergent           Set // D (⊆ X on success; the set to exclude on a DivergentHistory refusal)
	OnHistoryExclusions Set // D-48
	Lost                Set // latest-mode loss evidence (⊆ X on success)

	// BackupNewestCommit is maxOct(B) in µs; 0 means −∞ (no archived
	// member of B was read). BackupFrontierProven is false when it is only
	// a lower bound: B-frontier bytes are missing, or a row holding members
	// of B was left unread on its unverified hint (D-52, review F005).
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

// Segment is `mysqlbinlog --start-position=<StartPosition>
// --include-gtids=<GTIDs>` over Files[File], over transactions [Start, End)
// of that file.
type Segment struct {
	File       int
	Start, End int
	// StartPosition is the file offset of the GTID event of the first
	// transaction the segment replays (the first of [Start, End) outside
	// X), so mysqlbinlog does not scan the file from its start (issue
	// #271). No transaction before it is in GTIDs: a file holds a GTID
	// once.
	StartPosition     int64
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
	// ord is the file's index in planner.order.
	ord int
	// sids maps the file's source identifier indexes (Txn.SID) to the
	// planner's (planner.sidNames).
	sids []uint32
	// posW caches witnessPos.
	posW     int
	posWDone bool
}

func (f *file) n() int            { return len(f.idx.Txns) }
func (f *file) gtid(i int) GTID   { return f.idx.GTID(i) }
func (f *file) txn(i int) Txn     { return f.idx.Txns[i] }
func (f *file) origin(i int) bool { return f.idx.Txns[i].Srv == f.idx.WriterServerID }

// lostObjectAdvice is the remedy for a row whose object cannot be read
// (review F063). pitr-quarantine writes only an archive its caller owns,
// so a reader of an archive copy repairs the copy, or has the row
// quarantined at the source.
const lostObjectAdvice = "restore the object (in an archive copy, copy it again from the source archive); if it is lost at the source, quarantine the row there with bloodraven pitr-quarantine, and let the copy take the source's updated manifest"

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
	// promos are the promotion records: anchors and loss evidence.
	// fences are every record of every kind, including the promotion
	// records: a site's record observed after an anchor and not provably
	// after the witness fences that anchor off (§4.4, N3 revised).
	promos []promoRef
	fences []promoRef
	sites  []string

	B, X  Set
	T     int64 // µs; 0 in latest mode
	delta int64 // µs

	files  map[*Row]*file
	failed map[*Row]error
	order  []*file
	// occ is the occurrence registry (occ.go), keyed by the planner's
	// index of a GTID's source identifier and its GNO.
	occ      occTable
	sidIndex map[string]uint32
	sidNames []string
	mismatch *Refusal
	// reissue is the first downloaded copy of a member of B committed
	// after the dump finished (D-49 over every downloaded row, review
	// F050); serverEnd is the dumped server's clock at the dump's end (µs).
	reissue   *Refusal
	serverEnd int64

	// used are the rows whose bytes the plan relies on (closure U).
	used map[*Row]bool

	maxOctB     int64 // math.MinInt64 = −∞
	maxOctBGTID GTID
	maxOctBFile *file

	w      *Witness
	lambda Set
	// replayKnown is set once plan.Replay (P) is final.
	replayKnown bool

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
		occ: occTable{b: req.Budget}, sidIndex: map[string]uint32{}, used: map[*Row]bool{}, maxOctB: math.MinInt64, delta: req.Delta.Microseconds()}
	p.X = req.Exclude
	if p.X == nil {
		p.X = Set{}
	}
	b, err := ParseSet(req.Dump.GTIDExecuted)
	if err != nil {
		return nil, refuse(pitrarchive.TokenBackupInconsistent, "dump.json gtidExecuted %q is not a GTID set: %v", req.Dump.GTIDExecuted, err)
	}
	p.B = b
	p.serverEnd = req.Dump.ServerClock.End.UnixMicro()
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
				return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "site %s record %d has gtidExecuted %q: %v", sm.Site, i, rec.GTIDExecuted, err)
			}
			if err := p.budget.chargeSet(set, "promotion records"); err != nil {
				return nil, err
			}
			ref := promoRef{site: sm.Site, index: i, rec: rec, set: set}
			p.fences = append(p.fences, ref)
			if rec.Kind == pitrarchive.RecordPromotion {
				p.promos = append(p.promos, ref)
			}
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

// markedMayHold refuses PITRArchiveUnsafeHistory when an active row of a
// marked site satisfies may: the answer may be in bytes that cannot
// certify history.
func (p *planner) markedMayHold(may func(*Row) bool, what string) error {
	for _, r := range p.rows {
		if r.State == RowActive && p.isMarked(r.Site) && may(r) {
			return p.unsafe(r.Site, fmt.Sprintf("%s, and %s may hold it", what, r.Label()))
		}
	}
	return nil
}

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
	// are read (D-42). A count too large to multiply is over any budget.
	// With a spill arena the index need not fit in memory, and what the
	// staging volume can take is known only when it is asked.
	if r.Hints.Known && !p.budget.Spilling() {
		est := int64(math.MaxInt64)
		if n := r.Hints.GTIDs.Count(); n <= math.MaxInt64/(TxnBytes+occurrenceBytes) {
			est = n * (TxnBytes + occurrenceBytes)
		}
		if err := p.budget.Charge(est, "estimated index of "+r.Label()); err != nil {
			return nil, err
		}
		p.budget.Release(est)
	}
	var idx *FileIndex
	var err error
	if sf, ok := p.fetcher.(StagingFetcher); ok {
		idx, err = sf.FetchForPlan(p.ctx, r.ref(), p.mayStage(r))
	} else {
		idx, err = p.fetcher.Fetch(p.ctx, r.ref())
	}
	p.plan.Downloaded++
	if err != nil {
		if ref, ok := RefusalOf(err); ok && ref.Token != pitrarchive.TokenArchiveObjectInvalid {
			// Semantic (NamespaceReuse, ReplayUnsupported) and resource
			// (PlanTooLarge) refusals hold for the authentic bytes.
			return nil, err
		}
		if p.ctx.Err() != nil {
			return nil, p.ctx.Err()
		}
		if IsTransient(err) {
			return nil, err // not evidence about the row; ends planning
		}
		// A missing object, or bytes that fail their content integrity
		// (digest, framing, CRC: ObjectInvalid), are the same evidence:
		// the row's authentic bytes cannot be read. Each caller decides
		// what that costs, which lets a corrupt quarantined row take the
		// waiver (review F052).
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

// mayStage reports whether r could become a plan file: an active row of
// an unmarked site that may hold a member of P (or, before P is known,
// one not wholly inside B ∪ X, which no segment replays).
func (p *planner) mayStage(r *Row) bool {
	if r.State != RowActive || p.isMarked(r.Site) {
		return false
	}
	if p.replayKnown {
		return r.Hints.mayMeet(p.plan.Replay)
	}
	return !r.Hints.Known || !p.B.Union(p.X).Contains(r.Hints.GTIDs)
}

func (p *planner) ingest(r *Row, idx *FileIndex) (*file, error) {
	prev := idx.Previous.Union(r.Base)
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
	// prev and exec (⊆ prev ∪ gtids) are both retained (review F047:
	// prev was uncharged).
	if err := p.budget.Charge(2*setBytes(prev)+setBytes(idx.GTIDs), "file sets"); err != nil {
		return nil, err
	}
	f := &file{row: r, idx: idx, prev: prev, exec: prev.Union(idx.GTIDs), marked: p.isMarked(r.Site), ord: len(p.order)}
	if err := p.mapSources(f); err != nil {
		return nil, err
	}
	for i, t := range idx.Txns {
		if p.reissue == nil && t.OCT > p.serverEnd+p.delta {
			if g := f.gtid(i); p.B.Has(g) {
				p.reissue = p.reissueRefusal(g, f, t.OCT)
			}
		}
		sid := f.sids[t.SID]
		o, ok := p.occ.get(sid, t.GNO)
		if !ok {
			if err := p.occ.put(sid, t.GNO, occRec{file: uint32(f.ord) + 1, i: uint32(i), n: 1}); err != nil { //nolint:gosec // file and transaction counts fit: both index arrays
				return nil, err
			}
			continue
		}
		changed := false
		if o.n < math.MaxUint16 {
			o.n++
			changed = true
		}
		if p.mismatch == nil {
			// The copy this one is compared with: the first, or the first
			// that has an effect digest. A file holds a GTID once
			// (IndexBuilder), so it is in an earlier file.
			of := f
			if int(o.file)-1 < len(p.order) {
				of = p.order[o.file-1]
			}
			first := of.txn(int(o.i))
			g := f.gtid(i)
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
			// Row changes (issue #268) are compared only between two
			// comparable copies; a zero digest says nothing.
			effects := !first.Effect.IsZero() && !t.Effect.IsZero() && first.Effect != t.Effect
			if effects {
				why = append(why, fmt.Sprintf("row changes differ: effect digest %x vs %x", first.Effect, t.Effect))
			}
			switch {
			case effects && len(why) == 1:
				// The copies agree on everything a GTID event says.
				p.mismatch = refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s differs between %s and %s (%s); one copy is not a faithful copy of the other: a transaction with other changes was injected under this GTID (a manual repair with SET GTID_NEXT), or the two servers' data had diverged when they applied it; restore from a full backup taken later, once its newest transactions are archived in binlog files after these two",
					g, of.row.Label(), r.Label(), why[0])
			case len(why) > 0:
				p.mismatch = refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s differs between %s and %s (%s); the GTID was reissued, or one site's history was rewritten",
					g, of.row.Label(), r.Label(), joinComma(why))
			case first.Effect.IsZero() && !t.Effect.IsZero():
				// Later copies are compared with a comparable one: the
				// registry keeps the first copy that has a digest, so two
				// comparable copies always meet, whatever the download
				// order.
				o.file, o.i = uint32(f.ord)+1, uint32(i) //nolint:gosec // as above
				changed = true
			}
		}
		if changed {
			if err := p.occ.put(sid, t.GNO, o); err != nil {
				return nil, err
			}
		}
	}
	p.files[r] = f
	p.order = append(p.order, f)
	p.plan.IndexedTransactions += int64(len(idx.Txns))
	return f, nil
}

// mapSources fills f.sids, registering the file's source identifiers the
// planner has not seen.
func (p *planner) mapSources(f *file) error {
	if err := p.budget.Charge(4*int64(len(f.idx.SIDs)), "file source identifiers"); err != nil {
		return err
	}
	f.sids = make([]uint32, len(f.idx.SIDs))
	for k, sid := range f.idx.SIDs {
		id, ok := p.sidIndex[sid]
		if !ok {
			if err := p.budget.Charge(sourceBytes+sidBytes(sid), "source identifiers"); err != nil {
				return err
			}
			id = uint32(len(p.sidNames)) //nolint:gosec // one per source identifier, each charged
			p.sidIndex[sid] = id
			p.sidNames = append(p.sidNames, sid)
		}
		f.sids[k] = id
	}
	return nil
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
				return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold %s (%s); %s", err, g, purpose, lostObjectAdvice)
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
	// A write-back of spilled data that failed makes whatever was computed
	// over it unreliable, a refusal as much as a plan.
	if serr := p.budget.SpillErr(); serr != nil {
		err = serr
	}
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
		// A marked site may hold the target's witness: refuse as unsafe
		// history, not as an archive that has yet to catch up (whose
		// remedies cannot help a site that stopped uploading; review F057).
		bx := p.B.Union(p.X)
		may := func(r *Row) bool { return r.Hints.hi() >= p.T && (!r.Hints.Known || !bx.Contains(r.Hints.GTIDs)) }
		if err := p.markedMayHold(may, "no unmarked site archived a transaction at or after "+fmtMicros(p.T)); err != nil {
			return err
		}
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
	p.replayKnown = true
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
	if p.reissue != nil {
		return p.reissue
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
	if err := p.fillSegments(res); err != nil {
		return err
	}
	pl.Outcome = outcome
	for _, rule := range p.req.Rules {
		if err := rule(pl); err != nil {
			pl.Outcome = ""
			return err
		}
	}
	return nil
}
