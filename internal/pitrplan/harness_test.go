package pitrplan_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// The harness builds archives from real binlog bytes (binlogtest), with
// manifest hints computed by the sidecar's own inspection (InspectBinlog),
// and indexes downloads with the planner's index pass
// (sidecar.InspectTransactions). Each site writes one mixed file across
// role changes, as the operator does (§4.15 "real file layout").

// Short names for the examples' server UUIDs.
var uuids = map[string]string{
	"A":  "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	"Bu": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
	"Cu": "cccccccc-cccc-cccc-cccc-cccccccccccc",
}

// servers maps each UUID to its server_id (F26: 101 + index).
var servers = map[string]uint32{"A": 101, "Bu": 102, "Cu": 103}

var epoch = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

type world struct {
	t       *testing.T
	clock   map[string]time.Time // "A:5" → commit time
	now     time.Time
	sites   map[string]*sidecar.Manifest
	objects map[string][]byte
	fetches map[string]int
	dump    backupmeta.DumpMeta
	budget  int64
}

func newWorld(t *testing.T) *world {
	t.Helper()
	return &world{t: t, clock: map[string]time.Time{}, now: epoch, sites: map[string]*sidecar.Manifest{},
		objects: map[string][]byte{}, fetches: map[string]int{}}
}

// expand turns "A:1-3,Bu:7" into short GTID names in order.
func expand(spec string) []string {
	var out []string
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		short, rng, _ := strings.Cut(part, ":")
		lo, hi := rng, rng
		if a, b, ok := strings.Cut(rng, "-"); ok {
			lo, hi = a, b
		}
		var l, h int
		fmt.Sscan(lo, &l)
		fmt.Sscan(hi, &h)
		for n := l; n <= h; n++ {
			out = append(out, fmt.Sprintf("%s:%d", short, n))
		}
	}
	return out
}

// set converts short notation to a canonical set.
func (w *world) set(spec string) pitrplan.Set {
	s := pitrplan.Set{}.Union(nil)
	var parts []string
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		short, rng, _ := strings.Cut(part, ":")
		parts = append(parts, uuids[short]+":"+rng)
	}
	if len(parts) == 0 {
		return s
	}
	return pitrplan.MustParseSet(strings.Join(parts, ","))
}

// g is the full spelling of one short GTID.
func (w *world) g(short string) string {
	u, n, _ := strings.Cut(short, ":")
	return uuids[u] + ":" + n
}

// commit appends transactions to the global history, one second apart.
// Re-committing a GTID (a reissue) gives it a new time.
func (w *world) commit(spec string) {
	for _, g := range expand(spec) {
		w.now = w.now.Add(time.Second)
		w.clock[g] = w.now
	}
}

// commitAt sets one GTID's commit time explicitly.
func (w *world) commitAt(short string, at time.Time) {
	w.clock[short] = at
	if at.After(w.now) {
		w.now = at
	}
}

// pass advances the clock (a promotion, a pause).
func (w *world) pass(d time.Duration) time.Time {
	w.now = w.now.Add(d)
	return w.now
}

func (w *world) at(short string) time.Time {
	t, ok := w.clock[short]
	if !ok {
		w.t.Fatalf("no commit time for %s", short)
	}
	return t
}

// in returns a target in (t(prev), t(next)]: just before next.
func (w *world) in(next string) time.Time { return w.at(next).Add(-500 * time.Millisecond) }

func (w *world) manifest(site string) *sidecar.Manifest {
	m := w.sites[site]
	if m == nil {
		m = &sidecar.Manifest{Version: sidecar.ManifestVersion, ArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa", Site: site}
		w.sites[site] = m
	}
	return m
}

// file archives one binlog. Each spec element is short notation; a "~"
// suffix makes the transactions empty, "@<srv>" overrides their origin
// server_id.
func (w *world) file(site, name string, fsrv uint32, prev, base string, specs ...string) {
	w.t.Helper()
	var txns []binlogtest.Txn
	for _, spec := range specs {
		empty := false
		srvOverride := uint32(0)
		if s, at, ok := strings.Cut(spec, "@"); ok {
			spec = s
			var v int
			fmt.Sscan(at, &v)
			srvOverride = uint32(v)
		}
		if strings.HasSuffix(spec, "~") {
			spec, empty = strings.TrimSuffix(spec, "~"), true
		}
		for _, g := range expand(spec) {
			short, n, _ := strings.Cut(g, ":")
			var gno int64
			fmt.Sscan(n, &gno)
			srv := servers[short]
			if srvOverride != 0 {
				srv = srvOverride
			}
			txns = append(txns, binlogtest.Txn{SID: uuids[short], GNO: gno, Commit: w.at(g), ServerID: srv, Empty: empty})
		}
	}
	prevFull := w.set(prev).String()
	data := binlogtest.File{ServerID: fsrv, Previous: prevFull, Txns: txns, Created: epoch}.MustBytes()
	ev, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		w.t.Fatalf("inspect %s/%s: %v", site, name, err)
	}
	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])
	w.objects[site+"/"+name+"/"+id] = data
	m := w.manifest(site)
	m.Files = append(m.Files, sidecar.ManifestEntry{Name: name, ID: id, BaseGTIDs: w.set(base).String(), PreviousGTIDs: ev.Metadata.PreviousGTIDs,
		GTIDCoverage: ev.Metadata.GTIDCoverage, Size: int64(len(data)), FirstEventTime: ev.Metadata.FirstEventTime})
}

// lose deletes a row's object (the row stays listed).
func (w *world) lose(site, name string) {
	for k := range w.objects {
		if strings.HasPrefix(k, site+"/"+name+"/") {
			delete(w.objects, k)
		}
	}
}

// quarantine moves a row to Quarantined at the given time.
func (w *world) quarantine(site, name string, at time.Time) {
	m := w.manifest(site)
	for i, e := range m.Files {
		if e.Name == name {
			m.Quarantined = append(m.Quarantined, sidecar.ArchiveQuarantine{Entry: e, Reason: "lost", At: at})
			m.Files = append(m.Files[:i], m.Files[i+1:]...)
			return
		}
	}
	w.t.Fatalf("no row %s/%s", site, name)
}

// promote records a promotion of site's server (uuid short name).
func (w *world) promote(site, short, executed string, at time.Time) {
	m := w.manifest(site)
	m.AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: uuids[short], ServerID: servers[short], GTIDExecuted: w.set(executed).String(), ObservedAt: at})
}

// mark sets site's history-integrity marker.
func (w *world) mark(site string, v pitrarchive.HistoryViolation) {
	w.manifest(site).HistoryIntegrity = &pitrarchive.HistoryIntegrity{Violation: v, Detail: "test", At: w.now}
}

// backup sets the dump: B, the dumped site and server, and when it ended.
func (w *world) backup(b, site string, srv uint32, end time.Time) {
	created := end.Add(-time.Second)
	w.dump = backupmeta.DumpMeta{Version: backupmeta.DumpMetaVersion, Location: "s3://dumps/nightly", GTIDExecuted: w.set(b).String(),
		End: backupmeta.NewMicroTime(end), ServerClock: backupmeta.ServerClock{Start: backupmeta.NewMicroTime(created), End: backupmeta.NewMicroTime(end)},
		Stamp: backupmeta.Stamp{Kind: backupmeta.StampKind, Version: backupmeta.StampVersion, Site: site, ServerID: srv,
			Backup: backupmeta.ObjectRef{Namespace: "db", Name: "nightly", UID: "backup-uid"}, Source: backupmeta.ObjectRef{Namespace: "db", Name: "orders", UID: "group-uid"},
			ServerClock: backupmeta.ServerClock{Start: backupmeta.NewMicroTime(created), End: backupmeta.NewMicroTime(end)}, CreatedAt: backupmeta.NewMicroTime(created)}}
}

func (w *world) archive() pitrplan.Archive {
	w.t.Helper()
	var names []string
	for s := range w.sites {
		names = append(names, s)
	}
	sort.Strings(names)
	var a pitrplan.Archive
	for _, s := range names {
		sm, err := sidecar.PlanSite(w.sites[s])
		if err != nil {
			w.t.Fatal(err)
		}
		a.Sites = append(a.Sites, sm)
	}
	return a
}

var errNotFound = errors.New("object not found")

func (w *world) fetcher(budget *pitrplan.Budget) pitrplan.Fetcher {
	return pitrplan.FetchFunc(func(_ context.Context, r pitrplan.RowRef) (*pitrplan.FileIndex, error) {
		key := r.Site + "/" + r.Name + "/" + r.ID
		w.fetches[key]++
		data, ok := w.objects[key]
		if !ok {
			return nil, errNotFound
		}
		idx, err := sidecar.InspectTransactions(bytes.NewReader(data), sidecar.TxIndexOptions{Label: r.Site + "/" + r.Name, Budget: budget})
		if err != nil {
			return nil, err
		}
		if idx.Digest != r.ID {
			return nil, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: "digest mismatch"}
		}
		return idx, nil
	})
}

type query struct {
	mode    pitrplan.Mode
	stop    time.Time
	exclude string
	partial bool
}

func (w *world) plan(q query) (*pitrplan.Plan, error) {
	w.t.Helper()
	if q.mode == "" {
		q.mode = pitrplan.ModeTimestamp
	}
	limit := w.budget
	if limit == 0 {
		limit = 64 << 20
	}
	budget := pitrplan.NewBudget(limit)
	w.fetches = map[string]int{}
	return pitrplan.Run(context.Background(), pitrplan.Request{Mode: q.mode, Stop: q.stop, AllowPartial: q.partial, Exclude: w.set(q.exclude),
		Dump: w.dump, Budget: budget}, w.archive(), w.fetcher(budget))
}

// seg is an expected segment: "site/name" and short-notation GTIDs and
// start predecessors.
type seg struct{ file, gtids, start string }

type want struct {
	token   pitrarchive.Token
	msg     []string // substrings of the refusal (short GTIDs are expanded)
	outcome pitrplan.Outcome
	witness string
	truth   pitrplan.GroundTruth
	lambda  string
	replay  string
	div     string
	post    *string
	onHist  string
	lost    string
	segs    []seg
}

func (w *world) expandMsg(s string) string {
	for short, u := range uuids {
		s = strings.ReplaceAll(s, "{"+short+"}", u)
	}
	return s
}

func (w *world) check(name string, q query, want want) *pitrplan.Plan {
	w.t.Helper()
	p, err := w.plan(q)
	if want.token != "" {
		r, ok := pitrplan.RefusalOf(err)
		if !ok || r.Token != want.token {
			w.t.Fatalf("%s: got %v, want %s", name, err, want.token)
		}
		for _, sub := range want.msg {
			if sub = w.expandMsg(sub); !strings.Contains(r.Error(), sub) {
				w.t.Errorf("%s: refusal %q lacks %q", name, r.Error(), sub)
			}
		}
		return p
	}
	if err != nil {
		w.t.Fatalf("%s: unexpected refusal: %v", name, err)
	}
	if p.Outcome != want.outcome {
		w.t.Errorf("%s: outcome %s, want %s", name, p.Outcome, want.outcome)
	}
	if want.witness != "" {
		if p.Witness == nil || p.Witness.GTID.String() != w.g(want.witness) || p.Witness.GroundTruth != want.truth {
			w.t.Errorf("%s: witness %+v, want %s %s", name, p.Witness, want.witness, want.truth)
		}
	}
	eq := func(what string, got pitrplan.Set, spec string) {
		if !got.Equal(w.set(spec)) {
			w.t.Errorf("%s: %s = %s, want %s", name, what, got, w.set(spec))
		}
	}
	eq("Λ", p.Lineage, want.lambda)
	if want.replay != "" {
		eq("P", p.Replay, want.replay)
	}
	eq("D", p.Divergent, want.div)
	eq("onHistoryExclusions", p.OnHistoryExclusions, want.onHist)
	eq("lost", p.Lost, want.lost)
	if want.post != nil {
		eq("Q", p.PostStop, *want.post)
	}
	// P = Λ ∖ (B ∪ X), and the segments partition P in order.
	if exp := p.Lineage.Subtract(p.Baseline).Subtract(p.Excluded); !p.Replay.Equal(exp) {
		w.t.Errorf("%s: P = %s, want Λ∖(B∪X) = %s", name, p.Replay, exp)
	}
	var got []seg
	state := p.Baseline
	union := pitrplan.Set{}
	for _, s := range p.Segments {
		f := p.Files[s.File]
		got = append(got, seg{f.Row.Site + "/" + f.Row.Name, s.GTIDs.String(), s.StartPredecessors.String()})
		if !s.StartPredecessors.Subtract(p.Excluded).Equal(state) {
			w.t.Errorf("%s: segment %s starts at %s ∖ X ≠ state %s (R4)", name, f.Row.Name, s.StartPredecessors, state)
		}
		if union.Overlaps(s.GTIDs) {
			w.t.Errorf("%s: segments overlap", name)
		}
		union = union.Union(s.GTIDs)
		state = state.Union(s.GTIDs)
	}
	if !union.Equal(p.Replay) {
		w.t.Errorf("%s: ∪ segments = %s, want P = %s", name, union, p.Replay)
	}
	if !state.Equal(p.Lineage.Subtract(p.Excluded)) {
		w.t.Errorf("%s: final state %s, want Λ∖X", name, state)
	}
	var exp []seg
	for _, s := range want.segs {
		e := seg{s.file, w.set(s.gtids).String(), ""}
		if s.start != "" {
			e.start = w.set(s.start).String()
		}
		exp = append(exp, e)
	}
	ok := len(got) == len(exp)
	for i := 0; ok && i < len(got); i++ {
		ok = got[i].file == exp[i].file && got[i].gtids == exp[i].gtids && (exp[i].start == "" || got[i].start == exp[i].start)
	}
	if !ok {
		w.t.Errorf("%s: segments\n got %+v\nwant %+v", name, got, exp)
	}
	return p
}

func ptr(s string) *string { return &s }
