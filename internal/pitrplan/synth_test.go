package pitrplan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// synth builds archives from synthetic FileIndexes (no binlog bytes). Source
// identifiers are short strings ("A", "B"), which the GTID set parser
// accepts; sid(g) is then the whole identifier.

var synthEpoch = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

// sec is a commit time s seconds after synthEpoch, in µs.
func sec(s float64) int64 { return synthEpoch.Add(time.Duration(s * float64(time.Second))).UnixMicro() }

type stx struct {
	g     string // "A:5"
	oct   int64
	srv   uint32
	empty bool
	xa    bool
}

// run builds sequential txns sid:from-to with commit times start, start+1s, …
func run(sid string, from, to int64, start float64, srv uint32) []stx {
	var out []stx
	for n := from; n <= to; n++ {
		out = append(out, stx{g: fmt.Sprintf("%s:%d", sid, n), oct: sec(start + float64(n-from)), srv: srv})
	}
	return out
}

type synth struct {
	t     *testing.T
	sites map[string]*SiteManifest
	order []string
	idx   map[RowRef]*FileIndex
	lost  map[RowRef]bool
	// corrupt rows fail their content integrity check (a *Refusal with
	// ObjectInvalid, as a digest or CRC mismatch does).
	corrupt map[RowRef]bool
	fetches map[string]int
	dump    backupmeta.DumpMeta
	budget  int64
}

func newSynth(t *testing.T) *synth {
	return &synth{t: t, sites: map[string]*SiteManifest{}, idx: map[RowRef]*FileIndex{}, lost: map[RowRef]bool{}, corrupt: map[RowRef]bool{}, fetches: map[string]int{}}
}

func (s *synth) site(name string) *SiteManifest {
	if m := s.sites[name]; m != nil {
		return m
	}
	m := &SiteManifest{Site: name}
	s.sites[name] = m
	s.order = append(s.order, name)
	return m
}

func mustGTID(t *testing.T, g string) GTID {
	set := MustParseSet(g)
	first, ok := set.First()
	if !ok || set.Count() != 1 {
		t.Fatalf("bad GTID %q", g)
	}
	return first
}

// file archives an active row; it returns its ref.
func (s *synth) file(site, name string, fsrv uint32, prev, base string, groups ...[]stx) RowRef {
	s.t.Helper()
	b := NewIndexBuilder(site+"/"+name, NewBudget(1<<30))
	b.SetWriter(fsrv)
	if err := b.SetPrevious(MustParseSet(prev)); err != nil {
		s.t.Fatal(err)
	}
	for _, g := range groups {
		for _, t := range g {
			if err := b.BeginTxn(mustGTID(s.t, t.g), t.oct, t.srv); err != nil {
				s.t.Fatal(err)
			}
			if !t.empty {
				b.MarkData()
			}
			if t.xa {
				b.Mark(TxnXA)
			}
		}
	}
	idx, err := b.Finish(fmt.Sprintf("%s-%s", site, name), 100)
	if err != nil {
		s.t.Fatal(err)
	}
	ref := RowRef{Site: site, Name: name, ID: "id-" + name}
	s.idx[ref] = idx
	baseSet := MustParseSet(base)
	m := s.site(site)
	// Every synthetic file is opened at synthEpoch unless a test says
	// otherwise (Row.Created).
	m.Rows = append(m.Rows, Row{Site: site, Name: name, ID: ref.ID, Base: baseSet, Created: synthEpoch,
		Hints: Hints{Known: true, Previous: idx.Previous.Union(baseSet), GTIDs: idx.GTIDs, Empty: idx.EmptyGTIDs, Min: idx.CommitMin, Max: idx.CommitMax}})
	return ref
}

// quarantine moves the row to quarantined at time at.
func (s *synth) quarantine(ref RowRef, at time.Time) RowRef {
	m := s.sites[ref.Site]
	for i := range m.Rows {
		if m.Rows[i].Name == ref.Name && m.Rows[i].State == RowActive {
			m.Rows[i].State = RowQuarantined
			m.Rows[i].QuarantinedAt = at
			nref := ref
			nref.State = RowQuarantined
			s.idx[nref] = s.idx[ref]
			delete(s.idx, ref)
			return nref
		}
	}
	s.t.Fatalf("no row %v", ref)
	return ref
}

func (s *synth) promote(site, uuid string, srv uint32, executed string, at float64) {
	m := s.site(site)
	m.Promotions = append(m.Promotions, pitrarchive.PromotionRecord{ServerUUID: uuid, ServerID: srv, GTIDExecuted: executed,
		ObservedAt: synthEpoch.Add(time.Duration(at * float64(time.Second)))})
}

func (s *synth) backup(b, site string, srv uint32, end float64) {
	e := backupmeta.NewMicroTime(time.UnixMicro(sec(end)))
	s.dump = backupmeta.DumpMeta{Version: 1, GTIDExecuted: b, End: e, ServerClock: backupmeta.ServerClock{Start: e, End: e},
		Stamp: backupmeta.Stamp{Site: site, ServerID: srv, CreatedAt: e}}
}

var errSynthLost = errors.New("object missing")

func (s *synth) run(mode Mode, stop float64, exclude string, partial bool) (*Plan, error) {
	s.t.Helper()
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	limit := s.budget
	if limit == 0 {
		limit = 64 << 20
	}
	s.fetches = map[string]int{}
	var t time.Time
	if mode == ModeTimestamp {
		t = time.UnixMicro(sec(stop)).UTC()
	}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		s.fetches[r.Site+"/"+r.Name]++
		if s.lost[r] {
			return nil, errSynthLost
		}
		if s.corrupt[r] {
			return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "%s/%s (id %s): the bytes' digest does not match the row's content ID", r.Site, r.Name, r.ID)
		}
		idx := s.idx[r]
		if idx == nil {
			return nil, errSynthLost
		}
		return idx, nil
	})
	return Run(context.Background(), Request{Mode: mode, Stop: t, AllowPartial: partial, Exclude: MustParseSet(exclude), Dump: s.dump, Budget: NewBudget(limit)}, a, fetch)
}

func wantToken(t *testing.T, err error, tok pitrarchive.Token, subs ...string) {
	t.Helper()
	r, ok := RefusalOf(err)
	if !ok || r.Token != tok {
		t.Fatalf("got %v, want %s", err, tok)
	}
	for _, sub := range subs {
		if !strings.Contains(r.Error(), sub) {
			t.Errorf("refusal %q lacks %q", r.Error(), sub)
		}
	}
}

func segString(p *Plan) string {
	var parts []string
	for _, s := range p.Segments {
		f := p.Files[s.File]
		parts = append(parts, fmt.Sprintf("[%s/%s: %s]", f.Row.Site, f.Row.Name, s.GTIDs))
	}
	return strings.Join(parts, " ")
}
