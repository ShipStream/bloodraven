package pitrplan

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stagingSynth struct {
	s        *synth
	mayStage map[string]bool
}

func (f *stagingSynth) Fetch(ctx context.Context, r RowRef) (*FileIndex, error) {
	return f.FetchForPlan(ctx, r, true)
}

func (f *stagingSynth) FetchForPlan(_ context.Context, r RowRef, mayStage bool) (*FileIndex, error) {
	f.mayStage[r.Site+"/"+r.Name] = mayStage
	return f.s.idx[r], nil
}

// P2: the planner tells a staging fetcher which rows may become plan
// files; every staged file was fetched as one, and a row wholly inside B
// was not.
func TestFetchForPlanMayStage(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
	s.file("a", "a2", 101, "A:1-70", "", run("A", 71, 100, 71, 101))
	// b0 holds only members of B; the identity closure downloads it
	// because it overlaps a1.
	s.file("b", "b0", 102, "A:1-30", "", run("A", 31, 35, 31, 101))
	s.backup("A:1-35", "a", 101, 35.5)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	f := &stagingSynth{s: s, mayStage: map[string]bool{}}
	p, err := Run(context.Background(), Request{Mode: ModeTimestamp, Stop: time.UnixMicro(sec(80.5)).UTC(), Dump: s.dump, Budget: NewBudget(64 << 20)}, a, f)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Files) == 0 {
		t.Fatal("no plan files")
	}
	for _, pf := range p.Files {
		if !f.mayStage[pf.Row.Site+"/"+pf.Row.Name] {
			t.Errorf("plan file %s/%s was fetched with mayStage=false", pf.Row.Site, pf.Row.Name)
		}
	}
	if ms, ok := f.mayStage["b/b0"]; !ok || ms {
		t.Errorf("b/b0 (A:31-35 ⊆ B): fetched=%v mayStage=%v, want fetched without staging", ok, ms)
	}
}

// P4: a transient store error ends planning as itself: no refusal, and the
// row is not treated as unreadable (whose remedies include quarantining
// it).
func TestFetchTransientErrorIsNotUnreadable(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 70, 1, 101))
	s.backup("A:1-35", "a", 101, 35.5)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	reset := errors.New("connection reset by peer")
	fetch := FetchFunc(func(context.Context, RowRef) (*FileIndex, error) { return nil, &TransientError{Err: reset} })
	_, err := Run(context.Background(), Request{Mode: ModeTimestamp, Stop: time.UnixMicro(sec(50.5)).UTC(), Dump: s.dump, Budget: NewBudget(64 << 20)}, a, fetch)
	if _, ok := RefusalOf(err); ok || !IsTransient(err) || !errors.Is(err, reset) || isUnreadable(err) {
		t.Fatalf("err = %v (%T), want the transient error itself", err, err)
	}
}
