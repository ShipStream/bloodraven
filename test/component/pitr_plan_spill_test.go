package component

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/pitrplan/spillfile"
)

// propSpill plans every query of the property test a second time with the
// planner's arrays in a file arena (issue #266) and requires the same
// coverage.json and the same refusal, byte for byte. Where an array lives
// must never change a plan.
type propSpill struct {
	arena *spillfile.Arena
}

// newPropSpill opens an arena in a temporary directory. It reports false
// where the platform has no file-backed spill.
func newPropSpill(t *testing.T) (*propSpill, bool) {
	t.Helper()
	arena, err := spillfile.New(filepath.Join(t.TempDir(), "spill"), spillfile.Options{SlabBytes: 1 << 20})
	if errors.Is(err, spillfile.ErrUnsupported) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := arena.Check(); err != nil {
			t.Errorf("spill arena: %v", err)
		}
		_ = arena.Close()
	})
	return &propSpill{arena: arena}, true
}

// budget is a planner budget that spills every array, however small, and
// writes back after 16 pages.
func (s *propSpill) budget() *pitrplan.Budget {
	b := pitrplan.NewBudget(64 << 20)
	b.SetSpill(pitrplan.SpillOptions{Arena: s.arena, HeapMax: 0, MinBytes: 1, DirtyMax: 1})
	return b
}

func propCoverage(t *testing.T, plan *pitrplan.Plan, err error) []byte {
	t.Helper()
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	if plan == nil {
		return []byte("no plan: " + reason)
	}
	data, merr := plan.Coverage(pitrplan.CoverageArchive{}, pitrplan.CoverageDump{}, reason).Marshal()
	if merr != nil {
		t.Fatal(merr)
	}
	return append(data, reason...)
}

// mustMatchSpilled replans one query of o with spilled arrays and compares
// it with the in-memory answer.
func (o *propOracle) mustMatchSpilled(s *propSpill, what string, mode pitrplan.Mode, T time.Time, X pitrplan.Set, plan *pitrplan.Plan, err error) {
	o.t.Helper()
	budget := s.budget()
	spilled, serr := pitrplan.Run(context.Background(), pitrplan.Request{Mode: mode, Stop: T, Exclude: X, Delta: o.delta, Dump: o.p.backup, Budget: budget},
		o.arch, o.fetcher(budget))
	if budget.SpillPeak() == 0 && budget.Peak() > 0 && plan != nil && plan.IndexedTransactions > 0 {
		o.t.Fatalf("%s: nothing was spilled", what)
	}
	if want, got := propCoverage(o.t, plan, err), propCoverage(o.t, spilled, serr); !bytes.Equal(want, got) {
		o.t.Fatalf("%s: the spilled plan differs from the in-memory plan\nin memory:\n%s\nspilled:\n%s", what, want, got)
	}
}
