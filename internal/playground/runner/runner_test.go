package runner

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestRegistryRegisterListGet(t *testing.T) {
	r := &Registry{scenarios: map[string]Scenario{}}
	r.Register(Scenario{ID: "02", Title: "second"})
	r.Register(Scenario{ID: "01", Title: "first"})

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("List len = %d, want 2", len(list))
	}
	if list[0].ID != "01" || list[1].ID != "02" {
		t.Fatalf("List not lex-sorted: %v", []string{list[0].ID, list[1].ID})
	}

	if got, ok := r.Get("01"); !ok || got.Title != "first" {
		t.Fatalf("Get(01) = %+v ok=%v", got, ok)
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatalf("Get(missing) should report ok=false")
	}
}

func TestRegistryDuplicateIDPanics(t *testing.T) {
	r := &Registry{scenarios: map[string]Scenario{}}
	r.Register(Scenario{ID: "01"})
	defer func() {
		got := recover()
		if got == nil {
			t.Fatalf("expected panic on duplicate ID")
		}
		if !strings.Contains(fmt.Sprint(got), "duplicate scenario ID") {
			t.Fatalf("unexpected panic: %v", got)
		}
	}()
	r.Register(Scenario{ID: "01"})
}

func TestRegistryEmptyIDPanics(t *testing.T) {
	r := &Registry{scenarios: map[string]Scenario{}}
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on empty ID")
		}
	}()
	r.Register(Scenario{ID: ""})
}

func TestStepRunsSequentially(t *testing.T) {
	// Smoke test of the Step struct shape: ensure phases are
	// distinguishable. The full executor needs a live cluster and is
	// covered by scenario integration tests.
	var phases []Phase
	steps := []Step{
		{Phase: PhaseInject, Name: "a", Do: func(ctx context.Context, env *Env) error {
			phases = append(phases, PhaseInject)
			return nil
		}},
		{Phase: PhaseVerify, Name: "b", Do: func(ctx context.Context, env *Env) error {
			phases = append(phases, PhaseVerify)
			return nil
		}},
	}
	for _, s := range steps {
		if err := s.Do(context.Background(), nil); err != nil {
			t.Fatalf("step %s err: %v", s.Name, err)
		}
	}
	if len(phases) != 2 || phases[0] != PhaseInject || phases[1] != PhaseVerify {
		t.Fatalf("phase order = %v, want [inject verify]", phases)
	}
}
