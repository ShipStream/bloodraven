package runner

import (
	"fmt"
	"sort"
	"sync"
)

// Registry holds the global set of scenarios, populated by the
// scenarios package's init() functions.
type Registry struct {
	mu        sync.RWMutex
	scenarios map[string]Scenario
}

// DefaultRegistry is the package-global registry. Scenarios self-register
// at init time so the CLI doesn't need to know each scenario's import
// path.
var DefaultRegistry = &Registry{scenarios: map[string]Scenario{}}

// Register adds a scenario to the registry. Panics on duplicate ID;
// duplicate IDs almost always mean a copy-paste bug at scenario
// authoring time, and detecting it loudly at process start beats
// hunting through a run-all log later.
func (r *Registry) Register(s Scenario) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.ID == "" {
		panic("runner.Register: scenario.ID is empty")
	}
	if _, exists := r.scenarios[s.ID]; exists {
		panic(fmt.Sprintf("runner.Register: duplicate scenario ID %q", s.ID))
	}
	r.scenarios[s.ID] = s
}

// Get returns the scenario with the given ID, or false if no such ID.
func (r *Registry) Get(id string) (Scenario, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.scenarios[id]
	return s, ok
}

// List returns scenarios sorted by ID. Sort is lexical, which lines
// up with the "01-...", "02-..." ID convention.
func (r *Registry) List() []Scenario {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Scenario, 0, len(r.scenarios))
	for _, s := range r.scenarios {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Register adds a scenario to the package-default registry.
func Register(s Scenario) { DefaultRegistry.Register(s) }
