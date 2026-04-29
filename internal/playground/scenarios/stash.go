package scenarios

import (
	"sync"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// stashes maps Env pointers to per-scenario string state. The map is
// guarded by a mutex; scenarios are sequential within an Executor, but
// the Go race detector still flags concurrent reads/writes when ranges
// of tests run in parallel.
var (
	stashMu sync.Mutex
	stashes = map[*runner.Env]map[string]string{}
)

func getStash(env *runner.Env) map[string]string {
	stashMu.Lock()
	defer stashMu.Unlock()
	m, ok := stashes[env]
	if !ok {
		m = map[string]string{}
		stashes[env] = m
	}
	return m
}
