package scenarios

import (
	"sync"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// stashes maps Env pointers to per-scenario string state. All
// access — both the outer `stashes` map and the inner per-env map —
// is performed under stashMu so concurrent goroutines (and the Go
// race detector) see consistent state. Scenarios are sequential
// within an Executor today, but the lock keeps the helpers safe if
// that ever changes.
var (
	stashMu sync.Mutex
	stashes = map[*runner.Env]map[string]string{}
)

func stashSet(env *runner.Env, key, value string) {
	stashMu.Lock()
	defer stashMu.Unlock()
	m, ok := stashes[env]
	if !ok {
		m = map[string]string{}
		stashes[env] = m
	}
	m[key] = value
}

func stashGet(env *runner.Env, key string) string {
	stashMu.Lock()
	defer stashMu.Unlock()
	m, ok := stashes[env]
	if !ok {
		return ""
	}
	return m[key]
}
