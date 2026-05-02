package scenarios

import (
	"encoding/json"
	"fmt"
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

// stashMap encodes a map[string]string as JSON and stashes it under
// key. Used by scenarios that need to remember a structured value
// (e.g. per-site originals) across steps. Round-tripping through JSON
// keeps the stash backing-store a flat map[string]string.
func stashMap(env *runner.Env, key string, m map[string]string) error {
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("stash %s: marshal: %w", key, err)
	}
	stashSet(env, key, string(body))
	return nil
}

func stashFetchMap(env *runner.Env, key string) (map[string]string, error) {
	raw := stashGet(env, key)
	if raw == "" {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("stash %s: unmarshal %q: %w", key, raw, err)
	}
	return out, nil
}
