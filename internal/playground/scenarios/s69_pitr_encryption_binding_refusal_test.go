package scenarios

import (
	"testing"
	"time"
)

// TestS69RebuiltStable pins U30: the status-wipe recovery passes only after
// s69ShortNoRollWindow of continuous all-Bound polls; a poll that loses a
// binding in between restarts the window instead of latching the first
// match.
func TestS69RebuiltStable(t *testing.T) {
	const window = 30 * time.Second
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	type poll struct {
		at  time.Duration
		all bool
	}
	cases := []struct {
		name  string
		polls []poll
		want  bool
	}{
		{"never rebuilt", []poll{{0, false}, {40 * time.Second, false}}, false},
		{"rebuilt and stable for the window", []poll{{0, true}, {10 * time.Second, true}, {30 * time.Second, true}}, true},
		{"rebuilt, but not yet for the window", []poll{{0, true}, {29 * time.Second, true}}, false},
		{"lost in the window, then the window elapses", []poll{{0, true}, {10 * time.Second, false}, {30 * time.Second, true}}, false},
		{"lost in the window, then stable for a fresh window", []poll{{0, true}, {10 * time.Second, false}, {15 * time.Second, true}, {45 * time.Second, true}}, true},
		{"the returning poll must be all-Bound", []poll{{0, true}, {30 * time.Second, false}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var since time.Time
			var done bool
			for _, p := range c.polls {
				since, done = s69RebuiltStable(since, p.all, t0.Add(p.at), window)
				if done {
					break
				}
			}
			if done != c.want {
				t.Errorf("done=%v, want %v", done, c.want)
			}
		})
	}
}
