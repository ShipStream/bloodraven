package controller

import (
	"fmt"
	"time"
)

// compactDuration spells a whole-second bound the way the CEL markers and
// the docs do ("1s", "10m", "1h"), not time.Duration's "1h0m0s".
func compactDuration(d time.Duration) string {
	switch {
	case d != 0 && d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d != 0 && d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return d.String()
	}
}

// wholeSecondsCeil renders a positive Duration as whole seconds, rounded up
// and at least 1, for tools and MySQL variables that take integer seconds.
func wholeSecondsCeil(d time.Duration) int64 {
	secs := int64((d + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}
