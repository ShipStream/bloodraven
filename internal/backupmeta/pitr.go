package backupmeta

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Go silently truncates fractions past nanoseconds while restore.py rounds any
// non-zero digit up to the next second, so longer fractions are rejected.
var pitrFraction = regexp.MustCompile(`[.,](\d+)`)

// ParsePITRStopTime accepts RFC3339 or MySQL datetimes and returns UTC.
func ParsePITRStopTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if m := pitrFraction.FindStringSubmatch(value); m != nil && len(m[1]) > 9 {
		return time.Time{}, fmt.Errorf("datetime %q has more than nine fractional digits", value)
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized datetime: %q", value)
}
