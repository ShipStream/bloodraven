package backupmeta

import (
	"fmt"
	"strings"
	"time"
)

// ParsePITRStopTime accepts RFC3339 or MySQL datetimes and returns UTC.
func ParsePITRStopTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized datetime: %q", value)
}
