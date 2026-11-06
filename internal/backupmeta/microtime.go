package backupmeta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// MicroTimeLayout is the one wire format for PITR instants in the dump stamp,
// dump.json and coverage.json: RFC3339 in UTC with exactly six fractional
// digits, e.g. "2026-11-04T20:01:02.123456Z". restore.py parses the same
// layout.
const MicroTimeLayout = "2006-01-02T15:04:05.000000Z"

// MicroTime is a UTC instant at microsecond precision. It marshals as
// MicroTimeLayout, and the zero value marshals as JSON null. Unmarshal
// accepts null (zero) and any RFC3339 string with at most six fractional
// digits; it refuses finer precision rather than silently truncating it.
type MicroTime struct {
	time.Time
}

// NewMicroTime truncates t to the microsecond and converts it to UTC.
func NewMicroTime(t time.Time) MicroTime {
	if t.IsZero() {
		return MicroTime{}
	}
	return MicroTime{Time: t.UTC().Truncate(time.Microsecond)}
}

// String returns MicroTimeLayout, or "" for the zero value.
func (m MicroTime) String() string {
	if m.IsZero() {
		return ""
	}
	return m.UTC().Truncate(time.Microsecond).Format(MicroTimeLayout)
}

// MarshalJSON implements json.Marshaler.
func (m MicroTime) MarshalJSON() ([]byte, error) {
	if m.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(m.String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *MicroTime) UnmarshalJSON(b []byte) error {
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		*m = MicroTime{}
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("micro time: %w", err)
	}
	t, err := ParseMicroTime(s)
	if err != nil {
		return err
	}
	*m = t
	return nil
}

// ParseMicroTime parses an RFC3339 instant with at most six fractional
// digits and returns it in UTC.
func ParseMicroTime(s string) (MicroTime, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return MicroTime{}, fmt.Errorf("micro time %q: %w", s, err)
	}
	if t.Nanosecond()%int(time.Microsecond) != 0 {
		return MicroTime{}, fmt.Errorf("micro time %q is finer than a microsecond", s)
	}
	if t.IsZero() {
		return MicroTime{}, fmt.Errorf("micro time %q is the zero instant", s)
	}
	return MicroTime{Time: t.UTC()}, nil
}
