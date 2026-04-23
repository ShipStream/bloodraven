package util

import (
	"io"
	"log/slog"
)

// NewJSONLogger returns a slog.Logger that writes JSON records to w with
// timestamps normalized to UTC. Both bloodraven binaries call this so the
// log-schema contract's "time is UTC" commitment holds regardless of the
// pod's TZ.
func NewJSONLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
				return slog.Time(slog.TimeKey, a.Value.Time().UTC())
			}
			return a
		},
	}))
}
