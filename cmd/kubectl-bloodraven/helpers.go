package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
)

func encodeJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func encodeYAML(out io.Writer, v any) error {
	b, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	_, err = out.Write(b)
	return err
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func boolYesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// age formats a duration since t in a kubectl-style short form
// ("5s", "12m", "3h45m", "2d6h", "30d"). t.IsZero() prints "-".
func age(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return shortDuration(time.Since(t))
}

func shortDuration(d time.Duration) string {
	if d < 0 {
		return "0s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		h := int(d.Hours())
		m := int(d.Minutes()) - h*60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
	days := int(d.Hours() / 24)
	h := int(d.Hours()) - days*24
	if h == 0 || days >= 7 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd%dh", days, h)
}

func timeAge(t *metav1.Time) string {
	if t == nil {
		return "-"
	}
	return age(t.Time)
}

func lagString(secs *int64) string {
	if secs == nil {
		return "-"
	}
	if *secs < 0 {
		return "?"
	}
	return shortDuration(time.Duration(*secs) * time.Second)
}

func lastFailoverAge(fg *v1alpha1.MysqlFailoverGroup) string {
	rec, _, _ := brcontroller.EffectiveFailoverRecord(fg, time.Now())
	if rec.LastFailover.IsZero() {
		return "-"
	}
	return age(rec.LastFailover)
}

func plannedFailoverSummary(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Status.PlannedFailover == nil {
		return "-"
	}
	pf := fg.Status.PlannedFailover
	if pf.Reason != "" && pf.Phase == v1alpha1.PlannedFailoverPhaseFailed {
		return fmt.Sprintf("%s(%s)", pf.Phase, pf.Reason)
	}
	return string(pf.Phase)
}

func recoverySummary(fg *v1alpha1.MysqlFailoverGroup) string {
	for _, s := range fg.Status.Sites {
		if s.RecoveryState != "" {
			return fmt.Sprintf("%s=%s", s.Name, s.RecoveryState)
		}
	}
	return "-"
}

func siteCountSummary(fg *v1alpha1.MysqlFailoverGroup) string {
	writable, readonly, unhealthy := 0, 0, 0
	for _, s := range fg.Status.Sites {
		switch s.State {
		case "writable":
			writable++
		case "read-only":
			readonly++
		default:
			// Includes "unreachable", "unknown", and the empty
			// string the controller writes before the first
			// successful poll. Bucket them all into "unhealthy"
			// so the running total always equals the number of
			// known sites instead of silently dropping rows.
			unhealthy++
		}
	}
	return fmt.Sprintf("%dW/%dRO/%dX", writable, readonly, unhealthy)
}

// readyString extracts the Ready condition value, defaulting to "-".
func readyString(fg *v1alpha1.MysqlFailoverGroup) string {
	for _, c := range fg.Status.Conditions {
		if c.Type == "Ready" {
			return string(c.Status)
		}
	}
	return "-"
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// truncate returns s capped to at most max characters (runes), using
// "…" as the elision marker when truncation actually happens. Rune-
// safe: callers can pass arbitrary controller-supplied messages
// (including non-ASCII MySQL error strings) without producing an
// invalid-UTF-8 byte sequence on the wire — important for `status -o
// json`, where downstream JSON parsers reject malformed UTF-8.
func truncate(s string, max int) string {
	if max <= 1 {
		return s
	}
	// Count runes, not bytes — len(s) counts bytes. For all-ASCII
	// strings these are identical and the short-circuit is free; for
	// the multi-byte case it's a single linear scan.
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	rs := []rune(s)
	if max <= 3 {
		return string(rs[:max])
	}
	return string(rs[:max-1]) + "…"
}

// joinKVs renders annotation key=value override clauses (deterministic
// order). Used by the promote command to build annotation values like
// "<site>:maxLagWait=30s". The operator's annotation parser
// (internal/controller/planned_failover.go) splits the suffix on ':',
// so the separator used here MUST be ':' — using ',' would produce a
// single unparseable kv pair where the value contains the would-be
// next key.
func joinKVs(pairs []kv) string {
	if len(pairs) == 0 {
		return ""
	}
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.k+"="+p.v)
	}
	return strings.Join(out, ":")
}

type kv struct {
	k string
	v string
}
