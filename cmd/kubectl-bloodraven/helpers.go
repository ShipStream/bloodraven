package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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

func lastSeenAge(t *metav1.Time) string {
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
	if fg.Status.LastFailover == nil {
		return "-"
	}
	return age(fg.Status.LastFailover.Time)
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
		case "unreachable", "unknown":
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

func truncate(s string, max int) string {
	if max <= 1 || len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-1] + "…"
}

// joinKVs renders annotation key=value override clauses (deterministic
// order). Used by the promote/reclone commands to build annotation
// values like "<site>:maxLagWait=30s,drainTimeout=10s".
func joinKVs(pairs []kv) string {
	if len(pairs) == 0 {
		return ""
	}
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.k+"="+p.v)
	}
	return strings.Join(out, ",")
}

type kv struct {
	k string
	v string
}
