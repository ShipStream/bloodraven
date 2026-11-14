package scenarios

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const s76UUID = "00a2c8bb-dfba-11f1-98d6-d2e6f2f94621"

func TestS76TaggedTransactions(t *testing.T) {
	cases := []struct {
		name, before, after, want, err string
	}{
		{
			name:   "first tagged transaction, compact form",
			before: s76UUID + ":1-20",
			after:  s76UUID + ":1-21:chaos76:1",
			want:   s76UUID + ":chaos76:1",
		},
		{
			name:   "a rerun on the same server numbers the tag on",
			before: s76UUID + ":1-40:chaos76:1",
			after:  s76UUID + ":1-44:chaos76:1-2",
			want:   s76UUID + ":chaos76:2",
		},
		{
			name:   "tag case is folded as MySQL does",
			before: s76UUID + ":1-20",
			after:  s76UUID + ":1-20:CHAOS76:1",
			want:   s76UUID + ":chaos76:1",
		},
		{
			name:   "other tags are not ours",
			before: s76UUID + ":1-20",
			after:  s76UUID + ":1-20:app:1:chaos76:1",
			want:   s76UUID + ":chaos76:1",
		},
		{
			name:   "no tagged transaction",
			before: s76UUID + ":1-20",
			after:  s76UUID + ":1-21",
			err:    "found 0",
		},
		{
			name:   "two tagged transactions",
			before: s76UUID + ":1-20",
			after:  s76UUID + ":1-20:chaos76:1-2",
			err:    "found 2",
		},
		{
			name:   "unparsable set",
			before: s76UUID + ":1-20",
			after:  s76UUID + ":1-20:chaos76",
			err:    "parse gtid_executed after",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s76TaggedTransactions(tc.before, tc.after, s76GTIDTag)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err=%v, want one containing %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestS76ReadTimeline(t *testing.T) {
	at := func(sec int) time.Time { return time.Unix(1_800_000_000+int64(sec), 0) }
	lines := logLines(
		`{"level":"INFO","msg":"failover complete","promotedSite":"iad","promotionGtid":"x"}`, // an earlier failover elsewhere
		`{"level":"INFO","msg":"failover complete","promotedSite":"pdx","promotionGtid":"x"}`,
		`{"level":"WARN","msg":"dragonfly emergency: old master unreachable; skipping REPLTAKEOVER","site":"pdx","oldSource":"iad"}`,
		`{"level":"INFO","msg":"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)","site":"pdx"}`,
		`{"level":"INFO","msg":"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)","site":"pdx"}`,
	)
	tl := s76ReadTimeline(lines, "pdx")
	if !tl.FailoverComplete.Equal(at(1)) || !tl.Promoted.Equal(at(3)) || tl.PromotedMsg != logS76PromotedNoOne {
		t.Fatalf("timeline %+v", tl)
	}
	if !tl.TakeoverSkipped || tl.DragonflyOnly {
		t.Fatalf("path flags %+v", tl)
	}

	only := s76ReadTimeline(logLines(
		`{"level":"INFO","msg":"dragonfly-only emergency: active master unreachable; promoting replica","oldSource":"iad","target":"pdx"}`,
		`{"level":"INFO","msg":"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)","site":"pdx"}`,
		`{"level":"INFO","msg":"failover complete","promotedSite":"pdx"}`,
		// A line after the promotion does not change the path.
		`{"level":"WARN","msg":"dragonfly emergency: old master unreachable; skipping REPLTAKEOVER","site":"pdx"}`,
		`{"level":"INFO","msg":"dragonfly promotion: target already promoted since the request; skipping","site":"pdx"}`,
	), "pdx")
	if !only.DragonflyOnly || only.TakeoverSkipped || !only.Promoted.Equal(at(1)) || !only.FailoverComplete.Equal(at(2)) || !only.FollowUpSkipped {
		t.Fatalf("dragonfly-only timeline %+v", only)
	}

	takeover := s76ReadTimeline(logLines(
		`{"level":"INFO","msg":"dragonfly emergency: REPLTAKEOVER succeeded","site":"pdx"}`,
	), "pdx")
	if takeover.PromotedMsg != logS76TakeoverSucceeded {
		t.Fatalf("takeover timeline %+v", takeover)
	}

	if other := s76ReadTimeline(lines, "reader"); !other.FailoverComplete.IsZero() || !other.Promoted.IsZero() {
		t.Fatalf("lines of another site matched: %+v", other)
	}
	if len(tl.Elsewhere) != 0 {
		t.Fatalf("promotions elsewhere in a clean timeline: %v", tl.Elsewhere)
	}

	// Seen live: an observation of pdx failed, the manager's own promotion
	// took the reader site, then the queued follow-up promoted pdx.
	astray := s76ReadTimeline(logLines(
		`{"level":"INFO","msg":"failover complete","promotedSite":"pdx"}`,
		`{"level":"INFO","msg":"dragonfly-only emergency: active master unreachable; promoting replica","oldSource":"iad","target":"reader"}`,
		`{"level":"WARN","msg":"dragonfly emergency: old master unreachable; skipping REPLTAKEOVER","site":"reader","oldSource":"iad"}`,
		`{"level":"INFO","msg":"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)","site":"reader"}`,
		`{"level":"WARN","msg":"dragonfly emergency: old master unreachable; skipping REPLTAKEOVER","site":"pdx","oldSource":"iad"}`,
		`{"level":"INFO","msg":"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)","site":"pdx"}`,
	), "pdx")
	if len(astray.Elsewhere) != 2 || !strings.HasPrefix(astray.Elsewhere[0], "reader (") || astray.DragonflyOnly || !astray.Promoted.Equal(at(5)) {
		t.Fatalf("astray timeline %+v", astray)
	}
	// Before the MySQL promotion the manager decides alone.
	early := s76ReadTimeline(logLines(
		`{"level":"INFO","msg":"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)","site":"reader"}`,
		`{"level":"INFO","msg":"failover complete","promotedSite":"pdx"}`,
	), "pdx")
	if len(early.Elsewhere) != 0 {
		t.Fatalf("a promotion before the MySQL promotion counted: %v", early.Elsewhere)
	}
}

func TestS76PromotionDelay(t *testing.T) {
	failover := time.Date(2026, 11, 13, 12, 0, 0, 600_000_000, time.UTC)
	// A stamp of 12:00:07 (truncated) may stand for any time up to
	// 12:00:07.999, so the delay is taken as 7.4s, not 6.4s.
	stamp := time.Date(2026, 11, 13, 12, 0, 7, 0, time.UTC)
	if got := s76PromotionDelay(failover, stamp); got != 7400*time.Millisecond {
		t.Fatalf("delay=%s", got)
	}
	// The playground's failureThreshold is 3: the manager's own promotion
	// bounds it. A lower threshold leaves the follow-up's budget.
	if got := s76PromotionBound(3); got != 20*time.Second {
		t.Fatalf("s76PromotionBound(3)=%s; update the docs (§76) if the budgets moved", got)
	}
	if got := s76PromotionBound(1); got != 14*time.Second {
		t.Fatalf("s76PromotionBound(1)=%s, want the follow-up's 14s", got)
	}
}

// TestS76OperatorMirrors pins the messages and budgets scenario 76 mirrors
// to internal/controller.
func TestS76OperatorMirrors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "controller", "dragonfly_topology.go"))
	if err != nil {
		t.Fatal(err)
	}
	df := string(raw)
	failover, err := os.ReadFile(filepath.Join("..", "..", "controller", "failover.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []string{logS76TakeoverSkipped, logS76PromotedNoOne, logS76TakeoverSucceeded,
		logS76PromotionUnconfirmed, logS76DragonflyOnlyPromoted, logS76FollowUpSkipped} {
		if !strings.Contains(df, strconv.Quote(msg)) {
			t.Errorf("dragonfly_topology.go has no string literal %q; update the scenario's mirror", msg)
		}
	}
	if !strings.Contains(string(failover), strconv.Quote(logS76FailoverComplete)+`, "promotedSite"`) {
		t.Errorf("failover.go no longer logs %q with promotedSite", logS76FailoverComplete)
	}
	for _, field := range []string{`"dragonfly-only emergency: active master unreachable; promoting replica", "oldSource", oldSource, "target", target`,
		`"dragonfly emergency: old master unreachable; skipping REPLTAKEOVER", "site", target`,
		`"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)", "site", target`,
		`"dragonfly promotion: target already promoted since the request; skipping", "site", req.target`} {
		if !strings.Contains(df, field) {
			t.Errorf("dragonfly_topology.go no longer logs %s; the scenario reads those fields", field)
		}
	}
	for name, want := range map[string]time.Duration{
		"defaultEmergencySourceProbeTimeout": s76EmergencySourceProbeTimeout,
	} {
		if got := s76ConstDuration(t, df, name); got != want {
			t.Errorf("operator %s=%s, scenario mirror %s", name, got, want)
		}
	}
	m := regexp.MustCompile(`func \(m \*DragonflyManager\) bestEffortEmergencyClientKill[\s\S]*?context\.WithTimeout\(ctx, (\d+)\s*\*\s*time\.Second\)`).FindStringSubmatch(df)
	if m == nil {
		t.Fatal("bestEffortEmergencyClientKill has no context.WithTimeout(ctx, <n>*time.Second); update the mirror and this test")
	}
	if n, _ := strconv.Atoi(m[1]); time.Duration(n)*time.Second != s76EmergencyClientKillTimeout {
		t.Errorf("operator client-kill timeout=%ss, scenario mirror %s", m[1], s76EmergencyClientKillTimeout)
	}
}

// s76ConstDuration reads `name = <n> * time.Second` or `name = time.Second`
// from src.
func s76ConstDuration(t *testing.T, src, name string) time.Duration {
	t.Helper()
	m := regexp.MustCompile(regexp.QuoteMeta(name) + `\s*=\s*(?:(\d+)\s*\*\s*)?time\.Second\b`).FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("internal/controller declares no %s = [<n> *] time.Second; update the mirror and this test", name)
	}
	n := 1
	if m[1] != "" {
		n, _ = strconv.Atoi(m[1])
	}
	return time.Duration(n) * time.Second
}
