package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestShortDuration(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{45 * time.Second, "45s"},
		{2 * time.Minute, "2m"},
		{59 * time.Minute, "59m"},
		{90 * time.Minute, "1h30m"},
		{3 * time.Hour, "3h"},
		{36 * time.Hour, "1d12h"},
		{30 * 24 * time.Hour, "30d"},
	}
	for _, tc := range tests {
		got := shortDuration(tc.in)
		if got != tc.want {
			t.Errorf("shortDuration(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestLagString(t *testing.T) {
	if got := lagString(nil); got != "-" {
		t.Errorf("nil -> %q, want -", got)
	}
	v := int64(75)
	if got := lagString(&v); got != "1m" {
		t.Errorf("75s -> %q, want 1m", got)
	}
	neg := int64(-1)
	if got := lagString(&neg); got != "?" {
		t.Errorf("-1s -> %q, want ?", got)
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{500, "500B"},
		{1024, "1.0KiB"},
		{1500, "1.5KiB"},
		{1024 * 1024, "1.0MiB"},
		{2 * 1024 * 1024 * 1024, "2.0GiB"},
	}
	for _, tc := range tests {
		got := humanBytes(tc.in)
		if got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		in   string
		max  int
		want string
	}{
		{"hello", 10, "hello"},
		{"hello world", 8, "hello w…"},
	}
	for _, tc := range cases {
		got := truncate(tc.in, tc.max)
		if got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.in, tc.max, got, tc.want)
		}
	}
}

func TestReadyString(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
		},
	}
	if got := readyString(fg); got != "True" {
		t.Errorf("readyString = %q, want True", got)
	}
	empty := &v1alpha1.MysqlFailoverGroup{}
	if got := readyString(empty); got != "-" {
		t.Errorf("readyString(empty) = %q, want -", got)
	}
}

func TestSiteCountSummary(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: "writable"},
				{Name: "pdx", State: "read-only"},
				{Name: "lhr", State: "unreachable"},
				{Name: "fra", State: "unknown"},
			},
		},
	}
	if got := siteCountSummary(fg); got != "1W/1RO/2X" {
		t.Errorf("siteCountSummary = %q, want 1W/1RO/2X", got)
	}
}

func TestPlannedFailoverSummary(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	if got := plannedFailoverSummary(fg); got != "-" {
		t.Errorf("no planned failover -> %q, want -", got)
	}
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhaseFailed,
		Reason: "CooldownActive",
	}
	if got := plannedFailoverSummary(fg); got != "Failed(CooldownActive)" {
		t.Errorf("failed planned failover -> %q, want Failed(CooldownActive)", got)
	}
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseSucceeded
	fg.Status.PlannedFailover.Reason = ""
	if got := plannedFailoverSummary(fg); got != "Succeeded" {
		t.Errorf("succeeded planned failover -> %q, want Succeeded", got)
	}
}

// TestPrintGroupList exercises the table renderer's actual contract:
// wide-mode column set, table-mode column set, and the empty-active
// case rendering as "-" instead of a blank column.
func TestPrintGroupList(t *testing.T) {
	items := []v1alpha1.MysqlFailoverGroup{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
			Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}},
			// Leave ActiveSite empty to assert the emptyDash fallback.
		},
	}

	cases := []struct {
		format       string
		wantContains []string
		wantMissing  []string
	}{
		{
			format: "table",
			// Table mode: NAME ACTIVE READY SITES AGE; PLANNED /
			// RECOVERY / LAST-FAILOVER appear only in wide.
			wantContains: []string{"NAME", "ACTIVE", "READY", "SITES", "AGE", "orders"},
			wantMissing:  []string{"PLANNED", "RECOVERY", "LAST-FAILOVER"},
		},
		{
			format:       "wide",
			wantContains: []string{"NAME", "ACTIVE", "PLANNED", "LAST-FAILOVER", "RECOVERY", "AGE", "orders"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			var buf bytes.Buffer
			if err := printGroupList(&buf, items, tc.format, false); err != nil {
				t.Fatalf("format=%s: %v", tc.format, err)
			}
			out := buf.String()
			for _, want := range tc.wantContains {
				if !strings.Contains(out, want) {
					t.Errorf("format=%s: output missing %q\n%s", tc.format, want, out)
				}
			}
			for _, unwanted := range tc.wantMissing {
				if strings.Contains(out, unwanted) {
					t.Errorf("format=%s: output unexpectedly contains %q\n%s", tc.format, unwanted, out)
				}
			}
			// emptyDash contract: empty ActiveSite must render as
			// "-", not as a collapsed column.
			if !strings.Contains(out, " - ") && !strings.Contains(out, "\t-\t") {
				t.Errorf("format=%s: empty ActiveSite should render as '-'\n%s", tc.format, out)
			}
		})
	}
}

func TestPrintGroupStatus(t *testing.T) {
	now := metav1.Now()
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default", CreationTimestamp: now},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{
				{Name: "iad", Zone: "us-east-1a"},
				{Name: "pdx", Zone: "us-west-2a"},
			},
			DNS: v1alpha1.DNSSpec{Hostname: "orders.example.com", TTL: 60},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: "writable", LastSeen: &now},
				{Name: "pdx", State: "read-only", LastSeen: &now, Replicating: true},
			},
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
	var buf bytes.Buffer
	if err := printGroupStatus(&buf, fg, "table"); err != nil {
		t.Fatalf("printGroupStatus: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"MysqlFailoverGroup: default/orders",
		"Active site: iad",
		"DNS: orders.example.com",
		// Per-site row contents:
		"writable",
		"read-only",
		// Confirm both site names land in the table.
		"iad",
		"pdx",
		// Confirm the Sites header is rendered.
		"NAME",
		"ROLE",
		"STATE",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}
}

// TestPrintGroupStatus_EmptyActiveRendersDash verifies the "-"
// fallback so a freshly-deployed group with no active site doesn't
// produce a confusing blank row mid-table.
func TestPrintGroupStatus_EmptyActiveRendersDash(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "default"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad"}}},
	}
	var buf bytes.Buffer
	if err := printGroupStatus(&buf, fg, "table"); err != nil {
		t.Fatalf("printGroupStatus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Active site: -") {
		t.Errorf("empty ActiveSite should render as 'Active site: -'\n%s", out)
	}
}

func TestJoinKVs(t *testing.T) {
	if got := joinKVs(nil); got != "" {
		t.Errorf("empty -> %q, want empty", got)
	}
	pairs := []kv{{"a", "1"}, {"b", "2"}}
	// Separator MUST be ':' — the controller's annotation parser
	// (internal/controller/planned_failover.go) splits on ':'. Using
	// ',' silently produces a single unparseable kv where the value
	// contains the would-be next key.
	if got := joinKVs(pairs); got != "a=1:b=2" {
		t.Errorf("joinKVs -> %q, want a=1:b=2", got)
	}
}

func TestTruncateRuneSafe(t *testing.T) {
	// Multi-byte rune at the truncation boundary must not produce
	// invalid UTF-8 — `status -o json` would re-emit the truncated
	// string and downstream JSON parsers reject malformed UTF-8.
	in := "αβγδεζηθ" // 8 two-byte runes
	got := truncate(in, 4)
	if !utf8.ValidString(got) {
		t.Errorf("truncate produced invalid UTF-8: %q (% x)", got, []byte(got))
	}
	if want := "αβγ…"; got != want {
		t.Errorf("truncate(8-rune, 4) = %q, want %q", got, want)
	}
	if got := truncate("abc", 10); got != "abc" {
		t.Errorf("short string should be unchanged; got %q", got)
	}
}

func TestSiteCountSummaryBucketsUnknownStateAsUnhealthy(t *testing.T) {
	// The controller writes State lazily; before the first successful
	// poll a site's State is "". Previously this dropped silently out
	// of the W/RO/X totals — the column undercounted in exactly the
	// moment an operator most needs an accurate row count.
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: "writable"},
				{Name: "pdx", State: ""},
				{Name: "lhr", State: "weird-future-value"},
			},
		},
	}
	if got := siteCountSummary(fg); got != "1W/0RO/2X" {
		t.Errorf("siteCountSummary = %q, want 1W/0RO/2X (unknown / future state counts as unhealthy)", got)
	}
}
