package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

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

// TestPrintGroupListSmoke confirms the table renderer doesn't blow up
// on an empty list or a list with sparse status. The contents are
// intentionally not asserted line-for-line because the column widths
// shift between Go versions of tabwriter.
func TestPrintGroupListSmoke(t *testing.T) {
	items := []v1alpha1.MysqlFailoverGroup{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
			Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}},
		},
	}
	for _, format := range []string{"table", "wide"} {
		var buf bytes.Buffer
		if err := printGroupList(&buf, items, format, false); err != nil {
			t.Fatalf("format=%s: %v", format, err)
		}
		out := buf.String()
		if !strings.Contains(out, "orders") {
			t.Errorf("format=%s: output missing group name: %s", format, out)
		}
		if !strings.Contains(out, "NAME") {
			t.Errorf("format=%s: output missing header: %s", format, out)
		}
	}
}

func TestPrintGroupStatusSmoke(t *testing.T) {
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
	for _, want := range []string{"MysqlFailoverGroup: default/orders", "Active site: iad", "DNS: orders.example.com", "iad", "pdx"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}
}

func TestJoinKVs(t *testing.T) {
	if got := joinKVs(nil); got != "" {
		t.Errorf("empty -> %q, want empty", got)
	}
	pairs := []kv{{"a", "1"}, {"b", "2"}}
	if got := joinKVs(pairs); got != "a=1,b=2" {
		t.Errorf("joinKVs -> %q, want a=1,b=2", got)
	}
}
