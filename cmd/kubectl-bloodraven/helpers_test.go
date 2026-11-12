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
				{Name: "iad", State: v1alpha1.SiteStateWritable},
				{Name: "pdx", State: v1alpha1.SiteStateReadOnly},
				{Name: "lhr", State: v1alpha1.SiteStateUnreachable},
				{Name: "fra", State: v1alpha1.SiteStateUnknown},
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
		RunID: "run-1",
		Phase: v1alpha1.PlannedFailoverPhaseFailed,
		Result: &v1alpha1.PlannedFailoverResult{
			Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonCooldownActive},
	}
	if got := plannedFailoverSummary(fg); got != "Failed(CooldownActive)" {
		t.Errorf("failed planned failover -> %q, want Failed(CooldownActive)", got)
	}
	fg.Status.PlannedFailover.Result = &v1alpha1.PlannedFailoverResult{
		Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, Reason: v1alpha1.PlannedFailoverReasonLagTimeout}
	if got := plannedFailoverSummary(fg); got != "Failed(RolledBack:LagTimeout)" {
		t.Errorf("rolled-back planned failover -> %q, want Failed(RolledBack:LagTimeout)", got)
	}
	fg.Status.PlannedFailover.Result = &v1alpha1.PlannedFailoverResult{
		Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonForeignWritable}
	fg.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionTrue}}
	if got := plannedFailoverSummary(fg); got != "Failed(ManualRecoveryRequired:ForeignWritable)+held" {
		t.Errorf("held planned failover -> %q, want Failed(ManualRecoveryRequired:ForeignWritable)+held", got)
	}
	fg.Status.Conditions = nil
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseWaitingForLag
	fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`}
	if got := plannedFailoverSummary(fg); got != "Ended(SourceLeftFenced:StatusStalled)" {
		t.Errorf("marked planned failover -> %q, want Ended(SourceLeftFenced:StatusStalled)", got)
	}
	fg.Annotations = nil
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-2", Phase: v1alpha1.PlannedFailoverPhaseAdmitting,
		Admission: &v1alpha1.PlannedFailoverAdmission{WaitReason: v1alpha1.PlannedFailoverWaitPeerUnreadable}}
	if got := plannedFailoverSummary(fg); got != "Admitting(PeerUnreadable)" {
		t.Errorf("waiting planned failover -> %q, want Admitting(PeerUnreadable)", got)
	}
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-3", Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
		Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded}}
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
			Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}}},
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
				{Name: "iad", ServerID: 101, Zone: "us-east-1a"},
				{Name: "pdx", ServerID: 102, Zone: "us-west-2a"},
			},
			DNS: v1alpha1.DNSSpec{Hostname: "orders.example.com", TTLSeconds: 60},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateWritable, LastSeenTime: &now, Replication: v1alpha1.SiteReplicationNotApplicable},
				{Name: "pdx", State: v1alpha1.SiteStateReadOnly, LastSeenTime: &now, Replication: v1alpha1.SiteReplicationHealthy},
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
		"Writable",
		"ReadOnly",
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

func TestPrintGroupStatus_ShowsOrderedUpdateStall(t *testing.T) {
	msg := "Ordered update stalled: site pdx was updated but its pod mysql-orders-pdx-1 is not Ready (sidecar not ready: crash looping, last exit code 1 (Error))"
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}}},
		Status: v1alpha1.MysqlFailoverGroupStatus{Conditions: []metav1.Condition{{
			Type: "Updating", Status: metav1.ConditionTrue, Reason: "OrderedUpdateStalled", Message: msg,
		}}},
	}
	var buf bytes.Buffer
	if err := printGroupStatus(&buf, fg, "table"); err != nil {
		t.Fatalf("printGroupStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "  Update stalled: "+msg+"\n") {
		t.Errorf("stalled update not shown:\n%s", buf.String())
	}

	fg.Status.Conditions[0].Reason, fg.Status.Conditions[0].Message = "NotUpdating", "No update in progress"
	buf.Reset()
	if err := printGroupStatus(&buf, fg, "table"); err != nil {
		t.Fatalf("printGroupStatus: %v", err)
	}
	if strings.Contains(buf.String(), "Update stalled") {
		t.Errorf("idle update shown as stalled:\n%s", buf.String())
	}
}

// TestPrintGroupStatus_ShowsRollout: the detailed report says where the
// group stands in the paced rollout while it waits for or holds the slot,
// and says nothing for a group without a record or with a completed roll.
func TestPrintGroupStatus_ShowsRollout(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}}},
	}
	render := func() string {
		t.Helper()
		var buf bytes.Buffer
		if err := printGroupStatus(&buf, fg, "table"); err != nil {
			t.Fatalf("printGroupStatus: %v", err)
		}
		return buf.String()
	}
	if out := render(); strings.Contains(out, "Rollout:") {
		t.Errorf("a group without status.rollout shows a rollout line:\n%s", out)
	}
	for _, tc := range []struct {
		name string
		rec  v1alpha1.RolloutStatus
		want string
	}{
		{
			name: "halted",
			rec: v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhasePending, Reason: "RolloutHalted",
				Message: "the rollout is halted: the roll of default/billing stalled; this group keeps its pods"},
			want: "  Rollout: Pending (RolloutHalted): the rollout is halted: the roll of default/billing stalled; this group keeps its pods\n",
		},
		{
			name: "stalled",
			rec: v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhaseStalled, Reason: "OrderedUpdateStalled",
				Message: "Ordered update stalled: site pdx was updated but its pod is not Ready"},
			want: "  Rollout: Stalled (OrderedUpdateStalled): Ordered update stalled: site pdx was updated but its pod is not Ready\n",
		},
		{
			name: "no reason or message",
			rec:  v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhaseRolling},
			want: "  Rollout: Rolling\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := tc.rec
			fg.Status.Rollout = &rec
			if out := render(); !strings.Contains(out, tc.want) {
				t.Errorf("missing %q in output:\n%s", tc.want, out)
			}
		})
	}
	now := metav1.Now()
	fg.Status.Rollout = &v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhaseComplete, AdmittedTime: &now, StartedTime: &now, CompletedTime: &now}
	if out := render(); strings.Contains(out, "Rollout:") {
		t.Errorf("a completed roll shows a rollout line:\n%s", out)
	}
}

// TestPrintGroupStatus_EmptyActiveRendersDash verifies the "-"
// fallback so a freshly-deployed group with no active site doesn't
// produce a confusing blank row mid-table.
func TestPrintGroupStatus_EmptyActiveRendersDash(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "default"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}}},
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
				{Name: "iad", State: v1alpha1.SiteStateWritable},
				{Name: "pdx", State: ""},
				{Name: "lhr", State: "weird-future-value"},
			},
		},
	}
	if got := siteCountSummary(fg); got != "1W/0RO/2X" {
		t.Errorf("siteCountSummary = %q, want 1W/0RO/2X (unknown / future state counts as unhealthy)", got)
	}
}

// The detailed view says which transactions a reported site lacks or
// holds, and for a site that lacks its own why its reclone waits.
func TestPrintGroupStatus_ShowsSiteReports(t *testing.T) {
	lost, divergent := int64(10), int64(1)
	waiting := "pdx lacks 10 transactions it issued itself that iad holds (22222222-0000-11f1-8000-0000000000b2:91-100); automatic reclone waiting: another topology change holds the sites (an ordered update, a planned failover or its hold, or a promotion that is not observed yet) (by hand: kubectl bloodraven reclone orders pdx --cold). Replication cannot bring these transactions back (the site's receiver drops events with its own server_id), so the site is not replicated and not promoted until it is recloned."
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{
			{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}, {Name: "dfw", ServerID: 103}}},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateWritable},
				{Name: "pdx", State: v1alpha1.SiteStateReadOnly, RecoveryState: v1alpha1.RecoveryStateRecloneRequired,
					LostGtid: "22222222-0000-11f1-8000-0000000000b2:91-100", LostTransactionCount: &lost, LostComparedTo: "iad"},
				{Name: "dfw", State: v1alpha1.SiteStateReadOnly, RecoveryState: v1alpha1.RecoveryStateBlocked,
					DivergentGtid: "33333333-0000-11f1-8000-0000000000c3:7", DivergentTransactionCount: &divergent, DivergenceComparedTo: "iad"},
			},
			Conditions: []metav1.Condition{
				{Type: v1alpha1.ConditionDegraded, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonDivergentTransactions, Message: "dfw holds 1 transaction"},
				{Type: v1alpha1.ConditionRecoveryPending, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonOwnTransactionsLost, Message: waiting},
			},
		},
	}
	var buf bytes.Buffer
	if err := printGroupStatus(&buf, fg, "table"); err != nil {
		t.Fatalf("printGroupStatus: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"Site reports:\n",
		"  pdx lacks 10 transactions it issued itself, which the primary iad holds: 22222222-0000-11f1-8000-0000000000b2:91-100\n",
		"  dfw holds 1 transaction the primary lacks (compared with iad): 33333333-0000-11f1-8000-0000000000c3:7\n",
		// The whole message, not the 80 characters of the conditions table.
		"  Own transactions lost: " + waiting + "\n",
		"RecloneRequired",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}

	// A report that rests on another site than the primary says so, and
	// one whose holder the status does not carry says nothing about it.
	for holder, want := range map[string]string{
		"dfw": "  pdx lacks 10 transactions it issued itself, which dfw holds and the primary does not: 22222222-0000-11f1-8000-0000000000b2:91-100\n",
		"":    "  pdx lacks 10 transactions it issued itself: 22222222-0000-11f1-8000-0000000000b2:91-100\n",
	} {
		fg.Status.Sites[1].LostComparedTo = holder
		buf.Reset()
		if err := printGroupStatus(&buf, fg, "table"); err != nil {
			t.Fatalf("printGroupStatus: %v", err)
		}
		if !strings.Contains(buf.String(), want) {
			t.Errorf("holder %q: missing %q in output:\n%s", holder, want, buf.String())
		}
	}

	// A group without a report prints no section.
	fg.Status.Sites = fg.Status.Sites[:1]
	buf.Reset()
	if err := printGroupStatus(&buf, fg, "table"); err != nil {
		t.Fatalf("printGroupStatus: %v", err)
	}
	if strings.Contains(buf.String(), "Site reports") {
		t.Errorf("a group without a report shows the section:\n%s", buf.String())
	}
}
