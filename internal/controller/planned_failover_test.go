package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// plannedFG builds a minimal MysqlFailoverGroup suitable for driving
// the planned-failover parser + validator. Sites are declared as
// primary-candidate by default; the drOnly set overrides specific
// names. statusStates and replicating configure status.sites entries.
func plannedFG(sites, drOnly []string, active string, statusStates map[string]string, replicating map[string]bool) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
	}
	drSet := map[string]bool{}
	for _, n := range drOnly {
		drSet[n] = true
	}
	for _, name := range sites {
		role := v1alpha1.SiteRolePrimaryCandidate
		if drSet[name] {
			role = v1alpha1.SiteRoleDROnly
		}
		fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: name, Role: role})
		st := v1alpha1.SiteStatus{Name: name}
		if s, ok := statusStates[name]; ok {
			st.State = s
		} else {
			st.State = "read-only"
		}
		if r, ok := replicating[name]; ok {
			st.Replicating = r
		} else {
			st.Replicating = st.State == "read-only"
		}
		if name == active {
			st.State = "writable"
			st.Replicating = false
		}
		fg.Status.Sites = append(fg.Status.Sites, st)
	}
	fg.Status.ActiveSite = active
	return fg
}

func TestParsePlannedFailoverAnnotation(t *testing.T) {
	cases := []struct {
		in         string
		wantSite   string
		wantMaxLag time.Duration
		wantErr    string
	}{
		{"pdx", "pdx", 0, ""},
		{"pdx:maxLagWait=30s", "pdx", 30 * time.Second, ""},
		{"  pdx :maxLagWait= 2m ", "pdx", 2 * time.Minute, ""},
		{"pdx:maxLagWait=1h:maxLagWait=2m", "pdx", 2 * time.Minute, ""}, // last wins
		{"", "", 0, "empty"},
		{"pdx:foo=bar", "", 0, "unknown key"},
		{"pdx:maxLagWait=", "", 0, "not a valid duration"},
		{"pdx:maxLagWait=0s", "", 0, "must be positive"},
		{"pdx:abcdef", "", 0, "must be key=value"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parsePlannedFailoverAnnotation(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parse(%q) err = %v, want substring %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse(%q) unexpected error: %v", tc.in, err)
			}
			if got.Site != tc.wantSite {
				t.Errorf("site = %q, want %q", got.Site, tc.wantSite)
			}
			if got.MaxLagWait != tc.wantMaxLag {
				t.Errorf("maxLagWait = %v, want %v", got.MaxLagWait, tc.wantMaxLag)
			}
		})
	}
}

func TestValidatePlannedFailover_Accept(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	now := time.Now()
	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, now)
	if err != nil || result != PlannedFailoverAccept {
		t.Fatalf("expected accept, got result=%v err=%v", result, err)
	}
}

func TestValidatePlannedFailover_EmptySite(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{}, time.Now())
	if result != PlannedFailoverReject {
		t.Fatalf("expected reject, got %v", result)
	}
	if reason != "InvalidAnnotation" {
		t.Errorf("reason = %q, want InvalidAnnotation", reason)
	}
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("err = %v, want 'empty'", err)
	}
}

func TestValidatePlannedFailover_UnknownSite(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "sfo"}, time.Now())
	if result != PlannedFailoverReject || reason != "UnknownSite" {
		t.Fatalf("expected UnknownSite reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_DROnlyTargetRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx", "lhr"}, []string{"lhr"}, "iad", nil, nil)
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "lhr"}, time.Now())
	if result != PlannedFailoverReject || reason != "UnknownSite" {
		t.Fatalf("expected UnknownSite reject for dr-only target, got result=%v reason=%q err=%v", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "primary-candidate") {
		t.Errorf("err = %v, want to mention primary-candidate", err)
	}
}

func TestValidatePlannedFailover_ActiveSiteIsNoop(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "iad"}, time.Now())
	if result != PlannedFailoverSkip {
		t.Fatalf("expected skip (target is active), got %v (err=%v)", result, err)
	}
}

func TestValidatePlannedFailover_UnreachableTarget(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad",
		map[string]string{"pdx": "unreachable"},
		map[string]bool{"pdx": false})
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now())
	if result != PlannedFailoverReject || reason != "TargetUnhealthy" {
		t.Fatalf("expected TargetUnhealthy reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_TargetNotReplicating(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad",
		map[string]string{"pdx": "read-only"},
		map[string]bool{"pdx": false})
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now())
	if result != PlannedFailoverReject || reason != "TargetUnhealthy" {
		t.Fatalf("expected TargetUnhealthy reject, got result=%v reason=%q err=%v", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "not replicating") {
		t.Errorf("err = %v, want 'not replicating'", err)
	}
}

func TestValidatePlannedFailover_CooldownActive(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	cooldown := 5 * time.Minute
	fg.Spec.FailoverCooldown = &metav1.Duration{Duration: cooldown}
	now := time.Now()
	last := metav1.NewTime(now.Add(-2 * time.Minute))
	fg.Status.LastFailover = &last

	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, now)
	if result != PlannedFailoverReject || reason != "CooldownActive" {
		t.Fatalf("expected CooldownActive reject, got result=%v reason=%q err=%v", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "cooldown") {
		t.Errorf("err = %v, want 'cooldown'", err)
	}
}

func TestValidatePlannedFailover_CooldownExpired(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	cooldown := 5 * time.Minute
	fg.Spec.FailoverCooldown = &metav1.Duration{Duration: cooldown}
	now := time.Now()
	last := metav1.NewTime(now.Add(-10 * time.Minute))
	fg.Status.LastFailover = &last

	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, now)
	if result != PlannedFailoverAccept {
		t.Fatalf("expected accept once cooldown expired, got %v (err=%v)", result, err)
	}
}

func TestValidatePlannedFailover_ConcurrentRestoreRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring,
	}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now())
	if result != PlannedFailoverReject || reason != "ConcurrentOperation" {
		t.Fatalf("expected ConcurrentOperation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_ConcurrentUpdateRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.UpdatePhase = "UpdatingReplicas"
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now())
	if result != PlannedFailoverReject || reason != "ConcurrentOperation" {
		t.Fatalf("expected ConcurrentOperation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_InFlightPlannedFailoverRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhaseWaitingForLag,
	}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now())
	if result != PlannedFailoverReject || reason != "ConcurrentOperation" {
		t.Fatalf("expected ConcurrentOperation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_TerminalPlannedFailoverOK(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:  v1alpha1.PlannedFailoverPhaseSucceeded,
		Target: "pdx",
	}
	// Terminal Succeeded must not block a fresh run. Active site is "iad",
	// so pdx is a valid target.
	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now())
	if result != PlannedFailoverAccept {
		t.Fatalf("expected accept with prior terminal status, got %v (err=%v)", result, err)
	}
}

func TestEffectiveMaxLagWait(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	// Default fallback.
	if got := effectiveMaxLagWait(fg, PlannedFailoverRequest{}); got != defaultPlannedFailoverMaxLagWait {
		t.Errorf("default fallback: got %s, want %s", got, defaultPlannedFailoverMaxLagWait)
	}
	// Spec override.
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		MaxLagWait: &metav1.Duration{Duration: 2 * time.Minute},
	}
	if got := effectiveMaxLagWait(fg, PlannedFailoverRequest{}); got != 2*time.Minute {
		t.Errorf("spec override: got %s, want 2m", got)
	}
	// Annotation override beats spec.
	if got := effectiveMaxLagWait(fg, PlannedFailoverRequest{MaxLagWait: 10 * time.Second}); got != 10*time.Second {
		t.Errorf("annotation override: got %s, want 10s", got)
	}
}

func TestPlannedFailoverFencesSourcePrimary(t *testing.T) {
	cases := []struct {
		phase v1alpha1.PlannedFailoverPhase
		want  bool
	}{
		{v1alpha1.PlannedFailoverPhaseNone, false},
		{v1alpha1.PlannedFailoverPhasePending, false},
		{v1alpha1.PlannedFailoverPhaseValidating, false},
		{v1alpha1.PlannedFailoverPhaseDraining, true},
		{v1alpha1.PlannedFailoverPhaseWaitingForLag, true},
		{v1alpha1.PlannedFailoverPhasePromoting, true},
		{v1alpha1.PlannedFailoverPhaseResuming, false},
		{v1alpha1.PlannedFailoverPhaseSucceeded, false},
		{v1alpha1.PlannedFailoverPhaseFailed, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.phase), func(t *testing.T) {
			fg := &v1alpha1.MysqlFailoverGroup{
				Status: v1alpha1.MysqlFailoverGroupStatus{
					PlannedFailover: &v1alpha1.PlannedFailoverStatus{Phase: tc.phase},
				},
			}
			if got := plannedFailoverFencesSourcePrimary(fg); got != tc.want {
				t.Errorf("phase=%q fences=%v, want %v", tc.phase, got, tc.want)
			}
		})
	}
	// nil status returns false
	if plannedFailoverFencesSourcePrimary(&v1alpha1.MysqlFailoverGroup{}) {
		t.Error("nil status should not fence")
	}
}

func TestPlannedFailoverTransactionsLost(t *testing.T) {
	cases := []struct {
		name   string
		source string
		target string
		want   int64
	}{
		{"identical", "abc:1-10", "abc:1-10", 0},
		{"superset-target", "abc:1-10", "abc:1-15", 0},
		{"missing-tail", "abc:1-10", "abc:1-7", 3},
		{"disjoint-uuid", "abc:1-5", "def:1-5", 5},
		{"empty-source", "", "abc:1-5", 0},
		{"malformed", "not-a-gtid", "abc:1-5", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := plannedFailoverTransactionsLost(tc.source, tc.target)
			if got != tc.want {
				t.Errorf("lost(%q, %q) = %d, want %d", tc.source, tc.target, got, tc.want)
			}
		})
	}
}

func TestGtidContains(t *testing.T) {
	cases := []struct {
		sup, sub string
		want     bool
	}{
		{"abc:1-10", "abc:1-10", true},
		{"abc:1-10", "abc:1-5", true},
		{"abc:1-5", "abc:1-10", false},
		{"abc:1-10", "abc:1-5,def:1-2", false},
		{"abc:1-10,def:1-2", "abc:1-5", true},
	}
	for _, tc := range cases {
		t.Run(tc.sup+" ⊇ "+tc.sub, func(t *testing.T) {
			ok, err := gtidContains(tc.sup, tc.sub)
			if err != nil {
				t.Fatalf("unexpected parse err: %v", err)
			}
			if ok != tc.want {
				t.Errorf("contains(%q ⊇ %q) = %v, want %v", tc.sup, tc.sub, ok, tc.want)
			}
		})
	}
}
