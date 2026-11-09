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
func plannedFG(sites, drOnly []string, active string, statusStates map[string]v1alpha1.SiteState, replicating map[string]bool) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
	}
	drSet := map[string]bool{}
	for _, n := range drOnly {
		drSet[n] = true
	}
	for i, name := range sites {
		role := v1alpha1.SiteRolePrimaryCandidate
		if drSet[name] {
			role = v1alpha1.SiteRoleDisasterRecovery
		}
		fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: name, ServerID: int64(101 + i), Role: role})
		st := v1alpha1.SiteStatus{Name: name}
		if s, ok := statusStates[name]; ok {
			st.State = s
		} else {
			st.State = v1alpha1.SiteStateReadOnly
		}
		running, ok := replicating[name]
		if !ok {
			running = st.State == v1alpha1.SiteStateReadOnly
		}
		st.Replication = v1alpha1.SiteReplicationBroken
		if running {
			st.Replication = v1alpha1.SiteReplicationHealthy
		}
		if name == active {
			st.State = v1alpha1.SiteStateWritable
			st.Replication = v1alpha1.SiteReplicationNotApplicable
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
		{"  pdx:maxLagWait=2m\n", "pdx", 2 * time.Minute, ""}, // the whole value is trimmed
		{"pdx:maxLagWait=1.5m", "pdx", 90 * time.Second, ""},
		// A-78: <subject>(:<key>=<value>)*, values in [A-Za-z0-9._/+-], each
		// key once, no empty pair.
		{" pdx :maxLagWait= 2m ", "", 0, "characters outside"},
		{"pdx:maxLagWait=1h:maxLagWait=2m", "", 0, "more than once"},
		{"pdx::maxLagWait=2m", "", 0, "must be key=value"},
		{"pdx:", "", 0, "must be key=value"},
		{"pdx:=2m", "", 0, "must be key=value"},
		{"pdx:maxLagWait=2m=3m", "", 0, "the value must match"},
		{"", "", 0, "empty"},
		{"pdx:foo=bar", "", 0, "unknown key"},
		{"pdx:maxLagWait=", "", 0, "the value must match"},
		{"pdx:maxLagWait=soon", "", 0, "not a valid duration"},
		// WE-Q3: the override takes spec.plannedFailover.maxLagWait's 1s..1h.
		{"pdx:maxLagWait=0s", "", 0, "maxLagWait must be between 1s and 1h (got 0s)"},
		{"pdx:maxLagWait=-5s", "", 0, "maxLagWait must be between 1s and 1h"},
		{"pdx:maxLagWait=999ms", "", 0, "maxLagWait must be between 1s and 1h"},
		{"pdx:maxLagWait=1s", "pdx", time.Second, ""},
		{"pdx:maxLagWait=1h", "pdx", time.Hour, ""},
		{"pdx:maxLagWait=1h0m1s", "", 0, "maxLagWait must be between 1s and 1h"},
		{"pdx:maxLagWait=24h", "", 0, "maxLagWait must be between 1s and 1h"},
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
	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, now, false)
	if err != nil || result != PlannedFailoverAccept {
		t.Fatalf("expected accept, got result=%v err=%v", result, err)
	}
}

func TestValidatePlannedFailover_EmptySite(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{}, time.Now(), false)
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
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "sfo"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "UnknownSite" {
		t.Fatalf("expected UnknownSite reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_DROnlyTargetRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx", "lhr"}, []string{"lhr"}, "iad", nil, nil)
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "lhr"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != v1alpha1.PlannedFailoverReasonTargetNotPromotable {
		t.Fatalf("expected TargetNotPromotable reject for dr-only target, got result=%v reason=%q err=%v", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "PrimaryCandidate") {
		t.Errorf("err = %v, want to mention PrimaryCandidate", err)
	}
}

func TestValidatePlannedFailover_ActiveSiteIsNoop(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "iad"}, time.Now(), false)
	if result != PlannedFailoverSkip {
		t.Fatalf("expected skip (target is active), got %v (err=%v)", result, err)
	}
}

func TestValidatePlannedFailover_TargetMidRotation(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "pdx", Phase: v1alpha1.KeyringPhaseUnsealed, UnsealReason: v1alpha1.UnsealReasonRotation,
		}},
	}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "KeyringRotation" {
		t.Fatalf("expected KeyringRotation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "UnsealReason=Rotation") {
		t.Errorf("err = %v, want UnsealReason=Rotation", err)
	}
}

func TestValidatePlannedFailover_TargetMidRotationBeatsUnhealthy(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad",
		map[string]v1alpha1.SiteState{"pdx": v1alpha1.SiteStateUnreachable},
		map[string]bool{"pdx": false})
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "pdx", Phase: v1alpha1.KeyringPhaseUnsealed, UnsealReason: v1alpha1.UnsealReasonRotation,
		}},
	}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "KeyringRotation" {
		t.Fatalf("expected KeyringRotation over TargetUnhealthy, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_UnreachableTarget(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad",
		map[string]v1alpha1.SiteState{"pdx": v1alpha1.SiteStateUnreachable},
		map[string]bool{"pdx": false})
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "TargetUnhealthy" {
		t.Fatalf("expected TargetUnhealthy reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_TargetNotReplicating(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad",
		map[string]v1alpha1.SiteState{"pdx": v1alpha1.SiteStateReadOnly},
		map[string]bool{"pdx": false})
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "TargetUnhealthy" {
		t.Fatalf("expected TargetUnhealthy reject, got result=%v reason=%q err=%v", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "not replicating") {
		t.Errorf("err = %v, want 'not replicating'", err)
	}
}

func TestValidatePlannedFailover_Cooldown(t *testing.T) {
	cooldown := 5 * time.Minute
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		statusAgo   time.Duration
		annotation  *FailoverRecord
		wantResult  PlannedFailoverValidationResult
		wantReason  v1alpha1.PlannedFailoverReason
		wantErr     string
		wantRetryAt time.Time
	}{
		{
			name:        "status-only active",
			statusAgo:   2 * time.Minute,
			wantResult:  PlannedFailoverReject,
			wantReason:  "CooldownActive",
			wantErr:     "cooldown",
			wantRetryAt: now.Add(3 * time.Minute),
		},
		{
			name:        "status-only expired",
			statusAgo:   10 * time.Minute,
			wantResult:  PlannedFailoverAccept,
			wantRetryAt: now.Add(-5 * time.Minute),
		},
		{
			name:      "annotation newer",
			statusAgo: 10 * time.Minute,
			annotation: &FailoverRecord{
				LastFailover:       now.Add(-30 * time.Second),
				LastFailoverTarget: "pdx",
			},
			wantResult:  PlannedFailoverReject,
			wantReason:  "CooldownActive",
			wantErr:     "cooldown",
			wantRetryAt: now.Add(4*time.Minute + 30*time.Second),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
			fg.Spec.FailoverCooldown = &metav1.Duration{Duration: cooldown}
			last := metav1.NewTime(now.Add(-tc.statusAgo))
			fg.Status.SetLastFailover(&last, fg.Status.LastFailoverTarget(), fg.Status.PromotionGtidExecuted())
			fg.Status.SetLastFailover(fg.Status.LastFailoverTime(), "iad", fg.Status.PromotionGtidExecuted())
			if tc.annotation != nil {
				fg.Annotations = map[string]string{
					v1alpha1.LastFailoverAnnotation:       tc.annotation.LastFailover.Format(time.RFC3339),
					v1alpha1.LastFailoverTargetAnnotation: tc.annotation.LastFailoverTarget,
				}
			}

			result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, now, false)
			if result != tc.wantResult || reason != tc.wantReason {
				t.Fatalf("validation = (%v, %q, %v), want result=%v reason=%q", result, reason, err, tc.wantResult, tc.wantReason)
			}
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("err = %v, want substring %q", err, tc.wantErr)
			}
			if got := cooldownRetryAfter(fg, now); !got.Equal(tc.wantRetryAt) {
				t.Errorf("cooldown retry = %v, want %v", got, tc.wantRetryAt)
			}
		})
	}
}

func TestValidatePlannedFailover_DurableStateErrors(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		withStatus bool
		wantReason v1alpha1.PlannedFailoverReason
		wantErr    string
	}{
		{
			name:       "no valid copy",
			wantReason: "InvalidFailoverState",
			wantErr:    "durable anti-flap state is invalid",
		},
		{
			name:       "valid status fallback",
			withStatus: true,
			wantReason: "CooldownActive",
			wantErr:    "cooldown",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
			fg.Annotations = map[string]string{
				v1alpha1.LastFailoverAnnotation:       "not-a-timestamp",
				v1alpha1.LastFailoverTargetAnnotation: "iad",
			}
			if tc.withStatus {
				last := metav1.NewTime(now.Add(-time.Minute))
				fg.Status.SetLastFailover(&last, fg.Status.LastFailoverTarget(), fg.Status.PromotionGtidExecuted())
				fg.Status.SetLastFailover(fg.Status.LastFailoverTime(), "iad", fg.Status.PromotionGtidExecuted())
			}

			result, reason, err := validatePlannedFailoverRequest(
				fg, PlannedFailoverRequest{Site: "pdx"}, now, false)
			if result != PlannedFailoverReject || reason != tc.wantReason {
				t.Fatalf("expected %s reject, got result=%v reason=%q err=%v", tc.wantReason, result, reason, err)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidatePlannedFailover_ConcurrentRestoreRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring,
	}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "ConcurrentOperation" {
		t.Fatalf("expected ConcurrentOperation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_ConcurrentUpdateRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.UpdatePhase = "UpdatingReplicas"
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "ConcurrentOperation" {
		t.Fatalf("expected ConcurrentOperation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
}

func TestValidatePlannedFailover_InFlightPlannedFailoverRejected(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		RunID: "run-1", Phase: v1alpha1.PlannedFailoverPhaseWaitingForLag,
	}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "ConcurrentOperation" {
		t.Fatalf("expected ConcurrentOperation reject, got result=%v reason=%q err=%v", result, reason, err)
	}
	// The run itself passes the check on its own admission passes.
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseAdmitting
	if result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), true); result != PlannedFailoverAccept {
		t.Fatalf("own Admitting run refused: %v %v", result, err)
	}
	// A run the terminal marker ended does not block a new one.
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseWaitingForLag
	fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`}
	if result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false); result != PlannedFailoverAccept {
		t.Fatalf("marked run blocks a new request: %v %v", result, err)
	}
}

func TestValidatePlannedFailover_TerminalPlannedFailoverOK(t *testing.T) {
	fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:      v1alpha1.PlannedFailoverPhaseSucceeded,
		TargetSite: "pdx",
	}
	// Terminal Succeeded must not block a fresh run. Active site is "iad",
	// so pdx is a valid target.
	result, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
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

// syncPodLabels fences the run's source by name, only while the run owns
// the sites and is not marked.
func TestPlannedFailoverFencedSite(t *testing.T) {
	cases := []struct {
		phase v1alpha1.PlannedFailoverPhase
		want  string
	}{
		{v1alpha1.PlannedFailoverPhaseAdmitting, ""},
		{v1alpha1.PlannedFailoverPhaseDeferred, ""},
		{v1alpha1.PlannedFailoverPhaseFencing, "iad"},
		{v1alpha1.PlannedFailoverPhaseDraining, "iad"},
		{v1alpha1.PlannedFailoverPhaseWaitingForLag, "iad"},
		{v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync, "iad"},
		{v1alpha1.PlannedFailoverPhasePromotingDragonfly, "iad"},
		{v1alpha1.PlannedFailoverPhasePromoting, "iad"},
		{v1alpha1.PlannedFailoverPhaseRollingBack, "iad"},
		{v1alpha1.PlannedFailoverPhasePublishing, ""},
		{v1alpha1.PlannedFailoverPhaseSucceeded, ""},
		{v1alpha1.PlannedFailoverPhaseFailed, ""},
	}
	for _, tc := range cases {
		t.Run(string(tc.phase), func(t *testing.T) {
			fg := &v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{
				PlannedFailover: &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: tc.phase, SourceSite: "iad", TargetSite: "pdx"},
			}}
			if got := plannedFailoverFencedSite(fg); got != tc.want {
				t.Errorf("fenced site = %q, want %q", got, tc.want)
			}
			fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`}
			if got := plannedFailoverFencedSite(fg); got != "" {
				t.Errorf("marked run fences %q", got)
			}
		})
	}
	if plannedFailoverFencedSite(&v1alpha1.MysqlFailoverGroup{}) != "" {
		t.Error("nil status should not fence")
	}
}

// TestPlannedFailoverPredicates_MarkerIsTerminal: a terminal marker naming
// the run makes every predicate treat it as terminal, whatever the phase;
// a marker naming another run, or a malformed one, changes nothing.
func TestPlannedFailoverPredicates_MarkerIsTerminal(t *testing.T) {
	for _, phase := range plannedEnginePhases {
		fg := &v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{
			PlannedFailover: &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: phase, SourceSite: "iad", TargetSite: "pdx"},
		}}
		live := plannedFailoverActive(fg)
		if !live {
			t.Errorf("%s: unmarked run not active", phase)
		}
		for _, other := range []string{`{"schemaVersion":1,"runID":"run-2","outcome":"SourceLeftFenced","reason":"StatusStalled"}`, `{"schemaVersion":1,"runID":"run-1","outcome":"Bogus","reason":"StatusStalled"}`, "garbage"} {
			fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: other}
			if !plannedFailoverActive(fg) {
				t.Errorf("%s: marker %q ended the run", phase, other)
			}
		}
		fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`}
		if plannedFailoverActive(fg) || plannedFailoverOwnsSites(fg) || plannedFailoverPending(fg) || plannedFailoverFencedSite(fg) != "" ||
			plannedFailoverDragonflyStripActive(fg) || plannedDragonflyTakeoverSucceeded(fg) {
			t.Errorf("%s: a marked run is still live to a predicate", phase)
		}
		if _, _, fenced := groupFenced(fg); fenced {
			t.Errorf("%s: groupFenced on a marked run", phase)
		}
		if DeploymentUnstableReason(fg) == "PlannedFailover" {
			t.Errorf("%s: a marked run keeps the deployment unstable", phase)
		}
	}
}
