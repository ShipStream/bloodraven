package controller

import (
	"errors"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// contractEnumValues returns the values of the kubebuilder Enum marker that
// immediately precedes "type <typeName> string" in the API source, so the
// tests below fail when an enum value is added without classifying it.
func contractEnumValues(t *testing.T, typeName string) []string {
	t.Helper()
	src, err := os.ReadFile("../../api/v1alpha1/planned_failover_types.go")
	if err != nil {
		t.Fatalf("read API source: %v", err)
	}
	re := regexp.MustCompile(`// \+kubebuilder:validation:Enum=(\S+)\ntype ` + typeName + ` string`)
	m := re.FindSubmatch(src)
	if m == nil {
		t.Fatalf("no Enum marker found for %s", typeName)
	}
	var out []string
	for _, v := range strings.Split(string(m[1]), ";") {
		out = append(out, strings.Trim(v, `"`))
	}
	return out
}

func contractGroup(pf *v1alpha1.PlannedFailoverStatus, annotations map[string]string) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{}
	fg.Annotations = annotations
	fg.Status.PlannedFailover = pf
	return fg
}

// Every phase in the API enum is classified, and the predicates agree with
// the phase table: Admitting/Deferred are pending and own nothing; Fencing
// through Promoting and RollingBack own the sites; Publishing is only
// active; terminal and legacy phases are none of these. A terminal marker
// naming the run makes every phase terminal.
func TestPlannedFailoverPhasePredicates_Exhaustive(t *testing.T) {
	type want struct{ pending, owns, active bool }
	table := map[v1alpha1.PlannedFailoverPhase]want{
		plannedLegacyPhaseNone:                               {},
		plannedLegacyPhasePending:                            {},
		plannedLegacyPhaseValidating:                         {},
		plannedLegacyPhaseResuming:                           {},
		v1alpha1.PlannedFailoverPhaseAdmitting:               {pending: true, active: true},
		v1alpha1.PlannedFailoverPhaseDeferred:                {pending: true, active: true},
		v1alpha1.PlannedFailoverPhaseFencing:                 {owns: true, active: true},
		v1alpha1.PlannedFailoverPhaseDraining:                {owns: true, active: true},
		v1alpha1.PlannedFailoverPhaseWaitingForLag:           {owns: true, active: true},
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync: {owns: true, active: true},
		v1alpha1.PlannedFailoverPhasePromotingDragonfly:      {owns: true, active: true},
		v1alpha1.PlannedFailoverPhasePromoting:               {owns: true, active: true},
		v1alpha1.PlannedFailoverPhaseRollingBack:             {owns: true, active: true},
		v1alpha1.PlannedFailoverPhasePublishing:              {active: true},
		v1alpha1.PlannedFailoverPhaseSucceeded:               {},
		v1alpha1.PlannedFailoverPhaseFailed:                  {},
	}
	enum := contractEnumValues(t, "PlannedFailoverPhase")
	for _, v := range enum {
		if _, ok := table[v1alpha1.PlannedFailoverPhase(v)]; !ok {
			t.Errorf("phase %q is in the API enum but not classified here", v)
		}
	}
	legacyPhases := []v1alpha1.PlannedFailoverPhase{plannedLegacyPhaseNone, plannedLegacyPhasePending, plannedLegacyPhaseValidating, plannedLegacyPhaseResuming}
	for phase := range table {
		if inEnum := slices.Contains(enum, string(phase)); inEnum == slices.Contains(legacyPhases, phase) {
			t.Errorf("phase %q: in the API enum = %v, but it is legacy = %v", phase, inEnum, !inEnum)
		}
	}

	for phase, w := range table {
		pf := &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: phase, SourceSite: "iad", TargetSite: "pdx"}
		fg := contractGroup(pf, nil)
		if got := plannedFailoverPending(fg); got != w.pending {
			t.Errorf("%q: pending = %v, want %v", phase, got, w.pending)
		}
		if got := plannedFailoverOwnsSites(fg); got != w.owns {
			t.Errorf("%q: ownsSites = %v, want %v", phase, got, w.owns)
		}
		if got := plannedFailoverActive(fg); got != w.active {
			t.Errorf("%q: active = %v, want %v", phase, got, w.active)
		}
		wantFenced := ""
		if w.owns {
			wantFenced = "iad"
		}
		if got := plannedFailoverFencedSite(fg); got != wantFenced {
			t.Errorf("%q: fencedSite = %q, want %q", phase, got, wantFenced)
		}
		if _, marked := plannedFailoverRun(fg); marked {
			t.Errorf("%q: marked with no marker", phase)
		}

		// A marker naming the run ends it, whatever the phase.
		marker := TerminalMarker{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
			Reason: v1alpha1.PlannedFailoverReasonStatusStalled}
		fg = contractGroup(pf, map[string]string{PlannedFailoverTerminalAnnotation: marker.String()})
		if got, marked := plannedFailoverRun(fg); got != pf || !marked {
			t.Errorf("%q: plannedFailoverRun = %p, %v; want the block, marked", phase, got, marked)
		}
		if plannedFailoverPending(fg) || plannedFailoverOwnsSites(fg) || plannedFailoverActive(fg) || plannedFailoverFencedSite(fg) != "" {
			t.Errorf("%q: a marked run is still pending, owning or active", phase)
		}

		// A marker naming another run, or a malformed one, does not.
		for _, v := range []string{`{"schemaVersion":1,"runID":"run-2","outcome":"SourceLeftFenced","reason":"StatusStalled"}`, `{"schemaVersion":1,"runID":"run-1","outcome":"Bogus","reason":"StatusStalled"}`, "run-1"} {
			fg = contractGroup(pf, map[string]string{PlannedFailoverTerminalAnnotation: v})
			if _, marked := plannedFailoverRun(fg); marked {
				t.Errorf("%q: marker %q marks the run", phase, v)
			}
			if got := plannedFailoverOwnsSites(fg); got != w.owns {
				t.Errorf("%q: marker %q changed ownsSites to %v", phase, v, got)
			}
		}
	}

	// No block, or no group: nothing.
	for _, fg := range []*v1alpha1.MysqlFailoverGroup{nil, contractGroup(nil, nil)} {
		if pf, marked := plannedFailoverRun(fg); pf != nil || marked {
			t.Errorf("plannedFailoverRun(no block) = %v, %v", pf, marked)
		}
		if plannedFailoverPending(fg) || plannedFailoverOwnsSites(fg) || plannedFailoverActive(fg) || plannedFailoverFencedSite(fg) != "" {
			t.Error("a group with no block is pending, owning or active")
		}
	}

	// A legacy block has no runId, so no marker can name it.
	legacy := &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining}
	fg := contractGroup(legacy, map[string]string{PlannedFailoverTerminalAnnotation: "/SourceLeftFenced/"})
	if _, marked := plannedFailoverRun(fg); marked {
		t.Error("a legacy block without a runId is marked")
	}
}

// The outcome and reason lists the marker parser accepts are exactly the
// API enums.
func TestPlannedFailoverEnumListsMatchAPI(t *testing.T) {
	var outcomes, reasons []string
	for _, o := range plannedFailoverOutcomes {
		outcomes = append(outcomes, string(o))
	}
	for _, r := range plannedFailoverReasons {
		reasons = append(reasons, string(r))
	}
	if want := contractEnumValues(t, "PlannedFailoverOutcome"); !slices.Equal(outcomes, want) {
		t.Errorf("plannedFailoverOutcomes = %v, want the API enum %v", outcomes, want)
	}
	if want := contractEnumValues(t, "PlannedFailoverReason"); !slices.Equal(reasons, want) {
		t.Errorf("plannedFailoverReasons = %v, want the API enum %v", reasons, want)
	}
}

func TestTerminalMarker_RoundTrip(t *testing.T) {
	for _, m := range []TerminalMarker{
		{RunID: "3f1c2a9e-0000-4000-8000-000000000001", Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded},
		{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled},
		{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonForeignWritable},
	} {
		got, ok := ParseTerminalMarker(m.String())
		if !ok || got != m {
			t.Errorf("ParseTerminalMarker(%q) = %+v, %v; want %+v", m.String(), got, ok, m)
		}
	}
	if s := (TerminalMarker{RunID: "r", Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonStatusStalled}).String(); s != `{"schemaVersion":1,"runID":"r","outcome":"NotStarted","reason":"StatusStalled"}` {
		t.Errorf("String() = %q", s)
	}
	for _, bad := range []string{
		"", "run-1", "run-1/Succeeded", "/Succeeded/", "run-1/Unknown/", `{"schemaVersion":1,"runID":"run-1","outcome":"Succeeded","reason":"NotAReason"}`,
		"run-1/Succeeded//extra", "run/1/Succeeded/",
		// The pre-JSON form, a future schema version, an unknown member,
		// an empty run, trailing data.
		"run-1/Succeeded/",
		`{"schemaVersion":2,"runID":"run-1","outcome":"Succeeded"}`,
		`{"runID":"run-1","outcome":"Succeeded"}`,
		`{"schemaVersion":1,"runID":"run-1","outcome":"Succeeded","extra":true}`,
		`{"schemaVersion":1,"runID":"","outcome":"Succeeded"}`,
		`{"schemaVersion":1,"runID":"run-1","outcome":"Succeeded"} {}`,
	} {
		if m, ok := ParseTerminalMarker(bad); ok {
			t.Errorf("ParseTerminalMarker(%q) = %+v, accepted", bad, m)
		}
	}
}

// recordFailover stamps the in-memory record with a nanosecond clock, while
// both durable copies hold seconds. The comparison must match them.
func TestSameFailoverRecord_NanosecondClock(t *testing.T) {
	loc := time.FixedZone("UTC+2", 2*60*60)
	nanos := time.Date(2026, 11, 4, 12, 30, 45, 987654321, loc)
	inMemory := FailoverRecordRef{LastFailover: nanos, LastFailoverTarget: "pdx"}

	// The status copy round-trips through metav1.Time at second precision.
	status := failoverRecordRefFromAPI(newFailoverRecordRef(nanos, "pdx").toAPI())
	if !sameFailoverRecord(inMemory, status) {
		t.Fatalf("in-memory %v and status %v differ", inMemory, status)
	}
	if status.LastFailover.Nanosecond() != 0 || status.LastFailover.Location() != time.UTC {
		t.Fatalf("status copy %v is not second precision UTC", status.LastFailover)
	}
	// The annotation copy is RFC3339 at second precision.
	oob, err := FailoverRecordFromAnnotations(map[string]string{
		LastFailoverAnnotation:       failoverStampFormat(nanos),
		LastFailoverTargetAnnotation: "pdx",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sameFailoverRecord(inMemory, newFailoverRecordRef(oob.LastFailover, oob.LastFailoverTarget)) {
		t.Fatalf("in-memory %v and annotation %v differ", inMemory, oob)
	}

	if sameFailoverRecord(inMemory, newFailoverRecordRef(nanos.Add(time.Second), "pdx")) {
		t.Error("records a second apart are the same")
	}
	if sameFailoverRecord(inMemory, newFailoverRecordRef(nanos, "iad")) {
		t.Error("records with different targets are the same")
	}
	if !sameFailoverRecord(FailoverRecordRef{}, failoverRecordRefFromAPI(nil)) ||
		!sameFailoverRecord(FailoverRecordRef{}, failoverRecordRefFromAPI(FailoverRecordRef{}.toAPI())) {
		t.Error("no history does not round-trip as no history")
	}
	if (FailoverRecordRef{}).toAPI().Time != nil {
		t.Error("a zero time is persisted")
	}
}

// durableFailoverRecord is the newer of status and the out-of-band copy,
// truncated.
func TestDurableFailoverRecord(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	older := metav1.NewTime(now.Add(-time.Hour))
	fg := &v1alpha1.MysqlFailoverGroup{}
	fg.Spec.Sites = []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}
	fg.Status.LastFailover = &older
	fg.Status.LastFailoverTarget = "iad"
	fg.Annotations = map[string]string{
		LastFailoverAnnotation:       failoverStampFormat(now.Add(-time.Minute)),
		LastFailoverTargetAnnotation: "pdx",
	}
	got, err := durableFailoverRecord(fg, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := newFailoverRecordRef(now.Add(-time.Minute), "pdx"); !sameFailoverRecord(got, want) {
		t.Fatalf("durableFailoverRecord = %v, want the newer annotation copy %v", got, want)
	}
}

func TestPlannedHoldFrom(t *testing.T) {
	hold := v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		SourceSite: "iad", TargetSite: "pdx", AmbiguousSites: []string{"iad", "pdx"}, HoldTime: metav1.Now()}
	held := func() v1alpha1.MysqlFailoverGroupStatus {
		var st v1alpha1.MysqlFailoverGroupStatus
		setPlannedHold(&st, hold, 7)
		return st
	}
	acknowledged := func() v1alpha1.MysqlFailoverGroupStatus {
		st := held()
		if !ClearPlannedHold(&st, "run-1", 8, time.Now()) {
			t.Fatal("ClearPlannedHold did not clear the hold it names")
		}
		return st
	}
	st := held()
	c := apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionAutomaticFailoverHeld)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonPlannedFailoverManualRecoveryRequired ||
		c.ObservedGeneration != 7 || !strings.Contains(c.Message, AcknowledgePlannedFailoverHoldAnnotation+"=run-1") ||
		!strings.Contains(c.Message, "iad, pdx") {
		t.Fatalf("hold condition = %+v, want True/%s at generation 7 naming the ack and the sites", c,
			v1alpha1.ReasonPlannedFailoverManualRecoveryRequired)
	}
	if ClearPlannedHold(&st, "run-2", 8, time.Now()) || st.PlannedFailoverHold == nil {
		t.Fatal("ClearPlannedHold cleared a hold of another run")
	}
	st = acknowledged()
	c = apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionAutomaticFailoverHeld)
	if st.PlannedFailoverHold != nil || c == nil || c.Status != metav1.ConditionFalse ||
		c.Reason != v1alpha1.ReasonPlannedFailoverHoldAcknowledged || c.ObservedGeneration != 8 {
		t.Fatalf("acknowledged hold = %+v, condition %+v; want no hold and False/%s at generation 8",
			st.PlannedFailoverHold, c, v1alpha1.ReasonPlannedFailoverHoldAcknowledged)
	}
	holdMarker := TerminalMarker{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
		Reason: v1alpha1.PlannedFailoverReasonExecuteFailed}

	cases := []struct {
		name        string
		status      v1alpha1.MysqlFailoverGroupStatus
		annotations map[string]string
		want        *PlannedHold
	}{
		{name: "nothing"},
		{
			name:   "status hold",
			status: held(),
			want:   &PlannedHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable, FromStatus: true},
		},
		{name: "acknowledged (condition False)", status: acknowledged()},
		{
			name:        "marker",
			annotations: map[string]string{PlannedFailoverTerminalAnnotation: holdMarker.String()},
			want:        &PlannedHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonExecuteFailed, FromMarker: true},
		},
		{
			name: "marker with another outcome",
			annotations: map[string]string{PlannedFailoverTerminalAnnotation: TerminalMarker{RunID: "run-1",
				Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled}.String()},
		},
		{
			name:        "both",
			status:      held(),
			annotations: map[string]string{PlannedFailoverTerminalAnnotation: holdMarker.String()},
			want:        &PlannedHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable, FromStatus: true, FromMarker: true},
		},
		{
			name:   "both, different runs",
			status: held(),
			annotations: map[string]string{PlannedFailoverTerminalAnnotation: TerminalMarker{RunID: "run-2",
				Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}.String()},
			want: &PlannedHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable, FromStatus: true},
		},
		{
			name: "True condition without the status hold still holds, naming no run",
			status: v1alpha1.MysqlFailoverGroupStatus{Conditions: []metav1.Condition{{Type: v1alpha1.ConditionAutomaticFailoverHeld,
				Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPlannedFailoverManualRecoveryRequired, Message: "edited by hand"}}},
			want: &PlannedHold{FromStatus: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := contractGroup(nil, tc.annotations)
			fg.Status.Conditions = tc.status.Conditions
			fg.Status.PlannedFailoverHold = tc.status.PlannedFailoverHold
			got := PlannedHoldFrom(fg)
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("PlannedHoldFrom = %+v, want nil", *got)
			case tc.want != nil && (got == nil || *got != *tc.want):
				t.Fatalf("PlannedHoldFrom = %+v, want %+v", got, *tc.want)
			}
		})
	}
	if PlannedHoldFrom(nil) != nil {
		t.Fatal("PlannedHoldFrom(nil) is a hold")
	}
}

func TestDeadlineKindAnnotation(t *testing.T) {
	if DeadlineStatusStall.Annotation() != PlannedFailoverStallSinceAnnotation ||
		DeadlineManagerAbsent.Annotation() != PlannedFailoverManagerAbsentSinceAnnotation ||
		DeadlineKind("other").Annotation() != "" {
		t.Fatal("DeadlineKind.Annotation maps the wrong annotation")
	}
}

// The sentinels of the facade's error contract are distinct, so the engine
// can tell "nothing was sent" causes apart with errors.Is.
func TestPlannedFailoverSentinelsDistinct(t *testing.T) {
	sentinels := []error{errManagerAbsent, errClaimLost, errLeaderLost, errReproveRequired, errGateBusy}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if (i == j) != errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = %v", a, b, i != j)
			}
		}
	}
}

// TestRunnerPlannedClaim_NoManager: with no topology manager registered the
// runner's planned-failover entry points accept nothing (a claim waits on
// ManagerAbsent, no Dragonfly manager takes a pause or a follow-up), and
// the group's slot is still the one source of truth: a held claim is
// reported, and only its own run releases it.
func TestRunnerPlannedClaim_NoManager(t *testing.T) {
	nn := types.NamespacedName{Namespace: "ns", Name: "fg"}
	a, b := &PlannedClaimSlot{}, &PlannedClaimSlot{}
	if a == b {
		t.Error("two slots share an address")
	}

	r := &TopologyManagerRunner{}
	if r.PlannedClaimHeld(nn) || r.SetDragonflyUpgradePaused(nn, true) || r.EnqueueDragonflyFollowUp(nn, "pdx", "iad") ||
		r.ReleasePlannedFailoverClaim(nn, "run-1", ReleaseTerminal) {
		t.Error("a runner with no manager accepted a planned-failover call")
	}
	if site, ok := r.ActiveSite(nn); site != "" || ok {
		t.Errorf("ActiveSite = %q, %v", site, ok)
	}
	d, err := r.ClaimPlannedFailover(t.Context(), nn, PlannedClaimRequest{RunID: "run-1", Target: "pdx"})
	if err != nil || d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitManagerAbsent {
		t.Errorf("ClaimPlannedFailover = %+v, %v; want Wait{ManagerAbsent}", d, err)
	}
	r.Slot(nn).hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "iad", target: "pdx"})
	if !r.PlannedClaimHeld(nn) {
		t.Error("a held slot is not reported")
	}
	if r.ReleasePlannedFailoverClaim(nn, "run-2", ReleaseTerminal) || !r.ReleasePlannedFailoverClaim(nn, "run-1", ReleaseTerminal) {
		t.Error("the slot was released by another run, or not by its own")
	}
}

// The operator-written annotation values follow A-78: no ':' inside an
// instant, and every value of the deadline starts in the value charset.
func TestPlannedAnnotationValueFormats(t *testing.T) {
	at := time.Date(2026, 11, 4, 10, 0, 0, 987654321, time.FixedZone("x", 3600))
	v := FormatDeadlineStart("6f1d0c3a-5b8e-4c1f-9a2d-0e7b3c4d5e6f", at)
	if v != "6f1d0c3a-5b8e-4c1f-9a2d-0e7b3c4d5e6f:since=20261104T090000Z" {
		t.Fatalf("FormatDeadlineStart = %q", v)
	}
	id, got, ok := ParseDeadlineStart(v)
	if !ok || id != "6f1d0c3a-5b8e-4c1f-9a2d-0e7b3c4d5e6f" || !got.Equal(at.Truncate(time.Second)) {
		t.Fatalf("ParseDeadlineStart(%q) = %q %v %v", v, id, got, ok)
	}
	for _, bad := range []string{"", "run-1", "run-1/2026-11-04T09:00:00Z", ":since=20261104T090000Z",
		"run-1:until=20261104T090000Z", "run-1:since=2026-11-04T09:00:00Z"} {
		if _, _, ok := ParseDeadlineStart(bad); ok {
			t.Errorf("ParseDeadlineStart(%q) accepted", bad)
		}
	}
	valueRE := regexp.MustCompile(`^[A-Za-z0-9._/+-]+$`)
	_, pair, _ := strings.Cut(v, ":")
	if _, ts, _ := strings.Cut(pair, "="); !valueRE.MatchString(ts) {
		t.Errorf("deadline instant %q is outside the A-78 value charset", ts)
	}
	fence := FormatStatusFenceValue(at)
	if fence != "20261104T090000.987654321Z" || !valueRE.MatchString(fence) {
		t.Errorf("FormatStatusFenceValue = %q", fence)
	}
}
