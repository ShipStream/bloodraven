package controller

import (
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// What the own-loss Events say for a report that rests on a witness, why
// a report ended, and when the automatic reclone keeps failing.
func TestEmitOwnLossEvents_WitnessCauseAndFailures(t *testing.T) {
	fg := newTestFG()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	rec := record.NewFakeRecorder(40)
	r := &TopologyManagerRunner{recorder: rec, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{nn: {}}}
	emit := func(stored string, site SiteSnapshot) string {
		t.Helper()
		site.Name = "dc2"
		r.emitOwnLossEvents(fg, &v1alpha1.MysqlFailoverGroupStatus{Sites: []v1alpha1.SiteStatus{{Name: "dc2", LostGtid: stored}}},
			TopologySnapshot{Sites: []SiteSnapshot{site}})
		return strings.Join(drainEvents(rec), "\n")
	}
	report := SiteSnapshot{State: state.StateReadOnly, RecoveryState: v1alpha1.RecoveryStateRecloneRequired,
		LostGtid: lossLostGTID, LostTxnCount: 10, LostComparedTo: "dc3", LostToWitness: true,
		RecloneWaiting: "the transactions are held by dc3 only"}

	// A witness report: what it means and what ends it.
	got := emit("", report)
	for _, want := range []string{"Warning OwnTransactionsLost dc2 lacks 10 transactions it issued itself that dc3 holds",
		"the primary does not hold them either", "keeps replicating and is not recloned automatically", "not promoted while dc3 holds them",
		"Reclone dc3 to discard them"} {
		if !strings.Contains(got, want) {
			t.Errorf("events = %q, want %q", got, want)
		}
	}

	// The stored status has no lostGtid (an older CRD pruned it): the
	// report is not announced again.
	if got := emit("", report); got != "" {
		t.Errorf("events on the next write = %q, want none: the same report was announced", got)
	}

	// It ends, with its cause.
	if got := emit("", SiteSnapshot{State: state.StateReadOnly, LostCleared: "dc3 no longer holds transactions it issued and lacks"}); !strings.Contains(got,
		"Normal OwnTransactionsLostResolved dc2 is no longer reported as lacking transactions it issued itself: dc3 no longer holds transactions it issued and lacks") {
		t.Errorf("events = %q, want the report resolved with its cause", got)
	}
	if got := emit("", SiteSnapshot{State: state.StateReadOnly}); got != "" {
		t.Errorf("events after the report ended = %q, want none", got)
	}

	// A report against the primary whose automatic reclone keeps failing:
	// an Event from the third failure on, once per failure.
	hole := SiteSnapshot{State: state.StateReadOnly, RecoveryState: v1alpha1.RecoveryStateRecloneRequired,
		LostGtid: lossLostGTID, LostTxnCount: 10, LostComparedTo: "dc1",
		RecloneWaiting: "the last 3 automatic reclones did not complete; the next one is requested at 2026-11-11T10:00:00Z"}
	if got := emit("", hole); !strings.Contains(got, "Warning OwnTransactionsLost ") || strings.Contains(got, "RecloneFailing") {
		t.Errorf("events = %q, want the report and no failing Event yet", got)
	}
	for failures, want := range map[int]bool{1: false, 2: false, 3: true} {
		hole.RecloneFailures = failures
		got := emit("", hole)
		if has := strings.Contains(got, "Warning OwnTransactionsLostRecloneFailing the automatic reclone of dc2"); has != want {
			t.Errorf("%d failures: events = %q, failing Event wanted=%v", failures, got, want)
		}
	}
	hole.RecloneFailures = 3
	if got := emit("", hole); got != "" {
		t.Errorf("events for an unchanged failure count = %q, want none", got)
	}
	hole.RecloneFailures = 4
	if got := emit("", hole); !strings.Contains(got, "has failed 4 times in a row") {
		t.Errorf("events = %q, want the fourth failure announced", got)
	}
}

// Which values a write for an older CRD replaces, and which API errors
// lead to it.
func TestDegradeStatusForOlderCRD(t *testing.T) {
	st := &v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "dc1", Sites: []v1alpha1.SiteStatus{
		{Name: "dc1", State: v1alpha1.SiteStateWritable},
		{Name: "dc2", RecoveryState: v1alpha1.RecoveryStateRecloneRequired, SourceConvergenceReason: v1alpha1.SourceReasonOwnTransactionsLost, LostGtid: lossLostGTID},
		{Name: "dc3", RecoveryState: v1alpha1.RecoveryStateInProgress, SourceConvergenceReason: v1alpha1.SourceReasonGTIDDiverged},
	}}
	if !degradeStatusForOlderCRD(st) {
		t.Fatal("nothing was degraded")
	}
	if s := st.Sites[1]; s.RecoveryState != v1alpha1.RecoveryStateBlocked || s.SourceConvergenceReason != v1alpha1.SourceReasonGTIDDiverged || s.LostGtid != lossLostGTID {
		t.Fatalf("dc2 = %+v, want RecoveryBlocked, GTIDDiverged and the lost set kept", s)
	}
	if s := st.Sites[2]; s.RecoveryState != v1alpha1.RecoveryStateInProgress || st.ActiveSite != "dc1" {
		t.Fatalf("values every CRD knows were changed: %+v", st)
	}
	if degradeStatusForOlderCRD(st) {
		t.Fatal("a second pass degraded again")
	}

	gk := schema.GroupKind{Group: v1alpha1.GroupVersion.Group, Kind: "MysqlFailoverGroup"}
	unsupported := apierrors.NewInvalid(gk, "lion", field.ErrorList{
		field.NotSupported(field.NewPath("status", "sites").Index(1).Child("recoveryState"), "RecloneRequired", []string{"RecoveryInProgress", "RecoveryBlocked"})})
	other := apierrors.NewInvalid(gk, "lion", field.ErrorList{field.Required(field.NewPath("status", "sites").Index(1).Child("name"), "")})
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"an unsupported enum value", unsupported, true},
		{"another validation error", other, false},
		{"a conflict", apierrors.NewConflict(schema.GroupResource{Resource: "mysqlfailovergroups"}, "lion", errors.New("changed")), false},
		{"a plain error", errors.New("Unsupported value"), false},
		{"an Invalid without causes that says so", &apierrors.StatusError{ErrStatus: metav1.Status{Reason: metav1.StatusReasonInvalid, Code: 422,
			Message: `status.sites[1].recoveryState: Unsupported value: "RecloneRequired"`}}, true},
	} {
		if got := statusRefusedForUnsupportedValue(tc.err); got != tc.want {
			t.Errorf("%s: statusRefusedForUnsupportedValue = %v, want %v", tc.name, got, tc.want)
		}
	}
}
