package controller

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// TestDeploymentUnstableReason_DragonflyPhases pins the phase-to-reason
// table that replaced "Dragonfly" + phase: every DragonflyPhase except
// Disabled and Ready gates with "Dragonfly<phase>", and the two that do not
// gate, and an absent phase, leave the group stable.
func TestDeploymentUnstableReason_DragonflyPhases(t *testing.T) {
	for _, tc := range []struct {
		phase v1alpha1.DragonflyPhase
		want  string
	}{
		{"", ""},
		{v1alpha1.DragonflyPhaseDisabled, ""},
		{v1alpha1.DragonflyPhaseReady, ""},
		{v1alpha1.DragonflyPhaseReconciling, "DragonflyReconciling"},
		{v1alpha1.DragonflyPhaseConfiguringReplication, "DragonflyConfiguringReplication"},
		{v1alpha1.DragonflyPhaseDegraded, "DragonflyDegraded"},
		{v1alpha1.DragonflyPhasePromoting, "DragonflyPromoting"},
	} {
		fg := stableDeploymentGroup()
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Phase: tc.phase}
		if got := DeploymentUnstableReason(fg); got != tc.want {
			t.Errorf("phase %q: unstableReason %q, want %q", tc.phase, got, tc.want)
		}
	}
}

// TestDeploymentUnstableReason_SourceConvergence pins the
// source-convergence table: Pending and Blocked gate with
// "SourceConvergence<state>", Converged and empty do not.
func TestDeploymentUnstableReason_SourceConvergence(t *testing.T) {
	for _, tc := range []struct {
		state v1alpha1.SourceConvergenceState
		want  string
	}{
		{"", ""},
		{v1alpha1.SourceConvergenceConverged, ""},
		{v1alpha1.SourceConvergencePending, "SourceConvergencePending"},
		{v1alpha1.SourceConvergenceBlocked, "SourceConvergenceBlocked"},
	} {
		fg := stableDeploymentGroup()
		fg.Status.Sites[1].SourceConvergenceState = tc.state
		if got := DeploymentUnstableReason(fg); got != tc.want {
			t.Errorf("state %q: unstableReason %q, want %q", tc.state, got, tc.want)
		}
	}
}

// stableDeploymentGroup is a two-site group the deployment API reports
// stable.
func stableDeploymentGroup() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}}},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Conditions: []metav1.Condition{{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonHealthy}},
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateWritable},
				{Name: "pdx", State: v1alpha1.SiteStateReadOnly, Replicating: true},
			},
		},
	}
}

// TestNewStandbyArchiveRefusal_Reasons pins the SourceConfigKnown reason of
// a refused source archive: a mapped refusal token without its "PITR"
// prefix, whether it arrives as a VerifyError or as the first word of an
// error message, and ArchiveUnreadable for anything else.
func TestNewStandbyArchiveRefusal_Reasons(t *testing.T) {
	for tok, reason := range standbyArchiveRefusalReasons {
		if want := strings.TrimPrefix(string(tok), "PITR"); reason != want {
			t.Errorf("%s maps to %q, want %q", tok, reason, want)
		}
		if got := newStandbyArchiveRefusal(&pitrarchive.VerifyError{Token: tok, Detail: "x"}).reason; got != reason {
			t.Errorf("VerifyError %s: reason %q, want %q", tok, got, reason)
		}
		if got := newStandbyArchiveRefusal(errors.New(string(tok) + ": manifest m: bad")).reason; got != reason {
			t.Errorf("message %s: reason %q, want %q", tok, got, reason)
		}
	}
	for _, err := range []error{
		errors.New("no token here"),
		errors.New(string(pitrarchive.TokenStopAmbiguous) + ": not an archive refusal"),
	} {
		if got := newStandbyArchiveRefusal(err).reason; got != v1alpha1.ReasonArchiveUnreadable {
			t.Errorf("%v: reason %q, want ArchiveUnreadable", err, got)
		}
	}
}
