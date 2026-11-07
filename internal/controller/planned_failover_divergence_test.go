package controller

import (
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// A target with recorded divergence is refused before anything is fenced,
// like a mid-rotation target, with a message naming the set.
func TestValidatePlannedFailover_RefusesDivergentTarget(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	count := int64(221)
	fg.Status.Sites[1].RecoveryState = v1alpha1.RecoveryStateBlocked
	fg.Status.Sites[1].DivergentGtid = issue202ErrantGTID
	fg.Status.Sites[1].DivergentTransactionCount = &count

	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)

	if result != PlannedFailoverReject || reason != "DivergentTarget" {
		t.Fatalf("validate = %v/%q (%v), want Reject/DivergentTarget", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "221") || !strings.Contains(err.Error(), issue202ErrantGTID) {
		t.Errorf("error = %v, want it to name 221 transactions and the set", err)
	}
}
