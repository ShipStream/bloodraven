package controller

import (
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// The Events of a report's life, from the status the runner had and the
// snapshot it writes.
func TestEmitFailoverEvents_OwnTransactionsLost(t *testing.T) {
	site := func(st v1alpha1.SiteRecoveryState, lost string) v1alpha1.SiteStatus {
		return v1alpha1.SiteStatus{Name: "dc2", RecoveryState: st, LostGtid: lost}
	}
	snap := func(st v1alpha1.SiteRecoveryState, lost string, siteState state.SiteState) TopologySnapshot {
		return TopologySnapshot{Sites: []SiteSnapshot{{Name: "dc2", State: siteState, RecoveryState: st,
			LostGtid: lost, LostTxnCount: 10, LostComparedTo: "dc1"}}}
	}
	tests := []struct {
		name    string
		old     v1alpha1.SiteStatus
		snap    TopologySnapshot
		want    []string
		wantNot []string
	}{
		{"a new report", site("", ""), snap(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID, state.StateReadOnly),
			[]string{"Warning OwnTransactionsLost dc2 lacks 10 transactions it issued itself that dc1 holds", "reclone lion dc2 --cold"}, nil},
		{"an unchanged report is not announced again", site(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID),
			snap(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID, state.StateReadOnly), nil, []string{"OwnTransactionsLost"}},
		{"a changed set is", site(v1alpha1.RecoveryStateRecloneRequired, lossUUIDOld+":95-100"),
			snap(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID, state.StateReadOnly), []string{"Warning OwnTransactionsLost "}, nil},
		{"beside a divergence the remedy is the plain reclone", site("", ""), snap(v1alpha1.RecoveryStateBlocked, lossLostGTID, state.StateReadOnly),
			[]string{"not recloned automatically", "reclone lion dc2)"}, []string{"--cold"}},
		{"recloned", site(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID), snap("", "", state.StateReadOnly),
			[]string{"Normal OwnTransactionsLostResolved"}, []string{"OldPrimaryRecovered", "SitePromoted"}},
		{"made the primary by hand", site(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID), snap("", "", state.StateWritable),
			[]string{"Warning OwnTransactionsLostSitePromoted dc2"}, []string{"Resolved"}},
		// A rejoin under way that turns out to be a site with a hole is
		// not a completed recovery.
		{"a rejoin that became a reclone announces no recovery", site(v1alpha1.RecoveryStateInProgress, ""),
			snap(v1alpha1.RecoveryStateRecloneRequired, lossLostGTID, state.StateReadOnly),
			[]string{"Warning OwnTransactionsLost "}, []string{"OldPrimaryRecovered"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := record.NewFakeRecorder(20)
			r := &TopologyManagerRunner{recorder: rec, logger: testLogger()}
			r.emitFailoverEvents(newTestFG(), &v1alpha1.MysqlFailoverGroupStatus{Sites: []v1alpha1.SiteStatus{tt.old}}, tt.snap)
			got := strings.Join(drainEvents(rec), "\n")
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("events = %q, want %q", got, w)
				}
			}
			for _, w := range tt.wantNot {
				if strings.Contains(got, w) {
					t.Errorf("events = %q, must not contain %q", got, w)
				}
			}
		})
	}
}
