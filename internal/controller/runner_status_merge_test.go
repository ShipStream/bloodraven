package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// legacyMergeTopologySnapshotStatus is updateCRStatus's topology merge as it
// was before MergeTopologySnapshotStatus was extracted, copied verbatim
// (freshFG.Status is st; the early return is false). It is the golden
// reference for TestMergeTopologySnapshotStatus_EqualsLegacy.
func legacyMergeTopologySnapshotStatus(st *v1alpha1.MysqlFailoverGroupStatus, snap TopologySnapshot) bool {
	if snap.TopologyGeneration < st.TopologyGeneration {
		return false
	}

	st.ActiveSite = snap.ActiveSite
	if snap.TopologyGeneration > st.TopologyGeneration && snap.DeploymentActiveSite != "" {
		st.ActiveSite = snap.DeploymentActiveSite
	}
	st.TopologyGeneration = snap.TopologyGeneration
	if len(st.Sites) != len(snap.Sites) {
		st.Sites = make([]v1alpha1.SiteStatus, len(snap.Sites))
	}
	for i, s := range snap.Sites {
		st.Sites[i] = siteStatusFromSnapshot(s, snap.MaxLagSeconds)
	}

	if !snap.LastFailover.IsZero() {
		t := metav1.NewTime(snap.LastFailover)
		st.SetLastFailover(&t, st.LastFailoverTarget(), st.PromotionGtidExecuted())
	}
	st.SetLastFailover(st.LastFailoverTime(), snap.LastFailoverTarget, st.PromotionGtidExecuted())
	st.SetLastFailover(st.LastFailoverTime(), st.LastFailoverTarget(), snap.PromotionGtidExecuted)

	for i, s := range snap.Sites {
		st.Sites[i].RecoveryState = s.RecoveryState
		st.Sites[i].DivergentGtid = s.DivergentGtid
		if s.RecoveryState != "" && s.DivergentTxnCount > 0 {
			c := s.DivergentTxnCount
			st.Sites[i].DivergentTransactionCount = &c
		} else {
			st.Sites[i].DivergentTransactionCount = nil
		}
		st.Sites[i].DivergenceDetectedBy = ""
		st.Sites[i].DivergenceComparedTo = ""
		if s.RecoveryState == v1alpha1.RecoveryStateBlocked {
			st.Sites[i].DivergenceDetectedBy = s.DivergenceDetectedBy
			st.Sites[i].DivergenceComparedTo = s.DivergenceComparedTo
		}
	}

	// Assigned further down in updateCRStatus, after the conditions, which
	// do not read it.
	st.UpdatePhase = snap.UpdatePhase
	return true
}

// monotonicFailoverTriple applies the one intended change to the legacy
// merge (OPEN-2): the failover triple never moves to a strictly older
// lastFailover (second precision), and an equal one changes only the
// target.
func monotonicFailoverTriple(merged, stored *v1alpha1.MysqlFailoverGroupStatus, snap TopologySnapshot) {
	snapAt := snap.LastFailover.UTC().Truncate(time.Second)
	var storedAt time.Time
	if t := stored.LastFailoverTime(); t != nil {
		storedAt = t.UTC().Truncate(time.Second)
	}
	switch {
	case snapAt.Before(storedAt):
		merged.LastFailover = stored.LastFailover.DeepCopy()
	case snapAt.Equal(storedAt):
		merged.SetLastFailover(stored.LastFailoverTime().DeepCopy(), merged.LastFailoverTarget(), stored.PromotionGtidExecuted())
	}
}

// MergeTopologySnapshotStatus is the legacy inline merge plus the
// monotonic failover triple: for every pairing of stored status and
// snapshot it produces exactly what updateCRStatus's inline merge did with
// that one change, and leaves every other field alone.
func TestMergeTopologySnapshotStatus_EqualsLegacy(t *testing.T) {
	t0 := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	earlier := metav1.NewTime(t0.Add(-time.Hour))
	lag := int64(3)
	stale := int64(9)

	statuses := map[string]v1alpha1.MysqlFailoverGroupStatus{
		"empty": {},
		"populated": {
			ActiveSite:         "iad",
			TopologyGeneration: 4,
			Sites:              []v1alpha1.SiteStatus{{Name: "iad", State: "Writable", DivergentTransactionCount: &stale, DivergenceDetectedBy: "audit"}, {Name: "pdx", State: "ReadOnly"}},
			LastFailover:       &v1alpha1.FailoverRecord{FailoverRecordRef: v1alpha1.FailoverRecordRef{Time: &earlier, Target: "iad"}, PromotionGtidExecuted: "uuid1:1-10"},
			UpdatePhase:        "UpdatingReplicas",
			Conditions:         []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "TopologyPolled"}},
			PlannedFailover:    &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: v1alpha1.PlannedFailoverPhaseAdmitting, TargetSite: "pdx"},
			RestoreReclones:    &v1alpha1.RestoreReclonesStatus{Pending: []string{"pdx"}},
			Dragonfly:          &v1alpha1.DragonflyStatus{},
		},
		"three sites": {
			TopologyGeneration: 7,
			Sites:              []v1alpha1.SiteStatus{{Name: "iad"}, {Name: "pdx"}, {Name: "dfw"}},
		},
	}
	sites := []SiteSnapshot{
		{Name: "iad", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable, LastSeen: t0, ServingHealthy: true},
		{
			Name: "pdx", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, LastSeen: t0,
			Replication:        &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: &lag, ExecutedGtidSet: "uuid1:1-20"},
			ReplicationHealthy: true, SourceHost: "iad", SourceConvergenceState: sourceConvergenceConverged,
			RecoveryState: v1alpha1.RecoveryStateBlocked, DivergentGtid: "uuid2:1-3", DivergentTxnCount: 3,
			DivergenceDetectedBy: divergenceDetectedByAudit, DivergenceComparedTo: "iad",
		},
	}
	snapshots := map[string]TopologySnapshot{
		"zero": {},
		"same generation": {
			TopologyGeneration: 4, ActiveSite: "iad", Sites: sites,
			UpdatePhase: "UpdatingPrimary",
		},
		"newer generation with deployment site": {
			TopologyGeneration: 5, ActiveSite: "iad", DeploymentActiveSite: "pdx", Sites: sites,
			LastFailover: t0.Add(123456789 * time.Nanosecond), LastFailoverTarget: "pdx", PromotionGtidExecuted: "uuid1:1-20",
		},
		"newer generation without deployment site": {
			TopologyGeneration: 8, ActiveSite: "pdx", Sites: sites[1:],
		},
		"older generation": {
			TopologyGeneration: 3, ActiveSite: "pdx", Sites: sites,
			LastFailover: t0, LastFailoverTarget: "pdx",
		},
		"recovery in progress without count": {
			TopologyGeneration: 9, Sites: []SiteSnapshot{{Name: "iad", RecoveryState: v1alpha1.RecoveryStateInProgress, DivergentGtid: "uuid2:1", DivergenceDetectedBy: "x", DivergenceComparedTo: "y"}},
		},
	}

	for sn, base := range statuses {
		for nn, snap := range snapshots {
			got, want := base.DeepCopy(), base.DeepCopy()
			gotOK := MergeTopologySnapshotStatus(got, snap)
			wantOK := legacyMergeTopologySnapshotStatus(want, snap)
			if wantOK {
				monotonicFailoverTriple(want, &base, snap)
			}
			if gotOK != wantOK {
				t.Errorf("%s / %s: merged = %v, legacy = %v", sn, nn, gotOK, wantOK)
			}
			if !equality.Semantic.DeepEqual(got, want) {
				t.Errorf("%s / %s: status differs from the legacy merge\n got %+v\nwant %+v", sn, nn, *got, *want)
			}
			if !gotOK && !equality.Semantic.DeepEqual(got, base.DeepCopy()) {
				t.Errorf("%s / %s: a refused merge changed the status", sn, nn)
			}
		}
	}
}
