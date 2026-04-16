package controller

import (
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// newRunnerWithArchiverSnapshots produces a TopologyManagerRunner
// whose managers map has an archiverPoller pre-seeded with the given
// per-site snapshots. Nothing else is wired — tests using this helper
// only exercise populatePITRStatus.
func newRunnerWithArchiverSnapshots(t *testing.T, nn types.NamespacedName, snaps map[string]*internalmysql.ArchiverStatus) *TopologyManagerRunner {
	t.Helper()
	r := &TopologyManagerRunner{
		logger:   slog.Default(),
		managers: make(map[types.NamespacedName]*managedTopology),
	}
	poller := &archiverPoller{
		nn:        nameKey{Namespace: nn.Namespace, Name: nn.Name},
		logger:    slog.Default(),
		snapshots: map[string]*internalmysql.ArchiverStatus{},
	}
	for k, v := range snaps {
		poller.snapshots[k] = v
	}
	r.managers[nn] = &managedTopology{archiver: poller}
	return r
}

func pitrEnabledFG(ns, name string) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{
				PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly"},
			},
		},
	}
}

func TestPopulatePITRStatus_DisabledClearsField(t *testing.T) {
	nn := types.NamespacedName{Namespace: "default", Name: "orders"}
	r := newRunnerWithArchiverSnapshots(t, nn, nil)

	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: nn.Namespace, Name: nn.Name},
	}
	fg.Status.PITR = &v1alpha1.PITRStatus{Enabled: true, ArchivedFileCount: 10}

	r.populatePITRStatus(nn, fg)

	if fg.Status.PITR != nil {
		t.Errorf("PITR should be cleared when disabled, got %+v", fg.Status.PITR)
	}
}

func TestPopulatePITRStatus_NoSnapshotsPreservesExisting(t *testing.T) {
	nn := types.NamespacedName{Namespace: "default", Name: "orders"}
	r := newRunnerWithArchiverSnapshots(t, nn, map[string]*internalmysql.ArchiverStatus{
		"dc1": nil,
		"dc2": nil,
	})

	fg := pitrEnabledFG(nn.Namespace, nn.Name)
	existing := &v1alpha1.PITRStatus{Enabled: true, ArchivedFileCount: 5}
	fg.Status.PITR = existing

	r.populatePITRStatus(nn, fg)

	// No sidecar responded; we keep the previous status instead of
	// clobbering it with an empty object that would flap in /status.
	if fg.Status.PITR != existing {
		t.Errorf("PITR pointer changed; no snapshots should leave it alone. got=%+v", fg.Status.PITR)
	}
}

func TestPopulatePITRStatus_AggregatesAcrossSites(t *testing.T) {
	nn := types.NamespacedName{Namespace: "default", Name: "orders"}
	t0 := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	snaps := map[string]*internalmysql.ArchiverStatus{
		"dc1": {
			Enabled:            true,
			Site:               "dc1",
			ManifestFileCount:  5,
			ManifestBytes:      500,
			OldestArchivedTime: t0,
			NewestArchivedTime: t0.Add(10 * time.Minute),
			LastUploadAt:       t0.Add(11 * time.Minute),
		},
		"dc2": {
			Enabled:            true,
			Site:               "dc2",
			ManifestFileCount:  3,
			ManifestBytes:      1200,
			OldestArchivedTime: t0.Add(-30 * time.Minute), // earlier than dc1
			NewestArchivedTime: t0.Add(5 * time.Minute),   // later than dc1's oldest but older than dc1's newest
			LastUploadAt:       t0.Add(2 * time.Minute),
			LastError:          "slow upload",
		},
	}
	r := newRunnerWithArchiverSnapshots(t, nn, snaps)

	fg := pitrEnabledFG(nn.Namespace, nn.Name)
	r.populatePITRStatus(nn, fg)

	pitr := fg.Status.PITR
	if pitr == nil {
		t.Fatal("expected status.pitr to be populated")
	}
	if !pitr.Enabled {
		t.Error("expected Enabled=true")
	}
	if pitr.ProfileName != "nightly" {
		t.Errorf("ProfileName = %q, want nightly", pitr.ProfileName)
	}
	if pitr.ArchivedFileCount != 5 {
		t.Errorf("ArchivedFileCount = %d, want 5 (max across sites)", pitr.ArchivedFileCount)
	}
	if pitr.ArchivedBytes != 1200 {
		t.Errorf("ArchivedBytes = %d, want 1200 (max across sites)", pitr.ArchivedBytes)
	}
	if pitr.OldestArchivedTime == nil || !pitr.OldestArchivedTime.Time.Equal(t0.Add(-30*time.Minute)) {
		t.Errorf("OldestArchivedTime = %v, want %v", pitr.OldestArchivedTime, t0.Add(-30*time.Minute))
	}
	if pitr.NewestArchivedTime == nil || !pitr.NewestArchivedTime.Time.Equal(t0.Add(10*time.Minute)) {
		t.Errorf("NewestArchivedTime = %v, want %v", pitr.NewestArchivedTime, t0.Add(10*time.Minute))
	}
	if pitr.LastArchivedTime == nil || !pitr.LastArchivedTime.Time.Equal(t0.Add(11*time.Minute)) {
		t.Errorf("LastArchivedTime = %v, want %v", pitr.LastArchivedTime, t0.Add(11*time.Minute))
	}
	// Message carries the most recent LastError. Either site's error is
	// acceptable — just make sure we don't drop the signal entirely.
	if pitr.Message == "" {
		t.Error("expected Message to carry a lastError")
	}
}

func TestPopulatePITRStatus_EmptySnapshotsLeaveStale(t *testing.T) {
	nn := types.NamespacedName{Namespace: "default", Name: "orders"}
	r := newRunnerWithArchiverSnapshots(t, nn, map[string]*internalmysql.ArchiverStatus{})

	fg := pitrEnabledFG(nn.Namespace, nn.Name)
	existing := &v1alpha1.PITRStatus{Enabled: true, ArchivedFileCount: 1}
	fg.Status.PITR = existing

	r.populatePITRStatus(nn, fg)
	if fg.Status.PITR != existing {
		t.Errorf("expected stale PITR preserved when poller has no snapshots; got %+v", fg.Status.PITR)
	}
}
