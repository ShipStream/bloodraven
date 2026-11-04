package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// super_read_only=ON also sets read_only=ON, so a rollback that clears only
// super_read_only leaves the source read-only and the group without a
// writable primary (#210). These cases start from a writable source and let
// the production fence produce the state the rollback has to undo.

const unfenceTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func TestPlannedFailoverLagTimeout_LeavesSourceWritable(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDraining,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
		DrainTimeout:  &metav1.Duration{Duration: 30 * time.Second},
		MaxLagWait:    &metav1.Duration{Duration: time.Nanosecond},
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: false, gtidExecuted: unfenceTestUUID + ":1-10"}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: unfenceTestUUID + ":1-5"}
	r, _ := newReconciler(fg)
	r.Runner = plannedDrainRunner(nn, iad, pdx)

	for pass := 1; pass <= 3; pass++ {
		if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if pass == 1 && (!iad.superReadOnly || !iad.readOnly) {
			t.Fatalf("pass 1: source not fenced: super_read_only=%v read_only=%v", iad.superReadOnly, iad.readOnly)
		}
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseFailed || pf.Reason != "LagTimeout" {
		t.Fatalf("phase=%q reason=%q (%s), want Failed/LagTimeout", pf.Phase, pf.Reason, pf.Message)
	}
	if iad.superReadOnly || iad.readOnly {
		t.Errorf("after rollback iad super_read_only=%v read_only=%v; want the source writable", iad.superReadOnly, iad.readOnly)
	}
	if !pdx.readOnly {
		t.Error("lagging target was promoted")
	}
}

func TestFenceSite_GTIDReadFailureLeavesSourceWritable(t *testing.T) {
	fg := plannedFailoverFG("")
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: false, gtidExecutedErr: errors.New("connection reset")}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true}
	tm, err := plannedDrainRunner(nn, iad, pdx).plannedFailoverManager(nn)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tm.FenceSite(context.Background(), "iad"); err == nil {
		t.Fatal("FenceSite succeeded although the GTID read failed")
	}
	if iad.superReadOnly || iad.readOnly {
		t.Errorf("after fence rollback iad super_read_only=%v read_only=%v; want the source writable", iad.superReadOnly, iad.readOnly)
	}
}

// A fence write that reports an error may still have landed, so FenceSite
// unfences on that path too, and says so when the unfence also failed.
func TestFenceSite_FenceWriteErrorUnfencesSource(t *testing.T) {
	fg := plannedFailoverFG("")
	nn := fgNN(fg)
	// The fence landed but the reply was lost: the site is fenced and every
	// further super_read_only write errors.
	iad := &mockMySQL{readOnly: true, superReadOnly: true, superReadOnlyErr: errors.New("i/o timeout")}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true}
	tm, err := plannedDrainRunner(nn, iad, pdx).plannedFailoverManager(nn)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tm.FenceSite(context.Background(), "iad")
	if err == nil {
		t.Fatal("FenceSite succeeded although the fence write failed")
	}
	if !strings.Contains(err.Error(), "source primary unfence failed") {
		t.Errorf("error %q does not report the failed super_read_only=OFF", err)
	}
	if iad.superReadOnly || iad.readOnly {
		t.Errorf("after fence rollback iad super_read_only=%v read_only=%v; want read_only=OFF to have cleared both", iad.superReadOnly, iad.readOnly)
	}
}

// read_only=OFF also clears super_read_only, so it is still worth issuing
// when clearing super_read_only failed.
func TestUnfenceSite_ClearsReadOnlyWhenSuperReadOnlyClearFails(t *testing.T) {
	fg := plannedFailoverFG("")
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: true, superReadOnly: true, superReadOnlyErr: errors.New("lock wait timeout")}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true}
	tm, err := plannedDrainRunner(nn, iad, pdx).plannedFailoverManager(nn)
	if err != nil {
		t.Fatal(err)
	}

	if err := tm.UnfenceSite(context.Background(), "iad"); err == nil {
		t.Error("UnfenceSite hid the super_read_only failure")
	}
	if iad.superReadOnly || iad.readOnly {
		t.Errorf("after unfence iad super_read_only=%v read_only=%v; want read_only=OFF to have cleared both", iad.superReadOnly, iad.readOnly)
	}
}
