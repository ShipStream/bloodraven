package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// super_read_only=ON also sets read_only=ON, so a rollback that clears only
// super_read_only leaves the source read-only and the group without a
// writable primary (#210). These cases start from a writable source and let
// the topology manager's fence produce the state the unfence has to undo.

const unfenceTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

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
