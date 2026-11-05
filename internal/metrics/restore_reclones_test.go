package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetRestoreReclonePending(t *testing.T) {
	reg := prometheus.NewRegistry()
	Register(reg)
	t.Cleanup(func() { SetRestoreReclonePending("ns", "g", "", nil) })

	SetRestoreReclonePending("ns", "g", "Armed", []string{"pdx", "fra"})
	if got := testutil.CollectAndCount(RestoreReclonePending, "bloodraven_restore_reclone_pending"); got != 2 {
		t.Fatalf("series after arming = %d, want 2", got)
	}
	if v := testutil.ToFloat64(RestoreReclonePending.WithLabelValues("ns", "g", "pdx", "Armed")); v != 1 {
		t.Fatalf("pdx Armed = %v, want 1", v)
	}

	// A phase change replaces the series; a site that left is deleted.
	SetRestoreReclonePending("ns", "g", "Held", []string{"pdx"})
	if got := testutil.CollectAndCount(RestoreReclonePending, "bloodraven_restore_reclone_pending"); got != 1 {
		t.Fatalf("series after hold = %d, want 1", got)
	}
	if v := testutil.ToFloat64(RestoreReclonePending.WithLabelValues("ns", "g", "pdx", "Held")); v != 1 {
		t.Fatalf("pdx Held = %v, want 1", v)
	}

	// Another group's series are untouched by this group's update.
	SetRestoreReclonePending("ns", "other", "Draining", []string{"iad"})
	t.Cleanup(func() { SetRestoreReclonePending("ns", "other", "", nil) })
	SetRestoreReclonePending("ns", "g", "", nil)
	if got := testutil.CollectAndCount(RestoreReclonePending, "bloodraven_restore_reclone_pending"); got != 1 {
		t.Fatalf("series after clearing g = %d, want only the other group's", got)
	}
}
