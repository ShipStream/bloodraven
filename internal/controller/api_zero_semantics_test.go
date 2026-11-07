package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// A-37: only an absent timer takes its default; a present value (bounded at
// admission) is used as given, and no zero is rewritten.

func TestTopologyTimerDefaultsApplyToNilOnly(t *testing.T) {
	fg := newTestFG()
	fg.Spec.FailoverCooldown, fg.Spec.ConnectionDrainTimeout = nil, nil
	cfg, err := CRConfigToTopologyConfig(fg)
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(cfg.FailoverCooldown) != 5*time.Minute || time.Duration(cfg.ConnectionDrainTimeout) != 30*time.Second {
		t.Fatalf("unset timers: cooldown %v drain %v, want 5m and 30s", time.Duration(cfg.FailoverCooldown), time.Duration(cfg.ConnectionDrainTimeout))
	}
	fg.Spec.FailoverCooldown = &metav1.Duration{Duration: 30 * time.Second}
	if cfg, _ = CRConfigToTopologyConfig(fg); time.Duration(cfg.FailoverCooldown) != 30*time.Second {
		t.Fatalf("cooldown %v, want 30s", time.Duration(cfg.FailoverCooldown))
	}
	if got := failoverCooldownFromSpec(fg); got != 30*time.Second {
		t.Fatalf("failoverCooldownFromSpec = %v, want 30s", got)
	}

	// The topology manager no longer maps a zero cooldown to 5m: the config
	// is used as given (only CRConfigToTopologyConfig applies the default).
	tm := NewTopologyManager(TopologyConfig{Name: "fg", FailoverCooldown: int64(45 * time.Second)}, nil, nil, nil, nil, BootstrapConfig{}, nil, nil, nil, nil)
	if tm.failoverCooldown != 45*time.Second {
		t.Fatalf("manager cooldown %v, want 45s", tm.failoverCooldown)
	}
}

func TestPlannedTimersFallThroughOnlyWhenAbsent(t *testing.T) {
	fg := newTestFG()
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{}
	if effectiveMaxLagWait(fg, PlannedFailoverRequest{}) != defaultPlannedFailoverMaxLagWait || effectiveDrainTimeout(fg) != defaultPlannedFailoverDrainTimeout {
		t.Fatal("unset planned timers do not take the defaults")
	}
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{MaxLagWait: &metav1.Duration{Duration: time.Second}, DrainTimeout: &metav1.Duration{Duration: 2 * time.Second}}
	if effectiveMaxLagWait(fg, PlannedFailoverRequest{}) != time.Second || effectiveDrainTimeout(fg) != 2*time.Second {
		t.Fatal("set planned timers are not used as given")
	}
	if effectiveMaxLagWait(fg, PlannedFailoverRequest{MaxLagWait: time.Hour}) != time.Hour {
		t.Fatal("the annotation override does not win")
	}
	// A zero is never rewritten to the default (admission refuses it).
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{MaxLagWait: &metav1.Duration{}}
	if got := effectiveMaxLagWait(fg, PlannedFailoverRequest{}); got != 0 {
		t.Fatalf("a zero maxLagWait became %v", got)
	}
	fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, PlannedFailover: &v1alpha1.DragonflyPlannedFailoverSpec{MaxSyncWait: &metav1.Duration{Duration: time.Second}}}
	if got := effectiveDragonflyMaxSyncWait(fg); got != time.Second {
		t.Fatalf("maxSyncWait = %v, want 1s", got)
	}
}

func TestBackupRetryBackoffBounds(t *testing.T) {
	initial, maxBackoff := backupRetryBackoffBounds(&v1alpha1.BackupRetrySpec{})
	if initial != time.Minute || maxBackoff != 30*time.Minute {
		t.Fatalf("defaults %v/%v, want 1m/30m", initial, maxBackoff)
	}
	initial, maxBackoff = backupRetryBackoffBounds(&v1alpha1.BackupRetrySpec{InitialBackoff: &metav1.Duration{Duration: 1500 * time.Millisecond}, MaxBackoff: &metav1.Duration{Duration: 4 * time.Second}})
	if initial != 1500*time.Millisecond || maxBackoff != 4*time.Second {
		t.Fatalf("set bounds %v/%v", initial, maxBackoff)
	}
}

func TestWholeSecondsCeilAndCloneTimeout(t *testing.T) {
	for d, want := range map[time.Duration]int64{time.Millisecond: 1, time.Second: 1, 1500 * time.Millisecond: 2, time.Minute: 60, 24 * time.Hour: 86400} {
		if got := wholeSecondsCeil(d); got != want {
			t.Errorf("wholeSecondsCeil(%v) = %d, want %d", d, got, want)
		}
	}
	fg := newTestFG()
	fg.Spec.CloneTimeout = nil
	if cloneTimeoutFromSpec(fg) != 0 {
		t.Fatal("unset cloneTimeout must leave the bootstrap default (1h)")
	}
	fg.Spec.CloneTimeout = &metav1.Duration{Duration: 90 * time.Second}
	if cloneTimeoutFromSpec(fg) != 90*time.Second {
		t.Fatal("cloneTimeout not used as given")
	}
}
