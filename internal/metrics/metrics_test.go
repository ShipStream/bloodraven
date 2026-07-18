package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterIncludesRestoreMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	Register(reg)
	RestoreDurationSeconds.WithLabelValues("ns", "group", "init_from_backup", "iad").Observe(1)
	RestoreLastSuccessTimestamp.WithLabelValues("ns", "group", "init_from_backup", "iad").Set(1)
	RestoreLastSourceSizeBytes.WithLabelValues("ns", "group", "init_from_backup", "iad").Set(1)
	defer RestoreDurationSeconds.DeleteLabelValues("ns", "group", "init_from_backup", "iad")
	defer RestoreLastSuccessTimestamp.DeleteLabelValues("ns", "group", "init_from_backup", "iad")
	defer RestoreLastSourceSizeBytes.DeleteLabelValues("ns", "group", "init_from_backup", "iad")
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"bloodraven_restore_duration_seconds":               false,
		"bloodraven_restore_last_success_timestamp_seconds": false,
		"bloodraven_restore_last_source_size_bytes":         false,
	}
	for _, mf := range families {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Fatalf("metric %s was not registered", name)
		}
	}
}

func TestRegisterIncludesReplicationSourceState(t *testing.T) {
	reg := prometheus.NewRegistry()
	Register(reg)
	ReplicationSourceState.WithLabelValues("reader", "converged").Set(1)
	defer ReplicationSourceState.DeleteLabelValues("reader", "converged")
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "bloodraven_replication_source_state" {
			return
		}
	}
	t.Fatal("replication source state metric was not registered")
}
