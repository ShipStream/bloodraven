package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestReplicationSourceStateSeparatesFailoverGroups(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(ReplicationSourceState)
	ReplicationSourceState.WithLabelValues("ns-a", "orders", "reader", "converged").Set(1)
	ReplicationSourceState.WithLabelValues("ns-b", "orders", "reader", "converged").Set(1)
	defer ReplicationSourceState.DeleteLabelValues("ns-a", "orders", "reader", "converged")
	defer ReplicationSourceState.DeleteLabelValues("ns-b", "orders", "reader", "converged")

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 1 || len(families[0].Metric) != 2 {
		t.Fatalf("replication source series = %v, want two distinct group-scoped series", families)
	}
}

func TestRegisterIncludesMetrics(t *testing.T) {
	tests := []struct {
		name       string
		familyName string
		seed       func() func()
	}{
		{
			name: "restore duration", familyName: "bloodraven_restore_duration_seconds",
			seed: func() func() {
				labels := []string{"ns", "group", "init_from_backup", "iad"}
				RestoreDurationSeconds.WithLabelValues(labels...).Observe(1)
				return func() { RestoreDurationSeconds.DeleteLabelValues(labels...) }
			},
		},
		{
			name: "restore success", familyName: "bloodraven_restore_last_success_timestamp_seconds",
			seed: func() func() {
				labels := []string{"ns", "group", "init_from_backup", "iad"}
				RestoreLastSuccessTimestamp.WithLabelValues(labels...).Set(1)
				return func() { RestoreLastSuccessTimestamp.DeleteLabelValues(labels...) }
			},
		},
		{
			name: "restore source size", familyName: "bloodraven_restore_last_source_size_bytes",
			seed: func() func() {
				labels := []string{"ns", "group", "init_from_backup", "iad"}
				RestoreLastSourceSizeBytes.WithLabelValues(labels...).Set(1)
				return func() { RestoreLastSourceSizeBytes.DeleteLabelValues(labels...) }
			},
		},
		{
			name: "replication source state", familyName: "bloodraven_replication_source_state",
			seed: func() func() {
				labels := []string{"ns", "group", "reader", "converged"}
				ReplicationSourceState.WithLabelValues(labels...).Set(1)
				return func() { ReplicationSourceState.DeleteLabelValues(labels...) }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			Register(reg)
			cleanup := tt.seed()
			defer cleanup()

			families, err := reg.Gather()
			if err != nil {
				t.Fatalf("gather: %v", err)
			}
			for _, family := range families {
				if family.GetName() == tt.familyName {
					return
				}
			}
			t.Fatalf("metric %s was not registered", tt.familyName)
		})
	}
}

func TestDeleteKeyringSiteMetricsPreservesActiveSites(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(KeyringPhase, KeyringEscrowVersion, EncryptionCoverageGaps, EncryptionCoverageFlag)
	for _, site := range []string{"active", "gone"} {
		KeyringPhase.WithLabelValues("namespace", "group", site, "sealed").Set(1)
		KeyringEscrowVersion.WithLabelValues("namespace", "group", site).Set(2)
		EncryptionCoverageGaps.WithLabelValues("namespace", "group", site).Set(3)
		EncryptionCoverageFlag.WithLabelValues("namespace", "group", site, "redo_log").Set(1)
	}
	defer DeleteKeyringSiteMetrics("namespace", "group", "active")
	defer DeleteKeyringSiteMetrics("namespace", "group", "gone")

	DeleteKeyringSiteMetrics("namespace", "group", "gone")
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if len(family.Metric) != 1 {
			t.Fatalf("metric %s has %d series after cleanup, want one active series", family.GetName(), len(family.Metric))
		}
		for _, label := range family.Metric[0].Label {
			if label.GetName() == "site" && label.GetValue() != "active" {
				t.Fatalf("metric %s retained site %q", family.GetName(), label.GetValue())
			}
		}
	}
}
