package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

func TestSiteGaugeLabelSets(t *testing.T) {
	tests := []struct {
		name       string
		family     string
		seed       func()
		cleanup    func()
		wantNames  []string
		wantValues map[string]string
	}{
		{
			name:   "lag carries role for a reader",
			family: "bloodraven_replication_lag_seconds",
			seed: func() {
				ReplicationLag.WithLabelValues("warehouse", "orders", "reader", "read-only").Set(45)
			},
			cleanup: func() {
				DeleteReplicationGauges("warehouse", "orders", "reader")
			},
			wantNames: []string{"namespace", "group", "site", "role"},
			wantValues: map[string]string{
				"namespace": "warehouse", "group": "orders", "site": "reader", "role": "read-only",
			},
		},
		{
			name:   "lag carries role for a primary-candidate",
			family: "bloodraven_replication_lag_seconds",
			seed: func() {
				ReplicationLag.WithLabelValues("warehouse", "orders", "pdx", "primary-candidate").Set(12)
			},
			cleanup: func() {
				DeleteReplicationGauges("warehouse", "orders", "pdx")
			},
			wantNames: []string{"namespace", "group", "site", "role"},
			wantValues: map[string]string{
				"namespace": "warehouse", "group": "orders", "site": "pdx", "role": "primary-candidate",
			},
		},
		{
			name:   "running carries role and thread",
			family: "bloodraven_replication_running",
			seed: func() {
				ReplicationRunning.WithLabelValues("warehouse", "orders", "reader", "read-only", "io").Set(1)
			},
			cleanup: func() {
				DeleteReplicationGauges("warehouse", "orders", "reader")
			},
			wantNames: []string{"namespace", "group", "site", "role", "thread"},
			wantValues: map[string]string{
				"namespace": "warehouse", "group": "orders", "site": "reader", "role": "read-only", "thread": "io",
			},
		},
		{
			name:   "site state carries role",
			family: "bloodraven_site_state",
			seed: func() {
				SiteState.WithLabelValues("warehouse", "orders", "iad", "primary-candidate", "writable").Set(1)
			},
			cleanup: func() {
				DeleteSiteState("warehouse", "orders", "iad")
			},
			wantNames: []string{"namespace", "group", "site", "role", "state"},
			wantValues: map[string]string{
				"namespace": "warehouse", "group": "orders", "site": "iad", "role": "primary-candidate", "state": "writable",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			reg.MustRegister(ReplicationLag, ReplicationRunning, SiteState)
			tt.seed()
			t.Cleanup(tt.cleanup)

			got := gatherOne(t, reg, tt.family)
			if len(got.Metric) != 1 {
				t.Fatalf("series count = %d, want 1", len(got.Metric))
			}
			labels := labelMap(got.Metric[0])
			if len(labels) != len(tt.wantNames) {
				t.Fatalf("label names %v, want %v", labels, tt.wantNames)
			}
			for _, name := range tt.wantNames {
				if _, ok := labels[name]; !ok {
					t.Fatalf("missing label %q in %v", name, labels)
				}
			}
			for k, v := range tt.wantValues {
				if labels[k] != v {
					t.Fatalf("label %s = %q, want %q", k, labels[k], v)
				}
			}
		})
	}
}

func TestSiteGaugesSeparateFailoverGroups(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(ReplicationLag)
	ReplicationLag.WithLabelValues("ns-a", "orders", "reader", "read-only").Set(10)
	ReplicationLag.WithLabelValues("ns-b", "orders", "reader", "read-only").Set(20)
	t.Cleanup(func() {
		DeleteReplicationGauges("ns-a", "orders", "reader")
		DeleteReplicationGauges("ns-b", "orders", "reader")
	})

	got := gatherOne(t, reg, "bloodraven_replication_lag_seconds")
	if len(got.Metric) != 2 {
		t.Fatalf("series count = %d, want 2 distinct namespace-scoped series", len(got.Metric))
	}
}

func TestDeleteReplicationGaugesDropsOldRole(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(ReplicationLag, ReplicationRunning)
	ReplicationLag.WithLabelValues("ns", "g", "pdx", "dr-only").Set(9)
	ReplicationRunning.WithLabelValues("ns", "g", "pdx", "dr-only", "io").Set(1)
	ReplicationRunning.WithLabelValues("ns", "g", "pdx", "dr-only", "sql").Set(1)
	t.Cleanup(func() { DeleteReplicationGauges("ns", "g", "pdx") })

	DeleteReplicationGauges("ns", "g", "pdx")
	ReplicationLag.WithLabelValues("ns", "g", "pdx", "read-only").Set(3)

	got := gatherOne(t, reg, "bloodraven_replication_lag_seconds")
	if len(got.Metric) != 1 {
		t.Fatalf("lag series after role change = %d, want 1", len(got.Metric))
	}
	labels := labelMap(got.Metric[0])
	if labels["role"] != "read-only" {
		t.Fatalf("role = %q, want read-only", labels["role"])
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "bloodraven_replication_running" && len(family.Metric) != 0 {
			t.Fatalf("running series after delete = %d, want 0", len(family.Metric))
		}
	}
}

func gatherOne(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric %s not gathered", name)
	return nil
}

func labelMap(m *dto.Metric) map[string]string {
	out := make(map[string]string, len(m.Label))
	for _, l := range m.Label {
		out[l.GetName()] = l.GetValue()
	}
	return out
}
