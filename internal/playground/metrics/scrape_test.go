package metrics

import "testing"

const sampleMetrics = `
# HELP bloodraven_failovers_total Total number of failovers executed.
# TYPE bloodraven_failovers_total counter
bloodraven_failovers_total{target_site="iad"} 0
bloodraven_failovers_total{target_site="pdx"} 1
# HELP bloodraven_site_state Current site state as a state-set.
# TYPE bloodraven_site_state gauge
bloodraven_site_state{site="iad",state="writable"} 0
bloodraven_site_state{site="iad",state="read-only"} 1
bloodraven_site_state{site="pdx",state="writable"} 1
bloodraven_site_state{site="pdx",state="read-only"} 0
# HELP bloodraven_planned_failover_duration_seconds Wall-clock duration.
# TYPE bloodraven_planned_failover_duration_seconds histogram
bloodraven_planned_failover_duration_seconds_bucket{target_site="pdx",le="1"} 0
bloodraven_planned_failover_duration_seconds_bucket{target_site="pdx",le="2"} 1
bloodraven_planned_failover_duration_seconds_bucket{target_site="pdx",le="+Inf"} 1
bloodraven_planned_failover_duration_seconds_sum{target_site="pdx"} 1.5
bloodraven_planned_failover_duration_seconds_count{target_site="pdx"} 1
`

func TestSnapshotLookups(t *testing.T) {
	snap, err := ParseSnapshot([]byte(sampleMetrics))
	if err != nil {
		t.Fatalf("ParseSnapshot: %v", err)
	}

	if v, ok := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": "pdx"}); !ok || v != 1 {
		t.Fatalf("counter pdx = %v ok=%v want 1 true", v, ok)
	}
	if v, ok := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": "missing"}); ok {
		t.Fatalf("expected no match for missing, got %v", v)
	}
	if v, ok := snap.Gauge("bloodraven_site_state", map[string]string{"site": "pdx", "state": "writable"}); !ok || v != 1 {
		t.Fatalf("gauge pdx writable = %v ok=%v want 1 true", v, ok)
	}
	if c, ok := snap.HistogramSampleCount("bloodraven_planned_failover_duration_seconds", map[string]string{"target_site": "pdx"}); !ok || c != 1 {
		t.Fatalf("histogram count = %d ok=%v want 1 true", c, ok)
	}
}
