package component

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

func TestReplicationMetricsCarryRoleAndGroup(t *testing.T) {
	const (
		ns    = "warehouse"
		group = "orders-metrics-142"
	)
	lagCandidate := int64(5)
	lagReader := int64(90)
	primary := &mockMySQL{readOnly: false, gtidExecuted: "uuid:1-10"}
	candidate := &mockMySQL{
		readOnly:     true,
		gtidExecuted: "uuid:1-10",
		replicaStatus: &mysql.ReplicaStatus{
			IORunning: true, SQLRunning: true, SourceHost: "primary-internal",
			SecondsBehindSource: &lagCandidate,
		},
	}
	reader := &mockMySQL{
		readOnly:     true,
		gtidExecuted: "uuid:1-10",
		replicaStatus: &mysql.ReplicaStatus{
			IORunning: true, SQLRunning: true, SourceHost: "primary-internal",
			SecondsBehindSource: &lagReader,
		},
	}
	tm := newThreeSiteMetricsTM(t, ns, group, primary, candidate, reader,
		state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly)
	t.Cleanup(func() {
		metrics.DeleteSiteGauges(ns, group, "primary")
		metrics.DeleteSiteGauges(ns, group, "candidate")
		metrics.DeleteSiteGauges(ns, group, "reader")
	})

	tm.Poll(context.Background())
	tm.Poll(context.Background())

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.ReplicationLag, metrics.ReplicationRunning, metrics.SiteState)

	lag := gatherFamily(t, reg, "bloodraven_replication_lag_seconds")
	assertSeries(t, lag, map[string]string{
		"namespace": ns, "group": group, "site": "candidate", "role": "primary-candidate",
	}, 5)
	assertSeries(t, lag, map[string]string{
		"namespace": ns, "group": group, "site": "reader", "role": "read-only",
	}, 90)
	if _, ok := findSeries(lag, map[string]string{"namespace": ns, "group": group, "site": "primary"}); ok {
		t.Fatal("writable primary must not emit a lag series")
	}

	running := gatherFamily(t, reg, "bloodraven_replication_running")
	assertSeries(t, running, map[string]string{
		"namespace": ns, "group": group, "site": "reader", "role": "read-only", "thread": "io",
	}, 1)
	assertSeries(t, running, map[string]string{
		"namespace": ns, "group": group, "site": "candidate", "role": "primary-candidate", "thread": "sql",
	}, 1)

	siteState := gatherFamily(t, reg, "bloodraven_site_state")
	assertSeries(t, siteState, map[string]string{
		"namespace": ns, "group": group, "site": "primary", "role": "primary-candidate", "state": "writable",
	}, 1)
	assertSeries(t, siteState, map[string]string{
		"namespace": ns, "group": group, "site": "reader", "role": "read-only", "state": "read-only",
	}, 1)
}

func TestReplicationMetricsReplaceRoleOnReread(t *testing.T) {
	const (
		ns    = "warehouse"
		group = "orders-metrics-142-role"
	)
	lag := int64(8)
	status := &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "primary-internal",
		SecondsBehindSource: &lag,
	}
	primary := &mockMySQL{readOnly: false, gtidExecuted: "uuid:1-10"}
	follower := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-10", replicaStatus: status}

	first := newThreeSiteMetricsTM(t, ns, group, primary, follower, follower,
		state.SiteRoleDROnly, state.SiteRoleDROnly)
	first.Poll(context.Background())
	first.Poll(context.Background())

	second := newThreeSiteMetricsTM(t, ns, group, primary, follower, follower,
		state.SiteRoleReadOnly, state.SiteRoleReadOnly)
	t.Cleanup(func() {
		metrics.DeleteSiteGauges(ns, group, "primary")
		metrics.DeleteSiteGauges(ns, group, "candidate")
		metrics.DeleteSiteGauges(ns, group, "reader")
	})
	second.Poll(context.Background())
	second.Poll(context.Background())

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.ReplicationLag, metrics.SiteState)
	lagFam := gatherFamily(t, reg, "bloodraven_replication_lag_seconds")
	if _, ok := findSeries(lagFam, map[string]string{
		"namespace": ns, "group": group, "site": "reader", "role": "dr-only",
	}); ok {
		t.Fatal("stale dr-only lag series survived a role change")
	}
	assertSeries(t, lagFam, map[string]string{
		"namespace": ns, "group": group, "site": "reader", "role": "read-only",
	}, 8)
}

func newThreeSiteMetricsTM(t *testing.T, ns, group string, primary, candidate, reader *mockMySQL, candidateRole, readerRole state.SiteRole) *controller.TopologyManager {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return controller.NewTopologyManager(controller.TopologyConfig{
		Namespace: ns, Name: group, FailureThreshold: 1, RecoveryThreshold: 1,
		Sites: []controller.SiteTopologyConfig{
			{Name: "primary", Role: state.SiteRolePrimaryCandidate, Host: "primary-internal"},
			{Name: "candidate", Role: candidateRole, Host: "candidate-internal"},
			{Name: "reader", Role: readerRole, Host: "reader-internal"},
		},
	}, []mysql.Checker{primary, candidate, reader}, controller.NewFailoverController(logger), nil, nil,
		controller.BootstrapConfig{}, newMockTainter(), platform.NewHub(logger), nil, logger)
}

func gatherFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
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

func findSeries(family *dto.MetricFamily, want map[string]string) (*dto.Metric, bool) {
	for _, m := range family.Metric {
		got := make(map[string]string, len(m.Label))
		for _, l := range m.Label {
			got[l.GetName()] = l.GetValue()
		}
		match := true
		for k, v := range want {
			if got[k] != v {
				match = false
				break
			}
		}
		if match {
			return m, true
		}
	}
	return nil, false
}

func assertSeries(t *testing.T, family *dto.MetricFamily, want map[string]string, value float64) {
	t.Helper()
	m, ok := findSeries(family, want)
	if !ok {
		t.Fatalf("no series matching %v in %s", want, family.GetName())
	}
	if m.Gauge == nil || m.Gauge.Value == nil || *m.Gauge.Value != value {
		t.Fatalf("series %v value = %v, want %v", want, m.Gauge, value)
	}
}
