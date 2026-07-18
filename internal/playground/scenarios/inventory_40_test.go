package scenarios

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func TestScenario40RegisteredAndReleaseProfiled(t *testing.T) {
	const id = "40-reader-data-loss-reclone"
	scenario, ok := runner.DefaultRegistry.Get(id)
	if !ok {
		t.Fatalf("scenario %s is not registered", id)
	}
	if scenario.Quarantine != "" {
		t.Fatalf("scenario %s must not be quarantined: %q", id, scenario.Quarantine)
	}
	if !scenario.ResetBeforeRunAll {
		t.Fatalf("scenario %s must reset before run-all", id)
	}
	if scenario.Timeout <= 0 || scenario.Precheck == nil || len(scenario.Steps) == 0 {
		t.Fatalf("scenario %s is incomplete: timeout=%s precheck=%v steps=%d", id, scenario.Timeout, scenario.Precheck != nil, len(scenario.Steps))
	}
	if !inventoryContains(runner.SelectForProfile(runner.DefaultRegistry.List(), runner.ProfileRelease), id) {
		t.Fatalf("scenario %s is not selected by the release profile", id)
	}
}

func TestCanonicalMySQLHost(t *testing.T) {
	for _, input := range []string{
		"MYSQL-PLAYGROUND-IAD-INTERNAL.NS.SVC.CLUSTER.LOCAL.",
		" mysql-playground-iad-internal.ns.svc.cluster.local:3306 ",
	} {
		if got, want := canonicalMySQLHost(input), "mysql-playground-iad-internal.ns.svc.cluster.local"; got != want {
			t.Errorf("canonicalMySQLHost(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestS40AssertReaderStatus(t *testing.T) {
	maxLag := int64(10)
	lag := int64(10)
	mfg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Replication: &v1alpha1.ReplicationSpec{ReadOnlyMaxLagSeconds: &maxLag},
		},
	}
	status := v1alpha1.SiteStatus{
		Name:                   "reader",
		State:                  "read-only",
		Replicating:            true,
		SecondsBehindSource:    &lag,
		SourceHost:             "mysql-playground-iad-internal.ns.svc.cluster.local:3306",
		SourceConvergenceState: v1alpha1.SourceConvergenceConverged,
	}
	if err := s40AssertReaderStatus(mfg, &status, "mysql-playground-iad-internal.ns.svc.cluster.local"); err != nil {
		t.Fatalf("exact-threshold reader rejected: %v", err)
	}

	lag = 11
	if err := s40AssertReaderStatus(mfg, &status, "mysql-playground-iad-internal.ns.svc.cluster.local"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("over-threshold error = %v, want exceeds", err)
	}
	status.SecondsBehindSource = nil
	if err := s40AssertReaderStatus(mfg, &status, "mysql-playground-iad-internal.ns.svc.cluster.local"); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown-lag error = %v, want unknown", err)
	}
}
