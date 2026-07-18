package scenarios

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func TestReaderScenarios41To44RegisteredAndProfiled(t *testing.T) {
	releaseIDs := []string{
		"41-reader-availability-during-failover",
		"42-reader-stall-no-group-degradation",
		"43-writable-reader-fence",
		"44-reader-source-convergence-invariant",
	}
	release := runner.SelectForProfile(runner.DefaultRegistry.List(), runner.ProfileRelease)
	smoke := runner.SelectForProfile(runner.DefaultRegistry.List(), runner.ProfileSmoke)
	for _, id := range releaseIDs {
		scenario, ok := runner.DefaultRegistry.Get(id)
		if !ok {
			t.Errorf("scenario %s is not registered", id)
			continue
		}
		if scenario.Quarantine != "" {
			t.Errorf("scenario %s must not be quarantined: %q", id, scenario.Quarantine)
		}
		if scenario.Timeout <= 0 || scenario.Precheck == nil || len(scenario.Steps) == 0 {
			t.Errorf("scenario %s is incomplete: timeout=%s precheck=%v steps=%d", id, scenario.Timeout, scenario.Precheck != nil, len(scenario.Steps))
		}
		if !inventoryContains(release, id) {
			t.Errorf("scenario %s is not selected by the release profile", id)
		}
	}
	if !inventoryContains(smoke, "42-reader-stall-no-group-degradation") {
		t.Error("42-reader-stall-no-group-degradation must be in the smoke profile (issue #115 R4)")
	}
}

func TestFindReadOnlyReaderSite(t *testing.T) {
	readOnly := v1alpha1.SiteRoleReadOnly
	candidate := v1alpha1.SiteRolePrimaryCandidate
	mfg := &v1alpha1.MysqlFailoverGroup{Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{
		{Name: "iad", Role: candidate},
		{Name: "pdx", Role: candidate},
	}}}
	if _, err := findReadOnlyReaderSite(mfg); err == nil {
		t.Error("expected error with zero read-only sites")
	}
	mfg.Spec.Sites = append(mfg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", Role: readOnly})
	reader, err := findReadOnlyReaderSite(mfg)
	if err != nil || reader != "reader" {
		t.Errorf("findReadOnlyReaderSite = %q, %v; want reader, nil", reader, err)
	}
	mfg.Spec.Sites = append(mfg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader2", Role: readOnly})
	if _, err := findReadOnlyReaderSite(mfg); err == nil {
		t.Error("expected error with two read-only sites")
	}
}

func TestGtidSetTransactions(t *testing.T) {
	got, err := gtidSetTransactions("aaaa:1-3:7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"aaaa:1", "aaaa:2", "aaaa:3", "aaaa:7"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	if _, err := gtidSetTransactions("aaaa:1-2,bbbb:5"); err != nil {
		t.Errorf("multi-uuid set should parse: %v", err)
	}
	for _, bad := range []string{"", "no-intervals", "aaaa:x", "aaaa:5-2", "aaaa:1-100"} {
		if _, err := gtidSetTransactions(bad); err == nil {
			t.Errorf("gtidSetTransactions(%q) succeeded, want error", bad)
		}
	}
}

func TestS41ObservationResult(t *testing.T) {
	state := &s41RunState{
		oldHost: "mysql-playground-iad-internal.ns.svc.cluster.local",
		newHost: "mysql-playground-pdx-internal.ns.svc.cluster.local",
	}
	healthy := func() *s41Observation {
		return &s41Observation{reads: 40, sourceHosts: []string{state.oldHost, state.newHost}}
	}

	if err := healthy().result(state); err != nil {
		t.Errorf("clean old->new history rejected: %v", err)
	}

	o := healthy()
	o.reads = 5
	if err := o.result(state); err == nil || !strings.Contains(err.Error(), "too few") {
		t.Errorf("too-few-reads error = %v", err)
	}

	o = healthy()
	o.sourceHosts = []string{state.oldHost}
	if err := o.result(state); err == nil || !strings.Contains(err.Error(), "never reported the new primary") {
		t.Errorf("never-new error = %v", err)
	}

	o = healthy()
	o.sourceHosts = []string{state.oldHost, state.newHost, state.oldHost}
	if err := o.result(state); err == nil || !strings.Contains(err.Error(), "flipped back") {
		t.Errorf("flip-back error = %v", err)
	}

	o = healthy()
	o.sourceHosts = []string{state.oldHost, "mysql-playground-other-internal.ns.svc.cluster.local", state.newHost}
	if err := o.result(state); err == nil || !strings.Contains(err.Error(), "unexpected replication source") {
		t.Errorf("foreign-host error = %v", err)
	}
}
