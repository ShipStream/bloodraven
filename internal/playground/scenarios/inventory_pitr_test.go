package scenarios

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// scenariosPITR206 are the #206 cross-site PITR scenarios (DESIGN §9.4).
// They run in the release profile and in the label-gated e2e `pitr` job.
var scenariosPITR206 = []string{
	"37-pitr-planned-failback-no-reclone",
	"68-pitr-failback-with-reclone",
	"70-pitr-lossy-failover",
}

func TestScenariosPITR206RegisteredAndProfiled(t *testing.T) {
	all := runner.DefaultRegistry.List()
	smoke := runner.SelectForProfile(all, runner.ProfileSmoke)
	release := runner.SelectForProfile(all, runner.ProfileRelease)
	for _, id := range scenariosPITR206 {
		s, ok := runner.DefaultRegistry.Get(id)
		if !ok {
			t.Errorf("scenario %s is not registered", id)
			continue
		}
		if s.Quarantine != "" {
			t.Errorf("scenario %s must not be quarantined: %q", id, s.Quarantine)
		}
		if !s.ResetBeforeRunAll {
			t.Errorf("scenario %s must reset before run-all (destructive PITR state)", id)
		}
		if !strings.HasSuffix(s.DocLink, "#"+id) {
			t.Errorf("scenario %s DocLink %q must anchor at #%s", id, s.DocLink, id)
		}
		if !inventoryContains(release, id) || inventoryContains(smoke, id) {
			t.Errorf("scenario %s must be release-only (release=%v smoke=%v)", id, inventoryContains(release, id), inventoryContains(smoke, id))
		}
	}
}

func TestGTIDSetsEqualComparesSets(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const v = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
	cases := []struct {
		a, b string
		want bool
	}{
		{u + ":1-5", u + ":1-3:4-5", true},
		{u + ":1-5," + v + ":1", v + ":1,\n" + u + ":1-5", true},
		{u + ":1-5", u + ":1-4", false},
		{"", "", true},
		{u + ":1-5:app:1-2", u + ":1-5", false},
	}
	for _, c := range cases {
		got, err := gtidSetsEqual(c.a, c.b)
		if err != nil {
			t.Fatalf("gtidSetsEqual(%q,%q): %v", c.a, c.b, err)
		}
		if got != c.want {
			t.Errorf("gtidSetsEqual(%q,%q)=%v, want %v", c.a, c.b, got, c.want)
		}
	}
	if _, err := gtidSetsEqual("not-a-set", ""); err == nil {
		t.Error("a malformed set must be an error, not unequal")
	}
}

func TestPITRExpectationExpectedSet(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	e := pitrExpectation{Fence: pitrFence{GTID: u + ":1-10"}, Exclude: u + ":4-5"}
	got, err := e.expectedSet()
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := gtidSetsEqual(got, u+":1-3:6-10"); !ok {
		t.Errorf("expected set %q, want %s:1-3:6-10", got, u)
	}
}

func TestPITRMarkerQuery(t *testing.T) {
	got := pitrMarkerQuery("chaos`db", "run-1", []string{"baseline", "M1"})
	for _, frag := range []string{"= 'baseline,M1'", "FROM `chaos``db`.marker WHERE run_id='run-1'", "ORDER BY id"} {
		if !strings.Contains(got, frag) {
			t.Errorf("query %q lacks %q", got, frag)
		}
	}
}

func TestPITRSourceSpecForRestore(t *testing.T) {
	src := &v1alpha1.MysqlFailoverGroup{}
	src.Namespace = "ns"
	src.Status.ActiveSite = "pdx"
	src.Spec.DNS = v1alpha1.DNSSpec{Hostname: "playground-db.example.local", TTL: 10}
	src.Spec.Sites = []v1alpha1.SiteSpec{
		{Name: "iad", Role: v1alpha1.SiteRolePrimaryCandidate, Zone: "zone-iad", LBIP: "10.0.0.1", TaintNodeSelector: map[string]string{"a": "b"}},
		{Name: "pdx", Role: v1alpha1.SiteRolePrimaryCandidate, Zone: "zone-pdx", LBIP: "10.0.0.2"},
		{Name: "reader", Role: v1alpha1.SiteRoleReadOnly, Zone: "zone-reader"},
	}
	src.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true}
	src.Spec.Backup = &v1alpha1.BackupSpec{
		Profiles: []v1alpha1.BackupProfile{{Name: backupE2EProfile}},
		PITR:     &v1alpha1.PITRSpec{Enabled: true, ProfileName: backupE2EProfile},
	}
	got, err := pitrSourceSpecForRestore(src, "s37-restored", "bk", "2026-11-05T10:00:01Z")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spec.Sites) != 2 {
		t.Fatalf("sites=%d, want the two primary candidates", len(got.Spec.Sites))
	}
	for _, s := range got.Spec.Sites {
		if s.LBIP != "" || s.TaintNodeSelector != nil {
			t.Errorf("site %s keeps lbIP %q / taints %v", s.Name, s.LBIP, s.TaintNodeSelector)
		}
	}
	if p := got.Spec.SplitBrainPolicy.SitePriorities; len(p) != 2 || p[0] != "pdx" {
		t.Errorf("priorities %v must seed the source's active site pdx", p)
	}
	if got.Spec.Dragonfly != nil || got.Spec.Backup.PITR != nil || got.Spec.DNS.Hostname == src.Spec.DNS.Hostname {
		t.Errorf("restored spec must drop dragonfly and PITR and use its own DNS name: %+v", got.Spec)
	}
	pit := got.Spec.InitFromBackup.PointInTime
	if pit.ArchiveProfileName != backupE2EProfile || pit.StopDatetime != "2026-11-05T10:00:01Z" || got.Spec.InitFromBackup.Source.MysqlBackupRef.Name != "bk" {
		t.Errorf("initFromBackup = %+v", got.Spec.InitFromBackup)
	}
	if src.Spec.Backup.PITR == nil || src.Spec.Sites[0].LBIP == "" {
		t.Error("the source spec must not be mutated")
	}
}
