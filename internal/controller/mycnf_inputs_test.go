package controller

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// D-03: a maxBinlogSize that is not a size never reaches my.cnf. Admission
// refuses it; the render falls back to the default for a value stored past
// admission.
func TestEffectivePITRMaxBinlogSize(t *testing.T) {
	for _, tc := range []struct {
		spec, value, refused string
	}{
		{"", defaultPITRMaxBinlogSize, ""},
		{"512m", "512m", ""},
		{"1073741824", "1073741824", ""},
		{"1G", "1G", ""},
		{"100M\n[mysqld]\nskip-grant-tables", defaultPITRMaxBinlogSize, "100M\n[mysqld]\nskip-grant-tables"},
		{"100M\r", defaultPITRMaxBinlogSize, "100M\r"},
		{"1.5G", defaultPITRMaxBinlogSize, "1.5G"},
	} {
		fg := newTestFG()
		fg.Spec.Backup = &v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p", MaxBinlogSize: tc.spec}}
		value, refused := effectivePITRMaxBinlogSize(fg)
		if value != tc.value || refused != tc.refused {
			t.Errorf("maxBinlogSize %q: got (%q, %q), want (%q, %q)", tc.spec, value, refused, tc.value, tc.refused)
		}
	}
}

func TestGenerateMyCnf_RefusesInjectedMaxBinlogSize(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p", MaxBinlogSize: "100M\n[mysqld]\nskip-grant-tables"}}
	cnf := generateMyCnf(fg, true, fg.Spec.Sites[0])
	if strings.Contains(cnf, "skip-grant-tables") || strings.Count(cnf, "[mysqld]") != 1 {
		t.Fatalf("an injected maxBinlogSize reached my.cnf:\n%s", cnf)
	}
	if !strings.Contains(cnf, "\nmax-binlog-size="+defaultPITRMaxBinlogSize+"\n") {
		t.Fatalf("my.cnf lacks the default max-binlog-size:\n%s", cnf)
	}
}

func TestReconcileSiteConfigMaps_WarnsOnRefusedMaxBinlogSize(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p", MaxBinlogSize: "64M\nskip-grant-tables"}}
	r, _ := newReconciler(fg)
	render := groupPITRRender{inputs: pitrRenderInputs{Enabled: true}, action: pitrRenderApply}
	if err := r.reconcileSiteConfigMaps(context.Background(), fg, render); err != nil {
		t.Fatal(err)
	}
	events := r.Recorder.(*record.FakeRecorder).Events
	select {
	case e := <-events:
		if !strings.HasPrefix(e, "Warning "+v1alpha1.EventMysqlConfIgnored+" ") || !strings.Contains(e, "spec.backup.pitr.maxBinlogSize") {
			t.Fatalf("event %q, want a Warning %s naming spec.backup.pitr.maxBinlogSize", e, v1alpha1.EventMysqlConfIgnored)
		}
	default:
		t.Fatal("no Event for a refused maxBinlogSize")
	}
}

// crdRuleRegex returns the regex literal inside the first `matches('...')`
// of the rule on the CRD property at path.
func crdRuleRegex(t *testing.T, file string, path ...string) string {
	t.Helper()
	var out string
	for crdPath, root := range apiCRDRoots(t, file) {
		s, ok := root.at(path)
		if !ok {
			t.Fatalf("%s: no %s", crdPath, strings.Join(path, "."))
		}
		for _, v := range s.Validations {
			if m := regexp.MustCompile(`matches\('([^']*)'\)`).FindStringSubmatch(v.Rule); m != nil {
				if out != "" && out != m[1] {
					t.Fatalf("%s: rule regex %q differs from the other CRD copy's %q", crdPath, m[1], out)
				}
				out = m[1]
			}
		}
	}
	if out == "" {
		t.Fatalf("no matches() rule on %s", strings.Join(path, "."))
	}
	return out
}

// The controller's own checks mirror admission: the same key grammar and
// the same size grammar, so a value admitted is a value rendered.
func TestMyCnfChecksMirrorAdmission(t *testing.T) {
	const file = "shipstream.io_mysqlfailovergroups.yaml"
	for _, path := range [][]string{{"spec", "mysqlConf"}, {"spec", "sites", "[]", "mysqlConf"}} {
		if got := crdRuleRegex(t, file, path...); got != mysqlOptionKeyPattern.String() {
			t.Errorf("%s key rule %q, controller %q", strings.Join(path, "."), got, mysqlOptionKeyPattern.String())
		}
	}
	for crdPath, root := range apiCRDRoots(t, file) {
		s, _ := root.at([]string{"spec", "backup", "pitr", "maxBinlogSize"})
		if s.Pattern != mysqlSizePattern.String() {
			t.Errorf("%s: maxBinlogSize pattern %q, controller %q", crdPath, s.Pattern, mysqlSizePattern.String())
		}
	}
}

func TestFilterDragonflyUserArgs(t *testing.T) {
	for _, tc := range []struct {
		in, want []string
	}{
		{[]string{"--cache_mode=true", "--admin_nopass"}, []string{"--cache_mode=true", "--admin_nopass"}},
		{[]string{"--port", "7000", "--cache_mode=true"}, []string{"--cache_mode=true"}},
		{[]string{"-port", "7000", "--cache_mode=true"}, []string{"--cache_mode=true"}},
		{[]string{"--replicaof", "10.0.0.9 6379", "--cache_mode=true"}, []string{"--cache_mode=true"}},
		{[]string{"--replicaof=10.0.0.9 6379"}, []string{}},
		{[]string{"--maxmemory=1G", "--proactor_threads=2"}, []string{}},
		{[]string{"--nobreak_replication_on_master_restart", "--cache_mode=true"}, []string{"--cache_mode=true"}},
		// A boolean flag takes no separate value: the next arg survives.
		{[]string{"--break_replication_on_master_restart", "--cache_mode=true"}, []string{"--cache_mode=true"}},
		{[]string{"--s3_use_https", "--cache_mode=true"}, []string{"--cache_mode=true"}},
		{[]string{"--flagfile=/etc/df.flags", "--fromenv=port", "--tryfromenv=port"}, []string{}},
		{[]string{"--portable=1", "--dir_extra=x", "dir"}, []string{"--portable=1", "--dir_extra=x", "dir"}},
	} {
		got := filterDragonflyUserArgs(tc.in)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("filter(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestDragonflyArgsRuleCoversOwnedFlags: the admission rule on
// spec.dragonfly.args refuses exactly the args the controller filter
// drops, in every spelling.
func TestDragonflyArgsRuleCoversOwnedFlags(t *testing.T) {
	re := regexp.MustCompile(crdRuleRegex(t, "shipstream.io_mysqlfailovergroups.yaml", "spec", "dragonfly", "args"))
	var probes []string
	for flag := range dragonflyOperatorOwnedFlags {
		probes = append(probes, "--"+flag, "-"+flag, "--"+flag+"=1", "-"+flag+"=x y", "--no"+flag, "--"+flag+"_extra=1", "--"+flag+"x", "-"+flag+"-x")
	}
	probes = append(probes, "--cache_mode=true", "--admin_nopass", "--nodir_cache", "port", "--", "-", "")
	for _, p := range probes {
		owned, _ := dragonflyOwnedFlag(p)
		if refused := re.MatchString(p); refused != owned {
			t.Errorf("arg %q: admission refuses %v, controller filter drops %v", p, refused, owned)
		}
	}
}
