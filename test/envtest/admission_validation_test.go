//go:build envtest

package envtest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// e16Admit creates obj under a fresh name in ns and reports the error. The
// object is created as unstructured so a test can send values a typed
// client would drop (an explicit "").
func e16Admit(t *testing.T, ns string, obj client.Object, kind string, edit func(u *unstructured.Unstructured)) error {
	t.Helper()
	e16Seq++
	obj.SetNamespace(ns)
	obj.SetName(fmt.Sprintf("e16-%d", e16Seq))
	u := inertFieldsObject(t, obj, kind)
	if edit != nil {
		edit(u)
	}
	return k8sClient.Create(ctx, u)
}

var e16Seq int

// e16Expect checks one admission outcome: ok, or an Invalid error that
// contains want.
func e16Expect(t *testing.T, what string, err error, ok bool, want string) {
	t.Helper()
	switch {
	case ok && err != nil:
		t.Errorf("%s: refused: %v", what, err)
	case !ok && err == nil:
		t.Errorf("%s: admitted, want a refusal containing %q", what, want)
	case !ok && (!apierrors.IsInvalid(err) || !strings.Contains(err.Error(), want)):
		t.Errorf("%s: refused for the wrong reason (want %q): %v", what, want, err)
	}
}

func e16FG(mutate func(*v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG("")
	if mutate != nil {
		mutate(fg)
	}
	return fg
}

// A-90: the my.cnf injection paths are closed at admission. A key outside
// [A-Za-z0-9_.-], a value with CR, LF or NUL, a value over 1024
// characters and more than 128 entries are refused, on the group and on a
// site.
func TestE16_MysqlConfInjectionRefused(t *testing.T) {
	ns := createNamespace(t, "e16-mysqlconf")
	many := map[string]any{}
	for i := range 129 {
		many[fmt.Sprintf("opt_%d", i)] = "1"
	}
	limit := map[string]any{}
	for i := range 128 {
		limit[fmt.Sprintf("opt_%d", i)] = "1"
	}
	for _, tc := range []struct {
		name string
		conf map[string]any
		ok   bool
		want string
	}{
		{"plain options", map[string]any{"max_connections": "600", "loose-plugin-load-add": "auth_socket.so", "sql_mode": "STRICT_TRANS_TABLES,NO_ZERO_DATE", "init_connect": "SET a=1; # comment"}, true, ""},
		{"empty value (a flag option)", map[string]any{"skip-name-resolve": ""}, true, ""},
		{"1024-character value", map[string]any{"init_connect": strings.Repeat("x", 1024)}, true, ""},
		{"128 entries", limit, true, ""},
		{"LF in a value injects a section", map[string]any{"max_connections": "600\n[mysqld]\nskip-grant-tables"}, false, "should match"},
		{"CR in a value", map[string]any{"max_connections": "600\rskip-grant-tables"}, false, "should match"},
		{"NUL in a value", map[string]any{"max_connections": "600\x00"}, false, "should match"},
		{"LF in a key", map[string]any{"max_connections\nskip-grant-tables": "1"}, false, "mysqlConf keys must be option names"},
		{"key with a space", map[string]any{"max connections": "1"}, false, "mysqlConf keys must be option names"},
		{"key with '='", map[string]any{"a=b": "1"}, false, "mysqlConf keys must be option names"},
		{"section header as a key", map[string]any{"[client]": "1"}, false, "mysqlConf keys must be option names"},
		{"1025-character value", map[string]any{"init_connect": strings.Repeat("x", 1025)}, false, "Too long"},
		{"129 entries", many, false, "must have at most 128 items"},
	} {
		for _, field := range [][]string{{"spec", "mysqlConf"}, {"spec", "sites", "0", "mysqlConf"}} {
			path := strings.Join(field, ".")
			err := e16Admit(t, ns, e16FG(nil), "MysqlFailoverGroup", func(u *unstructured.Unstructured) {
				if field[1] == "sites" {
					sites, _, _ := unstructured.NestedSlice(u.Object, "spec", "sites")
					sites[0].(map[string]any)["mysqlConf"] = tc.conf
					inertFieldsSet(t, u, sites, "spec", "sites")
					return
				}
				inertFieldsSet(t, u, tc.conf, field...)
			})
			e16Expect(t, tc.name+" in "+path, err, tc.ok, tc.want)
		}
	}
}

// A-90, F-60: each size takes the suffixes its consumer reads. mysqld reads
// k/m/g in either case for max_binlog_size; mysqlsh's bytesPerChunk reads
// k, K, M and G only.
func TestE16_SizePatterns(t *testing.T) {
	ns := createNamespace(t, "e16-sizes")
	withPITR := func(size string) *v1alpha1.MysqlFailoverGroup {
		return e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup = &v1alpha1.BackupSpec{
				Profiles: []v1alpha1.BackupProfile{pitrS3Profile("a", "p1", "")},
				PITR:     &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a", MaxBinlogSize: size},
			}
		})
	}
	for _, tc := range []struct {
		size string
		ok   bool
	}{
		{"512m", true}, {"512M", true}, {"100M", true}, {"1g", true}, {"64k", true}, {"1073741824", true},
		{"1.5G", false}, {"100MB", false}, {"100Mi", false}, {" 100M", false}, {"100M\n[mysqld]", false}, {"-1", false},
		{strings.Repeat("1", 21), false},
	} {
		err := e16Admit(t, ns, withPITR(tc.size), "MysqlFailoverGroup", nil)
		e16Expect(t, fmt.Sprintf("maxBinlogSize %q", tc.size), err, tc.ok, "maxBinlogSize")
	}
	withChunk := func(chunk string) *v1alpha1.MysqlFailoverGroup {
		return e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			p := pitrS3Profile("a", "p1", "")
			p.Dump = &v1alpha1.DumpOptions{BytesPerChunk: chunk}
			fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{p}}
		})
	}
	for _, tc := range []struct {
		chunk string
		ok    bool
	}{
		{"512k", true}, {"512K", true}, {"64M", true}, {"1G", true}, {"1048576", true},
		{"64m", false}, {"1g", false}, {"64Mi", false}, {"64MB", false}, {"64M\n", false},
	} {
		err := e16Admit(t, ns, withChunk(tc.chunk), "MysqlFailoverGroup", nil)
		e16Expect(t, fmt.Sprintf("bytesPerChunk %q", tc.chunk), err, tc.ok, "bytesPerChunk")
	}
}

// A-91: profile references inside one group resolve at admission.
func TestE16_DanglingProfileReferences(t *testing.T) {
	ns := createNamespace(t, "e16-refs")
	const pitrMsg = "spec.backup.pitr.profileName must name an entry of spec.backup.profiles"
	const schedMsg = "spec.backup.schedules[].profileName must name an entry of spec.backup.profiles"
	const verifyMsg = "verification.schedule is required when verification.enabled is true"
	const archiveMsg = "spec.backup.profiles[].verification.pointInTime.archiveProfileName must name an entry of spec.backup.profiles"
	const initMsg = "spec.initFromBackup.pointInTime.archiveProfileName must name an entry of spec.backup.profiles"
	backup := func(b v1alpha1.BackupSpec) *v1alpha1.MysqlFailoverGroup {
		return e16FG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup = &b })
	}
	profile := pitrS3Profile("a", "p1", "")
	sched := func(profile string) v1alpha1.BackupSchedule {
		return v1alpha1.BackupSchedule{Name: "nightly", ProfileName: profile, Schedule: "0 3 * * *"}
	}
	verifying := func(v v1alpha1.VerificationSpec) v1alpha1.BackupProfile {
		p := pitrS3Profile("a", "p1", "")
		p.Verification = &v
		return p
	}
	for _, tc := range []struct {
		name string
		fg   *v1alpha1.MysqlFailoverGroup
		edit func(*unstructured.Unstructured)
		ok   bool
		want string
	}{
		{"no profiles and no schedules", backup(v1alpha1.BackupSpec{}), nil, true, ""},
		{"pitr absent", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}}), nil, true, ""},
		{"pitr disabled, unknown profile", backup(v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{ProfileName: "missing"}}), nil, true, ""},
		{"pitr enabled, existing profile", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}, PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"}}), nil, true, ""},
		{"pitr enabled without profileName", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}, PITR: &v1alpha1.PITRSpec{Enabled: true}}), nil, false, pitrMsg},
		{"pitr enabled, no profiles", backup(v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"}}), nil, false, pitrMsg},
		{"pitr enabled, unknown profile", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}, PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "missing"}}), nil, false, pitrMsg},
		{"pitr profileName \"\"", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}, PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"}}),
			func(u *unstructured.Unstructured) { inertFieldsSet(t, u, "", "spec", "backup", "pitr", "profileName") }, false, "spec.backup.pitr.profileName"},
		{"schedule, existing profile", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}, Schedules: []v1alpha1.BackupSchedule{sched("a")}}), nil, true, ""},
		{"schedule, unknown profile", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}, Schedules: []v1alpha1.BackupSchedule{sched("missing")}}), nil, false, schedMsg},
		{"schedule, no profiles", backup(v1alpha1.BackupSpec{Schedules: []v1alpha1.BackupSchedule{sched("a")}}), nil, false, schedMsg},
		{"verification enabled with a schedule", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{verifying(v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 4 * * *"})}}), nil, true, ""},
		{"verification disabled without a schedule", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{verifying(v1alpha1.VerificationSpec{})}}), nil, true, ""},
		{"verification enabled without a schedule", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{verifying(v1alpha1.VerificationSpec{Enabled: true})}}), nil, false, verifyMsg},
		{"verification enabled, schedule \"\"", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{verifying(v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 4 * * *"})}}),
			func(u *unstructured.Unstructured) {
				profiles, _, _ := unstructured.NestedSlice(u.Object, "spec", "backup", "profiles")
				profiles[0].(map[string]any)["verification"].(map[string]any)["schedule"] = ""
				inertFieldsSet(t, u, profiles, "spec", "backup", "profiles")
			}, false, verifyMsg},
		{"verification archiveProfileName, existing profile", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{verifying(v1alpha1.VerificationSpec{PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest, ArchiveProfileName: "a"}})}}), nil, true, ""},
		{"verification archiveProfileName, unknown profile", backup(v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{verifying(v1alpha1.VerificationSpec{PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest, ArchiveProfileName: "retired"}})}}), nil, false, archiveMsg},
		{"initFromBackup archiveProfileName, existing profile", e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{pitrS3Profile("source", "orders", "")}}
			fg.Spec.InitFromBackup = e16InitPITR("source")
		}), nil, true, ""},
		{"initFromBackup archiveProfileName, unknown profile", e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{pitrS3Profile("source", "orders", "")}}
			fg.Spec.InitFromBackup = e16InitPITR("missing")
		}), nil, false, initMsg},
		{"initFromBackup archiveProfileName, no backup block", e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup = e16InitPITR("source")
		}), nil, false, initMsg},
	} {
		err := e16Admit(t, ns, tc.fg, "MysqlFailoverGroup", tc.edit)
		e16Expect(t, tc.name, err, tc.ok, tc.want)
	}

	// Removing a profile a schedule, the PITR spec or a verification still
	// names is refused; removing it with the reference is not.
	for _, tc := range []struct {
		name string
		spec v1alpha1.BackupSpec
		drop func(*v1alpha1.BackupSpec)
		ok   bool
		want string
	}{
		{"profile a schedule names", v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile, pitrS3Profile("b", "p2", "")}, Schedules: []v1alpha1.BackupSchedule{sched("a")}},
			func(b *v1alpha1.BackupSpec) { b.Profiles = b.Profiles[1:] }, false, schedMsg},
		{"profile and its schedule together", v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile, pitrS3Profile("b", "p2", "")}, Schedules: []v1alpha1.BackupSchedule{sched("a")}},
			func(b *v1alpha1.BackupSpec) { b.Profiles, b.Schedules = b.Profiles[1:], nil }, true, ""},
		{"profile another profile's verification reads", v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{pitrS3Profile("retained", "p0", ""), verifying(v1alpha1.VerificationSpec{PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest, ArchiveProfileName: "retained"}})}},
			func(b *v1alpha1.BackupSpec) { b.Profiles = b.Profiles[1:] }, false, archiveMsg},
	} {
		fg := e16FG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup = tc.spec.DeepCopy() })
		e16Seq++
		fg.Namespace, fg.Name = ns, fmt.Sprintf("e16-%d", e16Seq)
		err := pitrUpdate(t, fg, func(cur *v1alpha1.MysqlFailoverGroup) { tc.drop(cur.Spec.Backup) })
		e16Expect(t, "update: remove "+tc.name, err, tc.ok, tc.want)
	}

	// The initFromBackup reference is checked when the group is created:
	// initFromBackup is fixed at creation, so a standing rule would pin the
	// source profile forever.
	fg := e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{pitrS3Profile("source", "orders", ""), pitrS3Profile("own", "p1", "")}}
		fg.Spec.InitFromBackup = e16InitPITR("source")
	})
	e16Seq++
	fg.Namespace, fg.Name = ns, fmt.Sprintf("e16-%d", e16Seq)
	err := pitrUpdate(t, fg, func(cur *v1alpha1.MysqlFailoverGroup) { cur.Spec.Backup.Profiles = cur.Spec.Backup.Profiles[1:] })
	e16Expect(t, "update: remove the initFromBackup source profile after creation", err, true, "")
	// Status writes still pass every rule (1.31 validates spec rules on
	// them without ratcheting).
	cur := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), cur); err != nil {
		t.Fatal(err)
	}
	cur.Status.ActiveSite = "dc1"
	if err := k8sClient.Status().Update(ctx, cur); err != nil {
		t.Errorf("status write after the source profile was removed: %v", err)
	}
}

func e16InitPITR(archiveProfile string) *v1alpha1.InitFromBackupSpec {
	return &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "orders-nightly"}},
		PointInTime: &v1alpha1.PointInTimeSpec{
			ArchiveProfileName: archiveProfile,
			StopTime:           metav1.Time{Time: time.Date(2026, 4, 15, 9, 30, 0, 0, time.UTC)},
		},
	}
}

// A-92, D-05, F-38: spec.dragonfly.args is bounded and cannot set a flag the
// operator owns, in any spelling Dragonfly's flag parser reads.
func TestE16_DragonflyArgs(t *testing.T) {
	ns := createNamespace(t, "e16-dragonfly")
	const msg = "spec.dragonfly.args must not set a flag the operator owns"
	withArgs := func(args ...string) *v1alpha1.MysqlFailoverGroup {
		return e16FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, Image: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0", Args: args}
		})
	}
	for _, tc := range []struct {
		args []string
		ok   bool
		want string
	}{
		{[]string{"--cache_mode=true", "--cluster_mode=emulated", "--admin_nopass", "--dbfilename=dump", "--portable=1", "--dir_extra=x"}, true, ""},
		{[]string{"--replicaof=10.0.0.9 6379"}, false, msg},
		{[]string{"--replicaof", "10.0.0.9 6379"}, false, msg},
		{[]string{"-replicaof=10.0.0.9:6379"}, false, msg},
		{[]string{"--maxmemory=1G"}, false, msg},
		{[]string{"--proactor_threads=2"}, false, msg},
		{[]string{"--nobreak_replication_on_master_restart"}, false, msg},
		{[]string{"--break_replication_on_master_restart=false"}, false, msg},
		{[]string{"-port=7000"}, false, msg},
		{[]string{"--admin_port", "9000"}, false, msg},
		{[]string{"--requirepass=x"}, false, msg},
		{[]string{"--masterauth=x"}, false, msg},
		{[]string{"--bind=0.0.0.0"}, false, msg},
		{[]string{"--dir=/data"}, false, msg},
		{[]string{"--s3_endpoint=minio:9000"}, false, msg},
		{[]string{"--nos3_use_https"}, false, msg},
		{[]string{"--s3_sign_payload=false"}, false, msg},
		{[]string{"--flagfile=/etc/dragonfly.flags"}, false, msg},
		{[]string{"--fromenv=port"}, false, msg},
		{[]string{"--tryfromenv=port"}, false, msg},
		{make([]string, 65), false, "must have at most 64 items"},
		{[]string{"--cache_mode=" + strings.Repeat("x", 256)}, false, "Too long"},
	} {
		args := tc.args
		if len(args) == 65 {
			for i := range args {
				args[i] = fmt.Sprintf("--opt%d=1", i)
			}
		}
		err := e16Admit(t, ns, withArgs(args...), "MysqlFailoverGroup", nil)
		e16Expect(t, fmt.Sprintf("args %.80q", args), err, tc.ok, tc.want)
	}
}

// A-92: spec.license is a compact JWS of at most 4096 characters.
func TestE16_LicenseShape(t *testing.T) {
	ns := createNamespace(t, "e16-license")
	seg := func(n int) string { return strings.Repeat("A", n) }
	for _, tc := range []struct {
		value string
		ok    bool
	}{
		{"eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl_-", true},
		{seg(1000) + "." + seg(2000) + "." + seg(1094), true},
		{seg(1000) + "." + seg(2000) + "." + seg(1095), false},
		{"not-a-jwt", false},
		{"a.b", false},
		{"a.b.c.d", false},
		{"a..c", false},
		{"a.b.c\n", false},
		{" a.b.c", false},
		{"a.b.c=", false},
	} {
		fg := e16FG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.License = tc.value })
		err := e16Admit(t, ns, fg, "MysqlFailoverGroup", nil)
		e16Expect(t, fmt.Sprintf("license %.40q (%d chars)", tc.value, len(tc.value)), err, tc.ok, "spec.license")
	}
}

// A-93: every kind serves its short names and the bloodraven and mysql
// categories, and no kind serves the generic backup category, which would
// shadow another operator's backups resource for `kubectl get backup`.
func TestE16_KindMetadataServed(t *testing.T) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	list, err := dc.ServerResourcesForGroupVersion(v1alpha1.GroupVersion.String())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]struct{ short, categories []string }{
		"mysqlbackups":             {[]string{"mbk", "mbks"}, []string{"bloodraven", "mysql"}},
		"mysqlbackupverifications": {[]string{"mbv", "mbvs"}, []string{"bloodraven", "mysql"}},
		"mysqlfailovergroups":      {[]string{"mfg", "mfgs"}, []string{"bloodraven", "mysql"}},
		"mysqldatabases":           {[]string{"mydb", "mydbs"}, []string{"bloodraven", "mysql"}},
		"mysqlstandbyclusters":     {[]string{"msc", "mscs"}, []string{"bloodraven", "mysql", "dr"}},
	}
	seen := 0
	for _, r := range list.APIResources {
		w, ok := want[r.Name]
		if !ok {
			continue
		}
		seen++
		if strings.Join(r.ShortNames, ",") != strings.Join(w.short, ",") {
			t.Errorf("%s: shortNames %v, want %v", r.Name, r.ShortNames, w.short)
		}
		if strings.Join(r.Categories, ",") != strings.Join(w.categories, ",") {
			t.Errorf("%s: categories %v, want %v", r.Name, r.Categories, w.categories)
		}
	}
	if seen != len(want) {
		t.Errorf("discovery served %d of the %d kinds", seen, len(want))
	}
}

// A-92: every CRD, the generated copy and the chart's, installs on this API
// server, which refuses a CRD whose CEL rules exceed the cost budget. Each
// is installed under a throwaway group so it does not replace the suite's.
func TestE16_CRDsInstallWithinCELCostBudget(t *testing.T) {
	crdGVK := schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}
	for i, dir := range []string{filepath.Join("..", "..", "config", "crd", "bases"), filepath.Join("..", "..", "charts", "bloodraven", "crds")} {
		files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
		if err != nil || len(files) != 5 {
			t.Fatalf("%s: want 5 CRDs, got %d (%v)", dir, len(files), err)
		}
		for _, f := range files {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			u := &unstructured.Unstructured{}
			if err := yaml.Unmarshal(b, &u.Object); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			group := fmt.Sprintf("cost%d.e16.example.com", i)
			plural, _, _ := unstructured.NestedString(u.Object, "spec", "names", "plural")
			inertFieldsSet(t, u, group, "spec", "group")
			u.SetName(plural + "." + group)
			u.SetGroupVersionKind(crdGVK)
			if err := k8sClient.Create(ctx, u); err != nil {
				t.Errorf("%s does not install (CEL cost budget or schema): %v", f, err)
				continue
			}
			_ = k8sClient.Delete(ctx, u)
		}
	}
}
