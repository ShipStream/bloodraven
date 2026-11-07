package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// boundedName returns a DNS-1035 label of n characters.
func boundedName(n int, fill string) string {
	return "a" + strings.Repeat(fill, n-1)
}

// TestDerivedNamesFitNameBounds: every name the operator derives from a
// group and site name that admission admits (a site of 1 to
// MaxSiteNameLength characters, the group taking the rest of
// MaxGroupAndSiteNameLength) is a valid object name of its kind, and the
// group and site names are valid label values.
func TestDerivedNamesFitNameBounds(t *testing.T) {
	for siteLen := 1; siteLen <= v1alpha1.MaxSiteNameLength; siteLen++ {
		group := boundedName(v1alpha1.MaxGroupAndSiteNameLength-siteLen, "g")
		site := boundedName(siteLen, "s")
		label := func(what, v string) {
			t.Helper()
			if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
				t.Errorf("%s %q is not a label value: %v", what, v, errs)
			}
		}
		service := func(what, v string) {
			t.Helper()
			if errs := validation.IsDNS1035Label(v); len(errs) > 0 {
				t.Errorf("%s Service %q (group %d, site %d characters): %v", what, v, len(group), len(site), errs)
			}
		}
		// A Job name is a label value (job-name), so a DNS-1123 label.
		dnsLabel := func(what, v string) {
			t.Helper()
			if errs := validation.IsDNS1123Label(v); len(errs) > 0 {
				t.Errorf("%s %q: %v", what, v, errs)
			}
		}
		subdomain := func(what, v string) {
			t.Helper()
			if errs := validation.IsDNS1123Subdomain(v); len(errs) > 0 {
				t.Errorf("%s %q: %v", what, v, errs)
			}
		}
		label("group name", group)
		label("site name", site)
		service("site", resourceName(group, site))
		service("internal", internalSiteServiceName(group, site))
		service("primary", "mysql-"+group+"-primary")
		service("replicas", "mysql-"+group+"-replicas")
		service("Dragonfly site", dragonflySiteServiceName(group, site))
		service("Dragonfly active", dragonflyActiveServiceName(group))
		// The StatefulSet's controller-revision-hash label is
		// <name>-<10-character hash>.
		if sts := dragonflyStatefulSetName(group, site); len(sts) > 52 {
			t.Errorf("Dragonfly StatefulSet %q is %d characters, want at most 52", sts, len(sts))
		}
		subdomain("Deployment", resourceName(group, site))
		subdomain("data PVC", resourceName(group, site)+"-data")
		subdomain("site ConfigMap", siteConfigMapName(group, site)+"-0123456789ab")
		subdomain("scripts ConfigMap", backupScriptsConfigMapName(group))
		subdomain("restore credentials Secret", restoreCredsSecretName(group))
		subdomain("owned backup PVC", ownedBackupPVCName(group, boundedName(63, "p")))
		dnsLabel("restore Job", restoreJobName(group, site))
		dnsLabel("in-place restore Job", inPlaceRestoreJobName(group, site))
		// The API server refuses a CronJob name over 52 characters.
		cronJob := func(what, v string) {
			t.Helper()
			dnsLabel(what, v)
			if len(v) > 52 {
				t.Errorf("%s %q is %d characters, want at most 52", what, v, len(v))
			}
		}
		cronJob("schedule CronJob", scheduleCronJobName(group, boundedName(63, "c")))
		cronJob("verification CronJob", verificationScheduleCronJobName(group, boundedName(63, "p")))
		dnsLabel("PITR check Job", pitrCheckJobName(group, "binding-id", site))
		subdomain("PITR key snapshot Secret", pitrKeySnapshotName(group, []byte("k"), "s3://bucket/prefix", []byte("p")))
		subdomain("digest key Secret", digestKeySecretName(group))
		subdomain("schedule trigger", scheduleTriggerName(group))
		if key := v1alpha1.ReadOnlyTaintKey(boundedName(63, "n"), group); len(validation.IsQualifiedName(key)) > 0 {
			t.Errorf("taint key %q: %v", key, validation.IsQualifiedName(key))
		}
	}

	// MysqlBackup and MysqlBackupVerification names are label values.
	run := boundedName(v1alpha1.MaxRunNameLength, "r")
	backup := &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: run, UID: types.UID("6f1c2d3e-0000-4000-8000-000000000001")}}
	for what, v := range map[string]string{
		"backup Job":               backupJobName(run),
		"cleanup Job":              cleanupJobName(run),
		"verification Job":         verificationJobName(run),
		"dump-key snapshot Secret": dumpKeySnapshotName(backup),
	} {
		if errs := validation.IsDNS1123Label(v); len(errs) > 0 {
			t.Errorf("%s %q: %v", what, v, errs)
		}
	}
	if errs := validation.IsValidLabelValue(run); len(errs) > 0 {
		t.Errorf("run name %q: %v", run, errs)
	}
}

// TestNameBoundsMatchTheCRDs: the CRD markers carry the bounds the
// derived-name test is sized against.
func TestNameBoundsMatchTheCRDs(t *testing.T) {
	for path, root := range apiCRDRoots(t, "shipstream.io_mysqlfailovergroups.yaml") {
		name, ok := root.at([]string{"spec", "sites", "[]", "name"})
		if !ok || name.MaxLength == nil || *name.MaxLength != v1alpha1.MaxSiteNameLength {
			t.Errorf("%s: spec.sites[].name maxLength = %v, want %d", path, name.MaxLength, v1alpha1.MaxSiteNameLength)
		}
		if !hasRule(root, "size(self.metadata.name) + size(s.name) <= 41") {
			t.Errorf("%s: no root rule bounding the group plus site name to %d", path, v1alpha1.MaxGroupAndSiteNameLength)
		}
	}
	for _, file := range []string{"shipstream.io_mysqlbackups.yaml", "shipstream.io_mysqlbackupverifications.yaml"} {
		for path, root := range apiCRDRoots(t, file) {
			if !hasRule(root, "size(self.metadata.name) <= 63") {
				t.Errorf("%s: no root rule bounding metadata.name to %d", path, v1alpha1.MaxRunNameLength)
			}
		}
	}
}

func hasRule(s apiCRDSchema, rule string) bool {
	for _, v := range s.Validations {
		if strings.Contains(v.Rule, rule) {
			return true
		}
	}
	return false
}
