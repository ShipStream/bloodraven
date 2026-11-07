//go:build envtest

package envtest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// dnsSubdomainOfLength returns a valid DNS-1123 subdomain of exactly n
// characters (n >= 1): 63-character labels joined by dots.
func dnsSubdomainOfLength(n int) string {
	var b strings.Builder
	for b.Len() < n {
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		label := n - b.Len()
		if label > 63 {
			label = 63
		}
		if n-b.Len()-label == 1 {
			// Never leave a single character for the next label, which
			// would follow a dot that ends the name.
			label--
		}
		b.WriteString(strings.Repeat("a", label))
	}
	return b.String()
}

func TestE5_DNSSubdomainOfLength(t *testing.T) {
	for _, n := range []int{1, 63, 64, 65, 127, 128, 253, 254} {
		got := dnsSubdomainOfLength(n)
		if len(got) != n || strings.HasPrefix(got, ".") || strings.HasSuffix(got, ".") || strings.Contains(got, "..") {
			t.Fatalf("dnsSubdomainOfLength(%d) = %q (len %d)", n, got, len(got))
		}
	}
}

// A-04: an empty backup reference is not admitted. On a verification it
// used to mean "latest backup"; on a restore it failed at reconcile.
func TestE5_EmptyBackupRefRejected(t *testing.T) {
	ns := createNamespace(t, "e5-empty-backupref")

	mbv := inertFieldsObject(t, &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-ref", Namespace: ns},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}, "MysqlBackupVerification")
	inertFieldsSet(t, mbv, map[string]any{}, "spec", "backupRef")
	if err := k8sClient.Create(ctx, mbv); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.backupRef.name") {
		t.Fatalf("MysqlBackupVerification backupRef: {}: want an Invalid error naming spec.backupRef.name, got %v", err)
	}

	for _, field := range []string{"initFromBackup", "restoreInPlace"} {
		fg := newTestFG(ns)
		fg.Name = "empty-ref-" + strings.ToLower(field)
		u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
		body := map[string]any{"source": map[string]any{"backupRef": map[string]any{}}}
		if field == "restoreInPlace" {
			body["confirm"] = "2026-11-06T00:00:00Z"
		}
		inertFieldsSet(t, u, body, "spec", field)
		err := k8sClient.Create(ctx, u)
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec."+field+".source.backupRef.name") {
			t.Fatalf("%s.source.backupRef: {}: want an Invalid error naming its name, got %v", field, err)
		}
	}

	// The renamed one-of rule still counts the reference.
	fg := newTestFG(ns)
	fg.Name = "two-sources"
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{
		BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "nightly"},
		PVC:       &v1alpha1.InitFromBackupPVCSource{ClaimName: "seed"},
	}}
	if err := k8sClient.Create(ctx, fg); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "exactly one of backupRef, s3, or pvc must be set") {
		t.Fatalf("two restore sources: want the one-of refusal, got %v", err)
	}
	fg = newTestFG(ns)
	fg.Name = "one-source"
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{
		BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "nightly"},
	}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("a backupRef restore source refused: %v", err)
	}
}

// A-01, A-02, A-04: every Secret name and object reference is a DNS
// subdomain of at most 253 characters, and every Secret key is at most 253
// characters of the Secret data-key alphabet.
func TestE5_ReferenceNameBounds(t *testing.T) {
	ns := createNamespace(t, "e5-name-bounds")
	type field struct {
		name, path string
		set        func(fg *v1alpha1.MysqlFailoverGroup, v string)
	}
	s3Profile := func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.S3Storage {
		fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{{
			Name: "nightly",
			Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{
				Bucket: "b", CredentialsSecretName: "s3-creds",
			}},
		}}}
		return fg.Spec.Backup.Profiles[0].Storage.S3
	}
	dragonfly := func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.DragonflySpec {
		fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, Image: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0"}
		return fg.Spec.Dragonfly
	}
	names := []field{
		{"operator", "credentials.operatorSecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { fg.Spec.Credentials.OperatorSecretName = v }},
		{"app", "credentials.appSecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { fg.Spec.Credentials.AppSecretName = v }},
		{"read-only", "credentials.readOnlySecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { fg.Spec.Credentials.ReadOnlySecretName = v }},
		{"monitor", "credentials.monitorSecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { fg.Spec.Credentials.MonitorSecretName = v }},
		{"backup", "credentials.backupSecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { fg.Spec.Credentials.BackupSecretName = v }},
		{"tls", "tls.secretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: v} }},
		{"s3", "s3.credentialsSecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) { s3Profile(fg).CredentialsSecretName = v }},
		{"passphrase", "encryption.passphraseSecretKeyRef.name", func(fg *v1alpha1.MysqlFailoverGroup, v string) {
			s3Profile(fg)
			fg.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: v}}
		}},
		{"dragonfly-auth", "auth.passwordSecretKeyRef.name", func(fg *v1alpha1.MysqlFailoverGroup, v string) {
			dragonfly(fg).Auth = &v1alpha1.DragonflyAuthSpec{PasswordSecretKeyRef: v1alpha1.DragonflyPasswordSecretKeyRef{Name: v}}
		}},
		{"dragonfly-snapshot", "snapshot.credentialsSecretName", func(fg *v1alpha1.MysqlFailoverGroup, v string) {
			dragonfly(fg).Snapshot = &v1alpha1.DragonflySnapshotSpec{CredentialsSecretName: v}
		}},
		{"restore-backup-ref", "initFromBackup.source.backupRef.name", func(fg *v1alpha1.MysqlFailoverGroup, v string) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: v}}}
		}},
	}
	keys := []field{
		{"passphrase-key", "encryption.passphraseSecretKeyRef.key", func(fg *v1alpha1.MysqlFailoverGroup, v string) {
			s3Profile(fg)
			fg.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "k", Key: v}}
		}},
		{"dragonfly-auth-key", "auth.passwordSecretKeyRef.key", func(fg *v1alpha1.MysqlFailoverGroup, v string) {
			dragonfly(fg).Auth = &v1alpha1.DragonflyAuthSpec{PasswordSecretKeyRef: v1alpha1.DragonflyPasswordSecretKeyRef{Name: "k", Key: v}}
		}},
	}
	run := func(t *testing.T, f field, value string, ok bool, label string) {
		t.Helper()
		fg := newTestFG(ns)
		fg.Name = fmt.Sprintf("%s-%s", f.name, label)
		f.set(fg, value)
		err := k8sClient.Create(ctx, fg)
		if ok {
			if err != nil {
				t.Fatalf("%s=%q (%s) refused: %v", f.path, value, label, err)
			}
			return
		}
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), f.path) {
			t.Fatalf("%s=%q (%s): want an Invalid error naming %s, got %v", f.path, value, label, f.path, err)
		}
	}
	for _, f := range names {
		t.Run(f.name, func(t *testing.T) {
			run(t, f, dnsSubdomainOfLength(253), true, "max")
			run(t, f, "orders-mysql.v2", true, "dotted")
			run(t, f, dnsSubdomainOfLength(254), false, "over")
			run(t, f, "Orders_MySQL", false, "pattern")
			run(t, f, "orders-", false, "trailing-hyphen")
		})
	}
	for _, f := range keys {
		t.Run(f.name, func(t *testing.T) {
			run(t, f, strings.Repeat("k", 253), true, "max")
			run(t, f, "MYSQL_ROOT.pass-word", true, "alphabet")
			run(t, f, strings.Repeat("k", 254), false, "over")
			run(t, f, "pass/word", false, "pattern")
		})
	}

	// An auth block without its Secret reference is refused structurally,
	// replacing the removed CEL rule.
	fg := newTestFG(ns)
	fg.Name = "dragonfly-auth-empty"
	u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
	inertFieldsSet(t, u, map[string]any{"enabled": true, "image": "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0", "auth": map[string]any{}}, "spec", "dragonfly")
	if err := k8sClient.Create(ctx, u); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.dragonfly.auth.passwordSecretKeyRef") {
		t.Fatalf("dragonfly auth: {}: want an Invalid error naming auth.passwordSecretKeyRef, got %v", err)
	}
	// The key defaults to "password".
	fg = newTestFG(ns)
	fg.Name = "dragonfly-auth-default"
	dragonfly(fg).Auth = &v1alpha1.DragonflyAuthSpec{PasswordSecretKeyRef: v1alpha1.DragonflyPasswordSecretKeyRef{Name: "dragonfly-auth"}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	var stored v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &stored); err != nil {
		t.Fatal(err)
	}
	if got := stored.Spec.Dragonfly.Auth.PasswordSecretKeyRef.Key; got != "password" {
		t.Fatalf("dragonfly passwordSecretKeyRef.key defaulted to %q, want password", got)
	}

	t.Run("other-kinds", func(t *testing.T) {
		over := dnsSubdomainOfLength(254)
		mdb := newMysqlDatabaseCR(ns, "mdb-group-over")
		mdb.Spec.FailoverGroupRef.Name = over
		if err := k8sClient.Create(ctx, mdb); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.failoverGroupRef.name") {
			t.Fatalf("MysqlDatabase failoverGroupRef.name over 253: got %v", err)
		}
		mdb = newMysqlDatabaseCR(ns, "mdb-owner-pattern")
		mdb.Spec.Owner.SecretName = "Owner_Secret"
		if err := k8sClient.Create(ctx, mdb); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.owner.secretName") {
			t.Fatalf("MysqlDatabase owner.secretName pattern: got %v", err)
		}
		mdb = newMysqlDatabaseCR(ns, "mdb-user-pattern")
		mdb.Spec.Users = []v1alpha1.MysqlDatabaseUser{{SecretName: "User_Secret", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.MysqlPrivilege("SELECT")}}}
		if err := k8sClient.Create(ctx, mdb); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.users[0].secretName") {
			t.Fatalf("MysqlDatabase users[].secretName pattern: got %v", err)
		}

		msc := minimalEnvtestStandby("msc-creds-pattern", ns)
		msc.Spec.Source.S3.CredentialsSecretName = "S3_Creds"
		if err := k8sClient.Create(ctx, msc); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.source.s3.credentialsSecretName") {
			t.Fatalf("MysqlStandbyCluster s3.credentialsSecretName pattern: got %v", err)
		}

		mbk := &v1alpha1.MysqlBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "mbk-group-pattern", Namespace: ns},
			Spec: v1alpha1.MysqlBackupSpec{
				FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "Lion"},
				ProfileName:      "nightly",
			},
		}
		if err := k8sClient.Create(ctx, mbk); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.failoverGroupRef.name") {
			t.Fatalf("MysqlBackup failoverGroupRef.name pattern: got %v", err)
		}
		mbv := &v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Name: "mbv-backup-over", Namespace: ns},
			Spec: v1alpha1.MysqlBackupVerificationSpec{
				FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
				ProfileName:      "nightly",
				BackupRef:        &v1alpha1.LocalMysqlBackupRef{Name: over},
			},
		}
		if err := k8sClient.Create(ctx, mbv); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.backupRef.name") {
			t.Fatalf("MysqlBackupVerification backupRef.name over 253: got %v", err)
		}
	})
}

// A-04, A-20: profileName references are DNS labels of at most 63
// characters, as BackupProfile.name is, and MysqlBackup.sourceSite takes
// the site-name pattern.
func TestE5_ProfileAndSiteSelectors(t *testing.T) {
	ns := createNamespace(t, "e5-selectors")
	mbk := func(name, profile, site string) *v1alpha1.MysqlBackup {
		return &v1alpha1.MysqlBackup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: v1alpha1.MysqlBackupSpec{
				FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
				ProfileName:      profile,
				SourceSite:       site,
			},
		}
	}
	mbv := func(name, profile string) *v1alpha1.MysqlBackupVerification {
		return &v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: v1alpha1.MysqlBackupVerificationSpec{
				FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
				ProfileName:      profile,
			},
		}
	}
	for _, tc := range []struct {
		name  string
		obj   client.Object
		field string
	}{
		{"mbk-ok", mbk("mbk-ok", strings.Repeat("p", 63), "dc1"), ""},
		{"mbk-profile-over", mbk("mbk-profile-over", strings.Repeat("p", 64), ""), "spec.profileName"},
		{"mbk-profile-pattern", mbk("mbk-profile-pattern", "Nightly", ""), "spec.profileName"},
		{"mbk-site-pattern", mbk("mbk-site-pattern", "nightly", "DC1"), "spec.sourceSite"},
		{"mbv-ok", mbv("mbv-ok", "nightly"), ""},
		{"mbv-profile-pattern", mbv("mbv-profile-pattern", "night.ly"), "spec.profileName"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := k8sClient.Create(ctx, tc.obj)
			if tc.field == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("want an Invalid error naming %s, got %v", tc.field, err)
			}
		})
	}

	fg := newTestFG(ns)
	fg.Name = "schedule-profile"
	fg.Spec.Backup = &v1alpha1.BackupSpec{Schedules: []v1alpha1.BackupSchedule{{Name: "hourly", ProfileName: "Hourly_S3", Schedule: "0 * * * *"}}}
	if err := k8sClient.Create(ctx, fg); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "schedules[0].profileName") {
		t.Fatalf("schedule profileName pattern: got %v", err)
	}
}

// A-66, A-67 (F-50): operatorEndpoint is host:port, and the Dragonfly
// snapshot endpoint is the backup storage URL form without a path. An
// explicit empty s3EndpointURL (a template that renders "") still means AWS
// S3; a scheme-less host:port is refused.
func TestE5_EndpointShapes(t *testing.T) {
	ns := createNamespace(t, "e5-endpoints")
	snapshot := func(name string, url any) *unstructured.Unstructured {
		fg := newTestFG(ns)
		fg.Name = name
		u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
		inertFieldsSet(t, u, map[string]any{
			"enabled": true,
			"image":   "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0",
			"snapshot": map[string]any{
				"dir":           "s3://dragonfly/orders",
				"s3EndpointURL": url,
			},
		}, "spec", "dragonfly")
		return u
	}
	for _, tc := range []struct {
		name, url string
		ok        bool
	}{
		{"empty", "", true},
		{"http-port", "http://rustfs.default.svc.cluster.local:9000", true},
		{"https-no-port", "https://s3.example.com", true},
		{"trailing-slash", "https://s3.example.com/", true},
		{"no-scheme", "rustfs:9000", false},
		{"path", "https://s3.example.com/bucket", false},
		{"credentials", "https://user:pass@s3.example.com", false},
		{"query", "https://s3.example.com?x=1", false},
		{"other-scheme", "s3://bucket", false},
		{"ipv6-port", "http://[fd00::1]:9000", true},
		{"max-port", "http://rustfs:65535", true},
		// N-13: Dragonfly would get --s3_endpoint=rustfs: or rustfs:abc.
		{"empty-port", "http://rustfs:", false},
		{"non-numeric-port", "http://rustfs:abc", false},
		{"port-zero", "http://rustfs:0", false},
		{"port-too-large", "http://rustfs:65536", false},
		{"two-ports", "http://rustfs:9000:1", false},
	} {
		t.Run("s3EndpointURL-"+tc.name, func(t *testing.T) {
			u := snapshot("snap-"+tc.name, tc.url)
			err := k8sClient.Create(ctx, u)
			if tc.ok {
				if err != nil {
					t.Fatalf("s3EndpointURL %q refused: %v", tc.url, err)
				}
				stored := inertFieldsGet(t, ns, u.GetName(), "MysqlFailoverGroup")
				if got, _, _ := unstructured.NestedString(stored.Object, "spec", "dragonfly", "snapshot", "s3EndpointURL"); got != tc.url {
					t.Fatalf("stored s3EndpointURL = %q, want %q", got, tc.url)
				}
				return
			}
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.dragonfly.snapshot.s3EndpointURL") {
				t.Fatalf("s3EndpointURL %q: want an Invalid error naming it, got %v", tc.url, err)
			}
		})
	}

	// The removed fields are pruned.
	u := snapshot("snap-removed", "http://rustfs:9000")
	inertFieldsSet(t, u, "rustfs:9000", "spec", "dragonfly", "snapshot", "s3Endpoint")
	inertFieldsSet(t, u, false, "spec", "dragonfly", "snapshot", "s3UseHTTPS")
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	stored := inertFieldsGet(t, ns, "snap-removed", "MysqlFailoverGroup")
	inertFieldsAbsent(t, stored, "spec", "dragonfly", "snapshot", "s3Endpoint")
	inertFieldsAbsent(t, stored, "spec", "dragonfly", "snapshot", "s3UseHTTPS")

	for _, tc := range []struct {
		name, endpoint string
		ok             bool
	}{
		{"fqdn", "bloodraven.bloodraven.svc.cluster.local:8082", true},
		{"ip", "10.0.0.7:8082", true},
		{"no-port", "bloodraven.bloodraven.svc", false},
		{"url", "http://bloodraven:8082", false},
		{"over", dnsSubdomainOfLength(249) + ":8082", false},
	} {
		t.Run("operatorEndpoint-"+tc.name, func(t *testing.T) {
			fg := newTestFG(ns)
			fg.Name = "endpoint-" + tc.name
			fg.Spec.Sidecar.OperatorEndpoint = tc.endpoint
			err := k8sClient.Create(ctx, fg)
			if tc.ok {
				if err != nil {
					t.Fatalf("operatorEndpoint %q refused: %v", tc.endpoint, err)
				}
				return
			}
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.sidecar.operatorEndpoint") {
				t.Fatalf("operatorEndpoint %q: want an Invalid error naming it, got %v", tc.endpoint, err)
			}
		})
	}
}

// WE-Q1: the dump key stays in status.artifactStorage as
// passphraseSecretKeyRef, and the MysqlBackup root rule keeps it immutable
// with the destination once the location is recorded.
func TestE5_ArtifactPassphraseSecretKeyRefImmutable(t *testing.T) {
	ns := createNamespace(t, "e5-artifact-key")
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	b := newSucceededBackup(ns, fg.Name, "nightly-s3")
	b.Name = "encrypted"
	if err := k8sClient.Create(ctx, b); err != nil {
		t.Fatal(err)
	}
	done := metav1.NewTime(time.Now())
	b.Status = v1alpha1.MysqlBackupStatus{
		Phase: v1alpha1.BackupPhaseSucceeded, CompletionTime: &done, StartTime: &done,
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{
			Type: v1alpha1.BackupStorageS3, Location: "lion/encrypted", Bucket: "bloodraven",
			PassphraseSecretKeyRef: &v1alpha1.SecretKeyRef{Name: "mysqlbackup-encrypted-dump-key-0a1b2c3d", Key: "passphrase"},
		},
	}
	if err := k8sClient.Status().Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		`{"status":{"artifactStorage":{"passphraseSecretKeyRef":{"key":"other"}}}}`,
		`{"status":{"artifactStorage":{"passphraseSecretKeyRef":{"name":"someone-else"}}}}`,
		`{"status":{"artifactStorage":{"passphraseSecretKeyRef":null}}}`,
	} {
		target := &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: b.Name, Namespace: ns}}
		err := k8sClient.Status().Patch(ctx, target, client.RawPatch(types.MergePatchType, []byte(p)))
		if err == nil || !strings.Contains(err.Error(), "status.artifactStorage cannot be changed or removed") {
			t.Fatalf("status patch %s: want the artifactStorage immutability refusal, got %v", p, err)
		}
	}

	// The reference carries the same bounds as every other one.
	other := newSucceededBackup(ns, fg.Name, "nightly-s3")
	other.Name = "bad-key"
	if err := k8sClient.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	other.Status = v1alpha1.MysqlBackupStatus{ArtifactStorage: &v1alpha1.BackupArtifactStorage{
		Location: "lion/bad-key", Bucket: "bloodraven",
		PassphraseSecretKeyRef: &v1alpha1.SecretKeyRef{Name: "dump-key", Key: "pass/phrase"},
	}}
	if err := k8sClient.Status().Update(ctx, other); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "status.artifactStorage.passphraseSecretKeyRef.key") {
		t.Fatalf("artifact passphraseSecretKeyRef.key pattern: got %v", err)
	}
}
