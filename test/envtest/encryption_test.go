//go:build envtest

package envtest

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// newEncryptionFG returns a minimal valid group with TLS configured, so
// individual tests only have to vary spec.encryptionAtRest.
func newEncryptionFG(namespace, name string) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Image:        "mysql:9.7",
			SidecarImage: "ghcr.io/shipstream/bloodraven-sidecar:0.9.0",
			SecretName:   "mysql-credentials",
			Sites: []v1alpha1.SiteSpec{
				{
					Name: "dc1", Zone: "lion-dc1", LBIP: "203.0.113.1",
					TaintNodeSelector: map[string]string{"shipstream.io/site.lion": "dc1"},
					Storage:           v1alpha1.StorageSpec{StorageClassName: "standard", Size: resource.MustParse("10Gi")},
				},
				{
					Name: "dc2", Zone: "lion-dc2", LBIP: "203.0.113.2",
					TaintNodeSelector: map[string]string{"shipstream.io/site.lion": "dc2"},
					Storage:           v1alpha1.StorageSpec{StorageClassName: "standard", Size: resource.MustParse("10Gi")},
				},
			},
			DNS: v1alpha1.DNSSpec{Hostname: "lion.az.example.com", TTL: 60},
			TLS: &v1alpha1.TLSSpec{
				SecretName: "mysql-tls",
				IssuerRef:  v1alpha1.IssuerRef{Name: "ca", Kind: "Issuer"},
			},
		},
	}
}

// TestEncryptionRequiresTLS exercises the CEL rule, which only runs
// against a real API server. MySQL mandates a secure connection when
// cloning encrypted data and Bloodraven bootstraps every replica with
// CLONE INSTANCE, so accepting encryption without TLS would produce a
// group that can never bring up a second site.
func TestEncryptionRequiresTLS(t *testing.T) {
	ns := createNamespace(t, "enc-tls")

	fg := newEncryptionFG(ns, "no-tls")
	fg.Spec.TLS = nil
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}

	err := k8sClient.Create(ctx, fg)
	if err == nil {
		t.Fatal("API server accepted encryptionAtRest.enabled without spec.tls")
	}
	if !strings.Contains(err.Error(), "requires spec.tls") {
		t.Errorf("unexpected rejection reason: %v", err)
	}
}

func TestEncryptionDisabledWithoutTLSIsAccepted(t *testing.T) {
	// The rule must only bite when encryption is actually on — an
	// existing group with no TLS and no encryption must still apply.
	ns := createNamespace(t, "enc-tls-off")

	fg := newEncryptionFG(ns, "off")
	fg.Spec.TLS = nil
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: false}

	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("disabled encryption without TLS should be accepted: %v", err)
	}
}

// TestKeyringDataFileDirRejectedInsideDataDir guards the requirement
// MySQL documents explicitly: the keyring data file must not live in the
// data directory. Co-locating them would also defeat the whole feature —
// a stolen PVC would carry both the ciphertext and the key.
func TestKeyringDataFileDirRejectedInsideDataDir(t *testing.T) {
	ns := createNamespace(t, "enc-datadir")

	for _, bad := range []string{"/var/lib/mysql", "/var/lib/mysql/keys"} {
		fg := newEncryptionFG(ns, "bad"+strings.ReplaceAll(bad, "/", "-"))
		fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{
			Enabled: true,
			Keyring: &v1alpha1.KeyringSpec{DataFileDir: bad},
		}
		err := k8sClient.Create(ctx, fg)
		if err == nil {
			t.Errorf("API server accepted keyring dataFileDir %q inside the data directory", bad)
			continue
		}
		if !strings.Contains(err.Error(), "data directory") {
			t.Errorf("dataFileDir %q rejected for the wrong reason: %v", bad, err)
		}
	}
}

func TestKeyringDefaultsAppliedByAPIServer(t *testing.T) {
	ns := createNamespace(t, "enc-defaults")

	fg := newEncryptionFG(ns, "defaults")
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{
		Enabled: true,
		Keyring: &v1alpha1.KeyringSpec{},
	}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create: %v", err)
	}

	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "defaults"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	kr := got.Spec.EncryptionAtRest.Keyring
	if kr.DataFileDir != v1alpha1.DefaultKeyringDataFileDir {
		t.Errorf("dataFileDir = %q, want the CRD default %q", kr.DataFileDir, v1alpha1.DefaultKeyringDataFileDir)
	}
	if kr.MysqldDir != v1alpha1.DefaultKeyringMysqldDir {
		t.Errorf("mysqldDir = %q", kr.MysqldDir)
	}
	if kr.PluginDir != v1alpha1.DefaultKeyringPluginDir {
		t.Errorf("pluginDir = %q", kr.PluginDir)
	}
	if kr.RetainVersions != v1alpha1.DefaultKeyringRetainVersions {
		t.Errorf("retainVersions = %d", kr.RetainVersions)
	}
	// The Go-side helper must agree with what admission produced,
	// otherwise the operator would render one path and validate another.
	if eff := got.Spec.EffectiveKeyring(); eff != *kr {
		t.Errorf("EffectiveKeyring() = %+v, admission produced %+v", eff, *kr)
	}
}

func TestKeyringRetainVersionsBounds(t *testing.T) {
	ns := createNamespace(t, "enc-retain")

	// Keeping only one version would leave no rollback target if a
	// rotation produced a bad keyring.
	fg := newEncryptionFG(ns, "too-few")
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{
		Enabled: true,
		Keyring: &v1alpha1.KeyringSpec{RetainVersions: 1},
	}
	if err := k8sClient.Create(ctx, fg); err == nil {
		t.Error("retainVersions=1 should be rejected")
	}

	fg2 := newEncryptionFG(ns, "too-many")
	fg2.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{
		Enabled: true,
		Keyring: &v1alpha1.KeyringSpec{RetainVersions: 500},
	}
	if err := k8sClient.Create(ctx, fg2); err == nil {
		t.Error("retainVersions=500 should be rejected")
	}
}

// TestEncryptionStatusRoundTrips proves the status subresource actually
// persists the per-site keyring state. The rendering decision (sealed vs
// unsealed) is read back off status on every reconcile, so a schema
// mistake here would silently make every site render unsealed forever.
func TestEncryptionStatusRoundTrips(t *testing.T) {
	ns := createNamespace(t, "enc-status")

	fg := newEncryptionFG(ns, "status")
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create: %v", err)
	}

	now := metav1.Now()
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sealed: true,
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name:           "dc1",
			Phase:          v1alpha1.KeyringPhaseSealed,
			KeyringSecret:  "mysql-status-dc1-keyring-v3",
			KeyringVersion: 3,
			KeyringDigest:  "sha256:abc123",
			LastEscrowTime: &now,
			Coverage: &v1alpha1.SiteEncryptionCoverage{
				KeyringComponent:          "component_keyring_file",
				KeyringReadOnly:           true,
				SystemTablespaceEncrypted: true,
				UnencryptedTablespaces:    7,
				RedoLogEncrypted:          true,
				UndoLogEncrypted:          true,
				BinlogEncrypted:           true,
				LastCheckTime:             &now,
			},
		}},
	}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("status update: %v", err)
	}

	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "status"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	st := got.Status.EncryptionAtRest
	if st == nil || !st.Sealed || len(st.Sites) != 1 {
		t.Fatalf("status did not round-trip: %+v", st)
	}
	s := st.Sites[0]
	if s.Phase != v1alpha1.KeyringPhaseSealed || s.KeyringVersion != 3 || s.KeyringDigest != "sha256:abc123" {
		t.Errorf("site status = %+v", s)
	}
	if s.Coverage == nil || s.Coverage.UnencryptedTablespaces != 7 || !s.Coverage.KeyringReadOnly {
		t.Errorf("coverage = %+v", s.Coverage)
	}
	if !got.SiteKeyringSealed("dc1") {
		t.Error("a Sealed site read back from the API server must render sealed")
	}
}

func TestInvalidKeyringPhaseRejected(t *testing.T) {
	// The phase enum gates the rendering switch; an unknown value must
	// not be storable.
	ns := createNamespace(t, "enc-phase")

	fg := newEncryptionFG(ns, "phase")
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create: %v", err)
	}
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{Name: "dc1", Phase: v1alpha1.SiteKeyringPhase("Bogus")}},
	}
	if err := k8sClient.Status().Update(ctx, fg); err == nil {
		t.Error("an unknown keyring phase should be rejected by the CRD enum")
	}
}

// createNamespace creates a fresh namespace for a test and returns its
// name. Kept local to this file so it does not collide with helpers in
// the other envtest files (go test compiles the package as one unit).
func createNamespace(t *testing.T, prefix string) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix + "-"}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return ns.Name
}
