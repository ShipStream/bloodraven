//go:build envtest

package envtest

// Admission tests for #203 (IMPL-203 §7.3): the root transition rule that
// freezes the active PITR profile's encryption identity while it archives to
// the same storage, the bounds that keep that rule inside the CEL cost budget,
// the profiles list-map under server-side apply, and the
// status.pitrArchiveBindings schema. CI runs these on the supported floor
// (1.31) as well as the current envtest version:
//
//	go test -tags envtest ./test/envtest -run 'TestPITRBinding(CEL|Status|SSA)'

import (
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// pitrRuleMessage is a stable fragment of the root rule's message.
const pitrRuleMessage = "cannot change while that profile archives PITR binlogs to the same storage"

// pitrS3Profile is an S3 profile at prefix, optionally encrypted with the
// passphrase in Secret secretName (key left to its default).
func pitrS3Profile(name, prefix, secretName string) v1alpha1.BackupProfile {
	p := v1alpha1.BackupProfile{
		Name: name,
		Storage: v1alpha1.BackupStorage{
			Type: v1alpha1.BackupStorageS3,
			S3: &v1alpha1.S3Storage{
				Bucket:            "archive",
				Prefix:            prefix,
				EndpointURL:       "http://rustfs.storage.svc:9000",
				CredentialsSecret: "s3-creds",
			},
		},
	}
	if secretName != "" {
		p.Encryption = &v1alpha1.BackupEncryptionSpec{
			PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: secretName},
		}
	}
	return p
}

// newPITRBindingFG is a valid group whose active PITR profile "a" archives,
// encrypted, to s3://archive/p1, with a second dump-only profile "b".
func newPITRBindingFG(ns, name string) *v1alpha1.MysqlFailoverGroup {
	fg := newEncryptionFG(ns, name)
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		Profiles: []v1alpha1.BackupProfile{
			pitrS3Profile("a", "p1", "pitr-key"),
			pitrS3Profile("b", "dumps", "dump-key"),
		},
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"},
	}
	return fg
}

func pitrProfile(fg *v1alpha1.MysqlFailoverGroup, name string) *v1alpha1.BackupProfile {
	for i := range fg.Spec.Backup.Profiles {
		if fg.Spec.Backup.Profiles[i].Name == name {
			return &fg.Spec.Backup.Profiles[i]
		}
	}
	return nil
}

// pitrUpdate creates fg (after setup), then applies edit to a fresh copy
// and returns the Update error.
func pitrUpdate(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, edit func(*v1alpha1.MysqlFailoverGroup)) error {
	t.Helper()
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create %s: %v", fg.Name, err)
	}
	cur := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), cur); err != nil {
		t.Fatalf("get %s: %v", fg.Name, err)
	}
	edit(cur)
	return k8sClient.Update(ctx, cur)
}

func requireRuleRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("API server accepted an encryption change on the active PITR profile with the destination unchanged")
	}
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), pitrRuleMessage) {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// TestPITRBindingCELInstalled proves every CRD installed (the root rule fits
// the CEL cost budget on this API server version) and that the rule is on the
// MysqlFailoverGroup kind root, not on the shared spec (so MSC templates are
// exempt).
func TestPITRBindingCELInstalled(t *testing.T) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := dc.ServerVersion(); err == nil {
		t.Logf("API server %s", v.GitVersion)
	}

	crdGVK := schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}
	for _, name := range []string{
		"mysqlfailovergroups.shipstream.io",
		"mysqlbackupverifications.shipstream.io",
		"mysqlstandbyclusters.shipstream.io",
		"mysqlbackups.shipstream.io",
	} {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(crdGVK)
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, crd); err != nil {
			t.Fatalf("get CRD %s: %v", name, err)
		}
		conds, _, _ := unstructured.NestedSlice(crd.Object, "status", "conditions")
		established := false
		for _, c := range conds {
			m, _ := c.(map[string]any)
			if m["type"] == "Established" && m["status"] == "True" {
				established = true
			}
		}
		if !established {
			t.Errorf("CRD %s is not Established: %v", name, conds)
		}
	}

	rootRule := func(name string) bool {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(crdGVK)
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, crd); err != nil {
			t.Fatalf("get CRD %s: %v", name, err)
		}
		versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
		for _, v := range versions {
			rules, _, _ := unstructured.NestedSlice(v.(map[string]any), "schema", "openAPIV3Schema", "x-kubernetes-validations")
			for _, r := range rules {
				if msg, _ := r.(map[string]any)["message"].(string); strings.Contains(msg, pitrRuleMessage) {
					return true
				}
			}
		}
		return false
	}
	if !rootRule("mysqlfailovergroups.shipstream.io") {
		t.Error("the PITR encryption transition rule is not on the MysqlFailoverGroup root")
	}
	if rootRule("mysqlstandbyclusters.shipstream.io") {
		t.Error("the PITR encryption transition rule is on the MysqlStandbyCluster root; templates must be exempt")
	}
}

// TestPITRBindingCELRefusesEncryptionChange: matrix rows 1 and 3. Every edit
// of the active profile's encryption identity is refused while PITR stays on,
// the profile name is unchanged and the destination is the same.
func TestPITRBindingCELRefusesEncryptionChange(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-refuse")

	cases := []struct {
		name  string
		setup func(*v1alpha1.MysqlFailoverGroup)
		edit  func(*v1alpha1.MysqlFailoverGroup)
	}{
		{
			name: "remove-encryption",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) { pitrProfile(fg, "a").Encryption = nil },
		},
		{
			name:  "add-encryption",
			setup: func(fg *v1alpha1.MysqlFailoverGroup) { pitrProfile(fg, "a").Encryption = nil },
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				pitrProfile(fg, "a").Encryption = &v1alpha1.BackupEncryptionSpec{
					PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "pitr-key"},
				}
			},
		},
		{
			name: "selector-name",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				pitrProfile(fg, "a").Encryption.PassphraseSecret.Name = "pitr-key-copy"
			},
		},
		{
			name: "selector-key",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				pitrProfile(fg, "a").Encryption.PassphraseSecret.Key = "other"
			},
		},
		{
			// Credentials and region are access fields, not identity: an
			// edit of them alongside does not open the rule.
			name: "selector-name-with-access-edit",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption.PassphraseSecret.Name = "pitr-key-copy"
				a.Storage.S3.CredentialsSecret = "s3-creds-2"
				a.Storage.S3.Region = "us-east-1"
			},
		},
		{
			name: "pvc-remove-encryption",
			setup: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Storage = v1alpha1.BackupStorage{
					Type: v1alpha1.BackupStoragePVC,
					PVC:  &v1alpha1.PVCStorage{ClaimName: "archive", SubPath: "p1"},
				}
			},
			edit: func(fg *v1alpha1.MysqlFailoverGroup) { pitrProfile(fg, "a").Encryption = nil },
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := newPITRBindingFG(ns, fmt.Sprintf("refuse-%d", i))
			if tc.setup != nil {
				tc.setup(fg)
			}
			requireRuleRefusal(t, pitrUpdate(t, fg, tc.edit))
		})
	}
}

// TestPITRBindingCELAllowedEdits: the rule bites only on the active profile,
// with PITR on in both objects, under the same profile name and the same
// destination; T-1 makes omitted defaults equal their explicit values.
func TestPITRBindingCELAllowedEdits(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-allow")

	cases := []struct {
		name  string
		setup func(*v1alpha1.MysqlFailoverGroup)
		edit  func(*v1alpha1.MysqlFailoverGroup)
	}{
		{
			name: "remove-encryption-with-prefix-change",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption = nil
				a.Storage.S3.Prefix = "p2"
			},
		},
		{
			name: "selector-with-bucket-change",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption.PassphraseSecret.Name = "other-key"
				a.Storage.S3.Bucket = "archive-2"
			},
		},
		{
			name: "selector-with-endpoint-change",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption.PassphraseSecret.Key = "k2"
				a.Storage.S3.EndpointURL = "http://rustfs.storage.svc.cluster.local:9000"
			},
		},
		{
			name: "s3-to-pvc-with-encryption-removed",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption = nil
				a.Storage = v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{ClaimName: "archive"}}
			},
		},
		{
			name: "pvc-subpath-change",
			setup: func(fg *v1alpha1.MysqlFailoverGroup) {
				pitrProfile(fg, "a").Storage = v1alpha1.BackupStorage{
					Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{ClaimName: "archive", SubPath: "p1"},
				}
			},
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption.PassphraseSecret.Name = "other-key"
				a.Storage.PVC.SubPath = "p2"
			},
		},
		{
			name: "pvc-claim-change",
			setup: func(fg *v1alpha1.MysqlFailoverGroup) {
				pitrProfile(fg, "a").Storage = v1alpha1.BackupStorage{
					Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{ClaimName: "archive", SubPath: "p1"},
				}
			},
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Encryption = nil
				a.Storage.PVC.ClaimName = "archive-2"
			},
		},
		{
			name: "non-pitr-profile",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				b := pitrProfile(fg, "b")
				b.Encryption.PassphraseSecret.Name = "dump-key-2"
				b.Encryption.PassphraseSecret.Key = "k2"
			},
		},
		{
			name: "non-pitr-profile-remove-encryption",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) { pitrProfile(fg, "b").Encryption = nil },
		},
		{
			name:  "pitr-disabled-in-old",
			setup: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.Enabled = false },
			edit:  func(fg *v1alpha1.MysqlFailoverGroup) { pitrProfile(fg, "a").Encryption = nil },
		},
		{
			name: "pitr-disabled-in-new",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.PITR.Enabled = false
				pitrProfile(fg, "a").Encryption = nil
			},
		},
		{
			name:  "pitr-section-absent-in-old",
			setup: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR = nil },
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"}
				pitrProfile(fg, "a").Encryption.PassphraseSecret.Name = "other-key"
			},
		},
		{
			name: "across-profile-name-change",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.PITR.ProfileName = "b"
				pitrProfile(fg, "a").Encryption = nil
				pitrProfile(fg, "b").Encryption.PassphraseSecret.Name = "dump-key-2"
			},
		},
		{
			// Renaming the active profile (and pitr.profileName with it) is a
			// profile-name change, which the rule leaves to reconcile.
			name: "active-profile-renamed",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Name = "c"
				a.Encryption = nil
				fg.Spec.Backup.PITR.ProfileName = "c"
			},
		},
		{
			name: "access-fields-only",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Storage.S3.CredentialsSecret = "s3-creds-2"
				a.Storage.S3.Region = strings.Repeat("r", 2048)
				a.Retention = 7
			},
		},
		{
			// F-E: CEL compares the raw strings, so a spelling-only storage
			// edit (the same normalized destination) admits an encryption
			// change; reconcile refuses it (EncryptionChangeRefused /
			// KeyChangeRefused, controller TestPITRBindingSpellingOnlyStorageEditRefused).
			name: "spelling-only-prefix-with-selector-change",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Storage.S3.Prefix += "/"
				a.Encryption.PassphraseSecret.Name = "other-key"
			},
		},
		{
			name: "spelling-only-endpoint-with-encryption-removed",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				a := pitrProfile(fg, "a")
				a.Storage.S3.EndpointURL = strings.Replace(a.Storage.S3.EndpointURL, "rustfs", "RustFS", 1) + "/"
				a.Encryption = nil
			},
		},
		{
			name: "algorithm-explicit-and-key-explicit",
			edit: func(fg *v1alpha1.MysqlFailoverGroup) {
				e := pitrProfile(fg, "a").Encryption
				e.Algorithm = string(v1alpha1.EncryptionAlgorithmAES256GCM)
				e.PassphraseSecret.Key = "passphrase"
			},
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := newPITRBindingFG(ns, fmt.Sprintf("allow-%d", i))
			if tc.setup != nil {
				tc.setup(fg)
			}
			if err := pitrUpdate(t, fg, tc.edit); err != nil {
				t.Fatalf("API server refused an allowed edit: %v", err)
			}
		})
	}
}

// TestPITRBindingCELLongRegionCreate: region is outside the rule and has no
// bound; a long value must not make the object unadmittable.
func TestPITRBindingCELLongRegionCreate(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-region")
	fg := newPITRBindingFG(ns, "long-region")
	pitrProfile(fg, "a").Storage.S3.Region = strings.Repeat("r", 4096)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create with a long region: %v", err)
	}
	cur := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), cur); err != nil {
		t.Fatal(err)
	}
	pitrProfile(cur, "a").Storage.S3.Region = strings.Repeat("s", 4096)
	if err := k8sClient.Update(ctx, cur); err != nil {
		t.Fatalf("update a long region: %v", err)
	}
}

// TestPITRBindingCELAlgorithmEnum (G13): only AES-256-GCM exists, so the
// enum refuses another algorithm at creation; a transition test between two
// algorithms is impossible until a second one is added.
func TestPITRBindingCELAlgorithmEnum(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-enum")

	fg := newPITRBindingFG(ns, "bad-alg")
	pitrProfile(fg, "a").Encryption.Algorithm = "AES-128-GCM"
	err := k8sClient.Create(ctx, fg)
	if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "algorithm") {
		t.Fatalf("want an enum refusal of algorithm AES-128-GCM, got %v", err)
	}
	if strings.Contains(err.Error(), pitrRuleMessage) {
		t.Fatalf("the enum, not the transition rule, must refuse a new object: %v", err)
	}

	fg = newPITRBindingFG(ns, "good-alg")
	pitrProfile(fg, "a").Encryption.Algorithm = string(v1alpha1.EncryptionAlgorithmAES256GCM)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("explicit AES-256-GCM refused: %v", err)
	}
}

var mfgGVK = schema.GroupVersionKind{Group: "shipstream.io", Version: "v1alpha1", Kind: "MysqlFailoverGroup"}

// unstructuredPITRFG is newPITRBindingFG written without the encryption
// defaults (algorithm, passphraseSecret.key), as a GitOps tool would.
func unstructuredPITRFG(t *testing.T, ns, name string) *unstructured.Unstructured {
	t.Helper()
	typed := newPITRBindingFG(ns, name)
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetGroupVersionKind(mfgGVK)
	profiles, _, _ := unstructured.NestedSlice(u.Object, "spec", "backup", "profiles")
	for _, p := range profiles {
		pm := p.(map[string]any)
		if enc, ok := pm["encryption"].(map[string]any); ok {
			delete(enc, "algorithm")
			if sel, ok := enc["passphraseSecret"].(map[string]any); ok {
				delete(sel, "key")
			}
		}
	}
	if err := unstructured.SetNestedSlice(u.Object, profiles, "spec", "backup", "profiles"); err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(u.Object, "status")
	return u
}

// TestPITRBindingCELDefaultsOmitted (T-1): a group written with the encryption
// defaults omitted survives the operator's typed writes (which carry the
// defaults explicitly) and an unstructured write that omits them again; a real
// change is still refused.
func TestPITRBindingCELDefaultsOmitted(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-t1")
	u := unstructuredPITRFG(t, ns, "omitted")
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatalf("create with defaults omitted: %v", err)
	}
	key := types.NamespacedName{Namespace: ns, Name: "omitted"}

	// The operator's typed finalizer Update.
	typed := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, key, typed); err != nil {
		t.Fatal(err)
	}
	typed.Finalizers = append(typed.Finalizers, "shipstream.io/envtest-t1")
	if err := k8sClient.Update(ctx, typed); err != nil {
		t.Fatalf("typed finalizer Update refused: %v", err)
	}
	// An annotation Update.
	if err := k8sClient.Get(ctx, key, typed); err != nil {
		t.Fatal(err)
	}
	if typed.Annotations == nil {
		typed.Annotations = map[string]string{}
	}
	typed.Annotations["example.com/touched"] = "yes"
	if err := k8sClient.Update(ctx, typed); err != nil {
		t.Fatalf("typed annotation Update refused: %v", err)
	}
	// An unstructured Update omitting the defaults again.
	again := unstructuredPITRFG(t, ns, "omitted")
	if err := k8sClient.Get(ctx, key, typed); err != nil {
		t.Fatal(err)
	}
	again.SetResourceVersion(typed.ResourceVersion)
	again.SetFinalizers(typed.Finalizers)
	again.SetAnnotations(typed.Annotations)
	if err := k8sClient.Update(ctx, again); err != nil {
		t.Fatalf("unstructured Update with defaults omitted refused: %v", err)
	}

	// A real change is still refused.
	if err := k8sClient.Get(ctx, key, typed); err != nil {
		t.Fatal(err)
	}
	pitrProfile(typed, "a").Encryption.PassphraseSecret.Key = "other"
	requireRuleRefusal(t, k8sClient.Update(ctx, typed))

	// Clean up the finalizer so namespace teardown is not blocked.
	if err := k8sClient.Get(ctx, key, typed); err != nil {
		t.Fatal(err)
	}
	typed.Finalizers = nil
	if err := k8sClient.Update(ctx, typed); err != nil {
		t.Fatalf("remove finalizer: %v", err)
	}
}

// TestPITRBindingCELBounds: the bounds that keep the rule's static cost under
// the limit, and the list-map uniqueness the rule's loop relies on.
func TestPITRBindingCELBounds(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-bounds")

	type bound struct {
		name  string
		field string // substring expected in the refusal
		max   int
		set   func(fg *v1alpha1.MysqlFailoverGroup, n int)
	}
	ascii := func(n int) string { return strings.Repeat("a", n) }
	pvc := func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.PVCStorage {
		b := pitrProfile(fg, "b")
		b.Storage = v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{ClaimName: "c"}}
		return b.Storage.PVC
	}
	bounds := []bound{
		{"bucket", "bucket", 63, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Storage.S3.Bucket = ascii(n)
		}},
		{"endpoint", "endpointURL", 512, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Storage.S3.EndpointURL = "http://h/" + ascii(n-len("http://h/"))
		}},
		{"claim", "claimName", 253, func(fg *v1alpha1.MysqlFailoverGroup, n int) { pvc(fg).ClaimName = ascii(n) }},
		{"subpath", "subPath", 512, func(fg *v1alpha1.MysqlFailoverGroup, n int) { pvc(fg).SubPath = ascii(n) }},
		{"secret-name", "passphraseSecret.name", 253, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Encryption.PassphraseSecret.Name = ascii(n)
		}},
		{"secret-key", "passphraseSecret.key", 253, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Encryption.PassphraseSecret.Key = ascii(n)
		}},
		{"pitr-profile-name", "pitr.profileName", 63, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Name = ascii(n)
			fg.Spec.Backup.PITR.ProfileName = ascii(n)
		}},
		{"init-from-backup-decryption-name", "initFromBackup.decryption.passphraseSecret.name", 253, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
				Source:     v1alpha1.InitFromBackupSource{PVC: &v1alpha1.InitFromBackupPVCSource{ClaimName: "seed"}},
				Decryption: &v1alpha1.BackupDecryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: ascii(n)}},
			}
		}},
		{"init-from-backup-decryption-key", "initFromBackup.decryption.passphraseSecret.key", 253, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
				Source:     v1alpha1.InitFromBackupSource{PVC: &v1alpha1.InitFromBackupPVCSource{ClaimName: "seed"}},
				Decryption: &v1alpha1.BackupDecryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "k", Key: ascii(n)}},
			}
		}},
	}
	for i, b := range bounds {
		t.Run(b.name, func(t *testing.T) {
			ok := newPITRBindingFG(ns, fmt.Sprintf("bound-%d-max", i))
			b.set(ok, b.max)
			if err := k8sClient.Create(ctx, ok); err != nil {
				t.Fatalf("%s at its bound %d refused: %v", b.name, b.max, err)
			}
			over := newPITRBindingFG(ns, fmt.Sprintf("bound-%d-over", i))
			b.set(over, b.max+1)
			err := k8sClient.Create(ctx, over)
			if err == nil || !apierrors.IsInvalid(err) {
				t.Fatalf("%s over its bound %d accepted (err %v)", b.name, b.max, err)
			}
			if !strings.Contains(err.Error(), b.field) {
				t.Fatalf("%s over its bound refused for the wrong field: %v", b.name, err)
			}
		})
	}

	// U15: the destination bounds count UTF-8 bytes, as the runtime's
	// (pitrarchive D-18) do: a multibyte value within the character bound
	// but over the byte bound is refused at admission, not rendered PITR
	// disabled at reconcile.
	multibyte := func(nBytes int) string { return strings.Repeat("é", nBytes/2) + strings.Repeat("a", nBytes%2) }
	byteBounds := []bound{
		{"bucket-bytes", "bucket must be at most 63 bytes", 63, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Storage.S3.Bucket = multibyte(n)
		}},
		{"prefix-bytes", "prefix must be at most 512 bytes", 512, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Storage.S3.Prefix = multibyte(n)
		}},
		{"endpoint-bytes", "endpointURL must be at most 512 bytes", 512, func(fg *v1alpha1.MysqlFailoverGroup, n int) {
			pitrProfile(fg, "a").Storage.S3.EndpointURL = "http://h/" + multibyte(n-len("http://h/"))
		}},
		{"claim-bytes", "claimName must be at most 253 bytes", 253, func(fg *v1alpha1.MysqlFailoverGroup, n int) { pvc(fg).ClaimName = multibyte(n) }},
		{"subpath-bytes", "subPath must be at most 512 bytes", 512, func(fg *v1alpha1.MysqlFailoverGroup, n int) { pvc(fg).SubPath = multibyte(n) }},
	}
	for i, b := range byteBounds {
		t.Run(b.name, func(t *testing.T) {
			ok := newPITRBindingFG(ns, fmt.Sprintf("bytes-%d-max", i))
			b.set(ok, b.max)
			if err := k8sClient.Create(ctx, ok); err != nil {
				t.Fatalf("%s at its byte bound %d refused: %v", b.name, b.max, err)
			}
			over := newPITRBindingFG(ns, fmt.Sprintf("bytes-%d-over", i))
			b.set(over, b.max+2)
			err := k8sClient.Create(ctx, over)
			if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), b.field) {
				t.Fatalf("%s two bytes over its byte bound (fewer characters than the bound): err %v", b.name, err)
			}
		})
	}

	t.Run("pitr-profile-name-pattern", func(t *testing.T) {
		fg := newPITRBindingFG(ns, "bad-profile-name")
		fg.Spec.Backup.PITR.ProfileName = "Bad_Name"
		err := k8sClient.Create(ctx, fg)
		if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "pitr.profileName") {
			t.Fatalf("want a pattern refusal of pitr.profileName, got %v", err)
		}
	})

	t.Run("duplicate-profile-names", func(t *testing.T) {
		fg := newPITRBindingFG(ns, "dup-profiles")
		fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, pitrS3Profile("a", "p9", ""))
		err := k8sClient.Create(ctx, fg)
		if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "Duplicate value") {
			t.Fatalf("want a duplicate list-map key refusal, got %v", err)
		}
	})

	t.Run("profiles-max-items", func(t *testing.T) {
		build := func(name string, n int) *v1alpha1.MysqlFailoverGroup {
			fg := newPITRBindingFG(ns, name)
			for i := len(fg.Spec.Backup.Profiles); i < n; i++ {
				fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, pitrS3Profile(fmt.Sprintf("p%d", i), fmt.Sprintf("x%d", i), "k"))
			}
			return fg
		}
		if err := k8sClient.Create(ctx, build("profiles-32", 32)); err != nil {
			t.Fatalf("32 profiles refused: %v", err)
		}
		err := k8sClient.Create(ctx, build("profiles-33", 33))
		if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "profiles") {
			t.Fatalf("want a MaxItems refusal of 33 profiles, got %v", err)
		}
	})

	t.Run("msc-source-decryption", func(t *testing.T) {
		for _, tc := range []struct {
			name, field string
			ref         v1alpha1.PassphraseSecretRef
			ok          bool
		}{
			{"msc-dec-name-max", "", v1alpha1.PassphraseSecretRef{Name: ascii(253)}, true},
			{"msc-dec-name-over", "decryption.passphraseSecret.name", v1alpha1.PassphraseSecretRef{Name: ascii(254)}, false},
			{"msc-dec-key-over", "decryption.passphraseSecret.key", v1alpha1.PassphraseSecretRef{Name: "k", Key: ascii(254)}, false},
		} {
			msc := minimalEnvtestStandby(tc.name, ns)
			msc.Spec.Source.Decryption = &v1alpha1.BackupDecryptionSpec{PassphraseSecret: tc.ref}
			err := k8sClient.Create(ctx, msc)
			if tc.ok {
				if err != nil {
					t.Errorf("%s refused: %v", tc.name, err)
				}
				continue
			}
			if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), tc.field) {
				t.Errorf("%s: want a refusal naming %s, got %v", tc.name, tc.field, err)
			}
		}
	})
}

// TestPITRBindingCELStandbyTemplateExempt: the rule sits on the MFG kind
// root, so an MSC template's backup spec can change freely (the template is
// not a running group).
func TestPITRBindingCELStandbyTemplateExempt(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-msc")
	msc := minimalEnvtestStandby("tmpl", ns)
	msc.Spec.Template.Spec.Backup = &v1alpha1.BackupSpec{
		Profiles: []v1alpha1.BackupProfile{pitrS3Profile("a", "p1", "pitr-key")},
		PITR:     &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"},
	}
	if err := k8sClient.Create(ctx, msc); err != nil {
		t.Fatalf("create MSC: %v", err)
	}
	cur := &v1alpha1.MysqlStandbyCluster{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(msc), cur); err != nil {
		t.Fatal(err)
	}
	cur.Spec.Template.Spec.Backup.Profiles[0].Encryption = nil
	if err := k8sClient.Update(ctx, cur); err != nil {
		t.Fatalf("MSC template encryption edit refused: %v", err)
	}
}

// TestPITRBindingCELVerificationArchiveProfileName: the MBV's new
// archiveProfileName is a DNS label of at most 63 characters.
func TestPITRBindingCELVerificationArchiveProfileName(t *testing.T) {
	ns := createNamespace(t, "pitr-cel-mbv")
	for i, tc := range []struct {
		value string
		ok    bool
	}{
		{"retained-a", true},
		{strings.Repeat("a", 63), true},
		{strings.Repeat("a", 64), false},
		{"Bad_Name", false},
		{"-leading", false},
	} {
		v := &v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("mbv-%d", i)},
			Spec: v1alpha1.MysqlBackupVerificationSpec{
				FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
				ProfileName:      "nightly",
				PointInTime:      &v1alpha1.PointInTimeVerificationSpec{Mode: "latest", ArchiveProfileName: tc.value},
			},
		}
		err := k8sClient.Create(ctx, v)
		switch {
		case tc.ok && err != nil:
			t.Errorf("archiveProfileName %q refused: %v", tc.value, err)
		case !tc.ok && (err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "archiveProfileName")):
			t.Errorf("archiveProfileName %q: want a refusal, got %v", tc.value, err)
		}
	}
}

// TestPITRBindingSSAProfilesListMap (G13): profiles is a list-map, so two
// field managers co-own it entry by entry, a reorder by one keeps the other's
// entry, and the operator's typed Update of the whole spec keeps every entry.
func TestPITRBindingSSAProfilesListMap(t *testing.T) {
	ns := createNamespace(t, "pitr-ssa")
	key := types.NamespacedName{Namespace: ns, Name: "ssa"}

	applyAs := func(manager string, u *unstructured.Unstructured) {
		t.Helper()
		if err := k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
			t.Fatalf("apply as %s: %v", manager, err)
		}
	}
	profileNames := func() []string {
		t.Helper()
		got := &v1alpha1.MysqlFailoverGroup{}
		if err := k8sClient.Get(ctx, key, got); err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, p := range got.Spec.Backup.Profiles {
			names = append(names, p.Name)
		}
		return names
	}
	profileObj := func(name, prefix string) map[string]any {
		obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&v1alpha1.MysqlFailoverGroup{Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{pitrS3Profile(name, prefix, "")}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		ps, _, _ := unstructured.NestedSlice(obj, "spec", "backup", "profiles")
		return ps[0].(map[string]any)
	}
	partial := func(profiles ...map[string]any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetGroupVersionKind(mfgGVK)
		u.SetNamespace(ns)
		u.SetName(key.Name)
		list := make([]any, 0, len(profiles))
		for _, p := range profiles {
			list = append(list, p)
		}
		if err := unstructured.SetNestedSlice(u.Object, list, "spec", "backup", "profiles"); err != nil {
			t.Fatal(err)
		}
		return u
	}

	// Manager "platform" owns the whole group, with profiles a and b.
	full := unstructuredPITRFG(t, ns, key.Name)
	unstructured.RemoveNestedField(full.Object, "metadata", "creationTimestamp")
	applyAs("platform", full)
	// Manager "team" adds its own profile t.
	applyAs("team", partial(profileObj("t", "team")))
	if got := strings.Join(profileNames(), ","); got != "a,b,t" {
		t.Fatalf("after the team apply, profiles = %s, want a,b,t", got)
	}

	// The platform reorders its own entries and adds c in front; t survives.
	reordered := unstructuredPITRFG(t, ns, key.Name)
	unstructured.RemoveNestedField(reordered.Object, "metadata", "creationTimestamp")
	ps, _, _ := unstructured.NestedSlice(reordered.Object, "spec", "backup", "profiles")
	ps = append([]any{profileObj("c", "pc")}, ps[1], ps[0])
	if err := unstructured.SetNestedSlice(reordered.Object, ps, "spec", "backup", "profiles"); err != nil {
		t.Fatal(err)
	}
	applyAs("platform", reordered)
	names := profileNames()
	if !containsAll(names, "a", "b", "c", "t") || len(names) != 4 {
		t.Fatalf("after the platform reorder, profiles = %v, want a, b, c and t", names)
	}

	// The operator's typed Update of the whole spec keeps every entry.
	typed := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, key, typed); err != nil {
		t.Fatal(err)
	}
	typed.Spec.Backup.MaxLagSecondsForSource = 42
	if err := k8sClient.Update(ctx, typed, client.FieldOwner("bloodraven")); err != nil {
		t.Fatalf("typed Update: %v", err)
	}
	if names := profileNames(); !containsAll(names, "a", "b", "c", "t") || len(names) != 4 {
		t.Fatalf("after the typed Update, profiles = %v", names)
	}

	// The team re-applies without its entry: only t goes.
	applyAs("team", partial())
	if names := profileNames(); containsAll(names, "t") || !containsAll(names, "a", "b", "c") {
		t.Fatalf("after the team withdrew t, profiles = %v", names)
	}

	// An apply that changes the active profile's encryption is still refused.
	bad := unstructuredPITRFG(t, ns, key.Name)
	unstructured.RemoveNestedField(bad.Object, "metadata", "creationTimestamp")
	ps, _, _ = unstructured.NestedSlice(bad.Object, "spec", "backup", "profiles")
	delete(ps[0].(map[string]any), "encryption")
	ps = append([]any{profileObj("c", "pc")}, ps...)
	if err := unstructured.SetNestedSlice(bad.Object, ps, "spec", "backup", "profiles"); err != nil {
		t.Fatal(err)
	}
	err := k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(bad), client.FieldOwner("platform"), client.ForceOwnership)
	requireRuleRefusal(t, err)
}

func containsAll(have []string, want ...string) bool {
	set := map[string]bool{}
	for _, h := range have {
		set[h] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

func pitrBindingFixture(i int, destination string) v1alpha1.PITRArchiveBinding {
	return v1alpha1.PITRArchiveBinding{
		ID:          fmt.Sprintf("pab-%016x", i),
		Destination: destination,
		ArchiveID:   "ar1-" + strings.Repeat("a", 26),
		Source:      &v1alpha1.PITRArchiveSource{Namespace: "ns", Name: "lion", UID: "0a1b2c3d-0000-4000-8000-000000000001"},
		Encryption: v1alpha1.PITRArchiveBindingEncryption{
			Mode:          v1alpha1.PITRArchiveEncryptionPassphrase,
			Algorithm:     v1alpha1.EncryptionAlgorithmAES256GCM,
			KeySecretName: fmt.Sprintf("mysql-lion-pitr-key-%010x", i),
		},
		State:  v1alpha1.PITRArchiveBindingBound,
		Reason: "Bound",
	}
}

// TestPITRBindingStatusBounds: status.pitrArchiveBindings holds 16 entries
// with maximal destinations (no ratcheting applies to a first status write),
// refuses a 17th and a malformed id, and a status write never evaluates the
// spec rule (the status subresource ignores spec changes).
func TestPITRBindingStatusBounds(t *testing.T) {
	ns := createNamespace(t, "pitr-status")
	fg := newPITRBindingFG(ns, "status")
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(fg)

	cur := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, key, cur); err != nil {
		t.Fatal(err)
	}
	prefix := "s3://archive/"
	for i := 0; i < 16; i++ {
		dest := fmt.Sprintf("%s%02d", prefix, i)
		dest += strings.Repeat("d", 4096-len(dest))
		cur.Status.PITRArchiveBindings = append(cur.Status.PITRArchiveBindings, pitrBindingFixture(i, dest))
	}
	// A spec edit the rule would refuse rides along; the status write must
	// ignore it, not evaluate the rule against it.
	pitrProfile(cur, "a").Encryption = nil
	if err := k8sClient.Status().Update(ctx, cur); err != nil {
		t.Fatalf("status Update with 16 bindings refused: %v", err)
	}
	got := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if n := len(got.Status.PITRArchiveBindings); n != 16 {
		t.Fatalf("stored %d bindings, want 16", n)
	}
	if len(got.Status.PITRArchiveBindings[15].Destination) != 4096 {
		t.Fatalf("destination length %d, want 4096", len(got.Status.PITRArchiveBindings[15].Destination))
	}
	if pitrProfile(got, "a").Encryption == nil {
		t.Fatal("a status write changed spec")
	}

	refuse := func(name, want string, edit func(*v1alpha1.MysqlFailoverGroup)) {
		t.Helper()
		c := &v1alpha1.MysqlFailoverGroup{}
		if err := k8sClient.Get(ctx, key, c); err != nil {
			t.Fatal(err)
		}
		edit(c)
		err := k8sClient.Status().Update(ctx, c)
		if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want a refusal mentioning %q, got %v", name, want, err)
		}
	}
	refuse("17th binding", "pitrArchiveBindings", func(c *v1alpha1.MysqlFailoverGroup) {
		c.Status.PITRArchiveBindings = append(c.Status.PITRArchiveBindings, pitrBindingFixture(16, "s3://archive/16"))
	})
	refuse("bad id", "id", func(c *v1alpha1.MysqlFailoverGroup) {
		c.Status.PITRArchiveBindings[0].ID = "pab-NOTHEX"
	})
	refuse("duplicate id", "Duplicate value", func(c *v1alpha1.MysqlFailoverGroup) {
		c.Status.PITRArchiveBindings[1].ID = c.Status.PITRArchiveBindings[0].ID
	})
	refuse("over-long destination", "destination", func(c *v1alpha1.MysqlFailoverGroup) {
		c.Status.PITRArchiveBindings[0].Destination += "x"
	})
	refuse("bad state", "state", func(c *v1alpha1.MysqlFailoverGroup) {
		c.Status.PITRArchiveBindings[0].State = "Accepted"
	})
	refuse("bad archive id", "archiveID", func(c *v1alpha1.MysqlFailoverGroup) {
		c.Status.PITRArchiveBindings[0].ArchiveID = "ar1-UPPER"
	})

	// A later spec Update still runs the rule against the unchanged spec.
	if err := k8sClient.Get(ctx, key, cur); err != nil {
		t.Fatal(err)
	}
	pitrProfile(cur, "a").Encryption = nil
	requireRuleRefusal(t, k8sClient.Update(ctx, cur))
}
