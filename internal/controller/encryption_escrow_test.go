package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func newEscrowStore(objs ...client.Object) (*keyringEscrowStore, client.Client) {
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(objs...).Build()
	return &keyringEscrowStore{client: c, scheme: scheme}, c
}

func TestEscrowStore_PutCreatesVersionOne(t *testing.T) {
	fg := encTestFG()
	store, c := newEscrowStore(fg)
	ctx := context.Background()

	v, err := store.put(ctx, fg, "dc1", []byte("keyring-bytes"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if v.Version != 1 {
		t.Errorf("first version = %d, want 1", v.Version)
	}
	if v.Name != "mysql-lion-dc1-keyring-v1" {
		t.Errorf("secret name = %q", v.Name)
	}
	if v.Digest != keyringDigest([]byte("keyring-bytes")) {
		t.Errorf("digest = %q", v.Digest)
	}

	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: v.Name}, &sec); err != nil {
		t.Fatalf("secret not created: %v", err)
	}
	// Immutability is what stops a later bug or a replayed token from
	// rewriting the keyring a running MySQL is already sealed against.
	if sec.Immutable == nil || !*sec.Immutable {
		t.Error("escrow Secret must be immutable")
	}
	if string(sec.Data[v1alpha1.KeyringDataFileName]) != "keyring-bytes" {
		t.Errorf("stored bytes = %q", sec.Data[v1alpha1.KeyringDataFileName])
	}
	if len(sec.OwnerReferences) == 0 {
		t.Error("escrow Secret must be owned by the failover group so it is garbage-collected with it")
	}
}

func TestEscrowStore_PutIsIdempotent(t *testing.T) {
	// The sidecar retries until the operator confirms. A retry must not
	// mint a new version on every attempt, or retention would churn
	// through the whole history in seconds.
	fg := encTestFG()
	store, _ := newEscrowStore(fg)
	ctx := context.Background()

	first, err := store.put(ctx, fg, "dc1", []byte("same"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	second, err := store.put(ctx, fg, "dc1", []byte("same"))
	if err != nil {
		t.Fatalf("put retry: %v", err)
	}
	if first.Version != second.Version || first.Name != second.Name {
		t.Errorf("retry minted a new version: %+v then %+v", first, second)
	}
}

func TestEscrowStore_PutIncrementsOnChange(t *testing.T) {
	fg := encTestFG()
	store, _ := newEscrowStore(fg)
	ctx := context.Background()

	if _, err := store.put(ctx, fg, "dc1", []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	second, err := store.put(ctx, fg, "dc1", []byte("v2"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if second.Version != 2 {
		t.Errorf("version = %d, want 2", second.Version)
	}

	cur, ok, err := store.current(ctx, fg, "dc1")
	if err != nil || !ok {
		t.Fatalf("current: %v ok=%v", err, ok)
	}
	if cur.Version != 2 || string(cur.Bytes) != "v2" {
		t.Errorf("current = %+v, want the newest version", cur)
	}
}

func TestEscrowStore_CurrentEmpty(t *testing.T) {
	fg := encTestFG()
	store, _ := newEscrowStore(fg)
	_, ok, err := store.current(context.Background(), fg, "dc1")
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if ok {
		t.Error("a site that never escrowed must report no current version")
	}
}

func TestEscrowStore_VersionsAreSiteScoped(t *testing.T) {
	fg := encTestFG()
	store, _ := newEscrowStore(fg)
	ctx := context.Background()

	if _, err := store.put(ctx, fg, "dc1", []byte("dc1-keys")); err != nil {
		t.Fatalf("put dc1: %v", err)
	}
	// Each site has its own independent keyring — a clone recipient
	// rewraps under its own master key — so dc2 must start at v1, not
	// inherit dc1's numbering or bytes.
	v, err := store.put(ctx, fg, "dc2", []byte("dc2-keys"))
	if err != nil {
		t.Fatalf("put dc2: %v", err)
	}
	if v.Version != 1 {
		t.Errorf("dc2 first version = %d, want 1", v.Version)
	}
	cur, _, _ := store.current(ctx, fg, "dc1")
	if string(cur.Bytes) != "dc1-keys" {
		t.Errorf("dc1 keyring leaked across sites: %q", cur.Bytes)
	}
}

func TestEscrowStore_Prune(t *testing.T) {
	fg := encTestFG()
	fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{RetainVersions: 2}
	store, c := newEscrowStore(fg)
	ctx := context.Background()

	for _, b := range []string{"a", "b", "c", "d"} {
		if _, err := store.put(ctx, fg, "dc1", []byte(b)); err != nil {
			t.Fatalf("put %s: %v", b, err)
		}
	}
	if err := store.prune(ctx, fg, "dc1", "mysql-lion-dc1-keyring-v4"); err != nil {
		t.Fatalf("prune: %v", err)
	}

	versions, err := store.listVersions(ctx, fg, "dc1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(versions) != 2 {
		t.Errorf("retained %d versions, want 2: %+v", len(versions), versions)
	}
	for _, v := range versions {
		if v.Version < 3 {
			t.Errorf("pruning kept an old version: v%d", v.Version)
		}
	}

	var gone corev1.Secret
	err = c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: "mysql-lion-dc1-keyring-v1"}, &gone)
	if err == nil {
		t.Error("v1 should have been pruned")
	}
}

func TestEscrowStore_PruneNeverDeletesTheInUseVersion(t *testing.T) {
	// Deleting the Secret a live MySQL is sealed against would make that
	// site unrecoverable on its next pod restart.
	fg := encTestFG()
	fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{RetainVersions: 2}
	store, c := newEscrowStore(fg)
	ctx := context.Background()

	for _, b := range []string{"a", "b", "c", "d"} {
		if _, err := store.put(ctx, fg, "dc1", []byte(b)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// Pin the OLDEST version as in-use, which retention would otherwise
	// reach.
	if err := store.prune(ctx, fg, "dc1", "mysql-lion-dc1-keyring-v1"); err != nil {
		t.Fatalf("prune: %v", err)
	}
	var pinned corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: fg.Namespace, Name: "mysql-lion-dc1-keyring-v1",
	}, &pinned); err != nil {
		t.Fatalf("in-use version was pruned: %v", err)
	}
}

func TestEscrowStore_SkipsSecretsWithoutKeyringData(t *testing.T) {
	fg := encTestFG()
	corrupt := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql-lion-dc1-keyring-v7",
			Namespace: fg.Namespace,
			Labels: map[string]string{
				labelAppName:        "mysql-keyring",
				labelFailoverGroup:  fg.Name,
				labelSite:           "dc1",
				labelManagedBy:      managerName,
				labelKeyringVersion: "7",
			},
		},
		Data: map[string][]byte{},
	}
	store, _ := newEscrowStore(fg, corrupt)

	// One malformed Secret must not wedge the site: it is skipped, and
	// the next escrow still lands.
	versions, err := store.listVersions(context.Background(), fg, "dc1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("empty-data secret should be skipped, got %+v", versions)
	}
}

// --- tokens ---------------------------------------------------------

func TestEnsureEscrowToken_MintsAndIsStable(t *testing.T) {
	fg := encTestFG()
	r, c := newReconciler(fg)
	ctx := context.Background()

	if err := r.ensureEscrowToken(ctx, fg, "dc1"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	var sec corev1.Secret
	name := v1alpha1.KeyringTokenSecretName(fg.Name, "dc1")
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &sec); err != nil {
		t.Fatalf("token not created: %v", err)
	}
	first := string(sec.Data[v1alpha1.KeyringTokenKey])
	if len(first) < 32 {
		t.Errorf("token too short: %d bytes", len(first))
	}

	// Re-running must not rotate the token out from under a running pod
	// that already mounted it.
	if err := r.ensureEscrowToken(ctx, fg, "dc1"); err != nil {
		t.Fatalf("ensure again: %v", err)
	}
	_ = c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &sec)
	if string(sec.Data[v1alpha1.KeyringTokenKey]) != first {
		t.Error("token must be stable across reconciles")
	}
}

func TestEnsureEscrowToken_ReplacesTruncatedToken(t *testing.T) {
	fg := encTestFG()
	bad := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      v1alpha1.KeyringTokenSecretName(fg.Name, "dc1"),
			Namespace: fg.Namespace,
		},
		Data: map[string][]byte{v1alpha1.KeyringTokenKey: []byte("short")},
	}
	r, c := newReconciler(fg, bad)
	ctx := context.Background()

	if err := r.ensureEscrowToken(ctx, fg, "dc1"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	var sec corev1.Secret
	_ = c.Get(ctx, types.NamespacedName{
		Namespace: fg.Namespace, Name: v1alpha1.KeyringTokenSecretName(fg.Name, "dc1"),
	}, &sec)
	if len(sec.Data[v1alpha1.KeyringTokenKey]) < 32 {
		t.Error("a malformed token must be re-minted, not left in place")
	}
}

func TestVerifyEscrowToken(t *testing.T) {
	fg := encTestFG()
	r, c := newReconciler(fg)
	ctx := context.Background()
	if err := r.ensureEscrowToken(ctx, fg, "dc1"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	var sec corev1.Secret
	_ = c.Get(ctx, types.NamespacedName{
		Namespace: fg.Namespace, Name: v1alpha1.KeyringTokenSecretName(fg.Name, "dc1"),
	}, &sec)
	good := string(sec.Data[v1alpha1.KeyringTokenKey])

	if err := verifyEscrowToken(ctx, c, fg.Namespace, fg.Name, "dc1", good); err != nil {
		t.Errorf("valid token rejected: %v", err)
	}
	if err := verifyEscrowToken(ctx, c, fg.Namespace, fg.Name, "dc1", good+"x"); err == nil {
		t.Error("wrong token accepted")
	}
	if err := verifyEscrowToken(ctx, c, fg.Namespace, fg.Name, "dc1", ""); err == nil {
		t.Error("empty token accepted")
	}
	// A token issued for one site must not work for another.
	if err := verifyEscrowToken(ctx, c, fg.Namespace, fg.Name, "dc2", good); err == nil {
		t.Error("dc1's token was accepted for dc2")
	}
}

func TestBearerToken(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"Bearer abc", "abc"},
		{"bearer abc", "abc"},
		{"BEARER  abc ", "abc"},
		{"abc", ""},
		{"Bearer", ""},
		{"", ""},
		{"Basic abc", ""},
	} {
		if got := bearerToken(tc.in); got != tc.want {
			t.Errorf("bearerToken(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
