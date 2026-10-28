package controller

import (
	"bytes"
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestEnsureDigestKeyCreatesOnceAndReplacesMalformed(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg.DeepCopy()).Build()

	first, err := ensureDigestKey(ctx, c, nil, testScheme(), fg)
	if err != nil {
		t.Fatalf("create digest key: %v", err)
	}
	if len(first) != digestKeyBytes {
		t.Fatalf("digest key length = %d, want %d", len(first), digestKeyBytes)
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: digestKeySecretName(fg.Name)}
	var sec corev1.Secret
	if err := c.Get(ctx, nn, &sec); err != nil {
		t.Fatalf("get digest key secret: %v", err)
	}
	if ref := metav1.GetControllerOf(&sec); ref == nil || ref.Name != fg.Name {
		t.Fatalf("digest key secret is not owned by the group: %v", sec.OwnerReferences)
	}

	again, err := ensureDigestKey(ctx, c, nil, testScheme(), fg)
	if err != nil {
		t.Fatalf("reuse digest key: %v", err)
	}
	if !bytes.Equal(first, again) {
		t.Fatal("digest key was regenerated although a valid one exists")
	}

	sec.Data[digestKeyDataKey] = []byte("short")
	if err := c.Update(ctx, &sec); err != nil {
		t.Fatalf("truncate digest key: %v", err)
	}
	replaced, err := ensureDigestKey(ctx, c, nil, testScheme(), fg)
	if err != nil {
		t.Fatalf("replace malformed digest key: %v", err)
	}
	if len(replaced) != digestKeyBytes || bytes.Equal(replaced, first) {
		t.Fatal("malformed digest key was not replaced with a fresh one")
	}
}

func TestSecretContentDigestIsKeyedAndContentSensitive(t *testing.T) {
	keyA := bytes.Repeat([]byte{1}, digestKeyBytes)
	keyB := bytes.Repeat([]byte{2}, digestKeyBytes)
	data := map[string]map[string][]byte{"cred.app": {"password": []byte("hunter2")}}

	if secretContentDigest(keyA, data) == secretContentDigest(keyB, data) {
		t.Fatal("digest does not depend on the key")
	}
	changed := map[string]map[string][]byte{"cred.app": {"password": []byte("hunter3")}}
	if secretContentDigest(keyA, data) == secretContentDigest(keyA, changed) {
		t.Fatal("digest does not depend on the Secret contents")
	}
	// Field boundaries are length-prefixed, so moving bytes between the
	// key name and the value is a different input.
	shifted := map[string]map[string][]byte{"cred.app": {"passwordh": []byte("unter2")}}
	if secretContentDigest(keyA, data) == secretContentDigest(keyA, shifted) {
		t.Fatal("digest collides when bytes move between a key and its value")
	}
	if secretContentDigest(keyA, nil) != "" {
		t.Fatal("digest of no Secrets should be empty")
	}
}

func TestDeploymentSpecHashNeedsDigestKey(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	fg.Spec.TLS = &v1alpha1.TLSSpec{
		IssuerRef:  v1alpha1.IssuerRef{Name: "letsencrypt", Kind: "ClusterIssuer"},
		SecretName: "mysql-tls",
	}
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-tls", Namespace: fg.Namespace},
		Data:       map[string][]byte{"tls.crt": []byte("cert"), "tls.key": []byte("key"), "ca.crt": []byte("ca")},
	}
	r, c := newReconciler(fg, tlsSecret)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	site := fg.Spec.Sites[0]
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name)}, &deploy); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	published := deploy.Annotations[specHashAnnotation]

	inputs, err := specSecretInputs(ctx, c, fg)
	if err != nil {
		t.Fatalf("spec secret inputs: %v", err)
	}
	if len(inputs) == 0 {
		t.Fatal("expected the TLS Secret among the spec hash inputs")
	}
	if published == ComputeSpecHash(fg, site, secretContentDigest(make([]byte, digestKeyBytes), inputs)) {
		t.Fatal("published spec-hash is reproducible without the group's digest key")
	}
	key, err := readDigestKey(ctx, c, fg)
	if err != nil {
		t.Fatalf("read digest key: %v", err)
	}
	if published != ComputeSpecHash(fg, site, secretContentDigest(key, inputs)) {
		t.Fatal("published spec-hash does not match the keyed computation")
	}
}
