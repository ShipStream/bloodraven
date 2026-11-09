//go:build envtest

package envtest

import (
	"log/slog"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// readSpecField reads one string field of an object's stored spec, and
// the stored generation, without the typed round trip that would
// canonicalize it.
func readSpecField(t *testing.T, gvk, ns, name string, path ...string) (string, int64) {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind(gvk)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, u); err != nil {
		t.Fatal(err)
	}
	v, _, err := unstructured.NestedString(u.Object, append([]string{"spec"}, path...)...)
	if err != nil {
		t.Fatal(err)
	}
	return v, u.GetGeneration()
}

// TestTypedUpdateCanonicalizesUserSpellings (K033) pins why the operator
// writes metadata with a merge patch: a typed Get, a metadata-only edit
// and a full Update re-serialize the whole spec, so a user's `2h` becomes
// `2h0m0s` and metadata.generation moves, which a GitOps tool then reports
// as drift. The same edit as a merge patch from the typed object carries
// only the metadata.
func TestTypedUpdateCanonicalizesUserSpellings(t *testing.T) {
	ns := createNamespace(t, "canonical-spellings")
	fg := newTestFG(ns)
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fg)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("MysqlFailoverGroup")
	u.SetAnnotations(map[string]string{"example.com/one-shot": "a", "example.com/other": "b"})
	if err := unstructured.SetNestedField(u.Object, "2h", "spec", "cloneTimeout"); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, u) })
	stored, gen := readSpecField(t, "MysqlFailoverGroup", ns, fg.Name, "cloneTimeout")
	if stored != "2h" {
		t.Fatalf("setup: stored cloneTimeout %q", stored)
	}

	// The merge patch the operator uses keeps the user's spelling and
	// the generation.
	var typed v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &typed); err != nil {
		t.Fatal(err)
	}
	base := typed.DeepCopy()
	delete(typed.Annotations, "example.com/one-shot")
	if err := k8sClient.Patch(ctx, &typed, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		t.Fatal(err)
	}
	if got, g := readSpecField(t, "MysqlFailoverGroup", ns, fg.Name, "cloneTimeout"); got != "2h" || g != gen {
		t.Fatalf("after a metadata merge patch: cloneTimeout %q generation %d, want %q and %d", got, g, "2h", gen)
	}

	// A full Update of the same typed object rewrites the spec.
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &typed); err != nil {
		t.Fatal(err)
	}
	delete(typed.Annotations, "example.com/other")
	if err := k8sClient.Update(ctx, &typed); err != nil {
		t.Fatal(err)
	}
	if got, g := readSpecField(t, "MysqlFailoverGroup", ns, fg.Name, "cloneTimeout"); got != "2h0m0s" || g != gen+1 {
		t.Fatalf("after a full Update: cloneTimeout %q generation %d; the API no longer canonicalizes on a typed Update (want %q and %d), revisit K033",
			got, g, "2h0m0s", gen+1)
	}
}

// TestVerificationFinalizerKeepsUserSpellings (K033): the verification
// reconciler's first pass (finalizer and labels) leaves an offset
// stopTime and a `1m` timeout as written, and the generation unchanged.
func TestVerificationFinalizerKeepsUserSpellings(t *testing.T) {
	ns := createNamespace(t, "canonical-verification")
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       "MysqlBackupVerification",
		"metadata":   map[string]any{"name": "spelled", "namespace": ns},
		"spec": map[string]any{
			"failoverGroupRef": map[string]any{"name": "lion"},
			"profileName":      "nightly-s3",
			"pointInTime":      map[string]any{"mode": "Timestamp", "stopTime": "2026-04-15T09:30:00+02:00"},
			"sanityCheck":      map[string]any{"query": "SELECT 1", "timeout": "1m"},
		},
	}}
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var v v1alpha1.MysqlBackupVerification
		if k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "spelled"}, &v) == nil {
			base := v.DeepCopy()
			v.Finalizers = nil
			_ = k8sClient.Patch(ctx, &v, client.MergeFrom(base))
			_ = k8sClient.Delete(ctx, &v)
		}
	})
	_, gen := readSpecField(t, "MysqlBackupVerification", ns, "spelled", "pointInTime", "stopTime")

	r := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "spelled"}}); err != nil {
		t.Fatal(err)
	}
	var v v1alpha1.MysqlBackupVerification
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "spelled"}, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Finalizers) == 0 {
		t.Fatal("setup: the first pass added no finalizer")
	}
	stop, g := readSpecField(t, "MysqlBackupVerification", ns, "spelled", "pointInTime", "stopTime")
	timeout, _ := readSpecField(t, "MysqlBackupVerification", ns, "spelled", "sanityCheck", "timeout")
	if stop != "2026-04-15T09:30:00+02:00" || timeout != "1m" || g != gen {
		t.Fatalf("after the finalizer pass: stopTime %q timeout %q generation %d, want the user's spelling and generation %d", stop, timeout, g, gen)
	}
}

// TestFailoverGroupMetadataWritesKeepUserSpellings (K033): every
// metadata-only write the operator makes on a MysqlFailoverGroup (the
// finalizer the first reconcile adds, the removal of the reclone and
// rotate-keyring annotations, the finalizer removal on deletion) leaves
// the user's duration spellings as written and metadata.generation where
// it was. A full typed Update would rewrite `2h` as `2h0m0s` and bump the
// generation (TestTypedUpdateCanonicalizesUserSpellings).
func TestFailoverGroupMetadataWritesKeepUserSpellings(t *testing.T) {
	ns := createNamespace(t, "canonical-mfg")
	ensureSecret(t, ns)
	fg := newTestFG(ns)
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fg)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("MysqlFailoverGroup")
	spelled := map[string]string{"cloneTimeout": "2h", "failoverCooldown": "300s", "connectionDrainTimeout": "1m"}
	for field, v := range spelled {
		if err := unstructured.SetNestedField(u.Object, v, "spec", field); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(fg)
	_, gen := readSpecField(t, "MysqlFailoverGroup", ns, fg.Name, "cloneTimeout")
	check := func(step string) {
		t.Helper()
		for field, want := range spelled {
			got, g := readSpecField(t, "MysqlFailoverGroup", ns, fg.Name, field)
			if got != want || g != gen {
				t.Fatalf("after %s: spec.%s %q generation %d, want %q and generation %d", step, field, got, g, want, gen)
			}
		}
	}
	check("create")

	r := &controller.MysqlFailoverGroupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	var typed v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, key, &typed); err != nil {
		t.Fatal(err)
	}
	if len(typed.Finalizers) == 0 {
		t.Fatal("setup: the first reconcile added no finalizer")
	}
	check("the finalizer add")

	// The action annotations, written the way a user writes them (a merge
	// patch of metadata only).
	annotate := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+
		v1alpha1.RecloneAnnotation+`":"dc2","`+v1alpha1.RotateKeyringAnnotation+`":"now"}}}`))
	if err := k8sClient.Patch(ctx, &typed, annotate); err != nil {
		t.Fatal(err)
	}
	check("the user's annotation patch")

	runner := controller.NewTopologyManagerRunner(k8sClient, nil, nil, record.NewFakeRecorder(10), slog.New(slog.DiscardHandler))
	if err := runner.RemoveRecloneAnnotationForTest(ctx, key, "dc2"); err != nil {
		t.Fatalf("remove reclone annotation: %v", err)
	}
	check("the reclone annotation removal")
	if err := r.ClearRotateAnnotationForTest(ctx, &typed); err != nil {
		t.Fatalf("clear rotate annotation: %v", err)
	}
	check("the rotate-keyring annotation removal")
	if err := k8sClient.Get(ctx, key, &typed); err != nil {
		t.Fatal(err)
	}
	for _, a := range []string{v1alpha1.RecloneAnnotation, v1alpha1.RotateKeyringAnnotation} {
		if _, ok := typed.Annotations[a]; ok {
			t.Fatalf("annotation %s was not removed", a)
		}
	}

	// Deletion: the finalizer removal is a patch as well, so it succeeds
	// against the stored object and the group goes away.
	if err := k8sClient.Delete(ctx, &typed); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	if err := k8sClient.Get(ctx, key, &typed); !apierrors.IsNotFound(err) {
		t.Fatalf("after the finalizer removal: get = %v (finalizers %v), want NotFound", err, typed.Finalizers)
	}
}
