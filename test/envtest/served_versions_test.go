//go:build envtest

package envtest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/crdcompat"
)

// warningRecorder collects the Warning headers of a client's responses.
type warningRecorder struct {
	mu   sync.Mutex
	text []string
}

func (w *warningRecorder) HandleWarningHeader(_ int, _ string, text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.text = append(w.text, text)
}

func (w *warningRecorder) take() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.text
	w.text = nil
	return out
}

// clientAt returns a typed client that reads and writes the Bloodraven
// kinds at gv, and what the API server warned it about.
func clientAt(t *testing.T, gv schema.GroupVersion) (client.Client, *warningRecorder) {
	t.Helper()
	warnings := &warningRecorder{}
	c := rest.CopyConfig(cfg)
	c.WarningHandler = warnings
	out, err := client.New(c, client.Options{Scheme: schemeAt(t, gv)})
	if err != nil {
		t.Fatal(err)
	}
	return out, warnings
}

var servedVersions = []schema.GroupVersion{v1beta1.GroupVersion, v1beta1.DeprecatedGroupVersion}

// C-9: an object written through either served version is the same object
// through the other, spec and status. With conversion strategy None the
// API server only rewrites apiVersion, which holds as long as the two
// versions have one schema (TestServedVersionsShareOneSchema).
func TestServedVersions_SpecAndStatusThroughBoth(t *testing.T) {
	for i, writer := range servedVersions {
		reader := servedVersions[1-i]
		t.Run("written through "+writer.Version, func(t *testing.T) {
			w, wWarnings := clientAt(t, writer)
			r, _ := clientAt(t, reader)
			ns := createNamespace(t, "served-"+writer.Version)

			fg := newTestFG(ns)
			if err := w.Create(ctx, fg); err != nil {
				t.Fatalf("create through %s: %v", writer.Version, err)
			}
			deprecated := false
			for _, text := range wWarnings.take() {
				deprecated = deprecated || strings.Contains(text, "deprecated")
			}
			if want := writer == v1beta1.DeprecatedGroupVersion; deprecated != want {
				t.Fatalf("a write through %s was warned as deprecated: %v, want %v", writer.Version, deprecated, want)
			}

			key := client.ObjectKeyFromObject(fg)
			var written v1beta1.MysqlFailoverGroup
			if err := w.Get(ctx, key, &written); err != nil {
				t.Fatal(err)
			}
			now := metav1.Now()
			written.Status.ActiveSite = "dc1"
			written.Status.Sites = []v1beta1.SiteStatus{
				{Name: "dc1", State: v1beta1.SiteStateWritable, Replication: v1beta1.SiteReplicationNotApplicable},
				{Name: "dc2", State: v1beta1.SiteStateReadOnly, Replication: v1beta1.SiteReplicationHealthy},
			}
			written.Status.Rollout = &v1beta1.RolloutStatus{Renderer: "0123456789abcdef", Phase: v1beta1.RolloutPhaseAdmitted, Reason: "Admitted", AdmittedTime: &now}
			written.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionReady, Status: metav1.ConditionTrue, Reason: v1beta1.ReasonHealthy,
				Message: "ok", LastTransitionTime: now, ObservedGeneration: written.Generation}}
			if err := w.Status().Update(ctx, &written); err != nil {
				t.Fatalf("status write through %s: %v", writer.Version, err)
			}
			if err := w.Get(ctx, key, &written); err != nil {
				t.Fatal(err)
			}

			var read v1beta1.MysqlFailoverGroup
			if err := r.Get(ctx, key, &read); err != nil {
				t.Fatalf("read through %s: %v", reader.Version, err)
			}
			if !equality.Semantic.DeepEqual(written.Spec, read.Spec) {
				t.Fatalf("the spec written through %s differs through %s", writer.Version, reader.Version)
			}
			if !equality.Semantic.DeepEqual(written.Status, read.Status) {
				t.Fatalf("the status written through %s differs through %s:\n%+v\n%+v", writer.Version, reader.Version, written.Status, read.Status)
			}
			if read.Status.ActiveSite != "dc1" || len(read.Status.Sites) != 2 || read.Status.Rollout == nil || len(read.Status.Conditions) != 1 {
				t.Fatalf("the status read through %s lost something: %+v", reader.Version, read.Status)
			}
			if written.UID != read.UID || written.ResourceVersion != read.ResourceVersion {
				t.Fatalf("the two versions show two objects: uid %s/%s, resourceVersion %s/%s", written.UID, read.UID, written.ResourceVersion, read.ResourceVersion)
			}

			// And the other way: a status write through the reader's
			// version lands on the same object.
			read.Status.ActiveSite = "dc2"
			if err := r.Status().Update(ctx, &read); err != nil {
				t.Fatalf("status write through %s: %v", reader.Version, err)
			}
			if err := w.Get(ctx, key, &written); err != nil {
				t.Fatal(err)
			}
			if written.Status.ActiveSite != "dc2" {
				t.Fatalf("a status write through %s is not visible through %s", reader.Version, writer.Version)
			}

			// Each version answers with its own apiVersion.
			dyn, err := dynamic.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, gv := range servedVersions {
				u, err := dyn.Resource(crdcompat.Resources[0].GVR(gv.Version)).Namespace(ns).Get(ctx, fg.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if u.GetAPIVersion() != gv.String() {
					t.Fatalf("read through %s answers apiVersion %s", gv, u.GetAPIVersion())
				}
			}
		})
	}
}

// C-9: admission does not depend on the version a request comes through.
// The destructive requests keep their confirmation, and the rules that
// refuse a change keep refusing it.
func TestServedVersions_ConfirmationAndImmutabilityThroughBoth(t *testing.T) {
	for _, gv := range servedVersions {
		t.Run(gv.Version, func(t *testing.T) {
			c, _ := clientAt(t, gv)
			ns := createNamespace(t, "confirm-"+gv.Version)
			restore := func(name, confirm string) error {
				fg := newTestFG(ns)
				fg.Name = name
				fg.Spec.RestoreInPlace = &v1beta1.RestoreInPlaceSpec{Confirm: confirm,
					Source: v1beta1.RestoreSource{BackupRef: &v1beta1.LocalMysqlBackupRef{Name: "b"}}}
				return c.Create(ctx, fg)
			}
			// An in-place restore drops and reloads data. Its confirm is
			// required, and must be a timestamp.
			e4WantInvalid(t, "restoreInPlace without confirm", restore("no-confirm", ""), "spec.restoreInPlace.confirm")
			e4WantInvalid(t, "restoreInPlace with a confirm that is no timestamp", restore("bad-confirm", "yes"), "spec.restoreInPlace.confirm")
			if err := restore("confirmed", "2026-04-17T14:32:00Z"); err != nil {
				t.Fatalf("a confirmed restoreInPlace is refused through %s: %v", gv.Version, err)
			}

			fg := newTestFG(ns)
			if err := c.Create(ctx, fg); err != nil {
				t.Fatal(err)
			}
			oldSize := fg.Spec.Sites[0].Storage.Size
			fg.Spec.Sites[0].Storage.Size = "20Gi"
			if err := c.Update(ctx, fg); err != nil {
				t.Fatalf("growing site storage through %s: %v", gv.Version, err)
			}
			shrunk := fg.DeepCopy()
			shrunk.Spec.Sites[0].Storage.Size = oldSize
			e4WantInvalid(t, "restoring the original size", c.Update(ctx, shrunk), "storage size cannot decrease")

			// A site cannot be removed (A-16): removing one would orphan
			// its data volume.
			fg.Spec.Sites = fg.Spec.Sites[:1]
			if err := c.Update(ctx, fg); !apierrors.IsInvalid(err) {
				t.Fatalf("removing a site through %s = %v, want Invalid", gv.Version, err)
			}

			// Encryption at rest cannot be turned off (A-24).
			enc := newEncryptionFG(ns, "encrypted")
			enc.Spec.EncryptionAtRest = &v1beta1.EncryptionAtRestSpec{Enabled: true}
			if err := c.Create(ctx, enc); err != nil {
				t.Fatal(err)
			}
			enc.Spec.EncryptionAtRest.Enabled = false
			if err := c.Update(ctx, enc); !apierrors.IsInvalid(err) {
				t.Fatalf("turning encryption at rest off through %s = %v, want Invalid", gv.Version, err)
			}
		})
	}
}

// exampleObjects returns the Bloodraven objects of every examples/*.yaml
// file.
func exampleObjects(t *testing.T) map[string][]*unstructured.Unstructured {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples (%v)", err)
	}
	out := map[string][]*unstructured.Unstructured{}
	for _, f := range files {
		raw, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(raw, 4096)
		for {
			obj := &unstructured.Unstructured{}
			if err := dec.Decode(&obj.Object); err != nil {
				break
			}
			if obj.GroupVersionKind().Group == crdcompat.Group {
				out[filepath.Base(f)] = append(out[filepath.Base(f)], obj)
			}
		}
		_ = raw.Close()
	}
	return out
}

// partialExamples are the examples that show one part of a group
// (spec.backup) and are meant to be applied over an existing one. They are
// dry-run as a server-side apply over the minimal example's group.
var partialExamples = map[string]bool{"pvc-backups.yaml": true, "s3-backups-with-verification.yaml": true}

// C-3: every example is accepted by a server-side dry run with strict
// field validation, at both served versions. That is the check a
// production pipeline runs before it applies a manifest
// (`kubectl apply --server-side --dry-run=server --validate=strict`), so a
// default or a rule that an example breaks fails here first.
func TestExamplesPassServerSideDryRun(t *testing.T) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	examples := exampleObjects(t)
	var minimal *unstructured.Unstructured
	for _, obj := range examples["minimal-failovergroup.yaml"] {
		minimal = obj
	}
	if minimal == nil {
		t.Fatal("examples/minimal-failovergroup.yaml holds no group")
	}
	kinds := map[string]bool{}
	for file, objs := range examples {
		for _, obj := range objs {
			kinds[obj.GetKind()] = true
			if obj.GetAPIVersion() != v1beta1.GroupVersion.String() {
				t.Errorf("%s: %s %s is written at %s, want %s", file, obj.GetKind(), obj.GetName(), obj.GetAPIVersion(), v1beta1.GroupVersion)
			}
			ns := createNamespace(t, "example")
			if partialExamples[file] {
				if obj.GetKind() != minimal.GetKind() || obj.GetName() != minimal.GetName() {
					t.Fatalf("%s: the partial example %s %s is not a part of the minimal example's group", file, obj.GetKind(), obj.GetName())
				}
				base := minimal.DeepCopy()
				base.SetNamespace(ns)
				if _, err := dyn.Resource(crdcompat.Resources[0].GVR(v1beta1.GroupVersion.Version)).Namespace(ns).Create(ctx, base, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			for _, gv := range servedVersions {
				body := obj.DeepCopy()
				body.SetAPIVersion(gv.String())
				body.SetNamespace(ns)
				res := dyn.Resource(resourceOf(t, body).GVR(gv.Version)).Namespace(ns)
				var err error
				if partialExamples[file] {
					_, err = res.Apply(context.Background(), body.GetName(), body, metav1.ApplyOptions{FieldManager: "example", DryRun: []string{metav1.DryRunAll}})
				} else {
					_, err = res.Create(context.Background(), body, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}, FieldValidation: "Strict"})
				}
				if err != nil {
					t.Errorf("%s: %s %s is refused by a server-side dry run at %s: %v", file, obj.GetKind(), obj.GetName(), gv.Version, err)
				}
			}
		}
	}
	for _, want := range []string{"MysqlFailoverGroup", "MysqlStandbyCluster"} {
		if !kinds[want] {
			t.Errorf("no example of kind %s was found", want)
		}
	}
}
