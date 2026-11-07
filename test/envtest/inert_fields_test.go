//go:build envtest

package envtest

import (
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// inertFieldsObject converts a typed object to unstructured so a test can
// add keys the Go types no longer have.
func inertFieldsObject(t *testing.T, obj runtime.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind(kind))
	return u
}

func inertFieldsSet(t *testing.T, u *unstructured.Unstructured, value any, path ...string) {
	t.Helper()
	if err := unstructured.SetNestedField(u.Object, value, path...); err != nil {
		t.Fatalf("set %s: %v", strings.Join(path, "."), err)
	}
}

func inertFieldsGet(t *testing.T, ns, name, kind string) *unstructured.Unstructured {
	t.Helper()
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind(kind))
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, stored); err != nil {
		t.Fatal(err)
	}
	return stored
}

func inertFieldsAbsent(t *testing.T, u *unstructured.Unstructured, path ...string) {
	t.Helper()
	if v, found, _ := unstructured.NestedFieldNoCopy(u.Object, path...); found {
		t.Errorf("%s survived admission: %v", strings.Join(path, "."), v)
	}
}

// A-03: spec.credentials is required. A group without it, or with an empty
// operator Secret name, is refused at admission; there is no other
// credential mode to fall back to.
func TestE3_FailoverGroupWithoutCredentialsRejected(t *testing.T) {
	ns := "e3-credentials-required"
	ensureNamespace(t, ns)

	u := inertFieldsObject(t, newTestFG(ns), "MysqlFailoverGroup")
	unstructured.RemoveNestedField(u.Object, "spec", "credentials")
	err := k8sClient.Create(ctx, u)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.credentials") {
		t.Fatalf("a group without spec.credentials: want an Invalid error naming spec.credentials, got %v", err)
	}

	// The old DSN mode does not satisfy the requirement either: the
	// unknown secretName is pruned and credentials is still missing.
	u = inertFieldsObject(t, newTestFG(ns), "MysqlFailoverGroup")
	unstructured.RemoveNestedField(u.Object, "spec", "credentials")
	inertFieldsSet(t, u, "mysql-credentials", "spec", "secretName")
	err = k8sClient.Create(ctx, u)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.credentials") {
		t.Fatalf("a DSN-mode group: want an Invalid error naming spec.credentials, got %v", err)
	}

	fg := newTestFG(ns)
	fg.Spec.Credentials = v1alpha1.CredentialsSpec{}
	err = k8sClient.Create(ctx, fg)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "operatorSecretName") {
		t.Fatalf("an empty operatorSecretName: want an Invalid error naming operatorSecretName, got %v", err)
	}
}

// A-03, A-56, A-58, A-59: the removed MysqlFailoverGroup fields are no
// longer in the schema, so the API server prunes them from a manifest that
// still carries them (OPUS-28).
func TestE3_FailoverGroupRemovedFieldsArePruned(t *testing.T) {
	ns := "e3-mfg-pruned"
	ensureNamespace(t, ns)

	fg := newVerificationFG(ns)
	fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"}
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 5 * * *"}
	fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{
		Confirm:     "2026-04-17T14:32:00Z",
		Source:      v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}},
		LoadOptions: &v1alpha1.LoadOptions{Threads: 2},
	}
	u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
	inertFieldsSet(t, u, "mysql-credentials", "spec", "secretName")
	inertFieldsSet(t, u, map[string]any{"name": "mysql-ca", "kind": "ClusterIssuer"}, "spec", "tls", "issuerRef")
	inertFieldsSet(t, u, map[string]any{"stopDatetime": "2026-04-17T13:00:00Z", "archiveProfileName": "nightly-s3"},
		"spec", "restoreInPlace", "pointInTime")
	inertFieldsSet(t, u, true, "spec", "restoreInPlace", "loadOptions", "resetProgress")
	profiles, _, _ := unstructured.NestedSlice(u.Object, "spec", "backup", "profiles")
	p := profiles[0].(map[string]any)
	p["storage"].(map[string]any)["s3"].(map[string]any)["storageClass"] = "STANDARD_IA"
	p["verification"].(map[string]any)["storage"] = map[string]any{"storageClassName": "standard", "size": "20Gi"}
	inertFieldsSet(t, u, profiles, "spec", "backup", "profiles")
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatalf("create with removed fields: %v", err)
	}

	stored := inertFieldsGet(t, ns, fg.Name, "MysqlFailoverGroup")
	inertFieldsAbsent(t, stored, "spec", "secretName")
	inertFieldsAbsent(t, stored, "spec", "tls", "issuerRef")
	inertFieldsAbsent(t, stored, "spec", "restoreInPlace", "pointInTime")
	inertFieldsAbsent(t, stored, "spec", "restoreInPlace", "loadOptions", "resetProgress")
	storedProfiles, _, _ := unstructured.NestedSlice(stored.Object, "spec", "backup", "profiles")
	sp := storedProfiles[0].(map[string]any)
	if _, ok := sp["storage"].(map[string]any)["s3"].(map[string]any)["storageClass"]; ok {
		t.Errorf("profile storage.s3.storageClass survived admission")
	}
	if _, ok := sp["verification"].(map[string]any)["storage"]; ok {
		t.Errorf("profile verification.storage survived admission")
	}
	if got, _, _ := unstructured.NestedString(stored.Object, "spec", "tls", "secretName"); got != "mysql-tls" {
		t.Errorf("spec.tls.secretName = %q, want mysql-tls", got)
	}
	if got, _, _ := unstructured.NestedString(stored.Object, "spec", "credentials", "operatorSecretName"); got != "mysql-credentials" {
		t.Errorf("spec.credentials.operatorSecretName = %q, want mysql-credentials", got)
	}
}

// A-56, A-57, A-65, A-31: MysqlBackup and MysqlBackupVerification drop
// spec.triggeredBy, spec.storage and the never-written status fields, and
// a verification phase the controller never enters is refused.
func TestE3_BackupAndVerificationRemovedFields(t *testing.T) {
	ns := "e3-mbk-mbv-pruned"
	ensureNamespace(t, ns)

	mb := &v1alpha1.MysqlBackup{}
	mb.Name, mb.Namespace = "manual", ns
	mb.Spec = v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"}, ProfileName: "nightly-s3"}
	ub := inertFieldsObject(t, mb, "MysqlBackup")
	inertFieldsSet(t, ub, "manual", "spec", "triggeredBy")
	if err := k8sClient.Create(ctx, ub); err != nil {
		t.Fatalf("create MysqlBackup: %v", err)
	}
	inertFieldsAbsent(t, inertFieldsGet(t, ns, "manual", "MysqlBackup"), "spec", "triggeredBy")

	v := &v1alpha1.MysqlBackupVerification{}
	v.Name, v.Namespace = "verify", ns
	v.Spec = v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"}, ProfileName: "nightly-s3"}
	uv := inertFieldsObject(t, v, "MysqlBackupVerification")
	inertFieldsSet(t, uv, "manual", "spec", "triggeredBy")
	inertFieldsSet(t, uv, map[string]any{"storageClassName": "standard", "size": "20Gi"}, "spec", "storage")
	if err := k8sClient.Create(ctx, uv); err != nil {
		t.Fatalf("create MysqlBackupVerification: %v", err)
	}
	stored := inertFieldsGet(t, ns, "verify", "MysqlBackupVerification")
	inertFieldsAbsent(t, stored, "spec", "triggeredBy")
	inertFieldsAbsent(t, stored, "spec", "storage")

	inertFieldsSet(t, stored, map[string]any{
		"phase": "Restoring", "jobName": "mysqlverify-verify",
		"podName": "p", "pvcName": "c", "serviceName": "s",
	}, "status")
	if err := k8sClient.Status().Update(ctx, stored); err != nil {
		t.Fatalf("status update with removed fields: %v", err)
	}
	stored = inertFieldsGet(t, ns, "verify", "MysqlBackupVerification")
	for _, f := range []string{"podName", "pvcName", "serviceName"} {
		inertFieldsAbsent(t, stored, "status", f)
	}
	if got, _, _ := unstructured.NestedString(stored.Object, "status", "jobName"); got != "mysqlverify-verify" {
		t.Errorf("status.jobName = %q, want it kept", got)
	}

	for _, phase := range []string{"Checking", "Cleaning"} {
		inertFieldsSet(t, stored, phase, "status", "phase")
		err := k8sClient.Status().Update(ctx, stored)
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "status.phase") {
			t.Errorf("status.phase=%s: want an Invalid error naming status.phase, got %v", phase, err)
		}
		stored = inertFieldsGet(t, ns, "verify", "MysqlBackupVerification")
	}
}
