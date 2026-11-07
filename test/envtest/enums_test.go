//go:build envtest

package envtest

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The A-27/A-28/A-29/A-30/A-32/A-33 enums at admission: the CamelCase
// values are accepted, and the released lower-case spellings, the removed
// values, "" and malformed reasons are refused.

func enumsObject(t *testing.T, obj client.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	return inertFieldsObject(t, obj, kind)
}

func enumsSet(t *testing.T, u *unstructured.Unstructured, value any, path ...string) {
	t.Helper()
	inertFieldsSet(t, u, value, path...)
}

func wantRefused(t *testing.T, what string, err error, field string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: accepted, want refused", what)
		return
	}
	if !strings.Contains(err.Error(), field) {
		t.Errorf("%s: refused for another reason: %v", what, err)
	}
}

func TestEnvtest_SpecEnumsRefuseReleasedSpellings(t *testing.T) {
	ns := "envtest-enums-spec"
	ensureNamespace(t, ns)

	for i, role := range []string{"primary-candidate", "dr-only", "read-only", "Primary"} {
		fg := newTestFG(ns)
		fg.Name = "role-" + string(rune('a'+i))
		u := enumsObject(t, fg, "MysqlFailoverGroup")
		sites, _, _ := unstructured.NestedSlice(u.Object, "spec", "sites")
		sites[1].(map[string]any)["role"] = role
		enumsSet(t, u, sites, "spec", "sites")
		wantRefused(t, "spec.sites[].role="+role, k8sClient.Create(ctx, u), "role")
	}

	// The CamelCase roles are admitted, and the CEL rules count them.
	fg := newTestFG(ns)
	fg.Name = "roles-ok"
	third := fg.Spec.Sites[1]
	third.Name, third.ServerID, third.Role = "dr", 103, v1alpha1.SiteRoleDisasterRecovery
	reader := fg.Spec.Sites[1]
	reader.Name, reader.ServerID, reader.Role, reader.TaintNodeSelector, reader.LBIP = "reader", 104, v1alpha1.SiteRoleReadOnly, nil, ""
	fg.Spec.Sites = append(fg.Spec.Sites, third, reader)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("CamelCase roles: %v", err)
	}
	if fg.Spec.Sites[0].Role != v1alpha1.SiteRolePrimaryCandidate {
		t.Errorf("role defaulted to %q, want PrimaryCandidate", fg.Spec.Sites[0].Role)
	}
	if fg.Spec.UpdateStrategy.Type != v1alpha1.UpdateStrategyOrderedUpdate {
		t.Errorf("updateStrategy defaulted to %+v, want {type: OrderedUpdate}", fg.Spec.UpdateStrategy)
	}
	// Only one PrimaryCandidate left: the quorum rule compares the new value.
	fg = newTestFG(ns)
	fg.Name = "one-candidate"
	fg.Spec.Sites[1].Role = v1alpha1.SiteRoleDisasterRecovery
	wantRefused(t, "one PrimaryCandidate", k8sClient.Create(ctx, fg), "PrimaryCandidate")

	// updateStrategy is an object.
	fg = newTestFG(ns)
	fg.Name = "strategy-string"
	u := enumsObject(t, fg, "MysqlFailoverGroup")
	enumsSet(t, u, "Recreate", "spec", "updateStrategy")
	wantRefused(t, "updateStrategy: Recreate", k8sClient.Create(ctx, u), "updateStrategy")
	fg = newTestFG(ns)
	fg.Name = "strategy-bogus"
	fg.Spec.UpdateStrategy.Type = "recreate"
	wantRefused(t, "updateStrategy.type=recreate", k8sClient.Create(ctx, fg), "updateStrategy")
	fg = newTestFG(ns)
	fg.Name = "strategy-recreate"
	fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("updateStrategy.type=Recreate: %v", err)
	}

	// MysqlBackupVerification pointInTime.mode: Latest|Timestamp, required.
	for i, mode := range []string{"none", "latest", "timestamp", ""} {
		v := &v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Name: "mode-" + string(rune('a'+i)), Namespace: ns},
			Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"}, ProfileName: "nightly-s3",
				PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest}},
		}
		u := enumsObject(t, v, "MysqlBackupVerification")
		if mode == "" {
			unstructured.RemoveNestedField(u.Object, "spec", "pointInTime", "mode")
		} else {
			enumsSet(t, u, mode, "spec", "pointInTime", "mode")
		}
		wantRefused(t, "pointInTime.mode="+mode, k8sClient.Create(ctx, u), "mode")
	}
	for i, mode := range []v1alpha1.PointInTimeMode{v1alpha1.PointInTimeModeLatest, v1alpha1.PointInTimeModeTimestamp} {
		v := &v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Name: "mode-ok-" + string(rune('a'+i)), Namespace: ns},
			Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"}, ProfileName: "nightly-s3",
				PointInTime: e8VerificationPointInTime(mode)},
		}
		if err := k8sClient.Create(ctx, v); err != nil {
			t.Errorf("pointInTime.mode=%s: %v", mode, err)
		}
	}
}

func TestEnvtest_StatusEnumsRefuseReleasedSpellings(t *testing.T) {
	ns := "envtest-enums-status"
	ensureNamespace(t, ns)
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	statusWrite := func(t *testing.T, mutate func(u *unstructured.Unstructured)) error {
		t.Helper()
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), u); err != nil {
			t.Fatal(err)
		}
		enumsSet(t, u, map[string]any{"activeSite": "dc1"}, "status")
		mutate(u)
		return k8sClient.Status().Update(ctx, u)
	}
	site := func(fields map[string]any) func(u *unstructured.Unstructured) {
		return func(u *unstructured.Unstructured) {
			s := map[string]any{"name": "dc1", "state": "Writable"}
			for k, v := range fields {
				s[k] = v
			}
			enumsSet(t, u, []any{s}, "status", "sites")
		}
	}
	at := func(value any, path ...string) func(u *unstructured.Unstructured) {
		return func(u *unstructured.Unstructured) { enumsSet(t, u, value, append([]string{"status"}, path...)...) }
	}

	for _, tc := range []struct {
		what, field string
		mutate      func(u *unstructured.Unstructured)
	}{
		{"sites[].state=writable", "state", site(map[string]any{"state": "writable"})},
		{"sites[].state=read-only", "state", site(map[string]any{"state": "read-only"})},
		{`sites[].recoveryState=""`, "recoveryState", site(map[string]any{"recoveryState": ""})},
		{`sites[].divergenceDetectedBy=""`, "divergenceDetectedBy", site(map[string]any{"divergenceDetectedBy": ""})},
		{`sites[].sourceConvergenceState=""`, "sourceConvergenceState", site(map[string]any{"sourceConvergenceState": ""})},
		{"sites[].sourceConvergenceReason=Whatever", "sourceConvergenceReason", site(map[string]any{"sourceConvergenceReason": "Whatever"})},
		{"updatePhase=Complete", "updatePhase", at("Complete", "updatePhase")},
		{"dragonfly.sites[].role=master", "role", at(map[string]any{"sites": []any{map[string]any{"name": "dc1", "role": "master"}}}, "dragonfly")},
		{"dragonfly.sites[].role=stale-master", "role", at(map[string]any{"sites": []any{map[string]any{"name": "dc1", "role": "stale-master"}}}, "dragonfly")},
		{`dragonfly.phase=""`, "phase", at(map[string]any{"phase": ""}, "dragonfly")},
		{"dragonfly.upgrade.reason with a space", "reason", at(map[string]any{"upgrade": map[string]any{"reason": "snapshot failed"}}, "dragonfly")},
		{`restoreInPlace.phase=""`, "phase", at(map[string]any{"phase": ""}, "restoreInPlace")},
		{"restoreInPlace.scope=full", "scope", at(map[string]any{"phase": "Succeeded", "scope": "full"}, "restoreInPlace")},
		{"restoreInPlace.scope Schema without schema", "schema", at(map[string]any{"phase": "Succeeded", "scope": map[string]any{"type": "Schema"}}, "restoreInPlace")},
		{"restoreInPlace.scope Full with schema", "schema", at(map[string]any{"phase": "Succeeded", "scope": map[string]any{"type": "Full", "schema": "orders"}}, "restoreInPlace")},
		{"restore.reason with a space", "reason", at(map[string]any{"phase": "Failed", "reason": "replay failed"}, "restore")},
		{`encryptionAtRest.sites[].phase=""`, "phase", at(map[string]any{"sites": []any{map[string]any{"name": "dc1", "phase": ""}}}, "encryptionAtRest")},
		{`encryptionAtRest.sites[].unsealReason=""`, "unsealReason", at(map[string]any{"sites": []any{map[string]any{"name": "dc1", "phase": "Unsealed", "unsealReason": ""}}}, "encryptionAtRest")},
		{"backupSchedules[].lastBackupPhase=done", "lastBackupPhase", at([]any{map[string]any{"name": "nightly", "lastBackupPhase": "done"}}, "backupSchedules")},
	} {
		wantRefused(t, tc.what, statusWrite(t, tc.mutate), tc.field)
	}

	// The CamelCase values are admitted together.
	err := statusWrite(t, func(u *unstructured.Unstructured) {
		site(map[string]any{"recoveryState": "RecoveryBlocked", "divergenceDetectedBy": "ReplicaAudit",
			"sourceConvergenceState": "Blocked", "sourceConvergenceReason": "RestorePeerHold"})(u)
		at("WaitReplica", "updatePhase")(u)
		at(map[string]any{"phase": "Ready", "sites": []any{map[string]any{"name": "dc1", "role": "StaleMaster"}},
			"upgrade": map[string]any{"phase": "Failed", "reason": "DragonflySnapshotSaveFailed"}}, "dragonfly")(u)
		at(map[string]any{"phase": "Succeeded", "scope": map[string]any{"type": "Schema", "schema": "orders"}}, "restoreInPlace")(u)
		at(map[string]any{"phase": "Failed", "reason": string(v1alpha1.PITRTokenReplayFailed)}, "restore")(u)
		at([]any{map[string]any{"name": "nightly", "lastBackupPhase": "Succeeded"}}, "backupSchedules")(u)
	})
	if err != nil {
		t.Fatalf("CamelCase status values: %v", err)
	}
}

func TestEnvtest_MysqlDatabasePhaseApplying(t *testing.T) {
	ns := "envtest-enums-mdb"
	ensureNamespace(t, ns)
	db := newMysqlDatabaseCR(ns, "phase")
	if err := k8sClient.Create(ctx, db); err != nil {
		t.Fatal(err)
	}
	db.Status.Phase = "Creating"
	wantRefused(t, "phase=Creating", k8sClient.Status().Update(ctx, db), "phase")
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(db), db); err != nil {
		t.Fatal(err)
	}
	db.Status.Phase = v1alpha1.MysqlDatabasePhaseApplying
	if err := k8sClient.Status().Update(ctx, db); err != nil {
		t.Fatalf("phase=Applying: %v", err)
	}
}
