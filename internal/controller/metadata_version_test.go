package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// TestTerminalMarkerVersionProblem pins WE-Q11d: a terminal marker with a
// schemaVersion the operator does not know is reported, and
// parseHoldMarker's fail-closed fallback is kept (a ManualRecoveryRequired
// marker still holds); a known or malformed marker is not reported here.
func TestTerminalMarkerVersionProblem(t *testing.T) {
	fgWith := func(v string) *v1alpha1.MysqlFailoverGroup {
		return &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: "lion",
			Annotations: map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: v}}}
	}
	known := TerminalMarker{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack}.String()
	for _, tc := range []struct {
		name, value, want string
		holds             bool
	}{
		{name: "known version", value: known},
		{name: "absent", value: ""},
		{name: "malformed", value: "{not json"},
		{name: "non-integer version", value: `{"schemaVersion":"2","runID":"run-1","outcome":"RolledBack"}`},
		{name: "newer version", value: `{"schemaVersion":2,"runID":"run-1","outcome":"RolledBack"}`, want: "proves no outcome"},
		{name: "unversioned", value: `{"runID":"run-1","outcome":"RolledBack"}`, want: "schemaVersion 0"},
		{name: "newer hold marker", value: `{"schemaVersion":2,"runID":"run-1","outcome":"ManualRecoveryRequired","reason":"Future"}`,
			want: "stays held", holds: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWith(tc.value)
			if tc.value == "" {
				fg.Annotations = nil
			}
			got := terminalMarkerVersionProblem(fg)
			if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
				t.Fatalf("problem = %q, want %q", got, tc.want)
			}
			if _, ok := ParseTerminalMarker(tc.value); ok && tc.want != "" {
				t.Fatal("an unknown version parsed strictly")
			}
			if hold := PlannedHoldFrom(fg); (hold != nil) != tc.holds {
				t.Fatalf("PlannedHoldFrom = %+v, want hold=%v (the fail-closed fallback must stay)", hold, tc.holds)
			}
		})
	}
}

// TestGroupRefusal_DegradedAndOneEvent: a recorded refusal sets
// Degraded=True with its reason on the next status write and emits one
// Warning Event; recording it again does not repeat the Event; clearing it
// restores the topology verdict. A topology alert still wins.
func TestGroupRefusal_DegradedAndOneEvent(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	_, c := newReconciler(fg)
	rec := record.NewFakeRecorder(16)
	runner := NewTopologyManagerRunner(c, nil, nil, rec, testLogger())
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		ActiveSite: "dc1",
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged},
		},
		DegradedReason: v1alpha1.ReasonHealthy,
	}
	degraded := func(s TopologySnapshot) *metav1.Condition {
		t.Helper()
		if err := runner.updateCRStatus(ctx, nn, s); err != nil {
			t.Fatal(err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		return apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded)
	}
	msg := "annotation x has schemaVersion 2"
	runner.SetGroupRefusal(fg, terminalMarkerRefusalKey, v1alpha1.ReasonMetadataVersionUnsupported, msg)
	runner.SetGroupRefusal(fg, terminalMarkerRefusalKey, v1alpha1.ReasonMetadataVersionUnsupported, msg)
	if ev := drainEventReasons(rec); len(ev) != 1 || ev[0] != v1alpha1.EventMetadataVersionUnsupported {
		t.Fatalf("Events = %v, want one %s", ev, v1alpha1.EventMetadataVersionUnsupported)
	}
	if d := degraded(snap); d == nil || d.Status != metav1.ConditionTrue || d.Reason != v1alpha1.ReasonMetadataVersionUnsupported || d.Message != msg {
		t.Fatalf("Degraded = %+v", d)
	}
	// A workload refusal outranks a payload refusal.
	runner.SetGroupRefusal(fg, workloadRefusalKey("Deployment", "d"), v1alpha1.ReasonWorkloadSelectorMismatch, "Deployment d")
	if d := degraded(snap); d == nil || d.Reason != v1alpha1.ReasonWorkloadSelectorMismatch {
		t.Fatalf("Degraded = %+v, want WorkloadSelectorMismatch", d)
	}
	// A topology alert wins over both.
	alert := snap
	alert.DegradedReason = v1alpha1.ReasonSplitBrain
	alert.Alert = "both writable"
	if d := degraded(alert); d == nil || d.Reason != v1alpha1.ReasonSplitBrain {
		t.Fatalf("Degraded = %+v, want SplitBrain", d)
	}
	runner.ClearGroupRefusal(nn, workloadRefusalKey("Deployment", "d"))
	runner.ClearGroupRefusal(nn, terminalMarkerRefusalKey)
	if d := degraded(snap); d == nil || d.Status != metav1.ConditionFalse || d.Reason != v1alpha1.ReasonHealthy {
		t.Fatalf("Degraded = %+v after the refusals cleared", d)
	}
}

// TestDeploymentLeaseRecordVersion: lease records carry schemaVersion 1;
// one of another version is refused with a group refusal (A-80).
func TestDeploymentLeaseRecordVersion(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	runner := NewTopologyManagerRunner(m.client, nil, nil, record.NewFakeRecorder(16), testLogger())
	m.SetGroupRefusals(runner)
	grantDeploymentLease(t, m, fg, "migration", "op1", 60)
	var leases coordinationv1.LeaseList
	if err := m.client.List(context.Background(), &leases, client.InNamespace(fg.Namespace)); err != nil || len(leases.Items) != 1 {
		t.Fatalf("leases = %v, %v", leases.Items, err)
	}
	l := &leases.Items[0]
	var probe map[string]any
	if err := json.Unmarshal([]byte(l.Annotations[v1alpha1.DeploymentLeaseAnnotation]), &probe); err != nil || probe["schemaVersion"] != float64(1) {
		t.Fatalf("record = %v (%v), want schemaVersion 1", probe, err)
	}
	probe["schemaVersion"] = 2
	b, _ := json.Marshal(probe)
	l.Annotations[v1alpha1.DeploymentLeaseAnnotation] = string(b)
	if err := m.client.Update(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	_, _, err := m.read(context.Background(), fg, "migration", "")
	var verr errLeaseRecordVersion
	if !errors.As(err, &verr) {
		t.Fatalf("read = %v, want errLeaseRecordVersion", err)
	}
	nn := client.ObjectKeyFromObject(fg)
	if reason, msg := runner.refusalDegraded(nn); reason != v1alpha1.ReasonMetadataVersionUnsupported || !strings.Contains(msg, l.Name) {
		t.Fatalf("refusal = %s %q", reason, msg)
	}
}

// TestDeploymentLeaseRecordVersionRefusalEndsWithTheLease (O-04): the
// refusal of an unreadable lease record lasts until the Lease is deleted,
// as its message says, not until the operator restarts. Grant refuses
// while Degraded reports it, before it reads any Lease, so housekeeping
// must notice the deletion. A hold Lease (named by its operation) is
// covered the same way.
func TestDeploymentLeaseRecordVersionRefusalEndsWithTheLease(t *testing.T) {
	for _, kind := range []string{"migration", "failover-hold"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			m, fg, _ := deploymentLeaseFixture(t)
			runner := NewTopologyManagerRunner(m.client, nil, nil, record.NewFakeRecorder(16), testLogger())
			m.SetGroupRefusals(runner)
			grantDeploymentLease(t, m, fg, "migration", "op1", 60)
			if kind == "failover-hold" {
				grantDeploymentLease(t, m, fg, "failover-hold", "op1", 60)
			}
			operation := ""
			if kind == "failover-hold" {
				operation = "op1"
			}
			l := &coordinationv1.Lease{}
			if err := m.client.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: deploymentLeaseName(fg.Name, kind, operation)}, l); err != nil {
				t.Fatal(err)
			}
			var probe map[string]any
			if err := json.Unmarshal([]byte(l.Annotations[v1alpha1.DeploymentLeaseAnnotation]), &probe); err != nil {
				t.Fatal(err)
			}
			probe["schemaVersion"] = 2
			b, _ := json.Marshal(probe)
			l.Annotations[v1alpha1.DeploymentLeaseAnnotation] = string(b)
			if err := m.client.Update(ctx, l); err != nil {
				t.Fatal(err)
			}
			nn := client.ObjectKeyFromObject(fg)
			snap := TopologySnapshot{
				TopologyGeneration: fg.Status.TopologyGeneration, ActiveSite: "iad", DegradedReason: v1alpha1.ReasonHealthy,
				Sites: []SiteSnapshot{
					{Name: "iad", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
					{Name: "pdx", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged,
						ReplicaStatusRead: true, Replication: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: ptrTo(int64(0))}},
				},
			}
			publish := func() {
				t.Helper()
				if err := runner.updateCRStatus(ctx, nn, snap); err != nil {
					t.Fatal(err)
				}
			}
			// Housekeeping finds the record and the status write reports it.
			if err := m.TopologyPersisted(ctx, fg, 0); err == nil {
				t.Fatal("housekeeping read the unknown record")
			}
			publish()
			fresh, err := m.fresh(ctx, fg)
			if err != nil {
				t.Fatal(err)
			}
			if c := apimeta.FindStatusCondition(fresh.Status.Conditions, v1alpha1.ConditionDegraded); c == nil || c.Reason != v1alpha1.ReasonMetadataVersionUnsupported {
				t.Fatalf("Degraded = %+v, want MetadataVersionUnsupported", c)
			}
			_, err = m.Grant(ctx, fg, "migration", "op2", "tenant", "tenant-ns", "deployer", 60)
			requireDeploymentError(t, err, 423, "unstable")

			// The admin deletes the Lease, as the message says.
			if err := m.client.Delete(ctx, l); err != nil {
				t.Fatal(err)
			}
			if err := m.TopologyPersisted(ctx, fg, 0); err != nil {
				t.Fatalf("housekeeping after the Lease was deleted: %v", err)
			}
			if reason, msg := runner.refusalDegraded(nn); reason != "" {
				t.Fatalf("refusal outlived its Lease: %s %q", reason, msg)
			}
			publish()
			fresh, err = m.fresh(ctx, fg)
			if err != nil {
				t.Fatal(err)
			}
			if c := apimeta.FindStatusCondition(fresh.Status.Conditions, v1alpha1.ConditionDegraded); c == nil || c.Status != metav1.ConditionFalse {
				t.Fatalf("Degraded = %+v, want False once the Lease is gone", c)
			}
			// A new lease of the deleted Lease's kind is granted.
			grantDeploymentLease(t, m, fresh, kind, "op1", 60)
		})
	}
}

// TestVerificationSourceRecordVersion: the verification-backup record
// carries schemaVersion 1; one of another version proves no source.
func TestVerificationSourceRecordVersion(t *testing.T) {
	ref := &v1alpha1.VerificationBackupRef{Name: "b", UID: "uid-b", ArtifactStorage: &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStorageS3, Location: "s3://bucket/x"}}
	b, err := encodeVerificationSource(ref)
	if err != nil {
		t.Fatal(err)
	}
	var probe map[string]any
	if err := json.Unmarshal(b, &probe); err != nil || probe["schemaVersion"] != float64(1) || probe["name"] != "b" {
		t.Fatalf("record = %s", b)
	}
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "v", Namespace: "ns", UID: "uid-v"}}
	for _, raw := range []string{`{"schemaVersion":2,"name":"b","uid":"uid-b"}`, `{"name":"b","uid":"uid-b"}`} {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "ns", OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1alpha1.GroupVersion.String(), Kind: "MysqlBackupVerification", Name: v.Name, UID: v.UID, Controller: ptrTo(true)}}},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{v1alpha1.VerificationBackupAnnotation: raw}}}},
		}
		_, err := verificationJobSource(v, job)
		var verr errVerificationSourceVersion
		if !errors.As(err, &verr) {
			t.Fatalf("%s: err = %v, want errVerificationSourceVersion", raw, err)
		}
	}
}

func ptrTo[T any](v T) *T { return &v }
