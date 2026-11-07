package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestVerificationHoldKeyProtectsBackup pins A-77's hold family: the key
// verification-hold.bloodraven.shipstream.io/<verification uid> protects a
// backup (retention and the artifact cleanup read BeingRead, the PITR
// cutoff BackupHasVerificationHold) while its verification is active, and
// while it is Terminating with its Job still present. The pre-A-77 key
// protects nothing: no dual-key reads.
func TestVerificationHoldKeyProtectsBackup(t *testing.T) {
	const uid = "6f1c2d3e-0000-4000-8000-0000000000aa"
	if got := verificationHoldKey(&v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{UID: uid}}); got != "verification-hold.bloodraven.shipstream.io/"+uid {
		t.Fatalf("verificationHoldKey = %q", got)
	}
	now := metav1.Now()
	for _, tc := range []struct {
		name        string
		key         string
		terminating bool
		want        bool
	}{
		{name: "active verification", key: v1alpha1.VerificationHoldAnnotationPrefix + uid, want: true},
		{name: "terminating verification", key: v1alpha1.VerificationHoldAnnotationPrefix + uid, terminating: true, want: true},
		{name: "the pre-A-77 key", key: "hold.verification.shipstream.io/" + uid, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backup := &v1alpha1.MysqlBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns", UID: "uid-b", Annotations: map[string]string{tc.key: "reader"}},
				Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "fg"}, ProfileName: "p"},
				Status:     v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded},
			}
			// Only the hold names the backup: the verification is for
			// another profile and has not pinned a source.
			v := &v1alpha1.MysqlBackupVerification{
				ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "ns", UID: uid},
				Spec:       v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "fg"}, ProfileName: "other"},
				Status:     v1alpha1.MysqlBackupVerificationStatus{Phase: v1alpha1.VerificationPhaseRestoring},
			}
			objs := []client.Object{backup}
			if tc.terminating {
				v.DeletionTimestamp = &now
				v.Finalizers = []string{v1alpha1.MysqlBackupVerificationFinalizer}
				objs = append(objs, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: verificationJobName(v.Name), Namespace: "ns"}})
			}
			objs = append(objs, v)
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
			read, err := NewBackupReadChecker(c).BeingRead(context.Background(), backup)
			if err != nil {
				t.Fatal(err)
			}
			if read != tc.want {
				t.Errorf("BeingRead = %v, want %v", read, tc.want)
			}
			if got := BackupHasVerificationHold(backup); got != tc.want {
				t.Errorf("BackupHasVerificationHold = %v, want %v", got, tc.want)
			}
		})
	}
}
