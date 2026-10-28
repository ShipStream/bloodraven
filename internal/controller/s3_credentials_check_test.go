package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func s3CredsTestFG() *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{{
		Name: "nightly",
		Storage: v1alpha1.BackupStorage{
			Type: v1alpha1.BackupStorageS3,
			S3:   &v1alpha1.S3Storage{Bucket: "b", CredentialsSecret: "s3-creds"},
		},
	}}}
	return fg
}

func s3CredsCondition(t *testing.T, r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup) *metav1.Condition {
	t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := r.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &got); err != nil {
		t.Fatal(err)
	}
	return apimeta.FindStatusCondition(got.Status.Conditions, conditionS3CredentialsReady)
}

func TestReconcileS3CredentialsCondition(t *testing.T) {
	for _, tc := range []struct {
		name       string
		secret     *corev1.Secret
		wantStatus metav1.ConditionStatus
		wantInMsg  string
	}{
		{
			name: "complete",
			secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "shared-lion"}, Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID": []byte("AKIA"), "AWS_SECRET_ACCESS_KEY": []byte("s"),
			}},
			wantStatus: metav1.ConditionTrue,
		},
		{
			name: "region only, relying on ambient credentials",
			secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "shared-lion"}, Data: map[string][]byte{
				"AWS_REGION": []byte("us-east-1"), "AWS_SECRET_ACCESS_KEY": []byte(" \n"),
			}},
			wantStatus: metav1.ConditionFalse,
			wantInMsg:  `Secret "s3-creds" has no non-empty AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY`,
		},
		{
			name:       "missing secret",
			wantStatus: metav1.ConditionFalse,
			wantInMsg:  `spec.backup.profiles[nightly].storage.s3.credentialsSecret: Secret "s3-creds" not found`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := s3CredsTestFG()
			objs := []client.Object{fg}
			if tc.secret != nil {
				objs = append(objs, tc.secret)
			}
			r, _ := newReconciler(objs...)
			rec := record.NewFakeRecorder(10)
			r.Recorder = rec

			if err := r.reconcileS3CredentialsCondition(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			cond := s3CredsCondition(t, r, fg)
			if cond == nil || cond.Status != tc.wantStatus {
				t.Fatalf("condition = %+v, want status %s", cond, tc.wantStatus)
			}
			if !strings.Contains(cond.Message, tc.wantInMsg) {
				t.Errorf("condition message = %q, want it to contain %q", cond.Message, tc.wantInMsg)
			}
			gotEvent := len(rec.Events) > 0
			if wantEvent := tc.wantStatus == metav1.ConditionFalse; gotEvent != wantEvent {
				t.Errorf("warning event emitted = %t, want %t", gotEvent, wantEvent)
			}
		})
	}
}

func TestReconcileS3CredentialsCondition_RemovedWithoutS3Storage(t *testing.T) {
	fg := newTestFG()
	fg.Status.Conditions = []metav1.Condition{{Type: conditionS3CredentialsReady, Status: metav1.ConditionFalse, Reason: "CredentialsIncomplete", LastTransitionTime: metav1.Now()}}
	r, _ := newReconciler(fg)
	if s3CredsCondition(t, r, fg) == nil {
		t.Fatal("test setup: seeded condition missing")
	}
	if err := r.reconcileS3CredentialsCondition(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if cond := s3CredsCondition(t, r, fg); cond != nil {
		t.Fatalf("condition = %+v, want it removed once no S3 storage is referenced", cond)
	}
}
