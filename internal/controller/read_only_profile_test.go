package controller

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// D-10, Δ23 (row 40): a schedule of a read-only profile renders no CronJob
// (an existing one is pruned) and warns once per change; it comes back when
// the profile is writable again.
func TestScheduleOfReadOnlyProfileRefused(t *testing.T) {
	ctx := context.Background()
	fg := fgWithBackup()
	fg.UID = "lion-uid"
	fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{{Name: "nightly", ProfileName: "nightly-s3", Schedule: "0 2 * * *"}}
	r, c := newReconciler(fg)
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	cronJob := func() bool {
		var cj batchv1.CronJob
		return c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: scheduleCronJobName(fg.Name, "nightly")}, &cj) == nil
	}
	if err := r.reconcileBackupSchedules(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if !cronJob() {
		t.Fatal("writable profile: no CronJob")
	}

	e := bindTestPITRArchive(t, fg, "nightly-s3")
	e.State, e.Reason = v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveForeign
	for i := 0; i < 3; i++ {
		if err := r.reconcileBackupSchedules(ctx, fg); err != nil {
			t.Fatal(err)
		}
	}
	if cronJob() {
		t.Fatal("read-only profile: CronJob kept")
	}
	if n := countEvents(rec, "Warning "+eventReasonBackupScheduleRefused); n != 1 {
		t.Fatalf("%d %s Events, want one per change", n, eventReasonBackupScheduleRefused)
	}

	fg.Status.PITRArchiveBindings = nil
	if err := r.reconcileBackupSchedules(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if !cronJob() {
		t.Fatal("profile writable again: no CronJob")
	}
}

// Δ16: the encryption check uses the one TrimPassphrase, leading
// whitespace included.
func TestValidateEncryptionSecretTrimsLeadingWhitespace(t *testing.T) {
	fg := fgWithEncryptedBackup()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "ns"},
		Data: map[string][]byte{"passphrase": []byte("\n\t  " + strings.Repeat("k", 13) + "  \n")}}
	r := &MysqlFailoverGroupReconciler{Client: fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(secret).Build(), Scheme: testScheme()}
	err := r.validateEncryptionSecret(context.Background(), fg, &fg.Spec.Backup.Profiles[0])
	if err == nil || !strings.Contains(err.Error(), "13-byte passphrase") {
		t.Fatalf("leading whitespace counted toward the length: %v", err)
	}
}
