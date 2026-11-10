package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func ownedPVC(name string, class *string, request string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: class,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(request),
			}},
		},
	}
}

// TestOwnedBackupPVCMismatch is A-18's comparison of a profile's owned claim
// with pvc.provisioned: it never fires on a correct claim, refuses another
// explicit class or a smaller request, and accepts a larger one.
func TestOwnedBackupPVCMismatch(t *testing.T) {
	for _, tc := range []struct {
		name         string
		class        *string
		size         v1alpha1.StorageQuantity
		pvcClass     *string
		pvcRequest   string
		wantRefusal  string
		wantExpanded bool
	}{
		{name: "nil class matches the default the cluster filled in", size: "10Gi", pvcClass: new("standard"), pvcRequest: "10Gi"},
		{name: "nil class matches a claim without a class", size: "10Gi", pvcRequest: "10Gi"},
		{name: "explicit class matches", class: new("fast"), size: "10Gi", pvcClass: new("fast"), pvcRequest: "10Gi"},
		{name: "size compared by value", class: new("fast"), size: "10240Mi", pvcClass: new("fast"), pvcRequest: "10Gi"},
		{name: "explicit class differs", class: new("fast"), size: "10Gi", pvcClass: new("slow"), pvcRequest: "10Gi", wantRefusal: `storageClassName "slow"`},
		{name: "explicit class against a claim without one", class: new("fast"), size: "10Gi", pvcRequest: "10Gi", wantRefusal: `storageClassName ""`},
		{name: "smaller request", size: "20Gi", pvcClass: new("fast"), pvcRequest: "10Gi", wantRefusal: "less than pvc.provisioned.size 20Gi"},
		{name: "larger request is an accepted expansion", size: "10Gi", pvcClass: new("fast"), pvcRequest: "50Gi", wantExpanded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusal, expanded := ownedBackupPVCMismatch(&v1alpha1.ProvisionedVolumeSpec{StorageClassName: tc.class, Size: tc.size},
				ownedPVC("mysql-lion-backup-local", tc.pvcClass, tc.pvcRequest))
			if tc.wantRefusal == "" && refusal != "" {
				t.Fatalf("refused a matching claim: %s", refusal)
			}
			if tc.wantRefusal != "" && !strings.Contains(refusal, tc.wantRefusal) {
				t.Fatalf("refusal %q, want it to contain %q", refusal, tc.wantRefusal)
			}
			if expanded != tc.wantExpanded {
				t.Fatalf("expanded = %v, want %v", expanded, tc.wantExpanded)
			}
		})
	}
}

// A backup of a profile whose owned claim was created for an earlier
// profile of the same name fails BackupStorageMismatch, without a Job; one
// whose claim was expanded runs, with a Normal Event.
func TestBackupRefusedOnMismatchedOwnedPVC(t *testing.T) {
	setup := func(t *testing.T, pvc *corev1.PersistentVolumeClaim) (*MysqlBackupReconciler, *v1alpha1.MysqlBackup, func() *v1alpha1.MysqlBackup) {
		fg := fgWithBackup()
		fg.UID = "lion-uid"
		fg.Status.ActiveSite = "iad"
		fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: v1alpha1.SiteStateWritable}}
		mb := backupCR("local", fg.Name, "daily-local")
		r, c := newBackupReconciler(t, fg, mb, operatorCredsSecret(), pvc)
		r.Recorder = record.NewFakeRecorder(50)
		return r, mb, func() *v1alpha1.MysqlBackup { return getBackup(t, c, mb) }
	}
	name := ownedBackupPVCName("lion", "daily-local")

	r, mb, get := setup(t, ownedPVC(name, new("slow"), "50Gi"))
	reconcileUntilStable(t, r, mb.Name)
	got := get()
	if got.Status.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(conditionReasons(got.Status.Conditions), v1alpha1.ReasonBackupStorageMismatch) {
		t.Fatalf("phase %s conditions %v, want Failed/%s", got.Status.Phase, got.Status.Conditions, v1alpha1.ReasonBackupStorageMismatch)
	}
	if got.Status.JobName != "" {
		t.Fatalf("a Job %s was created for a mismatched claim", got.Status.JobName)
	}
	if countEvents(r.Recorder.(*record.FakeRecorder), v1alpha1.EventBackupStorageMismatch) != 1 {
		t.Fatal("no BackupStorageMismatch Event")
	}

	r, mb, get = setup(t, ownedPVC(name, new("fast"), "80Gi"))
	reconcileUntilStable(t, r, mb.Name)
	if got := get(); got.Status.Phase == v1alpha1.BackupPhaseFailed {
		t.Fatalf("an expanded claim failed the backup: %+v", got.Status.Conditions)
	}
	if countEvents(r.Recorder.(*record.FakeRecorder), v1alpha1.EventBackupStorageExpansionAccepted) != 1 {
		t.Fatal("no BackupStorageExpansionAccepted Event")
	}
}
