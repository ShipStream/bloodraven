//go:build envtest

package envtest

import (
	"fmt"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPITRArchiveSitesValidation(t *testing.T) {
	for i, sites := range [][]string{{"source-old", "removed"}, {"iad", "iad"}, {"../escape"}, {""}, {strings.Repeat("a", 254)}} {
		ns := fmt.Sprintf("pitr-sites-%d", i)
		ensureNamespace(t, ns)
		fg := newVerificationFG(ns)
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"}}, PointInTime: &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", Sites: sites, AllowPartial: true, AllowUnverifiedCrossSite: true}}
		err := k8sClient.Create(ctx, fg)
		if (err == nil) != (i == 0) {
			t.Fatalf("restore sites %v: %v", sites, err)
		}
		if i == 0 {
			stored := &v1alpha1.MysqlFailoverGroup{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, stored); err != nil {
				t.Fatal(err)
			}
			if !stored.Spec.InitFromBackup.PointInTime.AllowUnverifiedCrossSite {
				t.Fatal("restore opt-in was pruned by CRD")
			}
		}
		v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify", Namespace: ns}, Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3", PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: "latest", Sites: sites}}}
		err = k8sClient.Create(ctx, v)
		if (err == nil) != (i == 0) {
			t.Fatalf("verification sites %v: %v", sites, err)
		}
	}
}
