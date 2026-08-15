package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestRotationBlockedSites(t *testing.T) {
	if got := rotationBlockedSites(nil); got != nil {
		t.Fatalf("nil fg = %v, want nil", got)
	}
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			EncryptionAtRest: &v1alpha1.EncryptionAtRestSpec{Enabled: true},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			EncryptionAtRest: &v1alpha1.EncryptionAtRestStatus{
				Sites: []v1alpha1.SiteEncryptionStatus{
					{Name: "pdx", UnsealReason: v1alpha1.UnsealReasonRotation},
					{Name: "iad", UnsealReason: v1alpha1.UnsealReasonClone},
					{Name: "fra", UnsealReason: v1alpha1.UnsealReasonRotation},
				},
			},
		},
	}
	got := rotationBlockedSites(fg)
	if len(got) != 2 || got[0] != "fra" || got[1] != "pdx" {
		t.Fatalf("got %v, want sorted [fra pdx]", got)
	}
	fg.Spec.EncryptionAtRest.Enabled = false
	if got := rotationBlockedSites(fg); got != nil {
		t.Fatalf("encryption off = %v, want nil", got)
	}
}
