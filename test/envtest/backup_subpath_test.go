//go:build envtest

package envtest

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestBackupPVCSubPathRejectsParentSegments exercises the subPath pattern:
// a ".." segment would place dumps outside the mounted PVC, while nested
// relative paths must keep applying.
func TestBackupPVCSubPathRejectsParentSegments(t *testing.T) {
	ns := createNamespace(t, "backup-subpath")

	withSubPath := func(name, sub string) *v1alpha1.MysqlFailoverGroup {
		fg := newVerificationFG(ns)
		fg.Name = name
		fg.Spec.Backup.Profiles[0].Storage = v1alpha1.BackupStorage{
			Type: v1alpha1.BackupStoragePVC,
			PVC:  &v1alpha1.PVCStorage{ClaimName: "backups", SubPath: sub},
		}
		return fg
	}

	for i, sub := range []string{"..", "../etc", "nightly/../../x", "a/.."} {
		err := k8sClient.Create(ctx, withSubPath("bad-"+string(rune('a'+i)), sub))
		if err == nil {
			t.Errorf("API server accepted subPath %q", sub)
			continue
		}
		if !strings.Contains(err.Error(), "subPath") || !strings.Contains(err.Error(), "should match") {
			t.Errorf("subPath %q rejected for the wrong reason: %v", sub, err)
		}
	}
	for i, sub := range []string{"nightly/site-a", "/nightly", "a..b", "..hidden/x"} {
		if err := k8sClient.Create(ctx, withSubPath("good-"+string(rune('a'+i)), sub)); err != nil {
			t.Errorf("subPath %q should be accepted: %v", sub, err)
		}
	}
}
