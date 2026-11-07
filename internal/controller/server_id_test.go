package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// podCommandLines joins every container's and init container's command and
// args, so a test can look for a rendered flag wherever it lands.
func podCommandLines(spec corev1.PodSpec) string {
	var b strings.Builder
	for _, cs := range [][]corev1.Container{spec.InitContainers, spec.Containers} {
		for _, c := range cs {
			b.WriteString(strings.Join(c.Command, " "))
			b.WriteString(" ")
			b.WriteString(strings.Join(c.Args, " "))
			b.WriteString("\n")
		}
	}
	return b.String()
}

// A-08: server_id comes from spec.sites[].serverID, not from the site's
// position, and the largest MySQL server_id renders unchanged (no int32
// wrap) in mysqld's --server-id, the conf.d server-id, and the server the
// backup stamp expects. The sites are declared in reverse id order, so a
// positional derivation (101 + index) cannot produce either value.
func TestServerID_ExplicitIDRendersUnchanged(t *testing.T) {
	ids := map[string]int64{"dc2": 7, "dc1": v1alpha1.MaxServerID}

	fg := newTestFG()
	fg.Spec.Sites[0], fg.Spec.Sites[1] = fg.Spec.Sites[1], fg.Spec.Sites[0]
	for i := range fg.Spec.Sites {
		fg.Spec.Sites[i].ServerID = ids[fg.Spec.Sites[i].Name]
	}
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for site, id := range ids {
		var deploy appsv1.Deployment
		if err := c.Get(context.Background(), types.NamespacedName{Name: "mysql-lion-" + site, Namespace: fg.Namespace}, &deploy); err != nil {
			t.Fatalf("deployment %s: %v", site, err)
		}
		lines := podCommandLines(deploy.Spec.Template.Spec)
		if !strings.Contains(lines, fmt.Sprintf("--server-id=%d ", id)) && !strings.Contains(lines, fmt.Sprintf("--server-id=%d\n", id)) {
			t.Errorf("%s: mysqld does not get --server-id=%d:\n%s", site, id, lines)
		}
		if !strings.Contains(lines, fmt.Sprintf(`server-id=%d\n`, id)) {
			t.Errorf("%s: conf.d server-id is not %d:\n%s", site, id, lines)
		}
	}

	bfg := identityFG()
	for i := range bfg.Spec.Sites {
		bfg.Spec.Sites[i].ServerID = map[string]int64{"iad": v1alpha1.MaxServerID, "pdx": 7}[bfg.Spec.Sites[i].Name]
	}
	for site, id := range map[string]uint32{"iad": v1alpha1.MaxServerID, "pdx": 7} {
		stamp, err := backupStampTemplate(bfg, identityBackup(), bfg.Spec.Backup.Profiles[0], site, testPITRRender(bfg).ArchiveID)
		if err != nil {
			t.Fatal(err)
		}
		if stamp.ServerID != id {
			t.Errorf("%s: stamp template expects server_id %d, want %d", site, stamp.ServerID, id)
		}
		job, err := BuildBackupJob(BackupJobInputs{
			DumpKeySecretName: testDumpKeySecret,
			PITRArchiveID:     testPITRRender(bfg).ArchiveID,
			FailoverGroup:     bfg, Profile: bfg.Spec.Backup.Profiles[0], Backup: identityBackup(), SourceSite: site,
			CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := stampTemplateFromContainer(t, job.Spec.Template.Spec.InitContainers[0]).ServerID; got != id {
			t.Errorf("%s: backup Job pins server_id %d, want %d", site, got, id)
		}
	}
}

// A server_id outside MySQL's range (only reachable by an object that
// bypassed admission) stops the render and the backup stamp instead of
// producing a wrong id.
func TestServerID_OutOfRangeRefused(t *testing.T) {
	for _, id := range []int64{0, -1, v1alpha1.MaxServerID + 1} {
		fg := newTestFG()
		fg.Spec.Sites[0].ServerID = id
		r, _ := newReconciler(fg)
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}})
		if err == nil || !strings.Contains(err.Error(), "serverID") {
			t.Errorf("serverID %d: reconcile error = %v, want a serverID refusal", id, err)
		}
		bfg := identityFG()
		bfg.Spec.Sites[0].ServerID = id
		if _, err := backupStampTemplate(bfg, identityBackup(), bfg.Spec.Backup.Profiles[0], bfg.Spec.Sites[0].Name, ""); err == nil {
			t.Errorf("serverID %d: stamp template accepted", id)
		}
	}
}
