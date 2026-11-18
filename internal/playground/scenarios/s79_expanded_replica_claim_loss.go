package scenarios

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	s79OldSize = "1Mi"
	s79Size    = "1Gi"
)

func init() {
	runner.Register(runner.Scenario{
		ID:         "79-expanded-replica-claim-loss",
		Title:      "Recover a replica whose data outgrew its original claim request",
		Hypothesis: "Raising the group request to match an expanded claim preserves that size through claim loss and a fresh clone, while another group still reconciles.",
		Risk:       "high",
		DocLink:    "site/content/docs/6.operations/2.operations.md#pvc-loss-recovery-runbook",
		Timeout:    18 * time.Minute,
		Precheck:   AssertHealthyBaseline,
		Steps: []runner.Step{
			{Phase: runner.PhaseInject, Name: "bootstrap a disposable group with previously enlarged claims", Do: s79Prepare},
			{Phase: runner.PhaseInject, Name: "record the enlarged replica size and seed data larger than its original request", Do: s79Seed},
			{Phase: runner.PhaseVerify, Name: "lose the replica claim, reconcile another group and verify the recovered clone", Do: s79Recover},
		},
		Cleanup: s79Cleanup,
	})
}

// The playground's local-path provisioner has no online resizer. Pre-existing
// claims model the state left by an administrator who expanded them earlier.
func s79Prepare(ctx context.Context, env *runner.Env) error {
	base, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	// A namespaced owner also collects pre-created claims if group creation fails.
	fixture, err := env.Kube.Kubernetes.CoreV1().ConfigMaps(env.Namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "s79-"},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	stashSet(env, "s79Group", fixture.Name)
	stashSet(env, "s79FixtureUID", string(fixture.UID))
	owner := []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: fixture.Name, UID: fixture.UID, BlockOwnerDeletion: new(true)}}
	fg, err := pacedAuxGroupObject(env.Namespace, fixture.Name, pacedSiteZone(base, base.Status.ActiveSite), 4)
	if err != nil {
		return err
	}
	fg.Labels = nil
	fg.OwnerReferences = owner
	for i := range fg.Spec.Sites {
		site := &fg.Spec.Sites[i]
		site.Storage.Size = s79OldSize
		_, err := env.Kube.Kubernetes.CoreV1().PersistentVolumeClaims(env.Namespace).Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pgkube.MysqlPVCName(fg.Name, site.Name), OwnerReferences: owner},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &site.Storage.StorageClassName,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(s79Size)}},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("pre-create enlarged claim for site %s: %w", site.Name, err)
		}
	}
	if err := env.Kube.Controller.Create(ctx, fg); err != nil {
		return err
	}
	return pacedWaitGroupsHealthy(ctx, env, []string{fg.Name}, 10*time.Second)
}

func s79Seed(ctx context.Context, env *runner.Env) error {
	group := stashGet(env, "s79Group")
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, group)
	if err != nil {
		return err
	}
	primary := fg.Status.ActiveSite
	replica, err := PeerOf(fg, primary)
	if err != nil {
		return err
	}
	stashSet(env, "s79Primary", primary)
	stashSet(env, "s79Replica", replica)
	for i, site := range fg.Spec.Sites {
		if site.Name != replica {
			continue
		}
		pvc, err := env.Kube.Kubernetes.CoreV1().PersistentVolumeClaims(env.Namespace).Get(ctx, pgkube.MysqlPVCName(group, replica), metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !metav1.IsControlledBy(pvc, fg) || pvc.Spec.Resources.Requests.Storage().Cmp(resource.MustParse(s79Size)) != 0 {
			return fmt.Errorf("site %s did not adopt its expanded claim at %s", replica, s79Size)
		}
		stashSet(env, "s79OldUID", string(pvc.UID))
		stashSet(env, "s79OldVolume", pvc.Spec.VolumeName)
		path := fmt.Sprintf("/spec/sites/%d/storage/size", i)
		if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, group, []pgkube.JSONPatchOp{
			{Op: "test", Path: path, Value: s79OldSize}, {Op: "replace", Path: path, Value: s79Size},
		}); err != nil {
			return err
		}
	}
	db, err := pgmysql.Open(ctx, env.Kube, env.Namespace, group, primary, env.Creds)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	payload := make([]byte, 2<<20)
	if _, err := rand.Read(payload); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, "CREATE DATABASE s79; CREATE TABLE s79.proof (id INT PRIMARY KEY, payload LONGBLOB NOT NULL)"); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, "INSERT INTO s79.proof VALUES (1, ?)", payload); err != nil {
		return err
	}
	stashSet(env, "s79SHA", fmt.Sprintf("%x", sha256.Sum256(payload)))
	aux := *env
	aux.FG = group
	return waitForReplicaGTID(ctx, &aux, primary, replica)
}

func s79Recover(ctx context.Context, env *runner.Env) error {
	group, primary, replica := stashGet(env, "s79Group"), stashGet(env, "s79Primary"), stashGet(env, "s79Replica")
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, group)
	if err != nil {
		return err
	}
	if fg.Status.ActiveSite != primary {
		return fmt.Errorf("active site changed to %s before loss of replica %s", fg.Status.ActiveSite, replica)
	}
	operator, err := env.Kube.FindPodWithLabel(ctx, env.Namespace, pgmetrics.OperatorPodSelector)
	if err != nil {
		return err
	}
	actions := pgchaos.New(env.Kube, env.Namespace, group)
	if err := actions.WipeSiteData(ctx, replica, group); err != nil {
		return err
	}
	if err := s79CheckOtherGroup(ctx, env); err != nil {
		return err
	}
	if err := actions.Revert(ctx); err != nil {
		return err
	}
	if err := pacedWaitGroupsHealthy(ctx, env, []string{group, env.FG}, 10*time.Second); err != nil {
		return err
	}
	pvc, err := env.Kube.Kubernetes.CoreV1().PersistentVolumeClaims(env.Namespace).Get(ctx, pgkube.MysqlPVCName(group, replica), metav1.GetOptions{})
	if err != nil {
		return err
	}
	desired := resource.MustParse(s79Size)
	if string(pvc.UID) == stashGet(env, "s79OldUID") || pvc.Spec.VolumeName == stashGet(env, "s79OldVolume") ||
		!metav1.IsControlledBy(pvc, fg) ||
		pvc.Spec.Resources.Requests.Storage().Cmp(desired) != 0 || pvc.Status.Capacity.Storage().Cmp(desired) < 0 {
		return fmt.Errorf("replacement claim %s UID %s has request %s, capacity %s and owners %v",
			pvc.Name, pvc.UID, pvc.Spec.Resources.Requests.Storage(), pvc.Status.Capacity.Storage(), pvc.OwnerReferences)
	}
	if err := s79VerifyClone(ctx, env, group, primary, replica); err != nil {
		return err
	}
	after, err := env.Kube.FindPodWithLabel(ctx, env.Namespace, pgmetrics.OperatorPodSelector)
	if err != nil {
		return err
	}
	if operator.UID != after.UID || len(operator.Status.ContainerStatuses) == 0 || len(after.Status.ContainerStatuses) == 0 ||
		operator.Status.ContainerStatuses[0].ContainerID != after.Status.ContainerStatuses[0].ContainerID {
		return fmt.Errorf("operator process changed during recovery (%s -> %s)", operator.Name, after.Name)
	}
	env.Capture.Measure(fmt.Sprintf("replica %s cloned the 2 MiB payload onto replacement claim %s at %s", replica, pvc.UID, desired.String()))
	return nil
}

func s79VerifyClone(ctx context.Context, env *runner.Env, group, primary, replica string) error {
	db, err := pgmysql.Open(ctx, env.Kube, env.Namespace, group, replica, env.Creds)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	state, err := db.ScalarString(ctx, "SELECT STATE FROM performance_schema.clone_status LIMIT 1")
	if err != nil || state != "Completed" {
		return fmt.Errorf("replacement replica clone state %q, error %v", state, err)
	}
	oldRequest := resource.MustParse(s79OldSize)
	digest, err := db.ScalarString(ctx, "SELECT SHA2(payload, 256) FROM s79.proof WHERE id=1 AND OCTET_LENGTH(payload)>?", oldRequest.Value())
	if err != nil || digest != stashGet(env, "s79SHA") {
		return fmt.Errorf("replacement replica payload digest %q, error %v", digest, err)
	}
	status, err := db.ShowReplicaStatus(ctx)
	if err != nil || !status.IORunning || !status.SQLRunning {
		return fmt.Errorf("replacement replica threads IO=%t SQL=%t, error %v", status.IORunning, status.SQLRunning, err)
	}
	writer, err := pgmysql.Open(ctx, env.Kube, env.Namespace, group, primary, env.Creds)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close() }()
	if _, err := writer.Exec(ctx, "INSERT INTO s79.proof VALUES (2, 'after clone')"); err != nil {
		return err
	}
	aux := *env
	aux.FG = group
	if err := waitForReplicaGTID(ctx, &aux, primary, replica); err != nil {
		return err
	}
	n, err := db.ScalarInt(ctx, "SELECT COUNT(*) FROM s79.proof WHERE id=2 AND payload='after clone'")
	if err != nil || n != 1 {
		return fmt.Errorf("post-clone row on replica: count=%d, error %v", n, err)
	}
	return nil
}

// Introduce metadata drift on the unrelated group's site Service while the
// auxiliary replica is down. The Service's routing fields remain untouched.
func s79CheckOtherGroup(ctx context.Context, env *runner.Env) error {
	base, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	services := env.Kube.Kubernetes.CoreV1().Services(env.Namespace)
	name := pgkube.MysqlDeploymentName(env.FG, base.Status.ActiveSite)
	patch := []byte(`[{"op":"test","path":"/metadata/labels/app.kubernetes.io~1managed-by","value":"bloodraven"},{"op":"replace","path":"/metadata/labels/app.kubernetes.io~1managed-by","value":"s79"}]`)
	if _, err := services.Patch(ctx, name, types.JSONPatchType, patch, metav1.PatchOptions{}); err != nil {
		return err
	}
	err = wait.PollUntilContextTimeout(ctx, time.Second, 45*time.Second, true, func(ctx context.Context) (bool, error) {
		svc, err := services.Get(ctx, name, metav1.GetOptions{})
		return err == nil && svc.Labels["app.kubernetes.io/managed-by"] == "bloodraven", err
	})
	if err != nil {
		return fmt.Errorf("operator did not repair Service %s while the other group's replica was down: %w", name, err)
	}
	return AssertHealthyBaseline(ctx, env)
}

func s79Cleanup(ctx context.Context, env *runner.Env) error {
	name := stashGet(env, "s79Group")
	if name == "" {
		return nil
	}
	fixtures := env.Kube.Kubernetes.CoreV1().ConfigMaps(env.Namespace)
	uid := types.UID(stashGet(env, "s79FixtureUID"))
	if err := fixtures.Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: new(metav1.DeletePropagationForeground),
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := fixtures.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}
