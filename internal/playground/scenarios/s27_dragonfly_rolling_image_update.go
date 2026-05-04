package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario27DragonflyRollingImageUpdate())
}

func scenario27DragonflyRollingImageUpdate() runner.Scenario {
	return runner.Scenario{
		ID:    "27-dragonfly-rolling-image-update",
		Title: "Dragonfly rolling image update promotes updated replica first",
		Hypothesis: "Patching spec.dragonfly.image to a digest reference rolls the non-active Dragonfly pod first, " +
			"promotes that updated replica, and only then rolls the old active pod without both Dragonfly pods being unavailable.",
		Risk:     "medium",
		DocLink:  "PLANS-Dragonfly-Chaos-Scenarios.md (D6)",
		Timeout:  8 * time.Minute,
		Precheck: AssertDragonflyHealthyBaseline,
		Steps: []runner.Step{
			selectDragonflyDigestImageForRollingUpdate(),
			patchDragonflyImageForRollingUpdate(),
			observeDragonflyRollingImageUpdate(),
		},
	}
}

func selectDragonflyDigestImageForRollingUpdate() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "select cached Dragonfly digest image",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Spec.Dragonfly == nil || mfg.Spec.Dragonfly.Image == "" {
				return fmt.Errorf("spec.dragonfly.image is not set")
			}
			current := mfg.Spec.Dragonfly.Image
			for _, site := range mfg.Spec.Sites {
				pod, err := env.Kube.GetSiteDragonflyPod(ctx, env.Namespace, env.FG, site.Name)
				if err != nil {
					return err
				}
				target, err := dragonflyDigestImageFromPod(current, pod)
				if err != nil {
					continue
				}
				if target != current {
					env.Capture.Note(fmt.Sprintf("patching Dragonfly image from %s to cached digest %s", current, target))
					if err := ctxStash(ctx, env, "s27TargetImage", target); err != nil {
						return err
					}
					return nil
				}
			}
			return fmt.Errorf("no Dragonfly pod reported a digest image different from spec image %q", current)
		},
	}
}

func dragonflyDigestImageFromPod(current string, pod *corev1.Pod) (string, error) {
	if strings.Contains(current, "@sha256:") {
		return "", fmt.Errorf("current image is already digest-pinned")
	}
	for _, st := range pod.Status.ContainerStatuses {
		if st.Name != "dragonfly" {
			continue
		}
		imageID := strings.TrimPrefix(st.ImageID, "docker-pullable://")
		imageID = strings.TrimPrefix(imageID, "containerd://")
		if strings.Contains(imageID, "@sha256:") {
			return imageID, nil
		}
	}
	return "", fmt.Errorf("pod %s has no dragonfly digest imageID", pod.Name)
}

func patchDragonflyImageForRollingUpdate() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "patch spec.dragonfly.image",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s27TargetImage")
			if target == "" {
				return fmt.Errorf("missing s27TargetImage")
			}
			return env.Chaos.PatchDragonflyImage(ctx, target)
		},
	}
}

func observeDragonflyRollingImageUpdate() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "Dragonfly rollout stays one pod at a time",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s27TargetImage")
			if target == "" {
				return fmt.Errorf("missing s27TargetImage")
			}
			waitCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
			defer cancel()
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			var last string
			for {
				ok, msg, err := dragonflyRollingImageUpdateComplete(waitCtx, env, target)
				if err != nil {
					return err
				}
				last = msg
				if ok {
					return nil
				}
				select {
				case <-waitCtx.Done():
					return fmt.Errorf("wait for Dragonfly rolling image update: %w (last: %s)", waitCtx.Err(), last)
				case <-tick.C:
				}
			}
		},
	}
}

func dragonflyRollingImageUpdateComplete(ctx context.Context, env *runner.Env, target string) (bool, string, error) {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return false, "", err
	}
	unavailable := 0
	allPodsTarget := true
	allStatefulSetsTarget := true
	for _, site := range mfg.Spec.Sites {
		pod, err := env.Kube.GetSiteDragonflyPod(ctx, env.Namespace, env.FG, site.Name)
		if err != nil {
			unavailable++
			allPodsTarget = false
		} else {
			if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
				unavailable++
			}
			if len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Image != target {
				allPodsTarget = false
			}
		}
		sts, err := env.Kube.Kubernetes.AppsV1().StatefulSets(env.Namespace).Get(ctx, pgkube.DragonflyStatefulSetName(env.FG, site.Name), metav1.GetOptions{})
		if err != nil {
			return false, "", err
		}
		if !dragonflyStatefulSetAtImage(sts, target) {
			allStatefulSetsTarget = false
		}
	}
	if unavailable > 1 {
		return false, "", fmt.Errorf("Dragonfly rolling image update made %d pods unavailable at once", unavailable)
	}
	if allPodsTarget && allStatefulSetsTarget {
		if err := assertDragonflyBaselineHealthy(mfg); err != nil {
			return false, err.Error(), nil
		}
		return true, "all Dragonfly pods are ready on target image", nil
	}
	return false, fmt.Sprintf("unavailable=%d podsTarget=%t statefulSetsTarget=%t", unavailable, allPodsTarget, allStatefulSetsTarget), nil
}

func dragonflyStatefulSetAtImage(sts *appsv1.StatefulSet, target string) bool {
	if len(sts.Spec.Template.Spec.Containers) == 0 || sts.Spec.Template.Spec.Containers[0].Image != target {
		return false
	}
	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	return sts.Status.ObservedGeneration >= sts.Generation &&
		sts.Status.UpdatedReplicas == desired &&
		sts.Status.ReadyReplicas == desired
}
