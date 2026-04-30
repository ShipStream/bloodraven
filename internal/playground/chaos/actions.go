// Package chaos exposes the failure injections used by scenarios.
// Each Apply* method also pushes a reverter onto the Actions stack so
// Cleanup can roll back in reverse order, even on assertion failure.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
)

// Actions is the per-scenario chaos handle. Methods are not safe for
// concurrent use; scenarios are sequential by design.
type Actions struct {
	K         *pgkube.Client
	Namespace string
	FG        string

	mu      sync.Mutex
	revStack []reverter
}

type reverter struct {
	what string
	fn   func(context.Context) error
}

// New builds an Actions bound to a kube client, namespace, and FG name.
func New(k *pgkube.Client, namespace, fg string) *Actions {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	if fg == "" {
		fg = pgkube.FailoverGroupName
	}
	return &Actions{K: k, Namespace: namespace, FG: fg}
}

func (a *Actions) push(what string, fn func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revStack = append(a.revStack, reverter{what: what, fn: fn})
}

// Revert runs the revert stack in LIFO order, joining errors. Always
// drains the stack before returning so a second Revert is a no-op.
func (a *Actions) Revert(ctx context.Context) error {
	a.mu.Lock()
	stack := a.revStack
	a.revStack = nil
	a.mu.Unlock()
	var errs []error
	for i := len(stack) - 1; i >= 0; i-- {
		if err := stack[i].fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("revert %s: %w", stack[i].what, err))
		}
	}
	return errors.Join(errs...)
}

// PendingReverts returns the descriptions of currently-pending
// reverters. Useful for logging.
func (a *Actions) PendingReverts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.revStack))
	for i, r := range a.revStack {
		out[i] = r.what
	}
	return out
}

// ScaleSiteToZero scales a site's MySQL Deployment to 0, holding the
// site offline past the brief Deployment-respawn window. Reverter
// scales back to 1.
func (a *Actions) ScaleSiteToZero(ctx context.Context, site string) error {
	dep := pgkube.MysqlDeploymentName(a.FG, site)
	if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 0); err != nil {
		return fmt.Errorf("scale %s to 0: %w", dep, err)
	}
	a.push(fmt.Sprintf("scale %s back to 1", dep), func(ctx context.Context) error {
		return a.K.ScaleDeployment(ctx, a.Namespace, dep, 1)
	})
	return nil
}

// DeleteSitePod force-deletes the site's MySQL pod (Deployment will
// respawn it within seconds). No reverter — the cluster recovers on
// its own.
func (a *Actions) DeleteSitePod(ctx context.Context, site string) error {
	zero := int64(0)
	return a.K.DeleteSitePod(ctx, a.Namespace, a.FG, site, &zero)
}

// ScaleOperatorToZero scales the operator Deployment to 0 replicas,
// holding the operator offline until Revert restores replicas=1. Used
// by self-fencing scenarios that need a sustained operator outage past
// the sidecar lease timeout (kill-pod isn't enough — the Deployment
// respawns the operator within seconds).
//
// The operator deployment name is hard-coded to "bloodraven", which
// matches the playground Helm release. If the playground is ever
// installed under a non-default release name, this needs updating.
func (a *Actions) ScaleOperatorToZero(ctx context.Context) error {
	const dep = "bloodraven"
	if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 0); err != nil {
		return fmt.Errorf("scale %s to 0: %w", dep, err)
	}
	a.push(fmt.Sprintf("scale %s back to 1", dep), func(ctx context.Context) error {
		return a.K.ScaleDeployment(ctx, a.Namespace, dep, 1)
	})
	return nil
}

// KillOperator force-deletes every operator pod (label
// app.kubernetes.io/name=bloodraven). The Deployment respawns the pod
// on its own, so no reverter is pushed — the cluster recovers without
// operator intervention. Mirrors `chaos.sh kill-operator`.
func (a *Actions) KillOperator(ctx context.Context) error {
	pods, err := a.K.Kubernetes.CoreV1().Pods(a.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: pgmetrics.OperatorPodSelector,
	})
	if err != nil {
		return fmt.Errorf("list operator pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no operator pod found (selector=%s)", pgmetrics.OperatorPodSelector)
	}
	zero := int64(0)
	for i := range pods.Items {
		name := pods.Items[i].Name
		if err := a.K.Kubernetes.CoreV1().Pods(a.Namespace).Delete(ctx, name, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		}); err != nil {
			return fmt.Errorf("delete operator pod %s: %w", name, err)
		}
	}
	return nil
}

// PartitionSite applies a deny-all NetworkPolicy that blocks the
// site's MySQL pod from any pod-network communication. Reverter
// removes the NP.
func (a *Actions) PartitionSite(ctx context.Context, site string) error {
	if err := a.K.ApplyDenyAllNetworkPolicy(ctx, a.Namespace, a.FG, site); err != nil {
		return fmt.Errorf("apply NetworkPolicy for site %s: %w", site, err)
	}
	a.push(fmt.Sprintf("remove NetworkPolicy for %s", site), func(ctx context.Context) error {
		return a.K.RemoveDenyAllNetworkPolicy(ctx, a.Namespace, site)
	})
	return nil
}

// AnnotatePlannedFailover sets the bloodraven.shipstream.io/planned-failover
// annotation to the named target site. Reverter clears the annotation.
//
// The annotation is the public CR-driven trigger documented on
// PlannedFailoverSpec.OnCooldown.
func (a *Actions) AnnotatePlannedFailover(ctx context.Context, target string) error {
	const key = "bloodraven.shipstream.io/planned-failover"
	if err := a.K.AnnotateMFG(ctx, a.Namespace, key, target); err != nil {
		return fmt.Errorf("set planned-failover annotation: %w", err)
	}
	a.push("clear planned-failover annotation", func(ctx context.Context) error {
		return a.K.AnnotateMFG(ctx, a.Namespace, key, "")
	})
	return nil
}

// KillSidecarPID1 attaches an ephemeral container to the named site's
// MySQL pod that targets the sidecar's PID namespace and runs `kill 1`.
// SIGTERM is the default — the kernel ignores SIGKILL on PID 1 from
// within the same PID namespace, so signal 9 silently fails. Sane Go
// binaries handle SIGTERM and exit, which causes the kubelet to
// restart the sidecar container (kept the pod alive, so MySQL on
// port 3306 is unaffected).
//
// No reverter is pushed: ephemeral containers cannot be removed once
// added, and the kill is one-shot. The unique name suffix lets the
// same scenario run twice on the same pod without name collision
// (ephemeral container names must be unique within a pod).
//
// Returns the pod name we mutated so callers can poll the sidecar's
// containerStatus.restartCount on the same pod identity.
func (a *Actions) KillSidecarPID1(ctx context.Context, site string) (string, error) {
	pod, err := a.K.GetSiteMysqlPod(ctx, a.Namespace, a.FG, site)
	if err != nil {
		return "", err
	}
	ecName := fmt.Sprintf("chaos-s15-killer-%d", time.Now().UnixMilli())
	ec := corev1.EphemeralContainer{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name:            ecName,
			Image:           "busybox:1.36",
			Command:         []string{"sh", "-c", "kill 1"},
			ImagePullPolicy: corev1.PullIfNotPresent,
		},
		TargetContainerName: "sidecar",
	}
	if err := a.K.AddEphemeralContainer(ctx, a.Namespace, pod.Name, ec); err != nil {
		return "", fmt.Errorf("add ephemeral container to %s: %w", pod.Name, err)
	}
	return pod.Name, nil
}

// LabelNode adds labels to a node via JSON merge patch and registers
// a reverter that removes only those keys (other labels are
// untouched). Used by placement scenarios that simulate multi-tenant
// nodes serving more than one failover group.
func (a *Actions) LabelNode(ctx context.Context, name string, labels map[string]string) error {
	if err := a.K.AddNodeLabels(ctx, name, labels); err != nil {
		return fmt.Errorf("label node %s: %w", name, err)
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	a.push(fmt.Sprintf("remove %d labels from node %s", len(keys), name), func(ctx context.Context) error {
		return a.K.RemoveNodeLabels(ctx, name, keys)
	})
	return nil
}

// CreateCanaryPod applies a Pod manifest to the namespace and
// registers a reverter that deletes it. The pod is expected to be a
// short-lived sleep canary, not a managed workload.
func (a *Actions) CreateCanaryPod(ctx context.Context, namespace string, pod *corev1.Pod) error {
	if err := a.K.CreatePod(ctx, namespace, pod); err != nil {
		return fmt.Errorf("create canary pod %s/%s: %w", namespace, pod.Name, err)
	}
	name := pod.Name
	a.push(fmt.Sprintf("delete canary pod %s/%s", namespace, name), func(ctx context.Context) error {
		return a.K.DeletePodByName(ctx, namespace, name)
	})
	return nil
}

// KillDragonflyPod force-deletes the named site's Dragonfly pod. The
// StatefulSet respawns it within seconds. No reverter — recovery is
// autonomous (and the regression target of scenario D7).
func (a *Actions) KillDragonflyPod(ctx context.Context, site string) error {
	zero := int64(0)
	return a.K.DeleteSiteDragonflyPod(ctx, a.Namespace, a.FG, site, &zero)
}

// ScaleDragonflyToZero scales a site's Dragonfly StatefulSet to 0
// replicas, holding the site's Dragonfly offline past the brief
// StatefulSet-respawn window. Reverter scales back to 1.
//
// Mirrors ScaleSiteToZero for MySQL but on the Dragonfly StatefulSet.
// Used by emergency-failover scenarios that need a sustained
// Dragonfly outage to prove MySQL recovery does not depend on
// Dragonfly availability.
func (a *Actions) ScaleDragonflyToZero(ctx context.Context, site string) error {
	if err := a.K.ScaleDragonflyStatefulSet(ctx, a.Namespace, a.FG, site, 0); err != nil {
		return fmt.Errorf("scale dragonfly %s to 0: %w", site, err)
	}
	a.push(fmt.Sprintf("scale dragonfly %s back to 1", site), func(ctx context.Context) error {
		return a.K.ScaleDragonflyStatefulSet(ctx, a.Namespace, a.FG, site, 1)
	})
	return nil
}

// ScaleAllDragonflyToZero scales every site's Dragonfly StatefulSet
// to 0. Used by scenario D5 (emergency MySQL failover with all
// Dragonfly unreachable). Reverter restores each scaled-down site.
func (a *Actions) ScaleAllDragonflyToZero(ctx context.Context) error {
	mfg, err := a.K.GetMFG(ctx, a.Namespace)
	if err != nil {
		return fmt.Errorf("read MFG for scale-all-dragonfly: %w", err)
	}
	for _, s := range mfg.Spec.Sites {
		if err := a.ScaleDragonflyToZero(ctx, s.Name); err != nil {
			return err
		}
	}
	return nil
}

// GlobalRecover is the safety-net cleanup the runner runs after every
// scenario, regardless of outcome. Mirrors `chaos.sh recover`:
// removes every chaos-partition NetworkPolicy and scales every MySQL
// site back to 1 replica, plus every Dragonfly StatefulSet back to
// 1 replica. Idempotent.
func (a *Actions) GlobalRecover(ctx context.Context) error {
	var errs []error
	if err := a.K.RemoveAllChaosNetworkPolicies(ctx, a.Namespace); err != nil {
		errs = append(errs, fmt.Errorf("remove chaos NetworkPolicies: %w", err))
	}
	mfg, err := a.K.GetMFG(ctx, a.Namespace)
	if err == nil {
		for _, s := range mfg.Spec.Sites {
			dep := pgkube.MysqlDeploymentName(a.FG, s.Name)
			if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 1); err != nil {
				errs = append(errs, fmt.Errorf("scale %s back to 1: %w", dep, err))
			}
			// Dragonfly is only reconciled when spec.dragonfly is
			// enabled. Calling ScaleDragonflyStatefulSet on a missing
			// StatefulSet returns a NotFound that we want to surface
			// so a misconfigured playground is loud.
			if mfg.Spec.Dragonfly != nil && mfg.Spec.Dragonfly.Enabled {
				if err := a.K.ScaleDragonflyStatefulSet(ctx, a.Namespace, a.FG, s.Name, 1); err != nil {
					errs = append(errs, fmt.Errorf("scale dragonfly %s back to 1: %w", s.Name, err))
				}
			}
		}
	} else {
		errs = append(errs, fmt.Errorf("read MFG for global recover: %w", err))
	}
	return errors.Join(errs...)
}
