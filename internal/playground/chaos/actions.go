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
	pgrustfs "github.com/shipstream/bloodraven/internal/playground/rustfs"
)

// Actions is the per-scenario chaos handle. Methods are not safe for
// concurrent use; scenarios are sequential by design.
type Actions struct {
	K         *pgkube.Client
	Namespace string
	FG        string

	mu       sync.Mutex
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

// ScaleSiteToOne brings a site's MySQL Deployment back without draining the
// scenario cleanup stack. Use this when an injection needs to reintroduce a
// scaled-down site while keeping other temporary chaos state in place.
func (a *Actions) ScaleSiteToOne(ctx context.Context, site string) error {
	dep := pgkube.MysqlDeploymentName(a.FG, site)
	if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 1); err != nil {
		return fmt.Errorf("scale %s to 1: %w", dep, err)
	}
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

// WaitForOperatorGone polls until no operator pod exists at all.
//
// ScaleOperatorToZero only writes replicas=0 to the Deployment spec. The
// running pod then works through its graceful-termination window, and
// until it exits it keeps reconciling — so a scenario that needs the
// operator genuinely absent (rather than merely scheduled for removal)
// must wait for this, or the operator it thinks it removed can still act.
func (a *Actions) WaitForOperatorGone(ctx context.Context) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var lastErr error
	remaining := -1
	for {
		pods, err := a.K.Kubernetes.CoreV1().Pods(a.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: pgmetrics.OperatorPodSelector,
		})
		if err != nil {
			lastErr = err
		} else {
			remaining = len(pods.Items)
			if remaining == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("operator pods still present in %s: %w (remaining=%d lastErr=%v)",
				a.Namespace, ctx.Err(), remaining, lastErr)
		case <-tick.C:
		}
	}
}

// ScaleOperatorToOne brings the operator back without draining the
// scenario cleanup stack. Use this when a step needs the operator
// offline only for its own duration and must restore it before later
// steps run. Pair with WaitForOperatorAvailable.
func (a *Actions) ScaleOperatorToOne(ctx context.Context) error {
	const dep = "bloodraven"
	if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 1); err != nil {
		return fmt.Errorf("scale %s to 1: %w", dep, err)
	}
	return nil
}

// WaitForOperatorAvailable polls the operator Deployment until its
// available-replicas count matches the desired count, signalling that
// the replacement pod has rolled out and passed its readiness probe.
// Use this after KillOperator + Revert to avoid racing the new
// operator's first reconcile cycle: until at least one operator
// replica is Available, the CR status visible to callers is whatever
// the killed operator last wrote, which can be a stale "looks healthy"
// snapshot from before the chaos injection.
//
// Returns nil only when an Available replica exists. Honors the
// caller's context for timeout / cancellation. Deliberately reuses the
// same `bloodraven` deployment name and namespace that the rest of the
// chaos surface assumes; callers that want a different deployment
// should add a dedicated helper instead of parameterising this one.
func (a *Actions) WaitForOperatorAvailable(ctx context.Context) error {
	const dep = "bloodraven"
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		ready, err := a.K.DeploymentHasAvailableReplica(ctx, a.Namespace, dep)
		if err == nil && ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("operator deployment %s/%s did not report an available replica: %w (last poll err=%v)",
				a.Namespace, dep, ctx.Err(), err)
		case <-tick.C:
		}
	}
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
	if err := a.K.AnnotateMFGNamed(ctx, a.Namespace, a.FG, key, target); err != nil {
		return fmt.Errorf("set planned-failover annotation: %w", err)
	}
	a.push("clear planned-failover annotation", func(ctx context.Context) error {
		return a.K.AnnotateMFGNamed(ctx, a.Namespace, a.FG, key, "")
	})
	return nil
}

// AnnotatePlannedFailoverRaw sets the planned-failover annotation to a raw
// value, so scenarios can drive the annotation grammar's override syntax
// (e.g. "<target>:maxLagWait=5s"). Reverter clears the annotation.
//
// Use AnnotatePlannedFailover for the bare-site form; use this when the
// scenario needs a per-request knob such as a short maxLagWait to force the
// WaitingForLag → Failed{LagTimeout} rollback.
func (a *Actions) AnnotatePlannedFailoverRaw(ctx context.Context, rawValue string) error {
	const key = "bloodraven.shipstream.io/planned-failover"
	if rawValue == "" {
		return fmt.Errorf("planned-failover raw annotation value must not be empty")
	}
	if err := a.K.AnnotateMFGNamed(ctx, a.Namespace, a.FG, key, rawValue); err != nil {
		return fmt.Errorf("set planned-failover annotation %q: %w", rawValue, err)
	}
	a.push("clear planned-failover annotation", func(ctx context.Context) error {
		return a.K.AnnotateMFGNamed(ctx, a.Namespace, a.FG, key, "")
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

// WipeSiteData scales the named site's MySQL deployment to 0, clears
// the operator-applied `db-readonly-<fg>` taint from every node so the
// local-path-provisioner helper pod can run, deletes the site's data
// PVC, and waits for the PVC to actually disappear. The reverter scales
// the site back to 1 — at which point the operator's reconciler creates
// a fresh PVC and the empty datadir trips the fresh-deploy bootstrap.
//
// Why scrub taints across all nodes (not just the replica's): the
// readonly taint is per-FG, and it's applied to every non-writable
// site's node. The replica node usually carries it, and the
// local-path-provisioner helper that fulfills a PVC does not tolerate
// it. Clearing it on every node is cheap, idempotent, and side-effect-
// free — the operator re-applies the taint on its next reconcile after
// the bootstrap is complete and the site is back in `read-only`.
//
// No taint reverter is pushed: the operator owns the taint lifecycle
// and will re-establish it once the cluster is healthy. Same for the
// PVC: the operator's reconcilePVC re-creates it.
func (a *Actions) WipeSiteData(ctx context.Context, site, fgGroup string) error {
	dep := pgkube.MysqlDeploymentName(a.FG, site)
	pvc := pgkube.MysqlPVCName(a.FG, site)
	// Capture the original PVC UID before we delete. The operator's
	// reconcile recreates the PVC almost instantly with the same name
	// but a fresh UID, so we wait on UID-change rather than absence.
	originalUID, err := a.K.PVCUID(ctx, a.Namespace, pvc)
	if err != nil {
		return fmt.Errorf("read pvc %s uid: %w", pvc, err)
	}
	if originalUID == "" {
		return fmt.Errorf("pvc %s does not exist; nothing to wipe", pvc)
	}
	if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 0); err != nil {
		return fmt.Errorf("scale %s to 0: %w", dep, err)
	}
	a.push(fmt.Sprintf("scale %s back to 1", dep), func(ctx context.Context) error {
		return a.K.ScaleDeployment(ctx, a.Namespace, dep, 1)
	})
	// Wait for the pod to actually go away — kubelet must release the
	// volume before PVC deletion can complete cleanly.
	deadline := time.Now().Add(60 * time.Second)
	for {
		n, err := a.K.PodCount(ctx, a.Namespace, pgkube.MysqlPodSelector(a.FG, site))
		if err == nil && n == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waiting for site %s pod to terminate: pods=%d err=%v", site, n, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	// Clear the readonly taint from every node so the local-path-
	// provisioner helper pod can run when the new PVC is created.
	taintKey := "shipstream.io/db-readonly-" + fgGroup
	nodes, err := a.K.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	for i := range nodes.Items {
		if err := a.K.RemoveNodeTaint(ctx, nodes.Items[i].Name, taintKey); err != nil {
			return fmt.Errorf("remove %s taint from node %s: %w", taintKey, nodes.Items[i].Name, err)
		}
	}
	if err := a.K.DeletePVC(ctx, a.Namespace, pvc); err != nil {
		return fmt.Errorf("delete pvc %s: %w", pvc, err)
	}
	// Local-path-provisioner finalises PVC deletion by spawning a
	// helper pod that runs `rm -rf` on the host backing directory; on
	// a busy k3d node this is 1–3s. The operator's reconcile re-
	// creates the PVC almost instantly, so we wait for the UID to
	// change rather than for the PVC to be absent.
	deadline = time.Now().Add(5 * time.Minute)
	for {
		curUID, err := a.K.PVCUID(ctx, a.Namespace, pvc)
		if err == nil && curUID != "" && curUID != originalUID {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waiting for pvc %s to be replaced: originalUID=%s currentUID=%s err=%v",
				pvc, originalUID, curUID, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// PatchSitesMemoryRequest applies a JSON Patch that replaces every
// site's resources.requests.memory in spec.sites with newMemory. The
// reverter restores the previous values per-site (sites may have had
// different originals). Returns the per-site original values for
// scenarios that want to assert "memory ended up at newMemory" later.
func (a *Actions) PatchSitesMemoryRequest(ctx context.Context, newMemory string) (map[string]string, error) {
	mfg, err := a.K.GetMFGNamed(ctx, a.Namespace, a.FG)
	if err != nil {
		return nil, err
	}
	originals := map[string]string{}
	var ops []pgkube.JSONPatchOp
	for i, s := range mfg.Spec.Sites {
		var orig string
		if mem, ok := s.Resources.Requests[corev1.ResourceMemory]; ok {
			orig = mem.String()
		}
		originals[s.Name] = orig
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "replace",
			Path:  fmt.Sprintf("/spec/sites/%d/resources/requests/memory", i),
			Value: newMemory,
		})
	}
	if err := a.K.PatchMFGNamed(ctx, a.Namespace, a.FG, ops); err != nil {
		return nil, err
	}
	a.push(fmt.Sprintf("restore memory request originals (%v)", originals), func(ctx context.Context) error {
		// Re-read the MFG: the operator may have mutated other fields
		// since our patch (failover bumps activeSite, etc.) so we
		// re-derive site indexes from spec.sites by name.
		current, err := a.K.GetMFGNamed(ctx, a.Namespace, a.FG)
		if err != nil {
			return err
		}
		var revertOps []pgkube.JSONPatchOp
		for i, s := range current.Spec.Sites {
			orig, ok := originals[s.Name]
			if !ok || orig == "" {
				continue
			}
			revertOps = append(revertOps, pgkube.JSONPatchOp{
				Op:    "replace",
				Path:  fmt.Sprintf("/spec/sites/%d/resources/requests/memory", i),
				Value: orig,
			})
		}
		return a.K.PatchMFGNamed(ctx, a.Namespace, a.FG, revertOps)
	})
	return originals, nil
}

// PatchSplitBrainPriorities replaces spec.splitBrainPolicy.sitePriorities and
// restores the previous policy during cleanup.
func (a *Actions) PatchSplitBrainPriorities(ctx context.Context, priorities []string) error {
	if len(priorities) == 0 {
		return fmt.Errorf("split-brain priorities must not be empty")
	}
	mfg, err := a.K.GetMFGNamed(ctx, a.Namespace, a.FG)
	if err != nil {
		return err
	}
	var original []string
	hadPolicy := mfg.Spec.SplitBrainPolicy != nil
	hadPriorities := hadPolicy && mfg.Spec.SplitBrainPolicy.SitePriorities != nil
	if hadPriorities {
		original = append(original, mfg.Spec.SplitBrainPolicy.SitePriorities...)
	}
	var ops []pgkube.JSONPatchOp
	if !hadPolicy {
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "add",
			Path:  "/spec/splitBrainPolicy",
			Value: map[string]any{"sitePriorities": priorities},
		})
	} else if !hadPriorities {
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "add",
			Path:  "/spec/splitBrainPolicy/sitePriorities",
			Value: priorities,
		})
	} else {
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "replace",
			Path:  "/spec/splitBrainPolicy/sitePriorities",
			Value: priorities,
		})
	}
	if err := a.K.PatchMFGNamed(ctx, a.Namespace, a.FG, ops); err != nil {
		return err
	}
	a.push("restore split-brain priorities", func(ctx context.Context) error {
		if !hadPolicy {
			return a.K.PatchMFGNamed(ctx, a.Namespace, a.FG, []pgkube.JSONPatchOp{{
				Op:   "remove",
				Path: "/spec/splitBrainPolicy",
			}})
		}
		if !hadPriorities {
			return a.K.PatchMFGNamed(ctx, a.Namespace, a.FG, []pgkube.JSONPatchOp{{
				Op:   "remove",
				Path: "/spec/splitBrainPolicy/sitePriorities",
			}})
		}
		return a.K.PatchMFGNamed(ctx, a.Namespace, a.FG, []pgkube.JSONPatchOp{{
			Op:    "replace",
			Path:  "/spec/splitBrainPolicy/sitePriorities",
			Value: original,
		}})
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

// PatchDragonflySyncBudget overrides
// spec.dragonfly.plannedFailover.{maxSyncWait,onSyncTimeout} on the MFG
// and pushes a reverter that restores the original values. Both fields
// must already exist in spec — callers that hand it a value of "" for
// onSyncTimeout skip patching that field.
//
// Used by scenario D4 to drive the planned-failover state machine into
// the WaitingForDragonflySync timeout branch deterministically: a 1ms
// budget plus a target Dragonfly that has been scaled to 0 forces both
// the offset-poll-against-target and the REPLTAKEOVER step to fail,
// independently of master/replica replication latency in the cluster.
//
// The values pass through metav1.Duration's JSON UnmarshalJSON, so
// maxSyncWait must be a Go duration string ("1ms", "30s"). Empty
// onSyncTimeout means "leave as-is, do not patch".
func (a *Actions) PatchDragonflySyncBudget(ctx context.Context, maxSyncWait, onSyncTimeout string) error {
	if maxSyncWait == "" && onSyncTimeout == "" {
		return nil
	}
	mfg, err := a.K.GetMFG(ctx, a.Namespace)
	if err != nil {
		return fmt.Errorf("read MFG for sync-budget patch: %w", err)
	}
	df := mfg.Spec.Dragonfly
	if df == nil || df.PlannedFailover == nil {
		// Without an existing plannedFailover object an `add` to a
		// missing parent path silently no-ops on some patch
		// implementations and errors on others. Refuse loudly so a
		// misconfigured playground does not present as a flaky
		// timeout test.
		return fmt.Errorf("spec.dragonfly.plannedFailover must already exist on the playground MFG; got dragonfly=%v", df)
	}

	// Capture the originals BEFORE we mutate, so the reverter can
	// restore them faithfully even if a later step mutates the same
	// fields (e.g. a future PatchDragonflySyncBudget call).
	var originalMaxSyncWait *string
	if df.PlannedFailover.MaxSyncWait != nil {
		s := df.PlannedFailover.MaxSyncWait.Duration.String()
		originalMaxSyncWait = &s
	}
	originalOnSyncTimeout := df.PlannedFailover.OnSyncTimeout

	var ops []pgkube.JSONPatchOp
	if maxSyncWait != "" {
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "add",
			Path:  "/spec/dragonfly/plannedFailover/maxSyncWait",
			Value: maxSyncWait,
		})
	}
	if onSyncTimeout != "" {
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "add",
			Path:  "/spec/dragonfly/plannedFailover/onSyncTimeout",
			Value: onSyncTimeout,
		})
	}
	if err := a.K.PatchMFG(ctx, a.Namespace, ops); err != nil {
		return fmt.Errorf("patch dragonfly sync budget: %w", err)
	}

	a.push("restore dragonfly sync budget", func(ctx context.Context) error {
		var revOps []pgkube.JSONPatchOp
		if maxSyncWait != "" {
			if originalMaxSyncWait != nil {
				revOps = append(revOps, pgkube.JSONPatchOp{
					Op:    "add",
					Path:  "/spec/dragonfly/plannedFailover/maxSyncWait",
					Value: *originalMaxSyncWait,
				})
			} else {
				revOps = append(revOps, pgkube.JSONPatchOp{
					Op:   "remove",
					Path: "/spec/dragonfly/plannedFailover/maxSyncWait",
				})
			}
		}
		if onSyncTimeout != "" {
			if originalOnSyncTimeout != "" {
				revOps = append(revOps, pgkube.JSONPatchOp{
					Op:    "add",
					Path:  "/spec/dragonfly/plannedFailover/onSyncTimeout",
					Value: originalOnSyncTimeout,
				})
			} else {
				revOps = append(revOps, pgkube.JSONPatchOp{
					Op:   "remove",
					Path: "/spec/dragonfly/plannedFailover/onSyncTimeout",
				})
			}
		}
		return a.K.PatchMFG(ctx, a.Namespace, revOps)
	})
	return nil
}

// PatchDragonflyImage patches spec.dragonfly.image and registers a reverter
// to restore the original image after the scenario.
func (a *Actions) PatchDragonflyImage(ctx context.Context, image string) error {
	if image == "" {
		return fmt.Errorf("dragonfly image must not be empty")
	}
	mfg, err := a.K.GetMFG(ctx, a.Namespace)
	if err != nil {
		return fmt.Errorf("read MFG for dragonfly image patch: %w", err)
	}
	if mfg.Spec.Dragonfly == nil || !mfg.Spec.Dragonfly.Enabled {
		return fmt.Errorf("spec.dragonfly.enabled must be true")
	}
	original := mfg.Spec.Dragonfly.Image
	if original == "" {
		return fmt.Errorf("spec.dragonfly.image must be set before patching")
	}
	if original == image {
		return fmt.Errorf("dragonfly image patch target equals current image %q", image)
	}
	ops := []pgkube.JSONPatchOp{{
		Op:    "replace",
		Path:  "/spec/dragonfly/image",
		Value: image,
	}}
	if err := a.K.PatchMFG(ctx, a.Namespace, ops); err != nil {
		return fmt.Errorf("patch dragonfly image: %w", err)
	}
	a.push("restore dragonfly image", func(ctx context.Context) error {
		return a.K.PatchMFG(ctx, a.Namespace, []pgkube.JSONPatchOp{{
			Op:    "replace",
			Path:  "/spec/dragonfly/image",
			Value: original,
		}})
	})
	return nil
}

// EnsureRustFSDragonflyBucket creates the playground Dragonfly snapshot bucket
// using a signed S3 request through a port-forward to the RustFS pod. This
// replaces the old aws-cli bucket-init Job so scenario 29 does not depend on
// pulling an additional image.
func (a *Actions) EnsureRustFSDragonflyBucket(ctx context.Context) error {
	return a.EnsureRustFSBucket(ctx, "dragonfly")
}

// EnsureRustFSBucket creates a bucket in the playground RustFS deployment using
// the shared Dragonfly S3 credentials Secret. Callers use distinct bucket/prefix
// combinations for scenario isolation.
func (a *Actions) EnsureRustFSBucket(ctx context.Context, bucket string) error {
	const (
		secretName = "dragonfly-s3-credentials"
		selector   = "app.kubernetes.io/name=rustfs"
	)
	if bucket == "" {
		return fmt.Errorf("RustFS bucket name must not be empty")
	}
	secret, err := a.K.Kubernetes.CoreV1().Secrets(a.Namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read RustFS credentials secret %s: %w", secretName, err)
	}
	accessKey, ok := secret.Data["AWS_ACCESS_KEY_ID"]
	if !ok || len(accessKey) == 0 {
		return fmt.Errorf("RustFS credentials secret %s missing AWS_ACCESS_KEY_ID", secretName)
	}
	secretKey, ok := secret.Data["AWS_SECRET_ACCESS_KEY"]
	if !ok || len(secretKey) == 0 {
		return fmt.Errorf("RustFS credentials secret %s missing AWS_SECRET_ACCESS_KEY", secretName)
	}
	region, ok := secret.Data["AWS_REGION"]
	if !ok || len(region) == 0 {
		return fmt.Errorf("RustFS credentials secret %s missing AWS_REGION", secretName)
	}
	creds := pgrustfs.Credentials{
		AccessKey: string(accessKey),
		SecretKey: string(secretKey),
		Region:    string(region),
	}
	if err := a.restartRustFS(ctx, selector); err != nil {
		return err
	}
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for {
		pod, err := a.K.FindPodWithLabel(ctx, a.Namespace, selector)
		if err != nil {
			lastErr = fmt.Errorf("find RustFS pod: %w", err)
		} else if pf, err := a.K.PortForwardPod(ctx, a.Namespace, pod.Name, 9000); err != nil {
			lastErr = fmt.Errorf("port-forward RustFS: %w", err)
		} else {
			endpoint := fmt.Sprintf("http://127.0.0.1:%d", pf.LocalPort)
			err := pgrustfs.EnsureBucket(ctx, endpoint, bucket, creds)
			pf.Stop()
			if err == nil {
				return nil
			}
			lastErr = err
		}
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ensure RustFS bucket %q: %w", bucket, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (a *Actions) restartRustFS(ctx context.Context, selector string) error {
	var oldUID string
	if pod, err := a.K.FindPodWithLabel(ctx, a.Namespace, selector); err == nil {
		oldUID = string(pod.UID)
		if err := a.K.DeletePodByName(ctx, a.Namespace, pod.Name); err != nil {
			return fmt.Errorf("delete stale RustFS pod %s: %w", pod.Name, err)
		}
	}

	deadline := time.Now().Add(90 * time.Second)
	var last string
	for {
		pods, err := a.K.Kubernetes.CoreV1().Pods(a.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			last = err.Error()
		} else {
			for i := range pods.Items {
				pod := &pods.Items[i]
				if string(pod.UID) == oldUID || pod.DeletionTimestamp != nil {
					continue
				}
				if pod.Status.Phase == corev1.PodRunning && podIsReady(pod) {
					return nil
				}
				last = fmt.Sprintf("pod %s phase=%s ready=%v", pod.Name, pod.Status.Phase, podIsReady(pod))
			}
			if len(pods.Items) == 0 {
				last = "no RustFS pods"
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wait for RustFS restart: timed out (last: %s)", last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func podIsReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// PatchDragonflySnapshot configures the playground MFG to use the in-cluster
// RustFS bucket for Dragonfly snapshots and restores the previous snapshot
// spec during cleanup.
func (a *Actions) PatchDragonflySnapshot(ctx context.Context) error {
	mfg, err := a.K.GetMFG(ctx, a.Namespace)
	if err != nil {
		return fmt.Errorf("read MFG for snapshot patch: %w", err)
	}
	if mfg.Spec.Dragonfly == nil || !mfg.Spec.Dragonfly.Enabled {
		return fmt.Errorf("spec.dragonfly.enabled must be true")
	}
	original := mfg.Spec.Dragonfly.Snapshot
	snapshot := map[string]any{
		"dir":                   "s3://dragonfly/playground",
		"credentialsSecretName": "dragonfly-s3-credentials",
		"s3Endpoint":            "rustfs.bloodraven-playground.svc.cluster.local:9000",
		"s3UseHTTPS":            false,
		"s3SignPayload":         false,
	}
	if err := a.K.PatchMFG(ctx, a.Namespace, []pgkube.JSONPatchOp{{
		Op:    "add",
		Path:  "/spec/dragonfly/snapshot",
		Value: snapshot,
	}}); err != nil {
		return fmt.Errorf("patch dragonfly snapshot: %w", err)
	}
	a.push("restore dragonfly snapshot spec", func(ctx context.Context) error {
		if original == nil {
			return a.K.PatchMFG(ctx, a.Namespace, []pgkube.JSONPatchOp{{
				Op:   "remove",
				Path: "/spec/dragonfly/snapshot",
			}})
		}
		return a.K.PatchMFG(ctx, a.Namespace, []pgkube.JSONPatchOp{{
			Op:    "add",
			Path:  "/spec/dragonfly/snapshot",
			Value: original,
		}})
	})
	return nil
}

// GlobalRecover is the safety-net cleanup the runner runs after every
// scenario, regardless of outcome. Mirrors `chaos.sh recover`:
// removes every chaos-partition NetworkPolicy, scales every MySQL
// site back to 1 replica, scales every Dragonfly StatefulSet back to
// 1 replica, and brings RustFS back up. Idempotent.
func (a *Actions) GlobalRecover(ctx context.Context) error {
	var errs []error
	if err := a.K.RemoveAllChaosNetworkPolicies(ctx, a.Namespace); err != nil {
		errs = append(errs, fmt.Errorf("remove chaos NetworkPolicies: %w", err))
	}
	// Object storage: a scenario that scaled RustFS to 0 and never ran its
	// reverter would otherwise break every backup/restore scenario after it.
	if err := a.RecoverRustFS(ctx); err != nil {
		errs = append(errs, fmt.Errorf("recover rustfs: %w", err))
	}
	mfg, err := a.K.GetMFGNamed(ctx, a.Namespace, a.FG)
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
