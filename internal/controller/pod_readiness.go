package controller

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodReadiness is whether the pod a site's Deployment runs for its current
// pod template is Ready.
type PodReadiness struct {
	// Ready is true when the Deployment has finished rolling out its
	// current template and that template's pod reports Ready=True, which
	// takes every container (mysql, sidecar and any extra container)
	// passing its readiness probe.
	Ready bool
	// Pod names the pod the verdict is about; empty when there is none.
	Pod string
	// Restarts is the pod's total container restart count. With Pod it
	// tells two Ready observations of one container run from a crash and
	// restart between them.
	Restarts int32
	// Reason explains a not-Ready verdict, for example
	// "sidecar not ready: CrashLoopBackOff, last exit code 1 (Error)".
	Reason string
}

// maxPodReasonLen bounds a not-Ready reason, which ends up in a condition
// message and an Event.
const maxPodReasonLen = 512

// RolloutIncompleteError reports that a site's Deployment was updated but
// its rollout did not complete. The Deployment already carries the new pod
// template, so the site no longer shows spec drift.
type RolloutIncompleteError struct {
	Site string
	Err  error
}

func (e *RolloutIncompleteError) Error() string { return e.Err.Error() }
func (e *RolloutIncompleteError) Unwrap() error { return e.Err }

// SitePodReadiness reports whether a site's MySQL pod is Ready. The ordered
// update needs this on top of its MySQL probes: those go through the
// -internal Service, which publishes not-ready addresses, so a pod whose
// sidecar cannot start still answers them.
//
// Reads bypass the cache, as in waitForDeploymentRollout: a cached view can
// still show the replaced pod Ready.
func (r *MysqlFailoverGroupReconciler) SitePodReadiness(ctx context.Context, fgName types.NamespacedName, siteName string) (PodReadiness, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	return deploymentPodReadiness(ctx, reader, types.NamespacedName{Namespace: fgName.Namespace, Name: resourceName(fgName.Name, siteName)})
}

// deploymentPodReadiness reports the readiness of the pods of a
// Deployment's current pod template.
//
// The operator has no ReplicaSet access, so the current template's pods are
// found through the Deployment status instead: once the controller has
// observed the current generation and reports every replica updated and
// none left from an older template, every pod that is not terminating
// belongs to the new ReplicaSet. Terminating and terminated (evicted) pods
// are skipped, so a replaced pod that is still Ready while it shuts down
// is never mistaken for the new one, and pods of more than one ReplicaSet
// read as a rollout still in progress.
func deploymentPodReadiness(ctx context.Context, reader client.Reader, nn types.NamespacedName) (PodReadiness, error) {
	var dep appsv1.Deployment
	if err := reader.Get(ctx, nn, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return PodReadiness{Reason: fmt.Sprintf("deployment %s not found", nn.Name)}, nil
		}
		return PodReadiness{}, fmt.Errorf("get deployment %s: %w", nn, err)
	}
	desired := int32(1)
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	switch {
	case desired == 0:
		return PodReadiness{Reason: "deployment is scaled to 0 replicas"}, nil
	case dep.Status.ObservedGeneration < dep.Generation:
		return PodReadiness{Reason: "deployment controller has not observed the current pod template yet"}, nil
	case dep.Status.UpdatedReplicas < desired || dep.Status.Replicas > dep.Status.UpdatedReplicas:
		return PodReadiness{Reason: "rollout of the current pod template in progress"}, nil
	}

	selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil {
		return PodReadiness{}, fmt.Errorf("deployment %s selector: %w", nn, err)
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(nn.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return PodReadiness{}, fmt.Errorf("list pods of deployment %s: %w", nn, err)
	}
	var current []*corev1.Pod
	replicaSets := map[string]struct{}{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		rs, ok := deploymentReplicaSetOf(pod, dep.Name)
		if !ok {
			continue
		}
		replicaSets[rs] = struct{}{}
		current = append(current, pod)
	}
	switch {
	case len(replicaSets) > 1 || int32(len(current)) > desired:
		return PodReadiness{Reason: "rollout of the current pod template in progress"}, nil
	case int32(len(current)) < desired:
		return PodReadiness{Reason: "no pod for the current pod template yet"}, nil
	}

	verdict := PodReadiness{Ready: true}
	for _, pod := range current {
		restarts := containerRestarts(pod)
		if !podIsReady(pod) {
			return PodReadiness{Pod: pod.Name, Restarts: restarts, Reason: podNotReadyReason(pod)}, nil
		}
		verdict.Pod = pod.Name
		verdict.Restarts += restarts
	}
	return verdict, nil
}

// deploymentReplicaSetOf returns the ReplicaSet of deployment that controls
// pod. The Deployment controller names each ReplicaSet
// <deployment>-<pod-template-hash> and labels its pods with the hash, so a
// pod that merely matches the selector is not mistaken for one of them.
func deploymentReplicaSetOf(pod *corev1.Pod, deployment string) (string, bool) {
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.Kind != "ReplicaSet" {
		return "", false
	}
	hash := pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
	if hash == "" || owner.Name != deployment+"-"+hash {
		return "", false
	}
	return owner.Name, true
}

func podIsReady(pod *corev1.Pod) bool {
	c := podConditionOf(pod, corev1.PodReady)
	return c != nil && c.Status == corev1.ConditionTrue
}

func podConditionOf(pod *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

func containerRestarts(pod *corev1.Pod) int32 {
	var n int32
	for _, cs := range pod.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}

// podNotReadyReason says why a pod is not Ready: the first init container
// that has not completed, otherwise every container that is not ready,
// otherwise the pod's scheduling or Ready condition. It leaves out restart
// counts and back-off timers so the text stays the same while the cause
// does, and a condition carrying it is not rewritten on every probe.
func podNotReadyReason(pod *corev1.Pod) string {
	var parts []string
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0 {
			continue
		}
		if cs.State.Running != nil && cs.Ready {
			// A restartable (native sidecar) init container.
			continue
		}
		parts = append(parts, "init container "+containerNotReadyReason(cs))
		break
	}
	if len(parts) == 0 {
		for _, cs := range pod.Status.ContainerStatuses {
			if !cs.Ready {
				parts = append(parts, containerNotReadyReason(cs))
			}
		}
	}
	if len(parts) == 0 {
		if c := podConditionOf(pod, corev1.PodScheduled); c != nil && c.Status != corev1.ConditionTrue {
			parts = append(parts, "pod not scheduled: "+conditionText(c.Reason, c.Message))
		} else if c := podConditionOf(pod, corev1.PodReady); c != nil && (c.Reason != "" || c.Message != "") {
			parts = append(parts, "pod not ready: "+conditionText(c.Reason, c.Message))
		} else {
			parts = append(parts, fmt.Sprintf("pod %s, not ready", strings.ToLower(string(pod.Status.Phase))))
		}
	}
	return truncateReason(strings.Join(parts, "; "))
}

func containerNotReadyReason(cs corev1.ContainerStatus) string {
	var s string
	switch {
	case cs.State.Waiting != nil:
		reason := cs.State.Waiting.Reason
		if reason == "" {
			reason = "waiting"
		}
		s = fmt.Sprintf("%s not ready: %s", cs.Name, reason)
		// A CrashLoopBackOff message only repeats the back-off timer;
		// others (CreateContainerConfigError, ErrImagePull) say what is
		// wrong.
		if msg := strings.TrimSpace(cs.State.Waiting.Message); msg != "" && reason != "CrashLoopBackOff" {
			s += ": " + msg
		}
	case cs.State.Terminated != nil:
		s = fmt.Sprintf("%s not ready: terminated, %s", cs.Name, exitText(cs.State.Terminated))
	case cs.State.Running != nil:
		s = fmt.Sprintf("%s not ready: running, readiness probe failing", cs.Name)
	default:
		s = cs.Name + " not ready"
	}
	if t := cs.LastTerminationState.Terminated; t != nil && cs.State.Terminated == nil {
		s += ", last exit " + exitText(t)
	}
	return s
}

func exitText(t *corev1.ContainerStateTerminated) string {
	if t.Reason == "" {
		return fmt.Sprintf("code %d", t.ExitCode)
	}
	return fmt.Sprintf("code %d (%s)", t.ExitCode, t.Reason)
}

func conditionText(reason, message string) string {
	switch {
	case reason == "":
		return message
	case message == "":
		return reason
	}
	return reason + ": " + message
}

func truncateReason(s string) string {
	if len(s) <= maxPodReasonLen {
		return s
	}
	cut := maxPodReasonLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
