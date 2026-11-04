package controller

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodReadiness is whether the pod a site's Deployment runs for its current
// pod template is Ready, and has stayed Ready long enough to count.
type PodReadiness struct {
	// Ready is true when the Deployment has finished rolling out its
	// current template and that template's pod has been Ready=True, which
	// takes every container (mysql, sidecar and any extra container)
	// passing its readiness probe, continuously for at least podStableFor.
	// A crash-looping container passes its probe between crashes, so a pod
	// that is Ready right now is not enough.
	Ready bool
	// Pod names the pod the verdict is about; empty when there is none.
	Pod string
	// Reason explains a not-Ready verdict, for example
	// "sidecar not ready: crash looping, last exit code 1 (Error)" or
	// "pod Ready for less than 30s".
	Reason string
}

// podStableFor is how long a pod must have been Ready, without a break and
// without a container restart, before the ordered update counts it as
// Ready. Every pod-readiness gate of the ordered update uses this one
// definition.
const podStableFor = 30 * time.Second

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
	return deploymentPodReadiness(ctx, reader, types.NamespacedName{Namespace: fgName.Namespace, Name: resourceName(fgName.Name, siteName)}, time.Now())
}

// deploymentPodReadiness reports the readiness, as of now, of the pods of a
// Deployment's current pod template. A pod counts as Ready only once it has
// been Ready for podStableFor (see podReadySince).
//
// The operator has no ReplicaSet access, so the current template's pods are
// found through the Deployment status instead: once the controller has
// observed the current generation and reports every replica updated and
// none left from an older template, every pod that is not terminating
// belongs to the new ReplicaSet. Terminating and terminated (evicted) pods
// are skipped, so a replaced pod that is still Ready while it shuts down
// is never mistaken for the new one, and pods of more than one ReplicaSet
// read as a rollout still in progress.
func deploymentPodReadiness(ctx context.Context, reader client.Reader, nn types.NamespacedName, now time.Time) (PodReadiness, error) {
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
		if !podReady(pod) {
			return PodReadiness{Pod: pod.Name, Reason: podNotReadyReason(pod)}, nil
		}
		if now.Sub(podReadySince(pod)) < podStableFor {
			return PodReadiness{Pod: pod.Name, Reason: podUnsettledReason(pod, now)}, nil
		}
		verdict.Pod = pod.Name
	}
	return verdict, nil
}

// podReadySince is when a Ready pod's current Ready spell began, as far as
// its status tells: the latest of the Ready condition's last transition and
// the start of the current run of every container and every restartable
// (native sidecar) init container. A container restart takes the pod out of
// Ready; the start times also cover a restart the Ready condition has not
// caught up with, for example a container without a readiness probe, which
// is ready again as soon as it runs. A timestamp the kubelet left unset
// adds nothing.
//
// The timestamps come from the node's clock and are compared with the
// operator's. A node clock ahead of the operator's makes the pod look
// Ready for less time than it has been (a start in the future reads as not
// yet stable), so the verdict comes late by the skew; a node clock behind
// it makes the verdict come early by the skew.
func podReadySince(pod *corev1.Pod) time.Time {
	var since time.Time
	if c := podConditionOf(pod, corev1.PodReady); c != nil && c.Status == corev1.ConditionTrue {
		since = c.LastTransitionTime.Time
	}
	later := func(cs corev1.ContainerStatus) {
		if cs.State.Running != nil && cs.State.Running.StartedAt.After(since) {
			since = cs.State.Running.StartedAt.Time
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if restartableInitContainer(pod, cs.Name) {
			later(cs)
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		later(cs)
	}
	return since
}

// podUnsettledReason says why a pod that is Ready does not count as Ready
// yet. A container that has restarted and whose current run is younger
// than podStableFor is crash looping and is reported in the words
// containerNotReadyReason uses for the other phases of the loop, so the
// reason stays the same whichever phase a probe lands on. Otherwise the pod
// has only not been Ready for long enough.
func podUnsettledReason(pod *corev1.Pod, now time.Time) string {
	var parts []string
	for _, cs := range pod.Status.InitContainerStatuses {
		if restartableInitContainer(pod, cs.Name) && restartedWithin(cs, now) {
			parts = append(parts, "init container "+crashLoopReason(cs))
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if restartedWithin(cs, now) {
			parts = append(parts, crashLoopReason(cs))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("pod Ready for less than %s", podStableFor)
	}
	return truncateReason(strings.Join(parts, "; "))
}

// restartedWithin reports whether a running container has restarted before
// and its current run started less than podStableFor before now.
func restartedWithin(cs corev1.ContainerStatus, now time.Time) bool {
	return cs.RestartCount > 0 && cs.State.Running != nil && now.Sub(cs.State.Running.StartedAt.Time) < podStableFor
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

func podConditionOf(pod *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// restartableInitContainer reports whether the named init container is a
// native sidecar (restartPolicy: Always), which, unlike an ordinary init
// container, runs for the pod's lifetime and can have a readiness probe.
// spec.extraInitContainers can add one.
func restartableInitContainer(pod *corev1.Pod, name string) bool {
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		if c.Name == name {
			return c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
		}
	}
	return false
}

// podNotReadyReason says why a pod is not Ready: the first init container
// that has not completed, otherwise every container that is not ready,
// otherwise the pod's scheduling or Ready condition. It leaves out restart
// counts, back-off timers and the phase of a crash loop so the text stays
// the same while the cause does, and a condition carrying it is not
// rewritten, nor an Event fired, on every probe.
func podNotReadyReason(pod *corev1.Pod) string {
	var parts []string
	for _, cs := range pod.Status.InitContainerStatuses {
		restartable := restartableInitContainer(pod, cs.Name)
		if !restartable && cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0 {
			continue
		}
		if restartable && cs.State.Running != nil && cs.Ready {
			continue
		}
		if !restartable && cs.State.Running != nil && cs.RestartCount == 0 {
			// An ordinary init container has no readiness probe; running
			// for the first time means the pod is still initializing.
			parts = append(parts, "init container "+cs.Name+" not ready: still running")
			break
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

// containerNotReadyReason says why a container is not ready. A crash loop
// reads the same in each of its phases (see crashLooping); any other
// waiting reason keeps its message, which says what is wrong
// (CreateContainerConfigError, ErrImagePull).
func containerNotReadyReason(cs corev1.ContainerStatus) string {
	if crashLooping(cs) {
		return crashLoopReason(cs)
	}
	var s string
	switch {
	case cs.State.Waiting != nil:
		reason := cs.State.Waiting.Reason
		if reason == "" {
			reason = "waiting"
		}
		s = fmt.Sprintf("%s not ready: %s", cs.Name, reason)
		if msg := strings.TrimSpace(cs.State.Waiting.Message); msg != "" {
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

// crashLooping reports whether a container that is not ready is in one of
// the phases of a crash loop: waiting out CrashLoopBackOff, or, having
// restarted before, running again or terminated again. The remaining
// phase, ready again but for less than podStableFor, is podUnsettledReason's.
// A probe can land on any of them, so they share one text.
func crashLooping(cs corev1.ContainerStatus) bool {
	switch {
	case cs.State.Waiting != nil:
		return cs.State.Waiting.Reason == "CrashLoopBackOff"
	case cs.State.Running != nil, cs.State.Terminated != nil:
		return cs.RestartCount > 0
	}
	return false
}

// crashLoopReason is the text for a crash-looping container: its name and
// its most recent exit, from the current state while it is terminated and
// from the last termination otherwise. It leaves out the restart count, the
// back-off timer and the loop's phase, so it reads the same on every probe
// while the loop lasts, for example
// "sidecar not ready: crash looping, last exit code 1 (Error)".
func crashLoopReason(cs corev1.ContainerStatus) string {
	s := cs.Name + " not ready: crash looping"
	t := cs.State.Terminated
	if t == nil {
		t = cs.LastTerminationState.Terminated
	}
	if t != nil {
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
