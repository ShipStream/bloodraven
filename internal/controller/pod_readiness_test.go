package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	readinessNS     = "shared-lion"
	readinessDeploy = "mysql-lion-dc2"
)

// readinessNow is the time the fixtures are judged at.
var readinessNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func readinessAgo(d time.Duration) metav1.Time { return metav1.NewTime(readinessNow.Add(-d)) }

func readinessDeployment(generation, observed int64, replicas, updated, total int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: readinessDeploy, Namespace: readinessNS, Generation: generation},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				labelAppName: "mysql", labelInstance: "lion", labelSite: "dc2",
			}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: observed, UpdatedReplicas: updated, Replicas: total, AvailableReplicas: total},
	}
}

// rolledOut is a Deployment whose controller reports the current template
// fully rolled out to its one replica.
func rolledOut() *appsv1.Deployment { return readinessDeployment(4, 4, 1, 1, 1) }

// containerReady is a container that has been running, and ready, for ten
// minutes without a restart.
func containerReady(name string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, Ready: true, State: corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{StartedAt: readinessAgo(10 * time.Minute)},
	}}
}

func exited(code int32, reason string) *corev1.ContainerStateTerminated {
	return &corev1.ContainerStateTerminated{ExitCode: code, Reason: reason}
}

// crashLoopBackOff is a container waiting out its crash-loop back-off.
func crashLoopBackOff(name string, exitCode int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: name, RestartCount: 6,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff", Message: "back-off 2m40s restarting failed container=" + name,
		}},
		LastTerminationState: corev1.ContainerState{Terminated: exited(exitCode, "Error")},
	}
}

// readinessPod is a pod of the ReplicaSet with the given pod-template-hash.
// Without status edits it is Running with both containers Ready, and has
// been Ready for nine minutes.
func readinessPod(name, hash string, edits ...func(*corev1.Pod)) *corev1.Pod {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: readinessNS,
			Labels: map[string]string{
				labelAppName: "mysql", labelInstance: "lion", labelSite: "dc2",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: readinessDeploy + "-" + hash, UID: types.UID("rs-" + hash), Controller: &controller,
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: readinessAgo(9 * time.Minute)},
			},
			ContainerStatuses: []corev1.ContainerStatus{containerReady("mysql"), containerReady("sidecar")},
		},
	}
	for _, edit := range edits {
		edit(pod)
	}
	return pod
}

func notReady(statuses ...corev1.ContainerStatus) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady"},
		}
		p.Status.ContainerStatuses = statuses
	}
}

// readySince sets when the pod's Ready condition last turned True.
func readySince(at metav1.Time) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		podConditionOf(p, corev1.PodReady).LastTransitionTime = at
	}
}

// withNativeSidecar adds a restartable init container named proxy.
func withNativeSidecar(status corev1.ContainerStatus) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		always := corev1.ContainerRestartPolicyAlways
		p.Spec.InitContainers = []corev1.Container{{Name: "proxy", RestartPolicy: &always}}
		status.Name = "proxy"
		p.Status.InitContainerStatuses = []corev1.ContainerStatus{status}
	}
}

func terminating(p *corev1.Pod) {
	now := metav1.NewTime(time.Now())
	p.DeletionTimestamp = &now
	// The fake client refuses a deleting object that nothing holds.
	p.Finalizers = []string{"test.shipstream.io/hold"}
}

func probeReadiness(t *testing.T, objs ...client.Object) PodReadiness {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
	got, err := deploymentPodReadiness(context.Background(), c, types.NamespacedName{Namespace: readinessNS, Name: readinessDeploy}, readinessNow)
	if err != nil {
		t.Fatalf("deploymentPodReadiness() error = %v", err)
	}
	return got
}

func TestDeploymentPodReadiness(t *testing.T) {
	restartedRecently := func(code int32, reason string) corev1.ContainerStatus {
		return corev1.ContainerStatus{Ready: true, RestartCount: 3,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: readinessAgo(5 * time.Second)}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(code, reason)},
		}
	}
	for _, tc := range []struct {
		name       string
		objs       []client.Object
		wantReady  bool
		wantPod    string
		wantReason string
	}{
		{
			name:      "pod of the current template Ready for long enough",
			objs:      []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new")},
			wantReady: true, wantPod: "mysql-lion-dc2-new-a",
		},
		{
			name:      "Ready for exactly the settle time",
			objs:      []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", readySince(readinessAgo(podStableFor)))},
			wantReady: true, wantPod: "mysql-lion-dc2-new-a",
		},
		{
			name:       "Ready for 10s",
			objs:       []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", readySince(readinessAgo(10*time.Second)))},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "pod Ready for less than 30s",
		},
		{
			// The node's clock runs ahead of the operator's.
			name:       "Ready condition in the future",
			objs:       []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", readySince(readinessAgo(-2*time.Minute)))},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "pod Ready for less than 30s",
		},
		{
			// The pod has been Ready for minutes as far as its condition
			// says, but the sidecar crashed and came back 5s ago without
			// the condition catching it.
			name: "container restarted 5s ago under an old Ready condition",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) {
				sidecar := restartedRecently(1, "Error")
				sidecar.Name = "sidecar"
				p.Status.ContainerStatuses[1] = sidecar
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "sidecar not ready: crash looping, last exit code 1 (Error)",
		},
		{
			name: "native sidecar init container restarted 5s ago under an old Ready condition",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new",
				withNativeSidecar(restartedRecently(137, "OOMKilled")))},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "init container proxy not ready: crash looping, last exit code 137 (OOMKilled)",
		},
		{
			// A restart an hour ago is not a crash loop; the readiness probe
			// failed and recovered since.
			name: "Ready for 10s after a restart long ago",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", readySince(readinessAgo(10*time.Second)), func(p *corev1.Pod) {
				p.Status.ContainerStatuses[1].RestartCount = 4
				p.Status.ContainerStatuses[1].LastTerminationState = corev1.ContainerState{Terminated: exited(1, "Error")}
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "pod Ready for less than 30s",
		},
		{
			// The case the rollout replaces a pod in: the old pod can
			// still report Ready while it shuts down.
			name: "replaced pod still Ready while terminating, new pod crash-looping",
			objs: []client.Object{rolledOut(),
				readinessPod("mysql-lion-dc2-old-a", "old", terminating),
				readinessPod("mysql-lion-dc2-new-a", "new", notReady(containerReady("mysql"), crashLoopBackOff("sidecar", 1))),
			},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "sidecar not ready: crash looping, last exit code 1 (Error)",
		},
		{
			name:       "controller has not observed the new template, old pod Ready",
			objs:       []client.Object{readinessDeployment(5, 4, 1, 1, 1), readinessPod("mysql-lion-dc2-old-a", "old")},
			wantReason: "deployment controller has not observed the current pod template yet",
		},
		{
			name:       "rollout still replacing the pod",
			objs:       []client.Object{readinessDeployment(5, 5, 1, 0, 1), readinessPod("mysql-lion-dc2-old-a", "old", terminating)},
			wantReason: "rollout of the current pod template in progress",
		},
		{
			name:       "new pod not created yet",
			objs:       []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-old-a", "old", terminating)},
			wantReason: "no pod for the current pod template yet",
		},
		{
			name: "pods of two ReplicaSets",
			objs: []client.Object{rolledOut(),
				readinessPod("mysql-lion-dc2-old-a", "old"),
				readinessPod("mysql-lion-dc2-new-a", "new"),
			},
			wantReason: "rollout of the current pod template in progress",
		},
		{
			name: "evicted pod skipped",
			objs: []client.Object{rolledOut(),
				readinessPod("mysql-lion-dc2-new-evicted", "new", func(p *corev1.Pod) {
					p.Status.Phase = corev1.PodFailed
					p.Status.Reason = "Evicted"
				}),
				readinessPod("mysql-lion-dc2-new-b", "new"),
			},
			wantReady: true, wantPod: "mysql-lion-dc2-new-b",
		},
		{
			name: "pod that only matches the selector",
			objs: []client.Object{rolledOut(),
				readinessPod("debug-copy", "new", func(p *corev1.Pod) { p.OwnerReferences = nil }),
			},
			wantReason: "no pod for the current pod template yet",
		},
		{
			name: "pod owned by a ReplicaSet of another Deployment",
			objs: []client.Object{rolledOut(),
				readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) { p.OwnerReferences[0].Name = "other-new" }),
			},
			wantReason: "no pod for the current pod template yet",
		},
		{
			name:       "scaled to zero",
			objs:       []client.Object{readinessDeployment(4, 4, 0, 0, 0)},
			wantReason: "deployment is scaled to 0 replicas",
		},
		{
			name:       "deployment missing",
			wantReason: "deployment mysql-lion-dc2 not found",
		},
		{
			name: "unschedulable",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Status.ContainerStatuses = nil
				p.Status.Conditions = []corev1.PodCondition{{
					Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable",
					Message: "0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.",
				}}
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "pod not scheduled: Unschedulable: 0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.",
		},
		{
			name: "init container crash-looping",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Spec.InitContainers = []corev1.Container{{Name: "keyring-init"}, {Name: "init"}}
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "keyring-init", State: corev1.ContainerState{Terminated: exited(0, "Completed")}},
					crashLoopBackOff("init", 2),
				}
				waiting := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
				notReady(corev1.ContainerStatus{Name: "mysql", State: waiting}, corev1.ContainerStatus{Name: "sidecar", State: waiting})(p)
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "init container init not ready: crash looping, last exit code 2 (Error)",
		},
		{
			// An ordinary init container has no readiness probe: running
			// means it is still initializing the pod.
			name: "init container still running",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Spec.InitContainers = []corev1.Container{{Name: "keyring-init"}}
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "keyring-init", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				}
				waiting := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
				notReady(corev1.ContainerStatus{Name: "mysql", State: waiting}, corev1.ContainerStatus{Name: "sidecar", State: waiting})(p)
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "init container keyring-init not ready: still running",
		},
		{
			// Running again after failing is a phase of its crash loop,
			// not the pod still initializing.
			name: "init container running again after failing",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Spec.InitContainers = []corev1.Container{{Name: "init"}}
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", RestartCount: 3,
					State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: readinessAgo(2 * time.Second)}},
					LastTerminationState: corev1.ContainerState{Terminated: exited(2, "Error")},
				}}
				waiting := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
				notReady(corev1.ContainerStatus{Name: "mysql", State: waiting}, corev1.ContainerStatus{Name: "sidecar", State: waiting})(p)
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "init container init not ready: crash looping, last exit code 2 (Error)",
		},
		{
			name: "native sidecar init container readiness probe failing",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new",
				withNativeSidecar(corev1.ContainerStatus{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}),
				notReady(containerReady("mysql"), containerReady("sidecar")),
			)},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "init container proxy not ready: running, readiness probe failing",
		},
		{
			name: "readiness probe failing",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", notReady(
				containerReady("mysql"),
				corev1.ContainerStatus{Name: "sidecar", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			))},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "sidecar not ready: running, readiness probe failing",
		},
		{
			name: "waiting message kept when it names the cause",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", notReady(
				containerReady("mysql"),
				corev1.ContainerStatus{Name: "sidecar", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "CreateContainerConfigError", Message: `secret "lion-pitr" not found`,
				}}},
			))},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: `sidecar not ready: CreateContainerConfigError: secret "lion-pitr" not found`,
		},
		{
			name: "image pull failure keeps its message",
			objs: []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new", notReady(
				containerReady("mysql"),
				corev1.ContainerStatus{Name: "sidecar", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "ErrImagePull", Message: `failed to pull image "bloodraven-sidecar:v9": not found`,
				}}},
			))},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: `sidecar not ready: ErrImagePull: failed to pull image "bloodraven-sidecar:v9": not found`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := probeReadiness(t, tc.objs...)
			if got.Ready != tc.wantReady || got.Pod != tc.wantPod || got.Reason != tc.wantReason {
				t.Fatalf("deploymentPodReadiness() = %+v, want ready=%v pod=%q reason=%q", got, tc.wantReady, tc.wantPod, tc.wantReason)
			}
		})
	}
}

// crashLoopPhase is one phase a probe can find a crash-looping container in:
// its status, and whether it, and with it the pod, is Ready.
type crashLoopPhase struct {
	name   string
	status corev1.ContainerStatus
	ready  bool
}

// crashLoopPhases are the phases of a crash loop whose last exit was code 1
// (Error), as of readinessNow: waiting out the back-off, running again
// before its readiness probe passes, Ready again for a few seconds, and
// terminated again. In the terminated phase the last termination is the
// crash before, which exited differently.
func crashLoopPhases() []crashLoopPhase {
	return []crashLoopPhase{
		{name: "back-off", status: corev1.ContainerStatus{RestartCount: 6,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 2m40s restarting failed container"}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(1, "Error")},
		}},
		{name: "running, not ready yet", status: corev1.ContainerStatus{RestartCount: 7,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: readinessAgo(2 * time.Second)}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(1, "Error")},
		}},
		{name: "Ready for a few seconds", ready: true, status: corev1.ContainerStatus{RestartCount: 7, Ready: true,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: readinessAgo(12 * time.Second)}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(1, "Error")},
		}},
		{name: "terminated", status: corev1.ContainerStatus{RestartCount: 7,
			State:                corev1.ContainerState{Terminated: exited(1, "Error")},
			LastTerminationState: corev1.ContainerState{Terminated: exited(2, "Error")},
		}},
	}
}

// sidecarCrashLoopPod is a site pod whose sidecar container is in the given
// phase of a crash loop. In a Ready phase the pod has been Ready for 4s.
func sidecarCrashLoopPod(phase crashLoopPhase) *corev1.Pod {
	cs := phase.status
	cs.Name = "sidecar"
	edit := notReady(containerReady("mysql"), cs)
	if phase.ready {
		edit = func(p *corev1.Pod) {
			p.Status.ContainerStatuses[1] = cs
			readySince(readinessAgo(4 * time.Second))(p)
		}
	}
	return readinessPod("mysql-lion-dc2-new-a", "new", edit)
}

// nativeSidecarCrashLoopPod is a site pod whose restartable init container
// is in the given phase of a crash loop.
func nativeSidecarCrashLoopPod(phase crashLoopPhase) *corev1.Pod {
	return readinessPod("mysql-lion-dc2-new-a", "new", withNativeSidecar(phase.status), func(p *corev1.Pod) {
		if phase.ready {
			readySince(readinessAgo(4 * time.Second))(p)
		} else {
			notReady(containerReady("mysql"), containerReady("sidecar"))(p)
		}
	})
}

// A probe can land on any phase of a crash loop. Each must read as the same
// cause, or the stall it is reported in would be rewritten, and a Warning
// Event fired, on almost every retry.
func TestDeploymentPodReadinessCrashLoopReadsTheSameInEveryPhase(t *testing.T) {
	for _, loop := range []struct {
		name string
		pod  func(crashLoopPhase) *corev1.Pod
		want string
	}{
		{name: "container", pod: sidecarCrashLoopPod, want: sidecarCrashLoop},
		{name: "native sidecar init container", pod: nativeSidecarCrashLoopPod,
			want: "init container proxy not ready: crash looping, last exit code 1 (Error)"},
	} {
		for _, phase := range crashLoopPhases() {
			t.Run(loop.name+"/"+phase.name, func(t *testing.T) {
				got := probeReadiness(t, rolledOut(), loop.pod(phase))
				if got.Ready || got.Reason != loop.want {
					t.Fatalf("deploymentPodReadiness() = %+v, want not Ready with reason %q", got, loop.want)
				}
			})
		}
	}
}

// A crash-looping container that never left a termination record still
// reads as crash looping, without an exit.
func TestCrashLoopReasonWithoutExit(t *testing.T) {
	got := containerNotReadyReason(corev1.ContainerStatus{Name: "sidecar", RestartCount: 2,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}})
	if want := "sidecar not ready: crash looping"; got != want {
		t.Fatalf("containerNotReadyReason() = %q, want %q", got, want)
	}
}

// A pod that cannot be read is an error, not a verdict: the update must
// not roll on an unknown.
func TestDeploymentPodReadinessReportsListErrors(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut()).Build()
	if _, err := deploymentPodReadiness(context.Background(), failingListReader{Reader: c},
		types.NamespacedName{Namespace: readinessNS, Name: readinessDeploy}, readinessNow); err == nil {
		t.Fatal("deploymentPodReadiness() error = nil, want the list failure")
	}
}

// The reconciler maps the group and site to the site Deployment and reads
// through the uncached APIReader, whose view of a replaced pod is current.
func TestSitePodReadinessReadsSiteDeploymentThroughAPIReader(t *testing.T) {
	api := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut(),
		readinessPod("mysql-lion-dc2-new-a", "new", notReady(containerReady("mysql"), crashLoopBackOff("sidecar", 1)))).Build()
	// The cached client still shows the replaced pod Ready.
	cached := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut(),
		readinessPod("mysql-lion-dc2-new-a", "new")).Build()
	r := &MysqlFailoverGroupReconciler{Client: cached, APIReader: api}
	got, err := r.SitePodReadiness(context.Background(), types.NamespacedName{Namespace: readinessNS, Name: "lion"}, "dc2")
	if err != nil {
		t.Fatalf("SitePodReadiness() error = %v", err)
	}
	if got.Ready || !strings.Contains(got.Reason, "crash looping") {
		t.Fatalf("SitePodReadiness() = %+v, want the APIReader's crash-looping pod", got)
	}
}

func TestPodNotReadyReasonIsBounded(t *testing.T) {
	long := strings.Repeat("é", maxPodReasonLen)
	pod := readinessPod("p", "new", notReady(corev1.ContainerStatus{Name: "sidecar", State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull", Message: long},
	}}))
	got := podNotReadyReason(pod)
	if len(got) > maxPodReasonLen+len("…") || !strings.HasSuffix(got, "…") || !strings.HasPrefix(got, "sidecar not ready: ErrImagePull: ") {
		t.Fatalf("podNotReadyReason() = %d bytes %q, want a bounded, marked truncation", len(got), got)
	}
	if !strings.HasSuffix(strings.TrimSuffix(got, "…"), "é") {
		t.Fatalf("podNotReadyReason() cut a multi-byte rune: %q", got)
	}
}
