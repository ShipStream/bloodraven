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

func containerReady(name string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
}

func crashLooping(name string, exitCode int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: name, RestartCount: 6,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff", Message: "back-off 2m40s restarting failed container=" + name,
		}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Reason: "Error"}},
	}
}

// readinessPod is a pod of the ReplicaSet with the given pod-template-hash.
// Without status edits it is Running with both containers Ready.
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
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}, {Type: corev1.PodReady, Status: corev1.ConditionTrue}},
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

func terminating(p *corev1.Pod) {
	now := metav1.NewTime(time.Now())
	p.DeletionTimestamp = &now
	// The fake client refuses a deleting object that nothing holds.
	p.Finalizers = []string{"test.shipstream.io/hold"}
}

func TestDeploymentPodReadiness(t *testing.T) {
	for _, tc := range []struct {
		name       string
		objs       []client.Object
		wantReady  bool
		wantPod    string
		wantReason string
	}{
		{
			name:      "pod of the current template Ready",
			objs:      []client.Object{rolledOut(), readinessPod("mysql-lion-dc2-new-a", "new")},
			wantReady: true, wantPod: "mysql-lion-dc2-new-a",
		},
		{
			// The case the rollout replaces a pod in: the old pod can
			// still report Ready while it shuts down.
			name: "replaced pod still Ready while terminating, new pod crash-looping",
			objs: []client.Object{rolledOut(),
				readinessPod("mysql-lion-dc2-old-a", "old", terminating),
				readinessPod("mysql-lion-dc2-new-a", "new", notReady(containerReady("mysql"), crashLooping("sidecar", 1))),
			},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "sidecar not ready: CrashLoopBackOff, last exit code 1 (Error)",
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
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "keyring-init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}}},
					crashLooping("init", 2),
				}
				waiting := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
				notReady(corev1.ContainerStatus{Name: "mysql", State: waiting}, corev1.ContainerStatus{Name: "sidecar", State: waiting})(p)
			})},
			wantPod:    "mysql-lion-dc2-new-a",
			wantReason: "init container init not ready: CrashLoopBackOff, last exit code 2 (Error)",
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(tc.objs...).Build()
			got, err := deploymentPodReadiness(context.Background(), c, types.NamespacedName{Namespace: readinessNS, Name: readinessDeploy})
			if err != nil {
				t.Fatalf("deploymentPodReadiness() error = %v", err)
			}
			if got.Ready != tc.wantReady || got.Pod != tc.wantPod || got.Reason != tc.wantReason {
				t.Fatalf("deploymentPodReadiness() = %+v, want ready=%v pod=%q reason=%q", got, tc.wantReady, tc.wantPod, tc.wantReason)
			}
		})
	}
}

// The restart count lets the ordered update tell one container run that
// stays Ready from a crash and restart between two Ready probes.
func TestDeploymentPodReadinessCountsRestarts(t *testing.T) {
	pod := readinessPod("mysql-lion-dc2-new-a", "new", func(p *corev1.Pod) {
		p.Status.ContainerStatuses[0].RestartCount = 1
		p.Status.ContainerStatuses[1].RestartCount = 4
	})
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut(), pod).Build()
	got, err := deploymentPodReadiness(context.Background(), c, types.NamespacedName{Namespace: readinessNS, Name: readinessDeploy})
	if err != nil || !got.Ready || got.Restarts != 5 {
		t.Fatalf("deploymentPodReadiness() = %+v, %v; want Ready with 5 restarts", got, err)
	}
}

// A pod that cannot be read is an error, not a verdict: the update must
// not roll on an unknown.
func TestDeploymentPodReadinessReportsListErrors(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut()).Build()
	if _, err := deploymentPodReadiness(context.Background(), failingListReader{Reader: c},
		types.NamespacedName{Namespace: readinessNS, Name: readinessDeploy}); err == nil {
		t.Fatal("deploymentPodReadiness() error = nil, want the list failure")
	}
}

// The reconciler maps the group and site to the site Deployment and reads
// through the uncached APIReader, whose view of a replaced pod is current.
func TestSitePodReadinessReadsSiteDeploymentThroughAPIReader(t *testing.T) {
	api := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut(),
		readinessPod("mysql-lion-dc2-new-a", "new", notReady(containerReady("mysql"), crashLooping("sidecar", 1)))).Build()
	// The cached client still shows the replaced pod Ready.
	cached := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(rolledOut(),
		readinessPod("mysql-lion-dc2-new-a", "new")).Build()
	r := &MysqlFailoverGroupReconciler{Client: cached, APIReader: api}
	got, err := r.SitePodReadiness(context.Background(), types.NamespacedName{Namespace: readinessNS, Name: "lion"}, "dc2")
	if err != nil {
		t.Fatalf("SitePodReadiness() error = %v", err)
	}
	if got.Ready || !strings.Contains(got.Reason, "CrashLoopBackOff") {
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
