package controller

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/shipstream/bloodraven/internal/testutil"
)

// A spec.image the mysql launcher cannot start (R4 B2): the launcher exits
// 127 when the image has no entrypoint it knows, and the container cannot
// be started at all when the image has no /bin/bash. Either way the mysql
// container crash-loops, and the ordered update must stop at the first
// site it rolled, with the primary untouched.

const launcherExit127 = "mysql not ready: crash looping, last exit code 127 (Error)"

// mysqlCrashLoopPod is a site pod whose mysql container is in the given
// phase of a crash loop with the given last exit; its sidecar is fine.
func mysqlCrashLoopPod(phase crashLoopPhase, code int32, reason string) *corev1.Pod {
	cs := phase.status
	cs.Name = "mysql"
	if cs.State.Terminated != nil {
		cs.State.Terminated = exited(code, reason)
	}
	cs.LastTerminationState = corev1.ContainerState{Terminated: exited(code, reason)}
	edit := notReady(cs, containerReady("sidecar"))
	if phase.ready {
		edit = func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0] = cs
			readySince(readinessAgo(4 * time.Second))(p)
		}
	}
	return readinessPod("mysql-lion-dc2-new-a", "new", edit)
}

// The readiness probe the ordered update gates on reads a mysql container
// that the launcher cannot start as not Ready, in every phase of its crash
// loop, and names the container and the exit code.
func TestDeploymentPodReadiness_MySQLLauncherThatCannotStart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   int32
		reason string
		want   string
	}{
		{name: "no image entrypoint (the launcher exits 127)", code: 127, reason: "Error", want: launcherExit127},
		{name: "no /bin/bash in the image (the container cannot be started)", code: 128, reason: "StartError",
			want: "mysql not ready: crash looping, last exit code 128 (StartError)"},
	} {
		for _, phase := range crashLoopPhases() {
			t.Run(tc.name+"/"+phase.name, func(t *testing.T) {
				got := probeReadiness(t, rolledOut(), mysqlCrashLoopPod(phase, tc.code, tc.reason))
				if got.Ready || got.Reason != tc.want {
					t.Fatalf("deploymentPodReadiness() = %+v, want not Ready with reason %q", got, tc.want)
				}
			})
		}
	}
}

// With the default OrderedUpdate strategy, a pod template whose mysql
// container cannot start is rolled onto one follower and no further: the
// update stalls there (a PodNotReadyError naming the launcher's exit), the
// active site is neither handed off nor rolled, and the next attempt
// refuses to roll anything while that site is not Ready.
func TestExecuteTargets_BrokenLauncherStopsAtTheFirstFollower(t *testing.T) {
	uc := podGateController()
	active := &testutil.FakeMySQL{ReadOnlyVal: false}
	standby, reader := directReplicaOf("primary-internal"), directReplicaOf("primary-internal")
	activePod, readerPod := &fakeSitePod{name: "mysql-lion-primary-a"}, &fakeSitePod{name: "mysql-lion-reader-c"}
	// The standby's pod is Ready until it gets the new template; from
	// then on its mysql container crash-loops on the launcher's exit.
	var standbyBroken atomic.Bool
	standbyProbe := func(context.Context) (PodReadiness, error) {
		if standbyBroken.Load() {
			return PodReadiness{Pod: "mysql-lion-standby-b", Reason: launcherExit127}, nil
		}
		return PodReadiness{Ready: true, Pod: "mysql-lion-standby-b"}, nil
	}
	var applied []string
	targets := func() (UpdateTarget, []UpdateTarget) {
		return UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active, Drifted: true, PodReady: activePod.probe},
			[]UpdateTarget{
				{Name: "standby", Host: "standby-internal", Checker: standby, Promotable: true, Drifted: len(applied) == 0,
					ExpectedSource: "primary-internal", PodReady: standbyProbe},
				{Name: "reader", Host: "reader-internal", Checker: reader, Drifted: true,
					ExpectedSource: "primary-internal", PodReady: readerPod.probe},
			}
	}
	apply := func(_ context.Context, name string) error {
		applied = append(applied, name)
		if name == "standby" {
			standbyBroken.Store(true)
		}
		return nil
	}
	moved := func(site, _ string) { t.Errorf("authority moved to %s", site) }

	a, f := targets()
	processed, err := uc.ExecuteTargets(context.Background(), a, f, apply, moved)
	stall := requirePodStall(t, err, "standby", true)
	if stall.Pod != "mysql-lion-standby-b" || stall.Reason != launcherExit127 {
		t.Fatalf("stall = %+v, want the pod whose launcher exits 127", stall)
	}
	if got := strings.Join(applied, ","); got != "standby" || len(processed) != 0 {
		t.Fatalf("applied = %q, processed = %v, want only the first follower rolled and none done", got, processed)
	}

	// The retry: the standby carries the new template now (no drift) and
	// still crash-loops. Nothing else is rolled, and nothing is handed off.
	a, f = targets()
	processed, err = uc.ExecuteTargets(context.Background(), a, f, apply, moved)
	stall = requirePodStall(t, err, "standby", false)
	if stall.Reason != launcherExit127 {
		t.Fatalf("stall on the retry = %+v, want the same cause", stall)
	}
	if got := strings.Join(applied, ","); got != "standby" || len(processed) != 0 {
		t.Fatalf("applied = %q, processed = %v after the retry, want no other site rolled", got, processed)
	}
	if fenced(active) || standby.Promoted || !standby.ReadOnlyVal {
		t.Fatalf("handoff ran: active fenced=%v, standby promoted=%v read_only=%v", fenced(active), standby.Promoted, standby.ReadOnlyVal)
	}
}
