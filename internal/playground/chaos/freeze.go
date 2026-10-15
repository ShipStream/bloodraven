package chaos

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FreezerLabelValue is the app label on every mysqld freezer pod, so
// GlobalRecover can delete (and therefore thaw) any a scenario leaked.
const FreezerLabelValue = "chaos-freezer"

// freezerScript finds the mysqld process belonging to one pod (by the
// pod UID in its cgroup path; systemd-driver paths spell the UID with
// underscores) and holds it in SIGSTOP. It thaws on SIGTERM, so deleting
// the pod thaws mysqld, and it thaws on its own after HOLD seconds, so
// a runner that dies mid-scenario cannot leave mysqld frozen.
const freezerScript = `set -u
uid_us=$(echo "$POD_UID" | tr - _)
pid=""
for d in /proc/[0-9]*; do
  [ "$(cat "$d/comm" 2>/dev/null)" = mysqld ] || continue
  if grep -q -e "$POD_UID" -e "$uid_us" "$d/cgroup" 2>/dev/null; then pid=${d#/proc/}; break; fi
done
if [ -z "$pid" ]; then echo "FREEZE-ERROR no mysqld process for pod $POD_UID"; exit 1; fi
thaw() { kill -CONT "$pid" 2>/dev/null; echo "THAWED pid=$pid"; exit 0; }
trap thaw TERM INT
kill -STOP "$pid" || { echo "FREEZE-ERROR kill -STOP $pid failed"; exit 1; }
echo "FROZEN pid=$pid"
sleep "$HOLD" & wait $!
thaw
`

// Freeze is a running mysqld freeze started by FreezeSiteMysqld.
type Freeze struct {
	Site     string
	PodName  string
	PID      string
	FrozenAt time.Time

	a *Actions
}

// FreezeSiteMysqld stops the site's mysqld with SIGSTOP for hold, then
// resumes it. A stopped mysqld is a hung server, not a dead one: TCP
// connects still succeed and every query blocks until the client's read
// timeout. Pod kills and scale-to-0 produce refused connections, which
// fail fast and hide every bug that only a hang exposes.
//
// SIGSTOP cannot be sent from inside the pod: mysqld is PID 1 of its
// container, and the kernel drops signals to a namespace's init that it
// has no handler for. The freezer is a privileged hostPID pod on the
// same node instead, which signals mysqld from the node's PID namespace.
//
// The freeze ends on its own after hold. The pushed reverter deletes the
// freezer pod, which thaws mysqld early.
func (a *Actions) FreezeSiteMysqld(ctx context.Context, site string, hold time.Duration) (*Freeze, error) {
	target, err := a.K.GetSiteMysqlPod(ctx, a.Namespace, a.FG, site)
	if err != nil {
		return nil, err
	}
	if target.Spec.NodeName == "" {
		return nil, fmt.Errorf("mysql pod %s is not scheduled", target.Name)
	}
	name := fmt.Sprintf("chaos-freeze-%s-%d", site, time.Now().UnixMilli())
	privileged := true
	grace := int64(10)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: a.Namespace,
			Labels:    map[string]string{"app": FreezerLabelValue, "shipstream.io/site": site},
		},
		Spec: corev1.PodSpec{
			NodeName:                      target.Spec.NodeName,
			HostPID:                       true,
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name:            "freezer",
				Image:           "busybox:1.36",
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", freezerScript},
				Env: []corev1.EnvVar{
					{Name: "POD_UID", Value: string(target.UID)},
					{Name: "HOLD", Value: fmt.Sprintf("%d", int(hold.Seconds()))},
				},
				SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
			}},
		},
	}
	if err := a.K.CreatePod(ctx, a.Namespace, pod); err != nil {
		return nil, fmt.Errorf("create freezer pod %s: %w", name, err)
	}
	ns := a.Namespace
	a.push(fmt.Sprintf("delete freezer pod %s (thaws mysqld on %s)", name, site), func(ctx context.Context) error {
		return a.K.DeletePodByName(ctx, ns, name)
	})

	f := &Freeze{Site: site, PodName: name, a: a}
	deadline := time.Now().Add(90 * time.Second)
	for {
		logs, _ := a.K.PodLogTailLines(ctx, a.Namespace, name, "freezer", 20)
		for _, line := range strings.Split(string(logs), "\n") {
			if pid, ok := strings.CutPrefix(strings.TrimSpace(line), "FROZEN pid="); ok {
				f.PID = pid
				f.FrozenAt = time.Now()
				return f, nil
			}
			if strings.Contains(line, "FREEZE-ERROR") {
				return nil, fmt.Errorf("freezer %s: %s", name, strings.TrimSpace(line))
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("freezer pod %s did not report FROZEN within 90s (last log: %q)", name, strings.TrimSpace(string(logs)))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// WaitThawed waits for the freezer to resume mysqld at the end of its
// hold and returns when it did. It deletes the finished pod.
func (f *Freeze) WaitThawed(ctx context.Context) (time.Time, error) {
	for {
		pod, err := f.a.K.GetPod(ctx, f.a.Namespace, f.PodName)
		if err != nil {
			return time.Time{}, fmt.Errorf("get freezer pod %s: %w", f.PodName, err)
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			logs, _ := f.a.K.PodLogTailLines(ctx, f.a.Namespace, f.PodName, "freezer", 20)
			if !strings.Contains(string(logs), "THAWED pid=") {
				return time.Time{}, fmt.Errorf("freezer %s exited without THAWED (logs: %q)", f.PodName, strings.TrimSpace(string(logs)))
			}
			thawedAt := time.Now()
			if term := freezerTerminatedAt(pod); !term.IsZero() {
				thawedAt = term
			}
			_ = f.a.K.DeletePodByName(ctx, f.a.Namespace, f.PodName)
			return thawedAt, nil
		case corev1.PodFailed:
			return time.Time{}, fmt.Errorf("freezer %s failed", f.PodName)
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Thaw resumes mysqld now by deleting the freezer pod.
func (f *Freeze) Thaw(ctx context.Context) error {
	return f.a.K.DeletePodByName(ctx, f.a.Namespace, f.PodName)
}

func freezerTerminatedAt(pod *corev1.Pod) time.Time {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			return cs.State.Terminated.FinishedAt.Time
		}
	}
	return time.Time{}
}
