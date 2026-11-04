package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

const sidecarCrashLoop = "sidecar not ready: CrashLoopBackOff, last exit code 1 (Error)"

// fakeSitePod is a site's pod as the readiness probe reports it. A broken
// pod reports not Ready until healAfter further probes have seen it broken
// (never when healAfter is 0). A flapping pod reports Ready on every probe
// but with one more restart each time: a crash loop sampled between crashes.
type fakeSitePod struct {
	name string

	mu        sync.Mutex
	broken    bool
	healAfter int
	flapping  bool
	err       error
	restarts  int32
	probes    int
	readyRun  int // consecutive Ready reports
	maxRun    int
}

func (p *fakeSitePod) breakIt() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.broken = true
}

func (p *fakeSitePod) probe(context.Context) (PodReadiness, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probes++
	if p.err != nil {
		p.readyRun = 0
		return PodReadiness{}, p.err
	}
	if p.broken {
		if p.healAfter > 0 {
			p.healAfter--
			p.broken = p.healAfter > 0
		}
		p.readyRun = 0
		return PodReadiness{Pod: p.name, Restarts: p.restarts, Reason: sidecarCrashLoop}, nil
	}
	if p.flapping {
		p.restarts++
	}
	p.readyRun++
	p.maxRun = max(p.maxRun, p.readyRun)
	return PodReadiness{Ready: true, Pod: p.name, Restarts: p.restarts}, nil
}

func (p *fakeSitePod) longestReadyRun() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxRun
}

func directReplicaOf(source string) *testutil.FakeMySQL {
	return &testutil.FakeMySQL{ReadOnlyVal: true, ReplicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: source,
	}}
}

func podGateController() *UpdateController {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.rolledSiteTimeout = 100 * time.Millisecond
	uc.handoff = HandoffOptions{PreFenceCatchUp: time.Second, CatchUp: time.Second, PollInterval: time.Millisecond}
	return uc
}

func requirePodStall(t *testing.T, err error, site string, rolled bool) *PodNotReadyError {
	t.Helper()
	var stall *PodNotReadyError
	if !errors.As(err, &stall) {
		t.Fatalf("ExecuteTargets() error = %v, want a *PodNotReadyError", err)
	}
	if stall.Site != site || stall.Rolled != rolled {
		t.Fatalf("stall = %+v, want site %s rolled=%v", stall, site, rolled)
	}
	return stall
}

func fenced(c *testutil.FakeMySQL) bool { return slices.Contains(c.GetCalls(), "SetSuperReadOnly(ON)") }

// Issue #209: the rolled standby's MySQL is a healthy direct replica, but
// its new pod's sidecar crash-loops. The update must stop there: no
// handoff, no fence on the active site, and the active site is not rolled.
func TestExecuteTargets_StopsOnRolledFollowerWhosePodIsNotReady(t *testing.T) {
	uc := podGateController()
	active := &testutil.FakeMySQL{ReadOnlyVal: false}
	standby := directReplicaOf("primary-internal")
	activePod, standbyPod := &fakeSitePod{name: "mysql-lion-primary-a"}, &fakeSitePod{name: "mysql-lion-standby-b"}
	var applied []string
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active, Drifted: true, PodReady: activePod.probe},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: standby, Promotable: true, Drifted: true,
			ExpectedSource: "primary-internal", PodReady: standbyPod.probe}},
		func(_ context.Context, name string) error {
			applied = append(applied, name)
			if name == "standby" {
				standbyPod.breakIt()
			}
			return nil
		}, func(site, _ string) { t.Errorf("authority moved to %s", site) })

	stall := requirePodStall(t, err, "standby", true)
	if stall.Pod != "mysql-lion-standby-b" || stall.Reason != sidecarCrashLoop || stall.Waited != uc.rolledSiteTimeout {
		t.Fatalf("stall = %+v, want the crash-looping pod after the full wait", stall)
	}
	if !strings.Contains(err.Error(), "wait for follower standby: site standby was updated but its pod mysql-lion-standby-b did not become Ready within 100ms ("+sidecarCrashLoop+")") {
		t.Fatalf("error = %q", err)
	}
	if got := strings.Join(applied, ","); got != "standby" {
		t.Fatalf("applied = %q, want only the standby; the active site must not be rolled", got)
	}
	if len(processed) != 0 {
		t.Fatalf("processed = %v, want none", processed)
	}
	if fenced(active) || standby.Promoted || !standby.ReadOnlyVal {
		t.Fatalf("handoff ran: active fenced=%v, standby promoted=%v read_only=%v", fenced(active), standby.Promoted, standby.ReadOnlyVal)
	}
}

// A pod that takes a few probes to come up passes once it has been Ready
// twice in a row, and the rollout goes on to the handoff and the old active.
func TestExecuteTargets_ProceedsOnceRolledPodIsReady(t *testing.T) {
	source := newOldActive(handoffUUID+":1-100", handoffUUID+":1-100")
	candidate := handoffCandidate(handoffUUID + ":1-100")
	activePod := &fakeSitePod{name: "mysql-lion-primary-a"}
	standbyPod := &fakeSitePod{name: "mysql-lion-standby-b"}
	uc := podGateController()
	uc.rolledSiteTimeout = 5 * time.Second
	var applied []string
	var readyRunAtHandoff int
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: source, Drifted: true, PodReady: activePod.probe},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: candidate.Checker(), Promotable: true, Drifted: true,
			ExpectedSource: "primary-internal", PodReady: standbyPod.probe}},
		func(_ context.Context, name string) error {
			applied = append(applied, name)
			switch name {
			case "standby":
				standbyPod.mu.Lock()
				standbyPod.broken, standbyPod.healAfter = true, 3
				standbyPod.mu.Unlock()
			case "primary":
				source.restart()
			}
			return nil
		}, func(string, string) { readyRunAtHandoff = standbyPod.longestReadyRun() })
	if err != nil {
		t.Fatalf("ExecuteTargets() error = %v", err)
	}
	if got := strings.Join(processed, ","); got != "standby,primary" {
		t.Fatalf("processed = %q, want standby,primary", got)
	}
	if got := strings.Join(applied, ","); got != "standby,primary" {
		t.Fatalf("applied = %q", got)
	}
	if readyRunAtHandoff < 2 {
		t.Fatalf("handoff ran after %d consecutive Ready probes of the rolled standby, want at least 2", readyRunAtHandoff)
	}
}

// After the handoff the old active is rolled; its new pod never becomes
// Ready. The handoff stands, but the old active is reported, not
// processed.
func TestExecuteTargets_OldActiveWhosePodIsNotReadyIsNotProcessed(t *testing.T) {
	source := newOldActive(handoffUUID+":1-100", handoffUUID+":1-100")
	candidate := handoffCandidate(handoffUUID + ":1-100")
	activePod := &fakeSitePod{name: "mysql-lion-primary-a"}
	standbyPod := &fakeSitePod{name: "mysql-lion-standby-b"}
	uc := podGateController()
	var promoted string
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: source, Drifted: true, PodReady: activePod.probe},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: candidate.Checker(), Promotable: true,
			ExpectedSource: "primary-internal", PodReady: standbyPod.probe}},
		func(_ context.Context, name string) error {
			if name == "primary" {
				source.restart()
				activePod.breakIt()
			}
			return nil
		}, func(site, _ string) { promoted = site })

	requirePodStall(t, err, "primary", true)
	if !strings.Contains(err.Error(), "wait for old active primary to replicate from standby: site primary was updated") {
		t.Fatalf("error = %q", err)
	}
	if promoted != "standby" {
		t.Fatalf("promoted = %q, want the Ready standby", promoted)
	}
	if slices.Contains(processed, "primary") {
		t.Fatalf("processed = %v; the old active's pod is not Ready", processed)
	}
}

// The primary watch runs for the whole wait: a primary that dies while the
// rolled pod is still not Ready ends the update at once, not after the
// wait budget, so emergency failover is not held off.
func TestExecuteTargets_PrimaryLossDuringPodWaitAbortsPromptly(t *testing.T) {
	uc := podGateController()
	uc.rolledSiteTimeout = time.Minute
	uc.primaryLossWindow = 10 * time.Millisecond
	active := &flakyReadOnlyChecker{FakeMySQL: &testutil.FakeMySQL{ReadOnlyVal: false}}
	standby := directReplicaOf("primary-internal")
	standbyPod := &fakeSitePod{name: "mysql-lion-standby-b"}
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := uc.ExecuteTargets(context.Background(),
			UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active, Drifted: true},
			[]UpdateTarget{{Name: "standby", Checker: standby, Promotable: true, Drifted: true,
				ExpectedSource: "primary-internal", PodReady: standbyPod.probe}},
			func(_ context.Context, name string) error {
				if name == "standby" {
					standbyPod.breakIt()
					active.mu.Lock()
					active.failFor = -1
					active.mu.Unlock()
				}
				return nil
			}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errActivePrimaryLost) {
			t.Fatalf("ExecuteTargets() error = %v, want errActivePrimaryLost", err)
		}
		if waited := time.Since(start); waited > 10*time.Second {
			t.Fatalf("primary loss took %s to end the update", waited)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the pod wait held the update after the active primary was lost")
	}
	if uc.IsUpdating() {
		t.Fatal("update lock still held after abort")
	}
}

// An earlier attempt rolled the standby, so it has no drift left, but its
// pod never became Ready. MySQL still answers through the -internal
// Service, which is how the update used to hand the active site to it and
// roll the last Ready site too.
func TestExecuteTargets_RefusesHandoffToUpdatedStandbyWhosePodIsNotReady(t *testing.T) {
	uc := podGateController()
	active := &testutil.FakeMySQL{ReadOnlyVal: false}
	standby := directReplicaOf("primary-internal")
	standbyPod := &fakeSitePod{name: "mysql-lion-standby-b", broken: true}
	var applied []string
	_, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active, Drifted: true, PodReady: (&fakeSitePod{name: "a"}).probe},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: standby, Promotable: true,
			ExpectedSource: "primary-internal", PodReady: standbyPod.probe}},
		func(_ context.Context, name string) error { applied = append(applied, name); return nil },
		func(site, _ string) { t.Errorf("authority moved to %s", site) })

	stall := requirePodStall(t, err, "standby", false)
	if stall.Next != "handing off the active site primary" {
		t.Fatalf("stall.Next = %q", stall.Next)
	}
	want := "site standby already runs the new pod template but its pod mysql-lion-standby-b is not Ready (" + sidecarCrashLoop + "); not handing off the active site primary"
	if err.Error() != want {
		t.Fatalf("error = %q\nwant    %q", err, want)
	}
	if len(applied) != 0 || fenced(active) || standby.Promoted {
		t.Fatalf("applied=%v fenced=%v promoted=%v, want nothing done", applied, fenced(active), standby.Promoted)
	}
}

// A follower already on the new template whose pod is not Ready keeps the
// next follower from being rolled, so a broken template stops at one site
// even across attempts.
func TestExecuteTargets_DoesNotRollFollowerWhileUpdatedSiteIsNotReady(t *testing.T) {
	uc := podGateController()
	broken := &fakeSitePod{name: "mysql-lion-a-1", broken: true}
	var applied []string
	_, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: &testutil.FakeMySQL{ReadOnlyVal: false}},
		[]UpdateTarget{
			{Name: "a", Checker: directReplicaOf("primary-internal"), ExpectedSource: "primary-internal", PodReady: broken.probe},
			{Name: "b", Checker: directReplicaOf("primary-internal"), Drifted: true, ExpectedSource: "primary-internal",
				PodReady: (&fakeSitePod{name: "mysql-lion-b-1"}).probe},
		},
		func(_ context.Context, name string) error { applied = append(applied, name); return nil }, nil)

	stall := requirePodStall(t, err, "a", false)
	if stall.Next != "updating follower b" {
		t.Fatalf("stall.Next = %q", stall.Next)
	}
	if len(applied) != 0 {
		t.Fatalf("applied = %v, want b left alone", applied)
	}
}

// A container that crashes between probes can be Ready on every probe; the
// restart count gives it away. It never counts as Ready, so the update
// stops instead of moving on.
func TestExecuteTargets_CrashLoopReadyBetweenProbesIsNotReady(t *testing.T) {
	uc := podGateController()
	pod := &fakeSitePod{name: "mysql-lion-a-1"}
	var applied []string
	_, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: &testutil.FakeMySQL{ReadOnlyVal: false}},
		[]UpdateTarget{
			{Name: "a", Checker: directReplicaOf("primary-internal"), Drifted: true, ExpectedSource: "primary-internal", PodReady: pod.probe},
			{Name: "b", Checker: directReplicaOf("primary-internal"), Drifted: true, ExpectedSource: "primary-internal",
				PodReady: (&fakeSitePod{name: "mysql-lion-b-1"}).probe},
		},
		func(_ context.Context, name string) error {
			applied = append(applied, name)
			if name == "a" {
				pod.mu.Lock()
				pod.flapping = true
				pod.mu.Unlock()
			}
			return nil
		}, nil)

	stall := requirePodStall(t, err, "a", true)
	if stall.Reason != "a container restarted between readiness probes" {
		t.Fatalf("stall.Reason = %q", stall.Reason)
	}
	if got := strings.Join(applied, ","); got != "a" {
		t.Fatalf("applied = %q, want only a", got)
	}
}

func TestPodReadyStreak(t *testing.T) {
	ready := func(pod string, restarts int32) PodReadiness {
		return PodReadiness{Ready: true, Pod: pod, Restarts: restarts}
	}
	notReady := PodReadiness{Pod: "p", Reason: sidecarCrashLoop}
	for _, tc := range []struct {
		name   string
		obs    []PodReadiness
		errAt  int // 1-based observation that fails, 0 for none
		stable bool
		reason string
	}{
		{name: "one Ready probe", obs: []PodReadiness{ready("p", 0)}, reason: "Ready for less than one probe interval"},
		{name: "two Ready probes", obs: []PodReadiness{ready("p", 0), ready("p", 0)}, stable: true},
		{name: "restart in between", obs: []PodReadiness{ready("p", 0), ready("p", 1)}, reason: "a container restarted between readiness probes"},
		{name: "Ready again after the restart", obs: []PodReadiness{ready("p", 0), ready("p", 1), ready("p", 1)}, stable: true},
		{name: "restart while not Ready", obs: []PodReadiness{{Pod: "p", Reason: sidecarCrashLoop}, ready("p", 1)}, reason: "a container restarted between readiness probes"},
		{name: "pod replaced in between", obs: []PodReadiness{ready("p", 0), ready("q", 0)}, reason: "Ready for less than one probe interval"},
		{name: "not Ready in between", obs: []PodReadiness{ready("p", 0), notReady, ready("p", 0)}, reason: "Ready for less than one probe interval"},
		{name: "Ready after not Ready", obs: []PodReadiness{notReady, ready("p", 0), ready("p", 0)}, stable: true},
		{name: "not Ready", obs: []PodReadiness{ready("p", 0), notReady}, reason: sidecarCrashLoop},
		{name: "probe error", obs: []PodReadiness{ready("p", 0), {}}, errAt: 2, reason: "pod readiness unknown: apiserver unavailable"},
		{name: "probe error in between", obs: []PodReadiness{ready("p", 0), {}, ready("p", 0)}, errAt: 2, reason: "Ready for less than one probe interval"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s podReadyStreak
			for i, o := range tc.obs {
				var err error
				if i+1 == tc.errAt {
					err = errors.New("apiserver unavailable")
				}
				s.observe(o, err)
			}
			if s.stable() != tc.stable {
				t.Fatalf("stable() = %v, want %v", s.stable(), tc.stable)
			}
			if !tc.stable && s.reason() != tc.reason {
				t.Fatalf("reason() = %q, want %q", s.reason(), tc.reason)
			}
		})
	}
}

// ReconcileSiteDeployment gives up on a rollout that does not complete in
// its own 5 minutes. When the pod is what is missing, that is the same
// stall as a pod that goes not Ready later, and the site counts as rolled.
func TestExecuteTargets_IncompleteRolloutOfNotReadyPodIsAStall(t *testing.T) {
	rolloutErr := &RolloutIncompleteError{Site: "a", Err: errors.New("timeout waiting for deployment ns/mysql-lion-a rollout: context deadline exceeded")}
	for _, tc := range []struct {
		name      string
		podBroken bool
		applyErr  error
		wantStall bool
	}{
		{name: "pod not Ready", podBroken: true, applyErr: rolloutErr, wantStall: true},
		{name: "pod Ready now", applyErr: rolloutErr},
		{name: "patch failed", podBroken: true, applyErr: errors.New("admission webhook refused the deployment")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc := podGateController()
			pod := &fakeSitePod{name: "mysql-lion-a-1", broken: tc.podBroken}
			_, err := uc.ExecuteTargets(context.Background(),
				UpdateTarget{Name: "primary", Host: "primary-internal", Checker: &testutil.FakeMySQL{ReadOnlyVal: false}},
				[]UpdateTarget{{Name: "a", Checker: directReplicaOf("primary-internal"), Drifted: true,
					ExpectedSource: "primary-internal", PodReady: pod.probe}},
				func(context.Context, string) error { return tc.applyErr }, nil)
			var stall *PodNotReadyError
			if got := errors.As(err, &stall); got != tc.wantStall {
				t.Fatalf("ExecuteTargets() error = %v, stall = %v, want %v", err, got, tc.wantStall)
			}
			if !errors.Is(err, tc.applyErr) {
				t.Fatalf("ExecuteTargets() error = %v lost the apply error", err)
			}
			if tc.wantStall {
				if !stall.Rolled || stall.Waited != 0 || stall.Reason != sidecarCrashLoop {
					t.Fatalf("stall = %+v", stall)
				}
				if want := "update follower a: site a was updated but its pod mysql-lion-a-1 is not Ready (" + sidecarCrashLoop + ")"; err.Error() != want {
					t.Fatalf("error = %q, want %q", err, want)
				}
			}
		})
	}
}

// A probe that fails is not a Ready verdict: the update does not take
// another site down on an unknown.
func TestExecuteTargets_PodProbeErrorRefusesToRoll(t *testing.T) {
	uc := podGateController()
	pod := &fakeSitePod{name: "mysql-lion-a-1", err: fmt.Errorf("list pods of deployment ns/mysql-lion-a: %w", errors.New("connection refused"))}
	var applied []string
	_, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: &testutil.FakeMySQL{ReadOnlyVal: false}},
		[]UpdateTarget{
			{Name: "a", Checker: directReplicaOf("primary-internal"), ExpectedSource: "primary-internal", PodReady: pod.probe},
			{Name: "b", Checker: directReplicaOf("primary-internal"), Drifted: true, ExpectedSource: "primary-internal"},
		},
		func(_ context.Context, name string) error { applied = append(applied, name); return nil }, nil)
	stall := requirePodStall(t, err, "a", false)
	if !strings.HasPrefix(stall.Reason, "pod readiness unknown: list pods of deployment") || len(applied) != 0 {
		t.Fatalf("stall = %+v applied = %v", stall, applied)
	}
}

// Without a probe (tests, or a manager the runner never wired) the gate is
// off for that site: the update waits on MySQL alone, as it did before.
// Sites that have a probe are still gated.
func TestExecuteTargets_SiteWithoutPodProbeIsGatedOnMySQLOnly(t *testing.T) {
	uc := podGateController()
	uc.rolledSiteTimeout = 5 * time.Second
	var applied []string
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: &testutil.FakeMySQL{ReadOnlyVal: false}},
		[]UpdateTarget{
			{Name: "unprobed", Checker: directReplicaOf("primary-internal"), ExpectedSource: "primary-internal"},
			{Name: "a", Checker: directReplicaOf("primary-internal"), Drifted: true, ExpectedSource: "primary-internal"},
			{Name: "b", Checker: directReplicaOf("primary-internal"), Drifted: true, ExpectedSource: "primary-internal",
				PodReady: (&fakeSitePod{name: "mysql-lion-b-1"}).probe},
		},
		func(_ context.Context, name string) error { applied = append(applied, name); return nil }, nil)
	if err != nil {
		t.Fatalf("ExecuteTargets() error = %v", err)
	}
	if got := strings.Join(processed, ","); got != "a,b" || strings.Join(applied, ",") != "a,b" {
		t.Fatalf("processed = %q applied = %v", got, applied)
	}
}
