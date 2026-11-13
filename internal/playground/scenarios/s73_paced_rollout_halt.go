package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario73PacedRolloutHalt())
}

const (
	s73ID = "73-paced-rollout-bad-build-halt"

	// s73BadTagSuffix makes a sidecar image reference no registry and no
	// node has: the "bad build" whose pods never become Ready.
	s73BadTagSuffix = "-s73-missing"

	// The pacer's durations for the scenario. A real stall is found by the
	// ordered updater after its 5m wait for the rolled pod, or by
	// stallAfter (default 30m); the scenario shortens stallAfter instead of
	// waiting. The soak (default 1m) is shortened too.
	s73Soak           = "10s"
	s73BadStallAfter  = "45s"
	s73GoodStallAfter = "30m"

	// s73HaltHold is how long the halted rollout is watched after the
	// operator restarted and its pacer ran: six pacer passes.
	s73HaltHold = 30 * time.Second
)

// s73Run is the state the steps of one run share.
type s73Run struct {
	groups   []string // the auxiliary groups
	original string   // the operator's default sidecar image before the run
	bad      string
	good     string
	observer *pacedObserver
	base     pacedSample // every group (auxiliary and playground) before the bad release
	first    string      // the group the pacer admitted
	second   string      // the group that waits
	started  *v1alpha1.RolloutStatus
	// Times for the measurements.
	badAt, stalledAt, fixAt, firstDoneAt, secondDoneAt time.Time
}

var (
	s73Mu   sync.Mutex
	s73Runs = map[*runner.Env]*s73Run{}
)

func s73State(env *runner.Env) *s73Run {
	s73Mu.Lock()
	defer s73Mu.Unlock()
	r, ok := s73Runs[env]
	if !ok {
		r = &s73Run{}
		s73Runs[env] = r
	}
	return r
}

// scenario73PacedRolloutHalt runs a release-driven pod change through the
// paced rollout (proposals/23-paced-rollouts.md, issue #244) against the
// real operator: a bad release first, which must stop at the first group,
// then a good one, which must finish that group before the next starts.
//
// The release is a change of the operator's default sidecar image
// (BLOODRAVEN_SIDECAR_IMAGE on the operator Deployment). It changes the
// renderer identity and the spec hash of every group that does not pin
// spec.sidecar.image, which is what a new operator release does. The bad
// release is a tag that cannot be pulled; the good one is the same sidecar
// build under a second tag (bloodraven-sidecar:playground-next).
//
// Two auxiliary groups are created for the run (paced_rollout_helpers.go
// says where they can run and why). The playground group is pinned to the
// sidecar image it already runs, so the change of the default does not roll
// it: an ordered update of that group ends in a handoff, whose taint would
// evict the auxiliary pods.
func scenario73PacedRolloutHalt() runner.Scenario {
	return runner.Scenario{
		ID:    s73ID,
		Title: "Paced rollout: a bad release halts at the first group, a good one rolls one group at a time",
		Hypothesis: "When an operator release changes the pod template of two groups, the pacer admits one and the other keeps its pods (status.rollout.phase=Pending). " +
			"A release whose pods cannot become Ready stalls the admitted group (Stalled) before its primary is rolled or handed over, and the waiting group stays untouched (Pending/RolloutHalted), across an operator restart too. " +
			"After a release that works, the first group completes and only then the second is admitted and rolled; a group that pins its sidecar image is not rolled at all.",
		Risk:           "medium",
		DocLink:        "playground/chaos-scenarios.md#73-paced-rollout-bad-build-halt",
		Timeout:        30 * time.Minute,
		CleanupTimeout: 10 * time.Minute,
		Precheck:       s73Precheck,
		Steps: []runner.Step{
			s73Step(runner.PhaseInject, "remove what an abandoned run left, pin the playground group's sidecar image, create two auxiliary groups", s73Prepare),
			s73Step(runner.PhaseInject, "bad release: the operator's default sidecar image becomes a tag that cannot be pulled", s73BadRelease),
			s73Step(runner.PhaseObserve, "exactly one group is admitted and starts its roll; the other reports Pending and keeps its pods", s73ObserveOneAdmitted),
			s73Step(runner.PhaseObserve, "the roll stalls on the first follower: Stalled, and the waiting group reports RolloutHalted", s73ObserveStalled),
			s73Step(runner.PhaseVerify, "halted: the first group's primary is not rolled or handed over, the second group and the pinned group are untouched", s73VerifyHalt),
			s73Step(runner.PhaseVerify, "the halt survives an operator restart", s73VerifyHaltSurvivesRestart),
			s73Step(runner.PhaseInject, "good release: the operator's default sidecar image becomes the second tag of the same build", s73GoodRelease),
			s73Step(runner.PhaseObserve, "the first group completes its roll", s73ObserveFirstCompletes),
			s73Step(runner.PhaseObserve, "the second group is admitted after it, and completes", s73ObserveSecondCompletes),
			s73Step(runner.PhaseVerify, "one group at a time in every sample, both groups rolled and healthy, the pinned group never rolled", s73VerifyFinal),
		},
		Cleanup: s73Cleanup,
	}
}

// s73Step wraps a step so that a failure leaves the auxiliary groups and
// the sampled timeline in the capture directory: the runner's own capture
// covers the playground group only.
func s73Step(phase runner.Phase, name string, do func(context.Context, *runner.Env, *s73Run) error) runner.Step {
	return runner.Step{
		Phase: phase,
		Name:  name,
		Do: func(ctx context.Context, env *runner.Env) error {
			run := s73State(env)
			err := do(ctx, env, run)
			if err != nil {
				dumpCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var samples []pacedSample
				if run.observer != nil {
					samples = run.observer.Samples()
				}
				pacedDumpForensics(dumpCtx, env, append(append([]string{}, run.groups...), env.FG), samples)
			}
			return err
		},
	}
}

func s73Precheck(ctx context.Context, env *runner.Env) error {
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return err
	}
	image, pacing, err := pacedOperatorSidecarDefault(ctx, env)
	if err != nil {
		return fmt.Errorf("precheck: read the operator Deployment: %w", err)
	}
	if !pacing {
		return fmt.Errorf("precheck: paced rollouts are switched off on the operator (%s) — deploy with rollout.pacing=true (./playground/setup.sh)", pacedEnvPacing)
	}
	if image == "" {
		return fmt.Errorf("precheck: the operator Deployment sets no %s — run ./playground/setup.sh", pacedEnvSidecarImage)
	}
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if pin := strings.TrimSpace(fg.Spec.Sidecar.Image); pin != "" && fg.Annotations[pacedPinAnnotation] == "" {
		return fmt.Errorf("precheck: group %s pins spec.sidecar.image=%q; this scenario needs a group that follows the operator's default", env.FG, pin)
	}
	// The good release must already be on the node the auxiliary pods run
	// on. A node reports its images, so a missing tag is said now instead
	// of ten minutes in.
	zone := pacedSiteZone(fg, fg.Status.ActiveSite)
	if zone == "" {
		return fmt.Errorf("precheck: active site %q of group %s has no zone", fg.Status.ActiveSite, env.FG)
	}
	nodes, err := env.Kube.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("precheck: list nodes: %w", err)
	}
	next := s73ImageWithSuffix(image, pacedNextTagSuffix)
	if present, known := pacedImageOnNodes(nodes.Items, zone, next); known && !present {
		return fmt.Errorf("precheck: image %s is not on the node of zone %s — ./playground/setup.sh (or ./playground/rebuild.sh sidecar) tags and loads it", next, zone)
	}
	return nil
}

// s73ImageWithSuffix appends suffix to the tag of image
// ("repo/name:tag" -> "repo/name:tag<suffix>"). An image without a tag gets
// ":latest<suffix>".
func s73ImageWithSuffix(image, suffix string) string {
	if at := strings.Index(image, "@"); at >= 0 {
		image = image[:at]
	}
	slash := strings.LastIndex(image, "/")
	if colon := strings.LastIndex(image, ":"); colon > slash {
		return image + suffix
	}
	return image + ":latest" + suffix
}

func s73Prepare(ctx context.Context, env *runner.Env, run *s73Run) error {
	*run = s73Run{groups: append([]string(nil), pacedAuxGroupNames...)}

	// A run that was killed may have left auxiliary groups, a changed
	// operator environment or the pin. The cleanup is idempotent and reads
	// everything from the cluster.
	if err := s73RestoreCluster(ctx, env); err != nil {
		return fmt.Errorf("remove what an abandoned run left: %w", err)
	}

	image, _, err := pacedOperatorSidecarDefault(ctx, env)
	if err != nil {
		return err
	}
	run.original = image
	run.bad = s73ImageWithSuffix(image, s73BadTagSuffix)
	run.good = s73ImageWithSuffix(image, pacedNextTagSuffix)

	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	zone := pacedSiteZone(fg, fg.Status.ActiveSite)
	if err := pacedPinSidecar(ctx, env, image); err != nil {
		return fmt.Errorf("pin the sidecar image of group %s: %w", env.FG, err)
	}
	env.Capture.Note(fmt.Sprintf("group %s pinned to sidecar image %s; auxiliary groups %v go to zone %s (the active site %s)",
		env.FG, image, run.groups, zone, fg.Status.ActiveSite))
	if err := pacedCreateAuxGroups(ctx, env, zone); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	start := time.Now()
	if err := pacedWaitGroupsHealthy(waitCtx, env, run.groups, 10*time.Second); err != nil {
		return fmt.Errorf("auxiliary groups did not bootstrap: %w", err)
	}
	env.Capture.Measure(fmt.Sprintf("auxiliary groups %v healthy after %s", run.groups, time.Since(start).Round(time.Second)))
	// The pin must not have moved the playground group.
	return AssertHealthyBaseline(ctx, env)
}

func s73BadRelease(ctx context.Context, env *runner.Env, run *s73Run) error {
	all := append(append([]string{}, run.groups...), env.FG)
	base, err := pacedReadGroups(ctx, env.Kube, env.Namespace, all)
	if err != nil {
		return fmt.Errorf("read the groups before the release: %w", err)
	}
	for _, name := range run.groups {
		g := base.Groups[name]
		if g.Rollout != nil && g.Rollout.Phase != v1alpha1.RolloutPhaseComplete {
			return fmt.Errorf("group %s already has a rollout record before the release: %s", name, pacedRolloutString(g.Rollout))
		}
		for site, w := range g.Workloads {
			if w.SidecarImage != run.original {
				return fmt.Errorf("group %s site %s runs sidecar image %q, not the operator default %q", name, site, w.SidecarImage, run.original)
			}
		}
	}
	run.base = base
	run.observer = pacedStartObserver(env.Kube, env.Namespace, all)
	run.badAt = time.Now()
	env.Capture.Note(fmt.Sprintf("bad release: %s=%s (was %s), soak=%s stallAfter=%s", pacedEnvSidecarImage, run.bad, run.original, s73Soak, s73BadStallAfter))
	if err := pacedSetOperatorEnv(ctx, env, map[string]string{
		pacedEnvSidecarImage: run.bad,
		pacedEnvSoak:         s73Soak,
		pacedEnvStallAfter:   s73BadStallAfter,
	}); err != nil {
		return err
	}
	return s73RefreshMetrics(ctx, env)
}

// s73RefreshMetrics points the metrics scraper (and with it the failure
// capture) at the operator pod that runs now.
func s73RefreshMetrics(ctx context.Context, env *runner.Env) error {
	if env.RefreshMetrics == nil {
		return nil
	}
	refreshCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var err error
	for {
		if err = env.RefreshMetrics(refreshCtx); err == nil {
			return nil
		}
		select {
		case <-refreshCtx.Done():
			return fmt.Errorf("reopen the operator metrics port-forward: %w", err)
		case <-time.After(2 * time.Second):
		}
	}
}

func s73ObserveOneAdmitted(ctx context.Context, env *runner.Env, run *s73Run) error {
	// The first group is the one whose roll starts (startedTime, the
	// latch). The pacer's queue is by name, so paced-a is expected, but
	// the scenario follows whichever it is.
	waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	started := time.Now()
	for run.first == "" {
		for _, name := range run.groups {
			fg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, name)
			if err == nil && fg.Status.Rollout.Started() {
				run.first = name
				run.started = fg.Status.Rollout.DeepCopy()
			}
		}
		if run.first != "" {
			break
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("no group started its roll within %s of the release: %s", time.Since(started).Round(time.Second), s73Records(ctx, env, run))
		case <-time.After(time.Second):
		}
	}
	for _, name := range run.groups {
		if name != run.first {
			run.second = name
		}
	}
	env.Capture.Measure(fmt.Sprintf("first group %s started its roll %s after the bad release", run.first, time.Since(run.badAt).Round(time.Second)))

	// The other group waits, with a record that says so.
	if _, err := pacedWaitRollout(ctx, env, run.second, "status.rollout.phase=Pending", time.Minute, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		r := fg.Status.Rollout
		return r != nil && r.Phase == v1alpha1.RolloutPhasePending
	}); err != nil {
		return err
	}
	now, err := pacedReadGroup(ctx, env.Kube, env.Namespace, run.second)
	if err != nil {
		return err
	}
	if now.Rollout.InFlight() {
		return fmt.Errorf("group %s is admitted too (%s) while %s holds the slot", run.second, pacedRolloutString(now.Rollout), run.first)
	}
	if diff := pacedPodsUnchanged(run.base.Groups[run.second].Pods, now.Pods); diff != "" {
		return fmt.Errorf("waiting group %s did not keep its pods: %s", run.second, diff)
	}
	return nil
}

// s73Records is the rollout record of every auxiliary group, for messages.
func s73Records(ctx context.Context, env *runner.Env, run *s73Run) string {
	var parts []string
	for _, name := range run.groups {
		fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, name)
		if err != nil {
			parts = append(parts, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %s", name, pacedRolloutString(fg.Status.Rollout)))
	}
	return strings.Join(parts, "; ")
}

func s73ObserveStalled(ctx context.Context, env *runner.Env, run *s73Run) error {
	// The ordered update rolls the follower first; its pod cannot pull the
	// sidecar image and never becomes Ready.
	if _, err := pacedWaitRollout(ctx, env, run.first, "status.rollout.phase=Stalled", 4*time.Minute, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		r := fg.Status.Rollout
		return r != nil && r.Phase == v1alpha1.RolloutPhaseStalled && r.StalledTime != nil
	}); err != nil {
		return err
	}
	run.stalledAt = time.Now()
	env.Capture.Measure(fmt.Sprintf("rollout halted: %s reported Stalled %s after the bad release", run.first, run.stalledAt.Sub(run.badAt).Round(time.Second)))
	_, err := pacedWaitRollout(ctx, env, run.second, "status.rollout Pending/RolloutHalted", time.Minute, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		r := fg.Status.Rollout
		return r != nil && r.Phase == v1alpha1.RolloutPhasePending && r.Reason == "RolloutHalted"
	})
	return err
}

// s73HaltProblems lists what contradicts a halted rollout right now: the
// first group stalled with only followers on the bad release and its
// primary untouched, the second group and the pinned group untouched.
func s73HaltProblems(ctx context.Context, env *runner.Env, run *s73Run) ([]string, error) {
	now, err := pacedReadGroups(ctx, env.Kube, env.Namespace, append(append([]string{}, run.groups...), env.FG))
	if err != nil {
		return nil, err
	}
	return s73HaltProblemsIn(run, env.FG, now), nil
}

// s73HaltProblemsIn is s73HaltProblems on a sample.
func s73HaltProblemsIn(run *s73Run, mainGroup string, now pacedSample) []string {
	var bad []string
	first, second := now.Groups[run.first], now.Groups[run.second]
	baseFirst, baseSecond := run.base.Groups[run.first], run.base.Groups[run.second]

	if r := first.Rollout; r == nil || r.Phase != v1alpha1.RolloutPhaseStalled || r.StalledTime == nil || r.StartedTime == nil {
		bad = append(bad, fmt.Sprintf("%s is not Stalled: %s", run.first, pacedRolloutString(r)))
	} else if run.started != nil && run.started.StartedTime != nil && !r.StartedTime.Equal(run.started.StartedTime) {
		bad = append(bad, fmt.Sprintf("%s: startedTime moved from %s to %s", run.first, run.started.StartedTime.UTC().Format(time.RFC3339), r.StartedTime.UTC().Format(time.RFC3339)))
	}
	primary := baseFirst.ActiveSite
	if first.ActiveSite != primary {
		bad = append(bad, fmt.Sprintf("%s was handed over: active site %s, was %s", run.first, first.ActiveSite, primary))
	}
	if diff := pacedPodsUnchanged(map[string][]pacedPod{primary: baseFirst.Pods[primary]}, first.Pods); diff != "" {
		bad = append(bad, fmt.Sprintf("%s primary was rolled: %s", run.first, diff))
	}
	if w := first.Workloads[primary]; w != baseFirst.Workloads[primary] {
		bad = append(bad, fmt.Sprintf("%s primary Deployment was written: %+v, was %+v", run.first, w, baseFirst.Workloads[primary]))
	}
	rolled := 0
	for site, w := range first.Workloads {
		if site == primary {
			continue
		}
		if w.SidecarImage == run.bad {
			rolled++
			for _, p := range first.Pods[site] {
				if p.Ready {
					bad = append(bad, fmt.Sprintf("%s site %s pod %s is Ready on an image that cannot be pulled", run.first, site, p.UID))
				}
			}
		}
	}
	if rolled == 0 {
		bad = append(bad, fmt.Sprintf("%s: no follower Deployment carries the bad release %s", run.first, run.bad))
	}

	if r := second.Rollout; r == nil || r.Phase != v1alpha1.RolloutPhasePending || r.Reason != "RolloutHalted" {
		bad = append(bad, fmt.Sprintf("%s is not Pending/RolloutHalted: %s", run.second, pacedRolloutString(r)))
	}
	if diff := pacedPodsUnchanged(baseSecond.Pods, second.Pods); diff != "" {
		bad = append(bad, fmt.Sprintf("%s did not keep its pods: %s", run.second, diff))
	}
	if diff := pacedWorkloadsUnchanged(baseSecond.Workloads, second.Workloads); diff != "" {
		bad = append(bad, fmt.Sprintf("%s: %s", run.second, diff))
	}
	if second.ActiveSite != baseSecond.ActiveSite {
		bad = append(bad, fmt.Sprintf("%s active site moved from %s to %s", run.second, baseSecond.ActiveSite, second.ActiveSite))
	}

	main := now.Groups[mainGroup]
	if diff := pacedPodsUnchanged(run.base.Groups[mainGroup].Pods, main.Pods); diff != "" {
		bad = append(bad, fmt.Sprintf("pinned group %s did not keep its pods: %s", mainGroup, diff))
	}
	if main.Rollout.InFlight() {
		bad = append(bad, fmt.Sprintf("pinned group %s was admitted: %s", mainGroup, pacedRolloutString(main.Rollout)))
	}
	return bad
}

func s73VerifyHalt(ctx context.Context, env *runner.Env, run *s73Run) error {
	bad, err := s73HaltProblems(ctx, env, run)
	if err != nil {
		return err
	}
	if len(bad) > 0 {
		return fmt.Errorf("the rollout is not halted as it must be: %s", strings.Join(bad, "; "))
	}
	// Record the stall as stored, to compare after the restart.
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, run.first)
	if err != nil {
		return err
	}
	run.started = fg.Status.Rollout.DeepCopy()
	env.Capture.Note(fmt.Sprintf("halted: %s", s73Records(ctx, env, run)))
	return nil
}

func s73VerifyHaltSurvivesRestart(ctx context.Context, env *runner.Env, run *s73Run) error {
	killed, err := operatorPodNames(ctx, env)
	if err != nil {
		return err
	}
	if err := env.Chaos.KillOperator(ctx); err != nil {
		return err
	}
	restartCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	pod, err := waitReplacementOperatorPod(restartCtx, env, killed)
	if err != nil {
		return err
	}
	if err := s73RefreshMetrics(ctx, env); err != nil {
		return err
	}
	// The gauge is set by a pacer pass, and this operator process has
	// just started: once it reports the halt, its pacer has read the
	// records and decided on them.
	if err := env.Wait.UntilMetric(restartCtx, env.Metrics, "the restarted operator's pacer reports the halt (bloodraven_rollout_groups stalled=1, pending>=1)",
		func(snap *pgmetrics.Snapshot) (bool, string) {
			stalled, _ := snap.Gauge("bloodraven_rollout_groups", map[string]string{"phase": "stalled"})
			pending, _ := snap.Gauge("bloodraven_rollout_groups", map[string]string{"phase": "pending"})
			return stalled == 1 && pending >= 1, fmt.Sprintf("stalled=%v pending=%v", stalled, pending)
		}); err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("operator restarted (pod %s); watching the halt for %s", pod, s73HaltHold))

	deadline := time.Now().Add(s73HaltHold)
	for {
		bad, err := s73HaltProblems(ctx, env, run)
		if err != nil {
			return err
		}
		if len(bad) > 0 {
			return fmt.Errorf("after the operator restart the rollout did not stay halted: %s", strings.Join(bad, "; "))
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, run.first)
	if err != nil {
		return err
	}
	r := fg.Status.Rollout
	if r == nil || !r.StartedTime.Equal(run.started.StartedTime) || !r.StalledTime.Equal(run.started.StalledTime) || !r.AdmittedTime.Equal(run.started.AdmittedTime) {
		return fmt.Errorf("the stall record of %s changed across the operator restart: %s, was %s", run.first, pacedRolloutString(r), pacedRolloutString(run.started))
	}
	env.Capture.Measure(fmt.Sprintf("halt held for %s after the bad release, an operator restart included", time.Since(run.badAt).Round(time.Second)))
	return nil
}

func s73GoodRelease(ctx context.Context, env *runner.Env, run *s73Run) error {
	run.fixAt = time.Now()
	env.Capture.Note(fmt.Sprintf("good release: %s=%s, stallAfter=%s", pacedEnvSidecarImage, run.good, s73GoodStallAfter))
	if err := pacedSetOperatorEnv(ctx, env, map[string]string{
		pacedEnvSidecarImage: run.good,
		pacedEnvSoak:         s73Soak,
		pacedEnvStallAfter:   s73GoodStallAfter,
	}); err != nil {
		return err
	}
	return s73RefreshMetrics(ctx, env)
}

func s73ObserveFirstCompletes(ctx context.Context, env *runner.Env, run *s73Run) error {
	fg, err := pacedWaitRollout(ctx, env, run.first, "status.rollout.phase=Complete", 8*time.Minute, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		r := fg.Status.Rollout
		return r != nil && r.Phase == v1alpha1.RolloutPhaseComplete && r.CompletedTime != nil
	})
	if err != nil {
		return err
	}
	run.firstDoneAt = time.Now()
	r := fg.Status.Rollout
	// The record is the one that stalled: retargeted to the good release,
	// not started over.
	if !r.StartedTime.Equal(run.started.StartedTime) || !r.AdmittedTime.Equal(run.started.AdmittedTime) {
		return fmt.Errorf("group %s completed on another admission than the one that stalled: %s, was %s", run.first, pacedRolloutString(r), pacedRolloutString(run.started))
	}
	env.Capture.Measure(fmt.Sprintf("first group %s completed %s after the good release", run.first, run.firstDoneAt.Sub(run.fixAt).Round(time.Second)))
	return nil
}

func s73ObserveSecondCompletes(ctx context.Context, env *runner.Env, run *s73Run) error {
	if _, err := pacedWaitRollout(ctx, env, run.second, "status.rollout.phase=Complete", 8*time.Minute, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		r := fg.Status.Rollout
		return r != nil && r.Phase == v1alpha1.RolloutPhaseComplete && r.CompletedTime != nil
	}); err != nil {
		return err
	}
	run.secondDoneAt = time.Now()
	env.Capture.Measure(fmt.Sprintf("second group %s completed %s after the first", run.second, run.secondDoneAt.Sub(run.firstDoneAt).Round(time.Second)))
	return nil
}

func s73VerifyFinal(ctx context.Context, env *runner.Env, run *s73Run) error {
	// Both groups end healthy.
	healthyCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if err := pacedWaitGroupsHealthy(healthyCtx, env, run.groups, 5*time.Second); err != nil {
		return err
	}
	all := append(append([]string{}, run.groups...), env.FG)
	final, err := pacedReadGroups(ctx, env.Kube, env.Namespace, all)
	if err != nil {
		return err
	}
	samples := run.observer.Stop()
	if n, lastErr := run.observer.Failures(); n > 0 {
		env.Capture.Note(fmt.Sprintf("%d of %d samples could not be read (last: %v)", n, n+len(samples), lastErr))
	}
	if len(samples) < 30 {
		return fmt.Errorf("only %d samples were taken; the invariants need the whole run", len(samples))
	}
	env.Capture.Note("timeline:\n  " + strings.Join(pacedTimeline(samples, all), "\n  "))

	var problems []error
	problems = append(problems, pacedCheckOneAtATime(samples))
	problems = append(problems, pacedCheckHeldGroupsKeepPods(run.base, samples, run.groups))
	order := []string{run.first, run.second}
	problems = append(problems, pacedCheckSequential(run.base, samples, final, order))
	if seen, err := pacedAdmissionOrder(samples, run.groups); err != nil {
		problems = append(problems, err)
	} else if strings.Join(seen, ",") != strings.Join(order, ",") {
		problems = append(problems, fmt.Errorf("groups were seen admitted in the order %v, want %v", seen, order))
	}
	renderers := map[string]bool{}
	for _, name := range run.groups {
		renderer, err := pacedCheckRolled(run.base.Groups[name], final.Groups[name])
		if err != nil {
			problems = append(problems, err)
			continue
		}
		renderers[renderer] = true
		for site, w := range final.Groups[name].Workloads {
			if w.SidecarImage != run.good {
				problems = append(problems, fmt.Errorf("group %s site %s runs sidecar image %q, want the good release %q", name, site, w.SidecarImage, run.good))
			}
		}
	}
	if len(renderers) > 1 {
		problems = append(problems, fmt.Errorf("the groups ended on different renderers: %v", renderers))
	}
	// The pinned group: restamped at most, never rolled.
	for _, s := range append(samples, final) {
		if diff := pacedPodsUnchanged(run.base.Groups[env.FG].Pods, s.Groups[env.FG].Pods); diff != "" {
			problems = append(problems, fmt.Errorf("pinned group %s was rolled by a change of the default sidecar image: %s", env.FG, diff))
			break
		}
	}
	if err := errors.Join(problems...); err != nil {
		return err
	}
	fr, sr := final.Groups[run.first].Rollout, final.Groups[run.second].Rollout
	env.Capture.Measure(fmt.Sprintf("second group admitted %s after the first completed (soak %s); %d samples, never more than one group admitted",
		sr.AdmittedTime.Sub(fr.CompletedTime.Time).Round(time.Second), s73Soak, len(samples)))
	return nil
}

// s73Cleanup is the scenario's Cleanup: it runs on a pass and on a failure.
func s73Cleanup(ctx context.Context, env *runner.Env) error {
	s73Mu.Lock()
	run := s73Runs[env]
	delete(s73Runs, env)
	s73Mu.Unlock()
	if run != nil && run.observer != nil {
		run.observer.Stop()
	}
	return s73RestoreCluster(ctx, env)
}

// s73RestoreCluster undoes everything the scenario changes, from what the
// cluster shows, so it also cleans up after a run that was killed: the
// auxiliary groups first (the operator runs their finalizer, and a group
// that is gone is not rolled back), then the operator's environment, then
// the pin.
func s73RestoreCluster(ctx context.Context, env *runner.Env) error {
	var errs []error
	if err := pacedDeleteAuxGroups(ctx, env); err != nil {
		errs = append(errs, err)
	}
	restored, err := pacedRestoreOperatorEnv(ctx, env)
	if err != nil {
		errs = append(errs, err)
	}
	if restored {
		if err := s73RefreshMetrics(ctx, env); err != nil {
			errs = append(errs, err)
		}
	}
	// With the default back, the pin and the default name the same image
	// again: removing the pin changes no hash.
	if err == nil {
		if err := pacedUnpinSidecar(ctx, env); err != nil {
			errs = append(errs, err)
		}
	} else {
		errs = append(errs, fmt.Errorf("the sidecar pin of group %s is left in place because the operator's default was not restored; remove spec.sidecar.image and the %s annotation once it is", env.FG, pacedPinAnnotation))
	}
	return errors.Join(errs...)
}
