package scenarios

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario74UpgradePacedRollout())
}

const (
	s74ID = "74-upgrade-paced-rollout"

	// s74UpgradeCommandEnv overrides the command that upgrades the
	// operator. It runs through `bash -c` in the runner's working
	// directory, with BLOODRAVEN_UPGRADE_KUBE_CONTEXT set to the context
	// the runner uses.
	s74UpgradeCommandEnv     = "BLOODRAVEN_UPGRADE_E2E_COMMAND"
	s74DefaultUpgradeCommand = "./playground/upgrade-e2e.sh upgrade"
	s74UpgradeScript         = "playground/upgrade-e2e.sh"
)

// s74Run is the state the steps of one run share.
type s74Run struct {
	groups    []string // auxiliary groups and the playground group
	observer  *pacedObserver
	base      pacedSample
	upgradeAt time.Time
}

var (
	s74Mu   sync.Mutex
	s74Runs = map[*runner.Env]*s74Run{}
)

func s74State(env *runner.Env) *s74Run {
	s74Mu.Lock()
	defer s74Mu.Unlock()
	r, ok := s74Runs[env]
	if !ok {
		r = &s74Run{}
		s74Runs[env] = r
	}
	return r
}

// scenario74UpgradePacedRollout is the upgrade E2E of issue #244 (C-8 in
// proposals/20-api-v1beta1.md): on a cluster that runs the previous
// release (the published v1.7.0 chart and images, installed by
// `./playground/upgrade-e2e.sh install`), with three failover groups, it
// upgrades the operator to the build under test and asserts that the pod
// change the new release causes is rolled one group at a time.
//
// v1.8.0 changes the pod template of every group
// (deployment-pod-render-v8-mysql-launcher), and nothing v1.7.0 rendered
// carries the rendered-by annotation, so every MySQL pod of every group
// must roll, paced.
//
// The playground group rolls too here, and its ordered update ends in a
// handoff whose db-readonly taint evicts other groups' pods from the old
// active site's node. The auxiliary groups therefore run on a spare node
// (paced_rollout_helpers.go), which the cluster must have.
//
// Quarantined like the encryption scenarios: it needs a cluster prepared
// for it, so no batch profile runs it. The e2e `upgrade` job names it.
func scenario74UpgradePacedRollout() runner.Scenario {
	return runner.Scenario{
		ID:    s74ID,
		Title: "Upgrade from the previous release: every group rolls, one at a time",
		Hypothesis: "Upgrading the operator from v1.7.0 changes the pod template of every failover group. The groups are rolled one at a time: no two groups hold an unfinished admission in any sample, " +
			"a group keeps its pods until every group admitted before it has a completedTime, the admission order matches admittedTime, and at the end every site Deployment carries the new rendered-by value and a new spec hash, on new pods, with every group Ready.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#74-upgrade-paced-rollout",
		Quarantine: "requires a cluster installed at the previous release with a spare node (./playground/upgrade-e2e.sh install); " +
			"the e2e `upgrade` job runs it by name",
		Timeout:        45 * time.Minute,
		CleanupTimeout: 8 * time.Minute,
		Precheck:       s74Precheck,
		Steps: []runner.Step{
			s74Step(runner.PhaseInject, "create two auxiliary groups on a spare node, under the previous release", s74Prepare),
			s74Step(runner.PhaseInject, "upgrade the operator to the build under test (CRDs first, then the chart)", s74Upgrade),
			s74Step(runner.PhaseObserve, "every group completes its paced roll", s74ObserveAllComplete),
			s74Step(runner.PhaseVerify, "one group at a time in every sample, in admittedTime order; every pod rolled; every group Ready", s74VerifyFinal),
		},
		Cleanup: s74Cleanup,
	}
}

func s74Step(phase runner.Phase, name string, do func(context.Context, *runner.Env, *s74Run) error) runner.Step {
	return runner.Step{
		Phase: phase,
		Name:  name,
		Do: func(ctx context.Context, env *runner.Env) error {
			run := s74State(env)
			err := do(ctx, env, run)
			if err != nil {
				dumpCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var samples []pacedSample
				if run.observer != nil {
					samples = run.observer.Samples()
				}
				pacedDumpForensics(dumpCtx, env, s74Groups(env), samples)
			}
			return err
		},
	}
}

func s74Groups(env *runner.Env) []string {
	return append(append([]string{}, pacedAuxGroupNames...), env.FG)
}

func s74UpgradeCommand() string {
	if cmd := strings.TrimSpace(os.Getenv(s74UpgradeCommandEnv)); cmd != "" {
		return cmd
	}
	return s74DefaultUpgradeCommand
}

func s74Precheck(ctx context.Context, env *runner.Env) error {
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return err
	}
	if os.Getenv(s74UpgradeCommandEnv) == "" {
		if _, err := os.Stat(s74UpgradeScript); err != nil {
			return fmt.Errorf("precheck: %s not found (%v) — run from the repository root, or set %s", s74UpgradeScript, err, s74UpgradeCommandEnv)
		}
	}
	// The previous release rendered the workloads: none carries the
	// renderer annotation, which v1.8.0 introduced.
	g, err := pacedReadGroup(ctx, env.Kube, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	for site, w := range g.Workloads {
		if w.RenderedBy != "" {
			return fmt.Errorf("precheck: the Deployment of site %s already carries %s=%s: this cluster does not run the previous release — install it with ./playground/upgrade-e2e.sh install on a fresh cluster",
				site, v1alpha1.RenderedByAnnotation, w.RenderedBy)
		}
	}
	return nil
}

func s74Prepare(ctx context.Context, env *runner.Env, run *s74Run) error {
	*run = s74Run{groups: s74Groups(env)}
	if err := s74RestoreCluster(ctx, env); err != nil {
		return fmt.Errorf("remove what an abandoned run left: %w", err)
	}
	zone, err := pacedEnsureSpareZone(ctx, env)
	if err != nil {
		return err
	}
	if err := pacedCreateAuxGroups(ctx, env, zone); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	start := time.Now()
	if err := pacedWaitGroupsHealthy(waitCtx, env, run.groups, 10*time.Second); err != nil {
		return fmt.Errorf("groups not healthy under the previous release: %w", err)
	}
	env.Capture.Measure(fmt.Sprintf("auxiliary groups %v healthy in zone %s after %s, under the previous release", pacedAuxGroupNames, zone, time.Since(start).Round(time.Second)))
	return nil
}

func s74Upgrade(ctx context.Context, env *runner.Env, run *s74Run) error {
	base, err := pacedReadGroups(ctx, env.Kube, env.Namespace, run.groups)
	if err != nil {
		return fmt.Errorf("read the groups before the upgrade: %w", err)
	}
	for _, name := range run.groups {
		g := base.Groups[name]
		if g.Rollout != nil {
			return fmt.Errorf("group %s has a rollout record before the upgrade: %s", name, pacedRolloutString(g.Rollout))
		}
		for site, w := range g.Workloads {
			if w.RenderedBy != "" {
				return fmt.Errorf("group %s site %s already carries %s before the upgrade", name, site, v1alpha1.RenderedByAnnotation)
			}
			if len(g.Pods[site]) != 1 {
				return fmt.Errorf("group %s site %s has %d pods before the upgrade, want 1", name, site, len(g.Pods[site]))
			}
		}
	}
	run.base = base
	run.observer = pacedStartObserver(env.Kube, env.Namespace, run.groups)
	run.upgradeAt = time.Now()

	command := s74UpgradeCommand()
	env.Capture.Note("upgrade command: " + command)
	cmdCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "bash", "-c", command)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "BLOODRAVEN_UPGRADE_KUBE_CONTEXT="+env.Kube.CurrentCtx)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upgrade command %q: %w", command, err)
	}
	if err := pacedWaitOperatorRolledOut(ctx, env); err != nil {
		return err
	}
	env.Capture.Measure(fmt.Sprintf("operator upgraded in %s", time.Since(run.upgradeAt).Round(time.Second)))
	return s73RefreshMetrics(ctx, env)
}

func s74ObserveAllComplete(ctx context.Context, env *runner.Env, run *s74Run) error {
	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Minute)
	defer cancel()
	started := time.Now()
	lastLog := started
	done := map[string]bool{}
	for {
		var state []string
		for _, name := range run.groups {
			fg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, name)
			if err != nil {
				state = append(state, fmt.Sprintf("%s: %v", name, err))
				continue
			}
			r := fg.Status.Rollout
			state = append(state, fmt.Sprintf("%s: %s", name, pacedRolloutString(r)))
			if r != nil && r.Phase == v1alpha1.RolloutPhaseStalled {
				return fmt.Errorf("the roll of group %s stalled during the upgrade: %s (%s)", name, pacedRolloutString(r), r.Message)
			}
			if r != nil && r.Phase == v1alpha1.RolloutPhaseComplete && r.CompletedTime != nil && !done[name] {
				done[name] = true
				env.Capture.Measure(fmt.Sprintf("group %s completed its roll %s after the upgrade", name, time.Since(run.upgradeAt).Round(time.Second)))
			}
		}
		if len(done) == len(run.groups) {
			return nil
		}
		if time.Since(lastLog) >= 15*time.Second {
			lastLog = time.Now()
			env.Logger.Info("wait", "what", "every group completes its paced roll", "last", strings.Join(state, "; "), "elapsed", time.Since(started).Round(time.Second))
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("not every group completed its roll within %s of the upgrade: %s", time.Since(started).Round(time.Second), strings.Join(state, "; "))
		case <-time.After(time.Second):
		}
	}
}

func s74VerifyFinal(ctx context.Context, env *runner.Env, run *s74Run) error {
	healthyCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := pacedWaitGroupsHealthy(healthyCtx, env, run.groups, 5*time.Second); err != nil {
		return err
	}
	final, err := pacedReadGroups(ctx, env.Kube, env.Namespace, run.groups)
	if err != nil {
		return err
	}
	samples := run.observer.Stop()
	if n, lastErr := run.observer.Failures(); n > 0 {
		env.Capture.Note(fmt.Sprintf("%d of %d samples could not be read (last: %v)", n, n+len(samples), lastErr))
	}
	if len(samples) < 30 {
		return fmt.Errorf("only %d samples were taken; the invariants need the whole rollout", len(samples))
	}
	env.Capture.Note("timeline:\n  " + strings.Join(pacedTimeline(samples, run.groups), "\n  "))

	var problems []error
	problems = append(problems, pacedCheckOneAtATime(samples))
	problems = append(problems, pacedCheckHeldGroupsKeepPods(run.base, samples, run.groups))

	// The order the samples saw the admissions in, against admittedTime.
	seen, err := pacedAdmissionOrder(samples, run.groups)
	if err != nil {
		problems = append(problems, err)
	} else {
		byTime := append([]string(nil), run.groups...)
		complete := true
		for _, name := range byTime {
			if r := final.Groups[name].Rollout; r == nil || r.AdmittedTime == nil {
				complete = false
			}
		}
		if complete {
			sort.SliceStable(byTime, func(i, j int) bool {
				return final.Groups[byTime[i]].Rollout.AdmittedTime.Time.Before(final.Groups[byTime[j]].Rollout.AdmittedTime.Time)
			})
			if strings.Join(seen, ",") != strings.Join(byTime, ",") {
				problems = append(problems, fmt.Errorf("groups were seen admitted in the order %v, but admittedTime orders them %v", seen, byTime))
			}
		}
		problems = append(problems, pacedCheckSequential(run.base, samples, final, seen))
		env.Capture.Measure(fmt.Sprintf("admission order: %v", seen))
	}

	renderers := map[string]bool{}
	for _, name := range run.groups {
		renderer, err := pacedCheckRolled(run.base.Groups[name], final.Groups[name])
		if err != nil {
			problems = append(problems, err)
			continue
		}
		renderers[renderer] = true
	}
	if len(renderers) > 1 {
		problems = append(problems, fmt.Errorf("the groups ended on different renderers: %v", renderers))
	}
	if err := errors.Join(problems...); err != nil {
		return err
	}
	for r := range renderers {
		env.Capture.Measure(fmt.Sprintf("every site Deployment of %d groups carries %s=%s; %d samples, never more than one group admitted; rollout took %s",
			len(run.groups), v1alpha1.RenderedByAnnotation, r, len(samples), time.Since(run.upgradeAt).Round(time.Second)))
	}
	return nil
}

func s74Cleanup(ctx context.Context, env *runner.Env) error {
	s74Mu.Lock()
	run := s74Runs[env]
	delete(s74Runs, env)
	s74Mu.Unlock()
	if run != nil && run.observer != nil {
		run.observer.Stop()
	}
	return s74RestoreCluster(ctx, env)
}

// s74RestoreCluster removes the auxiliary groups and the spare node's zone
// label. The upgrade itself is not undone: the cluster stays on the build
// under test, which is what the scenarios after it in the job run against.
func s74RestoreCluster(ctx context.Context, env *runner.Env) error {
	var errs []error
	if err := pacedDeleteAuxGroups(ctx, env); err != nil {
		errs = append(errs, err)
	} else if err := pacedRemoveSpareZone(ctx, env); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
