// Command playground-chaos runs scripted chaos scenarios against a
// Bloodraven playground cluster (k3d/kind/minikube). It refuses to
// mutate any context that does not match the playground allowlist.
//
// See playground/chaos-scenarios.md for scenario documentation.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	"github.com/shipstream/bloodraven/internal/playground/scenarios"
)

// Exit codes (CLI surface, see plan §7).
const (
	exitOK          = 0
	exitFailure     = 1
	exitFlagParse   = 2
	exitGuard       = 3
	exitEnvironment = 4
)

func main() {
	rootFlags := flag.NewFlagSet("playground-chaos", flag.ContinueOnError)
	namespace := rootFlags.String("namespace", pgkube.PlaygroundNamespace, "playground namespace")
	fg := rootFlags.String("fg", pgkube.FailoverGroupName, "MysqlFailoverGroup name")
	resultsDir := rootFlags.String("results-dir", "./playground/chaos-results", "directory to write forensic captures into on failure")
	timeout := rootFlags.Duration("timeout", 0, "override per-scenario timeout (default: scenario-defined or 5m)")
	noCleanup := rootFlags.Bool("no-cleanup", false, "skip revert + global recover after the scenario (loud warning printed)")
	force := rootFlags.Bool("force", false, "delete any prior chaos in-progress marker before preflight (run, run-all)")
	autoReset := rootFlags.Bool("auto-reset", false, "on precheck failure: run playground-chaos reset, then retry once (3s pause unless CI=1)")
	continueOnFailure := rootFlags.Bool("continue-on-failure", false, "run-all only: keep going past the first failure")
	junitOut := rootFlags.String("junit-out", "", "run-all only: write JUnit XML report to this path")
	profile := rootFlags.String("profile", string(runner.DefaultProfile), "run-all only: scenario subset (smoke|release|full)")
	shardFlag := rootFlags.String("shard", "", "run-all, list: run only part <index>/<total> of the profile, e.g. 2/4 (the parts are disjoint and together cover the profile)")
	scenarioList := rootFlags.String("scenarios", "", "run-all only: comma-separated scenario IDs to run in this order instead of a profile (resets before each scenario that requires it)")
	baselineWait := rootFlags.Duration("baseline-wait", 5*time.Minute, "run-all: wait up to this long for a healthy baseline before the first scenario (0 disables); run: only when given explicitly")
	verbose := rootFlags.Bool("verbose", false, "verbose logging")
	kubeconfig := rootFlags.String("kubeconfig", "", "kubeconfig path (default: KUBECONFIG / ~/.kube/config)")
	kctx := rootFlags.String("context", "", "kubectl context to use (default: current-context)")

	// Subcommand parsing: first positional is the subcommand.
	// Syntax: playground-chaos [flags] <subcmd> [args] [flags]
	//
	// Go's stdlib flag package stops at the first non-flag arg, so
	// after `Parse(os.Args[1:])` only flags BEFORE the subcommand
	// have been consumed. To accept the cobra-style ordering
	// (`playground-chaos run-all --junit-out=foo`) without silently
	// dropping post-subcommand flags, we re-parse the remainder and
	// concatenate the resulting positionals.
	if err := rootFlags.Parse(os.Args[1:]); err != nil {
		os.Exit(exitFlagParse)
	}
	args := rootFlags.Args()
	if len(args) == 0 {
		usage()
		os.Exit(exitFlagParse)
	}
	subcmd := args[0]
	rest := args[1:]
	var subArgs []string
	for len(rest) > 0 {
		if !strings.HasPrefix(rest[0], "-") {
			subArgs = append(subArgs, rest[0])
			rest = rest[1:]
			continue
		}
		if err := rootFlags.Parse(rest); err != nil {
			os.Exit(exitFlagParse)
		}
		rest = rootFlags.Args()
	}

	logger := buildLogger(*verbose)
	slog.SetDefault(logger)

	switch subcmd {
	case "list":
		os.Exit(listScenarios(os.Stdout, runner.DefaultRegistry.List(), flagGiven(rootFlags, "profile"), runner.Profile(*profile), *shardFlag))
	case "check":
		os.Exit(check(*kubeconfig, *kctx, *namespace, *fg))
	case "reset":
		os.Exit(resetPlayground(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, logger))
	case "run":
		if len(subArgs) != 1 {
			fmt.Fprintln(os.Stderr, "usage: playground-chaos run <scenario-id>")
			os.Exit(exitFlagParse)
		}
		os.Exit(runOne(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, *timeout, runBaselineWait(rootFlags, *baselineWait), *noCleanup, *force, *autoReset, subArgs[0], logger))
	case "run-all":
		p := runner.Profile(*profile)
		if !p.IsValid() {
			fmt.Fprintf(os.Stderr, "invalid profile %q; valid: smoke, release, full\n", p)
			os.Exit(exitFlagParse)
		}
		os.Exit(runAll(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, *timeout, *baselineWait, *noCleanup, *force, *autoReset, *continueOnFailure, *junitOut, p, *scenarioList, *shardFlag, logger))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", subcmd)
		usage()
		os.Exit(exitFlagParse)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
	playground-chaos [flags] list                 List registered scenarios (--profile/--shard: only that selection)
	playground-chaos [flags] check                Verify baseline health
	playground-chaos [flags] reset                Wipe and re-bootstrap MySQL playground state
	playground-chaos [flags] run <scenario-id>    Run a single scenario
  playground-chaos [flags] run-all              Run all scenarios (or --scenarios=<id,...>)

Flags:
  --namespace            playground namespace (default: bloodraven-playground)
  --fg                   MysqlFailoverGroup name (default: playground)
  --results-dir          forensic capture root (default: ./playground/chaos-results)
  --timeout              override per-scenario timeout (default: scenario-defined or 5m)
  --no-cleanup           skip revert + global recover (loud warning)
  --force                delete any prior chaos in-progress marker before preflight
  --auto-reset           on precheck failure: playground-chaos reset, retry once
  --continue-on-failure  run-all only: keep going past first failure
  --junit-out            run-all only: write JUnit XML to path
  --profile              run-all only: scenario subset (smoke|release|full)
  --scenarios            run-all only: comma-separated scenario IDs, run in order instead of --profile
  --shard                run-all, list: only part <index>/<total> of the profile (e.g. 2/4); the parts are
                         disjoint, cover the profile, and are balanced by scenario Timeout. Each part
                         needs its own cluster. Not combinable with --scenarios.
  --baseline-wait        wait for healthy baseline before the first scenario (run-all: default 5m, 0 disables;
                         run: only when given, e.g. for the first of a CI scenario list)
  --verbose              verbose logging
  --kubeconfig           kubeconfig path
  --context              kubectl context

Exit codes:
  0  success
  1  scenario failure
  2  flag parse / unknown scenario
  3  context guard rejected the kubeconfig context
  4  environmental error (cluster unreachable, port-forward died, etc.)`)
}

func buildLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h)
}

// flagGiven reports whether the named flag was set on the command line.
func flagGiven(fs *flag.FlagSet, name string) bool {
	given := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})
	return given
}

// listScenarios prints the registered scenarios. With --profile or --shard
// it prints what `run-all` with the same flags would run (so quarantined
// scenarios are left out), which is how to see what a CI shard contains.
func listScenarios(w io.Writer, registered []runner.Scenario, profileGiven bool, profile runner.Profile, shardSpec string) int {
	scenarios := registered
	if profileGiven || strings.TrimSpace(shardSpec) != "" {
		if !profile.IsValid() {
			fmt.Fprintf(os.Stderr, "invalid profile %q; valid: smoke, release, full\n", profile)
			return exitFlagParse
		}
		shard, err := runner.ParseShard(shardSpec)
		if err != nil {
			fmt.Fprintln(os.Stderr, "list:", err)
			return exitFlagParse
		}
		scenarios, err = runner.SelectShard(runner.SelectForProfile(registered, profile), shard)
		if err != nil {
			fmt.Fprintln(os.Stderr, "list:", err)
			return exitFlagParse
		}
	}
	if len(scenarios) == 0 {
		fmt.Fprintln(w, "(no scenarios)")
		return exitOK
	}
	for _, s := range scenarios {
		marker := ""
		if s.Quarantine != "" {
			marker = "  [QUARANTINED]"
		}
		fmt.Fprintf(w, "%s  %s%s\n", s.ID, s.Title, marker)
		if s.Quarantine != "" {
			fmt.Fprintf(w, "    quarantined: %s\n", s.Quarantine)
		}
		if s.DocLink != "" {
			fmt.Fprintf(w, "    doc: %s\n", s.DocLink)
		}
		if s.Hypothesis != "" {
			fmt.Fprintf(w, "    hypothesis: %s\n", s.Hypothesis)
		}
	}
	return exitOK
}

func loadKube(kubeconfig, kctx string, skipGuard bool) (*pgkube.Client, error) {
	return pgkube.New(pgkube.LoadOptions{
		Kubeconfig: kubeconfig,
		Context:    kctx,
		SkipGuard:  skipGuard,
	})
}

func check(kubeconfig, kctx, namespace, fg string) int {
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if isGuardErr(err) {
			return exitGuard
		}
		return exitEnvironment
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mfg, err := k.GetMFGNamed(ctx, namespace, fg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "check: get MFG:", err)
		return exitEnvironment
	}
	fmt.Printf("context:    %s\n", k.CurrentCtx)
	fmt.Printf("namespace:  %s\n", namespace)
	fmt.Printf("activeSite: %s\n", mfg.Status.ActiveSite)
	fmt.Printf("ready:      %s\n", pgkube.ReadyCondition(mfg))
	for _, s := range mfg.Status.Sites {
		fmt.Printf("  site %-6s state=%s replication=%s\n", s.Name, s.State, s.Replication)
	}

	// inProgress: <yes/no + summary>. Read the marker even before we
	// run structural checks — it's the most useful signal when a prior
	// run was interrupted, and it doesn't depend on the cluster being
	// otherwise healthy.
	marker, mErr := k.ReadChaosMarkerNamed(ctx, namespace, fg)
	switch {
	case mErr != nil:
		fmt.Printf("inProgress: <read failed: %v>\n", mErr)
	case marker == nil:
		fmt.Println("inProgress: no")
	default:
		age := time.Since(marker.StartedAt).Round(time.Second)
		fmt.Printf("inProgress: yes — scenario=%s host=%s pid=%d age=%s capture=%s\n",
			marker.Scenario, marker.Host, marker.PID, age, marker.CaptureDir)
	}

	// Run the same structural checks scenarios use as a Precheck.
	// Anything subtle (stuck scale-to-0, bothReadOnly NoPrimary, off
	// replication, anti-flap cooldown still ticking, lastFailoverTarget
	// pointing at a nonexistent site) surfaces here with its
	// remediation hint, instead of waiting for a 4-min converge timeout.
	if err := scenarios.CheckBaseline(ctx, k, namespace, fg); err != nil {
		fmt.Fprintln(os.Stderr, "check:", err)
		return exitEnvironment
	}
	fmt.Println("baseline:   ok")
	return exitOK
}

// runBaselineWait is the suite-entry baseline wait for `run`: zero unless
// --baseline-wait was given explicitly. A hand-run scenario keeps its
// instant precheck; a CI job that runs a scenario list one `run` at a time
// right after setup.sh passes the flag on the first one, the way run-all
// waits by default.
func runBaselineWait(fs *flag.FlagSet, wait time.Duration) time.Duration {
	if !flagGiven(fs, "baseline-wait") {
		return 0
	}
	return wait
}

func runOne(kubeconfig, kctx, namespace, fg, resultsDir string, timeout, baselineWait time.Duration, noCleanup, force, autoReset bool, id string, logger *slog.Logger) int {
	scen, ok := runner.DefaultRegistry.Get(id)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario: %s — try `playground-chaos list`\n", id)
		return exitFlagParse
	}
	if timeout > 0 {
		scen.Timeout = timeout
	}
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if isGuardErr(err) {
			return exitGuard
		}
		return exitEnvironment
	}
	if force {
		fmt.Fprintln(os.Stderr, "!! --force: will delete any prior chaos in-progress marker before preflight")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if baselineWait > 0 && !suiteEntryBaseline(ctx, k, namespace, fg, baselineWait, "run", logger) {
		return exitEnvironment
	}
	res := runScenarioWithAutoReset(ctx, k, kubeconfig, kctx, scen, namespace, fg, resultsDir, noCleanup, force, autoReset, logger)
	printResult(res)
	if !res.Passed {
		return exitFailure
	}
	return exitOK
}

// runScenarioWithAutoReset runs a single scenario, and on a Precheck
// failure with --auto-reset set, shells out to this binary's reset
// subcommand and retries once. The retry has --auto-reset disabled so
// we can never reset more than once per scenario invocation.
func runScenarioWithAutoReset(ctx context.Context, k *pgkube.Client, kubeconfig, kctx string, scen runner.Scenario, namespace, fg, resultsDir string, noCleanup, force, autoReset bool, logger *slog.Logger) runner.Result {
	executor := &runner.Executor{
		K: k,
		Cfg: runner.ExecutorConfig{
			Namespace:  namespace,
			FG:         fg,
			ResultsDir: resultsDir,
			NoCleanup:  noCleanup,
			Force:      force,
			Logger:     logger,
		},
	}
	res := executor.Run(ctx, scen)
	if res.Passed || !autoReset || res.Phase != runner.PhasePrecheck {
		return res
	}
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "!! --auto-reset: precheck failed, will run playground-chaos reset with the same kube context")
	fmt.Fprintln(os.Stderr, "!! this will WIPE MySQL data — set CI=1 to skip the 3-second pause")
	if os.Getenv("CI") == "" {
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return res
		}
	}
	if err := runReset(ctx, kubeconfig, kctx, k.CurrentCtx, namespace, fg, resultsDir); err != nil {
		fmt.Fprintln(os.Stderr, "!! --auto-reset: shell-out failed:", err)
		return res
	}
	// Retry exactly once. Pass force=true so any leftover marker from
	// the prior failed precheck is dropped — autoReset implies "I want
	// this to run regardless of stale state".
	executor.Cfg.Force = true
	return executor.Run(ctx, scen)
}

// baselineStableWindow is how long the suite-entry baseline must stay
// healthy before the first scenario starts. One healthy sample is not
// enough: a site still coming up after setup.sh can read healthy and be
// reported unreachable a few milliseconds later, which failed the first
// scenario's precheck right after the wait had passed (#262). It covers
// several operator polls (the playground's pollInterval is 2s and a site
// is declared unreachable after failureThreshold=3 of them).
const baselineStableWindow = 15 * time.Second

// baselineWaitConfig tunes waitForBaseline.
type baselineWaitConfig struct {
	// Timeout bounds the wait for the baseline to become healthy.
	Timeout time.Duration
	// Interval is the poll interval while the baseline is unhealthy.
	Interval time.Duration
	// StableFor is how long every check must pass before the baseline is
	// reported healthy. Zero accepts the first healthy check.
	StableFor time.Duration
	// StableInterval is the poll interval inside the stable window. Zero
	// means Interval.
	StableInterval time.Duration
}

// waitForBaseline polls check until it has returned nil for a whole stable
// window (cfg.StableFor), or cfg.Timeout elapses without a healthy check.
// The first check runs immediately. A failed check restarts the window.
// progress is invoked after every failed check with the error and the
// elapsed time. Returns nil once the baseline held; on timeout it returns
// the last check error, and on context cancellation the last check error,
// or the context error when no check has failed.
//
// The timeout bounds the wait for a healthy check, not the window: a
// baseline that turns healthy just before the deadline still gets its
// window, and is reported unhealthy if it breaks inside it.
func waitForBaseline(ctx context.Context, cfg baselineWaitConfig, check func(context.Context) error, progress func(err error, elapsed time.Duration)) error {
	start := time.Now()
	deadline := start.Add(cfg.Timeout)
	stableInterval := cfg.StableInterval
	if stableInterval <= 0 {
		stableInterval = cfg.Interval
	}
	var lastErr error
	var healthySince time.Time
	for {
		err := check(ctx)
		now := time.Now()
		pause := cfg.Interval
		if err == nil {
			if healthySince.IsZero() {
				healthySince = now
			}
			if now.Sub(healthySince) >= cfg.StableFor {
				return nil
			}
			pause = stableInterval
		} else {
			healthySince = time.Time{}
			lastErr = err
			progress(err, now.Sub(start))
			if now.After(deadline) {
				return lastErr
			}
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-time.After(pause):
		}
	}
}

// suiteEntryBaseline waits up to wait for a baseline that stays healthy for
// baselineStableWindow, by the predicate every scenario's Precheck uses
// (scenarios.CheckBaseline: Ready, the active site, every site's state and
// the structural checks). A timeout is not fatal: it reports and returns
// true, and the scenario's precheck (and --auto-reset) decide. It returns
// false only when ctx was cancelled.
func suiteEntryBaseline(ctx context.Context, k *pgkube.Client, namespace, fg string, wait time.Duration, cmd string, logger *slog.Logger) bool {
	start := time.Now()
	cfg := baselineWaitConfig{Timeout: wait, Interval: 5 * time.Second, StableFor: baselineStableWindow, StableInterval: time.Second}
	err := waitForBaseline(ctx, cfg, func(ctx context.Context) error {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return scenarios.CheckBaseline(checkCtx, k, namespace, fg)
	}, func(lastErr error, elapsed time.Duration) {
		logger.Info("wait", "what", cmd+" suite-entry healthy baseline",
			"last", lastErr.Error(), "elapsed", elapsed.Round(time.Second))
	})
	switch {
	case err != nil && ctx.Err() != nil:
		return false
	case err != nil:
		fmt.Fprintf(os.Stderr, "!! %s: baseline still unhealthy after %s (%v) — proceeding; per-scenario precheck and --auto-reset take it from here\n",
			cmd, wait, err)
	default:
		logger.Info(cmd+" suite-entry baseline healthy", "elapsed", time.Since(start).Round(time.Second),
			"stableFor", baselineStableWindow.String())
	}
	return true
}

// runReset shells out to this same binary's reset subcommand. Streams
// output through to stderr so the user can watch reset progress.
func runReset(ctx context.Context, kubeconfig, kctx, currentCtx, namespace, fg, resultsDir string) error {
	args := []string{"reset", "--namespace", namespace, "--fg", fg, "--results-dir", resultsDir}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	if kctx != "" {
		args = append(args, "--context", kctx)
	} else if currentCtx != "" {
		args = append(args, "--context", currentCtx)
	}
	fmt.Fprintln(os.Stderr, "==>", os.Args[0], strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("playground-chaos reset: %w", err)
	}
	return nil
}

// selectRunAllScenarios picks run-all's scenarios: the explicit --scenarios
// list when given (in its order, quarantined entries included because they
// were named), otherwise the profile. Either way the result goes through the
// same suite loop, so every selected scenario with ResetBeforeRunAll gets
// its reset; that is why CI runs scenario lists through run-all rather than
// one `run` per ID, which never resets.
//
// A shard narrows a profile to one of its disjoint parts (runner.SelectShard).
// It is refused with a list: a list is already an explicit, ordered choice.
// A shard may be empty (more shards than scenarios); the others cover the
// profile, so that is not an error.
func selectRunAllScenarios(registered []runner.Scenario, profile runner.Profile, list string, shard runner.Shard) ([]runner.Scenario, error) {
	if strings.TrimSpace(list) != "" {
		if !shard.IsZero() {
			return nil, fmt.Errorf("--shard cannot be combined with --scenarios")
		}
		return runner.SelectByIDs(registered, list)
	}
	scens := runner.SelectForProfile(registered, profile)
	if len(scens) == 0 {
		return nil, fmt.Errorf("no scenarios selected for profile %q", profile)
	}
	return runner.SelectShard(scens, shard)
}

func runAll(kubeconfig, kctx, namespace, fg, resultsDir string, timeout, baselineWait time.Duration, noCleanup, force, autoReset, continueOnFailure bool, junitOut string, profile runner.Profile, scenarioList, shardSpec string, logger *slog.Logger) int {
	registered := runner.DefaultRegistry.List()
	shard, err := runner.ParseShard(shardSpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run-all:", err)
		return exitFlagParse
	}
	scens, err := selectRunAllScenarios(registered, profile, scenarioList, shard)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run-all:", err)
		if strings.TrimSpace(scenarioList) != "" {
			return exitFlagParse
		}
		return exitFailure
	}
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if isGuardErr(err) {
			return exitGuard
		}
		return exitEnvironment
	}
	switch {
	case strings.TrimSpace(scenarioList) != "":
		fmt.Fprintf(os.Stderr, "Running %d listed scenarios (--profile ignored)\n", len(scens))
		for _, s := range scens {
			if s.Quarantine != "" {
				fmt.Fprintf(os.Stderr, "!! quarantined but explicitly listed (running): %s — %s\n", s.ID, s.Quarantine)
			}
		}
	default:
		if profile != runner.ProfileFull && profile != "" {
			fmt.Fprintf(os.Stderr, "Running profile %q: %d of %d scenarios\n", profile, len(scens), len(registered))
		}
		if !shard.IsZero() {
			ids := make([]string, 0, len(scens))
			for _, s := range scens {
				ids = append(ids, s.ID)
			}
			fmt.Fprintf(os.Stderr, "Running shard %s of profile %q: %d scenarios: %s\n", shard, profile, len(scens), strings.Join(ids, ","))
		}
		// Surface any quarantined scenarios excluded from this run so a green
		// suite is never mistaken for full coverage.
		for _, s := range registered {
			if s.Quarantine != "" {
				fmt.Fprintf(os.Stderr, "!! quarantined (skipped): %s — %s\n", s.ID, s.Quarantine)
			}
		}
	}
	if force {
		fmt.Fprintln(os.Stderr, "!! --force: will delete any prior chaos in-progress marker before each scenario's preflight")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Suite-entry convergence wait. A freshly deployed playground
	// legitimately takes tens of seconds to settle after setup.sh
	// returns (operator first reconcile, replication catch-up), and CI
	// starts run-all immediately after setup. Without this wait the
	// first scenario's instant Precheck catches the still-converging
	// cluster and --auto-reset reaches for a full MySQL wipe — a
	// five-minute destructive remedy for a thirty-second problem (the
	// v0.6.1 post-release E2E failed exactly this way: scenario 01
	// FAILed in 38ms while every later scenario passed).
	//
	// A timeout here is deliberately NOT fatal: the per-scenario
	// Precheck and --auto-reset remain the authoritative remedies, and
	// they produce forensics that aborting the suite here would not.
	if baselineWait > 0 && !suiteEntryBaseline(ctx, k, namespace, fg, baselineWait, "run-all", logger) {
		return exitEnvironment
	}

	// The report is rewritten after every scenario, not only at the end: a
	// run killed at its job timeout then still leaves the results of the
	// scenarios that finished (#236 lost a whole 90-minute run this way).
	writeJUnit := func(results []runner.Result, final bool) {
		if junitOut == "" {
			return
		}
		path, err := filepath.Abs(junitOut)
		if err != nil {
			path = junitOut
		}
		if err := runner.WriteJUnit(path, results); err != nil {
			fmt.Fprintln(os.Stderr, "junit write failed:", err)
		} else if final {
			fmt.Println("junit:", path)
		}
	}

	var results []runner.Result
	exitCode := exitOK
	for _, s := range scens {
		if timeout > 0 {
			s.Timeout = timeout
		}
		if s.ResetBeforeRunAll {
			fmt.Fprintf(os.Stderr, "!! run-all: %s requires a pristine playground; running reset first\n", s.ID)
			if err := runReset(ctx, kubeconfig, kctx, k.CurrentCtx, namespace, fg, resultsDir); err != nil {
				// A failed pre-scenario reset used to `return exitEnvironment`
				// here, which aborted the whole suite mid-loop — bypassing
				// --continue-on-failure AND the JUnit write below, so a single
				// wedged scenario (e.g. 09 leaving the cluster unhealthy)
				// produced a red job with no report. Record it as a synthetic
				// failure for this scenario instead, so the result lands in
				// JUnit and the run honors --continue-on-failure like any other
				// failure.
				fmt.Fprintf(os.Stderr, "run-all reset before %s failed: %v\n", s.ID, err)
				res := runner.Result{
					ID:        s.ID,
					Title:     s.Title,
					Passed:    false,
					Phase:     runner.PhasePrecheck,
					StepName:  "ResetBeforeRunAll",
					Failure:   fmt.Sprintf("pre-scenario reset failed: %v", err),
					StartTime: time.Now(),
				}
				results = append(results, res)
				printResult(res)
				writeJUnit(results, false)
				exitCode = exitFailure
				if !continueOnFailure {
					break
				}
				continue
			}
		}
		res := runScenarioWithAutoReset(ctx, k, kubeconfig, kctx, s, namespace, fg, resultsDir, noCleanup, force, autoReset, logger)
		results = append(results, res)
		printResult(res)
		writeJUnit(results, false)
		if !res.Passed {
			exitCode = exitFailure
			if !continueOnFailure {
				break
			}
		}
	}
	writeJUnit(results, true)
	return exitCode
}

func printResult(r runner.Result) {
	writeResult(os.Stdout, r)
}

// writeResult prints one scenario's outcome: the PASS/FAIL line, the
// measurements the scenario recorded (on a pass too, so a green CI log
// shows them), and the failure and cleanup details.
func writeResult(w io.Writer, r runner.Result) {
	status := "PASS"
	if !r.Passed {
		status = "FAIL"
	}
	fmt.Fprintf(w, "%s  %s  (%s)  duration=%s\n", status, r.ID, r.Title, r.Duration.Round(time.Millisecond))
	for _, m := range r.Measurements {
		fmt.Fprintf(w, "       measured: %s\n", m)
	}
	if !r.Passed {
		fmt.Fprintf(w, "       phase=%s step=%q\n       %s\n       forensics: %s\n",
			r.Phase, r.StepName, r.Failure, r.CapturePath)
	}
	if r.CleanupErr != nil {
		fmt.Fprintf(w, "       cleanup: %v\n", r.CleanupErr)
	}
}

// isGuardErr recognizes the "playground:" prefix the kube package's
// RequirePlaygroundContext uses for context-allowlist rejections.
func isGuardErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "playground:")
}
