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
	autoReset := rootFlags.Bool("auto-reset", false, "on precheck failure: shell out to reset-mysql.sh + setup.sh, then retry once (3s pause unless CI=1)")
	continueOnFailure := rootFlags.Bool("continue-on-failure", false, "run-all only: keep going past the first failure")
	junitOut := rootFlags.String("junit-out", "", "run-all only: write JUnit XML report to this path")
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
		listScenarios()
		return
	case "check":
		os.Exit(check(*kubeconfig, *kctx, *namespace, *fg))
	case "reset":
		os.Exit(resetPlayground(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, logger))
	case "run":
		if len(subArgs) != 1 {
			fmt.Fprintln(os.Stderr, "usage: playground-chaos run <scenario-id>")
			os.Exit(exitFlagParse)
		}
		os.Exit(runOne(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, *timeout, *noCleanup, *force, *autoReset, subArgs[0], logger))
	case "run-all":
		os.Exit(runAll(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, *timeout, *noCleanup, *force, *autoReset, *continueOnFailure, *junitOut, logger))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", subcmd)
		usage()
		os.Exit(exitFlagParse)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
	playground-chaos [flags] list                 List registered scenarios
	playground-chaos [flags] check                Verify baseline health
	playground-chaos [flags] reset                Wipe and re-bootstrap MySQL playground state
	playground-chaos [flags] run <scenario-id>    Run a single scenario
  playground-chaos [flags] run-all              Run all scenarios

Flags:
  --namespace            playground namespace (default: bloodraven-playground)
  --fg                   MysqlFailoverGroup name (default: playground)
  --results-dir          forensic capture root (default: ./playground/chaos-results)
  --timeout              override per-scenario timeout (default: scenario-defined or 5m)
  --no-cleanup           skip revert + global recover (loud warning)
  --force                delete any prior chaos in-progress marker before preflight
  --auto-reset           on precheck failure: reset-mysql.sh + setup.sh, retry once
  --continue-on-failure  run-all only: keep going past first failure
  --junit-out            run-all only: write JUnit XML to path
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

func listScenarios() {
	scenarios := runner.DefaultRegistry.List()
	if len(scenarios) == 0 {
		fmt.Println("(no scenarios registered)")
		return
	}
	for _, s := range scenarios {
		fmt.Printf("%s  %s\n", s.ID, s.Title)
		if s.DocLink != "" {
			fmt.Printf("    doc: %s\n", s.DocLink)
		}
		if s.Hypothesis != "" {
			fmt.Printf("    hypothesis: %s\n", s.Hypothesis)
		}
	}
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
	mfg, err := k.GetMFG(ctx, namespace)
	if err != nil {
		fmt.Fprintln(os.Stderr, "check: get MFG:", err)
		return exitEnvironment
	}
	fmt.Printf("context:    %s\n", k.CurrentCtx)
	fmt.Printf("namespace:  %s\n", namespace)
	fmt.Printf("activeSite: %s\n", mfg.Status.ActiveSite)
	fmt.Printf("ready:      %s\n", pgkube.ReadyCondition(mfg))
	for _, s := range mfg.Status.Sites {
		fmt.Printf("  site %-6s state=%s replicating=%v\n", s.Name, s.State, s.Replicating)
	}

	// inProgress: <yes/no + summary>. Read the marker even before we
	// run structural checks — it's the most useful signal when a prior
	// run was interrupted, and it doesn't depend on the cluster being
	// otherwise healthy.
	marker, mErr := k.ReadChaosMarker(ctx, namespace)
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

func runOne(kubeconfig, kctx, namespace, fg, resultsDir string, timeout time.Duration, noCleanup, force, autoReset bool, id string, logger *slog.Logger) int {
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

	res := runScenarioWithAutoReset(ctx, k, kubeconfig, kctx, scen, namespace, fg, resultsDir, noCleanup, force, autoReset, logger)
	printResult(res)
	if !res.Passed {
		return exitFailure
	}
	return exitOK
}

// runScenarioWithAutoReset runs a single scenario, and on a Precheck
// failure with --auto-reset set, shells out to reset-mysql.sh + setup.sh
// and retries once. The retry has --auto-reset disabled so we can never
// reset more than once per scenario invocation.
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

func runAll(kubeconfig, kctx, namespace, fg, resultsDir string, timeout time.Duration, noCleanup, force, autoReset, continueOnFailure bool, junitOut string, logger *slog.Logger) int {
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if isGuardErr(err) {
			return exitGuard
		}
		return exitEnvironment
	}
	scens := runner.DefaultRegistry.List()
	if len(scens) == 0 {
		fmt.Fprintln(os.Stderr, "no scenarios registered")
		return exitFailure
	}
	if force {
		fmt.Fprintln(os.Stderr, "!! --force: will delete any prior chaos in-progress marker before each scenario's preflight")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var results []runner.Result
	exitCode := exitOK
	for _, s := range scens {
		if timeout > 0 {
			s.Timeout = timeout
		}
		res := runScenarioWithAutoReset(ctx, k, kubeconfig, kctx, s, namespace, fg, resultsDir, noCleanup, force, autoReset, logger)
		results = append(results, res)
		printResult(res)
		if !res.Passed {
			exitCode = exitFailure
			if !continueOnFailure {
				break
			}
		}
	}
	if junitOut != "" {
		path, err := filepath.Abs(junitOut)
		if err != nil {
			path = junitOut
		}
		if err := runner.WriteJUnit(path, results); err != nil {
			fmt.Fprintln(os.Stderr, "junit write failed:", err)
		} else {
			fmt.Println("junit:", path)
		}
	}
	return exitCode
}

func printResult(r runner.Result) {
	status := "PASS"
	if !r.Passed {
		status = "FAIL"
	}
	fmt.Printf("%s  %s  (%s)  duration=%s\n", status, r.ID, r.Title, r.Duration.Round(time.Millisecond))
	if !r.Passed {
		fmt.Printf("       phase=%s step=%q\n       %s\n       forensics: %s\n",
			r.Phase, r.StepName, r.Failure, r.CapturePath)
	}
	if r.CleanupErr != nil {
		fmt.Printf("       cleanup: %v\n", r.CleanupErr)
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
