// Command playground-chaos runs scripted chaos scenarios against a
// Bloodraven playground cluster (k3d/kind/minikube). It refuses to
// mutate any context that does not match the playground allowlist.
//
// See playground/chaos-scenarios.md for scenario documentation and
// /home/colin/.claude/plans/look-at-playground-chaos-scenarios-twinkling-hartmanis.md
// for the design.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"

	// Import the scenarios package for its side-effect: each
	// scenario file's init() registers itself with
	// runner.DefaultRegistry. The blank import is intentional.
	_ "github.com/shipstream/bloodraven/internal/playground/scenarios"
)

// Exit codes (CLI surface, see plan §7).
const (
	exitOK           = 0
	exitFailure      = 1
	exitFlagParse    = 2
	exitGuard        = 3
	exitEnvironment  = 4
)

func main() {
	rootFlags := flag.NewFlagSet("playground-chaos", flag.ContinueOnError)
	namespace := rootFlags.String("namespace", pgkube.PlaygroundNamespace, "playground namespace")
	fg := rootFlags.String("fg", pgkube.FailoverGroupName, "MysqlFailoverGroup name")
	resultsDir := rootFlags.String("results-dir", "./playground/chaos-results", "directory to write forensic captures into on failure")
	timeout := rootFlags.Duration("timeout", 0, "override per-scenario timeout (default: scenario-defined or 5m)")
	noCleanup := rootFlags.Bool("no-cleanup", false, "skip revert + global recover after the scenario (loud warning printed)")
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
		os.Exit(check(*kubeconfig, *kctx, *namespace))
	case "run":
		if len(subArgs) != 1 {
			fmt.Fprintln(os.Stderr, "usage: playground-chaos run <scenario-id>")
			os.Exit(exitFlagParse)
		}
		os.Exit(runOne(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, *timeout, *noCleanup, subArgs[0], logger))
	case "run-all":
		os.Exit(runAll(*kubeconfig, *kctx, *namespace, *fg, *resultsDir, *timeout, *noCleanup, *continueOnFailure, *junitOut, logger))
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
  playground-chaos [flags] run <scenario-id>    Run a single scenario
  playground-chaos [flags] run-all              Run all scenarios

Flags:
  --namespace            playground namespace (default: bloodraven-playground)
  --fg                   MysqlFailoverGroup name (default: playground)
  --results-dir          forensic capture root (default: ./playground/chaos-results)
  --timeout              override per-scenario timeout (default: scenario-defined or 5m)
  --no-cleanup           skip revert + global recover (loud warning)
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

func check(kubeconfig, kctx, namespace string) int {
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, errGuard) || isGuardErr(err) {
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
	if pgkube.ReadyCondition(mfg) != "True" {
		fmt.Fprintln(os.Stderr, "check: cluster is not Ready — run ./playground/setup.sh and wait for bootstrap")
		return exitEnvironment
	}
	for _, s := range mfg.Status.Sites {
		if s.State == "" || s.State == "unknown" || s.State == "unreachable" {
			fmt.Fprintf(os.Stderr, "check: site %s in state %q — run ./playground/reset-mysql.sh and retry\n", s.Name, s.State)
			return exitEnvironment
		}
	}
	return exitOK
}

func runOne(kubeconfig, kctx, namespace, fg, resultsDir string, timeout time.Duration, noCleanup bool, id string, logger *slog.Logger) int {
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
	exec := &runner.Executor{
		K: k,
		Cfg: runner.ExecutorConfig{
			Namespace:  namespace,
			FG:         fg,
			ResultsDir: resultsDir,
			NoCleanup:  noCleanup,
			Logger:     logger,
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	res := exec.Run(ctx, scen)
	printResult(res)
	if !res.Passed {
		return exitFailure
	}
	return exitOK
}

func runAll(kubeconfig, kctx, namespace, fg, resultsDir string, timeout time.Duration, noCleanup, continueOnFailure bool, junitOut string, logger *slog.Logger) int {
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if isGuardErr(err) {
			return exitGuard
		}
		return exitEnvironment
	}
	scenarios := runner.DefaultRegistry.List()
	if len(scenarios) == 0 {
		fmt.Fprintln(os.Stderr, "no scenarios registered")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var results []runner.Result
	exitCode := exitOK
	for _, s := range scenarios {
		if timeout > 0 {
			s.Timeout = timeout
		}
		exec := &runner.Executor{
			K: k,
			Cfg: runner.ExecutorConfig{
				Namespace:  namespace,
				FG:         fg,
				ResultsDir: resultsDir,
				NoCleanup:  noCleanup,
				Logger:     logger,
			},
		}
		res := exec.Run(ctx, s)
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

// errGuard sentinel marker — guard errors are matched by string
// rather than wrapping (the kube package's RequirePlaygroundContext
// uses fmt.Errorf for richer messaging) so we use a small isGuardErr
// helper that recognizes the prefix.
var errGuard = errors.New("playground:")

func isGuardErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return len(s) >= 11 && s[:11] == "playground:"
}
