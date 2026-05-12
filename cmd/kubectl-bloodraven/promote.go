package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// PlannedFailoverAnnotation must match
// internal/controller/planned_failover.go's PlannedFailoverAnnotation
// constant. Duplicated here rather than imported so the plugin doesn't
// pull in the entire controller package.
const PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"

const promoteUsage = `Trigger a planned (graceful) failover via annotation.

Usage:
  kubectl bloodraven promote <group> <site> [flags]

The operator drains writes on the current primary, waits for <site> to
catch up on the fenced GTID set, and atomically promotes it. Status
lands on status.plannedFailover.phase=Succeeded with transactionsLost=0.

Per-request overrides (default to spec.plannedFailover values):
  --max-lag-wait DURATION   override spec.plannedFailover.maxLagWait
  --drain-timeout DURATION  override spec.plannedFailover.drainTimeout

Flags:
  --wait               block until terminal (Succeeded/Failed/Deferred)
  --timeout DURATION   timeout for --wait (default: 10m)
  --kubeconfig string  path to kubeconfig
  --context string     kubeconfig context
  --namespace, -n      namespace (default: context namespace, or "default")
`

func runPromote(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("promote", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf          commonFlags
		maxLagWait  time.Duration
		drainTimout time.Duration
		wait        bool
		timeout     time.Duration
	)
	registerCommonFlags(fs, &cf)
	fs.DurationVar(&maxLagWait, "max-lag-wait", 0, "override spec.plannedFailover.maxLagWait")
	fs.DurationVar(&drainTimout, "drain-timeout", 0, "override spec.plannedFailover.drainTimeout")
	fs.BoolVar(&wait, "wait", false, "block until the planned failover reaches a terminal phase")
	fs.DurationVar(&timeout, "timeout", 10*time.Minute, "timeout for --wait")
	fs.Usage = func() { fmt.Fprint(stderr, promoteUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	positional := fs.Args()
	if len(positional) != 2 {
		fmt.Fprint(stderr, promoteUsage)
		return fmt.Errorf("promote requires exactly two positional arguments (group and site)")
	}
	group, site := positional[0], positional[1]
	if group == "" || site == "" {
		return fmt.Errorf("group and site must be non-empty")
	}

	value, err := buildPlannedFailoverValue(site, maxLagWait, drainTimout)
	if err != nil {
		return err
	}

	cl, ns, err := cf.newClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var fg v1alpha1.MysqlFailoverGroup
	if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: group}, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("MysqlFailoverGroup %s/%s not found", ns, group)
		}
		return fmt.Errorf("get failover group: %w", err)
	}
	if !groupHasSite(&fg, site) {
		return fmt.Errorf("site %q is not defined in spec.sites of %s/%s", site, ns, group)
	}

	patched := fg.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[PlannedFailoverAnnotation] = value
	if err := cl.Patch(ctx, patched, client.MergeFrom(&fg)); err != nil {
		return fmt.Errorf("annotate failover group: %w", err)
	}
	fmt.Fprintf(stdout, "Annotated %s/%s with %s=%s\n", ns, group, PlannedFailoverAnnotation, value)

	if !wait {
		fmt.Fprintln(stdout, "Watch progress with: kubectl bloodraven status", group, "-n", ns)
		return nil
	}
	return waitForPlannedFailover(stdout, stderr, cl, ns, group, timeout)
}

// buildPlannedFailoverValue assembles the annotation value the operator
// parses. Mirrors the controller's grammar exactly: "<site>" or
// "<site>:k1=v1,k2=v2". Caller is expected to validate the site.
func buildPlannedFailoverValue(site string, maxLagWait, drainTimeout time.Duration) (string, error) {
	if site == "" {
		return "", fmt.Errorf("site is required")
	}
	pairs := []kv{}
	if maxLagWait > 0 {
		pairs = append(pairs, kv{"maxLagWait", maxLagWait.String()})
	}
	if drainTimeout > 0 {
		pairs = append(pairs, kv{"drainTimeout", drainTimeout.String()})
	}
	if len(pairs) == 0 {
		return site, nil
	}
	return site + ":" + joinKVs(pairs), nil
}

func groupHasSite(fg *v1alpha1.MysqlFailoverGroup, site string) bool {
	for _, s := range fg.Spec.Sites {
		if s.Name == site {
			return true
		}
	}
	return false
}

// waitForPlannedFailover polls every 2s until the planned-failover
// state machine reaches a terminal phase (Succeeded, Failed) or the
// caller-supplied timeout expires. Deferred is also a stable terminal
// state for the wait — the operator will not advance further until the
// cooldown expires.
func waitForPlannedFailover(stdout, stderr io.Writer, cl client.Client, ns, group string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	lastPhase := v1alpha1.PlannedFailoverPhaseNone
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	startedAt := time.Now()

	for {
		var fg v1alpha1.MysqlFailoverGroup
		if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: group}, &fg); err != nil {
			return fmt.Errorf("get failover group during wait: %w", err)
		}
		pf := fg.Status.PlannedFailover
		phase := v1alpha1.PlannedFailoverPhaseNone
		if pf != nil {
			phase = pf.Phase
		}
		if phase != lastPhase {
			elapsed := time.Since(startedAt).Round(time.Second)
			if pf != nil && pf.Message != "" {
				fmt.Fprintf(stdout, "[%s] phase: %s — %s\n", elapsed, emptyDash(string(phase)), pf.Message)
			} else {
				fmt.Fprintf(stdout, "[%s] phase: %s\n", elapsed, emptyDash(string(phase)))
			}
			lastPhase = phase
		}
		switch phase {
		case v1alpha1.PlannedFailoverPhaseSucceeded:
			fmt.Fprintln(stdout, "Planned failover succeeded.")
			return nil
		case v1alpha1.PlannedFailoverPhaseFailed:
			reason := ""
			msg := ""
			if pf != nil {
				reason = pf.Reason
				msg = pf.Message
			}
			fmt.Fprintf(stderr, "Planned failover failed: reason=%s message=%s\n", reason, msg)
			return fmt.Errorf("planned failover failed: %s", reason)
		case v1alpha1.PlannedFailoverPhaseDeferred:
			// Deferred is terminal for the wait — the operator
			// will not advance until cooldown expires, which is
			// typically minutes away. Surface and return.
			retry := ""
			if pf != nil && pf.RetryAfter != nil {
				retry = pf.RetryAfter.Time.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(stdout, "Planned failover deferred; will retry after %s\n", retry)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s waiting for planned failover to complete (last phase: %s)", timeout, lastPhase)
		case <-tick.C:
		}
	}
}
