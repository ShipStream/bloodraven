package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
)

const ackPlannedFailoverHoldUsage = `Acknowledge a planned-failover automation hold via annotation.

Usage:
  kubectl bloodraven ack-planned-failover-hold <group> [flags]

A planned failover that may have written to its target and ended
ManualRecoveryRequired holds automatic failover for the group
(status.plannedFailoverHold, condition AutomaticFailoverHeld=True).
Resolve the split brain by hand first, then acknowledge the hold. The
plugin writes
  bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>
with the held run's id from status.plannedFailoverHold.runID, or from
the planned-failover-terminal marker when status writes stalled.

Flags:
  --run-id string       the held run's id (default: read from the group)
  --kubeconfig string   path to kubeconfig
  --context string      kubeconfig context
  --namespace, -n       namespace
`

func runAckPlannedFailoverHold(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("ack-planned-failover-hold", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf    commonFlags
		runID string
	)
	registerCommonFlags(fs, &cf)
	fs.StringVar(&runID, "run-id", "", "the held run's id; defaults to status.plannedFailoverHold.runID")
	fs.Usage = func() { fmt.Fprint(stderr, ackPlannedFailoverHoldUsage) }
	positional, err := parseFlagsAndArgs(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 || positional[0] == "" {
		fmt.Fprint(stderr, ackPlannedFailoverHoldUsage)
		return fmt.Errorf("ack-planned-failover-hold requires exactly one positional argument (group)")
	}
	group := positional[0]

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
	value, err := plannedHoldAckValue(&fg, runID)
	if err != nil {
		return err
	}

	patched := fg.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[brcontroller.AcknowledgePlannedFailoverHoldAnnotation] = value
	if err := cl.Patch(ctx, patched, client.MergeFrom(&fg)); err != nil {
		return fmt.Errorf("annotate failover group: %w", err)
	}
	fmt.Fprintf(stdout, "Annotated %s/%s with %s=%s\n", ns, group, brcontroller.AcknowledgePlannedFailoverHoldAnnotation, value)
	fmt.Fprintln(stdout, "Watch the AutomaticFailoverHeld condition turn False with: kubectl bloodraven status", group, "-n", ns)
	return nil
}

// plannedHoldAckValue returns the acknowledgement value for the group's
// hold: the explicit runID when given (it must name the hold), otherwise
// the held run's id. It refuses when no hold stands, or when the hold
// names no run (an administrator then removes it by hand).
func plannedHoldAckValue(fg *v1alpha1.MysqlFailoverGroup, runID string) (string, error) {
	hold := brcontroller.PlannedHoldFrom(fg)
	switch {
	case hold == nil:
		return "", fmt.Errorf("%s/%s has no planned-failover automation hold", fg.Namespace, fg.Name)
	case hold.RunID == "":
		return "", fmt.Errorf("the AutomaticFailoverHeld condition of %s/%s names no run (status.plannedFailoverHold is absent); no acknowledgement can clear it, see the hold runbook", fg.Namespace, fg.Name)
	case runID != "" && runID != hold.RunID:
		return "", fmt.Errorf("--run-id %q does not name the held run %q", runID, hold.RunID)
	}
	return hold.RunID, nil
}
