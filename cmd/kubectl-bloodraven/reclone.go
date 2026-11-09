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

const recloneUsage = `Trigger a re-clone of a site via annotation.

Usage:
  kubectl bloodraven reclone <group> <site> [flags]

CLONE INSTANCE wipes the site's datadir, so the operator's safety
interlock requires the annotation value to name the group and its uid:

  <site>:confirm=<group>:uid=<group uid>

  * Divergent-GTID case (status.sites[<site>].divergentGtid is set): the
    value also carries gtidPrefix=, a prefix of the source UUID of the
    divergent GTID set's first entry. The plugin fills in the whole
    source UUID from status; pass --gtid-prefix to give a shorter one
    (at least 8 hex characters).

  * Cold-reclone case (no divergent GTID): pass --cold to confirm that
    you intend to wipe a site that has not diverged; without --cold the
    plugin refuses.

The plugin builds the value from the group it reads (name, uid and
status) and checks it against the operator's interlock before writing it.

Flags:
  --gtid-prefix string   source-UUID prefix to embed (default: the whole
                         source UUID from status)
  --cold                 confirm a cold reclone (no divergent GTID).
                         Mutually exclusive with --gtid-prefix.
  --kubeconfig string    path to kubeconfig
  --context string       kubeconfig context
  --namespace, -n        namespace
`

func runReclone(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("reclone", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf         commonFlags
		gtidPrefix string
		cold       bool
	)
	registerCommonFlags(fs, &cf)
	fs.StringVar(&gtidPrefix, "gtid-prefix", "", "source-UUID prefix to embed; defaults to the source UUID of status.sites[site].divergentGtid")
	fs.BoolVar(&cold, "cold", false, "confirm a cold reclone (no divergent GTID)")
	fs.Usage = func() { fmt.Fprint(stderr, recloneUsage) }
	positional, err := parseFlagsAndArgs(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		fmt.Fprint(stderr, recloneUsage)
		return fmt.Errorf("reclone requires exactly two positional arguments (group and site)")
	}
	group, site := positional[0], positional[1]
	if group == "" || site == "" {
		return fmt.Errorf("group and site must be non-empty")
	}
	if cold && gtidPrefix != "" {
		return fmt.Errorf("--cold and --gtid-prefix are mutually exclusive: --gtid-prefix is for a site with a divergent GTID")
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

	observedGtid := siteDivergentGtid(&fg, site)

	// Build the value from the group as read, and run it through the
	// operator's interlock, so a missing --cold or a wrong prefix fails
	// before round-tripping a doomed annotation.
	var value string
	switch {
	case gtidPrefix != "":
		value = brcontroller.RecloneAnnotationValueWithPrefix(&fg, site, gtidPrefix)
	case cold:
		if observedGtid != "" {
			return fmt.Errorf("--cold refused: site %q has a recorded divergent GTID (%q); rerun without --cold so the operator can verify intent against the divergent set",
				site, observedGtid)
		}
		value = brcontroller.RecloneAnnotationValue(&fg, site)
	case observedGtid != "":
		value = brcontroller.RecloneAnnotationValue(&fg, site)
	default:
		// No GTID, no --cold — cold reclone not confirmed. Reject
		// with the exact remediation so a fat-finger can't wipe the
		// datadir.
		return fmt.Errorf("site %q has no divergent GTID — this is a cold reclone (CLONE INSTANCE will wipe the datadir). Re-run with --cold to confirm",
			site)
	}
	if err := brcontroller.ValidateRecloneAnnotation(&fg, value); err != nil {
		return err
	}

	patched := fg.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[v1alpha1.RecloneAnnotation] = value
	if err := cl.Patch(ctx, patched, client.MergeFrom(&fg)); err != nil {
		return fmt.Errorf("annotate failover group: %w", err)
	}

	fmt.Fprintf(stdout, "Annotated %s/%s with %s=%s\n", ns, group, v1alpha1.RecloneAnnotation, value)
	fmt.Fprintln(stdout, "Watch progress with: kubectl bloodraven status", group, "-n", ns)
	return nil
}

// siteDivergentGtid returns the recorded divergentGtid for the named
// site, or "" if no such status entry exists.
func siteDivergentGtid(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range fg.Status.Sites {
		if s.Name == site {
			return s.DivergentGtid
		}
	}
	return ""
}
