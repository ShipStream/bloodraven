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
)

// RecloneAnnotation must match internal/controller/reconciler.go's
// RecloneAnnotation constant.
const RecloneAnnotation = "bloodraven.shipstream.io/reclone-site"

// minRecloneGtidPrefix mirrors internal/controller/reclone.go's
// constant. The operator rejects divergent-GTID prefixes shorter than
// this; we mirror the value here so the plugin fails fast (with a
// clearer error than the operator's "shorter than the required %d
// characters" event) instead of round-tripping an invalid annotation.
const minRecloneGtidPrefix = 8

const recloneUsage = `Trigger a re-clone of a divergent site via annotation.

Usage:
  kubectl bloodraven reclone <group> <site> [flags]

The operator's safety interlock requires the annotation value to carry
intent confirmation:

  * Divergent-GTID case (status.sites[<site>].divergentGtid is set):
    the value must include a prefix of the observed GTID. The plugin
    auto-fills the first 12 chars from status; pass --gtid-prefix to
    override.

  * Cold-reclone case (no divergent GTID): the operator requires the
    annotation value to be exactly "<site>:confirm=<group>" because
    CLONE INSTANCE wipes the target's datadir. Pass --cold to tell the
    plugin you intend this and want the confirm token generated for
    you; without --cold (or an explicit --gtid-prefix), the plugin
    refuses.

Flags:
  --gtid-prefix string   explicit prefix to embed (default: auto-pull from status)
  --cold                 confirm a cold reclone — embeds confirm=<group>
                         when the site has no divergent GTID. Mutually
                         exclusive with --gtid-prefix.
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
	fs.StringVar(&gtidPrefix, "gtid-prefix", "", "GTID prefix to embed; defaults to status.sites[site].divergentGtid")
	fs.BoolVar(&cold, "cold", false, "confirm a cold reclone (no divergent GTID); embeds confirm=<group>")
	fs.Usage = func() { fmt.Fprint(stderr, recloneUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	positional := fs.Args()
	if len(positional) != 2 {
		fmt.Fprint(stderr, recloneUsage)
		return fmt.Errorf("reclone requires exactly two positional arguments (group and site)")
	}
	group, site := positional[0], positional[1]
	if group == "" || site == "" {
		return fmt.Errorf("group and site must be non-empty")
	}
	if cold && gtidPrefix != "" {
		return fmt.Errorf("--cold and --gtid-prefix are mutually exclusive; --cold automatically generates the confirm token")
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

	// Resolve the annotation suffix. The plugin mirrors the operator's
	// safety interlock client-side so a missing --cold on a fresh site
	// fails before round-tripping a doomed annotation.
	suffix := gtidPrefix
	switch {
	case suffix != "":
		// User-provided prefix wins verbatim. If they passed
		// confirm=<group> by hand for a cold reclone, that's their
		// call — the operator validates it.
	case cold:
		if observedGtid != "" {
			return fmt.Errorf("--cold refused: site %q has a recorded divergent GTID (%q); rerun without --cold or pass --gtid-prefix=<prefix> so the operator can verify intent against the divergent set",
				site, observedGtid)
		}
		suffix = "confirm=" + group
	case observedGtid != "":
		suffix = autoPullDivergentGtidPrefix(&fg, site)
		if suffix == "" {
			return fmt.Errorf("site %q has a divergent GTID (%q) that is too short to forward safely (< %d chars); pass --gtid-prefix=<value> manually",
				site, observedGtid, minRecloneGtidPrefix)
		}
	default:
		// No GTID, no --cold — cold reclone not confirmed. Reject
		// with the exact remediation so a fat-finger can't wipe the
		// datadir.
		return fmt.Errorf("site %q has no divergent GTID — this is a cold reclone (CLONE INSTANCE will wipe the datadir). Re-run with --cold to confirm, or pass --gtid-prefix=confirm=%s explicitly",
			site, group)
	}

	value := site
	if suffix != "" {
		value = site + ":" + suffix
	}

	patched := fg.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[RecloneAnnotation] = value
	if err := cl.Patch(ctx, patched, client.MergeFrom(&fg)); err != nil {
		return fmt.Errorf("annotate failover group: %w", err)
	}

	fmt.Fprintf(stdout, "Annotated %s/%s with %s=%s\n", ns, group, RecloneAnnotation, value)
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

// autoPullDivergentGtidPrefix returns the first 12 chars of the
// observed divergent GTID for the site — comfortably above the
// operator's minimum of minRecloneGtidPrefix (8) so the value is
// recognisable in logs and event messages without forwarding the
// entire GTID set on the command line. If the recorded GTID is
// shorter than minRecloneGtidPrefix (8), the helper returns "" so the
// caller can refuse the operation rather than ship an annotation the
// operator will reject.
func autoPullDivergentGtidPrefix(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range fg.Status.Sites {
		if s.Name == site && s.DivergentGtid != "" {
			gtid := s.DivergentGtid
			if len(gtid) < minRecloneGtidPrefix {
				return ""
			}
			if len(gtid) > 12 {
				return gtid[:12]
			}
			return gtid
		}
	}
	return ""
}
