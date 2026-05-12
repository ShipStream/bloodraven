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

const recloneUsage = `Trigger a re-clone of a divergent site via annotation.

Usage:
  kubectl bloodraven reclone <group> <site> [flags]

When status.sites[<site>].divergentGtid is non-empty the operator
refuses to reclone unless the annotation value carries a prefix of the
observed GTID. This command can fetch the current divergent GTID and
auto-fill that prefix; pass --gtid-prefix to override.

Flags:
  --gtid-prefix string   explicit prefix to embed (default: auto-pull from status)
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
	)
	registerCommonFlags(fs, &cf)
	fs.StringVar(&gtidPrefix, "gtid-prefix", "", "GTID prefix to embed; defaults to status.sites[site].divergentGtid")
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

	if gtidPrefix == "" {
		gtidPrefix = autoPullDivergentGtidPrefix(&fg, site)
	}

	value := site
	if gtidPrefix != "" {
		value = site + ":" + gtidPrefix
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

// autoPullDivergentGtidPrefix returns the first 8+ chars of the
// observed divergent GTID for the site. The operator's matcher
// accepts any prefix of length >= minRecloneGtidPrefix (currently 8),
// so we forward enough of the GTID to satisfy the safety interlock
// without echoing the whole set on the command line.
func autoPullDivergentGtidPrefix(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range fg.Status.Sites {
		if s.Name == site && s.DivergentGtid != "" {
			gtid := s.DivergentGtid
			if len(gtid) > 12 {
				return gtid[:12]
			}
			return gtid
		}
	}
	return ""
}
