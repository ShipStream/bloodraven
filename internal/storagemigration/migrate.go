// Package storagemigration is `kubectl bloodraven migrate-storage` (C-7 and
// C-5b of proposals/20-api-v1beta1.md, requirements U-10 to U-13): after the
// CRDs of a release that changes the storage version were applied, it
// rewrites every Bloodraven object at the new storage version and then
// trims the old version out of each CRD's status.storedVersions. Only then
// can a later release remove the old version from the CRDs.
//
// Order of work, and what each step guarantees:
//
//  1. Permissions. The caller's permissions are checked with
//     SelfSubjectAccessReviews before anything is read in bulk or written.
//  2. CRDs. Every CRD must be installed and store at the version this
//     command migrates to.
//  3. Objects. Every object of the five kinds is validated against the
//     installed schema as a create would be (internal/crdcompat). An object
//     that does not conform is listed and nothing is written: rewriting it
//     would persist what the API server pruned from it.
//  4. Rewrite. Each object is written back as it was read. The API server
//     encodes it at the storage version. A conflict (the operator writes
//     status all the time) is retried on a fresh read. Objects are never
//     deleted or recreated.
//  5. Trim. status.storedVersions of a CRD is set to the storage version
//     only after every object of its kind was rewritten. From the moment
//     the CRDs were applied the API server writes that version only, so an
//     object created or changed while this command runs needs no rewrite.
//
// The command can be run again at any time: a kind that is already
// migrated is skipped.
package storagemigration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/internal/crdcompat"
)

// DocsURL is where a refusal sends the reader.
const DocsURL = "https://bloodraven.dev/docs/configuration/crd-versioning#storage-version-migration"

// Options configures Run.
type Options struct {
	// Client reads and writes CustomResourceDefinitions and creates
	// SelfSubjectAccessReviews. Its scheme needs apiextensions/v1 and
	// authorization/v1.
	Client client.Client
	// Dynamic reads and rewrites the objects. The typed client is not
	// used: it cannot decode an object that does not fit the schema.
	Dynamic dynamic.Interface
	// DryRun makes every check and reports what would be done, and writes
	// nothing.
	DryRun bool
	// Out receives the report.
	Out io.Writer

	// conflictRetries and conflictBackoff bound the retries of one
	// object's rewrite (tests shorten the backoff).
	conflictRetries int
	conflictBackoff time.Duration
}

// Permission is one permission migrate-storage needs.
type Permission struct {
	Group, Resource, Subresource, Name, Verb string
	// Write is true for the permissions a dry run does not use.
	Write bool
}

func (p Permission) String() string {
	resource := p.Resource
	if p.Subresource != "" {
		resource += "/" + p.Subresource
	}
	out := fmt.Sprintf("%s %s in %s", p.Verb, resource, p.Group)
	if p.Name != "" {
		out += " (" + p.Name + ")"
	}
	return out
}

// RequiredPermissions lists what the caller needs, in the order it is
// checked. The example ClusterRole bloodraven-storage-migrator
// (examples/storage-migrator-clusterrole.yaml) grants exactly this.
func RequiredPermissions() []Permission {
	var out []Permission
	for _, r := range crdcompat.Resources {
		for _, verb := range []string{"get", "list", "update"} {
			out = append(out, Permission{Group: crdcompat.Group, Resource: r.Plural, Verb: verb, Write: verb == "update"})
		}
	}
	for _, r := range crdcompat.Resources {
		out = append(out,
			Permission{Group: apiextensionsv1.GroupName, Resource: "customresourcedefinitions", Name: r.CRDName(), Verb: "get"},
			Permission{Group: apiextensionsv1.GroupName, Resource: "customresourcedefinitions", Subresource: "status", Name: r.CRDName(), Verb: "update", Write: true},
		)
	}
	return out
}

// Refusal is an error that says the migration was not started, or not
// finished, for a reason the report already spelled out.
type Refusal struct{ Reason string }

func (r *Refusal) Error() string { return r.Reason }

func refuse(format string, args ...any) error {
	return &Refusal{Reason: fmt.Sprintf(format, args...)}
}

// Run migrates the stored objects of the five kinds to the storage
// version. See the package comment for the steps.
func Run(ctx context.Context, o Options) error {
	if o.conflictRetries == 0 {
		o.conflictRetries = 8
	}
	if o.conflictBackoff == 0 {
		o.conflictBackoff = 250 * time.Millisecond
	}
	target := crdcompat.StorageVersion
	say := func(format string, args ...any) { fmt.Fprintf(o.Out, format+"\n", args...) }

	// 1. Permissions.
	missing, err := missingPermissions(ctx, o.Client, o.DryRun)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		say("Missing permissions:")
		for _, p := range missing {
			say("  %s", p)
		}
		say("")
		say("Nothing was changed. Bind the bloodraven-storage-migrator ClusterRole to the account that runs this command: %s", DocsURL)
		return refuse("%d permission(s) missing", len(missing))
	}

	// 2. CRDs.
	get := func(ctx context.Context, name string) (*apiextensionsv1.CustomResourceDefinition, error) {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		return crd, o.Client.Get(ctx, client.ObjectKey{Name: name}, crd)
	}
	installed, err := crdcompat.InstalledCRDs(ctx, get)
	if err != nil {
		return err
	}
	var crdProblems []string
	pending := map[string]bool{} // CRD names whose storedVersions still list another version
	say("CustomResourceDefinitions:")
	tw := tabwriter.NewWriter(o.Out, 0, 4, 2, ' ', 0)
	for _, r := range crdcompat.Resources {
		name := r.CRDName()
		crd := installed[name]
		if crd == nil {
			crdProblems = append(crdProblems, name+" is not installed")
			fmt.Fprintf(tw, "  %s\tnot installed\t\n", name)
			continue
		}
		stored := crdcompat.StoredAs(crd)
		fmt.Fprintf(tw, "  %s\tstorage version %s\tstoredVersions %s\n", name, stored, strings.Join(crd.Status.StoredVersions, ","))
		if v := crdcompat.Version(crd, target); v == nil || !v.Served || stored != target {
			crdProblems = append(crdProblems, fmt.Sprintf("%s stores objects as %s, and this command migrates to %s", name, stored, target))
			continue
		}
		if len(crd.Status.StoredVersions) != 1 || crd.Status.StoredVersions[0] != target {
			pending[name] = true
		}
	}
	_ = tw.Flush()
	say("")
	if len(crdProblems) > 0 {
		for _, p := range crdProblems {
			say("  %s", p)
		}
		say("")
		say("Nothing was changed. Apply the CRDs of the release you are upgrading to first, and use the kubectl-bloodraven of that release: %s", DocsURL)
		return refuse("%d CustomResourceDefinition(s) are not ready for the migration", len(crdProblems))
	}
	if len(pending) == 0 {
		say("Every Bloodraven object is already stored as %s/%s. Nothing to do.", crdcompat.Group, target)
		return nil
	}

	// 3. Objects, before any write.
	list := crdcompat.DynamicLister(o.Dynamic)
	check, err := crdcompat.CheckObjects(ctx, list, installed)
	if err != nil {
		return err
	}
	total := 0
	for _, n := range check.Checked {
		total += n
	}
	if len(check.Problems) > 0 {
		say("%d of %d object(s) do not conform to the installed schema:", len(check.Problems), total)
		for _, p := range check.Problems {
			say("  %s", p)
		}
		say("")
		say("Nothing was changed. An object that does not fit the schema was stored by a release whose schema differs. Rewriting it would make permanent what the API server now drops from it. See the migration table for what each field became: %s", DocsURL)
		return refuse("%d object(s) do not conform to the installed schema", len(check.Problems))
	}
	say("%d object(s) checked against the installed schema: all conform.", total)
	say("")

	if o.DryRun {
		say("Dry run: would rewrite the objects of %s and set their storedVersions to %s. Nothing was changed.", strings.Join(sortedKeys(pending), ", "), target)
		return nil
	}

	// 4. Rewrite.
	say("Rewriting objects as %s/%s:", crdcompat.Group, target)
	var failures []string
	trimmable := map[string]bool{}
	tw = tabwriter.NewWriter(o.Out, 0, 4, 2, ' ', 0)
	for _, r := range crdcompat.Resources {
		if !pending[r.CRDName()] {
			fmt.Fprintf(tw, "  %s\talready migrated\t\n", r.Kind)
			continue
		}
		rewritten, failed, err := rewriteAll(ctx, o, list, r)
		if err != nil {
			_ = tw.Flush()
			return err
		}
		fmt.Fprintf(tw, "  %s\t%d written back\t%d failed\n", r.Kind, rewritten, len(failed))
		failures = append(failures, failed...)
		if len(failed) == 0 {
			trimmable[r.CRDName()] = true
		}
	}
	_ = tw.Flush()
	say("")

	// 5. Trim, per kind, only where every object was rewritten.
	say("Setting status.storedVersions:")
	tw = tabwriter.NewWriter(o.Out, 0, 4, 2, ' ', 0)
	for _, r := range crdcompat.Resources {
		name := r.CRDName()
		if !trimmable[name] {
			continue
		}
		if err := trimStoredVersions(ctx, o, name, target); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			fmt.Fprintf(tw, "  %s\tnot changed: %v\n", name, err)
			continue
		}
		fmt.Fprintf(tw, "  %s\t[%s]\n", name, target)
	}
	_ = tw.Flush()
	say("")
	if len(failures) > 0 {
		say("Not finished:")
		for _, f := range failures {
			say("  %s", f)
		}
		say("")
		say("The objects that were rewritten stay rewritten, and status.storedVersions was left alone for every kind with a failure. Run the command again.")
		return refuse("%d object(s) or CustomResourceDefinition(s) were not migrated", len(failures))
	}
	say("Done. Every Bloodraven object is stored as %s/%s.", crdcompat.Group, target)
	return nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// missingPermissions returns the required permissions the caller lacks. A
// dry run needs the read permissions only.
func missingPermissions(ctx context.Context, c client.Client, dryRun bool) ([]Permission, error) {
	var missing []Permission
	for _, p := range RequiredPermissions() {
		if dryRun && p.Write {
			continue
		}
		review := &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group: p.Group, Resource: p.Resource, Subresource: p.Subresource, Name: p.Name, Verb: p.Verb,
			},
		}}
		if err := c.Create(ctx, review); err != nil {
			return nil, fmt.Errorf("check permission %q: %w", p, err)
		}
		if !review.Status.Allowed {
			missing = append(missing, p)
		}
	}
	return missing, nil
}

// rewriteAll writes every object of r back as it was read. It returns how
// many were written and one line per object that could not be.
func rewriteAll(ctx context.Context, o Options, list crdcompat.Lister, r crdcompat.Resource) (rewritten int, failed []string, err error) {
	res := o.Dynamic.Resource(r.GVR(crdcompat.StorageVersion))
	err = crdcompat.EachObject(ctx, list, r, func(obj *unstructured.Unstructured) error {
		ns, name := obj.GetNamespace(), obj.GetName()
		current := obj
		for attempt := 0; ; attempt++ {
			_, werr := res.Namespace(ns).Update(ctx, current, metav1.UpdateOptions{})
			switch {
			case werr == nil:
				rewritten++
				return nil
			case apierrors.IsNotFound(werr):
				// Deleted since it was listed: nothing is stored.
				return nil
			case apierrors.IsConflict(werr) && attempt < o.conflictRetries:
				// Someone else wrote it (the operator's status writes).
				// That write was already encoded at the storage version,
				// but only a write of our own proves it: read and retry.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(o.conflictBackoff):
				}
				fresh, gerr := res.Namespace(ns).Get(ctx, name, metav1.GetOptions{})
				if apierrors.IsNotFound(gerr) {
					return nil
				}
				if gerr != nil {
					failed = append(failed, fmt.Sprintf("%s %s/%s: read after a conflict failed, retry later: %v", r.Kind, ns, name, gerr))
					return nil
				}
				current = fresh
			case apierrors.IsConflict(werr):
				failed = append(failed, fmt.Sprintf("%s %s/%s: still changing after %d attempts (another writer keeps updating it); this is not a schema problem, run the command again",
					r.Kind, ns, name, attempt+1))
				return nil
			case errors.Is(werr, context.Canceled), errors.Is(werr, context.DeadlineExceeded):
				return werr
			default:
				failed = append(failed, fmt.Sprintf("%s %s/%s: the API server refused the write: %v", r.Kind, ns, name, werr))
				return nil
			}
		}
	})
	return rewritten, failed, err
}

// trimStoredVersions sets the CRD's status.storedVersions to target. It
// re-reads the CRD first: the storage version must still be target, or
// objects written since would be stored at a version the list no longer
// names.
func trimStoredVersions(ctx context.Context, o Options, name, target string) error {
	for attempt := 0; ; attempt++ {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := o.Client.Get(ctx, client.ObjectKey{Name: name}, crd); err != nil {
			return err
		}
		if stored := crdcompat.StoredAs(crd); stored != target {
			return fmt.Errorf("its storage version changed to %s while the objects were rewritten", stored)
		}
		if len(crd.Status.StoredVersions) == 1 && crd.Status.StoredVersions[0] == target {
			return nil
		}
		crd.Status.StoredVersions = []string{target}
		err := o.Client.Status().Update(ctx, crd)
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) || attempt >= o.conflictRetries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.conflictBackoff):
		}
	}
}
