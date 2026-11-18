package crdcompat

import (
	"context"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// CRDGetter reads one installed CustomResourceDefinition by name.
type CRDGetter func(ctx context.Context, name string) (*apiextensionsv1.CustomResourceDefinition, error)

// maxListed bounds how many differences of one CRD a message spells out.
const maxListed = 8

// Finding is one thing the startup check found.
type Finding struct {
	// Subject is the CRD's name, or "<Kind> <namespace>/<name>" for an
	// object.
	Subject string
	// Detail says what is wrong with it.
	Detail string
}

func (f Finding) String() string { return f.Subject + ": " + f.Detail }

// StartupReport is the result of CheckStartup.
type StartupReport struct {
	// Refusals are the reasons the operator must not start: an installed
	// CRD is missing or older than this build needs, or a stored object
	// does not have the shape the schema describes.
	Refusals []Finding
	// NewerCRDs are the installed CRDs that have something this build
	// does not know. The operator starts.
	NewerCRDs []Finding
	// RuleViolations are stored objects that break a value rule of the
	// installed schema and are otherwise well-formed. The operator starts.
	RuleViolations []Finding
	// Checked counts the stored objects that were validated, by kind.
	Checked map[string]int
}

// InstalledCRDs reads the five CRDs. A CRD that is not installed is absent
// from the result.
func InstalledCRDs(ctx context.Context, get CRDGetter) (map[string]*apiextensionsv1.CustomResourceDefinition, error) {
	out := make(map[string]*apiextensionsv1.CustomResourceDefinition, len(Resources))
	for _, r := range Resources {
		crd, err := get(ctx, r.CRDName())
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read CustomResourceDefinition %s: %w", r.CRDName(), err)
		}
		out[r.CRDName()] = crd
	}
	return out, nil
}

// CheckStartup is the operator's check before it starts any controller
// (C-14): it compares the installed CRDs with the ones this build was
// generated with and, when none of them is older, validates every stored
// object of the five kinds against the installed schema. It writes nothing.
//
// An error means the check could not be made (the API server did not
// answer); it says nothing about the cluster.
func CheckStartup(ctx context.Context, get CRDGetter, list Lister) (StartupReport, error) {
	var report StartupReport
	built, err := BuiltCRDs()
	if err != nil {
		return report, err
	}
	installed, err := InstalledCRDs(ctx, get)
	if err != nil {
		return report, err
	}
	for _, r := range Resources {
		name := r.CRDName()
		crd := installed[name]
		if crd == nil {
			report.Refusals = append(report.Refusals, Finding{Subject: name, Detail: "the CustomResourceDefinition is not installed"})
			continue
		}
		d := Compare(built[name], crd)
		if len(d.Older) > 0 {
			report.Refusals = append(report.Refusals, Finding{Subject: name,
				Detail: "the installed CustomResourceDefinition is older than this operator needs: " + Summary(d.Older, maxListed)})
		}
		if len(d.Newer) > 0 {
			report.NewerCRDs = append(report.NewerCRDs, Finding{Subject: name,
				Detail: "the installed CustomResourceDefinition is newer than this operator: " + Summary(d.Newer, maxListed)})
		}
	}
	if len(report.Refusals) > 0 {
		// The objects cannot be judged against a schema this build cannot
		// use, and may not even be readable at its version.
		return report, nil
	}
	objects, err := CheckObjects(ctx, list, installed)
	if err != nil {
		return report, err
	}
	report.Checked = objects.Checked
	for _, p := range objects.Problems {
		subject := fmt.Sprintf("%s %s/%s", p.Resource.Kind, p.Namespace, p.Name)
		if len(p.Structural) > 0 {
			report.Refusals = append(report.Refusals, Finding{Subject: subject,
				Detail: "the stored object does not fit the installed schema: " + p.Structural.ToAggregate().Error()})
		}
		if len(p.Rules) > 0 {
			report.RuleViolations = append(report.RuleViolations, Finding{Subject: subject,
				Detail: "the stored object breaks a validation rule of the installed schema: " + p.Rules.ToAggregate().Error()})
		}
	}
	return report, nil
}
