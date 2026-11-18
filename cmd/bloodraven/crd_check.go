package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/internal/crdcompat"
)

// The operator's check at start (C-14 of proposals/20-api-v1beta1.md).
// Before any controller starts, and whether or not this replica becomes
// the leader, the operator compares the installed CRDs with the ones it was
// built with and validates every stored object of the five kinds:
//
//   - An installed CRD that is missing, or older than this build needs (the
//     version it uses is not served, or a field, a field's type or an enum
//     value it writes is absent), refuses the start. That is a chart-only
//     `helm upgrade`: Helm never upgrades a chart's crds/. Against such a
//     CRD the API server prunes what the operator writes without an error.
//   - An installed CRD that is newer only warns: it is what the previous
//     operator sees between the two steps of an upgrade, and what an
//     operator rolled back without its CRDs sees.
//   - A stored object that does not have the shape the installed schema
//     describes refuses the start: it is from before a breaking schema
//     change, the typed client cannot decode it, and one such object fails
//     the list of its whole kind. A stored object that only breaks a value
//     rule warns.
//
// The check writes nothing. A refusal names every problem and points at the
// upgrade docs.

const (
	crdCheckEnforce = "enforce"
	crdCheckWarn    = "warn"

	crdCheckDocs = "https://bloodraven.dev/docs/operations/upgrade-policy#the-operators-check-at-start"

	// Log messages of the check (site/content/docs/8.observability/7.log-schema.md).
	msgCRDCheckRefused       = "CRD check: the operator must not start"
	msgCRDCheckNewerCRD      = "CRD check: an installed CRD is newer than this operator"
	msgCRDCheckRuleViolation = "CRD check: a stored object breaks a validation rule"
	msgCRDCheckPassed        = "CRD check passed"
	msgCRDCheckOverridden    = "CRD check failed; starting anyway because of --crd-check=warn"
	msgCRDCheckRetry         = "CRD check could not be made; retrying"

	crdCheckAttempts = 6
	crdCheckBackoff  = 5 * time.Second
	crdCheckTimeout  = 2 * time.Minute
)

// runCRDCheck makes the check and returns what the reconciler reports on
// each group when an installed CRD is newer ("" when none is). It returns an
// error when the operator must not start.
func runCRDCheck(ctx context.Context, cfg *rest.Config, mode string, logger *slog.Logger) (newerCRDs string, err error) {
	if mode != crdCheckEnforce && mode != crdCheckWarn {
		return "", fmt.Errorf("--crd-check=%q: want %q or %q", mode, crdCheckEnforce, crdCheckWarn)
	}
	scheme := k8sruntime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return "", err
	}
	crds, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return "", fmt.Errorf("CRD check: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("CRD check: %w", err)
	}
	get := func(ctx context.Context, name string) (*apiextensionsv1.CustomResourceDefinition, error) {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		return crd, crds.Get(ctx, client.ObjectKey{Name: name}, crd)
	}

	var report crdcompat.StartupReport
	for attempt := 1; ; attempt++ {
		checkCtx, cancel := context.WithTimeout(ctx, crdCheckTimeout)
		report, err = crdcompat.CheckStartup(checkCtx, get, crdcompat.DynamicLister(dyn))
		cancel()
		if err == nil {
			break
		}
		// The check could not be made. That says nothing about the
		// cluster, so it is neither a pass nor a refusal.
		err = crdCheckError(err)
		if mode == crdCheckWarn {
			logger.Warn(msgCRDCheckOverridden, "problems", 1, "error", err.Error(), "docs", crdCheckDocs)
			return "", nil
		}
		// A missing permission does not heal by waiting.
		if attempt == crdCheckAttempts || ctx.Err() != nil || isForbidden(err) {
			return "", err
		}
		logger.Warn(msgCRDCheckRetry, "attempt", attempt, "error", err.Error())
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(crdCheckBackoff):
		}
	}
	return reportCRDCheck(report, mode, logger)
}

// crdCheckError says what a failed attempt means. A Forbidden answer is
// almost always an operator image upgraded without its ClusterRole: the
// check reads the five CRDs, a permission the role has from v1.8.0 on.
func crdCheckError(err error) error {
	if isForbidden(err) {
		return fmt.Errorf("CRD check could not be made: the operator's ClusterRole lacks a permission the check needs "+
			"(get on the five shipstream.io CustomResourceDefinitions by name, and list on the five kinds); upgrade the RBAC with the operator: %w", err)
	}
	return fmt.Errorf("CRD check could not be made: %w", err)
}

// isForbidden reports whether err, or an error it wraps, is a Forbidden
// answer of the API server.
func isForbidden(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if apierrors.IsForbidden(err) {
			return true
		}
	}
	return false
}

// reportCRDCheck logs report and decides whether the operator starts.
func reportCRDCheck(report crdcompat.StartupReport, mode string, logger *slog.Logger) (string, error) {
	var newer []string
	for _, f := range report.NewerCRDs {
		logger.Warn(msgCRDCheckNewerCRD, "subject", f.Subject, "detail", f.Detail)
		newer = append(newer, f.String())
	}
	for _, f := range report.RuleViolations {
		logger.Warn(msgCRDCheckRuleViolation, "subject", f.Subject, "detail", f.Detail)
	}
	for _, f := range report.Refusals {
		logger.Error(msgCRDCheckRefused, "subject", f.Subject, "detail", f.Detail, "docs", crdCheckDocs)
	}
	newerCRDs := strings.Join(newer, "; ")
	if len(report.Refusals) == 0 {
		objects := 0
		for _, n := range report.Checked {
			objects += n
		}
		logger.Info(msgCRDCheckPassed, "objects", objects, "newerCRDs", len(report.NewerCRDs), "ruleViolations", len(report.RuleViolations))
		return newerCRDs, nil
	}
	if mode == crdCheckWarn {
		logger.Warn(msgCRDCheckOverridden, "problems", len(report.Refusals), "docs", crdCheckDocs)
		return newerCRDs, nil
	}
	return "", fmt.Errorf("%d problem(s) with the installed CRDs or the stored objects, the first: %s. See %s",
		len(report.Refusals), report.Refusals[0], crdCheckDocs)
}
