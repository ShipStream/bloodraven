package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	rbacv1 "k8s.io/api/rbac/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// VerbDenial names the verbs to strip from a single apiGroup/resource pair on
// the operator's bound ClusterRole. Multiple denials are applied in sequence,
// each splitting multi-resource rules so unrelated grants survive.
type VerbDenial struct {
	APIGroup string
	Resource string
	Verbs    []string
}

// RBACDenialResult is returned by DenyOperatorClusterRoleVerbs for evidence
// capture: the resolved role/binding and the before/after rule JSON, suitable
// for env.Capture.
type RBACDenialResult struct {
	RoleName      string
	BindingName   string
	OriginalRules string
	PatchedRules  string
}

// DenyOperatorClusterRoleVerbs resolves the ClusterRole bound to the operator
// ServiceAccount, saves its current rules, removes the requested verbs from
// the named apiGroup/resource pairs (splitting multi-resource rules so
// unrelated grants survive), and writes the updated role. Refuses to proceed
// if the requested denial changes no rules — a scenario that believes it
// denied a verb but did not would otherwise pass hollowly.
//
// It returns the before/after evidence and an idempotent restore closure. The
// SAME closure is also pushed as a LIFO cleanup reverter, so a scenario may
// call restore() explicitly mid-run (e.g. "restore RBAC before scaling the old
// primary back up") and rely on cleanup to restore it anyway if the scenario
// fails first — the closure's guard makes the second call a no-op.
func (a *Actions) DenyOperatorClusterRoleVerbs(ctx context.Context, saNamespace, saName string, denials []VerbDenial) (RBACDenialResult, func(context.Context) error, error) {
	roleName, bindingName, err := a.K.ResolveBoundClusterRole(ctx, saNamespace, saName)
	if err != nil {
		return RBACDenialResult{}, nil, err
	}
	cr, err := a.K.GetClusterRole(ctx, roleName)
	if err != nil {
		return RBACDenialResult{}, nil, fmt.Errorf("get bound ClusterRole %s: %w", roleName, err)
	}
	original := pgkube.CloneClusterRoleRules(cr.Rules)
	originalJSON, err := json.MarshalIndent(original, "", "  ")
	if err != nil {
		return RBACDenialResult{}, nil, fmt.Errorf("marshal original ClusterRole rules: %w", err)
	}

	patched := pgkube.CloneClusterRoleRules(cr.Rules)
	for _, d := range denials {
		patched = pgkube.RemoveVerbsForResource(patched, d.APIGroup, d.Resource, d.Verbs)
	}
	patchedJSON, err := json.MarshalIndent(patched, "", "  ")
	if err != nil {
		return RBACDenialResult{}, nil, fmt.Errorf("marshal patched ClusterRole rules: %w", err)
	}

	if string(originalJSON) == string(patchedJSON) {
		return RBACDenialResult{}, nil, fmt.Errorf(
			"RBAC denial changed no rules on ClusterRole %s (denials=%+v); the operator role layout may have moved — refuse to run a hollow denial",
			roleName, denials)
	}
	restoreRules := removedRBACGrants(original, denials)

	cr.Rules = patched
	if err := a.K.UpdateClusterRole(ctx, cr); err != nil {
		return RBACDenialResult{}, nil, fmt.Errorf("apply RBAC denial to ClusterRole %s: %w", roleName, err)
	}

	// Idempotent restore closure: re-read the role for the latest
	// resourceVersion and add back only grants removed by this injection, so
	// unrelated concurrent rule updates survive. Only marks itself done on success, so a failed
	// explicit restore still gets retried by the cleanup reverter. Scenarios
	// are sequential, so no locking is needed.
	restored := false
	restore := func(ctx context.Context) error {
		if restored {
			return nil
		}
		fresh, err := a.K.GetClusterRole(ctx, roleName)
		if err != nil {
			return fmt.Errorf("re-read ClusterRole %s for restore: %w", roleName, err)
		}
		fresh.Rules = append(pgkube.CloneClusterRoleRules(fresh.Rules), pgkube.CloneClusterRoleRules(restoreRules)...)
		if err := a.K.UpdateClusterRole(ctx, fresh); err != nil {
			return fmt.Errorf("restore ClusterRole %s rules: %w", roleName, err)
		}
		restored = true
		return nil
	}
	a.push(fmt.Sprintf("restore ClusterRole %s rules", roleName), restore)

	return RBACDenialResult{
		RoleName:      roleName,
		BindingName:   bindingName,
		OriginalRules: string(originalJSON),
		PatchedRules:  string(patchedJSON),
	}, restore, nil
}

func removedRBACGrants(original []rbacv1.PolicyRule, denials []VerbDenial) []rbacv1.PolicyRule {
	var grants []rbacv1.PolicyRule
	for _, denial := range denials {
		denied := make(map[string]bool, len(denial.Verbs))
		for _, verb := range denial.Verbs {
			denied[verb] = true
		}
		for _, rule := range original {
			if !slices.Contains(rule.APIGroups, denial.APIGroup) || !slices.Contains(rule.Resources, denial.Resource) {
				continue
			}
			verbs := make([]string, 0, len(rule.Verbs))
			for _, verb := range rule.Verbs {
				if denied[verb] {
					verbs = append(verbs, verb)
				}
			}
			if len(verbs) == 0 {
				continue
			}
			grant := *rule.DeepCopy()
			grant.APIGroups = []string{denial.APIGroup}
			grant.Resources = []string{denial.Resource}
			grant.Verbs = verbs
			grants = append(grants, grant)
		}
	}
	return grants
}
