// Package chaos exposes the failure injections used by scenarios.
// Each Apply* method also pushes a reverter onto the Actions stack so
// Cleanup can roll back in reverse order, even on assertion failure.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// Actions is the per-scenario chaos handle. Methods are not safe for
// concurrent use; scenarios are sequential by design.
type Actions struct {
	K         *pgkube.Client
	Namespace string
	FG        string

	mu      sync.Mutex
	revStack []reverter
}

type reverter struct {
	what string
	fn   func(context.Context) error
}

// New builds an Actions bound to a kube client, namespace, and FG name.
func New(k *pgkube.Client, namespace, fg string) *Actions {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	if fg == "" {
		fg = pgkube.FailoverGroupName
	}
	return &Actions{K: k, Namespace: namespace, FG: fg}
}

func (a *Actions) push(what string, fn func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revStack = append(a.revStack, reverter{what: what, fn: fn})
}

// Revert runs the revert stack in LIFO order, joining errors. Always
// drains the stack before returning so a second Revert is a no-op.
func (a *Actions) Revert(ctx context.Context) error {
	a.mu.Lock()
	stack := a.revStack
	a.revStack = nil
	a.mu.Unlock()
	var errs []error
	for i := len(stack) - 1; i >= 0; i-- {
		if err := stack[i].fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("revert %s: %w", stack[i].what, err))
		}
	}
	return errors.Join(errs...)
}

// PendingReverts returns the descriptions of currently-pending
// reverters. Useful for logging.
func (a *Actions) PendingReverts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.revStack))
	for i, r := range a.revStack {
		out[i] = r.what
	}
	return out
}

// ScaleSiteToZero scales a site's MySQL Deployment to 0, holding the
// site offline past the brief Deployment-respawn window. Reverter
// scales back to 1.
func (a *Actions) ScaleSiteToZero(ctx context.Context, site string) error {
	dep := pgkube.MysqlDeploymentName(a.FG, site)
	if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 0); err != nil {
		return fmt.Errorf("scale %s to 0: %w", dep, err)
	}
	a.push(fmt.Sprintf("scale %s back to 1", dep), func(ctx context.Context) error {
		return a.K.ScaleDeployment(ctx, a.Namespace, dep, 1)
	})
	return nil
}

// DeleteSitePod force-deletes the site's MySQL pod (Deployment will
// respawn it within seconds). No reverter — the cluster recovers on
// its own.
func (a *Actions) DeleteSitePod(ctx context.Context, site string) error {
	zero := int64(0)
	return a.K.DeleteSitePod(ctx, a.Namespace, a.FG, site, &zero)
}

// PartitionSite applies a deny-all NetworkPolicy that blocks the
// site's MySQL pod from any pod-network communication. Reverter
// removes the NP.
func (a *Actions) PartitionSite(ctx context.Context, site string) error {
	if err := a.K.ApplyDenyAllNetworkPolicy(ctx, a.Namespace, a.FG, site); err != nil {
		return fmt.Errorf("apply NetworkPolicy for site %s: %w", site, err)
	}
	a.push(fmt.Sprintf("remove NetworkPolicy for %s", site), func(ctx context.Context) error {
		return a.K.RemoveDenyAllNetworkPolicy(ctx, a.Namespace, site)
	})
	return nil
}

// AnnotatePlannedFailover sets the bloodraven.shipstream.io/planned-failover
// annotation to the named target site. Reverter clears the annotation.
//
// The annotation is the public CR-driven trigger documented on
// PlannedFailoverSpec.OnCooldown.
func (a *Actions) AnnotatePlannedFailover(ctx context.Context, target string) error {
	const key = "bloodraven.shipstream.io/planned-failover"
	if err := a.K.AnnotateMFG(ctx, a.Namespace, key, target); err != nil {
		return fmt.Errorf("set planned-failover annotation: %w", err)
	}
	a.push("clear planned-failover annotation", func(ctx context.Context) error {
		return a.K.AnnotateMFG(ctx, a.Namespace, key, "")
	})
	return nil
}

// GlobalRecover is the safety-net cleanup the runner runs after every
// scenario, regardless of outcome. Mirrors `chaos.sh recover`:
// removes every chaos-partition NetworkPolicy and scales every MySQL
// site back to 1 replica. Idempotent.
func (a *Actions) GlobalRecover(ctx context.Context) error {
	var errs []error
	if err := a.K.RemoveAllChaosNetworkPolicies(ctx, a.Namespace); err != nil {
		errs = append(errs, fmt.Errorf("remove chaos NetworkPolicies: %w", err))
	}
	mfg, err := a.K.GetMFG(ctx, a.Namespace)
	if err == nil {
		for _, s := range mfg.Spec.Sites {
			dep := pgkube.MysqlDeploymentName(a.FG, s.Name)
			if err := a.K.ScaleDeployment(ctx, a.Namespace, dep, 1); err != nil {
				errs = append(errs, fmt.Errorf("scale %s back to 1: %w", dep, err))
			}
		}
	} else {
		errs = append(errs, fmt.Errorf("read MFG for global recover: %w", err))
	}
	return errors.Join(errs...)
}
