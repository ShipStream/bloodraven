package scenarios

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// clusterPodCIDRs returns every node's pod CIDR. NetworkPolicies that
// must cut one pod's traffic but keep kubelet probes (which come from
// the node IP) need an ipBlock that admits everything except pod IPs:
// an ipBlock that also covered pod IPs would re-admit the very pod the
// policy is meant to cut.
func clusterPodCIDRs(ctx context.Context, env *runner.Env) ([]string, error) {
	nodes, err := env.Kube.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var cidrs []string
	for _, n := range nodes.Items {
		if len(n.Spec.PodCIDRs) > 0 {
			cidrs = append(cidrs, n.Spec.PodCIDRs...)
		} else if n.Spec.PodCIDR != "" {
			cidrs = append(cidrs, n.Spec.PodCIDR)
		}
	}
	if len(cidrs) == 0 {
		return nil, fmt.Errorf("no node reports spec.podCIDR; cannot build a pod-scoped NetworkPolicy")
	}
	sort.Strings(cidrs)
	return cidrs, nil
}

// mysqlPodSelector selects one site's MySQL pod.
func mysqlPodSelector(fg, site string) metav1.LabelSelector {
	return metav1.LabelSelector{MatchLabels: map[string]string{
		"app.kubernetes.io/name":     "mysql",
		"app.kubernetes.io/instance": fg,
		"shipstream.io/site":         site,
	}}
}

// peersExcept admits every pod in every namespace whose label key is
// not one of values, plus everything outside the pod network.
func peersExcept(key string, values []string, podCIDRs []string) []networkingv1.NetworkPolicyPeer {
	return []networkingv1.NetworkPolicyPeer{
		{
			NamespaceSelector: &metav1.LabelSelector{},
			PodSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: key, Operator: metav1.LabelSelectorOpNotIn, Values: values,
			}}},
		},
		{
			NamespaceSelector: &metav1.LabelSelector{},
			PodSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: key, Operator: metav1.LabelSelectorOpDoesNotExist,
			}}},
		},
		{IPBlock: &networkingv1.IPBlock{CIDR: "0.0.0.0/0", Except: podCIDRs}},
	}
}

// repairDivergentSites reclones every site the operator has blocked
// (RecoveryBlocked, or a follower whose source convergence is Blocked),
// one at a time, and waits for a healthy baseline. Scenarios that are
// expected to lose data while a design decision is open use it so a
// failing run does not wedge every scenario after it.
func repairDivergentSites(ctx context.Context, env *runner.Env, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	requested := map[string]bool{}
	for {
		if err := AssertHealthyBaseline(ctx, env); err == nil {
			return nil
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		boot := findCondition(mfg.Status.Conditions, "Bootstrapping")
		// A failed bootstrap also reports Status=True (Reason=Failed).
		cloning := boot != nil && boot.Status == metav1.ConditionTrue && boot.Reason != "Failed"
		_, pending := mfg.GetAnnotations()["bloodraven.shipstream.io/reclone-site"]
		if !cloning && !pending && mfg.Status.ActiveSite != "" {
			if site, value := nextBlockedSite(mfg, requested); site != "" {
				if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
					"bloodraven.shipstream.io/reclone-site", value); err != nil {
					return fmt.Errorf("annotate repair reclone %s: %w", value, err)
				}
				requested[site] = true
				env.Capture.Note("repair: requested reclone-site=" + value)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("group not healthy %s after repair began: %v", timeout, AssertHealthyBaseline(ctx, env))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func nextBlockedSite(mfg *v1alpha1.MysqlFailoverGroup, requested map[string]bool) (site, annotation string) {
	for _, s := range mfg.Status.Sites {
		if s.Name == mfg.Status.ActiveSite || requested[s.Name] {
			continue
		}
		blocked := s.RecoveryState == "RecoveryBlocked" ||
			s.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked
		if !blocked {
			continue
		}
		if gtid := strings.TrimSpace(s.DivergentGtid); len(gtid) >= 8 {
			return s.Name, s.Name + ":" + gtid[:8]
		}
		return s.Name, s.Name + ":confirm=" + mfg.Name
	}
	return "", ""
}
