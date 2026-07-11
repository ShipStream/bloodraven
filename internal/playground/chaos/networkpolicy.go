package chaos

import (
	"context"
	"fmt"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// DenyDNSEgress applies a NetworkPolicy on the site's MySQL pod that blocks
// egress to the kube-dns ClusterIP and its backend pod IPs (a scoped
// cluster-DNS outage) and pushes a reverter that removes it. Callers discover
// denyIPs via Kube.DiscoverKubeDNSClusterIP and Kube.DiscoverKubeDNSEndpointIPs
// — both are needed because CNIs differ on whether NetworkPolicy is enforced
// before or after Service DNAT. Non-DNS egress (direct-IP paths) is
// unaffected — this is deliberately narrower than PartitionSite's deny-all.
func (a *Actions) DenyDNSEgress(ctx context.Context, site string, denyIPs []string) error {
	if err := a.K.ApplyDNSEgressDenyNetworkPolicy(ctx, a.Namespace, a.FG, site, denyIPs); err != nil {
		return fmt.Errorf("apply DNS egress deny for %s: %w", site, err)
	}
	name := pgkube.DNSEgressDenyPolicyName(site)
	a.push(fmt.Sprintf("remove DNS-deny NetworkPolicy %s", name), func(ctx context.Context) error {
		return a.K.RemoveNetworkPolicy(ctx, a.Namespace, name)
	})
	return nil
}

// PartitionDragonflyPod applies a deny-all NetworkPolicy selecting only the
// site's Dragonfly pod (MySQL pods and Services untouched) and pushes a
// reverter that removes it.
func (a *Actions) PartitionDragonflyPod(ctx context.Context, site string) error {
	if err := a.K.ApplyDragonflyPodPartition(ctx, a.Namespace, a.FG, site); err != nil {
		return fmt.Errorf("apply dragonfly partition for %s: %w", site, err)
	}
	name := pgkube.DragonflyPartitionPolicyName(site)
	a.push(fmt.Sprintf("remove dragonfly partition NetworkPolicy %s", name), func(ctx context.Context) error {
		return a.K.RemoveNetworkPolicy(ctx, a.Namespace, name)
	})
	return nil
}
