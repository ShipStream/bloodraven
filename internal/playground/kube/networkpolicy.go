package kube

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ChaosNetworkPolicyLabelValue is the value of the "app" label stamped on
// every chaos NetworkPolicy so RemoveAllChaosNetworkPolicies (and therefore
// GlobalRecover) sweep them even if a scenario's own reverter did not run.
// Matches the existing ApplyDenyAllNetworkPolicy convention.
const ChaosNetworkPolicyLabelValue = "chaos-partition"

// ApplyChaosNetworkPolicy creates a NetworkPolicy, stamping the
// app=chaos-partition label so the standard recover sweep removes it.
// Idempotent: an already-existing policy of the same name is treated as
// applied. The caller owns the NP name for a matching RemoveNetworkPolicy.
func (c *Client) ApplyChaosNetworkPolicy(ctx context.Context, namespace string, np *networkingv1.NetworkPolicy) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	if np.Labels == nil {
		np.Labels = map[string]string{}
	}
	np.Labels["app"] = ChaosNetworkPolicyLabelValue
	np.Namespace = namespace
	_, err := c.Kubernetes.NetworkingV1().NetworkPolicies(namespace).Create(ctx, np, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// RemoveNetworkPolicy deletes a NetworkPolicy by name. Idempotent.
func (c *Client) RemoveNetworkPolicy(ctx context.Context, namespace, name string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	err := c.Kubernetes.NetworkingV1().NetworkPolicies(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// DiscoverKubeDNSClusterIP reads the kube-system/kube-dns Service ClusterIP so
// the scoped-DNS-outage scenario can carve exactly that IP out of an egress
// allow rule without hard-coding 10.43.0.10.
func (c *Client) DiscoverKubeDNSClusterIP(ctx context.Context) (string, error) {
	svc, err := c.Kubernetes.CoreV1().Services("kube-system").Get(ctx, "kube-dns", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get kube-system/kube-dns service: %w", err)
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == corev1.ClusterIPNone {
		return "", fmt.Errorf("kube-dns service has no ClusterIP (got %q)", svc.Spec.ClusterIP)
	}
	return svc.Spec.ClusterIP, nil
}

// DiscoverKubeDNSEndpointIPs reads the kube-system/kube-dns Endpoints object
// and returns the CoreDNS backend pod IPs. Some CNIs enforce NetworkPolicy
// after kube-proxy's Service DNAT, so a rule that excepts only the kube-dns
// ClusterIP never matches the post-DNAT packet and silently fails to block
// DNS (the canary in scenario 33 proves this per-CNI). Excepting the actual
// backend pod IPs is the documented fallback: it works regardless of whether
// the CNI enforces policy before or after DNAT.
func (c *Client) DiscoverKubeDNSEndpointIPs(ctx context.Context) ([]string, error) {
	ep, err := c.Kubernetes.CoreV1().Endpoints("kube-system").Get(ctx, "kube-dns", metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get kube-system/kube-dns endpoints: %w", err)
	}
	var ips []string
	for _, sub := range ep.Subsets {
		for _, addr := range sub.Addresses {
			ips = append(ips, addr.IP)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("kube-system/kube-dns endpoints has no backing pod addresses")
	}
	return ips, nil
}

// DNSEgressDenyPolicyName is the NetworkPolicy name for a site's scoped DNS
// outage.
func DNSEgressDenyPolicyName(site string) string {
	return fmt.Sprintf("chaos-dns-deny-%s", site)
}

// BuildDNSEgressDenyPolicy returns a NetworkPolicy that selects only the
// site's MySQL pod and blocks egress to the given deny IPs (all ports, which
// includes 53/TCP and 53/UDP) while allowing every other egress. This is a
// scoped cluster-DNS outage: the sidecar can no longer resolve
// *.svc.cluster.local, but direct-IP paths (API server, peer pods) stay up.
//
// denyIPs should include both the kube-dns Service ClusterIP and its backend
// pod IPs (see DiscoverKubeDNSClusterIP / DiscoverKubeDNSEndpointIPs): some
// CNIs enforce NetworkPolicy before Service DNAT (packet destination is still
// the ClusterIP) and others enforce it after (destination is already a
// backend pod IP). Excepting both makes the policy work either way instead of
// silently allowing DNS through — proven per-CNI by the scenario 33 canary.
//
// It is a positive allow-rule to 0.0.0.0/0 with the deny IPs carved out via
// IPBlock.Except; traffic to those IPs matches no allow rule and is therefore
// denied.
func BuildDNSEgressDenyPolicy(fg, site string, denyIPs []string) *networkingv1.NetworkPolicy {
	return BuildDNSEgressDenyPolicyForSelector(DNSEgressDenyPolicyName(site), map[string]string{
		"app.kubernetes.io/name":     "mysql",
		"app.kubernetes.io/instance": fg,
		"shipstream.io/site":         site,
	}, denyIPs)
}

// BuildDNSEgressDenyPolicyForSelector is the general form of
// BuildDNSEgressDenyPolicy: it selects pods by arbitrary labels and blocks
// egress to an arbitrary set of deny IPs. Scenario 33 uses it both for a
// disposable DNS canary pod (to prove the CNI actually enforces the exception
// before touching the real cluster) and, through BuildDNSEgressDenyPolicy,
// for the active MySQL pod. Duplicate and empty entries in denyIPs are
// dropped.
func BuildDNSEgressDenyPolicyForSelector(name string, selector map[string]string, denyIPs []string) *networkingv1.NetworkPolicy {
	except := make([]string, 0, len(denyIPs))
	seen := make(map[string]bool, len(denyIPs))
	for _, ip := range denyIPs {
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true
		except = append(except, ip+"/32")
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"app": ChaosNetworkPolicyLabelValue},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selector},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							IPBlock: &networkingv1.IPBlock{
								CIDR:   "0.0.0.0/0",
								Except: except,
							},
						},
					},
				},
			},
		},
	}
}

// ApplyDNSEgressDenyNetworkPolicy builds and applies BuildDNSEgressDenyPolicy.
func (c *Client) ApplyDNSEgressDenyNetworkPolicy(ctx context.Context, namespace, fg, site string, denyIPs []string) error {
	return c.ApplyChaosNetworkPolicy(ctx, namespace, BuildDNSEgressDenyPolicy(fg, site, denyIPs))
}

// DragonflyPartitionPolicyName is the NetworkPolicy name for a site's
// Dragonfly master partition.
func DragonflyPartitionPolicyName(site string) string {
	return fmt.Sprintf("chaos-df-partition-%s", site)
}

// BuildDragonflyPartitionPolicy returns a deny-all (ingress+egress)
// NetworkPolicy selecting only the site's Dragonfly pod. MySQL pods and
// Services are untouched, so a Dragonfly-only partition can be proven not to
// move the MySQL active site.
func BuildDragonflyPartitionPolicy(fg, site string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:   DragonflyPartitionPolicyName(site),
			Labels: map[string]string{"app": ChaosNetworkPolicyLabelValue},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":     "dragonfly",
					"app.kubernetes.io/instance": fg,
					"shipstream.io/site":         site,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{},
			Egress:  []networkingv1.NetworkPolicyEgressRule{},
		},
	}
}

// ApplyDragonflyPodPartition builds and applies BuildDragonflyPartitionPolicy.
func (c *Client) ApplyDragonflyPodPartition(ctx context.Context, namespace, fg, site string) error {
	return c.ApplyChaosNetworkPolicy(ctx, namespace, BuildDragonflyPartitionPolicy(fg, site))
}

// StandbyIngressHoldPolicyName is the NetworkPolicy name for the ordered-update
// standby hold.
func StandbyIngressHoldPolicyName(site string) string {
	return fmt.Sprintf("chaos-standby-hold-%s", site)
}

// BuildStandbyIngressHoldPolicy returns a deny-all-INGRESS NetworkPolicy on a
// site's MySQL pod. Scenario 34 applies it to the standby AFTER the ordered
// update has started, so the operator's health check of the standby fails and
// status.updatePhase holds at WaitReplica long enough to kill the operator.
// Only ingress is denied: the standby's OUTBOUND replication to the primary is
// unaffected (so it stays a valid replica at the DB layer), which keeps the
// updater's "replicating standby" precondition satisfiable once the hold is
// lifted.
func BuildStandbyIngressHoldPolicy(fg, site string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:   StandbyIngressHoldPolicyName(site),
			Labels: map[string]string{"app": ChaosNetworkPolicyLabelValue},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":     "mysql",
					"app.kubernetes.io/instance": fg,
					"shipstream.io/site":         site,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{},
		},
	}
}
