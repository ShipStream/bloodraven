package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario33ScopedDNSOutage())
}

const (
	s33ID          = "33-scoped-dns-outage"
	s33CanaryPod   = "chaos-s33-dns-canary"
	s33CanaryLabel = "chaos-dns-canary"
	s33CanaryNP    = "chaos-s33-dns-canary-deny"
)

// scenario33ScopedDNSOutage applies a NetworkPolicy that blocks only kube-dns
// egress from the active MySQL pod — a scoped cluster-DNS outage rather than a
// pod/network partition. The deny rule excepts both the kube-dns Service
// ClusterIP and its backend pod IPs, since CNIs differ on whether
// NetworkPolicy is enforced before or after Service DNAT (excepting only the
// ClusterIP silently fails to block DNS on a CNI that enforces post-DNAT).
// Before touching the real cluster it runs a disposable canary pod to prove
// the CNI actually enforces the exception with the real pod IPs discovered
// live — the scenario must not silently pass if it does not. It then asserts
// the MySQL writability invariant holds and that this is a DNS-resolution
// outage, not the DNSEndpoint-API-write denial of scenario 38.
func scenario33ScopedDNSOutage() runner.Scenario {
	return runner.Scenario{
		ID:    s33ID,
		Title: "Scoped cluster-DNS outage on active site — DNS-only, not a partition",
		Hypothesis: "A NetworkPolicy that blocks only kube-dns egress from the active MySQL pod (allowing all " +
			"other egress) is a DNS-resolution outage: the operator's DNSEndpoint API writes still succeed, no " +
			"split-brain occurs, and MySQL either stays stable or the sidecar self-fences cleanly — but there is " +
			"no forbidden DNSEndpoint apply (that is scenario 38).",
		Risk:              "medium",
		DocLink:           "playground/chaos-scenarios.md#33-scoped-cluster-dns-outage",
		Timeout:           6 * time.Minute,
		ResetBeforeRunAll: false,
		Precheck:          AssertHealthyBaseline,
		Steps: []runner.Step{
			s33Canary(),
			s33InjectDNSDeny(),
			s33ObserveDNSScoped(),
			s33RemovePolicyAndRecover(),
		},
	}
}

// s33Canary proves the policy shape blocks DNS on this CNI before applying it
// to the real active pod.
func s33Canary() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "canary proves the DNS-deny policy blocks resolution on this CNI",
		Do: func(ctx context.Context, env *runner.Env) error {
			kubeDNSIP, err := env.Kube.DiscoverKubeDNSClusterIP(ctx)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "kubeDNSIP", kubeDNSIP); err != nil {
				return err
			}
			podIPs, err := env.Kube.DiscoverKubeDNSEndpointIPs(ctx)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "kubeDNSPodIPs", strings.Join(podIPs, ",")); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("kube-dns ClusterIP = %s, backend pod IPs = %v", kubeDNSIP, podIPs))
			denyIPs := append([]string{kubeDNSIP}, podIPs...)

			// A busybox pod that logs a DNS probe every 2s forever.
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      s33CanaryPod,
					Namespace: env.Namespace,
					Labels:    map[string]string{"app": s33CanaryLabel},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "probe",
						Image:           "busybox:1.36",
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command: []string{"sh", "-c",
							"while true; do if nslookup kubernetes.default.svc.cluster.local >/dev/null 2>&1; then echo PROBE dns=ok; else echo PROBE dns=fail; fi; sleep 2; done"},
					}},
				},
			}
			if err := env.Chaos.CreateCanaryPod(ctx, env.Namespace, pod); err != nil {
				return fmt.Errorf("create DNS canary pod: %w", err)
			}

			if err := s33WaitCanaryProbe(ctx, env, "ok", 60*time.Second); err != nil {
				return fmt.Errorf("canary never resolved DNS before the policy (env problem): %w", err)
			}
			env.Capture.Note("canary baseline: DNS resolves (dns=ok)")

			np := pgkube.BuildDNSEgressDenyPolicyForSelector(s33CanaryNP, map[string]string{"app": s33CanaryLabel}, denyIPs)
			if err := env.Kube.ApplyChaosNetworkPolicy(ctx, env.Namespace, np); err != nil {
				return fmt.Errorf("apply canary DNS-deny policy: %w", err)
			}
			// Remove the canary policy regardless of outcome; the canary pod is
			// torn down by CreateCanaryPod's reverter at cleanup.
			defer func() { _ = env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, s33CanaryNP) }()

			if err := s33WaitCanaryProbe(ctx, env, "fail", 45*time.Second); err != nil {
				return fmt.Errorf("canary DNS still resolved after excepting both the kube-dns ClusterIP and its %d backend pod IP(s) — this CNI does not enforce egress NetworkPolicy for DNS traffic via either pre- or post-DNAT destination; investigate CNI-level policy support before retrying this scenario: %w", len(podIPs), err)
			}
			env.Capture.Note("canary proved the DNS-deny policy blocks resolution (dns=fail); safe to apply to the active pod")
			return nil
		},
	}
}

// s33WaitCanaryProbe polls the canary pod's most recent PROBE line until it
// reports the wanted state ("ok" or "fail").
func s33WaitCanaryProbe(ctx context.Context, env *runner.Env, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := "no probe line yet"
	for {
		body, err := env.Kube.PodLogTailLines(ctx, env.Namespace, s33CanaryPod, "probe", 8)
		if err == nil {
			if state := lastProbeState(string(body)); state != "" {
				last = "dns=" + state
				if state == want {
					return nil
				}
			}
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("canary probe did not reach dns=%s within %s (last: %s)", want, timeout, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// lastProbeState returns "ok"/"fail" from the most recent "PROBE dns=<state>"
// line, or "" if none is present.
func lastProbeState(logs string) string {
	lines := strings.Split(strings.TrimSpace(logs), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if strings.HasPrefix(l, "PROBE dns=") {
			return strings.TrimPrefix(l, "PROBE dns=")
		}
	}
	return ""
}

// splitStashedIPs reverses the comma-join used to stash a []string of IPs
// through ctxStash/ctxFetch (which only carry strings). Empty input yields
// an empty (nil) slice rather than [""].
func splitStashedIPs(joined string) []string {
	if joined == "" {
		return nil
	}
	return strings.Split(joined, ",")
}

func s33InjectDNSDeny() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "block kube-dns egress from the active MySQL pod",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			if err := ctxStash(ctx, env, "activeSite", active); err != nil {
				return err
			}
			kubeDNSIP := ctxFetch(env, "kubeDNSIP")
			podIPs := splitStashedIPs(ctxFetch(env, "kubeDNSPodIPs"))
			denyIPs := append([]string{kubeDNSIP}, podIPs...)
			// Open tailers before the policy lands so their SinceTime windows
			// cover the whole outage.
			if _, err := env.Logs("sidecar:" + active); err != nil {
				env.Capture.Note("open sidecar tailer failed: " + err.Error())
			}
			if _, err := env.Logs("operator"); err != nil {
				env.Capture.Note("open operator tailer failed: " + err.Error())
			}
			env.Capture.Note(fmt.Sprintf("applying scoped DNS-deny to active site %s (kube-dns ClusterIP %s and %d backend pod IP(s) blocked)", active, kubeDNSIP, len(podIPs)))
			return env.Chaos.DenyDNSEgress(ctx, active, denyIPs)
		},
	}
}

func s33ObserveDNSScoped() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "MySQL invariant holds; outage is DNS-scoped (no forbidden DNSEndpoint apply)",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeSite")
			// Hold the outage across >2x leaseTimeout (20s) while asserting the
			// safety invariant every poll: never more than one writable site,
			// never RecoveryBlocked. Record whether a clean failover occurred.
			holdUntil := time.Now().Add(45 * time.Second)
			var sawFailover bool
			for {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				writable := 0
				for _, s := range mfg.Status.Sites {
					if s.State == "writable" {
						writable++
					}
					if s.RecoveryState == "RecoveryBlocked" {
						return fmt.Errorf("site %s RecoveryBlocked during scoped DNS outage", s.Name)
					}
				}
				if writable > 1 {
					return fmt.Errorf("split-brain during scoped DNS outage: %d writable sites", writable)
				}
				if mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != active {
					sawFailover = true
				}
				if time.Now().After(holdUntil) {
					env.Capture.Note(fmt.Sprintf("held DNS outage; activeSite=%q failoverObserved=%v (both stable and clean-failover are acceptable)", mfg.Status.ActiveSite, sawFailover))
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(3 * time.Second):
				}
			}

			// This must NOT be the DNSEndpoint-API-write denial of scenario 38:
			// the operator's DNS writes still succeed, so no "apply DNSEndpoint
			// ... forbidden" line should appear, and the DNSEndpoint stays
			// readable through the Kubernetes API.
			if tail, err := env.Logs("operator"); err == nil {
				if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("apply DNSEndpoint")); hit && strings.Contains(line, "forbidden") {
					return fmt.Errorf("unexpected forbidden DNSEndpoint apply during a DNS-resolution outage (that is scenario 38): %s", line)
				}
			}
			if _, found, err := env.Kube.GetDNSEndpointTargets(ctx, env.Namespace, env.FG); err != nil {
				return fmt.Errorf("DNSEndpoint should stay readable via the API during a DNS-resolution outage: %w", err)
			} else if !found {
				env.Capture.Note("note: DNSEndpoint object not present (playground may not seed one); API path still reachable")
			}

			// Best-effort evidence that the sidecar saw DNS resolution failures.
			if tail, err := env.Logs("sidecar:" + active); err == nil {
				if hit, line := firstMatchSince(tail, env.StartTime, s33DNSFailurePredicate()); hit {
					env.Capture.Note("sidecar DNS-resolution failure observed: " + line)
				} else {
					env.Capture.Note("no explicit sidecar DNS-resolution error captured (sidecar may tolerate DNS loss via cached lookups/existing connections)")
				}
			}
			return nil
		},
	}
}

// s33DNSFailurePredicate matches the common Go/resolver DNS-failure markers a
// sidecar emits when it cannot resolve peer/operator hostnames.
func s33DNSFailurePredicate() pglogs.Predicate {
	markers := []string{"lookup ", "no such host", "name resolution", "server misbehaving", "svc.cluster.local"}
	return func(line string) bool {
		for _, m := range markers {
			if strings.Contains(line, m) {
				return true
			}
		}
		return false
	}
}

func s33RemovePolicyAndRecover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "remove DNS policy; cluster returns to Ready with replication healthy",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeSite")
			if err := env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, pgkube.DNSEgressDenyPolicyName(active)); err != nil {
				return fmt.Errorf("remove DNS-deny policy: %w", err)
			}
			env.Capture.Note("removed scoped DNS-deny policy; awaiting recovery")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "Ready=True with one writable and replicating peer",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					writable, replicatingPeers := 0, 0
					for _, s := range mfg.Status.Sites {
						if s.State == "writable" {
							writable++
						}
						if s.RecoveryState == "RecoveryBlocked" {
							return false, "RecoveryBlocked on " + s.Name, fmt.Errorf("site %s RecoveryBlocked after policy removal", s.Name)
						}
						if s.Name != mfg.Status.ActiveSite && s.Replicating {
							replicatingPeers++
						}
					}
					msg := fmt.Sprintf("ready=%s active=%q writable=%d replicatingPeers=%d", readyOf(mfg), mfg.Status.ActiveSite, writable, replicatingPeers)
					done := readyOf(mfg) == "True" && writable == 1 && replicatingPeers >= 1
					return done, msg, nil
				})
			return err
		},
	}
}
