package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario38DNSEndpointWriteDenial())
}

const s38ID = "38-dnsendpoint-write-denial-during-failover"

// scenario38DNSEndpointWriteDenial denies the operator create/patch/update/
// delete on externaldns.k8s.io/dnsendpoints (keeping get/list/watch), then
// forces an emergency promotion. MySQL promotion and CR status must proceed
// normally (status writes are NOT denied), but the DNSEndpoint target stays
// stale because the operator's apply is forbidden. After the ClusterRole is
// restored the DNSEndpoint must flip to the promoted site's LBIP within 90s
// via the operator's per-poll DNS retry — with no second MySQL failover.
func scenario38DNSEndpointWriteDenial() runner.Scenario {
	var (
		activeSite  string
		peerSite    string
		peerLBIP    string
		dnsBefore   []string
		restoreRBAC func(context.Context) error
	)

	return runner.Scenario{
		ID:    s38ID,
		Title: "DNSEndpoint write denial during failover heals after RBAC restore without re-failover",
		Hypothesis: "Denying the operator write verbs on dnsendpoints then scaling the active primary to 0 " +
			"still promotes the peer (status.activeSite and lastFailoverTarget flip) while the DNSEndpoint target " +
			"stays stale. After RBAC is restored the DNSEndpoint catches up to the promoted site's LBIP within " +
			"90s without another failover.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#38-dnsendpoint-write-denial-during-failover",
		Timeout:           10 * time.Minute,
		ResetBeforeRunAll: false,
		Precheck:          precheckHealthyWithOperatorRole,
		Steps: []runner.Step{
			{
				Phase: runner.PhaseInject,
				Name:  "capture DNS target, deny dnsendpoints writes, scale active primary to 0",
				Do: func(ctx context.Context, env *runner.Env) error {
					mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					activeSite = mfg.Status.ActiveSite
					peerSite, err = PeerOf(mfg, activeSite)
					if err != nil {
						return err
					}
					peerLBIP = siteLBIP(mfg, peerSite)
					if peerLBIP == "" {
						return fmt.Errorf("peer site %s has no spec.lbIP", peerSite)
					}

					targets, found, err := env.Kube.GetDNSEndpointTargets(ctx, env.Namespace, env.FG)
					if err != nil {
						return fmt.Errorf("read DNSEndpoint targets: %w", err)
					}
					if !found {
						return fmt.Errorf("DNSEndpoint %s not found; the playground must seed it", pgDNSEndpointName(env.FG))
					}
					dnsBefore = targets
					activeLBIP := siteLBIP(mfg, activeSite)
					env.Capture.Note(fmt.Sprintf("pre-injection: activeSite=%s (lbIP=%s) peer=%s (lbIP=%s) DNSEndpoint targets=%v",
						activeSite, activeLBIP, peerSite, peerLBIP, dnsBefore))
					if stringsContain(dnsBefore, peerLBIP) {
						return fmt.Errorf("DNSEndpoint already points at peer LBIP %s before injection; cannot prove a flip", peerLBIP)
					}

					if _, err := env.Logs("operator"); err != nil {
						env.Capture.Note("open operator tailer failed: " + err.Error())
					}

					res, restore, err := env.Chaos.DenyOperatorClusterRoleVerbs(ctx, env.Namespace, operatorServiceAccount,
						[]pgchaos.VerbDenial{{APIGroup: "externaldns.k8s.io", Resource: "dnsendpoints", Verbs: []string{"create", "patch", "update", "delete"}}})
					if err != nil {
						return fmt.Errorf("deny dnsendpoints writes: %w", err)
					}
					restoreRBAC = restore
					_ = env.Capture.WriteFile("s38-clusterrole-original.json", []byte(res.OriginalRules))
					_ = env.Capture.WriteFile("s38-clusterrole-patched.json", []byte(res.PatchedRules))
					env.Capture.Note(fmt.Sprintf("denied create/patch/update/delete on dnsendpoints on ClusterRole %s (binding %s); kept get/list/watch", res.RoleName, res.BindingName))

					env.Capture.Note(fmt.Sprintf("scaling active primary %s to 0 to force emergency promotion", activeSite))
					return env.Chaos.ScaleSiteToZero(ctx, activeSite)
				},
			},
			{
				Phase: runner.PhaseObserve,
				Name:  "peer promoted (status flips) but DNSEndpoint target stays stale",
				Do: func(ctx context.Context, env *runner.Env) error {
					// Status writes are NOT denied here, so the CR reflects the
					// promotion normally.
					waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
					defer cancel()
					_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
						fmt.Sprintf("status.activeSite==%s and lastFailoverTarget==%s", peerSite, peerSite),
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							msg := fmt.Sprintf("activeSite=%q lastFailoverTarget=%q ready=%s", mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, readyOf(mfg))
							if mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != peerSite && mfg.Status.ActiveSite != activeSite {
								return false, msg, fmt.Errorf("unexpected activeSite %q (want %q)", mfg.Status.ActiveSite, peerSite)
							}
							return mfg.Status.ActiveSite == peerSite && mfg.Status.LastFailoverTarget == peerSite, msg, nil
						})
					if err != nil {
						return fmt.Errorf("status did not flip to promoted peer: %w", err)
					}

					// The DNSEndpoint apply is forbidden, so the target must not
					// have advanced to the peer LBIP. Confirm over a short dwell
					// so a slow apply is not mistaken for a stale target.
					dwell := time.Now().Add(20 * time.Second)
					for {
						targets, _, err := env.Kube.GetDNSEndpointTargets(ctx, env.Namespace, env.FG)
						if err != nil {
							return fmt.Errorf("read DNSEndpoint targets during denial: %w", err)
						}
						if stringsContain(targets, peerLBIP) {
							return fmt.Errorf("DNSEndpoint advanced to peer LBIP %s while writes were denied (targets=%v)", peerLBIP, targets)
						}
						if time.Now().After(dwell) {
							env.Capture.Note(fmt.Sprintf("DNSEndpoint target stayed stale during denial: %v (peer LBIP %s absent)", targets, peerLBIP))
							break
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(3 * time.Second):
						}
					}

					if tail, err := env.Logs("operator"); err == nil {
						if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("DNS flip failed after successful promotion")); hit {
							env.Capture.Note("observed denied DNS flip in operator log: " + line)
						} else if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("apply DNSEndpoint")); hit {
							env.Capture.Note("observed forbidden DNSEndpoint apply in operator log: " + line)
						}
					}
					return nil
				},
			},
			{
				Phase: runner.PhaseVerify,
				Name:  "restore RBAC; DNSEndpoint heals to promoted LBIP within 90s (no re-failover)",
				Do: func(ctx context.Context, env *runner.Env) error {
					if restoreRBAC == nil {
						return fmt.Errorf("internal: restoreRBAC closure not set")
					}
					// Record lastFailoverTarget so we can prove no second failover.
					mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					lastFailoverBefore := mfg.Status.LastFailover
					if err := restoreRBAC(ctx); err != nil {
						return fmt.Errorf("restore ClusterRole rules: %w", err)
					}
					env.Capture.Note("restored dnsendpoints write verbs; awaiting DNSEndpoint heal")

					waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
					defer cancel()
					deadline := time.Now().Add(90 * time.Second)
					var lastTargets []string
					for {
						targets, _, err := env.Kube.GetDNSEndpointTargets(ctx, env.Namespace, env.FG)
						if err == nil {
							lastTargets = targets
							if stringsContain(targets, peerLBIP) {
								env.Capture.Note(fmt.Sprintf("DNSEndpoint healed to promoted LBIP %s (targets=%v)", peerLBIP, targets))
								break
							}
						}
						if time.Now().After(deadline) {
							return fmt.Errorf("DNSEndpoint did not heal to peer LBIP %s within 90s (last targets=%v)", peerLBIP, lastTargets)
						}
						select {
						case <-waitCtx.Done():
							return fmt.Errorf("DNSEndpoint heal wait cancelled: %w (last targets=%v)", waitCtx.Err(), lastTargets)
						case <-time.After(3 * time.Second):
						}
					}

					// No second MySQL failover: activeSite and lastFailover unchanged.
					after, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					if after.Status.ActiveSite != peerSite || after.Status.LastFailoverTarget != peerSite {
						return fmt.Errorf("activeSite/lastFailoverTarget changed during DNS heal (active=%q target=%q) — DNS heal must not re-run failover",
							after.Status.ActiveSite, after.Status.LastFailoverTarget)
					}
					if lastFailoverBefore != nil && after.Status.LastFailover != nil && !after.Status.LastFailover.Time.Equal(lastFailoverBefore.Time) {
						return fmt.Errorf("lastFailover advanced during DNS heal (%v -> %v) — a second failover occurred", lastFailoverBefore.Time, after.Status.LastFailover.Time)
					}
					return nil
				},
			},
			{
				Phase: runner.PhaseSettle,
				Name:  "scale old primary back up; verify DNS matches final active site",
				Do: func(ctx context.Context, env *runner.Env) error {
					if err := env.Chaos.ScaleSiteToOne(ctx, activeSite); err != nil {
						return fmt.Errorf("scale old primary %s back to 1: %w", activeSite, err)
					}
					waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
					defer cancel()
					_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
						fmt.Sprintf("old primary %s rejoins without RecoveryBlocked", activeSite),
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							var old *v1alpha1.SiteStatus
							for i := range mfg.Status.Sites {
								if mfg.Status.Sites[i].Name == activeSite {
									old = &mfg.Status.Sites[i]
								}
							}
							if old == nil {
								return false, "old primary not in status yet", nil
							}
							msg := fmt.Sprintf("old=%s state=%s replicating=%v recovery=%q ready=%s", old.Name, old.State, old.Replicating, old.RecoveryState, readyOf(mfg))
							if old.RecoveryState == "RecoveryBlocked" {
								return false, msg, fmt.Errorf("old primary %s RecoveryBlocked", activeSite)
							}
							healthyReplica := old.State == "read-only" && old.Replicating && readyOf(mfg) == "True"
							return healthyReplica || old.RecoveryState == "RecoveryInProgress", msg, nil
						})
					if err != nil {
						return err
					}
					targets, _, err := env.Kube.GetDNSEndpointTargets(ctx, env.Namespace, env.FG)
					if err != nil {
						return fmt.Errorf("read final DNSEndpoint targets: %w", err)
					}
					if !stringsContain(targets, peerLBIP) {
						return fmt.Errorf("final DNSEndpoint targets %v do not include the active site LBIP %s", targets, peerLBIP)
					}
					env.Capture.Note(fmt.Sprintf("final DNSEndpoint targets=%v match active site %s LBIP %s", targets, peerSite, peerLBIP))
					return nil
				},
			},
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			if restoreRBAC != nil {
				if err := restoreRBAC(ctx); err != nil {
					env.Capture.Note("cleanup: restore RBAC: " + err.Error())
				}
			}
			return nil
		},
	}
}

// pgDNSEndpointName mirrors kube.DNSEndpointName for error messages without
// re-importing under a second alias.
func pgDNSEndpointName(fg string) string { return "bloodraven-" + fg }
