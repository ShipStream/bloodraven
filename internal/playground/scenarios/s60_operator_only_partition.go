package scenarios

import (
	"context"
	"fmt"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	s60DB    = "chaos_s60"
	s60Table = "acked"
	// s60PartitionHold covers detection, any promotion, and the sidecar
	// lease timeout, so whatever the operator decides has happened by
	// the time the partition heals.
	s60PartitionHold = 60 * time.Second
)

func init() {
	runner.Register(scenario60OperatorOnlyPartition())
}

type s60State struct {
	primary string
	policy  string
	writer  *ackedWriter
}

// scenario60OperatorOnlyPartition cuts only the operator's path to the
// primary (chaos round 2026-10-15, F5 and F7). Clients, the replicas'
// IO threads and the peer sidecars still reach it, so the primary is
// alive and still committing acknowledged writes.
//
// What happens today: the operator sees the primary as unreachable and
// promotes the candidate, although the candidate's IO thread is still
// streaming from that primary, which proves it is alive. The old
// primary's sidecar learns the new active site and fences, but every
// write it acknowledged between the promotion GTID and the fence is lost,
// and the reader, still replicating from the old primary, holds them as
// errant transactions. In the original run that was 66 acknowledged
// writes, with the old primary and the reader both left needing a
// reclone. Clients that reconnected to the old primary through the
// write Service then got super_read_only errors for 2+ minutes, because
// the emergency path does not strip its role label at failover and the
// sidecar's eviction ran once.
//
// The hypothesis is the property a fix must restore: no acknowledged
// write is lost. The scenario is quarantined until the design decision
// on F5 is made (a replica-witness check before declaring the primary
// dead is the leading option).
func scenario60OperatorOnlyPartition() runner.Scenario {
	st := &s60State{}
	return runner.Scenario{
		ID:    "60-operator-only-partition-no-acked-loss",
		Title: "Operator-only partition of the primary loses no acknowledged write",
		Hypothesis: "With only operator->primary traffic blocked for " + s60PartitionHold.String() + ", while clients " +
			"and replication still reach the primary, every write the primary acknowledged is present on the " +
			"active site after the partition heals, and no site is left RecoveryBlocked.",
		Risk:           "high",
		DocLink:        "playground/chaos-scenarios.md#60-operator-only-partition",
		Timeout:        15 * time.Minute,
		CleanupTimeout: 15 * time.Minute,
		Quarantine:     "known failure F5 (chaos round 2026-10-15): promotion while replicas still stream from the primary loses acknowledged writes; pending a design decision on replica-witness failure detection",
		Precheck:       AssertHealthyBaseline,
		Steps: []runner.Step{
			{
				Phase: runner.PhaseInject,
				Name:  "start acknowledged writes on the primary, then block operator->primary only",
				Do: func(ctx context.Context, env *runner.Env) error {
					mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					st.primary = mfg.Status.ActiveSite
					// The writer's port-forward goes through the kubelet,
					// which a NetworkPolicy does not filter: it plays a
					// client that can still reach the primary.
					st.writer, err = startAckedWriter(ctx, env, st.primary, s60DB, s60Table, 100*time.Millisecond)
					if err != nil {
						return err
					}
					cidrs, err := clusterPodCIDRs(ctx, env)
					if err != nil {
						return err
					}
					st.policy = "chaos-operator-cut-" + st.primary
					np := &networkingv1.NetworkPolicy{
						ObjectMeta: metav1.ObjectMeta{Name: st.policy},
						Spec: networkingv1.NetworkPolicySpec{
							PodSelector: mysqlPodSelector(env.FG, st.primary),
							PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
							Ingress: []networkingv1.NetworkPolicyIngressRule{{
								From: peersExcept("app.kubernetes.io/name", []string{"bloodraven"}, cidrs),
							}},
						},
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(5 * time.Second):
					}
					if err := env.Kube.ApplyChaosNetworkPolicy(ctx, env.Namespace, np); err != nil {
						return fmt.Errorf("apply %s: %w", st.policy, err)
					}
					env.Capture.Note(fmt.Sprintf("operator cut from primary %s at %s; %s",
						st.primary, time.Now().Format(time.RFC3339), st.writer.summary()))
					return nil
				},
			},
			{
				Phase: runner.PhaseObserve,
				Name:  "hold the partition",
				Do: func(ctx context.Context, env *runner.Env) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(s60PartitionHold):
					}
					mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					env.Capture.Note(fmt.Sprintf("during partition: activeSite=%q lastFailoverTarget=%q; %s",
						mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, st.writer.summary()))
					return nil
				},
			},
			{
				Phase: runner.PhaseVerify,
				Name:  "heal, then every acknowledged write is on the active site and nothing is blocked",
				Do: func(ctx context.Context, env *runner.Env) error {
					if err := env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, st.policy); err != nil {
						return err
					}
					st.writer.stop()
					acked := st.writer.ackedIDs()
					env.Capture.Note("after heal: " + st.writer.summary())

					// Let recovery run; a fixed operator settles quickly,
					// the current one settles into RecoveryBlocked.
					waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
					mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "group settles after heal",
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							var blocked []string
							for _, s := range mfg.Status.Sites {
								if s.RecoveryState == "RecoveryBlocked" || s.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked {
									blocked = append(blocked, s.Name)
								}
							}
							if len(blocked) > 0 {
								return true, fmt.Sprintf("blocked=%v", blocked), nil
							}
							return AssertHealthyBaseline(ctx, env) == nil, "activeSite=" + mfg.Status.ActiveSite, nil
						})
					cancel()
					if err != nil {
						return err
					}
					missing, err := missingAckedIDs(ctx, env, mfg.Status.ActiveSite, s60DB+"."+s60Table, acked)
					if err != nil {
						return err
					}
					var blocked []string
					for _, s := range mfg.Status.Sites {
						if s.RecoveryState == "RecoveryBlocked" || s.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked {
							blocked = append(blocked, s.Name)
						}
					}
					if len(missing) > 0 || len(blocked) > 0 {
						return fmt.Errorf("operator-only partition of %s: %d of %d acknowledged writes missing on active site %s %s; blocked sites %v",
							st.primary, len(missing), len(acked), mfg.Status.ActiveSite, formatIDs(missing), blocked)
					}
					return nil
				},
			},
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			st.writer.stop()
			if st.policy != "" {
				_ = env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, st.policy)
			}
			return repairDivergentSites(ctx, env, 10*time.Minute)
		},
	}
}
