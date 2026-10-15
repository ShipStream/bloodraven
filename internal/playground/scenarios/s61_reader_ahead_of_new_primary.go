package scenarios

import (
	"context"
	"fmt"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	s61DB    = "chaos_s61"
	s61Table = "acked"
	// s61LagWindow is how long the candidate's replication link is cut
	// while the primary keeps committing, so the reader ends up ahead.
	s61LagWindow = 10 * time.Second
	// s61ConvergeBudget is how long the reader gets to serve from the
	// new primary after the failover.
	s61ConvergeBudget = 3 * time.Minute
)

func init() {
	runner.Register(scenario61ReaderAheadOfNewPrimary())
}

type s61State struct {
	topo   readerTopology
	policy string
	writer *ackedWriter
}

// scenario61ReaderAheadOfNewPrimary covers a reader that has applied
// more of the old primary's transactions than the candidate that gets
// promoted (chaos round 2026-10-15, F6). With asynchronous replication
// this is ordinary: the reader and the candidate replicate from the
// primary independently, and either can be further ahead when it dies.
//
// The scenario makes it deterministic by cutting only the candidate's
// replication link for s61LagWindow before scaling the primary to 0.
// Losing those writes is the documented asynchronous RPO; that is not
// what is asserted. What is asserted is the reader's recovery: today
// the transactions the new primary never had are errant on the reader,
// its source convergence stays Blocked, and its Service has no
// endpoints until someone reclones it. The hypothesis is the behaviour
// a fix must provide. Quarantined until the design decision on F6.
func scenario61ReaderAheadOfNewPrimary() runner.Scenario {
	st := &s61State{}
	return runner.Scenario{
		ID:    "61-reader-ahead-of-new-primary-recovers",
		Title: "A reader ahead of the promoted candidate returns to service without a manual reclone",
		Hypothesis: "When the reader has applied transactions the promoted candidate never received, the reader " +
			"converges on the new primary and serves again within " + s61ConvergeBudget.String() + ", without a " +
			"manual reclone.",
		Risk:           "medium",
		DocLink:        "playground/chaos-scenarios.md#61-reader-ahead-of-new-primary",
		Timeout:        15 * time.Minute,
		CleanupTimeout: 15 * time.Minute,
		Quarantine:     "known failure F6 (chaos round 2026-10-15): a reader ahead of the new primary keeps errant GTIDs and stays Blocked until recloned; pending a design decision on automatic reader recovery",
		Precheck: func(ctx context.Context, env *runner.Env) error {
			topo, err := resolveReaderTopology(ctx, env)
			if err != nil {
				return err
			}
			st.topo = topo
			return nil
		},
		Steps: []runner.Step{
			{
				Phase: runner.PhaseInject,
				Name:  fmt.Sprintf("write on the primary and cut only the candidate's replication link for %s", s61LagWindow),
				Do: func(ctx context.Context, env *runner.Env) error {
					var err error
					st.writer, err = startAckedWriter(ctx, env, st.topo.active, s61DB, s61Table, 100*time.Millisecond)
					if err != nil {
						return err
					}
					cidrs, err := clusterPodCIDRs(ctx, env)
					if err != nil {
						return err
					}
					// Replication dials the active site's Service, and the
					// policy's off-pod-network ipBlock would admit its
					// ClusterIP; exclude the active site's Services too.
					for _, svc := range []string{
						pgkube.MysqlDeploymentName(env.FG, st.topo.active) + "-internal",
						pgkube.MysqlDeploymentName(env.FG, st.topo.active),
					} {
						ip, err := env.Kube.ServiceClusterIP(ctx, env.Namespace, svc)
						if err != nil {
							return err
						}
						cidrs = append(cidrs, ip+"/32")
					}
					st.policy = "chaos-replication-cut-" + st.topo.standby
					np := &networkingv1.NetworkPolicy{
						ObjectMeta: metav1.ObjectMeta{Name: st.policy},
						Spec: networkingv1.NetworkPolicySpec{
							PodSelector: mysqlPodSelector(env.FG, st.topo.standby),
							PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
							Egress: []networkingv1.NetworkPolicyEgressRule{{
								To: peersExcept("shipstream.io/site", []string{st.topo.active}, cidrs),
							}},
						},
					}
					if err := env.Kube.ApplyChaosNetworkPolicy(ctx, env.Namespace, np); err != nil {
						return fmt.Errorf("apply %s: %w", st.policy, err)
					}
					// A NetworkPolicy filters new connections only; the
					// established replication stream would keep flowing.
					// Restart the IO thread so it must reconnect through the
					// policy.
					cand, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.topo.standby, env.Creds)
					if err != nil {
						return err
					}
					_, err = cand.Exec(ctx, "STOP REPLICA IO_THREAD; START REPLICA IO_THREAD")
					_ = cand.Close()
					if err != nil {
						return fmt.Errorf("restart IO thread on %s: %w", st.topo.standby, err)
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(s61LagWindow):
					}
					env.Capture.Note("before primary loss: " + st.writer.summary())
					return env.Chaos.ScaleSiteToZero(ctx, st.topo.active)
				},
			},
			{
				Phase: runner.PhaseObserve,
				Name:  "candidate promoted; restore its network",
				Do: func(ctx context.Context, env *runner.Env) error {
					st.writer.stop()
					waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
					defer cancel()
					if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "candidate promoted",
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							return mfg.Status.ActiveSite == st.topo.standby, "activeSite=" + mfg.Status.ActiveSite, nil
						}); err != nil {
						return err
					}
					if err := env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, st.policy); err != nil {
						return err
					}
					// The precondition: the reader holds transactions the new
					// primary never received. Without it the scenario proves
					// nothing.
					reader, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.topo.reader, env.Creds)
					if err != nil {
						return err
					}
					defer func() { _ = reader.Close() }()
					readerGtid, err := reader.GtidExecuted(ctx)
					if err != nil {
						return err
					}
					primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.topo.standby, env.Creds)
					if err != nil {
						return err
					}
					defer func() { _ = primary.Close() }()
					extra, err := primary.ScalarString(ctx, "SELECT GTID_SUBTRACT(?, @@GLOBAL.gtid_executed)", readerGtid)
					if err != nil {
						return err
					}
					if extra == "" {
						return fmt.Errorf("precondition not met: the reader holds nothing the new primary %s lacks "+
							"(the candidate's replication link was not cut)", st.topo.standby)
					}
					env.Capture.Note(fmt.Sprintf("reader ahead of new primary by %s", extra))
					return nil
				},
			},
			{
				Phase: runner.PhaseVerify,
				Name:  fmt.Sprintf("reader serves from the new primary within %s", s61ConvergeBudget),
				Do: func(ctx context.Context, env *runner.Env) error {
					newHost := playgroundInternalSiteHost(env.FG, st.topo.standby, env.Namespace)
					waitCtx, cancel := context.WithTimeout(ctx, s61ConvergeBudget)
					defer cancel()
					_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "reader converged on the new primary",
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							rs := statusSiteByName(mfg, st.topo.reader)
							if rs == nil {
								return false, "reader missing from status", nil
							}
							if err := assertReaderServingStatus(mfg, rs, newHost); err != nil {
								return false, fmt.Sprintf("%v (reason=%q)", err, rs.SourceConvergenceReason), nil
							}
							return true, "converged", nil
						})
					if err != nil {
						return fmt.Errorf("reader %s did not return to service after the failover: %w", st.topo.reader, err)
					}
					return waitReaderClientEndpoint(ctx, env, st.topo.reader, time.Minute)
				},
			},
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			st.writer.stop()
			if st.policy != "" {
				_ = env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, st.policy)
			}
			// The old primary comes back holding the writes the candidate
			// never received (the accepted RPO), so it is RecoveryBlocked
			// as well as, today, the reader.
			if err := env.Chaos.ScaleSiteToOne(ctx, st.topo.active); err != nil {
				return err
			}
			return repairDivergentSites(ctx, env, 12*time.Minute)
		},
	}
}
