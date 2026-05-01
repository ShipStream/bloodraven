package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario22ReplicationStatusAfterRecovery())
}

// scenario22ReplicationStatusAfterRecovery is the regression test the
// project wishlist asks for in WISHLIST.md item #36. The bug: after
// any operator-driven recovery (emergency failover + auto-recover,
// auto-fail-back, "no GTID divergence" rejoin), the CR's
// status.sites[].replicating field for the post-recovery read-only
// site stops being populated and stays at its zero value for the rest
// of the operator lifecycle, even though the sidecar /status endpoint
// correctly reports replica_io_running && replica_sql_running. Only
// an operator restart re-populates the field.
//
// This regression matters operationally because
// internal/controller/planned_failover.go reads
// `targetStatus.Replicating` directly as a safety check, so any
// planned switchover after the first chaos event in the same operator
// lifecycle is rejected as TargetUnhealthy even though replication is
// fine on the wire.
//
// The scenario asserts the contract the wishlist ratifies: within 30s
// of the cluster reconverging after a clean primary kill, the read-
// only site's status.sites[].replicating MUST be true. The 30s window
// is chosen to be more generous than the topology-poll cadence
// (~2s × failureThreshold + the enrichment loop's tick) so any
// genuine "field will populate, just slowly" recovery still passes.
//
// Until WISHLIST #36 is fixed this scenario is expected to FAIL —
// that is the point of a regression test. It guards future work from
// silently bringing the bug back, and it gives a CI-reachable signal
// the moment the fix lands.
func scenario22ReplicationStatusAfterRecovery() runner.Scenario {
	return runner.Scenario{
		ID:    "22-replication-status-after-recovery",
		Title: "CR status.sites[].replicating populates within 30s after recovery",
		Hypothesis: "After a clean primary kill and old-primary auto-recovery, the read-only site's " +
			"status.sites[].replicating becomes true within 30s — without requiring an operator restart. " +
			"This is the contract that planned_failover.go's TargetUnhealthy check depends on.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#22-replication-status-after-recovery",
		Timeout:  6 * time.Minute,
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s22InjectKillPrimary(),
			s22ObserveFailoverFlip(),
			s22ObserveRecovery(),
			s22VerifyCRReplicatingTrue(),
			s22VerifySidecarReplicatingTrue(),
		},
	}
}

func s22InjectKillPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0 then back up to drive failover + recovery",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			env.Capture.Note(fmt.Sprintf("active primary at start: %s", active))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func s22ObserveFailoverFlip() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite flip",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"activeSite changes",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite)
					return mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original, msg, nil
				},
			)
			return err
		},
	}
}

// s22ObserveRecovery scales the original primary back up and waits
// for the cluster to reach a healthy two-site state — exactly one
// writable, exactly one read-only, no RecoveryBlocked. We do not
// inspect the .replicating field here; that's the next step's job.
// The point of this step is to land at the moment the operator
// considers the recovery finished, so we can time the 30s window
// from a stable reference.
func s22ObserveRecovery() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "scale old primary back up; cluster reconverges to writable + read-only",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("scale old primary back up: %w", err)
			}
			env.Capture.Note("old primary scaled back to 1; awaiting healthy two-site convergence")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"sites: writable=1 read-only=1 blocked=0",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other, blocked []string
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable = append(writable, s.Name)
						case "read-only":
							readOnly = append(readOnly, s.Name)
						default:
							other = append(other, fmt.Sprintf("%s=%s", s.Name, s.State))
						}
						if s.RecoveryState == "RecoveryBlocked" {
							blocked = append(blocked, s.Name)
						}
					}
					msg := fmt.Sprintf("writable=%v read-only=%v other=%v blocked=%v",
						writable, readOnly, other, blocked)
					done := len(writable) == 1 && len(readOnly) == 1 && len(blocked) == 0
					return done, msg, nil
				},
			)
			if err != nil {
				return err
			}
			// Stamp the moment of reconvergence so the verifier knows
			// when the 30s window opens.
			recoveredAt := time.Now().UTC().Format(time.RFC3339Nano)
			env.Capture.Note(fmt.Sprintf("reconverged at %s, active=%s", recoveredAt, mfg.Status.ActiveSite))
			return ctxStash(ctx, env, "recoveredAt", recoveredAt)
		},
	}
}

// s22VerifyCRReplicatingTrue is the regression assertion: within 30s
// of the post-recovery convergence stamp, the read-only site's
// status.sites[].replicating field MUST become true. We poll the CR
// rather than relying on a single snapshot because the operator's
// status enrichment runs on a cadence; the wishlist's 30s window is
// what we promise users.
func s22VerifyCRReplicatingTrue() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status.sites[read-only].replicating == true within 30s of recovery",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"read-only site status.replicating becomes true",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var replica *v1alpha1.SiteStatus
					for i := range mfg.Status.Sites {
						if mfg.Status.Sites[i].State == "read-only" {
							replica = &mfg.Status.Sites[i]
							break
						}
					}
					if replica == nil {
						return false, "no read-only site present", nil
					}
					msg := fmt.Sprintf("site=%s state=%s replicating=%v gtidExecuted=%q",
						replica.Name, replica.State, replica.Replicating, replica.GtidExecuted)
					return replica.Replicating, msg, nil
				},
			)
			return err
		},
	}
}

// s22VerifySidecarReplicatingTrue cross-checks against the sidecar
// /status endpoint. If this passes but s22VerifyCRReplicatingTrue
// fails, that is the exact bug WISHLIST #36 describes (sidecar
// reports running, CR field is stale). Including this step in the
// scenario makes triage trivial: a failure here means MySQL itself
// stopped replicating and the bug isn't WISHLIST #36 — it's a real
// data-plane regression.
func s22VerifySidecarReplicatingTrue() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "sidecar /status reports replica_io && replica_sql on the read-only site",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			var replica string
			for _, s := range mfg.Status.Sites {
				if s.State == "read-only" {
					replica = s.Name
					break
				}
			}
			if replica == "" {
				return fmt.Errorf("no read-only site present at verify time (sites=%+v)", mfg.Status.Sites)
			}
			probe, err := env.Sidecar(replica)
			if err != nil {
				return fmt.Errorf("open sidecar probe for %s: %w", replica, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilSidecarStatus(waitCtx, probe,
				fmt.Sprintf("site %s replica_io_running && replica_sql_running", replica),
				func(st *pgsidecar.StatusResponse) (bool, string) {
					msg := fmt.Sprintf("role=%s read_only=%v replica_io=%v replica_sql=%v",
						st.Role, st.ReadOnly, st.ReplicaIORunning, st.ReplicaSQLRunning)
					return st.ReplicaIORunning && st.ReplicaSQLRunning, msg
				},
			)
		},
	}
}
