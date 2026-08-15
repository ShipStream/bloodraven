package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario52RotationPromotionRefused())
}

const (
	s52RotateKeyringAnnotation     = "bloodraven.shipstream.io/rotate-keyring"
	s52PlannedFailoverRejectReason = "KeyringRotation"
)

type s52RunState struct {
	activeSite  string
	replicaSite string

	// rotated records that the rotate-keyring annotation was applied,
	// so cleanup only tries to clear one that exists.
	rotated bool

	// scaled records that the active primary was scaled to 0, so the
	// verify/settle steps know whether ScaleSiteToOne is needed if a
	// later step bails before the explicit scale-back.
	scaled bool

	plannedRejectedBefore float64
	blockedRefusedBefore  float64
	failoversBefore       float64
}

// scenario52RotationPromotionRefused is the real-MySQL regression for
// #160 / EXP-01 and the inverse half of EXP-16: a site whose
// UnsealReason=Rotation must not be a promotion target.
//
// Component tests already pin the matrix, planned-failover validator,
// and dirty-bit heal. This scenario only asserts what those cannot:
// the gate is wired in a running operator against a live encrypted
// replica whose keyring is actually mid-rotation.
//
// Quarantined with 48/50/51 — it needs the TLS + encryption baseline
// and it holds the primary down, so it must not ride run-all.
func scenario52RotationPromotionRefused() runner.Scenario {
	state := &s52RunState{}
	return runner.Scenario{
		ID:    "52-rotation-promotion-refused",
		Title: "A replica mid-keyring-rotation is refused as a planned and emergency promotion target",
		Hypothesis: "Annotating rotate-keyring on a sealed replica moves it to UnsealReason=Rotation. " +
			"A planned failover onto that site fails immediately with reason=KeyringRotation and does not " +
			"change activeSite. Scaling the primary to 0 then leaves the group without a writable primary " +
			"(Degraded/NoPrimary naming Rotation) rather than promoting the rotating replica; " +
			"bloodraven_failovers_total does not increment for that site.",
		Risk:    "high",
		DocLink: "playground/chaos-scenarios.md#52-rotation-promotion-refused",
		Timeout: 20 * time.Minute,
		Quarantine: "requires the dedicated TLS + spec.encryptionAtRest baseline and holds the " +
			"primary down; CI and local encryption jobs run this scenario explicitly",
		Precheck: s52Precheck(state),
		Steps: []runner.Step{
			s52StartReplicaRotation(state),
			s52RefusePlannedFailover(state),
			s52RefuseEmergencyPromotion(state),
			s52RestoreAndReseal(state),
		},
		Cleanup: s52Cleanup(state),
	}
}

func s52Precheck(state *s52RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s52RunState{}
		if err := AssertHealthyBaseline(ctx, env); err != nil {
			return err
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		if err := s50RequireEncryptedBaseline(mfg); err != nil {
			return err
		}
		state.activeSite = mfg.Status.ActiveSite
		state.replicaSite, err = PeerOf(mfg, state.activeSite)
		if err != nil {
			return err
		}
		if state.replicaSite == state.activeSite {
			return fmt.Errorf("refusing to rotate the active primary %s", state.activeSite)
		}
		return nil
	}
}

func s52StartReplicaRotation(state *s52RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate the sealed replica for rotation and wait for UnsealReason=Rotation",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			before := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			if before == nil || before.Phase != v1alpha1.KeyringPhaseSealed {
				return fmt.Errorf("replica %s is not sealed before rotation: %+v", state.replicaSite, before)
			}

			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
				s52RotateKeyringAnnotation, state.replicaSite); err != nil {
				return fmt.Errorf("annotate for replica rotation: %w", err)
			}
			state.rotated = true

			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s UnsealReason=Rotation", state.replicaSite),
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.EncryptionAtRest == nil {
						return false, "status.encryptionAtRest not populated", nil
					}
					s := m.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
					if s == nil {
						return false, "no status entry for " + state.replicaSite, nil
					}
					if s.UnsealReason == v1alpha1.UnsealReasonRotation {
						return true, "", nil
					}
					return false, fmt.Sprintf("phase=%s reason=%s (%s)", s.Phase, s.UnsealReason, s.Message), nil
				})
			if err != nil {
				return fmt.Errorf("waiting for rotation unseal: %w", err)
			}
			env.Logger.Info("replica is mid-rotation", "site", state.replicaSite)
			return nil
		},
	}
}

func s52RefusePlannedFailover(state *s52RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "planned failover onto the rotating replica is rejected with KeyringRotation",
		Do: func(ctx context.Context, env *runner.Env) error {
			var err error
			state.plannedRejectedBefore, err = metricCounter(ctx, env, "bloodraven_planned_failovers_total", map[string]string{
				"target_site": state.replicaSite,
				"result":      "rejected",
			})
			if err != nil {
				return err
			}

			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != state.activeSite {
				return fmt.Errorf("active site drifted from %s to %s before the planned-failover probe",
					state.activeSite, mfg.Status.ActiveSite)
			}
			if !mfg.SiteKeyringRotationBlocksPromotion(state.replicaSite) {
				return fmt.Errorf("replica %s is no longer mid-rotation; the planned-failover window closed",
					state.replicaSite)
			}

			if err := env.Chaos.AnnotatePlannedFailover(ctx, state.replicaSite); err != nil {
				return err
			}

			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			staleCutoff := env.StartTime.Add(-2 * time.Second)
			got, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("plannedFailover Failed reason=%s target=%s", s52PlannedFailoverRejectReason, state.replicaSite),
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.ActiveSite != "" && m.Status.ActiveSite != state.activeSite {
						return false, fmt.Sprintf("activeSite=%s", m.Status.ActiveSite),
							fmt.Errorf("planned failover onto a rotating site changed activeSite from %s to %s",
								state.activeSite, m.Status.ActiveSite)
					}
					pf := m.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status yet", nil
					}
					if pf.StartTime == nil || pf.StartTime.Time.Before(staleCutoff) {
						return false, fmt.Sprintf("ignoring stale plannedFailover (startTime=%v)", pf.StartTime), nil
					}
					msg := fmt.Sprintf("phase=%q target=%q reason=%q active=%q",
						pf.Phase, pf.Target, pf.Reason, m.Status.ActiveSite)
					if pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
						return false, msg, fmt.Errorf("planned failover onto rotating %s unexpectedly Succeeded", state.replicaSite)
					}
					if pf.Phase == v1alpha1.PlannedFailoverPhaseFailed {
						if pf.Reason != s52PlannedFailoverRejectReason {
							return false, msg, fmt.Errorf("planned failover Failed with reason %q, want %s",
								pf.Reason, s52PlannedFailoverRejectReason)
						}
						if pf.Target != state.replicaSite {
							return false, msg, fmt.Errorf("plannedFailover target=%q, want %q", pf.Target, state.replicaSite)
						}
						return true, msg, nil
					}
					return false, msg, nil
				})
			if err != nil {
				return err
			}
			if got.Status.ActiveSite != state.activeSite {
				return fmt.Errorf("activeSite=%q after planned reject, want %q", got.Status.ActiveSite, state.activeSite)
			}
			if _, present := got.GetAnnotations()["bloodraven.shipstream.io/planned-failover"]; present {
				return fmt.Errorf("planned-failover annotation was not consumed after KeyringRotation reject")
			}

			after, err := metricCounter(ctx, env, "bloodraven_planned_failovers_total", map[string]string{
				"target_site": state.replicaSite,
				"result":      "rejected",
			})
			if err != nil {
				return err
			}
			if after < state.plannedRejectedBefore+1 {
				return fmt.Errorf("bloodraven_planned_failovers_total{result=rejected} did not increment (before=%v after=%v)",
					state.plannedRejectedBefore, after)
			}
			env.Logger.Info("planned failover rejected mid-rotation",
				"target", state.replicaSite, "reason", s52PlannedFailoverRejectReason)
			return nil
		},
	}
}

func s52RefuseEmergencyPromotion(state *s52RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scaling the primary to 0 refuses to promote the rotating replica",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if !mfg.SiteKeyringRotationBlocksPromotion(state.replicaSite) {
				return fmt.Errorf("replica %s left Rotation before the emergency probe; window closed",
					state.replicaSite)
			}

			var ferr error
			state.failoversBefore, ferr = metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{
				"target_site": state.replicaSite,
			})
			if ferr != nil {
				return ferr
			}
			state.blockedRefusedBefore, ferr = metricCounter(ctx, env, "bloodraven_keyring_promotions_blocked_total", map[string]string{
				"namespace": env.Namespace,
				"group":     env.FG,
				"site":      state.replicaSite,
				"outcome":   "refused",
			})
			if ferr != nil {
				return ferr
			}

			eventBefore := time.Now()
			if err := env.Chaos.ScaleSiteToZero(ctx, state.activeSite); err != nil {
				return err
			}
			state.scaled = true

			// Relay-log drain on an unreachable primary is ~30s plus
			// failureThreshold*pollInterval. Degraded is a single
			// condition and is routinely overwritten by ReplicationError
			// once the replica notices the source is gone. The Event is
			// only emitted when the refused set *changes*, so a second
			// run against the same operator process will not see a
			// fresh KeyringPromotionRefused. The counter increments on
			// each decision and is the durable signal; the Event is
			// accepted when it does appear.
			deadline := time.Now().Add(3 * time.Minute)
			var last string
			for {
				m, gerr := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if gerr != nil {
					last = gerr.Error()
				} else {
					rotating := m.SiteKeyringRotationBlocksPromotion(state.replicaSite)
					if rotating && (s52SiteWritable(m, state.replicaSite) || m.Status.ActiveSite == state.replicaSite) {
						return fmt.Errorf("rotating replica %s was promoted — the #160 gate is not wired", state.replicaSite)
					}
					reason, _ := s52Degraded(m)
					blocked, berr := metricCounter(ctx, env, "bloodraven_keyring_promotions_blocked_total", map[string]string{
						"namespace": env.Namespace,
						"group":     env.FG,
						"site":      state.replicaSite,
						"outcome":   "refused",
					})
					if berr != nil {
						last = berr.Error()
					} else {
						_, evOK, evErr := findMFGEvent(ctx, env, eventBefore, "KeyringPromotionRefused", state.replicaSite)
						if evErr != nil {
							return evErr
						}
						last = fmt.Sprintf("active=%q degraded=%s rotating=%v blocked=%v event=%v",
							m.Status.ActiveSite, reason, rotating, blocked, evOK)
						if evOK || blocked >= state.blockedRefusedBefore+1 {
							env.Logger.Info("emergency refusal observed",
								"event", evOK, "blocked", blocked, "before", state.blockedRefusedBefore)
							break
						}
					}
				}
				if !time.Now().Before(deadline) {
					return fmt.Errorf("waiting for rotation-aware emergency refusal: timed out (%s)", last)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}

			// Hold through the rest of the failover budget so a delayed
			// promote cannot pass after the first NoPrimary observation.
			// Promotion after Rotation clears is the #160 heal path and
			// is allowed.
			holdUntil := time.Now().Add(45 * time.Second)
			for time.Now().Before(holdUntil) {
				cur, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				if cur.SiteKeyringRotationBlocksPromotion(state.replicaSite) &&
					(s52SiteWritable(cur, state.replicaSite) || cur.Status.ActiveSite == state.replicaSite) {
					return fmt.Errorf("rotating replica %s was promoted during the post-refusal hold", state.replicaSite)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}

			blockedAfter, err := metricCounter(ctx, env, "bloodraven_keyring_promotions_blocked_total", map[string]string{
				"namespace": env.Namespace,
				"group":     env.FG,
				"site":      state.replicaSite,
				"outcome":   "refused",
			})
			if err != nil {
				return err
			}
			if blockedAfter < state.blockedRefusedBefore+1 {
				return fmt.Errorf("bloodraven_keyring_promotions_blocked_total{outcome=refused} did not increment (before=%v after=%v)",
					state.blockedRefusedBefore, blockedAfter)
			}

			// failovers_total must stay put for as long as the replica is
			// still rotating. If Rotation cleared during the hold, a
			// subsequent promote is the documented heal and will increment.
			still, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if still.SiteKeyringRotationBlocksPromotion(state.replicaSite) {
				failoversAfter, ferr := metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{
					"target_site": state.replicaSite,
				})
				if ferr != nil {
					return ferr
				}
				if failoversAfter != state.failoversBefore {
					return fmt.Errorf("bloodraven_failovers_total{%s} incremented %v → %v while UnsealReason=Rotation; a refused promotion must not count as a failover",
						state.replicaSite, state.failoversBefore, failoversAfter)
				}
			}
			env.Logger.Info("emergency promotion refused mid-rotation",
				"replica", state.replicaSite, "blockedDelta", blockedAfter-state.blockedRefusedBefore)
			return nil
		},
	}
}

func s52RestoreAndReseal(state *s52RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "restore the primary and wait for the replica to re-seal",
		Do: func(ctx context.Context, env *runner.Env) error {
			if state.scaled {
				if err := env.Chaos.ScaleSiteToOne(ctx, state.activeSite); err != nil {
					return err
				}
			}

			// One writable primary-candidate is enough. Fail-back is
			// current-state-driven: if rotation finished while the
			// original primary was down, the replica may now be primary
			// and the returning site rejoins as a replica.
			waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"exactly one writable primary-candidate and the rotating site has left Rotation",
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					writable := s52WritableCandidates(m)
					rotating := m.SiteKeyringRotationBlocksPromotion(state.replicaSite)
					msg := fmt.Sprintf("writable=%v active=%q rotating=%v", writable, m.Status.ActiveSite, rotating)
					// Two writables is the expected moment after a heal
					// promote of the now-sealed replica plus the original
					// primary coming back with an empty my.cnf read_only.
					// The operator fences the extra; do not fail it.
					if len(writable) != 1 || rotating {
						return false, msg, nil
					}
					return true, msg, nil
				})
			if err != nil {
				return fmt.Errorf("waiting for topology to settle after restore: %w", err)
			}

			sealCtx, sealCancel := context.WithTimeout(ctx, 8*time.Minute)
			defer sealCancel()
			_, err = env.Wait.UntilCR(sealCtx, env.Namespace,
				fmt.Sprintf("site %s Sealed after rotation", state.replicaSite),
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.EncryptionAtRest == nil {
						return false, "status.encryptionAtRest not populated", nil
					}
					s := m.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
					if s == nil {
						return false, "no status entry for " + state.replicaSite, nil
					}
					if s.Phase == v1alpha1.KeyringPhaseFailed {
						return false, s.Message, fmt.Errorf("replica %s failed during rotation: %s", state.replicaSite, s.Message)
					}
					if s.Phase != v1alpha1.KeyringPhaseSealed {
						return false, fmt.Sprintf("phase=%s reason=%s (%s)", s.Phase, s.UnsealReason, s.Message), nil
					}
					return true, "", nil
				})
			if err != nil {
				return fmt.Errorf("waiting for replica to re-seal: %w", err)
			}

			// Sealed flips UnsealReason off before the ordered update
			// that rolled the sealed rendering has left UpdateReplica /
			// WaitReplica. AssertHealthyBaseline treats a non-empty
			// updatePhase as dirty, so wait it out.
			rollCtx, rollCancel := context.WithTimeout(ctx, 3*time.Minute)
			defer rollCancel()
			_, err = env.Wait.UntilCR(rollCtx, env.Namespace, "ordered update idle after rotation reseal",
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.UpdatePhase != "" {
						return false, "updatePhase=" + m.Status.UpdatePhase, nil
					}
					return true, "", nil
				})
			if err != nil {
				return fmt.Errorf("waiting for the post-rotation rollout to finish: %w", err)
			}
			if err := AssertHealthyBaseline(ctx, env); err != nil {
				return fmt.Errorf("baseline after rotation-promotion scenario: %w", err)
			}
			env.Logger.Info("cluster healthy after rotation-promotion refusal",
				"originalPrimary", state.activeSite, "replica", state.replicaSite)
			return nil
		},
	}
}

func s52Cleanup(state *s52RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if !state.rotated {
			return nil
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return fmt.Errorf("read MFG to clear leftover rotate annotation: %w", err)
		}
		if _, present := mfg.GetAnnotations()[s52RotateKeyringAnnotation]; !present {
			state.rotated = false
			return nil
		}
		if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
			s52RotateKeyringAnnotation, ""); err != nil {
			return fmt.Errorf("clear rotate annotation: %w", err)
		}
		state.rotated = false
		return nil
	}
}

func s52SiteWritable(mfg *v1alpha1.MysqlFailoverGroup, name string) bool {
	for _, s := range mfg.Status.Sites {
		if s.Name == name {
			return s.State == "writable"
		}
	}
	return false
}

func s52WritableCandidates(mfg *v1alpha1.MysqlFailoverGroup) []string {
	var out []string
	for _, s := range mfg.Status.Sites {
		if s.State == "writable" && isPromotableSite(mfg, s.Name) {
			out = append(out, s.Name)
		}
	}
	return out
}

func s52Degraded(mfg *v1alpha1.MysqlFailoverGroup) (reason, message string) {
	for _, c := range mfg.Status.Conditions {
		if c.Type == "Degraded" {
			return c.Reason, c.Message
		}
	}
	return "", ""
}
