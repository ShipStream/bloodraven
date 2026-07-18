package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario19RecloneInterlock())
}

// scenario19RecloneInterlock exercises the bloodraven.shipstream.io/reclone-site
// annotation safety interlock end-to-end. It first produces a
// divergent-GTID condition through an actual failover and old-primary
// rogue write (this scenario is self-contained — it does not depend on
// scenario 8 having been run), then submits three
// annotation values and asserts the operator reaction:
//
//	A) bare site name      — REJECTED (RecloneRejected event), annotation cleared
//	B) site:wrongprefix    — REJECTED (RecloneRejected event), annotation cleared
//	C) site:correctprefix  — ACCEPTED (RecloneRequested event), Bootstrapping condition cycles to Done
//
// The cold-reclone "must include :confirm=<fg-name>" interlock branch
// is intentionally NOT exercised here — sub-case C drives the divergent
// site through a successful clone, leaving the cluster in a clean state
// where the cold path is the next thing to test, but a separate scenario
// would need to set that up after a reset. Keeping this scenario focused
// on the divergent-GTID interlock keeps the timing budget reasonable.
//
// Cleanup expectations: after sub-case C the cluster ends with both
// sites in {writable, read-only}, so the executor's reconvergence
// gate passes without assistance.
func scenario19RecloneInterlock() runner.Scenario {
	return runner.Scenario{
		ID:    "19-reclone-interlock",
		Title: "Reclone safety interlock rejects fat-finger annotations",
		Hypothesis: "Against a site with status.divergentGtid populated, the reclone-site annotation: " +
			"(A) bare site name is REJECTED with RecloneRejected event; " +
			"(B) wrong GTID prefix is REJECTED with RecloneRejected event; " +
			"(C) correct 8-char prefix is ACCEPTED with RecloneRequested event and clone runs to completion.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#19-reclone-safety-interlock",
		Timeout:  8 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectDivergenceViaFailover(),
			observeDivergenceRecorded(),
			verifyRecloneRejectedBare(),
			verifyRecloneRejectedMismatch(),
			verifyRecloneAcceptedMatching(),
		},
		Cleanup: cleanupRecloneInterlockReplica,
	}
}

func cleanupRecloneInterlockReplica(ctx context.Context, env *runner.Env) error {
	site := ctxFetch(env, "divergentSite")
	if site == "" {
		return nil
	}
	db, err := env.MySQL(site)
	if err != nil {
		return nil
	}
	if _, err := db.Exec(ctx, "START REPLICA"); err != nil {
		env.Capture.Note(fmt.Sprintf("cleanup: START REPLICA on %s skipped/failed: %v", site, err))
	}
	return nil
}

// injectDivergenceViaFailover triggers a failover, then writes a
// rogue transaction on the old primary so its gtid_executed contains a
// UUID that the new primary has never seen. The operator's recovery
// path then computes oldGtid.Subtract(newGtid) → non-empty →
// "divergence detected" is logged and status.sites[old].divergentGtid
// is populated.
func injectDivergenceViaFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "force a failover then write a rogue transaction on the old primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s peer=%s; forcing failover then injecting divergence on %s", active, peer, active))
			if err := ctxStash(ctx, env, "divergentSite", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "newPrimarySite", peer); err != nil {
				return err
			}
			// Step 1: trigger the failover by scaling the primary to 0.
			if err := env.Chaos.ScaleSiteToZero(ctx, active); err != nil {
				return err
			}
			// Step 2: wait for activeSite to flip away from the original.
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite flips away from %s", active),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite)
					return mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != active, msg, nil
				},
			)
			cancel()
			if err != nil {
				return fmt.Errorf("waiting for failover: %w", err)
			}
			if err := env.Chaos.PatchSplitBrainPriorities(ctx, []string{peer, active}); err != nil {
				return fmt.Errorf("patch split-brain priorities to prefer new primary %s over old primary %s: %w", peer, active, err)
			}
			env.Capture.Note(fmt.Sprintf("split-brain priorities patched to [%s %s] for divergence injection", peer, active))
			waitCtx, cancel = context.WithTimeout(ctx, 90*time.Second)
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("new primary %s stable and cooldown clear before reintroducing old primary %s", peer, active),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					peerState := "<missing>"
					activeState := "<missing>"
					for _, s := range mfg.Status.Sites {
						switch s.Name {
						case peer:
							peerState = s.State
						case active:
							activeState = s.State
						}
					}
					cooldownRemaining := time.Duration(0)
					if mfg.Spec.FailoverCooldown != nil && mfg.Status.LastFailover != nil {
						cooldownRemaining = mfg.Spec.FailoverCooldown.Duration - time.Since(mfg.Status.LastFailover.Time)
						if cooldownRemaining < 0 {
							cooldownRemaining = 0
						}
					}
					msg := fmt.Sprintf("activeSite=%q peerState=%s oldState=%s lastFailoverTarget=%q cooldownRemaining=%s",
						mfg.Status.ActiveSite, peerState, activeState, mfg.Status.LastFailoverTarget, cooldownRemaining.Round(time.Second))
					done := mfg.Status.ActiveSite == peer &&
						mfg.Status.LastFailoverTarget == peer &&
						peerState == "writable" &&
						activeState != "writable" &&
						cooldownRemaining == 0
					return done, msg, nil
				},
			)
			cancel()
			if err != nil {
				return fmt.Errorf("waiting for stable new primary before old-primary reentry: %w", err)
			}
			// Step 3: scale the old primary back up so we can write to it.
			if err := env.Chaos.ScaleSiteToOne(ctx, active); err != nil {
				return fmt.Errorf("scale old primary back up: %w", err)
			}
			// Step 4: open a MySQL connection to the old primary and
			// write rogue data with super_read_only cleared. We race the
			// operator's recovery loop here: if it recovers the site
			// first, STOP REPLICA plus the rogue write still creates the
			// divergent old-primary state for the next poll.
			db, err := env.MySQL(active)
			mysqlDeadline := time.Now().Add(45 * time.Second)
			for err != nil && time.Now().Before(mysqlDeadline) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
				}
				db, err = env.MySQL(active)
			}
			if err != nil {
				return fmt.Errorf("open mysql client for %s before sidecar lease timeout: %w", active, err)
			}
			oldGtidBefore, err := db.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("read pre-rogue GTID on %s: %w", active, err)
			}
			// STOP REPLICA is permitted under super_read_only; it just
			// stops anything the operator may have started. Ignoring
			// errors because there might be no replica to stop yet.
			_, _ = db.Exec(ctx, "STOP REPLICA")
			if err := db.SetSuperReadOnly(ctx, false); err != nil {
				return fmt.Errorf("clear super_read_only on %s: %w", active, err)
			}
			if _, err := db.Exec(ctx, "SET SESSION sql_log_bin = 1"); err != nil {
				return fmt.Errorf("enable sql_log_bin on %s: %w", active, err)
			}
			if _, err := db.Exec(ctx, "SET @@SESSION.GTID_NEXT = 'AUTOMATIC'"); err != nil {
				return fmt.Errorf("set GTID_NEXT automatic on %s: %w", active, err)
			}
			rogueDDL := []string{
				"CREATE DATABASE IF NOT EXISTS chaos_divergence",
				"CREATE TABLE IF NOT EXISTS chaos_divergence.rogue (id INT PRIMARY KEY AUTO_INCREMENT, payload VARCHAR(64))",
				fmt.Sprintf("INSERT INTO chaos_divergence.rogue (payload) VALUES ('rogue-%d-1'), ('rogue-%d-2'), ('rogue-%d-3')",
					time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano()),
			}
			for _, q := range rogueDDL {
				if _, err := db.Exec(ctx, q); err != nil {
					return fmt.Errorf("rogue write %q on %s: %w", q, active, err)
				}
			}
			oldGtidAfter, err := db.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("read post-rogue GTID on %s: %w", active, err)
			}
			if oldGtidAfter == oldGtidBefore {
				return fmt.Errorf("rogue write on %s did not advance gtid_executed (still %q)", active, oldGtidAfter)
			}
			if err := db.SetSuperReadOnly(ctx, true); err != nil {
				return fmt.Errorf("re-fence divergent old primary %s after rogue write: %w", active, err)
			}
			env.Capture.Note(fmt.Sprintf("rogue write advanced %s GTID from %q to %q", active, oldGtidBefore, oldGtidAfter))
			env.Capture.Note(fmt.Sprintf("rogue transactions written to %s and site re-fenced; awaiting divergence detection", active))
			return nil
		},
	}
}

func observeDivergenceRecorded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for status.sites[divergent].divergentGtid to be populated",
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "divergentSite")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s has non-empty divergentGtid", site),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var observed *v1alpha1.SiteStatus
					for _, s := range mfg.Status.Sites {
						if s.DivergentGtid == "" {
							continue
						}
						st := s
						if s.Name == site {
							observed = &st
							break
						}
						if observed == nil {
							observed = &st
						}
					}
					if observed != nil {
						msg := fmt.Sprintf("site=%s state=%s recoveryState=%s divergentGtid=%q",
							observed.Name, observed.State, observed.RecoveryState, observed.DivergentGtid)
						return true, msg, nil
					}
					for _, s := range mfg.Status.Sites {
						if s.Name == site {
							return false, fmt.Sprintf("site=%s state=%s recoveryState=%s divergentGtid=%q",
								s.Name, s.State, s.RecoveryState, s.DivergentGtid), nil
						}
					}
					return false, fmt.Sprintf("site %s not present in status.sites yet", site), nil
				},
			)
			if err != nil {
				return err
			}
			// Stash the observed GTID so later sub-cases can compute
			// the prefix; the divergent UUID:GNO is the same for the
			// rest of the scenario.
			for _, s := range mfg.Status.Sites {
				if s.DivergentGtid != "" {
					if s.Name != site {
						env.Capture.Note(fmt.Sprintf("divergent site changed from injected site %s to observed site %s", site, s.Name))
						if err := ctxStash(ctx, env, "divergentSite", s.Name); err != nil {
							return err
						}
					}
					return ctxStash(ctx, env, "divergentGtid", s.DivergentGtid)
				}
			}
			return fmt.Errorf("divergent GTID vanished from status.sites between waits")
		},
	}
}

// verifyRecloneRejectedBare submits the bare site name as the
// annotation value, which must be rejected with RecloneRejected and
// the annotation cleared by the operator.
func verifyRecloneRejectedBare() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "sub-case A: bare site name is rejected against divergent site",
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "divergentSite")
			return submitAndExpectRejection(ctx, env, site, "must include the divergent-GTID prefix")
		},
	}
}

// verifyRecloneRejectedMismatch submits a non-matching prefix and
// expects RecloneRejected with the "does not match" message.
func verifyRecloneRejectedMismatch() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "sub-case B: mismatched GTID prefix is rejected",
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "divergentSite")
			// "deadbeef" is intentionally chosen to be the right length
			// (8 chars passes the length check) and obviously wrong.
			return submitAndExpectRejection(ctx, env, site+":deadbeef", "does not match the observed divergentGtid")
		},
	}
}

// verifyRecloneAcceptedMatching submits the correct 8-character prefix
// and waits for the clone to complete. The Bootstrapping condition is
// the operator-side narrative: it cycles Cloning → WaitingForRestart →
// SetupReplication → Done as the clone runs.
func verifyRecloneAcceptedMatching() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "sub-case C: matching 8-char prefix is accepted; clone runs to completion",
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "divergentSite")
			gtid := ctxFetch(env, "divergentGtid")
			if len(gtid) < 8 {
				return fmt.Errorf("recorded divergentGtid %q shorter than 8 characters", gtid)
			}
			prefix := gtid[:8]
			value := site + ":" + prefix
			env.Capture.Note(fmt.Sprintf("submitting reclone-site=%s (gtid prefix from %q)", value, gtid))
			before := time.Now()
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, "bloodraven.shipstream.io/reclone-site", value); err != nil {
				return fmt.Errorf("set reclone annotation: %w", err)
			}
			// Expect a RecloneRequested event within the operator's
			// sync interval (~30s, plus some slack).
			eventCtx, eventCancel := context.WithTimeout(ctx, 60*time.Second)
			ev, err := waitForMFGEvent(eventCtx, env, before, "RecloneRequested", "")
			eventCancel()
			if err != nil {
				return fmt.Errorf("waiting for RecloneRequested event: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("RecloneRequested observed: %s", ev.Message))
			// Wait for the cluster to reconverge to a clean two-site
			// state with no RecoveryBlocked, no divergentGtid. Clone +
			// restart + replication setup is 30–60s in the playground.
			doneCtx, doneCancel := context.WithTimeout(ctx, 4*time.Minute)
			defer doneCancel()
			var cleanSince time.Time
			const stableWindow = 20 * time.Second
			_, err = env.Wait.UntilCR(doneCtx, env.Namespace,
				"divergence cleared, cluster healthy and stable",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, blocked, divergent []string
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable = append(writable, s.Name)
						case "read-only":
							readOnly = append(readOnly, s.Name)
						}
						if s.RecoveryState == "RecoveryBlocked" {
							blocked = append(blocked, s.Name)
						}
						if s.DivergentGtid != "" {
							divergent = append(divergent, s.Name)
						}
					}
					boot := findCondition(mfg.Status.Conditions, "Bootstrapping")
					bootState := "<missing>"
					bootDone := false
					if boot != nil {
						bootState = fmt.Sprintf("%s/%s", boot.Status, boot.Reason)
						if boot.Status == metav1.ConditionFalse && boot.Reason == "Failed" {
							return false, bootState, fmt.Errorf("reclone bootstrap failed: %s", boot.Message)
						}
						bootDone = boot.Status == metav1.ConditionFalse && boot.Reason == "Done"
					}
					clean := len(writable) == 1 && len(readOnly) == len(mfg.Status.Sites)-1 && len(blocked) == 0 && len(divergent) == 0 && bootDone
					now := time.Now()
					if clean {
						if cleanSince.IsZero() {
							cleanSince = now
						}
					} else {
						cleanSince = time.Time{}
					}
					stableFor := time.Duration(0)
					if !cleanSince.IsZero() {
						stableFor = now.Sub(cleanSince).Round(time.Second)
					}
					done := clean && stableFor >= stableWindow
					msg := fmt.Sprintf("writable=%v read-only=%v blocked=%v divergent=%v boot=%s stableFor=%s/%s",
						writable, readOnly, blocked, divergent, bootState, stableFor, stableWindow)
					return done, msg, nil
				},
			)
			return err
		},
	}
}

// submitAndExpectRejection sets the reclone annotation, polls for a
// RecloneRejected Warning event whose message contains expectedSnippet,
// and asserts the operator cleared the annotation. Returns nil on the
// happy path.
func submitAndExpectRejection(ctx context.Context, env *runner.Env, value, expectedSnippet string) error {
	env.Capture.Note(fmt.Sprintf("submitting reclone-site=%q (expecting rejection: %q)", value, expectedSnippet))
	before := time.Now()
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, "bloodraven.shipstream.io/reclone-site", value); err != nil {
		return fmt.Errorf("set reclone annotation: %w", err)
	}
	// Operator sync ticker is 30s; allow 60s of slack.
	evCtx, evCancel := context.WithTimeout(ctx, 60*time.Second)
	ev, err := waitForMFGEvent(evCtx, env, before, "RecloneRejected", expectedSnippet)
	evCancel()
	if err != nil {
		return fmt.Errorf("waiting for RecloneRejected event with %q: %w", expectedSnippet, err)
	}
	env.Capture.Note(fmt.Sprintf("RecloneRejected observed: %s", ev.Message))
	// Annotation should be cleared by the operator within a couple of
	// reconciles. Poll briefly to confirm.
	clearCtx, clearCancel := context.WithTimeout(ctx, 30*time.Second)
	defer clearCancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		mfg, err := env.Kube.GetMFGNamed(clearCtx, env.Namespace, env.FG)
		if err == nil {
			if _, present := mfg.GetAnnotations()["bloodraven.shipstream.io/reclone-site"]; !present {
				return nil
			}
		}
		select {
		case <-clearCtx.Done():
			return fmt.Errorf("annotation was not cleared within deadline (last err=%v)", err)
		case <-tick.C:
		}
	}
}

// waitForMFGEvent polls the namespace's events for one whose
// involvedObject.name matches the failover group, reason matches
// expectedReason, lastTimestamp is at or after notBefore, and (when
// expectedSnippet is non-empty) message contains expectedSnippet.
//
// We poll rather than using a watch because the executor doesn't yet
// thread an EventClient, and the ~30s sync cadence means a 5s poll is
// well within budget.
func waitForMFGEvent(ctx context.Context, env *runner.Env, notBefore time.Time, expectedReason, expectedSnippet string) (corev1.Event, error) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	check := func() (corev1.Event, bool, error) {
		events, err := env.Kube.RecentEvents(ctx, env.Namespace, 200)
		if err != nil {
			return corev1.Event{}, false, err
		}
		for _, ev := range events {
			if ev.InvolvedObject.Kind != "MysqlFailoverGroup" || ev.InvolvedObject.Name != env.FG {
				continue
			}
			if ev.Reason != expectedReason {
				continue
			}
			ts := ev.LastTimestamp.Time
			if ts.IsZero() {
				ts = ev.EventTime.Time
			}
			// core/v1 Event.LastTimestamp is second-granularity, so an event
			// emitted immediately after the annotation can appear slightly
			// before the caller's sub-second notBefore value.
			if ts.Before(notBefore.Add(-2 * time.Second)) {
				continue
			}
			if expectedSnippet != "" && !strings.Contains(ev.Message, expectedSnippet) {
				continue
			}
			return ev, true, nil
		}
		return corev1.Event{}, false, nil
	}
	if ev, ok, err := check(); err != nil {
		return corev1.Event{}, err
	} else if ok {
		return ev, nil
	}
	for {
		select {
		case <-ctx.Done():
			return corev1.Event{}, fmt.Errorf("no matching event (reason=%s snippet=%q) before deadline: %w", expectedReason, expectedSnippet, ctx.Err())
		case <-tick.C:
			if ev, ok, err := check(); err != nil {
				return corev1.Event{}, err
			} else if ok {
				return ev, nil
			}
		}
	}
}
