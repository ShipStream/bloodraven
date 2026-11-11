package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario68PITRFailbackWithReclone())
}

const (
	s68ID     = "68-pitr-failback-with-reclone"
	s68DBName = "chaos_s68_pitr"
)

// scenario68PITRFailbackWithReclone is DESIGN §9.4 s68 and §4.15 Example 4.
// Cycle 1 fails over to b, flushes b, reclones a and fails back: every era
// verifies exactly. Cycle 2 does the same without flushing b. The reclone
// wipes a's replica copies of b's era, so until the failback that era
// exists only in b's active binlog. The failback archives it (#264): the
// operator rotates the fenced b and b's sidecar, by then read-only, uploads
// the sealed file. Targets inside the era and after it then verify exactly.
// Before #264 nothing sealed that file, and a verification after the
// failback refused PITRArchiveGap naming b's transactions (FU-1).
//
// Each reclone also checks the operator's step before CLONE wipes the
// recipient's binary logs: it rotates them, waits for the recipient's
// sidecar to report its tail archived, logs "pre-reclone archive tail
// archived", and raises no PITRArchiveTailNotArchived Event
// (pitrRecloneSite).
func scenario68PITRFailbackWithReclone() runner.Scenario {
	return runner.Scenario{
		ID:    s68ID,
		Title: "PITR across failback with reclone: exact eras, and an era nobody flushed is archived at the failback",
		Hypothesis: "Cycle 1 (A→B, flush b, reclone a, failback, flush a): timestamp verifications in every era certify " +
			"exactly the fenced history. Cycle 2 (A→B without a flush, reclone a, failback, flush a): the failback archives " +
			"b's unflushed era as the outgoing primary's tail, so verifications inside that era and after it certify exactly " +
			"the fenced history; a backup taken after the failback is covered. Each reclone starts only after the operator " +
			"confirmed the recipient's binlog tail archived, with no PITRArchiveTailNotArchived Event, and leaves the " +
			"recipient with a new server_uuid, under which it numbers the era after its failback.",
		Risk:              "medium",
		DocLink:           "playground/chaos-scenarios.md#68-pitr-failback-with-reclone",
		Timeout:           75 * time.Minute,
		CleanupTimeout:    pitrCleanupBudget(0),
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s68Configure(),
			s68Backup(),
			s68Cycle1(),
			s68VerifyCycle1(),
			s68Cycle2(),
			s68VerifyCycle2Tail(),
			s68VerifyNewBackup(),
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			if err := pitrCleanup(ctx, env, s68DBName); err != nil {
				return fmt.Errorf("s68 cleanup: %w", err)
			}
			return nil
		},
	}
}

func s68Configure() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure a plaintext RustFS PITR archive",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := pitrConfigureArchive(ctx, env, s68ID, "s68", "", false); err != nil {
				return err
			}
			a, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "siteA", a); err != nil {
				return err
			}
			return ctxStash(ctx, env, "siteB", b)
		},
	}
}

func s68Backup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed the marker table and take backup β1",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			if err := pitrCreateMarkerTable(ctx, env, a, s68DBName); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s68DBName, 1, "baseline"); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			return pitrBackup(ctx, env, s68ID, "b1")
		},
	}
}

// s68Era writes marker phase (id) on site, fences T under key and writes
// the witness marker phase+"b" (id+1) while a background writer commits on
// site, stops the writer, and optionally flushes. The writer stays stopped
// until the next era, so in cycle 1 nothing commits on b between its flush
// and the failback: every era of cycle 1 is archived by a scenario flush,
// and cycle 2 alone depends on the failback archiving b's tail.
func s68Era(ctx context.Context, env *runner.Env, site string, id int, phase, key string, flush bool) error {
	if err := pitrWithWriter(ctx, env, site, s68DBName, func() error {
		if err := pitrInsertMarker(ctx, env, site, s68DBName, id, phase); err != nil {
			return err
		}
		f, err := pitrTakeFence(ctx, env, site, false)
		if err != nil {
			return err
		}
		if err := pitrStashFence(ctx, env, key, f); err != nil {
			return err
		}
		return pitrInsertMarker(ctx, env, site, s68DBName, id+1, phase+"b")
	}); err != nil {
		return err
	}
	if flush {
		return pitrFlush(ctx, env, site, true)
	}
	return nil
}

func s68Cycle1() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "cycle 1: era A, A→B, era B + flush b, reclone a, failback, era A' + flush a",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			if err := s68Era(ctx, env, a, 2, "M1", "T1", true); err != nil {
				return err
			}
			if _, err := pitrPlannedFailover(ctx, env, b, false); err != nil {
				return err
			}
			if err := s68Era(ctx, env, b, 4, "M2", "T2", true); err != nil {
				return err
			}
			if err := pitrRecloneSite(ctx, env, a, false); err != nil {
				return err
			}
			if _, err := pitrPlannedFailover(ctx, env, a, false); err != nil {
				return err
			}
			return s68Era(ctx, env, a, 6, "M3", "T3", true)
		},
	}
}

func s68VerifyCycle1() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "cycle 1: β1 verifies exactly in eras A, B and A'",
		Do: func(ctx context.Context, env *runner.Env) error {
			cases := []struct {
				label, fence string
				markers      []string
			}{
				{"c1-t1", "T1", []string{"baseline", "M1"}},
				{"c1-t2", "T2", []string{"baseline", "M1", "M1b", "M2"}},
				{"c1-t3", "T3", []string{"baseline", "M1", "M1b", "M2", "M2b", "M3"}},
			}
			for _, c := range cases {
				f, err := pitrFetchFence(env, c.fence)
				if err != nil {
					return err
				}
				if err := pitrVerify(ctx, env, s68ID, s68DBName, c.label, pitrExpectation{
					Backup: "b1", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered, Markers: c.markers,
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func s68Cycle2() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "cycle 2: A→B, era B without a flush, reclone a, failback (archives b's tail), era A'' + flush a",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			// Everything a executed before the failover was sealed and
			// archived by cycle 1's last flushing fence (nothing commits
			// in between), so b's tail is exactly what b executes from
			// here until the failback.
			before, err := pitrSiteGTID(ctx, env, a)
			if err != nil {
				return err
			}
			if _, err := pitrPlannedFailover(ctx, env, b, false); err != nil {
				return err
			}
			// b's era (marker M4, fence T4, witness M4b, the writer's
			// commits) is never sealed while b is writable. The writer
			// stops before the reclone.
			if err := s68Era(ctx, env, b, 8, "M4", "T4", false); err != nil {
				return err
			}
			after, err := pitrSiteGTID(ctx, env, b)
			if err != nil {
				return err
			}
			tail, err := gtidSubtract(after, before)
			if err != nil {
				return err
			}
			if tail == "" {
				return fmt.Errorf("b executed nothing in cycle 2's era (%s ⊆ %s)", after, before)
			}
			env.Capture.Note("cycle 2: b's unflushed era is " + tail)
			// The reclone wipes a's binlogs, and with them a's replica
			// copies of that era: from here to the failback it exists only
			// in b's active binlog.
			if err := pitrRecloneSite(ctx, env, a, false); err != nil {
				return err
			}
			// The failback must archive it: the operator's rotation of the
			// fenced b seals it and b's sidecar uploads the sealed file.
			if _, err := pitrPlannedFailoverArchivingTail(ctx, env, a, false); err != nil {
				return err
			}
			return s68Era(ctx, env, a, 10, "M5", "T5", true)
		},
	}
}

func s68VerifyCycle2Tail() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "cycle 2: β1 verifies exactly inside b's unflushed era (T4) and after it (T5)",
		Do: func(ctx context.Context, env *runner.Env) error {
			// Both replays cross b's cycle-2 era, whose only archived copy
			// is the tail b's sidecar uploaded at the failback; T4's witness
			// is in that tail too. Before #264 the T5 verification refused
			// PITRArchiveGap naming exactly that era.
			cases := []struct {
				label, fence string
				markers      []string
			}{
				{"c2-t4", "T4", []string{"baseline", "M1", "M1b", "M2", "M2b", "M3", "M3b", "M4"}},
				{"c2-t5", "T5", []string{"baseline", "M1", "M1b", "M2", "M2b", "M3", "M3b", "M4", "M4b", "M5"}},
			}
			for _, c := range cases {
				f, err := pitrFetchFence(env, c.fence)
				if err != nil {
					return err
				}
				if err := pitrVerify(ctx, env, s68ID, s68DBName, c.label, pitrExpectation{
					Backup: "b1", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered, Markers: c.markers,
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func s68VerifyNewBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "a backup taken after the failback covers the history again",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			if err := pitrBackup(ctx, env, s68ID, "b2"); err != nil {
				return err
			}
			if err := s68Era(ctx, env, a, 12, "M6", "T6", true); err != nil {
				return err
			}
			f, err := pitrFetchFence(env, "T6")
			if err != nil {
				return err
			}
			return pitrVerify(ctx, env, s68ID, s68DBName, "c2-b2", pitrExpectation{
				Backup: "b2", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered,
				Markers: []string{"baseline", "M1", "M1b", "M2", "M2b", "M3", "M3b", "M4", "M4b", "M5", "M5b", "M6"},
			})
		},
	}
}
