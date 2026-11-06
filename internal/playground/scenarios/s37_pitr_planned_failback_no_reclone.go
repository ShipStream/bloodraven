package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario37PITRPlannedFailbackNoReclone())
}

const (
	s37ID       = "37-pitr-planned-failback-no-reclone"
	s37DBName   = "chaos_s37_pitr"
	s37Restored = "s37-restored"
)

// scenario37PITRPlannedFailbackNoReclone is DESIGN §9.4 s37 and §4.15
// Example 3: a planned failover A→B and failback B→A without a reclone or a
// flush on b, so b's short era reaches the archive only as replica copies in
// a's mixed binlog. Restores inside b's era are anchored by b's promotion
// record. Every verification asserts the exact replayed history.
func scenario37PITRPlannedFailbackNoReclone() runner.Scenario {
	return runner.Scenario{
		ID:    s37ID,
		Title: "PITR across a planned failback without reclone restores every era exactly",
		Hypothesis: "With an encrypted RustFS archive and a background writer committing through every era, a planned A→B→A failback " +
			"without reclone and without a flush on b: before the failback a timestamp in b's era refuses " +
			"PITRTargetNotCovered and latest is ArchiveHead; after a's flush, timestamp verifications before the " +
			"failover, inside b's era (anchored by b's promotion record) and after the failback, a latest " +
			"verification, and an initFromBackup restore inside b's era each certify exactly the fenced history.",
		Risk:              "medium",
		DocLink:           "playground/chaos-scenarios.md#37-pitr-planned-failback-no-reclone",
		Timeout:           60 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s37Configure(),
			s37Backup(),
			s37EraA(),
			s37FailoverToB(),
			s37VerifyBetweenEras(),
			s37FailbackToA(),
			s37VerifyEveryEra(),
			s37RestoreInsideBEra(),
		},
		Cleanup: s37Cleanup,
	}
}

func s37Configure() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure an encrypted RustFS PITR archive",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := pitrConfigureArchive(ctx, env, s37ID, "s37", "", true); err != nil {
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

func s37Backup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed the marker table and take backup β1",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			if err := pitrCreateMarkerTable(ctx, env, a, s37DBName); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s37DBName, 1, "baseline"); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			return pitrBackup(ctx, env, s37ID, "b1")
		},
	}
}

func s37EraA() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "era A: marker M1, fence T1, witness M1b, flush a",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			if err := pitrWithWriter(ctx, env, a, s37DBName, func() error {
				return s37Era(ctx, env, a, 2, "M1", "T1")
			}); err != nil {
				return err
			}
			// Archives era A through M1b, so latest has something to replay
			// before b's era is archived.
			flushed, err := pitrTakeFence(ctx, env, a, true)
			if err != nil {
				return err
			}
			return pitrStashFence(ctx, env, "A-flush", flushed)
		},
	}
}

func s37FailoverToB() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "planned A→B (no flush); b's promotion record; marker M2, fence T2, witness M2b on b",
		Do: func(ctx context.Context, env *runner.Env) error {
			b := ctxFetch(env, "siteB")
			recorded, err := pitrPlannedFailover(ctx, env, b, false)
			if err != nil {
				return err
			}
			// The fence follows the record, so T2 is outside the
			// promotion-record lag window (D-53) and a replica-copy witness
			// is anchored by the record. The writer starts after the
			// record too, so no era-B commit is in the lag window.
			if err := pitrWithWriter(ctx, env, b, s37DBName, func() error {
				return s37Era(ctx, env, b, 4, "M2", "T2")
			}); err != nil {
				return err
			}
			f2, err := pitrFetchFence(env, "T2")
			if err != nil {
				return err
			}
			if t2, _ := time.Parse(pitrTargetLayout, f2.T); !t2.After(recorded) {
				return fmt.Errorf("T2 %s is not after b's promotion record %s", f2.T, recorded.Format(time.RFC3339Nano))
			}
			return nil
		},
	}
}

// s37Era writes marker phase (id) on site, fences T under key, and writes
// the witness marker phase+"b" (id+1). Callers run it inside a writer
// window.
func s37Era(ctx context.Context, env *runner.Env, site string, id int, phase, key string) error {
	if err := pitrInsertMarker(ctx, env, site, s37DBName, id, phase); err != nil {
		return err
	}
	f, err := pitrTakeFence(ctx, env, site, false)
	if err != nil {
		return err
	}
	if err := pitrStashFence(ctx, env, key, f); err != nil {
		return err
	}
	return pitrInsertMarker(ctx, env, site, s37DBName, id+1, phase+"b")
}

func s37VerifyBetweenEras() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "between failover and failback: T2 not covered, latest is the archive head",
		Do: func(ctx context.Context, env *runner.Env) error {
			f2, err := pitrFetchFence(env, "T2")
			if err != nil {
				return err
			}
			if _, err := pitrVerifyRefused(ctx, env, s37ID, s37DBName, "t2-early", pitrExpectation{
				Backup: "b1", Mode: "timestamp", Fence: f2,
				Markers: []string{"baseline", "M1", "M1b", "M2"},
			}, "PITRTargetNotCovered"); err != nil {
				return err
			}
			flushed, err := pitrFetchFence(env, "A-flush")
			if err != nil {
				return err
			}
			return pitrVerify(ctx, env, s37ID, s37DBName, "latest-early", pitrExpectation{
				Backup: "b1", Mode: "latest", Fence: flushed, Outcome: v1alpha1.PointInTimeArchiveHead,
				Markers: []string{"baseline", "M1", "M1b"},
			})
		},
	}
}

func s37FailbackToA() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "planned B→A (no flush on b); marker M3, fence T3, witness M3b, flush a",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			if _, err := pitrPlannedFailover(ctx, env, a, false); err != nil {
				return err
			}
			if err := pitrWithWriter(ctx, env, a, s37DBName, func() error {
				return s37Era(ctx, env, a, 6, "M3", "T3")
			}); err != nil {
				return err
			}
			// Seals a's mixed file: era A's tail (origin), b's era (replica
			// copies) and the start of era A' (origin).
			flushed, err := pitrTakeFence(ctx, env, a, true)
			if err != nil {
				return err
			}
			return pitrStashFence(ctx, env, "A2-flush", flushed)
		},
	}
}

func s37VerifyEveryEra() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "verify T1 (origin), T2 inside b's era (anchored), T3, and latest",
		Do: func(ctx context.Context, env *runner.Env) error {
			cases := []struct {
				label, fence, mode string
				outcome            v1alpha1.PointInTimeOutcome
				markers            []string
			}{
				{"t1", "T1", "timestamp", v1alpha1.PointInTimeTargetCovered, []string{"baseline", "M1"}},
				{"t2", "T2", "timestamp", v1alpha1.PointInTimeTargetCovered, []string{"baseline", "M1", "M1b", "M2"}},
				{"t3", "T3", "timestamp", v1alpha1.PointInTimeTargetCovered, []string{"baseline", "M1", "M1b", "M2", "M2b", "M3"}},
				{"latest", "A2-flush", "latest", v1alpha1.PointInTimeArchiveHead, []string{"baseline", "M1", "M1b", "M2", "M2b", "M3", "M3b"}},
			}
			for _, c := range cases {
				f, err := pitrFetchFence(env, c.fence)
				if err != nil {
					return err
				}
				if err := pitrVerify(ctx, env, s37ID, s37DBName, c.label, pitrExpectation{
					Backup: "b1", Mode: c.mode, Fence: f, Outcome: c.outcome, Markers: c.markers,
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func s37RestoreInsideBEra() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "initFromBackup restore into " + s37Restored + " at T2 inside b's era",
		Do: func(ctx context.Context, env *runner.Env) error {
			f2, err := pitrFetchFence(env, "T2")
			if err != nil {
				return err
			}
			if err := pitrCheckTargetAfterBackup(env, pitrExpectation{Backup: "b1", Mode: "timestamp", Fence: f2}); err != nil {
				return err
			}
			src, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			restored, err := pitrSourceSpecForRestore(src, s37Restored, ctxFetch(env, "backup:b1"), f2.T)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "restoredCreated", "true"); err != nil {
				return err
			}
			if err := env.Kube.Controller.Create(ctx, restored); err != nil {
				return fmt.Errorf("create %s: %w", s37Restored, err)
			}
			env.Capture.Note(fmt.Sprintf("created %s restoring backup %s to %s", s37Restored, ctxFetch(env, "backup:b1"), f2.T))
			rs, err := s37WaitRestore(ctx, env)
			if err != nil {
				return err
			}
			if err := assertPITRResult(env, "restore "+s37Restored, rs.PointInTime, pitrExpectation{
				Backup: "b1", Mode: "timestamp", Fence: f2, Outcome: v1alpha1.PointInTimeTargetCovered,
			}); err != nil {
				return err
			}
			c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, s37Restored, rs.TargetSite, env.Creds)
			if err != nil {
				return fmt.Errorf("open restored site %s: %w", rs.TargetSite, err)
			}
			defer c.Close()
			want := []string{"baseline", "M1", "M1b", "M2"}
			ok, err := c.ScalarInt(ctx, pitrMarkerQuery(s37DBName, ctxFetch(env, "backupRunStem"), want))
			if err != nil {
				return fmt.Errorf("read restored markers: %w", err)
			}
			if ok != 1 {
				return fmt.Errorf("restored %s marker rows are not exactly %v", s37Restored, want)
			}
			env.Capture.Note(fmt.Sprintf("%s holds exactly markers %v", s37Restored, want))
			return nil
		},
	}
}

func s37WaitRestore(ctx context.Context, env *runner.Env) (*v1alpha1.RestoreStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var last string
	for {
		mfg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, s37Restored)
		if err != nil {
			last = err.Error()
		} else if rs := mfg.Status.Restore; rs == nil {
			last = "status.restore is not set"
		} else {
			last = fmt.Sprintf("phase=%s reason=%s target=%s message=%q", rs.Phase, rs.Reason, rs.TargetSite, rs.Message)
			switch rs.Phase {
			case v1alpha1.BackupPhaseSucceeded:
				return rs, nil
			case v1alpha1.BackupPhaseFailed:
				if rs.JobName != "" {
					captureJobPodLogs(ctx, env, rs.JobName, "s37-restore")
				}
				return nil, fmt.Errorf("restore into %s failed: %s", s37Restored, last)
			}
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("restore into %s did not finish: %w (last: %s)", s37Restored, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

// pitrDeleteGroup deletes a scenario-created failover group and waits until
// it is gone; its Deployments, Services and PVCs are owned by it.
func pitrDeleteGroup(ctx context.Context, env *runner.Env, name string) error {
	mfg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace}}
	if err := env.Kube.Controller.Delete(ctx, mfg); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete %s: %w", name, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		err := env.Kube.Controller.Get(waitCtx, types.NamespacedName{Namespace: env.Namespace, Name: name}, &v1alpha1.MysqlFailoverGroup{})
		if apierrors.IsNotFound(err) {
			env.Capture.Note("deleted failover group " + name)
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("failover group %s not deleted: %w", name, waitCtx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// pitrCleanup removes what a PITR scenario created and waits for a healthy
// baseline with the original backup spec.
func pitrCleanup(ctx context.Context, env *runner.Env, db string, groups ...string) error {
	var errs []error
	for _, g := range groups {
		if err := pitrDeleteGroup(ctx, env, g); err != nil {
			errs = append(errs, err)
		}
	}
	for _, name := range pitrTrackedCRs(env, "MysqlBackupVerification") {
		if err := deleteBackupCRs(ctx, env, "", name); err != nil {
			errs = append(errs, err)
		}
	}
	for _, name := range pitrTrackedCRs(env, "MysqlBackup") {
		if err := deleteBackupCRs(ctx, env, name, ""); err != nil {
			errs = append(errs, err)
		}
	}
	if err := dropMarkerSchemaAndReplicate(ctx, env, db); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if err := deleteBackupPassphraseSecret(ctx, env); err != nil {
		errs = append(errs, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "PITR scenario cleanup healthy baseline", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ready := conditionTrue(mfg.Status.Conditions, "Ready")
		wantPITR, wantProfile, err := originalPITRExpected(env)
		if err != nil {
			return false, "", err
		}
		pitrRestored := mfg.Status.PITR == nil || (!mfg.Status.PITR.Enabled && !wantPITR)
		if wantPITR {
			pitrRestored = mfg.Status.PITR != nil && mfg.Status.PITR.Enabled && mfg.Status.PITR.ProfileName == wantProfile
		}
		var cooldown string
		if mfg.Spec.FailoverCooldown != nil && mfg.Status.LastFailover != nil {
			if rem := mfg.Spec.FailoverCooldown.Duration - time.Since(mfg.Status.LastFailover.Time); rem > 0 {
				cooldown = rem.Round(time.Second).String()
			}
		}
		done := ready && mfg.Status.ActiveSite != "" && mfg.Status.UpdatePhase == "" && pitrRestored && cooldown == ""
		return done, fmt.Sprintf("ready=%v active=%q pitrRestored=%v cooldown=%q", ready, mfg.Status.ActiveSite, pitrRestored, cooldown), nil
	}); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func s37Cleanup(ctx context.Context, env *runner.Env) error {
	var groups []string
	if ctxFetch(env, "restoredCreated") != "" {
		groups = append(groups, s37Restored)
	}
	if err := pitrCleanup(ctx, env, s37DBName, groups...); err != nil {
		return fmt.Errorf("s37 cleanup: %w", err)
	}
	return nil
}
