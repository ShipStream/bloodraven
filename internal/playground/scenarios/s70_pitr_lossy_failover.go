package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario70PITRLossyFailover())
}

const (
	s70ID     = "70-pitr-lossy-failover"
	s70DBName = "chaos_s70_pitr"
	// s70SourceDelay holds the lost markers in b's relay log past the
	// failover's 30 s relay-log drain, so b is promoted without them. The
	// operator auto-recovers a stopped SQL thread (see s14), so the
	// applier is delayed instead of stopped; the effect for the archive is
	// the same: a's archived L never reaches b.
	s70SourceDelay = 90 * time.Second
)

// scenario70PITRLossyFailover is DESIGN §9.4 s70 and §4.15 Examples 5, 8(c)
// and 9 (O2-5): a lossy failover whose lost tail L was archived by a. It
// covers loss evidence in latest mode, the D window and MBV excludeGtids
// in timestamp mode, a backup after the loss needing no exclusion, and the
// D-46 fail-closed path (reclone + failback reissues L's GTIDs:
// PITRArchiveReissueRisk, then PITRArchiveHistoryReset until a new prefix
// and a full backup).
func scenario70PITRLossyFailover() runner.Scenario {
	return runner.Scenario{
		ID:    s70ID,
		Title: "PITR after a lossy failover: lost set, excludeGtids, reissue risk, history reset and remedy",
		Hypothesis: "After a failover that loses archived transactions L: latest refuses PITRArchiveDivergentHistory with L " +
			"and verifies with excludeGtids=L; a timestamp verification of the old backup after new writes refuses the " +
			"same way and verifies with excludeGtids; a backup taken after the loss verifies without exclusions; " +
			"recloning the old primary and failing back to it emits PITRArchiveReissueRisk, its new writes make every " +
			"verification refuse PITRArchiveHistoryReset, and a new archive prefix plus a full backup verifies again.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#70-pitr-lossy-failover",
		Timeout:           75 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s70Configure(),
			s70Backup(),
			s70LossyFailover(),
			s70VerifyLatestLoss(),
			s70VerifyTimestampAfterLoss(),
			s70VerifyBackupAfterLoss(),
			s70RecloneAndFailback(),
			s70VerifyHistoryReset(),
			s70Remedy(),
		},
		Cleanup: s70Cleanup,
	}
}

func s70Configure() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure a plaintext RustFS PITR archive",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := pitrConfigureArchive(ctx, env, s70ID, "s70", "", false); err != nil {
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

func s70Backup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed the marker table and take backup β1",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			if err := pitrCreateMarkerTable(ctx, env, a, s70DBName); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s70DBName, 1, "baseline"); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			return pitrBackup(ctx, env, s70ID, "b1")
		},
	}
}

func s70LossyFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "delay b's applier, archive lost markers L on a, scale a to 0, b promoted without L",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			replica, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, b, env.Creds)
			if err != nil {
				return err
			}
			err = setSourceDelay(ctx, replica, int(s70SourceDelay.Seconds()))
			_ = replica.Close()
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "delayApplied", "true"); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s70DBName, 2, "L1"); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s70DBName, 3, "L2"); err != nil {
				return err
			}
			// Archive L, then take a down before b's delayed applier
			// reaches it.
			flushed, err := pitrTakeFence(ctx, env, a, true)
			if err != nil {
				return err
			}
			if err := pitrStashFence(ctx, env, "A-flush", flushed); err != nil {
				return err
			}
			killed := time.Now()
			if err := env.Chaos.ScaleSiteToZero(ctx, a); err != nil {
				return err
			}
			foCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			defer cancel()
			if _, err := env.Wait.UntilCR(foCtx, env.Namespace, "failover to "+b, func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				return m.Status.ActiveSite == b, "active=" + m.Status.ActiveSite, nil
			}); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "failedOver", "true"); err != nil {
				return err
			}
			if _, err := pitrWaitPromotionRecorded(ctx, env, b, killed); err != nil {
				return err
			}
			// b can never receive L now (a is down and b's channel was
			// reset at promotion), so L = what a archived minus what b has.
			bSet, err := pitrSiteGTID(ctx, env, b)
			if err != nil {
				return err
			}
			lost, err := gtidSubtract(flushed.GTID, bSet)
			if err != nil {
				return err
			}
			if lost == "" {
				return fmt.Errorf("the failover lost nothing (b has %s ⊇ a's archived %s); the delay did not hold L back", bSet, flushed.GTID)
			}
			env.Capture.Note(fmt.Sprintf("lossy failover to %s: lost archived set L=%s", b, lost))
			return ctxStash(ctx, env, "lost", lost)
		},
	}
}

func s70VerifyLatestLoss() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "latest refuses PITRArchiveDivergentHistory with L; with excludeGtids=L it verifies",
		Do: func(ctx context.Context, env *runner.Env) error {
			flushed, err := pitrFetchFence(env, "A-flush")
			if err != nil {
				return err
			}
			lost := ctxFetch(env, "lost")
			uuid := strings.SplitN(lost, ":", 2)[0]
			if _, err := pitrVerifyRefused(ctx, env, s70ID, s70DBName, "latest-lost", pitrExpectation{
				Backup: "b1", Mode: "latest", Fence: flushed, Markers: []string{"baseline"},
			}, "PITRArchiveDivergentHistory", uuid); err != nil {
				return err
			}
			return pitrVerify(ctx, env, s70ID, s70DBName, "latest-x", pitrExpectation{
				Backup: "b1", Mode: "latest", Fence: flushed, Exclude: lost, Outcome: v1alpha1.PointInTimeArchiveHead,
				Markers: []string{"baseline"},
			})
		},
	}
}

func s70VerifyTimestampAfterLoss() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "b writes S; β1 after S refuses DivergentHistory, and verifies with excludeGtids=L",
		Do: func(ctx context.Context, env *runner.Env) error {
			b := ctxFetch(env, "siteB")
			if err := pitrInsertMarker(ctx, env, b, s70DBName, 4, "S"); err != nil {
				return err
			}
			f, err := pitrTakeFence(ctx, env, b, false)
			if err != nil {
				return err
			}
			if err := pitrStashFence(ctx, env, "TS", f); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, b, s70DBName, 5, "Sb"); err != nil {
				return err
			}
			if err := pitrFlush(ctx, env, b, true); err != nil {
				return err
			}
			lost := ctxFetch(env, "lost")
			e := pitrExpectation{Backup: "b1", Mode: "timestamp", Fence: f, Markers: []string{"baseline", "S"}}
			if _, err := pitrVerifyRefused(ctx, env, s70ID, s70DBName, "ts-lost", e, "PITRArchiveDivergentHistory", strings.SplitN(lost, ":", 2)[0]); err != nil {
				return err
			}
			e.Exclude, e.Outcome = lost, v1alpha1.PointInTimeTargetCovered
			if err := pitrVerifyExcluded(ctx, env, "ts-x", e); err != nil {
				return err
			}
			return nil
		},
	}
}

// pitrVerifyExcluded is pitrVerify plus the excluded-divergent count: every
// GTID of L lies in the D window of a target after the loss.
func pitrVerifyExcluded(ctx context.Context, env *runner.Env, label string, e pitrExpectation) error {
	if err := pitrVerify(ctx, env, s70ID, s70DBName, label, e); err != nil {
		return err
	}
	name := fmt.Sprintf("%s-%s", pitrShortName(s70ID, label), backupRunStamp(env))
	v, err := getMysqlBackupVerification(ctx, env, name)
	if err != nil {
		return err
	}
	want, err := gtidCount(e.Exclude)
	if err != nil {
		return err
	}
	if got := v.Status.PointInTime.ExcludedDivergentTransactionCount; got != want {
		return fmt.Errorf("verification %s: excludedDivergentTransactionCount=%d, want %d (|L|)", name, got, want)
	}
	return nil
}

func s70VerifyBackupAfterLoss() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "a backup taken after the loss verifies without exclusions",
		Do: func(ctx context.Context, env *runner.Env) error {
			b := ctxFetch(env, "siteB")
			if err := pitrBackup(ctx, env, s70ID, "b2"); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, b, s70DBName, 6, "S2"); err != nil {
				return err
			}
			f, err := pitrTakeFence(ctx, env, b, false)
			if err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, b, s70DBName, 7, "S2b"); err != nil {
				return err
			}
			if err := pitrFlush(ctx, env, b, true); err != nil {
				return err
			}
			return pitrVerify(ctx, env, s70ID, s70DBName, "b2", pitrExpectation{
				Backup: "b2", Mode: "timestamp", Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered,
				Markers: []string{"baseline", "S", "Sb", "S2"},
			})
		},
	}
}

func s70RecloneAndFailback() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "bring a back, reclone it, fail back: PITRArchiveReissueRisk",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			if err := env.Chaos.ScaleSiteToOne(ctx, a); err != nil {
				return err
			}
			if err := pitrRecloneSite(ctx, env, a); err != nil {
				return err
			}
			// a's own manifest still holds the archived row with L, which
			// its recloned server no longer has: the audit flags it, which
			// is what the runner's reissue warning keys on.
			if err := s70WaitSuspectRows(ctx, env, a); err != nil {
				return err
			}
			requested := time.Now()
			if _, err := pitrPlannedFailover(ctx, env, a); err != nil {
				return err
			}
			evCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			ev, err := waitForMFGEvent(evCtx, env, requested, "PITRArchiveReissueRisk", "site "+a)
			if err != nil {
				return err
			}
			if ev.Type != "Warning" {
				return fmt.Errorf("PITRArchiveReissueRisk event type=%s, want Warning", ev.Type)
			}
			env.Capture.Note("reissue risk emitted: " + ev.Message)
			return nil
		},
	}
}

func s70WaitSuspectRows(ctx context.Context, env *runner.Env, site string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var last string
	for {
		probe, err := pgsidecar.Open(waitCtx, env.Kube, env.Namespace, env.FG, site)
		if err != nil {
			last = err.Error()
		} else {
			st, err := probe.ArchiverStatus(waitCtx)
			probe.Close()
			if err != nil {
				last = err.Error()
			} else {
				last = fmt.Sprintf("divergentRows=%d quarantinedRows=%d %s", st.DivergentRows, st.QuarantinedRows, archiverSummary(st))
				if st.DivergentRows+st.QuarantinedRows > 0 {
					env.Capture.Note("recloned " + site + " flags its lost rows: " + last)
					return nil
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("recloned %s never flagged its archived lost rows: %w (last: %s)", site, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

func s70VerifyHistoryReset() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "a's new writes reissue L's GTIDs: every verification refuses PITRArchiveHistoryReset",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			if err := pitrInsertMarker(ctx, env, a, s70DBName, 8, "R"); err != nil {
				return err
			}
			// The upload is what records the reuse (DESIGN O2-5). The
			// refusal below is the assertion; an archiver that reports the
			// reuse instead of uploading is noted, not failed here.
			if err := pitrFlush(ctx, env, a, false); err != nil {
				env.Capture.Note("flush after reissue: " + err.Error())
			}
			_, err := pitrVerifyRefused(ctx, env, s70ID, s70DBName, "reset", pitrExpectation{
				Backup: "b2", Mode: "latest", Markers: []string{"baseline"},
			}, "PITRArchiveHistoryReset")
			return err
		},
	}
}

func s70Remedy() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the documented remedy: a new archive prefix and a full backup verify again",
		Do: func(ctx context.Context, env *runner.Env) error {
			old := ctxFetch(env, "archiveID")
			if err := pitrConfigureArchive(ctx, env, s70ID, "s70", "fresh", false); err != nil {
				return err
			}
			if ctxFetch(env, "archiveID") == old {
				return fmt.Errorf("the new prefix kept archive ID %s", old)
			}
			// The prefix change rolls the pods; an ordered update may move
			// the primary, so write on whichever site is active now.
			a, _, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := pitrBackup(ctx, env, s70ID, "b3"); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s70DBName, 9, "F"); err != nil {
				return err
			}
			f, err := pitrTakeFence(ctx, env, a, false)
			if err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s70DBName, 10, "Fb"); err != nil {
				return err
			}
			if err := pitrFlush(ctx, env, a, true); err != nil {
				return err
			}
			return pitrVerify(ctx, env, s70ID, s70DBName, "fresh", pitrExpectation{
				Backup: "b3", Mode: "timestamp", Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered,
				Markers: []string{"baseline", "S", "Sb", "S2", "S2b", "R", "F"},
			})
		},
	}
}

func s70Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	// The ScaleSiteToZero reverter has brought a back. If the run stopped
	// before the failover, b still carries the applier delay; if it stopped
	// before the reclone, a is RecoveryBlocked.
	if ctxFetch(env, "delayApplied") != "" && ctxFetch(env, "failedOver") == "" {
		if c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, ctxFetch(env, "siteB"), env.Creds); err == nil {
			if err := clearSourceDelay(ctx, c); err != nil {
				errs = append(errs, err)
			}
			_ = c.Close()
		} else {
			errs = append(errs, err)
		}
	}
	if err := repairDivergentSites(ctx, env, 12*time.Minute); err != nil {
		errs = append(errs, err)
	}
	if err := pitrCleanup(ctx, env, s70DBName); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("s70 cleanup: %w", err)
	}
	return nil
}
