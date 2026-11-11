package scenarios

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	// the same: a's archived L never reaches b. The delay must outlast
	// everything between L's first commit and b's promotion: the writer
	// window (seconds), the flushing fence's upload wait (up to 3 min,
	// pitrWaitArchiverAdvanced), the second lost batch L' and its tail
	// watch (seconds), failure detection (~6 s) and the bounded drain
	// (30 s): 5 min leaves most of a minute of margin. The drain is bounded
	// whatever the delay, and promotion resets b's channel, so a longer
	// delay does not slow the failover.
	s70SourceDelay = 5 * time.Minute
	// Marker ids, in history order (pitrMarkerQuery lists phases by id):
	// baseline 1, P (kept by b) 2, L1/L2 (lost) 3-4, S/Sb 5-6, S2/S2b
	// 7-8, R/Rb (a's era after the failback) 9-10. L3, the lost marker a
	// never flushed, is in no history any verification expects; its id is
	// clear of the others.
	s70IDPreLoss  = 2
	s70IDLost     = 3
	s70IDS        = 5
	s70IDS2       = 7
	s70IDFailback = 9
	s70IDLostTail = 20
	// s70ReissueWarningWait is how long after a failback has succeeded
	// the scenario still looks for a PITRArchiveReissueRiskDetected Event.
	// The operator emits it with the status write that moves the active
	// site, which the failback wait has already seen; this covers the
	// Event reaching the API.
	s70ReissueWarningWait = 15 * time.Second
	// s70RepairTimeout bounds cleanup's reclone of a RecoveryBlocked site.
	s70RepairTimeout = 12 * time.Minute
	// pitrOperatorArchiverPollInterval mirrors the operator's
	// archiverPollInterval (internal/controller/archiver_poller.go): how
	// often it polls each sidecar's /archiver/status.
	pitrOperatorArchiverPollInterval = 30 * time.Second
)

// scenario70PITRLossyFailover is DESIGN §9.4 s70 and §4.15 Examples 5, 8(c)
// and 9 (O2-5): a lossy failover whose lost tail L was archived by a. It
// covers loss evidence in latest mode, the D window and MBV excludeGtids
// in timestamp mode, a backup after the loss needing no exclusion, and
// what a reclone of the old primary and a failback to it do to the
// archive.
//
// Until #265 that last part was the D-46 fail-closed path: the recloned a
// kept its server_uuid, its new writes reissued L's GTIDs
// (PITRArchiveReissueRiskDetected), and every restore refused
// PITRArchiveHistoryReset until a new prefix and a full backup. A clone
// now gives its recipient a new server_uuid, so the scenario asserts the
// opposite: a's lost rows stay in the archive as divergent history that
// is evidence and not an archiver error, the failback raises no reissue
// warning, a numbers its new era under the new server_uuid, no GTID of
// the lost set exists on a again, and the archive stays usable. The old
// backup, restored past the failback, still refuses naming exactly the
// lost set and verifies with it excluded; a backup taken after the loss
// verifies with no exclusion. The fail-closed path remains for a server
// that lost history and kept its server_uuid (a data directory restored or
// reset without a clone, a clone by an earlier release); unit and
// component tests cover it, no playground scenario produces it.
//
// It also covers the lost transactions a had not archived when it died
// (#264). a commits a second lost batch L' after its last flush; while a is
// down the archive knows only L. When a comes back its sidecar, read-only,
// archives the binary log that held L' (the restart sealed it), so the old
// backup's verification then refuses naming L ∪ L' and verifies with both
// excluded; before #264 the reclone wiped L' unseen. The reclone of a (and
// of every reader) starts only after the operator confirmed the site's
// tail archived, with no PITRArchiveTailNotArchived Event (pitrRecloneSite).
func scenario70PITRLossyFailover() runner.Scenario {
	return runner.Scenario{
		ID:    s70ID,
		Title: "PITR after a lossy failover: lost set, excludeGtids, and a reclone and failback that leave the archive usable",
		Hypothesis: "After a failover that loses archived transactions L: latest refuses PITRArchiveDivergentHistory with L " +
			"and verifies with excludeGtids=L; a timestamp verification of the old backup after new writes refuses the " +
			"same way and verifies with excludeGtids; a backup taken after the loss verifies without exclusions; " +
			"when the old primary comes back its sidecar archives the lost transactions L' it had not archived, and the " +
			"old backup's verification refuses with L ∪ L' and verifies with both excluded; " +
			"recloning the old primary (after the operator confirmed its binlog tail archived) gives it a new server_uuid, " +
			"its archived lost rows are divergent evidence and not an archiver error, and failing back to it emits no " +
			"PITRArchiveReissueRiskDetected; its new writes are numbered under the new server_uuid, reissue no GTID of " +
			"L ∪ L', and no verification refuses PITRArchiveHistoryReset: the old backup past the failback refuses " +
			"PITRArchiveDivergentHistory with L ∪ L' and verifies with both excluded, and the backup taken after the loss " +
			"verifies without exclusions.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#70-pitr-lossy-failover",
		Timeout:           75 * time.Minute,
		CleanupTimeout:    pitrCleanupBudget(s70RepairTimeout),
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s70Configure(),
			s70Backup(),
			s70LossyFailover(),
			s70VerifyLatestLoss(),
			s70VerifyTimestampAfterLoss(),
			s70VerifyBackupAfterLoss(),
			s70OldPrimaryReturns(),
			s70RecloneAndFailback(),
			s70VerifyAfterFailback(),
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
		Name:  "era P reaches b; delay b's applier, archive lost markers L on a, write L' unflushed, scale a to 0, b promoted without either; reclone readers holding L",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			// The emergency failover below must not wait out an anti-flap
			// cooldown (s70Configure's rollout may hand the primary off):
			// L's first commit to b's promotion has to stay well inside
			// s70SourceDelay.
			if err := pitrWaitCooldown(ctx, env); err != nil {
				return err
			}
			// P is history b keeps: written on a and applied by b before
			// the delay. Without it a's archive holds nothing beyond β1
			// but L, so latest with excludeGtids=L has nothing to replay
			// and refuses PITRNoArchivedReplay (planner latest(): Replay
			// = Λ ∖ B ∖ X = ∅).
			if err := pitrWithWriter(ctx, env, a, s70DBName, func() error {
				return pitrInsertMarker(ctx, env, a, s70DBName, s70IDPreLoss, "P")
			}); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			replica, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, b, env.Creds)
			if err != nil {
				return err
			}
			// Stashed first: a partly applied delay (STOP and CHANGE done,
			// START failed) must be cleaned up too. Cleanup checks b's live
			// state before touching it (F193).
			if err := ctxStash(ctx, env, "delayPending", "true"); err != nil {
				_ = replica.Close()
				return err
			}
			err = setSourceDelay(ctx, replica, int(s70SourceDelay.Seconds()))
			_ = replica.Close()
			if err != nil {
				return err
			}
			// L is the two markers plus the writer's commits around them.
			// The writer stops before the flushing fence, so a's sealed
			// file holds all of it.
			if err := pitrWithWriter(ctx, env, a, s70DBName, func() error {
				if err := pitrInsertMarker(ctx, env, a, s70DBName, s70IDLost, "L1"); err != nil {
					return err
				}
				return pitrInsertMarker(ctx, env, a, s70DBName, s70IDLost+1, "L2")
			}); err != nil {
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
			// L' is a second lost batch, committed after the flush: it stays
			// in a's active binlog, so nothing archives it before a goes
			// down. It reaches the archive when a comes back
			// (s70OldPrimaryReturns). The watch records the file that holds
			// it and a's manifest rows as they are now.
			if err := pitrWithWriter(ctx, env, a, s70DBName, func() error {
				return pitrInsertMarker(ctx, env, a, s70DBName, s70IDLostTail, "L3")
			}); err != nil {
				return err
			}
			tail, err := pitrStartTailWatch(ctx, env, a)
			if err != nil {
				return err
			}
			if err := pitrStashTailWatch(ctx, env, "a", tail); err != nil {
				return err
			}
			// Read on b's clock, which b's sidecar records with (F182).
			killed, err := pitrSiteNow(ctx, env, b)
			if err != nil {
				return err
			}
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
			if _, err := pitrWaitPromotionRecorded(ctx, env, b, killed, false); err != nil {
				return err
			}
			// b can never receive L now (a is down and b's channel was
			// reset at promotion), so L = what a archived minus what b has.
			// L' is not in a's archived set: the archive does not know it
			// until a comes back.
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
			if err := ctxStash(ctx, env, "lost", lost); err != nil {
				return err
			}
			return s70RecloneReadersHolding(ctx, env, lost)
		},
	}
}

// s70RecloneReadersHolding reclones every reader (read-only role) site that
// executed any of L. A reader replicates straight from a with no delay, so
// it applied L before a went down and now holds transactions b lacks: the
// operator refuses to repoint it (SourceConvergenceBlocked) and its replica
// GTID audit reports it RecoveryBlocked with L. Nothing but a reclone ends
// that: no later history contains L (a's writes after its failback are
// numbered under a new server_uuid), so left alone the reader would stay
// without replication through the failback and the rest of the run.
// Recloning it from b now is the documented remedy for a replica that
// holds a lost tail.
func s70RecloneReadersHolding(ctx context.Context, env *runner.Env, lost string) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	for _, r := range pitrReaderSites(mfg) {
		set, err := pitrSiteGTID(ctx, env, r)
		if err != nil {
			return fmt.Errorf("read reader %s: %w", r, err)
		}
		held, err := gtidOverlap(lost, set)
		if err != nil {
			return err
		}
		if held == "" {
			env.Capture.Note(fmt.Sprintf("reader %s holds none of L (%s); left in place", r, set))
			continue
		}
		env.Capture.Note(fmt.Sprintf("reader %s holds %s of the lost set; recloning it from the new primary", r, held))
		if err := pitrRecloneSite(ctx, env, r, true); err != nil {
			return fmt.Errorf("reclone reader %s, which holds the lost %s: %w", r, held, err)
		}
	}
	return nil
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
			if err := s70VerifyRefusedNamingL(ctx, env, "latest-lost", pitrExpectation{
				Backup: "b1", Mode: v1alpha1.PointInTimeModeLatest, Fence: flushed, Markers: []string{"baseline", "P"},
			}, lost); err != nil {
				return err
			}
			// excludedDivergentTransactionCount is |D|, the timestamp-mode
			// D window; latest mode's loss evidence is not counted there, so
			// latest-x asserts L through gtidExecuted = A-flush ∖ L exactly,
			// which replays era P.
			return pitrVerify(ctx, env, s70ID, s70DBName, "latest-x", pitrExpectation{
				Backup: "b1", Mode: v1alpha1.PointInTimeModeLatest, Fence: flushed, Exclude: lost, Outcome: v1alpha1.PointInTimeArchiveHead,
				Markers: []string{"baseline", "P"},
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
			f, err := s70Era(ctx, env, b, s70IDS, "S")
			if err != nil {
				return err
			}
			if err := pitrStashFence(ctx, env, "TS", f); err != nil {
				return err
			}
			if err := pitrFlush(ctx, env, b, true); err != nil {
				return err
			}
			lost := ctxFetch(env, "lost")
			e := pitrExpectation{Backup: "b1", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Markers: []string{"baseline", "P", "S"}}
			if err := s70VerifyRefusedNamingL(ctx, env, "ts-lost", e, lost); err != nil {
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

// s70Era writes marker phase (id) on site, fences T and writes the witness
// marker phase+"b" (id+1) while a background writer commits on site, and
// returns the fence. The writer is stopped before the caller flushes.
func s70Era(ctx context.Context, env *runner.Env, site string, id int, phase string) (pitrFence, error) {
	var f pitrFence
	err := pitrWithWriter(ctx, env, site, s70DBName, func() error {
		if err := pitrInsertMarker(ctx, env, site, s70DBName, id, phase); err != nil {
			return err
		}
		var err error
		if f, err = pitrTakeFence(ctx, env, site, false); err != nil {
			return err
		}
		return pitrInsertMarker(ctx, env, site, s70DBName, id+1, phase+"b")
	})
	return f, err
}

// s70VerifyRefusedNamingL runs a verification that must refuse
// PITRArchiveDivergentHistory and print exactly L (as a GTID set), not just
// a set of a's UUID.
func s70VerifyRefusedNamingL(ctx context.Context, env *runner.Env, label string, e pitrExpectation, lost string) error {
	v, err := pitrVerifyRefused(ctx, env, s70ID, s70DBName, label, e, string(v1alpha1.PITRTokenArchiveDivergentHistory), strings.SplitN(lost, ":", 2)[0])
	if err != nil {
		return err
	}
	cond := findCondition(v.Status.Conditions, v1alpha1.ConditionVerified)
	if err := refusalNamesExactly(cond.Message, lost); err != nil {
		return fmt.Errorf("verification %s: %w", v.Name, err)
	}
	return nil
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
			f, err := s70Era(ctx, env, b, s70IDS2, "S2")
			if err != nil {
				return err
			}
			if err := pitrFlush(ctx, env, b, true); err != nil {
				return err
			}
			return pitrVerify(ctx, env, s70ID, s70DBName, "b2", pitrExpectation{
				Backup: "b2", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered,
				Markers: []string{"baseline", "P", "S", "Sb", "S2"},
			})
		},
	}
}

// s70ReturnWait bounds the wait for a's pod to be back and observed
// read-only after it is scaled up.
const s70ReturnWait = 4 * time.Minute

// s70OldPrimaryReturns brings a back and asserts what #264 changed for a
// primary that died with unarchived transactions: its sidecar archives them
// while a is read-only, before anything reclones a, and restores see them.
func s70OldPrimaryReturns() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "a comes back: its sidecar archives the unflushed lost tail L'; β1 after S refuses DivergentHistory with L ∪ L', and verifies with both excluded",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b := ctxFetch(env, "siteA"), ctxFetch(env, "siteB")
			tail, err := pitrFetchTailWatch(env, "a")
			if err != nil {
				return err
			}
			returned := time.Now()
			if err := env.Chaos.ScaleSiteToOne(ctx, a); err != nil {
				return err
			}
			backCtx, cancel := context.WithTimeout(ctx, s70ReturnWait)
			defer cancel()
			if _, err := env.Wait.UntilCR(backCtx, env.Namespace, a+" back and read-only", func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				for _, st := range m.Status.Sites {
					if st.Name == a {
						return st.State == v1alpha1.SiteStateReadOnly, fmt.Sprintf("state=%s recovery=%q", st.State, st.RecoveryState), nil
					}
				}
				return false, "site not in status", nil
			}); err != nil {
				return err
			}
			// The restart sealed the binary log a was writing when it went
			// down, which holds L'. a's latest record is still its own
			// promotion record and its server is the same incarnation, so
			// its sidecar uploads that file while a is read-only.
			if err := tail.wait(ctx, env, true, returned); err != nil {
				return fmt.Errorf("the returning old primary did not archive the transactions it had not archived when it went down: %w", err)
			}
			// Everything a holds that b does not: L, which a archived
			// before it went down, and L'.
			aSet, err := pitrSiteGTID(ctx, env, a)
			if err != nil {
				return err
			}
			bSet, err := pitrSiteGTID(ctx, env, b)
			if err != nil {
				return err
			}
			lostAll, err := gtidSubtract(aSet, bSet)
			if err != nil {
				return err
			}
			lostTail, err := gtidSubtract(lostAll, ctxFetch(env, "lost"))
			if err != nil {
				return err
			}
			if lostTail == "" {
				return fmt.Errorf("a holds nothing beyond L that b lacks (a %s, b %s): the unflushed lost batch L' did not happen", aSet, bSet)
			}
			env.Capture.Note(fmt.Sprintf("old primary %s returned: unflushed lost tail L'=%s now archived; L ∪ L'=%s", a, lostTail, lostAll))
			if err := ctxStash(ctx, env, "lostAll", lostAll); err != nil {
				return err
			}
			f, err := pitrFetchFence(env, "TS")
			if err != nil {
				return err
			}
			// The same verification as ts-lost, which named L alone while
			// a was down: the archive now holds L' as well.
			e := pitrExpectation{Backup: "b1", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Markers: []string{"baseline", "P", "S"}}
			if err := s70VerifyRefusedNamingL(ctx, env, "ts-lost-tail", e, lostAll); err != nil {
				return err
			}
			e.Exclude, e.Outcome = lostAll, v1alpha1.PointInTimeTargetCovered
			return pitrVerifyExcluded(ctx, env, "ts-x-tail", e)
		},
	}
}

func s70RecloneAndFailback() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "reclone a (its tail confirmed archived first): a new server_uuid, its lost rows are evidence and not an archiver error; fail back with no reissue warning",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			// a is RecoveryBlocked with L ∪ L' as its divergent set: the
			// reclone is confirmed by that GTID's prefix (F067). Its tail
			// is in the archive already, which the operator confirms after
			// its own rotation before it lets CLONE wipe a's binary logs.
			// The helper also requires that the clone gave a a new
			// server_uuid (#265).
			if err := pitrRecloneSite(ctx, env, a, true); err != nil {
				return err
			}
			// a's own manifest still holds the archived rows with L and
			// L', which its recloned server no longer has. Its audit
			// records them as divergent, which is what restores read. They
			// were issued under the server_uuid a no longer has, so a
			// cannot issue them again: nothing is reissuable and nothing
			// is an archiver error.
			if err := s70WaitDivergentEvidence(ctx, env, a); err != nil {
				return err
			}
			// The reissue warning reads the operator's cached poll of a,
			// not the sidecar: fail back only once a poll after the rows
			// appeared has run (F070), so that its absence below is the
			// operator's verdict on those rows and not a poll it had not
			// made yet.
			if err := s70WaitOperatorPolled(ctx, env, a); err != nil {
				return err
			}
			requested := time.Now()
			// No tolerance for an archiver error on a: the failback wait
			// requires a's archiver clean with its promotion recorded.
			if _, err := pitrPlannedFailover(ctx, env, a, false); err != nil {
				return err
			}
			return s70AssertNoReissueWarning(ctx, env, requested, a)
		},
	}
}

// s70ArchiverStatus reads site's /archiver/status once.
func s70ArchiverStatus(ctx context.Context, env *runner.Env, site string) (*pgsidecar.ArchiverStatusResponse, error) {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return nil, err
	}
	defer probe.Close()
	return probe.ArchiverStatus(ctx)
}

// s70WaitSuspectRows waits until site's sidecar reports divergent or
// quarantined rows in its own manifest.
func s70WaitSuspectRows(ctx context.Context, env *runner.Env, site string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var last string
	for {
		st, err := s70ArchiverStatus(waitCtx, env, site)
		if err != nil {
			last = err.Error()
		} else {
			last = fmt.Sprintf("divergentRows=%d quarantinedRows=%d %s", st.DivergentRows, st.QuarantinedRows, archiverSummary(st))
			if st.DivergentRows+st.QuarantinedRows > 0 {
				env.Capture.Note("recloned " + site + " flags its lost rows: " + last)
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("recloned %s never flagged its archived lost rows: %w (last: %s)", site, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

// s70DivergentEvidence judges one archiver status of the recloned old
// primary. done is true once the status is the expected one: divergent
// rows recorded, a verdict that none of their transactions can be issued
// again, and no archiver error. err is a state that cannot become that: a
// non-empty reissuable set means the server still has the server_uuid its
// lost transactions were issued under, and a quarantined row is not what
// the audit of a lost tail records.
func s70DivergentEvidence(st *pgsidecar.ArchiverStatusResponse) (done bool, msg string, err error) {
	if st == nil {
		return false, "no status", nil
	}
	reissuable := "<no verdict>"
	if st.ReissuableGTIDs != nil {
		reissuable = fmt.Sprintf("%q", *st.ReissuableGTIDs)
	}
	msg = fmt.Sprintf("divergentRows=%d quarantinedRows=%d reissuableGtids=%s %s", st.DivergentRows, st.QuarantinedRows, reissuable, archiverSummary(st))
	if st.ReissuableGTIDs != nil && *st.ReissuableGTIDs != "" {
		return false, msg, fmt.Errorf("the recloned site can reissue %s: its server kept the server_uuid its lost transactions were issued under (%s)", *st.ReissuableGTIDs, msg)
	}
	if st.QuarantinedRows > 0 {
		return false, msg, fmt.Errorf("the recloned site's manifest holds quarantined rows; a lost tail is recorded as divergent (%s)", msg)
	}
	return st.DivergentRows > 0 && st.ReissuableGTIDs != nil && st.LastError == "", msg, nil
}

// s70WaitDivergentEvidence waits until the recloned site's sidecar reports
// its archived lost rows as divergent evidence (s70DivergentEvidence).
func s70WaitDivergentEvidence(ctx context.Context, env *runner.Env, site string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var last string
	for {
		st, err := s70ArchiverStatus(waitCtx, env, site)
		if err != nil {
			last = err.Error()
		} else {
			done, msg, err := s70DivergentEvidence(st)
			if err != nil {
				return err
			}
			if last = msg; done {
				env.Capture.Note("recloned " + site + " records its lost rows as divergent evidence: " + msg)
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("recloned %s did not report its archived lost rows as divergent evidence with no archiver error and nothing reissuable: %w (last: %s)", site, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

// s70AssertNoReissueWarning requires that the promotion of site raised no
// PITRArchiveReissueRiskDetected Event since requested.
func s70AssertNoReissueWarning(ctx context.Context, env *runner.Env, requested time.Time, site string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s70ReissueWarningWait):
	}
	ev, found, err := findMFGEvent(ctx, env, requested, v1alpha1.EventPITRArchiveReissueRiskDetected, "site "+site)
	if err != nil {
		return fmt.Errorf("list events after the failback to %s: %w", site, err)
	}
	if found {
		return fmt.Errorf("the failback to the recloned %s raised %s although its server has a new server_uuid and can reissue nothing: %s", site, ev.Reason, ev.Message)
	}
	env.Capture.Note("failback to " + site + ": no " + v1alpha1.EventPITRArchiveReissueRiskDetected + " Event")
	return nil
}

// s70OperatorPollWait is how long after a sidecar first reports suspect
// rows an operator poll of that sidecar has certainly started and finished:
// two poll intervals plus slack for the poll itself.
const s70OperatorPollWait = 2*pitrOperatorArchiverPollInterval + 10*time.Second

// s70OperatorPollFailing reports whether status.pitr.message says the
// operator's last poll of site failed, in which case its cached suspect-row
// count may be stale.
func s70OperatorPollFailing(pitr *v1alpha1.PITRStatus, site string) bool {
	return pitr != nil && strings.Contains(pitr.Message, site+": archiver status unavailable")
}

// s70WaitOperatorPolled waits s70OperatorPollWait after the suspect rows were
// seen, then requires that the operator's latest poll of site succeeded and
// the sidecar still reports the rows. The operator publishes no suspect-row
// count, so the poll cadence is the synchronization: a poll started after
// the rows appeared sees them, since a manifest's divergent rows stay until
// quarantined.
func s70WaitOperatorPolled(ctx context.Context, env *runner.Env, site string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s70OperatorPollWait):
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "operator polls "+site+" successfully", func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		msg := "status.pitr unset"
		if m.Status.PITR != nil {
			msg = "status.pitr.message=" + m.Status.PITR.Message
		}
		return m.Status.PITR != nil && !s70OperatorPollFailing(m.Status.PITR, site), msg, nil
	}); err != nil {
		return err
	}
	return s70WaitSuspectRows(ctx, env, site)
}

// s70NoReissue checks a's executed set after its new era: it holds none of
// the lost set (a server that kept its server_uuid would have numbered the
// new era with the lost transactions' GTIDs), and the new era is numbered
// from 1 under a's current server_uuid.
func s70NoReissue(executed, lost, serverUUID string) error {
	again, err := gtidOverlap(lost, executed)
	if err != nil {
		return err
	}
	if again != "" {
		return fmt.Errorf("the recloned old primary executed %s of the lost set %s again: its new writes reissued those GTIDs (gtid_executed %s)", again, lost, executed)
	}
	own, err := gtidOverlap(serverUUID+":1", executed)
	if err != nil {
		return err
	}
	if own == "" {
		return fmt.Errorf("the recloned old primary's gtid_executed %s holds no transaction under its server_uuid %s: its new era was not numbered under the new identity", executed, serverUUID)
	}
	if strings.Contains(strings.ToLower(lost), strings.ToLower(serverUUID)+":") {
		return fmt.Errorf("the recloned old primary's server_uuid %s is one the lost set %s was issued under", serverUUID, lost)
	}
	return nil
}

func s70VerifyAfterFailback() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "a writes R under its new server_uuid: no lost GTID is reissued and nothing refuses PITRArchiveHistoryReset; β1 past the failback refuses DivergentHistory with L ∪ L' and verifies with both excluded; β2 verifies without exclusions",
		Do: func(ctx context.Context, env *runner.Env) error {
			a := ctxFetch(env, "siteA")
			lostAll := ctxFetch(env, "lostAll")
			if lostAll == "" {
				return fmt.Errorf("the lost set L ∪ L' was not recorded")
			}
			f, err := s70Era(ctx, env, a, s70IDFailback, "R")
			if err != nil {
				return err
			}
			// a archives its new era like any primary: the divergent rows
			// in its manifest are not an archiver error, so the upload is
			// clean.
			if err := pitrFlush(ctx, env, a, true); err != nil {
				return fmt.Errorf("the recloned old primary did not archive its new era cleanly: %w", err)
			}
			executed, err := pitrSiteGTID(ctx, env, a)
			if err != nil {
				return err
			}
			uuid, err := siteServerUUID(ctx, env, a)
			if err != nil {
				return err
			}
			if err := s70NoReissue(executed, lostAll, uuid); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("a's new era is under server_uuid %s; none of L ∪ L'=%s was issued again", uuid, lostAll))
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if cond := findCondition(mfg.Status.Conditions, v1alpha1.ConditionPITRArchiveReady); cond == nil || cond.Status != metav1.ConditionTrue {
				return fmt.Errorf("PITRArchiveReady is not True after the failback to the recloned old primary: %+v", cond)
			}

			// The old backup, restored past the failback: the lost set is
			// still divergent history inside its window, named exactly,
			// and with it excluded the restore crosses a's old era, b's
			// era and a's new era. A PITRArchiveHistoryReset here would be
			// a reissued GTID.
			markers := []string{"baseline", "P", "S", "Sb", "S2", "S2b", "R"}
			e := pitrExpectation{Backup: "b1", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Markers: markers}
			if err := s70VerifyRefusedNamingL(ctx, env, "fb-lost", e, lostAll); err != nil {
				return err
			}
			e.Exclude, e.Outcome = lostAll, v1alpha1.PointInTimeTargetCovered
			if err := pitrVerifyExcluded(ctx, env, "fb-x", e); err != nil {
				return err
			}
			// The backup taken after the loss: every lost transaction was
			// committed before its newest one, so none is in its window.
			return pitrVerify(ctx, env, s70ID, s70DBName, "fb-b2", pitrExpectation{
				Backup: "b2", Mode: v1alpha1.PointInTimeModeTimestamp, Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered,
				Markers: markers,
			})
		},
	}
}

// s70ShouldClearDelay decides from b's live state whether cleanup must
// reset SOURCE_DELAY: only while b is still a read-only replica whose
// channel carries a delay. A promoted b has no channel to change (F193).
func s70ShouldClearDelay(readOnly, hasChannel bool, delay int64) bool {
	return readOnly && hasChannel && delay > 0
}

// s70ReplicaDelay reads b's role and the default channel's configured
// SOURCE_DELAY.
func s70ReplicaDelay(ctx context.Context, c *pgmysql.SiteClient) (readOnly, hasChannel bool, delay int64, err error) {
	var ro int64
	if err := c.DB.QueryRowContext(ctx, "SELECT @@global.read_only").Scan(&ro); err != nil {
		return false, false, 0, fmt.Errorf("read read_only: %w", err)
	}
	err = c.DB.QueryRowContext(ctx, "SELECT DESIRED_DELAY FROM performance_schema.replication_applier_configuration WHERE CHANNEL_NAME = ''").Scan(&delay)
	if errors.Is(err, sql.ErrNoRows) {
		return ro != 0, false, 0, nil
	}
	if err != nil {
		return false, false, 0, fmt.Errorf("read SOURCE_DELAY: %w", err)
	}
	return ro != 0, true, delay, nil
}

func s70Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	// The ScaleSiteToZero reverter has brought a back. If the run stopped
	// before the failover, b may still carry the applier delay (judged
	// from b's live state); if it stopped before the reclone, a is
	// RecoveryBlocked.
	if ctxFetch(env, "delayPending") != "" {
		b := ctxFetch(env, "siteB")
		if c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, b, env.Creds); err == nil {
			readOnly, hasChannel, delay, err := s70ReplicaDelay(ctx, c)
			switch {
			case err != nil:
				errs = append(errs, err)
			case s70ShouldClearDelay(readOnly, hasChannel, delay):
				if err := clearSourceDelay(ctx, c); err != nil {
					errs = append(errs, err)
				} else {
					env.Capture.Note(fmt.Sprintf("cleared SOURCE_DELAY=%d on replica %s", delay, b))
				}
			}
			_ = c.Close()
		} else {
			errs = append(errs, err)
		}
	}
	if err := repairDivergentSites(ctx, env, s70RepairTimeout); err != nil {
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
