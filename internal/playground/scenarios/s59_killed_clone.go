package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	s59DB          = "chaos_s59"
	s59MarkerTable = s59DB + ".marker"
	s59FillerTable = s59DB + ".filler"
	// s59FillerMiB of incompressible rows, cloned at s59CloneMiBps,
	// keeps any clone of the candidate running for minutes: long enough
	// to kill the reclone after it has dropped the recipient's data, and
	// to keep the operator's automatic re-clone of the now-empty site
	// from finishing before the primary is taken away.
	s59FillerMiB  = 120
	s59CloneMiBps = 1
	// s59RefusalWindow is how long the empty candidate must stay
	// unpromoted with the primary down.
	s59RefusalWindow = 45 * time.Second
)

func init() {
	runner.Register(scenario59KilledClone())
}

type s59State struct {
	primary, candidate string
	marker             string
	recloneAt          time.Time
	primaryDownAt      time.Time
	failoversBefore    float64
}

// scenario59KilledClone covers the total-data-loss chain from the chaos
// round of 2026-10-15 (F12). The recipient's own sidecar fenced the
// recipient during a reclone and killed the operator's CLONE INSTANCE
// session; the operator treated the dropped connection as the expected
// post-clone restart and reported success on a site whose data the clone
// had already dropped. When the primary then died, the picker promoted
// the empty site.
//
// The scenario kills the clone itself, after the recipient's data is
// gone, and asserts both defences:
//   - the operator reports the bootstrap as failed, never as completed,
//     and the recipient stays super_read_only;
//   - with the primary down, the empty candidate is never promoted, and
//     the group recovers with no data loss when the primary returns.
func scenario59KilledClone() runner.Scenario {
	st := &s59State{}
	return runner.Scenario{
		ID:    "59-killed-clone-never-promoted",
		Title: "A killed reclone fails the bootstrap and the emptied site is never promoted",
		Hypothesis: "Killing the CLONE INSTANCE session of a candidate reclone after the recipient's data was dropped " +
			"makes the operator log 'bootstrap failed' (not 'bootstrap completed successfully') and leaves the " +
			"recipient super_read_only. With the primary then scaled to 0, the operator logs 'failover picker: " +
			"refusing candidate whose data regressed' and promotes nothing; when the primary returns it is active " +
			"again with the marker row intact, and the candidate is re-cloned.",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#59-killed-clone",
		Timeout:  20 * time.Minute,
		Precheck: AssertHealthyBaseline,
		// The filler table drop and bandwidth reset run here; a failed run
		// may also leave the candidate needing a clone.
		CleanupTimeout: 10 * time.Minute,
		Steps: []runner.Step{
			s59Seed(st),
			s59KillReclone(st),
			s59VerifyBootstrapFailed(st),
			s59KillPrimary(st),
			s59VerifyEmptyCandidateRefused(st),
			s59RestorePrimary(st),
			s59RepairCandidate(st),
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			if st.candidate == "" {
				return nil
			}
			return s59ResetCloneBandwidth(ctx, env, st.candidate)
		},
	}
}

func s59Seed(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  fmt.Sprintf("seed a marker row and %d MiB of filler on the primary", s59FillerMiB),
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			st.primary = mfg.Status.ActiveSite
			if st.candidate, err = PeerOf(mfg, st.primary); err != nil {
				return err
			}
			st.marker = fmt.Sprintf("s59-%d", time.Now().UnixNano())
			if err := seedMarkerRow(ctx, env, st.primary, s59MarkerTable, st.marker); err != nil {
				return err
			}
			client, err := env.MySQL(st.primary)
			if err != nil {
				return err
			}
			if _, err := client.Exec(ctx, "DROP TABLE IF EXISTS "+s59FillerTable+"; "+
				"CREATE TABLE "+s59FillerTable+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, pad VARBINARY(1024))"); err != nil {
				return fmt.Errorf("create filler table: %w", err)
			}
			// ~1 MiB per statement (1000 rows, under the default
			// cte_max_recursion_depth) keeps every transaction small.
			const oneMiB = "INSERT INTO " + s59FillerTable + " (pad) " +
				"WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 1000) " +
				"SELECT RANDOM_BYTES(1024) FROM seq"
			for i := 0; i < s59FillerMiB; i++ {
				if _, err := client.Exec(ctx, oneMiB); err != nil {
					return fmt.Errorf("write filler MiB %d: %w", i, err)
				}
			}
			if err := waitForMarkerOnSite(ctx, env, st.candidate, s59MarkerTable, st.marker, 2*time.Minute); err != nil {
				return err
			}
			// The candidate must have the filler too, or its clone would
			// be no slower than the playground's tiny default dataset.
			cand, err := env.MySQL(st.candidate)
			if err != nil {
				return err
			}
			deadline := time.Now().Add(3 * time.Minute)
			for {
				n, err := cand.ScalarInt(ctx, "SELECT COUNT(*) FROM "+s59FillerTable)
				if err == nil && n >= int64(s59FillerMiB*1000) {
					break
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("filler did not replicate to %s within 3m (rows=%d err=%v)", st.candidate, n, err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
			env.Capture.Note(fmt.Sprintf("primary=%s candidate=%s marker=%s", st.primary, st.candidate, st.marker))
			return nil
		},
	}
}

func s59KillReclone(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "throttle and request a reclone of the candidate, then kill CLONE INSTANCE after it drops the data",
		Do: func(ctx context.Context, env *runner.Env) error {
			cand, err := env.MySQL(st.candidate)
			if err != nil {
				return err
			}
			// Recipient-side setting; not persisted, and reset in cleanup.
			if _, err := cand.Exec(ctx, fmt.Sprintf("SET GLOBAL clone_max_data_bandwidth = %d", s59CloneMiBps)); err != nil {
				return fmt.Errorf("throttle clone on %s: %w", st.candidate, err)
			}
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			st.recloneAt = time.Now()
			value := fmt.Sprintf("%s:confirm=%s", st.candidate, env.FG)
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, "bloodraven.shipstream.io/reclone-site", value); err != nil {
				return fmt.Errorf("annotate reclone: %w", err)
			}
			env.Capture.Note("requested reclone-site=" + value)

			// Wait until the clone is past DROP DATA and copying files.
			deadline := time.Now().Add(2 * time.Minute)
			var cloneID int64
			for {
				// The processlist shows the CLONE session's info as NULL
				// (it carries a password); clone_status names it.
				id, idErr := cand.ScalarInt(ctx, "SELECT PID FROM performance_schema.clone_status "+
					"WHERE STATE = 'In Progress' LIMIT 1")
				copying, cpErr := cand.ScalarInt(ctx, "SELECT COUNT(*) FROM performance_schema.clone_progress "+
					"WHERE STAGE = 'FILE COPY' AND STATE = 'In Progress'")
				if idErr == nil && cpErr == nil && copying == 1 {
					cloneID = id
					break
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("no CLONE INSTANCE in FILE COPY on %s within 2m (id err=%v, progress err=%v)",
						st.candidate, idErr, cpErr)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
				}
			}
			if _, err := cand.Exec(ctx, fmt.Sprintf("KILL %d", cloneID)); err != nil {
				return fmt.Errorf("kill clone session %d on %s: %w", cloneID, st.candidate, err)
			}
			env.Capture.Note(fmt.Sprintf("killed CLONE INSTANCE session %d on %s during FILE COPY", cloneID, st.candidate))
			return nil
		},
	}
}

func s59VerifyBootstrapFailed(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "operator reports the bootstrap failed and the recipient stays fenced",
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			// The log is the durable record: the operator may start an
			// automatic re-clone of the now-empty site on a later poll and
			// overwrite the Bootstrapping condition.
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			m, _, err := env.Wait.UntilAnyLog(waitCtx, st.recloneAt, "outcome of the killed reclone",
				pglogs.Watch{Label: "failed", Tailer: tail, Pred: pglogs.Substring("bootstrap failed")},
				pglogs.Watch{Label: "completed", Tailer: tail, Pred: pglogs.Substring("bootstrap completed successfully")},
			)
			cancel()
			if err != nil {
				return err
			}
			if hit, line := firstMatchSince(tail, st.recloneAt, pglogs.Substring("bootstrap completed successfully")); hit {
				return fmt.Errorf("operator reported the killed clone as a successful bootstrap: %s", line)
			}
			env.Capture.Note("bootstrap outcome: " + m.Line)

			cand, err := env.MySQL(st.candidate)
			if err != nil {
				return err
			}
			sro, err := cand.SuperReadOnly(ctx)
			if err != nil {
				return fmt.Errorf("read super_read_only on %s: %w", st.candidate, err)
			}
			if !sro {
				return fmt.Errorf("recipient %s is not super_read_only after the failed clone", st.candidate)
			}
			gtid, err := cand.GtidExecuted(ctx)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("recipient %s after the killed clone: super_read_only=ON gtid_executed=%q", st.candidate, gtid))
			return nil
		},
	}
}

func s59KillPrimary(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale the primary to 0 while the candidate is empty",
		Do: func(ctx context.Context, env *runner.Env) error {
			snap, err := env.Metrics.Scrape(ctx)
			if err != nil {
				return fmt.Errorf("scrape metrics: %w", err)
			}
			st.failoversBefore, _ = snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": st.candidate})
			st.primaryDownAt = time.Now()
			return env.Chaos.ScaleSiteToZero(ctx, st.primary)
		},
	}
}

func s59VerifyEmptyCandidateRefused(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  fmt.Sprintf("empty candidate refused and not promoted for %s", s59RefusalWindow),
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			logCtx, cancel := context.WithDeadline(ctx, st.primaryDownAt.Add(s59RefusalWindow))
			m, err := env.Wait.UntilLog(logCtx, tail, st.primaryDownAt, "picker refuses the empty candidate",
				pglogs.Substring("failover picker: refusing candidate whose data regressed"))
			cancel()
			if err != nil {
				return fmt.Errorf("operator never refused the empty candidate: %w", err)
			}
			env.Capture.Note("refusal: " + m.Line)

			for time.Now().Before(st.primaryDownAt.Add(s59RefusalWindow)) {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				if mfg.Status.ActiveSite == st.candidate {
					return fmt.Errorf("empty candidate %s was promoted", st.candidate)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
			snap, err := env.Metrics.Scrape(ctx)
			if err != nil {
				return fmt.Errorf("scrape metrics: %w", err)
			}
			after, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": st.candidate})
			if after != st.failoversBefore {
				return fmt.Errorf("failovers_total{target_site=%s} moved %g -> %g while the candidate was empty",
					st.candidate, st.failoversBefore, after)
			}
			return nil
		},
	}
}

func s59RestorePrimary(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "primary returns as the active site with the marker row intact",
		Do: func(ctx context.Context, env *runner.Env) error {
			// No clone can run while the donor is down, so this is the
			// moment to lift the throttle for the repair clone.
			if err := s59ResetCloneBandwidth(ctx, env, st.candidate); err != nil {
				return err
			}
			if err := env.Chaos.ScaleSiteToOne(ctx, st.primary); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			defer cancel()
			if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "original primary active again",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					s := statusSiteByName(mfg, st.primary)
					state := ""
					if s != nil {
						state = s.State
					}
					return mfg.Status.ActiveSite == st.primary && state == "writable",
						fmt.Sprintf("activeSite=%q %s=%q", mfg.Status.ActiveSite, st.primary, state), nil
				}); err != nil {
				return err
			}
			client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.primary, env.Creds)
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			n, err := client.ScalarInt(ctx, "SELECT COUNT(*) FROM "+s59MarkerTable+" WHERE marker = ?", st.marker)
			if err != nil {
				return fmt.Errorf("read marker on %s: %w", st.primary, err)
			}
			if n != 1 {
				return fmt.Errorf("marker %s missing on the returned primary %s", st.marker, st.primary)
			}
			return nil
		},
	}
}

// s59RepairCandidate waits for the empty candidate to be cloned again.
// The operator does not retry a failed reclone on its own today: the
// recipient keeps its replication metadata, so empty-site auto-bootstrap
// skips it. If the Bootstrapping condition is still Failed a minute after
// the primary returned, the scenario requests the reclone itself.
func s59RepairCandidate(st *s59State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "candidate re-cloned and the group healthy",
		Do: func(ctx context.Context, env *runner.Env) error {
			start := time.Now()
			requested := false
			waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "candidate re-cloned, group healthy",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					boot := findCondition(mfg.Status.Conditions, "Bootstrapping")
					bootState := "<missing>"
					if boot != nil {
						bootState = boot.Reason
					}
					// A failed bootstrap reports Status=True, Reason=Failed.
					if !requested && time.Since(start) > time.Minute && boot != nil && boot.Reason == "Failed" {
						value := fmt.Sprintf("%s:confirm=%s", st.candidate, env.FG)
						if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
							"bloodraven.shipstream.io/reclone-site", value); err != nil {
							return false, "", fmt.Errorf("annotate repair reclone: %w", err)
						}
						requested = true
						env.Capture.Note("requested repair reclone-site=" + value)
					}
					if err := AssertHealthyBaseline(ctx, env); err != nil {
						return false, "boot=" + bootState + " " + err.Error(), nil
					}
					return boot == nil || boot.Reason != "Failed", "boot=" + bootState, nil
				})
			if err != nil {
				return err
			}
			// A fresh connection: the primary's pod restarted, so the
			// cached client's port-forward is gone.
			client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.primary, env.Creds)
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			_, err = client.Exec(ctx, "DROP TABLE IF EXISTS "+s59FillerTable)
			return err
		},
	}
}

func s59ResetCloneBandwidth(ctx context.Context, env *runner.Env, site string) error {
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s to reset clone bandwidth: %w", site, err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Exec(ctx, "SET GLOBAL clone_max_data_bandwidth = 0"); err != nil {
		return fmt.Errorf("reset clone bandwidth on %s: %w", site, err)
	}
	return nil
}
