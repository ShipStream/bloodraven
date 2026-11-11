package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brmysql "github.com/shipstream/bloodraven/internal/mysql"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	s71ID    = "71-old-primary-lost-own-transactions"
	s71DB    = "chaos_s71"
	s71Table = "marker"
	// s71Lost is how many of its own newest transactions the old primary
	// comes back without.
	s71Lost = 3
	// s71RecloneBudget is how long the operator gets, from its start, to
	// report the site, reclone it and have it replicating.
	s71RecloneBudget = 10 * time.Minute
)

func init() {
	runner.Register(scenario71OldPrimaryLostOwnTransactions())
}

type s71State struct {
	a, b       string
	uuidBefore string
	lost       string
	returnedAt time.Time
}

// scenario71OldPrimaryLostOwnTransactions is "Found, not fixed" 1 of PR
// #280: an old primary that returns after a failover without transactions
// it issued itself, which the new primary holds. Its GTID set is a subset
// of the new primary's, so recovery used to rejoin it by replication; its
// receiver drops events that carry its own server_id, so it never applied
// them again and would have issued the same GTIDs at its next promotion.
//
// A real cluster loses such a tail with a volume that comes back older.
// The playground cannot swap a volume's contents under a stopped pod, so
// the scenario gives the site the GTID state of the older volume instead:
// with the operator scaled to 0, it runs RESET BINARY LOGS AND GTIDS on
// the returned (read-only, fenced) old primary and sets gtid_purged to
// its former set minus its newest s71Lost own transactions. That works
// under super_read_only (measured on MySQL 9.7). The rows of those
// transactions stay in the site's tables, which an older volume would not
// have; the operator decides on GTID sets alone, and the reclone it runs
// replaces the site's data either way.
//
// Asserted: the operator reports the site (OwnTransactionsLost with the
// exact set, recoveryState RecloneRequired), never announces a rejoin by
// replication (no OldPrimaryRecovered Event), reclones it without being
// asked, and the site ends as a healthy replica of the new primary under
// a new server_uuid holding the transactions it had lost.
func scenario71OldPrimaryLostOwnTransactions() runner.Scenario {
	st := &s71State{}
	return runner.Scenario{
		ID:    s71ID,
		Title: "An old primary that returns without transactions it issued is recloned, not rejoined by replication",
		Hypothesis: "When the old primary comes back after a failover lacking transactions it issued itself that the new " +
			"primary holds, the operator does not rejoin it by replication: it reports OwnTransactionsLost with the exact " +
			"set (recoveryState RecloneRequired), reclones the site without a request, and the site ends as a healthy " +
			"replica under a new server_uuid that holds the lost transactions again.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#71-old-primary-lost-own-transactions",
		Timeout:           25 * time.Minute,
		CleanupTimeout:    15 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s71SeedAndFailover(st),
			s71ReturnWithoutOwnTail(st),
			s71ObserveReport(st),
			s71VerifyRecloned(st),
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			// The chaos stack scales the operator and the site back; a site
			// the operator did not get to reclone is recloned by it once it
			// runs again. Give it the budget, and fail the cleanup loudly
			// rather than hand the next scenario a site without replication.
			if st.a == "" {
				return nil
			}
			if err := env.Chaos.ScaleSiteToOne(ctx, st.a); err != nil {
				return err
			}
			if err := env.Kube.ScaleDeployment(ctx, env.Namespace, "bloodraven", 1); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, s71RecloneBudget)
			defer cancel()
			if err := env.Chaos.WaitForOperatorAvailable(waitCtx); err != nil {
				return err
			}
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, st.a+" replicating after cleanup",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return pitrSiteReplicating(mfg, st.a)
				})
			return err
		},
	}
}

func s71SeedAndFailover(st *s71State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "write on the primary, let the replica receive it, then lose the primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			st.a, st.b = a, b
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, a, env.Creds)
			if err != nil {
				return err
			}
			defer func() { _ = primary.Close() }()
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS " + quoteSQLIdentifier(s71DB),
				fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id INT PRIMARY KEY, phase VARCHAR(32) NOT NULL)",
					quoteSQLIdentifier(s71DB), quoteSQLIdentifier(s71Table)),
			} {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("seed on %s: %w", a, err)
				}
			}
			// One transaction per row: the last s71Lost of them are the
			// ones the site will come back without.
			for id := 1; id <= 10; id++ {
				if _, err := primary.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s (id, phase) VALUES (?, 'a-era')",
					quoteSQLIdentifier(s71DB), quoteSQLIdentifier(s71Table)), id); err != nil {
					return fmt.Errorf("insert on %s: %w", a, err)
				}
			}
			if st.uuidBefore, err = siteServerUUID(ctx, env, a); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return fmt.Errorf("%s did not receive %s's transactions: %w", b, a, err)
			}
			full, err := primary.GtidExecuted(ctx)
			if err != nil {
				return err
			}
			if st.lost, err = s71NewestOwn(full, st.uuidBefore, s71Lost); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("%s (server_uuid %s) will come back without %s", a, st.uuidBefore, st.lost))

			if err := env.Chaos.ScaleSiteToZero(ctx, a); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, b+" promoted",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return mfg.Status.ActiveSite == b, "activeSite=" + mfg.Status.ActiveSite, nil
				}); err != nil {
				return err
			}
			// The new primary's own era, and the precondition: it holds
			// every transaction the old primary is about to lose.
			newPrimary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, b, env.Creds)
			if err != nil {
				return err
			}
			defer func() { _ = newPrimary.Close() }()
			for id := 11; id <= 13; id++ {
				if _, err := newPrimary.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s (id, phase) VALUES (?, 'b-era')",
					quoteSQLIdentifier(s71DB), quoteSQLIdentifier(s71Table)), id); err != nil {
					return fmt.Errorf("insert on %s: %w", b, err)
				}
			}
			missing, err := newPrimary.ScalarString(ctx, "SELECT GTID_SUBTRACT(?, @@GLOBAL.gtid_executed)", st.lost)
			if err != nil {
				return err
			}
			if missing != "" {
				return fmt.Errorf("precondition not met: the new primary %s lacks %s of the set the old primary is to lose", b, missing)
			}
			return nil
		},
	}
}

// s71NewestOwn returns the newest n transactions of uuid in the GTID set
// full, as a GTID set. They must be the tail of one interval, so that the
// set without them is what an older volume would hold.
func s71NewestOwn(full, uuid string, n int64) (string, error) {
	set, err := brmysql.ParseGTIDSet(full)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", full, err)
	}
	own := set.OriginatedBy(uuid)
	var last int64
	var width int64
	for _, intervals := range own {
		for _, iv := range intervals {
			if iv.End > last {
				last, width = iv.End, iv.End-iv.Start+1
			}
		}
	}
	if last == 0 || width <= n {
		return "", fmt.Errorf("server %s issued too few transactions in a row to lose %d of them (set %q)", uuid, n, full)
	}
	return fmt.Sprintf("%s:%d-%d", strings.ToLower(uuid), last-n+1, last), nil
}

func s71ReturnWithoutOwnTail(st *s71State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "with the operator stopped, bring the old primary back with the GTID state of an older volume",
		Do: func(ctx context.Context, env *runner.Env) error {
			// The operator must not see the site before its GTID state is
			// the older one: it would rejoin a whole old primary at once.
			if err := env.Chaos.ScaleOperatorToZero(ctx); err != nil {
				return err
			}
			goneCtx, cancelGone := context.WithTimeout(ctx, 90*time.Second)
			defer cancelGone()
			for {
				n, err := env.Kube.PodCount(goneCtx, env.Namespace, pgmetrics.OperatorPodSelector)
				if err == nil && n == 0 {
					break
				}
				select {
				case <-goneCtx.Done():
					return fmt.Errorf("operator pods still present (%d): %w", n, goneCtx.Err())
				case <-time.After(2 * time.Second):
				}
			}
			if err := env.Chaos.ScaleSiteToOne(ctx, st.a); err != nil {
				return err
			}
			upCtx, cancelUp := context.WithTimeout(ctx, 5*time.Minute)
			defer cancelUp()
			var old *pgmysql.SiteClient
			for {
				c, err := pgmysql.Open(upCtx, env.Kube, env.Namespace, env.FG, st.a, env.Creds)
				if err == nil {
					old = c
					break
				}
				select {
				case <-upCtx.Done():
					return fmt.Errorf("%s did not come back: %w (last: %v)", st.a, upCtx.Err(), err)
				case <-time.After(3 * time.Second):
				}
			}
			defer func() { _ = old.Close() }()
			ro, err := old.ReadOnly(ctx)
			if err != nil {
				return err
			}
			if !ro {
				return fmt.Errorf("%s came back writable; the scenario needs the read_only boot", st.a)
			}
			kept, err := old.ScalarString(ctx, "SELECT GTID_SUBTRACT(@@GLOBAL.gtid_executed, ?)", st.lost)
			if err != nil {
				return err
			}
			// Nothing writes to the site between the two statements: it
			// is fenced and has no replication channel.
			if _, err := old.Exec(ctx, "RESET BINARY LOGS AND GTIDS"); err != nil {
				return fmt.Errorf("reset GTID state on %s: %w", st.a, err)
			}
			if _, err := old.Exec(ctx, "SET GLOBAL gtid_purged = ?", kept); err != nil {
				return fmt.Errorf("set gtid_purged on %s: %w", st.a, err)
			}
			after, err := old.GtidExecuted(ctx)
			if err != nil {
				return err
			}
			stillThere, err := old.ScalarString(ctx, "SELECT GTID_SUBTRACT(?, @@GLOBAL.gtid_executed)", st.lost)
			if err != nil {
				return err
			}
			if stillThere != st.lost {
				return fmt.Errorf("%s still holds part of %s after the reset (gtid_executed %s)", st.a, st.lost, after)
			}
			uuid, err := old.ScalarString(ctx, "SELECT @@GLOBAL.server_uuid")
			if err != nil {
				return err
			}
			if uuid != st.uuidBefore {
				return fmt.Errorf("%s came back under server_uuid %s, not %s: the lost set is not its own any more", st.a, uuid, st.uuidBefore)
			}
			env.Capture.Note(fmt.Sprintf("%s is back with gtid_executed %s (lacks %s)", st.a, truncGtid(after), st.lost))

			st.returnedAt = time.Now()
			if err := env.Kube.ScaleDeployment(ctx, env.Namespace, "bloodraven", 1); err != nil {
				return fmt.Errorf("scale the operator back: %w", err)
			}
			availCtx, cancelAvail := context.WithTimeout(ctx, 3*time.Minute)
			defer cancelAvail()
			if err := env.Chaos.WaitForOperatorAvailable(availCtx); err != nil {
				return err
			}
			if env.RefreshMetrics != nil {
				_ = env.RefreshMetrics(ctx)
			}
			return nil
		},
	}
}

func s71ObserveReport(st *s71State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "the operator reports the site with the exact lost set",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			ev, err := waitForMFGEvent(waitCtx, env, st.returnedAt, v1alpha1.EventOwnTransactionsLost, st.a+" lacks")
			if err != nil {
				return fmt.Errorf("no OwnTransactionsLost Event for %s: %w", st.a, err)
			}
			if !strings.Contains(ev.Message, fmt.Sprintf("%d transactions", s71Lost)) {
				return fmt.Errorf("OwnTransactionsLost Event does not count %d transactions: %q", s71Lost, ev.Message)
			}
			env.Capture.Note("OwnTransactionsLost: " + ev.Message)
			return nil
		},
	}
}

func s71VerifyRecloned(st *s71State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the site is recloned without a request and replicates under a new server_uuid",
		Do: func(ctx context.Context, env *runner.Env) error {
			newHost := playgroundInternalSiteHost(env.FG, st.b, env.Namespace)
			waitCtx, cancel := context.WithTimeout(ctx, s71RecloneBudget)
			defer cancel()
			if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, st.a+" recloned and replicating",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					site := statusSiteByName(mfg, st.a)
					if site == nil {
						return false, "site missing from status", nil
					}
					if site.RecoveryState == v1alpha1.RecoveryStateBlocked {
						return false, "", fmt.Errorf("%s is RecoveryBlocked (%s): the site was to lack transactions, not hold extra ones",
							st.a, site.DivergentGtid)
					}
					if site.LostGtid != "" || site.RecoveryState != "" {
						return false, fmt.Sprintf("recoveryState=%s lostGtid=%s", site.RecoveryState, truncGtid(site.LostGtid)), nil
					}
					if mfg.Status.ActiveSite != st.b {
						return false, "", fmt.Errorf("active site moved to %q while %s was repaired", mfg.Status.ActiveSite, st.a)
					}
					return pitrSiteReplicating(mfg, st.a)
				}); err != nil {
				return err
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if site := statusSiteByName(mfg, st.a); site == nil || canonicalMySQLHost(site.SourceHost) != canonicalMySQLHost(newHost) {
				return fmt.Errorf("%s does not replicate from the new primary %s (status %+v)", st.a, newHost, site)
			}

			// A reclone, not a rejoin: a new server_uuid, the report
			// resolved, and no announcement of a recovery by replication.
			uuid, err := siteServerUUID(ctx, env, st.a)
			if err != nil {
				return err
			}
			if strings.EqualFold(uuid, st.uuidBefore) {
				return fmt.Errorf("%s still runs under server_uuid %s: it was not recloned", st.a, uuid)
			}
			if _, err := waitForMFGEvent(waitCtx, env, st.returnedAt, v1alpha1.EventOwnTransactionsLostResolved, st.a); err != nil {
				return fmt.Errorf("no OwnTransactionsLostResolved Event for %s: %w", st.a, err)
			}
			if ev, found, err := findMFGEvent(ctx, env, st.returnedAt, v1alpha1.EventOldPrimaryRecovered, st.a); err != nil {
				return err
			} else if found {
				return fmt.Errorf("the operator announced a rejoin by replication of %s: %q", st.a, ev.Message)
			}

			// The site holds what it had lost, and the same rows as the
			// primary.
			old, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.a, env.Creds)
			if err != nil {
				return err
			}
			defer func() { _ = old.Close() }()
			missing, err := old.ScalarString(ctx, "SELECT GTID_SUBTRACT(?, @@GLOBAL.gtid_executed)", st.lost)
			if err != nil {
				return err
			}
			if missing != "" {
				return fmt.Errorf("%s still lacks %s after the reclone", st.a, missing)
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.b, env.Creds)
			if err != nil {
				return err
			}
			defer func() { _ = primary.Close() }()
			count := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quoteSQLIdentifier(s71DB), quoteSQLIdentifier(s71Table))
			want, err := primary.ScalarInt(ctx, count)
			if err != nil {
				return err
			}
			got, err := old.ScalarInt(ctx, count)
			if err != nil {
				return err
			}
			if got != want || want != 13 {
				return fmt.Errorf("marker rows: %s has %d, %s has %d, want 13 on both", st.a, got, st.b, want)
			}
			env.Capture.Note(fmt.Sprintf("%s recloned: server_uuid %s -> %s", st.a, st.uuidBefore, uuid))
			return nil
		},
	}
}
