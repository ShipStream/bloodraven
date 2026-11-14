package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	brmysql "github.com/shipstream/bloodraven/internal/mysql"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario76DragonflyBlackholeMySQLFailover())
}

const (
	s76ID = "76-dragonfly-blackhole-mysql-failover"
	s76DB = "chaos_s76"
	// s76Table holds the marker row of the tagged transaction.
	s76Table = "tagged_marker"
	// s76GTIDTag tags the one transaction the failover has to carry. MySQL
	// prints a UUID's tagged intervals after its untagged ones
	// (uuid:1-20:chaos76:1), the compact form the failover picker could
	// not parse before #199.
	s76GTIDTag = "chaos76"
	// s76DragonflyKey is the key seeded on the Dragonfly master.
	s76DragonflyKey = "scenario76:key"
	// s76LogWait bounds the wait for the operator's promotion lines once
	// the status shows both promotions; it covers log delivery only.
	s76LogWait = 30 * time.Second
	// s76RejoinWait bounds the wait for the old MySQL primary to come back
	// as a replica after its Deployment is scaled back up: a pod start and
	// the operator's old-primary recovery.
	s76RejoinWait = 3 * time.Minute
)

// Mirrors of the operator's emergency Dragonfly promotion budgets
// (internal/controller/dragonfly_topology.go). TestS76OperatorMirrors pins
// them to the source.
const (
	// s76EmergencySourceProbeTimeout is defaultEmergencySourceProbeTimeout:
	// the dial and PING of the old master that decides whether REPLTAKEOVER
	// is sent. A blackholed master uses all of it.
	s76EmergencySourceProbeTimeout = time.Second
	// s76EmergencyClientKillTimeout is the dial budget of
	// bestEffortEmergencyClientKill against the old master, spent in full
	// on a blackholed one before the status is stamped.
	s76EmergencyClientKillTimeout = 2 * time.Second
	// s76Allowance covers what the operator bounds only by its generous
	// per-step contexts (5s each), all of which take milliseconds on a
	// healthy cluster: the REPLICAOF NO ONE reply of the healthy target,
	// the label patches, the old pod's delete, the status stamp, and
	// MySQL's status and DNS writes between "failover complete" and the
	// queued request.
	s76Allowance = 3 * time.Second
)

// s76PromotionBound is how long after the MySQL promotion ("failover
// complete") the Dragonfly target may be stamped master, for a group with
// spec.failureThreshold failureThreshold. Two promotions race for the
// group's Dragonfly gate, and the first one to hold it does the work:
//
//   - The follow-up the MySQL failover queues. The manager's Run goroutine
//     takes it after the Tick in flight, which observes the blackholed
//     master within DefaultDialTimeout+DefaultIOTimeout (observe); the
//     other sites answer, and a replica that holds keys is not re-pointed
//     at a master that was not observed. Run then picks at random between
//     the queued request and the next tick when both are ready, so the
//     follow-up can lose to further Ticks.
//   - The manager's own promotion, made by the Tick that counts the
//     failureThreshold-th consecutive miss of the master. A blackhole drops
//     the dial, so each miss costs DefaultDialTimeout; the first one after
//     the policy may cost DefaultIOTimeout more (a connection made just
//     before the policy took effect, then a reply that never comes). The
//     misses start at the blackhole, before the MySQL promotion.
//
// Either way promoteLocked then probes the old master (source probe),
// skips the takeover, sends REPLICAOF NO ONE, dials the old master for
// CLIENT KILL and stamps the status; s76Allowance covers the calls that
// answer in milliseconds. With the playground's failureThreshold of 3 the
// own promotion bounds it: 3x3s+5s+1s+2s+3s = 20s, the "about 20s" PR #199
// estimated (the follow-up alone: 3s+5s+1s+2s+3s = 14s).
func s76PromotionBound(failureThreshold int) time.Duration {
	followUp := dragonfly.DefaultDialTimeout + dragonfly.DefaultIOTimeout
	own := time.Duration(failureThreshold)*dragonfly.DefaultDialTimeout + dragonfly.DefaultIOTimeout
	return max(followUp, own) + s76EmergencySourceProbeTimeout + s76EmergencyClientKillTimeout + s76Allowance
}

// s76FailureThreshold is spec.failureThreshold with the operator's default.
func s76FailureThreshold(mfg *v1alpha1.MysqlFailoverGroup) int {
	if mfg.Spec.FailureThreshold > 0 {
		return int(mfg.Spec.FailureThreshold)
	}
	return 3
}

// The operator's log messages the scenario reads (log schema,
// site/content/docs/8.observability/7.log-schema.md). TestS76OperatorMirrors
// pins each to the controller's source.
const (
	logS76FailoverComplete      = "failover complete"
	logS76TakeoverSkipped       = "dragonfly emergency: old master unreachable; skipping REPLTAKEOVER"
	logS76PromotedNoOne         = "dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)"
	logS76TakeoverSucceeded     = "dragonfly emergency: REPLTAKEOVER succeeded"
	logS76PromotionUnconfirmed  = "dragonfly emergency: promotion outcome unconfirmed; target observed as master"
	logS76DragonflyOnlyPromoted = "dragonfly-only emergency: active master unreachable; promoting replica"
	logS76FollowUpSkipped       = "dragonfly promotion: target already promoted since the request; skipping"
)

// scenario76DragonflyBlackholeMySQLFailover covers two PR #199 fixes on a
// real cluster (issue #214). An emergency MySQL failover whose Dragonfly
// master does not answer must move Dragonfly within the emergency budgets:
// before #199 the takeover against a dead master used up the one context
// of the promotion and the REPLICAOF NO ONE fallback ran on an expired
// context. And a GTID set that holds a tagged transaction must not stop
// the failover: MySQL 8.4+ prints such sets in a compact form that the
// picker could not parse before #199.
//
// The Dragonfly master's pod is blackholed with a deny-all NetworkPolicy
// and the MySQL primary on the same site is scaled to 0 right after. The
// MySQL failover (failureThreshold polls of a dead mysqld) queues a
// Dragonfly follow-up about 5s later, before the Dragonfly manager's own
// promotion (failureThreshold observations of the blackholed master at a
// dial timeout each) is due. Which of the two then moves Dragonfly depends
// on whether the manager's Run loop takes the queued request or the next
// tick first (s76PromotionBound). Both are correct operator behaviour and
// both skip REPLTAKEOVER; the scenario reports which one ran and holds
// either to the bound.
func scenario76DragonflyBlackholeMySQLFailover() runner.Scenario {
	return runner.Scenario{
		ID:    s76ID,
		Title: "Blackholed Dragonfly master and a killed MySQL primary with a tagged GTID — both fail over",
		Hypothesis: "With a tagged-GTID transaction on both sites, scaling the MySQL primary to 0 and blackholing its " +
			"Dragonfly master (deny-all NetworkPolicy on only that pod) makes the operator fail MySQL over to the peer, " +
			"and the Dragonfly target is stamped master within the bound derived from the operator's budgets (" +
			s76PromotionBound(3).String() + " at failureThreshold 3) of the MySQL promotion (REPLTAKEOVER skipped, " +
			"REPLICAOF NO ONE): status.dragonfly.activeSite equals the new MySQL active site, the " +
			"seeded key reads back from it and the active Dragonfly Service selects only its pod. The new primary holds the " +
			"tagged transaction and its marker row. After the blackhole is removed and MySQL is scaled back, the old MySQL " +
			"primary and the old Dragonfly master rejoin as replicas.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#76-blackholed-dragonfly-master-during-a-mysql-failover",
		// About a minute when nothing falls back (45s live). The
		// Dragonfly force-delete fallback of the settle step adds up to
		// 3.5 minutes.
		Timeout:        6 * time.Minute,
		CleanupTimeout: s01RepairTimeout + 2*time.Minute,
		Precheck:       s76Precheck,
		Steps: []runner.Step{
			s76SeedTaggedTransactionAndKey(),
			s76BlackholeDragonflyThenKillPrimary(),
			s76ObserveMySQLFailover(),
			s76ObserveDragonflyFollows(),
			s76MeasurePromotionDelay(),
			s76VerifyDragonflyOnTarget(),
			s76VerifyTaggedTransactionCarried(),
			s76SettleBothRejoin(),
		},
		Cleanup: s76Cleanup,
	}
}

// s76Precheck needs the Dragonfly baseline with the Dragonfly master on
// the MySQL active site, so blackholing that site's Dragonfly pod and
// killing its mysqld take out both masters of one site.
func s76Precheck(ctx context.Context, env *runner.Env) error {
	if err := AssertDragonflyHealthyBaseline(ctx, env); err != nil {
		return err
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	master, err := dragonflyActiveSite(mfg)
	if err != nil {
		return err
	}
	if master != mfg.Status.ActiveSite {
		return fmt.Errorf("precheck: dragonfly master %q is not on the MySQL active site %q", master, mfg.Status.ActiveSite)
	}
	masterPod, err := env.Kube.GetSiteDragonflyPod(ctx, env.Namespace, env.FG, master)
	if err != nil {
		return fmt.Errorf("precheck: get dragonfly master pod: %w", err)
	}
	eps, err := env.Kube.DragonflyActiveServiceEndpointPods(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("precheck: read dragonfly active service endpoints: %w", err)
	}
	if len(eps) != 1 || eps[0] != masterPod.Name {
		return fmt.Errorf("precheck: active dragonfly Service endpoints=%v, want only master pod %s", eps, masterPod.Name)
	}
	return nil
}

func s76SeedTaggedTransactionAndKey() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed a Dragonfly key and one tagged-GTID marker transaction; both reach the peer",
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
			for k, v := range map[string]string{"originalPrimary": active, "expectedNewPrimary": peer} {
				if err := ctxStash(ctx, env, k, v); err != nil {
					return err
				}
			}
			// Opened before the fault so the promotion lines are buffered.
			if _, err := env.Logs("operator"); err != nil {
				return err
			}
			for key, labels := range map[string]map[string]string{
				"mysqlFailoversBefore": {"target_site": peer},
				"dfPromotionsBefore":   {"target_site": peer, "result": "success"},
				"dfSessionsLostBefore": {"target_site": peer, "result": "sessions_lost"},
			} {
				name := "bloodraven_dragonfly_promotions_total"
				if key == "mysqlFailoversBefore" {
					name = "bloodraven_failovers_total"
				}
				if err := stashMetricCounter(ctx, env, key, name, labels); err != nil {
					return err
				}
			}

			cli, err := env.Dragonfly(active)
			if err != nil {
				return fmt.Errorf("open dragonfly master %s: %w", active, err)
			}
			val := fmt.Sprintf("scenario76-%d", env.StartTime.UnixNano())
			if _, err := cli.Set(ctx, s76DragonflyKey, val); err != nil {
				return fmt.Errorf("SET on dragonfly master %s: %w", active, err)
			}
			if err := ctxStash(ctx, env, "dfSeedValue", val); err != nil {
				return err
			}
			if err := s39WaitKeyOnSite(ctx, env, peer, s76DragonflyKey, val, 30*time.Second); err != nil {
				return fmt.Errorf("seed key did not replicate to peer %s: %w", peer, err)
			}

			marker := fmt.Sprintf("s76-%d", env.StartTime.UnixNano())
			if err := ctxStash(ctx, env, "s76SchemaCreated", "true"); err != nil {
				return err
			}
			tagged, raw, err := s76CommitTaggedMarker(ctx, env, active, marker)
			if err != nil {
				return err
			}
			for k, v := range map[string]string{"taggedGtid": tagged, "marker": marker} {
				if err := ctxStash(ctx, env, k, v); err != nil {
					return err
				}
			}
			env.Capture.Measure(fmt.Sprintf("tagged transaction %s committed on %s; its gtid_executed reads %q", tagged, active, raw))
			if err := s76WaitTaggedOnSite(ctx, env, peer, tagged, marker); err != nil {
				return fmt.Errorf("the tagged transaction did not reach the peer %s before the fault: %w", peer, err)
			}
			env.Capture.Note(fmt.Sprintf("peer %s holds %s and the marker row", peer, tagged))
			return nil
		},
	}
}

// s76CommitTaggedMarker creates the marker table untagged, then commits one
// row under gtid_next='AUTOMATIC:<tag>' on a pinned connection. It returns
// the transaction's GTID (s76TaggedTransactions) and the primary's
// gtid_executed as MySQL prints it.
func s76CommitTaggedMarker(ctx context.Context, env *runner.Env, site, marker string) (tagged, raw string, err error) {
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return "", "", fmt.Errorf("open primary %s: %w", site, err)
	}
	defer client.Close()
	table := quoteSQLIdentifier(s76DB) + "." + quoteSQLIdentifier(s76Table)
	if _, err := client.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteSQLIdentifier(s76DB)+"; "+
		"CREATE TABLE IF NOT EXISTS "+table+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, marker VARCHAR(64) NOT NULL)"); err != nil {
		return "", "", fmt.Errorf("create %s on %s: %w", table, site, err)
	}
	conn, err := client.DB.Conn(ctx)
	if err != nil {
		return "", "", fmt.Errorf("pin a connection to %s: %w", site, err)
	}
	defer conn.Close()
	var before string
	if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&before); err != nil {
		return "", "", fmt.Errorf("read gtid_executed on %s: %w", site, err)
	}
	// TRANSACTION_GTID_TAG is needed for this; the playground's root has it.
	if _, err := conn.ExecContext(ctx, "SET gtid_next='AUTOMATIC:"+s76GTIDTag+"'"); err != nil {
		return "", "", fmt.Errorf("SET gtid_next='AUTOMATIC:%s' on %s: %w", s76GTIDTag, site, err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO "+table+" (marker) VALUES ("+quoteSQLString(marker)+")"); err != nil {
		return "", "", fmt.Errorf("insert the tagged marker on %s: %w", site, err)
	}
	if _, err := conn.ExecContext(ctx, "SET gtid_next='AUTOMATIC'"); err != nil {
		return "", "", fmt.Errorf("reset gtid_next on %s: %w", site, err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&raw); err != nil {
		return "", "", fmt.Errorf("read gtid_executed on %s: %w", site, err)
	}
	raw = strings.ReplaceAll(strings.TrimSpace(raw), "\n", "")
	tagged, err = s76TaggedTransactions(before, raw, s76GTIDTag)
	if err != nil {
		return "", raw, fmt.Errorf("on %s: %w", site, err)
	}
	return tagged, raw, nil
}

// s76TaggedTransactions returns the transactions under tag that after holds
// and before does not, and requires exactly one. Other writers (the
// counter app) commit untagged transactions meanwhile; only this scenario
// uses the tag. The sets are parsed with the operator's parser.
func s76TaggedTransactions(before, after, tag string) (string, error) {
	b, err := brmysql.ParseGTIDSet(strings.ReplaceAll(before, "\n", ""))
	if err != nil {
		return "", fmt.Errorf("parse gtid_executed before the tagged transaction %q: %w", before, err)
	}
	a, err := brmysql.ParseGTIDSet(strings.ReplaceAll(after, "\n", ""))
	if err != nil {
		return "", fmt.Errorf("parse gtid_executed after the tagged transaction %q: %w", after, err)
	}
	added := a.Subtract(b)
	out := brmysql.GTIDSet{}
	suffix := ":" + strings.ToLower(tag)
	for sid, ivs := range added {
		if strings.HasSuffix(sid, suffix) {
			out[sid] = ivs
		}
	}
	if n := out.TransactionCount(); n != 1 {
		return "", fmt.Errorf("want exactly one new transaction tagged %q, found %d (%q); gtid_executed before %q, after %q",
			tag, n, out.String(), before, after)
	}
	return out.String(), nil
}

// s76WaitTaggedOnSite waits until site has executed tagged and holds the
// marker row.
func s76WaitTaggedOnSite(ctx context.Context, env *runner.Env, site, tagged, marker string) error {
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s: %w", site, err)
	}
	defer client.Close()
	if rc, err := client.ScalarInt(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", tagged); err != nil {
		return fmt.Errorf("WAIT_FOR_EXECUTED_GTID_SET(%s) on %s: %w", tagged, site, err)
	} else if rc != 0 {
		return fmt.Errorf("%s did not execute %s within 30s", site, tagged)
	}
	return s76MarkerPresent(ctx, client, marker)
}

func s76MarkerPresent(ctx context.Context, client *pgmysql.SiteClient, marker string) error {
	n, err := client.ScalarInt(ctx, "SELECT COUNT(*) FROM "+quoteSQLIdentifier(s76DB)+"."+quoteSQLIdentifier(s76Table)+
		" WHERE marker = "+quoteSQLString(marker))
	if err != nil {
		return fmt.Errorf("count the marker row on %s: %w", client.Site, err)
	}
	if n != 1 {
		return fmt.Errorf("%s holds %d marker rows %q, want 1", client.Site, n, marker)
	}
	return nil
}

// s76BlackholeDragonflyThenKillPrimary applies the blackhole first, then
// scales the MySQL primary to 0 at once. The pod's preStop fences mysqld
// and it stops answering within about a second, so the operator promotes
// the peer after failureThreshold polls (about 4.5s on the playground),
// while the Dragonfly manager needs failureThreshold observations of the
// blackholed master at a dial timeout each (3 x 3s) before it would
// promote on its own. The blackhole is in place long before the queued
// follow-up probes the old master.
func s76BlackholeDragonflyThenKillPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "blackhole the Dragonfly master's pod, then scale the MySQL primary on the same site to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "originalPrimary")
			faultAt := time.Now().UTC()
			if err := ctxStash(ctx, env, faultInjectedAtKey, faultAt.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if err := env.Chaos.PartitionDragonflyPod(ctx, active); err != nil {
				return err
			}
			if err := env.Chaos.ScaleSiteToZero(ctx, active); err != nil {
				return err
			}
			env.Capture.Measure(fmt.Sprintf("dragonfly master on %s blackholed at %s; MySQL primary scaled to 0 %s later",
				active, faultAt.Format(time.RFC3339Nano), roundMs(time.Since(faultAt))))
			return nil
		},
	}
}

func s76ObserveMySQLFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "MySQL fails over to the peer",
		Do: func(ctx context.Context, env *runner.Env) error {
			original, peer := ctxFetch(env, "originalPrimary"), ctxFetch(env, "expectedNewPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, failoverObserveBudget)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite changes from %s and is observed writable", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					done, msg := failoverObserved(mfg, original)
					return done, msg, nil
				})
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != peer {
				return fmt.Errorf("activeSite=%q, want the peer %q", mfg.Status.ActiveSite, peer)
			}
			return nil
		},
	}
}

func s76ObserveDragonflyFollows() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.dragonfly.activeSite moves to the new MySQL active site",
		Do: func(ctx context.Context, env *runner.Env) error {
			peer := ctxFetch(env, "expectedNewPrimary")
			faultAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, faultInjectedAtKey))
			if err != nil {
				return err
			}
			// Waiting is generous; the bound is checked on the operator's
			// timestamps in the next step.
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("status.dragonfly.activeSite==%s with a promotion stamped after the fault", peer),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					df := mfg.Status.Dragonfly
					if df == nil {
						return false, "no status.dragonfly", nil
					}
					stamped := df.LastPromotionTime != nil && !df.LastPromotionTime.Time.Before(faultAt.Truncate(time.Second))
					msg := fmt.Sprintf("dfActive=%q lastPromotionTarget=%q lastPromotionTime=%v phase=%q mysqlActive=%q",
						df.ActiveSite, df.LastPromotionTarget, df.LastPromotionTime, df.Phase, mfg.Status.ActiveSite)
					return df.ActiveSite == peer && df.LastPromotionTarget == peer && stamped && mfg.Status.ActiveSite == peer, msg, nil
				})
			return err
		},
	}
}

// s76PromotionTimeline is what the operator logged about the two
// promotions, read from its log lines since the fault.
type s76PromotionTimeline struct {
	FailoverComplete time.Time
	// Promoted is the line that says the Dragonfly target became master,
	// PromotedMsg its message.
	Promoted    time.Time
	PromotedMsg string
	// TakeoverSkipped: promoteLocked found the old master unreachable.
	TakeoverSkipped bool
	// DragonflyOnly: the manager's own promotion (failure threshold of the
	// blackholed master) started before the target became master.
	DragonflyOnly bool
	// FollowUpSkipped: the request the MySQL failover queued found the
	// target already promoted.
	FollowUpSkipped bool
	// Elsewhere lists the Dragonfly promotions of sites other than target
	// logged after the MySQL promotion, as "<site> (<message>) at <time>".
	Elsewhere []string
}

// s76ReadTimeline scans lines for the MySQL promotion of target and the
// first Dragonfly promotion of target after it started failing.
func s76ReadTimeline(lines []pglogs.Match, target string) s76PromotionTimeline {
	var tl s76PromotionTimeline
	for _, m := range lines {
		fields, ok := pglogs.Fields(m.Line)
		if !ok {
			continue
		}
		msg := fields["msg"]
		// Before the MySQL promotion the manager decides on its own
		// observation; from it on, MySQL has named the site.
		if site := s76PromotedSite(msg, fields); site != "" && site != target && !tl.FailoverComplete.IsZero() {
			tl.Elsewhere = append(tl.Elsewhere, fmt.Sprintf("%s (%q) at %s", site, msg, m.Time.UTC().Format(time.RFC3339Nano)))
		}
		switch {
		case msg == logS76FailoverComplete && fields["promotedSite"] == target:
			if tl.FailoverComplete.IsZero() {
				tl.FailoverComplete = m.Time
			}
		case msg == logS76FollowUpSkipped && fields["site"] == target:
			tl.FollowUpSkipped = true
		case !tl.Promoted.IsZero():
		case msg == logS76DragonflyOnlyPromoted && fields["target"] == target:
			tl.DragonflyOnly = true
		case msg == logS76TakeoverSkipped && fields["site"] == target:
			tl.TakeoverSkipped = true
		case (msg == logS76PromotedNoOne || msg == logS76TakeoverSucceeded || msg == logS76PromotionUnconfirmed) && fields["site"] == target:
			tl.Promoted, tl.PromotedMsg = m.Time, msg
		}
	}
	return tl
}

// s76PromotedSite is the Dragonfly site a promotion line names, or "" for
// any other line.
func s76PromotedSite(msg string, fields map[string]string) string {
	switch msg {
	case logS76DragonflyOnlyPromoted:
		return fields["target"]
	case logS76PromotedNoOne, logS76TakeoverSucceeded, logS76PromotionUnconfirmed:
		return fields["site"]
	}
	return ""
}

// path names the promotion that ran.
func (tl s76PromotionTimeline) path() string {
	if tl.DragonflyOnly {
		p := "the Dragonfly manager's own promotion (failure threshold of the blackholed master)"
		if tl.FollowUpSkipped {
			p += "; the follow-up the MySQL failover queued ran after it and found the target promoted"
		}
		return p
	}
	return "the follow-up the MySQL failover queued"
}

// s76PromotionDelay is the time from the MySQL promotion to the Dragonfly
// promotion's status stamp. The stamp has one-second resolution, so a
// second is added: the delay is never understated.
func s76PromotionDelay(failoverComplete, stamp time.Time) time.Duration {
	return stamp.Add(time.Second).Sub(failoverComplete)
}

func s76MeasurePromotionDelay() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the Dragonfly target, and no other site, is stamped master within the bound of the MySQL promotion, without REPLTAKEOVER",
		Do: func(ctx context.Context, env *runner.Env) error {
			peer := ctxFetch(env, "expectedNewPrimary")
			faultAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, faultInjectedAtKey))
			if err != nil {
				return err
			}
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			var tl s76PromotionTimeline
			waitCtx, cancel := context.WithTimeout(ctx, s76LogWait)
			defer cancel()
			for {
				tl = s76ReadTimeline(logLinesSince(tail, faultAt.Add(-logClockSlack)), peer)
				if !tl.FailoverComplete.IsZero() && !tl.Promoted.IsZero() {
					break
				}
				select {
				case <-waitCtx.Done():
					return fmt.Errorf("the operator log since the fault lacks %q for %s (seen=%v) or a Dragonfly promotion of %s (seen=%v)",
						logS76FailoverComplete, peer, !tl.FailoverComplete.IsZero(), peer, !tl.Promoted.IsZero())
				case <-time.After(500 * time.Millisecond):
				}
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			df := mfg.Status.Dragonfly
			if df == nil || df.LastPromotionTime == nil {
				return fmt.Errorf("status.dragonfly.lastPromotionTime is not set")
			}
			stamp := df.LastPromotionTime.Time
			delay := s76PromotionDelay(tl.FailoverComplete, stamp)
			bound := s76PromotionBound(s76FailureThreshold(mfg))
			env.Capture.Measure(fmt.Sprintf("MySQL promotion (%q) at %s; Dragonfly target %s became master %s later (%q) and was stamped at %s: "+
				"delay %s (stamp + 1s resolution) against the %s bound; path: %s; REPLTAKEOVER skipped: %v",
				logS76FailoverComplete, tl.FailoverComplete.Format(time.RFC3339Nano), peer,
				roundMs(tl.Promoted.Sub(tl.FailoverComplete)), tl.PromotedMsg, stamp.UTC().Format(time.RFC3339),
				roundMs(delay), bound, tl.path(), tl.TakeoverSkipped))
			env.Logger.Info("dragonfly follow-up measured", "target", peer, "delayMs", delay.Milliseconds(),
				"boundMs", bound.Milliseconds(), "path", tl.path(), "takeoverSkipped", tl.TakeoverSkipped)
			// Dragonfly follows MySQL: once the MySQL failover is
			// decided, no other site may take the Dragonfly master, even
			// for a moment. Such a master takes writes that are discarded
			// when it is re-attached to the target, and while both carry
			// the master and traffic labels the active Service selects
			// both.
			if len(tl.Elsewhere) > 0 {
				return fmt.Errorf("the operator promoted Dragonfly on a site other than the new MySQL active site %s: %s",
					peer, strings.Join(tl.Elsewhere, "; "))
			}
			if tl.PromotedMsg == logS76TakeoverSucceeded {
				return fmt.Errorf("REPLTAKEOVER succeeded: the old Dragonfly master answered, so the blackhole was not in effect when the promotion ran")
			}
			if delay > bound {
				return fmt.Errorf("the Dragonfly target %s was stamped master %s after the MySQL promotion, over the %s bound",
					peer, roundMs(delay), bound)
			}
			return nil
		},
	}
}

func s76VerifyDragonflyOnTarget() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "seeded key on the new Dragonfly master, endpoints moved, promotion and failover counted",
		Do: func(ctx context.Context, env *runner.Env) error {
			peer := ctxFetch(env, "expectedNewPrimary")
			if err := s39WaitKeyOnSite(ctx, env, peer, s76DragonflyKey, ctxFetch(env, "dfSeedValue"), 60*time.Second); err != nil {
				return fmt.Errorf("seeded key unavailable on the promoted master %s: %w", peer, err)
			}
			targetPod, err := env.Kube.GetSiteDragonflyPod(ctx, env.Namespace, env.FG, peer)
			if err != nil {
				return fmt.Errorf("get the promoted dragonfly pod: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			var lastEps []string
			for {
				eps, err := env.Kube.DragonflyActiveServiceEndpointPods(ctx, env.Namespace, env.FG)
				if err == nil {
					lastEps = eps
					if len(eps) == 1 && eps[0] == targetPod.Name {
						break
					}
				}
				select {
				case <-waitCtx.Done():
					return fmt.Errorf("active dragonfly endpoints did not converge to only the promoted pod %s (last=%v)", targetPod.Name, lastEps)
				case <-time.After(2 * time.Second):
				}
			}
			mysqlBefore, err := fetchStashedFloat(env, "mysqlFailoversBefore")
			if err != nil {
				return err
			}
			okBefore, err := fetchStashedFloat(env, "dfPromotionsBefore")
			if err != nil {
				return err
			}
			lostBefore, err := fetchStashedFloat(env, "dfSessionsLostBefore")
			if err != nil {
				return err
			}
			metricCtx, cancelMetric := context.WithTimeout(ctx, 30*time.Second)
			defer cancelMetric()
			return env.Wait.UntilMetric(metricCtx, env.Metrics,
				fmt.Sprintf("failovers_total{target_site=%q} and dragonfly_promotions_total{target_site=%q,result=\"sessions_lost\"} increment", peer, peer),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					fo, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": peer})
					ok, _ := snap.Counter("bloodraven_dragonfly_promotions_total", map[string]string{"target_site": peer, "result": "success"})
					lost, _ := snap.Counter("bloodraven_dragonfly_promotions_total", map[string]string{"target_site": peer, "result": "sessions_lost"})
					// REPLICAOF NO ONE counts as sessions_lost; a takeover
					// (success) would mean the blackhole was not in effect.
					return fo > mysqlBefore && lost > lostBefore && ok == okBefore,
						fmt.Sprintf("failovers=%g/%g sessions_lost=%g/%g success=%g/%g", fo, mysqlBefore, lost, lostBefore, ok, okBefore)
				})
		},
	}
}

func s76VerifyTaggedTransactionCarried() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the new primary holds the tagged transaction and its marker row",
		Do: func(ctx context.Context, env *runner.Env) error {
			peer, tagged := ctxFetch(env, "expectedNewPrimary"), ctxFetch(env, "taggedGtid")
			client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, peer, env.Creds)
			if err != nil {
				return fmt.Errorf("open the new primary %s: %w", peer, err)
			}
			defer client.Close()
			raw, err := client.GtidExecuted(ctx)
			if err != nil {
				return err
			}
			raw = strings.ReplaceAll(raw, "\n", "")
			// MySQL's own containment check, then the operator's parser.
			subset, err := client.ScalarInt(ctx, "SELECT GTID_SUBSET(?, @@global.gtid_executed)", tagged)
			if err != nil {
				return fmt.Errorf("GTID_SUBSET(%s) on %s: %w", tagged, peer, err)
			}
			if subset != 1 {
				return fmt.Errorf("the new primary %s does not hold %s (gtid_executed %q)", peer, tagged, raw)
			}
			have, err := brmysql.ParseGTIDSet(raw)
			if err != nil {
				return fmt.Errorf("the operator's parser rejects the new primary's gtid_executed %q: %w", raw, err)
			}
			want, err := brmysql.ParseGTIDSet(tagged)
			if err != nil {
				return err
			}
			if !have.Contains(want) {
				return fmt.Errorf("parsed gtid_executed of %s does not contain %s (MySQL says it does): %q", peer, tagged, raw)
			}
			if !strings.Contains(raw, ":"+s76GTIDTag+":") {
				return fmt.Errorf("gtid_executed of %s does not print the tag %q: %q", peer, s76GTIDTag, raw)
			}
			if err := s76MarkerPresent(ctx, client, ctxFetch(env, "marker")); err != nil {
				return err
			}
			env.Capture.Measure(fmt.Sprintf("new primary %s holds %s and the marker row; gtid_executed %q", peer, tagged, raw))
			return nil
		},
	}
}

func s76SettleBothRejoin() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "remove the blackhole and scale MySQL back; the old Dragonfly master and the old MySQL primary rejoin as replicas",
		Do: func(ctx context.Context, env *runner.Env) error {
			oldSite, peer := ctxFetch(env, "originalPrimary"), ctxFetch(env, "expectedNewPrimary")
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("remove the blackhole and scale %s back: %w", oldSite, err)
			}
			env.Capture.Note("removed the dragonfly blackhole and scaled MySQL back; awaiting both rejoins")

			if err := s39WaitOldMasterHealthyReplica(ctx, env, oldSite, 90*time.Second); err != nil {
				env.Capture.Note(fmt.Sprintf("old dragonfly master %s did not rejoin within 90s (%v); force-deleting its pod (documented conservative recovery)", oldSite, err))
				if err := env.Chaos.KillDragonflyPod(ctx, oldSite); err != nil {
					return fmt.Errorf("force-delete the dragonfly pod of %s: %w", oldSite, err)
				}
				if err := s39WaitOldMasterHealthyReplica(ctx, env, oldSite, 2*time.Minute); err != nil {
					return fmt.Errorf("old dragonfly master %s did not rejoin as a healthy replica even after its pod was recreated: %w", oldSite, err)
				}
			}

			waitCtx, cancel := context.WithTimeout(ctx, s76RejoinWait)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("old primary %s replicates from %s", oldSite, peer),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.ActiveSite != peer {
						return false, "", fmt.Errorf("activeSite moved to %q while the old primary rejoined", mfg.Status.ActiveSite)
					}
					for _, s := range mfg.Status.Sites {
						if s.Name != oldSite {
							continue
						}
						msg := fmt.Sprintf("state=%q replication=%q recovery=%q", s.State, s.Replication, s.RecoveryState)
						if s.RecoveryState == v1alpha1.RecoveryStateBlocked {
							return false, msg, fmt.Errorf("old primary %s is RecoveryBlocked: it did not rejoin by replication (%s)", oldSite, msg)
						}
						return s.State == v1alpha1.SiteStateReadOnly && s.Replication.Running() && s.RecoveryState == "", msg, nil
					}
					return false, fmt.Sprintf("site %s not in status yet", oldSite), nil
				})
			return err
		},
	}
}

// s76Cleanup reclones a site left blocked (a failed run must not wedge the
// next scenario) and drops the marker schema. It does nothing when the run
// never created the schema.
func s76Cleanup(ctx context.Context, env *runner.Env) error {
	if ctxFetch(env, "s76SchemaCreated") == "" {
		return nil
	}
	if err := repairDivergentSites(ctx, env, s01RepairTimeout); err != nil {
		return fmt.Errorf("s76 cleanup: %w", err)
	}
	if err := dropMarkerSchemaAndReplicate(ctx, env, s76DB); err != nil {
		return fmt.Errorf("s76 cleanup: %w", err)
	}
	return nil
}
