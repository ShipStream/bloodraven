package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

const (
	// divergenceDetectedByRecovery marks a block recorded by old-primary
	// recovery: a returning former primary held transactions the new
	// primary never received.
	divergenceDetectedByRecovery = "OldPrimaryRecovery"
	// divergenceDetectedByAudit marks a block recorded by the replica GTID
	// audit: a follower, possibly replicating normally, holds transactions
	// the primary lacks.
	divergenceDetectedByAudit = "ReplicaAudit"

	// gtidAuditInterval is how often each follower is audited. It is the
	// recovery retry cadence: a divergence report is re-verified at the
	// same rate whichever check recorded it.
	gtidAuditInterval = recoveryRetryDelay
	// gtidAuditBudget bounds one audit pass across every due follower, so
	// a slow server cannot hold the poll loop (and with it emergency
	// failure detection) for longer than one MySQL safety timeout.
	gtidAuditBudget = mysqlSafetyProbeTimeout
)

// DivergentPromotion records that a site carrying a divergence block
// became the confirmed primary: its extra transactions are now part of the
// group's authoritative history. The runner persists it as the
// DivergentPromotion condition, which only an administrator removes, so
// the fact survives the block itself being dropped.
type DivergentPromotion struct {
	Site                  string
	DivergentGtid         string
	DivergentTransactions int64
	// ComparedTo is the primary the set was computed against, if known.
	ComparedTo string
}

// gtidSnapshotReader is implemented by checkers that read a server's
// identity together with its executed GTID set in one statement. The
// production checker implements it; the audit falls back to
// GetGtidExecuted with an unknown identity for checkers that do not.
type gtidSnapshotReader interface {
	ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error)
}

// gtidAuditSchedule is when a follower is next audited.
type gtidAuditSchedule struct {
	nextAt time.Time
}

// gtidAuditSuppressedLocked reports whether authority is in flux or the
// cluster is being reshaped, so a comparison against the active site would
// not describe a settled primary and follower. Must hold tm.mu.
func (tm *TopologyManager) gtidAuditSuppressedLocked() bool {
	return tm.promotedSite != "" || tm.autoBootstrapSuppressed || tm.topologyFrozen ||
		tm.plannedFailoverActive || !bootstrapIdlePhase(tm.bootstrapPhase) ||
		tm.deploymentUpdateRunning || tm.deploymentChanging > 0 || tm.deploymentAwaitingObservation
}

// auditFollowerDivergence checks every due read-only follower for
// transactions the confirmed primary lacks. Thread health says nothing
// about this: a follower can apply every new primary transaction while
// keeping history the primary never had (issue #202). A follower with
// extras gets a RecoveryBlocked report; a follower with a blocked report
// that now provably holds nothing extra has it cleared. Nothing else
// clears a block on a replicating follower.
//
// Each follower is read before the primary. A follower's set only grows
// with transactions that are on the primary first, so anything the
// follower had that the later primary read lacks is genuinely extra; read
// the other way round, a transaction applied between the reads would look
// extra. A result is discarded if authority moved or either server's
// server_uuid changed between the reads. A read or parse failure leaves
// the site's state as it was: unverified is neither clean nor divergent.
//
// The audit runs after the poll's failure handling, at most once per
// gtidAuditInterval per site. Every follower is due again on the first
// pass with a new primary (including after start), after a completed
// authority change (a recorded promotion or handoff, a successful
// re-assert), and when a follower's receiver reports a new Source_UUID for
// the active host: the primary was replaced under the same name. A
// maintenance handoff refused for a divergent candidate wakes that one
// candidate (wakeGTIDAudit). The due list comes from tracker state, so
// a poll with nothing due probes nothing. Confirming the primary and every
// read share one gtidAuditBudget, the most the audit can add to a poll.
// skip names sites whose replica probe failed this poll. Returns true when
// a report was recorded, changed or cleared.
func (tm *TopologyManager) auditFollowerDivergence(ctx context.Context, siteRepl []*mysql.ReplicaStatus, skip map[string]struct{}) bool {
	tm.mu.Lock()
	activeName := tm.activeSiteLocked()
	if tm.gtidAuditSuppressedLocked() || activeName == "" {
		tm.mu.Unlock()
		return false
	}
	activeHost := canonicalSourceHost(tm.getSite(activeName).host)
	// authorityEpoch only discards a result that raced a mutation claim
	// (see auditFollower); forced passes follow completed authority
	// changes, so a rollout that fails every poll cannot force one every
	// poll or erase the backoff an unverified pass left.
	epoch := tm.authorityEpoch
	if tm.gtidAuditPrimary != activeName || tm.gtidAuditAuthority != tm.authorityChanges {
		tm.gtidAudit = make(map[string]*gtidAuditSchedule)
		tm.gtidAuditPrimary, tm.gtidAuditPrimaryUUID, tm.gtidAuditAuthority = activeName, "", tm.authorityChanges
	}
	for i := range tm.sites {
		if i >= len(siteRepl) || siteRepl[i] == nil || siteRepl[i].SourceUUID == "" ||
			canonicalSourceHost(siteRepl[i].SourceHost) != activeHost {
			continue
		}
		name, uuid := tm.sites[i].name, siteRepl[i].SourceUUID
		if prev := tm.gtidAuditSourceUUID[name]; prev != "" && prev != uuid {
			tm.gtidAudit = make(map[string]*gtidAuditSchedule)
			tm.gtidAuditPrimaryUUID = ""
		}
		tm.gtidAuditSourceUUID[name] = uuid
	}
	now := tm.clock.Now()
	var due []*siteTracker
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.name == activeName || site.state != state.StateReadOnly || site.probeFailed {
			continue
		}
		if _, skipped := skip[site.name]; skipped {
			continue
		}
		if i >= len(siteRepl) || siteRepl[i] == nil || strings.TrimSpace(siteRepl[i].SourceHost) == "" {
			// No channel: a source-less site is old-primary recovery's or
			// bootstrap's, and recovery runs its own comparison.
			continue
		}
		if sched := tm.gtidAudit[site.name]; sched != nil && now.Before(sched.nextAt) {
			continue
		}
		due = append(due, site)
	}
	tm.mu.Unlock()
	if len(due) == 0 {
		return false
	}

	passCtx, cancel := context.WithTimeout(ctx, gtidAuditBudget)
	defer cancel()
	active, err := tm.confirmedActivePrimary(passCtx)
	if err != nil || active.name != activeName {
		// Unverified authority: nothing is published, and the due
		// followers wait out the interval rather than spend the budget
		// on every poll against a slow primary.
		names := make([]string, 0, len(due))
		for _, follower := range due {
			tm.stampGTIDAudit(follower.name)
			names = append(names, follower.name)
		}
		if err == nil {
			err = fmt.Errorf("active site changed to %q", active.name)
		}
		tm.logger.Warn("replica GTID audit unverified",
			"site", strings.Join(names, ","), "primary", activeName, "error", fmt.Errorf("confirm primary: %w", err))
		return false
	}
	changed := false
	for i, follower := range due {
		followerChanged, primaryFailed := tm.auditFollower(passCtx, epoch, active, follower)
		changed = changed || followerChanged
		if primaryFailed || passCtx.Err() != nil {
			// A primary that cannot be read, or a spent budget, leaves the
			// rest of the pass unverified too: those followers wait out
			// their interval instead of each costing a budget on the
			// following polls.
			rest := make([]string, 0, len(due)-i-1)
			for _, f := range due[i+1:] {
				tm.stampGTIDAudit(f.name)
				rest = append(rest, f.name)
			}
			if len(rest) > 0 {
				tm.logger.Warn("replica GTID audit unverified",
					"site", strings.Join(rest, ","), "primary", activeName,
					"error", errors.New("pass abandoned: the primary could not be read within the audit budget"))
			}
			break
		}
	}
	return changed
}

// wakeGTIDAudit makes site due on the next poll: a maintenance handoff
// found it holding transactions the source lacks, which only the audit
// records.
func (tm *TopologyManager) wakeGTIDAudit(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.gtidAudit, site)
}

// auditFollower runs one follower-first comparison and publishes it. It
// reports whether the report changed, and whether a primary read failed,
// which leaves the rest of the pass unverified.
func (tm *TopologyManager) auditFollower(ctx context.Context, epoch uint64, active, follower *siteTracker) (changed, primaryFailed bool) {
	read := func(c mysql.Checker) (mysql.GTIDSnapshot, error) {
		var snap mysql.GTIDSnapshot
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			if r, ok := c.(gtidSnapshotReader); ok {
				var err error
				snap, err = r.ReadGTIDSnapshot(probeCtx)
				return err
			}
			raw, err := c.GetGtidExecuted(probeCtx)
			snap = mysql.GTIDSnapshot{GtidExecuted: raw}
			return err
		})
		return snap, err
	}
	unverified := func(err error) bool {
		tm.stampGTIDAudit(follower.name)
		tm.logger.Warn("replica GTID audit unverified",
			"site", follower.name, "primary", active.name, "error", err)
		return false
	}

	primaryBefore, err := read(active.mysql)
	if err != nil {
		return unverified(fmt.Errorf("read primary: %w", err)), true
	}
	followerSnap, err := read(follower.mysql)
	if err != nil {
		return unverified(fmt.Errorf("read follower: %w", err)), false
	}
	primarySnap, err := read(active.mysql)
	if err != nil {
		return unverified(fmt.Errorf("read primary: %w", err)), true
	}
	followerAfter, err := read(follower.mysql)
	if err != nil {
		return unverified(fmt.Errorf("read follower: %w", err)), false
	}
	if primaryBefore.ServerUUID != primarySnap.ServerUUID || followerSnap.ServerUUID != followerAfter.ServerUUID {
		tm.logger.Info("replica GTID audit discarded: a server changed while it was read",
			"site", follower.name, "primary", active.name)
		return false, false
	}
	followerSet, err := mysql.ParseGTIDSet(followerSnap.GtidExecuted)
	if err != nil {
		return unverified(fmt.Errorf("parse follower GTID set: %w", err)), false
	}
	primarySet, err := mysql.ParseGTIDSet(primarySnap.GtidExecuted)
	if err != nil {
		return unverified(fmt.Errorf("parse primary GTID set: %w", err)), true
	}
	extra := followerSet.Subtract(primarySet)

	tm.mu.Lock()
	if tm.authorityEpoch != epoch || tm.activeSiteLocked() != active.name || tm.gtidAuditSuppressedLocked() {
		tm.mu.Unlock()
		tm.logger.Info("replica GTID audit discarded: authority changed while it ran",
			"site", follower.name, "primary", active.name)
		return false, false
	}
	if uuid := primarySnap.ServerUUID; uuid != "" {
		if tm.gtidAuditPrimaryUUID != "" && tm.gtidAuditPrimaryUUID != uuid {
			// Same site, different server (restored or replaced): every
			// earlier result is against a primary that no longer exists.
			tm.gtidAudit = make(map[string]*gtidAuditSchedule)
		}
		tm.gtidAuditPrimaryUUID = uuid
	}
	tm.stampGTIDAuditLocked(follower.name)
	rec := tm.recovery[follower.name]
	wasBlocked := rec != nil && rec.state == recoveryStateBlocked

	if extra.IsEmpty() {
		if !wasBlocked {
			tm.mu.Unlock()
			return false, false
		}
		delete(tm.recovery, follower.name)
		tm.mu.Unlock()
		metrics.DivergentTransactions.WithLabelValues(follower.name).Set(0)
		tm.logger.Info("divergence cleared: replica holds no transactions the primary lacks",
			"site", follower.name, "primary", active.name)
		return true, false
	}

	set, count := extra.String(), extra.TransactionCount()
	sameSet := wasBlocked && rec.divergentGtid == set
	// The audit refreshes the set and the comparison, but a block it did
	// not create keeps its origin: an old primary whose replication was
	// reattached by hand still holds the transactions the new primary
	// never received, and that block keeps gating the deployment API. A
	// block restored without provenance (an older CRD pruned the field)
	// stays unclassified, which gates the same way. Only a clean proof
	// (above) or a reclone retires either.
	detectedBy := divergenceDetectedByAudit
	if wasBlocked && rec.detectedBy != divergenceDetectedByAudit {
		detectedBy = rec.detectedBy
	}
	sameProvenance := wasBlocked && rec.detectedBy == detectedBy && rec.comparedTo == active.name
	if rec == nil {
		rec = &siteRecovery{}
		tm.recovery[follower.name] = rec
	}
	rec.state = recoveryStateBlocked
	rec.divergentGtid = set
	rec.divergentCount = count
	rec.detectedBy = detectedBy
	rec.comparedTo = active.name
	tm.mu.Unlock()

	metrics.DivergentTransactions.WithLabelValues(follower.name).Set(float64(count))
	if !sameSet {
		tm.logger.Warn("divergence detected",
			"site", follower.name,
			"divergentTransactions", count,
			"divergentGtid", set,
			"oldPrimaryGtid", followerSnap.GtidExecuted,
			"newPrimaryGtid", primarySnap.GtidExecuted,
			"detectedBy", detectedBy,
			"primary", active.name)
	}
	return !sameSet || !sameProvenance, false
}

func (tm *TopologyManager) stampGTIDAudit(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.stampGTIDAuditLocked(site)
}

func (tm *TopologyManager) stampGTIDAuditLocked(site string) {
	tm.gtidAudit[site] = &gtidAuditSchedule{nextAt: tm.clock.Now().Add(gtidAuditInterval)}
}
