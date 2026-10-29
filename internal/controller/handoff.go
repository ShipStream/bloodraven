package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

const (
	// defaultHandoffCatchUp bounds each catch-up phase of a maintenance
	// handoff. The fenced phase, which runs from the fence to the proof of
	// catch-up, is the write pause the handoff imposes before it either
	// promotes or aborts, so it matches the 30s relay-log drain bound of
	// the emergency path.
	defaultHandoffCatchUp = 30 * time.Second
	// defaultHandoffPollInterval is how often the candidate's
	// GTID_EXECUTED is read while waiting.
	defaultHandoffPollInterval = 100 * time.Millisecond
	// handoffRollbackTimeout bounds the source unfence after an abort. It
	// runs on a context detached from the caller's so a cancelled update
	// still hands the source its writes back.
	handoffRollbackTimeout = 10 * time.Second
	// handoffLostSourceRollbackTimeout bounds the unfence attempt when the
	// abort is because the source stopped answering: it will most likely
	// fail, and emergency failover is waiting for the update to end.
	handoffLostSourceRollbackTimeout = 2 * time.Second
)

// HandoffOptions bounds a maintenance handoff. Zero values use the defaults.
type HandoffOptions struct {
	// PreFenceCatchUp bounds the wait, before the source is fenced, for the
	// candidate to reach the source's current GTID set. Writes continue
	// during this wait.
	PreFenceCatchUp time.Duration
	// CatchUp bounds the fenced phase: from the fence, through evicting
	// application connections and reading the fenced GTID set, to the
	// candidate containing it. Every probe in the phase shares this
	// deadline. After it, the handoff promotes or rolls back.
	CatchUp time.Duration
	// PollInterval is how often the candidate is probed while waiting.
	PollInterval time.Duration
	// SourceLossWindow is how long the source may fail its probes in a row
	// during either wait before the handoff gives up, so the emergency
	// failover that ordered updates hold off can act on a lost primary.
	SourceLossWindow time.Duration
	// RestoreReplication is the channel the candidate replicated through
	// (source host and replication credentials). An abort after RESET
	// REPLICA ALL re-creates it so the standby keeps replicating; an empty
	// Host leaves the channel for an operator to restore.
	RestoreReplication mysql.ReplicationSourceOpts
}

func (o HandoffOptions) withDefaults() HandoffOptions {
	if o.PreFenceCatchUp <= 0 {
		o.PreFenceCatchUp = defaultHandoffCatchUp
	}
	if o.CatchUp <= 0 {
		o.CatchUp = defaultHandoffCatchUp
	}
	if o.PollInterval <= 0 {
		o.PollInterval = defaultHandoffPollInterval
	}
	if o.SourceLossWindow <= 0 {
		o.SourceLossWindow = defaultActivePrimaryLossWindow
	}
	return o
}

// ExecuteHandoff moves the primary role from a healthy, writable source to a
// direct replica without losing committed transactions. It is the
// maintenance counterpart of Execute: where the emergency path promotes
// whatever the candidate has after a best-effort fence and relay-log drain,
// the handoff promotes only once the candidate provably contains everything
// the fenced source committed, and otherwise gives the source back its
// writes and returns an error.
//
// The sequence:
//  1. Confirm the source is writable (that is the state an abort restores).
//  2. Read the source's GTID_EXECUTED and wait, unfenced, for the candidate
//     to contain it, so the fenced phase below is normally short. A
//     refusal here pauses no writes.
//  3. Fence the source with super_read_only=ON. A failed fence aborts.
//  4. Kill application connections on the source (best effort).
//  5. Read the fenced source's GTID_EXECUTED; nothing can be added to it now.
//  6. Wait for the candidate to contain that set. A candidate applier error,
//     an unreadable or unparseable set, or the CatchUp budget, which runs
//     from step 3, aborts.
//  7. Promote the candidate, re-checking after STOP REPLICA and after
//     RESET REPLICA ALL that it still contains the fenced set.
//
// Both waits probe the source too: if it stops answering for
// SourceLossWindow, the handoff ends with errActivePrimaryLost so emergency
// failover is not held off for the rest of the budget.
//
// Any abort before the candidate's read_only=OFF is issued clears both
// super_read_only and read_only on the source. Once read_only=OFF has been
// issued the candidate may be accepting writes, so the source stays fenced.
func (f *FailoverController) ExecuteHandoff(ctx context.Context, candidate, source mysql.Checker, candidateSite string, opts HandoffOptions) (string, error) {
	opts = opts.withDefaults()

	var sourceReadOnly bool
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		sourceReadOnly, err = source.CheckReadOnly(probeCtx)
		return err
	}); err != nil {
		return "", f.refuseHandoff(candidateSite, fmt.Errorf("maintenance handoff: probe source: %w", err))
	}
	if sourceReadOnly {
		return "", f.refuseHandoff(candidateSite, errors.New("maintenance handoff: source is read-only; refusing to hand off from a fenced primary"))
	}

	current, err := readGTIDSet(ctx, source)
	if err != nil {
		return "", f.refuseHandoff(candidateSite, fmt.Errorf("maintenance handoff: read source GTID set: %w", err))
	}
	if err := f.waitForCandidateGTIDs(ctx, candidate, source, current, opts.PreFenceCatchUp, opts); err != nil {
		return "", f.refuseHandoff(candidateSite,
			fmt.Errorf("maintenance handoff: candidate %s did not reach the source GTID set before fencing: %w", candidateSite, err))
	}

	// The fenced phase: fence, evict, read the fenced set and prove
	// catch-up, all against one deadline that starts with the fence.
	fencedCtx, cancelFenced := context.WithTimeout(ctx, opts.CatchUp)
	defer cancelFenced()
	if err := withMySQLSafetyTimeout(fencedCtx, func(probeCtx context.Context) error {
		return source.SetSuperReadOnly(probeCtx, true)
	}); err != nil {
		// The statement may have applied before the error was returned.
		return "", f.abortHandoff(ctx, candidateSite, source, fmt.Errorf("maintenance handoff: fence source: %w", err))
	}
	var killed int
	if err := withMySQLSafetyTimeout(fencedCtx, func(probeCtx context.Context) error {
		var err error
		killed, err = source.KillAppConnections(probeCtx)
		return err
	}); err != nil {
		f.logger.Warn("maintenance handoff: failed to kill app connections on source", "candidate", candidateSite, "error", err)
	} else {
		f.logger.Info("maintenance handoff: killed app connections on source", "candidate", candidateSite, "count", killed)
	}

	fencedRaw, fenced, err := readGTIDSetRaw(fencedCtx, source)
	if err != nil {
		return "", f.abortHandoff(ctx, candidateSite, source, fmt.Errorf("maintenance handoff: read fenced source GTID set: %w", err))
	}
	f.logger.Info("maintenance handoff: fenced source", "candidate", candidateSite, "sourceGtid", fencedRaw)
	if err := f.waitForCandidateGTIDs(fencedCtx, candidate, source, fenced, opts.CatchUp, opts); err != nil {
		cause := fmt.Errorf("maintenance handoff: candidate %s did not reach the fenced source GTID set: %w", candidateSite, err)
		if errors.Is(err, errActivePrimaryLost) {
			return "", f.abortHandoffWithin(ctx, handoffLostSourceRollbackTimeout, candidateSite, source, cause)
		}
		return "", f.abortHandoff(ctx, candidateSite, source, cause)
	}
	cancelFenced()
	f.logger.Info("maintenance handoff: candidate caught up to fenced source", "candidate", candidateSite, "sourceGtid", fencedRaw)

	promotionGtid, stage, err := f.promoteVerified(ctx, candidate, candidateSite, fenced)
	if err != nil {
		if stage == promoteStageWriteAttempted {
			return "", fmt.Errorf("maintenance handoff: promote %s: %w (source left fenced: the candidate may already accept writes)", candidateSite, err)
		}
		abortErr := fmt.Errorf("maintenance handoff: promote %s: %w", candidateSite, err)
		switch stage {
		case promoteStageReplicaStopped:
			// The channel is intact; let the candidate keep replicating.
			if restartErr := f.restartCandidateReplication(ctx, candidate, candidateSite, nil); restartErr != nil {
				abortErr = fmt.Errorf("%w (candidate replication restart failed: %v)", abortErr, restartErr)
			}
		case promoteStageReplicaReset:
			// The channel may be gone; re-create it so the standby keeps
			// replicating from the source it was proven against.
			if opts.RestoreReplication.Host == "" {
				abortErr = fmt.Errorf("%w (candidate replication may have been reset and must be reconfigured)", abortErr)
				f.logger.Warn("maintenance handoff: failed to restore candidate replication channel after abort",
					"candidate", candidateSite, "error", "no replication source configured for the handoff")
			} else if restoreErr := f.restartCandidateReplication(ctx, candidate, candidateSite, &opts.RestoreReplication); restoreErr != nil {
				abortErr = fmt.Errorf("%w (candidate replication may have been reset and could not be restored: %v)", abortErr, restoreErr)
			}
		}
		return "", f.abortHandoff(ctx, candidateSite, source, abortErr)
	}
	return promotionGtid, nil
}

// restartCandidateReplication restarts replication on the candidate after
// an aborted promotion, best effort, on the pooled checker (the pinned
// promotion connection may be what failed). With channel set, it first
// re-creates the channel, for an abort that may have followed RESET
// REPLICA ALL; CHANGE REPLICATION SOURCE with auto-positioning is safe on
// a stopped channel that still exists.
func (f *FailoverController) restartCandidateReplication(ctx context.Context, candidate mysql.Checker, candidateSite string, channel *mysql.ReplicationSourceOpts) error {
	restartCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handoffRollbackTimeout)
	defer cancel()
	if channel != nil {
		if err := candidate.ChangeReplicationSource(restartCtx, *channel); err != nil {
			f.logger.Warn("maintenance handoff: failed to restore candidate replication channel after abort", "candidate", candidateSite, "error", err)
			return err
		}
	}
	if err := candidate.StartReplica(restartCtx); err != nil {
		f.logger.Warn("maintenance handoff: failed to restart replication on candidate after abort", "candidate", candidateSite, "error", err)
		return err
	}
	if channel != nil {
		f.logger.Info("maintenance handoff: restored candidate replication channel", "candidate", candidateSite, "host", channel.Host)
	}
	return nil
}

// refuseHandoff logs a handoff refused before the source was fenced, so
// no writes were paused and there is nothing to roll back.
func (f *FailoverController) refuseHandoff(candidateSite string, cause error) error {
	f.logger.Warn("maintenance handoff refused; source not fenced", "candidate", candidateSite, "error", cause)
	return cause
}

// abortHandoff gives the source its writes back after a handoff failed
// before the candidate could accept writes, and returns cause annotated with
// the rollback outcome. Both flags are cleared: super_read_only=ON also sets
// read_only=ON, and clearing super_read_only alone leaves it set.
func (f *FailoverController) abortHandoff(ctx context.Context, candidateSite string, source mysql.Checker, cause error) error {
	return f.abortHandoffWithin(ctx, handoffRollbackTimeout, candidateSite, source, cause)
}

func (f *FailoverController) abortHandoffWithin(ctx context.Context, budget time.Duration, candidateSite string, source mysql.Checker, cause error) error {
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	rbErr := errors.Join(source.SetSuperReadOnly(rbCtx, false), source.SetReadOnly(rbCtx, false))
	if rbErr != nil {
		f.logger.Error("maintenance handoff: rollback failed: source may be left read-only", "candidate", candidateSite, "cause", cause, "error", rbErr)
		return fmt.Errorf("%w; source unfence failed: %v", cause, rbErr)
	}
	f.logger.Warn("maintenance handoff aborted; source unfenced", "candidate", candidateSite, "error", cause)
	return cause
}

// waitForCandidateGTIDs polls the candidate until its GTID_EXECUTED contains
// want, the candidate's applier reports an error, the source stops
// answering for opts.SourceLossWindow, or timeout elapses. Every probe runs
// under the wait's deadline, so a slow probe cannot stretch it.
func (f *FailoverController) waitForCandidateGTIDs(ctx context.Context, candidate, source mysql.Checker, want mysql.GTIDSet, timeout time.Duration, opts HandoffOptions) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	timer := time.NewTimer(opts.PollInterval)
	defer timer.Stop()
	var (
		have            mysql.GTIDSet
		lastReadErr     error
		sourceLostSince time.Time
	)
	for {
		_, parsed, err := readGTIDSetRaw(waitCtx, candidate)
		switch {
		case err == nil:
			have, lastReadErr = parsed, nil
			if have.Contains(want) && waitCtx.Err() == nil {
				return nil
			}
		case errors.Is(err, errUnparseableGTID):
			return err
		default:
			lastReadErr = err
		}

		var rs *mysql.ReplicaStatus
		if probeErr := withMySQLSafetyTimeout(waitCtx, func(probeCtx context.Context) error {
			var err error
			rs, err = candidate.ShowReplicaStatus(probeCtx)
			return err
		}); probeErr == nil && rs != nil && rs.LastSQLError != "" {
			return fmt.Errorf("candidate SQL thread error: %d: %s", rs.LastSQLErrno, rs.LastSQLError)
		}

		sourceErr := withMySQLSafetyTimeout(waitCtx, func(probeCtx context.Context) error {
			_, err := source.CheckReadOnly(probeCtx)
			return err
		})
		switch {
		case sourceErr == nil:
			sourceLostSince = time.Time{}
		case waitCtx.Err() != nil:
			// The wait's own deadline or cancellation, not evidence
			// about the source.
		case sourceLostSince.IsZero():
			sourceLostSince = time.Now()
		case time.Since(sourceLostSince) >= opts.SourceLossWindow:
			return fmt.Errorf("%w: %v", errActivePrimaryLost, sourceErr)
		}

		if waitCtx.Err() == nil {
			select {
			case <-waitCtx.Done():
			case <-timer.C:
				timer.Reset(opts.PollInterval)
				continue
			}
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}
		if have == nil {
			return fmt.Errorf("timed out after %s; last GTID read failed: %w", timeout, lastReadErr)
		}
		missing := want.Subtract(have)
		if missing.IsEmpty() {
			return fmt.Errorf("timed out after %s: catch-up was confirmed only after the budget expired", timeout)
		}
		return fmt.Errorf("timed out after %s with %d transactions missing (%s)",
			timeout, missing.TransactionCount(), truncateGtidHint(missing.String()))
	}
}

var errUnparseableGTID = errors.New("unparseable GTID set")

func readGTIDSet(ctx context.Context, c gtidExecutedReader) (mysql.GTIDSet, error) {
	_, set, err := readGTIDSetRaw(ctx, c)
	return set, err
}

// readGTIDSetRaw reads and parses GTID_EXECUTED with the MySQL safety
// timeout. A parse failure wraps errUnparseableGTID.
// gtidExecutedReader is what readGTIDSetRaw needs: a mysql.Checker or a
// pinned mysql.Session.
type gtidExecutedReader interface {
	GetGtidExecuted(ctx context.Context) (string, error)
}

func readGTIDSetRaw(ctx context.Context, c gtidExecutedReader) (string, mysql.GTIDSet, error) {
	var raw string
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		raw, err = c.GetGtidExecuted(probeCtx)
		return err
	}); err != nil {
		return "", nil, err
	}
	set, err := mysql.ParseGTIDSet(raw)
	if err != nil {
		return "", nil, fmt.Errorf("%w %q: %v", errUnparseableGTID, truncateGtidHint(raw), err)
	}
	return raw, set, nil
}
