package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// FailoverController orchestrates MySQL failover between sites.
type FailoverController struct {
	logger *slog.Logger
}

// NewFailoverController creates a new FailoverController.
func NewFailoverController(logger *slog.Logger) *FailoverController {
	return &FailoverController{logger: logger}
}

// Execute performs a failover: fences old primary, drains relay logs on candidate, promotes candidate.
// Returns the candidate's GTID executed set captured just before promotion (before it accepts writes).
func (f *FailoverController) Execute(ctx context.Context, candidate mysql.Checker, oldPrimary mysql.Checker, candidateSite string) (string, error) {
	// 1. Fence old primary: SET GLOBAL super_read_only=ON (ignore error if unreachable).
	if oldPrimary != nil {
		if err := oldPrimary.SetSuperReadOnly(ctx, true); err != nil {
			f.logger.Warn("failed to fence old primary (may be unreachable)", "error", err)
		} else {
			f.logger.Info("fenced old primary with super_read_only=ON")
		}

		// 2. Kill app connections on old primary to force reconnection via DNS.
		if killed, err := oldPrimary.KillAppConnections(ctx); err != nil {
			f.logger.Warn("failed to kill app connections on old primary", "error", err)
		} else {
			f.logger.Info("killed app connections on old primary", "count", killed)
		}
	}

	// 3. On candidate: WaitForRelayLogDrain (30s timeout).
	if err := candidate.WaitForRelayLogDrain(ctx, 30*time.Second); err != nil {
		f.logger.Warn("relay log drain did not complete cleanly, proceeding with promotion", "error", err)
	} else {
		f.logger.Info("relay log drain complete")
	}

	return f.promote(ctx, candidate, candidateSite)
}

// promoteStage records how far promote got before it returned, so a caller
// that can still undo the handoff knows what it is undoing.
type promoteStage int

const (
	// promoteStageNotStarted: nothing was sent to the candidate.
	promoteStageNotStarted promoteStage = iota
	// promoteStageReplicaStopped: STOP REPLICA was issued (and may have
	// taken effect even if it returned an error); the channel is still
	// configured.
	promoteStageReplicaStopped
	// promoteStageReplicaReset: RESET REPLICA ALL was issued (and may have
	// taken effect even if it returned an error); the candidate may no
	// longer have a replication channel but is still read-only.
	promoteStageReplicaReset
	// promoteStageWriteAttempted: read_only=OFF was issued. Whether or not
	// it returned an error, the candidate may accept writes.
	promoteStageWriteAttempted
)

// promote turns a candidate into a writable primary for the emergency
// path: STOP REPLICA, RESET REPLICA ALL, record GTID_EXECUTED, clear
// super_read_only, then read_only. Each statement takes any pooled
// connection, and a failed GTID read is only logged: with the primary
// gone, availability comes first.
func (f *FailoverController) promote(ctx context.Context, candidate mysql.Checker, candidateSite string) (string, error) {
	if err := candidate.StopReplica(ctx); err != nil {
		return "", err
	}
	if err := candidate.ResetReplicaAll(ctx); err != nil {
		return "", err
	}
	// Record GTID before promotion (high-water mark of replicated data).
	promotionGtid, err := candidate.GetGtidExecuted(ctx)
	if err != nil {
		f.logger.Warn("failed to record promotion GTID", "error", err)
	}
	// Clear super_read_only (may have been set by sidecar fencing or previous state).
	if err := candidate.SetSuperReadOnly(ctx, false); err != nil {
		return "", err
	}
	if err := candidate.SetReadOnly(ctx, false); err != nil {
		return "", err
	}
	f.logger.Info("failover complete", "promotedSite", candidateSite, "promotionGtid", promotionGtid)
	return promotionGtid, nil
}

// promoteVerified is the maintenance handoff's promotion: the same steps
// as promote, all on one pinned connection to the candidate, with the
// fenced source set re-proven on that connection after STOP REPLICA and on
// the recorded promotion GTID after RESET REPLICA ALL. The connection
// cannot silently move to another server, so the server that passed the
// proof is the one made writable; if the connection breaks (the candidate
// restarted or its address now reaches a different pod), the next
// statement fails and the promotion stops. An unreadable or unparseable
// set, a set missing fenced transactions, or one holding transactions the
// fenced source lacks (a *DivergentCandidateError) also stops it.
func (f *FailoverController) promoteVerified(ctx context.Context, candidate mysql.Checker, candidateSite string, require mysql.GTIDSet) (string, promoteStage, error) {
	sess, err := candidate.PinSession(ctx)
	if err != nil {
		return "", promoteStageNotStarted, err
	}
	defer func() { _ = sess.Close() }()
	return f.promoteVerifiedFrom(ctx, sess, candidateSite, require, PromotionStageNotStarted, promoteHooks{})
}

// promoteHooks instrument promoteVerifiedFrom for the planned failover.
// The maintenance handoff passes the zero value.
type promoteHooks struct {
	// before runs immediately before each statement, with the stage it
	// sends; the planned failover records it in its promotion memo.
	before func(promoteMemoStage)
	// after runs after each statement returns.
	after func()
	// gate runs immediately before SET super_read_only=OFF; an error
	// stops the promotion with nothing written by this attempt.
	gate func(ctx context.Context) error
	// guard runs immediately before each statement, after the gate's
	// probes for the writes: the planned failover re-checks the leader
	// context and the run's claim there (§3.3), so a pass that lost either
	// while it waited on a probe sends nothing more. An error stops the
	// promotion before the statement.
	guard func() error
	// statementTimeout, when positive, bounds each statement on its own
	// (§2.4 step 8: 5s per statement for the planned failover), so one slow
	// statement cannot use up the pass.
	statementTimeout time.Duration
	// planned suppresses the "failover complete" log line: the planned
	// failover emits it only after its live confirmation.
	planned bool
}

func (h promoteHooks) send(ctx context.Context, s promoteMemoStage, stmt func(context.Context) error) error {
	if h.guard != nil {
		if err := h.guard(); err != nil {
			return err
		}
	}
	if h.before != nil {
		h.before(s)
	}
	var err error
	if h.statementTimeout > 0 {
		stmtCtx, cancel := context.WithTimeout(ctx, h.statementTimeout)
		err = stmt(stmtCtx)
		cancel()
	} else {
		err = stmt(ctx)
	}
	if h.after != nil {
		h.after()
	}
	return err
}

// promoteVerifiedFrom resumes the staged pinned promotion on sess from the
// classified stage (§7.1). It never repeats a STOP or RESET the live stage
// is already past, and never sends KILL or anything to another site:
//
//	NotStarted      STOP, prove (= require), RESET, prove, gate, clear SRO, clear RO
//	ReplicaStopped  prove, RESET, prove, gate, clear SRO, clear RO
//	ReplicaReset    gate, clear SRO, clear RO
//	WasWritable     gate, clear SRO, clear RO
//
// From ReplicaReset and WasWritable the gate re-reads the GTID set itself.
// The returned promoteStage is promoteVerified's: how far it got.
func (f *FailoverController) promoteVerifiedFrom(ctx context.Context, sess mysql.Session, candidateSite string, require mysql.GTIDSet,
	from PromotionStage, h promoteHooks) (string, promoteStage, error) {
	reached := promoteStageNotStarted
	promotionGtid := ""
	switch from {
	case PromotionStageNotStarted, PromotionStageReplicaStopped:
		if from == PromotionStageNotStarted {
			if err := h.send(ctx, promoteMemoStopSent, sess.StopReplica); err != nil {
				return "", promoteStageReplicaStopped, err
			}
		}
		reached = promoteStageReplicaStopped
		if _, err := requireCandidateGTIDs(ctx, sess, candidateSite, require); err != nil {
			return "", reached, fmt.Errorf("after STOP REPLICA: %w", err)
		}
		if err := h.send(ctx, promoteMemoResetSent, sess.ResetReplicaAll); err != nil {
			return "", promoteStageReplicaReset, err
		}
		reached = promoteStageReplicaReset
		raw, err := requireCandidateGTIDs(ctx, sess, candidateSite, require)
		if err != nil {
			return "", reached, fmt.Errorf("after RESET REPLICA ALL: %w", err)
		}
		promotionGtid = raw
	case PromotionStageReplicaReset, PromotionStageWasWritable:
		reached = promoteStageReplicaReset
		promotionGtid = require.String()
	default:
		return "", reached, fmt.Errorf("promotion cannot resume from stage %q", from)
	}
	if h.gate != nil {
		if err := h.gate(ctx); err != nil {
			return "", reached, err
		}
	}
	if err := h.send(ctx, promoteMemoSROClearSent, func(c context.Context) error { return sess.SetSuperReadOnly(c, false) }); err != nil {
		return "", reached, err
	}
	if err := h.send(ctx, promoteMemoROClearSent, func(c context.Context) error { return sess.SetReadOnly(c, false) }); err != nil {
		return "", promoteStageWriteAttempted, err
	}
	if !h.planned {
		f.logger.Info("failover complete", "promotedSite", candidateSite, "promotionGtid", promotionGtid)
	}
	return promotionGtid, promoteStageWriteAttempted, nil
}

// errCandidateGTIDMismatch marks a proof that read the candidate's set and
// found it missing part of the required set (as opposed to a failed read);
// a candidate holding extras returns a *DivergentCandidateError instead.
var errCandidateGTIDMismatch = errors.New("candidate GTID set does not equal the fenced source set")

// requireCandidateGTIDs reads the candidate's GTID_EXECUTED and fails
// unless it equals want: it must contain every fenced source transaction
// and hold none the source lacks. It returns the raw set on success.
func requireCandidateGTIDs(ctx context.Context, candidate gtidExecutedReader, candidateSite string, want mysql.GTIDSet) (string, error) {
	raw, have, err := readGTIDSetRaw(ctx, candidate)
	if err != nil {
		return "", fmt.Errorf("read candidate GTID set: %w", err)
	}
	if !have.Contains(want) {
		missing := want.Subtract(have)
		return "", fmt.Errorf("%w: candidate GTID set no longer contains the fenced source set: %d transactions missing (%s)",
			errCandidateGTIDMismatch, missing.TransactionCount(), truncateGtidHint(missing.String()))
	}
	if extra := have.Subtract(want); !extra.IsEmpty() {
		return "", newDivergentCandidateError(candidateSite, extra)
	}
	return raw, nil
}
