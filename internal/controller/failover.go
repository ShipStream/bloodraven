package controller

import (
	"context"
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

	if err := sess.StopReplica(ctx); err != nil {
		return "", promoteStageReplicaStopped, err
	}
	if _, err := requireCandidateGTIDs(ctx, sess, candidateSite, require); err != nil {
		return "", promoteStageReplicaStopped, fmt.Errorf("after STOP REPLICA: %w", err)
	}
	if err := sess.ResetReplicaAll(ctx); err != nil {
		return "", promoteStageReplicaReset, err
	}
	promotionGtid, err := requireCandidateGTIDs(ctx, sess, candidateSite, require)
	if err != nil {
		return "", promoteStageReplicaReset, fmt.Errorf("after RESET REPLICA ALL: %w", err)
	}
	if err := sess.SetSuperReadOnly(ctx, false); err != nil {
		return "", promoteStageReplicaReset, err
	}
	if err := sess.SetReadOnly(ctx, false); err != nil {
		return "", promoteStageWriteAttempted, err
	}
	f.logger.Info("failover complete", "promotedSite", candidateSite, "promotionGtid", promotionGtid)
	return promotionGtid, promoteStageWriteAttempted, nil
}

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
		return "", fmt.Errorf("candidate GTID set no longer contains the fenced source set: %d transactions missing (%s)",
			missing.TransactionCount(), truncateGtidHint(missing.String()))
	}
	if extra := have.Subtract(want); !extra.IsEmpty() {
		return "", newDivergentCandidateError(candidateSite, extra)
	}
	return raw, nil
}
