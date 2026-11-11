package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// FailoverController orchestrates MySQL failover between sites.
type FailoverController struct {
	logger *slog.Logger
	// clock bounds the best-effort binary log rotations (rotateBinlog), so
	// a simulation's fake clock decides when one has taken too long.
	clock clock.Clock
}

// NewFailoverController creates a new FailoverController.
func NewFailoverController(logger *slog.Logger) *FailoverController {
	return NewFailoverControllerWithClock(logger, clock.RealClock{})
}

// NewFailoverControllerWithClock creates a FailoverController with an
// injectable clock, for tests and simulations that drive time themselves.
func NewFailoverControllerWithClock(logger *slog.Logger, clk clock.Clock) *FailoverController {
	return &FailoverController{logger: logger, clock: clk}
}

// BinlogRotationTimeout bounds one best-effort FLUSH BINARY LOGS on the
// failover path, connection setup included. A flush that has not answered
// by then is abandoned and the promotion, handoff or reclone continues.
const BinlogRotationTimeout = time.Second

// binlogFlusher is the part of mysql.Checker a rotation needs.
type binlogFlusher interface {
	FlushBinaryLogs(ctx context.Context) error
}

// binlogRotation names one use of rotateBinlog by its two log messages
// (both are in the log schema).
type binlogRotation struct {
	done   string
	failed string
}

var (
	// promotionRotation seals a promotion candidate's replica-era binary
	// log before its writes are enabled.
	promotionRotation = binlogRotation{done: "promotion binlog rotated", failed: "promotion binlog rotation failed"}
	// outgoingPrimaryRotation seals a fenced primary's writable-era tail
	// during a planned handoff, so its sidecar can archive it.
	outgoingPrimaryRotation = binlogRotation{done: "outgoing primary binlog rotated", failed: "outgoing primary binlog rotation failed"}
	// preRecloneRotation seals a reclone recipient's binary log before
	// CLONE wipes its data directory.
	preRecloneRotation = binlogRotation{done: "pre-reclone binlog rotated", failed: "pre-reclone binlog rotation failed"}
)

// rotateBinlog sends one best-effort FLUSH BINARY LOGS to site and reports
// whether it was acknowledged within BinlogRotationTimeout. It never blocks
// its caller longer than that and never fails it: on an error, a timeout or
// a cancelled ctx it logs r.failed at WARN and returns false; on success it
// logs r.done with the duration, so the cost is measurable.
//
// The flush runs in a goroutine and the wait is bounded by the controller's
// clock rather than by a context deadline: the caller continues at the
// bound even if the statement, or the connection attempt behind it, ignores
// cancellation (a blackholed address answers only at the driver's own I/O
// deadline). The abandoned call is cancelled; mysql.Checker.FlushBinaryLogs
// runs on a connection of its own, so that neither breaks a pinned session
// nor occupies a pooled connection. The server may still complete an
// abandoned rotation later, even after writes were enabled. That is one
// more rotation at an arbitrary point, which nothing depends on.
func (f *FailoverController) rotateBinlog(ctx context.Context, c binlogFlusher, site string, r binlogRotation, attrs ...any) bool {
	clk := f.clock
	if clk == nil {
		clk = clock.RealClock{}
	}
	start := clk.Now()
	// The timer exists before the flush starts: a simulated flush advances
	// a fake clock from inside the call.
	timer := clk.NewTimer(BinlogRotationTimeout)
	defer timer.Stop()
	flushCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.FlushBinaryLogs(flushCtx) }()
	var err error
	timedOut := false
	select {
	case err = <-done:
	case <-timer.C():
		timedOut = true
		err = fmt.Errorf("no answer within %s; continuing without it", BinlogRotationTimeout)
	case <-ctx.Done():
		err = ctx.Err()
	}
	fields := append([]any{"site", site, "durationMs", clk.Since(start).Milliseconds()}, attrs...)
	if err != nil {
		f.logger.Warn(r.failed, append(fields, "timedOut", timedOut, "error", err)...)
		return false
	}
	f.logger.Info(r.done, fields...)
	return true
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
// path: STOP REPLICA, RESET REPLICA ALL, record GTID_EXECUTED, rotate the
// binary log (best effort, rotateBinlog), clear
// super_read_only, then read_only. Each statement takes any pooled
// connection, and a failed GTID read is only logged: with the primary
// gone, availability comes first.
//
// The rotation sits after the GTID read and before the clears: the
// candidate is read-only with no replication channel, so its executed set
// cannot change in between, and the file the rotation opens has a
// Previous_gtids header equal to the recorded promotion GTID. Everything
// the candidate logged as a replica is then in sealed files its sidecar can
// archive, and its first own transaction starts a file.
//
// That property is an expectation, not something this path proves, and a
// reader of the returned (recorded) promotion GTID must treat an empty or
// mismatching one as unknown. It does not hold:
//
//   - when the rotation was not acknowledged: a failed one leaves the
//     replica-era file open, and one abandoned at its bound may still
//     complete on the server later, even after the clears. The promotion
//     is the same either way.
//   - when the GTID read failed: the rotation is still sent and the
//     returned set is empty, so the recorded promotion GTID says nothing
//     about the file the rotation opened.
//   - when the three statements do not meet one unchanged, read-only
//     server. The GTID read and the clears take pooled connections and
//     the rotation dials a connection of its own; all reach the candidate
//     by the same address, and nothing proves they reach the same server
//     (promoteVerified pins one connection for that reason; the rotation is
//     off that connection there too). Nothing re-reads read_only between
//     the GTID read and the rotation either: a candidate that is already
//     writable, or one with read_only but not super_read_only and a
//     privileged session writing, can commit in between, and the new file
//     then starts past the recorded set. An already writable candidate is
//     a path the operator takes by design: a split brain with no failover
//     history, resolved by spec.splitBrainPolicy.sitePriorities, fences
//     the other sites and runs this promotion on the preferred site, which
//     is one of the writable ones.
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
	f.rotateBinlog(ctx, candidate, candidateSite, promotionRotation)
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
	return f.promoteVerifiedFrom(ctx, sess, candidate, candidateSite, require, PromotionStageNotStarted, promoteHooks{})
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
	// flush wraps the promotion's FLUSH BINARY LOGS: it calls send to send
	// it, or returns without calling it. The planned failover sends it at
	// most once per run and process. nil sends it on every attempt.
	flush func(send func())
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
//
// Before the gate, every stage but WasWritable rotates the candidate's
// binary log through flusher (nil skips it), best effort, as promote does:
// the candidate is read-only, has no channel and holds exactly the proven
// set, so the file the rotation opens starts at the promotion GTID set.
// As in promote that holds only for an acknowledged rotation, and it rests
// on flusher's connection reaching the server sess is pinned to (it dials
// the same address; only sess is proven). A promotion resumed from
// WasWritable, and a planned failover whose re-run finds the target
// already writable (it never gets here), send none: whether the attempt
// that cleared the fences rotated first is not known to them, and a new
// operator process does not rotate again.
// The rotation is not a stage and has no memo entry: it changes nothing
// classifyPromotionStage reads, so a resumed attempt classifies the same
// with or without it, and repeating it only adds an empty file. It is sent
// on flusher's own connection, not on sess: cancelling a statement on sess
// would close the pinned connection and fail the clears that follow. It is
// guarded like a statement (h.guard), so a pass that lost its claim does
// not send it. From WasWritable the candidate already holds its own
// transactions and may take writes again at once; a rotation there would
// seal nothing the promotion is about.
func (f *FailoverController) promoteVerifiedFrom(ctx context.Context, sess mysql.Session, flusher binlogFlusher, candidateSite string, require mysql.GTIDSet,
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
	if flusher != nil && from != PromotionStageWasWritable {
		if h.guard != nil {
			if err := h.guard(); err != nil {
				return "", reached, err
			}
		}
		send := func() { f.rotateBinlog(ctx, flusher, candidateSite, promotionRotation) }
		if h.flush != nil {
			h.flush(send)
		} else {
			send()
		}
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
		// The statement may have applied: the proven set comes back with
		// the error, so a caller that confirms the candidate writable can
		// record the promotion it made.
		return promotionGtid, promoteStageWriteAttempted, err
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
