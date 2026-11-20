package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// The emergency path's catch-up gate (issue #202). An emergency failover
// fences the old primary best effort. When the old primary answers that
// fence it is alive, and it is the only copy of every transaction it
// committed that the candidate's receiver has not fetched yet: a flapping
// or overloaded primary, a partition between the sites that the operator
// is not part of, or a primary that came back between the poll that
// declared it unreachable and the fence. Draining the candidate's relay
// log applies only what it already received, so promoting it then
// discards those transactions although their only holder answers. The
// gate waits, with the old primary fenced, until the candidate executes
// the old primary's whole fenced set, and otherwise gives up the attempt
// without touching the candidate.
const (
	// defaultEmergencyCatchUp bounds the gate. The old primary refuses
	// writes for the whole wait, so this is also the write pause an
	// aborted attempt costs. It matches the relay-log drain bound and the
	// maintenance handoff's fenced budget.
	defaultEmergencyCatchUp = 30 * time.Second
	// defaultEmergencyCatchUpPoll is how often the candidate is probed.
	defaultEmergencyCatchUpPoll = 100 * time.Millisecond
	// defaultEmergencyOldPrimaryLoss is how long the fenced old primary may
	// fail its probes in a row before the gate stops waiting for it and
	// the promotion goes ahead, as it does when the fence fails: a dead
	// primary cannot send the candidate anything more.
	defaultEmergencyOldPrimaryLoss = 5 * time.Second
)

// FailoverOptions adjusts one emergency failover (ExecuteWithOptions).
// Zero values use the defaults.
type FailoverOptions struct {
	// OldPrimarySite names the old primary in log lines and errors.
	OldPrimarySite string
	// AllowDivergentOldPrimary skips the catch-up gate. Only split-brain
	// auto-resolution sets it: both sides accepted writes, so the winner
	// can never contain the loser's set, and
	// spec.splitBrainPolicy.sitePriorities has already chosen to leave the
	// loser's transactions behind for old-primary recovery to report.
	AllowDivergentOldPrimary bool
	// CatchUp bounds the gate, from the end of the fence to the proof.
	CatchUp time.Duration
	// PollInterval is how often the candidate is probed while waiting.
	PollInterval time.Duration
	// OldPrimaryLossWindow is how long the fenced old primary may fail its
	// probes before the gate stops waiting for it.
	OldPrimaryLossWindow time.Duration
}

func (o FailoverOptions) withDefaults() FailoverOptions {
	if o.CatchUp <= 0 {
		o.CatchUp = defaultEmergencyCatchUp
	}
	if o.PollInterval <= 0 {
		o.PollInterval = defaultEmergencyCatchUpPoll
	}
	if o.OldPrimaryLossWindow <= 0 {
		o.OldPrimaryLossWindow = defaultEmergencyOldPrimaryLoss
	}
	return o
}

// Old-primary outcomes of a refused emergency failover
// (CandidateNotCaughtUpError.OldPrimary).
const (
	// OldPrimaryUnfenced: it was writable before the failover fenced it,
	// and its writes were given back.
	OldPrimaryUnfenced = "unfenced"
	// OldPrimaryLeftFenced: it was read-only before the failover fenced it
	// (its sidecar's fence, or the read_only every mysqld boots with), so
	// the failover did not lift anything; the primary re-assert decides.
	OldPrimaryLeftFenced = "leftFenced"
	// OldPrimaryNotReleased: it was writable before the fence, but the
	// release found it restarted, without transactions it held at the
	// fence, or could not unfence it; the error says which.
	OldPrimaryNotReleased = "notReleased"
)

// CandidateNotCaughtUpError is an emergency failover refused by the
// catch-up gate: the old primary answered the fence, and the candidate did
// not execute the old primary's fenced GTID set. Nothing was sent to the
// candidate. OldPrimary says what was done with the old primary's fence.
type CandidateNotCaughtUpError struct {
	Candidate      string
	OldPrimarySite string
	// MissingTransactions and MissingGtid are what the candidate lacked at
	// its last successful read (MissingGtid truncated for long sets); both
	// are zero when the candidate's or the old primary's set was never
	// read.
	MissingTransactions int64
	MissingGtid         string
	// Waited is how long the gate waited, on the controller's clock.
	Waited time.Duration
	// Reason is why the gate ended: the candidate's applier stopped, its
	// receiver stopped short of the set, a set could not be read or
	// parsed, the budget ran out, or the caller was cancelled.
	Reason string
	// OldPrimary is one of OldPrimaryUnfenced, OldPrimaryLeftFenced or
	// OldPrimaryNotReleased.
	OldPrimary string
	// Err is Reason with the release outcome appended.
	Err error
}

func (e *CandidateNotCaughtUpError) Error() string {
	return fmt.Sprintf("emergency failover to %s refused: the old primary %s answered the fence and the candidate has not caught up with it: %v",
		e.Candidate, e.OldPrimarySite, e.Err)
}

func (e *CandidateNotCaughtUpError) Unwrap() error { return e.Err }

// OldPrimaryNotFencedError is an emergency failover refused because the
// old primary answered writable after its fence failed: promoting the
// candidate would leave two writable primaries, and the candidate without
// whatever the old primary committed since it last replicated.
type OldPrimaryNotFencedError struct {
	OldPrimarySite string
	Err            error
}

func (e *OldPrimaryNotFencedError) Error() string {
	return fmt.Sprintf("emergency failover refused: the old primary %s still answers writable and could not be fenced: %v", e.OldPrimarySite, e.Err)
}

func (e *OldPrimaryNotFencedError) Unwrap() error { return e.Err }

// SetCatchUpSleepForSim replaces the wait between the catch-up gate's
// probes. The deterministic simulation advances its fake clock there, so
// the gate spends simulated time and no wall-clock time. Call it before the
// controller is used.
func (f *FailoverController) SetCatchUpSleepForSim(fn func(ctx context.Context, d time.Duration) error) {
	f.catchUpSleep = fn
}

func (f *FailoverController) clk() clock.Clock {
	if f.clock == nil {
		return clock.RealClock{}
	}
	return f.clock
}

func (f *FailoverController) sleepCatchUp(ctx context.Context, d time.Duration) error {
	if f.catchUpSleep != nil {
		return f.catchUpSleep(ctx, d)
	}
	t := f.clk().NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
		return nil
	}
}

// fenceOldPrimary is the emergency path's fence. It reports whether the old
// primary answered writable before it (what an aborted attempt may give
// back) and whether it is fenced now. A fence that returns an error is
// checked with one more probe when the old primary answered the first one,
// because the statement may have applied (a timeout) or been blocked on a
// live server: read-only now counts as fenced, and writable now refuses the
// failover (OldPrimaryNotFencedError). A probe that fails says the old
// primary is unreachable, and the promotion goes ahead without waiting.
func (f *FailoverController) fenceOldPrimary(ctx context.Context, oldPrimary mysql.Checker, oldPrimarySite string) (wasWritable, fenced bool, err error) {
	var readOnly bool
	probeErr := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		readOnly, err = oldPrimary.CheckReadOnly(probeCtx)
		return err
	})
	wasWritable = probeErr == nil && !readOnly
	fenceErr := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		return oldPrimary.SetSuperReadOnly(probeCtx, true)
	})
	if fenceErr == nil {
		f.logger.Info("fenced old primary with super_read_only=ON")
		return wasWritable, true, nil
	}
	f.logger.Warn("failed to fence old primary (may be unreachable)", "error", fenceErr)
	if probeErr != nil {
		return wasWritable, false, nil
	}
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		readOnly, err = oldPrimary.CheckReadOnly(probeCtx)
		return err
	}); err != nil {
		return wasWritable, false, nil
	}
	if !readOnly {
		return wasWritable, false, &OldPrimaryNotFencedError{OldPrimarySite: oldPrimarySite, Err: fenceErr}
	}
	f.logger.Warn("emergency failover: old primary answers read-only after its fence failed; treating it as fenced",
		"oldPrimary", oldPrimarySite, "error", fenceErr)
	return wasWritable, true, nil
}

// catchUpOutcome is how the emergency catch-up gate ended without a refusal.
type catchUpOutcome int

const (
	// catchUpReached: the candidate executes the old primary's fenced set.
	catchUpReached catchUpOutcome = iota
	// catchUpOldPrimaryLost: the old primary stopped answering; the
	// promotion goes ahead with what the candidate has.
	catchUpOldPrimaryLost
)

// catchUpRefusal is the gate's refusal before the old primary's fence is
// released (CandidateNotCaughtUpError without the release outcome).
type catchUpRefusal struct {
	reason  string
	missing mysql.GTIDSet
	waited  time.Duration
}

// waitForEmergencyCatchUp reads the fenced old primary's GTID_EXECUTED and
// polls the candidate until its GTID_EXECUTED contains that set. The fence
// stops the set from growing, so the wait ends whenever the candidate's
// replication works. It ends early, refused, when the candidate's applier
// reports an error or its receiver has stopped short of the set: neither
// recovers by itself. It ends without a refusal when the old primary stops
// answering for opts.OldPrimaryLossWindow. The budget is measured on the
// controller's clock and, as a backstop, on a real-time context.
func (f *FailoverController) waitForEmergencyCatchUp(ctx context.Context, candidate, oldPrimary mysql.Checker, candidateSite string, opts FailoverOptions) (string, mysql.GTIDSet, catchUpOutcome, *catchUpRefusal) {
	clk := f.clk()
	start := clk.Now()
	waitCtx, cancel := context.WithTimeout(ctx, opts.CatchUp)
	defer cancel()
	var (
		fencedRaw  string
		fencedSet  mysql.GTIDSet
		haveFenced bool
		have       mysql.GTIDSet
		lastErr    error
		lostSince  time.Time
		lostKnown  bool
	)
	refuse := func(reason string) (string, mysql.GTIDSet, catchUpOutcome, *catchUpRefusal) {
		var missing mysql.GTIDSet
		if haveFenced && have != nil {
			missing = fencedSet.Subtract(have)
		}
		return fencedRaw, fencedSet, catchUpReached, &catchUpRefusal{reason: reason, missing: missing, waited: clk.Since(start)}
	}
	for {
		var oldErr error
		if !haveFenced {
			raw, set, err := readGTIDSetRaw(waitCtx, oldPrimary)
			switch {
			case err == nil:
				fencedRaw, fencedSet, haveFenced = raw, set, true
			case errors.Is(err, errUnparseableGTID):
				return refuse(fmt.Sprintf("the fenced old primary's GTID set is unparseable: %v", err))
			default:
				oldErr, lastErr = err, fmt.Errorf("read fenced old primary GTID set: %w", err)
			}
		} else {
			oldErr = withMySQLSafetyTimeout(waitCtx, func(probeCtx context.Context) error {
				_, err := oldPrimary.CheckReadOnly(probeCtx)
				return err
			})
		}

		if haveFenced {
			_, set, err := readGTIDSetRaw(waitCtx, candidate)
			switch {
			case err == nil:
				have, lastErr = set, nil
				if have.Contains(fencedSet) {
					f.logger.Info("emergency failover: candidate caught up with fenced old primary",
						"candidate", candidateSite, "oldPrimary", opts.OldPrimarySite,
						"oldPrimaryGtid", truncateGtidSet(fencedRaw), "waitedMs", clk.Since(start).Milliseconds())
					return fencedRaw, fencedSet, catchUpReached, nil
				}
			case errors.Is(err, errUnparseableGTID):
				return refuse(fmt.Sprintf("the candidate's GTID set is unparseable: %v", err))
			default:
				lastErr = fmt.Errorf("read candidate GTID set: %w", err)
			}
			if have != nil {
				var rs *mysql.ReplicaStatus
				if err := withMySQLSafetyTimeout(waitCtx, func(probeCtx context.Context) error {
					var err error
					rs, err = candidate.ShowReplicaStatus(probeCtx)
					return err
				}); err == nil && rs != nil {
					if rs.LastSQLError != "" {
						return refuse(fmt.Sprintf("the candidate's applier stopped: %d: %s", rs.LastSQLErrno, rs.LastSQLError))
					}
					if !rs.IORunning && !rs.IOConnecting {
						received := have
						if retrieved, err := mysql.ParseGTIDSet(rs.RetrievedGtidSet); err == nil {
							received = have.Union(retrieved)
						}
						if !received.Contains(fencedSet) {
							reason := "the candidate's receiver is stopped and has not received the set"
							if rs.LastIOError != "" {
								reason += fmt.Sprintf(" (%d: %s)", rs.LastIOErrno, rs.LastIOError)
							}
							return refuse(reason)
						}
					}
				}
			}
		}

		switch {
		case oldErr == nil:
			lostKnown = false
		case waitCtx.Err() != nil:
			// The wait's own deadline or cancellation, not evidence about
			// the old primary.
		case !lostKnown:
			lostSince, lostKnown = clk.Now(), true
		case clk.Since(lostSince) >= opts.OldPrimaryLossWindow:
			f.logger.Warn("emergency failover: old primary stopped answering during catch-up; promoting without it",
				"candidate", candidateSite, "oldPrimary", opts.OldPrimarySite, "error", oldErr)
			return fencedRaw, fencedSet, catchUpOldPrimaryLost, nil
		}

		if errors.Is(ctx.Err(), context.Canceled) {
			return refuse(fmt.Sprintf("cancelled: %v", ctx.Err()))
		}
		if clk.Since(start) >= opts.CatchUp || waitCtx.Err() != nil {
			break
		}
		if err := f.sleepCatchUp(waitCtx, opts.PollInterval); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return refuse(fmt.Sprintf("cancelled: %v", ctx.Err()))
			}
			break
		}
	}
	switch {
	case !haveFenced:
		return refuse(fmt.Sprintf("timed out after %s: %v", opts.CatchUp, lastErr))
	case have == nil:
		return refuse(fmt.Sprintf("timed out after %s: %v", opts.CatchUp, lastErr))
	}
	missing := fencedSet.Subtract(have)
	return refuse(fmt.Sprintf("timed out after %s with %d transactions missing (%s)",
		opts.CatchUp, missing.TransactionCount(), truncateGtidHint(missing.String())))
}

// refuseEmergencyFailover gives the old primary back the writes this
// failover's fence took, if it had them (releaseFence), and builds the
// refusal. A site that was read-only before the fence keeps it.
func (f *FailoverController) refuseEmergencyFailover(ctx context.Context, candidateSite string, oldPrimary mysql.Checker, wasWritable bool, held mysql.GTIDSet, r *catchUpRefusal, opts FailoverOptions) error {
	e := &CandidateNotCaughtUpError{
		Candidate:           candidateSite,
		OldPrimarySite:      opts.OldPrimarySite,
		MissingTransactions: r.missing.TransactionCount(),
		Waited:              r.waited,
		Reason:              r.reason,
	}
	if !r.missing.IsEmpty() {
		e.MissingGtid = truncateGtidSet(r.missing.String())
	}
	cause := errors.New(r.reason)
	if !wasWritable {
		f.logger.Warn("emergency failover aborted; old primary left fenced: it was read-only before the failover fenced it",
			"candidate", candidateSite, "oldPrimary", opts.OldPrimarySite, "error", cause)
		e.OldPrimary, e.Err = OldPrimaryLeftFenced, cause
		return e
	}
	if held == nil {
		held = mysql.GTIDSet{}
	}
	released := f.releaseFence(ctx, handoffRollbackTimeout, emergencyFenceRelease, candidateSite, oldPrimary, held, cause)
	e.Err = released
	if released == cause {
		e.OldPrimary = OldPrimaryUnfenced
	} else {
		e.OldPrimary = OldPrimaryNotReleased
	}
	return e
}
