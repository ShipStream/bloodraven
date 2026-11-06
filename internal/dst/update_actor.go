package dst

import (
	"context"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// updateActor is a step model of ExecuteTargets' handoff (§15.1 "The update
// actor"). It uses the real claim API through the …ForSim hooks
// (BeginOrderedUpdateForSim, ClaimOrderedUpdateForSim, RecordHandoffForSim,
// FinishOrderedUpdateForSim) and issues the handoff's MySQL statements one
// per step, each attributed to ActorUpdate and each slow-capable: fence the
// active site, wait for the standby to catch up (abort after
// updateCatchUpSteps by unfencing), then STOP, RESET and clear read-only on
// the standby over one pinned session.
//
// It is honest about fidelity: it exercises the claim protocol, predecessor
// tracking and their interaction with a planned run, not ExecuteHandoff's
// internals, which the component tests cover with the production updater.
type updateActor struct {
	r   *trialRunner
	tm  *controller.TopologyManager
	upd *controller.SimOrderedUpdate

	standby string
	step    updateStep
	waited  int
	fenced  string // the fenced active set, once read
	sess    mysql.Session
	done    bool
}

type updateStep int

const (
	updClaim updateStep = iota
	updFence
	updReadFenced
	updCatchUp
	updStop
	updReset
	updClearRO
	updRecord // the record is made in the read_only=OFF step
)

// updateCatchUpSteps bounds the fenced catch-up wait, in steps.
const updateCatchUpSteps = 5

// startUpdateActor starts an ordered update on tm, refused exactly where
// startUpdate would refuse. nil when refused or when there is no standby.
func startUpdateActor(r *trialRunner, tm *controller.TopologyManager) *updateActor {
	upd, err := tm.BeginOrderedUpdateForSim()
	if err != nil {
		return nil
	}
	a := &updateActor{r: r, tm: tm, upd: upd}
	for i, name := range r.trial.SiteNames {
		if name != upd.Active() && r.trial.Roles[i] == state.SiteRolePrimaryCandidate {
			a.standby = name
			break
		}
	}
	if a.standby == "" {
		a.finish()
		return nil
	}
	return a
}

// active reports whether the update has not finished.
func (a *updateActor) active() bool { return !a.done }

// claimed reports whether the update holds its mutation window.
func (a *updateActor) claimed() bool { return !a.done && a.step > updClaim }

func (a *updateActor) finish() {
	if a.sess != nil {
		_ = a.sess.Close()
		a.sess = nil
	}
	a.upd.FinishOrderedUpdateForSim()
	a.done = true
}

// abort gives the active site its writes back (before the standby's
// read_only was touched): super_read_only and read_only both cleared, as
// abortHandoff does, then finishes.
func (a *updateActor) abort(ctx context.Context) {
	if a.step > updFence && a.step <= updReset {
		c := a.r.cluster.NewChecker(a.upd.Active())
		_ = c.SetSuperReadOnly(ctx, false)
		_ = c.SetReadOnly(ctx, false)
	}
	a.finish()
}

// stepOnce runs the next statement. ctx must carry ActorUpdate.
func (a *updateActor) stepOnce(ctx context.Context) {
	if a.done || a.r.cluster.OperatorDead() {
		return
	}
	activeC := a.r.cluster.NewChecker(a.upd.Active())
	standbyC := a.r.cluster.NewChecker(a.standby)
	switch a.step {
	case updClaim:
		ok, err := a.upd.ClaimOrderedUpdateForSim(ctx)
		if err != nil || !ok {
			a.finish()
			return
		}
		a.step = updFence
	case updFence:
		if err := activeC.SetSuperReadOnly(ctx, true); err != nil {
			a.step = updReadFenced // the fence may have applied: abort unfences
			a.abort(ctx)
			return
		}
		a.step = updReadFenced
	case updReadFenced:
		g, err := activeC.GetGtidExecuted(ctx)
		if err != nil {
			a.abort(ctx)
			return
		}
		a.fenced = g
		a.step = updCatchUp
	case updCatchUp:
		have, err := standbyC.GetGtidExecuted(ctx)
		want, perr := mysql.ParseGTIDSet(a.fenced)
		got, gerr := mysql.ParseGTIDSet(have)
		if err == nil && perr == nil && gerr == nil && want.Subtract(got).IsEmpty() {
			sess, err := standbyC.PinSession(ctx)
			if err != nil {
				a.abort(ctx)
				return
			}
			a.sess = sess
			a.step = updStop
			return
		}
		a.waited++
		if a.waited >= updateCatchUpSteps {
			a.abort(ctx)
		}
	case updStop:
		if err := a.sess.StopReplica(ctx); err != nil {
			a.abort(ctx)
			return
		}
		a.step = updReset
	case updReset:
		if err := a.sess.ResetReplicaAll(ctx); err != nil {
			a.abort(ctx)
			return
		}
		a.step = updClearRO
	case updClearRO:
		if err := a.sess.SetReadOnly(ctx, false); err != nil {
			// read_only=OFF may have been sent: the source stays fenced.
			// As ExecuteTargets does (N2), a standby the pooled probe
			// finds writable is recorded as the promotion it is, with the
			// set proven before the promotion; without that evidence
			// nothing is recorded.
			if ro, perr := standbyC.CheckReadOnly(ctx); perr == nil && !ro {
				a.record(ctx, a.fenced)
			}
			a.finish()
			return
		}
		gtid, _ := a.sess.GetGtidExecuted(ctx)
		a.record(ctx, gtid)
		a.finish()
	}
}

// record records the handoff as ExecuteTargets' callback does, right after
// the promotion returns, in the same goroutine: no poll can run between.
func (a *updateActor) record(ctx context.Context, gtid string) {
	a.step = updRecord
	a.upd.RecordHandoffForSim(ctx, a.standby, gtid)
	a.r.armPromotion(a.standby, a.r.clk.Now())
	if a.tm != a.r.tm {
		// Recorded on a retired manager: its status writes are dropped,
		// and the record reaches its replacement only by the carry
		// (DST-1, fixed); the tag marks a regression of that carry.
		a.r.replacedBeforeRecord = true
		if a.r.retiredRecordPolls == nil {
			a.r.retiredRecordPolls = map[int]bool{}
		}
		a.r.retiredRecordPolls[a.r.cluster.Poll()] = true
	}
}
