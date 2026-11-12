package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// dragonflySiteDialer opens a connection to a site's Dragonfly. The three
// callers that prove a replica's sync before a takeover (the planned
// failover, the rollout, the manager's take-back) each dial their own way.
type dragonflySiteDialer func(ctx context.Context, site string) (DragonflyConnection, error)

// dragonflySourcePositionVia reads the replication position of source, the
// Dragonfly master a promotion is about to take over from. The site must
// answer as a master: a position read from anything else proves nothing
// about what a replica of the master has to reach.
func dragonflySourcePositionVia(ctx context.Context, dial dragonflySiteDialer, source string) (dragonfly.Position, error) {
	conn, err := dial(ctx, source)
	if err != nil {
		return dragonfly.Position{}, fmt.Errorf("dial source: %w", err)
	}
	defer func() { _ = conn.Close() }()
	pos, err := conn.ReplicationPosition(ctx)
	if err != nil {
		return dragonfly.Position{}, fmt.Errorf("source replication position: %w", err)
	}
	if pos.Role != "master" {
		return dragonfly.Position{}, fmt.Errorf("source reports role %q, not master", pos.Role)
	}
	return pos, nil
}

// dragonflyTargetSyncVia looks at target once; see
// MysqlFailoverGroupReconciler.dragonflyTargetReached.
func dragonflyTargetSyncVia(ctx context.Context, dial dragonflySiteDialer, target string, source dragonfly.Position) (dragonflyTargetLook, error) {
	conn, err := dial(ctx, target)
	if err != nil {
		return dragonflyTargetLook{}, fmt.Errorf("target unreachable: %w", err)
	}
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		// The RESP buffer is in an unknown state; do not issue another
		// command on this connection.
		_ = conn.Close()
		return dragonflyTargetLook{}, fmt.Errorf("INFO replication failed: %w", err)
	}
	persist, _ := conn.InfoPersistence(ctx)
	if !dragonfly.CandidateSyncReady(info, persist) {
		_ = conn.Close()
		return dragonflyTargetLook{Detail: fmt.Sprintf("target not in stable sync (role=%s link=%s fullSync=%v loading=%v)",
			info.Role, info.MasterLinkStatus, info.MasterSyncInProgress, persist.Loading)}, nil
	}
	pos, err := conn.ReplicationPosition(ctx)
	_ = conn.Close()
	if err != nil {
		return dragonflyTargetLook{Linked: true}, fmt.Errorf("target replication position: %w", err)
	}
	reached, detail := source.ReachedBy(pos)
	look := dragonflyTargetLook{Reached: reached, Linked: true, Detail: detail}
	switch {
	case reached:
	case pos.Role == "slave" && len(pos.Flows) > 0 && len(source.Flows) > 0 && pos.Scalar != source.Scalar:
		return look, fmt.Errorf("%w: %s", errDragonflyPositionsIncomparable, detail)
	case pos.Role == "slave" && pos.ReplID != "" && source.ReplID != "" && pos.ReplID != source.ReplID:
		return look, fmt.Errorf("%w: %s", errDragonflyTargetFollowsOther, detail)
	}
	return look, nil
}

// dragonflyLinkProof is what is left to check when the sync of target with
// source can never be proven by position (dragonflySyncUnprovable: a site
// refuses the position command, or the two report positions in different
// forms). It reports whether the two sites' own INFO answers say that
// target is in stable sync with the instance that is the master at source
// now: source answers as a master with a replication id, and target is a
// replica that reports its link up, has received data, is in no full sync
// and no load (dragonfly.CandidateSyncReady), and follows that replication
// id. detail explains a "no"; err is a site that could not be read.
//
// This does not say how far behind target is: a replica cut off from its
// master without a reset keeps reporting a healthy link. A promotion that
// rests on it must therefore be a REPLTAKEOVER with no REPLICAOF NO ONE
// behind it: the takeover makes the master stop taking commands and wait
// for the replica to acknowledge every shard's journal, and it fails,
// changing nothing, when that does not happen within its budget.
func dragonflyLinkProof(ctx context.Context, dial dragonflySiteDialer, target, source string) (ok bool, detail string, err error) {
	src, err := dial(ctx, source)
	if err != nil {
		return false, "", fmt.Errorf("dial source: %w", err)
	}
	srcInfo, err := src.InfoReplication(ctx)
	_ = src.Close()
	if err != nil {
		return false, "", fmt.Errorf("source INFO replication failed: %w", err)
	}
	if srcInfo.Role != "master" || srcInfo.MasterReplID == "" {
		return false, fmt.Sprintf("source reports role %q and replication id %q, not a master with one", srcInfo.Role, srcInfo.MasterReplID), nil
	}
	tgt, err := dial(ctx, target)
	if err != nil {
		return false, "", fmt.Errorf("target unreachable: %w", err)
	}
	defer func() { _ = tgt.Close() }()
	info, err := tgt.InfoReplication(ctx)
	if err != nil {
		return false, "", fmt.Errorf("INFO replication failed: %w", err)
	}
	persist, err := tgt.InfoPersistence(ctx)
	if err != nil {
		return false, "", fmt.Errorf("INFO persistence failed: %w", err)
	}
	if info.Role != "slave" || !dragonfly.CandidateSyncReady(info, persist) {
		return false, fmt.Sprintf("target not in stable sync (role=%s link=%s fullSync=%v loading=%v)",
			info.Role, info.MasterLinkStatus, info.MasterSyncInProgress, persist.Loading), nil
	}
	if info.MasterReplID != srcInfo.MasterReplID {
		return false, fmt.Sprintf("target syncs from replication id %q, source is %q", info.MasterReplID, srcInfo.MasterReplID), nil
	}
	return true, "", nil
}

// dragonflySyncCheck is the result of one check of a replica that is about
// to take over from a live master, outside a planned failover: before a
// rollout promotes it, and before the manager takes the master back to the
// MySQL active site.
type dragonflySyncCheck struct {
	// Ready: the target may take over.
	Ready bool
	// TakeoverOnly: Ready rests on the link checks alone
	// (dragonflyLinkProof), because the sync can never be proven by
	// position. The promotion must then be a REPLTAKEOVER with no REPLICAOF
	// NO ONE fallback. Detail says why the position was not available.
	TakeoverOnly bool
	// State names what kept it from being ready, one of the
	// dragonflyRollout* states; "" when Ready.
	State string
	// Detail is the look's own explanation, with its numbers.
	Detail string
	// Err is the error that made a site unreadable.
	Err error
}

// The states of a sync check that is not Ready. They are the `state` field
// of the rollout's and the take-back's log lines.
const (
	// The target is not in stable sync: link down, full sync, loading.
	// Looking again within seconds does not change it.
	dragonflyRolloutNotLinked = "NotLinked"
	// The target is linked and behind the source's position after the
	// check's wait.
	dragonflyRolloutBehind = "Behind"
	// A site could not be read (unreachable, a transport error).
	dragonflyRolloutUnreadable = "Unreadable"
)

// dragonflyProveSync reports whether target can take over from source: it
// reads the source's replication position, then looks at the target
// (dragonflyTargetSyncVia).
//
// It waits only where waiting can help: a target that is linked and behind
// is looked at again every poll for up to wait, or until ctx ends.
// Everything else returns at once: a target that is not in stable sync
// (link down, full sync, loading) and a site that cannot be read.
//
// When the sync can never be proven by position (dragonflySyncUnprovable),
// the check falls back to the link checks (dragonflyLinkProof): if they
// hold, the target is Ready with TakeoverOnly set; if they do not, it is
// NotLinked or Unreadable like any other target, and the caller waits.
func dragonflyProveSync(ctx context.Context, dial dragonflySiteDialer, target, source string, wait, poll time.Duration) dragonflySyncCheck {
	failed := func(err error) dragonflySyncCheck {
		if !dragonflySyncUnprovable(err) {
			return dragonflySyncCheck{State: dragonflyRolloutUnreadable, Detail: err.Error(), Err: err}
		}
		ok, detail, lerr := dragonflyLinkProof(ctx, dial, target, source)
		switch {
		case lerr != nil:
			return dragonflySyncCheck{State: dragonflyRolloutUnreadable, Detail: lerr.Error(), Err: lerr}
		case !ok:
			return dragonflySyncCheck{State: dragonflyRolloutNotLinked, Detail: detail}
		}
		return dragonflySyncCheck{Ready: true, TakeoverOnly: true, Detail: err.Error()}
	}
	pos, err := dragonflySourcePositionVia(ctx, dial, source)
	if err != nil {
		return failed(err)
	}
	deadline := time.Now().Add(wait)
	for {
		look, err := dragonflyTargetSyncVia(ctx, dial, target, pos)
		switch {
		case errors.Is(err, errDragonflyTargetFollowsOther):
			// The source was replaced between the two reads; the next
			// pass reads its position again.
			return dragonflySyncCheck{State: dragonflyRolloutBehind, Detail: look.Detail}
		case err != nil:
			return failed(err)
		case look.Reached:
			return dragonflySyncCheck{Ready: true}
		case !look.Linked:
			return dragonflySyncCheck{State: dragonflyRolloutNotLinked, Detail: look.Detail}
		}
		if !time.Now().Add(poll).Before(deadline) {
			return dragonflySyncCheck{State: dragonflyRolloutBehind, Detail: look.Detail}
		}
		select {
		case <-ctx.Done():
			return dragonflySyncCheck{State: dragonflyRolloutUnreadable, Detail: ctx.Err().Error(), Err: ctx.Err()}
		case <-time.After(poll):
		}
	}
}

// dragonflySyncUnprovenMessage is the text of the DragonflySyncUnproven
// Event and of the matching log lines.
func dragonflySyncUnprovenMessage(target, source, why string) string {
	return fmt.Sprintf("the sync of Dragonfly replica %q with master %q could not be proven by position (%s); "+
		"relying on REPLTAKEOVER, which waits for the replica itself and changes nothing when it fails", target, source, why)
}
