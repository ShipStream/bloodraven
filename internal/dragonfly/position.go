package dragonfly

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Position is where an instance is in the replication journal.
//
// Dragonfly replicates one flow per master shard. A master numbers the
// journal records of each shard (the LSN); a replica counts the records
// each flow has executed, on the same scale, which is what it acknowledges
// to the master. A replica has applied everything the master had written at
// some moment when every flow's count has reached the LSN the master's
// shard had at that moment.
//
// Neither side prints these numbers in INFO in a comparable form: a master
// prints no offset at all (v1.38, v2.1), and a replica prints only the sum
// over its flows (slave_repl_offset). They are read with `DFLY
// REPLICAOFFSET` on the master (dflycmd.cc, DflyCmd::ReplicaOffset: the
// LSN of each shard) and `DEBUG REPLICA OFFSET` on the replica
// (debugcmd.cc, DebugCmd::Replica: the executed count of each flow), the
// pair Dragonfly's own replication tests compare.
type Position struct {
	// Role is "master" or "slave", as INFO replication reported it.
	Role string

	// Flows is the journal position per master shard: the LSN on a master,
	// the executed record count on a replica. With Scalar it has one
	// element.
	Flows []int64

	// Scalar is true when the server does not have the per-flow commands
	// and reported one offset in INFO instead (master_repl_offset on a
	// master, slave_repl_offset on a replica), as a Redis-style server
	// does. Flows then holds that one value. A scalar and a per-flow
	// position are never compared.
	Scalar bool

	// ReplID is master_replid: the instance's own replication id on a
	// master, the id of the master it syncs from on a replica.
	ReplID string
}

// Total is the sum over the flows, the figure a replica prints as
// slave_repl_offset.
func (p Position) Total() int64 {
	var sum int64
	for _, f := range p.Flows {
		sum += f
	}
	return sum
}

// ReachedBy reports whether target, a replica's position, has reached p, a
// master's position read earlier: target syncs from that master and every
// flow has executed at least the records the master's shard had written.
// detail says why not.
//
// Anything that cannot be compared is "not reached": a position of the
// wrong role, an empty one, a scalar against a per-flow one, a different
// number of flows, a replica that reports another master's replication id
// or none. An unknown never reads as "in sync".
func (p Position) ReachedBy(target Position) (reached bool, detail string) {
	switch {
	case p.Role != "master":
		return false, fmt.Sprintf("source reports role %q, not master", p.Role)
	case target.Role != "slave":
		return false, fmt.Sprintf("target reports role %q, not a replica", target.Role)
	case len(p.Flows) == 0:
		return false, "source reported no replication position"
	case len(target.Flows) == 0:
		return false, "target reported no replication position"
	case p.Scalar != target.Scalar:
		return false, "source and target report their positions in different forms"
	case len(p.Flows) != len(target.Flows):
		return false, fmt.Sprintf("target has %d replication flows, source has %d shards", len(target.Flows), len(p.Flows))
	case p.ReplID != target.ReplID:
		return false, fmt.Sprintf("target syncs from replication id %q, source is %q", target.ReplID, p.ReplID)
	}
	for i, want := range p.Flows {
		if target.Flows[i] < want {
			if p.Scalar {
				return false, fmt.Sprintf("target offset %d behind source %d", target.Flows[i], want)
			}
			return false, fmt.Sprintf("target flow %d at %d behind source %d", i, target.Flows[i], want)
		}
	}
	return true, ""
}

// ReplicationPosition reads the instance's Position: INFO replication for
// the role and replication id, then the per-flow command of that role.
//
// When the server refuses the per-flow command (a server error, as from a
// server that does not have it) and INFO carried the role's offset, that
// offset is returned as a scalar position. Otherwise the refusal is the
// error: the caller has no position, which it must not read as zero. A
// transport error is always returned, and the connection must not be used
// afterwards.
func (c *Client) ReplicationPosition(ctx context.Context) (Position, error) {
	info, err := c.InfoReplication(ctx)
	if err != nil {
		return Position{}, err
	}
	pos := Position{Role: info.Role, ReplID: info.MasterReplID}
	switch info.Role {
	case "master":
		val, err := c.execValue(ctx, "DFLY", "REPLICAOFFSET")
		if err == nil {
			pos.Flows, err = parseFlowOffsets(val)
			if err != nil {
				return Position{}, fmt.Errorf("dragonfly: DFLY REPLICAOFFSET: %w", err)
			}
			return pos, nil
		}
		var se *ServerError
		if !errors.As(err, &se) {
			return Position{}, err
		}
		if !info.MasterReplOffsetReported {
			return Position{}, fmt.Errorf("dragonfly: master position unavailable: DFLY REPLICAOFFSET: %w, and INFO replication has no master_repl_offset", err)
		}
		pos.Flows, pos.Scalar = []int64{info.MasterReplOffset}, true
		return pos, nil
	case "slave":
		val, err := c.execValue(ctx, "DEBUG", "REPLICA", "OFFSET")
		if err == nil {
			pos.Flows, err = parseReplicaFlowOffsets(val)
			if err != nil {
				return Position{}, fmt.Errorf("dragonfly: DEBUG REPLICA OFFSET: %w", err)
			}
			return pos, nil
		}
		var se *ServerError
		if !errors.As(err, &se) {
			return Position{}, err
		}
		if !info.SlaveReplOffsetReported {
			return Position{}, fmt.Errorf("dragonfly: replica position unavailable: DEBUG REPLICA OFFSET: %w, and INFO replication has no slave_repl_offset", err)
		}
		pos.Flows, pos.Scalar = []int64{info.SlaveReplOffset}, true
		return pos, nil
	default:
		return Position{}, fmt.Errorf("dragonfly: no replication position for role %q", info.Role)
	}
}

// parseFlowOffsets reads the reply of `DFLY REPLICAOFFSET`: an array of one
// integer per shard.
func parseFlowOffsets(v respValue) ([]int64, error) {
	if v.null || v.typ != '*' {
		return nil, fmt.Errorf("unexpected reply type %q", string(v.typ))
	}
	if len(v.array) == 0 {
		return nil, errors.New("empty reply")
	}
	out := make([]int64, len(v.array))
	for i, elt := range v.array {
		if elt.null || elt.typ != ':' || elt.integer < 0 {
			return nil, fmt.Errorf("element %d is not a non-negative integer", i)
		}
		out[i] = elt.integer
	}
	return out, nil
}

// parseReplicaFlowOffsets reads the reply of `DEBUG REPLICA OFFSET`: the
// sync id (SYNC<n>) and an array of one integer per flow.
func parseReplicaFlowOffsets(v respValue) ([]int64, error) {
	if v.null || v.typ != '*' || len(v.array) != 2 {
		return nil, errors.New("unexpected reply shape")
	}
	if id := v.array[0]; id.null || (id.typ != '$' && id.typ != '+') || !strings.HasPrefix(id.str, "SYNC") {
		return nil, errors.New("no sync id in the reply")
	}
	return parseFlowOffsets(v.array[1])
}
