package state

// DCState represents the observed state of a single DC's MySQL instance.
type DCState int

const (
	StateUnknown     DCState = iota
	StateWritable            // read_only=0
	StateReadOnly            // read_only=1
	StateUnreachable         // connection failed
)

func (s DCState) String() string {
	switch s {
	case StateWritable:
		return "writable"
	case StateReadOnly:
		return "read-only"
	case StateUnreachable:
		return "unreachable"
	default:
		return "unknown"
	}
}

// Action describes what the watcher should do for a given DC on a state transition.
type Action struct {
	Taint     *bool  // nil=no change, true=apply, false=remove
	Broadcast string // "" = no broadcast, "online" or "offline"
}

// ShouldTaint returns true if the DC's MySQL is not writable (readonly or unreachable).
func ShouldTaint(state DCState) bool {
	return state != StateWritable
}

// EvalPerDCTransition returns the action for a single DC's state transition.
func EvalPerDCTransition(prev, curr DCState) Action {
	if prev == curr {
		return Action{}
	}

	var a Action

	switch {
	case curr == StateWritable:
		// Becoming writable: remove taint, broadcast online
		f := false
		a.Taint = &f
		a.Broadcast = "online"
	case curr == StateReadOnly || curr == StateUnreachable:
		if prev == StateWritable || prev == StateUnknown {
			// Lost writability: apply taint, broadcast offline
			t := true
			a.Taint = &t
			a.Broadcast = "offline"
		}
		// read-only <-> unreachable: no new action (taint already applied)
	}

	return a
}
