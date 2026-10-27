package state

// SiteState represents the observed state of a single site's MySQL instance.
type SiteState int

const (
	StateUnknown     SiteState = iota
	StateWritable              // read_only=0
	StateReadOnly              // read_only=1
	StateUnreachable           // connection failed
)

func (s SiteState) String() string {
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

// Action describes what the watcher should do for a given site on a state transition.
type Action struct {
	Taint *bool // nil=no change, true=apply, false=remove
}

// EvalPerSiteTransition returns the action for a single site's state transition.
func EvalPerSiteTransition(prev, curr SiteState) Action {
	if prev == curr {
		return Action{}
	}

	var a Action

	switch {
	case curr == StateWritable:
		// Becoming writable: remove taint.
		f := false
		a.Taint = &f
	case curr == StateReadOnly || curr == StateUnreachable:
		if prev == StateWritable || prev == StateUnknown {
			// Lost writability: apply taint.
			t := true
			a.Taint = &t
		}
		// read-only <-> unreachable: no new action (taint already applied)
	}

	return a
}
