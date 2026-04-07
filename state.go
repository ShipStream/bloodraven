package main

import "fmt"

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

// CrossDCAction describes actions that require both DCs' states.
type CrossDCAction struct {
	PromoteDC string // which DC to promote ("" = none)
	FlipDNS   string // which DC gets DNS ("" = no change)
	Alert     string // alert message ("" = none)
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

// EvalCrossDC evaluates the combined state matrix and returns cross-DC actions.
func EvalCrossDC(dc1State, dc2State, dc1Prev, dc2Prev DCState, dc1Name, dc2Name string) CrossDCAction {
	var a CrossDCAction

	switch {
	case dc1State == StateUnreachable && dc2State == StateReadOnly:
		// dc1 down, dc2 healthy replica -> promote dc2
		a.PromoteDC = dc2Name
		a.FlipDNS = dc2Name

	case dc1State == StateReadOnly && dc2State == StateUnreachable:
		// dc2 down, dc1 healthy replica -> promote dc1
		a.PromoteDC = dc1Name
		a.FlipDNS = dc1Name

	case dc1State == StateWritable && dc2State == StateUnreachable:
		// dc1 primary ok, dc2 down
		a.Alert = fmt.Sprintf("%s unreachable while %s is primary", dc2Name, dc1Name)

	case dc1State == StateUnreachable && dc2State == StateWritable:
		// dc2 primary ok, dc1 down
		a.Alert = fmt.Sprintf("%s unreachable while %s is primary", dc1Name, dc2Name)

	case dc1State == StateWritable && dc2State == StateWritable:
		a.Alert = "SPLIT BRAIN: both DCs are writable"

	case dc1State == StateReadOnly && dc2State == StateReadOnly:
		a.Alert = "NO PRIMARY: both DCs are read-only"

	case dc1State == StateUnreachable && dc2State == StateUnreachable:
		a.Alert = "TOTAL LOSS: both DCs are unreachable"
	}

	return a
}
