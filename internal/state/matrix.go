package state

import "fmt"

// CrossSiteAction describes actions that require both sites' states.
type CrossSiteAction struct {
	PromoteSite string // which site to promote ("" = none)
	FlipDNS     string // which site gets DNS ("" = no change)
	Alert       string // alert message ("" = none)
}

// EvalCrossSite evaluates the combined state matrix and returns cross-site actions.
func EvalCrossSite(site0State, site1State, site0Prev, site1Prev SiteState, site0Name, site1Name string) CrossSiteAction {
	var a CrossSiteAction

	switch {
	case site0State == StateUnreachable && site1State == StateReadOnly:
		// site0 down, site1 healthy replica -> promote site1
		a.PromoteSite = site1Name
		a.FlipDNS = site1Name

	case site0State == StateReadOnly && site1State == StateUnreachable:
		// site1 down, site0 healthy replica -> promote site0
		a.PromoteSite = site0Name
		a.FlipDNS = site0Name

	case site0State == StateWritable && site1State == StateUnreachable:
		// site0 primary ok, site1 down
		a.Alert = fmt.Sprintf("%s unreachable while %s is primary", site1Name, site0Name)

	case site0State == StateUnreachable && site1State == StateWritable:
		// site1 primary ok, site0 down
		a.Alert = fmt.Sprintf("%s unreachable while %s is primary", site0Name, site1Name)

	case site0State == StateWritable && site1State == StateWritable:
		a.Alert = "SPLIT BRAIN: both sites are writable"

	case site0State == StateReadOnly && site1State == StateReadOnly:
		a.Alert = "NO PRIMARY: both sites are read-only"

	case site0State == StateUnreachable && site1State == StateUnreachable:
		a.Alert = "TOTAL LOSS: both sites are unreachable"
	}

	return a
}
