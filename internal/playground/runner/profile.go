package runner

// Profile selects which scenarios run-all executes. The three profiles
// form a strict superset chain: smoke ⊂ release ⊂ full.
//
//   - smoke:  short PR-label/manual subset covering emergency failover,
//     planned switchover, and operator restart durability (~3 scenarios).
//   - release: curated release/nightly subset covering the WISHLIST #32/#43
//     behaviours (emergency failover, planned switchover, operator restart,
//     data integrity, operator kill during failover, self-fencing,
//     network partition, PVC loss/re-bootstrap, old-primary recovery,
//     failover state durability, backup verification, PITR verification).
//   - full:   every registered scenario (existing run-all behaviour).
type Profile string

const (
	ProfileSmoke   Profile = "smoke"
	ProfileRelease Profile = "release"
	ProfileFull    Profile = "full"
)

// DefaultProfile is used when --profile is not supplied.
const DefaultProfile Profile = ProfileFull

// smokeScenarios is the hard-coded smoke subset. These three scenarios
// exercise the critical path — emergency failover, planned switchover,
// operator restart — and complete in roughly 3-5 minutes on a warm
// playground cluster.
var smokeScenarios = map[string]bool{
	"01-clean-primary-kill":    true, // emergency failover
	"02-planned-switchover":    true, // planned switchover
	"02-operator-kill-restart": true, // operator restart durability
}

// releaseScenarios is the hard-coded release/nightly subset. In addition
// to the smoke scenarios, this covers the behaviours called out in
// WISHLIST #32: real MySQL pods/PVCs/Services, DNS/DNSEndpoint, taints,
// planned failover, emergency failover, operator restart, PVC loss and
// re-bootstrap, network partition / self-fencing, old-primary recovery,
// failover state durability across operator restarts, and dedicated RustFS
// backup/PITR verification coverage.
var releaseScenarios = map[string]bool{
	// smoke scenarios (superset)
	"01-clean-primary-kill":    true,
	"02-planned-switchover":    true,
	"02-operator-kill-restart": true,
	// additional release scenarios
	"04-data-integrity-on-failover":         true, // data plane correctness
	"05-operator-kill-during-failover":      true, // operator resilience mid-failover
	"06-self-fence-isolated-primary":        true, // taint/DNS self-fencing
	"09-network-partition-self-fence":       true, // NetworkPolicy/partition
	"10-full-bootstrap-after-data-wipe":     true, // PVC loss → re-bootstrap
	"12-old-primary-recovery-no-divergence": true, // old-primary recovery
	"23-failover-state-durability":          true, // state survives operator restart
	"30-backup-verification-rustfs":         true, // RustFS backup restore verification
	"31-pitr-verification-rustfs":           true, // RustFS PITR replay verification
}

// Profiles returns the list of valid profile names for CLI help and
// validation.
func Profiles() []Profile {
	return []Profile{ProfileSmoke, ProfileRelease, ProfileFull}
}

// IsValid reports whether p is a recognised profile name.
func (p Profile) IsValid() bool {
	switch p {
	case ProfileSmoke, ProfileRelease, ProfileFull:
		return true
	default:
		return false
	}

}

// SelectForProfile filters the given scenario list to the subset that
// belongs to the requested profile. For ProfileFull (the default) all
// scenarios are returned unfiltered. Unknown scenario IDs in the profile
// allowlist are silently ignored so that adding new scenarios does not
// break existing profiles.
func SelectForProfile(all []Scenario, p Profile) []Scenario {
	if p == ProfileFull || p == "" {
		return all
	}
	var allowlist map[string]bool
	switch p {
	case ProfileSmoke:
		allowlist = smokeScenarios
	case ProfileRelease:
		allowlist = releaseScenarios
	default:
		return all
	}
	var out []Scenario
	for _, s := range all {
		if allowlist[s.ID] {
			out = append(out, s)
		}
	}
	return out
}
