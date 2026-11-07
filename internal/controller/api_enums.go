package controller

import (
	"fmt"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/state"
)

// The API enums and the internal vocabularies they meet. Each mapping is an
// exhaustive switch: a Go string conversion would compile and silently
// carry one vocabulary into the other, which the CRD enum (or the failover
// decision) then gets wrong.

// topologySiteRole maps spec.sites[].role to the role the topology manager
// and the cross-site matrix decide on. An unset role is the API default,
// PrimaryCandidate. Any other value fails closed: the topology is not
// configured from a role the operator cannot interpret.
func topologySiteRole(r v1alpha1.SiteRole) (state.SiteRole, error) {
	switch r {
	case v1alpha1.SiteRolePrimaryCandidate, "":
		return state.SiteRolePrimaryCandidate, nil
	case v1alpha1.SiteRoleDisasterRecovery:
		return state.SiteRoleDROnly, nil
	case v1alpha1.SiteRoleReadOnly:
		return state.SiteRoleReadOnly, nil
	}
	return "", fmt.Errorf("spec.sites[].role %q is not one of %s, %s, %s",
		r, v1alpha1.SiteRolePrimaryCandidate, v1alpha1.SiteRoleDisasterRecovery, v1alpha1.SiteRoleReadOnly)
}

// APISiteRole is the reverse of topologySiteRole, for the surfaces that
// report a topology-manager role: the /status and /ws/status endpoints, the
// metric role label and the logs. It returns "" only for a value
// topologySiteRole never produces.
func APISiteRole(r state.SiteRole) v1alpha1.SiteRole {
	switch r {
	case state.SiteRolePrimaryCandidate:
		return v1alpha1.SiteRolePrimaryCandidate
	case state.SiteRoleDROnly:
		return v1alpha1.SiteRoleDisasterRecovery
	case state.SiteRoleReadOnly:
		return v1alpha1.SiteRoleReadOnly
	}
	return ""
}

// apiSiteState maps the topology manager's site state to
// status.sites[].state, the /status and /ws/status state, the metric state
// labels and the logs.
func apiSiteState(s state.SiteState) v1alpha1.SiteState {
	switch s {
	case state.StateWritable:
		return v1alpha1.SiteStateWritable
	case state.StateReadOnly:
		return v1alpha1.SiteStateReadOnly
	case state.StateUnreachable:
		return v1alpha1.SiteStateUnreachable
	case state.StateUnknown:
		return v1alpha1.SiteStateUnknown
	}
	return v1alpha1.SiteStateUnknown
}

// apiDragonflyRole maps the Dragonfly classifier's role (also the
// dragonfly-role pod label value) to status.dragonfly.sites[].role.
func apiDragonflyRole(r dragonfly.SiteRole) v1alpha1.DragonflyRole {
	switch r {
	case dragonfly.RoleMaster:
		return v1alpha1.DragonflyRoleMaster
	case dragonfly.RoleReplica:
		return v1alpha1.DragonflyRoleReplica
	case dragonfly.RoleStaleMaster:
		return v1alpha1.DragonflyRoleStaleMaster
	case dragonfly.RoleUnconfigured:
		return v1alpha1.DragonflyRoleUnconfigured
	case dragonfly.RoleUnreachable:
		return v1alpha1.DragonflyRoleUnreachable
	case dragonfly.RoleUnknown:
		return v1alpha1.DragonflyRoleUnknown
	}
	return v1alpha1.DragonflyRoleUnknown
}

// pitrWireMode maps a point-in-time mode to the replay wire value the
// download planner, coverage.json and restore.py read
// (BLOODRAVEN_PITR_MODE, BLOODRAVEN_VERIFY_PITR_MODE).
func pitrWireMode(m v1alpha1.PointInTimeMode) (pitrplan.Mode, error) {
	switch m {
	case v1alpha1.PointInTimeModeLatest:
		return pitrplan.ModeLatest, nil
	case v1alpha1.PointInTimeModeTimestamp:
		return pitrplan.ModeTimestamp, nil
	}
	return "", fmt.Errorf("pointInTime.mode %q is not one of %s, %s", m, v1alpha1.PointInTimeModeLatest, v1alpha1.PointInTimeModeTimestamp)
}
