package controller

import (
	"context"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// leaseAdmissionGuard wraps each planned-failover admission pass with the
// deployment lease manager's admission reservation, and while the pass
// runs no lease can be granted past it; the engine persists its phase
// before the release. A standing failover-hold lease defers a new or
// Deferred request (Deferred{DeploymentHold}), but ends an Admitting run
// NotStarted{ConcurrentOperation}: an admitted run is not put back to
// waiting without a deadline. A refused reservation is an admission wait
// (see plannedAdmissionWaitReason).
type leaseAdmissionGuard struct {
	r *MysqlFailoverGroupReconciler
}

var _ plannedAdmissionGuard = leaseAdmissionGuard{}

func (g leaseAdmissionGuard) Now() time.Time {
	if m := g.r.deploymentLeaseManager(); m != nil {
		return m.now()
	}
	return time.Now()
}

func (g leaseAdmissionGuard) BeginPlanned(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (*DeploymentLeaseView, func(), error) {
	m := g.r.deploymentLeaseManager()
	if m == nil {
		return nil, func() {}, nil
	}
	hold, err := m.BeginPlanned(ctx, fg, deploymentOperationPlannedFailover)
	if err != nil || hold != nil {
		return hold, func() {}, err
	}
	return nil, func() { m.EndPlanned(fg) }, nil
}
