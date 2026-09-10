package controller

import (
	"context"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsLeader closes the deployment API as soon as the elected runner is cancelled.
func (r *TopologyManagerRunner) IsLeader() bool {
	r.mu.RLock()
	ctx := r.leaderContext
	r.mu.RUnlock()
	return ctx != nil && ctx.Err() == nil
}

// SetDeploymentLeases wires the shared admission manager before starting Run.
func (r *TopologyManagerRunner) SetDeploymentLeases(m *DeploymentLeaseManager) {
	r.deploymentLeases = m
	if m != nil {
		m.requireTopology.Store(true)
	}
}

func (r *MysqlFailoverGroupReconciler) deploymentLeaseManager() *DeploymentLeaseManager {
	if r.Runner == nil {
		return nil
	}
	return r.Runner.deploymentLeases
}

func (tm *TopologyManager) deploymentTopologyPendingLocked() {
	if tm.deploymentLeases != nil {
		tm.deploymentEpoch = tm.deploymentLeases.TopologyPending(types.NamespacedName{Namespace: tm.cfg.Namespace, Name: tm.cfg.Name})
		tm.statusWriteFailed = true
	}
}

func (tm *TopologyManager) deploymentTopologyPending() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.deploymentTopologyPendingLocked()
}

func (tm *TopologyManager) beginDeploymentTopologyChange() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.deploymentUpdateRunning || tm.deploymentChanging > 0 {
		return false
	}
	tm.authorityEpoch++
	tm.deploymentChanging++
	tm.deploymentTopologyPendingLocked()
	return true
}

func (tm *TopologyManager) finishDeploymentTopologyChange() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.deploymentChanging--
	tm.deploymentAwaitingObservation = true
	tm.deploymentTopologyPendingLocked()
}

func (tm *TopologyManager) deploymentPromotionExecuted(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	// SET read_only=OFF succeeded even if the subsequent probe fails.
	tm.deploymentAuthorityLocked(site)
}

func (tm *TopologyManager) deploymentAuthorityLocked(site string) {
	if site != "" && site != tm.deploymentActiveSite {
		tm.topologyGeneration++
		tm.deploymentActiveSite = site
		tm.deploymentTopologyPendingLocked()
	}
}

func (r *TopologyManagerRunner) hydrateDeploymentGeneration(fg *v1alpha1.MysqlFailoverGroup) {
	r.mu.RLock()
	mt := r.managers[client.ObjectKeyFromObject(fg)]
	r.mu.RUnlock()
	if mt == nil {
		return
	}
	mt.tm.mu.Lock()
	defer mt.tm.mu.Unlock()
	if fg.Status.TopologyGeneration > mt.tm.topologyGeneration {
		mt.tm.topologyGeneration = fg.Status.TopologyGeneration
		mt.tm.deploymentActiveSite = fg.Status.ActiveSite
		mt.tm.deploymentTopologyPendingLocked()
	}
}

func (tm *TopologyManager) beginDeploymentUpdate(ctx context.Context) (func(), bool) {
	if tm.deploymentLeases == nil {
		return func() {}, true
	}
	fg := &v1alpha1.MysqlFailoverGroup{}
	if err := tm.deploymentLeases.reader.Get(ctx, types.NamespacedName{Namespace: tm.cfg.Namespace, Name: tm.cfg.Name}, fg); err != nil {
		return nil, false
	}
	// A durable phase blocks API admission after a crash; the reservation covers
	// the gap before the updater goroutine publishes that phase.
	hold, err := tm.deploymentLeases.BeginPlanned(ctx, fg, "OrderedUpdate")
	if err != nil || hold != nil {
		return nil, false
	}
	return func() { tm.deploymentLeases.EndPlanned(fg) }, true
}
