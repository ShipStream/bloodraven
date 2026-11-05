package controller

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// ReasonTopologyManagerStartFailed is the Warning Event reason for a
	// group left without a running topology manager because its start
	// keeps failing.
	ReasonTopologyManagerStartFailed = "TopologyManagerStartFailed"
	// ReasonTopologyManagerStarted is the Normal Event reason for a start
	// that succeeds after a TopologyManagerStartFailed Warning.
	ReasonTopologyManagerStarted = "TopologyManagerStarted"

	// startFailureWarningFloor is the minimum time between two
	// TopologyManagerStartFailed Warnings for a group, whatever the error.
	// client-go throttles Events per involved object and type, not per
	// reason (burst 25, one token per 5 minutes), so a Warning every resync
	// would starve the group's other Warnings, such as SplitBrainDetected.
	// Matching the refill rate keeps this reason from draining the bucket.
	startFailureWarningFloor = 5 * time.Minute
	// startFailureWarningHeartbeat re-emits an unchanged failure so it
	// stays visible within the default one-hour Event TTL.
	startFailureWarningHeartbeat = 10 * time.Minute
)

// startFailureState tracks one episode of consecutive topology manager
// start failures for a group.
type startFailureState struct {
	uid   types.UID
	count int
	// hadManager is set when the episode began by tearing down a
	// registered manager: a working group went dark, so the first failure
	// is already worth a Warning.
	hadManager bool
	warned     bool
	emittedErr string
}

// startWarning records when a group object was last sent a
// TopologyManagerStartFailed Warning. The floor applies per object: client-go
// throttles Events per involved object UID, so a group deleted and recreated
// under the same name starts with a fresh floor.
type startWarning struct {
	uid types.UID
	at  time.Time
}

func (r *TopologyManagerRunner) clockNow() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// noteStartFailure records a failed topology manager start and emits a
// rate-limited TopologyManagerStartFailed Warning. A group that never had a
// manager in this process is warned about only once the failure outlives a
// resync, which absorbs a CR applied just before its Secret. hadManager
// reports that this sync tore down a registered manager.
func (r *TopologyManagerRunner) noteStartFailure(fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, err error, hadManager bool) {
	msg := err.Error()
	now := r.clockNow()

	r.mu.Lock()
	if r.startFailures == nil {
		r.startFailures = make(map[types.NamespacedName]*startFailureState)
	}
	if r.lastStartWarning == nil {
		r.lastStartWarning = make(map[types.NamespacedName]startWarning)
	}
	st := r.startFailures[nn]
	if st == nil || st.uid != fg.UID {
		st = &startFailureState{uid: fg.UID}
		r.startFailures[nn] = st
	}
	st.count++
	st.hadManager = st.hadManager || hadManager

	emit := false
	if st.hadManager || st.count >= 2 {
		last, warnedBefore := r.lastStartWarning[nn]
		warnedBefore = warnedBefore && last.uid == fg.UID
		since := now.Sub(last.at)
		if !warnedBefore || since >= startFailureWarningFloor {
			emit = !st.warned || msg != st.emittedErr || since >= startFailureWarningHeartbeat
		}
	}
	if emit {
		st.warned = true
		st.emittedErr = msg
		r.lastStartWarning[nn] = startWarning{uid: fg.UID, at: now}
	}
	r.mu.Unlock()

	if emit && r.recorder != nil {
		r.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonTopologyManagerStartFailed,
			"no topology manager is running for this group (no polling, status updates or failover); start failed: %s; retrying every 30s", msg)
	}
}

// noteStartSuccess ends a start-failure episode, emitting a Normal
// TopologyManagerStarted Event when the episode was warned about.
func (r *TopologyManagerRunner) noteStartSuccess(fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) {
	r.mu.Lock()
	st := r.startFailures[nn]
	delete(r.startFailures, nn)
	r.mu.Unlock()

	if st == nil || !st.warned || st.uid != fg.UID || r.recorder == nil {
		return
	}
	r.recorder.Eventf(fg, corev1.EventTypeNormal, ReasonTopologyManagerStarted,
		"topology manager started after %d failed attempts", st.count)
}

// pruneStartFailures drops start-failure bookkeeping for groups that no
// longer exist. A group that never started has no managers entry, so this
// cannot ride on the deleted-manager loop.
func (r *TopologyManagerRunner) pruneStartFailures(seen map[types.NamespacedName]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for nn := range r.startFailures {
		if _, ok := seen[nn]; !ok {
			delete(r.startFailures, nn)
		}
	}
	for nn := range r.lastStartWarning {
		if _, ok := seen[nn]; !ok {
			delete(r.lastStartWarning, nn)
		}
	}
}
