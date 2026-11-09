package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// RestoreReclonePending is 1 for each site a full-instance in-place
// restore still has to reclone (status.restoreReclones.pending). phase is
// the queue phase in kebab-case (armed, draining or held). Such a site's replication
// receiver is stopped by design, so alerts on bloodraven_replication_running
// exclude it with `unless on(namespace, group, site)
// bloodraven_restore_reclone_pending == 1`.
var RestoreReclonePending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bloodraven_restore_reclone_pending",
	Help: "1 for each site a full-instance in-place restore has not recloned yet (status.restoreReclones.pending); phase is the queue phase (armed, draining, held). The site's replication receiver is stopped until its reclone.",
}, []string{"namespace", "group", "site", "phase"})

var (
	restoreReclonePendingMu      sync.Mutex
	restoreReclonePendingEmitted = map[[2]string]map[string]string{}
)

// SetRestoreReclonePending makes the group's bloodraven_restore_reclone_pending
// series exactly one per site in sites, with the given phase. Series of sites
// that left the queue (or changed phase) are deleted; an empty sites list
// removes every series of the group. Current series are set before stale
// ones are deleted, so a scrape never sees a queued site without one.
func SetRestoreReclonePending(namespace, group, phase string, sites []string) {
	restoreReclonePendingMu.Lock()
	defer restoreReclonePendingMu.Unlock()
	key := [2]string{namespace, group}
	prev := restoreReclonePendingEmitted[key]
	next := make(map[string]string, len(sites))
	for _, site := range sites {
		next[site] = phase
		RestoreReclonePending.WithLabelValues(namespace, group, site, phase).Set(1)
	}
	for site, oldPhase := range prev {
		if p, ok := next[site]; ok && p == oldPhase {
			continue
		}
		RestoreReclonePending.DeleteLabelValues(namespace, group, site, oldPhase)
	}
	if len(next) == 0 {
		delete(restoreReclonePendingEmitted, key)
		return
	}
	restoreReclonePendingEmitted[key] = next
}
