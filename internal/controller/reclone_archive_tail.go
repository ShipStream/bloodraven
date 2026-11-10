package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

const (
	// preRecloneArchiveTailTimeout bounds how long a reclone waits for its
	// recipient's sidecar to archive the binary log tail CLONE is about to
	// wipe. After it the clone starts regardless, with a Warning Event.
	//
	// Two minutes covers what the wait is for: the sidecar scans on the
	// rotation itself (inotify on the binlog index), hashes and indexes the
	// sealed file and uploads it, and a full-size tail (max_binlog_size is
	// 1 GiB by default) uploads in that time at 10 MB/s or more. A reclone
	// is not urgent: its recipient is a replica that is being rebuilt, and
	// the clone itself takes minutes to hours. A longer bound would only
	// delay a reclone whose sidecar cannot archive at all (an unreachable
	// store, a refused archive), which is when the bound is reached.
	preRecloneArchiveTailTimeout = 2 * time.Minute
	// archiveTailProbeTimeout bounds one /archiver/status request of the
	// wait, like the archiver poller's.
	archiveTailProbeTimeout = 3 * time.Second
)

// ArchiveTailProbe reads one site's /archiver/status for the pre-reclone
// wait. enabled is false when PITR archiving is not enabled for the group:
// the reclone then skips the rotation, the wait and the Event.
type ArchiveTailProbe func(ctx context.Context, site string) (status *mysql.ArchiverStatus, enabled bool, err error)

// recloneTailWait is one pre-reclone wait: from the first poll that found
// the reclone ready to start until the recipient's tail is archived or the
// bound has passed. It lives in memory only. An operator restart starts
// over: the reclone annotation is still there, so the new process rotates
// again (one more empty file) and waits again, from zero.
type recloneTailWait struct {
	site      string
	startedAt time.Time
	// baseline is the active binary log the sidecar reported before the
	// operator's rotation; a later scan that reports another one has seen
	// the rotation.
	baseline     string
	haveBaseline bool
	// rotated: the operator's FLUSH BINARY LOGS was acknowledged.
	rotated bool
	// last is why the tail is not confirmed archived yet, for the Event.
	last string
}

// recloneArchiveTailSettled is the step a reclone takes before CLONE wipes
// its recipient's data directory (#264): the recipient is often an old
// primary, and the last binary log it wrote as the primary may never have
// been archived. It reports true when the clone may start.
//
// Driven by checkReclone once per poll, never blocking beyond one status
// request and one bounded rotation:
//
//  1. Read the recipient sidecar's archive tail (the baseline: which file
//     the server is writing).
//  2. Rotate the recipient's binary log, best effort (rotateBinlog), so the
//     tail is a sealed file; retried every poll until acknowledged.
//  3. Wait until the sidecar reports a scan that saw the rotation (another
//     active file) and ended Drained: every sealed file the site has to
//     archive is in its manifest.
//
// After preRecloneArchiveTailTimeout the clone starts anyway, and the gap
// is announced: a WARN line and, through ArchiveTailNotArchivedCallback, a
// Warning Event naming the site. Restores to a target inside an unarchived
// tail refuse PITRArchiveGap; the Event is what tells an operator why.
//
// The wait holds nothing: the reclone request stays pending, no claim is
// taken, and every other check of the poll (failover, recovery) runs as
// usual. checkReclone validates the request again on every poll, so a
// recipient that became the primary meanwhile is never cloned.
func (tm *TopologyManager) recloneArchiveTailSettled(ctx context.Context, recipient *siteTracker) bool {
	probe := tm.ArchiveTailProbe
	if probe == nil {
		tm.recloneTail = nil
		return true
	}
	if tm.recloneTailSettled == recipient.name {
		// Settled on an earlier poll; the clone was deferred after it (a
		// keyring unseal, another topology change).
		return true
	}
	now := tm.clock.Now()
	w := tm.recloneTail
	if w == nil || w.site != recipient.name {
		w = &recloneTailWait{site: recipient.name, startedAt: now, last: "the site's sidecar has not reported its archive tail"}
	}
	probeCtx, cancel := context.WithTimeout(ctx, archiveTailProbeTimeout)
	status, enabled, err := probe(probeCtx, w.site)
	cancel()
	if !enabled {
		tm.recloneTail = nil
		return true
	}
	if tm.recloneTail != w {
		tm.recloneTail = w
		tm.logger.Info("pre-reclone archive tail wait started", "site", w.site, "timeout", preRecloneArchiveTailTimeout.String())
	}
	settle := func(reason string) bool {
		waited := now.Sub(w.startedAt).Milliseconds()
		tm.recloneTail = nil
		tm.recloneTailSettled = w.site
		if reason == "" {
			tm.logger.Info("pre-reclone archive tail archived", "site", w.site, "waitedMs", waited)
			return true
		}
		tm.logger.Warn("pre-reclone archive tail not archived", "site", w.site, "waitedMs", waited, "reason", reason)
		if tm.ArchiveTailNotArchivedCallback != nil {
			tm.ArchiveTailNotArchivedCallback(w.site, reason)
		}
		return true
	}
	switch {
	case err != nil:
		w.last = fmt.Sprintf("the site's sidecar did not answer: %v", err)
	case status == nil || !status.Enabled:
		// PITR is enabled for the group but this sidecar runs no archiver
		// (its pod has not rolled to the PITR configuration): nothing can
		// archive the tail, and waiting would not change that.
		return settle("the site's sidecar runs no binlog archiver")
	case status.Tail == nil:
		w.last = "the site's sidecar reports no archive tail (it has not scanned the binary log index yet, or it predates the field and archives nothing while read-only)"
	case !w.haveBaseline:
		w.baseline, w.haveBaseline = status.Tail.ActiveFile, true
	case w.rotated && status.Tail.ActiveFile != "" && status.Tail.ActiveFile != w.baseline && status.Tail.State == mysql.ArchiverTailDrained:
		return settle("")
	default:
		w.last = archiveTailPendingReason(status.Tail, w.rotated)
	}
	if w.haveBaseline && !w.rotated {
		// Only after the baseline: the wait recognises the rotation by the
		// active file changing from it.
		fc := tm.failover
		if fc == nil {
			fc = NewFailoverControllerWithClock(tm.logger, tm.clock)
		}
		if w.rotated = fc.rotateBinlog(ctx, recipient.mysql, w.site, preRecloneRotation); !w.rotated {
			w.last = "FLUSH BINARY LOGS on the site was not acknowledged"
		} else {
			w.last = "the site's sidecar has not reported a scan of the rotated binary log yet"
		}
	}
	if now.Sub(w.startedAt) >= preRecloneArchiveTailTimeout {
		return settle(w.last)
	}
	return false
}

// archiveTailPendingReason words a tail that is not Drained yet, or a
// Drained one from before the rotation.
func archiveTailPendingReason(tail *mysql.ArchiverTailStatus, rotated bool) string {
	switch {
	case !rotated:
		return "FLUSH BINARY LOGS on the site was not acknowledged"
	case tail.State == mysql.ArchiverTailBlocked:
		return fmt.Sprintf("the site's archiver cannot archive the tail: %s", tail.Reason)
	case tail.State == mysql.ArchiverTailPending:
		return fmt.Sprintf("the site's archiver still had %d sealed binary log(s) to archive", tail.PendingFiles)
	}
	return "the site's sidecar has not reported a scan of the rotated binary log yet"
}

// clearRecloneArchiveTail forgets the pre-reclone wait and its result: the
// reclone started, or its request is gone.
func (tm *TopologyManager) clearRecloneArchiveTail() {
	tm.recloneTail = nil
	tm.recloneTailSettled = ""
}
