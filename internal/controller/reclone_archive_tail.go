package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

const (
	// preRecloneArchiveTailTimeout is how long a reclone gives its
	// recipient's sidecar to archive the binary log tail CLONE is about to
	// wipe, counted from the moment the tail was sealed: the operator's
	// FLUSH BINARY LOGS on the recipient was acknowledged. After it the
	// clone starts regardless, with a Warning Event.
	//
	// Two minutes covers what the wait is for: the sidecar scans on the
	// rotation itself (inotify on the binlog index), hashes and indexes the
	// sealed file and uploads it, and a full-size tail (max_binlog_size is
	// 1 GiB by default) uploads in that time at 10 MB/s or more. A reclone
	// is not urgent: its recipient is a replica that is being rebuilt, and
	// the clone itself takes minutes to hours. A longer bound would only
	// delay a reclone whose sidecar cannot archive at all (an unreachable
	// store, a refused archive), which is when the bound is reached.
	//
	// The same span bounds the attempts to get the rotation acknowledged,
	// counted from the wait's start, and the whole wait is capped at twice
	// the span (recloneTailWait.deadline). The verdict at the deadline is
	// given on a status request that began at or after it, so a wait ends
	// at most one step (archiveTailStepTimeout) and one poll past it.
	preRecloneArchiveTailTimeout = 2 * time.Minute
	// archiveTailStepTimeout bounds one step of the wait (a status request
	// and, until one is acknowledged, a rotation) on the manager's clock.
	// A step that has not returned by then is abandoned: its context is
	// cancelled and the next poll starts another. The sidecar client's own
	// HTTP timeout and the rotation's bound (BinlogRotationTimeout) end a
	// step well before this; it exists for a call that ignores both.
	archiveTailStepTimeout = 10 * time.Second
)

// ArchiveTailSource is what the pre-reclone wait reads PITR archiving
// from. The runner wires the group's archiver poller.
type ArchiveTailSource interface {
	// ArchivingEnabled reports whether PITR archiving is enabled for the
	// group. It is called on the poll goroutine and must not block: no
	// I/O. When false the reclone skips the rotation, the wait and the
	// Event.
	ArchivingEnabled() bool
	// TailStatus reads one site's /archiver/status. It is called off the
	// poll goroutine; ctx is cancelled when the wait no longer needs the
	// answer.
	TailStatus(ctx context.Context, site string) (*mysql.ArchiverStatus, error)
}

// recloneTailWait is one pre-reclone wait: from the first poll that found
// the reclone ready to start until the recipient's tail is archived or the
// deadline has passed. It lives in memory only. An operator restart starts
// over: the reclone annotation is still there, so the new process rotates
// again (one more empty file) and waits again, from zero.
//
// Only the poll goroutine reads and writes it. The step it has in flight
// runs on another goroutine and shares nothing with it but its own result
// (recloneTailStep).
type recloneTailWait struct {
	site      string
	startedAt time.Time
	// rotated: a FLUSH BINARY LOGS on the recipient was acknowledged at
	// rotatedAt and the server then wrote rotatedFile. Every file before
	// rotatedFile is sealed, so a scan of the sidecar that names
	// rotatedFile or a later file as the active one has seen everything
	// the site wrote before the rotation as sealed files.
	rotated     bool
	rotatedAt   time.Time
	rotatedFile string
	// judged: at least one step has reported; freshAt is when the newest
	// report was taken (the start of a step that returned, or the moment
	// a step was abandoned).
	judged  bool
	freshAt time.Time
	// archived and noArchiver are what the newest report says: the tail
	// is confirmed archived, or the sidecar runs no archiver at all.
	archived   bool
	noArchiver bool
	// last is why the tail is not confirmed archived yet, for the Event.
	last string
	// step is the step in flight, nil when none.
	step *recloneTailStep
}

// deadline is when the wait gives up: preRecloneArchiveTailTimeout after
// the acknowledged rotation, or after the wait's start while no rotation
// has been acknowledged, and never later than twice that span after the
// start. A rotation first acknowledged late in the first span therefore
// still leaves the sidecar the whole span to archive the file it sealed;
// only the time the last attempt itself took comes off it.
func (w *recloneTailWait) deadline() time.Time {
	if !w.rotated {
		return w.startedAt.Add(preRecloneArchiveTailTimeout)
	}
	limit := w.startedAt.Add(2 * preRecloneArchiveTailTimeout)
	if d := w.rotatedAt.Add(preRecloneArchiveTailTimeout); d.Before(limit) {
		return d
	}
	return limit
}

// recloneTailStep is one asynchronous step of the wait. The poll goroutine
// owns startedAt and cancel; the worker writes res and done under
// TopologyManager.recloneTailMu, and the poll goroutine reads them there.
type recloneTailStep struct {
	startedAt time.Time
	cancel    context.CancelFunc
	done      bool
	res       recloneTailReport
}

// recloneTailReport is what one step found.
type recloneTailReport struct {
	// status and statusErr are the sidecar's /archiver/status answer.
	status    *mysql.ArchiverStatus
	statusErr error
	// rotatedFile is the active binary log after an acknowledged rotation
	// at rotatedAt, sent after status was read; "" when this step sent
	// none or it was not acknowledged, and rotationNote then says why.
	rotatedFile  string
	rotatedAt    time.Time
	rotationNote string
}

// recloneArchiveTailSettled is the step a reclone takes before CLONE wipes
// its recipient's data directory (#264): the recipient is often an old
// primary, and the last binary log it wrote as the primary may never have
// been archived. It reports true when the clone may start.
//
// checkReclone drives it once per poll, and it never waits on the
// recipient: the status request to the recipient's sidecar and the
// rotation run as one step on a goroutine of their own, at most one at a
// time, and a later poll picks the result up. A recipient whose sidecar or
// server hangs therefore costs the poll loop nothing; primary-loss
// detection keeps its period (the same reason probe backoff is per site,
// see Run).
//
//  1. Read the recipient sidecar's archive tail: the baseline, the active
//     file of its last scan.
//  2. In the same step, rotate the recipient's binary log, best effort
//     (rotateBinlog), and read the name of the file the server writes
//     afterwards, on the same connection (mysql.Checker.RotateBinaryLog).
//     Retried by every step until one is acknowledged and its file comes
//     after the baseline.
//  3. Wait until the sidecar reports a scan that saw that rotation (its
//     active file is the one read in step 2, or a later one) and ended
//     Drained: every sealed file the site has to archive is in its
//     manifest.
//
// The file names are what ties a scan to the rotation. The sidecar's last
// published scan can be older than the server's last rotation, so "another
// active file than before" proves nothing; "the file the operator's
// rotation opened, or a later one" does, because a scan that read the
// binary log index before the rotation names an earlier file. That holds
// only within one numbering: the baseline must name an earlier file than
// the rotation opened, or the rotation does not count (a baseline at or
// past it is a scan of a numbering that has since started again, by RESET
// BINARY LOGS AND GTIDS or a new data directory), and a later scan that
// names an earlier file, or a name that cannot be compared, never ends the
// wait.
//
// At the deadline (recloneTailWait.deadline) the clone starts anyway, and
// the gap is announced: a WARN line and, through
// ArchiveTailNotArchivedCallback, a Warning Event naming the site. That
// includes a rotation that was never acknowledged or whose file name could
// not be read: the tail is then unconfirmed, never "archived". Restores to
// a target inside an unarchived tail refuse PITRArchiveGap; the Event is
// what tells an operator why.
//
// The wait holds nothing: the reclone request stays pending, no claim is
// taken, and every other check of the poll (failover, recovery) runs as
// usual. checkReclone validates the request again on every poll, so a
// recipient that became the primary meanwhile is never cloned. A poll that
// does not get here (the request is gone or deferred) cancels the step in
// flight (pauseRecloneArchiveTail).
func (tm *TopologyManager) recloneArchiveTailSettled(ctx context.Context, recipient *siteTracker) bool {
	src := tm.ArchiveTail
	if src == nil || !src.ArchivingEnabled() {
		tm.dropRecloneTailWait()
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
		tm.dropRecloneTailWait()
		w = &recloneTailWait{site: recipient.name, startedAt: now, last: "the site's sidecar has not answered yet"}
		tm.recloneTail = w
		tm.logger.Info("pre-reclone archive tail wait started", "site", w.site, "timeout", preRecloneArchiveTailTimeout.String())
	}
	// At most two passes: collect and judge what an earlier poll started,
	// start the next step, and judge that one too if it already returned
	// (it does when a simulation runs steps inline).
	for pass := 0; ; pass++ {
		tm.collectRecloneTailStep(w, now)
		if reason, done := w.verdict(now); done {
			return tm.settleRecloneTailWait(w, now, reason)
		}
		if w.step != nil || pass > 0 {
			return false
		}
		tm.startRecloneTailStep(ctx, w, recipient, now)
	}
}

// verdict reports whether the wait is over and, when the tail is not
// confirmed archived, why ("" when it is).
func (w *recloneTailWait) verdict(now time.Time) (reason string, done bool) {
	switch {
	case !w.judged:
		return "", false
	case w.noArchiver:
		// PITR is enabled for the group but this sidecar runs no archiver
		// (its pod has not rolled to the PITR configuration): nothing can
		// archive the tail, and waiting would not change that.
		return "the site's sidecar runs no binlog archiver", true
	case w.archived:
		return "", true
	}
	// The deadline's verdict needs a report taken at or after it: a wait
	// that was not driven for a while (the clone was deferred behind a
	// planned failover, say) must not end on what the sidecar said before.
	if deadline := w.deadline(); !now.Before(deadline) && !w.freshAt.Before(deadline) {
		return w.last, true
	}
	return "", false
}

// settleRecloneTailWait ends the wait: the clone may start. reason is ""
// when the tail is confirmed archived.
func (tm *TopologyManager) settleRecloneTailWait(w *recloneTailWait, now time.Time, reason string) bool {
	waited := now.Sub(w.startedAt).Milliseconds()
	tm.dropRecloneTailWait()
	tm.recloneTailSettled = w.site
	if reason == "" {
		tm.logger.Info("pre-reclone archive tail archived", "site", w.site, "waitedMs", waited, "rotatedFile", w.rotatedFile)
		return true
	}
	tm.logger.Warn("pre-reclone archive tail not archived", "site", w.site, "waitedMs", waited, "rotatedFile", w.rotatedFile, "reason", reason)
	if tm.ArchiveTailNotArchivedCallback != nil {
		tm.ArchiveTailNotArchivedCallback(w.site, reason)
	}
	return true
}

// collectRecloneTailStep takes the result of the step in flight if it has
// returned, and abandons a step that outlived archiveTailStepTimeout.
func (tm *TopologyManager) collectRecloneTailStep(w *recloneTailWait, now time.Time) {
	step := w.step
	if step == nil {
		return
	}
	tm.recloneTailMu.Lock()
	done, res := step.done, step.res
	tm.recloneTailMu.Unlock()
	switch {
	case done:
		w.step = nil
		w.absorb(res, step.startedAt)
	case now.Sub(step.startedAt) >= archiveTailStepTimeout:
		step.cancel()
		w.step = nil
		w.judged, w.freshAt = true, now
		w.archived, w.noArchiver = false, false
		w.last = fmt.Sprintf("the status request to the site's sidecar or the rotation of its binary log did not return within %s", archiveTailStepTimeout)
	}
}

// absorb folds one step's report, taken at takenAt, into the wait.
func (w *recloneTailWait) absorb(res recloneTailReport, takenAt time.Time) {
	w.judged, w.freshAt = true, takenAt
	w.archived, w.noArchiver = false, false
	switch {
	case res.statusErr != nil:
		w.last = fmt.Sprintf("the site's sidecar did not answer: %v", res.statusErr)
		return
	case res.status == nil || !res.status.Enabled:
		w.noArchiver = true
		return
	case res.status.Tail == nil:
		w.last = "the site's sidecar reports no archive tail (it has not scanned the binary log index yet, or it predates the field and archives nothing while read-only)"
		return
	}
	tail := res.status.Tail
	if !w.rotated {
		// This step read the baseline and then sent the rotation.
		switch {
		case res.rotatedFile == "":
			w.last = res.rotationNote
			if w.last == "" {
				w.last = "FLUSH BINARY LOGS on the site was not acknowledged"
			}
		case !baselinePrecedesRotation(tail.ActiveFile, res.rotatedFile):
			w.last = fmt.Sprintf("the site's sidecar last scanned with %s as the active binary log, but the rotation opened %s: the numbering of the site's binary logs started again, and that scan says nothing about the files the site has now",
				tail.ActiveFile, res.rotatedFile)
		default:
			w.rotated, w.rotatedAt, w.rotatedFile = true, res.rotatedAt, res.rotatedFile
			w.last = "the site's sidecar has not reported a scan of the rotated binary log yet"
		}
		// Whatever the baseline says, it was read before the rotation.
		return
	}
	if why := scanPredatesRotation(tail.ActiveFile, w.rotatedFile); why != "" {
		w.last = why
		return
	}
	switch tail.State {
	case mysql.ArchiverTailDrained:
		w.archived = true
	case mysql.ArchiverTailBlocked:
		w.last = fmt.Sprintf("the site's archiver cannot archive the tail: %s", tail.Reason)
	case mysql.ArchiverTailPending:
		w.last = fmt.Sprintf("the site's archiver still had %d sealed binary log(s) to archive", tail.PendingFiles)
	default:
		w.last = fmt.Sprintf("the site's sidecar reports an archive tail state this operator does not know (%q)", tail.State)
	}
}

// baselinePrecedesRotation reports whether the active file of the sidecar's
// last scan before the rotation, baseline, is an earlier file of the same
// numbering than rotated, the file the rotation opened. A sidecar that had
// no file to name ("") precedes every file.
func baselinePrecedesRotation(baseline, rotated string) bool {
	if baseline == "" {
		return true
	}
	baseBase, baseSeq, ok := mysql.BinlogFileSeq(baseline)
	if !ok {
		return false
	}
	rotatedBase, rotatedSeq, ok := mysql.BinlogFileSeq(rotated)
	return ok && baseBase == rotatedBase && baseSeq < rotatedSeq
}

// scanPredatesRotation says why a sidecar scan whose active binary log was
// active does not prove it saw the operator's rotation, which opened
// rotated; "" when it does. The files of one binary log are numbered in
// order, so the scan saw the rotation when its active file is rotated or a
// later one. An earlier one is a scan that read the index before the
// rotation, or numbering that started again (RESET BINARY LOGS AND GTIDS, a
// new data directory); a name that cannot be compared proves nothing
// either.
func scanPredatesRotation(active, rotated string) string {
	rotatedBase, rotatedSeq, ok := mysql.BinlogFileSeq(rotated)
	if !ok {
		return fmt.Sprintf("the binary log the rotation opened has a name without a sequence number (%q)", rotated)
	}
	if active == "" {
		return "the site's sidecar has not reported a scan of the rotated binary log yet"
	}
	activeBase, activeSeq, ok := mysql.BinlogFileSeq(active)
	if !ok || activeBase != rotatedBase {
		return fmt.Sprintf("the site's sidecar last scanned with %s as the active binary log, which cannot be compared with %s, the file the rotation opened", active, rotated)
	}
	if activeSeq < rotatedSeq {
		return fmt.Sprintf("the site's sidecar has not reported a scan of the rotated binary log yet (its last scan saw %s as the active file; the rotation opened %s)", active, rotated)
	}
	return ""
}

// startRecloneTailStep starts the wait's next step off the poll goroutine.
func (tm *TopologyManager) startRecloneTailStep(ctx context.Context, w *recloneTailWait, recipient *siteTracker, now time.Time) {
	stepCtx, cancel := context.WithCancel(ctx)
	step := &recloneTailStep{startedAt: now, cancel: cancel}
	w.step = step
	src, site, checker, rotate := tm.ArchiveTail, w.site, recipient.mysql, !w.rotated
	fc := tm.failover
	if fc == nil {
		fc = NewFailoverControllerWithClock(tm.logger, tm.clock)
	}
	run := func() {
		defer tm.recloneTailWorkers.Done()
		defer cancel()
		res := tm.recloneTailStepReport(stepCtx, src, fc, checker, site, rotate)
		tm.recloneTailMu.Lock()
		step.res, step.done = res, true
		tm.recloneTailMu.Unlock()
	}
	tm.recloneTailWorkers.Add(1)
	if tm.recloneTailRun != nil {
		tm.recloneTailRun(run)
		return
	}
	go run()
}

// recloneTailStepReport is the body of one step: read the recipient
// sidecar's archiver status and, while no rotation has been acknowledged,
// rotate the recipient's binary log. It runs off the poll goroutine and
// touches no wait state.
//
// The rotation is sent only after a status with a tail block was read:
// that status is the baseline the rotation's file is checked against
// (recloneTailWait.absorb), and a sidecar that does not answer, or runs no
// archiver, would not archive what the rotation seals.
//
// The claim check and the rotation are not atomic. checkReclone starts no
// step while another actor holds the sites (a planned failover, an ordered
// update, a promotion) and cancels the step in flight when one does; the
// step checks again right before the statement. A claim taken after that
// check, while the statement is on its way (at most BinlogRotationTimeout),
// still meets the rotation. That is harmless: FLUSH BINARY LOGS changes no
// data and no GTID, works on a fenced or a writable server alike, and
// leaves one more file boundary on a site whose files are about to be
// wiped. Holding tm.mu across the statement would close the window and
// make every claim, and every poll, wait on the recipient instead.
func (tm *TopologyManager) recloneTailStepReport(ctx context.Context, src ArchiveTailSource, fc *FailoverController, checker mysql.Checker, site string, rotate bool) recloneTailReport {
	var res recloneTailReport
	res.status, res.statusErr = src.TailStatus(ctx, site)
	if !rotate || res.statusErr != nil || res.status == nil || !res.status.Enabled || res.status.Tail == nil {
		return res
	}
	if ctx.Err() != nil {
		res.rotationNote = "the wait's step was cancelled before FLUSH BINARY LOGS was sent"
		return res
	}
	tm.mu.RLock()
	busy := tm.bootstrapClaimRefusedLocked()
	tm.mu.RUnlock()
	if busy != "" {
		res.rotationNote = fmt.Sprintf("FLUSH BINARY LOGS on the site was not sent: %s", busy)
		return res
	}
	rot := &namedRotation{checker: checker, file: make(chan string, 1)}
	if !fc.rotateBinlog(ctx, rot, site, preRecloneRotation) {
		res.rotationNote = "FLUSH BINARY LOGS on the site was not acknowledged, or the name of the file it opened could not be read"
		return res
	}
	// rotateBinlog reports true only after FlushBinaryLogs returned nil,
	// which is after the name was sent.
	res.rotatedFile = <-rot.file
	res.rotatedAt = tm.clock.Now()
	return res
}

// namedRotation lets rotateBinlog bound mysql.Checker.RotateBinaryLog: the
// flush and the read of the file it opened count as one best-effort
// rotation, acknowledged only when both answered and the name can be
// compared with a sidecar's.
type namedRotation struct {
	checker mysql.Checker
	file    chan string
}

func (r *namedRotation) FlushBinaryLogs(ctx context.Context) error {
	file, err := r.checker.RotateBinaryLog(ctx)
	if err != nil {
		return err
	}
	if _, _, ok := mysql.BinlogFileSeq(file); !ok {
		return fmt.Errorf("the server names its active binary log %q, which has no sequence number", file)
	}
	r.file <- file
	return nil
}

// pauseRecloneArchiveTail cancels the step in flight and discards its
// result, keeping the wait: a poll did not drive the wait (the reclone
// request is deferred behind another topology change, or is being
// dropped), so nothing should be sent to the recipient on its behalf. The
// next poll that drives the wait starts a new step.
func (tm *TopologyManager) pauseRecloneArchiveTail() {
	if w := tm.recloneTail; w != nil && w.step != nil {
		w.step.cancel()
		w.step = nil
	}
}

// dropRecloneTailWait forgets the wait in progress and cancels its step.
func (tm *TopologyManager) dropRecloneTailWait() {
	tm.pauseRecloneArchiveTail()
	tm.recloneTail = nil
}

// clearRecloneArchiveTail forgets the pre-reclone wait and its result: the
// reclone started, or its request is gone.
func (tm *TopologyManager) clearRecloneArchiveTail() {
	tm.dropRecloneTailWait()
	tm.recloneTailSettled = ""
}

// waitRecloneTailWorkers blocks until every step goroutine the wait
// started has returned. Steps end on their own bounds or when their
// context is cancelled (the manager's Run context, pauseRecloneArchiveTail);
// nothing in production waits for them.
func (tm *TopologyManager) waitRecloneTailWorkers() {
	tm.recloneTailWorkers.Wait()
}
