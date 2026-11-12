package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// cloneServerUUIDProbeAttempts is how many times the recipient's
// server_uuid is read after a clone before the check gives up. The read
// follows verifyCloneOutcome, which has just had an answer from the
// restarted server, so more than one attempt only covers a dropped pooled
// connection.
const cloneServerUUIDProbeAttempts = 3

// readServerUUID reads @@server_uuid of a clone recipient. It returns ""
// when the checker cannot read a server's identity (gtidSnapshotReader) or
// the read fails; the error says which.
func readServerUUID(ctx context.Context, c mysql.Checker) (string, error) {
	r, ok := c.(gtidSnapshotReader)
	if !ok {
		return "", fmt.Errorf("the checker does not read @@server_uuid")
	}
	var snap mysql.GTIDSnapshot
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, err = r.ReadGTIDSnapshot(probeCtx)
		return err
	})
	if err != nil {
		return "", err
	}
	if snap.ServerUUID == "" {
		return "", fmt.Errorf("the server reported an empty @@server_uuid")
	}
	return snap.ServerUUID, nil
}

// cloneServerUUIDBefore reads the recipient's server_uuid before CLONE
// INSTANCE, for verifyCloneServerUUIDRotated. A failed read is logged and
// returns "": the clone goes ahead and the check after it is skipped.
func (tm *TopologyManager) cloneServerUUIDBefore(ctx context.Context, recipient mysql.Checker, site string) string {
	uuid, err := readServerUUID(ctx, recipient)
	if err != nil {
		tm.logger.Warn("could not read the recipient server_uuid before clone", "site", site, "error", err)
	}
	return uuid
}

// cloneIdentityVerdict is the outcome of one server_uuid comparison.
type cloneIdentityVerdict int

const (
	// cloneIdentityUnknownBefore: the recipient's server_uuid before the
	// clone is not known, so there is nothing to compare.
	cloneIdentityUnknownBefore cloneIdentityVerdict = iota
	// cloneIdentityUnreadAfter: the recipient's server_uuid could not be
	// read after the clone. The comparison can be made later.
	cloneIdentityUnreadAfter
	cloneIdentityRotated
	cloneIdentityKept
)

// verifyCloneServerUUIDRotated checks, after a clone that
// verifyCloneOutcome confirmed, that the recipient came back with another
// server_uuid than before had (issue #265).
//
// The mysql container's launcher (mysqlLauncher) removes auto.cnf on the
// start that installs a clone, so the recipient gets a new server_uuid
// and cannot reissue a GTID its discarded history already used. Nothing
// in the operator can make that happen: the launcher runs inside the pod,
// between the clone and mysqld. This is the check that a launcher which
// did not run (a pod rendered before it, an image whose mysqld is not
// started through it) or could not remove the file is seen, not silently
// tolerated.
//
// An unchanged server_uuid is reported and nothing else: a WARN line and,
// through CloneServerUUIDNotRotatedCallback, a Warning Event. It does not
// fail the bootstrap and does not make the site ineligible for promotion.
// Replication is still set up, because the recipient's data is correct and
// a replica that is not replicating is the larger risk. Promotion is not
// refused, because the hazard is to the PITR archive and only if the site
// is promoted after losing transactions, and that case already fails
// closed where it matters (the sidecar's reissue verdict,
// PITRArchiveReissueRiskDetected at the promotion, PITRArchiveHistoryReset
// on restores); refusing the site as a failover candidate would turn a
// recoverable archive problem into an outage of a two-site group.
func (tm *TopologyManager) verifyCloneServerUUIDRotated(ctx context.Context, recipient mysql.Checker, site, before string) cloneIdentityVerdict {
	if before == "" {
		tm.logger.Info("clone recipient server_uuid rotation not verified", "site", site,
			"reason", "the recipient's server_uuid before the clone is unknown")
		return cloneIdentityUnknownBefore
	}
	var after string
	var err error
	for attempt := 0; attempt < cloneServerUUIDProbeAttempts; attempt++ {
		if after, err = readServerUUID(ctx, recipient); err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		tm.logger.Warn("clone recipient server_uuid rotation not verified", "site", site,
			"reason", fmt.Sprintf("could not read the recipient's server_uuid after the clone: %v", err))
		return cloneIdentityUnreadAfter
	}
	if after != before {
		tm.logger.Info("clone recipient server_uuid rotated", "site", site, "previousServerUuid", before, "serverUuid", after)
		return cloneIdentityRotated
	}
	tm.logger.Warn("clone recipient kept its server_uuid", "site", site, "serverUuid", after)
	if tm.CloneServerUUIDNotRotatedCallback != nil {
		tm.CloneServerUUIDNotRotatedCallback(site, after)
	}
	return cloneIdentityKept
}

// The comparison above needs the recipient's server_uuid from before the
// clone, and a manager that only keeps it in memory loses it with the
// process. A clone outlives the operator: mysqld finishes copying and
// restarts on its own, and the next manager finds the donor's data in
// place and only sets up replication (canSkipClone). Without a durable
// record that clone would never be checked, in exactly the case where the
// check matters most: with encryption at rest, a finished clone installed
// by a pod whose keyring was sealed in the meantime keeps its old
// server_uuid (serverUUIDRotationScript), and the only trace would be a
// line in the mysql container's log.
//
// So the value is written to the group before CLONE INSTANCE is issued
// (CloneIdentityStore; the runner keeps it in CloneServerUUIDAnnotation)
// and removed once the clone it belongs to has an outcome:
//
//   - the manager that ran the clone compares right after
//     verifyCloneOutcome, as before;
//   - any later look (settleCloneIdentity: at the start of every
//     bootstrap, before it decides whether to skip the clone, and from the
//     poll loop) reads the recipient's clone_status. A Completed row newer
//     than the one recorded with the value is that clone, installed:
//     compare. The recorded row still being the newest means CLONE never
//     began, and a newer Failed row that it failed: the record is dropped
//     with no verdict (a failed clone keeps its server_uuid by design). A
//     clone still in progress, or a recipient that does not answer, leaves
//     the record for the next look.
//
// Every record gets at most one verdict from a manager: the record is
// marked decided in memory before it is removed from the group. A manager
// that dies between the report and the removal leaves the record, and its
// successor reports again; a report is never lost that way.
//
// The same record decides when a recipient's Clone keyring hold may be
// released (cloneRestartUnconfirmed).

// CloneIdentity is what is recorded about a clone recipient before CLONE
// INSTANCE runs on it.
type CloneIdentity struct {
	// ServerUUID is the recipient's @@server_uuid before the clone.
	ServerUUID string `json:"serverUUID"`
	// PreviousCloneBegin is BEGIN_TIME of the recipient's
	// performance_schema.clone_status row before the clone ("" when it had
	// none): a row with another BEGIN_TIME belongs to a clone issued after
	// this record was written. nil when the row could not be read; such a
	// record never yields a "kept" verdict, because an earlier clone's
	// Completed row cannot be told from this one's.
	PreviousCloneBegin *string `json:"previousCloneBegin,omitempty"`
}

func (c CloneIdentity) equal(o CloneIdentity) bool {
	if c.ServerUUID != o.ServerUUID || (c.PreviousCloneBegin == nil) != (o.PreviousCloneBegin == nil) {
		return false
	}
	return c.PreviousCloneBegin == nil || *c.PreviousCloneBegin == *o.PreviousCloneBegin
}

// CloneIdentityStore keeps CloneIdentity records where they survive the
// operator. Implemented by the runner on the group's metadata
// (groupCloneIdentityStore). Nil in tests and harnesses that do not
// exercise it: the manager then compares in memory only, as it did before
// the record existed.
type CloneIdentityStore interface {
	// Load returns the stored records by site.
	Load(ctx context.Context) (map[string]CloneIdentity, error)
	// Record stores (or replaces) site's record. The group's store also
	// adds rec.ServerUUID and the clone marker to the site's server_uuid
	// timeline in the same write (groupCloneIdentityStore.Record).
	Record(ctx context.Context, site string, rec CloneIdentity) error
	// Resolve removes site's record if it still equals rec.
	Resolve(ctx context.Context, site string, rec CloneIdentity) error
}

// cloneIdentityRetryInterval is how often the poll loop looks again at a
// record it could not settle (the recipient is still cloning or
// restarting, or the group could not be written).
const cloneIdentityRetryInterval = 15 * time.Second

// pendingCloneIdentity is a record and what this manager knows about it.
type pendingCloneIdentity struct {
	rec CloneIdentity
	// decided: the clone has its outcome (a verdict, or none to give), and
	// only the removal of the stored record is left.
	decided bool
	// restarted: the recipient has restarted since the clone (it answered
	// with a Completed clone_status row), although its server_uuid could
	// not be read yet.
	restarted bool
	// installed: the clone is known to have installed (the recipient
	// answered with the clone's Completed clone_status row, or under
	// another server_uuid). The site's server_uuid timeline gets its clone
	// marker before the record is removed (finishCloneIdentity).
	installed bool
	// releaseHold: the recipient's Clone keyring hold was kept at the end
	// of a bootstrap because of this record, or may have been by an
	// earlier manager; it is released when the record is settled.
	releaseHold bool
}

// cloneIdentityTracker is the manager's view of the stored records.
type cloneIdentityTracker struct {
	// settleMu serializes the looks at a record (the bootstrap goroutine
	// and the poll loop), so a record gets one verdict.
	settleMu sync.Mutex

	mu      sync.Mutex
	store   CloneIdentityStore
	loaded  bool
	pending map[string]pendingCloneIdentity
	// nextLook rate-limits the poll loop's looks.
	nextLook time.Time
}

// SetCloneIdentityStore wires the durable store of pre-clone server_uuids.
func (tm *TopologyManager) SetCloneIdentityStore(s CloneIdentityStore) {
	tm.cloneIdentity.mu.Lock()
	tm.cloneIdentity.store = s
	tm.cloneIdentity.loaded = false
	tm.cloneIdentity.mu.Unlock()
}

// loadCloneIdentities reads the stored records once per manager. Records
// found there were written by an earlier manager, which may have kept the
// recipient's Clone keyring hold for them.
func (tm *TopologyManager) loadCloneIdentities(ctx context.Context) (CloneIdentityStore, error) {
	t := &tm.cloneIdentity
	t.mu.Lock()
	store, loaded := t.store, t.loaded
	t.mu.Unlock()
	if store == nil || loaded {
		return store, nil
	}
	stored, err := store.Load(ctx)
	if err != nil {
		return store, fmt.Errorf("load recorded clone recipient server_uuids: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.loaded {
		t.loaded = true
		if t.pending == nil {
			t.pending = map[string]pendingCloneIdentity{}
		}
		for site, rec := range stored {
			if _, ok := t.pending[site]; !ok {
				t.pending[site] = pendingCloneIdentity{rec: rec, releaseHold: true}
			}
		}
	}
	return store, nil
}

func (tm *TopologyManager) pendingCloneIdentity(site string) (pendingCloneIdentity, bool) {
	tm.cloneIdentity.mu.Lock()
	defer tm.cloneIdentity.mu.Unlock()
	p, ok := tm.cloneIdentity.pending[site]
	return p, ok
}

// recordCloneIdentity stores the recipient's server_uuid before CLONE
// INSTANCE is issued. An error means the record is not durable: the caller
// must not start the clone, or a clone that outlives this manager would
// never be checked.
//
// A record that is still waiting for its clone's outcome and names the
// same server_uuid is kept as it is: its older clone_status row covers
// both clones, and replacing it could mistake the earlier clone's row,
// if that clone is still running, for "this clone never began".
func (tm *TopologyManager) recordCloneIdentity(ctx context.Context, site, serverUUID string, prev *mysql.CloneStatus, prevErr error) error {
	store, err := tm.loadCloneIdentities(ctx)
	if store == nil || err != nil {
		if err == nil {
			// No record to write the server_uuid timeline with (a test or
			// a simulation): it is stored on its own, best effort.
			tm.noteSiteServerUUID(ctx, site, serverUUID)
		}
		return err
	}
	if serverUUID == "" {
		// Nothing to compare this clone against. An older record, if any,
		// stays: a clone completed since it was written with the same
		// server_uuid is still a clone that did not rotate.
		return nil
	}
	rec := CloneIdentity{ServerUUID: serverUUID}
	if prevErr == nil {
		begin := ""
		if prev != nil {
			begin = prev.BeginTime
		}
		rec.PreviousCloneBegin = &begin
	}
	if p, ok := tm.pendingCloneIdentity(site); ok && !p.decided && p.rec.ServerUUID == serverUUID {
		// The write that stored that record stored the uuid in the
		// site's timeline too, unless an operator from before that did
		// it wrote it: make sure, best effort.
		tm.noteSiteServerUUID(ctx, site, serverUUID)
		return nil
	}
	if err := store.Record(ctx, site, rec); err != nil {
		return fmt.Errorf("record the server_uuid of clone recipient %s: %w", site, err)
	}
	// The same write stored the uuid in the site's timeline. The clone
	// itself is marked there when it is known to have installed
	// (finishCloneIdentity).
	tm.siteCloneRecorded(site, serverUUID)
	t := &tm.cloneIdentity
	t.mu.Lock()
	if t.pending == nil {
		t.pending = map[string]pendingCloneIdentity{}
	}
	t.pending[site] = pendingCloneIdentity{rec: rec}
	t.mu.Unlock()
	return nil
}

// markCloneIdentity updates site's pending record if it is still rec.
func (tm *TopologyManager) markCloneIdentity(site string, rec CloneIdentity, update func(*pendingCloneIdentity)) {
	t := &tm.cloneIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.pending[site]; ok && p.rec.equal(rec) {
		update(&p)
		t.pending[site] = p
	}
}

// finishCloneIdentity removes a decided record from the group, and from
// memory once that write succeeded. A failed removal is retried from the
// poll loop; the record is decided, so it is never judged again by this
// manager.
func (tm *TopologyManager) finishCloneIdentity(ctx context.Context, site string) {
	t := &tm.cloneIdentity
	t.mu.Lock()
	store := t.store
	p, ok := t.pending[site]
	t.mu.Unlock()
	if !ok || !p.decided || store == nil {
		return
	}
	// The outcome is known; do not lose the removal to a manager that is
	// shutting down.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	// An installed clone supersedes the server_uuids the site had. The
	// marker is stored before the record goes: while the record stands, a
	// manager that replaces this one looks at the clone again and marks
	// it. Until the marker is stored the earlier server_uuids stay live,
	// which errs toward reporting the site.
	if p.installed && !tm.markSiteCloned(writeCtx, site) {
		tm.logger.Warn("could not record the installed clone in the site's server_uuid history; will retry", "site", site)
		return
	}
	err := store.Resolve(writeCtx, site, p.rec)
	if err != nil {
		tm.logger.Warn("could not remove the recorded server_uuid of a clone recipient; will retry", "site", site, "error", err)
		return
	}
	// The hold that was kept for this clone's restart can go now.
	if p.releaseHold && !tm.handOverCloneHoldRelease(site) {
		return
	}
	t.mu.Lock()
	if cur, ok := t.pending[site]; ok && cur.rec.equal(p.rec) {
		delete(t.pending, site)
	}
	t.mu.Unlock()
}

// handOverCloneHoldRelease asks the poll loop (releaseCloneHold) to release
// the Clone keyring hold that was kept for site's clone. It reports false
// when another site's release is still being retried there; the caller
// then keeps the record in memory and hands over at its next look. While a
// bootstrap of the site is in flight nothing is handed over: that
// bootstrap decides about the hold when it ends.
func (tm *TopologyManager) handOverCloneHoldRelease(site string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	switch tm.bootstrapPhase {
	case BootstrapPhaseCloning, BootstrapPhaseRestarting, BootstrapPhaseSetupRepl:
		if tm.bootstrapRecipient == site {
			return true
		}
	}
	if tm.cloneHoldReleaseSite != "" && tm.cloneHoldReleaseSite != site {
		return false
	}
	tm.cloneHoldReleaseSite = site
	return true
}

// cloneCompletedInProcess is the check of the manager that ran the clone,
// right after verifyCloneOutcome confirmed it. before is the server_uuid
// this run read; when that read failed, an older record of the site still
// names a server_uuid the recipient had before this clone.
func (tm *TopologyManager) cloneCompletedInProcess(ctx context.Context, recipient mysql.Checker, site, before string) {
	tm.cloneIdentity.settleMu.Lock()
	defer tm.cloneIdentity.settleMu.Unlock()
	p, recorded := tm.pendingCloneIdentity(site)
	if recorded && p.decided {
		recorded = false
	}
	if before == "" && recorded {
		before = p.rec.ServerUUID
	}
	verdict := tm.verifyCloneServerUUIDRotated(ctx, recipient, site, before)
	if !recorded {
		// No record whose removal waits for the marker: the installed
		// clone is marked on the site's timeline now, best effort.
		tm.markSiteCloned(ctx, site)
		return
	}
	if verdict == cloneIdentityUnreadAfter {
		// The clone is installed; only the read is missing. The poll loop
		// compares once the recipient answers.
		tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.restarted, p.installed = true, true })
		tm.markSiteCloned(ctx, site)
		return
	}
	tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.decided, p.installed = true, true })
	tm.finishCloneIdentity(ctx, site)
}

// settleCloneIdentity gives site's recorded clone its outcome if the
// recipient can tell it yet (see the comment above CloneIdentity). It is
// called where no CLONE INSTANCE of this manager is running on the site.
func (tm *TopologyManager) settleCloneIdentity(ctx context.Context, recipient mysql.Checker, site string) {
	if _, err := tm.loadCloneIdentities(ctx); err != nil {
		tm.logger.Warn("could not read the recorded server_uuids of clone recipients", "site", site, "error", err)
		return
	}
	tm.cloneIdentity.settleMu.Lock()
	defer tm.cloneIdentity.settleMu.Unlock()
	p, ok := tm.pendingCloneIdentity(site)
	if !ok {
		return
	}
	if p.decided {
		tm.finishCloneIdentity(ctx, site)
		return
	}
	var st *mysql.CloneStatus
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		st, err = recipient.CloneStatus(probeCtx)
		return err
	}); err != nil {
		return
	}
	state, begin := "", ""
	if st != nil {
		state, begin = st.State, st.BeginTime
	}
	notVerified := func(reason string) {
		tm.logger.Info("clone recipient server_uuid rotation not verified", "site", site, "reason", reason)
		tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.decided = true })
		tm.finishCloneIdentity(ctx, site)
	}
	prev := p.rec.PreviousCloneBegin
	switch {
	case st == nil || prev != nil && begin == *prev:
		notVerified("the clone its server_uuid was recorded for did not begin")
	case state == mysql.CloneStateFailed:
		notVerified("the clone its server_uuid was recorded for failed")
	case state != mysql.CloneStateCompleted:
		// Still copying, or waiting for the restart that installs it.
	case prev == nil:
		// A Completed row that may be an earlier clone's. A changed
		// server_uuid still proves a clone was installed since.
		after, err := readServerUUID(ctx, recipient)
		switch {
		case err != nil:
		case after != p.rec.ServerUUID:
			tm.logger.Info("clone recipient server_uuid rotated", "site", site, "previousServerUuid", p.rec.ServerUUID, "serverUuid", after)
			tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.decided, p.installed = true, true })
			tm.finishCloneIdentity(ctx, site)
		default:
			notVerified("the recipient's clone status before the clone is unknown, so its completed clone cannot be told from an earlier one")
		}
	default:
		// The recorded clone's Completed row: it installed.
		tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.restarted, p.installed = true, true })
		tm.markSiteCloned(ctx, site)
		if tm.verifyCloneServerUUIDRotated(ctx, recipient, site, p.rec.ServerUUID) == cloneIdentityUnreadAfter {
			return
		}
		tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.decided = true })
		tm.finishCloneIdentity(ctx, site)
	}
}

// settlePendingCloneIdentities is the poll loop's look at the records no
// bootstrap has settled: a clone that finished after the manager that
// issued it was gone, on a site whose replication something other than a
// bootstrap then set up, would otherwise never be checked.
func (tm *TopologyManager) settlePendingCloneIdentities(ctx context.Context) {
	t := &tm.cloneIdentity
	t.mu.Lock()
	wired, loaded, waiting, due := t.store != nil, t.loaded, len(t.pending) > 0, !tm.clock.Now().Before(t.nextLook)
	t.mu.Unlock()
	if !wired || loaded && !waiting || !due {
		return
	}
	tm.mu.RLock()
	busy := tm.bootstrapAdmitting != ""
	tm.mu.RUnlock()
	if busy || tm.isBootstrapping() {
		return
	}
	t.mu.Lock()
	t.nextLook = tm.clock.Now().Add(cloneIdentityRetryInterval)
	t.mu.Unlock()
	if _, err := tm.loadCloneIdentities(ctx); err != nil {
		tm.logger.Warn("could not read the recorded server_uuids of clone recipients", "error", err)
		return
	}
	t.mu.Lock()
	sites := make([]string, 0, len(t.pending))
	for site := range t.pending {
		sites = append(sites, site)
	}
	t.mu.Unlock()
	for _, name := range sites {
		site := tm.getSite(name)
		if site == nil {
			// The site left the group: nothing can be cloned into it.
			tm.cloneIdentity.settleMu.Lock()
			if p, ok := tm.pendingCloneIdentity(name); ok {
				tm.markCloneIdentity(name, p.rec, func(p *pendingCloneIdentity) { p.decided = true })
				tm.finishCloneIdentity(ctx, name)
			}
			tm.cloneIdentity.settleMu.Unlock()
			continue
		}
		tm.settleCloneIdentity(ctx, site.mysql, name)
	}
}

// cloneRestartUnconfirmed reports, at the end of a bootstrap of site,
// whether a clone issued into it may still be waiting for the restart that
// installs it: its record has no outcome and the recipient has not been
// seen restarted. The recipient's Clone keyring hold is kept while that is
// so. A finished clone installed by a pod whose keyring was sealed in the
// meantime cannot store the binary log encryption key a new server_uuid
// needs, and so keeps the old one (serverUUIDRotationScript); a clone
// still copying when its hold goes is killed by the pod roll that seals
// the site. settleCloneIdentity releases the hold once the record has its
// outcome.
//
// With no store wired nothing is recorded, and the hold is released as it
// was before the record existed.
func (tm *TopologyManager) cloneRestartUnconfirmed(ctx context.Context, recipient mysql.Checker, site string) bool {
	if p, ok := tm.pendingCloneIdentity(site); !ok || p.decided || p.restarted {
		return false
	}
	// One look now, also for a manager that is shutting down: a clone
	// that never began, or that failed, must not keep the hold.
	lookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mysqlSafetyProbeTimeout+5*time.Second)
	tm.settleCloneIdentity(lookCtx, recipient, site)
	cancel()
	p, ok := tm.pendingCloneIdentity(site)
	if !ok || p.decided || p.restarted {
		return false
	}
	tm.markCloneIdentity(site, p.rec, func(p *pendingCloneIdentity) { p.releaseHold = true })
	return true
}
