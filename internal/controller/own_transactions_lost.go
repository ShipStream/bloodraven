package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// A site that lacks transactions it issued itself.
//
// A replication receiver drops every event that carries the replica's own
// server_id (measured on MySQL 9.7: the events never reach the relay log,
// Retrieved_Gtid_Set never lists them, both threads report running and no
// error is raised; the filter is by server_id, a new server_uuid does not
// lift it). So a site that comes back without transactions it originated
// (its volume came back older, or a crash lost them from both the redo log
// and the binary log) while the primary holds them can never get them by
// replication. It would replicate everything else, look healthy, and at
// its next promotion issue the same GTID numbers for other transactions.
//
// The evidence is exact and needs no assumption about lag: every
// transaction under a site's server_uuid was committed on that site first,
// so whatever the primary holds under that server_uuid the site held once.
// lost = primary.gtid_executed − site.gtid_executed, restricted to the
// server_uuids the site has had. A site's server_id is spec.sites[].serverID
// and stays, while its server_uuid changes at every clone (#265), so the
// uuids a site has had are recorded on the group (SiteIdentityStore): a
// volume that comes back from before a reclone carries the earlier
// server_uuid and lacks everything issued under the later one.
//
// A site with a non-empty lost set is never joined to replication by the
// operator (initiateRecovery, withReplicaStartGate, canSkipClone), never
// promoted by it (ownLossLocked beside every restore-reclone promotion
// gate), and recloned (requestOwnLossReclone). A reclone discards the
// site's data: by the comparison it holds nothing the primary lacks (a
// site that does is a divergence, which stays with an administrator), and
// InnoDB persists a transaction's GTID with the transaction, so the site
// holds no committed change outside its gtid_executed either (measured:
// after losing the binary log tail with sync_binlog=0 the server still
// reported the GTIDs and the rows).
//
// Two checks find it: old-primary recovery, before it configures
// replication on a source-less site, and the replica GTID audit, for a
// site whose channel already exists (started by hand, by an operator that
// predates this check, or while no operator ran).
//
// Whose set is evidence. There are two questions, and they take different
// evidence:
//
//   - "Can this site be a replica of the primary?" The holder is the
//     confirmed active primary, the comparison runs under every
//     server_uuid the site has had, and nothing is subtracted: whatever
//     the primary holds with the site's server_id the site can only get
//     by a clone, whatever happened before. This also covers transactions
//     the site once discarded on purpose (a reclone that was told to drop
//     a divergence, a volume replaced after a restore): if the site that
//     kept them is the primary now, they are part of the data the group
//     serves, and the receiver still drops them.
//   - "May this site be made writable?" There is no primary, so the
//     holders are the other sites (witnesses), and only transactions
//     under a server_uuid of the site that no clone has superseded count
//     (liveServerUUIDs, ownLostOnWitness): the one it runs under now,
//     whose numbers it would issue again, and any it had since its last
//     clone. A witness that holds transactions under a server_uuid the
//     site gave up at a clone proves nothing about the site: from the
//     clone on the site holds of that identity exactly what its donor
//     held, so what the witness has beyond that the site discarded at the
//     clone (its donor did not have it), on purpose when an administrator
//     recloned a divergent site or an in-place restore recloned its
//     peers. Such a witness is divergent, which the divergence checks
//     report once a primary exists. The clone is recorded on the site's
//     server_uuid timeline by the write the clone waits for
//     (groupCloneIdentityStore.Record). A site queued for the reclone of
//     an in-place restore (ownLossWitnessExcludedLocked) is no witness at
//     all: its data is void by a durable record, the operator never
//     promotes it or replicates from it, and it is cloned over.
//
// The automatic reclone is the one destructive step, so it does not rest
// on a report: verifyOwnLossReclone reads the fenced site and its donor
// again when the clone is about to start and lets it run only if the
// donor is the confirmed primary, holds everything the site holds, and
// still holds transactions the site issued and lacks.

const (
	// recloneOwnerOwnLoss is the owner of a reclone the operator requests
	// for a site that lost its own transactions. It has no annotation to
	// consume and no restore queue to report to.
	recloneOwnerOwnLoss = "operator/own-transactions-lost"

	// ownLossRecloneRetryDelay is how long the operator waits before it
	// requests another reclone of a site whose clone failed once. Each
	// further consecutive failure doubles the wait, up to
	// ownLossRecloneRetryMax (ownLossRecloneBackoff): a clone that fails
	// late (a full disk, a timeout) has already made the primary send the
	// whole data set, and must not do so every few minutes for ever.
	ownLossRecloneRetryDelay = 5 * time.Minute
	ownLossRecloneRetryMax   = 2 * time.Hour

	// ownLossRecloneFailureEvent is the number of consecutive failed
	// automatic reclones of a site after which the runner raises
	// OwnTransactionsLostRecloneFailing.
	ownLossRecloneFailureEvent = 3

	// serverUUIDHistoryLimit bounds the recorded server_uuids per site.
	serverUUIDHistoryLimit = 8

	// siteIdentityRetryInterval rate-limits writes of a newly seen
	// server_uuid that could not be stored.
	siteIdentityRetryInterval = 15 * time.Second

	// siteIdentityWriteTimeout bounds one load or write of the history.
	siteIdentityWriteTimeout = 5 * time.Second

	// ownLossWitnessGrace is the most a promotion decision waits, after
	// its candidates have answered, for the other sites' GTID sets
	// (readOwnLossWitnesses).
	ownLossWitnessGrace = 250 * time.Millisecond
)

// errOwnTransactionsLost is returned for a replication start refused
// because the site lacks transactions it issued itself.
var errOwnTransactionsLost = errors.New("the site lacks transactions it issued itself (status.sites[].lostGtid); it is recloned, not rejoined by replication")

// ownLossRecord is what the manager knows about a site that lacks
// transactions it issued. Protected by tm.mu.
type ownLossRecord struct {
	lostGtid   string
	lostCount  int64
	comparedTo string
	// witness is true when comparedTo was not the confirmed primary: the
	// site lacks transactions it issued that another site (a witness)
	// holds, and nothing the primary holds. Such a site can replicate
	// from the primary, so its replication is not refused and it is not
	// recloned automatically; it is not promoted while the witness holds
	// those transactions (it would issue their GTIDs again, or become a
	// primary without part of its own history). The report ends when the
	// witness no longer holds them (reviewWitnessOwnLoss) or the site is
	// recloned. False for a report restored from status, whose kind is
	// not stored: it holds like a report against the primary until the
	// next comparison.
	witness bool
	// reviewAfter is when a witness report is compared with its witness
	// again (reviewWitnessOwnLoss).
	reviewAfter time.Time
	// detectedBy is the check that made the comparison
	// (divergenceDetectedByRecovery or divergenceDetectedByAudit), "" for
	// a record restored from status and not compared again yet.
	detectedBy v1alpha1.DivergenceDetector
	// recloneRetryAfter is when the operator may request the next
	// automatic reclone: ownLossRecloneBackoff after a clone of the site
	// failed, zero otherwise. A request that was displaced, withdrawn or
	// never started its clone is made again at once.
	recloneRetryAfter time.Time
	// recloneFailures counts the automatic reclones of the site that
	// failed in a row. In memory: it is carried to a manager that
	// replaces this one in the same process (carryOwnLossRecloneBackoff)
	// and starts at zero after an operator restart.
	recloneFailures int
	// recloneWaiting says why no automatic reclone runs or is requested
	// ("" while one is).
	recloneWaiting string
	// recloneAnnounced is true while a request of this record stands, so
	// the request, which is made again on every poll until its clone
	// starts, is logged once.
	recloneAnnounced bool
	// recloneRefusal is the last reason verifyOwnLossReclone logged for
	// not starting the clone, so a refusal repeated on every poll is
	// logged once.
	recloneRefusal string
}

// ownLossRecloneBackoff is the wait after the failures-th consecutive
// failed automatic reclone of a site.
func ownLossRecloneBackoff(failures int) time.Duration {
	d := ownLossRecloneRetryDelay
	for i := 1; i < failures && d < ownLossRecloneRetryMax; i++ {
		d *= 2
	}
	return min(d, ownLossRecloneRetryMax)
}

// ownLostTransactions returns the transactions the primary holds under one
// of uuids that the site lacks.
func ownLostTransactions(primary, site mysql.GTIDSet, uuids []string) mysql.GTIDSet {
	return primary.Subtract(site).OriginatedBy(uuids...)
}

// ownLostOnWitness returns the transactions witness holds under one of
// live, the server_uuids of site that no clone has superseded
// (liveSiteServerUUIDs), that site lacks. Only those count when the holder
// is not the primary (see "Whose set is evidence" above).
func ownLostOnWitness(witness, site mysql.GTIDSet, live []string) mysql.GTIDSet {
	return witness.Subtract(site).OriginatedBy(live...)
}

// SiteIdentityStore keeps the server_uuids each site has run under where
// they survive the operator. Implemented by the runner on the group's
// metadata (groupSiteIdentityStore). Nil in tests and harnesses that do
// not exercise it: the manager then knows only the uuids it has seen.
//
// A site's record is a timeline, oldest first: its server_uuids, and
// serverUUIDCloneMarker where a clone into the site was issued. Every
// server_uuid before the last marker was given up at a clone
// (liveServerUUIDs).
type SiteIdentityStore interface {
	// Load returns the recorded timelines by site.
	Load(ctx context.Context) (map[string][]string, error)
	// Add appends entry (a server_uuid, or serverUUIDCloneMarker) to
	// site's record: a server_uuid unless it is there, the marker unless
	// it is the last entry.
	Add(ctx context.Context, site, entry string) error
}

// serverUUIDCloneMarker is the entry of a site's recorded timeline that
// stands for a clone issued into the site.
const serverUUIDCloneMarker = "clone"

// appendServerUUIDEntry appends entry to a site's timeline and reports
// whether it changed: a server_uuid unless it is there, the clone marker
// unless it is the last entry.
func appendServerUUIDEntry(timeline []string, entry string) ([]string, bool) {
	switch {
	case entry == "":
		return timeline, false
	case entry == serverUUIDCloneMarker:
		if n := len(timeline); n == 0 || timeline[n-1] == serverUUIDCloneMarker {
			// Nothing was recorded before it, or it is recorded already.
			return timeline, false
		}
	case slices.Contains(timeline, entry):
		return timeline, false
	}
	return append(timeline, entry), true
}

// recordedServerUUIDs returns the server_uuids of a timeline.
func recordedServerUUIDs(timeline []string) []string {
	out := make([]string, 0, len(timeline))
	for _, e := range timeline {
		if e != serverUUIDCloneMarker {
			out = append(out, e)
		}
	}
	return out
}

// liveServerUUIDs returns the server_uuids of a timeline that no clone has
// superseded, and current, the one the site runs under now: the
// identities whose transactions the site can only lack by having lost
// them.
//
// A clone makes the site a copy of its donor under a new server_uuid, and
// the site's receiver drops every later event of its earlier identities.
// So from the clone on, the site holds of those identities' transactions
// exactly what its donor held then: one that another site still holds and
// the site lacks was given up at the clone (the donor did not have it),
// not lost. A server_uuid that changed without a clone (a data directory
// replaced by hand) carries no such proof and stays live. current always
// counts: a site that runs under a server_uuid from before a clone (its
// volume came back from before it, or the clone never installed) can
// issue that identity's GTIDs again.
func liveServerUUIDs(timeline []string, current string) []string {
	start := 0
	for i, e := range timeline {
		if e == serverUUIDCloneMarker {
			start = i + 1
		}
	}
	live := recordedServerUUIDs(timeline[start:])
	if current != "" && !slices.Contains(live, current) {
		live = append(live, current)
	}
	return live
}

// siteIdentityTracker is the manager's view of the recorded server_uuids.
type siteIdentityTracker struct {
	mu     sync.Mutex
	store  SiteIdentityStore
	loaded bool
	// timeline is what is known per site (stored or seen by this
	// manager), oldest first: server_uuids and clone markers.
	timeline map[string][]string
	// unsaved are entries seen but not stored yet, in order.
	unsaved map[string][]string
	// nextTry rate-limits store access after a failure.
	nextTry time.Time
	// loading is true while Poll's background load runs.
	loading bool
}

// SetSiteIdentityStore wires the durable store of the sites' server_uuids.
func (tm *TopologyManager) SetSiteIdentityStore(s SiteIdentityStore) {
	t := &tm.siteIdentity
	t.mu.Lock()
	t.store = s
	t.loaded = false
	t.mu.Unlock()
}

// LoadSiteIdentities reads the stored timelines now, unless they are
// loaded already. A promotion decision does not wait on the store
// (knownSiteServerUUIDs), so a manager that starts into an outage must
// know the sites' earlier server_uuids before its first poll: the runner
// calls this when it starts a manager. A failure is logged and returned;
// Poll then retries in the background (loadSiteIdentitiesInBackground),
// and until it succeeds comparisons run under the uuids this manager has
// seen.
func (tm *TopologyManager) LoadSiteIdentities(ctx context.Context) error {
	t := &tm.siteIdentity
	t.mu.Lock()
	store, loaded := t.store, t.loaded
	t.mu.Unlock()
	if store == nil || loaded {
		return nil
	}
	tm.syncSiteIdentities(ctx, store, true)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.loaded {
		return errors.New("the server_uuid history could not be loaded")
	}
	return nil
}

// loadSiteIdentitiesInBackground retries a load that failed when the
// manager started, off the poll loop: a slow API server must not hold a
// failure decision.
func (tm *TopologyManager) loadSiteIdentitiesInBackground(ctx context.Context) {
	t := &tm.siteIdentity
	t.mu.Lock()
	start := t.store != nil && !t.loaded && !t.loading && !tm.clock.Now().Before(t.nextTry)
	if start {
		t.loading = true
	}
	t.mu.Unlock()
	if !start {
		return
	}
	go func() {
		_ = tm.LoadSiteIdentities(ctx)
		t.mu.Lock()
		t.loading = false
		t.mu.Unlock()
	}()
}

func normalizeServerUUID(uuid string) string {
	return strings.ToLower(strings.TrimSpace(uuid))
}

// rememberLocked appends entry to site's timeline in memory and queues it
// for the store. Caller holds t.mu.
func (t *siteIdentityTracker) rememberLocked(site, entry string) {
	if t.timeline == nil {
		t.timeline = map[string][]string{}
	}
	if t.unsaved == nil {
		t.unsaved = map[string][]string{}
	}
	if next, changed := appendServerUUIDEntry(t.timeline[site], entry); changed {
		t.timeline[site] = next
		t.unsaved[site] = append(t.unsaved[site], entry)
	}
}

// noteSiteEntry records entry (a server_uuid site runs or ran under, or
// the clone marker) and brings the store up to date. The store is read
// once per manager and written when an entry is new; a failed read or
// write is retried on a later call and never fails the caller.
func (tm *TopologyManager) noteSiteEntry(ctx context.Context, site, entry string) {
	t := &tm.siteIdentity
	now := tm.clock.Now()

	t.mu.Lock()
	t.rememberLocked(site, entry)
	store := t.store
	needLoad := store != nil && !t.loaded
	pending := len(t.unsaved) > 0
	due := !now.Before(t.nextTry)
	t.mu.Unlock()

	if store != nil && due && (needLoad || pending) {
		tm.syncSiteIdentities(ctx, store, needLoad)
	}
}

// noteSiteServerUUID records that site runs (or ran) under serverUUID and
// returns every server_uuid known for the site, serverUUID included: the
// identities a comparison with the primary runs under. The caller compares
// under the uuids known so far when the store cannot be reached.
func (tm *TopologyManager) noteSiteServerUUID(ctx context.Context, site, serverUUID string) []string {
	tm.noteSiteEntry(ctx, site, normalizeServerUUID(serverUUID))
	t := &tm.siteIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	return recordedServerUUIDs(t.timeline[site])
}

// markSiteCloned records that a clone into site was issued: the
// server_uuids recorded so far are superseded once it installs
// (liveServerUUIDs). The bootstrap writes the marker with the clone
// identity record, in the write the clone waits for
// (groupCloneIdentityStore.Record); this is for a manager without that
// record (a test, a simulation) and stores it best effort.
func (tm *TopologyManager) markSiteCloned(ctx context.Context, site string) {
	tm.noteSiteEntry(ctx, site, serverUUIDCloneMarker)
}

// syncSiteIdentities loads the stored timelines (once) and stores the
// entries seen since.
func (tm *TopologyManager) syncSiteIdentities(ctx context.Context, store SiteIdentityStore, load bool) {
	t := &tm.siteIdentity
	fail := func(what string, err error) {
		t.mu.Lock()
		t.nextTry = tm.clock.Now().Add(siteIdentityRetryInterval)
		t.mu.Unlock()
		tm.logger.Warn("server_uuid history not "+what, "fg", tm.cfg.Name, "error", err)
	}
	if load {
		loadCtx, cancel := context.WithTimeout(ctx, siteIdentityWriteTimeout)
		stored, err := store.Load(loadCtx)
		cancel()
		if err != nil {
			fail("loaded", err)
			return
		}
		t.mu.Lock()
		if !t.loaded {
			t.loaded = true
			if t.timeline == nil {
				t.timeline = map[string][]string{}
			}
			for site, entries := range stored {
				// What is stored is older than anything this manager saw.
				var merged []string
				for _, e := range entries {
					merged, _ = appendServerUUIDEntry(merged, normalizeServerUUID(e))
				}
				var still []string
				for _, e := range t.timeline[site] {
					var added bool
					if merged, added = appendServerUUIDEntry(merged, e); added && slices.Contains(t.unsaved[site], e) {
						// Not stored yet: still to write.
						still = append(still, e)
					}
				}
				t.timeline[site] = merged
				if len(still) == 0 {
					delete(t.unsaved, site)
				} else {
					t.unsaved[site] = still
				}
			}
		}
		t.mu.Unlock()
	}
	t.mu.Lock()
	sites := make([]string, 0, len(t.unsaved))
	for site := range t.unsaved {
		sites = append(sites, site)
	}
	slices.Sort(sites)
	type entry struct{ site, value string }
	var todo []entry
	for _, site := range sites {
		for _, e := range t.unsaved[site] {
			todo = append(todo, entry{site, e})
		}
	}
	t.mu.Unlock()
	for _, e := range todo {
		addCtx, cancel := context.WithTimeout(ctx, siteIdentityWriteTimeout)
		err := store.Add(addCtx, e.site, e.value)
		cancel()
		if err != nil {
			fail("stored", err)
			return
		}
		t.siteEntryStored(e.site, e.value)
	}
}

// siteEntryStored drops entry from the entries of site still to store.
func (t *siteIdentityTracker) siteEntryStored(site, entry string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i := slices.Index(t.unsaved[site], entry); i >= 0 {
		t.unsaved[site] = slices.Delete(t.unsaved[site], i, i+1)
		if len(t.unsaved[site]) == 0 {
			delete(t.unsaved, site)
		}
	}
}

// siteCloneRecorded notes that the clone identity record's write stored
// serverUUID (when known) and the clone marker in site's timeline, so the
// tracker holds both and does not write them again.
func (tm *TopologyManager) siteCloneRecorded(site, serverUUID string) {
	t := &tm.siteIdentity
	for _, entry := range []string{normalizeServerUUID(serverUUID), serverUUIDCloneMarker} {
		t.mu.Lock()
		t.rememberLocked(site, entry)
		t.mu.Unlock()
		t.siteEntryStored(site, entry)
	}
}

// readSiteGTIDSnapshot reads a site's server_uuid with its executed GTID
// set. identityKnown is false for a checker that cannot read a server's
// identity (a test double): the set is then read alone and the site is
// not compared for transactions of its own.
func readSiteGTIDSnapshot(ctx context.Context, c mysql.Checker) (snap mysql.GTIDSnapshot, identityKnown bool, err error) {
	if r, ok := c.(gtidSnapshotReader); ok {
		snap, err = r.ReadGTIDSnapshot(ctx)
		return snap, err == nil && snap.ServerUUID != "", err
	}
	raw, err := c.GetGtidExecuted(ctx)
	return mysql.GTIDSnapshot{GtidExecuted: raw}, false, err
}

// ownLossLocked reports whether site is known to lack transactions it
// issued. Caller holds tm.mu.
func (tm *TopologyManager) ownLossLocked(site string) bool {
	return tm.ownLoss[site] != nil
}

// ownLossSite is ownLossLocked for callers that do not hold tm.mu.
func (tm *TopologyManager) ownLossSite(site string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.ownLossLocked(site)
}

// ownLossHoleLocked reports whether site is known to lack transactions it
// issued that the primary holds (or a report restored from status says
// so): replication cannot fill that hole, so its replication is not
// started. A witness report is not one: the site holds everything the
// primary does. Caller holds tm.mu.
func (tm *TopologyManager) ownLossHoleLocked(site string) bool {
	rec := tm.ownLoss[site]
	return rec != nil && !rec.witness
}

// ownLossHoleSite is ownLossHoleLocked for callers that do not hold tm.mu.
func (tm *TopologyManager) ownLossHoleSite(site string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.ownLossHoleLocked(site)
}

// settleOwnLossLocked publishes one comparison of site with comparedTo:
// lost is what the site lacks of its own transactions that comparedTo
// holds. witness is false when comparedTo is the confirmed primary, true
// when it is another site (see ownLossRecord.witness).
//
// A non-empty set records (or refreshes) the report; an empty one is the
// proof that clears it, for the kind of report the comparison is about:
// a clean comparison with the primary says nothing about what a witness
// holds, and a witness that holds nothing more does not fill a hole
// against the primary. A report against the primary is the stronger one
// and is kept over a witness's.
//
// It reports whether the report changed, and returns the log call to make
// once tm.mu is released (nil when there is nothing to say). Caller holds
// tm.mu.
func (tm *TopologyManager) settleOwnLossLocked(site string, lost mysql.GTIDSet, comparedTo string, detectedBy v1alpha1.DivergenceDetector, uuids []string, witness bool) (changed bool, log func()) {
	rec := tm.ownLoss[site]
	if lost.IsEmpty() {
		if rec == nil || rec.witness != witness || (witness && rec.comparedTo != comparedTo) {
			return false, nil
		}
		delete(tm.ownLoss, site)
		tm.forgetOwnLossRefusalLocked(site)
		metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
		msg := "own transactions lost cleared: the site holds every transaction it issued"
		why := fmt.Sprintf("it holds every transaction it issued that %s holds", comparedTo)
		switch {
		case witness:
			msg = "own transactions lost cleared: the witness no longer holds transactions the site lacks"
			why = fmt.Sprintf("%s no longer holds transactions it issued and lacks", comparedTo)
		case rec.comparedTo != "" && rec.comparedTo != comparedTo:
			// The report named another holder: nothing was repaired, the
			// primary it is compared with now never had the transactions.
			why = fmt.Sprintf("the report was made against %s; the primary is %s now, which does not hold those transactions, so the site lacks nothing it holds",
				rec.comparedTo, comparedTo)
		}
		tm.noteOwnLossClearedLocked(site, why)
		return true, func() {
			tm.logger.Info(msg, "site", site, "comparedTo", comparedTo, "reason", why)
		}
	}
	if rec != nil && !rec.witness && rec.detectedBy != "" && witness {
		// A confirmed hole against the primary stands; a witness adds
		// nothing to it.
		return false, nil
	}
	set, count := lost.String(), lost.TransactionCount()
	if rec != nil && rec.lostGtid == set && rec.detectedBy != "" && rec.witness == witness {
		rec.comparedTo = comparedTo
		return false, nil
	}
	if tm.ownLoss == nil {
		tm.ownLoss = map[string]*ownLossRecord{}
	}
	if rec == nil {
		rec = &ownLossRecord{}
		tm.ownLoss[site] = rec
	}
	delete(tm.ownLossCleared, site)
	sameSet := rec.lostGtid == set
	rec.lostGtid, rec.lostCount = set, count
	rec.comparedTo, rec.detectedBy = comparedTo, detectedBy
	rec.witness = witness
	// A routine rejoin that was under way is void: a site with a hole
	// against the primary is not rejoined. A divergence block stays; it
	// outranks this report.
	if r := tm.recovery[site]; !witness && r != nil && r.state == v1alpha1.RecoveryStateInProgress {
		r.state = ""
	}
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(float64(count))
	if sameSet {
		// A restored report confirmed by a fresh comparison: recorded,
		// not announced again.
		return true, nil
	}
	if witness {
		remedy := fmt.Sprintf("the site is not promoted while %s holds these transactions, which the primary does not have: reclone %s to discard them or make the primary contain them, or reclone this site (kubectl bloodraven reclone %s %s --cold)",
			comparedTo, comparedTo, tm.cfg.Name, site)
		return true, func() {
			tm.logger.Warn("own transactions lost",
				"site", site,
				"lostTransactions", count,
				"lostGtid", set,
				"comparedTo", comparedTo,
				"serverUUIDs", strings.Join(uuids, ","),
				"detectedBy", detectedBy,
				"remedy", remedy)
		}
	}
	// A site without a divergent GTID is recloned by hand with --cold; one
	// that also holds transactions the primary lacks is confirmed with
	// the prefix of that set, which the plugin reads from status.
	recloneCmd := fmt.Sprintf("kubectl bloodraven reclone %s %s --cold", tm.cfg.Name, site)
	remedy := "the site is not rejoined by replication and not promoted; the operator reclones it, or run: " + recloneCmd
	if r := tm.recovery[site]; r != nil && r.state == v1alpha1.RecoveryStateBlocked {
		remedy = fmt.Sprintf("the site also holds transactions the primary lacks, so it is not recloned automatically; decide about those, then run: kubectl bloodraven reclone %s %s", tm.cfg.Name, site)
	}
	return true, func() {
		tm.logger.Warn("own transactions lost",
			"site", site,
			"lostTransactions", count,
			"lostGtid", set,
			"comparedTo", comparedTo,
			"serverUUIDs", strings.Join(uuids, ","),
			"detectedBy", detectedBy,
			"remedy", remedy)
	}
}

// SetOwnTransactionsLost restores a persisted report from CR status after
// an operator restart. The restored report keeps the site out of
// replication and promotion at once; the next comparison with a confirmed
// primary confirms or clears it.
func (tm *TopologyManager) SetOwnTransactionsLost(site, lostGtid string, lostCount int64) {
	if lostGtid == "" {
		return
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.ownLoss == nil {
		tm.ownLoss = map[string]*ownLossRecord{}
	}
	tm.ownLoss[site] = &ownLossRecord{lostGtid: lostGtid, lostCount: lostCount}
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(float64(lostCount))
}

// clearOwnLossAfterClone drops site's report: a completed clone made the
// site a copy of its donor.
func (tm *TopologyManager) clearOwnLossAfterClone(site string) {
	tm.mu.Lock()
	rec := tm.ownLoss[site]
	delete(tm.ownLoss, site)
	tm.forgetOwnLossRefusalLocked(site)
	if rec != nil {
		tm.noteOwnLossClearedLocked(site, "it was recloned")
	}
	// A report that rests on this site as its witness is reviewed now: the
	// clone replaced what it held.
	for _, other := range tm.ownLoss {
		if other.witness && other.comparedTo == site {
			other.reviewAfter = time.Time{}
		}
	}
	tm.mu.Unlock()
	if rec == nil {
		return
	}
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
	tm.logger.Info("own transactions lost cleared: the site was recloned", "site", site)
	tm.RequestStatusRefresh()
}

// clearRestoredOwnLoss drops site's report when it is still the one
// restored from status (no comparison of this manager has confirmed it):
// the primary re-assert read every other site, all of them read-only, and
// none holds transactions the site issued, under a server_uuid no clone
// has superseded, and lacks. It reports whether a report was dropped.
//
// What the restored report rested on is not known. If it was a hole
// against a primary under a server_uuid the site has since given up at a
// clone, that site is divergent, not this one short, and the first
// comparison with a confirmed primary reports whichever of the two it
// finds again.
func (tm *TopologyManager) clearRestoredOwnLoss(site string) bool {
	const why = "no other site holds transactions it issued and lacks"
	tm.mu.Lock()
	rec := tm.ownLoss[site]
	if rec == nil || rec.witness || rec.detectedBy != "" {
		tm.mu.Unlock()
		return false
	}
	delete(tm.ownLoss, site)
	tm.forgetOwnLossRefusalLocked(site)
	tm.noteOwnLossClearedLocked(site, why)
	tm.mu.Unlock()
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
	tm.logger.Info("own transactions lost cleared: no site holds transactions the site lacks", "site", site, "reason", why)
	return true
}

// refuseRejoinWithoutOwnTransactions compares site, whose replication an
// ordered update is about to start from source (a follower it restarted,
// or the old active it hands off from), with source, and returns
// errOwnTransactionsLost when the site lacks transactions it issued: its
// pod was just replaced, and a volume that came back older would otherwise
// be rejoined and look healthy until the audit ran, after the update. The
// report is recorded, so the gate refuses every later start too. A site
// whose identity cannot be read is not compared, and a set that cannot be
// read refuses this start only (the update's wait tries again).
func (tm *TopologyManager) refuseRejoinWithoutOwnTransactions(ctx context.Context, site string, replica, source mysql.Checker) error {
	var snap mysql.GTIDSnapshot
	var identityKnown bool
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, identityKnown, err = readSiteGTIDSnapshot(probeCtx, replica)
		return err
	}); err != nil {
		return fmt.Errorf("read %s before starting its replication: %w", site, err)
	}
	if !identityKnown {
		return nil
	}
	siteSet, err := mysql.ParseGTIDSet(snap.GtidExecuted)
	if err != nil {
		return fmt.Errorf("parse the GTID set of %s before starting its replication: %w", site, err)
	}
	_, sourceSet, err := readGTIDSetRaw(ctx, source)
	if err != nil {
		return fmt.Errorf("read the source of %s before starting its replication: %w", site, err)
	}
	uuids := tm.noteSiteServerUUID(ctx, site, snap.ServerUUID)
	lost := ownLostTransactions(sourceSet, siteSet, uuids)
	if lost.IsEmpty() {
		// Nothing lost against this source. A standing report is left to
		// recovery and the audit, which compare with the confirmed
		// primary; the gate still refuses on it.
		return nil
	}
	tm.mu.Lock()
	comparedTo := tm.activeSiteLocked()
	for i := range tm.sites {
		if tm.sites[i].mysql == source {
			comparedTo = tm.sites[i].name
			break
		}
	}
	changed, lossLog := tm.settleOwnLossLocked(site, lost, comparedTo, divergenceDetectedByRecovery, uuids, false)
	tm.mu.Unlock()
	if lossLog != nil {
		lossLog()
	}
	if changed {
		tm.RequestStatusRefresh()
	}
	return errOwnTransactionsLost
}

// wakeOwnLossChecks makes every follower due for its GTID audit and every
// witness report due for its review on the next pass.
func (tm *TopologyManager) wakeOwnLossChecks() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.gtidAudit = make(map[string]*gtidAuditSchedule)
	for _, rec := range tm.ownLoss {
		rec.reviewAfter = time.Time{}
	}
}

// crossCheckOwnLoss looks at a site found to hold transactions the primary
// lacks (extra, of witness's set witnessSet) for transactions another site
// issued: if that site no longer holds them, it lost them, and made
// writable it would issue their GTIDs again while the witness still holds
// the old ones. The site is recorded with a witness report, which keeps
// every promotion path away from it (the ordered update's handoff and a
// planned failover read the report; they compare with no witness of their
// own).
//
// The primary itself is not a subject: it is writable already. Sites are
// read only when extra names a server_uuid of theirs that no clone has
// superseded, so the usual divergence (a site's own unreplicated tail)
// costs no read. It reports whether a report changed.
func (tm *TopologyManager) crossCheckOwnLoss(ctx context.Context, witness *siteTracker, witnessSet, extra mysql.GTIDSet, active string) bool {
	if extra.IsEmpty() {
		return false
	}
	tm.mu.RLock()
	excluded := tm.ownLossWitnessExcludedLocked(witness.name)
	tm.mu.RUnlock()
	if excluded {
		return false
	}
	changed := false
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.name == witness.name || site.name == active || site.state != state.StateReadOnly {
			continue
		}
		if extra.OriginatedBy(tm.liveSiteServerUUIDs(site.name, "")...).IsEmpty() {
			continue
		}
		if tm.compareWithWitness(ctx, site, witness.name, witnessSet) {
			changed = true
		}
	}
	return changed
}

// compareWithWitness reads site and settles its witness report against
// witness, whose set is witnessSet. A site that cannot be read keeps what
// is recorded. It reports whether the report changed.
func (tm *TopologyManager) compareWithWitness(ctx context.Context, site *siteTracker, witness string, witnessSet mysql.GTIDSet) bool {
	var snap mysql.GTIDSnapshot
	var identityKnown bool
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, identityKnown, err = readSiteGTIDSnapshot(probeCtx, site.mysql)
		return err
	}); err != nil || !identityKnown {
		return false
	}
	siteSet, err := mysql.ParseGTIDSet(snap.GtidExecuted)
	if err != nil {
		return false
	}
	live := tm.liveSiteServerUUIDs(site.name, snap.ServerUUID)
	uuids := tm.noteSiteServerUUID(ctx, site.name, snap.ServerUUID)
	lost := ownLostOnWitness(witnessSet, siteSet, live)
	tm.mu.Lock()
	changed, lossLog := tm.settleOwnLossLocked(site.name, lost, witness, divergenceDetectedByAudit, uuids, true)
	if rec := tm.ownLoss[site.name]; rec != nil && rec.witness {
		rec.reviewAfter = tm.clock.Now().Add(gtidAuditInterval)
	}
	tm.mu.Unlock()
	if lossLog != nil {
		lossLog()
	}
	if changed {
		tm.RequestStatusRefresh()
	}
	return changed
}

// reviewWitnessOwnLoss compares every site that carries a witness report
// with its witness again, once per gtidAuditInterval: the report ends when
// the witness no longer holds transactions the site lacks (it was
// recloned, or its volume was replaced), when the witness left the group,
// or when an in-place restore queued the witness for its reclone. A
// witness that does not answer proves nothing, and the report stays: a
// site is not made promotable again on silence. It runs with or without a
// primary, and only reads.
func (tm *TopologyManager) reviewWitnessOwnLoss(ctx context.Context) bool {
	type review struct{ site, witness string }
	now := tm.clock.Now()
	tm.mu.Lock()
	var due []review
	for name, rec := range tm.ownLoss {
		if rec.witness && !now.Before(rec.reviewAfter) {
			rec.reviewAfter = now.Add(gtidAuditInterval)
			due = append(due, review{name, rec.comparedTo})
		}
	}
	tm.mu.Unlock()
	slices.SortFunc(due, func(a, b review) int { return strings.Compare(a.site, b.site) })
	changed := false
	for _, r := range due {
		site, witness := tm.getSite(r.site), tm.getSite(r.witness)
		if site == nil {
			continue
		}
		tm.mu.RLock()
		void := witness == nil || tm.ownLossWitnessExcludedLocked(r.witness)
		tm.mu.RUnlock()
		var witnessSet mysql.GTIDSet
		if !void {
			var err error
			if _, witnessSet, err = readGTIDSetRaw(ctx, witness.mysql); err != nil {
				continue
			}
		}
		// A void witness holds nothing that counts: an empty set clears.
		if tm.compareWithWitness(ctx, site, r.witness, witnessSet) {
			changed = true
		}
	}
	return changed
}

// noteOwnLossClearedLocked records why site's report ended, for the
// OwnTransactionsLostResolved Event. Caller holds tm.mu.
func (tm *TopologyManager) noteOwnLossClearedLocked(site, why string) {
	if tm.ownLossCleared == nil {
		tm.ownLossCleared = map[string]string{}
	}
	tm.ownLossCleared[site] = why
}

// ownLossCandidates returns the read-only primary-candidates automatic
// promotion skips because they lack transactions they issued. Caller must
// not hold tm.mu.
func (tm *TopologyManager) ownLossCandidates() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var out []string
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.state == state.StateReadOnly && site.isPromotable() && tm.ownLossLocked(site.name) {
			out = append(out, site.name)
		}
	}
	return out
}

// forgetOwnLossRefusalLocked drops site from the recorded promotion
// refusals once its report is gone, so a later refusal of the same site
// is announced again. Caller holds tm.mu.
func (tm *TopologyManager) forgetOwnLossRefusalLocked(site string) {
	if i := slices.Index(tm.ownLossPromotionRefused, site); i >= 0 {
		tm.ownLossPromotionRefused = slices.Delete(slices.Clone(tm.ownLossPromotionRefused), i, i+1)
	}
}

// recordOwnLossPromotionRefused records the candidates the last promotion
// decision refused for lost own transactions (nil when it refused none),
// for the runner's Warning event.
func (tm *TopologyManager) recordOwnLossPromotionRefused(sites []string) {
	tm.mu.Lock()
	tm.ownLossPromotionRefused = append([]string(nil), sites...)
	tm.mu.Unlock()
}

// refuseOwnLossPromotion logs and records the refusal to promote sites.
func (tm *TopologyManager) refuseOwnLossPromotion(sites []string) {
	tm.mu.RLock()
	same := stringSlicesEqual(tm.ownLossPromotionRefused, sites)
	tm.mu.RUnlock()
	if !same {
		// The refusal stands for as long as the report does and is
		// re-decided on every poll: log it when it changes.
		tm.logger.Warn("promotion refused: candidate lacks transactions it issued itself and would issue their GTIDs again",
			"sites", sites)
	}
	tm.recordOwnLossPromotionRefused(sites)
}

// requestOwnLossReclone asks for the reclone of one site that lacks its
// own transactions, through the same request an administrator's reclone
// annotation makes (checkReclone: the donor check, the pre-reclone archive
// tail wait, the clone identity record, the keyring hold). It also drops
// the report of a site that became the primary.
//
// The operator's own request never outlives the poll that made it: every
// poll first withdraws a request whose clone has not started and then
// makes it again only if nothing forbids it now. A report that cleared, a
// divergence block recorded since, a hold that began (an ordered update, a
// planned failover, a promotion not yet observed) or a primary that
// changed therefore leave no request behind to run later. The request
// alone starts nothing destructive: checkReclone and the bootstrap
// goroutine both run verifyOwnLossReclone before the clone.
//
// It is made only for a report a check of this manager made against the
// current primary (not one restored from status, and not one a promotion
// decision made against a witness): recovery or the audit compares the
// site with the primary first. One request at a time, and never beside
// another reclone request. A site that also holds transactions the primary
// lacks is left to its RecoveryBlocked report: an administrator decides
// about those transactions, and the reclone they then ask for repairs
// both.
func (tm *TopologyManager) requestOwnLossReclone() {
	now := tm.clock.Now()
	tm.mu.Lock()
	withdrawn := ""
	if site := tm.reclonePendingSite; site != "" && tm.reclonePendingOwner == recloneOwnerOwnLoss {
		started := tm.bootstrapAdmitting == site || (tm.bootstrapRecipient == site && !bootstrapIdlePhase(tm.bootstrapPhase))
		if !started {
			tm.reclonePendingSite, tm.reclonePendingOwner = "", ""
			withdrawn = site
		}
	}
	if len(tm.ownLoss) == 0 {
		tm.mu.Unlock()
		if withdrawn != "" {
			tm.logger.Info("automatic reclone request withdrawn", "site", withdrawn, "reason", "the report was cleared")
		}
		return
	}
	active := tm.activeSiteLocked()
	changed := false
	var logs []func()
	setWaiting := func(rec *ownLossRecord, why string) {
		if rec.recloneWaiting != why {
			rec.recloneWaiting = why
			changed = true
		}
	}
	requested := false
	for i := range tm.sites {
		name := tm.sites[i].name
		rec := tm.ownLoss[name]
		if rec == nil {
			continue
		}
		if tm.sites[i].state == state.StateWritable && active == name {
			// Made writable without a reclone (the operator does not
			// promote it): the report no longer describes a replica to
			// repair. The runner records the promotion from the status
			// change.
			delete(tm.ownLoss, name)
			tm.forgetOwnLossRefusalLocked(name)
			metrics.OwnTransactionsLost.WithLabelValues(name).Set(0)
			lost, count := rec.lostGtid, rec.lostCount
			logs = append(logs, func() {
				tm.logger.Error("site that lacks transactions it issued itself is now the primary; unless its server_uuid was changed it issues their GTIDs again",
					"site", name, "lostTransactions", count, "lostGtid", truncateGtidSet(lost))
			})
			changed = true
			continue
		}
		pendingHere := tm.reclonePendingSite == name
		cloningHere := tm.bootstrapRecipient == name && !bootstrapIdlePhase(tm.bootstrapPhase)
		switch {
		case pendingHere || cloningHere:
			setWaiting(rec, "")
		case requested:
			setWaiting(rec, "another site is recloned first")
		case tm.recovery[name] != nil && tm.recovery[name].state == v1alpha1.RecoveryStateBlocked:
			setWaiting(rec, "the site also holds transactions the primary lacks; an administrator decides about them and reclones it")
		case rec.witness:
			// Nothing to repair against the primary: a clone would only
			// change the site's server_uuid. The report ends with the
			// witness's own repair.
			setWaiting(rec, fmt.Sprintf("the transactions are held by %s only, not by the primary, so the site is not recloned automatically; it is not promoted until %s is recloned or the primary contains them",
				lostHeldBy(rec.comparedTo), lostHeldBy(rec.comparedTo)))
		case tm.bootstrap == nil || tm.bootstrapCfg.ReplUser == "":
			setWaiting(rec, "replication credentials are not configured, so the operator cannot clone")
		case tm.topologyFrozen || slices.Contains(tm.restoreRecloneBlocked, name) || tm.restoreReclonePendingLocked(name):
			setWaiting(rec, "an in-place restore holds the site; its reclone queue reclones it")
		case tm.autoBootstrapSuppressed:
			setWaiting(rec, "an initFromBackup restore has not succeeded; nothing is cloned until it has")
		case tm.reclonePendingSite != "" || tm.recloneCompletedSite != "":
			setWaiting(rec, "another reclone is pending")
		case !bootstrapIdlePhase(tm.bootstrapPhase):
			setWaiting(rec, "another clone is running")
		case active == "" || active == name:
			setWaiting(rec, "there is no confirmed writable primary to clone from")
		case tm.followerMutationSuppressedLocked():
			// The hold that stops recovery, the audit and source
			// convergence: while it lasts nothing compares the site, so
			// nothing may be requested on what was compared before it.
			setWaiting(rec, "another topology change holds the sites (an ordered update, a planned failover or its hold, or a promotion that is not observed yet)")
		case tm.sites[i].state != state.StateReadOnly:
			setWaiting(rec, "the site is not reachable")
		case rec.detectedBy == "":
			setWaiting(rec, "the report was restored from status and has not been compared with the primary again yet")
		case rec.comparedTo != active:
			setWaiting(rec, fmt.Sprintf("the report was made against %s; the site has not been compared with the primary %s yet", lostHeldBy(rec.comparedTo), active))
		case now.Before(rec.recloneRetryAfter):
			setWaiting(rec, fmt.Sprintf("%s; the next one is requested at %s",
				ownLossRecloneFailuresText(rec.recloneFailures), rec.recloneRetryAfter.UTC().Format(time.RFC3339)))
		default:
			tm.reclonePendingSite, tm.reclonePendingOwner = name, recloneOwnerOwnLoss
			// A refusal by the check before the clone stays visible until
			// that check passes.
			setWaiting(rec, rec.recloneRefusal)
			requested = true
			if !rec.recloneAnnounced {
				rec.recloneAnnounced = true
				lost, count, primary := rec.lostGtid, rec.lostCount, active
				logs = append(logs, func() {
					tm.logger.Info("reclone requested for a site that lacks transactions it issued itself",
						"site", name, "donor", primary, "lostTransactions", count, "lostGtid", truncateGtidSet(lost))
				})
			}
		}
		if pendingHere || cloningHere {
			requested = true
		}
	}
	if withdrawn != "" && tm.reclonePendingSite != withdrawn {
		why := "the report was cleared"
		if rec := tm.ownLoss[withdrawn]; rec != nil {
			why = rec.recloneWaiting
			rec.recloneAnnounced = false
		}
		logs = append(logs, func() {
			tm.logger.Info("automatic reclone request withdrawn", "site", withdrawn, "reason", why)
		})
	}
	tm.mu.Unlock()
	for _, log := range logs {
		log()
	}
	if changed {
		tm.RequestStatusRefresh()
	}
}

// ownLossRecloneFailuresText says how many automatic reclones failed in a
// row.
func ownLossRecloneFailuresText(failures int) string {
	if failures <= 1 {
		return "the last automatic reclone did not complete"
	}
	return fmt.Sprintf("the last %d automatic reclones did not complete", failures)
}

// noteOwnLossCloneFailedLocked counts a failed clone into site against its
// report and sets when the operator asks again. Caller holds tm.mu.
func (tm *TopologyManager) noteOwnLossCloneFailedLocked(site string) {
	rec := tm.ownLoss[site]
	if rec == nil {
		return
	}
	rec.recloneFailures++
	rec.recloneRetryAfter = tm.clock.Now().Add(ownLossRecloneBackoff(rec.recloneFailures))
	rec.recloneAnnounced = false
}

// carryOwnLossRecloneBackoff takes over the failed-reclone count and the
// retry time of the manager this one replaces, for the sites both hold a
// report for, so a spec change does not restart a failing clone at once.
// An operator restart does: the count is not stored.
func (tm *TopologyManager) carryOwnLossRecloneBackoff(prev *TopologyManager) {
	if prev == nil || prev == tm {
		return
	}
	type backoff struct {
		failures int
		after    time.Time
	}
	prev.mu.RLock()
	carried := make(map[string]backoff, len(prev.ownLoss))
	for site, rec := range prev.ownLoss {
		if rec.recloneFailures > 0 {
			carried[site] = backoff{rec.recloneFailures, rec.recloneRetryAfter}
		}
	}
	prev.mu.RUnlock()
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for site, b := range carried {
		if rec := tm.ownLoss[site]; rec != nil && rec.recloneFailures == 0 {
			rec.recloneFailures, rec.recloneRetryAfter = b.failures, b.after
		}
	}
}

// verifyOwnLossReclone decides, when the automatic reclone of recipient is
// about to start, whether the evidence for it holds now. It is the only
// thing the destructive step rests on; the report that led to the request
// is not consulted for anything but its existence.
//
// It fences the recipient (so no client can add to its set after the
// read), reads the recipient's server_uuid and executed set, then the
// donor's set (in that order: what the recipient applied from the donor
// between the reads is in the donor's set when it is read), and lets the
// clone run only if
//
//   - the donor is still the confirmed active primary under the authority
//     this check started with,
//   - the donor's set contains the recipient's: the clone discards nothing
//     the primary lacks, so a site that is divergent as well as lossy can
//     never be cloned over automatically, whatever was or was not recorded
//     about it, and
//   - the donor still holds transactions the recipient issued and lacks.
//
// Otherwise the request is dropped. A site found to hold transactions the
// primary lacks is handed to recovery and the audit, which record the
// RecoveryBlocked report on their next pass; a site found complete has
// its report cleared, which is the same proof the audit makes.
//
// What remains between this check and CLONE INSTANCE is the bootstrap's
// own preparation (the clone identity record, the keyring hold) on a
// recipient that is super_read_only. MySQL has no conditional CLONE, so a
// writer that lifts the fence in that time is not seen.
func (tm *TopologyManager) verifyOwnLossReclone(ctx context.Context, donor, recipient *siteTracker) bool {
	site := recipient.name
	refuse := func(why string, attrs ...any) bool {
		tm.mu.Lock()
		running := tm.bootstrapRecipient == site && !bootstrapIdlePhase(tm.bootstrapPhase)
		if tm.reclonePendingSite == site && tm.reclonePendingOwner == recloneOwnerOwnLoss && !running {
			tm.reclonePendingSite, tm.reclonePendingOwner = "", ""
		}
		rec := tm.ownLoss[site]
		first, changed := rec == nil, false
		if rec != nil {
			first = rec.recloneRefusal != why
			rec.recloneRefusal = why
			if rec.recloneWaiting != why {
				rec.recloneWaiting = why
				changed = true
			}
		}
		tm.mu.Unlock()
		if first {
			tm.logger.Warn("automatic reclone not started: the check before the clone does not allow it",
				append([]any{"site", site, "donor", donor.name, "reason", why}, attrs...)...)
		}
		if changed {
			tm.RequestStatusRefresh()
		}
		return false
	}

	tm.mu.RLock()
	rec := tm.ownLoss[site]
	blocked := tm.recovery[site] != nil && tm.recovery[site].state == v1alpha1.RecoveryStateBlocked
	epoch := tm.authorityEpoch
	detectedBy := divergenceDetectedByRecovery
	if rec != nil && rec.detectedBy != "" {
		detectedBy = rec.detectedBy
	}
	tm.mu.RUnlock()
	if rec == nil {
		return refuse("the report was cleared")
	}
	if blocked {
		return refuse("the site also holds transactions the primary lacks; an administrator decides about them and reclones it")
	}
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		return recipient.mysql.SetSuperReadOnly(probeCtx, true)
	}); err != nil {
		return refuse("the site could not be fenced before the clone", "error", err)
	}
	var snap mysql.GTIDSnapshot
	var identityKnown bool
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, identityKnown, err = readSiteGTIDSnapshot(probeCtx, recipient.mysql)
		return err
	}); err != nil {
		return refuse("the site's GTID set could not be read before the clone", "error", err)
	}
	if !identityKnown {
		return refuse("the site's server_uuid could not be read before the clone")
	}
	var donorRaw string
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		donorRaw, err = donor.mysql.GetGtidExecuted(probeCtx)
		return err
	}); err != nil {
		return refuse("the primary's GTID set could not be read before the clone", "error", err)
	}
	siteSet, err := mysql.ParseGTIDSet(snap.GtidExecuted)
	if err != nil {
		return refuse("the site's GTID set could not be parsed before the clone", "error", err)
	}
	donorSet, err := mysql.ParseGTIDSet(donorRaw)
	if err != nil {
		return refuse("the primary's GTID set could not be parsed before the clone", "error", err)
	}
	uuids := tm.noteSiteServerUUID(ctx, site, snap.ServerUUID)
	if extra := siteSet.Subtract(donorSet); !extra.IsEmpty() {
		// Recovery (a site without a channel) or the audit (a site with
		// one) records the block; both are made due now.
		tm.mu.Lock()
		delete(tm.gtidAudit, site)
		if r := tm.recovery[site]; r != nil {
			r.retryAfter = time.Time{}
		}
		tm.mu.Unlock()
		return refuse("the site holds transactions the primary lacks, so it is not cloned over; an administrator decides about them and reclones it",
			"extraTransactions", extra.TransactionCount(), "extraGtid", truncateGtidSet(extra.String()))
	}
	lost := ownLostTransactions(donorSet, siteSet, uuids)
	tm.mu.Lock()
	if tm.authorityEpoch != epoch || tm.activeSiteLocked() != donor.name {
		tm.mu.Unlock()
		return refuse("the primary changed while the site was compared with it")
	}
	changed, lossLog := tm.settleOwnLossLocked(site, lost, donor.name, detectedBy, uuids, false)
	if rec := tm.ownLoss[site]; rec != nil && rec.recloneRefusal != "" {
		rec.recloneRefusal, rec.recloneWaiting = "", ""
		changed = true
	}
	tm.mu.Unlock()
	if lossLog != nil {
		lossLog()
	}
	if changed {
		tm.RequestStatusRefresh()
	}
	if lost.IsEmpty() {
		// The primary holds nothing the site issued and lacks, so there is
		// nothing to clone it for. settleOwnLossLocked dropped a report
		// that was made against the primary; one that rests on a witness
		// stays, and is not a reason to clone.
		return refuse("the primary holds no transaction the site issued and lacks")
	}
	return true
}

// knownSiteServerUUIDs is noteSiteServerUUID without access to the store,
// for the promotion path, which must not wait on the API server: the uuid
// is remembered and stored by the next call that may.
func (tm *TopologyManager) knownSiteServerUUIDs(site, serverUUID string) []string {
	t := &tm.siteIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rememberLocked(site, normalizeServerUUID(serverUUID))
	return recordedServerUUIDs(t.timeline[site])
}

// liveSiteServerUUIDs returns the server_uuids under which a witness's set
// is evidence against site, which runs under serverUUID now
// (liveServerUUIDs). Like knownSiteServerUUIDs it does not wait on the
// store.
func (tm *TopologyManager) liveSiteServerUUIDs(site, serverUUID string) []string {
	serverUUID = normalizeServerUUID(serverUUID)
	t := &tm.siteIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rememberLocked(site, serverUUID)
	return liveServerUUIDs(t.timeline[site], serverUUID)
}

// readOwnLossWitnesses starts a read of the executed GTID set of every
// site that is not a promotion candidate, whatever the last poll saw of
// it, and returns a function that hands over the sets that have arrived.
//
// With the primary gone there is no primary to compare a candidate with,
// but any site that answers is a witness: whatever it holds under the
// server_uuid a candidate runs under the candidate issued. That includes
// the failed primary itself when it answers again in the poll that
// replaces it. A site that awaits the reclone of an in-place restore is
// not read: its data is void (ownLossWitnessExcludedLocked).
//
// A witness never decides how long the promotion takes: the returned
// function waits for the reads still outstanding for ownLossWitnessGrace
// at most, after the candidates' own reads have ended. A site that
// answers does so in milliseconds; a blackholed one costs that grace once
// and is then not a witness. Each read ends by itself after five seconds.
func (tm *TopologyManager) readOwnLossWitnesses(ctx context.Context, candidates []string) func() map[string]mysql.GTIDSet {
	out := make(map[string]mysql.GTIDSet)
	var mu sync.Mutex
	type witness struct {
		name    string
		checker mysql.Checker
	}
	var todo []witness
	tm.mu.RLock()
	for i := range tm.sites {
		if site := &tm.sites[i]; !slices.Contains(candidates, site.name) && !tm.ownLossWitnessExcludedLocked(site.name) {
			todo = append(todo, witness{site.name, site.mysql})
		}
	}
	tm.mu.RUnlock()
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, w := range todo {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			raw, err := w.checker.GetGtidExecuted(queryCtx)
			if err != nil {
				// No witness: the candidate is judged on the sites that
				// answer.
				return
			}
			set, err := mysql.ParseGTIDSet(raw)
			if err != nil {
				return
			}
			mu.Lock()
			out[w.name] = set
			mu.Unlock()
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return func() map[string]mysql.GTIDSet {
		// On the manager's clock, like every other wait of the decision:
		// a simulation whose clock stands still during a poll then waits
		// for the reads themselves, which return at once there.
		timer := tm.clock.NewTimer(ownLossWitnessGrace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C():
		}
		mu.Lock()
		defer mu.Unlock()
		got := make(map[string]mysql.GTIDSet, len(out))
		for name, set := range out {
			got[name] = set
		}
		return got
	}
}

// ownLossWitnessExcludedLocked reports whether site's GTID set is no
// evidence about any other site: it awaits the reclone of a full-instance
// in-place restore (pending, held or blocked in status.restoreReclones).
// Caller holds tm.mu.
func (tm *TopologyManager) ownLossWitnessExcludedLocked(site string) bool {
	return tm.restoreReclonePendingLocked(site) || slices.Contains(tm.restoreRecloneBlocked, site)
}

// refuseOwnLossCandidate compares a promotion candidate with every other
// site's set read for this decision. It records the candidate's report
// and returns true when one of them holds transactions the candidate
// issued, under a server_uuid no clone has superseded, and lacks.
func (tm *TopologyManager) refuseOwnLossCandidate(name, serverUUID string, set mysql.GTIDSet, sets map[string]mysql.GTIDSet) bool {
	live := tm.liveSiteServerUUIDs(name, serverUUID)
	uuids := tm.knownSiteServerUUIDs(name, serverUUID)
	peers := make([]string, 0, len(sets))
	for peer := range sets {
		if peer != name {
			peers = append(peers, peer)
		}
	}
	slices.Sort(peers)
	for _, peer := range peers {
		lost := ownLostOnWitness(sets[peer], set, live)
		if lost.IsEmpty() {
			continue
		}
		tm.mu.Lock()
		if tm.ownLossWitnessExcludedLocked(peer) {
			// Another candidate's set is in sets too; one that an in-place
			// restore queued meanwhile is no evidence either.
			tm.mu.Unlock()
			continue
		}
		_, lossLog := tm.settleOwnLossLocked(name, lost, peer, divergenceDetectedByRecovery, uuids, true)
		tm.mu.Unlock()
		if lossLog != nil {
			lossLog()
		}
		tm.logger.Error("failover picker: refusing candidate that lacks transactions it issued itself",
			"site", name, "lostTransactions", lost.TransactionCount(), "lostGtid", truncateGtidSet(lost.String()), "comparedTo", peer)
		return true
	}
	return false
}
