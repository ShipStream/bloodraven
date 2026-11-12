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
// and the binary log) while the primary holds them with the site's
// server_id can never get them by replication. It would replicate
// everything else, look healthy, and at its next promotion issue the same
// GTID numbers for other transactions.
//
// Every transaction under a site's server_uuid was committed on that site
// first, so whatever the primary holds under that server_uuid the site held
// once. lost = primary.gtid_executed − site.gtid_executed, restricted to
// the server_uuids the site has had. The GTID sets do not say which
// server_id a transaction is logged with: one that was applied on the
// primary from a decoded binary log carries the primary's, and replication
// does deliver it. For a site that has no channel that is a doubt the
// operator resolves toward the clone; for a site whose channel runs, the
// deficit counts only once it cannot be lag (see "A hole, or only lag?"
// below). A site's server_id is spec.sites[].serverID
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
//     server_uuid timeline when it is known to have installed
//     (markSiteCloned), and the server_uuid a site runs under is live
//     whatever the timeline says. A site queued for the reclone of
//     an in-place restore (ownLossWitnessExcludedLocked) is no witness at
//     all: its data is void by a durable record, the operator never
//     promotes it or replicates from it, and it is cloned over.
//
// The automatic reclone is the one destructive step, so it does not rest
// on a report: verifyOwnLossReclone reads the fenced site and its donor
// again when the clone is about to start and lets it run only if the
// donor is the confirmed primary, holds everything the site holds, and
// still holds transactions the site issued and lacks, which for a site
// whose channel runs must be a proven hole.

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
//
// A report is one of three kinds:
//
//   - a hole (witness false, detectedBy set): a check of this manager found
//     that the confirmed primary comparedTo holds transactions the site
//     issued and lacks. Replication cannot fill it, so the site's
//     replication is not started and the site is recloned. It ends by a
//     clean comparison with that same primary, or by the reclone.
//   - a witness report (witness true): the site lacks transactions it
//     issued that a site which is not the confirmed primary holds
//     (comparedTo, "" when status did not say). The site can replicate from
//     the primary, so its replication is not refused and it is not recloned
//     automatically; it is not promoted (it would issue those GTIDs again).
//   - restored (witness false, detectedBy ""): read from status after an
//     operator restart and not compared by this manager yet. It holds like
//     a hole (no replication start, no promotion) and asks for no reclone.
//     The first comparison with the confirmed primary makes it a hole (the
//     primary holds transactions the site lacks) or a witness report (it
//     does not).
//
// One rule ends a report that is not a hole against the current primary
// (reviewOwnLossReports, and the primary re-assert for its target): every
// other site of the group has answered in one pass and none holds
// transactions the site issued, under a server_uuid no clone has
// superseded, and lacks. A clean comparison with the primary alone never
// ends it, and neither does a holder that stops answering. A hole against
// a primary that is no longer the primary becomes a witness report naming
// that site when the new primary does not hold the transactions.
type ownLossRecord struct {
	lostGtid  string
	lostCount int64
	// comparedTo is the site that holds lostGtid: the primary for a hole,
	// the witness for a witness report. It is stored in
	// status.sites[].lostComparedTo and restored from it; "" for a report
	// restored from a status written without the field.
	comparedTo string
	// witness is true for a witness report (see above).
	witness bool
	// reviewAfter is when a witness report is compared with the other
	// sites again (reviewOwnLossReports).
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
	// replaces this one in the same process (carryOwnLossReports) and
	// starts at zero after an operator restart.
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
// serverUUIDCloneMarker where a clone was installed into the site. A
// server_uuid recorded only before the last marker was given up at that
// clone (liveServerUUIDs); one the site is seen under after it is recorded
// again, after the marker.
type SiteIdentityStore interface {
	// Load returns the recorded timelines by site.
	Load(ctx context.Context) (map[string][]string, error)
	// Add appends entry (a server_uuid, or serverUUIDCloneMarker) to
	// site's record by the rule of appendServerUUIDEntry.
	Add(ctx context.Context, site, entry string) error
}

// serverUUIDCloneMarker is the entry of a site's recorded timeline that
// stands for a clone installed into the site.
const serverUUIDCloneMarker = "clone"

// appendServerUUIDEntry appends entry to a site's timeline and reports
// whether it changed: the clone marker unless it is the last entry, a
// server_uuid unless it is recorded after the last marker already.
//
// A server_uuid that stands only before the last marker is appended again:
// the site runs under it after that clone (the clone installed without a
// new server_uuid, or the site's volume came back from before it), so it
// can issue that identity's GTIDs again and the clone no longer supersedes
// it. This is the durable form of the rule that the server_uuid a site
// runs under is always live (liveServerUUIDs).
func appendServerUUIDEntry(timeline []string, entry string) ([]string, bool) {
	switch {
	case entry == "":
		return timeline, false
	case entry == serverUUIDCloneMarker:
		if n := len(timeline); n == 0 || timeline[n-1] == serverUUIDCloneMarker {
			// Nothing was recorded before it, or it is recorded already.
			return timeline, false
		}
	case slices.Contains(timeline[lastCloneMarker(timeline)+1:], entry):
		return timeline, false
	}
	return append(timeline, entry), true
}

// lastCloneMarker returns the index of the last clone marker of a
// timeline, -1 when it has none.
func lastCloneMarker(timeline []string) int {
	for i := len(timeline) - 1; i >= 0; i-- {
		if timeline[i] == serverUUIDCloneMarker {
			return i
		}
	}
	return -1
}

// recordedServerUUIDs returns the server_uuids of a timeline, each once.
func recordedServerUUIDs(timeline []string) []string {
	out := make([]string, 0, len(timeline))
	for _, e := range timeline {
		if e != serverUUIDCloneMarker && !slices.Contains(out, e) {
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
// replaced by hand) carries no such proof and stays live.
//
// The server_uuid a site runs under is always live, whatever the timeline
// says: a site that runs under a server_uuid from before a clone (its
// volume came back from before it, or the clone installed without a new
// server_uuid) can issue that identity's GTIDs again. Every caller that
// has read the site passes it as current, and noting it
// (appendServerUUIDEntry) records it again after the marker, so it stays
// live when the site is not read. A caller that has not read the site
// (current == "") must not conclude from this list that the site has
// nothing to lose: the site may run under an identity that is recorded
// only before the marker and was not seen since. crossCheckOwnLoss
// filters on every recorded server_uuid for that reason.
func liveServerUUIDs(timeline []string, current string) []string {
	live := recordedServerUUIDs(timeline[lastCloneMarker(timeline)+1:])
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

// markSiteCloned records that a clone was installed into site: the
// server_uuids recorded so far are superseded (liveServerUUIDs). It is
// called when the clone is known to have installed, never when it is
// issued: a clone that fails or is killed leaves the site with the data
// and the identity it had, and a marker for it would void, at the site's
// next change of server_uuid, an identity no clone replaced. It reports
// whether the marker is stored (or the manager has no store): the clone
// identity record, which makes a later manager look at the clone again,
// is removed only then (finishCloneIdentity).
func (tm *TopologyManager) markSiteCloned(ctx context.Context, site string) bool {
	tm.noteSiteEntry(ctx, site, serverUUIDCloneMarker)
	t := &tm.siteIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.store == nil || !slices.Contains(t.unsaved[site], serverUUIDCloneMarker)
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
// serverUUID in site's timeline, so the tracker holds it and does not
// write it again.
func (tm *TopologyManager) siteCloneRecorded(site, serverUUID string) {
	t := &tm.siteIdentity
	entry := normalizeServerUUID(serverUUID)
	t.mu.Lock()
	t.rememberLocked(site, entry)
	t.mu.Unlock()
	t.siteEntryStored(site, entry)
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
	if rec == nil {
		return false
	}
	// A witness report that names the site which is the primary now (it
	// was made while no site was writable, by the picker or the
	// re-assert) says that the primary holds the transactions: it holds
	// like a hole until a comparison with the primary settles it.
	return !rec.witness || (rec.comparedTo != "" && rec.comparedTo == tm.activeSiteLocked())
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
// A non-empty set records (or refreshes) the report. A hole against the
// primary is the stronger one and is kept over a witness's.
//
// An empty set from a comparison with the confirmed primary ends a hole
// that was made against that same primary: the site holds again what the
// primary holds of its own transactions. It ends nothing else. A report
// that names another site, or whose holder status did not say, rests on
// what that site holds, and the primary not holding it is no proof that
// nobody does: such a report becomes (or stays) a witness report, the site
// may replicate from the primary again, and reviewOwnLossReports ends it
// when every other site has answered and none holds the set. An empty set
// from a comparison with one witness changes nothing, for the same reason.
//
// It reports whether the report changed, and returns the log call to make
// once tm.mu is released (nil when there is nothing to say). Caller holds
// tm.mu.
func (tm *TopologyManager) settleOwnLossLocked(site string, lost mysql.GTIDSet, comparedTo string, detectedBy v1alpha1.DivergenceDetector, uuids []string, witness bool) (changed bool, log func()) {
	rec := tm.ownLoss[site]
	if lost.IsEmpty() {
		if rec == nil || witness {
			return false, nil
		}
		if rec.detectedBy != "" && !rec.witness && rec.comparedTo == comparedTo {
			why := fmt.Sprintf("it holds every transaction it issued that %s holds", comparedTo)
			tm.dropOwnLossLocked(site, why)
			return true, func() {
				tm.logger.Info("own transactions lost cleared: the site holds every transaction it issued",
					"site", site, "comparedTo", comparedTo, "reason", why)
			}
		}
		if rec.witness && rec.detectedBy != "" {
			// A witness report: the primary was never its holder.
			return false, nil
		}
		// A hole against another primary, or a restored report: the
		// primary of now does not hold the transactions.
		holder := rec.comparedTo
		rec.witness = true
		if rec.detectedBy == "" {
			rec.detectedBy = detectedBy
		}
		rec.reviewAfter = time.Time{}
		rec.recloneRefusal, rec.recloneWaiting, rec.recloneAnnounced = "", "", false
		return true, func() {
			tm.logger.Info("own transactions lost kept: the primary does not hold the transactions, the report stands until every other site has been compared",
				"site", site, "comparedTo", comparedTo, "heldBy", lostHeldBy(holder))
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
		remedy := fmt.Sprintf("the site is not promoted while %s holds these transactions, which the primary does not have: reclone %s to discard them, or replay them onto the primary (this site then receives them by replication), or reclone this site (kubectl bloodraven reclone %s %s --cold)",
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

// dropOwnLossLocked ends site's report for the reason why. Caller holds
// tm.mu.
func (tm *TopologyManager) dropOwnLossLocked(site, why string) {
	delete(tm.ownLoss, site)
	delete(tm.ownLossProof, site)
	tm.forgetOwnLossRefusalLocked(site)
	tm.noteOwnLossClearedLocked(site, why)
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
}

// SetOwnTransactionsLost is SetOwnTransactionsLostFrom for a status that
// does not say which site the report was compared with (written against a
// CRD without status.sites[].lostComparedTo).
func (tm *TopologyManager) SetOwnTransactionsLost(site, lostGtid string, lostCount int64) {
	tm.SetOwnTransactionsLostFrom(site, lostGtid, lostCount, "")
}

// SetOwnTransactionsLostFrom restores a persisted report from CR status
// after an operator restart: the lost set and comparedTo, the site that
// held it. The restored report keeps the site out of replication and
// promotion at once. Its kind is settled by the first comparison with the
// confirmed primary (settleOwnLossLocked), and it ends by the rule of
// every report (ownLossRecord).
func (tm *TopologyManager) SetOwnTransactionsLostFrom(site, lostGtid string, lostCount int64, comparedTo string) {
	if lostGtid == "" {
		return
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.ownLoss == nil {
		tm.ownLoss = map[string]*ownLossRecord{}
	}
	tm.ownLoss[site] = &ownLossRecord{lostGtid: lostGtid, lostCount: lostCount, comparedTo: comparedTo}
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(float64(lostCount))
}

// SetOwnTransactionsLostUnknown restores a report whose lost set and holder
// the stored status does not carry: the installed CRD is older than the
// operator, so the report was stored as recoveryState RecoveryBlocked with
// no set at all (degradeStatusForOlderCRD). Nothing is known but
// that the site was reported, so the report takes the conservative rule
// whole: it keeps the site out of replication and promotion, a comparison
// with the primary makes it a hole or a witness report, and it ends only
// when every other site has answered and none holds transactions the site
// issued and lacks (reviewOwnLossReports), or by the site's reclone.
func (tm *TopologyManager) SetOwnTransactionsLostUnknown(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.ownLoss == nil {
		tm.ownLoss = map[string]*ownLossRecord{}
	}
	if tm.ownLoss[site] == nil {
		tm.ownLoss[site] = &ownLossRecord{}
	}
}

// carryOwnLossReports takes over the reports of the manager this one
// replaces in the same process (a spec or credential change), for the
// sites both have: prev's reports are newer than the status this manager
// was started from, which may not hold a report prev made in its last
// polls, and they carry their kind. A report restored from status for a
// site prev has no report for is dropped: prev cleared it, or never had
// it. The failed-reclone count and its retry time come with the record,
// so a spec change does not restart a failing clone at once; an operator
// restart does (the count is not stored).
func (tm *TopologyManager) carryOwnLossReports(prev *TopologyManager) {
	if prev == nil || prev == tm {
		return
	}
	prev.mu.RLock()
	carried := make(map[string]ownLossRecord, len(prev.ownLoss))
	for site, rec := range prev.ownLoss {
		carried[site] = *rec
	}
	prev.mu.RUnlock()
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for site := range tm.ownLoss {
		if _, ok := carried[site]; !ok {
			delete(tm.ownLoss, site)
			metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
		}
	}
	for site, rec := range carried {
		if tm.getSite(site) == nil {
			continue
		}
		if tm.ownLoss == nil {
			tm.ownLoss = map[string]*ownLossRecord{}
		}
		rec := rec
		// The request, its refusal and the review time are this manager's
		// to make again.
		rec.recloneAnnounced, rec.recloneRefusal, rec.recloneWaiting = false, "", ""
		rec.reviewAfter = time.Time{}
		tm.ownLoss[site] = &rec
		metrics.OwnTransactionsLost.WithLabelValues(site).Set(float64(rec.lostCount))
	}
}

// clearOwnLossAfterClone drops site's report: a completed clone made the
// site a copy of its donor.
func (tm *TopologyManager) clearOwnLossAfterClone(site string) {
	tm.mu.Lock()
	rec := tm.ownLoss[site]
	delete(tm.ownLossProof, site)
	if rec != nil {
		tm.dropOwnLossLocked(site, "it was recloned")
	}
	// A report that may rest on what this site held is reviewed now: the
	// clone replaced it.
	for _, other := range tm.ownLoss {
		other.reviewAfter = time.Time{}
	}
	tm.mu.Unlock()
	if rec == nil {
		return
	}
	tm.logger.Info("own transactions lost cleared: the site was recloned", "site", site)
	tm.RequestStatusRefresh()
}

// clearOwnLossNoHolder drops site's report, of whatever kind: one pass
// read every other site of the group and none holds transactions the site
// issued, under a server_uuid no clone has superseded, and lacks. The
// callers are the two passes that read them all: reviewOwnLossReports, and
// the primary re-assert for its target (which refuses unless every peer
// answers read-only). It reports whether a report was dropped.
//
// A hole made against a primary under a server_uuid the site has since
// given up at a clone is dropped too: that site is divergent, not this
// one short, and the first comparison with a confirmed primary reports
// whichever of the two it finds again.
func (tm *TopologyManager) clearOwnLossNoHolder(site string) bool {
	const why = "no other site holds transactions it issued and lacks"
	tm.mu.Lock()
	if tm.ownLoss[site] == nil {
		tm.mu.Unlock()
		return false
	}
	tm.dropOwnLossLocked(site, why)
	tm.mu.Unlock()
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
// The primary itself is not a subject: it is writable already. Only extra
// counts, the part of the witness's set the primary lacks: what the
// primary holds too is the primary's to be compared with, where lag can be
// told from a hole (auditFollower). Sites are read only when extra names a
// server_uuid they have ever had, so the usual divergence (a site's own
// unreplicated tail) costs no read. The filter is on every recorded
// server_uuid, not on the live ones: which of them are live depends on the
// server_uuid the site runs under now, which only the read tells
// (liveServerUUIDs). It reports whether a report changed.
func (tm *TopologyManager) crossCheckOwnLoss(ctx context.Context, witness *siteTracker, extra mysql.GTIDSet, active string) bool {
	if extra.IsEmpty() {
		return false
	}
	tm.mu.RLock()
	excluded := tm.ownLossWitnessExcludedLocked(witness.name)
	tm.mu.RUnlock()
	if excluded {
		return false
	}
	tm.notePrimaryLacksOwnTransactions(active, witness.name, extra)
	changed := false
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.name == witness.name || site.name == active || site.state != state.StateReadOnly {
			continue
		}
		if extra.OriginatedBy(tm.recordedSiteServerUUIDs(site.name)...).IsEmpty() {
			continue
		}
		snap, siteSet, ok := tm.readOwnLossSubject(ctx, site)
		if !ok {
			continue
		}
		live := tm.liveSiteServerUUIDs(site.name, snap.ServerUUID)
		uuids := tm.noteSiteServerUUID(ctx, site.name, snap.ServerUUID)
		lost := ownLostOnWitness(extra, siteSet, live)
		if lost.IsEmpty() {
			continue
		}
		tm.mu.Lock()
		siteChanged, lossLog := tm.settleOwnLossLocked(site.name, lost, witness.name, divergenceDetectedByAudit, uuids, true)
		if rec := tm.ownLoss[site.name]; rec != nil && rec.witness {
			rec.reviewAfter = tm.clock.Now().Add(gtidAuditInterval)
		}
		tm.mu.Unlock()
		if lossLog != nil {
			lossLog()
		}
		if siteChanged {
			tm.RequestStatusRefresh()
			changed = true
		}
	}
	return changed
}

// notePrimaryLacksOwnTransactions logs, once per set, that the writable
// primary lacks transactions it issued itself under a server_uuid no clone
// has superseded, which the site holder has (extra is what holder holds
// and the primary lacks). The primary is no subject of an own-loss report:
// it is writable already, the operator did not make it so on this evidence
// (every promotion path compares first), and fencing the only primary
// would turn a loss that has happened into an outage. It came to be this
// way outside the operator: it restarted on older data and booted
// writable or was made writable by hand, for example the source of an
// ordered update's handoff that the abort found writable and regressed.
// What it commits now reuses the GTIDs of the transactions it lost. The
// holder is reported RecoveryBlocked with those transactions as its
// divergent set (the caller has just recorded that); this line says which
// of the two sites is the one that lost data.
func (tm *TopologyManager) notePrimaryLacksOwnTransactions(primary, holder string, extra mysql.GTIDSet) {
	if primary == "" {
		return
	}
	own := extra.OriginatedBy(tm.liveSiteServerUUIDs(primary, "")...)
	key := ""
	if !own.IsEmpty() {
		key = primary + "/" + holder + "/" + own.String()
	}
	tm.mu.Lock()
	same := tm.primaryOwnLossLogged == key
	tm.primaryOwnLossLogged = key
	tm.mu.Unlock()
	if key == "" || same {
		return
	}
	tm.logger.Error("the primary lacks transactions it issued itself, which another site holds; it restarted on older data and was made writable outside the operator, and reuses their GTIDs for what it commits now",
		"site", primary, "heldBy", holder, "lostTransactions", own.TransactionCount(), "lostGtid", truncateGtidSet(own.String()),
		"remedy", fmt.Sprintf("the transactions exist on %s only (its divergentGtid). Decide which history to keep: replay them onto the primary and reclone %s, or stop writes, make %s the primary by hand and reclone %s",
			holder, holder, holder, primary))
}

// recordedSiteServerUUIDs returns every server_uuid recorded for site,
// without access to the store.
func (tm *TopologyManager) recordedSiteServerUUIDs(site string) []string {
	t := &tm.siteIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	return recordedServerUUIDs(t.timeline[site])
}

// readOwnLossSubject reads the server_uuid and executed set of a site that
// is compared with other sites. ok is false when the site does not answer,
// its identity cannot be read (a test double) or its set does not parse:
// nothing is concluded about it then.
func (tm *TopologyManager) readOwnLossSubject(ctx context.Context, site *siteTracker) (snap mysql.GTIDSnapshot, set mysql.GTIDSet, ok bool) {
	var identityKnown bool
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, identityKnown, err = readSiteGTIDSnapshot(probeCtx, site.mysql)
		return err
	}); err != nil || !identityKnown {
		return snap, set, false
	}
	set, err := mysql.ParseGTIDSet(snap.GtidExecuted)
	return snap, set, err == nil
}

// reviewOwnLossReports looks, once per gtidAuditInterval, at every report
// that is not a hole against the primary (a witness report, or one
// restored from status that nothing has compared yet), by the one rule
// that ends such a report: every other site of the group answered in this
// pass and none holds transactions the site issued, under a server_uuid no
// clone has superseded, and lacks.
//
// The site and every other site are read, whatever site the report names:
// two sites can hold the same lost transactions, and a report restored
// from an older status names none. A site that awaits the reclone of an
// in-place restore holds nothing that counts and need not answer, and a
// site that left the group is not asked. Any other site that does not
// answer proves nothing, and the report stays: a site is not made
// promotable again on silence. A site that holds the transactions becomes
// the report's holder. The primary is asked under every server_uuid the
// site has had: what it holds of them is a hole, which the audit and
// recovery report with their own proof, and until they have, this pass
// leaves the report alone. It runs with or without a primary, and only
// reads.
func (tm *TopologyManager) reviewOwnLossReports(ctx context.Context) bool {
	now := tm.clock.Now()
	tm.mu.Lock()
	var due []string
	for name, rec := range tm.ownLoss {
		if (rec.witness || rec.detectedBy == "") && !now.Before(rec.reviewAfter) {
			rec.reviewAfter = now.Add(gtidAuditInterval)
			due = append(due, name)
		}
	}
	active := tm.activeSiteLocked()
	tm.mu.Unlock()
	slices.Sort(due)
	changed := false
	for _, name := range due {
		site := tm.getSite(name)
		if site == nil || site.state != state.StateReadOnly {
			continue
		}
		// Only a read-only site is reviewed, by its live flag (the tracked
		// state is debounced). A site that is writable issues transactions:
		// once it has issued as many as it lost its set looks complete,
		// and its report is what makes the split-brain fence choose it
		// (fenceWritableOwnLossSites). Its report ends when it is fenced
		// and reviewed, or by the promotion record (requestOwnLossReclone).
		var readOnly bool
		if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var err error
			readOnly, err = site.mysql.CheckReadOnly(probeCtx)
			return err
		}); err != nil || !readOnly {
			continue
		}
		// The other sites first, then the site: what the site applied
		// between the reads is then in its set, so a holder's set never
		// shows a transaction the site is about to hold.
		sets := tm.readOwnLossWitnesses(ctx, []string{name})()
		snap, siteSet, ok := tm.readOwnLossSubject(ctx, site)
		if !ok {
			continue
		}
		live := tm.liveSiteServerUUIDs(name, snap.ServerUUID)
		uuids := tm.noteSiteServerUUID(ctx, name, snap.ServerUUID)
		tm.mu.RLock()
		named := ""
		if rec := tm.ownLoss[name]; rec != nil {
			named = rec.comparedTo
		}
		silent, primaryHolds := false, false
		var peers []string
		for i := range tm.sites {
			peer := tm.sites[i].name
			if peer == name || tm.ownLossWitnessExcludedLocked(peer) {
				continue
			}
			set, answered := sets[peer]
			switch {
			case peer == active && answered:
				primaryHolds = !ownLostTransactions(set, siteSet, uuids).IsEmpty()
			case !answered || tm.sites[i].state != state.StateReadOnly:
				// No answer, or a site that is not seen read-only (not
				// classified yet, or writable beside another): nothing
				// is concluded from it, in either direction.
				silent = true
			default:
				peers = append(peers, peer)
			}
		}
		tm.mu.RUnlock()
		// The site the report names is asked first, so a report keeps its
		// holder while that site still holds the transactions.
		slices.SortStableFunc(peers, func(a, b string) int {
			switch {
			case a == named && b != named:
				return -1
			case b == named && a != named:
				return 1
			}
			return strings.Compare(a, b)
		})
		held := false
		for _, peer := range peers {
			lost := ownLostOnWitness(sets[peer], siteSet, live)
			if lost.IsEmpty() {
				continue
			}
			held = true
			tm.mu.Lock()
			siteChanged, lossLog := tm.settleOwnLossLocked(name, lost, peer, divergenceDetectedByAudit, uuids, true)
			tm.mu.Unlock()
			if lossLog != nil {
				lossLog()
			}
			changed = changed || siteChanged
			break
		}
		if held || silent || primaryHolds {
			continue
		}
		if tm.clearOwnLossNoHolder(name) {
			changed = true
		}
	}
	if changed {
		tm.RequestStatusRefresh()
	}
	return changed
}

// A hole, or only lag?
//
// "Whatever the primary holds under a server_uuid of the site, the site
// held once" is true, but it does not follow that the site can never get
// it by replication. The receiver drops events by server_id, and a
// transaction keeps its server_uuid but takes the server_id of the server
// that logs it when it is applied there from a decoded binary log
// (mysqlbinlog | mysql, one of the documented remedies for a divergent
// site) or under a hand-set gtid_next. Measured on MySQL 9.7: transactions
// of B replayed onto A are logged on A with A's server_id, and B,
// replicating from A, receives and applies every one of them. While such a
// replay runs, a follower that is merely behind lacks, at every
// comparison, transactions under its own server_uuid that the primary
// holds.
//
// So for a site whose channel from the primary runs, a deficit is a hole
// only once it cannot be lag. The proof is by position, not by time: the
// primary's binary log coordinates are read together with its executed set
// (SHOW BINARY LOG STATUS), and the deficit counts once the site's applier
// has executed the primary's log past those coordinates
// (ReplicaStatus.ExecutedPast) and the site still lacks part of that set.
// Everything the primary had logged by then the site has applied, or its
// receiver dropped. Measured: with a real hole the site's executed position
// reaches the primary's while the transactions stay missing, also on an
// idle primary; with a delayed or stopped applier it stays behind the
// snapshot until the transactions are applied.
//
// A site with no channel, or a channel that does not run from the primary,
// is compared once, exactly, as before: nothing is delivering anything to
// it. If the primary holds the site's transactions by a replay, replication
// would have delivered them and the reclone is not needed; the two cases
// look the same in the GTID sets, and a needless clone (the primary holds
// everything the site does) is the safe side of that doubt, a rejoin the
// unsafe one.

// binlogPositionReader is implemented by checkers that read a server's
// binary log coordinates with its executed set. The production checker
// does. For one that does not (a test double, a simulation without a
// replay) a deficit is taken as a hole at once, as for a site without a
// channel.
type binlogPositionReader interface {
	ReadBinlogPosition(ctx context.Context) (mysql.BinlogPosition, error)
}

// ownLossProof is a snapshot of the primary that a site with a running
// channel must have executed past before what it lacks of the snapshot
// counts as a hole. Protected by tm.mu.
type ownLossProof struct {
	// primary and primaryUUID name the server the snapshot was read from.
	primary, primaryUUID string
	pos                  mysql.BinlogPosition
	set                  mysql.GTIDSet
}

// ownLossProofRetry is how soon a follower whose deficit could still be
// lag is audited again.
const ownLossProofRetry = 5 * time.Second

// proveOwnLossHole decides whether lost, what site lacks of its own
// transactions that the primary holds (from a comparison just made, under
// uuids), is a hole. It returns the hole and proven=true, or proven=false
// when the deficit can still be replication lag: nothing is concluded
// then, and the caller looks again.
//
// It is a hole at once when the primary cannot report its binary log
// coordinates, or the site has no channel that runs from the primary
// (both threads running, the receiver connected to this primary).
// Otherwise the primary's coordinates and set are recorded on the first
// sight of a deficit, and the hole is what the site still lacks of that
// set once its applier has executed past those coordinates; that can be
// in the same call, when the site is caught up. An empty hole with
// proven=true means the site holds everything the snapshot held: the
// deficit was lag.
func (tm *TopologyManager) proveOwnLossHole(ctx context.Context, primary, site *siteTracker, primaryUUID, siteUUID string, uuids []string, lost mysql.GTIDSet) (hole mysql.GTIDSet, proven bool) {
	positions, ok := primary.mysql.(binlogPositionReader)
	if !ok || lost.IsEmpty() {
		return lost, true
	}
	readStatus := func() (*mysql.ReplicaStatus, error) {
		var rs *mysql.ReplicaStatus
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var err error
			rs, err = site.mysql.ShowReplicaStatus(probeCtx)
			return err
		})
		return rs, err
	}
	rs, err := readStatus()
	if err != nil {
		// Whether a channel runs is not known: nothing is concluded.
		return mysql.GTIDSet{}, false
	}
	if !channelRunsFrom(rs, primary.host) {
		tm.mu.Lock()
		delete(tm.ownLossProof, site.name)
		tm.mu.Unlock()
		return lost, true
	}
	// Two rounds at most: a snapshot the site has passed without a hole is
	// replaced by one taken now, which the site may have passed too.
	for round := 0; round < 2; round++ {
		tm.mu.RLock()
		proof := tm.ownLossProof[site.name]
		tm.mu.RUnlock()
		if proof == nil || proof.primary != primary.name || proof.primaryUUID != primaryUUID {
			var pos mysql.BinlogPosition
			if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				var err error
				pos, err = positions.ReadBinlogPosition(probeCtx)
				return err
			}); err != nil {
				// The coordinates cannot be read (binary logging off, a
				// missing privilege): the single comparison stands.
				tm.logger.Warn("own transactions lost: the primary's binary log position could not be read; a deficit is not checked against replication lag",
					"site", site.name, "primary", primary.name, "error", err)
				return lost, true
			}
			set, err := mysql.ParseGTIDSet(pos.GtidExecuted)
			if err != nil {
				return mysql.GTIDSet{}, false
			}
			proof = &ownLossProof{primary: primary.name, primaryUUID: primaryUUID, pos: pos, set: set}
			tm.mu.Lock()
			if tm.ownLossProof == nil {
				tm.ownLossProof = map[string]*ownLossProof{}
			}
			tm.ownLossProof[site.name] = proof
			tm.mu.Unlock()
		}
		// The applier's position first, then the set: what was applied up
		// to that position is in the set read after it. A receiver that
		// last connected to another server under the primary's name (the
		// primary was replaced) reports that server's positions, not this
		// primary's: wait for it to reconnect.
		if rs, err = readStatus(); err != nil || !channelRunsFrom(rs, primary.host) ||
			(rs.SourceUUID != "" && primaryUUID != "" && !strings.EqualFold(rs.SourceUUID, primaryUUID)) {
			return mysql.GTIDSet{}, false
		}
		snap, siteSet, ok := tm.readOwnLossSubject(ctx, site)
		if !ok || !strings.EqualFold(snap.ServerUUID, siteUUID) {
			return mysql.GTIDSet{}, false
		}
		if !rs.ExecutedPast(proof.pos) {
			// Behind the snapshot. A snapshot that lies beyond the
			// primary's log as it is now (the log was reset) can never be
			// passed: it is dropped, and the next look takes another.
			var now mysql.BinlogPosition
			if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				var err error
				now, err = positions.ReadBinlogPosition(probeCtx)
				return err
			}); err == nil && !(&mysql.ReplicaStatus{ExecSourceLogFile: now.File, ExecSourceLogPos: now.Pos}).ExecutedPast(proof.pos) {
				tm.mu.Lock()
				delete(tm.ownLossProof, site.name)
				tm.mu.Unlock()
			}
			return mysql.GTIDSet{}, false
		}
		if hole = ownLostTransactions(proof.set, siteSet, uuids); !hole.IsEmpty() {
			return hole, true
		}
		// The site holds everything of that snapshot. What it lacked in
		// the caller's comparison came later: prove against a new one.
		tm.mu.Lock()
		delete(tm.ownLossProof, site.name)
		tm.mu.Unlock()
		if round == 0 {
			_, primarySet, err := readGTIDSetRaw(ctx, primary.mysql)
			if err != nil || ownLostTransactions(primarySet, siteSet, uuids).IsEmpty() {
				return mysql.GTIDSet{}, err == nil
			}
		}
	}
	return mysql.GTIDSet{}, false
}

// channelRunsFrom reports whether rs is a channel from host with both
// threads running.
func channelRunsFrom(rs *mysql.ReplicaStatus, host string) bool {
	return rs != nil && rs.IORunning && rs.SQLRunning && canonicalSourceHost(rs.SourceHost) == canonicalSourceHost(host)
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
			if !tm.everyOtherSiteReadOnlyLocked(name) {
				// The only writable site the operator sees, but not every
				// other site answers read-only: one of them may be the
				// primary an administrator promoted in its place. The
				// report stays, so that this site, not that one, is fenced
				// if both turn out writable (fenceWritableOwnLossSites).
				setWaiting(rec, "the site was made writable without a reclone; the report stays until every other site is seen read-only")
				continue
			}
			// Made writable without a reclone (the operator does not
			// promote it), and no other site can be writing: the report
			// no longer describes a replica to repair. The runner records
			// the promotion from the status change.
			delete(tm.ownLoss, name)
			delete(tm.ownLossProof, name)
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
		case rec.witness && rec.comparedTo != active:
			// Nothing to repair against the primary: a clone would only
			// change the site's server_uuid. The report ends with the
			// witness's own repair. (A witness report that names the
			// primary of now is compared with it like any other; the
			// check before the clone decides.)
			setWaiting(rec, ownLossWitnessWaiting(rec.comparedTo))
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
		case rec.comparedTo != active || rec.witness:
			// Made against another site, or against this one before it
			// was the primary: recovery or the audit compares first.
			if rec.witness {
				setWaiting(rec, fmt.Sprintf("the report was made against %s before it was the primary; the site has not been compared with it as the primary yet", active))
				break
			}
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

// everyOtherSiteReadOnlyLocked reports whether every site but name is
// observed read-only: reachable, and not writable. Caller holds tm.mu.
func (tm *TopologyManager) everyOtherSiteReadOnlyLocked(name string) bool {
	for i := range tm.sites {
		if tm.sites[i].name != name && tm.sites[i].state != state.StateReadOnly {
			return false
		}
	}
	return true
}

// ownLossWitnessWaiting says why a site with a witness report is not
// recloned automatically, and what ends the report. holder is the site
// the report names ("" when status did not say).
func ownLossWitnessWaiting(holder string) string {
	if holder == "" {
		return "the primary does not hold the transactions, so the site is not recloned automatically; it is not promoted until every other site has been compared and none holds them (the report does not say which site did)"
	}
	return fmt.Sprintf("the transactions are held by %s, not by the primary, so the site is not recloned automatically; it is not promoted until every other site has been compared and none holds them: reclone %s, or replay the transactions onto the primary",
		holder, holder)
}

// ownLossPromotionRemedyLocked says, for a refusal to promote site, what
// ends its report ("" when it has none). Caller holds tm.mu.
func (tm *TopologyManager) ownLossPromotionRemedyLocked(site string) string {
	rec := tm.ownLoss[site]
	switch {
	case rec == nil:
		return ""
	case rec.witness && rec.comparedTo != "":
		return fmt.Sprintf("%s holds them and the primary does not; no reclone of %s is coming: reclone %s (or replay the transactions onto the primary), or reclone %s with kubectl bloodraven reclone %s %s --cold",
			rec.comparedTo, site, rec.comparedTo, site, tm.cfg.Name, site)
	case rec.witness:
		return fmt.Sprintf("the primary does not hold them, so no reclone of %s is coming: the report ends once every other site answers and none holds them, or reclone %s with kubectl bloodraven reclone %s %s --cold",
			site, site, tm.cfg.Name, site)
	case rec.detectedBy == "":
		return "the report was restored from status and is being compared again"
	}
	return "wait for its reclone"
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
//   - the donor still holds transactions the recipient issued and lacks,
//     and, when the recipient's channel from the donor runs, its applier
//     has executed the donor's log past the point at which the donor held
//     them (proveOwnLossHole): a recipient that is only behind is not
//     cloned.
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
	// A site whose channel from the primary runs may only be behind: the
	// clone waits until what it lacks cannot be lag (proveOwnLossHole).
	// The request is made again on every poll, so this is asked again
	// until the site's applier has passed the primary's snapshot.
	donorUUID := ""
	if !lost.IsEmpty() {
		// Best effort: it only tells a receiver that still names the
		// primary's predecessor from one that names this server.
		donorUUID, _ = readServerUUID(ctx, donor.mysql)
	}
	lost, proven := tm.proveOwnLossHole(ctx, donor, recipient, donorUUID, snap.ServerUUID, uuids, lost)
	if !proven {
		return refuse("the site replicates from the primary and has not applied everything the primary held when the loss was seen; what it lacks may be replication lag, which is no reason to clone")
	}
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
