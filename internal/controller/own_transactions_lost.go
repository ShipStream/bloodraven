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

const (
	// recloneOwnerOwnLoss is the owner of a reclone the operator requests
	// for a site that lost its own transactions. It has no annotation to
	// consume and no restore queue to report to.
	recloneOwnerOwnLoss = "operator/own-transactions-lost"

	// ownLossRecloneRetryDelay is how long the operator waits before it
	// requests another reclone of a site whose last clone failed, so a
	// clone that keeps failing does not load the primary at poll
	// frequency.
	ownLossRecloneRetryDelay = 5 * time.Minute

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
	// detectedBy is the check that made the comparison
	// (divergenceDetectedByRecovery or divergenceDetectedByAudit), "" for
	// a record restored from status and not compared again yet.
	detectedBy v1alpha1.DivergenceDetector
	// recloneRetryAfter is when the operator may request the next
	// automatic reclone: ownLossRecloneRetryDelay after a clone of the
	// site failed, zero otherwise. A request that was displaced, withdrawn
	// or never started its clone is made again at once.
	recloneRetryAfter time.Time
	// recloneWaiting says why no automatic reclone runs or is requested
	// ("" while one is).
	recloneWaiting string
}

// ownLostTransactions returns the transactions the primary holds under one
// of uuids that the site lacks.
func ownLostTransactions(primary, site mysql.GTIDSet, uuids []string) mysql.GTIDSet {
	return primary.Subtract(site).OriginatedBy(uuids...)
}

// SiteIdentityStore keeps the server_uuids each site has run under where
// they survive the operator. Implemented by the runner on the group's
// metadata (groupSiteIdentityStore). Nil in tests and harnesses that do
// not exercise it: the manager then knows only the uuids it has seen.
type SiteIdentityStore interface {
	// Load returns the recorded server_uuids by site, oldest first.
	Load(ctx context.Context) (map[string][]string, error)
	// Add appends serverUUID to site's record unless it is there.
	Add(ctx context.Context, site, serverUUID string) error
}

// siteIdentityTracker is the manager's view of the recorded server_uuids.
type siteIdentityTracker struct {
	mu     sync.Mutex
	store  SiteIdentityStore
	loaded bool
	// uuids are the server_uuids known per site (stored or seen by this
	// manager), oldest first.
	uuids map[string][]string
	// unsaved are uuids seen but not stored yet.
	unsaved map[string][]string
	// nextTry rate-limits store access after a failure.
	nextTry time.Time
}

// SetSiteIdentityStore wires the durable store of the sites' server_uuids.
func (tm *TopologyManager) SetSiteIdentityStore(s SiteIdentityStore) {
	t := &tm.siteIdentity
	t.mu.Lock()
	t.store = s
	t.loaded = false
	t.mu.Unlock()
}

func normalizeServerUUID(uuid string) string {
	return strings.ToLower(strings.TrimSpace(uuid))
}

// noteSiteServerUUID records that site runs (or ran) under serverUUID and
// returns every server_uuid known for the site, serverUUID included. The
// store is read once per manager and written when a uuid is new; a failed
// read or write is retried on a later call and never fails the caller,
// which then compares under the uuids known so far.
func (tm *TopologyManager) noteSiteServerUUID(ctx context.Context, site, serverUUID string) []string {
	serverUUID = normalizeServerUUID(serverUUID)
	t := &tm.siteIdentity
	now := tm.clock.Now()

	t.mu.Lock()
	if t.uuids == nil {
		t.uuids = map[string][]string{}
	}
	if t.unsaved == nil {
		t.unsaved = map[string][]string{}
	}
	if serverUUID != "" && !slices.Contains(t.uuids[site], serverUUID) {
		t.uuids[site] = append(t.uuids[site], serverUUID)
		t.unsaved[site] = append(t.unsaved[site], serverUUID)
	}
	store := t.store
	needLoad := store != nil && !t.loaded
	pending := len(t.unsaved) > 0
	due := !now.Before(t.nextTry)
	t.mu.Unlock()

	if store != nil && due && (needLoad || pending) {
		tm.syncSiteIdentities(ctx, store, needLoad)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.uuids[site]...)
}

// syncSiteIdentities loads the stored history (once) and stores the uuids
// seen since.
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
			for site, uuids := range stored {
				// Stored uuids are older than anything this manager saw.
				merged := make([]string, 0, len(uuids)+len(t.uuids[site]))
				for _, u := range uuids {
					if u = normalizeServerUUID(u); u != "" && !slices.Contains(merged, u) {
						merged = append(merged, u)
					}
				}
				for _, u := range t.uuids[site] {
					if !slices.Contains(merged, u) {
						merged = append(merged, u)
					}
				}
				t.uuids[site] = merged
				// What is stored already needs no write.
				var still []string
				for _, u := range t.unsaved[site] {
					if !slices.Contains(uuids, u) {
						still = append(still, u)
					}
				}
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
	type entry struct{ site, uuid string }
	var todo []entry
	for site, uuids := range t.unsaved {
		for _, u := range uuids {
			todo = append(todo, entry{site, u})
		}
	}
	t.mu.Unlock()
	slices.SortFunc(todo, func(a, b entry) int {
		if c := strings.Compare(a.site, b.site); c != 0 {
			return c
		}
		return strings.Compare(a.uuid, b.uuid)
	})
	for _, e := range todo {
		addCtx, cancel := context.WithTimeout(ctx, siteIdentityWriteTimeout)
		err := store.Add(addCtx, e.site, e.uuid)
		cancel()
		if err != nil {
			fail("stored", err)
			return
		}
		t.mu.Lock()
		if i := slices.Index(t.unsaved[e.site], e.uuid); i >= 0 {
			t.unsaved[e.site] = slices.Delete(t.unsaved[e.site], i, i+1)
			if len(t.unsaved[e.site]) == 0 {
				delete(t.unsaved, e.site)
			}
		}
		t.mu.Unlock()
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

// settleOwnLossLocked publishes one comparison of site with the confirmed
// primary comparedTo: lost is what the site lacks of its own transactions.
// A non-empty set records (or refreshes) the report; an empty one is the
// proof that clears it. It reports whether the report changed, and returns
// the log call to make once tm.mu is released (nil when there is nothing
// to say). Caller holds tm.mu.
func (tm *TopologyManager) settleOwnLossLocked(site string, lost mysql.GTIDSet, comparedTo string, detectedBy v1alpha1.DivergenceDetector, uuids []string) (changed bool, log func()) {
	rec := tm.ownLoss[site]
	if lost.IsEmpty() {
		if rec == nil {
			return false, nil
		}
		delete(tm.ownLoss, site)
		tm.forgetOwnLossRefusalLocked(site)
		metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
		return true, func() {
			tm.logger.Info("own transactions lost cleared: the site holds every transaction it issued",
				"site", site, "comparedTo", comparedTo)
		}
	}
	set, count := lost.String(), lost.TransactionCount()
	if rec != nil && rec.lostGtid == set && rec.detectedBy != "" {
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
	sameSet := rec.lostGtid == set
	rec.lostGtid, rec.lostCount = set, count
	rec.comparedTo, rec.detectedBy = comparedTo, detectedBy
	// A routine rejoin that was under way is void: the site is not
	// rejoined. A divergence block stays; it outranks this report.
	if r := tm.recovery[site]; r != nil && r.state == v1alpha1.RecoveryStateInProgress {
		r.state = ""
	}
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(float64(count))
	if sameSet {
		// A restored report confirmed by a fresh comparison: recorded,
		// not announced again.
		return true, nil
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
	tm.mu.Unlock()
	if rec == nil {
		return
	}
	metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
	tm.logger.Info("own transactions lost cleared: the site was recloned", "site", site)
	tm.RequestStatusRefresh()
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
// One request at a time, and never beside another reclone request. A site
// that also holds transactions the primary lacks is left to its
// RecoveryBlocked report: an administrator decides about those
// transactions, and the reclone they then ask for repairs both.
func (tm *TopologyManager) requestOwnLossReclone() {
	now := tm.clock.Now()
	tm.mu.Lock()
	if len(tm.ownLoss) == 0 {
		tm.mu.Unlock()
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
		case tm.bootstrap == nil || tm.bootstrapCfg.ReplUser == "":
			setWaiting(rec, "replication credentials are not configured, so the operator cannot clone")
		case tm.topologyFrozen || slices.Contains(tm.restoreRecloneBlocked, name) || tm.restoreReclonePendingLocked(name):
			setWaiting(rec, "an in-place restore holds the site; its reclone queue reclones it")
		case tm.reclonePendingSite != "" || tm.recloneCompletedSite != "":
			setWaiting(rec, "another reclone is pending")
		case !bootstrapIdlePhase(tm.bootstrapPhase):
			setWaiting(rec, "another clone is running")
		case active == "" || active == name:
			setWaiting(rec, "there is no confirmed writable primary to clone from")
		case tm.sites[i].state != state.StateReadOnly:
			setWaiting(rec, "the site is not reachable")
		case now.Before(rec.recloneRetryAfter):
			setWaiting(rec, fmt.Sprintf("the last automatic reclone did not complete; the next one is requested at %s", rec.recloneRetryAfter.UTC().Format(time.RFC3339)))
		default:
			tm.reclonePendingSite, tm.reclonePendingOwner = name, recloneOwnerOwnLoss
			setWaiting(rec, "")
			requested = true
			lost, count, primary := rec.lostGtid, rec.lostCount, active
			logs = append(logs, func() {
				tm.logger.Info("reclone requested for a site that lacks transactions it issued itself",
					"site", name, "donor", primary, "lostTransactions", count, "lostGtid", truncateGtidSet(lost))
			})
		}
		if pendingHere || cloningHere {
			requested = true
		}
	}
	tm.mu.Unlock()
	for _, log := range logs {
		log()
	}
	if changed {
		tm.RequestStatusRefresh()
	}
}

// knownSiteServerUUIDs is noteSiteServerUUID without access to the store,
// for the promotion path, which must not wait on the API server: the uuid
// is remembered and stored by the next call that may.
func (tm *TopologyManager) knownSiteServerUUIDs(site, serverUUID string) []string {
	serverUUID = normalizeServerUUID(serverUUID)
	t := &tm.siteIdentity
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.uuids == nil {
		t.uuids = map[string][]string{}
	}
	if t.unsaved == nil {
		t.unsaved = map[string][]string{}
	}
	if serverUUID != "" && !slices.Contains(t.uuids[site], serverUUID) {
		t.uuids[site] = append(t.uuids[site], serverUUID)
		t.unsaved[site] = append(t.unsaved[site], serverUUID)
	}
	return append([]string(nil), t.uuids[site]...)
}

// readOwnLossWitnesses starts a read of the executed GTID set of every
// site that is not a promotion candidate, whatever the last poll saw of
// it, and returns a function that hands over the sets that have arrived.
//
// With the primary gone there is no primary to compare a candidate with,
// but any site that answers is a witness: whatever it holds under a
// candidate's server_uuid the candidate issued. That includes the failed
// primary itself when it answers again in the poll that replaces it.
//
// A witness never decides how long the promotion takes: the returned
// function waits for the reads still outstanding for ownLossWitnessGrace
// at most, after the candidates' own reads have ended. A site that
// answers does so in milliseconds; a blackholed one costs that grace once
// and is then not a witness.
func (tm *TopologyManager) readOwnLossWitnesses(ctx context.Context, candidates []string) func() map[string]mysql.GTIDSet {
	out := make(map[string]mysql.GTIDSet)
	var mu sync.Mutex
	type witness struct {
		name    string
		checker mysql.Checker
	}
	var todo []witness
	for i := range tm.sites {
		if site := &tm.sites[i]; !slices.Contains(candidates, site.name) {
			todo = append(todo, witness{site.name, site.mysql})
		}
	}
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
	grace := tm.ownLossWitnessGrace
	if grace <= 0 {
		grace = ownLossWitnessGrace
	}
	return func() map[string]mysql.GTIDSet {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
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

// refuseOwnLossCandidate compares a promotion candidate with every other
// site's set read for this decision. It records the candidate's report
// and returns true when one of them holds transactions the candidate
// issued and lacks.
func (tm *TopologyManager) refuseOwnLossCandidate(name, serverUUID string, set mysql.GTIDSet, sets map[string]mysql.GTIDSet) bool {
	uuids := tm.knownSiteServerUUIDs(name, serverUUID)
	peers := make([]string, 0, len(sets))
	for peer := range sets {
		if peer != name {
			peers = append(peers, peer)
		}
	}
	slices.Sort(peers)
	for _, peer := range peers {
		lost := ownLostTransactions(sets[peer], set, uuids)
		if lost.IsEmpty() {
			continue
		}
		tm.mu.Lock()
		_, lossLog := tm.settleOwnLossLocked(name, lost, peer, divergenceDetectedByRecovery, uuids)
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

// gtidSetHolds reports whether the GTID set raw (as a server reports it)
// contains want. A set that does not parse proves nothing.
func gtidSetHolds(raw string, want mysql.GTIDSet) bool {
	set, err := mysql.ParseGTIDSet(raw)
	return err == nil && set.Contains(want)
}

// lacksOwnTransactions reports whether site, running under serverUUID with
// the executed set raw, lacks transactions it issued itself that holder
// has. A set that does not parse proves nothing.
func (tm *TopologyManager) lacksOwnTransactions(site, serverUUID, raw string, holder mysql.GTIDSet) bool {
	set, err := mysql.ParseGTIDSet(raw)
	if err != nil || serverUUID == "" {
		return false
	}
	return !ownLostTransactions(holder, set, tm.knownSiteServerUUIDs(site, serverUUID)).IsEmpty()
}
