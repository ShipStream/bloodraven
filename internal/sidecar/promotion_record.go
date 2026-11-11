package sidecar

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// RoleObservation is one role probe of the local server (D-43).
type RoleObservation struct {
	ReadOnly     bool
	ServerUUID   string
	ServerID     uint32
	GTIDExecuted string
	// ObservedAt and Took are set by the archiver, not by a prober:
	// ObservedAt is the archiver's clock when the probe returned (with its
	// monotonic reading, which orders observations), Took how long the
	// probe ran. The server read GTIDExecuted somewhere inside
	// [ObservedAt-Took, ObservedAt].
	ObservedAt time.Time
	Took       time.Duration
}

// maxRoleProbeSpread bounds how long a role probe may run for its
// observation to open a promotion record. The record's time is when the
// probe returned, so a slow probe would spend part of the planner's
// clock-skew margin δ (pitrarchive.ClockSkewMargin, 5s) on the gap
// between the GTID snapshot and its time; a slower one waits for the next.
// The planner relies on the bound (pitrarchive.PromotionProbeSpread).
const maxRoleProbeSpread = pitrarchive.PromotionProbeSpread

// roleProber is the role probe the archiver needs to write promotion
// records. *LiveMysql implements it; a checker without it can still report
// read-only, but a writable episode can never be recorded, so it uploads
// nothing (fail closed).
type roleProber interface {
	ProbeRole(ctx context.Context) (RoleObservation, error)
}

var _ roleProber = (*LiveMysql)(nil)

// ProbeRole reads the role and the identity a promotion record holds in one
// statement, so the GTID set belongs to the same observation as the role.
func (m *LiveMysql) ProbeRole(ctx context.Context) (RoleObservation, error) {
	var (
		readOnly int
		obs      RoleObservation
		serverID uint64
	)
	err := m.db.QueryRowContext(ctx,
		"SELECT @@global.read_only, @@server_uuid, @@server_id, @@global.gtid_executed").
		Scan(&readOnly, &obs.ServerUUID, &serverID, &obs.GTIDExecuted)
	if err != nil {
		return RoleObservation{}, fmt.Errorf("query role: %w", err)
	}
	if serverID > uint64(^uint32(0)) {
		return RoleObservation{}, fmt.Errorf("query role: server_id %d out of range", serverID)
	}
	obs.ReadOnly = readOnly == 1
	obs.ServerID = uint32(serverID)
	return obs, nil
}

// errRoleProbeUnsupported: the checker reports read-only but not the server
// identity, so a writable episode cannot be recorded.
var errRoleProbeUnsupported = errors.New("the MySQL role probe does not report server_uuid, server_id and gtid_executed")

// roleState is what the latest role observation showed.
type roleState int

const (
	roleUnknown  roleState = iota // no observation yet, or the latest probe failed
	roleReadOnly                  // read-only
	roleWritable                  // writable
)

// roleTimeline is the role history that the role sampler goroutine and the
// scan both feed (D-43). An epoch is one run of writable observations of
// one server incarnation with a growing gtid_executed: a read-only or
// failed observation ends it, and a writable observation starts a new one
// when the server identity (server_uuid, server_id) changed or its
// gtid_executed no longer contains the previous one (a reclone, a reset, a
// re-seed: a new incarnation). Each epoch is one writable episode with its
// own promotion record. The sampler runs apart from the archive I/O, so an
// episode boundary is seen, and the record's observation is timed, within
// one probe interval even while a scan is busy.
type roleTimeline struct {
	mu       sync.Mutex
	epoch    uint64
	state    roleState
	at       time.Time       // ObservedAt of the latest observation fed
	last     RoleObservation // the latest writable observation (state == roleWritable)
	complete bool            // last came from a probe that reports identity
	// first is the epoch's first writable observation that can open a
	// record: complete and no slower than maxRoleProbeSpread.
	first *RoleObservation
	// readOnly is the latest observation while it is read-only;
	// readOnlyBefore is the gtid_executed of the read-only observation
	// directly before the epoch, of the same server ("" when there was
	// none). It is recorded. readOnlyObserved says that there was one and
	// that the observation which opened the epoch continues its
	// incarnation: only then is it the first source of the record's
	// promotion set (promotionSet, issue #269). A server that has executed
	// nothing reports "", so the flag, not the string, says there was one.
	readOnly         *RoleObservation
	readOnlyBefore   string
	readOnlyObserved bool
	// confirm is the epoch's first observation whose read started at least
	// confirmDelay after first returned (issue #269). Every observation
	// between the two showed the server writable and of first's
	// incarnation (anything else ends the epoch), so its gtid_executed
	// contains first's. nil until there is one.
	confirm      *RoleObservation
	confirmDelay time.Duration
	// firstSince is when the server read the gtid_executed of the
	// complete observation directly before first (zero: none in this
	// process). The episode began after it.
	firstSince time.Time

	// lastComplete is the latest observation, in any role, that reported
	// the server identity; start is this process's first one and
	// startEpoch its epoch (0: read-only). resets are the incarnation
	// changes seen between complete observations and not yet written as
	// records (resetCandidate).
	lastComplete *RoleObservation
	start        *RoleObservation
	startEpoch   uint64
	resets       []resetCandidate
}

// resetCandidate is an observed incarnation change: obs no longer
// continues the incarnation seen at since (sameIncarnation), so the change
// happened in (since, obs.ObservedAt]. epoch is obs's writable epoch (0:
// read-only). A candidate that is its epoch's first observation is
// covered by the episode's promotion record, which carries the same
// observation and a ProbeInterval reaching back to since; it is written as
// a reset record only if that episode ends unrecorded.
type resetCandidate struct {
	obs   RoleObservation
	since time.Time
	epoch uint64
}

// maxPendingResets bounds the unwritten reset candidates. Beyond it the
// two oldest are merged into one that spans both windows, which only
// widens the fence (fail closed).
const maxPendingResets = 16

// roleSnapshot is a copy of the timeline.
type roleSnapshot struct {
	epoch            uint64
	state            roleState
	complete         bool
	first            *RoleObservation
	firstSince       time.Time
	readOnlyBefore   string
	readOnlyObserved bool
	confirm          *RoleObservation
}

// observe feeds one probe result. An observation that finished before the
// latest one fed is stale (the sampler and the scan probe concurrently)
// and is dropped.
func (t *roleTimeline) observe(obs RoleObservation, complete bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !obs.ObservedAt.IsZero() {
		if obs.ObservedAt.Before(t.at) {
			return
		}
		t.at = obs.ObservedAt
	}
	identified := err == nil && complete && obs.ServerUUID != ""
	var prevStart time.Time
	changed := false
	if identified {
		if t.lastComplete != nil {
			prevStart = readStart(*t.lastComplete)
			changed = !sameIncarnation(*t.lastComplete, obs)
		}
		last := obs
		t.lastComplete = &last
	}
	switch {
	case err != nil:
		t.state, t.readOnly, t.first, t.confirm = roleUnknown, nil, nil, nil
	case obs.ReadOnly:
		ro := obs
		t.state, t.readOnly, t.first, t.confirm = roleReadOnly, &ro, nil, nil
	default:
		if t.state != roleWritable || !complete || !t.complete || !sameIncarnation(t.last, obs) {
			// The read-only observation counts only when the observation
			// that opens the epoch continues its incarnation. Every later
			// observation of the epoch continues the one before it, so the
			// epoch is then one incarnation from the read-only observation
			// on. An incarnation change in between is recorded as a reset,
			// and a set that has grown back over it by the time the record
			// is opened would otherwise pass for one history (review r5).
			// It is still recorded for diagnosis when the server is the same.
			before, observed := "", false
			if t.state == roleReadOnly && complete && t.readOnly != nil &&
				t.readOnly.ServerUUID == obs.ServerUUID && t.readOnly.ServerID == obs.ServerID {
				before, observed = t.readOnly.GTIDExecuted, sameIncarnation(*t.readOnly, obs)
			}
			t.epoch++
			t.first, t.confirm, t.readOnlyBefore, t.readOnlyObserved = nil, nil, before, observed
		}
		t.state, t.last, t.complete, t.readOnly = roleWritable, obs, complete, nil
		switch {
		case t.first == nil && identified && obs.Took <= maxRoleProbeSpread:
			first := obs
			t.first, t.firstSince = &first, prevStart
		case t.first != nil && t.confirm == nil && identified && t.confirmDelay > 0 &&
			!readStart(obs).Before(t.first.ObservedAt.Add(t.confirmDelay)):
			confirm := obs
			t.confirm = &confirm
		}
	}
	if !identified {
		return
	}
	epoch := uint64(0)
	if t.state == roleWritable {
		epoch = t.epoch
	}
	if t.start == nil {
		start := obs
		t.start, t.startEpoch = &start, epoch
	}
	if changed {
		t.resets = append(t.resets, resetCandidate{obs: obs, since: prevStart, epoch: epoch})
		if len(t.resets) > maxPendingResets {
			merged := t.resets[1]
			if t.resets[0].since.Before(merged.since) {
				merged.since = t.resets[0].since
			}
			t.resets = append([]resetCandidate{merged}, t.resets[2:]...)
		}
	}
}

// readStart is the earliest time the server can have read obs's
// gtid_executed: when its probe started.
func readStart(obs RoleObservation) time.Time {
	return obs.ObservedAt.Add(-obs.Took)
}

// startObservation returns this process's first identified observation
// and its epoch (0: read-only), or nil.
func (t *roleTimeline) startObservation() (*RoleObservation, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == nil {
		return nil, 0
	}
	start := *t.start
	return &start, t.startEpoch
}

// queueStartReset queues the change the start-up check found against the
// site's latest record, ahead of the in-process ones.
func (t *roleTimeline) queueStartReset(c resetCandidate) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resets = append([]resetCandidate{c}, t.resets...)
}

// pendingResets returns a copy of the unwritten reset candidates.
func (t *roleTimeline) pendingResets() []resetCandidate {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]resetCandidate(nil), t.resets...)
}

// dropResets forgets the candidates of the observations at done.
func (t *roleTimeline) dropResets(done []time.Time) {
	if len(done) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := t.resets[:0]
	for _, c := range t.resets {
		drop := false
		for _, at := range done {
			if c.obs.ObservedAt.Equal(at) {
				drop = true
				break
			}
		}
		if !drop {
			kept = append(kept, c)
		}
	}
	t.resets = kept
}

// sameIncarnation reports whether next continues prev's server
// incarnation: the same server_uuid and server_id, and a gtid_executed
// that still contains prev's. An unparsable set is no proof (fail closed:
// a new episode and record).
func sameIncarnation(prev, next RoleObservation) bool {
	if prev.ServerUUID != next.ServerUUID || prev.ServerID != next.ServerID {
		return false
	}
	if prev.GTIDExecuted == next.GTIDExecuted {
		return true
	}
	p, err := internalmysql.ParseGTIDSet(prev.GTIDExecuted)
	if err != nil {
		return false
	}
	n, err := internalmysql.ParseGTIDSet(next.GTIDExecuted)
	if err != nil {
		return false
	}
	return n.Contains(p)
}

func (t *roleTimeline) snapshot() roleSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := roleSnapshot{epoch: t.epoch, state: t.state, complete: t.complete, firstSince: t.firstSince,
		readOnlyBefore: t.readOnlyBefore, readOnlyObserved: t.readOnlyObserved}
	if t.first != nil {
		first := *t.first
		s.first = &first
	}
	if t.confirm != nil {
		confirm := *t.confirm
		s.confirm = &confirm
	}
	return s
}

// promotionEpisode tracks the current writable episode (one timeline
// epoch). A record is due at the first writable observation after process
// start, after every read-only (or failed) observation and at a new server
// incarnation; uploads wait until it is confirmed.
type promotionEpisode struct {
	epoch    uint64
	pending  *PromotionRecord // the episode's record, kept across write retries
	recorded bool
	// confirmed: the record's confirmation is saved, or will never be
	// (confirmEpisode).
	confirmed bool
}

// probeRole runs the role probe, falling back to IsReadOnly for checkers
// that cannot report identity (complete=false), and times it.
func (a *BinlogArchiver) probeRole(ctx context.Context) (obs RoleObservation, complete bool, err error) {
	start := a.now()
	if p, ok := a.mysql.(roleProber); ok {
		obs, err = p.ProbeRole(ctx)
		complete = err == nil
	} else {
		var ro bool
		ro, err = a.mysql.IsReadOnly(ctx)
		obs = RoleObservation{ReadOnly: ro}
	}
	obs.ObservedAt = a.now()
	obs.Took = obs.ObservedAt.Sub(start)
	return obs, complete, err
}

// sampleRole probes the role and feeds the timeline. The role sampler
// goroutine calls it every PEER_CHECK_INTERVAL; it touches no archive
// state, so it never waits for archive I/O.
func (a *BinlogArchiver) sampleRole(ctx context.Context) (RoleObservation, bool, error) {
	obs, complete, err := a.probeRole(ctx)
	a.roles.observe(obs, complete, err)
	return obs, complete, err
}

// runRoleSampler samples the role every PEER_CHECK_INTERVAL until ctx ends.
func (a *BinlogArchiver) runRoleSampler(ctx context.Context) {
	interval := roleProbeInterval(a.cfg)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(ctx, interval)
			_, _, _ = a.sampleRole(probeCtx)
			cancel()
		}
	}
}

// syncEpisode aligns the scan goroutine's episode with the timeline: a
// read-only, failed or new-incarnation observation since the episode began
// ends it, before any session, integrity or record work can return.
func (a *BinlogArchiver) syncEpisode() roleSnapshot {
	snap := a.roles.snapshot()
	if snap.state != roleWritable || snap.epoch != a.episode.epoch {
		a.resetEpisode()
		a.episode.epoch = snap.epoch
	}
	return snap
}

// episodeCurrent refuses manifest writes of a recorded episode that the
// timeline has since ended, so no row is published under another
// episode's record; the next scan or tick starts the new episode. A
// read-only site's tail drain runs under no episode: its writes are refused
// once the timeline no longer shows the read-only server the drain was
// decided on, and the next scan, as whatever the server then is, continues.
func (a *BinlogArchiver) episodeCurrent() error {
	if a.tailUpload != nil {
		return a.roles.tailCurrent(a.tailUpload.obs)
	}
	if !a.episode.recorded {
		return nil
	}
	if snap := a.roles.snapshot(); snap.state != roleWritable || snap.epoch != a.episode.epoch {
		return errEpisodeEnded
	}
	return nil
}

// errEpisodeEnded: the role sampler saw the writable episode end while a
// scan was writing.
var errEpisodeEnded = errors.New("the writable episode ended during this scan (a read-only, failed or new-incarnation role observation); its writes wait for the next episode's promotion record")

// resetEpisode ends the current episode: the next writable observation
// starts a new one and writes a new record. An unknown role (a failed
// probe) also ends it, so a demotion and re-promotion between two probes
// that both fail is never mistaken for one episode.
func (a *BinlogArchiver) resetEpisode() {
	a.episode = promotionEpisode{}
	a.mu.Lock()
	a.promotionRecordedAt = time.Time{}
	a.mu.Unlock()
}

// observeRole writes the episode's record while the timeline is writable
// and the episode unrecorded. snap comes from syncEpisode. It returns the
// error that keeps uploads gated.
func (a *BinlogArchiver) observeRole(ctx context.Context, snap roleSnapshot) error {
	if snap.state != roleWritable || a.episode.recorded {
		return nil
	}
	if a.episode.pending == nil {
		if snap.first == nil {
			if !snap.complete {
				return fmt.Errorf("promotion record: %w", errRoleProbeUnsupported)
			}
			return fmt.Errorf("promotion record: no role probe of this writable episode returned within %s; the record waits for one that does", maxRoleProbeSpread)
		}
		if !a.startChecked {
			// The start-up check may find this very observation a new
			// incarnation; the record must carry that (recordResets).
			return fmt.Errorf("promotion record: %w", errStartCheckPending)
		}
		first := snap.first
		// The episode began after the previous observation's read; an
		// incarnation change seen at first (a reset candidate it covers)
		// may reach back further, to the site's latest record.
		since := snap.firstSince
		for _, c := range a.roles.pendingResets() {
			if c.covers(first) && (since.IsZero() || c.since.Before(since)) {
				since = c.since
			}
		}
		at := first.ObservedAt.UTC().Truncate(time.Microsecond)
		// The promotion set is derived once, here, and never delays the
		// record beyond its bound: a record without one is complete.
		promotionSet, why := a.promotionSet(ctx, snap, first)
		if promotionSet == "" {
			a.logger.Info("promotion set not recorded", "serverUuid", first.ServerUUID, "serverId", first.ServerID, "reason", why)
		}
		a.episode.pending = &PromotionRecord{
			ServerUUID:            first.ServerUUID,
			ServerID:              first.ServerID,
			GTIDExecuted:          first.GTIDExecuted,
			ObservedAt:            at,
			ProbeInterval:         a.probeInterval(at, since),
			ReadOnlyGTIDExecuted:  snap.readOnlyBefore,
			PromotionGTIDExecuted: promotionSet,
		}
	}
	if err := a.writePromotionRecord(ctx, *a.episode.pending); err != nil {
		return wrapKeepToken("promotion record", err)
	}
	rec := *a.episode.pending
	a.episode.recorded = true
	// The record is the fence for the change its observation showed.
	if snap.first != nil {
		a.roles.dropResets([]time.Time{snap.first.ObservedAt})
	}
	a.mu.Lock()
	a.promotionRecordedAt = rec.ObservedAt
	a.mu.Unlock()
	a.logger.Info("promotion recorded",
		"serverUuid", rec.ServerUUID, "serverId", rec.ServerID, "gtidExecuted", rec.GTIDExecuted,
		"promotionGtidExecuted", rec.PromotionGTIDExecuted)
	return nil
}

// promotionSetReadBound and promotionSetMaxFiles bound the local file
// reads of one promotion-set derivation: the record gates the episode's
// uploads and must not wait for a slow volume or a long run of files.
// Each header read is itself capped (localBinlogHeader: 1 MiB).
const (
	promotionSetReadBound = time.Second
	promotionSetMaxFiles  = 64
)

// promotionSetReader is how promotionSet reads local binlog headers;
// tests replace its parts.
type promotionSetReader struct {
	header func(path string) (binlogHeader, error) // nil: localBinlogHeader
	bound  time.Duration                           // 0: promotionSetReadBound
}

// promotionSet derives the promotion set P of the record that first opens
// (issue #269, PromotionRecord.PromotionGTIDExecuted): a set every member
// of which this server incarnation executed before any transaction under
// its own server_uuid that the read-only observation directly before the
// episode (G_ro, snap.readOnlyBefore) does not hold. It returns the set in
// canonical form, or "" and why there is none.
//
//	P = G_ro ∪ Previous_gtids(F*)
//
// G_ro is in P because gtid_executed only grows within one incarnation:
// whatever G_ro lacks was executed after it was read. F* is the newest
// local binlog file whose Previous_gtids hold no transaction under the
// server's own server_uuid (plain or tagged) beyond G_ro, and nothing
// first's gtid_executed lacks. A file's Previous_gtids are a subset of
// what the server had executed when it opened the file (on MySQL 9.7 they
// leave out the GTIDs that are only in mysql.gtid_executed, such as a
// clone's or an assigned gtid_purged; G_ro supplies those), and they hold
// every transaction the server itself had committed by then. Only that is
// used: every member was executed before the file was opened, and a
// transaction under the server's own server_uuid outside them was
// committed after it. That makes every boundary of this incarnation
// sound, and this choice the
// useful one: F* was opened before the episode's first own transaction,
// so all of the episode's own transactions are outside P. After an
// acknowledged promotion-time FLUSH BINARY LOGS, F* is the file it opened
// and P is the promotion GTID set. Without one F* is an older file and P
// is smaller; with none at all P is G_ro.
//
// What keeps a file of another incarnation out (binlog numbering restarts
// after a clone or a reset): G_ro and every observation from it to first
// must be one incarnation (roleTimeline.observe), with no incarnation
// change pending; the files from F* on must still be in
// the index, in order, when the reads are done; and a role probe taken
// after the reads must show first's incarnation, still writable, in the
// same epoch. A header that cannot be read (an encrypted or truncated
// file, a file a purge removed) ends the derivation: the file might be
// F*. The reads run on their own goroutine and are abandoned at the
// bound; the role probe itself is never touched.
func (a *BinlogArchiver) promotionSet(ctx context.Context, snap roleSnapshot, first *RoleObservation) (set, why string) {
	if !snap.readOnlyObserved {
		if snap.readOnlyBefore != "" {
			return "", "the server changed incarnation between the sidecar's last read-only observation and the episode"
		}
		return "", "the sidecar did not observe this server read-only directly before the episode"
	}
	if len(a.roles.pendingResets()) > 0 {
		return "", "the server changed incarnation and the change is not recorded yet"
	}
	gro, err := internalmysql.ParseGTIDSet(snap.readOnlyBefore)
	if err != nil {
		return "", fmt.Sprintf("the read-only gtid_executed is not a GTID set: %v", err)
	}
	recorded, err := internalmysql.ParseGTIDSet(first.GTIDExecuted)
	if err != nil {
		return "", fmt.Sprintf("the record's gtid_executed is not a GTID set: %v", err)
	}
	if !recorded.Contains(gro) {
		return "", "the record's gtid_executed does not contain the read-only observation's: another incarnation"
	}
	read := a.promotionSets.header
	if read == nil {
		read = localBinlogHeader
	}
	bound := a.promotionSets.bound
	if bound <= 0 {
		bound = promotionSetReadBound
	}
	type boundary struct {
		previous internalmysql.GTIDSet
		err      error
	}
	done := make(chan boundary, 1)
	dir, index, uuid, deadline := a.cfg.BinlogDir, a.cfg.BinlogIndex, first.ServerUUID, time.Now().Add(bound)
	go func() {
		previous, err := promotionBoundary(dir, index, uuid, gro, recorded, read, deadline)
		done <- boundary{previous, err}
	}()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	var previous internalmysql.GTIDSet
	select {
	case b := <-done:
		if b.err != nil {
			return "", b.err.Error()
		}
		previous = b.previous
	case <-timer.C:
		return "", fmt.Sprintf("reading the local binlog headers took longer than %s", bound)
	case <-ctx.Done():
		return "", ctx.Err().Error()
	}
	p := UnionBinlogGTIDs(gro, previous)
	if !recorded.Contains(p) {
		return "", "the binlog boundary holds transactions the record's gtid_executed lacks"
	}
	// The incarnation must be first's after the last file read.
	probeCtx, cancel := context.WithTimeout(ctx, maxRoleProbeSpread)
	obs, complete, err := a.sampleRole(probeCtx)
	cancel()
	switch now := a.roles.snapshot(); {
	case err != nil:
		return "", fmt.Sprintf("the role probe after the binlog headers were read failed: %v", err)
	case !complete || obs.ReadOnly || !sameIncarnation(*first, obs) || now.state != roleWritable || now.epoch != snap.epoch:
		return "", "the server left the episode or changed incarnation while the binlog headers were read"
	case len(a.roles.pendingResets()) > 0:
		return "", "the server changed incarnation while the binlog headers were read"
	case p.IsEmpty():
		// An empty set anchors nothing, and "" is the absent field.
		return "", "the promotion set is empty: the server had executed nothing before the episode"
	}
	return p.String(), ""
}

// promotionBoundary returns Previous_gtids(F*) for promotionSet: of the
// newest file in the binlog index, the active one included, whose
// Previous_gtids hold no transaction under uuid beyond gro and nothing
// recorded lacks. It returns an empty set when no file in the index
// qualifies (the boundary was purged), and an error when a header cannot
// be read, the bound is exceeded, or the files from F* on are no longer
// the index's when the reads are done.
func promotionBoundary(dir, index, uuid string, gro, recorded internalmysql.GTIDSet, read func(string) (binlogHeader, error), deadline time.Time) (internalmysql.GTIDSet, error) {
	indexPath := filepath.Join(dir, index)
	entries, err := readBinlogIndex(indexPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errors.New("the binlog index lists no file")
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if len(entries)-i > promotionSetMaxFiles {
			return nil, fmt.Errorf("none of the %d newest binlog files was opened before the episode's first transaction", promotionSetMaxFiles)
		}
		if !time.Now().Before(deadline) {
			return nil, errors.New("reading the local binlog headers exceeded its bound")
		}
		name := filepath.Base(entries[i])
		header, err := read(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read the header of %s: %w", name, err)
		}
		previous, err := internalmysql.ParseGTIDSet(header.previous)
		if err != nil {
			return nil, fmt.Errorf("Previous_gtids of %s: %w", name, err)
		}
		if hasSourceUUID(previous.Subtract(gro), uuid) || !recorded.Contains(previous) {
			continue
		}
		again, err := readBinlogIndex(indexPath)
		if err != nil {
			return nil, err
		}
		if !indexStillHolds(again, entries[i:]) {
			return nil, fmt.Errorf("the binlog index changed while %s was read", name)
		}
		return previous, nil
	}
	return internalmysql.GTIDSet{}, nil
}

// indexStillHolds reports whether index holds run as a contiguous run, in
// order. A rotation appends to the index and a purge removes from its
// front; a reset or a clone restarts it.
func indexStillHolds(index, run []string) bool {
	for i := range index {
		if filepath.Base(index[i]) != filepath.Base(run[0]) {
			continue
		}
		if len(index)-i < len(run) {
			return false
		}
		for k := range run {
			if filepath.Base(index[i+k]) != filepath.Base(run[k]) {
				return false
			}
		}
		return true
	}
	return false
}

// confirmEpisode adds the confirmation to the recorded episode's
// promotion record when that carries a promotion set (issue #269): once,
// from the timeline's confirming
// observation (roleTimeline.confirm), through the same manifest save with
// read-back as the record itself. It only runs for a recorded episode, so
// it never stands between an episode and its first upload; a failed save
// is retried on the next tick or scan, and saveManifest refuses it once
// the timeline has ended the episode. A record whose sidecar or server
// stops first simply has no confirmation.
func (a *BinlogArchiver) confirmEpisode(ctx context.Context, snap roleSnapshot) {
	if !a.episode.recorded || a.episode.confirmed || a.episode.pending == nil {
		return
	}
	if snap.state != roleWritable || snap.epoch != a.episode.epoch || snap.confirm == nil {
		return
	}
	rec := *a.episode.pending
	if rec.PromotionGTIDExecuted == "" {
		// A confirmation is evidence only beside a promotion set (the
		// planner reads it for nothing else), so a record without one gets
		// none: a field that is written must count.
		a.episode.confirmed = true
		return
	}
	// The epoch guarantees containment; an unparsable set proves nothing.
	recorded, err := internalmysql.ParseGTIDSet(rec.GTIDExecuted)
	if err != nil {
		a.episode.confirmed = true
		return
	}
	confirmed, err := internalmysql.ParseGTIDSet(snap.confirm.GTIDExecuted)
	if err != nil || !confirmed.Contains(recorded) || snap.confirm.ServerUUID != rec.ServerUUID || snap.confirm.ServerID != rec.ServerID {
		a.episode.confirmed = true
		return
	}
	at := snap.confirm.ObservedAt.UTC().Truncate(time.Microsecond)
	if at.Before(rec.ObservedAt.Add(a.roles.confirmDelay)) {
		// The timeline orders observations by the monotonic clock; the
		// record carries wall-clock times. A wall clock that stepped back
		// in between would date the confirmation before its delay, which a
		// reader takes for damaged evidence: write none.
		a.episode.confirmed = true
		return
	}
	added := confirmed.Subtract(recorded).String()
	m, err := a.loadManifest(ctx)
	if err != nil {
		a.logger.Debug("promotion record not confirmed yet", "error", err)
		return
	}
	i := promotionRecordIndex(m.Promotions, rec)
	if i < 0 {
		// The cap dropped the record; there is nothing to confirm.
		a.episode.confirmed = true
		return
	}
	if m.Promotions[i].ConfirmedAt.IsZero() {
		m.Promotions[i].ConfirmedAt, m.Promotions[i].ConfirmedGTIDAdded = at, added
		if err := a.saveManifest(ctx, m); err != nil {
			a.logger.Debug("promotion record not confirmed yet", "error", err)
			return
		}
		if i = promotionRecordIndex(m.Promotions, rec); i < 0 || !m.Promotions[i].ConfirmedAt.Equal(at) || m.Promotions[i].ConfirmedGTIDAdded != added {
			a.logger.Debug("promotion record not confirmed yet", "error", "the saved manifest does not hold the confirmation")
			return
		}
	}
	saved := m.Promotions[i]
	a.episode.pending.ConfirmedAt, a.episode.pending.ConfirmedGTIDAdded = saved.ConfirmedAt, saved.ConfirmedGTIDAdded
	a.episode.confirmed = true
	a.logger.Info("promotion record confirmed", "serverUuid", saved.ServerUUID, "serverId", saved.ServerID,
		"observedAt", saved.ObservedAt, "confirmedAt", saved.ConfirmedAt, "confirmedGtidAdded", saved.ConfirmedGTIDAdded)
}

// promotionRecordIndex is the index of rec in records (hasPromotionRecord's
// identity), or -1.
func promotionRecordIndex(records []PromotionRecord, rec PromotionRecord) int {
	for i, r := range records {
		if r.Kind == rec.Kind && r.ServerUUID == rec.ServerUUID && r.ServerID == rec.ServerID && r.GTIDExecuted == rec.GTIDExecuted && r.ObservedAt.Equal(rec.ObservedAt) {
			return i
		}
	}
	return -1
}

// writePromotionRecord appends rec to this site's manifest through the
// single-writer save with read-back, and succeeds only when the saved
// manifest holds rec. A retry after a save whose read-back failed does not
// append the same record twice.
func (a *BinlogArchiver) writePromotionRecord(ctx context.Context, rec PromotionRecord) error {
	m, err := a.loadManifest(ctx)
	if err != nil {
		return err
	}
	if hasPromotionRecord(m.Promotions, rec) {
		return nil
	}
	m.AppendPromotionRecord(rec)
	if err := a.saveManifest(ctx, m); err != nil {
		return err
	}
	// SaveManifest merges and caps the records it writes; the episode is
	// recorded only if this record survived into the saved manifest.
	if !hasPromotionRecord(m.Promotions, rec) {
		return fmt.Errorf("the saved manifest for %s does not hold the promotion record observed at %s", a.site, rec.ObservedAt.Format(time.RFC3339Nano))
	}
	return nil
}

func hasPromotionRecord(records []PromotionRecord, rec PromotionRecord) bool {
	return promotionRecordIndex(records, rec) >= 0
}

// errStartCheckPending: the start-up incarnation check against the
// site's latest record has not run yet (it needs a verified archive
// session and an observation that reported the server identity).
var errStartCheckPending = errors.New("the start-up incarnation check against this site's latest record has not run yet")

// probeInterval is a record's ProbeInterval: from since (exclusive; the
// earliest time the change it reports can have happened) to at, rounded
// up to the microsecond, and never less than the configured probe
// interval. A zero since (no earlier observation) gives the configured
// interval.
func (a *BinlogArchiver) probeInterval(at, since time.Time) pitrarchive.Duration {
	floor := roleProbeInterval(a.cfg)
	d := time.Duration(0)
	if !since.IsZero() {
		d = at.Sub(since)
		if r := d % time.Microsecond; r != 0 {
			d += time.Microsecond - r
		}
	}
	return pitrarchive.Duration(max(d, floor))
}

// covers reports whether c is the change first showed: the episode's
// promotion record, which holds first, is then its fence.
func (c resetCandidate) covers(first *RoleObservation) bool {
	return first != nil && c.epoch != 0 && c.obs.ObservedAt.Equal(first.ObservedAt)
}

// recordResets writes the incarnation-reset records (N3 revised). On the
// first call with a verified session it checks this process's first
// identified observation against the site's latest record, so a reclone,
// reset or replaced datadir while the sidecar was not running is seen,
// with a window reaching back to that record. Every in-process change
// (roleTimeline.observe) is queued as it is observed, in any role. A
// change that opened a writable episode at the episode's first
// observation is left to that episode's promotion record while the
// episode lasts. It returns errStartCheckPending until the start-up check
// ran, and the write error while candidates remain; either keeps uploads
// waiting.
func (a *BinlogArchiver) recordResets(ctx context.Context, snap roleSnapshot) error {
	if a.session == nil || !a.session.Verified() {
		if a.startChecked && len(a.roles.pendingResets()) == 0 {
			return nil
		}
		return fmt.Errorf("incarnation reset record: %w", errStartCheckPending)
	}
	var m *Manifest
	if !a.startChecked {
		start, epoch := a.roles.startObservation()
		if start == nil {
			return fmt.Errorf("incarnation reset record: %w", errStartCheckPending)
		}
		var err error
		if m, err = a.loadManifest(ctx); err != nil {
			return wrapKeepToken("incarnation reset record", err)
		}
		if n := len(m.Promotions); n > 0 {
			latest := m.Promotions[n-1]
			prev := RoleObservation{ServerUUID: latest.ServerUUID, ServerID: latest.ServerID, GTIDExecuted: latest.GTIDExecuted}
			if !sameIncarnation(prev, *start) {
				a.roles.queueStartReset(resetCandidate{obs: *start, since: latest.ObservedAt, epoch: epoch})
				a.logger.Warn("server incarnation changed while the sidecar was not running",
					"latestRecordAt", latest.ObservedAt, "serverUuid", start.ServerUUID, "serverId", start.ServerID)
			}
		}
		a.startChecked = true
	}
	var due []resetCandidate
	for _, c := range a.roles.pendingResets() {
		if snap.state == roleWritable && c.epoch == snap.epoch && c.covers(snap.first) {
			continue
		}
		due = append(due, c)
	}
	if len(due) == 0 {
		return nil
	}
	if m == nil {
		var err error
		if m, err = a.loadManifest(ctx); err != nil {
			return wrapKeepToken("incarnation reset record", err)
		}
	}
	recs := make([]PromotionRecord, 0, len(due))
	for _, c := range due {
		at := c.obs.ObservedAt.UTC().Truncate(time.Microsecond)
		rec := PromotionRecord{Kind: pitrarchive.RecordReset, ServerUUID: c.obs.ServerUUID, ServerID: c.obs.ServerID,
			GTIDExecuted: c.obs.GTIDExecuted, ObservedAt: at, ProbeInterval: a.probeInterval(at, c.since)}
		recs = append(recs, rec)
		if !hasPromotionRecord(m.Promotions, rec) {
			m.AppendPromotionRecord(rec)
		}
	}
	if err := a.saveManifest(ctx, m); err != nil {
		return wrapKeepToken("incarnation reset record", err)
	}
	done := make([]time.Time, 0, len(due))
	for i, rec := range recs {
		if !hasPromotionRecord(m.Promotions, rec) {
			return fmt.Errorf("incarnation reset record: the saved manifest for %s does not hold the reset record observed at %s", a.site, rec.ObservedAt.Format(time.RFC3339Nano))
		}
		done = append(done, due[i].obs.ObservedAt)
		a.logger.Warn("server incarnation reset recorded", "serverUuid", rec.ServerUUID, "serverId", rec.ServerID,
			"gtidExecuted", rec.GTIDExecuted, "observedAt", rec.ObservedAt, "probeInterval", time.Duration(rec.ProbeInterval))
	}
	a.roles.dropResets(done)
	return nil
}

// roleTick probes the role and then runs recordTick. Run does not call
// it: its role sampler goroutine probes, and its PEER_CHECK_INTERVAL tick
// runs recordTick.
func (a *BinlogArchiver) roleTick(ctx context.Context) {
	_, _, _ = a.sampleRole(ctx)
	a.recordTick(ctx)
}

// recordTick records a promotion promptly instead of waiting for the next
// scan, confirms a recorded one, and ends the episode the timeline ended;
// the scan reports errors.
func (a *BinlogArchiver) recordTick(ctx context.Context) {
	snap := a.syncEpisode()
	// Establishing or re-opening the archive is the scan's job; the tick
	// only records on a verified session, so a refused archive is not
	// listed every PEER_CHECK_INTERVAL.
	if a.session == nil || !a.session.Verified() {
		return
	}
	if err := a.recordResets(ctx, snap); err != nil {
		a.logger.Debug("incarnation reset record not written yet", "error", err)
	}
	if snap.state != roleWritable {
		return
	}
	if !a.episode.recorded {
		if err := a.observeRole(ctx, snap); err != nil {
			a.logger.Debug("promotion record not written yet", "error", err)
			return
		}
	}
	a.confirmEpisode(ctx, snap)
}

// promotionConfirmDelay is the configured confirmation delay.
func promotionConfirmDelay(cfg *PITRConfig) time.Duration {
	if cfg.PromotionConfirmDelay > 0 {
		return cfg.PromotionConfirmDelay
	}
	return pitrarchive.PromotionConfirmDelay
}

func roleProbeInterval(cfg *PITRConfig) time.Duration {
	if cfg.RoleProbeInterval > 0 {
		return cfg.RoleProbeInterval
	}
	return defaultPeerCheckInterval
}
