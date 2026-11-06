package sidecar

import (
	"context"
	"errors"
	"fmt"
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
// clock-skew margin δ (pitrplan.DefaultClockSkewMargin, 5s) on the gap
// between the GTID snapshot and its time; a slower one waits for the next.
const maxRoleProbeSpread = time.Second

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
	// none). It is recorded, not used by the planner (N5).
	readOnly       *RoleObservation
	readOnlyBefore string
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
	epoch          uint64
	state          roleState
	complete       bool
	first          *RoleObservation
	firstSince     time.Time
	readOnlyBefore string
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
		t.state, t.readOnly, t.first = roleUnknown, nil, nil
	case obs.ReadOnly:
		ro := obs
		t.state, t.readOnly, t.first = roleReadOnly, &ro, nil
	default:
		if t.state != roleWritable || !complete || !t.complete || !sameIncarnation(t.last, obs) {
			before := ""
			if t.state == roleReadOnly && complete && t.readOnly != nil &&
				t.readOnly.ServerUUID == obs.ServerUUID && t.readOnly.ServerID == obs.ServerID {
				before = t.readOnly.GTIDExecuted
			}
			t.epoch++
			t.first, t.readOnlyBefore = nil, before
		}
		t.state, t.last, t.complete, t.readOnly = roleWritable, obs, complete, nil
		if t.first == nil && identified && obs.Took <= maxRoleProbeSpread {
			first := obs
			t.first, t.firstSince = &first, prevStart
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
	s := roleSnapshot{epoch: t.epoch, state: t.state, complete: t.complete, firstSince: t.firstSince, readOnlyBefore: t.readOnlyBefore}
	if t.first != nil {
		first := *t.first
		s.first = &first
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
// episode's record; the next scan or tick starts the new episode.
func (a *BinlogArchiver) episodeCurrent() error {
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
		a.episode.pending = &PromotionRecord{
			ServerUUID:           first.ServerUUID,
			ServerID:             first.ServerID,
			GTIDExecuted:         first.GTIDExecuted,
			ObservedAt:           at,
			ProbeInterval:        a.probeInterval(at, since),
			ReadOnlyGTIDExecuted: snap.readOnlyBefore,
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
		"serverUuid", rec.ServerUUID, "serverId", rec.ServerID, "gtidExecuted", rec.GTIDExecuted)
	return nil
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
	for _, r := range records {
		if r.Kind == rec.Kind && r.ServerUUID == rec.ServerUUID && r.ServerID == rec.ServerID && r.GTIDExecuted == rec.GTIDExecuted && r.ObservedAt.Equal(rec.ObservedAt) {
			return true
		}
	}
	return false
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
// scan, and ends the episode the timeline ended; the scan reports errors.
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
	if snap.state != roleWritable || a.episode.recorded {
		return
	}
	if err := a.observeRole(ctx, snap); err != nil {
		a.logger.Debug("promotion record not written yet", "error", err)
	}
}

func roleProbeInterval(cfg *PITRConfig) time.Duration {
	if cfg.RoleProbeInterval > 0 {
		return cfg.RoleProbeInterval
	}
	return defaultPeerCheckInterval
}
