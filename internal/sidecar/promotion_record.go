package sidecar

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RoleObservation is one role probe of the local server (D-43).
type RoleObservation struct {
	ReadOnly     bool
	ServerUUID   string
	ServerID     uint32
	GTIDExecuted string
}

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

// promotionEpisode tracks the current writable episode. A record is due at
// the first writable observation after process start and after every
// read-only (or failed) observation; uploads wait until it is confirmed.
type promotionEpisode struct {
	writable bool
	pending  *PromotionRecord // the episode's first writable observation
	recorded bool
}

// probeRole runs the role probe, falling back to IsReadOnly for checkers
// that cannot report identity (complete=false).
func (a *BinlogArchiver) probeRole(ctx context.Context) (obs RoleObservation, complete bool, err error) {
	if p, ok := a.mysql.(roleProber); ok {
		obs, err = p.ProbeRole(ctx)
		return obs, err == nil, err
	}
	ro, err := a.mysql.IsReadOnly(ctx)
	return RoleObservation{ReadOnly: ro}, false, err
}

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

// observeRole advances the episode with one observation and, while
// writable and unrecorded, writes the episode's record. It returns the
// error that keeps uploads gated.
func (a *BinlogArchiver) observeRole(ctx context.Context, obs RoleObservation, complete bool) error {
	if obs.ReadOnly {
		a.resetEpisode()
		return nil
	}
	a.episode.writable = true
	if a.episode.recorded {
		return nil
	}
	if a.episode.pending == nil {
		if !complete || obs.ServerUUID == "" {
			return fmt.Errorf("promotion record: %w", errRoleProbeUnsupported)
		}
		a.episode.pending = &PromotionRecord{
			ServerUUID:   obs.ServerUUID,
			ServerID:     obs.ServerID,
			GTIDExecuted: obs.GTIDExecuted,
			ObservedAt:   a.now().UTC().Truncate(time.Microsecond),
		}
	}
	if err := a.writePromotionRecord(ctx, *a.episode.pending); err != nil {
		return fmt.Errorf("promotion record: %w", err)
	}
	rec := *a.episode.pending
	a.episode.recorded = true
	a.mu.Lock()
	a.promotionRecordedAt = rec.ObservedAt
	a.mu.Unlock()
	a.logger.Info("promotion recorded",
		"serverUuid", rec.ServerUUID, "serverId", rec.ServerID, "gtidExecuted", rec.GTIDExecuted)
	return nil
}

// writePromotionRecord appends rec to this site's manifest through the
// single-writer save with read-back. A retry after a save whose read-back
// failed does not append the same record twice.
func (a *BinlogArchiver) writePromotionRecord(ctx context.Context, rec PromotionRecord) error {
	m, err := a.loadManifest(ctx)
	if err != nil {
		return err
	}
	for _, r := range m.Promotions {
		if r.ServerUUID == rec.ServerUUID && r.ServerID == rec.ServerID && r.GTIDExecuted == rec.GTIDExecuted && r.ObservedAt.Equal(rec.ObservedAt) {
			return nil
		}
	}
	m.AppendPromotionRecord(rec)
	return a.saveManifest(ctx, m)
}

// roleTick is the PEER_CHECK_INTERVAL role probe. It records a promotion
// promptly instead of waiting for the next scan, and ends the episode on a
// read-only or failed observation; the scan reports errors.
func (a *BinlogArchiver) roleTick(ctx context.Context) {
	obs, complete, err := a.probeRole(ctx)
	if err != nil {
		a.resetEpisode()
		return
	}
	// Establishing or re-opening the archive is the scan's job; the tick
	// only records on a verified session, so a refused archive is not
	// listed every PEER_CHECK_INTERVAL.
	if !obs.ReadOnly && !a.episode.recorded && (a.session == nil || !a.session.Verified()) {
		return
	}
	if err := a.observeRole(ctx, obs, complete); err != nil {
		a.logger.Debug("promotion record not written yet", "error", err)
	}
}

func roleProbeInterval(cfg *PITRConfig) time.Duration {
	if cfg.RoleProbeInterval > 0 {
		return cfg.RoleProbeInterval
	}
	return defaultPeerCheckInterval
}
