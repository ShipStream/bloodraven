package sidecar

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ArchiveStoreFactory builds the PITR archive store. NewArchiveStore in
// production.
type ArchiveStoreFactory func(ctx context.Context, cfg *PITRConfig) (ArchiveStore, error)

const (
	// archiveStoreRetryInitial and archiveStoreRetryMax bound the delay
	// between archive store construction attempts. The causes are
	// configuration (an unreadable passphrase or credentials file, a
	// missing mount), so retrying faster gains nothing; retrying at all
	// lets a fixed Secret projection recover without a restart.
	archiveStoreRetryInitial = 5 * time.Second
	archiveStoreRetryMax     = 5 * time.Minute
)

// ArchiverSupervisor owns the PITR archiver's lifecycle inside the
// sidecar. It retries archive store construction with capped, jittered
// exponential backoff and starts exactly one BinlogArchiver once a store
// exists.
//
// A store that cannot be built must never take the sidecar down: the
// same process serves readiness, the startup safety net and self-fencing,
// and losing those over an archive misconfiguration removes the pod from
// every Service. Until the store is ready, Snapshot reports archival as
// configured but stopped (enabled, storeReady=false, the init error) so
// the operator can tell it apart from a site without PITR.
//
// The supervisor is the server's archiver status source for the whole
// process lifetime; its state is guarded by mu, so handlers never see an
// unsynchronised swap.
type ArchiverSupervisor struct {
	cfg      *PITRConfig
	site     string
	mysql    roleChecker
	newStore ArchiveStoreFactory
	logger   *slog.Logger

	// configure runs on the archiver after construction and before it
	// starts, for wiring that needs the archiver (the retention client).
	configure func(*BinlogArchiver)

	// Test seams. wait sleeps for d unless ctx ends first and reports
	// whether the full delay elapsed; runArchiver runs the archiver
	// until ctx ends.
	wait        func(ctx context.Context, d time.Duration) bool
	jitter      func(d time.Duration) time.Duration
	runArchiver func(ctx context.Context, a *BinlogArchiver)

	mu       sync.Mutex
	archiver *BinlogArchiver
	initErr  string
	failures int64
}

// NewArchiverSupervisor builds a supervisor. It starts nothing; call Run.
func NewArchiverSupervisor(cfg *PITRConfig, site string, mysql roleChecker, newStore ArchiveStoreFactory, logger *slog.Logger) *ArchiverSupervisor {
	if newStore == nil {
		newStore = NewArchiveStore
	}
	return &ArchiverSupervisor{
		cfg:         cfg,
		site:        site,
		mysql:       mysql,
		newStore:    newStore,
		logger:      logger,
		wait:        waitOrDone,
		jitter:      archiveStoreRetryJitter,
		runArchiver: func(ctx context.Context, a *BinlogArchiver) { a.Run(ctx) },
	}
}

// SetConfigure registers wiring to apply to the archiver before it
// starts. Call before Run.
func (s *ArchiverSupervisor) SetConfigure(fn func(*BinlogArchiver)) { s.configure = fn }

// Run builds the store, retrying until it succeeds or ctx ends, then runs
// the archiver until ctx ends.
func (s *ArchiverSupervisor) Run(ctx context.Context) {
	delay := archiveStoreRetryInitial
	var (
		firstFailure time.Time
		lastLogged   string
	)
	for attempt := 1; ; attempt++ {
		store, err := s.newStore(ctx, s.cfg)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			a := NewBinlogArchiver(s.cfg, s.site, s.mysql, store, s.logger)
			if s.configure != nil {
				s.configure(a)
			}
			s.mu.Lock()
			s.archiver = a
			s.initErr = ""
			s.mu.Unlock()
			if attempt > 1 {
				s.logger.Info("archive store initialized after retries",
					"attempts", attempt, "failingFor", time.Since(firstFailure).Round(time.Second))
			}
			s.runArchiver(ctx, a)
			return
		}

		s.mu.Lock()
		s.failures++
		s.initErr = err.Error()
		s.mu.Unlock()
		if firstFailure.IsZero() {
			firstFailure = time.Now()
		}
		retryIn := s.jitter(delay)
		// ERROR when the cause is new, WARN while it repeats. The
		// backoff caps the rate at one line per archiveStoreRetryMax.
		level := slog.LevelWarn
		if err.Error() != lastLogged {
			level = slog.LevelError
			lastLogged = err.Error()
		}
		s.logger.Log(ctx, level, "failed to init archive store",
			"error", err, "attempt", attempt, "retryIn", retryIn)
		if !s.wait(ctx, retryIn) {
			return
		}
		delay = min(delay*2, archiveStoreRetryMax)
	}
}

// Snapshot reports the archiver's status. Before the store exists it
// returns the configured identity, storeReady=false, the latest init
// error and the failed attempt count. The init error lives here rather
// than in the archiver, whose scan clears lastError on read-only sites.
func (s *ArchiverSupervisor) Snapshot(ctx context.Context) Status {
	s.mu.Lock()
	a, initErr, failures := s.archiver, s.initErr, s.failures
	s.mu.Unlock()

	ready := a != nil
	if ready {
		st := a.Snapshot(ctx)
		st.StoreReady = &ready
		// Keep the counter monotonic across the transition.
		st.UploadFailures += failures
		return st
	}
	primary := false
	if ro, err := s.mysql.IsReadOnly(ctx); err == nil {
		primary = !ro
	}
	st := Status{
		Enabled:        true,
		StoreReady:     &ready,
		Primary:        primary,
		StorageType:    s.cfg.StorageType,
		ManifestPrefix: s.cfg.ManifestPrefix,
		Site:           s.site,
		UploadFailures: failures,
	}
	if initErr != "" {
		st.LastError = "archive store init: " + initErr
	}
	return st
}

// archiveStoreRetryJitter returns a delay in [0.8d, d], so sidecars that
// failed together do not retry in lockstep and the cap is never exceeded.
func archiveStoreRetryJitter(d time.Duration) time.Duration {
	spread := int64(d / 5)
	if spread <= 0 {
		return d
	}
	return d - time.Duration(rand.Int64N(spread+1))
}

func waitOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
