package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// These adapters exercise legacy per-site planner tests, without certifying
// archive-wide omission, anchoring, or chronological safety.
func parsePITRStopTime(s string) (time.Time, error) { return backupmeta.ParsePITRStopTime(s) }

// discoverSites exercises legacy manifest inventory parsing in tests.
func discoverSites(ctx context.Context, store sidecar.ArchiveStore, prefix string) ([]string, error) {
	keys, err := store.List(ctx, sidecar.ManifestKeyPrefix(prefix))
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var sites []string
	for _, k := range keys {
		site := sidecar.SiteFromManifestKey(k)
		if site == "" {
			continue
		}
		if _, dup := seen[site]; dup {
			continue
		}
		seen[site] = struct{}{}
		sites = append(sites, site)
	}
	return sites, nil
}

// downloadSiteBinlogs validates one site's causal replay window and stages its
// prefix through a confirmed stop barrier under unique ordinal names. Metadata
// hints never independently omit a file; START_GTIDS optionally supplies the
// trusted backup lower bound. Every selected stream is checked before publication.
func downloadSiteBinlogs(
	ctx context.Context,
	store sidecar.ArchiveStore,
	prefix, site string,
	stopTime time.Time,
	outDir string,
	logger *slog.Logger,
) (int, error) {
	if !isSafeBasename(site) {
		return 0, fmt.Errorf("reject unsafe site name %q", site)
	}
	return downloadPITRWindow(ctx, store, prefix, site, stopTime, outDir, os.Getenv("BLOODRAVEN_PITR_START_GTIDS"), logger)
}

func downloadPITRWindow(ctx context.Context, store sidecar.ArchiveStore, prefix, site string, stop time.Time, outDir, start string, logger *slog.Logger) (int, error) {
	result, err := planPITRWindow(ctx, store, prefix, site, stop, outDir, start, logger)
	return len(result.Staged), err
}
