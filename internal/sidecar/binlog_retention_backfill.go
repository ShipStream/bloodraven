package sidecar

import (
	"context"
	"time"
)

const (
	maxRetentionBackfillFiles       = 8
	maxRetentionBackfillBytes int64 = 64 << 20
	maxRetentionBackfillTime        = 2 * time.Second
)

func coverageBoundToEntry(entry ManifestEntry) bool {
	id := entry.ID
	if id == "" {
		id = entry.ContentID
	}
	return id != "" && entry.GTIDCoverage != nil && entry.GTIDCoverage.ContentID == id
}

type retentionInspectionIdentity struct {
	Name, ID, ContentID string
}
type retentionBackfillResult struct {
	pending  bool // Eligible work remains after this bounded batch.
	progress bool // New evidence was inspected, even if its save needs retrying.
}

func inspectionIdentity(entry ManifestEntry) retentionInspectionIdentity {
	return retentionInspectionIdentity{entry.Name, entry.ID, entry.ContentID}
}
func needsRetentionInspection(entry ManifestEntry, cutoff time.Time) bool {
	return !entry.LastEventTime.IsZero() && entry.LastEventTime.Before(cutoff) && !coverageBoundToEntry(entry)
}

// backfillRetentionCoverage persists one bounded batch. Failed identities wait
// the normal retention interval while unrelated rows can keep making progress.
func (a *BinlogArchiver) backfillRetentionCoverage(ctx context.Context, m *Manifest, cutoff time.Time) (retentionBackfillResult, error) {
	result := retentionBackfillResult{}
	live := make(map[retentionInspectionIdentity]bool, len(m.Files))
	for _, f := range m.Files {
		if needsRetentionInspection(f, cutoff) {
			live[inspectionIdentity(f)] = true
		}
	}
	for key := range a.retentionInspectionFailures {
		if !live[key] {
			delete(a.retentionInspectionFailures, key)
		}
	}
	if len(m.Files) == 0 {
		return result, nil
	}
	started := time.Now()
	reads := 0
	var readBytes int64
	start := a.retentionNextInspection % len(m.Files)
	for offset := 0; offset < len(m.Files); offset++ {
		index := (start + offset) % len(m.Files)
		f := &m.Files[index]
		key := inspectionIdentity(*f)
		if !needsRetentionInspection(*f, cutoff) || time.Now().Before(a.retentionInspectionFailures[key]) {
			continue
		}
		if reads > 0 && (reads >= maxRetentionBackfillFiles || readBytes >= maxRetentionBackfillBytes || time.Since(started) >= maxRetentionBackfillTime) {
			a.retentionNextInspection = index
			break
		}
		reads++
		a.retentionNextInspection = (index + 1) % len(m.Files)
		inspectedSize := f.Size
		err := a.inspectArchivedBinlog(ctx, *f, func(evidence BinlogEvidence) error {
			id := f.ID
			if id == "" {
				id = a.store.BinlogContentID(evidence.Digest)
			}
			meta := evidence.Metadata
			inspectedSize = meta.Size
			meta.GTIDCoverage.ContentID = id
			f.GTIDCoverage = meta.GTIDCoverage
			f.Size = meta.Size
			f.FirstEventTime = meta.FirstEventTime
			f.LastEventTime = meta.LastEventTime
			f.PreviousGTIDs = meta.PreviousGTIDs
			if f.ID == "" {
				f.ContentID = id
			}
			result.progress = true
			return nil
		})
		readBytes += max(inspectedSize, 0)
		if err != nil {
			if a.retentionInspectionFailures == nil {
				a.retentionInspectionFailures = make(map[retentionInspectionIdentity]time.Time)
			}
			delay := time.Hour
			if a.retentionCfg != nil && a.retentionCfg.Interval > 0 {
				delay = a.retentionCfg.Interval
			}
			a.retentionInspectionFailures[key] = time.Now().Add(delay)
			a.logger.Warn("retention: coverage unavailable", "file", f.Name, "error", err)
		} else {
			delete(a.retentionInspectionFailures, key)
		}
	}
	for _, f := range m.Files {
		if needsRetentionInspection(f, cutoff) && !time.Now().Before(a.retentionInspectionFailures[inspectionIdentity(f)]) {
			result.pending = true
			break
		}
	}
	if result.progress {
		return result, saveManifest(ctx, a.store, a.cfg.ManifestPrefix, m)
	}
	return result, nil
}
