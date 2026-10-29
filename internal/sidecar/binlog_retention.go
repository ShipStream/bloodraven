package sidecar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// retryPrunedDeletes executes previously persisted retirement decisions, even
// when the operator cannot provide a new retention proof. Acknowledged records
// retain the retired content identity.
func (a *BinlogArchiver) retryPrunedDeletes(ctx context.Context, m *Manifest) (int, error) {
	liveKeys := make(map[string]struct{}, len(m.Files))
	for _, entry := range m.Files {
		if key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, entry); err == nil {
			liveKeys[key] = struct{}{}
		}
	}
	deleted := 0
	for i := range m.Pruned {
		p := &m.Pruned[i]
		if p.Deleted {
			continue
		}
		key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, ManifestEntry{Name: p.Name, ID: p.ID})
		contentID := p.ID
		if contentID == "" {
			contentID = p.ContentID
		}
		if err == nil {
			_, err = ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, ManifestEntry{Name: p.Name, ID: contentID})
			if contentID == "" {
				err = fmt.Errorf("missing retired content identity for %s", p.Name)
			}
		}
		if err != nil {
			a.logger.Warn("retention: delete object", "key", p.Key, "error", err)
			continue
		}
		if _, live := liveKeys[key]; live {
			a.logger.Warn("retention: delete object", "key", key, "error", fmt.Errorf("object remains referenced by the live manifest"))
			continue
		}
		// Key is diagnostic metadata; only the validated name and original ID select the object.
		if err := a.store.Delete(ctx, key); err != nil {
			a.logger.Warn("retention: delete object", "key", key, "error", err)
			continue
		}
		p.Key = key
		p.Deleted = true
		deleted++
	}
	if deleted > 0 {
		if err := saveManifest(ctx, a.store, a.cfg.ManifestPrefix, m); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

// retiredBinlogIdentities includes both pending and acknowledged retirements.
func (a *BinlogArchiver) retiredBinlogIdentities(m *Manifest) map[binlogIdentity]struct{} {
	retired := make(map[binlogIdentity]struct{}, len(m.Pruned))
	for _, p := range m.Pruned {
		id := p.ID
		if id == "" {
			id = p.ContentID
		}
		if id == "" {
			continue
		}
		if _, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, ManifestEntry{Name: p.Name, ID: id}); err != nil {
			continue
		}
		retired[binlogIdentity{Name: p.Name, ID: id}] = struct{}{}
	}
	for _, q := range m.Quarantined {
		id := q.Entry.ID
		if id == "" {
			id = q.Entry.ContentID
		}
		if id != "" {
			retired[binlogIdentity{Name: q.Entry.Name, ID: id}] = struct{}{}
		}
	}
	return retired
}

// maintainArchive runs on every site, including scans that cannot upload.
func (a *BinlogArchiver) maintainArchive(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if manifest, err := loadManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site); err == nil {
		if err := a.auditArchive(ctx, manifest); err != nil {
			a.historyAuditError = err.Error()
			a.setError(err.Error())
			a.incUploadFailures()
			a.logger.Warn("archive binlog", "file", a.site, "error", err)
		} else if a.historyAuditError != "" {
			a.mu.Lock()
			if a.lastError == a.historyAuditError {
				a.lastError = ""
			}
			a.mu.Unlock()
			a.historyAuditError = ""
		}
	} else if a.historyAuditError != "" {
		// An unreadable manifest cannot establish that the prior failure resolved.
		a.setError(a.historyAuditError)
	}
	a.maybeRunRetention(ctx)
	m, err := loadManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site)
	if err != nil {
		return
	}
	a.compactRetirementRecords(ctx, m)
	a.updateManifestAggregates(m.Files)
}

// compactRetirementRecords forgets acknowledged records only after their local
// file is absent from both a readable index and the filesystem.
func (a *BinlogArchiver) compactRetirementRecords(ctx context.Context, m *Manifest) {
	if len(m.Pruned) == 0 {
		return
	}
	indexPath := filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex)
	info, err := os.Stat(indexPath)
	if err != nil || !info.Mode().IsRegular() {
		return
	}
	entries, err := readBinlogIndex(indexPath)
	if err != nil {
		return
	}
	indexed := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		indexed[filepath.Base(entry)] = struct{}{}
	}
	kept := m.Pruned[:0]
	removed := false
	for _, p := range m.Pruned {
		_, listed := indexed[p.Name]
		if p.Deleted && !listed && isPlainFileName(p.Name) {
			_, err := os.Lstat(filepath.Join(a.cfg.BinlogDir, p.Name))
			if os.IsNotExist(err) {
				removed = true
				continue
			}
		}
		kept = append(kept, p)
	}
	if !removed {
		return
	}
	m.Pruned = kept
	if err := saveManifest(ctx, a.store, a.cfg.ManifestPrefix, m); err != nil {
		a.logger.Warn("retention: compact retirement records", "error", err)
	}
}
