package sidecar

import (
	"context"
	"fmt"
)

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

func (a *BinlogArchiver) retiredBinlogIdentities(m *Manifest) map[binlogIdentity]struct{} {
	retired := make(map[binlogIdentity]struct{}, len(m.Quarantined))
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
	m, err := loadManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site)
	if err != nil {
		return
	}
	a.updateManifestAggregates(m.Files)
}
