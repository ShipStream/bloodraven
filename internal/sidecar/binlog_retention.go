package sidecar

import "context"

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
