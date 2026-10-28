package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

type binlogStateReader interface {
	BinlogState(context.Context) (string, string, error)
}

type binlogBootstrapReader interface {
	BinlogHistoryReady(context.Context) (bool, error)
}

// BinlogHistoryReady excludes a clone recipient and a read-only server with no
// replication channel. Such a server has not yet established archive lineage.
func (m *LiveMysql) BinlogHistoryReady(ctx context.Context) (bool, error) {
	cloning, err := m.CloneInProgress(ctx)
	if err != nil || cloning {
		return false, err
	}
	readOnly, err := m.IsReadOnly(ctx)
	if err != nil {
		return false, err
	}
	if !readOnly {
		return true, nil
	}
	rows, err := m.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	configured := rows.Next()
	return configured, rows.Err()
}

type binlogBaseReader interface {
	BinlogBase(context.Context) (string, error)
}

func (m *LiveMysql) BinlogBase(ctx context.Context) (string, error) {
	var base string
	err := m.db.QueryRowContext(ctx, "SELECT @@global.gtid_purged").Scan(&base)
	return base, err
}

func (m *LiveMysql) BinlogState(ctx context.Context) (string, string, error) {
	var uuid, gtids string
	err := m.db.QueryRowContext(ctx, "SELECT @@server_uuid, @@global.gtid_executed").Scan(&uuid, &gtids)
	return uuid, gtids, err
}
func observedArchiveGTIDs(entry ManifestEntry) string {
	if entry.GTIDCoverage != nil {
		return entry.GTIDCoverage.GTIDs
	}
	return ""
}
func (a *BinlogArchiver) noteArchiveEpoch(ctx context.Context, m *Manifest, entry ManifestEntry) error {
	// GTID source UUIDs and verified issuance are intrinsic to the archived
	// bytes. Current server state is used by the separate divergence audit.
	return a.detectArchiveReuse(ctx, m, entry)
}
func (a *BinlogArchiver) auditArchive(ctx context.Context, m *Manifest) error {
	auditError := a.archiveReuseError()
	// Raw identity repair and proof inspection share one bounded history lane.
	if !a.historyScanBusy {
		auditError = a.reconcileArchiveReuse(ctx, m)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		return fmt.Errorf("site %s: %s", a.site, m.Epoch.ResetReason)
	}
	// Check one unresolved legacy row per cycle even on replicas or when its
	// local binlog has already left the index. Never sweep remote history here.
	if len(m.Files) > 0 {
		row := m.Files[a.historyNextAudit%len(m.Files)]
		a.historyNextAudit = (a.historyNextAudit + 1) % len(m.Files)
		if row.ID == "" && row.ContentID == "" {
			if err := a.legacyExists(ctx, row); err != nil {
				if auditError == nil {
					auditError = fmt.Errorf("site %s name %s id %s P=%s object missing or unreadable, digest unknown: %w; preserve objects and use pitr-quarantine with a canonical full-backup bound", a.site, row.Name, row.ID, row.PreviousGTIDs, err)
				}
			}
		}
	}
	source, ok := a.mysql.(binlogStateReader)
	if !ok {
		return auditError
	}
	uuid, text, err := source.BinlogState(ctx)
	if err != nil {
		return err
	}
	current, err := internalmysql.ParseGTIDSet(text)
	if err != nil {
		return err
	}
	// Empty execution is a fresh/bootstrap observation, not divergence evidence.
	if current.IsEmpty() {
		return auditError
	}
	if source, ok := a.mysql.(binlogBootstrapReader); ok {
		ready, err := source.BinlogHistoryReady(ctx)
		if err != nil {
			return err
		}
		if !ready {
			return auditError
		}
	}
	// A same-source number can return while its replacement is still active and
	// therefore absent from the archive. Preserve known loss for that issuer;
	// a fresh-PVC UUID cannot reissue the old source's GTIDs.
	prior := make(map[binlogIdentity]internalmysql.GTIDSet, len(m.Divergent))
	for _, d := range m.Divergent {
		set, err := internalmysql.ParseGTIDSet(d.MissingGTIDs)
		if err != nil {
			return err
		}
		for sid := range set {
			sourceUUID, _, _ := strings.Cut(sid, ":")
			if uuid != "" && !strings.EqualFold(sourceUUID, uuid) {
				delete(set, sid)
			}
		}
		key := binlogIdentity{d.Name, d.ID}
		prior[key] = UnionBinlogGTIDs(prior[key], set)
	}
	var divergent []ArchiveDivergence
	firstError := auditError
	for _, row := range m.Files {
		tx, err := internalmysql.ParseGTIDSet(strings.Join([]string{row.PreviousGTIDs, row.BaseGTIDs, observedArchiveGTIDs(row)}, ","))
		if err != nil {
			return err
		}
		missing := UnionBinlogGTIDs(tx.Subtract(current), prior[row.identity()])
		if !missing.IsEmpty() {
			divergent = append(divergent, ArchiveDivergence{Name: row.Name, ID: row.ID, ContentID: row.ContentID, MissingGTIDs: missing.String()})
			if firstError == nil {
				firstError = fmt.Errorf("site %s name %s id %s P=%s divergent transactions=%s; preserve objects and use pitr-quarantine with a canonical full-backup bound", a.site, row.Name, row.ID, row.PreviousGTIDs, missing.String())
			}
		}
	}
	before, _ := json.Marshal(m.Divergent)
	after, _ := json.Marshal(divergent)
	if string(before) != string(after) {
		m.Divergent = divergent
		if err := saveManifest(ctx, a.store, a.cfg.ManifestPrefix, m); err != nil {
			return fmt.Errorf("save divergence evidence: %w", err)
		}
	}
	return firstError
}

// QuarantineArchive moves metadata only. Objects remain available for recovery.
// resumeGTIDs is the operator-chosen trusted full-backup bound for future replay.
func QuarantineArchive(ctx context.Context, store ArchiveStore, prefix, site, name, id, reason, resumeGTIDs string, undo bool) error {
	if !isPlainFileName(name) || !isPlainFileName(site) {
		return fmt.Errorf("invalid site or file name")
	}
	if _, err := ArchiveEntryKey(prefix, site, ManifestEntry{Name: name, ID: id}); err != nil {
		return err
	}
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return err
	}
	if undo {
		found := false
		kept := m.Quarantined[:0]
		for _, q := range m.Quarantined {
			if q.Entry.Name == name && q.Entry.ID == id {
				for _, f := range m.Files {
					if f.identity() == q.Entry.identity() {
						return fmt.Errorf("active row already exists")
					}
				}
				m.Files = append(m.Files, q.Entry)
				if q.Divergent != nil {
					m.Divergent = append(m.Divergent, *q.Divergent)
				}
				if len(q.Reuse) > 0 {
					if m.Epoch == nil {
						m.Epoch = &ArchiveEpoch{}
					}
					for _, evidence := range q.Reuse {
						found := false
						for _, old := range m.Epoch.Reuse {
							if old.GTIDs == evidence.GTIDs && old.First.identity() == evidence.First.identity() && old.Second.identity() == evidence.Second.identity() {
								found = true
								break
							}
						}
						if !found {
							m.Epoch.Reuse = append(m.Epoch.Reuse, evidence)
						}
					}
					m.Epoch.ResetReason = "verified GTID namespace reuse restored by quarantine undo; quarantine the abandoned branch or use a new manifest prefix and full backup"
				}
				found = true
			} else {
				kept = append(kept, q)
			}
		}
		if !found {
			return fmt.Errorf("quarantine row not found")
		}
		m.Quarantined = kept
	} else {
		set, err := internalmysql.ParseGTIDSet(resumeGTIDs)
		if err != nil || set.IsEmpty() || strings.TrimSpace(reason) == "" {
			return fmt.Errorf("reason and valid nonempty resume GTIDs are required")
		}
		found := false
		kept := m.Files[:0]
		for _, f := range m.Files {
			if f.Name == name && f.ID == id {
				q := ArchiveQuarantine{Entry: f, Reason: reason, ResumeGTIDs: set.String(), At: time.Now().UTC()}
				for _, d := range m.Divergent {
					if d.Name == f.Name && d.ID == f.ID {
						copy := d
						q.Divergent = &copy
						break
					}
				}
				if m.Epoch != nil {
					for _, r := range m.Epoch.Reuse {
						if r.First.identity() == f.identity() || r.Second.identity() == f.identity() {
							q.Reuse = append(q.Reuse, r)
						}
					}
				}
				m.Quarantined = append(m.Quarantined, q)
				found = true
			} else {
				kept = append(kept, f)
			}
		}
		if !found {
			return fmt.Errorf("active archive row not found")
		}
		m.Files = kept
	}
	active := make(map[binlogIdentity]bool)
	for _, f := range m.Files {
		active[f.identity()] = true
	}
	keptDivergence := m.Divergent[:0]
	for _, d := range m.Divergent {
		if active[binlogIdentity{d.Name, d.ID}] {
			keptDivergence = append(keptDivergence, d)
		}
	}
	m.Divergent = keptDivergence
	if m.Epoch != nil && len(m.Epoch.Reuse) > 0 {
		// GTID containment cannot identify a backup's generation after proven
		// reuse. Quarantining rows must not reopen that ambiguous generation.
		m.Epoch.ResetReason = "verified GTID namespace reuse; a new manifest prefix and full backup are required"
	}

	return saveManifest(ctx, store, prefix, m)
}

// AcknowledgeArchiveHistory supersedes an obsolete guard only behind a verified
// canonical backup covering every active archived byte stream. Older backups
// remain governed by row-scoped quarantine bounds and verified issuance.
func AcknowledgeArchiveHistory(ctx context.Context, store ArchiveStore, prefix, site, reason, resumeGTIDs string) error {
	if !isPlainFileName(site) {
		return fmt.Errorf("invalid site")
	}
	bound, err := internalmysql.ParseGTIDSet(resumeGTIDs)
	if err != nil || bound.IsEmpty() || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("reason and valid nonempty canonical backup GTIDs are required")
	}
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return err
	}
	if m.Epoch != nil && len(m.Epoch.Reuse) > 0 {
		return fmt.Errorf("verified GTID reuse cannot be acknowledged away; use a new prefix and full backup")
	}

	for _, q := range m.Quarantined {
		prior, err := internalmysql.ParseGTIDSet(q.ResumeGTIDs)
		if err != nil || !bound.Contains(prior) {
			return fmt.Errorf("canonical backup must cover prior quarantine bound %s; use a new generation otherwise", q.ResumeGTIDs)
		}
	}
	for _, ack := range m.Acknowledged {
		prior, err := internalmysql.ParseGTIDSet(ack.ResumeGTIDs)
		if err != nil || !bound.Contains(prior) {
			return fmt.Errorf("canonical backup must cover prior acknowledgement bound %s", ack.ResumeGTIDs)
		}
	}
	var verified []ManifestEntry
	var issued BinlogTransactionIndex
	for _, entry := range m.Files {
		key, err := ArchiveEntryKey(prefix, site, entry)
		if err != nil {
			return err
		}
		r, err := store.Open(ctx, key)
		if err != nil {
			return err
		}
		e, readErr := InspectBinlog(r, time.Time{})
		_ = r.Close()
		if readErr != nil {
			return readErr
		}
		id := entry.ID
		if id == "" {
			id = entry.ContentID
		}
		if id != "" && !MatchesBinlogDigest(store, id, e.Digest) {
			return fmt.Errorf("identity mismatch for %s", entry.Name)
		}
		entry = bindArchiveEvidence(store, entry, e)
		for _, other := range issued.Overlaps(e.GTIDs) {
			prior := verified[other]
			if sameArchiveBytes(store, prior, entry) {
				continue
			}
			priorTX, _ := internalmysql.ParseGTIDSet(observedArchiveGTIDs(prior))
			overlap := e.GTIDs.Subtract(e.GTIDs.Subtract(priorTX))
			proof, err := CheckArchiveIssuance(ctx, store, prefix, site, prior, entry, overlap)
			if err != nil {
				return fmt.Errorf("cannot verify archived issuance: %w", err)
			}
			if proof != nil {
				return fmt.Errorf("verified GTID reuse cannot be acknowledged away; use a new prefix and full backup")
			}
		}
		issued.Add(len(verified), e.GTIDs)
		verified = append(verified, entry)
		base, err := internalmysql.ParseGTIDSet(entry.BaseGTIDs)
		if err != nil {
			return err
		}
		previous, err := internalmysql.ParseGTIDSet(e.Metadata.PreviousGTIDs)
		if err != nil {
			return err
		}
		if missing := UnionBinlogGTIDs(base, previous, e.GTIDs).Subtract(bound); !missing.IsEmpty() {
			return fmt.Errorf("canonical backup does not cover %s.%s: %s; quarantine unresolved history first", entry.Name, entry.ID, missing.String())
		}
	}
	ack := ArchiveHistoryAcknowledgement{Reason: reason, ResumeGTIDs: bound.String(), At: time.Now().UTC()}
	if m.Epoch != nil {
		ack.PriorResetReason = m.Epoch.ResetReason
		m.Epoch.ResetReason = ""
		m.Epoch.Reuse = nil
	}
	m.Acknowledged = append(m.Acknowledged, ack)
	m.Divergent = nil
	return saveManifest(ctx, store, prefix, m)
}
