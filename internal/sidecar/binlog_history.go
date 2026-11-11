package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
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

// auditArchive is the maintenance pass's history audit of this site's own
// manifest. It runs on every site and in every role, and it never stops
// uploads or retention. What it returns is reported as the archiver's
// LastError; what it only records is evidence.
//
// Fail closed (an error):
//   - a recorded GTID namespace reuse (Epoch.ResetReason): the archive
//     holds two different transactions under one GTID, and no reader can
//     tell which of them a backup contains;
//   - an overlapping issuance between two rows that cannot be verified
//     (reconcileArchiveReuse): the same, undecided;
//   - a server whose state cannot be read: nothing below is known;
//   - reissuable divergence: this server no longer holds archived
//     transactions that were issued under its own current server_uuid, so
//     it issues those GTIDs again as soon as it writes. The reuse is in
//     the server and its active binlog, not yet in any archived byte, so
//     no restore can see it until the replacement is sealed and uploaded
//     (and then the whole archive is reset). It is the one divergence that
//     needs action before the site is promoted.
//
// Recorded, not an error: any other divergence. A row whose transactions
// the server no longer holds, issued under another server_uuid (the tail a
// former primary archived before it was recloned, since a clone starts
// with a new server_uuid), is correct archive content. The manifest keeps
// the evidence in Divergent[], which the planner reads (pitrplan
// Row.Divergent): a restore whose window crosses those transactions
// refuses PITRArchiveDivergentHistory until excludeGtids lists them.
//
// The verdict on what this server could reissue (/archiver/status
// reissuableGtids) is this pass's or none. Every return that did not
// evaluate the server leaves no verdict: an earlier pass's "none" says
// nothing about a server that has changed since, and the runner reads it as
// no risk when the site is promoted.
func (a *BinlogArchiver) auditArchive(ctx context.Context, m *Manifest) error {
	evaluated := false
	defer func() {
		if !evaluated {
			a.setReissuable(nil)
		}
	}()
	auditError := a.archiveReuseError()
	// Proof inspection shares one bounded history lane with the scan.
	if !a.historyScanBusy {
		auditError = a.reconcileArchiveReuse(ctx, m)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		return fmt.Errorf("site %s: %s", a.site, m.Epoch.ResetReason)
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
	// Empty execution is a fresh/bootstrap observation, not divergence
	// evidence, and says nothing about what this server could reissue.
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
	// a server with another server_uuid cannot reissue the old source's GTIDs.
	prior := make(map[binlogIdentity]internalmysql.GTIDSet, len(m.Divergent))
	for _, d := range m.Divergent {
		set, err := internalmysql.ParseGTIDSet(d.MissingGTIDs)
		if err != nil {
			return err
		}
		key := binlogIdentity{d.Name, d.ID}
		prior[key] = UnionBinlogGTIDs(prior[key], issuedBy(set, uuid))
	}
	var divergent []ArchiveDivergence
	missingAll, reissuable := internalmysql.GTIDSet{}, internalmysql.GTIDSet{}
	var reissuableRow *ManifestEntry
	for i, row := range m.Files {
		tx, err := archivedRowGTIDs(row)
		if err != nil {
			return err
		}
		missing := UnionBinlogGTIDs(tx.Subtract(current), prior[row.identity()])
		if missing.IsEmpty() {
			continue
		}
		divergent = append(divergent, ArchiveDivergence{Name: row.Name, ID: row.ID, MissingGTIDs: missing.String()})
		missingAll = UnionBinlogGTIDs(missingAll, missing)
		if own := issuedBy(missing, uuid); !own.IsEmpty() {
			reissuable = UnionBinlogGTIDs(reissuable, own)
			if reissuableRow == nil {
				reissuableRow = &m.Files[i]
			}
		}
	}
	// A quarantine moves only metadata: this server can reissue a
	// quarantined row's transactions as well as an active row's
	// (detectArchiveReuse compares both). Quarantined rows carry no
	// Divergent[] entry; the evidence kept with the quarantine is their
	// sticky part, and a loss this server could reissue is added to it, so
	// that it survives the numbers' return as an active row's does.
	quarantineChanged := false
	for i, q := range m.Quarantined {
		tx, err := archivedRowGTIDs(q.Entry)
		if err != nil {
			return err
		}
		var known internalmysql.GTIDSet
		if q.Divergent != nil {
			if known, err = internalmysql.ParseGTIDSet(q.Divergent.MissingGTIDs); err != nil {
				return err
			}
		}
		own := issuedBy(UnionBinlogGTIDs(tx.Subtract(current), known), uuid)
		if own.IsEmpty() {
			continue
		}
		reissuable = UnionBinlogGTIDs(reissuable, own)
		if reissuableRow == nil {
			reissuableRow = &m.Quarantined[i].Entry
		}
		if !known.Contains(own) {
			m.Quarantined[i].Divergent = &ArchiveDivergence{Name: q.Entry.Name, ID: q.Entry.ID, MissingGTIDs: UnionBinlogGTIDs(known, own).String()}
			quarantineChanged = true
		}
	}
	// The server was evaluated: the verdict stands whether or not the
	// evidence below can be saved.
	a.setReissuable(reissuable)
	evaluated = true
	before, _ := json.Marshal(m.Divergent)
	after, _ := json.Marshal(divergent)
	if string(before) != string(after) || quarantineChanged {
		m.Divergent = divergent
		if err := a.saveManifest(ctx, m); err != nil {
			return fmt.Errorf("save divergence evidence: %w", err)
		}
		a.logger.Info("archive divergence evidence recorded", "rows", len(divergent),
			"missingGtids", missingAll.String(), "reissuableGtids", reissuable.String())
	}
	if auditError != nil {
		return auditError
	}
	if !reissuable.IsEmpty() {
		return fmt.Errorf("site %s: server_uuid %s no longer has archived transactions %s that it issued (row %s id %s) and reuses their GTIDs when it writes; once the reused GTIDs are archived, every restore from this archive refuses %s. Reclone this site before it is promoted (a clone starts with a new server_uuid); if it is already the primary, start a new archive prefix and take a full backup",
			a.site, uuid, reissuable.String(), reissuableRow.Name, reissuableRow.ID, pitrarchive.TokenArchiveHistoryReset)
	}
	return nil
}

// archivedRowGTIDs is every transaction a row vouches for: the ones it
// holds and the ones it builds on.
func archivedRowGTIDs(row ManifestEntry) (internalmysql.GTIDSet, error) {
	return internalmysql.ParseGTIDSet(strings.Join([]string{row.PreviousGTIDs, row.BaseGTIDs, observedArchiveGTIDs(row)}, ","))
}

// issuedBy is the part of set a server with server_uuid uuid issues GTIDs
// in: the plain and tagged source identifiers under uuid. An unknown uuid
// keeps the whole set: without it nothing proves the server cannot.
func issuedBy(set internalmysql.GTIDSet, uuid string) internalmysql.GTIDSet {
	out := make(internalmysql.GTIDSet, len(set))
	for sid, intervals := range set {
		source, _, _ := strings.Cut(sid, ":")
		if uuid == "" || strings.EqualFold(source, uuid) {
			out[sid] = intervals
		}
	}
	return out
}

// setReissuable publishes the audit's verdict on what this server could
// reissue for /archiver/status; nil when this pass did not evaluate it.
func (a *BinlogArchiver) setReissuable(set internalmysql.GTIDSet) {
	var text *string
	if set != nil {
		s := set.String()
		text = &s
	}
	a.mu.Lock()
	a.reissuableGTIDs = text
	a.mu.Unlock()
}

// QuarantineArchive moves metadata only. Objects remain available for recovery.
// resumeGTIDs is the operator-chosen trusted full-backup bound for future replay.
// It writes through session, a verified writer session for the archive
// (the site's own sidecar writer must be stopped first).
func QuarantineArchive(ctx context.Context, session *ArchiveSession, site, name, id, reason, resumeGTIDs string, undo bool) error {
	if !isPlainFileName(name) || !isPlainFileName(site) {
		return fmt.Errorf("invalid site or file name")
	}
	if _, err := ArchiveEntryKey(session.Prefix(), site, ManifestEntry{Name: name, ID: id}); err != nil {
		return err
	}
	m, err := session.LoadManifest(ctx, site)
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

	return session.SaveManifest(ctx, m)
}

// AcknowledgeArchiveHistory supersedes an obsolete guard only behind a verified
// canonical backup covering every active archived byte stream. Older backups
// remain governed by row-scoped quarantine bounds and verified issuance.
func AcknowledgeArchiveHistory(ctx context.Context, session *ArchiveSession, site, reason, resumeGTIDs string) error {
	store, prefix := session.Store(), session.Prefix()
	if !isPlainFileName(site) {
		return fmt.Errorf("invalid site")
	}
	bound, err := internalmysql.ParseGTIDSet(resumeGTIDs)
	if err != nil || bound.IsEmpty() || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("reason and valid nonempty canonical backup GTIDs are required")
	}
	m, err := session.LoadManifest(ctx, site)
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
		if !MatchesBinlogDigest(store, entry.ID, e.Digest) {
			return fmt.Errorf("identity mismatch for %s", entry.Name)
		}
		entry = bindArchiveEvidence(entry, e)
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
	return session.SaveManifest(ctx, m)
}
