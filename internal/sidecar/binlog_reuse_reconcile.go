package sidecar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

type archiveEvidenceKey struct{ name, id, content string }
type archiveReuseKey struct{ first, second archiveEvidenceKey }

func evidenceKey(e ManifestEntry) archiveEvidenceKey {
	return archiveEvidenceKey{e.Name, e.ID, e.ContentID}
}
func sameArchiveBytes(store ArchiveStore, a, b ManifestEntry) bool {
	x, y := a.ID, b.ID
	if x == "" {
		x = a.ContentID
	}
	if y == "" {
		y = b.ContentID
	}
	return x != "" && y != "" && (x == y || store.BinlogContentID(x) == y || store.BinlogContentID(y) == x)
}

type archiveReuseWork struct {
	key           archiveReuseKey
	first, second ManifestEntry
	overlap       internalmysql.GTIDSet
	cancel        context.CancelFunc
	source        io.ReadCloser
	done          chan struct{}
	notified      bool
	result        archiveReuseResult
}
type archiveReuseResult struct {
	entry ManifestEntry
	proof *ArchiveReuseEvidence
	err   error
}

func (w *archiveReuseWork) close() {
	w.cancel()
	if w.source != nil {
		_ = w.source.Close()
	}
	<-w.done
}
func (a *BinlogArchiver) reuseReady() <-chan struct{} {
	if a.reuseWork == nil || a.reuseWork.notified {
		return nil
	}
	return a.reuseWork.done
}
func copyEvidence(e ManifestEntry) ManifestEntry {
	if e.GTIDCoverage != nil {
		c := *e.GTIDCoverage
		e.GTIDCoverage = &c
	}
	return e
}
func bindArchiveEvidence(store ArchiveStore, entry ManifestEntry, e BinlogEvidence) ManifestEntry {
	id := entry.ID
	if id == "" {
		id = store.BinlogContentID(e.Digest)
		entry.ContentID = id
	}
	entry.GTIDCoverage = e.Metadata.GTIDCoverage
	entry.GTIDCoverage.ContentID = id
	entry.Size = e.Metadata.Size
	entry.FirstEventTime = e.Metadata.FirstEventTime
	entry.LastEventTime = e.Metadata.LastEventTime
	entry.PreviousGTIDs = e.Metadata.PreviousGTIDs
	return entry
}
func (a *BinlogArchiver) chooseArchiveReuseWork(m *Manifest, ignoreCooldown bool) *archiveReuseWork {
	active := make(map[archiveEvidenceKey]bool, len(m.Files))
	var index BinlogTransactionIndex
	var bound []int
	for i, row := range m.Files {
		active[evidenceKey(row)] = true
		if coverageBoundToEntry(row) {
			tx, err := internalmysql.ParseGTIDSet(observedArchiveGTIDs(row))
			if err == nil && !tx.IsEmpty() {
				bound = append(bound, i)
			}
		}
	}
	for key := range a.reuseFailed {
		if !active[key.first] || (key.second != (archiveEvidenceKey{}) && !active[key.second]) {
			delete(a.reuseFailed, key)
			delete(a.reuseErrors, key)
		}
	}
	for key := range a.reuseCompared {
		if !active[key.first] || !active[key.second] {
			delete(a.reuseCompared, key)
		}
	}
	eligible := func(key archiveReuseKey) bool { return ignoreCooldown || !time.Now().Before(a.reuseFailed[key]) }
	for _, i := range bound {
		row := m.Files[i]
		tx, _ := internalmysql.ParseGTIDSet(observedArchiveGTIDs(row))
		for _, j := range index.Overlaps(tx) {
			old := m.Files[j]
			if sameArchiveBytes(a.store, old, row) {
				continue
			}
			key := archiveReuseKey{evidenceKey(old), evidenceKey(row)}
			if a.reuseCompared[key] || !eligible(key) {
				continue
			}
			oldTX, _ := internalmysql.ParseGTIDSet(observedArchiveGTIDs(old))
			return &archiveReuseWork{key: key, first: copyEvidence(old), second: copyEvidence(row), overlap: tx.Subtract(tx.Subtract(oldTX))}
		}
		index.Add(i, tx)
	}
	if len(m.Files) < 2 {
		return nil
	}
	first := m.Files[0]
	var other *ManifestEntry
	for i := 1; i < len(m.Files); i++ {
		row := m.Files[i]
		if row.identity() != first.identity() && !sameArchiveBytes(a.store, first, row) {
			copy := row
			other = &copy
			break
		}
	}
	if other == nil {
		return nil
	}
	for offset := 0; offset < len(m.Files); offset++ {
		i := (a.reuseNext + offset) % len(m.Files)
		row := m.Files[i]
		if coverageBoundToEntry(row) {
			continue
		}
		key := archiveReuseKey{first: evidenceKey(row)}
		if !eligible(key) {
			continue
		}
		// Unknown legacy observations remain pending even when every row
		// predates proof binding. Two representatives avoid a quadratic scan.
		different := func(peer ManifestEntry) bool {
			return peer.identity() != row.identity() && !sameArchiveBytes(a.store, row, peer)
		}
		if different(first) || different(*other) {
			a.reuseNext = (i + 1) % len(m.Files)
			return &archiveReuseWork{key: key, first: copyEvidence(row)}
		}
	}
	return nil
}
func (a *BinlogArchiver) runArchiveReuseWork(ctx context.Context, w *archiveReuseWork, input io.Reader) archiveReuseResult {
	if w.key.second != (archiveEvidenceKey{}) {
		proof, err := CheckArchiveIssuance(ctx, a.store, a.cfg.ManifestPrefix, a.site, w.first, w.second, w.overlap)
		return archiveReuseResult{proof: proof, err: err}
	}
	e, err := InspectBinlog(input, time.Time{})
	if err != nil {
		return archiveReuseResult{err: err}
	}
	id := w.first.ID
	if id == "" {
		id = w.first.ContentID
	}
	if id != "" && !MatchesBinlogDigest(a.store, id, e.Digest) {
		return archiveReuseResult{err: fmt.Errorf("legacy identity mismatch for %s", w.first.Name)}
	}
	return archiveReuseResult{entry: bindArchiveEvidence(a.store, w.first, e)}
}
func (a *BinlogArchiver) beginArchiveReuseWork(ctx context.Context, w *archiveReuseWork) {
	ctx, w.cancel = context.WithCancel(ctx)
	w.done = make(chan struct{})
	finish := func(result archiveReuseResult) {
		w.result = result
		if w.source != nil {
			_ = w.source.Close()
		}
		w.cancel()
		close(w.done)
	}
	if w.key.second != (archiveEvidenceKey{}) {
		if w.first.Size > 0 && w.second.Size > 0 && w.first.Size+w.second.Size <= 64<<10 {
			finish(a.runArchiveReuseWork(ctx, w, nil))
		} else {
			go func() { finish(a.runArchiveReuseWork(ctx, w, nil)) }()
		}
		return
	}
	key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, w.first)
	if err != nil {
		finish(archiveReuseResult{err: err})
		return
	}
	w.source, err = a.store.Open(ctx, key)
	if err != nil {
		finish(archiveReuseResult{err: err})
		return
	}
	// At most 64 KiB is held. Unknown or stale legacy Size metadata cannot turn
	// synchronous reconciliation into an unbounded read on the scan goroutine.
	prefix := make([]byte, (64<<10)+1)
	n, err := io.ReadFull(w.source, prefix)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		finish(a.runArchiveReuseWork(ctx, w, bytes.NewReader(prefix[:n])))
		return
	}
	if err != nil {
		finish(archiveReuseResult{err: err})
		return
	}
	go func() { finish(a.runArchiveReuseWork(ctx, w, io.MultiReader(bytes.NewReader(prefix[:n]), w.source))) }()
}
func (a *BinlogArchiver) reconcileArchiveReuse(ctx context.Context, m *Manifest) error {
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		if a.reuseWork != nil {
			a.reuseWork.close()
			a.reuseWork = nil
		}
		return nil
	}
	changed := false
	var firstError error
	// Batch small completed jobs, but only one larger stream may run at a time.
	started := time.Now()
jobsLoop:
	for jobs := 0; jobs < 8 && time.Since(started) < 2*time.Second; jobs++ {
		if a.reuseWork != nil {
			w := a.reuseWork
			select {
			case <-w.done:
			default:
				break jobsLoop
			}
			a.reuseWork = nil
			result := w.result
			if result.err == nil {
				delete(a.reuseErrors, w.key)
				delete(a.reuseFailed, w.key)
			}
			if result.err != nil {
				if a.reuseFailed == nil {
					a.reuseFailed = make(map[archiveReuseKey]time.Time)
				}
				a.reuseFailed[w.key] = time.Now().Add(time.Hour)
				if a.reuseErrors == nil {
					a.reuseErrors = make(map[archiveReuseKey]error)
				}
				a.reuseErrors[w.key] = fmt.Errorf("site %s name %s id %s: verify archived GTID issuance: %w; preserve objects, repair archive access/bytes or use pitr-quarantine with a canonical full-backup bound", a.site, w.first.Name, w.first.ID, result.err)
				firstError = a.reuseErrors[w.key]
			} else if w.key.second != (archiveEvidenceKey{}) {
				present := make(map[archiveEvidenceKey]bool)
				for _, row := range m.Files {
					present[evidenceKey(row)] = true
				}
				if present[w.key.first] && present[w.key.second] {
					if result.proof != nil {
						recordArchiveReuse(m, *result.proof)
						changed = true
					} else {
						if a.reuseCompared == nil {
							a.reuseCompared = make(map[archiveReuseKey]bool)
						}
						a.reuseCompared[w.key] = true
					}
				}
			} else {
				for i, row := range m.Files {
					if row.identity() != w.first.identity() {
						continue
					}
					if evidenceKey(row) != w.key.first && !sameArchiveBytes(a.store, row, result.entry) {
						break
					}
					// Preserve current base/provenance and administrative state; merge only
					// observations verified against this immutable object's content identity.
					next := result.entry
					next.BaseGTIDs = row.BaseGTIDs
					next.ArchivedAt = row.ArchivedAt
					m.Files[i] = next
					for j, d := range m.Divergent {
						if d.Name == row.Name && d.ID == row.ID {
							m.Divergent[j].ContentID = next.ContentID
						}
					}
					changed = true
					break
				}
			}
			if m.Epoch != nil && m.Epoch.ResetReason != "" {
				break
			}
		}
		if a.historyTask != nil && a.historyTask.remote {
			break
		}
		w := a.chooseArchiveReuseWork(m, false)
		if w == nil {
			break
		}
		a.reuseWork = w
		a.beginArchiveReuseWork(ctx, w)
	}
	if changed {
		if err := saveManifest(ctx, a.store, a.cfg.ManifestPrefix, m); err != nil {
			return err
		}
	}
	if firstError != nil {
		return firstError
	}
	return a.archiveReuseError()
}

// Keep failures visible throughout cooldown, until successful verification or
// removal of the affected identity (including explicit quarantine).
func (a *BinlogArchiver) archiveReuseError() error {
	for _, err := range a.reuseErrors {
		return err
	}
	return nil
}

// Unverified compatibility must not disappear by retiring its only counterpart.
func (a *BinlogArchiver) archiveReusePending(m *Manifest) bool {
	return a.reuseWork != nil || a.chooseArchiveReuseWork(m, true) != nil
}
