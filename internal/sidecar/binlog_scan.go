package sidecar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

type binlogHeader struct {
	previous string
}
type binlogHistoryTask struct {
	name           string
	info           os.FileInfo
	reader         io.ReadCloser
	hash           hash.Hash
	id             string
	remote         bool
	row            int
	remoteIdentity binlogIdentity
	remoteResult   chan remoteHistoryResult
}

type remoteHistoryResult struct {
	digest string
	err    error
}

// A large remote read keeps consuming in one bounded worker instead of parking
// an HTTP response until the next scan. Only the scan goroutine publishes it.
func (t *binlogHistoryTask) drainRemote() {
	reader, h := t.reader, t.hash
	t.remoteResult = make(chan remoteHistoryResult, 1)
	result := t.remoteResult
	go func() {
		_, err := io.Copy(h, reader)
		_ = reader.Close()
		result <- remoteHistoryResult{digest: hex.EncodeToString(h.Sum(nil)), err: err}
	}()
}
func (t *binlogHistoryTask) close() {
	if t != nil && t.reader != nil {
		_ = t.reader.Close()
		t.reader = nil
	}
	if t != nil && t.remoteResult != nil {
		<-t.remoteResult
		t.remoteResult = nil
	}
}
func localBinlogHeader(path string) (binlogHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return binlogHeader{}, err
	}
	defer f.Close()
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return binlogHeader{}, err
	}
	p := replication.NewBinlogParser()
	var previous string
	found := false
	limited := io.LimitReader(f, 1<<20)
	for i := 0; i < 3 && !found; i++ {
		eof, err := p.ParseSingleEvent(limited, func(e *replication.BinlogEvent) error {
			if event, ok := e.Event.(*replication.PreviousGTIDsEvent); ok {
				previous = event.GTIDSets
				found = true
			}
			return nil
		})
		if err != nil {
			return binlogHeader{}, err
		}
		if eof {
			break
		}
	}
	if !found {
		return binlogHeader{}, fmt.Errorf("missing Previous_gtids header")
	}
	return binlogHeader{previous}, nil
}
func sameStoredIdentity(store ArchiveStore, stored, current string) bool {
	return stored != "" && (stored == current || store.BinlogContentID(stored) == current)
}
func (a *BinlogArchiver) retiredIdentity(retired map[binlogIdentity]struct{}, name, id string) bool {
	for key := range retired {
		if key.Name == name && sameStoredIdentity(a.store, key.ID, id) {
			return true
		}
	}
	return false
}
func (a *BinlogArchiver) candidateSeen(rows []ManifestEntry, id string) bool {
	for _, row := range rows {
		if row.ID != "" && sameStoredIdentity(a.store, row.ID, id) {
			return true
		}
	}
	return false
}
func (a *BinlogArchiver) legacyExists(ctx context.Context, row ManifestEntry) error {
	key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, row)
	if err != nil {
		return err
	}
	if until := a.legacyChecked[key]; time.Now().Before(until) {
		return nil
	}
	r, err := a.store.Open(ctx, key)
	if err != nil {
		return err
	}
	_ = r.Close()
	if a.legacyChecked == nil {
		a.legacyChecked = make(map[string]time.Time)
	}
	a.legacyChecked[key] = time.Now().Add(time.Hour)
	return nil
}
func (a *BinlogArchiver) missingLegacy(ctx context.Context, name, id string, rows []ManifestEntry) (bool, error) {
	for _, row := range rows {
		if row.ID == "" && sameStoredIdentity(a.store, row.ContentID, id) {
			err := a.legacyExists(ctx, row)
			if errors.Is(err, os.ErrNotExist) || isS3NotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			return false, nil
		}
	}
	return false, nil
}
func (a *BinlogArchiver) scanFiles(ctx context.Context, m *Manifest, sealed []string, retired map[binlogIdentity]struct{}) (int, int64, bool) {
	rows := make(map[string][]ManifestEntry)
	for _, row := range m.Files {
		rows[row.Name] = append(rows[row.Name], row)
	}
	names := make(map[string]os.FileInfo, len(sealed))
	foreground := make(map[string]bool)
	pending := make(map[string]bool)
	for i, entry := range sealed {
		name := filepath.Base(entry)
		path := filepath.Join(a.cfg.BinlogDir, name)
		info, err := os.Stat(path)
		if err != nil {
			pending[name] = true
			continue
		}
		names[name] = info
		cached, known := a.localIDs[name]
		validCache := known && stableBinlogFile(cached.info, info)
		if !validCache {
			pending[name] = true
		}
		newlySealed := a.sealedSnapshot != nil && a.sealedSnapshot[name] == nil
		changed := known && !validCache
		if len(rows[name]) == 0 || newlySealed || changed {
			foreground[name] = true
			continue
		}
		if i >= len(sealed)-8 && !validCache {
			header, err := localBinlogHeader(path)
			match := false
			if err == nil {
				for _, row := range rows[name] {
					if row.Size == info.Size() && row.PreviousGTIDs == header.previous {
						match = true
						break
					}
				}
			}
			if !match {
				foreground[name] = true
			}
		}
	}
	a.sealedSnapshot = names
	uploaded := 0
	hadError := false
	fail := func(name string, err error) {
		hadError = true
		a.incUploadFailures()
		a.setError(fmt.Sprintf("archive %s: %v", name, err))
		a.logger.Warn("archive binlog", "file", name, "error", err)
	}
	for _, entry := range sealed {
		name := filepath.Base(entry)
		if !foreground[name] {
			continue
		}
		if a.historyTask != nil && a.historyTask.name == name {
			a.historyTask.close()
			a.historyTask = nil
		}
		path := filepath.Join(a.cfg.BinlogDir, name)
		id, err := a.localBinlogID(path)
		if err != nil {
			fail(name, err)
			continue
		}
		if a.retiredIdentity(retired, name, id) || a.candidateSeen(rows[name], id) {
			delete(pending, name)
			continue
		}
		replace, err := a.missingLegacy(ctx, name, id, rows[name])
		if err != nil {
			fail(name, err)
			continue
		}
		legacySame := false
		for _, row := range rows[name] {
			if row.ID == "" && sameStoredIdentity(a.store, row.ContentID, id) {
				legacySame = true
			}
		}
		if legacySame && !replace {
			delete(pending, name)
			continue
		}
		if err := a.archivePinned(ctx, path, name, id, replace); err != nil {
			pending[name] = true
			fail(name, err)
			continue
		}
		uploaded++
		delete(pending, name)
		if replace {
			rows[name] = nil
		}
		rows[name] = append(rows[name], ManifestEntry{Name: name, ID: id})
	}
	budget := int64(64 << 20)
	started := time.Now()
	updates := make(map[binlogIdentity]string)
history:
	for completed := 0; completed < 8 && len(sealed) > 0 && budget > 0 && time.Since(started) < 2*time.Second; {
		if a.historyTask == nil {
			index := len(sealed) - 1 - (a.historyNext % len(sealed))
			a.historyNext++
			name := filepath.Base(sealed[index])
			if foreground[name] {
				completed++
				continue
			}
			info := names[name]
			if info == nil {
				completed++
				continue
			}
			task := &binlogHistoryTask{name: name, info: info, hash: sha256.New()}
			if cached, ok := a.localIDs[name]; ok && stableBinlogFile(cached.info, info) {
				task.id = cached.id
			} else {
				f, err := os.Open(filepath.Join(a.cfg.BinlogDir, name))
				if err != nil {
					fail(name, err)
					completed++
					continue
				}
				task.reader = f
			}
			a.historyTask = task
		}
		task := a.historyTask
		if current := names[task.name]; current == nil || !stableBinlogFile(task.info, current) {
			task.close()
			a.historyTask = nil
			continue
		}
		if task.remoteResult != nil {
			select {
			case result := <-task.remoteResult:
				task.reader = nil
				task.remoteResult = nil
				if result.err != nil {
					fail(task.name, result.err)
					a.historyTask = nil
					completed++
					continue
				}
				identity := a.store.BinlogContentID(result.digest)
				updates[task.remoteIdentity] = identity
				for index, row := range rows[task.name] {
					if row.identity() == task.remoteIdentity {
						rows[task.name][index].ContentID = identity
						task.row = index + 1
						break
					}
				}
			default:
				break history
			}
		}
		if task.reader != nil {
			a.historyScanBusy = true
			buffer := make([]byte, 64<<10)
			for budget > 0 && time.Since(started) < 2*time.Second {
				n, err := task.reader.Read(buffer[:min(int64(len(buffer)), budget)])
				budget -= int64(n)
				_, _ = task.hash.Write(buffer[:n])
				if err != nil {
					task.close()
					if err != io.EOF {
						fail(task.name, err)
						a.historyTask = nil
						break
					}
					digest := hex.EncodeToString(task.hash.Sum(nil))
					identity := a.store.BinlogContentID(digest)
					if task.remote {
						updates[task.remoteIdentity] = identity
						if task.row < len(rows[task.name]) && rows[task.name][task.row].identity() == task.remoteIdentity {
							rows[task.name][task.row].ContentID = identity
							task.row++
						}
					} else {
						after, err := os.Stat(filepath.Join(a.cfg.BinlogDir, task.name))
						if err != nil || !stableBinlogFile(task.info, after) {
							a.historyTask = nil
							break
						}
						task.id = identity
						if a.localIDs == nil {
							a.localIDs = make(map[string]cachedBinlogIdentity)
						}
						a.localIDs[task.name] = cachedBinlogIdentity{info: after, id: identity}
					}
					break
				}
				if n == 0 {
					break
				}
			}
			if a.historyTask == nil {
				completed++
				continue
			}
			if task.reader != nil {
				if task.remote {
					task.drainRemote()
				}
				break
			}
		}
		wait := false
		problem := false
		for task.row < len(rows[task.name]) {
			row := rows[task.name][task.row]
			if row.ID != "" {
				task.row++
				continue
			}
			if row.ContentID == "" {
				if a.reuseWork != nil {
					wait = true
					break
				}
				key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, row)
				if err == nil {
					task.reader, err = a.store.Open(ctx, key)
				}
				if err != nil {
					fail(task.name, fmt.Errorf("object missing or unreadable, digest unknown; preserve surviving data and use pitr-quarantine: %w", err))
					problem = true
					if !errors.Is(err, os.ErrNotExist) && !isS3NotFound(err) {
						task.close()
						a.historyTask = nil
						completed++
						break
					}
					task.row++
					continue
				}
				task.remote = true
				task.remoteIdentity = row.identity()
				task.hash = sha256.New()
				wait = true
				break
			}
			if err := a.legacyExists(ctx, row); err != nil {
				if errors.Is(err, os.ErrNotExist) || isS3NotFound(err) {
					if sameStoredIdentity(a.store, row.ContentID, task.id) {
						if err := a.archivePinned(ctx, filepath.Join(a.cfg.BinlogDir, task.name), task.name, task.id, true); err != nil {
							fail(task.name, err)
							problem = true
						} else {
							uploaded++
							rows[task.name][task.row] = ManifestEntry{Name: task.name, ID: task.id}
						}
					} else {
						fail(task.name, fmt.Errorf("object missing; surviving bytes have a different identity; use pitr-quarantine"))
						problem = true
					}
				} else {
					fail(task.name, err)
					problem = true
				}
			}
			task.row++
		}
		if wait && a.reuseWork != nil {
			break history
		}
		if wait || a.historyTask == nil {
			continue
		}
		seen := a.candidateSeen(rows[task.name], task.id) || a.retiredIdentity(retired, task.name, task.id)
		for _, row := range rows[task.name] {
			seen = seen || sameStoredIdentity(a.store, row.ContentID, task.id)
		}
		if !seen {
			if err := a.archivePinned(ctx, filepath.Join(a.cfg.BinlogDir, task.name), task.name, task.id, false); err != nil {
				fail(task.name, err)
				problem = true
			} else {
				uploaded++
				rows[task.name] = append(rows[task.name], ManifestEntry{Name: task.name, ID: task.id})
			}
		}
		if !problem {
			delete(pending, task.name)
		}
		task.close()
		a.historyTask = nil
		completed++
	}
	if len(updates) > 0 {
		latest, err := loadManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site)
		if err == nil {
			for i := range latest.Files {
				if id, ok := updates[latest.Files[i].identity()]; ok {
					latest.Files[i].ContentID = id
				}
			}
			err = saveManifest(ctx, a.store, a.cfg.ManifestPrefix, latest)
		}
		if err != nil {
			fail("manifest", err)
		}
	}
	for name := range a.localIDs {
		if names[name] == nil {
			delete(a.localIDs, name)
		}
	}
	return uploaded, int64(len(pending)), hadError
}
