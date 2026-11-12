package sidecar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	// size is the size of the plaintext binlog: the file's, less the
	// encryption header of a file MySQL encrypted. It is what a manifest
	// row's Size holds.
	size int64
}

// binlogHistoryTask re-identifies one sealed local binlog in the history
// lane of scanFiles: its bytes are hashed within the scan's budget, across
// scans when the file is large.
type binlogHistoryTask struct {
	name   string
	info   os.FileInfo
	file   *localBinlog // open while reader is set
	reader io.Reader    // the file's plaintext
	hash   hash.Hash
	id     string
}

func (t *binlogHistoryTask) close() {
	if t != nil && t.reader != nil {
		_ = t.file.Close()
		t.file, t.reader = nil, nil
	}
}

// localBinlogHeader reads the header of a binlog file that MySQL did not
// encrypt (an encrypted one is an error: no keyring).
func localBinlogHeader(path string) (binlogHeader, error) {
	return localBinlogs{}.header(path)
}

// binlogs opens this server's local binlog files.
func (a *BinlogArchiver) binlogs() localBinlogs {
	return localBinlogs{keyringFile: a.cfg.KeyringFile}
}

func (l localBinlogs) header(path string) (binlogHeader, error) {
	file, err := l.open(path)
	if err != nil {
		return binlogHeader{}, err
	}
	defer file.Close()
	f := file.NewReader()
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return binlogHeader{}, err
	}
	p := replication.NewBinlogParser()
	var previous string
	found, checksum := false, false
	limited := io.LimitReader(f, 1<<20)
	for i := 0; i < 3 && !found; i++ {
		eof, err := p.ParseSingleEvent(limited, func(e *replication.BinlogEvent) error {
			if format, ok := e.Event.(*replication.FormatDescriptionEvent); ok {
				checksum = format.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32
			}
			if _, ok := e.Event.(*replication.PreviousGTIDsEvent); ok {
				text, err := previousGTIDsOf(e, checksum)
				if err != nil {
					return err
				}
				previous = text
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
	return binlogHeader{previous: previous, size: file.Size()}, nil
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
		if sameStoredIdentity(a.store, row.ID, id) {
			return true
		}
	}
	return false
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
			header, err := a.binlogs().header(path)
			match := false
			if err == nil {
				for _, row := range rows[name] {
					if row.Size == header.size && row.PreviousGTIDs == header.previous {
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
	if a.tailUpload == nil {
		a.sealedSnapshot = names
	} else if a.sealedSnapshot != nil {
		// A tail drain scans a prefix of the index. What is known about
		// the sealed files after it stays, here and in localIDs below, or
		// the next scan of the whole index would take every one of them
		// for newly sealed and identify it again. (Without a snapshot no
		// file counts as newly sealed; a drain does not start one.)
		snapshot := make(map[string]os.FileInfo, len(names)+len(a.sealedSnapshot))
		for name, info := range a.sealedSnapshot {
			snapshot[name] = info
		}
		for name, info := range names {
			snapshot[name] = info
		}
		a.sealedSnapshot = snapshot
	}
	uploaded := 0
	hadError := false
	// What this pass uploaded, and the files it reconciled with the
	// manifest (uploaded, or found archived or retired), for the tail drain.
	var uploadedNames []string
	settled := make(map[string]bool)
	fail := func(name string, err error) {
		hadError = true
		a.incUploadFailures()
		// A manifest refusal keeps its leading PITR token (F087).
		a.setError(wrapKeepToken("archive "+name, err).Error())
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
			settled[name] = true
			continue
		}
		if err := a.archivePinned(ctx, path, name, id); err != nil {
			pending[name] = true
			fail(name, err)
			continue
		}
		uploaded++
		uploadedNames = append(uploadedNames, name)
		delete(pending, name)
		settled[name] = true
		rows[name] = append(rows[name], ManifestEntry{Name: name, ID: id})
	}
	budget := int64(64 << 20)
	started := time.Now()
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
				f, err := a.binlogs().open(filepath.Join(a.cfg.BinlogDir, name))
				if err != nil {
					fail(name, err)
					completed++
					continue
				}
				opened, err := f.Stat()
				if err != nil || !stableBinlogFile(info, opened) {
					// Not the file this scan saw: the next scan takes it.
					_ = f.Close()
					completed++
					continue
				}
				task.file, task.reader = f, f.NewReader()
			}
			a.historyTask = task
		}
		task := a.historyTask
		if current := names[task.name]; current == nil || !stableBinlogFile(task.info, current) {
			task.close()
			a.historyTask = nil
			continue
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
					after, err := os.Stat(filepath.Join(a.cfg.BinlogDir, task.name))
					if err != nil || !stableBinlogFile(task.info, after) {
						a.historyTask = nil
						break
					}
					task.id = a.store.BinlogContentID(hex.EncodeToString(task.hash.Sum(nil)))
					if a.localIDs == nil {
						a.localIDs = make(map[string]cachedBinlogIdentity)
					}
					a.localIDs[task.name] = cachedBinlogIdentity{info: after, id: task.id}
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
				// The budget ran out mid-file: the next scan continues.
				break
			}
		}
		problem := false
		seen := a.candidateSeen(rows[task.name], task.id) || a.retiredIdentity(retired, task.name, task.id)
		if !seen {
			if err := a.archivePinned(ctx, filepath.Join(a.cfg.BinlogDir, task.name), task.name, task.id); err != nil {
				fail(task.name, err)
				problem = true
			} else {
				uploaded++
				uploadedNames = append(uploadedNames, task.name)
				rows[task.name] = append(rows[task.name], ManifestEntry{Name: task.name, ID: task.id})
			}
		}
		if !problem {
			delete(pending, task.name)
			settled[task.name] = true
		}
		task.close()
		a.historyTask = nil
		completed++
	}
	for name := range a.localIDs {
		if names[name] == nil && a.tailUpload == nil {
			delete(a.localIDs, name)
		}
	}
	a.scanUploaded, a.scanSettled = uploadedNames, settled
	return uploaded, int64(len(pending)), hadError
}
