package sidecar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type binlogIdentity struct{ Name, ID string }

func (e ManifestEntry) identity() binlogIdentity { return binlogIdentity{Name: e.Name, ID: e.ID} }

// validBinlogID reports whether id is a content identity: the lowercase hex
// SHA-256 of the sealed bytes (keyed for encrypted stores).
func validBinlogID(id string) bool {
	if len(id) != sha256.Size*2 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ArchiveEntryKey derives object keys from validated identity fields, never
// RemotePath. Every archived binlog is content-addressed: an entry without a
// valid ID has no key.
func ArchiveEntryKey(prefix, site string, entry ManifestEntry) (string, error) {
	if !isPlainFileName(site) || !isPlainFileName(entry.Name) {
		return "", fmt.Errorf("invalid archive site/name: %q/%q", site, entry.Name)
	}
	if !validBinlogID(entry.ID) {
		return "", fmt.Errorf("invalid binlog identity %q", entry.ID)
	}
	return binlogObjectKey(prefix, site, entry.Name+"."+entry.ID), nil
}

func fileContentID(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type cachedBinlogIdentity struct {
	info os.FileInfo
	id   string
}

// localBinlogID caches immutable sealed files and invalidates replacements by
// inode, size, or modification time. The cache is owned by the scan goroutine.
func (a *BinlogArchiver) localBinlogID(filePath string) (string, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return "", err
	}
	name := filepath.Base(filePath)
	cached, ok := a.localIDs[name]
	if ok && os.SameFile(cached.info, info) && cached.info.Size() == info.Size() && cached.info.ModTime() == info.ModTime() {
		return cached.id, nil
	}
	id, err := a.fileContentID(filePath)
	if err != nil {
		return "", err
	}
	after, err := os.Stat(filePath)
	if err != nil {
		return "", err
	}
	if !os.SameFile(info, after) || info.Size() != after.Size() || info.ModTime() != after.ModTime() {
		return "", fmt.Errorf("sealed binlog changed while identifying %s", name)
	}
	if a.localIDs == nil {
		a.localIDs = make(map[string]cachedBinlogIdentity)
	}
	a.localIDs[name] = cachedBinlogIdentity{info: after, id: id}
	return id, nil
}

// inspectArchivedBinlog authenticates and parses a stream without scratch files.
func (a *BinlogArchiver) inspectArchivedBinlog(ctx context.Context, entry ManifestEntry, inspect func(BinlogEvidence) error) error {
	key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, entry)
	if err != nil {
		return err
	}
	reader, err := a.store.Open(ctx, key)
	if err != nil {
		return fmt.Errorf("read archived binlog %s: %w", key, err)
	}
	defer reader.Close()
	evidence, parseErr := InspectBinlog(reader, time.Time{})
	if !MatchesBinlogDigest(a.store, entry.ID, evidence.Digest) {
		return fmt.Errorf("archived binlog identity mismatch for %s", entry.Name)
	}
	if parseErr != nil {
		return parseErr
	}
	evidence.Metadata.Name = entry.Name
	return inspect(evidence)
}

// MatchesBinlogDigest accepts the original plaintext layout and the keyed layout.
func MatchesBinlogDigest(store ArchiveStore, identity, digest string) bool {
	return identity == digest || identity == store.BinlogContentID(digest)
}

func (a *BinlogArchiver) fileContentID(filePath string) (string, error) {
	digest, err := fileContentID(filePath)
	if err != nil {
		return "", err
	}
	return a.store.BinlogContentID(digest), nil
}
