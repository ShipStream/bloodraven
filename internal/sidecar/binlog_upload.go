package sidecar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

func (a *BinlogArchiver) archiveOne(ctx context.Context, path, name, id string) error {
	return a.archivePinned(ctx, path, name, id)
}

// archivePinned uploads the sealed binlog at path, whose content identity
// is id, and publishes its manifest row. A row that already holds this
// identity and whose object is readable is left as it is.
func (a *BinlogArchiver) archivePinned(ctx context.Context, path, name, id string) error {
	manifest, err := a.loadManifest(ctx)
	if err != nil {
		return err
	}
	for _, row := range manifest.Files {
		if row.Name == name && row.ID == id {
			key, _ := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, row)
			reader, openErr := a.store.Open(ctx, key)
			if openErr == nil {
				_ = reader.Close()
				return nil
			}
			if !errors.Is(openErr, os.ErrNotExist) && !isS3NotFound(openErr) {
				return openErr
			}
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return err
	}
	// A read-only site's tail drain uploads only the files it examined.
	if a.tailUpload != nil {
		if err := a.tailUpload.check(name, before); err != nil {
			return err
		}
	}
	base := ""
	if source, ok := a.mysql.(binlogBaseReader); ok {
		var err error
		base, err = source.BinlogBase(ctx)
		if err != nil {
			return err
		}
		current, err := os.Stat(path)
		if err != nil || !stableBinlogFile(before, current) {
			return fmt.Errorf("binlog changed while capturing purged base")
		}
	}
	evidence, parseErr := InspectBinlog(io.NewSectionReader(file, 0, before.Size()), time.Time{})
	if parseErr != nil && evidence.Metadata.FirstEventTime.IsZero() && evidence.Metadata.PreviousGTIDs == "" {
		return parseErr
	}
	if a.store.BinlogContentID(evidence.Digest) != id {
		return fmt.Errorf("sealed binlog changed before upload: %s", name)
	}
	key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, ManifestEntry{Name: name, ID: id})
	if err != nil {
		return err
	}
	// A previous attempt uploaded these exact bytes (the key is their
	// content identity and the local file still hashes to it) but could
	// not publish the row, for example because reuse verification against
	// an unreadable quarantined object failed. Retry the publication only;
	// verification still runs and still refuses (F213).
	if pending, ok := a.session.takeUnpublished(key); ok && pending.ID == id && pending.Name == name {
		if reader, openErr := a.store.Open(ctx, key); openErr == nil {
			_ = reader.Close()
			if err := a.publishBinlog(ctx, pending); err != nil {
				a.session.noteUnpublished(key, pending)
				return err
			}
			return nil
		} else if !errors.Is(openErr, os.ErrNotExist) && !isS3NotFound(openErr) {
			a.session.noteUnpublished(key, pending)
			return openErr
		}
		// The object is gone: upload it again below.
	}
	// The non-seekable reader is hashed once; managed multipart retries use buffers.
	digest := sha256.New()
	upload := io.TeeReader(io.NewSectionReader(file, 0, before.Size()), digest)
	if err := a.store.Put(ctx, key, upload, before.Size()); err != nil {
		return err
	}
	after, statErr := file.Stat()
	if statErr != nil || !stableBinlogFile(before, after) || hex.EncodeToString(digest.Sum(nil)) != evidence.Digest {
		_ = a.store.Delete(ctx, key)
		return fmt.Errorf("sealed binlog changed during upload: %s", name)
	}
	meta := evidence.Metadata
	meta.GTIDCoverage.ContentID = id
	entry := ManifestEntry{BaseGTIDs: base, Name: name, ID: id, RemotePath: key, Size: meta.Size, FirstEventTime: meta.FirstEventTime, LastEventTime: meta.LastEventTime, PreviousGTIDs: meta.PreviousGTIDs, GTIDCoverage: meta.GTIDCoverage, ArchivedAt: time.Now().UTC()}

	if err := a.publishBinlog(ctx, entry); err != nil {
		a.session.noteUnpublished(key, entry)
		return err
	}
	return nil
}
func stableBinlogFile(before, after os.FileInfo) bool {
	return after != nil && os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime() == after.ModTime()
}
func (a *BinlogArchiver) publishBinlog(ctx context.Context, entry ManifestEntry) error {
	m, err := a.loadManifest(ctx)
	if err != nil {
		return err
	}
	found := false
	kept := m.Files[:0]
	for _, row := range m.Files {
		if row.identity() == entry.identity() {
			kept = append(kept, entry)
			found = true
			continue
		}
		kept = append(kept, row)
	}
	if !found {
		kept = append(kept, entry)
	}
	m.Files = kept
	if !found {
		if err := a.noteArchiveEpoch(ctx, m, entry); err != nil {
			return err
		}
	}
	return a.saveManifest(ctx, m)
}
