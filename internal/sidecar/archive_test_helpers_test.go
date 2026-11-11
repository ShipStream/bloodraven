package sidecar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Test archive identity. Every archiver test runs under this group and
// destination unless it sets cfg.Archive itself.
var (
	testArchiveSource      = pitrarchive.Source{Namespace: "ns", Name: "fg", UID: "00000000-0000-4000-8000-000000000206"}
	testArchiveDestination = mustTestDestination()
	testArchiveID          = pitrarchive.CreateArchiveID(testArchiveSource.UID, testArchiveDestination)
)

func mustTestDestination() pitrarchive.Destination {
	d, err := pitrarchive.NewPVCDestination("ns", "archive", "")
	if err != nil {
		panic(err)
	}
	return d
}

// testArchiveIdentity is the test identity, with the encryption identity of
// store (Passphrase when store encrypts).
func testArchiveIdentity(store ArchiveStore) ArchiveIdentity {
	id := ArchiveIdentity{Source: testArchiveSource, Destination: testArchiveDestination,
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}
	if _, passphrase := splitArchiveStore(store); passphrase != nil {
		id.Encryption = pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}
	}
	return id
}

// withTestArchive returns cfg with the test archive identity when it has none.
func withTestArchive(cfg *PITRConfig, store ArchiveStore) *PITRConfig {
	if cfg.Archive.Source.UID != "" {
		return cfg
	}
	c := *cfg
	c.Archive = testArchiveIdentity(store)
	return &c
}

// newTestArchiver builds an archiver under the test identity on an
// established archive: the descriptor is seeded when absent, as if the
// archive already existed. Tests of establishment itself use
// NewBinlogArchiver on an empty store.
func newTestArchiver(cfg *PITRConfig, site string, mysql roleChecker, store ArchiveStore, logger *slog.Logger) *BinlogArchiver {
	if err := seedTestDescriptor(context.Background(), store, cfg.ManifestPrefix); err != nil {
		panic(fmt.Sprintf("seed test descriptor: %v", err))
	}
	return NewBinlogArchiver(withTestArchive(cfg, store), site, mysql, store, logger)
}

// seedTestDescriptor writes the test descriptor for store under prefix
// when none exists, through the plaintext backend.
func seedTestDescriptor(ctx context.Context, store ArchiveStore, prefix string) error {
	raw, passphrase := splitArchiveStore(store)
	key := pitrarchive.DescriptorKey(prefix)
	if _, ok, err := raw.Get(ctx, key); err != nil || ok {
		return err
	}
	id := testArchiveIdentity(store)
	desc, err := pitrarchive.NewDescriptor(id.Source, id.Destination, id.Encryption, passphrase, "test", time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC))
	if err != nil {
		return err
	}
	body, err := desc.Canonical()
	if err != nil {
		return err
	}
	return raw.Put(ctx, key, bytes.NewReader(body), int64(len(body)))
}

// seedManifest writes a fixture manifest as an established archive holds
// it: the descriptor is seeded and the manifest carries the test archive ID.
func seedManifest(ctx context.Context, store ArchiveStore, prefix string, m *Manifest) error {
	if err := seedTestDescriptor(ctx, store, prefix); err != nil {
		return fmt.Errorf("seed descriptor: %w", err)
	}
	if m.ArchiveID == "" {
		m.ArchiveID = testArchiveID
	}
	return saveManifest(ctx, store, prefix, m)
}

// appendManifestEntry loads, upserts by (Name, ID), and seeds the manifest.
func appendManifestEntry(ctx context.Context, store ArchiveStore, prefix, site string, entry ManifestEntry) error {
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return err
	}
	found := false
	for i := range m.Files {
		if m.Files[i].identity() == entry.identity() {
			m.Files[i] = entry
			found = true
			break
		}
	}
	if !found {
		m.Files = append(m.Files, entry)
	}
	return seedManifest(ctx, store, prefix, m)
}

// testWriterSession opens a verified writer session on store under the
// test identity, seeding the descriptor first.
func testWriterSession(ctx context.Context, store ArchiveStore, prefix string) (*ArchiveSession, error) {
	if err := seedTestDescriptor(ctx, store, prefix); err != nil {
		return nil, err
	}
	return OpenArchiveForWrite(ctx, store, prefix, testArchiveIdentity(store), "test", nil)
}

// testQuarantine runs QuarantineArchive through a test writer session.
func testQuarantine(ctx context.Context, store ArchiveStore, prefix, site, name, id, reason, resumeGTIDs string, undo bool) error {
	s, err := testWriterSession(ctx, store, prefix)
	if err != nil {
		return err
	}
	return QuarantineArchive(ctx, s, site, name, id, reason, resumeGTIDs, undo)
}

// testAcknowledge runs AcknowledgeArchiveHistory through a test writer session.
func testAcknowledge(ctx context.Context, store ArchiveStore, prefix, site, reason, resumeGTIDs string) error {
	s, err := testWriterSession(ctx, store, prefix)
	if err != nil {
		return err
	}
	return AcknowledgeArchiveHistory(ctx, s, site, reason, resumeGTIDs)
}

// testPrune runs pruneManifest through a test writer session.
func testPrune(ctx context.Context, store ArchiveStore, prefix, site string, drop map[binlogIdentity]string) ([]string, error) {
	s, err := testWriterSession(ctx, store, prefix)
	if err != nil {
		return nil, err
	}
	return pruneManifest(ctx, s, site, drop)
}

// unboundRow is a manifest row for data under name with its content
// identity and nothing else: its coverage is not bound to its bytes, so it
// proves nothing about its GTIDs until its object is inspected.
func unboundRow(name string, data []byte) ManifestEntry {
	sum := sha256.Sum256(data)
	return ManifestEntry{Name: name, ID: hex.EncodeToString(sum[:])}
}
