package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRereview3VerifiedGTIDReissuanceAndQuarantineUndo(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
	var ids []string
	for i, file := range []string{"incarnation-a.bin", "same-header.bin"} {
		source := filepath.Join("testdata", file)
		id, err := a.localBinlogID(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.archiveOne(ctx, source, []string{"mysql-bin.000001", "mysql-bin.000002"}[i], id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch == nil || len(m.Epoch.Reuse) != 1 || m.Epoch.ResetReason == "" {
		t.Fatalf("different transactions reissued as GTID 11 without a guard: %+v", m.Epoch)
	}
	if err := AcknowledgeArchiveHistory(ctx, store, "binlogs", "iad", "canonical backup", coverageTestSID+":1-11"); err == nil {
		t.Fatal("numeric coverage alone superseded verified conflicting issuance")
	}
	if err := QuarantineArchive(ctx, store, "binlogs", "iad", "mysql-bin.000001", ids[0], "canonical backup selects replacement history", coverageTestSID+":1-11", false); err != nil {
		t.Fatal(err)
	}
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch.ResetReason == "" || len(m.Quarantined[0].Reuse) != 1 {
		t.Fatal("quarantine erased verified generation-conflict protection")
	}
	if err := AcknowledgeArchiveHistory(ctx, store, "binlogs", "iad", "canonical backup", coverageTestSID+":1-11"); err == nil {
		t.Fatal("quarantine reopened a proven reused generation from numeric backup bounds alone")
	}
	if err := QuarantineArchive(ctx, store, "binlogs", "iad", "mysql-bin.000001", ids[0], "", "", true); err != nil {
		t.Fatal(err)
	}
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch.ResetReason == "" {
		t.Fatal("undo restored an abandoned branch without restoring its guard")
	}
}

// Re-logging can change the immediate commit time and all file positions while
// retaining the original commit identity. A true new issuance changes the latter.
func TestRereview3NativeCommitIdentityDistinguishesRelogging(t *testing.T) {
	raw, err := os.ReadFile("testdata/native/initial.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, reissue := range []bool{false, true} {
		t.Run(map[bool]string{false: "relog", true: "reissue"}[reissue], func(t *testing.T) {
			var changed []byte
			changed = append(changed, raw[:4]...)
			edited := false
			for off := 4; off < len(raw); {
				size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
				header := bytes.Clone(raw[off : off+19])
				body := bytes.Clone(raw[off+19 : off+size-4])
				if raw[off+4] == 33 && !edited {
					original := bytes.Clone(body[42:49])
					tail := 49
					if original[6]&0x80 != 0 {
						original = bytes.Clone(body[49:56])
						tail = 56
					}
					immediate := bytes.Clone(original)
					immediate[0] ^= 1
					immediate[6] |= 0x80
					if reissue {
						original[0] ^= 2
					}
					body = append(append(append(bytes.Clone(body[:42]), immediate...), original...), body[tail:]...)
					edited = true
				}
				binary.LittleEndian.PutUint32(header[9:13], uint32(19+len(body)+4))
				binary.LittleEndian.PutUint32(header[13:17], uint32(len(changed)+19+len(body)+4))
				event := append(header, body...)
				event = binary.LittleEndian.AppendUint32(event, crc32.ChecksumIEEE(event))
				changed = append(changed, event...)
				off += size
			}
			meta, err := InspectBinlog(bytes.NewReader(raw), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			uuid := ""
			for sid := range meta.GTIDs {
				uuid = strings.SplitN(sid, ":", 2)[0]
				break
			}
			state := &historyStateStub{uuid: uuid, executed: meta.GTIDs.String()}
			store := newMemStore()
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
			for i, data := range [][]byte{raw, changed} {
				source := filepath.Join(t.TempDir(), "source")
				if err := os.WriteFile(source, data, 0600); err != nil {
					t.Fatal(err)
				}
				id, err := a.localBinlogID(source)
				if err != nil {
					t.Fatal(err)
				}
				if err := a.archiveOne(context.Background(), source, []string{"mysql-bin.000002", "mysql-bin.000001"}[i], id); err != nil {
					t.Fatal(err)
				}
			}
			m, err := loadManifest(context.Background(), store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			guarded := m.Epoch != nil && m.Epoch.ResetReason != ""
			if guarded != reissue {
				t.Fatalf("file order/framing confused with original issuance: reissue=%v epoch=%+v", reissue, m.Epoch)
			}
		})
	}
}
func TestRereview3ObsoleteGuardHasBoundedRecovery(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-11"}, store, slog.Default())
	id, err := a.localBinlogID("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, "testdata/incarnation-a.bin", "mysql-bin.000001", id); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch == nil {
		m.Epoch = &ArchiveEpoch{}
	}
	m.Epoch.ResetReason = "obsolete upload-order guard"
	if err := saveManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	if err := AcknowledgeArchiveHistory(ctx, store, "binlogs", "iad", "verified canonical backup", coverageTestSID+":1-10"); err == nil {
		t.Fatal("acknowledgement omitted a needed transaction")
	}
	if err := AcknowledgeArchiveHistory(ctx, store, "binlogs", "iad", "verified canonical backup", coverageTestSID+":1-11"); err != nil {
		t.Fatal(err)
	}
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch.ResetReason != "" || len(m.Acknowledged) != 1 || m.Acknowledged[0].PriorResetReason == "" {
		t.Fatal("false guard was not recoverable with an auditable bound")
	}
}
