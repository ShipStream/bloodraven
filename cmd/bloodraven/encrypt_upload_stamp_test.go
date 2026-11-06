package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// Review B7: encrypt-upload is the encrypted Job's final step and finishes
// the backup stamp with the serverClock.end dump.py left in
// BLOODRAVEN_DUMP_META.json. Without an end it fails before uploading, so
// no complete-looking artifact sits beside an unfinished stamp. Review
// F016: a location whose stamp is no longer this backup's (a same-name
// backup of another namespace replaced it) refuses before any upload.
func TestEncryptUploadFinishesBackupStamp(t *testing.T) {
	if os.Getenv("BLOODRAVEN_TEST_ENCRYPT_STAMP") == "1" {
		useTempDirMountVerifier(t)
		runEncryptUpload(nil)
		return
	}
	const passphrase = "test-encryption-passphrase"
	end := "2026-11-04T13:00:00.654321Z"
	for _, tc := range []struct {
		name       string
		meta       dumpMeta
		foreign    bool
		wantFinish bool
		wantOutput string
	}{
		{name: "end from dump meta", meta: dumpMeta{GTIDExecuted: "uuid:1-3", ServerClockEnd: end}, wantFinish: true},
		{name: "missing end fails before the upload", meta: dumpMeta{GTIDExecuted: "uuid:1-3"}, wantOutput: "did not report serverClock.end"},
		{name: "another backup's stamp fails before the upload", meta: dumpMeta{GTIDExecuted: "uuid:1-3", ServerClockEnd: end}, foreign: true,
			wantOutput: "holds another backup's stamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, mount := t.TempDir(), t.TempDir()
			data, err := json.Marshal(tc.meta)
			if err != nil {
				t.Fatal(err)
			}
			for name, contents := range map[string][]byte{dumpMetaFileName: data, "chunk.json": []byte("dump data")} {
				if err := os.WriteFile(filepath.Join(src, name), contents, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			passphraseFile := filepath.Join(t.TempDir(), "passphrase")
			if err := os.WriteFile(passphraseFile, []byte(passphrase), 0o600); err != nil {
				t.Fatal(err)
			}
			// The backup-stamp init container's stamp: started, no end.
			store, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", PassphraseFile: passphraseFile,
				PVC: &sidecar.PITRPVCConfig{MountPath: mount, Writable: true, WritePrefix: "prod", MountVerifier: sidecar.TempDirMountVerifierForTest}})
			if err != nil {
				t.Fatal(err)
			}
			key := "prod/nightly-1" + backupmeta.StampSuffix
			stamp := startedStamp(stampStart)
			if tc.foreign {
				stamp.Backup.Namespace, stamp.Backup.UID = "other-ns", "other-uid"
			}
			if _, _, err := writeBackupStamp(context.Background(), store, store, key, stamp); err != nil {
				t.Fatal(err)
			}
			template, err := json.Marshal(stampTemplate())
			if err != nil {
				t.Fatal(err)
			}
			location := filepath.Join(mount, "prod", "nightly-1")
			cmd := exec.Command(os.Args[0], "-test.run=^TestEncryptUploadFinishesBackupStamp$")
			cmd.Env = append(os.Environ(),
				"BLOODRAVEN_TEST_ENCRYPT_STAMP=1", "BLOODRAVEN_BACKUP_NAME=nightly-1",
				"BLOODRAVEN_SOURCE_DIR="+src, "BLOODRAVEN_STORAGE_TYPE=PVC",
				"BLOODRAVEN_PVC_MOUNT_PATH="+mount, "BLOODRAVEN_OUTPUT_URL="+location,
				"BLOODRAVEN_BACKUP_PASSPHRASE_FILE="+passphraseFile, "BLOODRAVEN_ENCRYPTION_ALGORITHM=AES-256-GCM",
				backupStampTemplateEnv+"="+string(template),
			)
			out, runErr := cmd.CombinedOutput()

			got, err := backupmeta.ParseStamp(stampBytes(t, store, key))
			if err != nil {
				t.Fatal(err)
			}
			_, uploadErr := os.Stat(filepath.Join(location, "chunk.json"))
			if !tc.wantFinish {
				if runErr == nil || got.Complete() || !sameStampBytes(got, stamp) || !bytes.Contains(out, []byte(tc.wantOutput)) {
					t.Fatalf("err=%v complete=%t; output %s", runErr, got.Complete(), out)
				}
				if !os.IsNotExist(uploadErr) || strings.Contains(string(out), "BLOODRAVEN_DUMP_COMPLETE") {
					t.Fatalf("uploaded a dump whose stamp cannot be finished (%v): %s", uploadErr, out)
				}
				return
			}
			if runErr != nil {
				t.Fatalf("encrypt-upload: %v: %s", runErr, out)
			}
			want := startedStamp(stampStart)
			wantEnd, _ := backupmeta.ParseMicroTime(end)
			want.ServerClock.End = wantEnd
			if !sameStampBytes(got, want) {
				gb, _ := got.Canonical()
				wb, _ := want.Canonical()
				t.Fatalf("finished stamp\n got %s\nwant %s", gb, wb)
			}
			if !got.ServerClock.End.Equal(time.Date(2026, 11, 4, 13, 0, 0, 654321000, time.UTC)) || uploadErr != nil {
				t.Fatalf("end %s, upload %v", got.ServerClock.End, uploadErr)
			}
		})
	}
}
