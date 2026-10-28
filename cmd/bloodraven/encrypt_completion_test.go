package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/backupmeta"
)

func TestEncryptUploadCompletionSentinel(t *testing.T) {
	if os.Getenv("BLOODRAVEN_TEST_ENCRYPT_SENTINEL") == "1" {
		runEncryptUpload(nil)
		return
	}
	src, mount := t.TempDir(), t.TempDir()
	meta := dumpMeta{GTIDExecuted: "uuid:my_tag:1-3,uuid2:1-5", BinlogFile: "mysql_bin.000001", BinlogPos: 42}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string][]byte{dumpMetaFileName: data, "chunk.json": []byte("dump data")} {
		if err := os.WriteFile(filepath.Join(src, name), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	passphraseFile := filepath.Join(t.TempDir(), "passphrase")
	if err := os.WriteFile(passphraseFile, []byte("test-encryption-passphrase"), 0o600); err != nil {
		t.Fatal(err)
	}
	location := filepath.Join(mount, "prod_backups/db dumps/備份%+=")
	cmd := exec.Command(os.Args[0], "-test.run=^TestEncryptUploadCompletionSentinel$")
	cmd.Env = append(os.Environ(),
		"BLOODRAVEN_TEST_ENCRYPT_SENTINEL=1", "BLOODRAVEN_BACKUP_NAME=encoding-test",
		"BLOODRAVEN_SOURCE_DIR="+src, "BLOODRAVEN_STORAGE_TYPE=PVC",
		"BLOODRAVEN_PVC_MOUNT_PATH="+mount, "BLOODRAVEN_OUTPUT_URL="+location,
		"BLOODRAVEN_BACKUP_PASSPHRASE_FILE="+passphraseFile, "BLOODRAVEN_ENCRYPTION_ALGORITHM=AES-256-GCM",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("encrypt-upload subprocess: %v: %s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		got, ok := backupmeta.ParseDumpComplete(line)
		if !ok {
			continue
		}
		if got.Location != location || got.GtidExecuted != meta.GTIDExecuted || got.BinlogFile != meta.BinlogFile || got.BinlogPos != 42 || !got.Encrypted || got.EncryptionAlgorithm != "AES-256-GCM" || got.SizeBytes != 9 || got.Files != 1 || got.CiphertextBytes <= 9 {
			t.Fatalf("encrypted producer corrupted metadata: %+v", got)
		}
		if _, err := os.Stat(filepath.Join(location, "chunk.json")); err != nil {
			t.Fatalf("sentinel does not identify the actual artifact: %v", err)
		}
		return
	}
	t.Fatalf("encrypted producer emitted no valid completion sentinel: %s", out)
}
