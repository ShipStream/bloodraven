package sidecar

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// TestPITRNativeEncryptedBinlogNotArchivedToday is opt-in
// (BLOODRAVEN_PITR_NATIVE_TEST=1, docker, the mysql:9.7 image; CI job
// test-pitr-native). It pins what the PITR archiver does TODAY on a server
// with binlog_encryption=ON, which spec.encryptionAtRest renders by default
// (coverage.binaryLog): it archives nothing. proposals/22-pitr-binlog-
// encryption.md describes the options; a fix has to change this test on
// purpose.
//
// The server is a real mysqld with component_keyring_file configured as the
// operator renders it (keyringManifestJSON and keyringComponentConfigJSON in
// internal/controller/encryption.go) and the encryption settings of
// encryptionMySQLSettings. The archiver is the sidecar's own, over the
// server's data directory, with the sidecar's MySQL client as its role
// probe. The test asserts:
//
//   - the file format: every binlog starts with the magic 0xFD 'b' 'i' 'n'
//     and a version-1 header that names the keyring key
//     MySQLReplicationKey_<server_uuid>_<seqno>, a 32-byte encrypted file
//     password and a 16-byte IV, and the keyring file holds that key;
//   - a read-only site: the tail is Blocked on the first header read, with
//     no archiver error (a quiet standby);
//   - the promotion: the record is written without a promotion set;
//   - a writable site: every sealed file fails with "invalid binlog magic",
//     nothing is uploaded, no manifest row is written, and the failure is
//     reported (lastError, backlogFiles, consecutiveFailedScans), which the
//     operator turns into PITRArchiveReady=False/ArchiverError;
//   - the demoted site again: the tail stays Blocked.
//
// It then records two facts the proposal's options rest on, with test-only
// code (the sidecar has no such code): a sealed file decrypts with the
// keyring's key to a plaintext binlog the archiver's parser accepts, and
// ALTER INSTANCE ROTATE BINLOG MASTER KEY rewrites the header of every
// sealed file in place, so the bytes of a sealed encrypted file are not
// stable while its plaintext is.
func TestPITRNativeEncryptedBinlogNotArchivedToday(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("set BLOODRAVEN_PITR_NATIVE_TEST=1 (needs docker and the mysql:9.7 image)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	ctx := context.Background()
	srv := startEncNativeServer(t, docker, true)
	db := srv.live.db
	sql := func(statements ...string) {
		t.Helper()
		for _, s := range statements {
			if _, err := db.ExecContext(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	var uuid string
	var encryption int
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid, @@binlog_encryption").Scan(&uuid, &encryption); err != nil {
		t.Fatal(err)
	}
	if encryption != 1 {
		t.Fatalf("binlog_encryption = %d, want ON", encryption)
	}

	// A writable era the archiver never saw, then read-only: the state of
	// a site whose sidecar starts beside a standby.
	sql("CREATE DATABASE wp", "CREATE TABLE wp.t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(32))",
		"INSERT INTO wp.t (v) VALUES ('one'), ('two')", "FLUSH BINARY LOGS", "SET GLOBAL super_read_only = ON")

	// The file format.
	index, err := readBinlogIndex(filepath.Join(srv.dataDir, "mysql-bin.index"))
	if err != nil || len(index) < 2 {
		t.Fatalf("binlog index: %v, %v", index, err)
	}
	wantKey := "MySQLReplicationKey_" + uuid + "_1"
	for _, entry := range index {
		h := readEncNativeHeader(t, filepath.Join(srv.dataDir, filepath.Base(entry)))
		if h.version != 1 || h.keyID != wantKey || len(h.password) != 32 || len(h.iv) != 16 {
			t.Fatalf("%s: header %+v, want version 1 and key %s", entry, h, wantKey)
		}
	}
	keys := srv.keyring(t)
	if len(keys[wantKey]) != 32 || keys["MySQLReplicationKey_"+uuid] == nil {
		t.Fatalf("keyring entries %v lack the binary log key %s or its sequence number", encNativeKeyNames(keys), wantKey)
	}

	// The sidecar's readers, one by one, on a sealed file.
	sealed := filepath.Join(srv.dataDir, filepath.Base(index[0]))
	raw, err := os.ReadFile(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InspectBinlog(bytes.NewReader(raw), time.Time{}); err == nil || err.Error() != "invalid binlog magic" {
		t.Fatalf("InspectBinlog: %v, want invalid binlog magic", err)
	}
	if _, err := parseBinlogMetadata(sealed); err == nil || err.Error() != "invalid binlog magic" {
		t.Fatalf("parseBinlogMetadata: %v, want invalid binlog magic", err)
	}
	_, headerErr := localBinlogHeader(sealed)
	if headerErr == nil {
		t.Fatal("localBinlogHeader read the Previous_gtids of an encrypted file")
	}
	t.Logf("localBinlogHeader: %v", headerErr)
	if _, err := (&originWalk{}).Walk(bytes.NewReader(raw), int64(len(raw)), newOriginBudget()); !errors.Is(err, errEncryptedBinlog) {
		t.Fatalf("origin walk: %v, want %v", err, errEncryptedBinlog)
	}

	// The archiver.
	store := newTailStore()
	log := &psLog{}
	cfg := &PITRConfig{BinlogDir: srv.dataDir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour}
	a := newTestArchiver(cfg, "iad", srv.live, store, slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Read-only, before any promotion.
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	t.Logf("read-only: %s", encNativeStatus(st))
	if st.Tail == nil || st.Tail.State != internalmysql.ArchiverTailBlocked || !strings.Contains(st.Tail.Reason, "read the header of mysql-bin.") {
		t.Fatalf("read-only tail = %+v, want Blocked on a header read", st.Tail)
	}
	assertQuiet(t, st)

	// The promotion, as the operator does it: writable, then a rotation.
	sql("SET GLOBAL read_only = OFF", "FLUSH BINARY LOGS", "INSERT INTO wp.t (v) VALUES ('three')")
	var records []PromotionRecord
	for i := 0; i < 20 && len(records) == 0; i++ {
		a.roleTick(ctx)
		a.scanAndArchive(ctx)
		m, err := loadManifest(ctx, store, "binlogs", "iad")
		if err != nil {
			t.Fatal(err)
		}
		records = m.Promotions
		if len(records) == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Logf("records: %+v", records)
	if len(records) != 1 || records[0].ServerUUID != uuid || records[0].ReadOnlyGTIDExecuted == "" || records[0].PromotionGTIDExecuted != "" {
		t.Fatalf("promotion records = %+v, want one of %s with a read-only observation and no promotion set", records, uuid)
	}
	if text := log.String(); !strings.Contains(text, `msg="promotion set not recorded"`) || !strings.Contains(text, `reason="read the header of mysql-bin.`) {
		t.Fatalf("the log does not say why the promotion set is missing:\n%s", text)
	}

	sql("INSERT INTO wp.t (v) VALUES ('four')", "FLUSH BINARY LOGS", "INSERT INTO wp.t (v) VALUES ('five')", "FLUSH BINARY LOGS")
	for range 3 {
		a.scanAndArchive(ctx)
	}
	st = a.Snapshot(ctx)
	t.Logf("writable: %s", encNativeStatus(st))
	index, err = readBinlogIndex(filepath.Join(srv.dataDir, "mysql-bin.index"))
	if err != nil {
		t.Fatal(err)
	}
	sealedFiles := int64(len(index) - 1)
	if !st.Primary || st.FilesArchived != 0 || st.BacklogFiles != sealedFiles || st.ConsecutiveFailedScans < 2 ||
		!strings.HasPrefix(st.LastError, "archive mysql-bin.") || !strings.HasSuffix(st.LastError, ": invalid binlog magic") ||
		st.Tail == nil || st.Tail.State != internalmysql.ArchiverTailBlocked || st.Tail.PendingFiles != sealedFiles {
		t.Fatalf("writable status = %s, want every one of the %d sealed files failing with invalid binlog magic", encNativeStatus(st), sealedFiles)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 0 || store.uploaded() != "" {
		t.Fatalf("the archive holds %d rows and the uploads %q, want nothing", len(m.Files), store.uploaded())
	}

	sql("SET GLOBAL super_read_only = ON")
	for range 2 {
		a.scanAndArchive(ctx)
	}
	st = a.Snapshot(ctx)
	t.Logf("demoted: %s", encNativeStatus(st))
	// The upload failure of the writable era stands (the drain cannot
	// decide), so the demoted site keeps reporting it.
	if st.Primary || st.FilesArchived != 0 || st.Tail == nil || st.Tail.State != internalmysql.ArchiverTailBlocked ||
		!strings.Contains(st.Tail.Reason, "read the header of mysql-bin.") || !strings.HasSuffix(st.LastError, ": invalid binlog magic") ||
		store.uploaded() != "" {
		t.Fatalf("demoted status = %s, uploads %q; want a Blocked tail, the standing error and no upload", encNativeStatus(st), store.uploaded())
	}

	// What the options of the proposal rest on. Every sealed file
	// decrypts to a binlog the archiver's parser reads to its end, with
	// checksums verified, and together they hold the server's transactions.
	logged := internalmysql.GTIDSet{}
	var plain []byte
	for i, entry := range index[:len(index)-1] {
		data, err := os.ReadFile(filepath.Join(srv.dataDir, filepath.Base(entry)))
		if err != nil {
			t.Fatal(err)
		}
		decrypted := decryptEncNativeBinlog(t, data, keys)
		if len(decrypted) != len(data)-encNativeHeaderSize {
			t.Fatalf("%s: %d plaintext bytes of %d", entry, len(decrypted), len(data))
		}
		evidence, err := InspectBinlog(bytes.NewReader(decrypted), time.Time{})
		if err != nil {
			t.Fatalf("decrypted %s does not parse: %v", entry, err)
		}
		logged = UnionBinlogGTIDs(logged, evidence.GTIDs)
		if i == 0 {
			plain = decrypted
		}
	}
	if !hasSourceUUID(logged, uuid) {
		t.Fatalf("the decrypted files hold %s, nothing of %s", logged, uuid)
	}
	t.Logf("decrypted %d sealed files: %s", len(index)-1, logged)
	raw, err = os.ReadFile(sealed)
	if err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(raw)
	sql("SET GLOBAL read_only = OFF", "ALTER INSTANCE ROTATE BINLOG MASTER KEY")
	rotated, err := os.ReadFile(sealed)
	if err != nil {
		t.Fatal(err)
	}
	after := sha256.Sum256(rotated)
	h := readEncNativeHeader(t, sealed)
	keys = srv.keyring(t)
	t.Logf("after rotation: key %s, same bytes %v, same size %v, keyring %v", h.keyID, before == after, len(raw) == len(rotated), encNativeKeyNames(keys))
	if h.keyID != "MySQLReplicationKey_"+uuid+"_2" || before == after || len(raw) != len(rotated) || keys[wantKey] != nil || len(keys[h.keyID]) != 32 {
		t.Fatalf("after the rotation %s names %s (keyring %v); want its header rewritten in place for key 2 and key 1 removed", filepath.Base(sealed), h.keyID, encNativeKeyNames(keys))
	}
	if !bytes.Equal(decryptEncNativeBinlog(t, rotated, keys), plain) {
		t.Fatal("the plaintext changed with the master key")
	}

	// Turning binlog_encryption off later (coverage.binaryLog: false on a
	// group that ran with it on) does not repair the archive: the files
	// written from then on are plaintext and are uploaded, the encrypted
	// ones before them still fail, so the error stands and the archive
	// starts with a gap.
	sql("SET GLOBAL binlog_encryption = OFF", "INSERT INTO wp.t (v) VALUES ('six')", "FLUSH BINARY LOGS",
		"INSERT INTO wp.t (v) VALUES ('seven')", "FLUSH BINARY LOGS")
	for range 4 {
		a.roleTick(ctx)
		a.scanAndArchive(ctx)
	}
	st = a.Snapshot(ctx)
	t.Logf("binlog_encryption off again: %s, uploads %q", encNativeStatus(st), store.uploaded())
	if st.FilesArchived != 2 || len(strings.Split(store.uploaded(), ",")) != 2 || st.BacklogFiles == 0 || !strings.HasSuffix(st.LastError, ": invalid binlog magic") {
		t.Fatalf("status = %s, uploads %q; want the two plaintext files uploaded and the encrypted ones still failing", encNativeStatus(st), store.uploaded())
	}
}

// TestPITRNativeEncryptionAtRestWithoutBinlogEncryptionArchives is the
// control of TestPITRNativeEncryptedBinlogNotArchivedToday (same opt-in):
// the same server with the keyring and every other encryption setting on
// and binlog_encryption off (spec.encryptionAtRest.coverage.binaryLog:
// false) writes plaintext binlogs, and the archiver uploads every sealed
// one.
func TestPITRNativeEncryptionAtRestWithoutBinlogEncryptionArchives(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("set BLOODRAVEN_PITR_NATIVE_TEST=1 (needs docker and the mysql:9.7 image)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	ctx := context.Background()
	srv := startEncNativeServer(t, docker, false)
	db := srv.live.db
	for _, s := range []string{"CREATE DATABASE wp", "CREATE TABLE wp.t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(32))",
		"INSERT INTO wp.t (v) VALUES ('one')", "FLUSH BINARY LOGS", "INSERT INTO wp.t (v) VALUES ('two')", "FLUSH BINARY LOGS"} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	var tableEncryption string
	if err := db.QueryRowContext(ctx, "SELECT CREATE_OPTIONS FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'wp' AND TABLE_NAME = 't'").Scan(&tableEncryption); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tableEncryption, "ENCRYPTION='Y'") {
		t.Fatalf("wp.t has options %q: the keyring is not in use", tableEncryption)
	}
	index, err := readBinlogIndex(filepath.Join(srv.dataDir, "mysql-bin.index"))
	if err != nil {
		t.Fatal(err)
	}
	store := newTailStore()
	cfg := &PITRConfig{BinlogDir: srv.dataDir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour}
	a := newTestArchiver(cfg, "iad", srv.live, store, slog.New(slog.NewTextHandler(&psLog{}, nil)))
	var st Status
	for i := 0; i < 20; i++ {
		a.roleTick(ctx)
		a.scanAndArchive(ctx)
		if st = a.Snapshot(ctx); st.LastError == "" && st.BacklogFiles == 0 && st.FilesArchived > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if sealed := len(index) - 1; st.LastError != "" || st.BacklogFiles != 0 || int(st.FilesArchived) != sealed || len(m.Files) != sealed {
		t.Fatalf("status = %s with %d manifest rows, want the %d sealed files archived", encNativeStatus(st), len(m.Files), sealed)
	}
}

// encNativeStatus is what the operator reads from /archiver/status.
func encNativeStatus(st Status) string {
	tail := "none"
	if st.Tail != nil {
		tail = fmt.Sprintf("%s pending=%d active=%s reason=%q", st.Tail.State, st.Tail.PendingFiles, st.Tail.ActiveFile, st.Tail.Reason)
	}
	return fmt.Sprintf("primary=%v filesArchived=%d backlogFiles=%d uploadFailures=%d consecutiveFailedScans=%d lastError=%q tail={%s}",
		st.Primary, st.FilesArchived, st.BacklogFiles, st.UploadFailures, st.ConsecutiveFailedScans, st.LastError, tail)
}

const encNativeRootPassword = "enc-native-root"

// encNativeServer is a mysqld with an encrypted binary log whose data
// directory and keyring file the test reads from the host.
type encNativeServer struct {
	dataDir    string
	keyringDir string
	live       *LiveMysql
}

// startEncNativeServer runs mysql:9.7 as the test's own user, so the binlog
// files (mode 0640) are readable here the way they are to the sidecar
// container, which shares mysqld's uid.
func startEncNativeServer(t *testing.T, docker string, binlogEncryption bool) *encNativeServer {
	t.Helper()
	srv := &encNativeServer{dataDir: t.TempDir(), keyringDir: t.TempDir()}
	conf := t.TempDir()
	// The two component files, as internal/controller/encryption.go
	// renders them for an unsealed site.
	manifest := filepath.Join(conf, "mysqld.my")
	component := filepath.Join(conf, "component_keyring_file.cnf")
	for path, body := range map[string]string{
		manifest:  "{\n  \"components\": \"file://component_keyring_file\"\n}\n",
		component: "{\n  \"read_local_config\": false,\n  \"path\": \"/run/mysql-keyring/keyring\",\n  \"read_only\": false\n}\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil { //nolint:gosec // read by the container's user
			t.Fatal(err)
		}
	}
	for _, dir := range []string{srv.dataDir, srv.keyringDir, conf} {
		if err := os.Chmod(dir, 0o755); err != nil { //nolint:gosec // bind-mounted into the container
			t.Fatal(err)
		}
	}
	name := fmt.Sprintf("bloodraven-enc-native-%d", time.Now().UnixNano())
	binlogFlag := "--binlog-encryption=OFF"
	if binlogEncryption {
		binlogFlag = "--binlog-encryption=ON"
	}
	out, err := exec.Command(docker, "run", "-d", "--name", name,
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-e", "MYSQL_ROOT_PASSWORD="+encNativeRootPassword,
		"-p", "127.0.0.1::3306",
		"-v", srv.dataDir+":/var/lib/mysql",
		"-v", srv.keyringDir+":/run/mysql-keyring",
		"-v", manifest+":/usr/sbin/mysqld.my:ro",
		"-v", component+":/usr/lib64/mysql/plugin/component_keyring_file.cnf:ro",
		"mysql:9.7", "mysqld",
		"--server-id=101", "--log-bin=mysql-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
		// encryptionMySQLSettings; binlog_encryption is coverage.binaryLog.
		"--default-table-encryption=ON", "--table-encryption-privilege-check=ON",
		"--innodb-redo-log-encrypt=ON", "--innodb-undo-log-encrypt=ON", binlogFlag,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v %s", err, out)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command(docker, "logs", "--tail", "40", name).CombinedOutput()
			t.Logf("mysqld log:\n%s", logs)
		}
		_ = exec.Command(docker, "rm", "-f", "-v", name).Run()
	})
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatal("mysqld did not accept connections within 3 minutes")
		}
		time.Sleep(time.Second)
		port, err := exec.Command(docker, "port", name, "3306/tcp").Output()
		if err != nil {
			continue
		}
		addr := strings.TrimSpace(strings.SplitN(string(port), "\n", 2)[0])
		live, err := NewLiveMysqlFromDSN(fmt.Sprintf("root:%s@tcp(%s)/", encNativeRootPassword, addr))
		if err != nil {
			t.Fatal(err)
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = live.db.PingContext(pingCtx)
		cancel()
		if err == nil {
			srv.live = live
			t.Cleanup(func() { _ = live.db.Close() })
			return srv
		}
		_ = live.db.Close()
	}
}

// keyring reads component_keyring_file's data file: JSON whose elements
// carry a data_id and the hex-encoded key.
func (s *encNativeServer) keyring(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.keyringDir, "keyring"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Elements []struct {
			DataID string `json:"data_id"`
			Data   string `json:"data"`
		} `json:"elements"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("keyring file: %v", err)
	}
	keys := make(map[string][]byte, len(file.Elements))
	for _, e := range file.Elements {
		key, err := hex.DecodeString(e.Data)
		if err != nil {
			t.Fatalf("keyring entry %s: %v", e.DataID, err)
		}
		keys[e.DataID] = key
	}
	return keys
}

func encNativeKeyNames(keys map[string][]byte) []string {
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	return names
}

// encNativeHeaderSize is the size of the encryption header in front of the
// encrypted stream.
const encNativeHeaderSize = 512

// encNativeHeader is the version-1 header of an encrypted binlog file:
// magic(4) version(1), then type-tagged fields, zero-padded to 512 bytes.
type encNativeHeader struct {
	version  byte
	keyID    string // field 1: one length byte, then the keyring key's id
	password []byte // field 2: the file password, encrypted, 32 bytes
	iv       []byte // field 3: the IV the file password was encrypted with, 16 bytes
}

func readEncNativeHeader(t *testing.T, path string) encNativeHeader {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return parseEncNativeHeader(t, data)
}

func parseEncNativeHeader(t *testing.T, data []byte) encNativeHeader {
	t.Helper()
	if len(data) < encNativeHeaderSize || !bytes.Equal(data[:4], []byte{0xfd, 'b', 'i', 'n'}) {
		t.Fatalf("not an encrypted binlog: %d bytes, magic % x", len(data), data[:min(4, len(data))])
	}
	h := encNativeHeader{version: data[4]}
	for p := 5; p < encNativeHeaderSize && data[p] != 0; {
		kind := data[p]
		p++
		switch kind {
		case 1:
			n := int(data[p])
			h.keyID = string(data[p+1 : p+1+n])
			p += 1 + n
		case 2:
			h.password = data[p : p+32]
			p += 32
		case 3:
			h.iv = data[p : p+16]
			p += 16
		default:
			t.Fatalf("unknown header field %d at offset %d", kind, p-1)
		}
	}
	return h
}

// decryptEncNativeBinlog decrypts an encrypted binlog file with the keyring
// key its header names. The file password is AES-256-CBC(master key, header
// IV) without padding; the stream key and nonce are the first 32 and the
// next 8 bytes of SHA-512(file password); the stream is AES-256-CTR with a
// big-endian block counter in the low 8 bytes, starting at 0 right after
// the 512-byte header. The plaintext is a whole binlog file, magic included.
func decryptEncNativeBinlog(t *testing.T, data []byte, keys map[string][]byte) []byte {
	t.Helper()
	h := parseEncNativeHeader(t, data)
	master := keys[h.keyID]
	if len(master) != 32 {
		t.Fatalf("the keyring does not hold %s (it holds %v)", h.keyID, encNativeKeyNames(keys))
	}
	block, err := aes.NewCipher(master)
	if err != nil {
		t.Fatal(err)
	}
	password := make([]byte, 32)
	cipher.NewCBCDecrypter(block, h.iv).CryptBlocks(password, h.password)
	derived := sha512.Sum512(password)
	stream, err := aes.NewCipher(derived[:32])
	if err != nil {
		t.Fatal(err)
	}
	iv := make([]byte, 16)
	copy(iv, derived[32:40])
	plain := make([]byte, len(data)-encNativeHeaderSize)
	cipher.NewCTR(stream, iv).XORKeyStream(plain, data[encNativeHeaderSize:])
	return plain
}
