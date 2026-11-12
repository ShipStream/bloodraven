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
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// TestPITRNativeEncryptedBinlogArchived is opt-in
// (BLOODRAVEN_PITR_NATIVE_TEST=1, docker, the mysql:9.7 image; CI job
// test-pitr-native). It runs the PITR archiver on a server with
// binlog_encryption=ON, which spec.encryptionAtRest renders by default
// (coverage.binaryLog), and checks that it archives the plaintext of every
// sealed file, read with the key in the server's keyring
// (local_binlog.go). The reader rests on a MySQL file format, so this test
// is what fails when a server changes it.
//
// The server is a real mysqld with component_keyring_file configured as the
// operator renders it (keyringManifestJSON and keyringComponentConfigJSON in
// internal/controller/encryption.go) and the encryption settings of
// encryptionMySQLSettings. The archiver is the sidecar's own, over the
// server's data directory, with the sidecar's MySQL client as its role
// probe and the server's keyring file. The test asserts:
//
//   - the file format: every binlog starts with the magic 0xFD 'b' 'i' 'n'
//     and a version-1 header that names the keyring key
//     MySQLReplicationKey_<server_uuid>_<seqno>, a 32-byte encrypted file
//     password and a 16-byte IV, and the keyring file holds that key;
//   - the reader: its plaintext is byte for byte what the test's own
//     decryption gives (written apart from it, with one whole-file
//     AES-CTR pass), at every offset it is asked for, for every sealed
//     file, one of them over 2^16 cipher blocks; every plaintext parses to
//     its end with its checksums verified; without a keyring the file is
//     refused with an error that says so;
//   - a read-only site before any promotion: no header read fails;
//   - the promotion: the record carries a promotion set;
//   - a writable site: every sealed file is uploaded, the uploaded bytes
//     are the plaintext, and its manifest row holds the plaintext's size
//     and SHA-256;
//   - the demoted site: its tail drains;
//   - ALTER INSTANCE ROTATE BINLOG MASTER KEY, which rewrites the header of
//     every file in place: the archiver identifies the files again as the
//     same content, uploads none of them twice and reports no error, and a
//     file written under the new key is archived;
//   - an archiver that was given no keyring archives nothing and names the
//     reason.
func TestPITRNativeEncryptedBinlogArchived(t *testing.T) {
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
	keyringFile := filepath.Join(srv.keyringDir, "keyring")
	binlogs := localBinlogs{keyringFile: keyringFile}
	sealedNames := func() []string {
		t.Helper()
		index, err := readBinlogIndex(filepath.Join(srv.dataDir, "mysql-bin.index"))
		if err != nil || len(index) < 2 {
			t.Fatalf("binlog index: %v, %v", index, err)
		}
		names := make([]string, 0, len(index)-1)
		for _, entry := range index[:len(index)-1] {
			names = append(names, filepath.Base(entry))
		}
		return names
	}

	// A writable era the archiver never saw, then read-only: the state of
	// a site whose sidecar starts beside a standby.
	sql("CREATE DATABASE wp", "CREATE TABLE wp.t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(32))",
		// A file of more than 2^16 cipher blocks, for the counter.
		"CREATE TABLE wp.big (id INT PRIMARY KEY, v LONGTEXT)", "INSERT INTO wp.big VALUES (1, REPEAT('x', 3000000))",
		"INSERT INTO wp.t (v) VALUES ('one'), ('two')", "FLUSH BINARY LOGS", "SET GLOBAL super_read_only = ON")

	// The file format.
	wantKey := "MySQLReplicationKey_" + uuid + "_1"
	for _, name := range sealedNames() {
		h := readEncNativeHeader(t, filepath.Join(srv.dataDir, name))
		if h.version != 1 || h.keyID != wantKey || len(h.password) != 32 || len(h.iv) != 16 {
			t.Fatalf("%s: header %+v, want version 1 and key %s", name, h, wantKey)
		}
	}
	keys := srv.keyring(t)
	if len(keys[wantKey]) != 32 || keys["MySQLReplicationKey_"+uuid] == nil {
		t.Fatalf("keyring entries %v lack the binary log key %s or its sequence number", encNativeKeyNames(keys), wantKey)
	}

	// The reader against the test's own decryption.
	plaintexts := map[string][]byte{}
	checkReader := func(name string) {
		t.Helper()
		path := filepath.Join(srv.dataDir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := decryptEncNativeBinlog(t, raw, srv.keyring(t))
		if old, ok := plaintexts[name]; ok && !bytes.Equal(old, want) {
			t.Fatalf("%s: the plaintext of a sealed file changed", name)
		}
		plaintexts[name] = want
		if _, err := InspectBinlog(bytes.NewReader(raw), time.Time{}); err == nil {
			t.Fatalf("%s: the bytes on disk parse as a binlog; the file is not encrypted", name)
		}
		if _, err := localBinlogHeader(path); !errors.Is(err, errEncryptedBinlogNoKeyring) {
			t.Fatalf("%s without a keyring: %v, want %v", name, err, errEncryptedBinlogNoKeyring)
		}
		file, err := binlogs.open(path)
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		defer file.Close()
		if file.Size() != int64(len(want)) || int64(len(raw)) != file.Size()+encryptedBinlogHeaderSize {
			t.Fatalf("%s: plaintext size %d of a %d-byte file, want %d", name, file.Size(), len(raw), len(want))
		}
		got, err := io.ReadAll(file.NewReader())
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: the reader's plaintext differs from the test's decryption (%d and %d bytes, %v)", name, len(got), len(want), err)
		}
		// Random access, as the origin walk reads: across block and
		// 2^16-block boundaries and up to the end.
		for _, at := range []struct{ off, n int }{{0, 4}, {1, 31}, {15, 2}, {16, 16}, {4093, 9000}, {1<<20 - 7, 40}, {len(want) - 5, 5}, {len(want) - 5, 50}} {
			if at.off < 0 || at.off >= len(want) {
				continue
			}
			buf := make([]byte, at.n)
			n, err := file.ReadAt(buf, int64(at.off))
			end := min(at.off+at.n, len(want))
			if !bytes.Equal(buf[:n], want[at.off:end]) || (n < at.n) != (err == io.EOF) || (err != nil && err != io.EOF) {
				t.Fatalf("%s: ReadAt(%d, %d) = %d bytes, %v; want the plaintext at that offset", name, at.off, at.n, n, err)
			}
		}
		evidence, err := InspectBinlog(bytes.NewReader(got), time.Time{})
		if err != nil {
			t.Fatalf("the plaintext of %s does not parse: %v", name, err)
		}
		sum := sha256.Sum256(want)
		if id, err := binlogs.contentID(path); err != nil || id != hex.EncodeToString(sum[:]) || id != evidence.Digest {
			t.Fatalf("%s: content identity %s, %v; want the SHA-256 of the plaintext", name, id, err)
		}
		header, err := binlogs.header(path)
		if err != nil || header.size != int64(len(want)) || header.previous != evidence.Metadata.PreviousGTIDs {
			t.Fatalf("%s: header %+v, %v; want Previous_gtids %q and the plaintext size", name, header, err, evidence.Metadata.PreviousGTIDs)
		}
	}
	largest := 0
	for _, name := range sealedNames() {
		checkReader(name)
		largest = max(largest, len(plaintexts[name]))
	}
	if largest < 1<<20 {
		t.Fatalf("the largest sealed file has %d plaintext bytes, want one over 1 MiB", largest)
	}

	// The archiver.
	store := newTailStore()
	log := &psLog{}
	cfg := &PITRConfig{BinlogDir: srv.dataDir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour, KeyringFile: keyringFile}
	a := newTestArchiver(cfg, "iad", srv.live, store, slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug})))
	scan := func(times int) Status {
		for range times {
			a.roleTick(ctx)
			a.scanAndArchive(ctx)
		}
		return a.Snapshot(ctx)
	}

	// Read-only, before any promotion: nothing to archive, nothing fails.
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	t.Logf("read-only: %s", encNativeStatus(st))
	if st.Tail != nil && strings.Contains(st.Tail.Reason, "read the header") {
		t.Fatalf("read-only tail = %+v: a header could not be read", st.Tail)
	}
	assertQuiet(t, st)
	if store.uploaded() != "" {
		t.Fatalf("uploads %q before any promotion record", store.uploaded())
	}

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
	if len(records) != 1 || records[0].ServerUUID != uuid || records[0].ReadOnlyGTIDExecuted == "" || records[0].PromotionGTIDExecuted == "" {
		t.Fatalf("promotion records = %+v, want one of %s with a read-only observation and a promotion set\n%s", records, uuid, log.String())
	}

	sql("INSERT INTO wp.t (v) VALUES ('four')", "FLUSH BINARY LOGS", "INSERT INTO wp.t (v) VALUES ('five')", "FLUSH BINARY LOGS")
	assertArchived := func(when string) {
		t.Helper()
		var st Status
		sealed := sealedNames()
		for i := 0; i < 40; i++ {
			if st = scan(1); st.LastError == "" && st.BacklogFiles == 0 && int(st.FilesArchived) >= len(sealed) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("%s: %s", when, encNativeStatus(st))
		if st.LastError != "" || st.BacklogFiles != 0 || st.UploadFailures != 0 {
			t.Fatalf("%s: status = %s, want every sealed file archived without an error\n%s", when, encNativeStatus(st), log.String())
		}
		m, err := loadManifest(ctx, store, "binlogs", "iad")
		if err != nil {
			t.Fatal(err)
		}
		rows := map[string]ManifestEntry{}
		for _, row := range m.Files {
			if _, twice := rows[row.Name]; twice {
				t.Fatalf("%s: two manifest rows for %s", when, row.Name)
			}
			rows[row.Name] = row
		}
		if len(rows) != len(sealed) {
			t.Fatalf("%s: %d manifest rows for %d sealed files", when, len(rows), len(sealed))
		}
		for _, name := range sealed {
			checkReader(name)
			want := plaintexts[name]
			sum := sha256.Sum256(want)
			row := rows[name]
			if row.Size != int64(len(want)) || row.ID != store.BinlogContentID(hex.EncodeToString(sum[:])) {
				t.Fatalf("%s: row of %s has size %d and id %s; want the plaintext's %d bytes and SHA-256", when, name, row.Size, row.ID, len(want))
			}
			key, err := ArchiveEntryKey("binlogs", "iad", row)
			if err != nil {
				t.Fatal(err)
			}
			object, ok, err := store.Get(ctx, key)
			if err != nil || !ok || !bytes.Equal(object, want) {
				t.Fatalf("%s: the archived object of %s is not the plaintext (%d bytes, found %v, %v)", when, name, len(object), ok, err)
			}
		}
	}
	assertArchived("writable")

	// Demoted: the tail drains.
	sql("SET GLOBAL super_read_only = ON", "FLUSH BINARY LOGS")
	st = scan(3)
	t.Logf("demoted: %s", encNativeStatus(st))
	if st.Primary || st.LastError != "" || st.Tail == nil || st.Tail.State != internalmysql.ArchiverTailDrained {
		t.Fatalf("demoted status = %s, want a drained tail and no error\n%s", encNativeStatus(st), log.String())
	}
	uploads := store.uploaded()

	// A master key rotation rewrites every header in place. The files
	// are the same content under another key: none is uploaded again.
	first := sealedNames()[0]
	raw, err := os.ReadFile(filepath.Join(srv.dataDir, first))
	if err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(raw)
	sql("SET GLOBAL read_only = OFF", "ALTER INSTANCE ROTATE BINLOG MASTER KEY")
	rotated, err := os.ReadFile(filepath.Join(srv.dataDir, first))
	if err != nil {
		t.Fatal(err)
	}
	after := sha256.Sum256(rotated)
	h := readEncNativeHeader(t, filepath.Join(srv.dataDir, first))
	keys = srv.keyring(t)
	t.Logf("after rotation: key %s, same bytes %v, same size %v, keyring %v", h.keyID, before == after, len(raw) == len(rotated), encNativeKeyNames(keys))
	if h.keyID != "MySQLReplicationKey_"+uuid+"_2" || before == after || len(raw) != len(rotated) || keys[wantKey] != nil || len(keys[h.keyID]) != 32 {
		t.Fatalf("after the rotation %s names %s (keyring %v); want its header rewritten in place for key 2 and key 1 removed", first, h.keyID, encNativeKeyNames(keys))
	}
	assertArchived("after the master key rotation")
	// The rotation and the demotion's flush each sealed a file; those are
	// new. Every file uploaded before it is uploaded once.
	if got := store.uploaded(); !strings.HasPrefix(got, uploads) || got != strings.Join(sealedNames(), ",") {
		t.Fatalf("uploads after the rotation %q, before %q: a file whose header was rewritten was uploaded again", got, uploads)
	}
	sql("INSERT INTO wp.t (v) VALUES ('six')", "FLUSH BINARY LOGS")
	assertArchived("a file written under the new key")

	if dir := os.Getenv("BLOODRAVEN_WRITE_ENCRYPTED_BINLOG_FIXTURE"); dir != "" {
		writeEncNativeFixture(t, srv, dir)
	}

	// An archiver without the keyring archives nothing and says why.
	bare := newTestArchiver(&PITRConfig{BinlogDir: srv.dataDir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour},
		"pdx", srv.live, newTailStore(), slog.New(slog.NewTextHandler(&psLog{}, nil)))
	for range 3 {
		bare.roleTick(ctx)
		bare.scanAndArchive(ctx)
	}
	st = bare.Snapshot(ctx)
	t.Logf("without a keyring: %s", encNativeStatus(st))
	if st.FilesArchived != 0 || !strings.Contains(st.LastError, "BLOODRAVEN_KEYRING_FILE is not set") {
		t.Fatalf("without a keyring: status = %s, want no upload and the reason", encNativeStatus(st))
	}
}

// writeEncNativeFixture copies the server's smallest sealed binlog that
// holds a transaction, and its keyring file, to dir: the committed fixture
// of TestEncryptedBinlogFixture. The keyring is this throwaway server's and
// protects nothing else.
func writeEncNativeFixture(t *testing.T, srv *encNativeServer, dir string) {
	t.Helper()
	index, err := readBinlogIndex(filepath.Join(srv.dataDir, "mysql-bin.index"))
	if err != nil {
		t.Fatal(err)
	}
	keys := srv.keyring(t)
	var smallest []byte
	for _, entry := range index[:len(index)-1] {
		data, err := os.ReadFile(filepath.Join(srv.dataDir, filepath.Base(entry)))
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := InspectBinlog(bytes.NewReader(decryptEncNativeBinlog(t, data, keys)), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(evidence.GTIDs) > 0 && (smallest == nil || len(data) < len(smallest)) {
			smallest = data
		}
	}
	if smallest == nil {
		t.Fatal("no sealed file holds a transaction")
	}
	keyring, err := os.ReadFile(filepath.Join(srv.keyringDir, "keyring"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"encrypted-binlog.bin": smallest, "encrypted-binlog.keyring": keyring} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil { //nolint:gosec // a test fixture
			t.Fatal(err)
		}
	}
}

// TestPITRNativeEncryptionAtRestWithoutBinlogEncryptionArchives is the
// control of TestPITRNativeEncryptedBinlogArchived (same opt-in):
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
