package sidecar

import (
	"bytes"
	"crypto/aes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar/binlogenctest"
)

// The committed capture of a real mysql:9.7 server: the reader decrypts it
// with the server's keyring to a binlog that parses to its end with its
// checksums verified and holds a transaction. This is the test of the file
// format that runs in every go test; TestPITRNativeEncryptedBinlogArchived
// is the one against a live server.
func TestEncryptedBinlogFixture(t *testing.T) {
	const path, keyring = "testdata/encrypted-binlog.bin", "testdata/encrypted-binlog.keyring"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[:4], encryptedBinlogMagic) {
		t.Fatalf("the fixture starts with % x: not an encrypted binlog", raw[:4])
	}
	file, err := localBinlogs{keyringFile: keyring}.open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if !file.encrypted || file.Size() != int64(len(raw))-encryptedBinlogHeaderSize {
		t.Fatalf("encrypted=%v size=%d of a %d-byte file", file.encrypted, file.Size(), len(raw))
	}
	plain, err := io.ReadAll(file.NewReader())
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := InspectBinlog(bytes.NewReader(plain), time.Time{})
	if err != nil {
		t.Fatalf("the plaintext does not parse: %v", err)
	}
	if len(evidence.GTIDs) == 0 || evidence.Metadata.Size != file.Size() {
		t.Fatalf("the plaintext holds %s in %d bytes, want a transaction in %d", evidence.GTIDs, evidence.Metadata.Size, file.Size())
	}
	// The plaintext of the capture; it changes only with the fixture.
	const want = "c6725f3da8d0e1435ec60d6812d0dba5ae825514ad74eb4795367971ba682342"
	id, err := localBinlogs{keyringFile: keyring}.contentID(path)
	if err != nil || id != want || id != evidence.Digest {
		t.Fatalf("content identity = %s, %v; want %s", id, err, want)
	}
	header, err := localBinlogs{keyringFile: keyring}.header(path)
	if err != nil || header.previous != evidence.Metadata.PreviousGTIDs || header.size != file.Size() {
		t.Fatalf("header = %+v, %v; want Previous_gtids %q and size %d", header, err, evidence.Metadata.PreviousGTIDs, file.Size())
	}
	walk := &originWalk{}
	if _, err := walk.Walk(file, file.Size(), newOriginBudget()); err != nil {
		t.Fatalf("origin walk of the plaintext: %v", err)
	}
	// Without the keyring the file is refused, by every reader.
	if _, err := localBinlogHeader(path); !errors.Is(err, errEncryptedBinlogNoKeyring) {
		t.Fatalf("header without a keyring: %v", err)
	}
	if _, err := fileContentID(path); !errors.Is(err, errEncryptedBinlogNoKeyring) {
		t.Fatalf("content identity without a keyring: %v", err)
	}
}

func encryptedTestFile(t *testing.T, plain []byte, keys ...binlogenctest.Key) (path string, l localBinlogs) {
	t.Helper()
	dir := t.TempDir()
	key := binlogenctest.TestKey()
	path = filepath.Join(dir, "mysql-bin.000001")
	if err := os.WriteFile(path, binlogenctest.EncryptDefault(plain, key), 0o600); err != nil {
		t.Fatal(err)
	}
	if keys == nil {
		keys = []binlogenctest.Key{key}
	}
	l.keyringFile = filepath.Join(dir, "keyring")
	if err := binlogenctest.WriteKeyring(l.keyringFile, keys...); err != nil {
		t.Fatal(err)
	}
	return path, l
}

// A file encrypted in one pass by other code reads back as its plaintext,
// sequentially and at any offset and length, across cipher blocks.
func TestEncryptedBinlogReadsPlaintextAtAnyOffset(t *testing.T) {
	plain, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	// A valid binlog start followed by noise: only the bytes matter here.
	rng := rand.New(rand.NewSource(7))
	noise := make([]byte, 300_000)
	rng.Read(noise)
	plain = append(plain, noise...)
	path, l := encryptedTestFile(t, plain)

	file, err := l.open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if file.Size() != int64(len(plain)) {
		t.Fatalf("size %d, want %d", file.Size(), len(plain))
	}
	for range 2 { // NewReader starts over
		got, err := io.ReadAll(file.NewReader())
		if err != nil || !bytes.Equal(got, plain) {
			t.Fatalf("sequential read: %d bytes, %v", len(got), err)
		}
	}
	for i := 0; i < 2000; i++ {
		off := rng.Intn(len(plain))
		n := 1 + rng.Intn(5000)
		buf := make([]byte, n)
		got, err := file.ReadAt(buf, int64(off))
		end := min(off+n, len(plain))
		if !bytes.Equal(buf[:got], plain[off:end]) || got != end-off || (got < n) != (err == io.EOF) {
			t.Fatalf("ReadAt(%d, %d) = %d, %v", off, n, got, err)
		}
	}
	if n, err := file.ReadAt(make([]byte, 1), file.Size()); n != 0 || err != io.EOF {
		t.Fatalf("ReadAt at the end = %d, %v", n, err)
	}
	sum := sha256.Sum256(plain)
	if id, err := l.contentID(path); err != nil || id != hex.EncodeToString(sum[:]) {
		t.Fatalf("content identity %s, %v", id, err)
	}
}

// The plaintext, and so the content identity, does not depend on the key:
// ROTATE BINLOG MASTER KEY rewrites the header, nothing else.
func TestEncryptedBinlogIdentityIsThePlaintexts(t *testing.T) {
	plain, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	want, err := fileContentID("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	path, l := encryptedTestFile(t, plain)
	if id, err := l.contentID(path); err != nil || id != want {
		t.Fatalf("content identity %s, %v; want the plaintext file's %s", id, err, want)
	}
	// The same file under another master key, password and IV.
	other := binlogenctest.Key{ID: "MySQLReplicationKey_11111111-2222-3333-4444-555555555555_2"}
	other.Master[5] = 9
	var password [32]byte
	password[0] = 1
	if err := os.WriteFile(path, binlogenctest.Encrypt(plain, other, password, [16]byte{4}), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := l.contentID(path); !errors.Is(err, errBinlogKeyMissing) || !strings.Contains(err.Error(), other.ID) {
		t.Fatalf("before the keyring holds the new key: %v", err)
	}
	if err := binlogenctest.WriteKeyring(l.keyringFile, other); err != nil {
		t.Fatal(err)
	}
	if id, err := l.contentID(path); err != nil || id != want {
		t.Fatalf("content identity under the new key %s, %v; want %s", id, err, want)
	}
	header, err := l.header(path)
	plainHeader, err2 := localBinlogHeader("testdata/incarnation-a.bin")
	if err != nil || err2 != nil || header != plainHeader {
		t.Fatalf("header %+v, %v; the plaintext file's %+v, %v", header, err, plainHeader, err2)
	}
}

// Whatever the reader does not know is refused: an encrypted file is read
// correctly or not at all.
func TestEncryptedBinlogFailsClosed(t *testing.T) {
	plain, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	key := binlogenctest.TestKey()
	good := binlogenctest.EncryptDefault(plain, key)
	wrong := key
	wrong.Master[0] ^= 1
	short := `{"version":"1.0","elements":[{"data_id":"` + key.ID + `","data_type":"AES","data":"00112233"}]}`
	const secret = "5ec2e7"
	for _, tc := range []struct {
		name    string
		file    func([]byte) []byte
		keyring string // "" writes the right keyring, "-" none
		want    string
	}{
		{name: "no keyring", keyring: "-", want: "BLOODRAVEN_KEYRING_FILE is not set"},
		{name: "keyring file missing", keyring: "missing", want: "read the keyring file"},
		{name: "key not in the keyring", keyring: string(binlogenctest.Keyring(binlogenctest.Key{ID: "other"})), want: "does not hold the binary log encryption key"},
		{name: "wrong key under the id", keyring: string(binlogenctest.Keyring(wrong)), want: "does not decrypt to a binlog"},
		{name: "key of the wrong length", keyring: short, want: "is not a 32-byte hex-encoded key"},
		{name: "keyring not JSON", keyring: secret + " not json", want: "is not the JSON"},
		{name: "version 2", file: func(b []byte) []byte { b[4] = 2; return b }, want: "version 2 is not supported"},
		{name: "unknown field", file: func(b []byte) []byte { b[5] = 9; return b }, want: "unknown field type 9"},
		{name: "data after the fields", file: func(b []byte) []byte { b[400] = 1; return b }, want: "data after its fields"},
		{name: "no IV", file: func(b []byte) []byte {
			p := 5 + 2 + len(key.ID) + 33
			for i := p; i < 512; i++ {
				b[i] = 0
			}
			return b
		}, want: "is missing"},
		{name: "two key ids", file: func(b []byte) []byte {
			p := 5 + 2 + len(key.ID) + 33 + 17
			b[p], b[p+1], b[p+2] = 1, 1, 'x'
			return b
		}, want: "two key ids"},
		{name: "cut inside the header", file: func(b []byte) []byte { return b[:300] }, want: "shorter than its 512-byte header"},
		{name: "header rewritten for another password", file: func(b []byte) []byte { b[5+2+len(key.ID)+1] ^= 0xff; return b }, want: "does not decrypt to a binlog"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "mysql-bin.000001")
			data := bytes.Clone(good)
			if tc.file != nil {
				data = tc.file(data)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			l := localBinlogs{keyringFile: filepath.Join(dir, "keyring")}
			switch tc.keyring {
			case "-":
				l.keyringFile = ""
			case "missing":
			case "":
				if err := binlogenctest.WriteKeyring(l.keyringFile, key); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.WriteFile(l.keyringFile, []byte(tc.keyring), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			for what, read := range map[string]func() error{
				"open": func() error {
					f, err := l.open(path)
					if err == nil {
						f.Close()
					}
					return err
				},
				"header":    func() error { _, err := l.header(path); return err },
				"contentID": func() error { _, err := l.contentID(path); return err },
			} {
				err := read()
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Errorf("%s: %v, want an error holding %q", what, err, tc.want)
				}
				if err != nil && strings.Contains(err.Error(), secret) {
					t.Errorf("%s: the error quotes the keyring file: %v", what, err)
				}
			}
		})
	}
}

// A file MySQL did not encrypt is read as before, keyring or not.
func TestLocalBinlogPlaintextUnchanged(t *testing.T) {
	const path = "testdata/incarnation-a.bin"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []localBinlogs{{}, {keyringFile: "testdata/encrypted-binlog.keyring"}, {keyringFile: "/nonexistent"}} {
		file, err := l.open(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(file.NewReader())
		if err != nil || !bytes.Equal(got, raw) || file.encrypted || file.Size() != int64(len(raw)) {
			t.Fatalf("keyring %q: %d bytes, encrypted=%v, %v", l.keyringFile, len(got), file.encrypted, err)
		}
		_ = file.Close()
	}
	// An empty file and one shorter than a magic stay the readers' errors.
	short := filepath.Join(t.TempDir(), "short")
	if err := os.WriteFile(short, []byte{0xfd, 'b'}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := localBinlogHeader(short); err == nil {
		t.Fatal("a two-byte file has a header")
	}
}

// zeroFile is an endless file of zero bytes: its "plaintext" at an offset is
// the keystream there.
type zeroFile struct{}

func (zeroFile) ReadAt(p []byte, _ int64) (int, error) {
	clear(p)
	return len(p), nil
}

// The counter is a 64-bit big-endian block number in the low half of the
// counter block, whatever the offset: past 2^16 and 2^32 blocks (a 64 GiB
// file) the keystream is still AES(key, nonce || block number).
func TestEncryptedBinlogCounterAtLargeOffsets(t *testing.T) {
	key := binlogenctest.TestKey()
	header, err := parseEncryptedBinlogHeader(binlogenctest.EncryptDefault(nil, key)[:encryptedBinlogHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	stream, err := newEncryptedBinlogStream(zeroFile{}, key.Master[:], header)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range []uint64{0, 1, 1<<16 - 1, 1 << 16, 1<<32 - 1, 1 << 32, 1<<32 + 7, 1 << 40} {
		var counter, want [aes.BlockSize]byte
		copy(counter[:8], stream.nonce[:])
		binary.BigEndian.PutUint64(counter[8:], block)
		stream.block.Encrypt(want[:], counter[:])
		for _, skip := range []int{0, 5, 15} {
			got := make([]byte, aes.BlockSize-skip)
			if _, err := stream.ReadAt(got, int64(block)*aes.BlockSize+int64(skip)); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want[skip:]) {
				t.Fatalf("keystream of block %d from byte %d = %x, want %x", block, skip, got, want[skip:])
			}
		}
	}
}

// mysqld rewrites the keyring file in place when it stores a key. A read
// that catches the file torn is repeated once; a keyring that stays
// unreadable is an error.
func TestKeyringReadSurvivesATornFile(t *testing.T) {
	key := binlogenctest.TestKey()
	path := filepath.Join(t.TempDir(), "keyring")
	whole := binlogenctest.Keyring(key)
	if err := os.WriteFile(path, whole[:len(whole)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readKeyringKey(path, key.ID); err == nil {
		t.Fatal("half a keyring file holds a key")
	}
	done := make(chan error, 1)
	go func() {
		time.Sleep(keyringRetryDelay / 5)
		done <- os.WriteFile(path, whole, 0o600)
	}()
	got, err := readKeyringKey(path, key.ID)
	if werr := <-done; werr != nil {
		t.Fatal(werr)
	}
	if err != nil || !bytes.Equal(got, key.Master[:]) {
		t.Fatalf("the key after the file was completed: %v", err)
	}
	// Two entries under the id (another user's): the one that is a key.
	dup := `{"version":"1.0","elements":[{"user":"app","data_id":"` + key.ID + `","data_type":"SECRET","data":"00"},` +
		strings.TrimPrefix(string(whole), `{"version":"1.0","elements":[`)
	if err := os.WriteFile(path, []byte(dup), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readKeyringKey(path, key.ID); err != nil || !bytes.Equal(got, key.Master[:]) {
		t.Fatalf("a keyring with two entries under the id: %v", err)
	}
}
