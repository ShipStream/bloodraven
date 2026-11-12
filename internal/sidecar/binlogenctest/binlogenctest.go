// Package binlogenctest writes binlog files and keyring files in the
// format MySQL uses with binlog_encryption=ON, for tests of the sidecar's
// reader (internal/sidecar/local_binlog.go). It is written apart from that
// reader: it encrypts in one pass over the whole file. That the format is
// MySQL's is established by the committed fixture of a real server and by
// TestPITRNativeEncryptedBinlogArchived, not by this package.
package binlogenctest

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"os"
)

// HeaderSize is the size of the encryption header.
const HeaderSize = 512

// Key is a binary log master key under its keyring id.
type Key struct {
	ID     string
	Master [32]byte
}

// TestKey is a fixed key for tests that only need one.
func TestKey() Key {
	k := Key{ID: "MySQLReplicationKey_11111111-2222-3333-4444-555555555555_1"}
	for i := range k.Master {
		k.Master[i] = byte(i*7 + 3)
	}
	return k
}

// Encrypt returns plain, a whole binlog file, as MySQL writes it under key
// with the given 32-byte file password and 16-byte IV.
func Encrypt(plain []byte, key Key, password [32]byte, iv [16]byte) []byte {
	wrap, err := aes.NewCipher(key.Master[:])
	if err != nil {
		panic(err)
	}
	wrapped := make([]byte, len(password))
	cipher.NewCBCEncrypter(wrap, iv[:]).CryptBlocks(wrapped, password[:])

	out := make([]byte, HeaderSize, HeaderSize+len(plain))
	p := copy(out, []byte{0xfd, 'b', 'i', 'n', 1})
	out[p] = 1
	out[p+1] = byte(len(key.ID))
	p += 2 + copy(out[p+2:], key.ID)
	out[p] = 2
	p += 1 + copy(out[p+1:], wrapped)
	out[p] = 3
	copy(out[p+1:], iv[:])

	derived := sha512.Sum512(password[:])
	stream, err := aes.NewCipher(derived[:32])
	if err != nil {
		panic(err)
	}
	counter := make([]byte, aes.BlockSize)
	copy(counter, derived[32:40])
	body := make([]byte, len(plain))
	cipher.NewCTR(stream, counter).XORKeyStream(body, plain)
	return append(out, body...)
}

// EncryptDefault is Encrypt with a fixed file password and IV.
func EncryptDefault(plain []byte, key Key) []byte {
	var password [32]byte
	var iv [16]byte
	for i := range password {
		password[i] = byte(200 - i)
	}
	for i := range iv {
		iv[i] = byte(i + 1)
	}
	return Encrypt(plain, key, password, iv)
}

// Keyring returns the content of a component_keyring_file data file that
// holds keys.
func Keyring(keys ...Key) []byte {
	type element struct {
		User     string `json:"user"`
		DataID   string `json:"data_id"`
		DataType string `json:"data_type"`
		Data     string `json:"data"`
		Ext      []any  `json:"extension"`
	}
	file := struct {
		Version  string    `json:"version"`
		Elements []element `json:"elements"`
	}{Version: "1.0"}
	for _, k := range keys {
		file.Elements = append(file.Elements, element{DataID: k.ID, DataType: "AES", Data: hex.EncodeToString(k.Master[:]), Ext: []any{}})
	}
	out, err := json.Marshal(file)
	if err != nil {
		panic(err)
	}
	return out
}

// WriteKeyring writes Keyring(keys...) to path.
func WriteKeyring(path string, keys ...Key) error {
	return os.WriteFile(path, Keyring(keys...), 0o600)
}

// EnvEncrypted is the variable that makes the archiver's simulations write
// their local binlog files encrypted (Local, KeyringFile): the suites that
// use them then run the archiver over encrypted files, and must behave as
// they do over plaintext ones.
const EnvEncrypted = "BLOODRAVEN_TEST_ENCRYPTED_BINLOGS"

// Encrypted reports whether EnvEncrypted is set to 1.
func Encrypted() bool { return os.Getenv(EnvEncrypted) == "1" }

// Local returns what a simulated server writes for the binlog file plain in
// dir: plain itself, or with EnvEncrypted its encryption under TestKey,
// whose keyring file it also writes (KeyringFile).
func Local(dir string, plain []byte) []byte {
	if !Encrypted() {
		return plain
	}
	if err := WriteKeyring(KeyringFile(dir), TestKey()); err != nil {
		panic(err)
	}
	return EncryptDefault(plain, TestKey())
}

// KeyringFile is the keyring file of the simulated server in dir: "" unless
// EnvEncrypted is set.
func KeyringFile(dir string) string {
	if !Encrypted() {
		return ""
	}
	return dir + "/.keyring"
}
