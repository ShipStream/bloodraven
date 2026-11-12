package sidecar

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/go-mysql-org/go-mysql/replication"
)

// A binlog file MySQL wrote with binlog_encryption=ON (MySQL 8.0.14 and
// later, unchanged in 9.7) is a 512-byte header followed by the plaintext
// binlog file, magic included, as an AES-256-CTR stream:
//
//	magic     FD 62 69 6E
//	version   01
//	field 1   01, one length byte, the id of the keyring key
//	          (MySQLReplicationKey_<server_uuid>_<seqno>)
//	field 2   02, the 32-byte file password, encrypted with the keyring key
//	field 3   03, the 16-byte IV the file password was encrypted with
//	          zero bytes up to offset 512
//
// The file password is AES-256-CBC-decrypt(keyring key, IV) of field 2
// without padding. The stream key is the first 32 bytes of SHA-512(file
// password) and the nonce the next 8; the counter is a big-endian block
// number in the low 8 bytes of the counter block, 0 for the first byte
// after the header.
//
// Measured on mysql:9.7 (TestPITRNativeEncryptedBinlogArchived): every
// sealed file decrypts to a binlog that parses to its end with its
// checksums verified. ALTER INSTANCE ROTATE BINLOG MASTER KEY rewrites the
// header of every file in place (another key id and encrypted password, the
// same size), so the bytes of an encrypted file are not stable; its
// plaintext is. Everything the archiver derives from a local binlog (the
// content identity, the size, the parse, the upload) is therefore derived
// from the plaintext, which is also what a file written without
// binlog_encryption holds.
const (
	encryptedBinlogHeaderSize = 512
	encryptedBinlogVersion    = 1
	encryptedBinlogKeySize    = 32
	encryptedBinlogIVSize     = aes.BlockSize
)

var encryptedBinlogMagic = []byte{0xfd, 'b', 'i', 'n'}

// errEncryptedBinlogNoKeyring: the file is encrypted by MySQL and this
// process was given no keyring to read its key from.
var errEncryptedBinlogNoKeyring = errors.New("the binlog file is encrypted by MySQL (binlog_encryption=ON) and the sidecar has no keyring file to decrypt it with (BLOODRAVEN_KEYRING_FILE is not set)")

// errBinlogKeyMissing: the keyring does not hold the key the file's header
// names. On an unsealed site this passes when mysqld has written the key;
// otherwise the file belongs to another keyring.
var errBinlogKeyMissing = errors.New("the keyring does not hold the binary log encryption key the file names")

// localBinlogs opens the binlog files of this server's data directory.
// keyringFile is component_keyring_file's data file (the one mysqld uses,
// mounted read-only into the sidecar), or "" when the site has none.
type localBinlogs struct {
	keyringFile string
}

// localBinlog is an open local binlog file, read as plaintext whether or
// not MySQL encrypted it. Size, ReadAt and NewReader address the plaintext;
// Stat is the file on disk.
type localBinlog struct {
	file      *os.File
	info      os.FileInfo
	plain     io.ReaderAt
	size      int64
	encrypted bool
}

// open opens path. An encrypted file is opened with the key its header
// names; every way that can fail (no keyring, no such key, a header this
// code does not know, a plaintext that is not a binlog) is an error, so an
// encrypted file is read correctly or not at all.
func (l localBinlogs) open(path string) (*localBinlog, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	b, err := l.wrap(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return b, nil
}

func (l localBinlogs) wrap(file *os.File) (*localBinlog, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	b := &localBinlog{file: file, info: info, plain: file, size: info.Size()}
	var magic [4]byte
	if n, err := file.ReadAt(magic[:], 0); n < len(magic) {
		if err == nil || err == io.EOF {
			// Shorter than a magic: the readers report it as they did.
			return b, nil
		}
		return nil, err
	}
	if !bytes.Equal(magic[:], encryptedBinlogMagic) {
		return b, nil
	}
	if info.Size() < encryptedBinlogHeaderSize {
		return nil, fmt.Errorf("encrypted binlog is %d bytes, shorter than its %d-byte header", info.Size(), encryptedBinlogHeaderSize)
	}
	header := make([]byte, encryptedBinlogHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("read the encryption header: %w", err)
	}
	h, err := parseEncryptedBinlogHeader(header)
	if err != nil {
		return nil, err
	}
	if l.keyringFile == "" {
		return nil, errEncryptedBinlogNoKeyring
	}
	master, err := readKeyringKey(l.keyringFile, h.keyID)
	if err != nil {
		return nil, err
	}
	stream, err := newEncryptedBinlogStream(file, master, h)
	if err != nil {
		return nil, err
	}
	b.plain, b.size, b.encrypted = stream, info.Size()-encryptedBinlogHeaderSize, true
	// A wrong key, or a header read while ROTATE BINLOG MASTER KEY rewrote
	// it, yields noise: nothing is derived from a plaintext that does not
	// start as a binlog.
	if b.size >= int64(len(magic)) {
		if _, err := b.plain.ReadAt(magic[:], 0); err != nil {
			return nil, fmt.Errorf("read the encrypted binlog: %w", err)
		}
		if !bytes.Equal(magic[:], replication.BinLogFileHeader) {
			return nil, fmt.Errorf("the binlog file does not decrypt to a binlog with keyring key %s (magic % x)", h.keyID, magic)
		}
	}
	return b, nil
}

func (b *localBinlog) Close() error { return b.file.Close() }

// Size is the size of the plaintext binlog.
func (b *localBinlog) Size() int64 { return b.size }

// Stat stats the open file on disk.
func (b *localBinlog) Stat() (os.FileInfo, error) { return b.file.Stat() }

// ReadAt reads plaintext at a plaintext offset.
func (b *localBinlog) ReadAt(p []byte, off int64) (int, error) {
	if off >= b.size {
		return 0, io.EOF
	}
	if max := b.size - off; int64(len(p)) > max {
		n, err := b.plain.ReadAt(p[:max], off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return b.plain.ReadAt(p, off)
}

// NewReader returns a reader of the whole plaintext from its start. Each
// call starts over.
func (b *localBinlog) NewReader() io.Reader {
	section := io.NewSectionReader(b, 0, b.size)
	if !b.encrypted {
		return section
	}
	// The parser reads an event in several small pieces, and each ReadAt
	// of the stream sets up its counter.
	return bufio.NewReaderSize(section, 64<<10)
}

type encryptedBinlogHeader struct {
	keyID    string
	password []byte
	iv       []byte
}

// parseEncryptedBinlogHeader reads the 512-byte header. Anything it does
// not know (another version, another field, a field that is missing, twice
// there or cut off, bytes after the fields) is an error.
func parseEncryptedBinlogHeader(header []byte) (encryptedBinlogHeader, error) {
	var h encryptedBinlogHeader
	if len(header) != encryptedBinlogHeaderSize || !bytes.Equal(header[:4], encryptedBinlogMagic) {
		return h, errors.New("not an encrypted binlog header")
	}
	if header[4] != encryptedBinlogVersion {
		return h, fmt.Errorf("encrypted binlog header version %d is not supported (only version %d is)", header[4], encryptedBinlogVersion)
	}
	take := func(p, n int) ([]byte, int, error) {
		if p+n > len(header) {
			return nil, 0, errors.New("encrypted binlog header: a field runs past the header")
		}
		return header[p : p+n], p + n, nil
	}
	p := 5
	for p < len(header) && header[p] != 0 {
		kind := header[p]
		p++
		var err error
		switch kind {
		case 1:
			if h.keyID != "" {
				return h, errors.New("encrypted binlog header: two key ids")
			}
			var n, id []byte
			if n, p, err = take(p, 1); err != nil {
				return h, err
			}
			if n[0] == 0 {
				return h, errors.New("encrypted binlog header: empty key id")
			}
			if id, p, err = take(p, int(n[0])); err != nil {
				return h, err
			}
			h.keyID = string(id)
		case 2:
			if h.password != nil {
				return h, errors.New("encrypted binlog header: two file passwords")
			}
			if h.password, p, err = take(p, encryptedBinlogKeySize); err != nil {
				return h, err
			}
		case 3:
			if h.iv != nil {
				return h, errors.New("encrypted binlog header: two IVs")
			}
			if h.iv, p, err = take(p, encryptedBinlogIVSize); err != nil {
				return h, err
			}
		default:
			return h, fmt.Errorf("encrypted binlog header: unknown field type %d at offset %d", kind, p-1)
		}
	}
	for _, c := range header[p:] {
		if c != 0 {
			return h, errors.New("encrypted binlog header: data after its fields")
		}
	}
	if h.keyID == "" || h.password == nil || h.iv == nil {
		return h, errors.New("encrypted binlog header: the key id, the file password or the IV is missing")
	}
	return h, nil
}

// readKeyringKey returns the key id of component_keyring_file's data file:
// JSON whose elements carry a data_id and the hex-encoded key. The file is
// read on every call: mysqld adds keys to an unsealed keyring, and a sealed
// one is a Secret projection that the kubelet replaces.
func readKeyringKey(path, id string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the keyring file: %w", err)
	}
	var file struct {
		Elements []struct {
			DataID   string `json:"data_id"`
			DataType string `json:"data_type"`
			Data     string `json:"data"`
		} `json:"elements"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		// Never the content: it is key material.
		return nil, errors.New("the keyring file is not the JSON component_keyring_file writes")
	}
	for _, e := range file.Elements {
		if e.DataID != id {
			continue
		}
		key, err := hex.DecodeString(e.Data)
		if err != nil || len(key) != encryptedBinlogKeySize {
			return nil, fmt.Errorf("keyring key %s is not a %d-byte hex-encoded key", id, encryptedBinlogKeySize)
		}
		return key, nil
	}
	return nil, fmt.Errorf("%w: %s", errBinlogKeyMissing, id)
}

// encryptedBinlogStream reads the plaintext of an encrypted binlog file at
// any offset: CTR needs no earlier byte to decrypt a later one.
type encryptedBinlogStream struct {
	file  io.ReaderAt
	block cipher.Block
	nonce [8]byte
}

func newEncryptedBinlogStream(file io.ReaderAt, master []byte, h encryptedBinlogHeader) (*encryptedBinlogStream, error) {
	wrap, err := aes.NewCipher(master)
	if err != nil {
		return nil, err
	}
	password := make([]byte, encryptedBinlogKeySize)
	cipher.NewCBCDecrypter(wrap, h.iv).CryptBlocks(password, h.password)
	derived := sha512.Sum512(password)
	block, err := aes.NewCipher(derived[:encryptedBinlogKeySize])
	if err != nil {
		return nil, err
	}
	s := &encryptedBinlogStream{file: file, block: block}
	copy(s.nonce[:], derived[encryptedBinlogKeySize:encryptedBinlogKeySize+8])
	return s, nil
}

func (s *encryptedBinlogStream) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	n, err := s.file.ReadAt(p, off+encryptedBinlogHeaderSize)
	if n > 0 {
		var counter [aes.BlockSize]byte
		copy(counter[:8], s.nonce[:])
		binary.BigEndian.PutUint64(counter[8:], uint64(off/aes.BlockSize))
		stream := cipher.NewCTR(s.block, counter[:])
		if skip := int(off % aes.BlockSize); skip > 0 {
			var discard [aes.BlockSize]byte
			stream.XORKeyStream(discard[:skip], discard[:skip])
		}
		stream.XORKeyStream(p[:n], p[:n])
	}
	return n, err
}
