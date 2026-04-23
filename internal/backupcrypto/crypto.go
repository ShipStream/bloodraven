// Package backupcrypto implements Bloodraven's client-side envelope
// encryption for backup artifacts and archived binlog files.
//
// # Wire format (BRV1)
//
// Each encrypted object is a self-describing byte stream:
//
//	Header (32 bytes):
//	    [0..3]   magic          "BRV1"
//	    [4]      version        0x01
//	    [5]      algorithm      0x01  (AES-256-GCM)
//	    [6..7]   chunk_log2     uint16 big-endian. Plaintext chunk size
//	                            is 1 << chunk_log2 bytes. Fixed at 16
//	                            (65 536-byte chunks) by current
//	                            encoders but honoured on decode so the
//	                            tuning knob can move forward-compatibly.
//	    [8..23]  salt           16 bytes of crypto/rand output, fed to
//	                            HKDF-SHA256 as the salt parameter.
//	    [24..31] nonce_prefix   first 8 bytes of each per-chunk
//	                            12-byte GCM nonce; chosen fresh per file
//	                            from crypto/rand.
//
//	Body: zero or more chunks. Each chunk is the AES-256-GCM seal of
//	one plaintext block; the last chunk carries the end-of-stream
//	flag in its nonce so truncation is detectable. Nonce layout:
//
//	    nonce[0..7]   = nonce_prefix  (constant across chunks)
//	    nonce[8..10]  = counter       (24-bit big-endian, starts at 0)
//	    nonce[11]     = final_flag    (0x00 on every chunk except the
//	                                   last; 0x01 on the last chunk)
//
//	Associated data is empty. The counter caps file size at
//	2^24 × chunk_size = 1 TiB per file at the default chunk size;
//	encoder refuses to continue if it overflows. The final chunk is
//	always emitted even if its plaintext is zero bytes, so a reader
//	that hits EOF before a final-flag chunk knows the stream was
//	truncated and fails decryption.
//
// # Key derivation
//
// The AES-256 key for a single file is derived with
// HKDF-SHA256(secret=passphrase, salt=file_salt, info="bloodraven-backup-encryption-v1",
// length=32). A random 16-byte salt per file means two encryptions of
// the same passphrase and plaintext do not share a data-encryption key,
// so an adversary who sees two ciphertexts can't exploit key reuse.
//
// # Threat model
//
// The operator mounts the passphrase Secret into every pod that legitimately
// needs it (backup Job, restore Job, verification Job, sidecar archiver,
// pitr-download init container). The passphrase itself never leaves
// Kubernetes Secret storage in the clear — it is read from a mounted file,
// not an environment variable, so that process inspection (/proc/PID/environ,
// crash dumps, kubectl describe pod) cannot expose it.
//
// The ciphertext alone does not leak plaintext size beyond one chunk of
// padding granularity. It does NOT protect object names (S3 keys, PVC
// filenames are plaintext) or the separately-stored dump manifest
// metadata (location, size, GTID coordinates) that the operator exposes
// on the MysqlBackup CR — those are considered observable.
//
// Rotation: changing the passphrase without re-encrypting renders
// existing ciphertexts unreadable. A future feature may add a bulk
// re-encryption path; for now, roll passphrases by waiting for existing
// artifacts to age out of retention.
package backupcrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Magic identifies the wire format. It is intentionally ASCII so
// hexdumps of the first bytes of any archived object make the framing
// obvious to humans, which shortens debugging sessions for on-call.
var Magic = [4]byte{'B', 'R', 'V', '1'}

// Version is the current wire-format version. Readers must reject any
// other value rather than guess — we'd rather fail loud on a version
// mismatch than silently decrypt garbage.
const Version byte = 0x01

// AlgorithmAESGCM is the algorithm byte for AES-256-GCM in the header.
// Reserved: 0x02 onwards for future algorithms (e.g. XChaCha20-Poly1305).
const AlgorithmAESGCM byte = 0x01

// AlgorithmAESGCMName is the string identifier recorded in
// MysqlBackup.status.encryptionAlgorithm and in CRD validation enums.
const AlgorithmAESGCMName = "AES-256-GCM"

// DefaultChunkLog2 is the default chunk-size exponent: 1<<16 = 65 536
// bytes per plaintext chunk. Encoders write this into the header so a
// future bump doesn't break older readers.
const DefaultChunkLog2 = 16

// MaxChunkLog2 bounds the chunk size on decode so a hostile header
// can't request gigabyte allocations during a single chunk read.
// 1<<24 = 16 MiB is well beyond the single-chunk MySQL binlog size and
// comfortably below process-level memory exhaustion.
const MaxChunkLog2 = 24

// HeaderSize is the total byte count the wire format reserves for the
// header. Exposed so readers can seek / skip past it.
const HeaderSize = 32

// GCMTagSize is the authentication tag length produced by AES-GCM in
// Go's stdlib. Repeated in this package for a self-documenting constant.
const GCMTagSize = 16

// KDFInfo is the fixed "info" string fed to HKDF-SHA256. Changing this
// constant invalidates every existing ciphertext — treat it as part of
// the wire format.
const KDFInfo = "bloodraven-backup-encryption-v1"

// ErrMagicMismatch is returned when a decrypt is attempted against a
// stream whose first four bytes are not the BRV1 magic. Callers use
// this to distinguish "plaintext input, no decryption needed" from
// "encrypted input with the wrong key".
var ErrMagicMismatch = errors.New("backupcrypto: input is not an encrypted BRV1 stream")

// ErrTruncated is returned when a decrypt hits EOF before seeing a
// chunk with the final flag set. Surfaced separately so operator logs
// distinguish "somebody deleted the tail of this file" from a generic
// auth failure.
var ErrTruncated = errors.New("backupcrypto: ciphertext truncated before final chunk")

// ErrUnsupportedVersion is returned on a header whose version byte
// doesn't match what this build knows how to read.
var ErrUnsupportedVersion = errors.New("backupcrypto: unsupported wire version")

// ErrUnsupportedAlgorithm is returned for a header that declares an
// algorithm other than AES-256-GCM.
var ErrUnsupportedAlgorithm = errors.New("backupcrypto: unsupported algorithm")

// ErrChunkTooLarge is returned when a header requests a chunk size
// above MaxChunkLog2.
var ErrChunkTooLarge = errors.New("backupcrypto: chunk size exceeds MaxChunkLog2")

// ErrCounterExhausted is returned when an encoder tries to write more
// than 2^24 chunks to a single file. The caller can split the dump
// into multiple artifacts if they hit this.
var ErrCounterExhausted = errors.New("backupcrypto: plaintext exceeds single-file chunk budget")

// ErrEmptyPassphrase is returned from Encrypt/Decrypt when the
// passphrase slice is zero-length. Guard against the accidental
// "empty Secret value passes silently" footgun.
var ErrEmptyPassphrase = errors.New("backupcrypto: passphrase is empty")

// Encrypt copies src into dst, wrapping it in the BRV1 wire format
// using passphrase as key material. It returns the number of plaintext
// bytes consumed.
//
// Passphrases shorter than 16 bytes are still accepted (HKDF doesn't
// care) but are flagged as "low entropy" in the caller's validation.
// The tooling preceding Encrypt is the source of truth for policy; this
// package just refuses the empty case.
func Encrypt(dst io.Writer, src io.Reader, passphrase []byte) (int64, error) {
	if len(passphrase) == 0 {
		return 0, ErrEmptyPassphrase
	}

	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return 0, fmt.Errorf("backupcrypto: generate salt: %w", err)
	}
	noncePrefix := make([]byte, 8)
	if _, err := rand.Read(noncePrefix); err != nil {
		return 0, fmt.Errorf("backupcrypto: generate nonce prefix: %w", err)
	}

	key, err := hkdf.Key(sha256.New, passphrase, salt, KDFInfo, 32)
	if err != nil {
		return 0, fmt.Errorf("backupcrypto: hkdf expand: %w", err)
	}

	aead, err := newAEAD(key)
	if err != nil {
		return 0, err
	}

	header := buildHeader(DefaultChunkLog2, salt, noncePrefix)
	if _, err := dst.Write(header); err != nil {
		return 0, fmt.Errorf("backupcrypto: write header: %w", err)
	}

	chunkSize := 1 << DefaultChunkLog2
	plaintext := make([]byte, chunkSize)
	ciphertext := make([]byte, 0, chunkSize+GCMTagSize)
	nonce := make([]byte, 12)
	copy(nonce[:8], noncePrefix)

	var counter uint32
	var total int64
	// Read-one-chunk-ahead loop. We need to know whether the NEXT
	// chunk is also present so we can mark the current one as final
	// when we're about to hit EOF; GCM can't retroactively change the
	// tag once a chunk is emitted.
	var pendingLen int
	pendingLen, err = io.ReadFull(src, plaintext)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, fmt.Errorf("backupcrypto: read plaintext: %w", err)
	}

	for {
		// Peek one byte past the current chunk to decide if this is final.
		peek := make([]byte, 1)
		var hadPeek bool
		n, readErr := io.ReadFull(src, peek)
		if readErr == nil && n == 1 {
			hadPeek = true
		} else if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return 0, fmt.Errorf("backupcrypto: read plaintext: %w", readErr)
		}

		final := !hadPeek

		if counter == 1<<24 {
			return 0, ErrCounterExhausted
		}
		writeCounter(nonce, counter, final)
		ciphertext = aead.Seal(ciphertext[:0], nonce, plaintext[:pendingLen], nil)
		if _, err := dst.Write(ciphertext); err != nil {
			return 0, fmt.Errorf("backupcrypto: write chunk: %w", err)
		}
		total += int64(pendingLen)
		counter++

		if final {
			break
		}

		// Slide the peeked byte into the start of the next chunk and
		// read the rest.
		plaintext[0] = peek[0]
		pendingLen = 1
		if chunkSize > 1 {
			n, readErr := io.ReadFull(src, plaintext[1:])
			if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				return 0, fmt.Errorf("backupcrypto: read plaintext: %w", readErr)
			}
			pendingLen += n
		}
	}

	return total, nil
}

// Decrypt reads a BRV1 stream from src, writing the decrypted plaintext
// to dst. Returns the plaintext byte count on success; returns
// ErrMagicMismatch if src does not start with the BRV1 header so the
// caller can choose to pass bytes through unchanged when the object
// was never encrypted in the first place.
func Decrypt(dst io.Writer, src io.Reader, passphrase []byte) (int64, error) {
	if len(passphrase) == 0 {
		return 0, ErrEmptyPassphrase
	}

	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(src, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, ErrMagicMismatch
		}
		return 0, fmt.Errorf("backupcrypto: read header: %w", err)
	}

	if header[0] != Magic[0] || header[1] != Magic[1] || header[2] != Magic[2] || header[3] != Magic[3] {
		return 0, ErrMagicMismatch
	}
	if header[4] != Version {
		return 0, ErrUnsupportedVersion
	}
	if header[5] != AlgorithmAESGCM {
		return 0, ErrUnsupportedAlgorithm
	}

	chunkLog2 := binary.BigEndian.Uint16(header[6:8])
	if chunkLog2 == 0 || chunkLog2 > MaxChunkLog2 {
		return 0, ErrChunkTooLarge
	}
	salt := header[8:24]
	noncePrefix := header[24:32]

	key, err := hkdf.Key(sha256.New, passphrase, salt, KDFInfo, 32)
	if err != nil {
		return 0, fmt.Errorf("backupcrypto: hkdf expand: %w", err)
	}
	aead, err := newAEAD(key)
	if err != nil {
		return 0, err
	}

	chunkSize := 1 << chunkLog2
	ciphertext := make([]byte, chunkSize+GCMTagSize)
	nonce := make([]byte, 12)
	copy(nonce[:8], noncePrefix)

	var counter uint32
	var total int64
	for {
		if counter == 1<<24 {
			// Mirrors the encode-side budget. A decoder that hits this
			// without seeing a final chunk is looking at input that no
			// legitimate encoder could have produced.
			return 0, ErrCounterExhausted
		}
		n, err := io.ReadFull(src, ciphertext)
		if errors.Is(err, io.EOF) {
			// EOF at a chunk boundary without a prior final flag ==
			// truncation.
			return 0, ErrTruncated
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Short last chunk — legal, try decoding what we got.
			if n < GCMTagSize {
				return 0, ErrTruncated
			}
		} else if err != nil {
			return 0, fmt.Errorf("backupcrypto: read chunk: %w", err)
		}

		// Try both final and non-final nonces for the last-block case:
		// we must decide which variant the encoder used. On a correctly
		// authenticated stream exactly one of the two will pass GCM's
		// tag check; the other will fail with cipher.message authentication.
		writeCounter(nonce, counter, false)
		plaintext, openErr := aead.Open(nil, nonce, ciphertext[:n], nil)
		if openErr != nil {
			// Try the final-flag nonce.
			writeCounter(nonce, counter, true)
			pt2, finalErr := aead.Open(nil, nonce, ciphertext[:n], nil)
			if finalErr != nil {
				return 0, fmt.Errorf("backupcrypto: chunk %d: %w", counter, openErr)
			}
			if _, err := dst.Write(pt2); err != nil {
				return 0, fmt.Errorf("backupcrypto: write plaintext: %w", err)
			}
			total += int64(len(pt2))
			// Final chunk consumed — there must be no more input. Read
			// one byte to confirm EOF; any residual data means the
			// stream was tampered with post-final.
			buf := make([]byte, 1)
			m, _ := src.Read(buf)
			if m > 0 {
				return 0, fmt.Errorf("backupcrypto: unexpected trailing data after final chunk")
			}
			return total, nil
		}

		if _, err := dst.Write(plaintext); err != nil {
			return 0, fmt.Errorf("backupcrypto: write plaintext: %w", err)
		}
		total += int64(len(plaintext))
		counter++
	}
}

// LooksEncrypted returns true if the first four bytes of b are the BRV1
// magic. Used by callers who want a cheap "should I try to decrypt"
// check without committing the whole stream.
func LooksEncrypted(b []byte) bool {
	return len(b) >= 4 && b[0] == Magic[0] && b[1] == Magic[1] && b[2] == Magic[2] && b[3] == Magic[3]
}

func buildHeader(chunkLog2 int, salt, noncePrefix []byte) []byte {
	h := make([]byte, HeaderSize)
	copy(h[0:4], Magic[:])
	h[4] = Version
	h[5] = AlgorithmAESGCM
	binary.BigEndian.PutUint16(h[6:8], uint16(chunkLog2))
	copy(h[8:24], salt)
	copy(h[24:32], noncePrefix)
	return h
}

// writeCounter fills nonce[8..11] with (counter:24 big-endian, final_flag:8).
// Keeping this in a tiny helper lets the encode and decode loops share
// the exact same byte layout.
func writeCounter(nonce []byte, counter uint32, final bool) {
	nonce[8] = byte(counter >> 16)
	nonce[9] = byte(counter >> 8)
	nonce[10] = byte(counter)
	if final {
		nonce[11] = 0x01
	} else {
		nonce[11] = 0x00
	}
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("backupcrypto: aes new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("backupcrypto: gcm: %w", err)
	}
	return aead, nil
}
