package backupcrypto

import (
	"bytes"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// Archive key check (design D-7, §3.2).
//
// A PITR archive descriptor is plaintext, so it cannot hold the key. It holds
// a keyCheck instead: a MAC over the descriptor's identity fields, keyed by
// the archive passphrase. A consumer that holds the right passphrase can
// recompute it; one with the wrong passphrase cannot. The construction is:
//
//	salt     = SHA-256(KeyCheckSaltDomain || archiveID)
//	K        = HKDF-SHA256(secret = TrimPassphrase(passphrase), salt, info = KeyCheckInfo, 32)
//	input    = KeyCheckInputDomain
//	           || lp(archiveID) || lp(source.uid) || lp(source.namespace) || lp(source.name)
//	           || lp(encryption.mode) || lp(encryption.algorithm)
//	lp(x)    = uint32_be(len(x)) || x
//	keyCheck = base64.RawURLEncoding(HMAC-SHA256(K, input))   // 43 characters
//
// The salt is bound to the archive ID, so the same passphrase used for two
// archives yields unrelated checks. The input is length-prefixed binary, not
// JSON, so no canonicalization question arises. keyCheck is an offline
// guessing oracle no stronger than the GCM tag on chunk 0 of any BRV1 object
// already in the same bucket (neither stretches the passphrase).
//
// The golden vector in keycheck_test.go pins this format. Changing any
// constant or the input layout makes every existing archive refuse
// PITRArchiveKeyMismatch, so it must never change.
const (
	KeyCheckSaltDomain  = "bloodraven-pitr-archive-keycheck-salt-v1\x00"
	KeyCheckInfo        = "bloodraven-pitr-archive-keycheck-v1"
	KeyCheckInputDomain = "bloodraven-pitr-archive-v1\x00"
	// KeyCheckEncodedLen is the length of an encoded keyCheck
	// (base64url, no padding, of a 32-byte HMAC-SHA256).
	KeyCheckEncodedLen = 43
)

// KeyCheckFields are the descriptor identity fields a keyCheck covers. They
// are plain strings so this package does not depend on the descriptor type
// (internal/pitrarchive builds them from a Descriptor).
type KeyCheckFields struct {
	ArchiveID       string
	SourceUID       string
	SourceNamespace string
	SourceName      string
	// Mode is the descriptor's encryption mode ("Passphrase"; a "None"
	// archive has no keyCheck).
	Mode string
	// Algorithm is the descriptor's algorithm ("AES-256-GCM").
	Algorithm string
}

// TrimPassphrase strips leading and trailing '\n', '\r', ' ' and '\t' from a
// passphrase. It is the single trim rule for passphrases (D-7):
// ReadPassphraseFile uses it, and so must every component that compares or
// snapshots passphrase bytes. It deliberately does not use
// strings.TrimSpace, which also strips '\v', '\f', U+0085 and U+00A0, so two
// components would disagree on which bytes are the key.
//
// The result aliases b; copy it if b will be mutated.
func TrimPassphrase(b []byte) []byte {
	return bytes.TrimFunc(b, isPassphraseSpace)
}

func isPassphraseSpace(r rune) bool {
	return r == '\n' || r == '\r' || r == ' ' || r == '\t'
}

// ArchiveKeyCheck computes the keyCheck for f under passphrase. The
// passphrase is trimmed with TrimPassphrase first; an empty result returns
// ErrEmptyPassphrase (crypto.go).
func ArchiveKeyCheck(passphrase []byte, f KeyCheckFields) (string, error) {
	mac, err := archiveKeyCheckMAC(passphrase, f)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(mac), nil
}

// VerifyArchiveKeyCheck reports whether keyCheck is the keyCheck of f under
// passphrase. It returns false for an empty passphrase or a malformed
// keyCheck, and compares in constant time.
func VerifyArchiveKeyCheck(passphrase []byte, f KeyCheckFields, keyCheck string) bool {
	if len(keyCheck) != KeyCheckEncodedLen {
		return false
	}
	want, err := base64.RawURLEncoding.Strict().DecodeString(keyCheck)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	got, err := archiveKeyCheckMAC(passphrase, f)
	if err != nil {
		return false
	}
	return hmac.Equal(got, want)
}

// ValidKeyCheckEncoding reports whether s is a well-formed keyCheck
// (43 characters of strict base64url that decode to 32 bytes). It does not
// check the key.
func ValidKeyCheckEncoding(s string) bool {
	if len(s) != KeyCheckEncodedLen {
		return false
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	return err == nil && len(b) == sha256.Size
}

func archiveKeyCheckMAC(passphrase []byte, f KeyCheckFields) ([]byte, error) {
	secret := TrimPassphrase(passphrase)
	if len(secret) == 0 {
		return nil, ErrEmptyPassphrase
	}
	salt := sha256.Sum256(append([]byte(KeyCheckSaltDomain), f.ArchiveID...))
	key, err := hkdf.Key(sha256.New, secret, salt[:], KeyCheckInfo, 32)
	if err != nil {
		return nil, fmt.Errorf("backupcrypto: keycheck hkdf: %w", err)
	}
	m := hmac.New(sha256.New, key)
	m.Write(keyCheckInput(f))
	return m.Sum(nil), nil
}

func keyCheckInput(f KeyCheckFields) []byte {
	var buf bytes.Buffer
	buf.WriteString(KeyCheckInputDomain)
	for _, s := range []string{f.ArchiveID, f.SourceUID, f.SourceNamespace, f.SourceName, f.Mode, f.Algorithm} {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s)))
		buf.Write(n[:])
		buf.WriteString(s)
	}
	return buf.Bytes()
}
