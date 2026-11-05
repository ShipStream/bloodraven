package pitrarchive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

// Descriptor constants (§3.2).
const (
	DescriptorKind    = "BloodravenPITRArchive"
	DescriptorVersion = 1
	// DescriptorFileName is the descriptor's name under the archive's key
	// prefix: <keyPrefix>/archive.json.
	DescriptorFileName = "archive.json"
	// CASProbeKeyPrefix starts the base name of ProbeAtomicCreate's
	// throwaway key: <keyPrefix>/.bloodraven-cas-probe-<16 hex>.
	CASProbeKeyPrefix = ".bloodraven-cas-probe-"
	// AlgorithmAES256GCM is the only supported Passphrase algorithm.
	AlgorithmAES256GCM = "AES-256-GCM"
)

// DescriptorKey returns the descriptor's store key for a key prefix.
func DescriptorKey(keyPrefix string) string {
	return path.Join(keyPrefix, DescriptorFileName)
}

// IsIgnoredArchiveKey reports whether a key under an archive prefix is a
// leftover the legacy check ignores (§3.4): "*.tmp", "*.part",
// "archive.json.tmp-*" and CAS probe keys. The descriptor itself is not
// ignored; callers handle it explicitly.
func IsIgnoredArchiveKey(key string) bool {
	base := path.Base(key)
	return strings.HasSuffix(base, ".tmp") ||
		strings.HasSuffix(base, ".part") ||
		strings.HasPrefix(base, DescriptorFileName+".tmp-") ||
		strings.HasPrefix(base, CASProbeKeyPrefix)
}

// EncryptionMode is an archive's encryption mode. The API mirrors it as
// v1alpha1.PITRArchiveEncryptionMode.
type EncryptionMode string

const (
	EncryptionNone       EncryptionMode = "None"
	EncryptionPassphrase EncryptionMode = "Passphrase"
)

// EncryptionIdentity is what an archive's descriptor records about its
// encryption, and what every consumer compares (D-7). For None, Algorithm
// is "". Build it from a profile with pitrprofile.EncryptionIdentityFor.
type EncryptionIdentity struct {
	Mode      EncryptionMode
	Algorithm string
}

// Validate checks the mode/algorithm pairing.
func (e EncryptionIdentity) Validate() error {
	switch e.Mode {
	case EncryptionNone:
		if e.Algorithm != "" {
			return fmt.Errorf("encryption mode None carries algorithm %q", e.Algorithm)
		}
	case EncryptionPassphrase:
		if e.Algorithm != AlgorithmAES256GCM {
			return fmt.Errorf("encryption mode Passphrase has algorithm %q, want %q", e.Algorithm, AlgorithmAES256GCM)
		}
	default:
		return fmt.Errorf("unknown encryption mode %q", e.Mode)
	}
	return nil
}

// String renders "None" or "Passphrase/AES-256-GCM".
func (e EncryptionIdentity) String() string {
	if e.Algorithm == "" {
		return string(e.Mode)
	}
	return string(e.Mode) + "/" + e.Algorithm
}

// Source names the group that owns an archive.
type Source struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

// DescriptorEncryption is the descriptor's encryption block. KeyCheck is set
// iff Mode is Passphrase (backupcrypto.ArchiveKeyCheck).
type DescriptorEncryption struct {
	Mode      EncryptionMode `json:"mode"`
	Algorithm string         `json:"algorithm"`
	KeyCheck  string         `json:"keyCheck"`
}

// Identity returns the mode/algorithm pair.
func (e DescriptorEncryption) Identity() EncryptionIdentity {
	return EncryptionIdentity{Mode: e.Mode, Algorithm: e.Algorithm}
}

// Descriptor is the write-once, plaintext <keyPrefix>/archive.json (§3.2):
//
//	{"kind":"BloodravenPITRArchive","version":1,"archiveID":"ar1-…",
//	 "source":{"namespace":"…","name":"…","uid":"…"},
//	 "encryption":{"mode":"None|Passphrase","algorithm":"|AES-256-GCM","keyCheck":"|<b64url>"},
//	 "createdAt":"…","createdBy":"<site>"}
//
// The identity fields are Version, ArchiveID, Source and Encryption
// (including KeyCheck). CreatedAt and CreatedBy are informational: two
// sibling writers racing to create may differ only there.
type Descriptor struct {
	Kind       string               `json:"kind"`
	Version    int                  `json:"version"`
	ArchiveID  string               `json:"archiveID"`
	Source     Source               `json:"source"`
	Encryption DescriptorEncryption `json:"encryption"`
	CreatedAt  time.Time            `json:"createdAt"`
	CreatedBy  string               `json:"createdBy"`
}

// NewDescriptor builds the descriptor a writer creates for its own group at
// d: the ID is CreateArchiveID(source.UID, d), and for Passphrase the
// keyCheck is computed from passphrase (trimmed by backupcrypto). createdAt
// is truncated to the microsecond, UTC.
func NewDescriptor(source Source, d Destination, enc EncryptionIdentity, passphrase []byte, site string, now time.Time) (*Descriptor, error) {
	if source.UID == "" || source.Namespace == "" || source.Name == "" {
		return nil, fmt.Errorf("pitrarchive: descriptor source %+v is incomplete", source)
	}
	if d.IsZero() {
		return nil, fmt.Errorf("pitrarchive: descriptor destination is empty")
	}
	if err := enc.Validate(); err != nil {
		return nil, fmt.Errorf("pitrarchive: %w", err)
	}
	desc := &Descriptor{
		Kind:       DescriptorKind,
		Version:    DescriptorVersion,
		ArchiveID:  CreateArchiveID(source.UID, d),
		Source:     source,
		Encryption: DescriptorEncryption{Mode: enc.Mode, Algorithm: enc.Algorithm},
		CreatedAt:  now.UTC().Truncate(time.Microsecond),
		CreatedBy:  site,
	}
	if enc.Mode == EncryptionPassphrase {
		kc, err := backupcrypto.ArchiveKeyCheck(passphrase, desc.KeyCheckFields())
		if err != nil {
			return nil, fmt.Errorf("pitrarchive: keyCheck: %w", err)
		}
		desc.Encryption.KeyCheck = kc
	}
	return desc, nil
}

// KeyCheckFields returns the fields the keyCheck covers.
func (d *Descriptor) KeyCheckFields() backupcrypto.KeyCheckFields {
	return backupcrypto.KeyCheckFields{
		ArchiveID:       d.ArchiveID,
		SourceUID:       string(d.Source.UID),
		SourceNamespace: d.Source.Namespace,
		SourceName:      d.Source.Name,
		Mode:            string(d.Encryption.Mode),
		Algorithm:       d.Encryption.Algorithm,
	}
}

// Canonical returns the descriptor's canonical JSON: fixed field order,
// createdAt in UTC at microsecond precision, no insignificant whitespace.
// Writers store exactly these bytes; ParseDescriptor(b).Canonical() == b
// for any b a writer stored.
func (d *Descriptor) Canonical() ([]byte, error) {
	c := *d
	c.CreatedAt = c.CreatedAt.UTC().Truncate(time.Microsecond)
	return json.Marshal(c)
}

// IdentityEqual reports whether a and b agree on every identity field
// (Kind, Version, ArchiveID, Source, Encryption including KeyCheck). It
// ignores CreatedAt and CreatedBy.
func IdentityEqual(a, b *Descriptor) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Kind == b.Kind && a.Version == b.Version && a.ArchiveID == b.ArchiveID &&
		a.Source == b.Source && a.Encryption == b.Encryption
}

// ParseDescriptor decodes and validates descriptor bytes. Unknown fields are
// tolerated within version 1 (forward-compatible additions); a different
// kind or version is refused, never read best-effort. Every failure is a
// *VerifyError with TokenArchiveVersionUnsupported.
func ParseDescriptor(b []byte) (*Descriptor, error) {
	var d Descriptor
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&d); err != nil {
		return nil, &VerifyError{Token: TokenArchiveVersionUnsupported, Detail: fmt.Sprintf("archive.json does not parse: %v", err)}
	}
	if dec.More() {
		return nil, &VerifyError{Token: TokenArchiveVersionUnsupported, Detail: "archive.json has trailing data"}
	}
	if err := d.validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// validate applies the structural rules ParseDescriptor enforces.
func (d *Descriptor) validate() error {
	fail := func(format string, a ...any) error {
		return &VerifyError{Token: TokenArchiveVersionUnsupported, ArchiveID: d.ArchiveID, Detail: fmt.Sprintf(format, a...)}
	}
	switch {
	case d.Kind != DescriptorKind:
		return fail("archive.json kind is %q, want %q", d.Kind, DescriptorKind)
	case d.Version != DescriptorVersion:
		return fail("archive.json version is %d; this release reads only version %d", d.Version, DescriptorVersion)
	case !ValidArchiveID(d.ArchiveID):
		return fail("archive.json archiveID %q is malformed", d.ArchiveID)
	case d.Source.UID == "" || d.Source.Namespace == "" || d.Source.Name == "":
		return fail("archive.json source {namespace,name,uid} is incomplete")
	}
	if err := d.Encryption.Identity().Validate(); err != nil {
		return fail("archive.json %v", err)
	}
	switch d.Encryption.Mode {
	case EncryptionNone:
		if d.Encryption.KeyCheck != "" {
			return fail("archive.json mode None carries a keyCheck")
		}
	case EncryptionPassphrase:
		if !backupcrypto.ValidKeyCheckEncoding(d.Encryption.KeyCheck) {
			return fail("archive.json keyCheck is missing or malformed")
		}
	}
	return nil
}
