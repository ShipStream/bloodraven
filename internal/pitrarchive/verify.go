package pitrarchive

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

// Role selects which descriptor checks apply (§3.3).
type Role string

const (
	// RoleWriter is a writing consumer: the site sidecar and pitr-quarantine.
	RoleWriter Role = "Writer"
	// RoleReader is a reading consumer: pitr-download (through
	// sidecar.OpenArchiveForRead), the standby reconciler's discovery scan,
	// and (#203) pitr-archive-check. dump-meta reads the dump stamp, not
	// the descriptor.
	RoleReader Role = "Reader"
)

// Expectation is what a consumer expects of a descriptor.
type Expectation struct {
	Role Role
	// SourceUID: a writer's own group UID (required). A reader's expected
	// source UID (the dump stamp's source, or the group's own UID for a
	// write binding), or "" when unknown.
	SourceUID types.UID
	// ArchiveID is a reader's expected archive ID (the stamp's
	// pitrArchiveID), or "" when unset. Writers ignore it: they check the
	// supported derivations instead (D-3).
	ArchiveID string
	// Destination is where the descriptor was read. A writer derives the
	// accepted IDs from it (required); both roles use it in messages.
	Destination Destination
	// Encryption is the consumer's configured identity
	// (pitrprofile.EncryptionIdentityFor).
	Encryption EncryptionIdentity
}

// ErrInvalidExpectation is returned for a caller bug, never for a descriptor
// problem.
var ErrInvalidExpectation = errors.New("pitrarchive: invalid descriptor expectation")

// VerifyError is a descriptor refusal. Error() renders the §3.3 message:
//
//	<Token>: archive <archiveID> at <destination>: <detail>; <procedure>
type VerifyError struct {
	Token       Token
	ArchiveID   string
	Destination string
	Detail      string
	Procedure   string
}

func (e *VerifyError) Error() string {
	id := e.ArchiveID
	if id == "" {
		id = "<unknown>"
	}
	msg := string(e.Token) + ": archive " + id
	if e.Destination != "" {
		msg += " at " + e.Destination
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Procedure != "" {
		msg += "; " + e.Procedure
	}
	return msg
}

// TokenOf returns the token of a *VerifyError anywhere in err's chain.
func TokenOf(err error) (Token, bool) {
	var ve *VerifyError
	if errors.As(err, &ve) {
		return ve.Token, true
	}
	return "", false
}

// Procedures appended to refusals. They name the remedy, not the cause.
const (
	procedureVersion    = "upgrade bloodraven to a release that reads this archive"
	procedureCorrupt    = "archive.json is damaged and no release can read it; restore the original object (for example from bucket versioning or a copy) or point this profile at a new prefix"
	procedureForeign    = "point this profile at a new prefix"
	procedureCopy       = "this is a copy of an archive (same group, other destination, or an ID from an unsupported derivation); writers never adopt a copy; point this profile at a new prefix"
	procedureIdentity   = "the dump stamp and the archive disagree; use the archive profile the backup was taken with"
	procedureEncryption = "restore the profile's original encryption settings or use a new prefix"
	procedureKey        = "put the original passphrase back or use a new prefix"
)

// VerifyDescriptor returns the first failing §3.3 check as a *VerifyError,
// or nil. Checks, in order:
//
//  1. kind, version and structure (TokenArchiveVersionUnsupported);
//  2. source.uid: writer — equals its own UID (TokenArchiveForeign); reader —
//     equals the expected UID when set (TokenArchiveIdentityMismatch);
//  3. archiveID: writer — the output of a supported derivation for its own
//     UID and destination (TokenArchiveCopy); reader — equals the expected ID
//     when set (TokenArchiveIdentityMismatch);
//  4. mode and algorithm equal the configured identity
//     (TokenArchiveEncryptionMismatch);
//  5. for Passphrase, keyCheck verifies with passphrase (TokenArchiveKeyMismatch).
//
// passphrase is trimmed with backupcrypto.TrimPassphrase; it is ignored for
// None. A malformed expectation returns ErrInvalidExpectation.
func VerifyDescriptor(d *Descriptor, expect Expectation, passphrase []byte) error {
	if d == nil {
		return fmt.Errorf("%w: nil descriptor", ErrInvalidExpectation)
	}
	switch expect.Role {
	case RoleWriter:
		if expect.SourceUID == "" || expect.Destination.IsZero() {
			return fmt.Errorf("%w: a writer needs its own UID and destination", ErrInvalidExpectation)
		}
	case RoleReader:
	default:
		return fmt.Errorf("%w: unknown role %q", ErrInvalidExpectation, expect.Role)
	}
	if err := expect.Encryption.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExpectation, err)
	}
	refuse := func(t Token, procedure, format string, a ...any) error {
		return &VerifyError{Token: t, ArchiveID: d.ArchiveID, Destination: expect.Destination.String(),
			Detail: fmt.Sprintf(format, a...), Procedure: procedure}
	}

	if err := d.validate(); err != nil {
		var ve *VerifyError
		if errors.As(err, &ve) {
			ve.Destination = expect.Destination.String()
		}
		return err
	}

	switch expect.Role {
	case RoleWriter:
		if d.Source.UID != expect.SourceUID {
			return refuse(TokenArchiveForeign, procedureForeign,
				"this prefix belongs to group %s/%s (uid %s), not this group (uid %s)",
				d.Source.Namespace, d.Source.Name, d.Source.UID, expect.SourceUID)
		}
		if !IsDerivedArchiveID(d.ArchiveID, expect.SourceUID, expect.Destination) {
			return refuse(TokenArchiveCopy, procedureCopy,
				"archive ID is not derived from this group and destination (expected %s)",
				CreateArchiveID(expect.SourceUID, expect.Destination))
		}
	case RoleReader:
		if expect.SourceUID != "" && d.Source.UID != expect.SourceUID {
			return refuse(TokenArchiveIdentityMismatch, procedureIdentity,
				"archive belongs to group %s/%s (uid %s), expected source uid %s",
				d.Source.Namespace, d.Source.Name, d.Source.UID, expect.SourceUID)
		}
		if expect.ArchiveID != "" && d.ArchiveID != expect.ArchiveID {
			return refuse(TokenArchiveIdentityMismatch, procedureIdentity,
				"expected archive %s", expect.ArchiveID)
		}
	}

	if got := d.Encryption.Identity(); got != expect.Encryption {
		return refuse(TokenArchiveEncryptionMismatch, procedureEncryption,
			"the archive is %s, the profile is %s", got, expect.Encryption)
	}
	if d.Encryption.Mode == EncryptionPassphrase {
		if len(backupcrypto.TrimPassphrase(passphrase)) == 0 {
			return refuse(TokenArchiveKeyMismatch, procedureKey, "no passphrase is mounted")
		}
		if !backupcrypto.VerifyArchiveKeyCheck(passphrase, d.KeyCheckFields(), d.Encryption.KeyCheck) {
			return refuse(TokenArchiveKeyMismatch, procedureKey, "the mounted passphrase did not write this archive")
		}
	}
	return nil
}

// ParseAndVerifyDescriptor is ParseDescriptor followed by VerifyDescriptor,
// with the destination filled into parse refusals. The parse refusal keeps
// the procedure ParseDescriptor chose (upgrade for a newer version,
// repair for corruption).
func ParseAndVerifyDescriptor(b []byte, expect Expectation, passphrase []byte) (*Descriptor, error) {
	d, err := ParseDescriptor(b)
	if err != nil {
		var ve *VerifyError
		if errors.As(err, &ve) {
			ve.Destination = expect.Destination.String()
		}
		return nil, err
	}
	if err := VerifyDescriptor(d, expect, passphrase); err != nil {
		return nil, err
	}
	return d, nil
}
