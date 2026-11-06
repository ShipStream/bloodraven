package pitrarchive

import (
	"crypto/sha256"
	"encoding/base32"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// ArchiveIDPattern matches every archive ID this release creates or accepts.
// It is the kubebuilder Pattern of the API's archiveID fields.
const ArchiveIDPattern = `^ar1-[a-z2-7]{26}$`

var archiveIDRE = regexp.MustCompile(ArchiveIDPattern)

// ValidArchiveID reports whether id matches ArchiveIDPattern.
func ValidArchiveID(id string) bool { return archiveIDRE.MatchString(id) }

// ArchiveIDDerivations lists every supported archive ID derivation (D-2),
// newest first. Index 0 is the one writers use to create a descriptor, so
// a new derivation is prepended, never appended. A
// writer accepts a descriptor whose ID equals the output of any entry for
// its own UID and destination (D-3); anything else is a copy.
//
// The derivation hashes Destination.String(), so a future change to
// normalization must add a new derivation here (with a new golden vector)
// instead of changing archiveIDV1, or every existing archive would become a
// copy to its own writers.
var ArchiveIDDerivations = []func(types.UID, Destination) string{archiveIDV1}

// archiveIDV1Domain is the domain-separation prefix of derivation v1.
const archiveIDV1Domain = "bloodraven-pitr-archive-id-v1\x00"

// archiveIDV1 is "ar1-" + the first 26 characters of the lowercase,
// unpadded base32 of SHA-256(domain || groupUID || "\0" || destination).
// 26 characters carry 130 bits.
func archiveIDV1(uid types.UID, d Destination) string {
	h := sha256.New()
	h.Write([]byte(archiveIDV1Domain))
	h.Write([]byte(uid))
	h.Write([]byte{0})
	h.Write([]byte(d.String()))
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(h.Sum(nil))
	return "ar1-" + strings.ToLower(enc[:26])
}

// CreateArchiveID returns the ID a writer creates a new descriptor with:
// ArchiveIDDerivations[0](uid, d). It is also BLOODRAVEN_PITR_ARCHIVE_ID and
// MysqlBackup.status.pitrArchiveID.
func CreateArchiveID(uid types.UID, d Destination) string {
	return ArchiveIDDerivations[0](uid, d)
}

// IsDerivedArchiveID reports whether id is the output of any supported
// derivation for uid and d (D-3's writer check).
func IsDerivedArchiveID(id string, uid types.UID, d Destination) bool {
	if uid == "" || d.IsZero() {
		return false
	}
	for _, derive := range ArchiveIDDerivations {
		if derive(uid, d) == id {
			return true
		}
	}
	return false
}
