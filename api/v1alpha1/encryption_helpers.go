package v1alpha1

import (
	"fmt"
	"path"
	"time"
)

// Defaults applied when the corresponding EncryptionAtRestSpec fields
// are unset. Kept here (rather than relying solely on kubebuilder
// defaults) so that in-memory objects built by tests and by the
// standby-cluster template path see the same effective values as
// objects that went through admission.
const (
	DefaultKeyringDataFileDir      = "/run/mysql-keyring"
	DefaultKeyringMysqldDir        = "/usr/sbin"
	DefaultKeyringPluginDir        = "/usr/lib64/mysql/plugin"
	DefaultKeyringRetainVersions   = int32(5)
	DefaultKeyringEscrowTimeoutSec = int32(600)

	// KeyringDataFileName is the basename of the keyring data file
	// inside DataFileDir. It is also the key under which the keyring
	// bytes are stored in the escrow Secret, so the Secret can be
	// projected straight onto the expected path with no rename.
	KeyringDataFileName = "keyring"

	// KeyringTokenKey is the Secret key holding the per-site bootstrap
	// token the sidecar presents when escrowing a keyring.
	KeyringTokenKey = "token"
)

// EncryptionEnabled reports whether data-at-rest encryption is turned on
// for this failover group.
func (s *MysqlFailoverGroupSpec) EncryptionEnabled() bool {
	return s.EncryptionAtRest != nil && s.EncryptionAtRest.Enabled
}

// EffectiveKeyring returns spec.encryptionAtRest.keyring with defaults
// filled in. Safe to call when encryption is disabled or the block is
// nil; the caller gets the default shape.
func (s *MysqlFailoverGroupSpec) EffectiveKeyring() KeyringSpec {
	out := KeyringSpec{
		DataFileDir:          DefaultKeyringDataFileDir,
		MysqldDir:            DefaultKeyringMysqldDir,
		PluginDir:            DefaultKeyringPluginDir,
		RetainVersions:       DefaultKeyringRetainVersions,
		EscrowTimeoutSeconds: DefaultKeyringEscrowTimeoutSec,
	}
	if s.EncryptionAtRest == nil || s.EncryptionAtRest.Keyring == nil {
		return out
	}
	k := s.EncryptionAtRest.Keyring
	if k.DataFileDir != "" {
		out.DataFileDir = k.DataFileDir
	}
	if k.MysqldDir != "" {
		out.MysqldDir = k.MysqldDir
	}
	if k.PluginDir != "" {
		out.PluginDir = k.PluginDir
	}
	if k.RetainVersions > 0 {
		out.RetainVersions = k.RetainVersions
	}
	if k.EscrowTimeoutSeconds > 0 {
		out.EscrowTimeoutSeconds = k.EscrowTimeoutSeconds
	}
	return out
}

// KeyringDataFilePath returns the absolute in-container path of the
// keyring data file.
func (s *MysqlFailoverGroupSpec) KeyringDataFilePath() string {
	return path.Join(s.EffectiveKeyring().DataFileDir, KeyringDataFileName)
}

// EscrowTimeout returns keyring.escrowTimeoutSeconds as a Duration.
func (s *MysqlFailoverGroupSpec) EscrowTimeout() time.Duration {
	return time.Duration(s.EffectiveKeyring().EscrowTimeoutSeconds) * time.Second
}

// EffectiveEncryptionCoverage returns spec.encryptionAtRest.coverage
// with every unset field defaulted to true. Returns the all-false zero
// value when encryption is disabled, so callers can use it directly to
// decide whether to emit a setting.
func (s *MysqlFailoverGroupSpec) EffectiveEncryptionCoverage() EncryptionCoverageSpec {
	if !s.EncryptionEnabled() {
		return EncryptionCoverageSpec{}
	}
	on := true
	out := EncryptionCoverageSpec{
		Tables:           &on,
		PrivilegeCheck:   &on,
		RedoLog:          &on,
		UndoLog:          &on,
		BinaryLog:        &on,
		SystemTablespace: &on,
	}
	c := s.EncryptionAtRest.Coverage
	if c == nil {
		return out
	}
	if c.Tables != nil {
		out.Tables = c.Tables
	}
	if c.PrivilegeCheck != nil {
		out.PrivilegeCheck = c.PrivilegeCheck
	}
	if c.RedoLog != nil {
		out.RedoLog = c.RedoLog
	}
	if c.UndoLog != nil {
		out.UndoLog = c.UndoLog
	}
	if c.BinaryLog != nil {
		out.BinaryLog = c.BinaryLog
	}
	if c.SystemTablespace != nil {
		out.SystemTablespace = c.SystemTablespace
	}
	return out
}

// KeyringTokenSecretName returns the name of the per-site Secret holding
// the escrow bearer token. The token is mounted into the sidecar only
// while the site is unsealed.
func KeyringTokenSecretName(group, site string) string {
	return fmt.Sprintf("mysql-%s-%s-keyring-token", group, site)
}

// KeyringSecretName returns the name of the immutable escrow Secret for
// a given site and version.
func KeyringSecretName(group, site string, version int32) string {
	return fmt.Sprintf("mysql-%s-%s-keyring-v%d", group, site, version)
}

// SiteEncryptionStatusByName returns a pointer to the per-site
// encryption status, or nil when absent.
func (st *EncryptionAtRestStatus) SiteEncryptionStatusByName(name string) *SiteEncryptionStatus {
	if st == nil {
		return nil
	}
	for i := range st.Sites {
		if st.Sites[i].Name == name {
			return &st.Sites[i]
		}
	}
	return nil
}

// EffectiveSitePhase returns the keyring phase the operator should act
// on for a site, applying the "no status yet means Pending" default.
// Returns the empty phase when encryption is disabled so callers can
// short-circuit.
func (fg *MysqlFailoverGroup) EffectiveSitePhase(site string) SiteKeyringPhase {
	if !fg.Spec.EncryptionEnabled() {
		return ""
	}
	if s := fg.Status.EncryptionAtRest.SiteEncryptionStatusByName(site); s != nil && s.Phase != "" {
		return s.Phase
	}
	return KeyringPhasePending
}

// SiteKeyringSealed reports whether a site's Deployment should render
// the sealed (read-only, Secret-projected) keyring.
//
// Escrowed is included deliberately: Escrowed means "the keyring is
// durably captured and verified, so it is now safe to seal". Flipping
// the rendering is what starts the roll; observing that roll complete is
// what advances the phase from Escrowed to Sealed. Failed is excluded so
// a site that could not escrow is never sealed against a Secret the
// operator does not trust.
func (fg *MysqlFailoverGroup) SiteKeyringSealed(site string) bool {
	switch fg.EffectiveSitePhase(site) {
	case KeyringPhaseEscrowed, KeyringPhaseSealed:
		return true
	default:
		return false
	}
}
