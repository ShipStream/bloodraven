package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EncryptionAtRestSpec configures MySQL InnoDB data-at-rest encryption
// backed by the GPL `component_keyring_file` keyring component.
//
// The design intentionally keeps MySQL key material off the MySQL data
// PVC and off worker-node disks:
//
//   - In steady state ("sealed"), the keyring data file is projected
//     read-only from a per-site Kubernetes Secret. kubelet materializes
//     Secret volumes on node-local tmpfs, so the key never lands on a
//     persistent disk, and `"read_only": true` in the component config
//     means mysqld physically cannot add keys.
//   - Key-creating operations (initial bootstrap, CLONE INSTANCE into a
//     recipient, master-key rotation) require an "unsealed" pod: the
//     keyring lives on a memory-backed emptyDir, seeded from the escrow
//     Secret. The sidecar escrows every change back to a new immutable
//     Secret version, and the operator re-seals only after it has
//     independently verified that the escrowed digest matches the live
//     keyring.
//
// Because escrow and sealing are gated, the only window in which a
// keyring can be lost is while a site is deliberately unsealed. The
// operator refuses to unseal the active primary for rotation, so a site
// that loses its keyring in that window is always recoverable by
// re-cloning it from a healthy peer.
//
// This is NOT Oracle "MySQL Enterprise Transparent Data Encryption".
// Oracle explicitly states that the file-based keyring components are
// not intended as regulatory-compliance solutions. See
// docs/docs/encryption-at-rest.mdx for the full threat model.
type EncryptionAtRestSpec struct {
	// Enabled turns on data-at-rest encryption for every site in the
	// group. Enabling requires spec.tls to be set, because MySQL
	// requires a secure connection to clone encrypted data.
	//
	// Encryption can only be enabled on a group that has not yet
	// bootstrapped, or adopted by an existing group via the replica-first
	// reclone procedure documented in docs/docs/encryption-at-rest.mdx.
	// The operator rejects flipping this flag on a group whose sites
	// already hold unencrypted data.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// Coverage selects which MySQL encryption settings the operator
	// enforces in the generated my.cnf. Every field defaults to true;
	// setting one to false narrows the security claim and is reported
	// on status.encryptionAtRest.
	// +optional
	Coverage *EncryptionCoverageSpec `json:"coverage,omitempty"`

	// Keyring configures the keyring component paths and the escrow
	// Secret lifecycle.
	// +optional
	Keyring *KeyringSpec `json:"keyring,omitempty"`
}

// EncryptionCoverageSpec selects the MySQL encryption settings the
// operator writes into the generated my.cnf. These are operator-owned
// once encryption is enabled: spec.mysqlConf overrides for the same keys
// are ignored, so a stray override cannot silently drop coverage.
type EncryptionCoverageSpec struct {
	// Tables sets default_table_encryption. When true (default), every
	// newly created schema and file-per-table tablespace is encrypted.
	// It does NOT retroactively encrypt tables that already exist.
	// +optional
	Tables *bool `json:"tables,omitempty"`

	// PrivilegeCheck sets table_encryption_privilege_check. When true
	// (default), creating a table whose encryption differs from the
	// schema default requires TABLE_ENCRYPTION_ADMIN, so application
	// users cannot opt individual tables out of encryption.
	// +optional
	PrivilegeCheck *bool `json:"privilegeCheck,omitempty"`

	// RedoLog sets innodb_redo_log_encrypt. Default true.
	// +optional
	RedoLog *bool `json:"redoLog,omitempty"`

	// UndoLog sets innodb_undo_log_encrypt. Default true.
	// +optional
	UndoLog *bool `json:"undoLog,omitempty"`

	// BinaryLog sets binlog_encryption, which covers both binary logs
	// on the primary and relay logs on replicas. Default true.
	// +optional
	BinaryLog *bool `json:"binaryLog,omitempty"`

	// SystemTablespace controls whether the operator runs
	// `ALTER TABLESPACE mysql ENCRYPTION='Y'` once a site is sealed.
	// The `mysql` system tablespace is not covered by
	// default_table_encryption and holds the data dictionary, so leaving
	// this false leaves table and column names in plaintext on disk.
	// Default true. The statement reuses the existing master key, so it
	// is safe to run against a sealed (read-only) keyring.
	// +optional
	SystemTablespace *bool `json:"systemTablespace,omitempty"`
}

// KeyringSpec configures `component_keyring_file` placement and the
// escrow Secret lifecycle.
//
// MysqldDir and PluginDir are image-specific. The defaults match the
// official `mysql:9.x` images (Oracle Linux based). If you run a
// different MySQL image, verify them with:
//
//	docker run --rm --entrypoint sh <image> -c 'command -v mysqld; mysqld --verbose --help | grep plugin_dir'
type KeyringSpec struct {
	// DataFileDir is the in-container directory holding the keyring data
	// file. It is backed by a Secret volume when sealed and by a
	// memory-backed emptyDir when unsealed. MySQL requires that this
	// path is NOT inside the data directory.
	// +kubebuilder:default="/run/mysql-keyring"
	// +kubebuilder:validation:Pattern=`^/[^\0]*[^/]$`
	// +optional
	DataFileDir string `json:"dataFileDir,omitempty"`

	// MysqldDir is the directory containing the mysqld binary. MySQL
	// only reads its global component manifest from there, so the
	// operator mounts `mysqld.my` into this directory.
	// +kubebuilder:default="/usr/sbin"
	// +kubebuilder:validation:Pattern=`^/[^\0]*[^/]$`
	// +optional
	MysqldDir string `json:"mysqldDir,omitempty"`

	// PluginDir is MySQL's plugin_dir. MySQL only reads the global
	// `component_keyring_file.cnf` from there, so the operator mounts
	// the generated config into this directory.
	// +kubebuilder:default="/usr/lib64/mysql/plugin"
	// +kubebuilder:validation:Pattern=`^/[^\0]*[^/]$`
	// +optional
	PluginDir string `json:"pluginDir,omitempty"`

	// RetainVersions is how many superseded escrow Secret versions the
	// operator keeps per site. Older versions are garbage-collected.
	// Keeping more than one lets an operator roll a site back onto a
	// previous keyring if a rotation went wrong.
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Maximum=50
	// +optional
	RetainVersions int32 `json:"retainVersions,omitempty"`

	// EscrowTimeoutSeconds is how long the operator waits for a freshly
	// unsealed site to escrow its keyring before reporting the site as
	// failed on status. The site is not sealed and not admitted to
	// service until escrow succeeds; the timeout only controls when the
	// condition flips to a loud failure.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=30
	// +optional
	EscrowTimeoutSeconds int32 `json:"escrowTimeoutSeconds,omitempty"`
}

// SiteKeyringPhase is the per-site keyring lifecycle state. It drives
// how the operator renders that site's MySQL Deployment.
// +kubebuilder:validation:Enum="";Pending;Unsealed;Escrowed;Sealed;Failed
type SiteKeyringPhase string

const (
	// KeyringPhasePending means the site has no escrowed keyring yet.
	// The Deployment renders unsealed with an empty memory keyring so
	// MySQL can create its master keys during initialization.
	KeyringPhasePending SiteKeyringPhase = "Pending"

	// KeyringPhaseUnsealed means the site is deliberately running with a
	// writable memory-backed keyring: initial bootstrap, a CLONE
	// INSTANCE into this site, or an admin-triggered rotation. Sites in
	// this phase are not considered protected.
	KeyringPhaseUnsealed SiteKeyringPhase = "Unsealed"

	// KeyringPhaseEscrowed means the live keyring has been captured into
	// an immutable Secret and the operator has verified the digest. The
	// Deployment is being rolled onto the sealed rendering.
	KeyringPhaseEscrowed SiteKeyringPhase = "Escrowed"

	// KeyringPhaseSealed is the steady state: the keyring is projected
	// read-only from the escrow Secret and mysqld cannot add keys.
	KeyringPhaseSealed SiteKeyringPhase = "Sealed"

	// KeyringPhaseFailed means the operator could not complete the
	// lifecycle (escrow timed out, digest mismatch, missing Secret).
	KeyringPhaseFailed SiteKeyringPhase = "Failed"
)

// KeyringUnsealReason records why a site is currently unsealed. It is
// purely diagnostic, but it is also what the operator uses to decide
// whether re-sealing is safe to attempt.
// +kubebuilder:validation:Enum="";Bootstrap;Clone;Rotation
type KeyringUnsealReason string

const (
	// UnsealReasonBootstrap is the initial keyring creation on a fresh
	// data directory.
	UnsealReasonBootstrap KeyringUnsealReason = "Bootstrap"

	// UnsealReasonClone is a CLONE INSTANCE into this site. The clone
	// recipient re-encrypts the donor's tablespace keys under its own
	// new master key, which requires a writable keyring.
	UnsealReasonClone KeyringUnsealReason = "Clone"

	// UnsealReasonRotation is an admin-triggered master-key rotation.
	UnsealReasonRotation KeyringUnsealReason = "Rotation"
)

// EncryptionAtRestStatus is the observed state of the encryption
// subsystem, reported on MysqlFailoverGroup.status.
type EncryptionAtRestStatus struct {
	// Sealed is true when encryption is enabled and every site in the
	// group is in the Sealed phase. It is the single field to alert on:
	// false means at least one site is running with a writable keyring
	// or has failed to escrow.
	// +optional
	Sealed bool `json:"sealed,omitempty"`

	// Sites is the per-site keyring lifecycle state.
	// +optional
	// +listType=map
	// +listMapKey=name
	Sites []SiteEncryptionStatus `json:"sites,omitempty"`

	// ObservedGeneration is the CR generation the encryption subsystem
	// last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// SiteEncryptionStatus is the per-site keyring lifecycle state.
type SiteEncryptionStatus struct {
	// Name is the site identifier, mirrored from spec.sites[].name.
	Name string `json:"name"`

	// Phase is the keyring lifecycle phase for this site.
	// +optional
	Phase SiteKeyringPhase `json:"phase,omitempty"`

	// UnsealReason records why the site is unsealed. Empty when the site
	// is sealed.
	// +optional
	UnsealReason KeyringUnsealReason `json:"unsealReason,omitempty"`

	// KeyringSecret is the name of the escrow Secret this site is
	// currently sealed against (or seeded from while unsealed).
	// +optional
	KeyringSecret string `json:"keyringSecret,omitempty"`

	// KeyringVersion is the escrow Secret version. Monotonically
	// increasing per site; each version is an immutable Secret.
	// +optional
	KeyringVersion int32 `json:"keyringVersion,omitempty"`

	// KeyringDigest is "sha256:<hex>" over the escrowed keyring bytes.
	// The operator refuses to seal a site until the sidecar reports the
	// same digest for the live keyring file.
	// +optional
	KeyringDigest string `json:"keyringDigest,omitempty"`

	// LastEscrowTime is when the current KeyringVersion was accepted.
	// +optional
	LastEscrowTime *metav1.Time `json:"lastEscrowTime,omitempty"`

	// UnsealedSince is when the site entered the Unsealed phase. Used to
	// evaluate keyring.escrowTimeoutSeconds.
	// +optional
	UnsealedSince *metav1.Time `json:"unsealedSince,omitempty"`

	// Message is a human-readable explanation of the current phase,
	// populated on Failed and on transient waits.
	// +optional
	Message string `json:"message,omitempty"`

	// Coverage is the encryption coverage last observed on this site's
	// live MySQL instance.
	// +optional
	Coverage *SiteEncryptionCoverage `json:"coverage,omitempty"`
}

// SiteEncryptionCoverage is what the operator actually observed on the
// live instance, as opposed to what the spec asked for. A site can be
// Sealed and still have incomplete coverage — for example when an
// existing schema was adopted and its tables were never rebuilt.
type SiteEncryptionCoverage struct {
	// KeyringComponent is the Component_name reported by
	// performance_schema.keyring_component_status.
	// +optional
	KeyringComponent string `json:"keyringComponent,omitempty"`

	// KeyringReadOnly mirrors the component's Read_only status. In the
	// Sealed phase this must be true.
	// +optional
	KeyringReadOnly bool `json:"keyringReadOnly,omitempty"`

	// SystemTablespaceEncrypted reports whether the `mysql` tablespace
	// is encrypted.
	// +optional
	SystemTablespaceEncrypted bool `json:"systemTablespaceEncrypted,omitempty"`

	// UnencryptedTablespaces counts user tablespaces reporting
	// ENCRYPTION='N'. Non-zero on an adopted cluster whose pre-existing
	// tables were never rebuilt.
	// +optional
	UnencryptedTablespaces int64 `json:"unencryptedTablespaces,omitempty"`

	// RedoLogEncrypted mirrors @@innodb_redo_log_encrypt.
	// +optional
	RedoLogEncrypted bool `json:"redoLogEncrypted,omitempty"`

	// UndoLogEncrypted mirrors @@innodb_undo_log_encrypt.
	// +optional
	UndoLogEncrypted bool `json:"undoLogEncrypted,omitempty"`

	// BinlogEncrypted mirrors @@binlog_encryption.
	// +optional
	BinlogEncrypted bool `json:"binlogEncrypted,omitempty"`

	// LastCheckTime is when coverage was last sampled.
	// +optional
	LastCheckTime *metav1.Time `json:"lastCheckTime,omitempty"`
}
