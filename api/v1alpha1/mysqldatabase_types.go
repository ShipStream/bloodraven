package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mydb;mydbs,categories=bloodraven;mysql
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.groupRef.name`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.databaseName`
// +kubebuilder:printcolumn:name="Owner",type=string,JSONPath=`.status.ownerUser`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Policy",type=string,JSONPath=`.spec.deletionPolicy`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MysqlDatabase declares one tenant database on a MysqlFailoverGroup, the
// user that owns it, and zero or more additional grants on it.
//
// The point of this CRD is custody: a caller that holds only
// create/get/list/watch/update/patch/delete on mysqldatabases in one
// namespace can provision a tenant database while holding no MySQL
// credential and no access to any Secret. Bloodraven already has to be the
// MySQL credential authority (see reconcileRole in
// internal/controller/credentials.go), so the admin connection stays where
// it already lives instead of being handed out to every provisioner.
//
// Deliberate limits, each of which is load-bearing:
//
//   - spec.grants[] is grant-only. It never runs CREATE USER. Without that
//     split, "create a database" would imply "create arbitrary MySQL users",
//     and this CRD would be a privilege-escalation primitive rather than a
//     narrowing of one.
//   - Privileges come from a fixed allowlist (see MysqlPrivilege), enforced
//     by the API server and re-checked in Go before any SQL is rendered.
//   - WITH GRANT OPTION is never emitted. An owner that can grant is an
//     owner that can escape its own database.
//   - spec.deletionPolicy defaults to Retain. Dropping a tenant database
//     because a CR was garbage-collected is an unrecoverable data-loss
//     incident.
//   - status carries no credential material, ever. status.ownerUser is the
//     username echoed from the referenced Secret; the password is never
//     read back out of Bloodraven.
type MysqlDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlDatabaseSpec   `json:"spec,omitempty"`
	Status MysqlDatabaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MysqlDatabaseList contains a list of MysqlDatabase.
type MysqlDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlDatabase `json:"items"`
}

// MysqlPrivilege is a single MySQL privilege that a MysqlDatabase may grant
// ON <databaseName>.*. The enum is the allowlist: anything outside it is
// rejected by the API server before it can reach the reconciler, and
// rejected again in Go before it can reach SQL rendering.
//
// The set is deliberately schema/DML-scoped. Notably absent: GRANT OPTION
// (see the MysqlDatabase doc comment), and every *_ADMIN dynamic privilege,
// which are instance-scoped and cannot be granted ON a single schema anyway.
// +kubebuilder:validation:Enum="ALL PRIVILEGES";SELECT;INSERT;UPDATE;DELETE;CREATE;DROP;ALTER;INDEX;REFERENCES;"LOCK TABLES";"SHOW VIEW";TRIGGER;EVENT;EXECUTE
type MysqlPrivilege string

const (
	// PrivilegeAllPrivileges is ALL PRIVILEGES scoped to the one database.
	// It is still schema-scoped: GRANT ALL PRIVILEGES ON `db`.* confers
	// nothing outside `db`, and is emitted without WITH GRANT OPTION.
	PrivilegeAllPrivileges MysqlPrivilege = "ALL PRIVILEGES"
	// PrivilegeSelect is SELECT.
	PrivilegeSelect MysqlPrivilege = "SELECT"
	// PrivilegeInsert is INSERT.
	PrivilegeInsert MysqlPrivilege = "INSERT"
	// PrivilegeUpdate is UPDATE.
	PrivilegeUpdate MysqlPrivilege = "UPDATE"
	// PrivilegeDelete is DELETE.
	PrivilegeDelete MysqlPrivilege = "DELETE"
	// PrivilegeCreate is CREATE.
	PrivilegeCreate MysqlPrivilege = "CREATE"
	// PrivilegeDrop is DROP.
	PrivilegeDrop MysqlPrivilege = "DROP"
	// PrivilegeAlter is ALTER.
	PrivilegeAlter MysqlPrivilege = "ALTER"
	// PrivilegeIndex is INDEX.
	PrivilegeIndex MysqlPrivilege = "INDEX"
	// PrivilegeReferences is REFERENCES.
	PrivilegeReferences MysqlPrivilege = "REFERENCES"
	// PrivilegeLockTables is LOCK TABLES.
	PrivilegeLockTables MysqlPrivilege = "LOCK TABLES"
	// PrivilegeShowView is SHOW VIEW.
	PrivilegeShowView MysqlPrivilege = "SHOW VIEW"
	// PrivilegeTrigger is TRIGGER.
	PrivilegeTrigger MysqlPrivilege = "TRIGGER"
	// PrivilegeEvent is EVENT.
	PrivilegeEvent MysqlPrivilege = "EVENT"
	// PrivilegeExecute is EXECUTE.
	PrivilegeExecute MysqlPrivilege = "EXECUTE"
)

// MysqlDatabaseDeletionPolicy controls what happens in MySQL when the CR is
// deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type MysqlDatabaseDeletionPolicy string

const (
	// MysqlDatabaseRetain leaves MySQL untouched when the CR is deleted.
	// This is the default, because the alternative failure mode —
	// dropping a live tenant database because a CR was garbage-collected
	// by a GitOps prune or a namespace delete — is unrecoverable.
	MysqlDatabaseRetain MysqlDatabaseDeletionPolicy = "Retain"
	// MysqlDatabaseDelete drops the database and the owner user, and
	// revokes the spec.grants[] entries on that database. It never drops
	// a spec.grants[] user; those are shared principals.
	MysqlDatabaseDelete MysqlDatabaseDeletionPolicy = "Delete"
)

// MysqlDatabaseSpec is the desired state of one tenant database.
//
// +kubebuilder:validation:XValidation:rule="!has(self.owner.privileges) || self.owner.privileges.all(p, p in ['ALL PRIVILEGES','SELECT','INSERT','UPDATE','DELETE','CREATE','DROP','ALTER','INDEX','REFERENCES','LOCK TABLES','SHOW VIEW','TRIGGER','EVENT','EXECUTE'])",message="spec.owner.privileges entries must come from the MysqlDatabase privilege allowlist"
// +kubebuilder:validation:XValidation:rule="!has(self.owner.privileges) || self.owner.privileges.size() < 2 || !('ALL PRIVILEGES' in self.owner.privileges)",message="spec.owner.privileges must not combine 'ALL PRIVILEGES' with other privileges"
// +kubebuilder:validation:XValidation:rule="!has(self.grants) || self.grants.all(g, g.privileges.all(p, p in ['ALL PRIVILEGES','SELECT','INSERT','UPDATE','DELETE','CREATE','DROP','ALTER','INDEX','REFERENCES','LOCK TABLES','SHOW VIEW','TRIGGER','EVENT','EXECUTE']))",message="spec.grants[].privileges entries must come from the MysqlDatabase privilege allowlist"
// +kubebuilder:validation:XValidation:rule="!has(self.grants) || self.grants.all(g, g.privileges.size() < 2 || !('ALL PRIVILEGES' in g.privileges))",message="spec.grants[].privileges must not combine 'ALL PRIVILEGES' with other privileges"
// +kubebuilder:validation:XValidation:rule="!has(self.grants) || self.grants.all(g, self.grants.filter(o, o.username == g.username).size() == 1)",message="spec.grants[].username must be unique"
type MysqlDatabaseSpec struct {
	// GroupRef identifies the MysqlFailoverGroup in the same namespace
	// that owns the MySQL instance this database lives on. Cross-namespace
	// and cross-group references are deliberately not expressible.
	GroupRef LocalGroupRef `json:"groupRef"`

	// DatabaseName is the MySQL schema name to create.
	//
	// The pattern is the security contract, not a convenience: the name is
	// interpolated into DDL, so anything outside [A-Za-z0-9_] is rejected
	// by the API server and re-rejected in Go before rendering. Escaping
	// alone is not the contract.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]{1,64}$`
	// +kubebuilder:validation:MaxLength=64
	DatabaseName string `json:"databaseName"`

	// CharacterSet is the database default character set. Defaults to
	// utf8mb4 when empty.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]{1,64}$`
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:default=utf8mb4
	// +optional
	CharacterSet string `json:"characterSet,omitempty"`

	// Collation is the database default collation. Defaults to
	// utf8mb4_unicode_ci when empty.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]{1,64}$`
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:default=utf8mb4_unicode_ci
	// +optional
	Collation string `json:"collation,omitempty"`

	// Owner is the user that owns this database. It is the only principal
	// a MysqlDatabase may bring into existence, and only because the
	// caller supplied its password in a Secret first.
	Owner MysqlDatabaseOwner `json:"owner"`

	// Grants are additional, already-existing principals to grant on this
	// database. Each entry's user must already exist in MySQL; if it does
	// not, the CR fails with reason GrantUserMissing and no user is
	// created. This is how a shared CDC reader is granted onto a tenant
	// schema without giving anyone the right to invent MySQL users.
	// +optional
	// +listType=atomic
	Grants []MysqlDatabaseGrant `json:"grants,omitempty"`

	// DeletionPolicy controls what happens in MySQL when this CR is
	// deleted. Defaults to Retain.
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy MysqlDatabaseDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// MysqlDatabaseOwner declares the owning user of a tenant database.
type MysqlDatabaseOwner struct {
	// SecretName references a Secret in the same namespace with keys
	// `username` and `password` — the same contract as
	// spec.credentials.*Secret on MysqlFailoverGroup. The Secret is
	// desired state for the MySQL user, not a credential to a user that
	// already exists: Bloodraven runs CREATE USER IF NOT EXISTS followed
	// by ALTER USER, so rotating the password is a Secret write and
	// nothing else.
	//
	// Bloodraven never generates, returns, or stores a password.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	SecretName string `json:"secretName"`

	// Privileges granted to the owner ON <databaseName>.*. Defaults to
	// ["ALL PRIVILEGES"], which is schema-scoped and carries no GRANT
	// OPTION.
	// +kubebuilder:default={"ALL PRIVILEGES"}
	// +optional
	// +listType=atomic
	Privileges []MysqlPrivilege `json:"privileges,omitempty"`
}

// MysqlDatabaseGrant grants an already-existing MySQL user on this database.
type MysqlDatabaseGrant struct {
	// Username of an existing MySQL user. The reconciler verifies the user
	// exists and fails the CR if it does not; it never creates the user.
	//
	// As with databaseName, the pattern is a pre-SQL-rendering rejection
	// contract, not a formatting preference.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_][A-Za-z0-9_.$-]{0,31}$`
	// +kubebuilder:validation:MaxLength=32
	Username string `json:"username"`

	// Privileges granted to this user ON <databaseName>.*.
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Privileges []MysqlPrivilege `json:"privileges"`
}

// MysqlDatabasePhase is the lifecycle phase of a MysqlDatabase.
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Failed;Deleting
type MysqlDatabasePhase string

const (
	// MysqlDatabasePhasePending means the CR is accepted but something it
	// depends on is not ready yet: the group is absent or has no active
	// site, the owner Secret has not been rendered, or the primary is
	// fenced by an in-place restore or a planned failover. Pending is not
	// an error — a MysqlDatabase applied before its group is normal
	// ordering.
	MysqlDatabasePhasePending MysqlDatabasePhase = "Pending"
	// MysqlDatabasePhaseCreating means the reconciler is applying DDL.
	MysqlDatabasePhaseCreating MysqlDatabasePhase = "Creating"
	// MysqlDatabasePhaseReady means the database, owner and grants are
	// applied on the current active primary.
	MysqlDatabasePhaseReady MysqlDatabasePhase = "Ready"
	// MysqlDatabasePhaseFailed means reconciliation hit a terminal-shaped
	// problem: an invalid identifier, a privilege outside the allowlist,
	// or a spec.grants[] user that does not exist.
	MysqlDatabasePhaseFailed MysqlDatabasePhase = "Failed"
	// MysqlDatabasePhaseDeleting means the CR is being finalized.
	MysqlDatabasePhaseDeleting MysqlDatabasePhase = "Deleting"
)

// MysqlDatabaseStatus is the observed state of a MysqlDatabase.
//
// No field here ever carries credential material.
type MysqlDatabaseStatus struct {
	// Phase is the lifecycle phase of this database.
	// +optional
	Phase MysqlDatabasePhase `json:"phase,omitempty"`

	// ObservedGeneration is the .metadata.generation this status was
	// computed against. Part of the contract: it is how a caller polling
	// the CR distinguishes "reconciled" from "not looked at yet" without
	// a MySQL connection.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DatabaseCreated records that CREATE DATABASE has been applied at
	// least once for this CR.
	// +optional
	DatabaseCreated bool `json:"databaseCreated,omitempty"`

	// OwnerUser is the username echoed from the referenced Secret. The
	// password is never echoed anywhere.
	// +optional
	OwnerUser string `json:"ownerUser,omitempty"`

	// AppliedGrants lists the usernames that were granted on this database
	// during the most recent successful apply, owner first.
	// +optional
	// +listType=atomic
	AppliedGrants []string `json:"appliedGrants,omitempty"`

	// ActiveSite is the site whose MySQL received the most recent
	// successful apply. Grants replicate, but a MysqlDatabase must not be
	// considered Ready against a stale primary, so this is stamped
	// alongside the hash and re-applied when the group flips.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// LastAppliedHash fingerprints the inputs of the most recent
	// successful apply — spec, the owner Secret's contents, and the active
	// site. Same construction as the credential hash on
	// MysqlFailoverGroup. When it matches, reconciliation issues zero
	// MySQL statements; this is what keeps a tenant-dense cluster from
	// hammering the primary.
	//
	// It is a hash of the Secret's bytes, not the bytes.
	// +optional
	LastAppliedHash string `json:"lastAppliedHash,omitempty"`

	// Message is a human-readable status message.
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions represent the latest available observations. The Ready
	// condition is part of the contract alongside observedGeneration.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlDatabase{}, &MysqlDatabaseList{})
}
