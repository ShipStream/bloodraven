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
//     narrowing of one. spec.users[] does mint principals, but only under
//     the owner's trust shape — each exists because the caller supplied its
//     credential in a Secret first — so the split survives: no entry of
//     grants[] ever creates a user, and no user is ever created without a
//     caller-held Secret.
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
//
// The Enum marker below is the allowlist enforcement at the API server: it
// is checked structurally, costs nothing against the CRD validation budget,
// and produces a better error message than an equivalent CEL rule. The CEL
// rules on MysqlDatabaseSpec cover only what an enum cannot — composition
// constraints between entries.
// +kubebuilder:validation:Enum="ALL PRIVILEGES";SELECT;INSERT;UPDATE;DELETE;CREATE;DROP;ALTER;INDEX;REFERENCES;"LOCK TABLES";"SHOW VIEW";TRIGGER;EVENT;EXECUTE
// +kubebuilder:validation:MaxLength=32
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
	// MysqlDatabaseDelete drops the database, the owner user and the
	// spec.users[] principals this CR created (per the status.appliedUsers
	// ledger), and revokes the spec.grants[] entries on that database. It
	// never drops a spec.grants[] user; those are shared principals this
	// CRD did not create.
	MysqlDatabaseDelete MysqlDatabaseDeletionPolicy = "Delete"
)

// MysqlDatabaseSpec is the desired state of one tenant database.
//
// Membership in the privilege allowlist is enforced by the MysqlPrivilege
// enum rather than by CEL: it is structural, cheaper, and the API server
// checks it before these rules run. Uniqueness of spec.grants[].username is
// enforced by the list-map key on the field. What is left for CEL is the one
// thing neither can express — that "ALL PRIVILEGES" is not combinable with
// other entries, since GRANT ALL PRIVILEGES, SELECT is not valid SQL.
//
// Every rule here is re-checked in Go (see MysqlDatabaseSpec.Validate)
// because the owner username arrives from a Secret, which the API server
// never sees.
//
// +kubebuilder:validation:XValidation:rule="!has(self.owner.privileges) || self.owner.privileges.size() < 2 || !('ALL PRIVILEGES' in self.owner.privileges)",message="spec.owner.privileges must not combine 'ALL PRIVILEGES' with other privileges"
// +kubebuilder:validation:XValidation:rule="!has(self.grants) || self.grants.all(g, g.privileges.size() < 2 || !('ALL PRIVILEGES' in g.privileges))",message="spec.grants[].privileges must not combine 'ALL PRIVILEGES' with other privileges"
// +kubebuilder:validation:XValidation:rule="!has(self.users) || self.users.all(u, !('ALL PRIVILEGES' in u.privileges))",message="spec.users[].privileges must not include 'ALL PRIVILEGES'; an all-privileges principal is the owner's shape, declare it via spec.owner"
// +kubebuilder:validation:XValidation:rule="!has(self.users) || self.users.all(u, u.secretName != self.owner.secretName)",message="spec.users[].secretName must not reuse spec.owner.secretName; the owner already manages that principal"
type MysqlDatabaseSpec struct {
	// GroupRef identifies the MysqlFailoverGroup in the same namespace
	// that owns the MySQL instance this database lives on. Cross-namespace
	// and cross-group references are deliberately not expressible.
	//
	// The field is immutable for the same reason databaseName is: the
	// reconciler has no way to move a schema between groups, so retargeting
	// would apply the database and owner to the new group, orphan them on
	// the old one, and aim any later deletionPolicy: Delete cleanup at the
	// wrong MySQL. Changing groups is a migration, not a spec edit.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.groupRef is immutable; moving a database between groups is a migration, not a spec edit"
	GroupRef LocalGroupRef `json:"groupRef"`

	// DatabaseName is the MySQL schema name to create.
	//
	// The pattern is the security contract, not a convenience: the name is
	// interpolated into DDL, so anything outside [A-Za-z0-9_] is rejected
	// by the API server and re-rejected in Go before rendering. Escaping
	// alone is not the contract.
	//
	// MySQL's own schemas are rejected as well (case-insensitively): a
	// tenant CR naming `mysql` would grant its owner ALL PRIVILEGES on the
	// grant tables, and `sys` is even droppable. Tenant databases get
	// their own schema; the system schemas belong to the server.
	//
	// The field is immutable because MySQL has no schema rename: editing it
	// would CREATE a second database and orphan the first, and a later
	// deletionPolicy: Delete would drop only the new name. Renaming a
	// tenant database is a migration, not a spec edit.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]{1,64}$`
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.databaseName is immutable; renaming would orphan the existing database"
	// +kubebuilder:validation:XValidation:rule="!(self.lowerAscii() in ['mysql', 'sys', 'information_schema', 'performance_schema'])",message="spec.databaseName must not name a MySQL system schema"
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
	//
	// Keyed by username: the API server rejects duplicate entries without
	// needing a quadratic CEL rule to notice them.
	// +optional
	// +listType=map
	// +listMapKey=username
	// +kubebuilder:validation:MaxItems=64
	Grants []MysqlDatabaseGrant `json:"grants,omitempty"`

	// Users are additional Secret-backed principals this database brings
	// into existence — the same trust shape as the owner, not a loosening
	// of grants[]: each user exists only because the caller supplied its
	// username and password in a same-namespace Secret first. The created
	// account is '%'-hosted, granted ON <databaseName>.* only from the
	// allowlist, and never receives GRANT OPTION or ALL PRIVILEGES. The
	// "grants[] never runs CREATE USER" escalation argument is preserved:
	// users[] is Secret-gated, so it does not reopen it.
	//
	// First consumer: the per-tenant support_ro reader.
	//
	// Keyed by secretName: one entry per Secret, duplicates rejected by
	// the API server.
	// +optional
	// +listType=map
	// +listMapKey=secretName
	// +kubebuilder:validation:MaxItems=8
	Users []MysqlDatabaseUser `json:"users,omitempty"`

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
	//
	// An explicit empty list is rejected (MinItems=1): it would otherwise
	// resolve to the ALL PRIVILEGES default, turning an expressed
	// revoke-all intent into a full-privilege owner. Absent is the only
	// way to ask for the default.
	// +kubebuilder:default={"ALL PRIVILEGES"}
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=15
	Privileges []MysqlPrivilege `json:"privileges,omitempty"`
}

// MysqlDatabaseUser declares one additional Secret-backed principal on this
// database. See MysqlDatabaseSpec.Users for the trust shape.
type MysqlDatabaseUser struct {
	// SecretName references a Secret in the same namespace with keys
	// `username` and `password` — the identical contract to
	// spec.owner.secretName. The Secret is desired state for the MySQL
	// account: Bloodraven runs CREATE USER IF NOT EXISTS followed by ALTER
	// USER, so rotating the password is a Secret write and nothing else.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	SecretName string `json:"secretName"`

	// Privileges granted to this user ON <databaseName>.*. Required, no
	// default, and ALL PRIVILEGES is rejected (by CEL and again in Go): a
	// second all-privileges principal is a shadow owner, and the first
	// consumer of users[] is a SELECT-only reader. Widening the allowlist
	// later is compatible; narrowing it would not be.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=15
	// +listType=atomic
	Privileges []MysqlPrivilege `json:"privileges"`

	// ResourceLimits caps this account's server-side resource usage.
	// Omitted limits render as 0 — MySQL's "no account-level cap" — on
	// every apply, so removing a limit from the spec actually clears it in
	// MySQL.
	// +optional
	ResourceLimits *MysqlUserResourceLimits `json:"resourceLimits,omitempty"`
}

// MysqlUserResourceLimits are per-account MySQL resource limits, applied via
// ALTER USER ... WITH. Typed integers on purpose (prior art considered and
// rejected: bitpoke's corev1.ResourceList, an unconstrained string-quantity
// map whose keys are escaped into SQL): the API server validates the range,
// and rendering never interpolates caller-controlled text.
type MysqlUserResourceLimits struct {
	// MaxUserConnections is MAX_USER_CONNECTIONS: the number of
	// simultaneous connections the account may hold. 0 (or omitted) sets no
	// account-level cap, which defers to the server's global
	// max_user_connections; the account is only truly unlimited when that
	// global is also 0.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxUserConnections int32 `json:"maxUserConnections,omitempty"`

	// MaxQueriesPerHour is MAX_QUERIES_PER_HOUR. 0 (or omitted) means
	// unlimited.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxQueriesPerHour int32 `json:"maxQueriesPerHour,omitempty"`
}

// MysqlDatabaseGrant grants an already-existing MySQL user on this database.
type MysqlDatabaseGrant struct {
	// Username of an existing MySQL user, matched at host '%' only —
	// the same host every account in credentials.go and this CRD is
	// created at. A principal that exists only at another host does not
	// satisfy the existence check and fails the CR with GrantUserMissing.
	//
	// As with databaseName, the pattern is a pre-SQL-rendering rejection
	// contract, not a formatting preference.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_][A-Za-z0-9_.$-]{0,31}$`
	// +kubebuilder:validation:MaxLength=32
	Username string `json:"username"`

	// Privileges granted to this user ON <databaseName>.*.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=15
	// +listType=atomic
	Privileges []MysqlPrivilege `json:"privileges"`
}

// MysqlDatabaseUserState is one spec.users[] ledger entry — the users[]
// generalization of status.ownerUser plus status.pendingOwnerUser, keyed by
// the Secret that carries the credential.
type MysqlDatabaseUserState struct {
	// SecretName is the spec.users[] entry this record belongs to. It
	// outlives the spec entry: removal-driven cleanup runs off this
	// record, then deletes it.
	SecretName string `json:"secretName"`

	// Username is the account name echoed from the Secret, recorded
	// before the entry's first CREATE USER executes. During a username
	// rotation it keeps the previous name until the old account has
	// actually been dropped, exactly like status.ownerUser.
	// +optional
	Username string `json:"username,omitempty"`

	// PendingUsername is the write-ahead record of an in-flight username
	// rotation for this entry, committed before any rotation SQL runs and
	// cleared by the successful Ready stamp — the per-entry analog of
	// status.pendingOwnerUser, and required for the same reason: a
	// rotation that creates the new account and then fails to persist
	// status must not wedge the next reconcile's adoption gate on an
	// account this CR itself created.
	// +optional
	PendingUsername string `json:"pendingUsername,omitempty"`
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

	// DatabaseCreated is the write-ahead record for cleanup: it is stamped
	// once the admin connection is open and the reconciler commits to
	// executing DDL for this CR, before the first statement runs.
	// deletionPolicy: Delete drops MySQL objects only when it is set — a
	// CR that failed before any SQL (invalid spec, reserved owner,
	// ownership conflict, unreachable primary) must not drop a database
	// something else created under the same name.
	// +optional
	DatabaseCreated bool `json:"databaseCreated,omitempty"`

	// OwnerUser is the username echoed from the referenced Secret,
	// recorded once the admin connection is open so cleanup covers partial
	// applies. During a username rotation it keeps the previous name until
	// the old account has actually been dropped. The password is never
	// echoed anywhere.
	// +optional
	OwnerUser string `json:"ownerUser,omitempty"`

	// PendingOwnerUser is the write-ahead record of an in-flight username
	// rotation: the new owner's name, committed before any rotation SQL
	// runs and cleared by the successful Ready stamp. It exists because a
	// rotation can create the new account, drop the old one, and then fail
	// to persist status: without the record, the next reconcile's adoption
	// gate would see an account it cannot attribute and wedge the CR on
	// PreExistingOwnerUser for a user it created itself. Deletion treats
	// it as an owner candidate for the same reason.
	// +optional
	PendingOwnerUser string `json:"pendingOwnerUser,omitempty"`

	// AppliedGrants lists the usernames granted on this database during
	// the most recent successful apply, owner first. It is the input to
	// revocation: an entry removed from spec.grants is revoked on the
	// next apply, and deletion revokes the union of the current list and
	// this record. spec.users[] principals are tracked separately in
	// AppliedUsers, which pairs each username with its Secret.
	// +optional
	// +listType=atomic
	AppliedGrants []string `json:"appliedGrants,omitempty"`

	// AppliedUsers is the write-ahead ledger for spec.users[]: one entry
	// per Secret this CR has committed to managing a principal for,
	// stamped before the first CREATE USER runs. The ledger — not the
	// Secret — is what authorizes later revocation and drop: a users[]
	// entry can be removed after its Secret is already gone (ESO
	// de-render, offboarding ordering), and the pairing of secretName to
	// username is the only durable record of which account that entry
	// created. Entries whose secretName has left the spec are dropped from
	// MySQL and then removed here. Usernames only; never any credential
	// material.
	// +optional
	// +listType=map
	// +listMapKey=secretName
	AppliedUsers []MysqlDatabaseUserState `json:"appliedUsers,omitempty"`

	// ActiveSite is the site whose MySQL received the most recent
	// successful apply. Grants replicate, but a MysqlDatabase must not be
	// considered Ready against a stale primary, so this is stamped
	// alongside the hash and re-applied when the group flips.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// LastAppliedHash fingerprints the inputs of the most recent
	// successful apply — spec, the owner Secret's revision, the active
	// site, and the group's identity. When it matches, reconciliation
	// issues zero MySQL statements; this is what keeps a tenant-dense
	// cluster from hammering the primary.
	//
	// The Secret contributes its UID and resourceVersion, never a digest
	// of its bytes: status is caller-readable, and a content digest would
	// let a status reader offline-check password guesses against it.
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
