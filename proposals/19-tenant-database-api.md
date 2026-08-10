# Proposal: Tenant Database API (`MysqlDatabase`)

**Status:** Draft
**Branch:** `feat/tenant-database-api`
**Origin:** ShipStream platform [ADR-021](https://github.com/ShipStream/platform) — the platform investigated replacing its standing MySQL admin credential with OpenBao dynamic credentials and found it structurally impossible. This CRD is the alternative that investigation identified.

## Motivation

ShipStream's tenant provisioner runs in the management cluster and must, per tenant, create a database, create an application user with grants scoped to it, and add a `SELECT, DELETE` grant for the Maester CDC user. Today the only way to do that is to connect to the group's MySQL as an admin — which means **the provisioner holds the group's operator credential**: `GRANT ALL PRIVILEGES ON *.* WITH GRANT OPTION`, plus `MYSQL_ROOT_PASSWORD`. Standing, long-lived, and effectively root on every tenant database in every group it provisions into.

The platform's original plan was to replace that with a leased credential from OpenBao's `database` secrets engine. That plan is dead, for reasons that are worth stating because they constrain this design:

1. **OpenBao dials the database.** Its `database` engine opens the connection itself, and configuration fails closed (`verify_connection`) against a host it cannot reach. Production OpenBao is on Railway; every Bloodraven MySQL is Tailscale-private. The engine cannot even be configured, let alone mint.
2. **Bloodraven is already the credential authority.** `reconcileRole` (`internal/controller/credentials.go:167-195`) issues `CREATE USER IF NOT EXISTS … IDENTIFIED BY` plus `ALTER USER … IDENTIFIED BY` from the referenced Secret's bytes. The Secret is *desired state for the MySQL user*, not a credential to a user that already exists. An external engine rotating those same principals would be a second writer with no arbitration.
3. **The operator credential cannot be leased even in principle.** It carries `MYSQL_ROOT_PASSWORD` — the value MySQL is *initialized* with — and `reconcileCredentials` falls back to `root` with it "for initial setup" when the operator user does not yet exist (`credentials.go:70-78`). A credential that must be known before the database exists cannot be minted by something that connects to the database.

The conclusion that matters for Bloodraven: **the component that should hold MySQL admin is Bloodraven, because it already must.** What is missing is a way for a caller to ask Bloodraven to create a tenant database without being handed the keys to do it itself.

That is exactly the shape point 2 already establishes. Bloodraven's credential model is *declare the desired user in a Secret, Bloodraven applies it to MySQL*. This proposal extends that one level up: declare the desired **database** in a CR, Bloodraven applies it. The provisioner's MySQL credential goes away and is replaced by Kubernetes RBAC on a namespaced CRD — a credential the management cluster already holds and already rotates for ArgoCD.

## Goals

1. A namespaced `MysqlDatabase` CRD that declares one database, one owning user, and zero or more additional grants on that database.
2. Reconciliation reuses the existing admin path — `openMySQL` against `mysql-<group>-primary`, operator credential, same `escapeSingleQuotes` discipline — so there is exactly one place in Bloodraven that holds MySQL admin.
3. The owning user's password arrives the same way every other Bloodraven credential does: a `Secret` reference. The caller writes the Secret (in ShipStream's case, ESO renders it from OpenBao); Bloodraven never generates or returns a password.
4. Password rotation for the owning user works through the same mechanism as `spec.credentials` — change the Secret, Bloodraven `ALTER USER`s it.
5. Phase-tracked status so a caller polling the CR can tell "created" from "pending" from "failed", without reading MySQL.
6. **Deletion never drops data by default.** `DROP DATABASE` requires explicit opt-in on the CR, and is off unless asked for.

## Non-goals

- **Replacing `spec.credentials`.** The five group-level roles stay exactly as they are. This is per-tenant databases, a different granularity.
- **Managing schema.** Bloodraven creates the database and the grants; migrations belong to the application.
- **Generating passwords.** Bloodraven does not mint, return, or store credentials. If Bloodraven generated a password it would need somewhere to put it, and that reintroduces the custody problem this proposal exists to remove.
- **Cross-group databases.** One `MysqlDatabase` targets one `MysqlFailoverGroup` in the same namespace, following `LocalGroupRef`.
- **Reading the CR back for secrets.** `status` carries no credential material, ever.

## API

### New CRD: `MysqlDatabase`

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlDatabase
metadata:
  name: tenant-acme
  namespace: bloodraven
spec:
  groupRef:
    name: main                      # MysqlFailoverGroup in the same namespace
  databaseName: acme_wms            # validated: ^[A-Za-z0-9_]{1,64}$
  characterSet: utf8mb4             # optional, default utf8mb4
  collation: utf8mb4_unicode_ci     # optional, default utf8mb4_unicode_ci

  owner:
    # Secret with keys `username` and `password`, same contract as
    # spec.credentials.*Secret on MysqlFailoverGroup.
    secretName: acme-mysql-owner
    # Grants applied to owner ON <databaseName>.*. Default below.
    privileges: [ALL PRIVILEGES]

  # Additional principals granted on this database. The user must already
  # exist -- these are grant-only, never CREATE USER. This is how the Maester
  # CDC grant is expressed without giving anyone the right to invent users.
  grants:
    - username: maester
      privileges: [SELECT, DELETE]

  # Default Retain. Delete drops the database and the owner user.
  deletionPolicy: Retain            # Retain | Delete
```

**`grants[]` is deliberately grant-only.** A `MysqlDatabase` cannot bring a new MySQL user into existence except its own `owner`, whose password must already have been placed in a Secret by the caller. Without that split, "create a database" would imply "create arbitrary users", and the CRD would be a privilege-escalation primitive rather than a narrowing of one.

**`privileges` is an allowlist, not a passthrough string.** Validate each entry against a fixed set (`ALL PRIVILEGES`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `INDEX`, `REFERENCES`, `LOCK TABLES`, `SHOW VIEW`, `TRIGGER`, `EVENT`, `EXECUTE`) via CEL on the CRD **and** in Go before rendering SQL. Never accept `WITH GRANT OPTION` here — an owner that can grant is an owner that can escape its database.

### Status

```yaml
status:
  phase: Ready                      # Pending|Creating|Ready|Failed|Deleting
  observedGeneration: 3
  databaseCreated: true
  ownerUser: acme_app               # echoed from the Secret; NOT the password
  appliedGrants: ["acme_app", "maester"]
  lastAppliedHash: "a1b2c3d4e5f6"   # same construction as credentialHashAnnotation
  message: "database acme_wms ready on site dc1"
  conditions:
    - type: Ready
      status: "True"
      reason: DatabaseReconciled
      lastTransitionTime: "2026-08-09T18:04:11Z"
```

`observedGeneration` and a `Ready` condition are what let the ShipStream provisioner report provisioning phase back to the portal without a MySQL connection — that is the whole point of the CRD, so treat both as part of the contract rather than nice-to-haves.

### RBAC

Two separate additions, and the distinction is the security story:

- **Operator ClusterRole** gains `mysqldatabases` (`get;list;watch;update;patch`) and `mysqldatabases/status` (`get;update;patch`) in `charts/bloodraven/templates/clusterrole.yaml`. It already has `secrets` (`get;list;watch`) and the MySQL admin path.
- **A caller Role**, shipped as an example rather than installed by default: `create;get;list;watch;update;patch;delete` on `mysqldatabases` in one namespace, and nothing else. This is what the ShipStream provisioner would bind to. It confers no ability to read Secrets, no MySQL credential, and no access to `MysqlFailoverGroup`.

## Reconciliation

New controller `internal/controller/mysqldatabase_reconciler.go`, following the shape of `reconcileCredentials`:

1. **Resolve the group.** `spec.groupRef.name` in the same namespace. Missing or not-yet-`Ready` group → `phase: Pending`, requeue. Do not error; a `MysqlDatabase` applied before its group is a normal ordering, not a fault.
2. **Skip if unchanged.** Hash `spec` plus the owner Secret's contents with the same construction as `computeCredentialHash` (`credentials.go:204-224`) and compare against `status.lastAppliedHash`. This is what keeps a tenant-dense cluster from hammering MySQL every reconcile.
3. **Gate on an active site.** `fg.Status.ActiveSite == ""` → `Pending`, requeue. Mirrors `reconcileCredentials:37-40`.
4. **Connect** through the existing helper — `openMySQL(operatorUser, operatorPass, primaryHost, tlsConfigName)` with the same root fallback and the same `mysqlTLSConfig` handling. Reuse the function; do not fork a second connection path.
5. **Apply**, idempotently, in this order:
   - `CREATE DATABASE IF NOT EXISTS <db> CHARACTER SET … COLLATE …`
   - `CREATE USER IF NOT EXISTS '<owner>'@'%' IDENTIFIED BY '<pw>'` then `ALTER USER … IDENTIFIED BY '<pw>'` — the `reconcileRole` pattern verbatim, so rotation works for free.
   - `GRANT <privileges> ON <db>.* TO '<owner>'@'%'`
   - For each `grants[]` entry: verify the user exists (`SELECT 1 FROM mysql.user WHERE user=? AND host='%'`) and **fail the CR** if not, rather than creating it; then `GRANT <privileges> ON <db>.* TO '<user>'@'%'`.
   - `FLUSH PRIVILEGES`
6. **Stamp status** and the hash.

**Run it on the primary only, and re-run after failover.** Grants replicate, but the CR must not be considered `Ready` against a stale primary. Watch `MysqlFailoverGroup` and enqueue every `MysqlDatabase` in the namespace whose `groupRef` matches when `status.activeSite` changes — the same trigger that makes `reconcileCredentials` correct across a flip.

### Deletion

Finalizer `bloodraven.shipstream.io/mysqldatabase`. On delete:

- `deletionPolicy: Retain` (default) — remove the finalizer, leave MySQL untouched, emit `DatabaseRetained`. **This is the default because the failure mode is unrecoverable.** A reconciler that drops a tenant database because a CR was garbage-collected is a data-loss incident, and ShipStream's own ADR-020 makes the same call for the OpenBao side: offboarding is an audited human action.
- `deletionPolicy: Delete` — `DROP DATABASE IF EXISTS <db>`, `DROP USER IF EXISTS '<owner>'@'%'`, revoke `grants[]` entries on that database, then remove the finalizer. Never drop a `grants[]` user; they are shared.

## Interaction with existing controllers

- **`reconcileCredentials` is untouched.** Both paths connect as operator to the same primary; they manage disjoint principals (group roles vs per-tenant owners). Note in the code that these are the only two MySQL-admin call sites, so a future reader knows where to look.
- **Ordering.** A `MysqlDatabase` whose `grants[]` names `maester` requires the `maester` user to exist, which is a group-level concern outside this CRD. Failing loudly with `reason: GrantUserMissing` is correct; do not paper over it.
- **In-place restore and planned failover** both fence the primary. Reconciliation must back off to `Pending` rather than erroring while `status.restoreInPlace.phase` is non-terminal or a planned failover is mid-flight.

## Testing

- Unit: SQL rendering, privilege-allowlist rejection, identifier validation (`databaseName` and usernames must be rejected before reaching `fmt.Sprintf`, not merely escaped), hash stability.
- `test/envtest`: CR lifecycle, finalizer behaviour under both deletion policies, `observedGeneration` and `Ready` condition transitions, `Pending` when the group is absent.
- `test/component`: reconciliation against a real MySQL — create, rotate the owner password through the Secret and prove the new password authenticates and the old one does not, add a `grants[]` entry, delete under both policies.
- Playground scenario: create a `MysqlDatabase`, trigger a planned failover, assert the CR returns to `Ready` against the new primary and the grants are present there.
- Negative: a `grants[]` entry naming a nonexistent user must fail the CR and must **not** create the user.

## Acceptance criteria

1. A caller with only `create` on `mysqldatabases` in one namespace can provision a tenant database, its owner user, and a Maester grant, holding **no** MySQL credential and no access to any Secret.
2. Rotating the owner password is a Secret write and nothing else.
3. Re-applying an unchanged CR performs zero MySQL statements.
4. Deleting a CR with the default policy leaves the database intact.
5. `make generate && make manifests && make vet && make lint && make test` all clean; CRD and RBAC diffs committed.

## Open questions

1. **`host` scoping.** Everything in `credentials.go` hardcodes `'%'`. Tenant owners are arguably the place to start scoping to a pod CIDR, but doing it here alone would be inconsistent. Recommend matching `'%'` for now and raising host scoping as a separate change across both paths.
2. **Should `grants[]` support revocation on removal?** Removing an entry from the array currently leaves the grant in place. Diffing against `status.appliedGrants` and revoking is the obvious behaviour and is probably correct, but it makes a CR edit destructive — worth a deliberate decision rather than falling out of the implementation.
3. **Quotas.** Nothing bounds how many databases one namespace can create. `ResourceQuota` on the CRD count is the Kubernetes-native answer and needs no Bloodraven code; confirm that is sufficient.
4. **Does the ShipStream provisioner need a synchronous path?** It polls, so probably not — but if "database ready" is on the critical path of a signup flow, the `Ready` condition's latency budget is worth stating.
