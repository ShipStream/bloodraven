# Proposal: API audit and cleanup before `shipstream.io/v1beta1`

**Status:** Draft, revision 3 (after critique rounds 1 and 2; see section 8.1)
**Branch:** `docs/api-v1beta1-audit`
**Tracking:** #237
**Audited at:** `3fa1900` (origin/main after #232). **Re-checked at** `2340f6b` (this branch with origin/main
`7c3626d` merged in, after #231, #233 and #234). Every `file:line` below refers to `2340f6b`. Citations that
moved were re-mapped line by line, and citations whose content changed were re-opened by hand. The diff of
`api/`, `config/crd`, `site/`, `.agents/` and the Event reasons since `3fa1900` was triaged against sections
1-4 (B.4).
**Appendix:** [`20-api-v1beta1-inventory.md`](20-api-v1beta1-inventory.md) (generated field inventory)

## Summary

This audits every served field, condition, phase, Event reason, annotation and label of the five kinds
before the API is frozen as `shipstream.io/v1beta1`. There are zero production users, so breaking changes
land in v1alpha1 now (D2), with no shims and no legacy adoption (D3). v1beta1 then ships with the identical
schema and conversion strategy `None` (D1). The release is v1.7.0 (D7).

v1.7.0 is a minor release that breaks v1alpha1 and has no in-place upgrade path. A user who reads only the
version number would expect a routine upgrade. The release notes therefore open with a prominent breaking-change
callout, before anything else: there is no in-place upgrade from v1.6.0, do not apply the v1.7.0 CRDs to a v1.6.0
cluster, and follow the documented new-group procedure (C-13).

A v1.6.0 group moves to v1.7.0 as a new group restored from a backup. Section 3 says why, and lists the
requirements that WS-F's written and tested procedure must meet (3.2). The upgrade-E2E baseline is therefore a
proposed amendment to D6 that needs sign-off (C-8).

**Counts**

| | Count |
|---|---|
| Raw findings from five audits (GPT 46, MFGSPEC 38, STATUS 33, KINDS 34, OPS 21) | 172 |
| Merged findings | 115 (A-01 to A-115; A-114 and A-115 added in revision 2) |
| must-fix | 49 (one, A-83, already done on main) |
| should-fix | 50 |
| leave | 16 |
| Breaking findings (one of them, A-50, owned by #235) | 81 (revision 3 adds A-33) |
| Migration-table rows | 106 (revision 3 adds five: A-33 and four served-surface rows) |
| Raw findings dropped entirely / partially rejected | 1 / 22 |
| Code and docs defects (Appendix A) | 15 |
| Critique points, round 1 (GPT 19, Opus 28) | 47: all verified; none rejected as wrong; 9 suggested fixes replaced by another fix (section 8.1) |
| Critique points, round 2 (GPT 9: 4 Major, 5 Minor; Opus 14: 4 Major, 10 Minor) | 23: all verified and applied; none rejected; one citation corrected and 4 suggested fixes, or parts of them, not taken (section 8.1) |
| Open questions | 6, all settled by MBOD (section 7) |

### How to read this

- **Section 2** is the decision list. Each finding names its severity, whether it is breaking, the raw findings
  it merges, its evidence at `2340f6b`, and the exact new shape.
- Findings that touch a pending redesign say **Owned by #N** and state the shape this cleanup expects that
  issue to leave. They do not plan the change. The redesigns are #235 (planned-failover claim), #206 (PITR
  archive identity), #203 (encryption identity), #205 (in-place PITR, additive) and #204 (reclone release,
  deferred).
- **Section 3** is the field-by-field migration table that goes into the v1.7.0 release notes. It also says
  why there is no in-place upgrade from v1.6.0, and lists the requirements for the new-group procedure that
  replaces one. WS-F writes and tests that procedure.
- **Section 4** lists every hand-written status path and its replacement.
- **Section 5** is the v1beta1 checklist rewritten for D1.
- **Section 6** is the ordered WS-E commit list.
- **Sections 7 and 8** record the decisions and the alternatives rejected.
- Kind abbreviations: MFG MysqlFailoverGroup, MBK MysqlBackup, MBV MysqlBackupVerification, MDB MysqlDatabase,
  MSC MysqlStandbyCluster.
- Raw finding IDs (`GPT-nn`, `MFGSPEC-nn`, `STATUS-nn`, `KINDS-nn`, `OPS-nn`) name the five working audits this
  proposal merges. Those reports are not published. Each finding here carries their evidence, re-checked, so
  this document is the record.
- Paths are relative to the repo root. Controller paths omit `internal/controller/` after their first use in a
  finding, and API paths omit `api/v1alpha1/`.

## 1. Inventory summary

The field-level inventory is generated from the controller-gen CRDs, not copied by hand:
[`20-api-v1beta1-inventory.md`](20-api-v1beta1-inventory.md). Regenerate it from the repo root (needs `yq` v4):

```sh
python3 -I proposals/tools/crd-inventory.py config/crd/bases > proposals/20-api-v1beta1-inventory.md
```

Embedded upstream types (containers, resources, security contexts, conditions) count as one field each.

### 1.1 Per-kind counts at `2340f6b`

| Kind | Spec fields | Status fields | CEL rules (transition) | Condition types | Phase enums | Printer columns | shortNames | categories |
|---|---|---|---|---|---|---|---|---|
| MysqlFailoverGroup (MFG) | 267 | 192 (#233 added `restoreReclones.fencedGtidExecuted`) | 20 (0) | 9 | 8 (`updatePhase` untyped) | 3 | `mfg`, `mfgs` | bloodraven, mysql |
| MysqlBackup (MBK) | 5 | 26 | 1 (1, `status.artifactStorage`, #232) | 4 | 1 | 8 | none | none |
| MysqlBackupVerification (MBV) | 21 | 25 | 0 (0) | 2 | 1 (2 of 7 values never written) | 7 | none | none |
| MysqlDatabase (MDB) | 23 | 18 | 7 (2) | 1 | 1 | 7 | `mydb`, `mydbs` | bloodraven, mysql |
| MysqlStandbyCluster (MSC) | 29 (template collapsed) | 41 | 24 (0), of which 20 come from the embedded MFG spec | 2 written, 3 declared and never written | 1 (never written) | 5 | `msc`, `mscs` | bloodraven, mysql, dr |

Notes that drive findings:

- `spec` is `omitempty` on all five kinds (`api/v1alpha1/types.go:21`, `mysqlbackup_types.go:26`,
  `mysqlbackupverification_types.go:37`, `mysqldatabase_types.go:53`, `mysqlstandbycluster_types.go:41`).
- Only MDB declares conditions as a list-map (`mysqldatabase_types.go:620-622`).
- MFG has no transition rule at all. The only transition rules are MDB's two identity rules
  (`mysqldatabase_types.go:191,219`) and MBK's root rule from #232 (`mysqlbackup_types.go:17`).
- Compact JSON sizes: MFG CRD 276 KB; MSC CRD 257 KB, of which 237 KB is the embedded `spec.template`. D1 serves
  two identical versions, which doubles each. C-4 adds a size budget (OPUS-27).

### 1.2 Condition and phase vocabulary

| Kind | Condition type | Status values | Reasons (current) | Set at |
|---|---|---|---|---|
| MFG | `Ready` | T/F/U | `TopologyPolled` (both T and F), `Initializing` (U) | `internal/controller/runner.go:1484-1508` |
| MFG | `Degraded` | F/T/U | `Healthy`, `Initializing`, `Degraded`, `TotalLoss`, `SplitBrain`, `NoPrimary`, `Alert`, plus overlays `ReplicationSourceMismatch`, `ReplicationBroken`, `ReplicationLagging`, `ReplicationError`, `DivergentTransactions`, `RestoreReclonePending` (new in #233, `runner.go:1599`) | `runner.go:1511-1617` |
| MFG | `DivergentPromotion` | T only | `DivergentSitePromoted` (never cleared by the operator) | `runner.go:1637` |
| MFG | `Bootstrapping` | T/F | `Cloning`, `WaitingForRestart`, `SetupReplication` (T); `NotBootstrapping`, `Done` (F); `Failed` (T) | `runner.go:2378-2414` |
| MFG | `Updating` | T/F | the `UpdatePhase` values (T); `OrderedUpdateStalled`, `NotUpdating` (F) | `runner.go:1652-1675` |
| MFG | `RecoveryPending` | T/F | `RecoveryInProgress`, `DivergentTransactions`, `NoRecoveryPending` | `runner.go:1682-1710` |
| MFG | `EncryptionAtRestReady` | T/F | `AllSitesSealed`, `KeyringFailed`, `KeyringNotSealed`, `AdoptionRefused` | `encryption_reconcile.go:1027`, `:1136-1139` |
| MFG | `S3CredentialsReady` | T/F/absent | `CredentialsComplete`, `CredentialsIncomplete` | `s3_credentials_check.go` |
| MFG | `PITRArchiveReady` | T/F/U/absent | `Archiving`, `ArchiveStoreInitFailed`, `ArchiverError`, `ArchiverDisabled`, `SidecarUnreachable`, `AwaitingFirstPoll`, `AwaitingRollout` | `pitr_archive_condition.go` |
| MBK | `JobCreated`, `Ready`, `MetadataComplete` | T/F (`Ready` absent while Running) | `Succeeded`, `Failed`, `GroupNotFound`, `ProfileNotFound`, `NoHealthySource` (both Pending and terminal), `BuildJobFailed`, `DestinationUnavailable`, `Collected`, `Sentinel*`, `LocationMismatch` | `backup_reconciler.go`, `backup_completion_metadata.go` |
| MBK | `ArtifactCleanup` | F/absent | `CleanupPending`, `CleanupFailed`, `CleanupRejected`, `DestinationUnverified`, `DestinationMismatch`, `CredentialsUnavailable`, `OperatorImageUnset`, `CleanupJobConflict`, `HeldByVerification`, `ForegroundDeletion` | `backup_cleanup.go:19-32`, `backup_reconciler.go:590-596` |
| MBV | `Verified`, `ResourcesCleanedUp` | T/F | `Succeeded`, `Failed`, `SanityCheck*`, `PITR*` (8 values), `GroupNotFound`, `ProfileNotFound`, `BlockedByActiveVerification`, `BackupNotAvailable`, `BuildJobFailed`, `BackupIdentityMismatch`, `CleanedUp` | `backup_verification_reconciler.go` |
| MDB | `Ready` | T/F | `DatabaseReconciled`, `Applying`, `Deleting`, 13 Pending reasons, 11 Failed reasons | `mysqldatabase_reconciler.go` |
| MSC | `BucketReadable`, `SourceConfigKnown` | T/F | `ListSucceeded`, `DumpFound`, `ConfigError`, `AuthFailed`, `ScanIncomplete`, `ListFailed`, `MetadataUnreadable`, `NoDumpFound`, `NoBinlogManifests` | `standbycluster_reconciler.go` |
| MSC | `Restorable`, `ActivationInProgress`, `Active` | none | constants only, never written | `api/v1alpha1/mysqlstandbycluster_types.go` |

| Phase enum | Field | Values | Typed | Finding |
|---|---|---|---|---|
| `BackupPhase` | MBK `status.phase`, MFG `status.restore.phase` | `Pending;Running;Succeeded;Failed` | yes | — |
| `VerificationPhase` | MBV `status.phase` | `Pending;Provisioning;Restoring;Checking;Cleaning;Succeeded;Failed` | yes | A-31 |
| `MysqlDatabasePhase` | MDB `status.phase` | `Pending;Creating;Ready;Failed;Deleting` | yes | A-32 |
| `StandbyActivationPhase` | MSC `status.activation.phase` | `"";Validating;Restoring;Replaying;Provisioning;Activated;Failed` | yes, never written | A-61 |
| `PlannedFailoverPhase` | MFG `status.plannedFailover.phase` | 12 values | yes | A-50 (#235) |
| `RestoreInPlacePhase` | MFG `status.restoreInPlace.phase` | `"";Preflight;Fencing;Restoring;Resuming;Succeeded;Failed` | yes | — |
| `RestoreReclonesPhase` | MFG `status.restoreReclones.phase` | `Armed;Draining;Held` | yes, admin-written in runbooks | A-85 |
| `DragonflyPhase`, `DragonflyUpgradePhase` | MFG `status.dragonfly.phase`, `.upgrade.phase` | CamelCase | yes | — |
| `SiteKeyringPhase` | MFG `status.encryptionAtRest.sites[].phase` | `"";Pending;Unsealed;Escrowed;Sealed;Failed` | yes, admin-written in a runbook | A-83 |
| `DragonflyRole` | MFG `status.dragonfly.sites[].role` | `unknown;master;replica;stale-master;unconfigured;unreachable` | yes, lowercase | A-28 |
| (untyped) | MFG `status.sites[].state` | `writable;read-only;unreachable;unknown` | enum marker on `string` | A-28 |
| (untyped) | MFG `status.updatePhase` | Go type in `internal/controller/updater.go:15-25`, includes `Complete` | no enum | A-29 |
| (untyped) | MFG `status.backupSchedules[].lastBackupPhase` | `BackupPhase` values | no | A-29 |
| (untyped) | MFG `status.restoreInPlace.scope` | `full`, `schema:<name>` | no | A-30 |

### 1.3 Annotations, labels, finalizers and taints that act as API

Writers: U = user, P = kubectl plugin, O = operator, C = chaos runner and playground.

| Key | On | Writer → reader | Value grammar | Evidence | Finding |
|---|---|---|---|---|---|
| `bloodraven.shipstream.io/reclone-site` | MFG | U, P, O → O (consumed) | `<site>:confirm=<group>` or `<site>:<gtid-prefix>` | `internal/controller/reconciler.go:90`, `reclone.go:15-21,97,118` | A-78 |
| `bloodraven.shipstream.io/planned-failover` | MFG | U, P, O (re-writes it on deferral) → O | `<site>[:maxLagWait=<dur>]` | `planned_failover.go:32`, `planned_failover_deployment.go:33-48` | A-50 (#235) |
| `bloodraven.shipstream.io/rotate-keyring` | MFG | U → O | `<site>` | `encryption_escrow.go:46` | A-81 |
| `bloodraven.shipstream.io/dragonfly-snapshot-upgrade` | MFG | U → O, and the operator then writes `spec.dragonfly.image` | image reference | `dragonfly_upgrade.go:22,401-402` | A-48 |
| `bloodraven.shipstream.io/revoke-deployment-leases` | MFG | U → O | presence | `deployment_leases.go:32` | A-79, A-81 |
| `bloodraven.shipstream.io/encryption-adopt` | MFG | U → O, never cleared | literal `confirm` | `encryption_escrow.go:58` | A-51 |
| `bloodraven.shipstream.io/last-failover`, `…/last-failover-target` | MFG | O → O | RFC 3339 seconds; site | `failover_state.go:40-41` | A-104 (leave) |
| `bloodraven.shipstream.io/restore-confirm` | Job | O → O | confirm token | `restore_inplace.go:133` | A-79 (Internal) |
| `bloodraven.shipstream.io/deployment-lease` | Lease | O → O | JSON | `deployment_leases.go:30` | A-80 |
| `shipstream.io/failover-group`, `/site`, `/role`, `/healthy` (`yes`/`no`) | owned objects, Pods | O → O, U (selectors) | names; `primary`/`replica`; `yes`/`no` | `reconciler.go:54-57` | A-77 |
| `shipstream.io/dragonfly-role`, `/dragonfly-traffic` (`enabled`) | Dragonfly Pods | O → O | `master`/`replica`; `enabled` | `dragonfly_resources.go:35,46` | A-77 |
| `shipstream.io/spec-hash`, `/managed-service-annotations`, `/credential-hash`, `/system-schema-restriction` | Deployments, Services, Secrets | O → O | hash, JSON | `reconciler.go:61-62`, `credentials.go:24,30` | A-79 (Internal) |
| `shipstream.io/keyring-version` (label), `/keyring-digest` | escrow Secret | O, and U in a runbook → O | int; `sha256:…` | `encryption_escrow.go:27,32` | A-84 |
| `shipstream.io/mysqlbackup`, `/backup-profile`, `/backup-schedule`, `/resource`, `/backup-encrypted` | Job, CronJob, PVC, MBK | O → O (the schedule rollup reads `backup-schedule`) | names, bool | `backup.go:19-23` | A-106 (leave) |
| `shipstream.io/backup-retry-of`, `/backup-retry-root`, `/backup-retry-root-uid`, `/backup-attempt` | MBK | O → O | UID, name, int | `backup_retry.go:12-15` | A-106 (leave) |
| `shipstream.io/cleanup-failure-recorded` (new in #232) | MBK | O → O | marker | `backup_cleanup.go:42` | A-79 (Internal) |
| `shipstream.io/encryption-status-fence` (new in #234, missed in revision 2) | MFG | O → nobody reads the value | opaque RFC 3339 timestamp; the write is what matters: it advances `resourceVersion`, so any status write conditioned on an older version, such as one whose response was lost, can no longer commit | `encryption_reconcile.go:292-307` | A-79 (Internal); E1 exports the constant, E14 moves it to `bloodraven.shipstream.io/` |
| `hold.verification.shipstream.io/<verification UID>` | MBK | O → O (retention, the artifact-cleanup finalizer and the PITR binlog cutoff honour it); doctor `triage.sh`, `support-bundle.sh` read it | verification name | `backup_verification_hold.go:29`, `cmd/bloodraven/pitr_retention.go:53`, `.agents/skills/bloodraven-doctor/scripts/support-bundle.sh:110` | A-77 (hold family), A-79 (Operator-owned) |
| `bloodraven.shipstream.io/deployment-kind`, `…/deployment-state` (labels) | Lease | O → O (lease selection) | `failover-hold`; `active` | `deployment_leases.go:154-155,226-227` | A-79 (Internal) |
| `shipstream.io/mysqlbackup-verification`, `/verification-schedule`, `/verification-backup` | Job, CronJob | O → O | names; JSON | `backup_verification.go:14-15`, `backup_verification_source.go:17` | A-80 |
| Finalizers `shipstream.io/graceful-shutdown`, `/mysqlbackup`, `/mysqlbackup-verification`, `/mysqldatabase` | MFG, MBK, MBV, MDB | O | — | `reconciler.go:48`, `backup.go:28`, `backup_verification.go:17`, `mysqldatabase_reconciler.go:44` | A-77 |
| Taint `shipstream.io/db-readonly-<group>=true:NoExecute` (plus legacy `shipstream.io/db-readonly`) | Node | O → U (tolerations) | — | `internal/platform/tainter.go:19-20,85` | A-21 |
| `shipstream.io/mysql-client=true` (namespace), `bloodraven.shipstream.io/deploy-client=true` (Pod) | user objects | U → chart NetworkPolicy | `"true"` | `charts/bloodraven/templates/networkpolicy-deploy-api.yaml` | A-77 |
| `chaos.playground.bloodraven.io/*`, `playground.bloodraven.io/purpose` | MFG, test objects | C → C | JSON marker | `internal/playground/kube/marker.go:17` | A-77 |

The plugin writes only `planned-failover` and `reclone-site`, and copies both key strings with a "must match"
comment (`cmd/kubectl-bloodraven/promote.go:21`, `reclone.go:18`).

### 1.4 Event reasons that runbooks, alerts, the doctor skill and the chaos runner key on

The code emitted 158 distinct reasons at `3fa1900`, all on the five CRs. #231, #233 and #234 added six more:
`TopologyManagerStarted`, `TopologyManagerStartFailed`, `BootstrapRefusedRestoreHold`,
`RestoreInPlaceWaitingForPreviousQueue`, `RestorePeerReplicationStopped` and `RestorePeerReplicationIncomplete`. `8.observability/2.monitoring.md` documents 67 of
them, including two reasons nothing emits (`BackupScheduleServiceAccountMissing`, `BackupPITRNotImplemented`).
No PrometheusRule keys on Events. These are the reasons consumers depend on:

| Consumer | Reasons |
|---|---|
| Chaos runner assertions (`internal/playground/scenarios/mfg_event.go`) | `RecloneRequested`, `RecloneRejected` (s19), `KeyringPromotionRefused` (s52), `KeyringRotationRefused` (s48), `DeploymentLeaseRevoked` (s53) |
| Doctor `SKILL.md` and `alert_runbook_matrix.md` | `DataLossDetected`, `DivergentReplicaDetected`, `DivergentSitePromoted`, `OrderedUpdateStalled`, `NoPrimaryDetected`, `PromotionRefusedPendingReclone`, `PITRArchiveNotReady`, `PITRSharedArchivePrefix`, `PointInTimePartial`, `PointInTimeCrossSiteUnverified`, `DeploymentLeaseRevoked`, `PlannedFailoverDeferred`, `S3CredentialsIncomplete`, `CredentialReconcileFailed`, `KeyringEscrowMissing`, `KeyringEscrowCorrupt`, `DragonflySessionsLost`, `DragonflyPromotionFailed`, `DragonflyPromotionWaiting`, `BackupRetryConflict`, `BackupMetadataIncomplete`, `EncryptionAttributionCorrected`, `BlockedByActiveVerification`, `BackupNotAvailable`, `BackupIdentityMismatch`, `RestoreFailed`, `RestoreInPlaceRejected`, `RestoreInPlaceSpecRemoved`, `RestoreInPlaceCancelled`, `RestoreInPlaceJobLost`, `RestoreReclone*`, `RestoreOfflineFence*`, `PartialRevokesOff`, `TenantPrivilegeRestriction`, MDB `fail()` reasons |
| Log-schema contract (`8.observability/7.log-schema.md`) | failover, divergence, keyring, Dragonfly, license and PITR reasons listed in its Event reference |
| Status fields that reuse Event reason tokens | `status.restore.reason` (`PointInTimePartial`, `PointInTimeCrossSiteUnverified`), `status.plannedFailover.dragonfly.reason` (`DragonflySyncTimeout`, `DragonflyPromotionFailed`) |
| Deploy API `unstableReason` | raw `Degraded`, `RecoveryPending` and `Bootstrapping` condition reasons (`DeploymentUnstableReason`, `internal/controller/deployment_leases.go:270`) |

The STATUS audit's full reason table is not published. A-76 replaces the hand-maintained registry with a
generated one, which then becomes the full table.

### 1.5 Served contracts outside the CRD schema that carry API tokens

These surfaces repeat enum values, reasons or status fields verbatim, so renaming a token or a field changes them
too. Revision 2 listed the first two; revision 3 adds the other three (GPT-R2-04, OPUS-R2-10). A-114 decides the
spelling of each.

| Surface | Tokens it carries | Evidence | Consumers |
|---|---|---|---|
| Prometheus labels `role` on `bloodraven_site_state`, `bloodraven_replication_lag_seconds` and `bloodraven_replication_running` | `string(site.role)`: `primary-candidate`, `dr-only`, `read-only`; and label `state` on `bloodraven_site_state`: `writable`, `read-only`, `unreachable`, `unknown` | `internal/controller/topology.go:315`, `internal/metrics/metrics.go:132-135` | PromQL in `8.observability/3.monitoring-prometheus.md:112,118,139,144` and `2.monitoring.md:223` (`state="writable"`), `examples/prometheusrule.yaml`, user alert rules |
| Prometheus `reason` labels on `bloodraven_planned_failovers_deferred_total`, `bloodraven_deploy_lease_revocations_total` and `bloodraven_backup_artifact_cleanup_failures_total` | reason tokens, already CamelCase: `DeploymentHold`, the lease revocation reasons, `CleanupFailed`, `CleanupRejected` | `internal/metrics/metrics.go:51-65,366-372`; written at `planned_failover_deployment.go:60`, `deployment_leases.go:263`, `backup_reconciler.go:809` | user alert rules and dashboards |
| `/deploy/v1` status JSON | `health.degradedReason`, `health.replicationRunning` (computed from `status.sites[].replicating`, which A-43 removes), `operations.updatePhase`, `operations.plannedFailover.{phase, reason}`, `operations.restoreInPlace`, `operations.dragonflyUpgrade`, `unstableReason` | `cmd/bloodraven/deploy_api.go:337-362`, `deployment_leases.go:270` | CI pipelines that gate deploys on the API (`4.configuration/11.deployment-api.md`) |
| Operator `GET /status` JSON | per site `role` (`string(role)`, so the API spec value), `state` (`state.String()`, the internal lowercase form), `sourceConvergenceState`, `sourceConvergenceReason`, `recoveryState` | `cmd/bloodraven/main.go:490`; `internal/controller/topology.go:1124-1140` | `8.observability/2.monitoring.md:543-590` (documented with `"state": "writable"`), user scripts |
| Operator `GET /ws/status` WebSocket broadcast | `TopologyMessage`: `activeSite`, `lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` (the flat record A-40 restructures), `updatePhase`, `alert`; per site `role`, `state`, `replicating` (the bool A-43 replaces), `recoveryState`, `divergentGtid` | `cmd/bloodraven/main.go:608`; `internal/controller/topology.go:2136-2140`; `internal/platform/websocket.go:19-44` | the playground dashboard, which compares `state === 'writable'` and `role === 'primary-candidate'` and uses the state as a CSS class (`playground/dashboard/index.html:17-18,131-150`); `2.monitoring.md:593-651` |

A-27 changes `role` on both operator endpoints automatically, because they serialize the API value. A-28 does not
change `state`, because they serialize the internal string. Without an explicit decision the two fields of one
site entry would use different vocabularies (GPT-R2-04).

## 2. Findings

Every finding has the same fields:

- **Sev**: `must` (painful or impossible to change after v1beta1), `should` (consistency or ergonomics worth
  doing in the same breaking release), or `leave` (considered and kept on purpose).
- **Brk**: breaking yes or no.
- **Merges**: the raw finding IDs from the five working audits (GPT, MFGSPEC, STATUS, KINDS, OPS).
- **Owned by #N**, where the finding touches the territory of a pending redesign.

Paths are repo-relative. "Status-only" means only the operator writes the field, so no manifest changes. It
does not mean the operator converts a stored v1.6.0 status: nothing does. A v1.7.0 group starts with an empty
status (section 3), and section 3 classifies what that drops.

Most MFG spec changes also apply to MSC `spec.template.spec`, which embeds `MysqlFailoverGroupSpec`
(`api/v1alpha1/mysqlstandbycluster_types.go:161`), unless A-62 removes the template first. A-62 is ordered
first in WS-E so the template stops carrying them.

### 2.1 Cross-kind references and secrets

#### A-01 One name for the failover-group reference

- **Sev** must · **Brk** yes · **Merges** GPT-01 (part), KINDS-02
- **Evidence:**
  - MBK and MBV use `failoverGroupRef` (`api/v1alpha1/mysqlbackup_types.go:43`,
    `mysqlbackupverification_types.go:54`), but MDB uses `groupRef` (`mysqldatabase_types.go:192`). It is the
    same `LocalGroupRef` type.
  - `LocalGroupRef.name` has only `MinLength=1` (`mysqlbackup_types.go:60-65`).
  - Consumers key on `.spec.groupRef.name`: the printer column (`mysqldatabase_types.go:10`) and the cache
    index (`internal/controller/mysqldatabase_reconciler.go:2658`).
- **Problem:** two names for one reference. The printer column, field index and deploy API key on it.
- **Decision:**
  - MDB `spec.groupRef` → `spec.failoverGroupRef`, keeping the immutability rule.
  - Go type `LocalGroupRef` → `LocalFailoverGroupRef`.
  - `name` gets `+required`, `MinLength=1`, `MaxLength=253` and the DNS-1123 subdomain pattern.
  - Update the printer column, the index key, `cmd/bloodraven/deploy_api.go` and the docs.
  - MSC's remote identity is handled in A-06, not here.

#### A-02 One Secret-reference shape across the five kinds

- **Sev** must · **Brk** yes · **Merges** GPT-02, MFGSPEC-08, MFGSPEC-27 (Secret part) · **Owned by #203**
  for the encryption selector binding
- **Evidence:**
  - Strings: `credentials.operatorSecret` (`types.go:525`; app, readOnly, monitor and backup at `:535-556`),
    `s3.credentialsSecret` (`backup_types.go:480`), `tls.secretName` (`types.go:421`).
  - Dragonfly: `auth.{secretName,passwordKey}` (`dragonfly_types.go:146,152`) and
    `snapshot.credentialsSecretName` (`:123`).
  - Object: `passphraseSecret{name,key}` (`backup_types.go:357,387`).
  - MDB: `owner.secretName` and `users[].secretName` (`mysqldatabase_types.go:297,349,432`). `users` is a
    list-map keyed by `secretName` (`:272-275`).
- **Problem:** five spellings, and two kinds of reference (with and without a key) are mixed. A list-map key
  must be a top-level scalar, so the "`xxxSecretRef` objects" pattern breaks the MDB `users` key.
- **Decision (MBOD Q3 = B):** `xxxSecretName` strings for whole Secrets. `xxxSecretKeyRef{name, key}` (a local
  struct with a required, bounded `name` and `key`) only where a key is selected.
  - Renames:
    - `credentials.{operator,app,readOnly,monitor,backup}Secret` → `…SecretName`
    - `s3.credentialsSecret` → `s3.credentialsSecretName` (backup, `initFromBackup` and `restoreInPlace`
      sources)
    - `dragonfly.auth.{secretName,passwordKey}` → `dragonfly.auth.passwordSecretKeyRef{name, key (default
      password)}`, which also drops the CEL rule at `dragonfly_types.go:16`
    - `encryption.passphraseSecret` and `decryption.passphraseSecret` → `…passphraseSecretKeyRef` (same body)
  - Unchanged: `tls.secretName`, `dragonfly.snapshot.credentialsSecretName`, and the MDB `secretName` fields,
    including the `users` and `appliedUsers` list-map keys.
  - Every name gets `MinLength=1`, `MaxLength=253` and the DNS-subdomain pattern.
  - #203 merges before WS-E (C-10), so it binds its accepted encryption identity to today's
    `passphraseSecret`. WS-E renames that binding with the field (E5); it does not ask #203 for the new name.
- **Disagreement:** GPT and MFGSPEC recommended `xxxSecretRef{name}` objects. MBOD rejected that unanimously in
  round 1: it would re-key the MDB `users` and `appliedUsers` ledger for an extensibility nobody needs.

#### A-03 Remove the legacy DSN credential mode

- **Sev** must · **Brk** yes · **Merges** GPT-03, MFGSPEC-07
- **Evidence:**
  - `spec.secretName` is described as "(legacy)" with an exactly-one CEL rule against `spec.credentials`
    (`types.go:35,68-78`).
  - The DSN Secret is env-injected whole into mysqld. Only credentials mode manages users.
- **Problem:** two credential models with different Secret contracts would freeze.
- **Decision:**
  - Delete `spec.secretName` and the CEL rule at `types.go:35`.
  - `spec.credentials` becomes required (`json:"credentials"`, `+required`).
  - Update `newTestFG` (`internal/controller/reconciler_test.go`), `playground/manifests/failovergroup.yaml`,
    the chart `NOTES.txt` and the docs manifests.

#### A-04 Typed backup references, and one name for them

- **Sev** should · **Brk** yes · **Merges** GPT-45 (reference part), MFGSPEC-21, KINDS-03
- **Evidence:**
  - `corev1.LocalObjectReference` is used for `initFromBackup.source.mysqlBackupRef` and
    `restoreInPlace.source.mysqlBackupRef` (`backup_types.go:635`) and MBV `spec.backupRef`
    (`mysqlbackupverification_types.go:65`). The generated `name` is optional with `default: ""`.
  - The one-of rules at `backup_types.go:603,791` count `has(self.mysqlBackupRef)`, so `{}` passes.
- **Problem:** `{}` is admitted. On MBV it silently means "latest backup"; on restore it fails at reconcile.
  The same reference has two names.
- **Decision:**
  - New type `LocalMysqlBackupRef{name string}` (`+required`, `MinLength=1`, `MaxLength=253`, DNS-subdomain
    pattern), used for MBV `spec.backupRef` and restore `source.backupRef`.
  - Rename `mysqlBackupRef` → `backupRef` and update both one-of rules.
  - Go-only: rename `InitFromBackupSource` → `RestoreSource`.
  - Every `profileName` reference (MBK `:48`, MBV `:59`, schedule `backup_types.go:552`, PITR `:134`) gets the
    `BackupProfile.name` pattern and `MaxLength=63` (A-09).

#### A-05 Required top-level `spec`, and explicit optionality

- **Sev** must · **Brk** yes · **Merges** GPT-04, MFGSPEC-17 (the `enabled` marker part)
- **Evidence:**
  - `json:"spec,omitempty"` on all five roots (`types.go:21`, `mysqlbackup_types.go:26`,
    `mysqlbackupverification_types.go:37`, `mysqldatabase_types.go:53`, `mysqlstandbycluster_types.go:41`).
  - `encryptionAtRest.enabled` is `json:"enabled"` with `default=false` (`encryption_types.go:46-47`).
  - `MysqlBackupSpec.TriggeredBy` lacks `+optional` (`mysqlbackup_types.go:58`).
- **Problem:** a spec-less object skips every nested required field and rule.
- **Decision:**
  - `Spec XSpec \`json:"spec"\`` with `+required` on all five kinds.
  - Every field carries an explicit `+optional` or `+required`.
  - `encryptionAtRest.enabled` becomes `+optional` with `omitempty`.

#### A-06 Remove the MSC source identity fields that nothing reads

- **Sev** should · **Brk** yes · **Merges** KINDS-30, GPT-01 (MSC part) · **Owned by #206** for verified
  source-archive identity
- **Evidence:**
  - `source.failoverGroupName` (required), `namespace`, `cluster` and `profileName` (required)
    (`mysqlstandbycluster_types.go:104,109,115,133`) are documented as "used in events + status".
  - No code outside `api/` reads them (searched `internal/` and `cmd/` for `Source.FailoverGroupName`,
    `.ProfileName`, `.Cluster`).
  - Discovery scans `source.storage.s3.prefix` only.
- **Problem:** users must invent values for required fields that do nothing. `profileName` claims to select
  the bucket layout, but it doesn't.
- **Decision:**
  - Remove all four.
  - The source's identity is the S3 prefix today. #206 adds a writer-stamped, verified source-archive
    identity; the observed identity goes in status.
  - Humans who want a display label use metadata labels.
- **Disagreement:**
  - GPT wanted `source.failoverGroupRef{name,namespace,cluster}`. KINDS wanted the fields made optional.
  - Chosen: removal. An unverified declared identity next to #206's verified one invites exactly the
    confusion GPT warned about, and re-adding a field later is additive.

### 2.2 List semantics and map keys

#### A-07 Conditions are list-maps on every kind

- **Sev** must · **Brk** yes (merge topology; status-only) · **Merges** GPT-05, STATUS-02, KINDS-04 (part)
- **Evidence:**
  - No list markers on MFG `types.go:574`, MBK `mysqlbackup_types.go:188`,
    MBV `mysqlbackupverification_types.go:295` or MSC `mysqlstandbycluster_types.go:212`.
  - MDB has them (`mysqldatabase_types.go:620-622`).
- **Decision:**
  - `+listType=map` and `+listMapKey=type` on all five. These markers give SSA per-condition ownership and
    define structural list-map semantics. JSON merge patch still replaces the whole array (GPT-R2-07).
    Strategic merge patch is not supported for CRs, so `+patchStrategy`/`+patchMergeKey` are not added
    (OPUS-13).
  - Copy the regenerated CRDs to `charts/bloodraven/crds/`.

#### A-08 `spec.sites` becomes a list-map, and `server_id` stops depending on order

- **Sev** must · **Brk** yes · **Merges** GPT-07, MFGSPEC-01
- **Evidence:**
  - `Sites []SiteSpec` has no list type (`types.go:66`). Uniqueness is an O(n²) CEL rule (`:36`).
  - `serverID := int32(101 + i)` (`internal/controller/reconciler.go:369`, again `:2564`), rendered at `:1283`.
  - The bootstrap seed is the first primary-candidate in `splitBrainPolicy.sitePriorities`, else in declared
    order (`api/v1alpha1/site_helpers.go:37-48`).
- **Problem:** reordering or inserting a site changes an existing data directory's `server_id`. SSA cannot
  merge per-site edits.
- **Decision:**
  - `+listType=map` and `+listMapKey=name`, keeping the existing `MinItems=2` and `MaxItems=16` (`types.go:64-65`).
    Delete the CEL uniqueness rule.
  - `server_id` (MBOD Q1 = A): required `spec.sites[].serverID int64`, `Minimum=1`, `Maximum=4294967295`,
    immutable per item (`self == oldSelf`). It is unique within the group, enforced by CEL
    `self.spec.sites.all(x, self.spec.sites.filter(y, y.serverID == x.serverID).size() == 1)`, cost-bounded by
    `MaxItems=16`.
  - The controller renders `--server-id` from it, and the `101 + i` derivation is deleted.
  - Docs and examples use `101`, `102`, … to match today's values.
  - The seed rule stays. `sitePriorities` is already the explicit override, so GPT's `spec.bootstrapSite` is
    not added.
- **Disagreement:**
  - MFGSPEC proposed operator allocation recorded in status. The drafter recommended a hash of the site name.
  - MBOD chose GPT's explicit field unanimously in round 1. A status-allocated id is lost when status is lost,
    and a hash collision is found only at reconcile.

#### A-09 Backup profiles and schedules are list-maps with bounded names

- **Sev** must · **Brk** yes · **Merges** GPT-06, MFGSPEC-02, GPT-10 (profile and schedule part)
- **Evidence:**
  - No list markers on `Profiles` and `Schedules` (`backup_types.go:41,47`).
  - Names have a pattern but no `MaxLength` (`:279`, `:548`).
  - `findProfile` returns the first match (`internal/controller/backup.go:153`).
  - The CronJob name `mysql-<fg>-backup-<sched>` is truncated to 63, not to the CronJob limit of 52
    (`backup.go:97-99`, `:124`).
- **Decision:**
  - `+listType=map`, `+listMapKey=name` and `MaxItems=32` on both lists.
  - `MaxLength=63` on both names (they are label values).
  - The CronJob truncation is a code defect (Appendix A, D-06).
  - Cross-reference rules are in A-91.

#### A-10 Named pod-level lists are list-maps

- **Sev** must · **Brk** yes (SSA semantics only) · **Merges** MFGSPEC-03, GPT-09 (part)
- **Evidence:** `ExtraContainers` and `ExtraInitContainers` (`types.go:159,163`) and `backup.imagePullSecrets`
  (`backup_types.go:36`) have no list type. Core `PodSpec` keys them by `name`.
- **Decision:** `+listType=map`, `+listMapKey=name` and `MaxItems=16` on all three.

#### A-11 Status lists keyed by name are list-maps with a required key

- **Sev** must · **Brk** yes (status-only) · **Merges** STATUS-03, GPT-09 (status part)
- **Evidence:**
  - `status.sites` (`types.go:571`; `name` is `omitempty` at `:659`).
  - `status.dragonfly.sites` (`dragonfly_types.go:261`; `name` at `:354`).
  - `status.backupSchedules` (`types.go:621`).
  - `status.restoreReclones.failures` (`backup_types.go:965`, key `site` at `:979`).
  - Compare encryption sites (`encryption_types.go:234-236`), which already have the right shape.
- **Decision:**
  - `name` becomes `json:"name"`, `+required`, `MinLength=1` on `SiteStatus` and `DragonflySiteStatus`.
  - `+listType=map` with key `name` on the first three lists and key `site` on `failures`.

#### A-12 Schema-name lists are sets with bounds

- **Sev** should · **Brk** yes · **Merges** MFGSPEC-28, GPT-09 (set part)
- **Evidence:** `dump.includeSchemas` and `excludeSchemas` (`backup_types.go:525,528`) and the `loadOptions`
  equivalents (`:695,700`) have no list type or bounds. The in-place rule "empty or exactly one entry" is
  enforced only in the controller (`backup_types.go:794-800`).
- **Decision:**
  - `+listType=set`, `MaxItems=256` and `items:MaxLength=64`.
  - On `RestoreInPlaceSpec`, CEL
    `!has(self.loadOptions) || !has(self.loadOptions.includeSchemas) || size(self.loadOptions.includeSchemas) <= 1`.

#### A-13 Ordered lists are explicitly atomic, and `sitePriorities` entries are unique

- **Sev** should · **Brk** yes (stricter) · **Merges** GPT-44, MFGSPEC-29, STATUS-03 (atomic part)
- **Evidence:**
  - `sitePriorities` is `+listType=atomic` and admits duplicates (`types.go:279-282`).
  - `restoreReclones.pending`, `superseded.pending` and `restore.pitrReplaySites`
    (`backup_types.go:948,996,1140`) and `dragonfly.args` (`dragonfly_types.go:62`) have no explicit list type.
- **Decision:**
  - `+listType=atomic` on the four ordered lists.
  - On `sitePriorities`: CEL `self.all(x, self.filter(y, y == x).size() == 1)`, cost-bounded by the existing
    `MaxItems=16` and `items:MaxLength=253` (`types.go:280-281`).
  - `pitrReplaySites` keeps its type; its meaning is owned by #206.

#### A-14 MDB `deploymentClients` uses a compound key

- **Sev** should · **Brk** yes (SSA identity changes; `MaxItems=16` rejects longer lists) · **Merges** KINDS-21
- **Evidence:**
  - Keyed by `namespace` only (`mysqldatabase_types.go:197-199`), so `7.tenant-databases.md` documents one
    ServiceAccount per namespace.
  - The deploy API authorizes per (namespace, serviceAccount) pair (`cmd/bloodraven/deploy_api.go:221`).
  - No `MaxItems`.
- **Decision:** `+listMapKey=namespace` plus `+listMapKey=serviceAccount`, and `MaxItems=16`. The
  one-per-namespace limit has no stated authority rationale, so it is dropped.

### 2.3 Site identity and naming

#### A-15 Name bounds that match the names the operator derives

- **Sev** must · **Brk** yes (stricter) · **Merges** GPT-10, MFGSPEC-14, OPS-01 (name-length part)
- **Evidence:**
  - Site names allow `MaxLength=253` and `^[a-z0-9-]+$`, which permits leading and trailing hyphens
    (`types.go:344-346`).
  - Derived names:
    - `mysql-<group>-<site>-internal` Service (`internal/controller/mysql_tls.go:110`)
    - `mysql-<group>-<site>-data` PVC (`reconciler.go:1136`)
    - Dragonfly StatefulSet `<group>-dragonfly-<site>` (`dragonfly_resources.go:67-68`). StatefulSet names
      must stay ≤ 52 so the controller-revision-hash label fits.
    - The group and site names are label values (`reconciler.go:54-55`).
  - MBK and MBV names are label values too (`backup.go:19`, `backup_verification.go:14`).
  - The group name starts the Dragonfly Service names `<group>-dragonfly` and `<group>-dragonfly-<site>`
    (`dragonfly_resources.go:75,82`) and sits inside `mysql-<group>-primary` (`reconciler.go:1799`). A CR name
    only has to be a DNS subdomain, but a Service name must be a DNS-1035 label: no dots, and a letter first.
- **Problem:** a group plus site longer than 41 characters, a site such as `-iad`, or a group named `a.b` or
  `1orders` passes admission and fails later when the child objects are created.
- **Decision:**
  - Site name: `Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, `MaxLength=20`.
  - MFG root rule: `self.spec.sites.all(s, size(self.metadata.name) + size(s.name) <= 41)`. The Dragonfly
    StatefulSet `<group>-dragonfly-<site>` is the tightest budget: its 11 fixed characters plus 41 make 52.
    Revision 2 said 40 with no reason for the spare character; 41 matches the problem statement
    (OPUS-R2-14). With a site name of at least one character, the rule also bounds the group name (≤ 40).
  - MFG root rule for the name grammar (R08): `self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')`, the
    DNS-1035 label that the derived Service names need. Envtest rejects `a.b` and `1orders`.
  - MBK and MBV root rule: `size(self.metadata.name) <= 63`.
  - The `sitePriorities` items (`types.go:282`) and the `pointInTime.sites` items (`backup_types.go:208`,
    `mysqlbackupverification_types.go:140`) get the site pattern. The meaning of `pointInTime.sites` is owned by
    #206; this changes only the item pattern.
- **Disagreement:** GPT proposed hash-truncating every child name instead. Rejected: Service and DNS names are
  user-facing and must stay predictable; admission bounds fail early.

#### A-16 Removing or renaming a site is refused

- **Sev** must · **Brk** yes (rejects updates) · **Merges** GPT-08, MFGSPEC open question 7
- **Evidence:**
  - Site names identify Deployments, PVCs, Services, archive paths and keyring records.
  - The main `Reconcile` loop creates per-site objects (`internal/controller/reconciler.go:367-369`), and no code prunes a removed site's
    resources.
- **Decision:**
  - MFG root rule: `oldSelf.spec.sites.all(o, self.spec.sites.exists(s, s.name == o.name))`, with the message
    "sites cannot be removed or renamed; see the site-replacement procedure".
  - Adding sites stays allowed.
  - Document replacement as a new group restored from a backup, which is the section 3 procedure (D3). A future
    decommission feature can relax the rule without a break.

#### A-17 Site storage and zone are immutable

- **Sev** must · **Brk** yes (rejects updates) · **Merges** GPT-16, MFGSPEC-13
- **Evidence:**
  - `StorageSpec.StorageClassName` and `Size` (`types.go:407,410`) are applied only when the PVC is created
    (`reconciler.go:1132-1158`).
  - `SiteSpec.Zone` (`:356`) is not creation-only. It is rendered into the node selector of the MySQL Deployment
    (`reconciler.go:1667`) and of the Dragonfly StatefulSet (`dragonfly_resources.go:234`), so an edit moves the
    pods. A zone-bound volume does not move with them.
  - `6.operations/2.operations.md:381` says "To resize PVCs, update the `spec.sites[].storage.size` field",
    which does nothing.
- **Problem:** storage edits are accepted and silently ignored. A zone edit reschedules the pods into a zone their
  volume cannot follow, so they stay Pending.
- **Decision:**
  - `self == oldSelf` on all three. The per-item rules work once A-08 lands.
  - Size is immutable, not grow-only, because the operator does not implement PVC expansion. The CEL
    `quantity()` library that a grow-only rule needs is available at the A-115 floor. A later release that
    implements expansion relaxes the rule, which is not breaking.
  - Fix `operations.md:381`.

#### A-18 Backup PVC storage: an existing claim or a provisioned claim, not both

- **Sev** should · **Brk** yes · **Merges** GPT-15, MFGSPEC-31
- **Evidence:**
  - `PVCStorage{claimName, storageClassName, size, subPath}` (`backup_types.go:490-506`).
  - `size` and `storageClassName` are ignored when `claimName` is set.
  - A zero `size` silently becomes 10Gi (`internal/controller/backup_schedule.go:225-226`).
  - Both are applied only when the PVC is created (`:223`).
- **Decision:**
  - New shape: `pvc: {claimName?: string, provisioned?: {storageClassName *string, size resource.Quantity
    (required)}, subPath?: string}`.
  - CEL: exactly one of `claimName` or `provisioned`.
  - `provisioned` is immutable, for the same reason as A-17. A field-level `self == oldSelf` would not stop
    removing the field and adding it back with new values (R04), so the transition rule sits on the `pvc`
    parent and checks presence too:
    `has(self.provisioned) == has(oldSelf.provisioned) && (!has(self.provisioned) || self.provisioned == oldSelf.provisioned)`.
    That also refuses switching between `claimName` and `provisioned`.
  - A transition rule cannot see a profile that was removed in one update and added back in the next. The
    controller therefore also compares the owned PVC `mysql-<fg>-backup-<profile>` with `provisioned`. On a
    class or size mismatch it refuses that profile's backups with a `BackupStorageMismatch` condition reason and
    Event. It does not use the old PVC silently. The comparison is defined so that it does not fire on
    correct claims (OPUS-R2-13):
    - A nil `storageClassName` uses the cluster default, and matches any class on the PVC. The
      DefaultStorageClass admission plugin writes the default's name into the PVC, so a literal compare would
      flag every nil-class profile on its first reconcile.
    - An explicit `storageClassName` must equal the PVC's `spec.storageClassName`.
    - Size compares `provisioned.size` with the PVC's `spec.resources.requests.storage`, not with
      `status.capacity`, which provisioners round up. A larger request is accepted with a Normal Event
      (`BackupStorageExpansionAccepted`, which follows the A-74 rule), so an administrator who expands the PVC out
      of band does not stop backups.
    - Only a smaller request or a different explicit class refuses. E6's unit test covers all four cases.
  - #232's `status.artifactStorage.claimName` records the claim in both cases.
  - The body is one Go type, `ProvisionedVolumeSpec{storageClassName *string, size resource.Quantity}`. Proposal
    21 (#205) already uses this body for `restoreInPlace.staging.volume`. The two field names stay different
    because the contexts differ: here it is one arm of a union, and there it is the only shape (OPUS-16).
- **Disagreement:** GPT also wanted the site storage reshaped into the same template. Rejected: site storage
  is always provisioned, so A-17's immutability is enough.

#### A-19 `lbIP` → `loadBalancerIP`, validated as IPv4

- **Sev** should · **Brk** yes · **Merges** MFGSPEC-20
- **Evidence:**
  - `LBIP string` with only `MinLength` (`types.go:366-369`).
  - It is referenced by the site CEL rule at `:336`.
  - It feeds an A record.
- **Decision:**
  - `LoadBalancerIP string \`json:"loadBalancerIP,omitempty"\``, `+kubebuilder:validation:Format=ipv4`.
  - Update the rule at `:336`.
  - CEL `isIP()` is available at the A-115 floor (1.31), but it is not needed: the OpenAPI format costs nothing
    in the CEL budget.

#### A-20 Site-selector field names

- **Sev** should · **Brk** yes · **Merges** GPT-36, KINDS-09 · **Owned by #235** for the planned-failover names
- **Evidence:**
  - MBK `sourceSiteOverride` (`mysqlbackup_types.go:53`) has no site validation. The plugin flag is already
    `--source-site`.
  - Planned-failover status uses `target` and `sourcePrimary` (`planned_failover_types.go:115,119`).
- **Decision:**
  - MBK `spec.sourceSiteOverride` → `spec.sourceSite`, with the A-15 site pattern and immutability (A-22).
  - House rule: `sourceSite` and `targetSite` for directional selectors, `site` for single-site operations,
    `activeSite` for observed authority.
  - #235 is expected to leave `status.plannedFailover.targetSite` and `sourceSite`.

#### A-21 Namespace-scoped read-only taint key

- **Sev** must · **Brk** yes · **Merges** GPT-11, OPS-01, OPS-09
- **Evidence:**
  - The key is `shipstream.io/db-readonly-<group>`, built from the group name only
    (`internal/platform/tainter.go:20`). Two groups with the same name in different namespaces share it.
  - The legacy unscoped key is still stripped (`tainter.go:19,85`).
  - Copies of the key: `internal/playground/chaos/actions.go:353`,
    `internal/playground/scenarios/s20_shared_node_selector_isolation.go:24-25`,
    `cmd/playground-chaos/reset.go:37`.
- **Decision:**
  - Key `<namespace>.db-readonly.bloodraven.shipstream.io/<group>`, value `true`, effect `NoExecute`.
  - The prefix is at most 100 characters (limit 253), and the name part is the group name, bounded by A-15.
  - Export `ReadOnlyTaintKey(namespace, name string) string` from `api/v1alpha1` (A-79). The tainter, the
    generated tolerations and the chaos tooling all use it.
  - Delete `LegacyTaintKey` (D3).
  - Release note: rewrite tolerations, and remove old taints with `kubectl taint nodes --all <old-key>-`.

### 2.4 Immutability and transition rules

#### A-22 MysqlBackup and MysqlBackupVerification execution fields are immutable

- **Sev** must · **Brk** yes (rejects updates) · **Merges** GPT-12, KINDS-01
- **Evidence:**
  - There is no transition rule on `MysqlBackupSpec` (`mysqlbackup_types.go:40-59`) or
    `MysqlBackupVerificationSpec` (`mysqlbackupverification_types.go:51-122`).
  - Workaround for the mutable spec: MBV terminal attribution reads the group and profile back from Job
    labels (`internal/controller/backup_verification_reconciler.go:432-435`).
  - After #232, cleanup uses the recorded `status.artifactStorage` (`backup_cleanup.go:64-83`), but retention
    still buckets by the live `spec.failoverGroupRef`/`profileName` (`backup_reconciler.go:1306-1317`).
- **Decision:**
  - Immutable fields:
    - MBK: `failoverGroupRef`, `profileName`, `sourceSite`.
    - MBV: `failoverGroupRef`, `profileName`, `backupRef`, `pointInTime`, `sanityCheck`.
  - Optional fields use `has(self.x) == has(oldSelf.x) && (!has(self.x) || self.x == oldSelf.x)` on the spec
    struct.
  - `keepOnFailure`, `ttlSecondsAfterFinished` and `resources` stay mutable. `resources` only shapes a Job that
    is not yet created. Whole-struct equality on `corev1.ResourceRequirements` would also risk the CEL cost
    budget, because its upstream `claims` list has no `MaxItems` and upstream types take no markers (OPUS-11).
  - Remove the Job-label workaround.

#### A-23 `initFromBackup` is fixed at creation

- **Sev** must · **Brk** yes (rejects updates) · **Merges** GPT-13, MFGSPEC-16 · **Owned by #206** for the
  `pointInTime` shape inside it
- **Evidence:**
  - The field is documented as a one-shot bootstrap (`types.go:170-175`).
  - Removing it clears `status.restore` (`internal/controller/restore.go:516-522`), so removing and re-adding it
    reruns the restore.
  - Adding it to a live group targets the active writable site (`restore.go:474-480`), gated only on the
    Deployment rollout (`:545-560`).
- **Problem:** a destructive load into a serving primary, with none of the fencing `restoreInPlace` has.
- **Decision:**
  - MFG root rule: `has(self.spec.initFromBackup) == has(oldSelf.spec.initFromBackup) &&
    (!has(self.spec.initFromBackup) || self.spec.initFromBackup == oldSelf.spec.initFromBackup)`.
  - Restoring into a running group is `restoreInPlace` only.
  - The rule is shape-agnostic, so #206 can reshape `pointInTime` freely.
  - Whole-block equality is costed over every string in the block, so each one gets a `MaxLength` (A-92). E6's
    envtest installs the CRD and fails on a cost-budget error (OPUS-11).
- **Disagreement:** MFGSPEC allowed edit and removal. Chosen: GPT's full immutability. The block is the
  bootstrap record, and removal followed by re-add is the rerun hazard.

#### A-24 Encryption at rest cannot be turned off

- **Sev** must · **Brk** yes (rejects updates) · **Merges** GPT-29, MFGSPEC-17
- **Evidence:**
  - Documented as enable-only (`api/v1alpha1/encryption_types.go:37-47`).
  - The disable path is "only a status cleanup" (`internal/controller/encryption_reconcile.go:61-63`), and
    rendering then drops the keyring from data that is still encrypted.
  - Keyring paths: `encryption_types.go:131,139,147`.
- **Decision:**
  - MFG root rule: `!has(oldSelf.spec.encryptionAtRest) || !oldSelf.spec.encryptionAtRest.enabled ||
    (has(self.spec.encryptionAtRest) && self.spec.encryptionAtRest.enabled)`.
  - `keyring.dataFileDir`, `mysqldDir` and `pluginDir` are immutable while enabled. This was not verified
    live, so the immutable choice is the reversible one.
  - Decrypting a group is a new-group migration (D3).

#### A-25 MFG transition rules live on the kind root

- **Sev** should · **Brk** no · **Merges** KINDS-31 (transition-rule part)
- **Evidence:** MSC embeds `MysqlFailoverGroupSpec` (`mysqlstandbycluster_types.go:161`), so a rule on the
  spec type would apply inside the template too.
- **Decision:**
  - Every MFG transition rule (A-16, A-23, A-24, plus the #203 encryption binding) is declared on
    `MysqlFailoverGroup` as `self.spec…` against `oldSelf.spec…`, never on the shared spec type.
  - Per-item rules on `SiteSpec` and `StorageSpec` (A-17) are the exception. They only exist on the MFG once
    A-62 removes the template.

#### A-26 `restoreInPlace.confirm` is validated as a timestamp at admission

- **Sev** should · **Brk** yes (invalid values only) · **Merges** MFGSPEC-22, STATUS-L5
- **Evidence:**
  - `Confirm string` with only `MinLength` (`backup_types.go:779-788`). It is parsed only at reconcile.
  - The status tokens `confirmTokenUsed` and `runConfirm` are compared byte for byte (`:1071,1080`).
- **Decision:**
  - Keep `string`.
  - Add `+kubebuilder:validation:Format=date-time` and `MaxLength=64`.
- **Disagreement:** MFGSPEC proposed `metav1.Time`. Rejected: it normalises the value, which breaks the byte
  equality with the status tokens.

### 2.5 Enums, phases and casing

Rule for the frozen API: custom enum values are CamelCase and backed by a named Go string type with exported
constants. External tokens keep their external spelling (MySQL privileges, mysqlsh compression names, the
algorithm id).

#### A-27 Spec enums: CamelCase and typed

- **Sev** must · **Brk** yes · **Merges** GPT-21, MFGSPEC-04, MFGSPEC-25, KINDS-17 (mode part) ·
  **Owned by #235** for `plannedFailover.onCooldown`
- **Evidence:**
  - `SiteRole` values `primary-candidate;dr-only;read-only` (`types.go:318-333`). The CEL rules compare the
    literals (`:37,38,336`).
  - `pointInTime.mode` `none;latest;timestamp` on a plain `string` (`mysqlbackupverification_types.go:143-145`).
  - `onCooldown` `reject;defer` (`planned_failover_types.go:37-40`).
  - `onSyncTimeout` `proceed;fail` (`dragonfly_types.go:176-179`).
  - Untyped strings: `updateStrategy` (`types.go:133-136`), `concurrencyPolicy` (`backup_types.go:580-582`,
    `mysqlbackupverification_types.go:390`), `dump.compression` (`backup_types.go:520-522`),
    `encryption.algorithm` (`:349-351`).
- **Decision:**
  - `SiteRole`: `PrimaryCandidate | DisasterRecovery | ReadOnly`. Update the three CEL rules. The metric `role`
    label follows A-114.
  - `PointInTimeVerificationMode`: `Latest | Timestamp`, required inside the block. `none` is dropped: an
    absent `pointInTime` block means no replay.
  - `DragonflySyncTimeoutPolicy`: `Proceed | Fail`. This is in the planned-failover path, so #235 may move it;
    the casing applies wherever it lands.
  - #235 is expected to leave `onCooldown` as a typed `Reject | Defer`, or remove it.
  - `updateStrategy` becomes an object: `updateStrategy: {type: OrderedUpdate | Recreate}` (typed
    `UpdateStrategyType`, default `OrderedUpdate`). Deployment and StatefulSet set this precedent, and it is
    the only cheap moment to make it an object.
  - Both `concurrencyPolicy` fields use `batchv1.ConcurrencyPolicy`.
  - Typed `DumpCompression` (values kept, passed verbatim to mysqlsh) and a shared `EncryptionAlgorithm`
    (`AES-256-GCM` kept, also used by A-45).
- **Disagreements:**
  - GPT spelled the role `DROnly`. Chose `DisasterRecovery`: it names the role, and "only" is implied by not
    being a candidate.
  - MFGSPEC kept `None`. Chose KINDS: absence should have one spelling.

#### A-28 Status enums: CamelCase and typed

- **Sev** must · **Brk** yes (status-only) · **Merges** STATUS-06, STATUS-07 (recovery and divergence part),
  GPT-21 (status part)
- **Evidence:**
  - `sites[].state` is a plain `string` with `Enum=writable;read-only;unreachable;unknown`
    (`types.go:661-663`).
  - `DragonflyRole` values `unknown;master;replica;stale-master;unconfigured;unreachable`
    (`dragonfly_types.go:214`).
  - `recoveryState` and `divergenceDetectedBy` are plain strings with enum markers (`types.go:685-686,705`).
- **Decision:**
  - `type SiteState` with `Writable;ReadOnly;Unreachable;Unknown`.
  - `DragonflyRole` values `Unknown;Master;Replica;StaleMaster;Unconfigured;Unreachable`.
  - `type SiteRecoveryState` and `type DivergenceDetector`, keeping their current values.
  - Pod label values (`role=primary`, `dragonfly-role=master`) are a separate contract (A-77). The metric
    `state` label and the `/deploy/v1` tokens follow A-114.
  - Sweep the string comparisons in the controller, kubectl plugin, dashboard, doctor scripts and chaos runner.

#### A-29 `updatePhase` and `lastBackupPhase` are API enums

- **Sev** must · **Brk** yes (status-only) · **Merges** GPT-22, STATUS-07
- **Evidence:**
  - `UpdatePhase string` with no enum (`types.go:589`). Its Go type lives in `internal/controller/updater.go:15-25`
    and includes `Complete`.
  - Read by the lease gate (`deployment_leases.go:283`) and the rotation gate (`encryption_reconcile.go:746`).
  - `LastBackupPhase string` (`backup_types.go:726`).
- **Decision:**
  - Move `UpdatePhase` to `api/v1alpha1` with
    `Enum=UpdateReplica;WaitReplica;Failover;UpdateOldPrimary;WaitOldPrimary`. `Complete` is never persisted;
    the field is cleared instead.
  - `LastBackupPhase BackupPhase`.

#### A-30 Structured restore scope in status

- **Sev** should · **Brk** yes (status-only) · **Merges** GPT-37, STATUS-09
- **Evidence:**
  - `status.restoreInPlace.scope` is documented as "human-readable" `full` or `schema:<name>`
    (`backup_types.go:1059-1061`).
  - The run is driven by parsing it back (`internal/controller/restore_inplace.go:254`, `strings.CutPrefix`).
- **Decision:**
  - `scope: {type: Full | Schema, schema?: string (MaxLength=64)}`, with CEL `self.type != 'Schema' ||
    has(self.schema)`.
  - The spec keeps inferring scope from `loadOptions.includeSchemas`, now bounded at admission (A-12).
- **Disagreement:** GPT also wanted an explicit spec `scope`. Rejected: it would be a second way to say what
  `includeSchemas` already says. #205 adds its staging fields next to `scope`.

#### A-31 MysqlBackupVerification phases match what is written

- **Sev** must · **Brk** yes (status-only) · **Merges** KINDS-16 (phase part)
- **Evidence:** the enum has `Checking` and `Cleaning` (`mysqlbackupverification_types.go:209,224-231`), but the
  controller writes only `Pending`, `Provisioning`, `Restoring`, `Succeeded` and `Failed`. No
  `VerificationPhaseChecking` or `VerificationPhaseCleaning` appears in non-test code.
- **Decision:** `Enum=Pending;Provisioning;Restoring;Succeeded;Failed`. A future replay or check phase is an
  additive enum value.

#### A-32 MysqlDatabase phase `Creating` → `Applying`

- **Sev** should · **Brk** yes (status-only) · **Merges** KINDS-22
- **Evidence:** `MysqlDatabasePhaseCreating` "means the reconciler is applying DDL"
  (`mysqldatabase_types.go:484-485`). It is stamped on every write-ahead apply, including rotations.
- **Decision:** `Enum=Pending;Applying;Ready;Failed;Deleting`.

#### A-33 Machine-readable reason strings in status are typed

- **Sev** should · **Brk** yes (status validation; status-only) · **Merges** STATUS-15
- **Evidence:**
  - Free strings: `status.restore.reason` (`backup_types.go:1143`), `status.dragonfly.upgrade.reason`
    (`dragonfly_types.go:343`), `status.plannedFailover.dragonfly.reason` (`planned_failover_types.go:258`).
  - `status.sites[].sourceConvergenceReason` (`types.go:724`), whose closed set exists only as constants in
    `internal/controller/topology.go:163-167`. It has no validation today (appendix).
- **Decision:** two parts, which differ in whether they break (GPT-R2-05; revision 2 said "not breaking"):
  - Go only, wire-compatible: named Go types and exported constants in `api/v1alpha1`.
  - Admission restrictions, which reject status values the current schema accepts: the metav1.Condition reason
    pattern, `MaxLength=1024`, and an `Enum` for `sourceConvergenceReason`, including `RestorePeerHold`, which
    #233 added (`topology.go:168-171`). They are breaking, but status-only: only the operator writes these
    fields, and a new group starts with an empty status (3.3), so no manifest changes. Section 3.4 has the row.
  - A status reason that equals an Event reason is documented as the same token (A-74 renames both together).

#### A-34 Sanity-check shape

- **Sev** should · **Brk** yes · **Merges** KINDS-18, GPT-17 (`durationMs` part)
- **Evidence:**
  - Spec: `sanityCheck.query` has no `MaxLength`. `expect.minRows` compares a scalar result, not a row count,
    and `expect.maxDurationSeconds` is a timeout (`mysqlbackupverification_types.go:165-189`).
  - Status: `ran bool`, `durationMs`, `resultRow` (`:325,329,334`).
- **Decision:**
  - Spec: `sanityCheck: {query (MinLength=1, MaxLength=4096), minResult *int64 (Minimum=0), timeout}`. The
    `timeout` encoding follows Q2 (recommended `*metav1.Duration`, default `60s`, `>= 1s`).
  - Status: `sanityCheck: {outcome: Passed|Failed|TimedOut|Skipped, result string, durationMilliseconds int64,
    error string}`.

### 2.6 Durations, timestamps and integer types

#### A-35 One timer encoding

- **Sev** must · **Brk** yes · **Merges** GPT-17, MFGSPEC-19 (encoding part), KINDS-32, MFGSPEC-05 (encoding
  part) · **Owned by #235** for the planned-failover timers
- **Evidence:**
  - `metav1.Duration`: `types.go:91,105,114,464,469`, `backup_types.go:149`, `planned_failover_types.go:17,27`,
    `dragonfly_types.go:167`, `mysqlstandbycluster_types.go:178`.
  - Integer seconds: `types.go:125` (`cloneTimeout`, no unit), `encryption_types.go:167`,
    `backup_types.go:266,271`, `mysqlbackupverification_types.go:189`, `types.go:447` (`dns.ttl`).
- **Decision (MBOD Q2 = A):**
  - Spec timers are `*metav1.Duration`.
  - Integer `xxxSeconds` only where the value is forwarded verbatim to a core field (`activeDeadlineSeconds`,
    `startingDeadlineSeconds`, `ttlSecondsAfterFinished`, `terminationGracePeriodSeconds`) or compared with a
    MySQL or DNS seconds value (`maxLagSeconds`, `maxLagSecondsForSource`, `dns.ttlSeconds`).
  - Status measurements are integers with the unit in the name.
  - Renames:
    - `cloneTimeout` (int) → `cloneTimeout` (Duration, A-36)
    - `escrowTimeoutSeconds` → `escrowTimeout`
    - `initialBackoffSeconds`/`maxBackoffSeconds` → `initialBackoff`/`maxBackoff`
    - `maxDurationSeconds` → `timeout` (A-34)
    - `dns.ttl` → `ttlSeconds`
    - `durationMs` → `durationMilliseconds`
  - MSC `discoveryInterval` stays a Duration.
  - Bounds, zero semantics and the CRD test are in A-38.
  - #235 is expected to keep its timers as Durations under the same bounds rule.
- **Disagreement:** GPT recommended integer seconds everywhere. GPT's debater moved to A in round 2: about 12
  renames, including #235's timers, for no added safety once every Duration is bounded.

#### A-36 No platform-sized `int` in the API

- **Sev** must · **Brk** yes · **Merges** MFGSPEC-05, STATUS-08
- **Evidence:**
  - `CloneTimeout int` (`types.go:125`). The CRD integer has no format.
  - `LastIOSecondsAgo int` (`dragonfly_types.go:391`), where `0` and absent look the same.
- **Decision:**
  - `cloneTimeout *metav1.Duration`, default `1h`, bounded `1m`..`24h` (A-38).
  - `lastIOSecondsAgo *int64`, nil when Dragonfly did not report it.
  - A lint test fails on any `int` or `uint` field in `api/`.

#### A-37 A valid zero is never rewritten to a default

- **Sev** must · **Brk** yes (types; explicit zeros gain meaning) · **Merges** GPT-19, MFGSPEC-06
- **Evidence:**
  - `maxLagSeconds == 0` → 300 (`api/v1alpha1/site_helpers.go:79-83`), while `readOnlyMaxLagSeconds` 0 means
    "caught up" (`:88-92`).
  - Cooldown 0 → 5m (`internal/controller/topology.go:835-837`).
  - `MaxFailedKeep == 0` → 10 (`backup_reconciler.go:1357-1359`).
  - `backoffLimit` (`backup_types.go:68-70`) and `maxLagSecondsForSource` (`:53-55`) are defaulted
    non-pointers with `omitempty`, so typed clients cannot send 0.
  - MBV `ttlSecondsAfterFinished` is `int32`, default 0 (`mysqlbackupverification_types.go:84-90,421`).
- **Decision:**
  - Rule: a defaulted scalar whose zero is valid is a pointer, the CRD default applies only to nil, and the
    controller never maps zero to a default.
  - Pointers: `replication.maxLagSeconds *int64` (`Minimum=0`; 0 means caught up), `backup.backoffLimit
    *int32`, `backup.maxLagSecondsForSource *int64`.
  - MBV `ttlSecondsAfterFinished *int32`, with no default: nil keeps the run until retention, 0 deletes it at
    once (Job semantics).
  - Retention counts are in A-55. A zero cooldown is rejected (A-38) rather than mapped.

#### A-38 Bounds on every timer

- **Sev** must · **Brk** yes (stricter) · **Merges** GPT-18, MFGSPEC-19 (bounds part), MFGSPEC-06 (cooldown
  part), GPT-24 (`startingDeadlineSeconds` part)
- **Evidence:**
  - Only the sidecar timers and `discoveryInterval` have CEL bounds (`types.go:457-459`,
    `mysqlstandbycluster_types.go:177`).
  - `pollInterval: 0s` makes the topology loop spin (`topology.go:1262`); only the Dragonfly runner clamps it
    (`runner.go:943-945`).
  - `startingDeadlineSeconds` has no minimum (`backup_types.go:577`).
- **Decision** (MBOD Q2 conditions): every Duration property has a field-specific lower and upper bound in CEL,
  `duration(self) >= duration('<min>') && duration(self) <= duration('<max>')`. The upper bounds fit
  `time.Duration`. A zero is rejected by the lower bound, or its meaning is documented on the field.

  | Field | Lower | Upper |
  |---|---|---|
  | `pollInterval` | `1s` | `5m` |
  | `connectionDrainTimeout` | `1s` | `10m` |
  | `failoverCooldown` | `30s` | `24h` |
  | `cloneTimeout` | `1m` | `24h` |
  | `backup.pitr.archivePollInterval` | `5s` | `1h` |
  | `backup.retry.initialBackoff` | `1s` | `1h` |
  | `backup.retry.maxBackoff` | `1s` | `24h` (plus `>= initialBackoff`) |
  | `encryptionAtRest.keyring.escrowTimeout` | `30s` | `1h` |
  | `sanityCheck.timeout` | `1s` | `1h` |
  | `dragonfly.plannedFailover.maxSyncWait` | `1s` | `10m` |
  | `sidecar.peerCheckInterval` | `1s` (kept) | `1m` |
  | `sidecar.leaseTimeout` | `3s` (kept) | `10m` (the 3× rule at `types.go:459` stays) |
  | MSC `freshness.discoveryInterval` | `30s` (kept) | `24h` |
  | `plannedFailover.*` timers | owned by #235: lower `1s` and an upper bound are expected | |

  - `Minimum=0` on both `startingDeadlineSeconds` fields.
  - Every rule's operands are bounded so the CEL cost check passes.
  - **CRD test (OPUS-10):** `metav1.Duration` generates `type: string` with no `format`
    (`config/crd/bases/shipstream.io_mysqlfailovergroups.yaml` has no `format: duration`), so scanning the CRDs for
    a format would pass vacuously. The test reflects over the Go types in `api/v1alpha1` and collects every
    `metav1.Duration` and `*metav1.Duration` field with its JSON path. It then asserts that the matching CRD
    property has `x-kubernetes-validations` with both a `>=` and a `<=` `duration()` bound. No
    `+kubebuilder:validation:Format=duration` marker is added: CEL would then type the field as `duration`, and
    the OpenAPI duration format accepts spellings that `time.ParseDuration` rejects.
  - Admission tests for the bounds run on envtest assets at the supported floor (A-115).
- **Disagreement:** MFGSPEC floated allowing a zero cooldown. Rejected: the cooldown is anti-flap safety, and
  zero cannot disable it today either. The playground's smallest value is 30s.

#### A-39 Requested instants are typed timestamps

- **Sev** must · **Brk** yes · **Merges** GPT-23, MFGSPEC-18, KINDS-17 (timestamp part) · **Owned by #206** for
  replay semantics
- **Evidence:**
  - `pointInTime.stopDatetime` accepts RFC 3339 or a zone-less MySQL datetime (`backup_types.go:236-241`).
  - Verification `pointInTime.timestamp` is an unchecked string (`mysqlbackupverification_types.go:148-151`),
    "required when Mode=timestamp" only in Go.
- **Decision:**
  - Restore: `stopTime metav1.Time` (required). Verification: `stopTime *metav1.Time`.
  - Verification CEL: `self.mode == 'Timestamp' ? has(self.stopTime) : !has(self.stopTime)`.
  - Converted to MySQL UTC syntax only when building commands.
  - #206 is expected to keep `stopTime` and change only `sites` and identity.
  - **WS-E owns these renames (E8).** #206 owns only the replay semantics (R11, OPUS-03). E8 changes the restore
    script's stop-time conversion (`internal/controller/restore_script.py`), the verification Job's timestamp
    argument, and the docs. Its envtest: a `stopTime` without a zone is rejected; `mode: Timestamp` without
    `stopTime` is rejected; `mode: Latest` with `stopTime` is rejected.

#### A-40 Status timestamps and the failover record

- **Sev** should · **Brk** yes (status-only) · **Merges** STATUS-13 (timestamp part), STATUS-14, KINDS-33 (time
  part)
- **Evidence:**
  - String timestamps: `pitrStopDatetime` (`backup_types.go:1118,1190`), `pitrNewestEventTime` (`:1147`).
  - MSC `discovered.lastScanAt` (`mysqlstandbycluster_types.go:277`) is already a `*metav1.Time` with
    `format: date-time`. Only its name is off the `…Time` convention (R14).
  - The flat record `lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` (`types.go:576-586`) is read
    as one record together with its annotation copies (`internal/controller/failover_state.go:40-41`).
- **Decision:**
  - PITR times become `*metav1.Time` inside A-97's struct.
  - `lastScanAt` → `lastScanTime`: a rename only, with no type change.
  - `status.lastFailover: {time *metav1.Time, target string, promotionGtidExecuted string}` (Go
    `FailoverRecord`). The annotation keys stay (A-104).
  - #235's planned-run watermark stays in its own run record.

#### A-41 Dragonfly memory as a Quantity

- **Sev** should · **Brk** yes · **Merges** MFGSPEC-26 (type part)
- **Evidence:** `maxMemoryMb int32` and `proactorThreads int32`, where 0 means default
  (`dragonfly_types.go:48,54`).
- **Decision:**
  - `maxMemory *resource.Quantity`, rendered as bytes.
  - `proactorThreads *int32` (`Minimum=1`); nil means Dragonfly's default.

#### A-42 One binlog-coordinate type

- **Sev** should · **Brk** yes (status-only) · **Merges** KINDS-10
- **Evidence:** MBK has flat `binlogFile`/`binlogPos` (`mysqlbackup_types.go:137,140`), while MBV and MSC use
  `BinlogReplayMark{file, position, timestamp}` (`mysqlbackupverification_types.go:302-315`).
- **Decision:**
  - MBK `status.binlogPosition *BinlogCoordinate{file string, position int64}`.
  - `BinlogReplayMark` embeds `BinlogCoordinate` inline.
  - The restore reader is updated in the same commit. No backfill (D3).

### 2.7 Booleans and tri-states

#### A-43 `sites[].replicating` becomes a replication state

- **Sev** must · **Brk** yes (status-only) · **Merges** STATUS-05, GPT-27 (MySQL part)
- **Evidence:**
  - `Replicating bool` "indicates whether replication is running" (`types.go:671-672`), but it is set from
    `ReplicationHealthy` (`internal/controller/runner.go:2459`).
  - The lease gate refuses on `!s.Replicating` (`deployment_leases.go:360`).
- **Problem:** `false` means the primary, not yet probed, threads stopped, or lagging.
- **Decision:**
  - `replication SiteReplicationState`, `Enum=Healthy;Lagging;Broken;Unknown;NotApplicable`.
  - `NotApplicable` for the writable primary, `Unknown` before the first probe.
  - Update the lease gate, the kubectl plugin, `triage.sh` and the playground readers.
  - **Served copies (OPUS-R2-10, A-114).** Today `replicating` means both replication threads run with a known
    source, debounced over a streak of polls, whatever the lag (`topology.go:1565-1578`). Two served fields keep
    that meaning:
    - `/deploy/v1` `health.replicationRunning` keeps its name. It is `true` when every non-active spec site has
      a status entry with `replication` in `Healthy` or `Lagging` (`deploy_api.go:337-349`). Lag stays in
      `health.replicationLagSeconds`. `11.deployment-api.md` documents this definition.
    - `/ws/status` `sites[].replicating` becomes `sites[].replication`, carrying the same enum value as the CR.
  - E10 changes both producers and adds a deploy-API test for each enum value.

#### A-44 Dragonfly sync observations distinguish "not observed"

- **Sev** should · **Brk** yes (status-only) · **Merges** GPT-27 (Dragonfly part)
- **Evidence:** `syncInProgress bool` (`dragonfly_types.go:385`) is written as false when INFO did not report
  it. `lastIOSecondsAgo` is covered by A-36.
- **Decision:**
  - `syncInProgress *bool`, set only when INFO actually reported the field. A successful INFO is not enough (R17):
    the parser stores `MasterSyncInProgress` as a plain `bool` (`internal/dragonfly/info.go:30,94`), so a reply
    without the key still yields `false`. The parser gains field-presence tracking, and the writer
    (`dragonfly_topology.go:572`) copies the pointer only when the field was present and parsed.
  - `reachable` and `ready` (`:363,398`) stay plain observations: each has one meaning for `false`.

#### A-45 Backup encryption attribution is a struct

- **Sev** must · **Brk** yes (status-only) · **Merges** GPT-28 (backup part), KINDS-11 · **Owned by #203** for
  the identity fields
- **Evidence:** `encrypted bool` and `encryptionAlgorithm string` (`mysqlbackup_types.go:170,177`).
  `encrypted=false` cannot tell plaintext from unattributed, and `false` plus an algorithm is representable.
- **Decision:**
  - `status.encryption *BackupEncryptionStatus{mode: None|Encrypted, algorithm EncryptionAlgorithm}`. Absent
    means not yet attributed (`MetadataComplete` not True).
  - The `shipstream.io/backup-encrypted` label stays as a selector mirror (A-77 decides its prefix).
  - #203 merges before WS-E (C-10) and writes its attribution fields wherever it chooses. E10 moves whatever
    #203 left (key selector, keyed fingerprint) inside this struct, not as siblings. WS-D is not asked to create
    `status.encryption` early, because that would collide with E10 (OPUS-15).

#### A-46 Encryption coverage observations are optional

- **Sev** should · **Brk** yes (status-only) · **Merges** GPT-28 (coverage part)
- **Evidence:** `SiteEncryptionCoverage` uses plain `bool` for `keyringReadOnly`, `systemTablespaceEncrypted`
  and similar fields (`encryption_types.go:296-312`), filled incrementally from optional probe responses.
- **Decision:** `*bool` for each observed flag; nil means the probe did not report it. Never fill an unknown
  value with false.

#### A-47 Remove `status.encryptionAtRest.sealed`

- **Sev** should · **Brk** yes (status-only) · **Merges** STATUS-11 (`sealed` part), STATUS open question 2
- **Evidence:**
  - `Sealed bool`, "the single field to alert on" (`encryption_types.go:224-230`), written at
    `encryption_reconcile.go:190,910,1230`.
  - Read by `internal/playground/scenarios/s48_keyring_seal_and_rotation.go:137` and
    `s50_encrypted_pod_replace.go:123`.
- **Problem:** `false` conflates "not yet observed", "rotating" and "failed". It duplicates
  `EncryptionAtRestReady`.
- **Decision:**
  - Remove it.
  - Alerting and the docs move to the `EncryptionAtRestReady` condition and its reasons, plus the keyring metrics.
  - Update s48, s50 and `8.encryption-at-rest.md`.

### 2.8 Desired state in status, and spec written by the operator

#### A-48 The Dragonfly snapshot upgrade is requested in spec, and the operator never writes spec

- **Sev** must · **Brk** yes · **Merges** GPT-30, OPS-02 · **Owned by #235** for the shared claim (the
  `plannedFailoverActive` split)
- **Evidence:**
  - The annotation `bloodraven.shipstream.io/dragonfly-snapshot-upgrade=<image>` (`dragonfly_upgrade.go:22`)
    becomes `status.dragonfly.upgrade.targetImage` (`dragonfly_types.go:319`).
  - The operator then sets `fresh.Spec.Dragonfly.Image = image` and calls `r.Update`
    (`dragonfly_upgrade.go:401-402`).
- **Problem:**
  - Desired state enters through an annotation and lives in status.
  - The operator writes the user's spec. A GitOps self-heal reverts it, which re-rolls Dragonfly.
- **Decision:**
  - Remove the annotation.
  - Add `spec.dragonfly.upgradeStrategy` (`Enum=Rolling;SnapshotRestore`, default `Rolling`), with CEL
    `self.upgradeStrategy != 'SnapshotRestore' || (has(self.snapshot) && self.snapshot.dir != '')`.
  - Drift is detected against the live StatefulSet, not against status (OPUS-18). When the strategy is
    `SnapshotRestore` and `spec.dragonfly.image` differs from the Dragonfly container image of the running
    StatefulSet, the operator runs the existing phases. If that image is unknown (no StatefulSet yet), it does
    nothing, so a new group or a group whose status was lost never starts an unrequested upgrade.
  - Add `status.dragonfly.image` as an observation only (the image actually running). Nothing compares spec
    with it.
  - `upgrade.sourceImage` and `upgrade.targetImage` remain as the run record.
  - The operator never writes spec. #235 is expected to treat the upgrade as a claimant triggered by spec drift.

#### A-49 Remove spec echoes from status

- **Sev** should · **Brk** yes (status-only) · **Merges** STATUS-11 (echo part)
- **Evidence:**
  - `status.pitr.enabled` "mirrors spec" (`backup_types.go:158-161`). It is read by the plugin
    (`cmd/kubectl-bloodraven/status.go:215`) and by s31 and s37.
  - `status.dragonfly.enabled` (`dragonfly_types.go:232`), written at `dragonfly_topology.go:1625`.
- **Decision:**
  - Remove both. Consumers read spec and the `PITRArchiveReady` condition.
  - `status.pitr.profileName` stays (A-111).
  - `plannedFailover.dragonfly.enabled` is in A-50.

#### A-50 Planned-failover request and run record

- **Sev** must · **Brk** yes · **Merges** GPT-31, STATUS-12, MFGSPEC-37, OPS (#235 notes) · **Owned by #235**
- **Evidence:**
  - The request is an annotation, `<site>[:maxLagWait=<dur>]` (`planned_failover.go:32`). On a deployment-hold
    deferral the operator re-writes the user's annotation (`planned_failover_deployment.go:33-48`).
  - Status:
    - `target`, `sourcePrimary` (`planned_failover_types.go:115,119`)
    - a free-string `reason` (`:150`)
    - `dragonfly.enabled` (`:199`)
    - `offsetCaptureFailed` plus a sentinel offset (`:215,226`)
    - `promotionMethod` with values containing spaces (`:252`)
  - **The in-flight #235 branch (revision 3, OPUS-R2-03).** WS-C's `fix/issue-235` at `d1e2d05` ("Add the
    planned-failover claim contract (WP0)") was read locally, before it was pushed. It adds a durable-claim record
    next to
    the legacy fields, and several of its served surfaces miss the house rules below:
    - a flat `status.plannedFailover.runId` (`api/v1alpha1/planned_failover_types.go` on that branch);
    - `PlannedFailoverReasonCancelled = "Cancelled"`, the British spelling that A-74 bans;
    - `DragonflyTakeoverMethod` value `REPLTAKEOVER`, and a run-settings `onSyncTimeout` enum `proceed;fail`;
    - a phase enum that keeps `""` and the legacy `Pending`, `Validating` and `Resuming` "while the claim lands",
      next to the legacy flat fields (`target`, `sourcePrimary`, the free-string `reason`, `dragonfly.enabled`,
      `offsetCaptureFailed`, …);
    - `source` and `target` site fields, and `…At` timestamps (`claimedAt`, `intendedAt`, `appliedAt`, …);
    - a `claimBaseline{lastFailover, lastFailoverTarget}` copy of the flat failover record that A-40 reshapes;
    - a new condition `AutomaticFailoverHeld` (reason `PlannedFailoverManualRecovery`), whose run id is parsed
      back out of the condition message (`parsePlannedHoldConditionMessage`,
      `internal/controller/planned_failover_contract.go`);
    - a new outcome enum `PlannedFailoverOutcome` beside the reason;
    - new annotations: the user acknowledgement `bloodraven.shipstream.io/ack-planned-failover-hold=<runId>`, and
      the operator-written `planned-failover-status-fence`, `planned-failover-terminal`
      (`<runId>/<outcome>/<reason>`),
      `planned-failover-stall-since` and `planned-failover-manager-absent-since` (`<runId>/<RFC3339>`), and
      `promotion-gtid-executed`.
- **Decision (revision 3):** #235 owns the semantics of the claim and the record structure. Revision 2 said "WS-E
  does not touch this block", which left a must-fix with no owner while the branch diverged. Ownership is now:
  - **WS-C lands the shape** and is asked to meet the checklist below while its branch is still at WP0.
  - **WS-E's first step** (with the re-triage in section 6) re-audits `spec/status.plannedFailover`, the new
    condition and the new annotations against the same checklist. It fixes every gap in the commit that applies
    that rule elsewhere: E5 (site names), E7 (enums), E8 (timers and timestamps), E10 (`claimBaseline`), E11
    (condition), E12 (Event reasons), E14 (annotations) and E15 (plugin verb). WS-E never changes claim
    semantics: what is fenced, when, and what a field means.
  - So the must-fix always has an owner: WS-C if the shape is right when it lands, WS-E otherwise.
- **The v1beta1 shape checklist for #235 (WS-C and WS-E):**
  1. **Typed CamelCase enums, US spelling.** Every enum value is CamelCase: `Cancelled` → `Canceled`,
     `REPLTAKEOVER` → `ReplTakeover` (`promotionMethod`/`takeover.method` as `ReplTakeover|ReplicaOfNoOne|Skipped`),
     `onSyncTimeout` `Proceed|Fail` (A-27), and spec `onCooldown` a typed `Reject|Defer` or removed. No `""` value.
     The reason set keeps the distinct "nothing sent", "promoted unconfirmed" and `ExecuteFailed` outcomes; a
     separate `outcome` enum next to the `reason` is accepted.
  2. **One record shape.** The v1beta1 freeze carries the durable-claim record only. The legacy phases (`Pending`,
     `Validating`, `Resuming`) and the legacy flat fields are gone: no `dragonfly.enabled`, no
     `offsetCaptureFailed` (a nil offset plus a reason instead), no free-string `reason`.
  3. **Names.** `runID` (the house form, as in `requestID`, `sourceServerUUID` and proposal 21's `runID`), not
     `runId`. `sourceSite` and `targetSite` (A-20), not `source` and `target`. Timestamps use the `…Time` suffix
     (A-40): `claimTime`, `intentTime`, `completionTime`. Any baseline copy of the failover record uses A-40's
     `FailoverRecord{time, target, promotionGtidExecuted}`.
  4. **No desired state in status.** Request intent stays outside status: in the annotation, or, if the request
     moves into spec, in `spec.plannedFailover.request{targetSite, requestID}`, not top-level scalars. Run
     settings are recorded only as the run's frozen effective values. The operator never rewrites the user's
     annotation.
  5. **Condition style.** `AutomaticFailoverHeld` and its reasons are A-69 constants in `conditions.go`, stamped
     with the applied generation (A-68). True only while held (A-72), with a named False reason when an
     acknowledgement clears it. Nothing parses a condition message: the held run's id and reason live in a typed
     status field that the condition summarises.
  6. **Annotations in the Q4 key family.** Every key is under `bloodraven.shipstream.io/` (A-77). User
     acknowledgements spell the verb out in the key and abbreviate it in the plugin, as A-86 does:
     `acknowledge-planned-failover-hold=<runID>` with a `kubectl bloodraven ack-planned-failover-hold` wrapper
     (A-81). Values follow A-78: the run id matches the value charset (a UUID does), and an instant inside a
     value uses the basic form, so `planned-failover-stall-since` and `…-manager-absent-since` drop RFC 3339's
     colons. The operator-written keys are classified in the A-79 page (Internal, or Operator-owned read-only
     for `promotion-gtid-executed`, which sits beside A-104's failover keys), and their structured values follow
     A-80.
  7. **Event reasons follow A-74** (`PlannedFailoverSucceeded` for completion). Metric `reason` labels and the
     `/deploy/v1` `plannedFailover` fields carry the typed constants verbatim (A-114).
  8. **The trigger grammar follows A-78.**

#### A-51 Encryption adoption is a spec policy, not an annotation

- **Sev** should · **Brk** yes · **Merges** OPS-03, OPS open question 4
- **Evidence:** `bloodraven.shipstream.io/encryption-adopt=confirm` (`encryption_escrow.go:58`) is never
  cleared, and is checked only while `status.encryptionAtRest` is nil (`encryption_reconcile.go:89-107`).
- **Decision:**
  - `spec.encryptionAtRest.existingData` (`Enum=Refuse;LeavePlaintext`, default `Refuse`).
  - The `AdoptionRefused` reason and its Event stay. Remove the annotation.

### 2.9 Defaults

#### A-52 No persisted sidecar image default

- **Sev** must · **Brk** yes · **Merges** MFGSPEC-09
- **Evidence:**
  - `+kubebuilder:default="ghcr.io/shipstream/bloodraven-sidecar:1.3.2"` (`types.go:50-53`). It is persisted
    into every object ("CRs persist the CRD default they were created with", `internal/controller/reconciler.go:79`).
  - The chart passes only `BLOODRAVEN_OPERATOR_IMAGE` (`charts/bloodraven/templates/deployment.yaml:91-92`).
  - Skew produces a Warning only (`internal/controller/sidecar_version.go:38,62`).
- **Problem:** operator upgrades never move existing groups' sidecars, and removing a CRD default later does
  not remove the values already written.
- **Decision:**
  - `spec.sidecar.image` is optional with no CRD default. Empty means the sidecar image the operator release
    ships with (new env `BLOODRAVEN_SIDECAR_IMAGE`). A non-empty value is an explicit pin, still reported by
    `SidecarVersionSkew`.
  - **The default is a chart value (OPUS-R2-08).** The chart renders `BLOODRAVEN_SIDECAR_IMAGE` from
    `sidecar.image.repository` (default `ghcr.io/shipstream/bloodraven-sidecar`) and `sidecar.image.tag` (default
    `.Chart.AppVersion`), the same pattern as the operator's `image.{repository, tag}`
    (`charts/bloodraven/values.yaml:9-14`). A mirrored or air-gapped registry then sets one value, instead of
    pinning every group, which would bring back the "never moves" behaviour this finding removes. The C-8 E2E
    sets both to the branch build (`bloodraven-sidecar:playground`, `.github/workflows/_e2e.yml:120`), so the
    rollout assertion sees a real image change. The critique suggested the key `sidecarImage.{repository, tag}`;
    `sidecar.image` is used instead so that no values file carries the old spec field name that E19's gate
    looks for.
  - `spec.sidecarImage` → `spec.sidecar.image`, and `spec.sidecarResources` → `spec.sidecar.resources`.
  - `spec.sidecar` stays a value type (`types.go:117`) and gains `+kubebuilder:default={}` (OPUS-17). A manifest
    without the block then still gets the `leaseTimeout` and `peerCheckInterval` defaults (`types.go:463,468`),
    and the struct-level rules, including the 3× rule (`:459`), still run. A pointer would skip both.
- **Cost (OPUS-07):** with no pin, an operator upgrade changes the effective pod spec of every unpinned group.
  Each one then runs an `OrderedUpdate`: replica restart, one planned failover, old-primary restart
  (`6.operations/10.upgrade-policy.md:149-152,227-230`). Today an operator upgrade never touches the data plane,
  and a sidecar bump is a deliberate `spec.sidecarImage` edit.
- **Mitigation chosen: serialized, health-gated rollouts driven by the default.**
  - The operator runs at most one default-driven sidecar rollout at a time across all the groups it manages
    (flag `--max-concurrent-default-sidecar-rollouts`, default `1`).
  - The next group starts only after the previous group's ordered update has finished with `Ready=True`. So a
    bad sidecar build stops at the first group.
  - **Liveness (OPUS-R2-09).** A run that never starts must not hold the queue for every other tenant. Ordered
    updates can be held or refused by actors unrelated to the sidecar: a group under restore, a group that is
    not `Ready`, and on the #235 branch the `AutomaticFailoverHeld` hold, which blocks ordered updates until an
    administrator acknowledges it. The queue therefore separates two cases:
    - **Not admitted.** A group is offered the head of the queue only when it is `Ready=True`. If its ordered
      update has not started (`status.updatePhase` still empty) within
      `--default-sidecar-rollout-admission-timeout` (default 10m), it is skipped and re-queued at the back with
      `Updating=False/SidecarRolloutDeferred`, and the next group is offered the slot.
    - **Started and stalled.** Only a run that started and did not reach `Ready=True` holds the queue. That group
      shows `Updating=True/OrderedUpdateStalled` (A-72), and the operator emits the Warning Event
      `SidecarDefaultRolloutHalted` once. This is the bad-build case, and stopping there is the point.
  - A waiting group reports `Updating=False/SidecarRolloutQueued`. Each start emits the Normal Event
    `SidecarDefaultRolloutStarted`.
  - A group with `spec.sidecar.image` set never rolls on an operator upgrade.
  - Tests: E13 unit and component tests for the queue, the gate, the admission-timeout skip (a held group is
    deferred and the next group rolls) and the halt (a started run that never reaches `Ready=True` stops the
    queue). The C-8 E2E asserts that after the chart upgrade an unpinned group runs exactly one ordered update
    with one planned failover, and a pinned group runs none.
  - Docs: the upgrade policy's "Sidecar ↔ operator skew" section, the release notes and the course (unit 7
    teaches "bump `spec.sidecarImage`"; see OPUS-06 in section 6).
- **Rejected mitigations:**
  - An observed `status.sidecar.image`, a `SidecarUpgradePending` condition and a one-shot `roll-sidecar`
    action. Every release would then need an imperative step per group, which GitOps cannot commit (A-81), so
    groups would drift past the one-minor skew window.
  - A required explicit pin with a `kubectl bloodraven` bump helper. It keeps today's "never moves" behaviour,
    which is the problem this finding fixes, and it puts a release-specific image tag in every manifest.
  - No mitigation at all: fleet-wide concurrent failovers on every `helm upgrade`.

#### A-53 No MysqlDatabase collation default

- **Sev** should · **Brk** yes (an unchanged manifest that omits `collation` creates new schemas with the server
  default instead; R15) · **Merges** KINDS-23, KINDS open question 5
- **Evidence:** `+kubebuilder:default=utf8mb4_unicode_ci` (`mysqldatabase_types.go:235-237`), the legacy UCA
  4.0 collation, is persisted into every object.
- **Decision:**
  - Remove the default. When `collation` is unset, render no `COLLATE`; MySQL uses the character set's
    default (`utf8mb4_0900_ai_ci` on 8.0 and later).
  - The `characterSet` default `utf8mb4` stays.
  - Migration: set `collation: utf8mb4_unicode_ci` explicitly to keep the old behaviour (section 3).

#### A-54 Security-context defaults for MySQL and Dragonfly pods

- **Sev** must (decide before the freeze) · **Brk** yes if hardened · **Merges** MFGSPEC-32
- **Evidence:**
  - MySQL pods use the user value as-is, or none (`types.go:211-226`; `internal/controller/reconciler.go:1693,1711`).
  - Dragonfly behaves the same (`dragonfly_types.go:87-96`).
  - Backup Jobs merge user fields over hardened defaults (`backup_types.go:89-102`;
    `internal/controller/backup_job.go:511`).
  - Jobs hard-code uid and gid 27 (`internal/controller/backup_job.go:497-499,514`).
- **Decision (MBOD Q6 = "A-gated"):** the API shape is the same whichever way the gate goes: every
  `podSecurityContext`/`containerSecurityContext` field (MFG, `backup`, `dragonfly`) is merged by one helper,
  with user fields winning. Only the defaults depend on the gate.
  - **WS-E ships hardened defaults merged under user fields:**
    - One uid and gid, 999, for MySQL, Dragonfly and every Job. This replaces the hard-coded 27.
    - RunAsNonRoot, RuntimeDefault seccomp, drop ALL, no privilege escalation.
    - fsGroup 999 with `fsGroupChangePolicy: OnRootMismatch`.
    - The launcher's root filesystem writes, `/tmp` and `/var/run/mysqld` move to emptyDirs.
    - No root init phase, and no per-image branch.
  - **Gate (timeboxed in WS-E):**
    - Restricted-PSS namespace admission.
    - Fresh init and restart on an existing volume.
    - Keyring seal and rotation, PITR archiving, backup and restore, and a chaos failover.
    - All of it on both supported images, and on local-path plus one CSI storage class.
    - **The gate's main risk is the image uid (OPUS-25).** Jobs hard-code uid 27 "to match the stock MySQL
      image" (`backup_job.go:497-499,509,514-515`). The backup and restore image defaults to the Oracle
      `container-registry.oracle.com/mysql/community-server:9.7` (`backup_types.go:32`), whose `mysql` user is
      uid 27, while the server image `mysql:9.7` (`types.go:46-48`) uses 999. Running the Oracle image as 999
      means a uid it does not define. The gate therefore runs Jobs on that image as uid 999 with an emptyDir
      `HOME`, and verifies mysqlsh dump, load and PITR replay there. If that fails, the fallback is a per-image
      uid table, and the record must drop the "no per-image branch" condition explicitly.
  - **If the gate fails or cannot be completed:** ship C, which is the same merge helper with empty MySQL and
    Dragonfly defaults, and record why in the PR.
- **Rejected:** B, as-is semantics with renamed backup fields; it freezes two meanings for one field name. Also
  C without attempting the gate.

#### A-55 One retention shape for backups and verifications

- **Sev** must · **Brk** yes · **Merges** GPT-20, MFGSPEC-10, KINDS-34, GPT-19 (retention part), GPT open
  question 3
- **Evidence:**
  - The legacy `retention` shorthand (default 7) is fully overridden by a set `retentionPolicy`
    (`backup_types.go:288-300`; `internal/controller/backup_reconciler.go:1347-1365`).
  - `RetentionPolicy{count, maxAgeDays, minKeep, maxFailedKeep}` (`backup_types.go:403-424`).
  - Pruning keeps a successful backup when any rule keeps it: minKeep, count or age
    (`backup_reconciler.go:1377-1413`). So `minKeep` duplicates `count`.
  - Verification uses `keepSuccessful` and `keepFailures` (`mysqlbackupverification_types.go:443-456`).
- **Decision:**
  - Profile: `retention: {keepLast int32 (default 7, Minimum=1), keepWithinDays *int32 (Minimum=1), keepFailed
    *int32 (default 10, Minimum=0)}`, with `+kubebuilder:default={}` on the parent.
  - Verification: `verification.retention: {keepLast *int32 (default 30, Minimum=0), keepFailed *int32
    (default 10, Minimum=0)}`.
  - Documented semantics: a successful backup is pruned only when it is outside `keepLast` and older than
    `keepWithinDays`.
  - Remove `minKeep`, the "0 disables" meanings and the legacy `retention` integer. Under keep-if-any, `count` and
    `minKeep` combine as their maximum, so a v1.6.0 policy maps to `keepLast = max(count, minKeep, 1)` (R07).
    For example, `count: 2, minKeep: 7` keeps seven today and must become `keepLast: 7`.
  - The v1.6.0 "no pruning" configurations, `retention: 0` without a policy, or a policy with `count: 0` and
    `maxAgeDays: 0` (`pruneSuccessful` returns early, `backup_reconciler.go:1377-1380`), have no equivalent. The
    new shape always prunes. Section 3 tells the user to choose an explicit `keepLast`, and no finite default is
    applied for them silently.
  - `keepLast >= 1` keeps the "never delete the last good backup" floor.
- **Disagreement:** names differed across auditors (`count`/`minKeep`, `successfulCount`, `keepSucceeded`).
  Chose `keepLast`/`keepWithinDays`/`keepFailed`: the names state the keep-if-any semantics, which no auditor's
  names did.

### 2.10 Dead, inert or reserved fields

#### A-56 Remove spec fields that are accepted and ignored

- **Sev** must · **Brk** yes · **Merges** GPT-25 (spec part), MFGSPEC-11, KINDS-15, MFGSPEC open question 4
- **Evidence:**
  - `loadOptions.resetProgress` "currently has no effect and is kept for compatibility"
    (`backup_types.go:670-676`).
  - MBV `spec.storage` and profile `verification.storage` (`mysqlbackupverification_types.go:71,410`; type at
    `:192-206`): "accepted … but not applied". The Job uses an emptyDir, and `autoSizeVerificationPVC` is not
    wired (`internal/controller/backup_verification_job.go:68-77`). Only writers copy it
    (`cmd/kubectl-bloodraven/verify.go:159`, `cmd/bloodraven/trigger_verification.go:103-104`).
  - `s3.storageClass` is "Informational; not enforced" (`backup_types.go:482-484`); no reader.
- **Decision:**
  - Delete all three, plus the `VerificationStorage` type.
  - Fix the "can't share a PVC" rationale on `verification.concurrencyPolicy`
    (`mysqlbackupverification_types.go:385-390`).
  - Ephemeral disk bounds go through `spec.resources.limits.ephemeral-storage`.
  - Adding a working S3 storage class later is additive.

#### A-57 Remove MysqlBackupVerification status fields nothing writes

- **Sev** must · **Brk** yes (status-only) · **Merges** GPT-25 (status part), KINDS-16 (field part)
- **Evidence:** `podName`, `pvcName` and `serviceName` (`mysqlbackupverification_types.go:262,265,269`) have no
  writer in `internal/` or `cmd/`. The only reader is `internal/playground/scenarios/backup_helpers.go:737`.
- **Decision:** delete all three. `jobName` stays.

#### A-58 TLS is a Secret reference only

- **Sev** must · **Brk** yes · **Merges** MFGSPEC-12
- **Evidence:** `tls.issuerRef` is required (`types.go:416`; `IssuerRef` at `:425-434`), but nothing outside
  `api/` reads it, and the operator has no `certificates` RBAC.
- **Decision:**
  - `tls: {secretName}` only (Q3=B keeps the core spelling). The Secret holds `tls.crt`, `tls.key` and `ca.crt`.
  - Operator-managed certificates, if ever wanted, add an optional `issuerRef{name, kind, group}`. That is
    additive.

#### A-59 Remove `restoreInPlace.pointInTime`

- **Sev** must · **Brk** yes · **Merges** MFGSPEC-23, GPT-46 (in-place part), OPS open question 5 · **Owned by
  #205**
- **Evidence:** the field is rejected at execution, and "kept so existing objects still validate"
  (`backup_types.go:802-811`).
- **Decision:**
  - Delete it.
  - #205 is expected to re-add `restoreInPlace.pointInTime` together with a `staging` block, as one additive
    change, reusing A-18's `ProvisionedVolumeSpec` body and A-30's scope.
  - Proposal 21 (#205, WS-B) is written against today's names (`mysqlBackupRef`, `stopDatetime`, a string
    `scope`, `status.location`). Its implementation lands after WS-E and uses the v1.7.0 names: `backupRef`
    (A-04), `stopTime` (A-39), `scope.type` (A-30) and `artifactStorage.location` (A-96). Its hold key follows the
    A-77 hold family (OPUS-16).

#### A-60 MSC `transport` admits only what is implemented

- **Sev** must · **Brk** yes · **Merges** GPT-26 (transport part), KINDS-27
- **Evidence:**
  - `Enum=ObjectStore;Network` (`mysqlstandbycluster_types.go:64,87`).
  - Both spec rules start with `self.transport != 'ObjectStore' || …` (`:55-56`), so `Network` passes admission
    and fails only at runtime.
- **Decision:** `Enum=ObjectStore`, default `ObjectStore`. A real second transport is an additive value later.

#### A-61 Remove MSC status for unimplemented phases

- **Sev** must · **Brk** yes (status-only; never written) · **Merges** GPT-26 (status part), STATUS-22, KINDS-28
- **Evidence:**
  - `status.lastVerified`, `status.activation` (`StandbyActivationPhase`, `confirmTokenUsed`) and
    `status.materializedFailoverGroup` (`mysqlstandbycluster_types.go:196,201,206`).
  - The condition constants `Restorable`, `ActivationInProgress` and `Active` (`:459,464,468`).
  - The `Phase` printer column (`:14`).
  - No writer exists for any of them.
- **Decision:** delete them all. Phase 2 and 3 re-add their status additively when they are built.

#### A-62 Remove MSC `spec.template`

- **Sev** must · **Brk** yes · **Merges** GPT-26 (template part), KINDS-31, KINDS open question 1
- **Evidence:**
  - `Template StandbyFailoverGroupTemplate` is required (`mysqlstandbycluster_types.go:76`), embeds the whole
    `MysqlFailoverGroupSpec` (`:161`), and is read by nothing in `internal/` or `cmd/`.
  - It is 237 KB of MSC's 257 KB compact schema, and carries all 20 MFG rules into MSC.
  - Every MFG schema change would also need an MSC conversion once versions diverge.
- **Decision:**
  - Delete it, and update `examples/standby-cluster.yaml:54` and
    `site/content/docs/5.architecture/3.multi-cluster-dr.md:651`.
  - Activation (Phase 3) brings its own request shape, which may reference or embed a spec. Design note for
    that work: the materialized MFG must not carry an ownerReference to the standby, or deleting the standby
    would garbage-collect the promoted group.
- **Disagreement:** KINDS recommended keeping the template with CEL that excludes activation-owned fields.
  Rejected: it is inert, it doubles the schema D1 serves twice, and re-adding it later is additive.

#### A-63 MSC `source.storage` → `source.s3`

- **Sev** should · **Brk** yes · **Merges** KINDS-29 · **Owned by #206/#203** for any identity fields on the
  prefix
- **Evidence:** `source.storage BackupStorage` (`mysqlstandbycluster_types.go:125`) needs two spec rules to
  forbid its PVC arm and require a prefix (`:55-56`). A PVC is always an error at runtime.
- **Decision:**
  - `source.s3 StandbyS3Source`, required: the `S3Storage` fields, with `prefix` required (`MinLength=1`).
  - Drop both rules.

#### A-64 MSC discovery status is structured

- **Sev** should · **Brk** yes (status-only) · **Merges** KINDS-33
- **Evidence:**
  - A flat struct of `dump*` and archive fields (`mysqlstandbycluster_types.go:216-283`).
  - `message` duplicates the conditions (`:282`).
  - The `Bucket` printer column shows `.status.discovered.dumpLocation` (`:10`).
- **Decision:**
  - `status.discovered: {lastScanTime, latestDump{name, location, completionTime, gtidExecuted, sizeBytes},
    binlogArchive{manifestCount, fileCount, sizeBytes, oldestEventTime, newestEventTime}}`.
  - Drop `message`.
  - Printer columns: `Dump`, `NewestBinlog` (date) and `Age`. The `Ready` column is added in E11 with A-70's
    condition (A-94); revision 2 listed it here, in E2, before anything writes it (OPUS-R2-14).

#### A-65 `spec.triggeredBy` moves to metadata

- **Sev** should · **Brk** yes · **Merges** KINDS-06, OPS-10 (`triggeredBy` part)
- **Evidence:** `triggeredBy` on MBK and MBV (`mysqlbackup_types.go:58`, `mysqlbackupverification_types.go:103`)
  is written by `backup_schedule.go:635`, `cmd/bloodraven/trigger.go:84` and `cmd/kubectl-bloodraven/backup.go:128`
  and `verify.go:149`. Nothing reads it.
- **Decision:** delete it. Writers set the annotation `bloodraven.shipstream.io/triggered-by` (A-77 prefix).

#### A-66 `sidecar.bloodravenAddress` → `sidecar.operatorEndpoint`

- **Sev** should · **Brk** yes · **Merges** MFGSPEC-30
- **Evidence:** `BloodravenAddress string` (`types.go:471-473`) overrides the chart's
  `BLOODRAVEN_DEFAULT_AUXILIARY_ADDRESS` (`charts/bloodraven/templates/deployment.yaml:95`).
- **Decision:** `operatorEndpoint` (`host:port`, `MaxLength=253`, pattern `^[A-Za-z0-9.-]+:[0-9]{1,5}$`).

#### A-67 Dragonfly snapshot S3 field names match backup S3

- **Sev** should · **Brk** yes · **Merges** MFGSPEC-27
- **Evidence:**
  - `snapshot.s3Endpoint` (`dragonfly_types.go:128`), whereas backup S3 uses `endpointURL` with a URL pattern
    (`backup_types.go:469`).
  - The two are not the same format (R06). `s3Endpoint` goes verbatim to Dragonfly's `--s3_endpoint`, which takes
    `host:port` (`dragonfly_resources.go:286-288`; the documented value is `rustfs.default.svc.cluster.local:9000`,
    `4.configuration/5.app-integration.md:131`). HTTPS is a separate `s3UseHTTPS *bool` (`dragonfly_types.go:134`,
    rendered at `dragonfly_resources.go:290-291`).
- **Decision:**
  - `s3Endpoint` and `s3UseHTTPS` → one `s3EndpointURL`, with the backup pattern narrowed to no path
    (`^https?://[^\s/?#@]+/?$`). One URL convention then covers every S3 endpoint in the API.
  - The operator adapts the URL for Dragonfly: the URL's host and port become `--s3_endpoint=<host:port>`, and
    the scheme becomes `--s3_use_https=<true|false>`. Unit tests pin both flags for `http://` and `https://`,
    with and without a port.
  - An empty `s3EndpointURL` still means AWS S3, as an empty `s3Endpoint` does today (`dragonfly_types.go:125-128`).
  - `s3SignPayload` stays (A-103).
  - `credentialsSecretName` already matches Q3=B.
  - `dir` stays a raw passthrough of Dragonfly's own flag.

### 2.11 Conditions and observedGeneration

#### A-68 MFG `status.observedGeneration`, and honest condition generations

- **Sev** must · **Brk** no (additive field; condition generations become accurate) · **Merges** GPT-34,
  STATUS-01
- **Evidence:**
  - MFG status has no `observedGeneration`. Only the encryption sub-status has one (`encryption_types.go:241`).
  - Conditions are stamped with `freshFG.Generation`, the generation of the object being written
    (`internal/controller/runner.go:1479-1516`), not the generation the topology manager applied.
  - ArgoCD health compares `Ready.observedGeneration` with `metadata.generation`
    (`charts/bloodraven/argocd/argocd-cm-patch.yaml:46-47`).
- **Decision:**
  - Add `ObservedGeneration int64` (`+optional`, `Minimum=0`).
  - The topology manager snapshot carries the generation of the config it applied. Every MFG condition, and
    `status.observedGeneration`, uses it.
  - Writers that own one condition stamp the generation they evaluated.
  - Point the MFG Lua at `status.observedGeneration`. The Lua already does this for other kinds (`:112-113`).
  - `encryptionAtRest.observedGeneration` stays, documented as the encryption reconcile's own generation.

#### A-69 Condition types and reasons are exported from `api/v1alpha1`

- **Sev** should · **Brk** no · **Merges** GPT-35, STATUS-16, KINDS-04 (constants part)
- **Evidence:**
  - MFG condition types and reasons are inline literals (`runner.go:1484`).
  - MBK, MBV and MDB constants live in `internal/controller` (`backup_verification_reconciler.go:35-36`).
  - Only MSC exports them (`mysqlstandbycluster_types.go:455-468`).
- **Decision:**
  - `api/v1alpha1/conditions.go` holds every type and reason, per kind.
  - The controllers use only these constants.
  - A unit test asserts that every reason the controllers set is declared there.

#### A-70 `Ready` on every kind

- **Sev** should · **Brk** no (additive) · **Merges** STATUS-17, GPT-35 (summary part)
- **Evidence:**
  - MBV has only `Verified` and `ResourcesCleanedUp` (`backup_verification_reconciler.go:35-36`).
  - MSC has only `BucketReadable` and `SourceConfigKnown`.
  - MBK writes `Ready` only on Pending or terminal paths.
- **Decision:**
  - MBV `Ready` mirrors `Verified`, with `False/InProgress` while running.
  - MSC `Ready=True` when both conditions are True, otherwise False with the first failing reason.
  - MBK writes `Ready=False/Pending|Running` from creation.
  - Add `Ready` printer columns to MBK, MBV and MSC.
- **Disagreement:** KINDS said this can wait. Chosen now: its reasons should freeze with the rest.

#### A-71 MFG `Ready` reasons say why

- **Sev** should · **Brk** yes (reason values) · **Merges** STATUS-18
- **Evidence:** `readyReason := "TopologyPolled"` for both True and False (`runner.go:1484`). STATUS said nothing
  keys on it, but `runner_test.go:33,43` and the course fixtures under `course/.../tests/fixtures/*.json` do.
- **Decision:**
  - True → `Healthy`.
  - False → `NoWritablePrimary` | `ReplicationUnhealthy`.
  - Unknown → `Initializing`.
  - Update the tests and the course fixtures.

#### A-72 Condition polarity

- **Sev** should · **Brk** yes (reason and status values) · **Merges** STATUS-19
- **Evidence:**
  - `Bootstrapping=True/Failed` (`runner.go:2405`) and `False/Done` kept forever.
  - `Updating=False/OrderedUpdateStalled`, read by the Lua (`argocd-cm-patch.yaml:77`).
  - `Degraded` reasons `Degraded` and `Alert`.
  - s19 asserts `Bootstrapping False/Failed`, which is never written
    (`internal/playground/scenarios/s19_reclone_interlock.go:377`).
- **Decision:**
  - `Bootstrapping` is True only while a clone runs. On failure: `False/BootstrapFailed` plus
    `Degraded=True/BootstrapFailed`. Otherwise `False/NotBootstrapping`; drop `Done`.
  - `Updating=True/OrderedUpdateStalled` while stalled.
  - `Degraded` reasons `Degraded` → `SiteUnhealthy`, `Alert` → `CrossSiteAlert`.
  - Update the Lua (`:37,69,77`), the deploy-API docs, `crd-reference.md` and s19.

#### A-73 One meaning per reason token

- **Sev** should · **Brk** yes (minor reason renames) · **Merges** STATUS-26, STATUS-27
- **Evidence:**
  - MBK `NoHealthySource` is used for both terminal failure and Pending
    (`internal/controller/backup_reconciler.go:222,224`).
  - MDB `GroupNotFound` is Pending, while on MBK and MBV it is terminal.
  - `PITRArchiveReady` is both a condition type and an Event reason.
  - The deploy API returns raw condition reasons (`deployment_leases.go:270`).
- **Decision:**
  - Pending uses `WaitingForHealthySource`, and MDB Pending uses `GroupNotYetCreated`.
  - The Event `PITRArchiveReady` → `PITRArchiveRecovered`.
  - Document the closed `unstableReason` set in `11.deployment-api.md`, sourced from the A-69 constants.
  - `DivergentTransactions` stays on both conditions: it is the same fact.

### 2.12 Event reasons

#### A-74 One naming rule for Event reasons

- **Sev** should · **Brk** yes (Event contract) · **Merges** STATUS-23, STATUS open question 3
- **Evidence:**
  - Mixed success words: `PlannedFailoverCompleted` (`planned_failover_reconciler.go:796`) next to
    `*Succeeded` and `*Complete`.
  - British spelling: `RestoreInPlaceCancelled` (`restore_inplace.go:431,1314`).
  - Mixed acronyms: `PointInTime*` next to `PITR*`.
  - Mixed refusal words: `Rejected` and `Refused`.
- **Decision:**
  - Rule: `<Subject><PastTenseVerb>`. `Succeeded`/`Failed` for operations, `Rejected` for user requests that
    are not admitted, `Refused` for automatic actions blocked for safety. US spelling, and `PITR` is the only
    acronym form.
  - Apply STATUS-23's rename table in one commit, together with A-33, A-73, the log-schema doc, the doctor skill
    and runner assertions:
    - `PlannedFailoverCompleted` → `PlannedFailoverSucceeded`: E12 renames whatever completion Event #235 leaves.
      #235 merges first (C-10). Revision 3 asks it to use the new name (the A-50 checklist), which reverses OPUS-15
      for this one block because the branch is still at WP0.
    - `DragonflyPromotionCompleted` → `DragonflyPromotionSucceeded`
    - `DragonflyUpgradeCompleted` → `DragonflyUpgradeSucceeded`
    - `RecoveryComplete` → `OldPrimaryRecovered`
    - `RestoreReclonesComplete` → `RestoreReclonesDrained`
    - `RestoreInPlaceCancelled` → `RestoreInPlaceCanceled`
    - `PointInTimePartial` → `PITRReplayPartial`
    - `PointInTimeCrossSiteUnverified` → `PITRCrossSiteUnverified`
    - `KeyringRotationRefused` → `KeyringRotationRejected` (update s48)
    - `EncryptionAdoptionRefused` → `EncryptionAdoptionRejected`
    - `PromotionRefusedPendingReclone` → `PromotionRefusedReclonePending`
    - `KeyringPhase` → `KeyringPhaseChanged`
    - `MetadataParseFailed` → `MetadataUnreadable`
  - The six reasons added since `3fa1900` (section 1.4) go through the same rule in E12. Today
    `RestorePeerReplicationIncomplete` and `RestoreInPlaceWaitingForPreviousQueue` are not `<Subject><PastTenseVerb>`.
  - Release notes carry the old → new table.
- Settled (Event reasons are not schema, but v1.7.0 is the cheap moment).

#### A-75 One reason, one Event type, one emission

- **Sev** should · **Brk** yes (minor Event contract) · **Merges** STATUS-24
- **Evidence:**
  - `DragonflyPromotionCompleted` is Normal at `dragonfly_topology.go:1318` and Warning at `:1402`, duplicating
    `DragonflySessionsLost` at `:1404`.
  - `DatabaseDropSkipped` is Normal at `mysqldatabase_reconciler.go:756` and Warning at `:973`; at `:1081,1086` it
    is used for an owner-user skip.
  - `RestoreInPlaceFailed` is emitted at `restore_inplace.go:1005` and again through `:1007`.
  - `KeyringPhase` picks its type at runtime (`encryption_reconcile.go:184`).
- **Decision:**
  - Drop the Warning at `:1402`.
  - Use `OwnerUserDropSkipped` at `:1081,1086`, and rename the Normal case at `:756` to `DatabaseDropNotNeeded`.
  - Remove the emit at `restore_inplace.go:1005`.
  - `KeyringPhaseChanged` is always Normal; Warning-worthy transitions use their specific reasons.
  - The advisory `RestoreInPlaceRejected` becomes `RestoreInPlaceConfirmIgnored`.

#### A-76 A generated Event-reason registry

- **Sev** should · **Brk** no · **Merges** STATUS-25
- **Evidence:**
  - `2.monitoring.md:353` says Events are emitted only on MFG and MBK.
  - The page lists 67 of 158 reasons, including two that nothing emits (`:427`, `:430`).
- **Decision:**
  - Every reason is a constant in `api/v1alpha1/events.go`.
  - The docs table is generated from it.
  - A test fails on an `Eventf` with an unregistered reason, and on a documented reason missing from the
    registry.

### 2.13 Annotations and labels as API

#### A-77 Every Bloodraven key uses `bloodraven.shipstream.io/`, and label booleans are `true`/`false`

- **Sev** must · **Brk** yes · **Merges** OPS-05, OPS-06, GPT-40 (prefix part), OPS open question 1
- **Evidence:** the section 1.3 table:
  - Routing and ownership labels, internal annotations and finalizers use `shipstream.io/`
    (`internal/controller/reconciler.go:48-62`, `backup.go:19-28`, `backup_verification.go:14-17`,
    `mysqldatabase_reconciler.go:44`).
  - User actions use `bloodraven.shipstream.io/`.
  - `healthy` is `yes`/`no` (`reconciler.go:1235`), and `dragonfly-traffic` is `enabled`.
- **Decision (MBOD Q4 = A):**
  - **Keys:** every operator-owned or user-facing label, annotation, finalizer and the taint (A-21) moves to
    `bloodraven.shipstream.io/`. Examples:
    - labels `bloodraven.shipstream.io/{failover-group,site,role,healthy,dragonfly-role,dragonfly-traffic,
      mysqlbackup,backup-profile,backup-schedule,resource,backup-encrypted,keyring-version,…}`
    - finalizers `bloodraven.shipstream.io/{failover-group,mysqlbackup,mysqlbackup-verification,mysqldatabase}`.
      The MFG finalizer is renamed too: today it is `shipstream.io/graceful-shutdown` (`reconciler.go:48`), and it
      becomes `bloodraven.shipstream.io/failover-group`, so every finalizer is named after its kind (OPUS-24).
    - **Holds (R10, OPUS-09):** `hold.verification.shipstream.io/<verification UID>` becomes
      `verification-hold.bloodraven.shipstream.io/<verification UID>`. Holds form one family,
      `<kind>-hold.bloodraven.shipstream.io/<id>`, with the same subdomain-prefix shape as the A-21 taint, so
      every hold of a kind is found by one prefix match. Proposal 21's `hold.restore.shipstream.io/<runID>` (#205)
      becomes `restore-hold.bloodraven.shipstream.io/<runID>`. The value stays the holder's name. Consumers that
      move with it: `BackupHasVerificationHold` (`backup_verification_hold.go`), retention, the artifact-cleanup
      finalizer, the PITR binlog cutoff (`cmd/bloodraven/pitr_retention.go:53`), doctor `triage.sh:180-183`,
      `support-bundle.sh:110` and the `SKILL.md`, playbook and alert-matrix text. A verification that is in flight
      across the key change needs no dual-key code: no group or backup crosses from v1.6.0 to v1.7.0 in place
      (section 3), so every hold a v1.7.0 operator sees was written with the new key. E14 adds tests that the new
      key protects a backup under an active verification and under a Terminating one.
    - tooling keys `playground.bloodraven.shipstream.io/{in-progress,created-by-e2e,scenario,purpose}`
  - **Unchanged:** the `app.kubernetes.io/*` labels, and the API group `shipstream.io` (a program decision).
  - **Values:** boolean label values are `"true"`/`"false"` everywhere.
  - **Immutable selectors:** a Deployment or StatefulSet whose immutable selector differs from the one the
    operator renders is not patched. The operator refuses that site's workload with
    `Degraded=True/WorkloadSelectorMismatch` and a Warning Event of the same reason naming the object. It does
    not retry the update in a loop. This is kept as general hygiene, because any immutable-field update error
    should become a condition rather than a hot loop.
  - No dual-key code: the operator reads and writes only the new keys.
  - **Q4's upgrade-runbook conditions lapse (section 8.1).** Q4 assumed an in-place upgrade. Its runbook recreated
    workloads one site at a time, never deleted the MFG, and reported old finalizers with a
    `LegacyFinalizerPresent` Event. Section 3 establishes that no v1.6.0 object is upgraded in place. No supported
    sequence then puts an old key or an old finalizer on an object a v1.7.0 operator manages, so WS-E builds
    neither the runbook nor the finalizer report. "Never delete a MysqlFailoverGroup to recreate it" stays a
    general rule (section 3).
  - This is bundled with the namespace-scoped taint key (A-21), so users write selectors, NetworkPolicies and
    tolerations against the new keys once, in the new group's manifests.
- **Disagreement:** GPT-40 wanted to keep the routing labels as they are. Rejected by MBOD: the vendor-wide
  prefix collides with other software from the same vendor, and renaming is cheap only now.

#### A-78 One value grammar for one-shot action annotations

- **Sev** should · **Brk** yes (reclone divergent form only) · **Merges** OPS-04 (annotation part), GPT-38
  (grammar part)
- **Evidence:**
  - Reclone accepts `<site>:confirm=<group>` or a positional `<site>:<gtid-prefix>`
    (`internal/controller/reclone.go:15-21,97,118`).
  - Planned failover uses `<site>:maxLagWait=<dur>`.
  - Adoption uses a literal `confirm` (moved to spec by A-51).
- **Decision:**
  - Grammar `<subject>(:<key>=<value>)*` (OPUS-14).
    - `<subject>` is a site name or an operation id.
    - A pair splits at its first `=`. Values match `^[A-Za-z0-9._/+-]+$`, so a value never contains `:` or `=`,
      and the split is unambiguous.
    - An instant inside a value uses the ISO 8601 basic UTC form, for example `20261104T100000Z`, and is compared
      as an instant. RFC 3339's colons would break the split.
  - Destructive actions require `confirm=<metadata.name>`. A request that must not apply to a re-created group of
    the same name also carries `uid=<metadata.uid>` (R12).
  - The reclone divergent form becomes `<site>:gtidPrefix=<p>`. `<p>` is a prefix of the divergent GTID's
    source UUID: hex and `-`, at least 8 characters (`minRecloneGtidPrefix`). Today the operator writes the GTID
    up to its first comma, interval included (`reclone.go:148-152`), so the value can contain `:`. It then
    writes the UUID part only.
  - #235 and #204 are expected to follow this grammar.

#### A-79 Well-known keys are exported and documented with a stability level

- **Sev** should · **Brk** no · **Merges** OPS-07, OPS-08, GPT-38 (documentation part)
- **Evidence:**
  - The plugin and runner copy key strings with "must match internal/controller" comments
    (`cmd/kubectl-bloodraven/promote.go:21`, `reclone.go:18`, `internal/playground/chaos/actions.go:212,230`).
  - `crd-reference.md:793-799` omits `revoke-deployment-leases`.
  - Finalizers are documented nowhere.
- **Decision:**
  - Add `api/v1alpha1/well_known.go` (constants plus `ReadOnlyTaintKey`) and use it everywhere.
  - Add one generated page, `4.configuration/12.well-known-metadata.md`, with key, kind, writer, value grammar
    and stability: `Public`, `Operator-owned read-only` or `Internal (may change)`.
  - Internal keys: `spec-hash`, `credential-hash`, `system-schema-restriction`, `managed-service-annotations`,
    `restore-confirm`, `deployment-lease*`, the Lease labels `deployment-kind` and `deployment-state`
    (`deployment_leases.go:154-155`; OPUS-24), `verification-backup`, `backup-retry-*`,
    `cleanup-failure-recorded`.
  - Operator-owned read-only: the hold family (`<kind>-hold.bloodraven.shipstream.io/<id>`, A-77). Users read it
    to see why a backup is kept, and removing one by hand while its holder runs can delete data under it.

#### A-80 Structured internal annotations carry a schema version

- **Sev** should · **Brk** no · **Merges** GPT-39
- **Evidence:** JSON payloads in `bloodraven.shipstream.io/deployment-lease`
  (`internal/controller/deployment_leases.go:30`) and `shipstream.io/verification-backup`
  (`backup_verification_source.go:17`) drive recovery decisions.
- **Decision:**
  - Each payload gets a `schemaVersion` member.
  - An unknown version is refused with a condition and an Event. A malformed payload never proves attribution
    (D3).

#### A-81 Action annotations are GitOps-safe and have plugin wrappers

- **Sev** should · **Brk** no · **Merges** OPS-12
- **Evidence:**
  - The actions are consumed and cleared (`runner.go:595-672`, `deployment_leases.go:800-835`).
  - `6.operations/11.gitops.md` does not warn that a committed action is re-applied on every sync.
  - The plugin wraps only `promote` and `reclone`.
- **Decision:**
  - GitOps rule: never commit action annotations, and add them to Argo CD `ignoreDifferences`.
  - Add `kubectl bloodraven rotate-keyring` and `revoke-leases --confirm=<group>`.
  - Add `ack-divergence` (A-86).

#### A-82 Declarative destructive operations use a monotonic token

- **Sev** should · **Brk** no · **Merges** OPS-04 (spec-token part)
- **Evidence:** `spec.restoreInPlace.confirm` must be RFC 3339 and greater than
  `status.restoreInPlace.confirmTokenUsed` (`backup_types.go:779-788,1071`).
  `3.crd-versioning.md` promises "explicit confirmation fields or annotations" without defining either.
- **Decision:**
  - Document two house patterns: one-shot annotations (A-78) and monotonic RFC 3339 spec tokens. A future
    standby activation uses the second.
  - Write both into the v1 contract in `3.crd-versioning.md`.

### 2.14 Hand-patched status and break-glass procedures

Section 4 lists every documented status write with its replacement.

#### A-83 Delete the "patch the keyring phase to `Sealed`" step

- **Sev** must · **Brk** no (docs) · **Merges** OPS-13, GPT-33, STATUS-04 (keyring part) · **Done at `2340f6b`**
  (docs half), by #231 and #234 (commit 0882712)
- **Evidence:**
  - At `3fa1900` the runbooks JSON-patched `status.encryptionAtRest.sites[i].phase` to `Sealed`. At `2340f6b` both
    pages say the opposite: "Do not set the phase back to `Sealed` by hand"
    (`site/content/docs/6.operations/4.runbooks.md:268-272`; `4.configuration/8.encryption-at-rest.md:516-521`).
    `--subresource=status` appears in neither page (OPUS-08).
  - The operator already moves `Failed` → `Sealed` itself once the escrow Secret is back and MySQL confirms
    the digest. It emits `KeyringRecovered` (`internal/controller/encryption_reconcile.go:674-690`).
  - The patch skips that confirmation.
- **Decision:**
  - The patch is already gone from both pages. Nothing is left for WS-E except the rule.
  - Add a docs rule: no runbook writes `status` (E18's lint, section 4).
  - GPT's typed recovery request is not needed (Appendix B).

#### A-84 Hand-forging the keyring escrow Secret is break-glass

- **Sev** should · **Brk** no · **Merges** OPS-14, OPS open question 2
- **Evidence:**
  - About 50 lines of shell recreate the operator's escrow Secret, including its labels, digest annotation,
    ownerRef and `immutable` (`4.runbooks.md:212-266`), using the keys in `encryption_escrow.go:27,32`.
  - #231/#234 added a doctor helper that automates the same forging,
    `.agents/skills/bloodraven-doctor/scripts/keyring-escrow-recover.sh` (it writes the `shipstream.io/*` keys at
    `:201-205`). `8.encryption-at-rest.md:499-524` now points at the runbook or the helper (OPUS-08).
  - Found in the re-check: `8.encryption-at-rest.md:536-541` tells the reader to roll a site back to an older
    keyring by patching `keyringSecret`/`keyringVersion`/`keyringDigest` in status. Section 4 now lists it.
- **Decision:**
  - Label the runbook block, the helper and the keyring rollback "break-glass, unsupported; the escrow Secret
    layout is Internal". E14's key sweep includes the helper's label and annotation keys.
  - File a follow-up: the operator re-escrows from the live sidecar when its read-only keyring digest matches
    the recorded one, or a `kubectl bloodraven keyring restore-escrow` subcommand does the same.
  - This is additive and not needed for v1beta1.

#### A-85 Reclone-queue release and resume are break-glass until #204

- **Sev** must · **Brk** no · **Merges** GPT-32, STATUS-04 (reclone part), OPS-15 · **Owned by #204** (deferred
  under D4)
- **Evidence:**
  - The docs patch `status.restoreReclones.phase` to `Draining` and remove `/status/restoreReclones`
    (`7.backup-and-restore/2.backup-restore.md:913,930`).
  - The same instructions appear in `.agents/skills/bloodraven-doctor/references/troubleshooting_playbooks.md:544,548`,
    `alert_runbook_matrix.md:58`, and as prose in `8.observability/2.monitoring.md:374,390`.
  - The operator's own messages embed the command (OPUS-02). The `PromotionRefusedPendingReclone` Warning Event
    says "accept that data by removing status.restoreReclones (kubectl patch mfg <group> --subresource=status
    …)" (`internal/controller/runner.go:2149-2152`). The refusal text behind `Degraded=NoPrimary` says "remove
    status.restoreReclones (kubectl patch --subresource=status)" (`internal/state/matrix.go:219-221`).
  - The API comment says "until an administrator sets the phase to Draining" (`backup_types.go:901-913`).
  - #233 changed what the patches do (OPUS-08). A `Held` queue now keeps the peers' replication receivers
    stopped. Removing the queue resumes their replication from the current primary. A failed restore that left
    the primary at the new `status.restoreReclones.fencedGtidExecuted` discards the queue instead of holding it
    (`backup_types.go:901-913,921-941`).
- **Decision:**
  - For v1.7.0, label both patches "break-glass, unsupported; needs status RBAC; releases every queued site
    without validation; removing the queue restarts replication from the current primary", everywhere they
    appear.
  - E18 also changes the two operator messages. They point to the break-glass runbook anchor instead of
    embedding a `kubectl patch --subresource=status` command. The log-schema row for
    `promotion refused: candidate is pending a post-restore reclone` (`7.log-schema.md:268`) mirrors the Event,
    so it is checked in the same commit. Its `msg` and fields do not change.
  - Remove the administrator sentence from the API comment. The phase is operator-written only.
  - #204 (additive, after v1.7.0) is expected to add both
    `bloodraven.shipstream.io/restore-reclones-release=<site>:confirm=<group>:uid=<group UID>:token=<confirmToken>`
    and a `restore-reclones-resume` action (`Held` → `Draining`). Both are bound to the group UID and to the queue
    `confirmToken`, as #204 requires (R12). The token travels in the A-78 basic form (`20261104T100000Z`), because
    the RFC 3339 colons would break the grammar, and the operator compares it as an instant. It must not invent
    a second queue identity.

#### A-86 `DivergentPromotion` acknowledgement is an annotation, not a status patch

- **Sev** must · **Brk** yes (status shape) · **Merges** OPS-16, STATUS-04 (divergence part), OPS open question 3
- **Evidence:**
  - The docs remove `/status/conditions/<index>` (`.agents/skills/bloodraven-doctor/references/troubleshooting_playbooks.md:935`;
    `8.observability/7.log-schema.md:169`; `4.configuration/2.crd-reference.md:709`). The index races operator
    writes.
  - Up to five records are packed into one condition message.
- **Decision:**
  - Add `status.divergentPromotions[]` (`+listType=map`, `+listMapKey=id`, `MaxItems=16`) with items `{id
    (promotion time in the A-78 basic form + "/" + site, for example `20261104T100000Z/iad`), site,
    divergentGtid, divergentTransactions int64, comparedTo, promotionTime metav1.Time}` (the `…Time` suffix of
    A-40; revision 2 said `promotedAt`).
  - When full (OPUS-22): with 16 unacknowledged records, the operator keeps the 16 oldest. It counts the rest in
    the condition message (`… and N more`) and emits `DivergentPromotionRecordsTruncated`. Each promotion's
    `DivergentSitePromoted` Event and ERROR log remain the full record. The status write never fails on the
    bound.
  - Add the one-shot annotation `bloodraven.shipstream.io/acknowledge-divergent-promotion=<id>[,<id>…]`. The
    operator removes the matching entries and emits `DivergentPromotionAcknowledged`.
  - The condition stays True while the list is non-empty, and its message counts the records.
  - Plugin: `kubectl bloodraven ack-divergence`.
- **Disagreement:** STATUS proposed a timestamp-bound annotation on the condition itself. OPS's per-record shape
  allows partial acknowledgement, so it is chosen.

#### A-87 Delete the MysqlDatabase "patch the record out of status" advice

- **Sev** must · **Brk** no (docs) · **Merges** OPS-17
- **Evidence:** `4.configuration/7.tenant-databases.md:356` (in the section "Upgrading to per-host write-ahead
  records" at `:352`) contradicts the doctor's "Never hand-edit `status`"
  (`troubleshooting_playbooks.md:221`).
- **Decision:** delete the section, which is legacy-upgrade guidance (D3). The remedy for a doubtful record is
  `deletionPolicy: Retain` and then delete.

#### A-88 Playground status resets are playground-only break-glass

- **Sev** leave · **Brk** no · **Merges** OPS-18
- **Evidence:** `cmd/playground-chaos/reset.go:365-371,384-410` removes ten status fields on the normalization
  pass and eleven, adding `encryptionAtRest`, on the wipe pass. It also clears the failover annotations through
  `ClearFailoverState`. The s05 setup does the same through `internal/playground/kube/mfg.go`.
- **Decision:**
  - Keep it.
  - Document it in `playground/chaos-scenarios.md` and `3.playground.md` as "break-glass, playground-only,
    unsupported on real clusters; safe only after the data plane is wiped".
  - `ClearFailoverState` stays in `internal/`. Fields #235 adds join the reset list.

#### A-89 The durable-status contract is written down

- **Sev** should · **Brk** no · **Merges** STATUS-10, KINDS-25 (consequences part)
- **Evidence:**
  - The topology manager rehydrates `activeSite`, `topologyGeneration`, `lastFailover*` and
    `promotionGtidExecuted` from status, plus the annotation copy (`restoreFailoverState`, `runner.go:1072-1141`).
  - The MDB ledger authorizes `DROP` statements (`mysqldatabase_types.go:512-592`).
  - The offline fence is re-applied from status after a restart.
  - #233 added `status.restoreReclones.fencedGtidExecuted`, which decides whether a failed restore discards its
    queue or holds it (`backup_types.go:932-941`). It is durable in the same sense.
- **Decision:**
  - Mark every durable field with `// Durable:` and list them in `crd-reference.md`.
  - `migrate-storage` (D5) rewrites objects in place and never deletes and recreates them.
  - Docs: status-write RBAC is a destructive privilege, and Velero needs `restoreStatus`.
  - Add a fail-safe test (R11). It goes in E10, the status-reshape commit that adds the `// Durable:` markers, as a
    component test in `test/component`, not in the docs-only E18. It covers two cases with different expected
    behaviour:
    - **Lost status on a running group.** Status is emptied while the sites still hold data and the annotation
      copy is present. The operator must not treat the group as a fresh deploy (no bootstrap clone). It
      rehydrates the failover record from the annotations (A-104), refuses promotion until it has observed a
      unique writable primary, and fences deployment leases meanwhile.
    - **True fresh deploy.** No status, no annotations and no replication metadata. Bootstrap proceeds as today
      (`isFreshDeploy`).

### 2.15 Validation bounds and CEL cost

#### A-90 Admission closes the my.cnf injection paths

- **Sev** must · **Brk** yes (stricter) · **Merges** MFGSPEC-15
- **Evidence:**
  - `pitr.maxBinlogSize` has no pattern (`backup_types.go:142-143`) and is written raw as `max-binlog-size`
    (`internal/controller/reconciler.go:1079-1084`). The CR/LF/NUL guard `unsafeMySQLSetting`
    (`reconciler.go:925`) covers only `mysqlConf`, and only at reconcile time.
  - `dump.bytesPerChunk` has no pattern either (`backup_types.go:516-517`).
- **Decision:**
  - `Pattern=^[0-9]+[KMG]?$` on both fields. Keep them strings: MySQL's `M` is MiB, not Quantity's 10^6.
  - On both `mysqlConf` maps (`types.go:128,378`): `MaxProperties=128`. Values get a named string type with
    `MaxLength=1024` and the OpenAPI `Pattern=^[^\r\n\x00]*$`, which costs nothing in the CEL budget (OPUS-11).
    Keys are checked by CEL `self.all(k, k.matches('^[A-Za-z0-9_.-]+$'))`.
  - Map keys cannot carry a `maxLength`, so the estimator sizes every key at the request limit. If E16's envtest
    shows the key rule over budget, the recorded fallback is to reshape `mysqlConf` into a list-map of
    `{name, value}` with bounded strings, keyed by `name`, with its own migration row. A reshape is not
    replaced by a reconcile-time check.
  - The controller fix is D-03.

#### A-91 Relationships checkable inside one object are checked at admission

- **Sev** should · **Brk** yes (stricter) · **Merges** GPT-24, MFGSPEC-24, MFGSPEC-02 (cross-reference part)
- **Evidence:**
  - `pitr.enabled` with an empty `profileName` passes, because `MinLength` does not fire on an omitted
    `omitempty` string (`backup_types.go:123-134`). Archiving then stays off with only an Event.
  - `verification.enabled` without a `schedule` passes too (`mysqlbackupverification_types.go:364-373`).
  - Schedules can name missing profiles (`backup_types.go:552`).
- **Decision:** CEL on `BackupSpec`, cost-bounded by A-09's `MaxItems`. Every optional field is guarded with
  `has()` (R05). `profiles`, `schedules`, `pitr.profileName` and `verification.schedule` are `omitempty` with
  no default (`backup_types.go:41,47,134`; `mysqlbackupverification_types.go:373`), and CEL errors on an absent
  field. `pitr.enabled` and `verification.enabled` default to `false` when their containing object exists
  (`backup_types.go:125`, `mysqlbackupverification_types.go:367`). Their `has()` guards are then redundant but
  harmless, and they keep the rules correct if a default is ever removed. They are not a reason to remove the
  defaults. Revision 2 wrongly said all six had no default (GPT-R2-06, OPUS-R2-06).
  - `!has(self.pitr) || !has(self.pitr.enabled) || !self.pitr.enabled || (has(self.pitr.profileName) &&
    has(self.profiles) && self.profiles.exists(p, p.name == self.pitr.profileName))`.
  - `!has(self.schedules) || self.schedules.all(s, has(self.profiles) && self.profiles.exists(p, p.name ==
    s.profileName))`.
  - On `VerificationSpec`: `!has(self.enabled) || !self.enabled || (has(self.schedule) && self.schedule != '')`.
    An explicitly empty schedule is rejected too.
  - Envtest cases: no `profiles` and no `schedules`; `pitr` absent, disabled, and enabled without
    `profileName`; `profileName: ""`; verification enabled with `schedule: ""`.
  - The `enabled` plus pointer-block pattern stays.
  - #206 is expected to keep `archiveProfileName` checkable the same way.

#### A-92 Bounded strings and collections everywhere CEL looks

- **Sev** should · **Brk** yes (stricter) · **Merges** MFGSPEC-33, MFGSPEC-26 (reserved-flag part), GPT-18
  (cost part)
- **Evidence:**
  - `spec.license` has no `MaxLength` (`types.go:247`).
  - `dragonfly.args` has no bounds (`dragonfly_types.go:62`).
  - The operator-owned flag list lacks `--maxmemory` and `--proactor_threads`
    (`internal/controller/dragonfly_resources.go:316-326`), although the docs call them reserved.
- **Decision:**
  - `license`: `MaxLength=4096` and the compact-JWS pattern.
  - `args`: `MaxItems=64`, `items:MaxLength=256`, and CEL rejecting the reserved flags (the controller list
    plus the two missing ones; see D-05).
  - Every string or list a CEL rule traverses gets a bound.
  - An envtest case installs every CRD and fails on a CEL cost-budget error.

### 2.16 Kind metadata

#### A-93 shortNames and categories for MysqlBackup and MysqlBackupVerification

- **Sev** should · **Brk** no · **Merges** KINDS-05
- **Evidence:**
  - There is no `+kubebuilder:resource` marker on MBK or MBV (`mysqlbackup_types.go:7-17`,
    `mysqlbackupverification_types.go:9-17`).
  - MFG, MDB and MSC have one (`types.go:11`, `mysqldatabase_types.go:9`, `mysqlstandbycluster_types.go:9`).
- **Decision:**
  - MBK: `shortName=mbk;mbks`. MBV: `shortName=mbv;mbvs`. Both get `categories=bloodraven;mysql;backup`.
  - MDB keeps `mydb`, because `mdb` collides with other operators.

#### A-94 Printer columns follow the new paths

- **Sev** should · **Brk** no · **Merges** STATUS-17 (column part), KINDS-28 (column part), KINDS-33 (column part)
- **Decision:**
  - MBK `Location` → `.status.artifactStorage.location` (A-96), and add a `Ready` column.
  - MBV gets a `Ready` column.
  - MDB `Group` → `.spec.failoverGroupRef.name` (A-01).
  - MSC columns per A-64. Its `Phase` column is removed (A-61).
  - Each column edit lands in the commit that changes its JSONPath or first writes its field (OPUS-26): MBK
    `Location` in E10 with A-96, the MSC `Ready` column in E11 with A-70, and MDB `Group` in E5 with A-01. This
    finding records only the final column set, so no commit leaves an empty column behind.

#### A-95 API doc comments carry no line numbers or roadmap prose

- **Sev** should · **Brk** no · **Merges** KINDS-08
- **Evidence:**
  - Stale file and line references: "backup_types.go:388-444" (`mysqlstandbycluster_types.go:118`) and
    "343-349" (`:138`).
  - The MBV type comment describes a PVC and Service that no longer exist
    (`mysqlbackupverification_types.go:26-32`), and so does the TTL comment (`:84-87`).
- **Decision:**
  - Remove line references and Phase-2/3 prose from `api/`. Roadmap prose moves to `proposals/`.
  - Fix the MBV comments.
  - Rebase on #233, which also edits `api/` doc comments.

### 2.17 Backup artifact identity and restore provenance

#### A-96 One recorded artifact identity, read by every consumer

- **Sev** must · **Brk** yes (status-only) · **Merges** GPT-14, KINDS-12, KINDS-20 (location part)
- **Evidence:**
  - #232 added `status.artifactStorage{location, bucket, endpointURL, region, claimName}`, immutable through
    a root CEL rule (`mysqlbackup_types.go:17,120,191-213`). Cleanup uses it (`backup_cleanup.go:64-83`).
  - The top-level `status.location` and `status.storageType` (`mysqlbackup_types.go:103,109`) still duplicate it.
  - Verification reads `status.location` and the live profile's claim
    (`internal/controller/backup_verification_job.go:116,137,287,442`).
  - Restore reads the live profile's claim (`restore.go:1046`).
  - MBV `status.backupRef` keeps its own `location` and `storageType` (`mysqlbackupverification_types.go:353,356`).
- **Decision:**
  - `artifactStorage` gains `type BackupStorageType`.
  - Remove the top-level `location` and `storageType`.
  - MBV `status.backupRef` carries the same `artifactStorage` snapshot.
  - Verification and restore read the bucket, endpoint, region and claim from it (D-12).
  - Credentials stay profile-resolved, as #232 decided. Rejected: a tagged sub-union. #232's flat struct plus
    `type` is enough and was just reviewed.
  - #203 attributes encryption in A-45, not here. #206 owns PITR archive identity, which is separate from the
    dump artifact.

#### A-97 One restore-provenance struct

- **Sev** should · **Brk** yes (status-only) · **Merges** STATUS-13 · **Owned by #206** for the replay sites and
  cross-site continuity
- **Evidence:**
  - `status.restore` and `status.restoreInPlace` repeat about 13 `source*`, `target*` and `pitr*` fields, with
    string timestamps (`backup_types.go:1118,1137-1147,1190`).
  - The `Pitr*` prefix differs from `PITR` everywhere else.
- **Decision:**
  - `provenance: {source: {sizeBytes *int64, gtidExecuted, binlogPosition *BinlogCoordinate}, target:
    {gtidExecuted, binlogPosition}, pointInTime *PointInTimeReplayStatus}` on both.
  - `PointInTimeReplayStatus{stopTime, newestEventTime *metav1.Time, replayedBinlogFile, replayedBinlogCount
    int32, sites []string (atomic), partial bool, …}`.
  - WS-E reshapes this after #206 merges. #206 is expected to replace `pitrCrossSiteUnverified bool` with an
    enum such as `crossSiteContinuity: Proven|Unverified|NotApplicable`.

#### A-98 MysqlBackup gets a deletion policy

- **Sev** should · **Brk** no (additive, defaulted) · **Merges** KINDS-07
- **Evidence:**
  - Deleting an MBK always deletes its artifact (`internal/controller/backup_cleanup.go`).
  - MDB already has `deletionPolicy` with default `Retain` (`mysqldatabase_types.go:279-281`).
- **Decision:**
  - A shared Go type `DeletionPolicy` (`Retain;Delete`).
  - MBK `spec.deletionPolicy`, default `Delete`, so retention still frees storage.
  - Deleting a `Retain` backup's CR leaves the artifact, and an Event says so.

### 2.18 Leave decisions

These were considered and kept. They are recorded so they are not reopened.

| ID | Kind / path | Reason kept | Merges |
|---|---|---|---|
| A-99 | MFG `spec.image`, `spec.backup.image` defaults (`types.go:46-48`, `backup_types.go:32`) | The MySQL version is the user's data-format decision. A persisted default stops an operator upgrade from silently upgrading MySQL. Contrast A-52. | MFGSPEC-34 |
| A-100 | MFG `spec.dns` required (`types.go:84`) | Making it optional later is additive. The `ttl` rename is A-35. | MFGSPEC-35 |
| A-101 | `failureThreshold`, `recoveryThreshold`, `terminationGracePeriodSeconds`, `podLabels`, `podAnnotations`, `serviceTemplate`, `readOnlyMaxLagSeconds`, `coverage.*`, `retainVersions`, `activeDeadlineSeconds`, `stagingVolumeSizeLimit`, `schedules[].{timeZone,suspend,*HistoryLimit}`, `dump.{consistent,ocimds}`, `excludeGtids`, `taintNodeSelector` | Sound as they are. | MFGSPEC-38 |
| A-102 | MFG `status.activeSite`, `status.topologyGeneration` (`types.go:562,568`) | Documented, keyed by the deploy API, correct types. A-68 adds `observedGeneration` next to them. | STATUS-L1 |
| A-103 | Tri-states and durable intent flags: `readOnlyMaxLagSeconds *int64`, `s3SignPayload *bool` (`s3UseHTTPS` folds into A-67's URL), `sessionsPreserved *bool`, `priorOfflineMode *bool`, `keepOnFailure *bool`, `restoreInPlace.jobCreating`, `superseded.held` | Each `false` or nil already has one meaning. Real enable and suspend switches stay `bool`. | GPT-42, STATUS-L2, KINDS-19 |
| A-104 | Annotations `bloodraven.shipstream.io/last-failover`, `…/last-failover-target` (`internal/controller/failover_state.go:40-41`) | A deliberate second durable copy of the cooldown record. Operator-owned and read-only. Clearing it is playground break-glass (A-88). | GPT-41, OPS-11 |
| A-105 | Phases on one-shot sub-state machines (`plannedFailover`, `restoreInPlace`, `restoreReclones`, `dragonfly.upgrade`, keyring sites, MBK, MBV, MDB) | Typed, enum-validated summaries of one operation each. Group health stays on conditions. | STATUS-L3 |
| A-106 | Backup schedule and retry lineage in labels and annotations (`backup_retry.go:12-15`, `backup.go:21`) | Operator-written creation metadata. The keys move prefix (A-77) and are marked Internal (A-79). Rejected: OPS's `spec.origin`, which would be a spec field only the operator writes. | KINDS-14, OPS-10 |
| A-107 | MDB identity immutability and the write-ahead status ledger (`mysqldatabase_types.go:191,219,512-592`) | The ledger is the only safe attribution of MySQL objects. A-89 documents its consequences. | GPT-43, KINDS-24, KINDS-25 |
| A-108 | MDB privilege enum values with spaces (`mysqldatabase_types.go:80-127`) | MySQL grammar tokens, copied verbatim on purpose. | KINDS-26 |
| A-109 | MBK `status.size` next to `sizeBytes` (`mysqlbackup_types.go:124,129`) | Printer columns cannot format bytes. Documented as derived. | KINDS-13 |
| A-110 | No license status or condition (`types.go:247`) | License checks never change behaviour. A condition is additive later. | STATUS-L4 |
| A-111 | MFG `status.pitr.profileName` (`backup_types.go:164`) | Attributes the archive counters, which can lag a profile switch. Observation, not an echo. | STATUS-L6 |
| A-112 | Routing label semantics: `role=primary\|replica`, operator labels take precedence over template labels | The values are right. Only the prefix and boolean values change (A-77). | GPT-40 (semantics part) |
| A-113 | PITR selectors `pointInTime.{sites,allowUnverifiedCrossSite,archiveProfileName,allowPartial}` (`backup_types.go:204-234`) | **Owned by #206 / #203.** Expected: #206 removes `allowUnverifiedCrossSite` or makes it a typed policy, and keeps `sites` as `listType=set` with the A-15 item pattern. #203 binds encryption identity with a root transition rule (A-25). Unsupported legacy archives are refused (D3). | GPT-46, MFGSPEC-36 |

### 2.19 Added in revision 2

The round 1 critiques (section 8.1) found two decisions the first draft lacked.

#### A-114 Metric labels and the operator's JSON endpoints are served contracts

- **Sev** must · **Brk** yes · **Merges** OPUS-04; revision 3 adds GPT-R2-04 and OPUS-R2-10
- **Evidence:** section 1.5.
  - The metric `role` label is `string(site.role)` (`internal/controller/topology.go:315`), and the
    `bloodraven_site_state` `state` label carries the site state (`internal/metrics/metrics.go:132-135`).
  - Documented PromQL keys on today's values: `{role=~"primary-candidate|dr-only"}`, `{role="read-only"}`
    (`8.observability/3.monitoring-prometheus.md:112,118,139,144`) and `{state="writable"}` (`2.monitoring.md:223`).
  - `/deploy/v1` returns raw status tokens (`cmd/bloodraven/deploy_api.go:353-362`). A-29, A-50 (#235), A-72 and
    A-73 change them, and the first draft documented only `unstableReason`. `health.replicationRunning` is computed
    from the `replicating` bool that A-43 removes (`deploy_api.go:337-349`).
  - Three metrics carry reasons as label values (`metrics.go:51-65,366-372`), and A-73, A-74 and #235 may rename
    those reasons.
  - The operator's `/status` and `/ws/status` serialize `role` from the API value and `state` from the internal
    string (`topology.go:1131-1132,2139-2140`), and `/ws/status` repeats the flat failover record and the
    `replicating` bool (`internal/platform/websocket.go:19-44`). The playground dashboard keys on the lowercase
    values (`playground/dashboard/index.html:131-150`).
- **Problem:** A-27 and A-28 would silently change the metric series that alerts select on, and A-29, A-72 and A-73
  would silently change the deploy API payload that CI gates parse. Neither break would reach the release notes.
  On the operator endpoints, A-27 would change `role` while `state` stayed lowercase, so one site entry would mix
  two vocabularies and the dashboard's health logic would break on `role` alone.
- **Decision:**
  - **Metric label values** are a documented contract. One exported function per label in `internal/metrics`
    produces the lowercase kebab-case form of the API enum value: `PrimaryCandidate` → `primary-candidate`,
    `DisasterRecovery` → `disaster-recovery`, `ReadOnly` → `read-only`, `Writable` → `writable`, and so on. So
    only `dr-only` → `disaster-recovery` changes value. Lowercase label values are the Prometheus convention, and
    most documented queries keep working.
  - **`/deploy/v1` tokens** are the A-69 constants, verbatim. `11.deployment-api.md` documents every token field
    with its closed value set, generated from those constants. The release notes list each renamed value. The
    path stays `/deploy/v1`: v1.7.0 is the breaking release, and there are no production users.
  - **`health.replicationRunning`** keeps its name and today's meaning, defined over the A-43 enum: every
    non-active spec site reports `Healthy` or `Lagging`.
  - **Metric `reason` labels** carry the A-69 reason constants verbatim, in CamelCase, as they do today. They are
    reasons, not enum values, so no kebab-case mapping applies. A rename by A-73, A-74 or #235 changes the label
    value, and the release-note rename table lists it next to the Event and condition renames.
  - **Operator `/status` and `/ws/status`** are JSON views of the same facts as the CR status, so they use the CR
    vocabulary verbatim: `role` and `state` carry the A-27 and A-28 values (`PrimaryCandidate`, `Writable`, …),
    produced by one adapter from the typed enums that replaces `state.String()`. Their field shapes follow the CR
    too: `/ws/status` replaces the flat `lastFailover`, `lastFailoverTarget` and `promotionGtidExecuted` with
    A-40's `lastFailover{time, target, promotionGtidExecuted}`, and `sites[].replicating` with
    `sites[].replication` (A-43). The paths stay. One rule results: JSON surfaces use the API values, and only
    Prometheus labels use the kebab-case form.
  - Obligations, by commit:
    - E7: the metric mapping functions; the `/status` and `/ws/status` role and state adapter; the dashboard's
      comparisons and CSS classes (`index.html:17-18,131-150`); `3.monitoring-prometheus.md`, `2.monitoring.md`
      (PromQL and the two endpoint examples at `:543-651`), `examples/prometheusrule.yaml` and the doctor scripts'
      PromQL. Tests: every `SiteRole` and `SiteState` maps to its documented label value, and a payload test
      asserts that both endpoints serialize every role and state as the CR value.
    - E10: the `/ws/status` `lastFailover` and `replication` reshapes and their dashboard readers;
      `replicationRunning` over the enum, with a deploy-API test per enum value.
    - E11: `11.deployment-api.md` (every token field with its closed value set, and the `replicationRunning`
      definition) and the deploy API tests.
    - E12: a unit test that every `reason` label value written is a declared A-69 constant, and the metric rows of
      the release-note rename table.
    - Section 3.4 carries a migration row for each surface.
- **Rejected:**
  - Raw CamelCase metric values, which would break every documented query for no gain.
  - Freezing `dr-only`, which would leave a second name for the role.
  - Keeping the internal lowercase strings on `/status` and `/ws/status`. Those endpoints would then disagree
    with the CR and with `/deploy/v1` for the same site, and `role` would change anyway.
  - Kebab-case on the JSON endpoints. JSON has no label-value convention to follow, and a third spelling buys
    nothing.

#### A-115 One tested Kubernetes floor: 1.31

- **Sev** must · **Brk** yes (drops Kubernetes 1.26-1.30) · **Merges** R18, OPUS-19
- **Evidence:**
  - The chart advertises `kubeVersion: ">=1.26.0"` (`charts/bloodraven/Chart.yaml:24`). The docs say 1.27
    (`site/content/docs/3.get-started/2.getting-started.md:13`, `4.install-production.md:11`).
  - No CI job runs at either floor. Envtest uses 1.37 (`.github/workflows/ci.yml:189`), and the E2E uses
    `kindest/node:v1.35.0` (`.github/workflows/_e2e.yml:100`).
  - The first draft shaped A-17 (`quantity()`), A-19 (`isIP()`) and the admission tests around 1.27, a floor
    nothing exercises.
- **Decision:**
  - v1.7.0 supports Kubernetes 1.31 and newer. The chart says `kubeVersion: ">=1.31.0-0"`, so that provider
    suffixes such as `-eks-…` match, and both docs pages say 1.31.
  - E4 adds a CI envtest job on 1.31 assets that runs every admission and transition-rule test, so the floor is
    tested.
  - What 1.31 gives the API: the CEL `quantity()`, IP and CIDR libraries, and `CRDValidationRatcheting` on by
    default (beta since 1.30). Ratcheting lets an update keep an unchanged value that a tightened validation
    would now reject. It does not cover transition rules, `required` changes, type changes or list-topology
    changes (GPT-R2-09). It helps v1beta1 → v1, but it is not a general compatibility mechanism: each future
    tightening still needs its own compatibility test against stored objects.
- **Rejected:**
  - Keep 1.27 and add a 1.27 envtest job. The workarounds stay, there is no ratcheting, and 1.27 is long past
    upstream support.
  - Keep advertising a floor that no job tests.

## 3. Migration table for breaking changes

Every breaking change lands in v1alpha1 before v1beta1 (D2). There are no dual-field shims and no automatic
rewrite of old objects (D3).

### 3.1 There is no in-place upgrade from v1.6.0 to v1.7.0

The first draft described an in-place path (export, edit, upgrade, re-apply, `migrate-storage`) and said the
operator rewrites status. Both critiques showed that the path cannot work (R02, R03, OPUS-01), and each reason
below was verified at `2340f6b`:

1. **Persisted defaults change type.** Every stored MFG carries `cloneTimeout: 3600` (`types.go:123-125`),
   which becomes a Duration string under A-36, and `updateStrategy: OrderedUpdate` (`:133-136`), which becomes an
   object under A-27. Every profile carries `retention: 7` (`backup_types.go:292-294`), which becomes an object
   under A-55. Pruning drops unknown fields but does not coerce types. The v1.7.0 operator's typed client therefore
   cannot decode a stored MFG, and one undecodable item fails the whole List/Watch.
2. **Required fields and type changes are not ratcheted.** An update validates the whole object, status included
   (`k8s.io/apiextensions-apiserver` v0.37.0, `pkg/registry/customresource/validator.go`, `ValidateUpdate`), and
   a status update runs the CEL rules against the whole object, spec included (`status_strategy.go`,
   `ValidateUpdate`). At the A-115 floor (1.31) `CRDValidationRatcheting` is on, so an unchanged value that only
   fails a new enum, pattern or non-transition CEL rule is tolerated: old status values such as `sites[].state:
   writable` no longer block a write by themselves (OPUS-R2-05; revision 2 cited them as a reason). Ratcheting
   does not cover new `required` fields (`spec.sites[].serverID`, `spec.credentials`), type changes (item 1) or
   transition rules (item 3), and every v1.6.0 MFG hits at least one of them.
3. **The new transition rules reject the re-apply on any version.** Ratcheting does not apply to transition
   rules.
   - A-23 rejects any change to `initFromBackup`, and every bootstrapped group's block is renamed:
     `mysqlBackupRef`, `credentialsSecret` and `stopDatetime`.
   - A-22's presence rules reject a renamed `sourceSiteOverride` and a reshaped MBV `pointInTime` or
     `sanityCheck`.
   - Deleting the MBK instead deletes its artifact (A-98's default is `Delete`).
4. **Re-applying cannot rewrite status.** A manifest re-apply goes through the main endpoint, and with the status
   subresource enabled that endpoint ignores `status`. Nothing converts a stored status. Several records cannot be
   rebuilt from observation (3.3).
5. **CRDs are cluster-scoped.** Applying the v1.7.0 CRDs replaces the v1.6.0 schema for every namespace at once.
   The two schemas cannot coexist in one cluster.
6. **Recreating the MFG deletes data.** The MFG owns its data PVCs (`internal/controller/reconciler.go:1136-1142`),
   and the site names that A-15 may force to change are part of the PVC names.

D3 rules out building migration machinery for this, and there are no production users. v1.7.0 therefore does not
attempt it. A cluster still on v1.6.0 moves by the procedure whose requirements 3.2 lists.

From `v1.7.0-rc.0` on, the path is in place. The rc.0 objects already have the cleaned schema, and v1beta1 is the
same schema (D1), so the upgrade is: apply the CRDs, upgrade the chart, then run `kubectl bloodraven
migrate-storage` (C-7). The upgrade E2E covers it (C-8, whose rc.0 baseline is pending sign-off).

### 3.2 Requirements for the v1.6.0 → v1.7.0 procedure

The supported path is a new group in a new cluster, bootstrapped from a v1.6.0 backup. Revisions 1 and 2 gave a
step-by-step procedure here, and both critique rounds found Major problems in it (round 1: R02, R03, OPUS-01;
round 2: OPUS-R2-02, GPT-R2-02). The program's three-rounds rule says that three consecutive rounds of Major
findings in one area mean re-planning, not re-patching. Rather than spend a third round on the steps, this
proposal no longer carries them. It states what the procedure must achieve. WS-F writes the procedure in the upgrade docs (C-12), tests it end to end (C-16), and
the release notes link it (C-13).

The procedure's outline stays as before: back up on v1.6.0 after application writes stop, install v1.7.0 in a
cluster with no Bloodraven CRDs, write the new manifests from the 3.4 table, bootstrap from the backup, load the
tenant databases, verify, cut over, and retire the old group. Its text must meet every requirement below.

| # | Requirement | Why |
|---|---|---|
| U-1 | **Never apply the v1.7.0 CRDs to a cluster that runs v1.6.0, by any route.** The procedure says so first, in a warning box. It names each route that does it: the documented "CRDs first, then the chart" routine (`6.operations/10.upgrade-policy.md:98`); a platform CRD app or a Helm release in Argo CD that renders the chart's `crds/` (`6.operations/11.gitops.md:11-27`); the course's answer that teaches the same routine (`course/course-bloodraven-in-production/unit-7-day-0-and-day-2/unit-test.md:125`). On the old cluster, pin both the chart and the CRD source in Git for as long as it runs. C-12 and E19 add the same warning to those three pages. | The API server prunes unknown fields when it reads objects, so the old field names vanish from every read while types stay wrong (3.1 item 1). The still-running v1.6.0 operator, or any v1.7.0 operator, would then persist pruned objects on its next `Update`. Nothing in v1.7.0 can stop a v1.6.0 operator (OPUS-R2-02). |
| U-2 | **Recovery from U-1.** The procedure gives the recovery: scale every Bloodraven operator to 0, re-apply the v1.6.0 CRDs with server-side apply, roll the chart back, and audit the objects. It says plainly that stored data survives only where nothing has written since the CRDs changed. | Pruning on read does not change etcd until something writes. |
| U-3 | **Fresh writable destinations.** The new group's backup profiles and PITR profile use prefixes that no v1.6.0 group has used. The old artifact is referenced only as a restore source (`initFromBackup.source.s3`, or a per-schema `restoreInPlace.source`), never as a profile of the new group. After every restore, including the tenant restores, the procedure takes a new full backup as the new group's baseline, and when PITR is enabled waits for `PITRArchiveReady=True`, before cut-over. | The PITR archive destination is derived from the profile's prefix (`<prefix>/binlogs`, `internal/controller/pitr.go:125-134`). An old profile copied into the new manifest would archive into the identity-less archive that #203 and #206 refuse (D3). Without a new baseline and a ready archive, the new group has no point-in-time recovery at cut-over (GPT-R2-02). |
| U-4 | **The right restore source.** The backup is taken on an **S3** profile that the new cluster can reach; a group with only PVC profiles adds one on v1.6.0 first. `source.s3.{bucket, prefix}` comes from that MysqlBackup's `status.location`, which is the dump's own path: `s3://<bucket>/<key>`, or a bare key in the profile's bucket (`internal/controller/restore.go:101-118`). It is not the profile prefix, which holds many dumps and `binlogs/`. The procedure adds `decryption` when the backup is encrypted (`backup_types.go:604-644`), and recreates the S3 credentials Secret in the new cluster. | A PVC backup is unreadable from a new cluster, and the profile prefix is not a dump (OPUS-R2-07). The docs' own example uses a dump path (`7.backup-and-restore/2.backup-restore.md:556-563`). |
| U-5 | **Tenant databases.** Bootstrap with the MysqlDatabase-managed schemas excluded (`initFromBackup.loadOptions.excludeSchemas`, `backup_types.go:697-700`). Apply the MysqlDatabase objects so they create their schemas and users. Then load each schema with a per-schema `restoreInPlace` (`loadOptions.includeSchemas: [<schema>]`), which drops and reloads that schema and replicates (`restore_script.py:294-330`; `restore_inplace.go:1412-1438` forces `skipBinlog=false`). | A schema that the restore already loaded makes its MysqlDatabase fail with `DatabasePreExists` (`4.configuration/7.tenant-databases.md:276`), and D3 rules out adoption. |
| U-6 | **Say what is not carried** (3.3), with the action for each: MySQL accounts and grants that neither the operator's credentials nor a MysqlDatabase create are recreated before cut-over; the new group mints its own keyrings, and the old escrow Secrets stay with the old cluster, which needs them while it runs; failover history, operation records, MysqlBackup and MysqlBackupVerification objects, and PITR across the old archive are dropped. | `util.dumpInstance` dumps carry no system users, and restores never set `loadUsers` (`2.backup-restore.md:602-604`). Accounts not created by the operator or a MysqlDatabase are otherwise lost silently (OPUS-R2-07). |
| U-7 | **Manifests** follow the 3.4 table: names valid under A-15; an explicit `serverID` per site (A-08); keys that only restated a persisted default deleted, not translated; `status` and server-set metadata stripped. They are applied with `kubectl apply --server-side --validate=strict`, so a leftover old field name is rejected instead of pruned. | Unchanged from revision 2. |
| U-8 | **Verify before cut-over:** `Ready=True` and `Degraded=False` on the group, `Ready` on every MysqlDatabase, `PITRArchiveReady=True` when PITR is on (U-3), a MysqlBackupVerification of the new baseline backup, and row counts or checksums of the application's tables against the frozen source. Retire the old group only after cut-over is verified, keep its backups as long as recovery policy requires, and never delete a MysqlFailoverGroup in order to recreate it. | Unchanged from revision 2, plus U-3. |
| U-9 | **Tested end to end once** by WS-F (C-16): published v1.6.0 chart and images, an encrypted S3 backup, PITR enabled on the new group with fresh prefixes, one tenant database, and one account created outside the operator. The test asserts `PITRArchiveReady=True` on the new group, that no object under the old prefix changes, and that the recreated account works. | Two drafts of the steps had defects that only running them would show. |

### 3.3 What a new group does not carry over

This replaces the first draft's "status-only rows: the operator rewrites them".

| Record | Class | Why that is safe, or what replaces it |
|---|---|---|
| Observations: `status.sites[]` (state, `replication`), `activeSite`, `topologyGeneration`, `conditions`, `observedGeneration`, `updatePhase`, `dragonfly.sites[]`, encryption coverage, `pitr` counters, `backupSchedules`, MSC `discovered` | **Rewritten** by the new group's operator | Observed from the new group's own pods within a poll. |
| Failover history: `lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` and their annotation copies (A-40, A-104); the `DivergentPromotion` record (A-86) | **Dropped** | These protect the old data directories: the cooldown, the divergence evidence and the promotion watermark. The restore creates every data directory of the new group, so none of that history describes them. If you want the record, export it with `kubectl get -o yaml` before retiring the old group. |
| Operation records: `status.restore`, `restoreInPlace`, `restoreReclones` (including `fencedGtidExecuted`), `plannedFailover`, `recovery`, `dragonfly.upgrade` | **Dropped** | Each one describes an operation on the old group's data. None is in flight on the new group, which is still frozen for cut-over. |
| Keyrings and escrow Secrets (`mysql-<group>-<site>-keyring-v<N>`) | **Not carried** (R09) | The new group generates its own keyring per site, so the old escrow Secrets and their `shipstream.io/*` label keys need no migration. Keep them with the old cluster, which needs them while it runs. Backups are logical dumps and do not depend on any keyring (`4.configuration/8.encryption-at-rest.md:532-534`). Backup encryption uses its own passphrase Secret, which the new cluster needs for `decryption` (3.2 U-4). |
| MysqlBackup and MysqlBackupVerification objects, verification holds and the old PITR archive | **Not carried** | The new group's history starts with the new baseline backup (3.2 U-3). Old artifacts stay in the bucket under the old prefix, which the new group only reads, as a restore source. PITR across the old archive is refused: it has no source-archive identity (#206, D3). |
| MySQL accounts and grants that neither the operator's `credentials.*` nor a MysqlDatabase create | **Not carried** (OPUS-R2-07) | Dumps carry no system users, and restores never set `loadUsers` (`7.backup-and-restore/2.backup-restore.md:602-604`). Recreate them in the new group before cut-over (3.2 U-6). |
| MysqlDatabase ledger (`ownerUser`, `ownerHosts`, `appliedUsers[]`, …) | **Rebuilt** | The MysqlDatabase objects applied in the new cluster (3.2 U-5) write fresh records for what they create. |
| A v1.6.0 object presented to a v1.7.0 schema | **Refused by v1.7.0 tools, with one gap** | `migrate-storage` refuses on a dry-run failure (C-7). The v1.7.0 operator refuses to start when its CRDs are not the schema it was built for, or when any MFG fails a dry-run update (C-14). `--validate=strict` rejects a re-applied old field name. **The gap (OPUS-R2-02):** when the v1.7.0 CRDs are applied to a v1.6.0 cluster, the still-running v1.6.0 operator is not stopped by anything in v1.7.0. Revision 2 called this case "refused"; only 3.2 U-1 and U-2 cover it. |

### 3.4 Field-by-field table

"How to migrate a manifest" means how to write the new group's manifest from an old one. "Status-only" rows
need no manifest change: the new group's operator writes them, and nothing converts the old value (3.3). Rows
marked "(present in every export)" are persisted CRD defaults: an exported v1.6.0 object contains them even if
nobody set them (OPUS-23).

| Kind | Old path | New path / shape | How to migrate a manifest | Finding |
|---|---|---|---|---|
| All five | `spec` optional | `spec` required | Add `spec` (malformed objects only) | A-05 |
| MDB | `spec.groupRef.name` | `spec.failoverGroupRef.name` | Rename the key | A-01 |
| MFG | `spec.credentials.operatorSecret` (and `appSecret`, `readOnlySecret`, `monitorSecret`, `backupSecret`) | `spec.credentials.operatorSecretName` (and so on) | Rename the keys | A-02 |
| MFG | `…storage.s3.credentialsSecret` (profiles, `initFromBackup.source.s3`, `restoreInPlace.source.s3`) | `…s3.credentialsSecretName` | Rename | A-02 |
| MFG | `spec.dragonfly.auth.{secretName, passwordKey}` | `spec.dragonfly.auth.passwordSecretKeyRef{name, key}` | Move the values | A-02 |
| MFG, MSC | `…encryption.passphraseSecret`, `…decryption.passphraseSecret` | `…passphraseSecretKeyRef` | Rename; same body | A-02 |
| MFG | `spec.secretName` (DSN mode) | removed; `spec.credentials` required | Create an operator Secret (`username`, `password`, `MYSQL_ROOT_PASSWORD`, optional replication keys) and set `credentials.operatorSecretName`. Express DSN `tls=` options through `spec.tls`. | A-03 |
| MFG | `…source.mysqlBackupRef` | `…source.backupRef` (`name` required) | Rename; remove empty `{}` | A-04 |
| MBV | `spec.backupRef: {}` | `name` required | Delete an empty `backupRef` | A-04 |
| MBK, MBV, MFG | `profileName` without a pattern | DNS-label pattern, ≤ 63 | Rename invalid profiles | A-04, A-09 |
| MSC | `spec.source.{failoverGroupName, namespace, cluster, profileName}` | removed | Delete the keys; use labels for display | A-06 |
| All with conditions | `status.conditions` atomic | list-map by `type` | Status-only | A-07 |
| MFG | `spec.sites` atomic, `server_id = 101 + index` | list-map by `name`; required immutable `sites[].serverID` | Add a `serverID` per site. Use `101`, `102`, … as the examples do. The restore creates new data directories, so the ids need not match the old group's | A-08 |
| MFG | `spec.backup.{profiles, schedules}` atomic | list-maps by `name`, ≤ 32 items, names ≤ 63 | Remove duplicate names | A-09 |
| MFG | `extraContainers`, `extraInitContainers`, `backup.imagePullSecrets` atomic | list-maps by `name` | Remove duplicate names | A-10 |
| MFG | `status.{sites, dragonfly.sites, backupSchedules, restoreReclones.failures}` atomic | list-maps | Status-only | A-11 |
| MFG | `includeSchemas`, `excludeSchemas` atomic | sets, ≤ 256 items; in-place `includeSchemas` ≤ 1 | Deduplicate | A-12 |
| MFG | `splitBrainPolicy.sitePriorities` duplicates allowed | unique | Deduplicate | A-13 |
| MDB | `spec.deploymentClients` keyed by `namespace` only, unbounded | list-map keyed by (`namespace`, `serviceAccount`), ≤ 16 items | Split entries that grouped ServiceAccounts by namespace; keep at most 16 | A-14 |
| MFG | `sites[].name` ≤ 253 chars, `^[a-z0-9-]+$` | DNS label ≤ 20; `len(group) + len(site) ≤ 41` | Choose valid names in the new group's manifest (3.2). Site names are part of PVC and Service names, so changing one means a new group, never an edit | A-15 |
| MBK, MBV | `metadata.name` any length | ≤ 63 | Rename | A-15 |
| MFG | `metadata.name` any DNS subdomain | DNS-1035 label (`^[a-z]([-a-z0-9]*[a-z0-9])?$`) | Name the new group with a letter first and no dots | A-15 |
| MFG | removing a site allowed | refused | Replace the group (3.2) | A-16 |
| MFG | `sites[].storage.{storageClassName, size}`, `sites[].zone` mutable (ignored) | immutable | Set the intended values when the group is created; they cannot change later | A-17 |
| MFG | `…storage.pvc{claimName, storageClassName, size, subPath}` | `pvc{claimName \| provisioned{storageClassName, size}, subPath}` | Keep `claimName`, or move class and size under `provisioned` | A-18 |
| MFG | `sites[].lbIP` | `sites[].loadBalancerIP` (IPv4) | Rename | A-19 |
| MBK | `spec.sourceSiteOverride` | `spec.sourceSite` | Rename | A-20 |
| Node taint | `shipstream.io/db-readonly-<group>` | `<namespace>.db-readonly.bloodraven.shipstream.io/<group>` | Write tolerations for the new key. Old taints stay on the old cluster's nodes until its groups are retired; remove them there with `kubectl taint nodes --all <old>-` | A-21 |
| MBK, MBV | execution fields mutable | immutable | Create a new object instead of editing | A-22 |
| MFG | `spec.initFromBackup` addable, removable, editable | fixed at creation | Set it once, at creation (3.2 U-4). Never edit or remove it | A-23 |
| MFG | `encryptionAtRest.enabled: true → false` allowed; keyring paths mutable | refused | Keep the configuration | A-24 |
| MFG | `restoreInPlace.confirm` any string | `format: date-time` | Use RFC 3339 | A-26 |
| MFG | `sites[].role: primary-candidate \| dr-only \| read-only` (present in every export) | `PrimaryCandidate \| DisasterRecovery \| ReadOnly` | Rewrite the values | A-27 |
| MFG, MBV | `pointInTime.mode: none \| latest \| timestamp` | `Latest \| Timestamp`, required in the block | `none` → delete the block; capitalise the others | A-27 |
| MFG | `dragonfly.plannedFailover.onSyncTimeout: proceed \| fail` (present in every export with Dragonfly) | `Proceed \| Fail` | Capitalise | A-27 |
| MFG | `spec.updateStrategy: OrderedUpdate \| Recreate` (present in every export) | `spec.updateStrategy: {type: …}` | Delete a defaulted `OrderedUpdate`; wrap `Recreate` in an object | A-27 |
| MFG | `status.sites[].state`, `status.dragonfly.sites[].role` lowercase | CamelCase | Status-only | A-28 |
| MFG | `status.updatePhase` untyped incl. `Complete`; `lastBackupPhase` string | typed enums | Status-only | A-29 |
| MFG | `status.restoreInPlace.scope: "full" \| "schema:<n>"` | `scope{type, schema}` | Status-only | A-30 |
| MBV | phases `Checking`, `Cleaning` | removed | Status-only | A-31 |
| MDB | phase `Creating` | `Applying` | Status-only | A-32 |
| MFG | free-string `status.restore.reason`, `dragonfly.upgrade.reason`, `plannedFailover.dragonfly.reason`, `sites[].sourceConvergenceReason` | typed; condition-reason pattern, `MaxLength=1024`; `sourceConvergenceReason` an enum | Status-only | A-33 |
| MFG, MBV | `sanityCheck.expect.{minRows, maxDurationSeconds}` | `sanityCheck.{minResult, timeout}` | `minRows` → `minResult`; `maxDurationSeconds: 60` → `timeout: 60s` | A-34 |
| MBV | `status.sanityCheck.{ran, resultRow, durationMs}` | `{outcome, result, durationMilliseconds, error}` | Status-only | A-34 |
| MFG | `spec.cloneTimeout: 3600` (present in every export) | `spec.cloneTimeout: 1h` | Delete the default; convert other values from seconds to a duration | A-35, A-36 |
| MFG | `encryptionAtRest.keyring.escrowTimeoutSeconds: 600` | `escrowTimeout: 10m` | Convert | A-35 |
| MFG | `backup.retry.{initialBackoffSeconds, maxBackoffSeconds}` (present in every export with a retry block) | `{initialBackoff, maxBackoff}` | `60` → `1m`, `1800` → `30m` | A-35 |
| MFG | `spec.dns.ttl` (present in every export) | `spec.dns.ttlSeconds` | Rename, or delete the default `60` | A-35 |
| MFG | `status.dragonfly.sites[].lastIOSecondsAgo` `int` | `*int64` | Status-only | A-36 |
| MFG, MBV | zero rewritten to a default (`maxLagSeconds`, `backoffLimit`, `maxLagSecondsForSource`, `ttlSecondsAfterFinished`) | pointers; 0 means 0 | Review explicit zeros. MBV `ttlSecondsAfterFinished` no longer defaults to 0: set 0 to keep immediate cleanup. | A-37 |
| MFG, MSC | unbounded Durations | lower and upper bounds | Fix out-of-range values (e.g. `pollInterval: 0s`, `failoverCooldown` < 30s) | A-38 |
| MFG | `pointInTime.stopDatetime: "2026-04-15 09:30:00"` | `pointInTime.stopTime: "2026-04-15T09:30:00Z"` | Rename; add the zone | A-39 |
| MBV, MFG | `pointInTime.timestamp` | `pointInTime.stopTime` | Rename | A-39 |
| MFG | `status.lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` | `status.lastFailover{time, target, promotionGtidExecuted}` | Status-only | A-40 |
| MSC | `status.discovered.lastScanAt` | `status.discovered.lastScanTime` | Status-only | A-40, A-64 |
| MFG | `spec.dragonfly.maxMemoryMb: 512` | `spec.dragonfly.maxMemory: 512Mi` | Convert | A-41 |
| MFG | `spec.dragonfly.proactorThreads: 0` | omit (nil = default) | Delete zero values | A-41 |
| MBK | `status.binlogFile`, `binlogPos` | `status.binlogPosition{file, position}` | Status-only | A-42 |
| MFG | `status.sites[].replicating` | `status.sites[].replication` (enum) | Status-only | A-43 |
| MFG | `status.dragonfly.sites[].syncInProgress` `bool` | `*bool` | Status-only | A-44 |
| MBK | `status.encrypted`, `encryptionAlgorithm` | `status.encryption{mode, algorithm}` | Status-only | A-45 |
| MFG | `status.encryptionAtRest.sites[].coverage.*` `bool` | `*bool` | Status-only | A-46 |
| MFG | `status.encryptionAtRest.sealed` | removed; use `EncryptionAtRestReady` | Status-only; update alerts | A-47 |
| MFG | annotation `bloodraven.shipstream.io/dragonfly-snapshot-upgrade=<img>` | `spec.dragonfly.upgradeStrategy: SnapshotRestore` + edit `spec.dragonfly.image` | Replace the annotation with a spec edit | A-48 |
| MFG | `status.pitr.enabled`, `status.dragonfly.enabled` | removed | Status-only | A-49 |
| MFG | `status.plannedFailover.{target, sourcePrimary, reason, …}` and the #235 additions | owned by #235, held to the A-50 checklist: one durable-claim record with `runID`, `sourceSite`, `targetSite`, typed CamelCase `outcome` and `reason`; condition `AutomaticFailoverHeld`; `acknowledge-planned-failover-hold` annotation | Per #235's table; status-only for the record | A-50 |
| MFG | annotation `bloodraven.shipstream.io/encryption-adopt=confirm` | `spec.encryptionAtRest.existingData: LeavePlaintext` | Replace the annotation | A-51 |
| MFG | `spec.sidecarImage` (present in every export: the persisted default 1.3.2), `spec.sidecarResources` | `spec.sidecar.image` (empty means the operator default), `spec.sidecar.resources` | Delete a persisted `1.3.2` value to track the operator, or move a pin. An unpinned group rolls its sidecar after every operator upgrade, one group at a time | A-52 |
| MFG | MySQL and Dragonfly pods get no security context by default; Jobs use uid 27 | merged hardened defaults, uid 999 (if the A-54 gate passes) | Remove contexts that only restated defaults; check volume ownership | A-54 |
| MFG | `profiles[].retention: N` and `retentionPolicy{count, maxAgeDays, minKeep, maxFailedKeep}` | `profiles[].retention{keepLast, keepWithinDays, keepFailed}` | `retention: N` (present in every export as `7`) → `retention: {keepLast: N}`. A policy maps to `keepLast: max(count, minKeep, 1)`, `maxAgeDays` → `keepWithinDays`, `maxFailedKeep` → `keepFailed`. `retention: 0`, or a policy with `count: 0` and `maxAgeDays: 0`, disabled pruning: there is no equivalent, so choose an explicit `keepLast` | A-55 |
| MFG | `verification.retentionPolicy{keepSuccessful, keepFailures}` | `verification.retention{keepLast, keepFailed}` | Rename | A-55 |
| MFG | `loadOptions.resetProgress`, `s3.storageClass`, `verification.storage` | removed | Delete the keys | A-56 |
| MBV | `spec.storage` | removed | Delete; bound disk through `spec.resources` | A-56 |
| MBV | `status.{podName, pvcName, serviceName}` | removed | Status-only | A-57 |
| MFG | `spec.tls.issuerRef` | removed; `spec.tls.secretName` only | Delete `issuerRef` | A-58 |
| MFG | `spec.restoreInPlace.pointInTime` | removed (re-added by #205) | Delete; use new-group PITR | A-59 |
| MSC | `spec.transport: Network` | not allowed | Use `ObjectStore` | A-60 |
| MSC | `status.{lastVerified, activation, materializedFailoverGroup}` | removed | Status-only | A-61 |
| MSC | `spec.template` | removed | Delete the block; keep it in Git for the future activation request | A-62 |
| MSC | `spec.source.storage{type: S3, s3{…}}` | `spec.source.s3{…}` (`prefix` required) | Unwrap | A-63 |
| MSC | `status.discovered.{dump*, …, message}` | `status.discovered{latestDump, binlogArchive, lastScanTime}` | Status-only | A-64 |
| MBK, MBV | `spec.triggeredBy` | annotation `bloodraven.shipstream.io/triggered-by` | Move the value | A-65 |
| MFG | `spec.sidecar.bloodravenAddress` | `spec.sidecar.operatorEndpoint` | Rename | A-66 |
| MFG | `spec.dragonfly.snapshot.{s3Endpoint: host:port, s3UseHTTPS}` | `spec.dragonfly.snapshot.s3EndpointURL` | `s3Endpoint: rustfs:9000` plus `s3UseHTTPS: false` → `s3EndpointURL: http://rustfs:9000`. An unset `s3UseHTTPS` means `https://` | A-67 |
| MFG | `Ready` reason `TopologyPolled`; `Bootstrapping`/`Updating`/`Degraded` reasons | per A-71 and A-72 | Status-only; update alert rules on reasons | A-71, A-72 |
| MBK, MDB | reasons `NoHealthySource` (Pending), `GroupNotFound` (Pending) | `WaitingForHealthySource`, `GroupNotYetCreated` | Status-only | A-73 |
| Events | the A-74 and A-75 rename lists | renamed reasons and types | Update Event routing | A-74, A-75 |
| Labels, annotations, finalizers | `shipstream.io/*`; `healthy=yes\|no`; `dragonfly-traffic=enabled` | `bloodraven.shipstream.io/*`; `"true"`/`"false"` | Write selectors, NetworkPolicies and monitors for the new keys in the new group's manifests | A-77 |
| MBK annotation | `hold.verification.shipstream.io/<uid>` | `verification-hold.bloodraven.shipstream.io/<uid>` (`<kind>-hold.bloodraven.shipstream.io/<id>` family) | Operator-written; update scripts and queries that read it | A-77 |
| MFG finalizer | `shipstream.io/graceful-shutdown` | `bloodraven.shipstream.io/failover-group` | Operator-written | A-77 |
| MFG | `reclone-site=<site>:<gtid-prefix>` | `reclone-site=<site>:gtidPrefix=<uuid-prefix>` | Use the source-UUID part only; `kubectl bloodraven reclone` builds the value | A-78 |
| MFG | condition `DivergentPromotion` removed by hand | `status.divergentPromotions[]` + `acknowledge-divergent-promotion` annotation | Use `kubectl bloodraven ack-divergence` | A-86 |
| MFG | `maxBinlogSize`, `bytesPerChunk`, `mysqlConf` unvalidated | patterns and CEL | Fix invalid values | A-90 |
| MFG | PITR, schedule and verification cross-references unchecked | CEL | Fix dangling references | A-91 |
| MFG | `license`, `dragonfly.args` unbounded; reserved flags in args | bounded, reserved flags rejected | Remove reserved flags | A-92 |
| MBK | `status.location`, `status.storageType` | `status.artifactStorage.{location, type}` | Status-only; update printer columns and scripts | A-96 |
| MBV | `status.backupRef.{location, storageType}` | `status.backupRef.artifactStorage` | Status-only | A-96 |
| MFG | `status.restore.*` and `status.restoreInPlace.*` `source*`/`target*`/`pitr*` fields | `provenance{source, target, pointInTime}` | Status-only | A-97 |
| MDB | `spec.collation` defaulted to `utf8mb4_unicode_ci` (present in every export) | no default; unset renders no `COLLATE` | Keep `collation: utf8mb4_unicode_ci` explicitly where new schemas must match the old behaviour | A-53 |
| Metrics | label `role="dr-only"` | `role="disaster-recovery"` (other `role` and `state` values unchanged) | Update alert rules and dashboards | A-114 |
| `/deploy/v1` | raw tokens in `updatePhase`, `degradedReason`, `plannedFailover`, `restoreInPlace`, `dragonflyUpgrade`, `unstableReason` | the A-69 constants, as renamed by A-29, A-72, A-73 and #235 | Update CI gates that compare the values (release-note table) | A-114 |
| `/deploy/v1` | `health.replicationRunning` from the `replicating` bool | same name and meaning: every non-active spec site reports `Healthy` or `Lagging` | None; the definition is now documented | A-43, A-114 |
| Metrics | `reason` label values on `bloodraven_planned_failovers_deferred_total`, `bloodraven_deploy_lease_revocations_total`, `bloodraven_backup_artifact_cleanup_failures_total` | the A-69 reason constants, as renamed by A-73, A-74 and #235 | Update alert rules that select on a renamed reason (release-note table) | A-114 |
| Operator `/status`, `/ws/status` | site `state` lowercase (`writable`, `read-only`, …); `role` kebab-case | `state` and `role` carry the CR values (`Writable`, `ReadOnly`, `PrimaryCandidate`, …) | Update scripts and dashboards that compare them | A-27, A-28, A-114 |
| Operator `/ws/status` | `lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted`; `sites[].replicating` | `lastFailover{time, target, promotionGtidExecuted}`; `sites[].replication` (enum) | Update WebSocket clients | A-40, A-43, A-114 |
| Cluster | Kubernetes ≥ 1.26 (chart) or ≥ 1.27 (docs) | Kubernetes ≥ 1.31 | Install v1.7.0 on 1.31 or newer | A-115 |

## 4. Hand-patched status inventory

This lists every status path that the docs, runbooks, doctor skill, playground or the operator's own messages
tell someone to write by hand, re-checked at `2340f6b`. No doctor script writes status: the new
`keyring-escrow-recover.sh` says so in its header. The kubectl plugin never writes status.

The rule for v1.7.0 is that no page under `site/`, no doctor reference, and no Event or condition message in
`internal/` or `cmd/` instructs a status write, except where labelled break-glass below. E18's lint enforces it.
The lint fails on `--subresource=status` outside a marked break-glass block in `site/` and `.agents/`, and in any
string literal in `internal/` or `cmd/` (OPUS-02).

| Status path | Where instructed (at `2340f6b`) | Why | Supported mechanism in v1.7.0 | Label | Finding |
|---|---|---|---|---|---|
| MFG `status.encryptionAtRest.sites[i].phase` → `Sealed` | Nowhere any more: #231/#234 replaced the patch with "Do not set the phase back to `Sealed` by hand" (`6.operations/4.runbooks.md:268-272`, `4.configuration/8.encryption-at-rest.md:516-521`) | Return a `Failed` site after recreating the escrow Secret | The operator recovers `Failed` → `Sealed` once MySQL confirms the digest, and emits `KeyringRecovered` (`internal/controller/encryption_reconcile.go:674-690`) | Done | A-83 |
| (related, not status) hand-built escrow Secret with operator labels, digest annotation, ownerRef and `immutable` | `4.runbooks.md:212-266`; the doctor helper `scripts/keyring-escrow-recover.sh`; pointed to by `8.encryption-at-rest.md:499-524` | Recreate a lost escrow from the live pod | Follow-up issue: operator re-escrow, or `kubectl bloodraven keyring restore-escrow` | **Break-glass, unsupported format** until the follow-up | A-84 |
| MFG `status.encryptionAtRest.sites[i].{keyringSecret, keyringVersion, keyringDigest}` → an older version | `4.configuration/8.encryption-at-rest.md:536-541` ("Rolling a site back onto an older keyring version"); found in the re-check | Roll a site back onto a retained older keyring | None in v1.7.0. File it with the A-84 follow-up: a confirmed per-site rollback action that checks the retained version against the recorded digest history | **Break-glass, unsupported**: needs status RBAC, and a wrong version makes the site's tablespaces unreadable | A-84 |
| MFG `status.restoreReclones` removed | `7.backup-and-restore/2.backup-restore.md:930`; doctor `troubleshooting_playbooks.md:544`, `alert_runbook_matrix.md:58`; prose in `8.observability/2.monitoring.md:374,390` | Accept stale peers while the restored primary is down | #204 (deferred under D4): a confirmed `restore-reclones-release=<site>:confirm=<group>:uid=<uid>:token=<t>` annotation | **Break-glass, unsupported**: needs status RBAC, releases every queued site without validation, and since #233 restarts their replication from the current primary | A-85 |
| MFG `status.restoreReclones` removed (operator messages) | `PromotionRefusedPendingReclone` Event text (`internal/controller/runner.go:2149-2152`); `Degraded=NoPrimary` refusal text (`internal/state/matrix.go:219-221`) | The same: the message embeds the command | E18 rewrites both messages to name the break-glass runbook anchor instead of embedding the command. The mirrored log-schema row (`7.log-schema.md:268`) keeps its `msg` and fields | **Break-glass, unsupported** (by reference) | A-85 |
| MFG `status.restoreReclones.phase` → `Draining` | `2.backup-restore.md:913`; `troubleshooting_playbooks.md:548`; prose in `2.monitoring.md:390`; API comment `api/v1alpha1/backup_types.go:901-913` | Resume a `Held` queue | #204 scope extended: a `restore-reclones-resume` action bound to the group UID and the queue `confirmToken`. The API comment loses the administrator sentence now | **Break-glass, unsupported** until #204 | A-85 |
| MFG `status.conditions[<index>]` (`DivergentPromotion`) removed | `troubleshooting_playbooks.md:931-937`; `8.observability/7.log-schema.md:169`; `4.configuration/2.crd-reference.md:709` | Acknowledge a reviewed divergent promotion | `bloodraven.shipstream.io/acknowledge-divergent-promotion=<id>` and `kubectl bloodraven ack-divergence`, over `status.divergentPromotions[]` | Supported (WS-E) | A-86 |
| MDB `status.{ownerUser, pendingOwnerUser, ownerHosts, appliedUsers[]}` "patch the record out" | `4.configuration/7.tenant-databases.md:356` (section at `:352`) | Remove doubtful records left by older operators | Delete the section. The remedy is `deletionPolicy: Retain`, then delete the CR | Removed (D3) | A-87 |
| MFG `status.{activeSite, sites, conditions, lastFailover, lastFailoverTarget, promotionGtidExecuted, plannedFailover, updatePhase, recovery, dragonfly}`, plus `encryptionAtRest` on the wipe pass, removed, plus the failover annotations | `cmd/playground-chaos/reset.go:365-371,384-410` (via `playground/reset-mysql.sh`) | Playground wipe, so that `isFreshDeploy` re-bootstraps | None | **Break-glass, playground-only, unsupported on real clusters** | A-88 |
| MFG `status.{lastFailover, lastFailoverTarget, promotionGtidExecuted}` removed | s05 setup through `internal/playground/kube/mfg.go` | Test setup that bypasses the cooldown | None | Test tooling only | A-88 |

Metadata patches that stay documented, because they are not status: removing old taints after A-21, on the old
cluster's nodes. There is no old-finalizer patch any more (A-77).

## 5. v1beta1 promotion checklist (rewritten for D1)

This replaces "Promotion criteria for v1beta1" in `site/content/docs/4.configuration/3.crd-versioning.md:24-38`.
The old criteria 1 (a conversion webhook with envtest) and 4 (the chart installs the webhook Service, CA injection
and RBAC) are both replaced by C-1. C-4 is new: it is the D1 invariant test. The old criteria 2, 3, 5 and 6 map to
C-2, C-3, C-5 and C-6 (OPUS-20). That page's hub-and-spoke "Conversion strategy" section and its storage-migration
steps (`:55-112`) are rewritten to match C-1 and C-7 (OPS-19).

| # | Criterion | Current state at `2340f6b` | Closed by |
|---|---|---|---|
| C-1 | **Conversion strategy `None`.** v1beta1 is served with the same schema as the final v1alpha1. v1beta1 is the storage version; v1alpha1 is served and marked `deprecated`, and removed in the next minor release. A conversion webhook, with its chart Service, CA injection and envtest, is a release blocker for the first release whose schemas diverge. | One version per CRD, `served: true, storage: true` (`config/crd/bases/shipstream.io_mysqlfailovergroups.yaml:7965-7966`). No `conversion:` stanza. | WS-F |
| C-2 | Every renamed, reshaped or removed field has a documented migration path. | Section 3 of this proposal (revision 3: the 3.4 table, and the 3.2 requirements for the v1.6.0 procedure). | WS-A (table and requirements), WS-E (keeps the table current), WS-F (writes and tests the procedure, C-16), WS-G (release notes) |
| C-3 | Defaults and validation are stable enough for server-side dry-run in production pipelines. | Partial. CEL rules per CRD: MFG 20, MBK 1, MBV 0, MDB 7, MSC 24. No dry-run test. `examples/*.yaml` are never applied in CI. Several timers are unbounded (A-38), and some fields are inert (A-56 to A-62). | WS-E (A-38, A-56 to A-62, A-90 to A-92, Duration-bounds CRD test); WS-F (dry-run of examples and fixtures against envtest) |
| C-4 | (OPS-20) **D1 invariant:** a test asserts that all served versions of each CRD have an identical `openAPIV3Schema`. The first failure means a webhook is now required (C-1). The same test fails when any CRD's compact JSON, with every served version, exceeds 900 KB (OPUS-27). That leaves headroom for the server-side-apply `managedFields` under etcd's default 1.5 MiB request limit. | Missing. Today MFG is 276 KB with one version, about 552 KB with two. | WS-F |
| C-5 | Generated CRDs in `config/crd/bases/` and `charts/bloodraven/crds/` are checked for drift in CI, and so is the hand-maintained chart ClusterRole against `config/rbac/role.yaml`. | CRDs done: the `generate-check` job (`.github/workflows/ci.yml:226-260`; `git diff --quiet` at `:245`, `diff -r` at `:256`). Gap: the ClusterRole is not checked (OPS-21, D-15). | WS-F (rule-coverage test) |
| C-5b | **`migrate-storage` caller RBAC (R19).** D5 keeps CRD write permissions off the operator, so the person or job running `migrate-storage` holds them. It needs `get`, `list` and `update` on the five kinds, `get` on `customresourcedefinitions`, and `update` on `customresourcedefinitions/status` to trim `storedVersions`. The docs ship an example `bloodraven-storage-migrator` ClusterRole. `migrate-storage` checks these permissions with a `SelfSubjectAccessReview` before any write and refuses with the list of missing ones. A chart test asserts the operator ClusterRole has no write verb on `customresourcedefinitions`. | Missing. | WS-F |
| C-6 | (OPS-20) Backward compatibility is tested against one real older CR fixture per kind. | Missing. The only `testdata` is `internal/sidecar/testdata`. | WS-F: `test/fixtures/v1alpha1/<kind>.yaml`, frozen at `v1.7.0-rc.0` |
| C-7 | **D5 storage migration.** `kubectl bloodraven migrate-storage` does three things. (1) A server-side `dryRun=All` update of every object of the five kinds; it refuses the whole run before any write if one fails, listing each failing object with a migration-table pointer (generic, no legacy field names). (2) A read and write of every object. (3) It trims `status.storedVersions` to `[v1beta1]`. It never deletes and recreates objects (A-89). At the 1.31 floor a no-op dry-run update ratchets every unchanged enum, pattern and non-transition CEL violation, so the refusal comes from what ratcheting does not skip: type errors (`cloneTimeout: 3600`, a string `updateStrategy`, `retention: 7`), missing `required` fields (`sites[].serverID`, `credentials`) and transition rules (OPUS-R2-05). Every v1.6.0 MFG fails; an MBK or MBV whose only differences are renamed enums or removed fields may pass, which is why the run is refused as a whole. Envtest covers it: install the v1.6.0 CRDs, create objects, swap in the new CRDs, and assert that every v1.6.0 MFG is listed and no `resourceVersion` changed; then the happy path. That envtest stays even if C-8's amendment is approved: it pins what happens when someone applies v1.7.0 over v1.6.0 (3.2 U-1). | Missing. No subcommand in `cmd/kubectl-bloodraven/`. | WS-F |
| C-8 | **D6 upgrade E2E. PROPOSED AMENDMENT to D6, pending orchestrator (Barry) and Colin sign-off and a program-plan update.** D6 as written installs the published v1.6.0 chart and images, creates objects through v1alpha1, upgrades to the branch build, runs `migrate-storage`, and asserts `storedVersions == [v1beta1]`, health and failover. **D6 as written cannot pass (R01, OPUS-05):** D6 conflicts with D2. D2's breaking renames include required fields and persisted defaults that change type, so a v1.6.0 object can be neither decoded nor re-applied under the cleaned schema (3.1). `migrate-storage`'s dry-run refuses every v1.6.0 MFG by design (C-7). A green D6 job would need the in-place path that 3.1 shows is impossible, or the legacy rewriting that D2 and D3 forbid (MBOD Q5 option C, rejected 3-0). **Until the amendment is signed off, WS-F builds C-7's refusal envtest and C-16's procedure test, and does not start the upgrade E2E job (OPUS-R2-12).** Revision 2 said "D6 as written stands", which told WS-F to build a job this row proves cannot pass. **Proposed (the MBOD Q5 outcome, 3-0):** the baseline is `v1.7.0-rc.0`, cut after WS-E merges (v1alpha1 only, cleaned schema). Install the rc.0 chart and images, create objects of every kind through v1alpha1, upgrade to the v1beta1 build, and run `migrate-storage`. Then assert `storedVersions == [v1beta1]`, group health, a working failover, and A-52's sidecar rollout: with `sidecar.image.{repository, tag}` set to the branch build, an unpinned group runs exactly one ordered update, and a pinned group runs none. **What still covers v1.6.0:** the C-7 refusal envtest on the v1.6.0 CRDs, C-14's startup refusal envtest, and C-16. **Prerequisite (OPUS-05):** `release.yml` triggers on `v*.*.*` (`.github/workflows/release.yml:5-6`), which matches `v1.7.0-rc.0`, and always adds `type=raw,value=latest` (`:157`). Cutting rc.0 today would move `ghcr.io/shipstream/bloodraven:latest` and `bloodraven-sidecar:latest` to a schema-breaking pre-release. Before the cut, pre-release tags (`-rc.N`) must not push `latest`, and their GitHub release must be marked as a pre-release. The program plan gains a wave between E and F: "cut `v1.7.0-rc.0`". | Missing. | WS-F (the job once signed off, and the `release.yml` pre-release rule); Barry (sign-off, the plan update, and the rc.0 cut after WS-E merges) |
| C-9 | Status writes work through every served version, and destructive operations still require their confirmation through both versions. | Missing. Envtest installs only `config/crd/bases` (`test/envtest/suite_test.go:43-44`). | WS-F |
| C-10 | The redesigns that reshape API semantics have landed (D4): #235, #206, #203. #205 is a proposal only. #204 is deferred and additive (A-85 labels its gap break-glass). | All open at `2340f6b`. WS-C (#235) and WS-D (#206) are in progress; proposal 21 (#205) is in review. | WS-C (#235), WS-D (#206, #203), WS-B (#205 proposal) |
| C-11 | The breaking cleanup (D2) has landed in v1alpha1. | Not started. | WS-E (section 6) |
| C-12 | Docs match the contract. `3.crd-versioning.md` is rewritten for D1 and D5, and adds the confirmation-pattern rule (A-82). The CRD reference, the well-known metadata page (A-79), the Event registry (A-76), the log schema, the doctor skill and the published course are current. The upgrade docs say plainly that there is no in-place upgrade from v1.6.0 and carry the procedure that meets 3.2 (C-16). `10.upgrade-policy.md`, `11.gitops.md` and course unit 7 warn that their "CRDs first, then the chart" routine does not apply to v1.6.0 → v1.7.0 (3.2 U-1). | `3.crd-versioning.md` still requires a webhook and the storage-version migrator (`:28,33-34,55-112`). | WS-E (field docs, doctor, log schema, course, and the U-1 warnings through E19), WS-F (`3.crd-versioning.md`, upgrade docs) |
| C-13 | Release: `v1.7.0` (D7). A minor version that breaks v1alpha1 and has no in-place upgrade looks routine to anyone who reads only the number, so the release notes open with a breaking-change callout before any other content: no in-place upgrade from v1.6.0, never apply the v1.7.0 CRDs to a v1.6.0 cluster, and a link to the procedure (C-16). They then carry the section 3 table, the Event rename table, the metric, `/deploy/v1`, `/status` and `/ws/status` value changes (A-114), the log-schema breaks and the new Kubernetes floor (A-115). | — | WS-G |
| C-14 | **The operator refuses a schema it was not built for.** Revision 2 checked only that each CRD serves `v1beta1`, which every CRD does from v1.7.0 on, so the check would stop protecting anything after this release (OPUS-R2-11). Before any controller starts, the operator runs two checks and on failure logs each problem with a pointer to the upgrade docs and exits non-zero. **(a) CRD schema stamp.** `make manifests` stamps each CRD with the annotation `bloodraven.shipstream.io/schema-digest: sha256:<hex>` (Operator-owned read-only in the A-79 page), a hash of the canonical JSON of its served versions' `openAPIV3Schema`, and the operator embeds the digests of the CRDs it was built with. A missing or different digest, or a CRD that does not serve `v1beta1`, refuses. This catches a chart-only `helm upgrade`, which never upgrades CRDs from `crds/`, in every later release. The C-5 drift job checks the stamps, and C-4's identical-schema test shares the digest code. **(b) Object preflight.** A server-side `dryRun=All` no-op update of every MFG, reusing C-7's code. It writes nothing and refuses rather than adopts, so D3 allows it. It catches v1.7.0 CRDs applied over v1.6.0 objects, the case (a) cannot see because the CRDs then match (OPUS-R2-02). **Cost:** a stamp mismatch also refuses an operator rolled back without its CRDs. The rollback docs say to re-apply the matching CRDs, and `--allow-crd-schema-skew` (default off) starts anyway with a Warning Event and log line, for an operator who has checked the difference. Rejected: a monotonic schema revision that lets an older operator run against newer CRDs, which needs release bookkeeping and still cannot tell a compatible newer schema from an incompatible one. Envtest: v1.6.0 CRDs and objects, swap in the v1.7.0 CRDs, start the manager, and assert that it exits and no object's `resourceVersion` changed; and a CRD with a wrong stamp refuses. RBAC: `get` and `list` on `customresourcedefinitions` (read-only, which D5 does not object to) and the `update` verb on the five kinds that the operator already holds. | Missing. | WS-F |
| C-15 | **The supported Kubernetes floor is tested (A-115).** The chart `kubeVersion`, both docs pages, and a CI envtest job all say 1.31. | Chart `>=1.26.0`, docs 1.27, no job at either. | WS-E (E4: the job, the chart and the docs) |
| C-16 | **The v1.6.0 → v1.7.0 procedure is written and tested.** The upgrade docs carry a procedure that meets every requirement in 3.2 (U-1 to U-8), and WS-F runs it end to end once as U-9 describes. | Missing. Revisions 1 and 2 drafted steps here; revision 3 hands them to WS-F as requirements. | WS-F |

The v1 criteria in `3.crd-versioning.md:40-54` stay as they are. Its deprecation table still applies after
v1beta1.

**WS-F items that revision 3 adds.** They come with the C rows above and belong in WS-F's brief:

- Write the v1.6.0 → v1.7.0 procedure in the upgrade docs so that it meets 3.2 U-1 to U-8, and run it end to end
  once as U-9 describes (C-16). This replaces revision 2's instruction to "verify the 3.2 procedure".
- **Precondition for the upgrade E2E (OPUS-R2-12):** the D6 baseline decision (C-8) is signed off and the program
  plan updated before WS-F starts that job. Until then WS-F builds the C-7 and C-14 envtests and C-16.
- Build both C-14 checks, the schema-digest stamp in `make manifests`, and their envtests.

## 6. WS-E work list

### Preconditions and sequencing

- WS-E branches from `main` after WS-C (#235) and WS-D (#206, then #203) merge, and after #233 and #234. It
  then reshapes on top of their fields instead of colliding with them.
- **Step 0, `plannedFailover` re-audit (revision 3, OPUS-R2-03).** Before E1, WS-E checks `spec/status.plannedFailover`,
  the `AutomaticFailoverHeld` condition, the outcome and reason enums and the #235 annotations against the A-50
  checklist, as #235 merged them. It records each gap in its PR and fixes it in the commit named in A-50 (E5, E7,
  E8, E10, E11, E12, E14 or E15). It does not change claim semantics.
- WS-E does not touch: `pointInTime.sites`, `allowUnverifiedCrossSite` and `archiveProfileName` (A-113); or
  #203's encryption binding. It does apply A-25 to whatever #203 adds.
- A-82 (monotonic tokens for declarative destructive operations) is in no E commit. It is a documentation rule
  owned by C-12 in WS-F, as A-50's semantics are owned by #235 (OPUS-R2-14).
- A-97 is written against the PITR fields as #206 leaves them.
- **Re-triage before E1 (OPUS-08).** This audit was re-checked at `2340f6b`. Before E1, WS-E diffs `api/`,
  `config/crd`, `site/`, `.agents/` and the emitted Event and condition reasons between `2340f6b` and its own base.
  WS-C and WS-D will have added fields, reasons and keys; WS-C's are named in A-50. WS-E triages each one against
  sections 1-4 (inventory row, finding, migration row, status-write row) and records the result in its PR before
  E1. This is the step
  that found #233's `fencedGtidExecuted`, `RestoreReclonePending` and `RestorePeerHold` for this revision (B.4).
- Regenerate the appendix (`proposals/tools/crd-inventory.py`) at the start and the end of WS-E.

**Every commit runs the repo gate:**

- `make generate && make manifests`, then copy `config/crd/bases/*` to `charts/bloodraven/crds/`
- `make vet`, `make lint`, `make test`
- `make test-envtest` whenever validation, list types or status writes change

**It also follows the AGENTS.md obligations:**

- Mirror RBAC marker changes into `charts/bloodraven/templates/clusterrole.yaml`.
- Update `site/content/docs/` for the changed fields.
- Update the doctor skill (`SKILL.md`, `references/*.md`, `scripts/*.sh`) when status fields, runbooks or
  annotations change.
- Update `8.observability/7.log-schema.md` when a documented `msg` or field changes.
- Add the `e2e` PR label when failover, restore, PITR or sidecar behaviour changes.
- **The published course (OPUS-06).** `course/course-bloodraven-in-production/` is built into `site/public/courses`
  and gated by an exact-match drift check (`.github/workflows/ci.yml:319-343`, `make course-check`). It uses the
  values WS-E renames: `primary-candidate` appears in 105 files, and others include `dr-only`, `lbIP`,
  `sidecarImage`, `operatorSecret`, `shipstream.io/role`, `groupRef` and `stopDatetime`. Unit 7 also teaches
  "bump `spec.sidecarImage`" (`unit-7-day-0-and-day-2/unit-test.md:125,128`). Every commit that renames a token
  the course uses updates `course/`, runs `make course`, and commits the rebuilt `site/public/courses` in the same
  commit, or the drift job fails. E19 closes any remainder with a CI gate.

The commits below are ordered so each one builds and passes on its own.

| # | Commit title | Findings | Areas touched | Tests to add | Docs, doctor, chart and log obligations |
|---|---|---|---|---|---|
| E1 | Export well-known keys, condition types, reasons and Event reasons from the API package | A-69, A-76 (registry scaffold), A-79 (constants and `ReadOnlyTaintKey` with today's values, including the `encryption-status-fence` key) | new `api/v1alpha1/{well_known,conditions,events}.go`; controller, plugin and runner use the constants | registry test: every `Eventf` reason and every condition reason is declared | none (no behaviour change) |
| E2 | Remove MSC fields and status for unimplemented phases | A-60, A-61, A-62, A-63, A-06, A-64 | `mysqlstandbycluster_types.go`, `standbycluster_reconciler.go`, `examples/standby-cluster.yaml` | envtest: `Network` rejected, PVC source unrepresentable; discovery status unit tests | `5.architecture/3.multi-cluster-dr.md`, `crd-reference.md`; chart CRDs (MSC CRD shrinks by about 237 KB) |
| E3 | Remove inert spec and status fields and the legacy DSN mode | A-03, A-56, A-57, A-58, A-59, A-65, A-31 | `types.go`, `backup_types.go`, `mysqlbackupverification_types.go`; credential and env rendering; `cmd/bloodraven/trigger*.go`, plugin `backup.go`/`verify.go`; `newTestFG` and fixtures; `playground/manifests` | envtest: an MFG without `spec.credentials` is rejected, and `spec.secretName` is pruned (OPUS-28); unit: a verification Job without storage | `security-model.md`, `production-hardening.md`, chart `NOTES.txt`, `6.backup-verification.md`; doctor references to `pvcName`/`podName` |
| E4 | Require `spec`; add list-map, set and atomic semantics; explicit site `serverID`; the Kubernetes floor | A-05, A-07, A-08, A-09, A-10, A-11, A-12, A-13, A-14, A-115 | all `*_types.go`; `reconciler.go` (`--server-id` from spec); examples and playground (`serverID: 101…`); `charts/bloodraven/Chart.yaml` `kubeVersion`; a new CI envtest job on 1.31 assets | envtest: a spec-less object of each kind is rejected (moved from E3 because A-05 lands here; R11); two field managers co-own `sites`, `extraContainers` and `conditions`; duplicate `serverID` rejected; `serverID` immutable; every admission test also runs in the 1.31 job | `crd-reference.md`, `failover.md`, `2.getting-started.md` and `4.install-production.md` (1.31); course examples with `serverID`; `e2e` label (`server_id` rendering) |
| E5 | One reference and Secret-reference shape | A-01, A-02, A-04, A-20, A-66, A-67 | types; `mysqldatabase_reconciler.go` index key; `cmd/bloodraven/deploy_api.go`; credential and S3 readers; plugin `backup`; `deployment.yaml` env name for `operatorEndpoint` | envtest: `backupRef: {}` rejected; MDB `failoverGroupRef` immutable; Secret name bounds; unit: A-67's URL becomes `--s3_endpoint` and `--s3_use_https` for `http`/`https`, with and without a port | every docs manifest; `11.deployment-api.md`; doctor scripts that read `spec.credentials.*`; the MDB `Group` printer column (A-94); course (`operatorSecret`, `groupRef`, `lbIP` via E6) |
| E6 | Name bounds, site removal refusal, and immutability rules on the kind roots | A-15, A-16, A-17, A-18, A-19, A-22, A-23, A-24, A-25, A-26 | MFG, MBK and MBV root XValidations; `StorageSpec`/`SiteSpec`; `PVCStorage` union; `backup_schedule.go` (`provisioned`); remove the MBV Job-label workaround | envtest table: every transition rule accepts unchanged updates, rejects changes and passes the CEL cost check; root `metadata.name` bounds and grammar (`a.b` and `1orders` rejected, R08); `provisioned` cannot be removed and re-added (R04); unit: `BackupStorageMismatch` on an owned PVC of the wrong class or size | `operations.md:381` (resize); `encryption-at-rest.md` (disable is refused); `4.backup-pvc.md`; course (`lbIP`); `e2e` label |
| E7 | CamelCase, typed enums for spec and status; stable metric label values; one vocabulary on the operator's JSON endpoints | A-27, A-28, A-29, A-30, A-32, A-33, A-114 (metrics, `/status`, `/ws/status` role and state) | types and constants; literal sweep across the controller, plugin, `internal/playground`, doctor `triage.sh`/`gtid-audit.sh`; `internal/metrics` label-value functions; the role and state adapter for `/status` (`topology.go:1124-1140`) and `/ws/status` (`:2136-2140`); the playground dashboard's comparisons and CSS classes (`playground/dashboard/index.html:17-18,131-150`) | unit: enum round-trip; unit: every `SiteRole` and `SiteState` maps to its documented label value; payload test: `/status` and `/ws/status` serialize every role and state as the CR value; envtest: old lowercase values and A-33's invalid reasons rejected | docs that show `primary-candidate` or `writable`; `3.monitoring-prometheus.md` and `2.monitoring.md` (PromQL, and the `/status` and `/ws/status` examples at `:543-651`), `examples/prometheusrule.yaml`; doctor references; log-schema fields that carry state values; course (`primary-candidate`, `dr-only`) |
| E8 | Timers, timestamps and numeric types: the Duration rule, bounds, zero semantics and `stopTime` | A-35, A-36, A-37, A-38, A-41, A-34, A-39 (R11, OPUS-03) | types; `topology.go` (drop the 0 → 5m mapping); `site_helpers.go` (drop the 0 → 300 mapping); retry and escrow readers; `restore_script.py` stop-time conversion and the verification Job's timestamp argument (A-39) | CRD test: reflects over the Go Duration fields and asserts both CEL bounds (OPUS-10); envtest bound checks; zero-value unit tests; envtest: `stopTime` without a zone rejected, `mode: Timestamp` needs `stopTime`, `mode: Latest` forbids it | `crd-reference.md` timer table; `production-hardening.md`; PITR docs (`stopTime`); course (`stopDatetime`) |
| E9 | One retention shape | A-55 | `backup_types.go`, `mysqlbackupverification_types.go`; `resolveRetention`, `pruneSuccessful` and the verification sweep | table test of keep-if-any (`keepLast`, `keepWithinDays`, `keepFailed`, explicit 0) | `2.backup-restore.md` retention section, with the `max(count, minKeep, 1)` mapping and the no-pruning note (R07); doctor playbook entries on retention |
| E10 | Status reshapes: replication state, encryption attribution, failover record, binlog coordinates, artifact identity, provenance; the durable-status markers | A-40, A-42, A-43, A-44, A-45, A-46, A-47, A-49, A-96, A-97, A-89 (markers and fail-safe test), A-114 (`replicationRunning`, `/ws/status` shapes) | status types; writers; `cmd/bloodraven/deploy_api.go` (`replicationRunning`); `internal/platform/websocket.go` `TopologyMessage` and its dashboard readers; `internal/dragonfly/info.go` field presence (A-44); restore and verification readers of `artifactStorage` (D-12); plugin `status.go`; s31, s37, s48 and s50; `// Durable:` markers | unit: verification and restore use the recorded claim and endpoint; lease gate uses the `replication` enum; deploy API: `replicationRunning` for each `replication` value; `/ws/status` carries `lastFailover{…}` and `sites[].replication`; unit: INFO without `master_sync_in_progress` leaves `syncInProgress` nil; component (`test/component`): the A-89 lost-status and fresh-deploy cases (R11) | doctor `triage.sh`, `SKILL.md`, alert matrix (`sealed` → condition); `8.encryption-at-rest.md`; the MBK `Location` printer column moves here with A-96 (OPUS-26) |
| E11 | Honest generations, the condition vocabulary and the deploy-API token set | A-68, A-70, A-71, A-72, A-73, A-114 (`/deploy/v1`) | `runner.go` (snapshot config generation); MBK, MBV and MSC `Ready` and their printer columns; chart ArgoCD Lua; `cmd/bloodraven/deploy_api.go` | unit: conditions are stamped with the applied generation; s19 assertion fixed; Lua test fixtures; course fixtures; deploy API test: every token field is drawn from the A-69 constants | `crd-reference.md` conditions; `11.deployment-api.md`, with the closed value set of every token field and the `replicationRunning` definition (A-43); `gitops.md`; the ArgoCD Lua in the chart |
| E12 | Event reason naming and types | A-74, A-75, A-76 (generated docs table) | Event call sites, including the six reasons added since `3fa1900` and the completion Event #235 leaves; s48 assertion | registry test (from E1) extended to doc coverage; unit: every metric `reason` label value written is a declared A-69 constant (A-114) | `2.monitoring.md` (generated), `7.log-schema.md` Event reference, doctor `SKILL.md`/alert matrix/playbooks; release-note rename table, including renamed metric `reason` values |
| E13 | Desired state out of annotations and status; no persisted sidecar default | A-48, A-51, A-52, A-53, A-98 | `dragonfly_upgrade.go` (no spec writes), encryption adoption gate, sidecar image env from the new chart values `sidecar.image.{repository, tag}` (chart `values.yaml`, `deployment.yaml`), the rollout queue and `--default-sidecar-rollout-admission-timeout`, MDB DDL, MBK finalizer | envtest: `SnapshotRestore` needs `snapshot.dir`; unit: a spec image that differs from the live StatefulSet drives the snapshot upgrade, an unknown live image does nothing (OPUS-18), and the operator never calls `Update` on spec; unit and component: the default-sidecar rollout queue runs one group at a time and waits for `Ready=True` (OPUS-07), skips and re-queues a group not admitted within the timeout, and halts on a started run that never reaches `Ready=True` (OPUS-R2-09); chart test: the env renders from `sidecar.image.*`; a `Retain` backup keeps its artifact | `5.app-integration.md` (Dragonfly upgrade); `10.upgrade-policy.md` (operator upgrades now roll unpinned sidecars, one group at a time); `7.tenant-databases.md`; doctor; course unit 7 (`sidecarImage`); `e2e` label |
| E14 | One key prefix, boolean label values, namespace-scoped taint, the hold family, and action-annotation grammar | A-77, A-21, A-78, A-80 | every label, annotation and finalizer constant (E1 makes this a one-file change plus selectors), including the MFG finalizer rename, `encryption-status-fence`, and the verification hold (`backup_verification_hold.go`, `cmd/bloodraven/pitr_retention.go`); `tainter.go` (delete `LegacyTaintKey`); selector-mismatch refusal; chart NetworkPolicy; playground and chaos tooling; the doctor escrow helper's keys | envtest and unit: selector-mismatch condition and Event without a retry loop; unit: taint key format and length; the new hold key protects a backup under an active verification and under a Terminating one (retention, cleanup and PITR cutoff); A-78 parsing (`gtidPrefix=`, values with `:` rejected, basic-form instants) | `12.well-known-metadata.md` (generated), `5.app-integration.md` tolerations, `architecture.md` labels, doctor scripts' selectors and hold queries (`triage.sh`, `support-bundle.sh`, `keyring-escrow-recover.sh`), `SKILL.md` and playbook 36; course (`shipstream.io/role`); `e2e` label |
| E15 | Divergent-promotion records and acknowledgement; GitOps-safe actions; plugin wrappers | A-86, A-81 | `gtid_audit.go`, `runner.go`; plugin `ack-divergence`, `rotate-keyring`, `revoke-leases` | unit: acknowledging one record keeps the others; a 17th record is counted in the message and emits `DivergentPromotionRecordsTruncated` (OPUS-22); plugin tests | `log-schema.md:169`, `crd-reference.md:709`, doctor playbook `troubleshooting_playbooks.md:931-937`, `gitops.md` |
| E16 | Admission validation for my.cnf inputs, cross-references and bounds; kind metadata; doc comments | A-90, A-91, A-92, A-93, A-94, A-95 | types; the D-03 controller fix | envtest: injection strings rejected, dangling profile references rejected, CEL cost passes | `crd-reference.md` |
| E17 | Hardened security-context defaults, gated | A-54 | one merge helper for MySQL, Dragonfly and Jobs; uid and gid 999; emptyDirs for `/tmp` and `/var/run/mysqld` | the Q6 gate: Restricted-PSS admission, fresh init, existing-volume restart, keyring, PITR, backup and restore, chaos failover, both images, local-path plus one CSI class. If it fails, ship option C and record why. | `production-hardening.md` (drop the FSGroup upgrade procedure); doctor; `e2e` label |
| E18 | Runbooks and operator messages without status writes | A-83 (rule only; the patch is already gone), A-84, A-85, A-87, A-88, A-89 (docs) | docs and doctor; the `backup_types.go:901-913` comment; the two operator messages (`runner.go:2149-2152`, `internal/state/matrix.go:219-221`; OPUS-02) | unit: neither message contains `--subresource=status`; the lint below runs in CI | `4.runbooks.md`, `8.encryption-at-rest.md` (escrow and keyring-rollback break-glass labels), `2.backup-restore.md`, `2.monitoring.md`, `7.tenant-databases.md`, `7.log-schema.md:268` (checked; `msg` unchanged), `chaos-scenarios.md`, `3.playground.md`, doctor `troubleshooting_playbooks.md` and `alert_runbook_matrix.md`; a lint that fails on `--subresource=status` outside break-glass blocks in `site/` and `.agents/`, and in string literals in `internal/` and `cmd/` |
| E19 | Course sweep, the U-1 warnings, and the old-token gate | OPUS-06 (no finding of its own); 3.2 U-1 | `course/`; the rebuilt `site/public/courses`; `10.upgrade-policy.md`, `11.gitops.md` and course unit 7 get the "not for v1.6.0 → v1.7.0" warning (C-12) | **A context-aware CI gate (revision 3; GPT-R2-03, OPUS-R2-04).** Revision 2 grepped bare substrings, so it failed on a correct tree: `operatorSecret` is a prefix of `operatorSecretName`, `shipstream.io/role` is inside `bloodraven.shipstream.io/role`, and A-114 keeps `primary-candidate` as a metric value. The gate scans `course/`, `site/content/`, `.agents/` and `examples/`, case-sensitively, matching whole tokens in context only: (1) obsolete field names as YAML or JSON keys or as path segments, a token with no word character on either side followed by `:` or preceded by `.` (`lbIP`, `sidecarImage`, `sidecarResources`, `operatorSecret`, `groupRef`, `stopDatetime`, `sourceSiteOverride`, `mysqlBackupRef`, `maxMemoryMb`, … from the 3.4 table); (2) exact old metadata keys not preceded by a DNS-label character or `.`, so `bloodraven.shipstream.io/…` and the API group `shipstream.io/v1alpha1` never match (`shipstream.io/{role,site,failover-group,healthy,graceful-shutdown,db-readonly…}`, `hold.verification.shipstream.io/`); (3) obsolete CR enum values only as a YAML or JSON field value (`role: primary-candidate`, `"state": "writable"`, `mode: timestamp`, a scalar `updateStrategy:`), plus `healthy=yes|no`, `dragonfly-traffic=enabled`, and `dr-only` anywhere; (4) the A-74 old Event reasons as whole tokens. **Allowed:** PromQL label selectors (`role="…"`, `role=~"…"`, `state="…"`), whose kebab-case values A-114 keeps; metric `reason` values, which are the A-69 constants; and the migration mapping on the C-12 upgrade page, inside a marked allow block. Unit test of the gate: one old and one new sample per pattern (`operatorSecret:` fails and `operatorSecretName:` passes; `shipstream.io/role` fails and `bloodraven.shipstream.io/role` passes; `role: primary-candidate` fails and `{role="primary-candidate"}` passes). | the course; `make course` |

After E19:

- File the follow-up issues: A-84 re-escrow and the keyring-version rollback action, the D-01…D-14 defects that
  WS-E does not fix itself, and the #204 scope addition (`resume`, and the UID binding of both actions).
- Regenerate the inventory appendix.
- Hand the cleaned schema to Barry for the `v1.7.0-rc.0` cut, once C-8 is signed off.

## 7. Open questions

Six questions were hard to undo after v1beta1 and had no obvious answer. They went to a multi-model debate
(MBOD).

- Debaters: Claude Opus 5.5 (pragmatic operator), GPT 6.1 Sol through Codex (paranoid security) and Grok 4.7
  (keep it simple).
- All six were settled unanimously over three rounds.
- The table below is the record. The question briefs and the round transcripts were working files and are not
  published (OPUS-21).
- No question remains open. Revision 2 corrects one premise that Q4's and Q5's conditions rested on: that an
  in-place v1.6.0 upgrade was possible (section 3.1). The decisions stand. The conditions that assumed the
  in-place path lapse, and the Q5 baseline is presented as a proposed amendment to D6 (C-8). Section 8.1 has
  the details.

| Q | Topic | Decision | Round, vote | Rejected | Conditions | Findings |
|---|---|---|---|---|---|---|
| Q1 | Source of a site's `server_id` | **A**: explicit immutable `spec.sites[].serverID` (`int64`, 1..4294967295), CEL-unique per group, `self == oldSelf` per item | Round 1, 3-0. The drafter had recommended C. | B: allocated by the operator in status (identity lost on restore). C: name hash (opaque ids; a collision is found only at reconcile) | Examples use `101…` to match today's values | A-08 |
| Q2 | Timer encoding | **A**: spec timers are `*metav1.Duration`. Integer `xxxSeconds` only when forwarded to a core field or compared with a MySQL or DNS seconds value. Status measurements are integers with the unit in the name. | Round 2, 3-0 (GPT moved from B) | B: integer seconds everywhere (about 12 renames, including #235's timers). C: status quo | Every Duration has field-specific CEL `duration()` lower and upper bounds that fit `time.Duration`. A zero is rejected or documented per field. A generated-CRD test fails on a Duration without both bounds. Admission tests on Kubernetes 1.27 (revision 2: on the 1.31 floor, A-115). | A-35, A-36, A-38 |
| Q3 | Secret reference shape | **B**: `xxxSecretName` strings for whole Secrets; `xxxSecretKeyRef{name, key}` only where a key is selected | Round 1, 3-0 | A: `xxxSecretRef{name}` everywhere (re-keys the MDB `users` and `appliedUsers` ledger). C: status quo | MDB `secretName` list-map keys unchanged | A-02, A-58 |
| Q4 | Key prefix for labels, annotations, finalizers and the taint | **A**: everything Bloodraven-owned under `bloodraven.shipstream.io/`; boolean label values `"true"`/`"false"` | Round 2, 3-0 (Grok moved from B) | B: a documented split. C: status quo | Immutable selectors: the operator refuses on a mismatch with a condition and Event, without an update loop. The runbook recreates workloads one site at a time with a cascading delete (never orphaning), and never deletes the MFG. Old finalizers are reported and removed by a documented patch. Bundled with the namespace-scoped taint key. No dual-key code. (Revision 2: the runbook and old-finalizer conditions lapse with the in-place path; the selector-mismatch refusal stays. See A-77.) | A-77, A-21 |
| Q5 | Baseline for the D6 upgrade E2E | **A**: `v1.7.0-rc.0`, cut after WS-E (v1alpha1 only, cleaned schema). The E2E goes rc.0 → v1beta1 build → `migrate-storage` → `storedVersions == [v1beta1]`, health and failover. | Round 2, 3-0 (GPT moved from B) | B: a v1.6.0 baseline with a manual-path E2E. C: `migrate-storage` rewrites old field names (a D2/D3 shim) | `migrate-storage` first runs a server-side `dryRun=All` update of every object and refuses before any write, listing each failing object with a migration-table pointer (generic, no legacy names). Envtest pins this with the v1.6.0 CRDs (refusal list, unchanged `resourceVersion`s). v1.6.0 → v1.7.0 is documented as: export manifests, re-apply, never delete the MFG, recreate workloads per Q4, or a fresh install plus logical restore. (Revision 2: the in-place half is impossible (3.1), so only the second half remains, as the new-group procedure, whose requirements are now 3.2. The baseline amends D6 and needs sign-off; see C-8.) | C-7, C-8, section 3 |
| Q6 | Security-context defaults for MySQL and Dragonfly | **"A-gated"**: hardened defaults merged under user fields, one uid and gid 999 for MySQL, Dragonfly and every Job (replacing the hard-coded 27), launcher root filesystem writes plus `/tmp` and `/var/run/mysqld` on emptyDirs, no root init phase, no per-image branch | Round 3, 3-0 (Grok moved from C) | B: as-is semantics with renamed backup fields. C now, without attempting the gate | A timeboxed gate: Restricted-PSS admission, fresh init, existing-volume restart, keyring, PITR, backup and restore, chaos failover, on both images and on local-path plus one CSI class. If the gate fails or is incomplete, ship C (one merge helper, empty MySQL and Dragonfly defaults) and record why. The API shape is identical in both outcomes. | A-54, E17 |

## 8. Decisions record

Program decisions D1-D7 (#237) are inputs and are not repeated here. "Settled" means the drafter decided
using the zero-production-users rule. "MBOD" means the section 7 debate decided.

| Finding | Decision | Rejected alternative | By |
|---|---|---|---|
| A-01 | MDB `groupRef` → `failoverGroupRef` | Keep two names for one type | Settled |
| A-02 | `xxxSecretName` / `xxxSecretKeyRef{name,key}` | `xxxSecretRef{name}` objects everywhere | MBOD Q3 |
| A-03 | Remove DSN `spec.secretName`; `credentials` required | Freeze both credential modes | Settled |
| A-04 | `LocalMysqlBackupRef`, field `backupRef` | Keep `corev1.LocalObjectReference` with `{}` admitted | Settled |
| A-05 | `spec` required on all kinds | Optional `spec` | Settled |
| A-06 | Remove MSC `source.{failoverGroupName,namespace,cluster,profileName}` | `source.failoverGroupRef{…}` (GPT); make them optional (KINDS) | Settled |
| A-07 | Conditions are list-maps everywhere | Atomic lists | Settled |
| A-08 | `sites` list-map; explicit immutable `serverID` | Status-allocated id; name hash | MBOD Q1 |
| A-08 | Keep the seed rule (`sitePriorities`, then declared order) | New `spec.bootstrapSite` | Settled |
| A-09 | Profiles and schedules: list-maps, `MaxItems=32`, names ≤ 63 | Names ≤ 40 plus a root length rule (MFGSPEC) | Settled |
| A-10, A-11 | Named spec and status lists are list-maps with required keys | Atomic | Settled |
| A-12, A-13 | Sets for schema lists; ordered lists explicitly atomic; `sitePriorities` unique | Implicit atomic | Settled |
| A-14 | `deploymentClients` keyed by (namespace, serviceAccount); breaking (revision 2) | Keep one ServiceAccount per namespace | Settled |
| A-15 | Admission bounds: site ≤ 20, group + site ≤ 41 (revision 3), MBK/MBV names ≤ 63; group name a DNS-1035 label (revision 2) | Hash-truncate generated names (GPT) | Settled |
| A-16 | Site removal and rename refused | Allow, and implement pruning now | Settled |
| A-17 | Site storage class, size and zone immutable | Grow-only size rule (expressible at the 1.31 floor, but PVC expansion is not implemented) | Settled |
| A-18 | Backup PVC `claimName \| provisioned` union; presence-checked parent rule plus a reconcile-time `BackupStorageMismatch` whose comparison is defined (nil class matches any; size against the PVC request; larger accepted; revision 3); shared `ProvisionedVolumeSpec` with #205 | CEL forbidding size and class next to `claimName` only; a field-level `self == oldSelf`; a literal or `status.capacity` compare | Settled |
| A-19 | `loadBalancerIP` with `Format=ipv4` | CEL `isIP()` (available at the floor, but it adds CEL cost) | Settled |
| A-20 | `sourceSite`/`targetSite`/`site` naming rule | `sourceSiteOverride` | Settled |
| A-21 | Taint `<ns>.db-readonly.bloodraven.shipstream.io/<group>`; no legacy key | Hashed key (GPT); keep stripping the legacy key | Settled and MBOD Q4 |
| A-22 | MBK/MBV execution fields immutable; cleanup knobs mutable | Defend against edits in the controller | Settled |
| A-23 | `initFromBackup` fully fixed at creation | Allow edit and removal, forbid only adding (MFGSPEC) | Settled |
| A-24 | Encryption disable refused; keyring paths immutable while enabled | Toggle back and forth | Settled |
| A-25 | MFG transition rules on the kind root | Rules on the shared spec type | Settled |
| A-26 | `confirm` stays a string with `Format=date-time` | `metav1.Time` (breaks token byte equality) | Settled |
| A-27 | CamelCase typed enums; role `DisasterRecovery`; drop `none`; `updateStrategy` object | `DROnly`; keep `None`; scalar `updateStrategy` | Settled |
| A-28, A-29 | CamelCase typed status enums; `UpdatePhase` in API without `Complete` | Keep lowercase to match MySQL vocabulary | Settled |
| A-30 | Structured status scope; spec keeps inferring from `includeSchemas` | An explicit spec `scope` (GPT) | Settled |
| A-31, A-32 | Drop unused MBV phases; MDB `Creating` → `Applying` | Freeze never-entered values | Settled |
| A-33 | Typed reason strings with constants, plus reason pattern, length and `sourceConvergenceReason` enum; breaking, status-only (revision 3) | Free strings | Settled |
| A-34 | `sanityCheck{query, minResult, timeout}` and an `outcome` enum | Keep `expect` wrapper and `ran bool` | Settled |
| A-35 | Duration rule | Integer seconds everywhere | MBOD Q2 |
| A-36 | No Go `int` in the API | Add a format only | Settled |
| A-37 | Pointers where zero is valid; no zero-to-default mapping | Non-pointer with an `omitempty` default | Settled |
| A-38 | Lower and upper bounds on every Duration; CRD test; cooldown ≥ 30s | Lower bounds only; allow a zero cooldown | MBOD Q2, settled (cooldown) |
| A-39 | `stopTime` as `metav1.Time` everywhere | Keep `stopDatetime` with its MySQL form | Settled |
| A-40 | `lastFailover{time,target,promotionGtidExecuted}` struct; `*Time` names | Rename to `lastFailoverTime` only | Settled |
| A-41, A-42 | `maxMemory` Quantity; shared `BinlogCoordinate` | Keep `maxMemoryMb`; keep two coordinate shapes | Settled |
| A-43 | `replication` enum; `/deploy/v1` `replicationRunning` keeps its name, defined as `Healthy` or `Lagging` on every non-active site (revision 3) | `replicationHealthy *bool` | Settled |
| A-44, A-46 | `*bool` for observed flags | Fill unknowns with `false` | Settled |
| A-45 | `status.encryption{mode, algorithm}`, extended by #203 | `encrypted *bool` | Settled |
| A-47 | Remove `encryptionAtRest.sealed` | Make it `*bool`; keep it | Settled |
| A-48 | `spec.dragonfly.upgradeStrategy`; the operator never writes spec | Annotation carrying the image | Settled |
| A-49 | Remove `status.pitr.enabled` and `status.dragonfly.enabled` | Keep the echoes | Settled |
| A-50 | #235 owns semantics and record structure; WS-C lands the shape against the A-50 checklist, and WS-E's first step re-audits and fixes any gap (revision 3) | "WS-E does not touch this block" (revision 2), which left the must-fix with no owner; WS-E redesigning the claim | Settled (revision 3; orchestrator ruling on OPUS-R2-03) |
| A-51 | `spec.encryptionAtRest.existingData` | Keep the persistent `encryption-adopt` annotation | Settled |
| A-52 | `spec.sidecar.image` with no default (operator-built default from chart values `sidecar.image.{repository, tag}`); default-driven rollouts serialized one group at a time and health-gated, with an admission timeout so a group that never starts cannot block the queue (revision 3); `spec.sidecar` stays a value type with `default={}` | A persisted CRD default bumped every release; a one-shot `roll-sidecar` action; a required pin; the chart key `sidecarImage.*`; a queue that waits forever on a group that never starts | Settled |
| A-53 | No collation default; breaking (revision 2) | `utf8mb4_0900_ai_ci` default | Settled |
| A-54 | Hardened merged defaults, uid 999, behind a gate; fallback C | As-is semantics (B); C without trying | MBOD Q6 |
| A-55 | `retention{keepLast, keepWithinDays, keepFailed}` on both kinds; migration `keepLast = max(count, minKeep, 1)`; no-pruning has no equivalent | `count`/`minKeep`/`maxFailedKeep`; keep the legacy integer; a finite default silently replacing no-pruning | Settled |
| A-56 to A-61 | Remove inert or unimplemented fields, values and status | Keep them "for compatibility" | Settled |
| A-56 | Remove `s3.storageClass` | Wire it to mysqlsh before the freeze | Settled |
| A-62 | Remove MSC `spec.template` | Keep it with CEL excluding activation-owned fields (KINDS) | Settled |
| A-63, A-64 | `source.s3`; structured discovery status | The `BackupStorage` union with a forbidden PVC arm; flat fields | Settled |
| A-65 | `triggeredBy` becomes an annotation | `spec.origin` (OPS) | Settled |
| A-66, A-67 | `operatorEndpoint`; `s3EndpointURL` replacing `s3Endpoint` and `s3UseHTTPS`, adapted to Dragonfly's `host:port` and HTTPS flags | Keep the old names; rename `s3Endpoint` without an adapter | Settled |
| A-68 | `status.observedGeneration` from the applied config generation | The fetched object's generation | Settled |
| A-69, A-70 | Constants in `api/`; `Ready` on every kind now | Defer `Ready` (KINDS) | Settled |
| A-71 to A-73 | Specific `Ready` reasons; polarity fixes; one meaning per token | `TopologyPolled` for both values | Settled |
| A-74, A-75 | Full Event rename table and one type per reason | Fix only within-family inconsistencies | Settled |
| A-76 | Generated Event registry with a test | Hand-maintained page | Settled |
| A-77 | One `bloodraven.shipstream.io/` prefix; `"true"`/`"false"` values; hold family `<kind>-hold.bloodraven.shipstream.io/<id>`; MFG finalizer → `failover-group` | Documented split; status quo; dual-key reads for in-flight holds | MBOD Q4; hold and finalizer settled in revision 2 |
| A-78, A-82 | Annotation grammar `<subject>(:k=v)*` with values restricted to `[A-Za-z0-9._/+-]`, basic-form instants, `uid=` binding; monotonic spec tokens | Positional reclone prefix; `;` as separator (a shell metacharacter) | Settled |
| A-79, A-80 | Exported keys, a generated metadata page, versioned payloads | Copied strings | Settled |
| A-83 | Delete the `Sealed` patch; rely on `KeyringRecovered` | A typed recovery request (GPT) | Settled |
| A-84 | Escrow forging is break-glass; follow-up issue | Build re-escrow before v1beta1 | Settled |
| A-85 | Reclone patches are break-glass until #204, plus `resume` added to its scope | Pull #204 into v1.7.0 | Settled |
| A-86 | `divergentPromotions[]` and a per-id acknowledgement annotation | A condition-timestamp acknowledgement (STATUS) | Settled |
| A-87, A-88 | Delete the MDB patch advice; playground resets are break-glass | Keep them documented as supported | Settled |
| A-89 | Durable status stays in status, with a written contract | Move it to annotations or a ConfigMap | Settled |
| A-90 to A-92 | Admission patterns, cross-reference CEL, bounds | Reconcile-time checks only | Settled |
| A-93 to A-95 | `mbk`/`mbv` short names; new printer paths; no line numbers in doc comments | — | Settled |
| A-96 | Fold `storageType` into #232's `artifactStorage`; every consumer reads it | A tagged sub-union; keep the top-level duplicates | Settled |
| A-97 | Shared `provenance` struct, after #206 | Three copies of the fields | Settled |
| A-98 | MBK `deletionPolicy`, default `Delete` | Always delete artifacts | Settled |
| A-99 to A-113 | Leave (section 2.18) | See each row | Settled |
| A-114 | Metric label values are the kebab-case form of the API enums (only `dr-only` changes); metric `reason` labels are the A-69 constants; `/deploy/v1`, `/status` and `/ws/status` carry the CR values and shapes (revision 3), documented | Raw CamelCase metric values; freezing `dr-only`; lowercase or kebab-case on the JSON endpoints | Settled (revision 2; extended in revision 3) |
| A-115 | Kubernetes floor 1.31, tested by a CI envtest job | Keep 1.27 with a 1.27 job; an untested floor | Settled (revision 2) |
| Section 3 | No in-place v1.6.0 → v1.7.0 upgrade; a new group restored from a backup, with a per-field classification of what is dropped. Revision 3 replaces the step-by-step with requirements U-1 to U-9 that WS-F's written and tested procedure must meet | The first draft's export/re-apply path; patching the steps a third time | Settled (revision 2; orchestrator rulings on R02, R03, OPUS-01, and in revision 3 the scope rule) |
| C-7 | Dry-run preflight in `migrate-storage`; the v1.6.0 refusal envtest stays | Rewriting legacy fields | MBOD Q5 |
| C-8 | **Proposed amendment to D6:** `v1.7.0-rc.0` baseline, without moving `latest`. Until sign-off WS-F does not start the upgrade E2E (revision 3) | D6 as written (v1.6.0 baseline), which conflicts with D2 | MBOD Q5; **pending Barry and Colin sign-off** |
| C-14 | The operator checks a schema-digest stamp on each CRD and dry-runs every MFG before starting; `--allow-crd-schema-skew` for rollbacks (revision 3) | "Serves `v1beta1`" only, which stops protecting after v1.7.0; a monotonic schema revision | Settled (revision 3; orchestrator ruling on OPUS-R2-11) |

### 8.1 Review rounds

**Round 1** critiqued the first draft (`34ac15a`, audited at `3fa1900`):

- GPT 6.1 Sol: 11 Major and 8 Minor (R01-R19).
- Claude Opus 5.5: 9 Major and 19 Minor (OPUS-01 to OPUS-28).
- The critiques overlap: R01 with OPUS-05, R10 with OPUS-09, R11 with OPUS-03, and R02/R03 with OPUS-01.

Every point was checked against `2340f6b` before it was acted on. None was wrong on the facts. Where a
critique offered options, the choice and the rejected option are recorded in the finding.

What changed:

- **Upgrade path** (R02, R03, OPUS-01, OPUS-23). Section 3.1 states why no in-place v1.6.0 → v1.7.0 upgrade
  exists. Section 3.2 gives the new-group procedure. Section 3.3 classifies every status record as rewritten,
  dropped or refused. The A-15 row now says "new group, never an edit". Q4's runbook conditions lapse (A-77), and
  C-14 makes the operator refuse stale CRDs.
- **D6 baseline** (R01, OPUS-05). C-8 is now a proposed amendment to D6, pending Barry and Colin's sign-off. It
  explains why D6 conflicts with D2 and keeps the v1.6.0 refusal envtest (C-7). It also requires that `-rc.N`
  tags do not move `latest`.
- **Served surfaces missing from the first draft:**
  - the verification hold and its family (R10, OPUS-09, A-77);
  - `ArtifactCleanup` (R10, 1.2);
  - metric label values and `/deploy/v1` tokens (OPUS-04, A-114);
  - the operator's status-patch messages (OPUS-02, A-85, E18);
  - the course (OPUS-06, E19);
  - the Lease labels (OPUS-24).
- **Rules that would have admitted or rejected the wrong objects:**
  - `provisioned` presence (R04, A-18);
  - CEL guards on optional fields (R05, A-91);
  - the group-name grammar (R08, A-15);
  - the Dragonfly endpoint adapter (R06, A-67);
  - the retention mapping (R07, A-55);
  - the `syncInProgress` field-presence check (R17, A-44);
  - a CRD test that would have passed vacuously (OPUS-10, A-38);
  - CEL cost (OPUS-11, A-22, A-23, A-90).
- **Commit plan:**
  - the E3/E4 test dependency (R11, OPUS-28);
  - A-39 assigned to E8 (R11, OPUS-03);
  - the A-89 test in E10 (R11);
  - printer columns moved into the commits that change their paths (OPUS-26);
  - the re-triage precondition (OPUS-08).
- **Decisions the first draft lacked:**
  - A-52's fleet-wide rollout cost and its mitigation (OPUS-07);
  - the Kubernetes floor (R18, OPUS-19, A-115);
  - migrate-storage caller RBAC (R19, C-5b);
  - the CRD size budget (OPUS-27, C-4).
- **Corrections:**
  - A-14 and A-53 are breaking (R15);
  - zone is placement, not creation-only (R13, A-17);
  - `lastScanAt` is already typed (R14);
  - `MaxItems=16` already exists (OPUS-12);
  - no patch-strategy markers (OPUS-13);
  - the A-78 grammar and the #204 UID binding (OPUS-14, R12);
  - #203/#235 wording (OPUS-15);
  - proposal 21 alignment (OPUS-16);
  - `spec.sidecar` keeps `default={}` (OPUS-17);
  - Dragonfly drift detected from the live StatefulSet (OPUS-18);
  - criterion renumbering (OPUS-20);
  - no references to unpublished working files (OPUS-21);
  - the `divergentPromotions` overflow (OPUS-22);
  - the uid 27/999 risk (OPUS-25).
- **Re-check at `2340f6b`** (OPUS-08). A-83 is closed as done. `fencedGtidExecuted`, `RestoreReclonePending`,
  `RestorePeerHold`, six Event reasons and the escrow helper were triaged, and every citation was re-mapped (B.4).
  The re-check also found one status write the first draft missed: the keyring rollback.
- **R16** (collapsed-field validation in the generator) was fixed in `proposals/tools/crd-inventory.py`, and the
  appendix was regenerated. No text change was needed here.

Suggested fixes not taken. The point itself was accepted in every case:

| Point | Suggested fix not taken | Why |
|---|---|---|
| R01 | Restore v1.6.0 as C-8's baseline | It cannot pass under D2 (3.1). Per the orchestrator's ruling, the rc.0 baseline is presented as a proposed amendment to D6 instead of being substituted silently. |
| R02 | Preserve durable wire shapes where needed | That would freeze the old shapes that D2 exists to remove. The new-group path drops the history instead (3.3). |
| R09 | A metadata-only migration of existing escrow Secrets | No v1.6.0 group is upgraded in place, so a v1.7.0 operator never reads an old escrow Secret. The new group mints its own keyrings, and backups are keyring-independent (3.3). |
| OPUS-01 | A fresh install "into a new namespace" | CRDs are cluster-scoped, so the v1.7.0 schema cannot coexist with v1.6.0 objects in one cluster. A new cluster is required (3.2). |
| OPUS-01 | A hardened in-place sequence (operator at 0, ratcheting, temporary CRDs without `oldSelf` rules, staged selectors) | D3 rules out migration machinery, and there are no production users. |
| OPUS-09 | Retention honours both hold prefixes for one release | No hold crosses the key change, because there is no in-place path. Dual-key code would contradict Q4. |
| OPUS-14 | `;` as the pair separator | `;` is a shell metacharacter, so an unquoted `kubectl annotate` would run the rest as a command. Values are restricted instead. |
| OPUS-16 | Ask WS-B to rename `staging.volume` to `provisioned` | The two fields share one Go type. The names differ because the contexts differ (A-18). |
| OPUS-19 | Keep 1.27 and add a 1.27 envtest job | 1.27 is long past upstream support and lacks ratcheting. 1.31 is tested instead (A-115). |

**Round 2** critiqued revision 2 (`fbf1b30`). The two reviews number their points independently, so they are
cited here as GPT-R2-nn and OPUS-R2-nn:

- GPT 6.1 Sol: 4 Major and 5 Minor (GPT-R2-01 to GPT-R2-09).
- Claude Opus 5.5: 4 Major and 10 Minor (OPUS-R2-01 to OPUS-R2-14).
- Overlaps: GPT-R2-01 with OPUS-R2-01 (the version), GPT-R2-03 with OPUS-R2-04 (the E19 gate), GPT-R2-04 with
  OPUS-R2-10 (served tokens), GPT-R2-06 with OPUS-R2-06 (A-91 defaults), GPT-R2-09 with OPUS-R2-05 (ratcheting).

Every point was checked against `fbf1b30`, and the #235 point against WS-C's branch. None was wrong on its main
claim, and none was rejected.

What changed:

- **Release version** (GPT-R2-01, OPUS-R2-01). D7 is v1.7.0 (Colin, 2026-11-05), not v2.0.0. Every version
  string, including the proposed `v1.7.0-rc.0` baseline, now follows D7. Because a minor release that breaks
  v1alpha1 looks routine, C-13 requires the release notes to open with a breaking-change callout.
- **Section 3 narrowed (scope rule).** Rounds 1 and 2 both found Major problems in the v1.6.0 procedure: the
  in-place path in round 1, then CRDs-first and GitOps flows, shared archive prefixes and the wrong restore path
  in round 2 (OPUS-R2-02, GPT-R2-02, OPUS-R2-07). Under the program's three-rounds rule (three consecutive rounds
  of Major findings in one area mean re-planning, not re-patching), the procedure was narrowed to requirements
  before a third round, and handed to WS-F. 3.2 no longer gives steps. It lists requirements U-1 to U-9 that
  WS-F's written and tested procedure must meet (C-16, and the WS-F items after section 5). 3.1 is
  corrected for the 1.31 floor and the limits of ratcheting (OPUS-R2-05, GPT-R2-09), and 3.3 gains the MySQL
  accounts row and an honest "refused, with one gap" row.
- **Startup checks** (OPUS-R2-02, OPUS-R2-11). C-14 checks a schema-digest stamp, so it keeps protecting after
  v1.7.0, and dry-runs every MFG, so it catches v1.7.0 CRDs over v1.6.0 objects. C-7 says which errors cause
  its refusal.
- **D6** (OPUS-R2-12). C-8 no longer says "D6 as written stands". Until sign-off, WS-F builds the refusal envtests
  and the procedure test and does not start the upgrade E2E.
- **#235 ownership** (OPUS-R2-03). The branch already diverged from the house rules (A-50 lists how). WS-C lands
  the shape against the A-50 checklist, and WS-E's step 0 re-audits it and fixes any gap. This reverses round 1's
  OPUS-15 choice for this one block, because the branch is still at WP0. A-74 now asks #235 for
  `PlannedFailoverSucceeded`.
- **Served surfaces** (GPT-R2-04, OPUS-R2-10, GPT-R2-08). Section 1.5 adds the operator `/status` and
  `/ws/status`, the playground dashboard, `replicationRunning` and the metric `reason` labels. A-114 decides
  their spelling (JSON surfaces use the CR values; only Prometheus labels use kebab-case), and E7, E10, E11 and
  E12 carry the producer, consumer and test work. Section 1.3 adds `encryption-status-fence`, which came in with
  #234 and which B.4's re-check missed.
- **E19** (GPT-R2-03, OPUS-R2-04). The gate matches whole tokens in context, and allows the PromQL values A-114
  keeps and the metric reason values.
- **Precision fixes:** A-33 is breaking (GPT-R2-05); A-91's defaults (GPT-R2-06, OPUS-R2-06); A-07's merge-patch
  sentence (GPT-R2-07); the A-52 chart value and rollout-queue liveness (OPUS-R2-08, OPUS-R2-09); A-18's
  `BackupStorageMismatch` comparison (OPUS-R2-13); A-64's printer column, A-15's bound of 41, and A-82's owner
  (OPUS-R2-14). A-86's `promotedAt` becomes `promotionTime`, to match the `…Time` rule that the A-50 checklist
  applies.
- **Counts:** findings unchanged at 115 (49 must, 50 should, 16 leave); breaking 80 → 81; migration rows
  101 → 106.

Suggested fixes not taken, or not in full. The point itself was accepted in every case:

| Point | Suggested fix not taken | Why |
|---|---|---|
| OPUS-R2-01 | A section 7 note asking Colin whether a release with no in-place upgrade should be a major version | D7 is Colin's decision, taken after the plan made the breaking scope clear. The risk the note was meant to flag is handled by C-13's opening callout. |
| OPUS-R2-06 | "The guards still matter in a dry-run before defaulting" | Not verified: defaulting runs before validation on dry-run requests too. A-91 calls the guards redundant but harmless instead. |
| OPUS-R2-08 | Chart values `sidecarImage.{repository, tag}` | That key repeats the old spec field name that E19's gate rejects. `sidecar.image.{repository, tag}` mirrors the operator's `image.*`. |
| OPUS-R2-09 | A new `Updating=True/SidecarRolloutHalted` condition reason | A-72 already gives a stalled run `Updating=True/OrderedUpdateStalled`. The halt is a Warning Event, `SidecarDefaultRolloutHalted`, instead. Also, one citation was corrected: `encryption_reconcile.go:746` refuses a key rotation during an ordered update, and `deployment_leases.go:283` reports an ordered update as unstable; neither holds an ordered update. The point stands on the #235 hold and on groups that are not `Ready`. |

## Appendix A. Code defects found during the audit

These are code or docs bugs with no API-shape change. File each as an issue; WS-E fixes those its commits
already touch.

| ID | Defect | Evidence | Recommendation |
|---|---|---|---|
| D-01 (STATUS-20) | `Degraded` gets a new `lastTransitionTime` on every poll while a replication overlay applies. That causes a status write every poll and destroys "degraded since". | The topology reason is written first (`internal/controller/runner.go:1511-1518`), then overlays set it again (`:1539-1617`). `setCondition` (`:2480`) compares against the just-written value. | Issue. Compute the final `Degraded` once, then set it. |
| D-02 (STATUS-21) | `EncryptionAtRestReady` survives disabling encryption. | `encryption_reconcile.go:1136-1139` writes the old condition back when encryption is off. | Issue. Remove the condition when disabled, as `PITRArchiveReady` does. |
| D-03 | `pitr.maxBinlogSize` is written into my.cnf without the CR/LF/NUL check. | `reconciler.go:1079-1084` vs `unsafeMySQLSetting` at `:925`. | Issue, security label. Route it through the same check (admission half: A-90). |
| D-04 | The encryption adoption refusal is advisory: rendering follows spec regardless. | Refusal Event at `encryption_reconcile.go:93`; rendering keyed only on `Spec.EncryptionEnabled()` (`reconciler.go:811,825,854,1363`). | Issue. Rendering must honour the refusal. |
| D-05 | `--maxmemory` and `--proactor_threads` are documented as reserved but missing from the operator-owned flags, so a user arg wins. | `dragonfly_resources.go:316-326`. | Issue (admission half: A-92). |
| D-06 | CronJob names are truncated to 63 characters, but the CronJob limit is 52, so the create fails and aborts the reconcile. | `scheduleCronJobName` (`backup.go:97-99`) uses `truncateDNS1123` (`:124`). | Issue. Truncate CronJob names to 52 with the hash. |
| D-07 | `pollInterval: 0s` makes the topology loop spin. Only the Dragonfly runner clamps it. | `topology.go:1262`; `runner.go:943-945`. | Issue. Clamp in the controller as well as A-38. |
| D-08 | `initFromBackup` added to a live group loads a dump into the serving primary without fencing. | `restore.go:474-480,516-560`. | Admission fix in A-23. Also add a controller refusal (condition plus Event) when `status.activeSite` predates the restore. |
| D-09 | Conditions are stamped with the fetched object's generation, not the one the topology manager applied. | `runner.go:1479-1516`. | Fixed by A-68 (E11). |
| D-10 | Duplicate or mistyped Events: `RestoreInPlaceFailed` twice; `DatabaseDropSkipped` for an owner-user skip; a Warning `DragonflyPromotionCompleted` duplicating `DragonflySessionsLost`. | `restore_inplace.go:1005,1007`; `mysqldatabase_reconciler.go:1081,1086`; `dragonfly_topology.go:1402-1404`. | Fixed by A-75 (E12). |
| D-11 | The s19 assertion checks a `Bootstrapping` state the operator never writes, so it never fires. | `internal/playground/scenarios/s19_reclone_interlock.go:377` vs `runner.go:2405`. | Fixed by A-72 (E11). |
| D-12 | Verification and restore read the PVC claim and S3 location from the live profile or the location string, not from #232's recorded `artifactStorage`. | `backup_verification_job.go:287,392,442,461`; `restore.go:1046`. | Fixed by A-96 (E10). |
| D-13 | A planned-failover deferral rewrites the user's annotation. | `planned_failover_deployment.go:33-48`. | Owned by #235. |
| D-14 | Docs drift: the resize claim (`6.operations/2.operations.md:381`); Event kinds and two dead reasons in `8.observability/2.monitoring.md:353,427,430`; `revoke-deployment-leases` missing from the `2.crd-reference.md` annotations table; a controller comment claiming the CRD already rejects `transport: Network` (`standbycluster_reconciler.go:175`). | As cited. | Fixed by E6, E12, E14 and E2. |
| D-15 | The chart ClusterRole is not checked against `config/rbac/role.yaml` in CI. | `.github/workflows/ci.yml:226-260` checks only CRDs. | WS-F rule-coverage test (C-5). |

## Appendix B. Dropped and rejected raw findings

### B.1 Dropped: the premise was false after verification

| Raw finding | Claim | Why dropped |
|---|---|---|
| GPT-33 | Keyring recovery needs a new typed, confirmed request API | The operator already moves `Failed` → `Sealed` itself once the Secret is back and MySQL confirms the digest (`internal/controller/encryption_reconcile.go:674-690`). Deleting the runbook patch is enough (A-83). |

### B.2 Rejected parts of findings that were otherwise kept

| Raw finding (part) | Why rejected |
|---|---|
| STATUS-04 (`keyring-reseal` annotation) | Same reason as GPT-33. |
| STATUS-04 (acknowledgement bound to the condition's `lastTransitionTime`) | Superseded by OPS-16's per-record list (A-86). |
| MFGSPEC-13 (grow-only size via CEL `quantity()`) | The operator does not implement PVC expansion, so immutable instead (A-17). The first draft also cited the 1.27 floor; A-115 raises it to 1.31. |
| MFGSPEC-20 (CEL `isIP()`) | `Format=ipv4` costs nothing in the CEL budget (A-19). `isIP()` is available at the new 1.31 floor but is not needed. |
| MFGSPEC-01 (`serverID` allocated in status); GPT-07 (`spec.bootstrapSite`) | MBOD Q1 chose an explicit spec field; `sitePriorities` already overrides the seed (A-08). |
| GPT-10 (hash-truncate every derived name) | Admission bounds keep user-facing names predictable (A-15). |
| GPT-15 (reshape site storage into a claim template) | Site storage is always provisioned; immutability is enough (A-17). |
| GPT-14 (cleanup follows profile edits) | Fixed by #232 (`backup_cleanup.go:64-83`). The rest is kept as A-96. |
| KINDS-02 (`migrate-storage` rewrites `groupRef` in place); KINDS-10 (one-time coordinate rewrite); KINDS-12 (backfill from the profile) | Legacy rewrites that D3 and D5 rule out. A new group's status is written from its own observations, and the historical records are dropped (3.3). |
| KINDS-31 (keep `spec.template` with CEL) | Removed instead (A-62). |
| KINDS-04 (`Ready` on MBV and MSC can wait) | Done now, so its reasons freeze with the rest (A-70). |
| OPS-10 (`spec.origin`) | Operator-only provenance does not belong in spec (A-106). |
| GPT-17 (integer seconds everywhere) | MBOD Q2. |
| GPT-02, MFGSPEC-08 (`xxxSecretRef{name}` objects) | MBOD Q3. |
| GPT-40 (keep `shipstream.io/` routing labels) | MBOD Q4. |
| MFGSPEC-06 (option to allow a zero `failoverCooldown`) | The cooldown is anti-flap safety (A-38). |
| MFGSPEC-19 (`pollInterval >= 500ms`) | `1s` instead. The playground uses `2s`. |
| STATUS-01 ("Breaking: yes") | Classified non-breaking: an additive field (A-68). |
| STATUS-01 (drop `encryptionAtRest.observedGeneration`) | Kept: the encryption reconcile can lag the topology manager (A-68). |
| STATUS-18 ("nothing keys on `TopologyPolled`") | Wrong: `runner_test.go:33,43` and the course fixtures do. The finding stays, with those consumers added (A-71). |
| GPT-37 (explicit spec `scope`) | It would be a second way to say what `includeSchemas` says (A-30). |
| MFGSPEC-04 (keep `None` in `pointInTime.mode`) | Absence has one spelling (A-27). |

### B.3 Citation corrections made at `3fa1900`

The audits ran at `b5b525e`. #232 moved these lines; the proposal cites the corrected ones.

| Raw citation | Now |
|---|---|
| `2.backup-restore.md:692,702` (reclone status patches) | `:798,808` |
| `alert_runbook_matrix.md:44` | `:50` |
| `mysqlbackup_types.go:25` (`spec` field) and later fields | `:26`; status fields +12 to +24 lines |
| `deployment_leases.go:299-324` (`unstableReason`) | `DeploymentUnstableReason` at `:270` |
| `restore.go:479-497` (`restoreTargetSite`) | `:474-480` |
| `runner.go:1040-1122` (`restoreFailoverState`) | `:1053-1122` |
| `backup_reconciler.go:990-1008` (retention) | `resolveRetention` `:1347-1365`, `pruneSuccessful` `:1377-1413` |
| `backup.go:145` (`findProfile`) | `:153` |

### B.4 Re-check at `2340f6b`

#231, #233 and #234 merged after `3fa1900`. The API diff is one field plus comments. Many controller and docs
lines moved.

- About 100 citations whose content did not change were re-mapped line by line, from a diff of each cited file
  between `3fa1900` and `2340f6b`. Citations whose target changed were re-opened by hand: the A-83, A-84, A-85
  and A-86 docs, the escrow runbook, the reclone API comment and the playground reset.
- Triage of what was added:
  - `status.restoreReclones.fencedGtidExecuted`, together with the new `Held`/discard semantics (A-85, A-89, 3.3).
  - The `Degraded` overlay reason `RestoreReclonePending` (1.2).
  - The `sourceConvergenceReason` value `RestorePeerHold` (A-33).
  - Six Event reasons (1.4, A-74).
  - The doctor helper `keyring-escrow-recover.sh` (A-84).
  - The docs change that already completes A-83.
- One older status write that the first draft missed: the keyring-version rollback in
  `8.encryption-at-rest.md:536-541` (section 4, A-84).
- B.3 is left as it was. It records the earlier move from `b5b525e` to `3fa1900`.

