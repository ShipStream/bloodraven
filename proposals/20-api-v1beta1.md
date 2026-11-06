# Proposal: API audit and cleanup before `shipstream.io/v1beta1`

**Status:** Draft, revision 5 (after critique rounds 1, 2 and 3 and the ultra review of PR #242; see section 8.1)
**Branch:** `docs/api-v1beta1-audit`
**Tracking:** #237
**Audited at:** `3fa1900` (origin/main after #232). **Re-checked at** `2340f6b` (this branch with origin/main
`7c3626d` merged in, after #231, #233 and #234). Every `file:line` below refers to `2340f6b`. Citations that
moved were re-mapped line by line, and citations whose content changed were re-opened by hand. The diff of
`api/`, `config/crd`, `site/`, `.agents/` and the Event reasons since `3fa1900` was triaged against sections
1-4 (B.4). Revision 4's citations were checked at `b543405`. Outside `proposals/`, that commit differs from
`2340f6b` only in `site/app/` and `site/content/index.md`, so the `2340f6b` line numbers still hold, and the
`site/app/` citations refer to `b543405`. Revision 5's citations were checked at `a5f155d`, which differs from
`b543405` only in `proposals/`.
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
| Merged findings | 115 (A-01 to A-115; A-114 and A-115 added in revision 2; A-111 withdrawn into A-49 in revision 5) |
| must-fix | 49 (one, A-83, already done on main) |
| should-fix | 50 |
| leave | 15 (revision 5 withdraws A-111, F-39) |
| Breaking findings (one of them, A-50, owned by #235) | 81 (revision 3 adds A-33) |
| Migration-table rows | 110 (revision 3 added five: A-33 and four served-surface rows; revision 4 added two: A-28's `""` values and A-40's three timestamp names; revision 5 adds two: the restore-reclone metric `phase` values and the lease revocation reasons) |
| Raw findings dropped entirely / partially rejected | 1 / 22 |
| Code and docs defects (Appendix A) | 15 |
| Critique points, round 1 (GPT 19, Opus 28) | 47: all verified; none rejected as wrong; 9 suggested fixes replaced by another fix (section 8.1) |
| Critique points, round 2 (GPT 9: 4 Major, 5 Minor; Opus 14: 4 Major, 10 Minor) | 23: all verified and applied; none rejected; one citation corrected and 4 suggested fixes, or parts of them, not taken (section 8.1) |
| Critique points, round 3 (GPT 4: 4 Major; Opus 10: 5 Major, 5 Minor) | 14: all verified at `b543405` and applied; none rejected; one set of citations corrected and one suggested fix not taken; the v1.6.0 procedure split into a WS-F follow-up issue (section 8.1) |
| Ultra review of PR #242 at `a5f155d` (GPT 6.1 Sol and Claude Opus 5.5) | 68 confirmed findings (8 high, 40 medium, 20 low), F-01 to F-68: all checked at `a5f155d` and applied, none rejected; 6 suggested fixes not taken; the A-52 rollout queue and the migration-check mechanism narrowed to requirements; WS-E split into five PRs (section 8.1) |
| Open questions | 6, all settled by MBOD (section 7) |

### How to read this

- **Section 2** is the decision list. Each finding names its severity, whether it is breaking, the raw findings
  it merges, its evidence at `2340f6b`, and the exact new shape.
- **What is normative (revision 5, ultra-review merits point 3).** Section 2 and the section 3.4 table are
  normative. Sections 6, 7 and 8 restate decisions as summaries. Where a summary differs from section 2, section 2
  wins, and WS-E records the difference in its PR. The inline "revision N" markers are provenance only; 8.1 has the
  history.
- Findings that touch a pending redesign say **Owned by #N** and state the shape this cleanup expects that
  issue to leave. They do not plan the change. The redesigns are #235 (planned-failover claim), #206 (PITR
  archive identity), #203 (encryption identity), #205 (in-place PITR; its PR1-PR3, PR1 being #241, may land before
  WS-E, A-59) and #204 (reclone release, deferred).
- **Section 3** is the field-by-field migration table that goes into the v1.7.0 release notes. It also says
  why there is no in-place upgrade from v1.6.0, and lists the requirements for the new-group procedure that
  replaces one. WS-F writes and tests that procedure.
- **Section 4** lists every hand-written status path and its replacement.
- **Section 5** is the v1beta1 checklist rewritten for D1.
- **Section 6** is the WS-E commit list, delivered as five sequential PRs, of which only the two freeze-critical
  ones gate WS-F and rc.0.
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
| MysqlFailoverGroup (MFG) | 262 (revision 5: `sidecarResources` collapsed like every other resources block, F-09) | 192 (#233 added `restoreReclones.fencedGtidExecuted`) | 20 (0) | 9 | 8 (`updatePhase` untyped) | 3 | `mfg`, `mfgs` | bloodraven, mysql |
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
| `RestoreInPlacePhase` | MFG `status.restoreInPlace.phase` | `"";Preflight;Fencing;Restoring;Resuming;Succeeded;Failed` | yes | A-28 (`""`), A-59 (#205 phases) |
| `RestoreReclonesPhase` | MFG `status.restoreReclones.phase` | `Armed;Draining;Held` | yes, admin-written in runbooks | A-85 |
| `DragonflyPhase`, `DragonflyUpgradePhase` | MFG `status.dragonfly.phase`, `.upgrade.phase` | CamelCase | yes | — |
| `SiteKeyringPhase` | MFG `status.encryptionAtRest.sites[].phase` | `"";Pending;Unsealed;Escrowed;Sealed;Failed` | yes, admin-written in a runbook | A-83, A-28 (`""`) |
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
| `shipstream.io/failover-group`, `/site`, `/role`, `/healthy` (`yes`/`no`) | owned objects, Pods; `failover-group` also on MBK and MBV | O, and P for `failover-group` on the MBKs and MBVs it creates → O, U (selectors) | names; `primary`/`replica`; `yes`/`no` | `reconciler.go:54-57` | A-77 |
| `shipstream.io/dragonfly-role`, `/dragonfly-traffic` (`enabled`) | Dragonfly Pods | O → O | `master`/`replica`; `enabled` | `dragonfly_resources.go:35,46` | A-77 |
| `shipstream.io/spec-hash`, `/managed-service-annotations`, `/credential-hash`, `/system-schema-restriction` | Deployments, Services, Secrets | O → O | hash, JSON | `reconciler.go:61-62`, `credentials.go:24,30` | A-79 (Internal) |
| `shipstream.io/keyring-version` (label), `/keyring-digest` | escrow Secret | O, and U in a runbook → O | int; `sha256:…` | `encryption_escrow.go:27,32` | A-84 |
| `shipstream.io/mysqlbackup`, `/backup-profile`, `/backup-schedule`, `/resource`, `/backup-encrypted` | Job, CronJob, PVC, MBK, MBV | O, and P for `backup-profile` on the MBKs and MBVs it creates → O (the schedule rollup reads `backup-schedule`) | names, bool | `backup.go:19-23` | A-106 (leave) |
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

The plugin writes two annotations, `planned-failover` and `reclone-site`, and copies both key strings with a "must
match" comment (`cmd/kubectl-bloodraven/promote.go:21`, `reclone.go:18`). It also hard-codes two label keys,
`shipstream.io/failover-group` and `shipstream.io/backup-profile`, as string literals on the MBKs and MBVs it
creates (`cmd/kubectl-bloodraven/backup.go:119-120`, `verify.go:141-142`). Revision 4 said the plugin wrote only
the two annotations (revision 5, F-52); E1 moves `backup.go` and `verify.go` onto the constants with the rest.

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
too. Revision 2 listed the first and fourth rows; revision 3 added the last three (GPT-R2-04, OPUS-R2-10); revision 4
adds the second (OPUS-R3-06), and revision 5 the third (F-17). A-114 decides the spelling of each.

| Surface | Tokens it carries | Evidence | Consumers |
|---|---|---|---|
| Prometheus labels `role` on `bloodraven_site_state`, `bloodraven_replication_lag_seconds` and `bloodraven_replication_running` | `string(site.role)`: `primary-candidate`, `dr-only`, `read-only`; and label `state` on `bloodraven_site_state`: `writable`, `read-only`, `unreachable`, `unknown` | `internal/controller/topology.go:315`, `internal/metrics/metrics.go:132-135` | PromQL in `8.observability/3.monitoring-prometheus.md:112,118,139,144` and `2.monitoring.md:223` (`state="writable"`), `examples/prometheusrule.yaml`, user alert rules |
| Prometheus labels `phase` on `bloodraven_keyring_phase`, `state` on `bloodraven_replication_source_state`, and `from`/`to` on `bloodraven_state_transitions_total` (revision 4, OPUS-R3-06) | `strings.ToLower` of `SiteKeyringPhase` (`sealed`, …) and `SourceConvergenceState` (`converged`, `pending`, `blocked`); `SiteState.String()` (`writable`, …) | `internal/controller/encryption_reconcile.go:993`, `source_convergence.go:304`, `topology.go:1440`; `internal/metrics/metrics.go:406,421` | `6.operations/4.runbooks.md:156` (`phase="sealed"`), `2.monitoring.md:150` (`to="writable"`), `charts/bloodraven/dashboards/failover.json:163`, user alert rules |
| Prometheus label `phase` on `bloodraven_restore_reclone_pending` (new in #233; revision 5, F-17) | `string(q.Phase)`, the raw API enum `RestoreReclonesPhase`: `Armed`, `Draining`, `Held` (`api/v1alpha1/backup_types.go:886-913`) | `internal/controller/restore_peer_replication.go:645`; `internal/metrics/restore_reclones.go:15-18` | `8.observability/2.monitoring.md:70` (metric table) and `:212-213` (alert `BloodravenRestoreReclonesHeld`, `{phase="Held"}`), `3.monitoring-prometheus.md:134`, `7.backup-and-restore/2.backup-restore.md:790`, doctor `SKILL.md:171` |
| Prometheus `reason` labels on `bloodraven_planned_failovers_deferred_total`, `bloodraven_deploy_lease_revocations_total` and `bloodraven_backup_artifact_cleanup_failures_total` | reason tokens: `DeploymentHold`, `CleanupFailed` and `CleanupRejected` in CamelCase, but the lease revocation reasons in snake_case, `operator_revoked` and `topology_changed` (`deployment_leases.go:193-199,263`), which the `/deploy/v1` 409 body also returns (`:630,636`). Revision 4 called them all CamelCase (revision 5, F-18) | `internal/metrics/metrics.go:51-65,366-372`; written at `planned_failover_deployment.go:60`, `deployment_leases.go:263`, `backup_reconciler.go:809` | user alert rules and dashboards |
| `/deploy/v1` status JSON | `health.degradedReason`, `health.replicationRunning` (computed from `status.sites[].replicating`, which A-43 removes), `operations.updatePhase`, `operations.plannedFailover.{phase, reason}`, `operations.restoreInPlace`, `operations.dragonflyUpgrade`, `unstableReason` | `cmd/bloodraven/deploy_api.go:337-362`, `deployment_leases.go:270` | CI pipelines that gate deploys on the API (`4.configuration/11.deployment-api.md`) |
| Operator `GET /status` JSON | per site `role` (`string(role)`, so the API spec value), `state` (`state.String()`, the internal lowercase form), `sourceConvergenceState`, `sourceConvergenceReason`, `recoveryState` | `cmd/bloodraven/main.go:490`; `internal/controller/topology.go:1124-1140` | `8.observability/2.monitoring.md:543-590` (documented with `"state": "writable"`), user scripts |
| Operator `GET /ws/status` WebSocket broadcast | `TopologyMessage`: `activeSite`, `lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` (the flat record A-40 restructures), `updatePhase`, `alert`; per site `role`, `state`, `replicating` (the bool A-43 replaces), `recoveryState`, `divergentGtid` | `cmd/bloodraven/main.go:608`; `internal/controller/topology.go:2136-2140`; `internal/platform/websocket.go:19-44` | the playground dashboard, which compares `state === 'writable'` and `role === 'primary-candidate'` and uses the state as a CSS class (`playground/dashboard/index.html:17-18,131-150`); `2.monitoring.md:593-651` |

`role` on both operator endpoints carries the API spelling today only by accident. The reconciler casts the API value
straight into the internal enum (`state.SiteRole(s.EffectiveRole())`, `internal/controller/reconciler.go:2383`), and
the endpoints serialize that internal value. The internal constants are separate lowercase literals
(`internal/state/matrix.go:14-19`), and `EvalCrossSite` fences any writable site whose role is not the internal
primary-candidate constant (`matrix.go:122-124`). If A-27 renamed the API constants and kept the cast, the classifier
would receive `PrimaryCandidate`, compare it with `primary-candidate`, and fence the writable primary (GPT-R3-01).
A-28 does not change `state` either, because the endpoints serialize the internal string. Without an explicit
decision the two fields of one site entry would use different vocabularies (GPT-R2-04). A-114 and E7 settle both
fields: an exhaustive adapter maps each value between the API and the internal enum, in both directions.

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
  - **Widen the controller's type (revision 4, OPUS-R3-10).** Server ids pass through the controller as `int32`
    (`serverID := int32(101 + i)` at `reconciler.go:369,2564`; `configInitScript(fg, serverID int32)` at `:806`;
    `reconcileDeployment(…, serverID int32, …)` at `:1164`). A cast such as `int32(site.ServerID)` would wrap every
    value above 2147483647. E4 widens both signatures to `int64` and adds a unit test that renders
    `serverID: 4294967295` unchanged.
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
  - Document replacement as a new group restored from a backup (D3), in its own procedure (revision 5, F-19).
    Revision 4 pointed at the section 3 procedure, which is the v1.6.0 → v1.7.0 move into a new cluster: it reads
    the restore source from the MBK `status.location` that A-96 removes, and #243 scopes it to that one move. A
    v1.7.0 user who loses a site needs a procedure for the same cluster. E6 adds "Replacing a site" to
    `6.operations/2.operations.md`. It must cover:
    - the restore source from the backup's `status.artifactStorage` (A-96), not `status.location`;
    - a new group name, because the old MFG still exists in the namespace and U-8's rule against deleting an MFG
      to recreate it applies here too;
    - the endpoint cut-over: every derived name changes with the group name, including the
      `mysql-<group>-primary` Service (`reconciler.go:1796-1800`), so clients move to the new endpoints;
    - fresh backup and PITR prefixes and a new baseline, as U-3 requires, and the retirement of the old group only
      after cut-over is verified.
  - A future decommission feature can relax the rule without a break. While v1alpha1 is still served, the
    relaxation lands in every served version identically (D1, C-4; revision 5, F-27).

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
  - Immutable, per house rule T-1: `storageClassName` and `zone` (strings) by `self == oldSelf`, and `size` by
    value, `quantity(self.size).compareTo(quantity(oldSelf.size)) == 0`, so the operator's re-encoding of
    `1024Mi` as `1Gi` is not a change (revision 5, F-01). The per-item rules work once A-08 lands.
  - Size is immutable, not grow-only, because the operator does not implement PVC expansion. The CEL
    `quantity()` library that a grow-only rule needs is available at the A-115 floor. A later release that
    implements expansion relaxes the rule, which is not breaking, in every served version identically (A-59;
    revision 5, F-27).
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
    `has(self.provisioned) == has(oldSelf.provisioned) && (!has(self.provisioned) ||
    (has(self.provisioned.storageClassName) == has(oldSelf.provisioned.storageClassName) &&
    (!has(self.provisioned.storageClassName) || self.provisioned.storageClassName ==
    oldSelf.provisioned.storageClassName) && quantity(self.provisioned.size).compareTo(quantity(oldSelf.provisioned.size)) == 0))`.
    That also refuses switching between `claimName` and `provisioned`. Revision 4 compared `self.provisioned ==
    oldSelf.provisioned`, which rejects the operator's own re-encoded `size` (house rule T-1; revision 5, F-01).
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

**House rule T-1: transition rules compare values, not encodings (revision 5, F-01).** The operator writes MFGs
with typed `Update` calls: the finalizer add (`internal/controller/reconciler.go:279-283`) and every annotation
consumer (`reconciler.go:2223`, `planned_failover_reconciler.go:932`, `restore_reclones.go:618,990`,
`encryption_reconcile.go:1246`, `dragonfly_upgrade.go:388,402`). Go re-encodes what it decoded:
`resource.Quantity` re-renders a non-canonical value (`1024Mi` becomes `1Gi`, `1.5Gi` becomes `1536Mi`;
`apimachinery` `quantity.go:283,332-340,694-716`), `metav1.Duration` writes `60s` as `1m0s`, `metav1.Time` writes
UTC at second precision, and a non-pointer `omitempty` field written as an explicit zero (`allowPartial: false`,
`backup_types.go:225`) is dropped. The apiserver stores what the user wrote, so a whole-object `self == oldSelf`
over such a value rejects the operator's own write: the MFG never gets its finalizer, and deletion hangs. One rule
therefore applies to every transition rule in this audit (A-08, A-16, A-17, A-18, A-22, A-23, A-24) and to every
rule #235, #203 and #205 add:

- A rule never compares a struct with `self == oldSelf` when the struct holds a Quantity, a Duration, a
  `metav1.Time` or a non-pointer `omitempty` field. Those fields are compared one by one, by value:
  - Quantity: `quantity(...)` from the CEL quantity library, with `compareTo(...) == 0`.
  - Duration: `duration(self.x) == duration(oldSelf.x)`.
  - `metav1.Time`: as CEL timestamps (CEL types a `date-time` string as a timestamp, so equality is by instant),
    and A-39's pattern admits no fractional seconds, so the operator's second-precision encoding never changes
    the value.
  - A non-pointer `omitempty` bool or number: with its zero substituted, `(has(self.f) && self.f) ==
    (has(oldSelf.f) && oldSelf.f)`, unless A-37 already makes it a pointer.
- Strings, string lists and pointers to them round-trip byte for byte, so whole-object equality stays allowed on a
  sub-struct that holds only those (`source`, `decryption`, `loadOptions`).
- **Test, in the commit that adds the rules (E6):** an envtest creates each kind through the unstructured client
  with non-canonical values (`size: 1.5Gi` and `1024Mi`, a Duration `60s`, a `stopTime` with a `+05:00` offset,
  an explicit `allowPartial: false`), then performs the operator's own typed finalizer `Update` and an
  annotation consumer's `Update`, and asserts that both are accepted and that a real change to each field is
  still rejected.
- Rejected: canonical forms required at admission (Go's canonical Duration for an hour is `1h0m0s`, and a
  canonical-Quantity pattern would reject `1024Mi`, which users write); and moving every operator write to a
  metadata-only patch, which removes the operator's own trigger but leaves every other typed-client writer, and
  every future operator write, exposed.

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
  - Optional fields use `has(self.x) == has(oldSelf.x) && (!has(self.x) || <x unchanged>)` on the spec
    struct, where "unchanged" follows house rule T-1: `pointInTime` compares `mode` as a string and `stopTime` as a
    timestamp, and `sanityCheck` compares `query` as a string, `timeout` with `duration()` and the pointer
    `minResult` by presence and value (revision 5, F-01). The references and `sourceSite` are strings.
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
    (!has(self.spec.initFromBackup) || <initFromBackup unchanged>)`. Under house rule T-1 (revision 5, F-01),
    "unchanged" is whole-object equality on `source`, `decryption` and `loadOptions`, which hold only strings,
    string lists and pointers, and a field-by-field comparison inside `pointInTime`: `stopTime` as a timestamp,
    `allowPartial` and `allowUnverifiedCrossSite` with their zero substituted, and the strings and `sites` list by
    equality. Revision 4 compared the whole block, which rejects the operator's finalizer `Update` whenever the
    user wrote `allowPartial: false` or a `stopTime` with an offset.
  - Restoring into a running group is `restoreInPlace` only.
  - #206 can reshape `pointInTime` freely. The `pointInTime` part of the rule is written for the shape #206
    leaves, by house rule T-1, in E6.
  - Whole-block equality is costed over every string in the block, so each one gets a `MaxLength` (A-92). E6's
    envtest installs the CRD and fails on a cost-budget error (OPUS-11).
  - **Those bounds land in E6, with the rule (revision 4, OPUS-R3-05).** At HEAD, `.spec.initFromBackup` holds 17
    strings and lists with no `maxLength` or `maxItems` (revision 4 said 16; F-53): `source.s3.{bucket, prefix, endpointURL, region,
    credentialsSecret, storageClass}`, `source.pvc.{claimName, subPath}`, `source.mysqlBackupRef.name`,
    `decryption.passphraseSecret.{name, key}`, `loadOptions.{include,exclude}Schemas`, and
    `pointInTime.{stopDatetime, excludeGtids, archiveProfileName, sites}` (the `sites` items already carry a `maxLength`;
    the list has no `maxItems`). Revision 3 left most of them to E16 (A-92)
    and E8 (A-34, A-39), so E6's rule would fail E6's own cost test. E6 therefore bounds every string and list
    reachable from `initFromBackup`, MBV `spec.pointInTime` and MBV `spec.sanityCheck` that E4 and E5 have not
    already bounded (E3 removes `storageClass`, A-56; E5 renames and bounds the Secret and backup references, A-02
    and A-04), before it adds A-22's and A-23's whole-block rules:
    - names and Secret keys `MaxLength=253`; `bucket` 63; `region` 64; `prefix`, `subPath` and `endpointURL` 1024;
    - both `stopTime` fields 64. E6 now runs after E8 (section 6, revision 5), so by then E8 has replaced
      `stopDatetime` and MBV `pointInTime.timestamp` with them;
    - `sanityCheck.query` 4096 (A-34's bound, moved from E8);
    - `excludeGtids` `MaxLength=131072`. A GTID set can be long. 128 KiB is the Linux limit on one argument, and
      the replay passes the set as one `mysqlbinlog` argument, so no longer value could run. Proposal 21's
      reconcile-time 96 KiB canonical-form refusal stays the semantic limit;
    - `pointInTime.sites` `MaxItems=16`, items with the A-15 site-name pattern and bound; `archiveProfileName` 63.
      These two are #206-owned. WS-E adds bounds only and does not change their meaning (section 6).

    E16 keeps A-92 for every other field. If #206 reshapes `pointInTime` before WS-E branches, E6 bounds the shape
    #206 leaves, by the same rule.
- **A failed bootstrap needs a remedy that the rule allows (revision 5, F-20).** Today a bootstrap that fails on
  a recoverable input error is fixed by editing the block. The operator's own `RestoreBuildFailed` messages say so
  ("set `…decryption.passphraseSecret`", "set `…source.<backend>` explicitly"; `internal/controller/restore.go:904-917,
  1061-1064`), so do the doctor matrix (`alert_runbook_matrix.md:57`, which also says to remove `skipBinlog`) and
  `7.backup-and-restore/5.backup-encryption.md:250-276`. Removing and re-adding the block is today's retry,
  because removal clears `status.restore` (`restore.go:516-522`). Under this rule all of those are rejected.
  - **Remedy: delete and recreate a group whose bootstrap never succeeded.** Such a group holds only a partial
    load of a backup that still exists, and serves no application. The procedure checks that `status.restore`
    has never reached `Succeeded`, that no application is connected, and that the backup is intact, then deletes
    the MFG and re-applies it with the corrected `initFromBackup`. U-8's "never delete a MysqlFailoverGroup in
    order to recreate it" gains this one exception. The cost: the partial load and the group's PVCs are deleted
    and provisioned again.
  - Rejected: allowing edits while `status.restore` reports a failure. Admission would then trust a status
    field that can be lost or written by hand (A-89), and a forged `Failed` would reopen the destructive load
    into a serving primary that this finding closes.
  - E6 rewrites the `initFromBackup` branch of the shared messages to point at this procedure (the
    `restoreInPlace` branch keeps its edit advice, which is still valid there), and updates the doctor row and
    `5.backup-encryption.md` in the same commit.
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
  - Add A-39's RFC 3339 `Pattern` as well (revision 4, OPUS-R3-03). `Format=date-time` alone admits a lowercase
    `t` or `z`, a non-digit fraction and an out-of-range offset such as `+25:00` (revision 5, F-02), which the
    reconcile-time parser then rejects. E6's envtest rejects all of them for `confirm`.
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
  - **The internal role boundary (revision 4, GPT-R3-01).** The internal `state.SiteRole` constants keep their
    values (`internal/state/matrix.go:14-19`). E7 replaces the cast at `reconciler.go:2383` with an exhaustive
    API-to-internal adapter that fails closed on an unknown value. The JSON endpoints and the metric label use the
    reverse adapter (A-114). A test in E7 takes each API role through the topology configuration into
    `EvalCrossSite`, including a writable `PrimaryCandidate` site, which must not be fenced.
  - `PointInTimeVerificationMode`: `Latest | Timestamp`, required inside the block. `none` is dropped: an
    absent `pointInTime` block means no replay.
  - **The PITR mode wire keeps its lowercase values (revision 5, F-10).** The verification Job copies the API
    value verbatim into `BLOODRAVEN_PITR_MODE` and `BLOODRAVEN_VERIFY_PITR_MODE`
    (`internal/controller/backup_verification_job.go:158-161,181-182`), and the consumers accept only `latest` and
    `timestamp` (`cmd/bloodraven/pitr_download.go:294`; `internal/controller/restore_script.py:450-464`). E7 adds an
    exhaustive adapter from `PointInTimeVerificationMode` to those wire values in `backup_verification_job.go`, so
    `pitr_download.go` and `restore_script.py` stay unchanged. A unit test renders a verification Job for each CR
    mode and feeds its env to the download planner.
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
  - **Every producer that casts an internal value into these types gets a mapping (revision 5, F-21).** A Go
    string conversion compiles and silently emits the old spelling, which the new enum then rejects for the whole
    status write. The Dragonfly writer casts the classifier result, `v1alpha1.DragonflyRole(role)`
    (`dragonfly_topology.go:550,567`), and `internal/dragonfly.SiteRole` keeps its lowercase values because they
    also drive the `dragonfly-role` pod label (`internal/dragonfly/siterole.go:9-14`; `dragonfly_labels.go:70-72`).
    `siteStatusFromSnapshot` writes `State: s.State.String()` (`runner.go:2448`). E7 adds an exhaustive
    internal-to-API mapping for each and routes both CR status writers through them. A unit test takes every
    classifier and state value through its mapping and validates the result against the API enum.
  - `type SiteRecoveryState` and `type DivergenceDetector`, keeping their current values.
  - **No `""` enum value (revision 4, OPUS-R3-01).** Nine status enums admit `""` today. A-61 deletes one of them,
    `StandbyActivationPhase` (`mysqlstandbycluster_types.go:387`). The other eight are `RestoreInPlacePhase`
    (`backup_types.go:837`), `DragonflyPhase` (`dragonfly_types.go:184`), `SiteKeyringPhase` and `KeyringUnsealReason`
    (`encryption_types.go:172,205`), `recoveryState`, `divergenceDetectedBy` and `SourceConvergenceState`
    (`types.go:684,703,728`), and `PlannedFailoverPhase` (`planned_failover_types.go:47`, owned by #235, A-50).
    Every field of these types is `omitempty`, so a typed write never sends `""`. Absence already has one spelling
    (A-27), so E7 removes `""` from each marker. The A-50 checklist requires the same of #235, and the #205
    checklist (A-59) of proposal 21's new phases.
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
  - **That last sentence is code E7 writes; it is not today's behaviour (revision 4, OPUS-R3-07).** The updater sets
    `UpdatePhaseComplete` (`updater.go:287,346,455`), and only a deferred reset clears it (`:245-250`). The
    snapshot copies `tm.updater.Phase()` verbatim (`topology.go:2054-2057`), so a poll between the two writes
    `Complete`. Under the new enum the apiserver would reject that status write, together with every condition and
    `activeSite` change in it. E7 deletes `UpdatePhaseComplete`, or maps it to `""` in the snapshot, and adds a
    unit test that a snapshot taken in that window has no `updatePhase`.
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
- **Proposal 21's `kind` (revision 4, OPUS-R3-02).** Proposal 21 adds `status.restoreInPlace.kind`
  (`RestoreInPlaceKind`, `Enum=Full;Schema;PITR`, "derived at acceptance from scope and pointInTime and pinned",
  `21-in-place-pitr-staging.md:183-196`), and never mentions `scope.type`. In-place PITR is full-instance only
  (its Q5), so `kind` repeats `scope.type` plus one bit: "full, with a point in time". Both frozen in v1beta1 would
  be two status fields for one fact, the duplication A-49 and A-96 remove elsewhere. Decided now, because the
  field is cheap to change only before the freeze:
  - **Chosen:** keep `scope{type: Full | Schema, schema}`, and #205 does not add `kind`. A PITR run is one whose
    accepted snapshot has `pointInTime`. Status records it as `provenance.pointInTime` (A-97), whose requested
    `stopTime` is written in the acceptance write, so it is pinned as `kind` was. Code tests
    `provenance.pointInTime != nil`, not a kind value.
  - **Rejected:** A-30 adopts `kind` with `Full | Schema` and #205 adds `PITR`. That is additive and also gives one
    field, but it mixes two independent facts (what is restored, and to which instant) in one enum. A future
    per-schema PITR would then need a fourth value.
  - Owned by #205: this states the shape its implementer uses. It does not change proposal 21's semantics.

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
- **Decision:**
  - Rule: a defaulted scalar whose zero is valid is a pointer, the CRD default applies only to nil, and the
    controller never maps zero to a default.
  - Pointers: `replication.maxLagSeconds *int64` (`Minimum=0`; 0 means caught up), `backup.backoffLimit
    *int32`, `backup.maxLagSecondsForSource *int64`.
  - **MBV `ttlSecondsAfterFinished` is a separate lifecycle decision, not an instance of the rule (revision 5,
    F-54).** It is `int32` with `default=0` (`mysqlbackupverification_types.go:84-90`), and the controller treats 0
    as "clean up now" (`backup_verification_reconciler.go:716-723`), so no valid zero is rewritten today. The
    decision is still to make it `*int32` with no default: nil keeps a successful verification's Pod, Service and
    PVC until retention removes the record, and 0 deletes them at once (Job semantics). That changes the default
    lifecycle of every verification whose manifest omits the field, and 3.4 says so.
  - **The profile mirror changes with it (revision 5, F-11).** `VerificationSpec.ttlSecondsAfterFinished`, the
    copy a profile carries for scheduled verifications, is also `int32` with `default=0`
    (`mysqlbackupverification_types.go:417-421`), and both creators copy it unconditionally
    (`cmd/bloodraven/trigger_verification.go:102`, `cmd/kubectl-bloodraven/verify.go:157`). Left as it is, every
    scheduled or plugin-created MBV would get an explicit 0 and never see the nil meaning. It becomes `*int32` with
    no default, and both creators copy the pointer so nil stays nil. E8 tests an omitted TTL and an explicit 0
    through the scheduled trigger and through the plugin.
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
  - **`format: date-time` is not enough (revision 4, OPUS-R3-03).** These are the first user-written `metav1.Time`
    fields; every existing one is a status field the operator writes. The apiserver's date-time check is looser
    than Go's RFC 3339 parser. `IsDateTime` lowercases the value before splitting on `t`, and its time pattern
    leaves the fraction's `.` unescaped (`k8s.io/kube-openapi` `pkg/validation/strfmt/time.go:30-45,56`). So
    `2026-11-04t10:00:00z` and `2026-11-04T10:00:00x5Z` pass admission, and `metav1.Time.UnmarshalJSON` rejects
    both. The apiserver stores the value as written. The typed client then cannot decode that object, and one
    undecodable item fails the whole List/Watch (3.1 item 1), so one typo would stop the operator's MFG or MBV
    informer for every object of the kind.
    - Both `stopTime` fields carry `+kubebuilder:validation:Pattern=` with
      `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$` (revision 5).
      Revision 4's pattern ended `(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$`, and the review measured two holes in
      it (F-02, F-01). `IsDateTime` checks the date and range-checks the hour, minute and second, but never the
      offset, so `+25:00`, `+99:00` and `+05:99` pass both checks on 1.31 and 1.37 envtest, are stored as written,
      and fail `metav1.Time.UnmarshalJSON` ("time zone offset hour out of range"). The bounded offset is slightly
      stricter than Go, which also takes `24` and `60`; stricter is safe. And a fractional second is admitted but
      lost: `metav1.Time` encodes at second precision, so the operator's first typed write would silently move the
      user's stop time and fail A-23's transition rule. The pattern therefore admits no fraction. This must
      be an OpenAPI pattern: CEL types a `date-time` string as a timestamp, so `matches()` is not available on it
      (the same reason A-38 avoids `Format=duration`). E8's CRD test asserts that the generated property carries
      the pattern.
    - E8's envtest adds a lowercase `t`, a lowercase `z`, a fraction, and the offsets `+25:00`, `+99:00` and
      `+05:99`, all rejected. It also decodes every value the envtest admitted with `metav1.Time.UnmarshalJSON`,
      so any other gap between the two checks fails CI rather than an informer.
    - **House rule:** every user-written time is a pattern-checked `metav1.Time`, and the pattern rejects every
      string that `time.Parse(time.RFC3339, …)` rejects. A-26's `confirm` string gets the
      same pattern, and #235's and proposal 21's request fields inherit the rule (A-50, A-59).

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
  - **One convention for every status timestamp (revision 4, OPUS-R3-04).** Three more existing `*metav1.Time`
    fields break the same rule, and revision 3 froze them as they are:
    - `status.restoreReclones.requestedAt` (`backup_types.go:959`) → `requestTime`
    - `status.encryptionAtRest.sites[].unsealedSince` (`encryption_types.go:281`) → `unsealTime`
    - `status.sites[].lastSeen` (`types.go:666`) → `lastSeenTime`

    These are renames only, with no type change, in E10, with a status-only 3.4 row. The rule is: every status
    timestamp ends in `Time`, in existing fields and in new ones (#235 through A-50, proposal 21 through A-59).
  - `status.lastFailover: {time *metav1.Time, target string, promotionGtidExecuted string}` (Go
    `FailoverRecord`). The annotation keys stay (A-104).
  - #235's planned-run watermark stays in its own run record.

#### A-41 Dragonfly memory as a Quantity

- **Sev** should · **Brk** yes · **Merges** MFGSPEC-26 (type part)
- **Evidence:** `maxMemoryMb int32` and `proactorThreads int32`, where 0 means default
  (`dragonfly_types.go:48,54`).
- **Decision:**
  - `maxMemory *resource.Quantity`, rendered as bytes. Nil means Dragonfly's default. A Quantity admits signed
    values and its int-or-string schema has no sign check, while today's field has `Minimum=0`
    (`dragonfly_types.go:44-48`) and the renderer emits the flag only when it is positive
    (`dragonfly_resources.go:279-280`). So `maxMemory` gets the CEL rule
    `!has(self.maxMemory) || quantity(self.maxMemory).isGreaterThan(quantity('0'))`, and `-1Gi` and `0` are rejected
    at admission instead of rendering `--maxmemory=-1073741824` or silently dropping the cap (revision 5, F-12).
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
- **Problem:** `false` means the primary, not yet probed, threads stopped or no source host, or a healthy streak
  still building. It is not set from lag: `replicating` is thread health with a source host, debounced
  (`topology.go:1565-1578`). Revision 4 listed "lagging" here, which was wrong (revision 5, F-23).
- **Decision:**
  - `replication SiteReplicationState`, `Enum=Healthy;Lagging;Broken;Unknown;NotApplicable`.
  - `NotApplicable` for the writable primary, `Unknown` before the first probe.
  - **Producer mapping (revision 4, OPUS-R3-07).** Today `replicating` turns true only after
    `replicatingStreakThreshold` consecutive healthy polls (`topology.go:1565-1578`). Lag is a separate overlay
    against `maxLagSeconds` (`runner.go:1562-1572`). The enum keeps the debounce:
    - `Healthy` and `Lagging` are reported only once the streak has reached the threshold. `Lagging` means
      `secondsBehindSource > replication.maxLagSeconds`, the same threshold as `Degraded=True/ReplicationLagging`;
      otherwise `Healthy`.
    - `Broken`: replica status was read, and the IO or SQL thread is stopped or no source is set. This resets the
      streak, as today.
    - `Unknown`: replica status could not be read, or the site has not been probed yet, or the healthy streak is
      still building.
    - The lease gate refuses unless the site is `Healthy` or `Lagging`, as it refuses on `!replicating` today.
  - **Every reader keeps its meaning (revision 5, F-24).** Each `Replicating` predicate maps to `replication` in
    `Healthy` or `Lagging`, and keeps whatever separate lag or source check it has. Mapping any of them to
    `Healthy` alone would silently stop accepting a lagging replica. The production readers are:
    - the deployment-lease gate (`deployment_leases.go:360`);
    - the planned-failover target check (`planned_failover.go:245`), owned by #235, which accepts a lagging but
      replicating target today;
    - backup source selection (`backup_reconciler.go:1506`), which prefers a `read-only` replica that replicates;
    - the reader pod label `healthy=yes` (`reconciler.go:2165-2169`), which routes reader traffic and applies its
      own lag threshold;
    - `/deploy/v1` `replicationRunning` (`cmd/bloodraven/deploy_api.go:341`).
    E10 tests each one with a `Lagging` site.
  - Update the lease gate, the three controller readers above, the kubectl plugin, `triage.sh` and the playground
    readers.
  - **Served copies (OPUS-R2-10, A-114).** In the CR, `replicating` means both replication threads run with a
    known source, debounced over a streak of polls, whatever the lag (`topology.go:1565-1578`). The two served
    fields:
    - `/deploy/v1` `health.replicationRunning` keeps its name. It is `true` when every non-active spec site has
      a status entry with `replication` in `Healthy` or `Lagging` (`deploy_api.go:337-349`). Lag stays in
      `health.replicationLagSeconds`. `11.deployment-api.md` documents this definition.
    - `/ws/status` `sites[].replicating` becomes `sites[].replication`, carrying the same enum value as the CR.
      **That is a change of meaning, not only of shape (revision 5, F-23).** Today's WebSocket field is the raw
      per-poll `IORunning && SQLRunning`, with no streak and no source-host check (`topology.go:2145-2147`), so
      the dashboard (`playground/dashboard/index.html:159,182`) moves from a per-poll flag to the CR's debounced
      enum. E10 updates those readers and says so in the release notes.
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
  - **The same for `lastIOSecondsAgo` (revision 5, F-22).** A-36 makes it `*int64`, "nil when Dragonfly did
    not report it". The parser initialises it to -1 and parses a reported value into the same `int`
    (`internal/dragonfly/info.go:70-73,95-96`), and a reported -1 already means "never received data"
    (`dragonfly_types.go:387-391`). So absence and a reported -1 cannot be told apart. The parser's presence
    tracking covers `master_last_io_seconds_ago` too. E10 tests that an omitted key gives nil, and that a
    reported -1 stays a non-nil -1.
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
- **The sidecar too (revision 5, F-13).** The operator receives flags whose presence is already lost. The
  sidecar collapses unknowns to false before they leave the pod: no row for the `mysql` tablespace leaves
  `SystemTablespaceEncrypted` false (`internal/sidecar/mysql.go:443-446`), and SQL NULL becomes false
  (`:467-469`). The wire types are plain `bool` on both sides (`internal/sidecar/keyring.go:124-131`,
  `internal/mysql/sidecar_client.go:51-57`). So E10 changes the sidecar as well: a missing row or NULL stays
  unknown, `KeyringCoverage` uses `*bool` with `omitempty` on both sides of `/keyring/status`, and the operator
  copies nil through. That payload is a doctor-skill contract, so `sidecar-probe.sh` and `SKILL.md` change in
  the same commit. Unit tests cover missing, false and true for each flag.

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
  - Update s48, s50 and `8.encryption-at-rest.md`, and `playground/enable-encryption.sh`, which polls
    `{.status.encryptionAtRest.sealed}` for `true` for up to 15 minutes (`:341-357`) and runs in CI's encryption
    E2E (`.github/workflows/_e2e.yml:134-139`). Its wait becomes `EncryptionAtRestReady=True` with reason
    `AllSitesSealed` (revision 5, F-25).

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
    `self.upgradeStrategy != 'SnapshotRestore' || (has(self.snapshot) && has(self.snapshot.dir) &&
    self.snapshot.dir != '')`. `snapshot.dir` is `+optional`/`omitempty` (`dragonfly_types.go:107-110`), so
    without the `has()` guard `snapshot: {}` raises "no such key" instead of the rule's message (the R05 pattern
    that A-91 guards against; revision 4, OPUS-R3-10). E13's envtest includes `snapshot: {}`.
  - Drift is detected against the live StatefulSet, not against status (OPUS-18). When the strategy is
    `SnapshotRestore` and `spec.dragonfly.image` differs from the Dragonfly container image of the running
    StatefulSet, the operator runs the existing phases. If that image is unknown (no StatefulSet yet), it does
    nothing, so a new group or a group whose status was lost never starts an unrequested upgrade.
  - **Level-trigger semantics (revision 5, F-03).** Today's annotation is one-shot: the operator removes it before
    the run starts (`dragonfly_upgrade.go:125-156`), so a failure or a rejection is final. A spec drift is still
    there after a failure, so the same phases would restart forever, and each start sheds the active site's cache
    traffic before SAVE (`:170-173`). When a run is rejected, the reconcile returns no requeue and falls through
    to the ordinary render, which rolls the StatefulSets onto `spec.dragonfly.image` the Rolling way
    (`reconciler.go:419-430`; `dragonfly_resources.go:160-162`). So:
    - **A terminal run is not restarted for the same target.** `upgrade` keeps `targetImage`, the outcome, and a
      digest of `spec.dragonfly` as the run saw it. While `spec.dragonfly.image` equals the target of a `Failed`
      or abandoned record and `spec.dragonfly` still matches its digest, the operator starts no new run. The
      record (`phase: Failed`) stays until a re-arm, and the run's existing Warning Event is emitted once per
      terminal run, not once per reconcile; no new condition reason is added. **Re-arm:** any change to `spec.dragonfly` (a new image, a
      fixed `snapshot` block or a strategy change) starts a new run. If the fix was outside spec (a Secret's
      contents), the user sets the image back to the running one, which clears the drift and the record, and
      then forward again. That two-step retry is the stated cost. Rejected: a retry annotation (the GitOps-unsafe
      trigger this finding removes), and re-arming on any `metadata.generation` change (an unrelated MFG edit
      would shed cache traffic again).
    - **Under `SnapshotRestore`, the ordinary render never moves the image.** The Dragonfly StatefulSet renderer
      keeps each site's live image, and only the upgrade's own image-changing phases move a site to
      `spec.dragonfly.image`. That also holds while admission is blocked (no `status.activeSite`, or a planned
      failover in flight) and after a terminal failure. A blocked run waits and starts once admission clears; it
      is not rejected and dropped.
    - **Drift is per site, and the source is live.** `upgrade.sourceImage` is the live image of the active site's
      StatefulSet, not `spec` (today it is copied from spec, `dragonfly_upgrade.go:153`, which is the target
      once spec carries the request). A run interrupted after one site moved resumes from the per-site drift
      after an operator restart; it does not start over.
    - Switching `upgradeStrategy` to `Rolling` while drift exists rolls the ordinary way. That is the user's
      choice, and the docs say so.
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
  - **`status.pitr.profileName` goes too (revision 5, F-39).** Revision 4 kept it as an observation that
    attributes the archive counters (A-111). It is an echo: the API says it "mirrors
    spec.backup.pitr.profileName" (`backup_types.go:162-164`), the writer copies it from spec
    (`runner.go:1910-1913`), and on the path that keeps the old counters while a site has not answered, it
    relabels them with the new spec profile (`:1976-1978`). So during the profile-switch lag A-111 cited, it
    names the wrong profile. Its readers move to spec: the status-change check
    (`pitr_archive_condition.go:615`), s31, s37 and `internal/playground/scenarios/backup_helpers.go:723`.
    A-111's leave decision is withdrawn.
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
  - **WS-C lands the shape** and is asked to meet the checklist below. Revision 3 said "while its branch is still at
    WP0"; the branch has since merged WP1 and WP3 (`2527bbe`), so WS-E's step 0 is the backstop that matters
    (revision 5, F-55).
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
  3. **Names.** `runID` (the house form, as in the served `runID` and `jobUID`, `backup_types.go:1023,1087,1134`, and
     proposal 21's `runID`), not `runId`. Revision 4 cited `requestID` and `sourceServerUUID`, which exist nowhere
     (revision 5, F-56). The `/deploy/v1` lease payload's `operationId` is a separate log-contract field and keeps
     its spelling (AGENTS.md). `sourceSite` and `targetSite` (A-20), not `source` and `target`. Timestamps use the `…Time` suffix
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
  - The `AdoptionRefused` reason and its Event, `EncryptionAdoptionRefused`, stay: the policy blocks an automatic
    action, which is A-74's `Refused` case (revision 5, F-57; revision 4's A-74 also renamed the Event). Remove the
    annotation.

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
    pinning every group, which would bring back the "never moves" behaviour this finding removes. Once the
    rollout-pacing PR lands, the C-8 E2E sets both to the branch build (`bloodraven-sidecar:playground`,
    `.github/workflows/_e2e.yml:120`), so its rollout assertion sees a real image change. The critique suggested the key `sidecarImage.{repository, tag}`;
    `sidecar.image` is used instead so that no values file carries the old spec field name that E19's gate
    looks for.
  - `spec.sidecarImage` → `spec.sidecar.image`, and `spec.sidecarResources` → `spec.sidecar.resources`.
  - `spec.sidecar` stays a value type (`types.go:117`) and gains `+kubebuilder:default={}` (OPUS-17). A manifest
    without the block then still gets the `leaseTimeout` and `peerCheckInterval` defaults (`types.go:463,468`),
    and the struct-level rules, including the 3× rule (`:459`), still run. A pointer would skip both.
- **What the freeze needs, and what it does not (revision 5, scope rule).** The frozen API part of A-52 is only
  the optional `spec.sidecar.image` with no persisted default, the `spec.sidecar.*` renames and `default={}`.
  That lands in WS-E PR1 (E13, section 6). How the operator paces the rollouts that an unpinned default causes
  is runtime behaviour, not API shape. Revisions 2 to 4 designed a cross-group queue here, and every round found
  a Major problem in it (OPUS-07, OPUS-R2-09, then F-04, F-05 and F-06 in the ultra review). Under the program's
  scope rule the queue design is removed from this audit. The rollout-pacing PR (WS-E PR3, its own `e2e` PR) and
  the follow-up issue #244 own it, and must meet the requirements below.
- **Cost (OPUS-07, corrected in revision 5, F-04):** with no pin, an operator upgrade that ships a new sidecar
  default changes the effective pod spec of every unpinned group. A group on `updateStrategy: OrderedUpdate` then
  runs an ordered update: replica restart, one planned failover, old-primary restart
  (`6.operations/10.upgrade-policy.md:149-152,227-230`). A group on `Recreate` restarts both sites at once
  (`internal/controller/reconciler.go:345-392`; `runner.go:686-691` keeps it out of the ordered updater).
  Revision 4 said that today an operator upgrade never touches the data plane. That is false: a release that bumps
  `deploymentPodRenderVersion`, `configMapRenderVersion` or `pitrPodRenderVersion` (`reconciler.go:62-68,
  2238-2275`; bumped at least six times in `git log`) changes `ComputeSpecHash` for every group, pinned or not,
  and `checkSpecDrift` starts an ordered update on each group independently (`runner.go:700-730`).
- **Requirements for the rollout-pacing PR (#244).** They are the invariants F-04, F-05 and F-06
  state. The design that meets them belongs to that PR and its issue, not to this audit.
  - **R-1. Every release-driven pod-spec change is paced.** Pacing governs every change to a group's rendered pod
    spec that an operator release causes: a new sidecar default, a render-version bump, or both at once. It is
    not limited to the sidecar default. A pinned `spec.sidecar.image` does not exempt a group from a
    render-version roll, and no doc or test may say that a pinned group "never rolls on an operator upgrade".
  - **R-2. A group outside its slot does not roll.** A queued, deferred or not-yet-admitted group keeps its
    current rendered pod spec, including through unrelated spec edits, until it is admitted. Both the renderer and
    the drift hash honour the same admission decision, so the drift check cannot start a roll that the pacing has
    not admitted.
  - **R-3. Every `updateStrategy` has an admission and completion signal.** `Recreate` groups either are excluded
    from automatic release-driven rolls (and the docs say how they move), or have a defined admission and
    completion signal and behaviour. A signal that only `OrderedUpdate` writes, such as `status.updatePhase`,
    cannot decide admission for `Recreate`.
  - **R-4. "Never started" and "started and stalled" are told apart by a latched fact.** Whether a group's roll
    has started is latched for the admitted target, and stays true for the whole of a stall. Today's
    `status.updatePhase` does not: the updater resets it on return (`updater.go:313-317`), and a pod-not-ready
    stall is reported only through `Updating=True/OrderedUpdateStalled` with an empty phase (`topology.go:3557-
    3566,3612-3622`; `runner.go:1649-1668`). A stalled roll, or a group whose workload is already on the new
    template, is never skipped as "not admitted". A bad build stops at the first group.
  - **R-5. Pacing state survives an operator restart.** The operator upgrade that triggers the rolls restarts the
    operator, and so can a second `helm upgrade` mid-rollout. In-flight admission and a halt are reconstructed
    from durable evidence after a restart; a halt is not forgotten.
  - **R-6. Liveness.** A group that is held or refused by an unrelated actor (a restore, a group that is not
    `Ready`, the #235 `AutomaticFailoverHeld` hold) does not block every other tenant forever, and is not then
    rolled outside its slot when the hold lifts (R-2).
  - **R-7. Tests use the real updater.** The halt test runs the real updater with a replica that stays unready,
    not a fake that keeps a phase set, and covers a combined render-version and sidecar-default change and a
    `Recreate` group.
- **When pacing must exist.** The rollout-pacing PR does not gate WS-F or `v1.7.0-rc.0`, because no release before
  rc.0 runs groups with the new schema. It gates the first release that upgrades such groups to a new default,
  which in practice is v1.7.0 itself (rc.0 groups upgrading to it). Until it lands, the C-8 E2E does not assert
  sidecar-rollout behaviour.
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
  - **Gate (timeboxed, in WS-E's PR4, which does not gate WS-F or rc.0; revision 5):**
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
  - **If the gate fails or cannot be completed by the v1.7.0 cut:** ship C, which is the same merge helper with empty MySQL and
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
  - **Explicit zero (revision 5, F-26).** In the new shape an explicit `keepFailed: 0`, or a verification
    `keepLast: 0`, means keep none. Today an explicit 0 in `maxFailedKeep`, `keepSuccessful` or `keepFailures`
    means "use the default", 10, 30 and 10 (`backup_reconciler.go:1357-1361`;
    `backup_verification_reconciler.go:1021-1034`). Revision 4 described those as "0 disables" meanings and mapped
    the fields by plain rename, so a user who followed the table would have deleted every failed backup or
    successful verification record that is kept today. The migration therefore maps a legacy explicit 0 in those
    three fields to an omitted field, which gets the same default, and 3.4 says so.
  - Remove `minKeep`, the zero-means-default meanings and the legacy `retention` integer. Under keep-if-any, `count` and
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
  - **"Targets v1beta1" means every served version (revision 5, F-27).** Proposal 21 plans "PR4 onward targets
    v1beta1 only" (`21-in-place-pitr-staging.md:878,885`). Under D1, v1alpha1 stays served with an identical schema
    until the next minor release (C-1), and C-4 fails on the first difference, which C-1 makes a webhook release
    blocker. So any field added after the freeze, by #205's PR4 to PR6 or by anyone else, lands in every served
    version identically until v1alpha1 is removed. The alternative is to hold PR4 until that removal. Adding it to
    `api/v1beta1` only is not allowed.
  - Proposal 21 (#205, WS-B) is written against today's names (`mysqlBackupRef`, `stopDatetime`, a string
    `scope`, `status.location`). Where it lands after WS-E, it uses the v1.7.0 names: `backupRef` (A-04),
    `stopTime` (A-39), the scope as A-30 decides it, and `artifactStorage.location` (A-96). Its hold key follows the
    A-77 hold family (OPUS-16).
- **Proposal 21 does not all land after WS-E (revision 4, OPUS-R3-01, OPUS-R3-02).** Revision 3 treated #205 as
  one additive change after the freeze that needed four renames. Proposal 21 itself says otherwise: "PR1-PR3 change
  whichever API version `main` serves when they merge (`api/v1alpha1` today) … the v1beta1 migration carries them
  unchanged whichever lands first". Only PR4 onward targets v1beta1 (`21-in-place-pitr-staging.md:878`). PR1 is
  #241, which changes today's non-PITR full-instance restores too. PR1-PR3 and PR4 add served surfaces that break
  the rules this audit freezes. If they merge before WS-E's base, nothing in revision 3 told WS-E to rename them. If
  they merge after, they bring old-prefix keys into v1.7.0, which A-77's "no dual-key code" forbids.

  **Conflicts in proposal 21's planned API, with the shape its implementer uses instead.** WS-B and the #205
  implementer meet these when the field lands. WS-E fixes any gap in a field already on its base (section 6). The
  semantics stay #205's; this list fixes only names, types and grammar.

  | Proposal 21 (line) | Conflicts with | Shape to use |
  |---|---|---|
  | `status.restoreInPlace.kind` `Full;Schema;PITR` (`:183-196`) | A-30 `scope.type`: two fields for one fact | Not added. `scope{type, schema}` stays, and a PITR run has `provenance.pointInTime` (A-30, A-97) |
  | Labels `shipstream.io/restore-loader`, `restore-run`, `restore-attempt` (`:535`, `:541`, `:601`) | A-77 prefix | `bloodraven.shipstream.io/restore-{loader,run,attempt}`, classified Internal in the A-79 page |
  | Hold `hold.restore.shipstream.io/<runID>` (`:442`, `:597`) | A-77 hold family | `restore-hold.bloodraven.shipstream.io/<runID>`, with the value unchanged: `<group name>/<group UID>` (A-77, GPT-R3-03) |
  | `restoreDamage.since`, `acceptPeersHeldAt`, `RestoreMarker.at`, artifact `verifiedAt`, `preflightSince`, `provisioningSince` (`:367`, `:371`, `:338`, `:327`, `:200`, `:222`) | A-40 `…Time` rule | `damageTime`, `acceptPeersHoldTime`, `time`, `verificationTime`, `preflightStartTime`, `provisioningStartTime` |
  | Free-string `reason` on `restoreDamage`, `RestoreStagingRunStatus` and the staging artifact (`:376`, `:229`, `:328`) | A-33 | Named Go types with A-69 constants, the reason pattern and `MaxLength=1024` |
  | `RestoreInPlacePhase` keeps `""` (`:173`) | A-28 (no `""` value) | `Staging;Staged;Preflight;Fencing;Restoring;Resuming;Succeeded;Failed` |
  | `restore-accept-damaged=<originRunID>`, `restore-staging-cleanup=<originRunID>`, both destructive, with no `confirm=` (`:437-438`) | A-78 confirmation rule | Kept as written: an operator-minted run-id subject satisfies `confirm=` and `uid=` (A-78, revision 4) |
  | Condition `RestoreInPlaceDamagedPrimaryFenced`, MFG `Ready=False/RestoreDamagedPrimary`, and the same reason on lease refusals (`:396-399`, `:429`) | A-71 closed `Ready` set; A-73 closed `unstableReason` set; A-114 generated `/deploy/v1` set | A-69 constants. `RestoreDamagedPrimary` joins A-71's set and A-73's set; the new condition is True only while fenced (A-72) |
  | Request instants (`pointInTime` in the accepted snapshot) | A-39 house rule | A-39's pattern-checked `metav1.Time` `stopTime` |
  | Prose that relies on `resetProgress` (`:162`), `sourceSiteOverride` (`:756-757`) and Jobs running as uid and gid 27 (`:601`, `:928`, `:946`) | A-56 (removed), A-20 (`sourceSite`), A-54 | The v1.7.0 names, and the uid A-54 settles (999 if its gate passes) |

  A-59 checks every row of this table, not only the four renames above. WS-B is asked to change
  `21-in-place-pitr-staging.md:878` from "carries them unchanged" to "carries them as WS-E leaves them". Two
  related issues add nothing to this list. #240 (release the damage fence after a proven pre-destruction refusal)
  changes only when the operator clears `restoreDamage`. Any reason it adds follows A-33 and A-74. #223 (PITR
  follow-ups) has no v1beta1 API impact: it covers retention re-inspection, precedence among existing reasons and an
  upstream decoder.

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
    (`^https?://[^\s/?#@]+/?$`, with the empty value admitted as below). One URL convention then covers every S3
    endpoint in the API.
  - The operator adapts the URL for Dragonfly: the URL's host and port become `--s3_endpoint=<host:port>`, and
    the scheme becomes `--s3_use_https=<true|false>`. Unit tests pin both flags for `http://` and `https://`,
    with and without a port.
  - An omitted or empty `s3EndpointURL` still means AWS S3, as an empty `s3Endpoint` does today
    (`dragonfly_types.go:125-128`). An OpenAPI pattern applies to an explicit `""` too, so the pattern admits it:
    `^(https?://[^\s/?#@]+/?)?$` (revision 5, F-50; revision 4's pattern rejected the empty value this bullet
    supports, which a Helm or Kustomize template that renders `""` would hit). E5's envtest admits `""` and
    rejects `rustfs:9000` without a scheme.
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
  - **"Applied" includes an edit that does not change the topology config (revision 5, F-28).** The runner keeps
    the live manager when the new `TopologyConfig` equals the old one and only runs its setters
    (`runner.go:497-511`), and backup schedules and pod labels are not in that config (`topology.go:50-89`). Taken
    literally, a backup-schedule edit would leave `observedGeneration` below `metadata.generation` forever, and the
    Argo CD Lua would report "Progressing" indefinitely (`argocd-cm-patch.yaml:46-50`). So the applied generation
    advances when the equal-config path has synchronised successfully as well as when a manager is created, and a
    generation advance by itself schedules a status write (today `topology.go:1675-1684` publishes only on listed
    changes). E11 tests a backup-schedule-only edit: `observedGeneration` and `Ready.observedGeneration` reach
    `metadata.generation`.
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
  - MBK writes `Ready=False` from creation: with the specific pending reason while it waits
    (`WaitingForHealthySource`, `BackupJobConflict`, …; A-73), `Pending` only as the generic fallback before a
    specific reason is known, and `Running` while the Job runs. Revision 4 gave `Pending|Running` only, which
    contradicted A-73's specific reasons for the same state (revision 5, F-29).
  - Add `Ready` printer columns to MBK, MBV and MSC.
- **Disagreement:** KINDS said this can wait. Chosen now: its reasons should freeze with the rest.

#### A-71 MFG `Ready` reasons say why

- **Sev** should · **Brk** yes (reason values) · **Merges** STATUS-18
- **Evidence:** `readyReason := "TopologyPolled"` for both True and False (`runner.go:1484`). STATUS said nothing
  keys on it, but `runner_test.go:33,43` and the course fixtures under `course/.../tests/fixtures/*.json` do.
- **Decision:**
  - True → `Healthy`.
  - False → `NoWritablePrimary` | `ReplicationUnhealthy` | `RestoreDamagedPrimary`. The third is proposal 21's
    override while `status.restoreDamage` exists (`21-in-place-pitr-staging.md:399`), added by #241 (its PR1).
    It is in the closed set whichever of #241 and WS-E lands first (revision 4, OPUS-R3-01).
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
  - **When `Degraded=True/BootstrapFailed` clears (revision 5, F-30).** The failure is held in the topology
    manager's bootstrap phase until the next bootstrap (`topology.go:4399-4404`), while `Degraded` is recomputed
    from the snapshot every poll (`runner.go:1482-1519,2229-2234`), and a True `Degraded` gates deployment leases
    (`deployment_leases.go:299-324`). So `BootstrapFailed` is one more input to `degradedReason`, computed each
    poll, not latched in the condition. It holds while the bootstrap phase is failed and the target site's
    `replication` is neither `Healthy` nor `Lagging`. It clears when a new clone starts or the site replicates by
    another path. It ranks below `CrossSiteAlert` and above `SiteUnhealthy` and `ReplicationLagging`. E11 tests
    that it persists across polls, clears on recovery without another clone, and that deployment leases are
    granted again afterwards.
  - `Updating=True/OrderedUpdateStalled` while stalled.
  - `Degraded` reasons `Degraded` → `SiteUnhealthy`, `Alert` → `CrossSiteAlert`.
  - Update the Lua (the `Bootstrapping` block at `:66-75`, which A-72 rewrites because failure moves to
    `Degraded`, and the stalled-update block at `:77-87`; revision 4 cited `:37,69,77`, F-58), the deploy-API docs,
    `crd-reference.md` and s19.
  - **Every consumer of the old bootstrap vocabulary (revision 5, F-31).** Four more scenarios key on `Done` and
    `True/Failed`, and run only in the E2E profiles, so `make test` does not catch them: s10 and s13 wait for
    `False/Done` and fail fast on `Failed` (`s10_full_bootstrap_after_data_wipe.go:156-159`,
    `s13_operator_kill_during_bootstrap.go:178-181`), s37 waits for `Done` after the request
    (`s37_pitr_archive_handoff.go:281-292`), and s59 starts its repair on `Failed` (`s59_killed_clone.go:386-399`;
    `repair_helpers.go:88-89` states the old polarity). `NotBootstrapping` is also the state before any clone, so
    a token swap is not enough. Completion becomes `False/NotBootstrapping` with a `lastTransitionTime` after the
    request (or after the observed bootstrap start) and the target's `replication` `Healthy`; failure and repair
    key on `BootstrapFailed`. The docs that teach the old tokens change too: `playground/chaos-scenarios.md:39,43,
    545,1479`, `6.operations/9.failure-mode-matrix.md:51` and the course (`unit-7-day-0-and-day-2/
    topic-24-provisioning/read.md:287`, `SOURCES.md:228`). All of it is in E11.

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
  - **The timeout escalation does not carry a pending reason (revision 5, F-29).** `pendBackup` turns a backup that
    stays pending past `maxBackupPendingDuration` (30m) into a terminal failure with the caller's pending reason
    (`backup_reconciler.go:45-49,979-982`; `failBackupOpts` keeps it, `:926,933-938`). Renamed as above, a timed-out
    backup would read `Phase=Failed`, `Ready=False/WaitingForHealthySource`. The escalation maps each pending
    reason to a terminal one: `WaitingForHealthySource` → `NoHealthySource`, and any other pending reason,
    `BackupJobConflict` included, → `PendingTimedOut` with the pending reason in the message. E11 tests the
    timeout path.
  - The Event `PITRArchiveReady` → `PITRArchiveRecovered`.
  - Document the closed `unstableReason` set in `11.deployment-api.md`, sourced from the A-69 constants.
  - **That set is not closed today (revision 4, OPUS-R3-07; made exhaustive in revision 5, F-32).** Revision 4
    cited only `:345-361`, and section 8.1 recorded the point as fixed while the list was still partial. The
    whole function is `deployment_leases.go:270-364`. Its return branches are:
    - the literals `DeploymentRevocationPending`, `Initializing`, `NoPrimary`, `PlannedFailover`, `OrderedUpdate`,
      `Restore`, `DragonflyUpgrade`, `RestoreInPlace`, `NotReady`, `SiteNotObserved`, `SiteNotReady`,
      `RestoreReclonePending` and `ReplicationNotRunning`;
    - two concatenations: `"Dragonfly" + string(d.Phase)` (`:293-295`), whose values follow A-28's `DragonflyPhase`
      values, and `"SourceConvergence" + string(s.SourceConvergenceState)`;
    - the raw `s.RecoveryState` and `gatingRecoveryState(fg)` (`:317`);
    - a True gating condition's reason, or its type name when the reason is empty (`return name`, `:300-324`).
    E1 declares an A-69 constant for every literal, for each value of both concatenations over its enum, and for
    every recovery state; the condition branch draws on the condition-type and reason constants. The function
    returns only declared constants. E1's registry test derives the set from every return branch, not from this
    list, and E11's deploy-API test asserts that every value returned is in it.
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
  - Rule: `<Subject><PastTenseVerb>`, optionally followed by a qualifier that names the cause
    (`PromotionRefusedReclonePending`). Revision 5 states the qualifier explicitly, and removes the table's
    targets that had no verb (F-33). `Succeeded`/`Failed` for operations, `Rejected` for user requests that
    are not admitted, `Refused` for automatic actions blocked for safety. US spelling, and `PITR` is the only
    acronym form.
  - E12 applies STATUS-23's rename table, A-73's Event rename and A-75's, together with the log-schema doc, the
    doctor skill and runner assertions (revision 5, F-34; revision 4 said "in one commit, together with A-33,
    A-73", while the work list put A-33 in E7 and A-73 in E1 and E11). E7 types the status reasons (A-33). Where a
    status reason shares an Event token (`status.restore.reason` reuses `PointInTimePartial`, section 1.4), E12
    renames the Event, the status writer and its constant together.
    - `PlannedFailoverCompleted` → `PlannedFailoverSucceeded`: E12 renames whatever completion Event #235 leaves,
      and puts every other Event reason that survives #235 through the same rule. #235 merges first (C-10).
      Revision 3 asked it to use the new name (the A-50 checklist), reversing OPUS-15 for this one block while the
      branch was at WP0. The branch has since moved on (WP1 and WP3 merged; `2527bbe` on 2026-11-05) and again
      emits `PlannedFailoverCompleted`, with about fifteen inline Event literals, several not in this rule's form
      (`PlannedFailoverLagOK`, `PlannedFailoverStatusStalled`, …). The ownership stands: WS-C is asked to comply,
      and WS-E's step 0 fixes whatever it leaves (A-50; revision 5, F-55).
    - `DragonflyPromotionCompleted` → `DragonflyPromotionSucceeded`
    - `DragonflyUpgradeCompleted` → `DragonflyUpgradeSucceeded`
    - `RecoveryComplete` → `OldPrimaryRecovered`
    - `RestoreReclonesComplete` → `RestoreReclonesDrained`
    - `RestoreInPlaceCancelled` → `RestoreInPlaceCanceled`
    - `PointInTimePartial` → `PITRReplayTruncated` (revision 4 said `PITRReplayPartial`, which has no verb)
    - `PointInTimeCrossSiteUnverified` → `PITRCrossSiteVerificationSkipped` (revision 4: `PITRCrossSiteUnverified`)
    - `KeyringRotationRefused` → `KeyringRotationRejected` (update s48): a rotation is a user request
    - `PromotionRefusedPendingReclone` → `PromotionRefusedReclonePending`
    - `KeyringPhase` → `KeyringPhaseChanged`
    - `PITRArchiveReady` → `PITRArchiveRecovered` (A-73; it is also a condition type)
    - Revision 4 also renamed `EncryptionAdoptionRefused` → `EncryptionAdoptionRejected` and `MetadataParseFailed`
      → `MetadataUnreadable`. Both rows are dropped (revision 5, F-33, F-57). Under A-51 adoption is a spec policy
      that blocks an automatic action, which is the `Refused` case, and A-51 keeps that Event.
      `MetadataParseFailed` already follows the rule; `MetadataUnreadable` stays the MSC condition reason
      (`standbycluster_reconciler.go:336,348`).
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
  - Use `OwnerUserDropSkipped` at `:1081,1086`. The Normal case at `:756` ("nothing to drop") keeps
    `DatabaseDropSkipped`, and the Warning at `:973` ("another MysqlDatabase still declares database …") becomes
    `DatabaseDropRefused`, A-74's word for an automatic action blocked for safety. Revision 4 renamed the Normal
    case to `DatabaseDropNotNeeded`, which has no verb (revision 5, F-33).
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
      becomes `restore-hold.bloodraven.shipstream.io/<runID>`. Only the key prefix changes. Verification-hold values
      stay the verification name. Restore-hold values stay `<group name>/<group UID>`, as proposal 21 specifies
      (`21-in-place-pitr-staging.md:442`). Its release rules need that UID to tell a deleted group from a
      same-name successor, and to keep repair sources while `restoreDamage` exists (revision 4, GPT-R3-03;
      revision 3 said "the value stays the holder's name", which is wrong for restore holds). The generated
      metadata page (A-79) gives each hold's value grammar separately, and #205's restore-hold tests use the
      UID form. Consumers that
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

- **Sev** should · **Brk** yes (both reclone forms; revision 4 broke only the divergent form) · **Merges** OPS-04 (annotation part), GPT-38
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
  - **An operation-id subject satisfies both rules (revision 4, OPUS-R3-01).** If the subject is an id that the
    operator minted and recorded in this group's status, and ids are never reused (a UUID run id, or A-86's
    promotion UUID; revision 5, F-14), the value cannot match another group or a re-created one. So it needs neither `confirm=` nor
    `uid=`. A site-name subject always needs them. Proposal 21's `restore-accept-damaged=<originRunID>` and
    `restore-staging-cleanup=<originRunID>` are destructive and use this form.
  - **A subject may be a comma-separated list of ids** (revision 4, OPUS-R3-10), for acknowledgements that clear
    several records at once (A-86). Each id matches `^[A-Za-z0-9._/+-]+$`. `,` is not a shell metacharacter, and it
    is in neither the value charset nor the pair syntax, so the split stays unambiguous. A site-name subject is
    never a list.
  - **Both reclone forms (revision 5, F-35).** A reclone wipes the site's datadir (`reclone.go:91-103`) and has a
    site-name subject, so both forms carry `confirm=` and `uid=`. Revision 4 gave the divergent form as
    `<site>:gtidPrefix=<p>` alone, which contradicted the two rules above. The forms are:
    - cold: `<site>:confirm=<metadata.name>:uid=<metadata.uid>`;
    - divergent: `<site>:confirm=<metadata.name>:uid=<metadata.uid>:gtidPrefix=<p>`. `gtidPrefix` is an extra
      interlock that proves the requester read the divergence; it does not replace `confirm=`. `<p>` is a prefix
      of the divergent GTID's source UUID: hex and `-`, at least 8 characters (`minRecloneGtidPrefix`). Today the
      operator writes the GTID up to its first comma, interval included (`reclone.go:148-152`), so the value can
      contain `:`. It then writes the UUID part only.
    - The operator's own writer (`recloneAnnotationValue`, `reclone.go:143-160`) and `kubectl bloodraven
      reclone` build the full form. A value missing any required pair is rejected with a message that prints the
      full form.
  - #235, #204 and #205 (proposal 21's action annotations, A-59) are expected to follow this grammar.

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
    (required), site, divergentGtid (MaxLength=8192), divergentGtidTruncated bool, divergentTransactions int64,
    comparedTo, promotionTime metav1.Time}` (the `…Time` suffix of A-40; revision 2 said `promotedAt`).
  - **The id is a UUID minted once per promotion (revision 5, F-14, F-36).** Revision 4 built it from the
    promotion second and the site (`20261104T100000Z/iad`). Site names repeat across groups, and a whole-site
    outage fails many groups over to the same site in the same poll, so identical ids across groups were the
    expected case, and A-78's claim that an operator-minted id "cannot match another group" was false for them.
    Two promotions of one site within a second would also have produced a duplicate list-map key. A UUID
    meets A-78's value charset and never repeats, so A-78's exemption from `confirm=` and `uid=` holds.
    `promotionTime` and `site` stay separate fields. The list-map key is marked `required`, as the apiserver
    demands of a list-map key with no default.
  - **Bounded size (revision 5, F-49).** A long-diverged site can carry thousands of GTID intervals
    (`handoff.go:366-367`), and 16 unbounded sets could exceed the request limit before any other status. Each
    record stores at most 8 KiB of the set, and `divergentGtidTruncated: true` says when it was cut. 16 records
    are then at most about 130 KiB.
  - **When full (OPUS-22; revised in revision 5, F-16, F-36).** With 16 unacknowledged records, the operator keeps
    the 16 oldest, counts every further promotion in a durable `status.divergentPromotionsOmitted int32`, and emits
    `DivergentPromotionRecordsTruncated`. Revision 4 kept the count only in the condition message, so it was lost
    on an operator restart, and acknowledging the 16 retained ids cleared the condition while an omitted
    promotion had never been acknowledged. The status write never fails on the bound.
  - **What the Events and logs keep (revision 5, F-15).** Revision 4 said each promotion's `DivergentSitePromoted`
    Event and ERROR log "remain the full record". They do not: both cut the GTID set at 1 KiB
    (`runner.go:1845,2309-2311`; `topology.go:5140-5142`; `handoff.go:366-376`). They carry the site, the
    transaction count and the first 1 KiB of the set. So the complete set of an overflowed promotion, or of a set
    longer than 8 KiB, is not retained by the operator, and the record says so. The runbook tells the
    administrator to capture `gtid_executed` on the old primary before any reclone of it, which is where the
    divergent transactions still are.
  - Add the one-shot annotation `bloodraven.shipstream.io/acknowledge-divergent-promotion=<id>[,<id>…]`, using
    A-78's list subject (revision 4, OPUS-R3-10). The operator removes the matching entries and emits
    `DivergentPromotionAcknowledged`. An id that is not in this group's list is ignored with a Warning Event, so
    an id copied from another group's status clears nothing. The omitted count is acknowledged with the subject
    `omitted`, which is not operator-minted and so needs A-78's binding:
    `acknowledge-divergent-promotion=omitted:confirm=<metadata.name>:uid=<metadata.uid>`.
  - The condition stays True while the list is non-empty or `divergentPromotionsOmitted` is above 0, and its
    message counts both.
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
    unsupported on real clusters", with one prerequisite per pass (revision 5, F-59): the normalization pass
    runs against a live, converged playground cluster, by design (`reset.go:359-364,378-381`, which is why it keeps
    `encryptionAtRest`); the wipe pass is safe only after the data plane is wiped. Revision 4 gave both passes
    the wipe's prerequisite.
  - `ClearFailoverState` stays in `internal/`. Fields #235 adds join the reset list, and so do A-86's
    `divergentPromotions` and `divergentPromotionsOmitted` (revision 5, F-37): they replace records that lived in
    `conditions`, which the reset already clears (`reset.go:365-370`), and A-86 keeps `DivergentPromotion` True
    while they are non-empty, so a wiped playground would otherwise re-raise it.

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
  - A pattern per field, matching what each tool accepts (revision 5, F-60; revision 4 gave both
    `^[0-9]+[KMG]?$`). Measured in the `mysql:9.7` image: `mysqld` takes its size suffixes in either case (`512m`
    and `512M` both give 536870912), and mysqlsh 9.7.1's `bytesPerChunk` accepts `k`, `K`, `M` and `G` and rejects
    `m` and `g`. So `maxBinlogSize` gets `^[0-9]+[kKmMgG]?$`, which keeps its documented "any value MySQL accepts"
    contract (`backup_types.go:136-141`), and `bytesPerChunk` gets `^[0-9]+[kKMG]?$`. Keep them strings: MySQL's
    `M` is MiB, not Quantity's 10^6.
  - On both `mysqlConf` maps (`types.go:128,378`): `MaxProperties=128`. Values get a named string type with
    `MaxLength=1024` and the OpenAPI `Pattern=^[^\r\n\x00]*$`, which costs nothing in the CEL budget (OPUS-11).
    Keys are checked by CEL `self.all(k, k.matches('^[A-Za-z0-9_.-]+$'))`.
  - **The key rule fits the CEL budget (revision 5, F-61).** Revision 4 said the estimator sizes every map key at
    the request limit, and recorded a reshape to a list-map as the fallback. The estimator does the opposite: it
    sizes `@keys` from the map's key type, the shared `StringType`, whose `MaxElements` is 0
    (`apiextensions-apiserver` v0.37.0 `pkg/apiserver/schema/cel/compilation.go:313-345`; `apiserver`
    `pkg/cel/common/schemas.go:104`). Measured: the rule on a map with `MaxProperties=128` and 1024-byte values
    costs about 1.2e3 against the 1e7 per-expression limit, while the same rule over an unbounded list of 128
    strings is refused at 20 times the budget. The fallback is dropped. A long key is bounded only by the runtime
    per-call cost limit and the request size, which is acceptable for a key that the pattern restricts to
    `[A-Za-z0-9_.-]`.
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
  - The operator-owned flag list lacks `--maxmemory`, `--proactor_threads` and `--replicaof`
    (`internal/controller/dragonfly_resources.go:316-327`), although the API doc calls all three reserved
    (`dragonfly_types.go:56-60`). Revision 4 named only the first two (revision 5, F-38). `--replicaof` makes a
    site's Dragonfly replicate from an arbitrary host at startup, outside the operator's `REPLICAOF` management.
- **Decision:**
  - `license`: `MaxLength=4096` and the compact-JWS pattern.
  - `args`: `MaxItems=64`, `items:MaxLength=256`, and CEL rejecting the reserved flags (the controller list
    plus the three missing ones; see D-05). E16's envtest rejects `--replicaof=<host> <port>`.
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
| A-111 | MFG `status.pitr.profileName` (`backup_types.go:164`) | **Withdrawn in revision 5 (F-39), folded into A-49.** Revision 4 kept it as an observation that attributes the archive counters. The code copies it from spec, and relabels kept counters with the new profile, so it is an echo. | STATUS-L6 |
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
    among the labels revision 4 listed, only `dr-only` → `disaster-recovery` changes value (revision 5 adds the
    restore-reclone `phase`, below). Lowercase label values are the Prometheus convention, and
    most documented queries keep working.
  - **Every enum-valued label goes through that mapping (revision 4, OPUS-R3-06).** Revision 3 covered only `role`
    and `state`. Three more labels carry API enum values, and their producers are ad hoc today:
    - `bloodraven_keyring_phase{phase}`, set from `strings.ToLower(string(site.Phase))`
      (`internal/controller/encryption_reconcile.go:993`), values `metrics.AllKeyringPhases`
      (`internal/metrics/metrics.go:421`). It is alerted on as `phase="sealed"` (`6.operations/4.runbooks.md:156`),
      and A-47 moves alerting onto it.
    - `bloodraven_replication_source_state{state}`, set from `strings.ToLower(string(site.sourceConvergenceState))`
      (`source_convergence.go:304`, `metrics.go:406`).
    - `bloodraven_state_transitions_total{from,to}`, set from `prevStates[i].String()` (`topology.go:1440`). The
      PromQL `{to="writable"}` (`8.observability/2.monitoring.md:150`) and the chart dashboard
      (`charts/bloodraven/dashboards/failover.json:163`) key on it.
    - (revision 5, F-17) `bloodraven_restore_reclone_pending{phase}`, set from `string(q.Phase)`
      (`restore_peer_replication.go:645`), so today it carries the CamelCase API values `Armed`, `Draining` and
      `Held`. Revision 4 missed it. It goes through the mapping like every other enum-valued label, so its values
      become `armed`, `draining` and `held`. That is a value change: the documented `BloodravenRestoreReclonesHeld`
      alert (`2.monitoring.md:212-213`) becomes `{phase="held"}`, and 3.4 has a row for it. Rejected: an
      exception that keeps this one label CamelCase, which would freeze two label spellings into v1beta1.

    `strings.ToLower` and the kebab-case mapping agree on today's one-word values, but they diverge on any two-word
    value (`ReadOnly` → `readonly`, not `read-only`). So every enum-valued label (`role`, `state`, `from`, `to`,
    `phase` and the source `state`) is produced by the one mapping function for its enum, and no producer lowercases
    by hand. The values of `role`, `state`, `from`, `to`, the keyring `phase` and the source `state` stay what
    they are today. The restore-reclone `phase` changes value (above), and 3.4 has a row for it.
  - **`/deploy/v1` tokens** are the A-69 constants, verbatim. `11.deployment-api.md` documents every token field
    with its closed value set, generated from those constants. The release notes list each renamed value. The
    path stays `/deploy/v1`: v1.7.0 is the breaking release, and there are no production users.
  - **`health.replicationRunning`** keeps its name and today's meaning, defined over the A-43 enum: every
    non-active spec site reports `Healthy` or `Lagging`.
  - **Metric `reason` labels** carry the A-69 reason constants verbatim, in CamelCase. They are reasons, not enum
    values, so no kebab-case mapping applies. **Two are snake_case today (revision 5, F-18):** the lease
    revocation reasons `operator_revoked` and `topology_changed` (`deployment_leases.go:193-199`), which are also
    the `/deploy/v1` 409 `reason` (`:630,636`), the documented PromQL (`6.operations/2.operations.md:48`), the
    documented 409 bodies (`4.configuration/11.deployment-api.md:199-200,232`) and s53's assertion
    (`s53_deployment_coordination.go:149`). They become the A-69 constants `OperatorRevoked` and `TopologyChanged`
    on the metric and on `/deploy/v1` alike, with a 3.4 row. Rejected: declaring them permanent exceptions, which
    freezes a second reason spelling into the deploy API. A rename by A-73, A-74 or #235 changes the label
    value, and the release-note rename table lists it next to the Event and condition renames.
  - **Operator `/status` and `/ws/status`** are JSON views of the same facts as the CR status, so they use the CR
    vocabulary verbatim: `role` and `state` carry the A-27 and A-28 values (`PrimaryCandidate`, `Writable`, …),
    produced by one adapter from the typed enums that replaces `state.String()`. For `role`, this is the reverse of
    A-27's API-to-internal adapter, because the endpoints read the internal `state.SiteRole` (GPT-R3-01). Their field shapes follow the CR
    too: `/ws/status` replaces the flat `lastFailover`, `lastFailoverTarget` and `promotionGtidExecuted` with
    A-40's `lastFailover{time, target, promotionGtidExecuted}`, and `sites[].replicating` with
    `sites[].replication` (A-43). The paths stay. One rule results: JSON surfaces use the API values, and only
    Prometheus labels use the kebab-case form.
  - Obligations, by commit:
    - E7: the metric mapping functions; the `/status` and `/ws/status` role and state adapter; the dashboard's
      comparisons and CSS classes (`index.html:17-18,131-150`); `3.monitoring-prometheus.md`, `2.monitoring.md`
      (PromQL and the two endpoint examples at `:543-651`), `examples/prometheusrule.yaml` and the doctor scripts'
      PromQL. Tests: a test enumerates every enum-valued label of every metric and asserts that each enum value
      maps to its documented label value (revision 4: `phase`, the source `state`, `from` and `to` as well as `role`
      and `state`; revision 5: the restore-reclone `phase`, with the `BloodravenRestoreReclonesHeld` alert and its
      docs updated in E7); a payload test asserts that both endpoints serialize every role and state as the CR value; and
      A-27's role-boundary test.
    - E10: the `/ws/status` `lastFailover` and `replication` reshapes and their dashboard readers;
      `replicationRunning` over the enum, with a deploy-API test per enum value.
    - E11: `11.deployment-api.md` (every token field with its closed value set, and the `replicationRunning`
      definition) and the deploy API tests.
    - E12: a unit test that every `reason` label value written is a declared A-69 constant, and the metric rows of
      the release-note rename table. E12 also renames `operator_revoked` and `topology_changed` in the controller,
      the 409 body, `11.deployment-api.md`, `2.operations.md`, `2.monitoring.md:50`, the doctor
      (`alert_runbook_matrix.md:64`, `SKILL.md`) and s53.
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
    would now reject.
  - **What ratcheting does, measured (revision 5, F-07, F-40).** Revision 4 said it does not cover `required`
    or type changes (GPT-R2-09). That is wrong for an unchanged node. On 1.31, 1.32, 1.34 and 1.37 envtest, every
    OpenAPI schema error on a node that is DeepEqual to the old value is downgraded (type, `required`, enum,
    pattern, list-map), and no warning is returned for it (`apiextensions-apiserver` v0.37.0,
    `pkg/apiserver/validation/ratcheting.go:136-156`). A transition rule passes on an unchanged value because
    `self == oldSelf`. What still refuses is validation of any changed node, and a CEL evaluation error on
    mistyped data (a `duration()` bound evaluated on an integer). **One exception at the floor:** on 1.31 and
    1.32, a write through the status subresource re-runs the spec-level CEL rules without ratcheting them, so an
    object whose unchanged spec violates a newly tightened non-transition spec CEL rule accepts main-endpoint
    updates but rejects every status write. 1.34 and 1.37 ratchet it; 1.33 was not tested. Losing status writes
    would lose the durable failover record (A-89) while the operator keeps acting on MySQL.
  - **Rule for every later tightening.** Ratcheting helps v1beta1 → v1, but it is not a general compatibility
    mechanism. Any change that tightens a spec CEL rule ships with an envtest on the 1.31 job (C-15) that stores
    an object violating the new rule and then performs a status write on it; the change is accepted only if that
    write succeeds or the release notes call the break out. The floor stays 1.31: raising it to 1.34 for this
    one case would drop three supported minors, and the test catches it.
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
2. **Ratcheting tolerates an unchanged old object, which is a hazard, not a path.** An update validates the
   whole object, status included (`k8s.io/apiextensions-apiserver` v0.37.0,
   `pkg/registry/customresource/validator.go`, `ValidateUpdate`), and a status update runs the CEL rules against
   the whole object, spec included (`status_strategy.go`, `ValidateUpdate`). At the A-115 floor
   `CRDValidationRatcheting` is on, and it downgrades every schema error on a node that did not change: type,
   `required`, enum, pattern and list-map errors alike, with no warning returned (A-115). Revision 4 said it does
   not cover `required` fields or type changes; that was wrong (F-07, measured on 1.31, 1.32, 1.34 and 1.37). So
   a no-op write of a pruned v1.6.0 object succeeds and persists the pruning, while the typed v1.7.0 client still
   cannot decode it (item 1). A changed node is validated in full, so a re-applied, edited manifest must meet the
   whole new schema, including the transition rules (item 3). Neither gives an upgrade path.
3. **The new transition rules reject the re-apply on any version.** A transition rule passes only when the
   value is unchanged, and the re-apply changes it.
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
the release notes link it (C-13). The procedure is tracked as a separate follow-up issue owned by WS-F
(#243). Round 3 again found a Major gap in this area (OPUS-R3-08), the third consecutive round, so revision
4 only adds that requirement to U-1 (section 8.1).

The procedure's outline stays as before: back up on v1.6.0 after application writes stop, install v1.7.0 in a
cluster with no Bloodraven CRDs, write the new manifests from the 3.4 table, bootstrap from the backup, load the
tenant databases, verify, cut over, and retire the old group. Its text must meet every requirement below.

| # | Requirement | Why |
|---|---|---|
| U-1 | **Never apply the v1.7.0 CRDs to a cluster that runs v1.6.0, by any route.** The procedure says so first, in a warning box. It names each route that does it: the documented "CRDs first, then the chart" routine (`6.operations/10.upgrade-policy.md:98`); a platform CRD app or a Helm release in Argo CD that renders the chart's `crds/` (`6.operations/11.gitops.md:11-27`); the production install examples, whose Argo CD layout commits the chart's CRDs and syncs them before the operator (`3.get-started/5.production-install-examples.md:80`; revision 5, F-62); the course's answer that teaches the same routine (`course/course-bloodraven-in-production/unit-7-day-0-and-day-2/unit-test.md:125`); and (revision 4, OPUS-R3-08) the production install page, whose "CRDs managed separately" loop applies all five CRDs with `--force-conflicts` from the `main` branch (`3.get-started/4.install-production.md:39-46`). On the old cluster, pin both the chart and the CRD source in Git for as long as it runs. C-12 and E19 add the same warning to the pages that teach the routine: the upgrade policy, the GitOps page, the production install examples and course unit 7. The install page's CRD URL is pinned to a release tag, and the site is built from the latest release tag, before any incompatible CRD change reaches `main`, including WS-C's and WS-D's (section 6 prerequisite; revision 5, F-41, F-42; revision 4 said "no later than the first WS-E commit"). E19's gate fails on any `main`-branch CRD URL. | The API server prunes unknown fields when it reads objects, so the old field names vanish from every read while types stay wrong (3.1 item 1). The still-running v1.6.0 operator, or any v1.7.0 operator, would then persist pruned objects on its next `Update`. Nothing in v1.7.0 can stop a v1.6.0 operator (OPUS-R2-02). The install-page route needs no upgrade at all, and it would go live the moment an incompatible schema reached `main`, before v1.7.0 is tagged, because the URL points at `main` and the published site is built from `main` today. |
| U-2 | **Recovery from U-1 (rewritten in revision 5, F-08).** The procedure scales every Bloodraven operator to 0 first. Recovery of the stored objects is possible only while no Bloodraven object has been written since the v1.7.0 CRDs were applied, and then only by the steps #243 designs and tests (U-13 rules out simply re-applying the v1.6.0 CRDs). Once any object has been written, it is stored as v1beta1 with its old fields pruned, and the procedure restores those objects from a cluster backup or the exported manifests instead (status only from a backup that restores status, A-89), then rolls the chart back. | Pruning on read does not change etcd until something writes. `status.storedVersions` changes the moment the CRDs are applied (U-13). Revision 4's "re-apply the v1.6.0 CRDs with server-side apply" is rejected by the apiserver. |
| U-3 | **Fresh writable destinations.** The new group's backup profiles and PITR profile use prefixes that no v1.6.0 group has used. The old artifact is referenced only as a restore source (`initFromBackup.source.s3`, or a per-schema `restoreInPlace.source`), never as a profile of the new group. After every restore, including the tenant restores, the procedure takes a new full backup as the new group's baseline, and when PITR is enabled waits for `PITRArchiveReady=True`, before cut-over. | The PITR archive destination is derived from the profile's prefix (`<prefix>/binlogs`, `internal/controller/pitr.go:125-134`). An old profile copied into the new manifest would archive into the identity-less archive that #203 and #206 refuse (D3). Without a new baseline and a ready archive, the new group has no point-in-time recovery at cut-over (GPT-R2-02). |
| U-4 | **The right restore source.** The backup is taken on an **S3** profile that the new cluster can reach; a group with only PVC profiles adds one on v1.6.0 first. `source.s3.{bucket, prefix}` comes from that MysqlBackup's `status.location`, which is the dump's own path: `s3://<bucket>/<key>`, or a bare key in the profile's bucket (`internal/controller/restore.go:101-118`). It is not the profile prefix, which holds many dumps and `binlogs/`. The procedure adds `decryption` when the backup is encrypted (`backup_types.go:604-644`), and recreates the S3 credentials Secret in the new cluster. | A PVC backup is unreadable from a new cluster, and the profile prefix is not a dump (OPUS-R2-07). The docs' own example uses a dump path (`7.backup-and-restore/2.backup-restore.md:556-563`). |
| U-5 | **Tenant databases.** Bootstrap with the MysqlDatabase-managed schemas excluded (`initFromBackup.loadOptions.excludeSchemas`, `backup_types.go:697-700`). Apply the MysqlDatabase objects so they create their schemas and users. Then load each schema with a per-schema `restoreInPlace` (`loadOptions.includeSchemas: [<schema>]`), which drops and reloads that schema and replicates (`restore_script.py:294-330`; `restore_inplace.go:1412-1438` forces `skipBinlog=false`). If the tenant schemas exceed A-12's 256-entry bound on `excludeSchemas`, the bootstrap lists the non-tenant schemas in `includeSchemas` instead (revision 5, F-63). | A schema that the restore already loaded makes its MysqlDatabase fail with `DatabasePreExists` (`4.configuration/7.tenant-databases.md:276`), and D3 rules out adoption. |
| U-6 | **Say what is not carried** (3.3), with the action for each: MySQL accounts and grants that neither the operator's credentials nor a MysqlDatabase create are recreated before cut-over; the new group mints its own keyrings, and the old escrow Secrets stay with the old cluster, which needs them while it runs; failover history, operation records, MysqlBackup and MysqlBackupVerification objects, and PITR across the old archive are dropped. | The backup Job passes no `users` option to `util.dumpInstance` (`internal/controller/backup_job.go:692-717`; `backup_script.py:320`), and mysqlsh's documented default (`users: true`) dumps the application accounts and grants and excludes only MySQL's internal accounts. So the accounts are in the dump; they are lost because restores never set `loadUsers` (`2.backup-restore.md:602-604`). Revision 4 said the dumps carry no users (revision 5, F-64). The procedure may extract them from the frozen dump (for example a users-only load on a scratch instance) or recreate them, but they must exist before cut-over. Otherwise they are lost silently (OPUS-R2-07). |
| U-7 | **Manifests** follow the 3.4 table: names valid under A-15; an explicit `serverID` per site (A-08); keys that only restated a persisted default deleted, not translated; `status` and server-set metadata stripped. They are applied with `kubectl apply --server-side --validate=strict`, so a leftover old field name is rejected instead of pruned. | Unchanged from revision 2. |
| U-8 | **Verify before cut-over:** `Ready=True` and `Degraded=False` on the group, `Ready` on every MysqlDatabase, `PITRArchiveReady=True` when PITR is on (U-3), a MysqlBackupVerification of the new baseline backup, and row counts or checksums of the application's tables against the frozen source. Retire the old group only after cut-over is verified, keep its backups as long as recovery policy requires, and never delete a MysqlFailoverGroup in order to recreate it, except a group whose bootstrap restore never succeeded (A-23; revision 5, F-20). | Unchanged from revision 2, plus U-3. |
| U-9 | **Tested end to end once** by WS-F (C-16): published v1.6.0 chart and images, an encrypted S3 backup, PITR enabled on the new group with fresh prefixes, one tenant database, and one account created outside the operator. The test asserts `PITRArchiveReady=True` on the new group, that no object under the old prefix changes, and that the recreated account works. | Two drafts of the steps had defects that only running them would show. |
| U-10 | **Mechanism requirements moved here in revision 5 (U-10 to U-13).** Under the scope rule, C-7 and C-14 no longer design their object checks; WS-F designs them under #243 to meet these rows. **Refusal of non-conforming objects, by a check that does not ratchet.** `migrate-storage` (C-7) and the operator (C-14) refuse, before any write, every object of the five kinds that does not conform to the schema, listing each one with a migration-table pointer (generic, no legacy field names). A no-op `dryRun=All` update is not such a check (3.1 item 2, A-115). The C-7 and C-14 envtests include an object whose only incompatibility is a missing `required` field, and one whose only incompatibility is a type change with no CEL rule on it, and assert that both are refused and that no `resourceVersion` changed. | Revision 4's dry-run preflight would have refused v1.6.0 MFGs only by accident, because A-38's `duration()` bounds error on an integer `cloneTimeout`. It would have passed an MDB, MBK, MBV or MSC whose only problem is a missing required field, and `migrate-storage` would then have persisted the pruned object and trimmed `storedVersions` (F-07). |
| U-11 | **Every kind, every cluster shape.** The operator's check covers all five kinds, not only MFGs. Any permission it needs beyond today's role is added to the RBAC markers and mirrored into `charts/bloodraven/templates/clusterrole.yaml`: today the operator has only `get`, `list` and `watch` on `mysqlstandbyclusters` (`config/rbac/role.yaml:189-196`). Envtest: a cluster with v1.6.0 MSCs and no MFG is refused. | A standby DR cluster holds MSCs and no MFG. Under v1.7.0, MSC `source.storage` and `template` are pruned (A-62, A-63), and the first MSC status write copies the stored, pruned spec (`validator.go:103-117`), which makes the pruning permanent (F-43). Revision 4 claimed `update` on all five kinds (F-68). |
| U-12 | **Concurrent writers.** The checks and `migrate-storage`'s write pass run while an operator writes status on its poll cadence (`runner.go:2435,2454-2456`). They re-read and retry on `409 Conflict`; a conflict that outlasts the retries is reported as transient, never as a schema refusal with a migration-table pointer. `status.storedVersions` is trimmed only after every object was rewritten successfully. With `replicaCount > 1`, standby replicas run the check while the leader writes. A test runs `migrate-storage` against a concurrent status writer. | A custom resource cannot be updated without a `resourceVersion` (`pkg/registry/customresource/strategy.go:269-271`), and 3.1 runs `migrate-storage` after the chart upgrade, with the operator up (F-44). |
| U-13 | **`storedVersions` is one-way.** Applying CRDs whose storage version is v1beta1 adds v1beta1 to `status.storedVersions` at once, before any object is written (`pkg/registry/customresourcedefinition/strategy.go:109-113`). The apiserver then rejects a CRD that drops v1beta1 from `spec.versions` (`pkg/apis/apiextensions/validation/validation.go:263-290`). No procedure, runbook or rollback doc tells a user to re-apply v1alpha1-only CRDs (v1.6.0 or rc.0) after that. Removing v1beta1 from `storedVersions` by hand is allowed only when no object is stored at v1beta1, and #243 says how that is established. | An object written at v1beta1 and then left in etcd under a CRD that no longer lists v1beta1 cannot be read (F-08). U-2 and C-14's rollback rule follow from this row. |

### 3.3 What a new group does not carry over

This replaces the first draft's "status-only rows: the operator rewrites them".

| Record | Class | Why that is safe, or what replaces it |
|---|---|---|
| Observations: `status.sites[]` (state, `replication`), `activeSite`, `topologyGeneration`, `conditions`, `observedGeneration`, `updatePhase`, `dragonfly.sites[]`, encryption coverage, `pitr` counters, `backupSchedules`, MSC `discovered` | **Rewritten** by the new group's operator | Observed from the new group's own pods within a poll. |
| Failover history: `lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` and their annotation copies (A-40, A-104); the `DivergentPromotion` record (A-86) | **Dropped** | These protect the old data directories: the cooldown, the divergence evidence and the promotion watermark. The restore creates every data directory of the new group, so none of that history describes them. If you want the record, export it with `kubectl get -o yaml` before retiring the old group. |
| Operation records: `status.restore`, `restoreInPlace`, `restoreReclones` (including `fencedGtidExecuted`), `plannedFailover`, `recovery`, `dragonfly.upgrade` | **Dropped** | Each one describes an operation on the old group's data. None is in flight on the new group, which is still frozen for cut-over. |
| Keyrings and escrow Secrets (`mysql-<group>-<site>-keyring-v<N>`) | **Not carried** (R09) | The new group generates its own keyring per site, so the old escrow Secrets and their `shipstream.io/*` label keys need no migration. Keep them with the old cluster, which needs them while it runs. Backups are logical dumps and do not depend on any keyring (`4.configuration/8.encryption-at-rest.md:532-534`). Backup encryption uses its own passphrase Secret, which the new cluster needs for `decryption` (3.2 U-4). |
| MysqlBackup and MysqlBackupVerification objects, verification holds and the old PITR archive | **Not carried** | The new group's history starts with the new baseline backup (3.2 U-3). Old artifacts stay in the bucket under the old prefix, which the new group only reads, as a restore source. PITR across the old archive is refused: it has no source-archive identity (#206, D3). |
| MySQL accounts and grants that neither the operator's `credentials.*` nor a MysqlDatabase create | **Not carried** (OPUS-R2-07) | The dumps contain the application accounts (mysqlsh's documented default; only MySQL's internal accounts are excluded), but restores never set `loadUsers` (`7.backup-and-restore/2.backup-restore.md:602-604`), so the restore does not load them (revision 5, F-64). Extract them from the dump or recreate them in the new group before cut-over (3.2 U-6). |
| MysqlDatabase ledger (`ownerUser`, `ownerHosts`, `appliedUsers[]`, …) | **Rebuilt** | The MysqlDatabase objects applied in the new cluster (3.2 U-5) write fresh records for what they create. |
| A v1.6.0 object presented to a v1.7.0 schema | **To be refused by v1.7.0 tools (U-10 to U-12); not refused by anything designed yet** | Revision 4 called this "refused, with one gap". Its dry-run preflights do not refuse what ratcheting downgrades (3.1 item 2; F-07), and the operator's preflight covered MFGs only (F-43). The refusal is now a set of requirements that WS-F meets under #243: all five kinds, a check that does not ratchet, conflict handling (U-10 to U-12). `--validate=strict` rejects a re-applied old field name. **The gap that remains by design (OPUS-R2-02):** when the v1.7.0 CRDs are applied to a v1.6.0 cluster, the still-running v1.6.0 operator is not stopped by anything in v1.7.0. Only 3.2 U-1 and U-2 cover it. |

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
| MDB | `spec.deploymentClients` keyed by `namespace` only, unbounded | list-map keyed by (`namespace`, `serviceAccount`), ≤ 16 items | No change for the compound key (v1.6.0 already allows one ServiceAccount per namespace); keep at most 16 entries. Revision 4 said to split grouped entries, which no v1.6.0 manifest can have (revision 5, F-65) | A-14 |
| MFG | `sites[].name` ≤ 253 chars, `^[a-z0-9-]+$` | DNS label ≤ 20; `len(group) + len(site) ≤ 41` | Choose valid names in the new group's manifest (3.2). Site names are part of PVC and Service names, so changing one means a new group, never an edit | A-15 |
| MBK, MBV | `metadata.name` any length | ≤ 63 | Rename | A-15 |
| MFG | `metadata.name` any DNS subdomain | DNS-1035 label (`^[a-z]([-a-z0-9]*[a-z0-9])?$`) | Name the new group with a letter first and no dots | A-15 |
| MFG | removing a site allowed | refused | Replace the group with the "Replacing a site" procedure (A-16), not the v1.6.0 procedure in 3.2 (revision 5, F-19) | A-16 |
| MFG | `sites[].storage.{storageClassName, size}`, `sites[].zone` mutable (ignored) | immutable | Set the intended values when the group is created; they cannot change later | A-17 |
| MFG | `…storage.pvc{claimName, storageClassName, size, subPath}` | `pvc{claimName \| provisioned{storageClassName, size}, subPath}` | Keep `claimName`, or move class and size under `provisioned` | A-18 |
| MFG | `sites[].lbIP` | `sites[].loadBalancerIP` (IPv4) | Rename | A-19 |
| MBK | `spec.sourceSiteOverride` | `spec.sourceSite` | Rename | A-20 |
| Node taint | `shipstream.io/db-readonly-<group>` | `<namespace>.db-readonly.bloodraven.shipstream.io/<group>` | Write tolerations for the new key. Old taints stay on the old cluster's nodes until its groups are retired; remove them there with `kubectl taint nodes --all <old>-` | A-21 |
| MBK, MBV | execution fields mutable | immutable | Create a new object instead of editing | A-22 |
| MFG | `spec.initFromBackup` addable, removable, editable | fixed at creation | Set it once, at creation (3.2 U-4). Never edit or remove it. If the bootstrap restore fails, delete and recreate the never-bootstrapped group with the corrected block (A-23) | A-23 |
| MFG | `encryptionAtRest.enabled: true → false` allowed; keyring paths mutable | refused | Keep the configuration | A-24 |
| MFG | `restoreInPlace.confirm` any string | `format: date-time` plus an RFC 3339 pattern (upper-case `T` and `Z`, digits-only fraction) | Use RFC 3339 | A-26 |
| MFG | `sites[].role: primary-candidate \| dr-only \| read-only` (present in every export) | `PrimaryCandidate \| DisasterRecovery \| ReadOnly` | Rewrite the values | A-27 |
| MBV | `pointInTime.mode: none \| latest \| timestamp` | `Latest \| Timestamp`, required in the block | `none` → delete the block; capitalise the others. The MFG restore `pointInTime` has no `mode` (revision 5, F-66) | A-27 |
| MFG | `dragonfly.plannedFailover.onSyncTimeout: proceed \| fail` (present in every export with Dragonfly) | `Proceed \| Fail` | Capitalise | A-27 |
| MFG | `spec.updateStrategy: OrderedUpdate \| Recreate` (present in every export) | `spec.updateStrategy: {type: …}` | Delete a defaulted `OrderedUpdate`; wrap `Recreate` in an object | A-27 |
| MFG | `status.sites[].state`, `status.dragonfly.sites[].role` lowercase | CamelCase | Status-only | A-28 |
| MFG | `""` admitted by the status enums listed in A-28 (`restoreInPlace.phase`, `dragonfly.phase`, keyring `phase` and `unsealReason`, `recoveryState`, `divergenceDetectedBy`, `sourceConvergenceState`) | `""` rejected; absent means unset (revision 4) | Status-only | A-28 |
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
| MFG, MBV | zero rewritten to a default (`maxLagSeconds`, `backoffLimit`, `maxLagSecondsForSource`), and the TTL lifecycle change (`ttlSecondsAfterFinished` on the MBV and on the profile's `verification`) | pointers; 0 means 0 | Review explicit zeros. MBV `ttlSecondsAfterFinished` and the profile's `verification.ttlSecondsAfterFinished` no longer default to 0: an omitted value now keeps the verification's resources until retention (a lifecycle change, not a zero rewrite; F-54). Set 0 to keep immediate cleanup. | A-37 |
| MFG, MSC | unbounded Durations | lower and upper bounds | Fix out-of-range values (e.g. `pollInterval: 0s`, `failoverCooldown` < 30s) | A-38 |
| MFG | `pointInTime.stopDatetime: "2026-04-15 09:30:00"` | `pointInTime.stopTime: "2026-04-15T09:30:00Z"` | Rename; add the zone | A-39 |
| MBV | `pointInTime.timestamp` | `pointInTime.stopTime` | Rename. The MFG restore `pointInTime` has no `timestamp`; its `stopDatetime` has its own row (revision 5, F-66) | A-39 |
| MFG | `status.lastFailover`, `lastFailoverTarget`, `promotionGtidExecuted` | `status.lastFailover{time, target, promotionGtidExecuted}` | Status-only | A-40 |
| MSC | `status.discovered.lastScanAt` | `status.discovered.lastScanTime` | Status-only | A-40, A-64 |
| MFG | `status.restoreReclones.requestedAt`, `status.encryptionAtRest.sites[].unsealedSince`, `status.sites[].lastSeen` | `requestTime`, `unsealTime`, `lastSeenTime` (revision 4) | Status-only | A-40 |
| MFG | `spec.dragonfly.maxMemoryMb: 512` | `spec.dragonfly.maxMemory: 512Mi` | Convert | A-41 |
| MFG | `spec.dragonfly.proactorThreads: 0` | omit (nil = default) | Delete zero values | A-41 |
| MBK | `status.binlogFile`, `binlogPos` | `status.binlogPosition{file, position}` | Status-only | A-42 |
| MFG | `status.sites[].replicating` | `status.sites[].replication` (enum) | Status-only | A-43 |
| MFG | `status.dragonfly.sites[].syncInProgress` `bool` | `*bool` | Status-only | A-44 |
| MBK | `status.encrypted`, `encryptionAlgorithm` | `status.encryption{mode, algorithm}` | Status-only | A-45 |
| MFG | `status.encryptionAtRest.sites[].coverage.*` `bool` | `*bool` | Status-only | A-46 |
| MFG | `status.encryptionAtRest.sealed` | removed; use `EncryptionAtRestReady` | Status-only; update alerts | A-47 |
| MFG | annotation `bloodraven.shipstream.io/dragonfly-snapshot-upgrade=<img>` | `spec.dragonfly.upgradeStrategy: SnapshotRestore` + edit `spec.dragonfly.image` | Replace the annotation with a spec edit | A-48 |
| MFG | `status.pitr.enabled`, `status.pitr.profileName` (revision 5, F-39), `status.dragonfly.enabled` | removed | Status-only: read `spec.backup.pitr` and the `PITRArchiveReady` condition | A-49 |
| MFG | `status.plannedFailover.{target, sourcePrimary, reason, …}` and the #235 additions | owned by #235, held to the A-50 checklist: one durable-claim record with `runID`, `sourceSite`, `targetSite`, typed CamelCase `outcome` and `reason`; condition `AutomaticFailoverHeld`; `acknowledge-planned-failover-hold` annotation | Per #235's table; status-only for the record | A-50 |
| MFG | annotation `bloodraven.shipstream.io/encryption-adopt=confirm` | `spec.encryptionAtRest.existingData: LeavePlaintext` | Replace the annotation | A-51 |
| MFG | `spec.sidecarImage` (present in every export: the persisted default 1.3.2), `spec.sidecarResources` | `spec.sidecar.image` (empty means the operator default), `spec.sidecar.resources` | Delete a persisted `1.3.2` value to track the operator, or move a pin. An unpinned group rolls its sidecar after every operator upgrade, one group at a time | A-52 |
| MFG | MySQL and Dragonfly pods get no security context by default; Jobs use uid 27 | merged hardened defaults, uid 999 (if the A-54 gate passes) | Remove contexts that only restated defaults; check volume ownership | A-54 |
| MFG | `profiles[].retention: N` and `retentionPolicy{count, maxAgeDays, minKeep, maxFailedKeep}` | `profiles[].retention{keepLast, keepWithinDays, keepFailed}` | `retention: N` (present in every export as `7`) → `retention: {keepLast: N}`. A policy maps to `keepLast: max(count, minKeep, 1)`, `maxAgeDays` → `keepWithinDays`, `maxFailedKeep` → `keepFailed`, except that `maxFailedKeep: 0`, which means the default 10 today, is omitted, not translated to `keepFailed: 0` (keep none; revision 5, F-26). `retention: 0`, or a policy with `count: 0` and `maxAgeDays: 0`, disabled pruning: there is no equivalent, so choose an explicit `keepLast` | A-55 |
| MFG | `verification.retentionPolicy{keepSuccessful, keepFailures}` | `verification.retention{keepLast, keepFailed}` | Rename, except an explicit 0: today it means the default (30 or 10), and in the new shape 0 means keep none, so omit the field instead (revision 5, F-26) | A-55 |
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
| Events | the A-74 and A-75 rename lists, including A-73's `PITRArchiveReady` → `PITRArchiveRecovered` (revision 5, F-34) | renamed reasons and types | Update Event routing | A-73, A-74, A-75 |
| Labels, annotations, finalizers | `shipstream.io/*`; `healthy=yes\|no`; `dragonfly-traffic=enabled` | `bloodraven.shipstream.io/*`; `"true"`/`"false"` | Write selectors, NetworkPolicies and monitors for the new keys in the new group's manifests | A-77 |
| MBK annotation | `hold.verification.shipstream.io/<uid>` | `verification-hold.bloodraven.shipstream.io/<uid>` (`<kind>-hold.bloodraven.shipstream.io/<id>` family) | Operator-written; update scripts and queries that read it | A-77 |
| MFG finalizer | `shipstream.io/graceful-shutdown` | `bloodraven.shipstream.io/failover-group` | Operator-written | A-77 |
| MFG | `reclone-site=<site>:confirm=<group>` and `reclone-site=<site>:<gtid-prefix>` | `reclone-site=<site>:confirm=<name>:uid=<uid>`, plus `:gtidPrefix=<uuid-prefix>` when the site has diverged | Add `uid=`; add `confirm=` and `uid=` to the divergent form; use the source-UUID part only. `kubectl bloodraven reclone` builds the value (revision 5, F-35) | A-78 |
| MFG | condition `DivergentPromotion` removed by hand | `status.divergentPromotions[]` + `acknowledge-divergent-promotion` annotation | Use `kubectl bloodraven ack-divergence` | A-86 |
| MFG | `maxBinlogSize`, `bytesPerChunk`, `mysqlConf` unvalidated | patterns and CEL | Fix invalid values | A-90 |
| MFG | PITR, schedule and verification cross-references unchecked | CEL | Fix dangling references | A-91 |
| MFG | `license`, `dragonfly.args` unbounded; reserved flags in args | bounded, reserved flags rejected | Remove reserved flags | A-92 |
| MBK | `status.location`, `status.storageType` | `status.artifactStorage.{location, type}` | Status-only; update printer columns and scripts | A-96 |
| MBV | `status.backupRef.{location, storageType}` | `status.backupRef.artifactStorage` | Status-only | A-96 |
| MFG | `status.restore.*` and `status.restoreInPlace.*` `source*`/`target*`/`pitr*` fields | `provenance{source, target, pointInTime}` | Status-only | A-97 |
| MDB | `spec.collation` defaulted to `utf8mb4_unicode_ci` (present in every export) | no default; unset renders no `COLLATE` | Keep `collation: utf8mb4_unicode_ci` explicitly where new schemas must match the old behaviour | A-53 |
| Metrics | label `role="dr-only"` | `role="disaster-recovery"` (other `role` and `state` values unchanged) | Update alert rules and dashboards | A-114 |
| Metrics | `bloodraven_restore_reclone_pending{phase}` values `Armed`, `Draining`, `Held` | `armed`, `draining`, `held` | Update alert rules such as `BloodravenRestoreReclonesHeld` (revision 5, F-17) | A-114 |
| Metrics, `/deploy/v1` | lease revocation reason `operator_revoked`, `topology_changed` (the `bloodraven_deploy_lease_revocations_total{reason}` label and the 409 `reason`) | `OperatorRevoked`, `TopologyChanged` | Update PromQL and deploy clients that compare the 409 reason (revision 5, F-18) | A-114 |
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
`internal/` or `cmd/` instructs a status write, except where labelled break-glass below. E18's lint enforces the
command form of that rule, and review enforces the rest. The lint fails on `--subresource=status` and
`--subresource status` outside a marked break-glass block in `site/` and `.agents/`, and in any string literal in
production Go code under `internal/` or `cmd/` (OPUS-02). **It cannot see a status write given in prose or through
`kubectl edit` (revision 5, F-67).** Two rows below are of that kind: the keyring-version rollback ("Patch the
site's `keyringSecret`/`keyringVersion`/`keyringDigest` in status", `8.encryption-at-rest.md:536-541`) and the
MDB "patch the record out of status" advice (`7.tenant-databases.md:352-356`). Revision 4 said the lint enforces
the whole rule. For those two, E18 rewrites or deletes the text and its PR review checks the result, and the
`8.encryption-at-rest.md` break-glass block is the one place such prose may appear.

**Lint scope (revision 4, GPT-R3-04, OPUS-R3-09).** Revision 3 matched every string literal under `internal/` and
`cmd/`. That rejects the regression test E18 requires, because a negative assertion has to contain the token it
asserts is absent. It also catches the existing assertion at `internal/controller/restore_reclones_test.go:958`.
"Production Go code" therefore excludes:

- `_test.go` files;
- test fixtures and `testdata/` directories;
- the lint's own test inputs;
- `internal/playground/` and `cmd/playground-chaos/`, whose status writes are playground-only break-glass (A-88).
  They patch status through the API (for example `s62_full_restore_fails_after_drop.go:387`) and never print the
  command for a person to run.

Today the token is in two production files, `internal/controller/runner.go` and `internal/state/matrix.go` (the two
messages E18 rewrites), and in one test. The lint has a self-test: a production message that embeds a status-patch
instruction fails, and a `_test.go` negative assertion with the same token passes.

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
| C-7 | **D5 storage migration.** `kubectl bloodraven migrate-storage` reads and writes every object of the five kinds, then trims `status.storedVersions` to `[v1beta1]`. It never deletes and recreates objects (A-89). Before any write it refuses the objects that do not conform to the schema, listing each one with a migration-table pointer (generic, no legacy field names). **Revision 5 (F-07):** revisions 2 to 4 built that refusal from a server-side `dryRun=All` no-op update and said ratcheting would not skip type and `required` errors. Ratcheting does skip them on unchanged nodes (3.1 item 2, A-115), so a no-op dry-run update is not a reliable refusal. Under the scope rule this row no longer designs the check: the refusal, its conflict handling and its tests are requirements U-10 to U-13 in 3.2, which WS-F meets under #243. The v1.6.0 refusal envtest stays even if C-8's amendment is approved: install the v1.6.0 CRDs, create objects, swap in the new CRDs, and assert that every non-conforming object is listed, including U-10's required-field and type-only fixtures, and that no `resourceVersion` changed; then the happy path. It pins what happens when someone applies v1.7.0 over v1.6.0 (3.2 U-1). | Missing. No subcommand in `cmd/kubectl-bloodraven/`. | WS-F (#243 for the refusal mechanism) |
| C-8 | **D6 upgrade E2E. PROPOSED AMENDMENT to D6, pending orchestrator (Barry) and Colin sign-off and a program-plan update.** D6 as written installs the published v1.6.0 chart and images, creates objects through v1alpha1, upgrades to the branch build, runs `migrate-storage`, and asserts `storedVersions == [v1beta1]`, health and failover. **D6 as written cannot pass (R01, OPUS-05):** D6 conflicts with D2. D2's breaking renames include required fields and persisted defaults that change type, so a v1.6.0 object can be neither decoded nor re-applied under the cleaned schema (3.1). `migrate-storage` must refuse every non-conforming v1.6.0 object (C-7, U-10). A green D6 job would need the in-place path that 3.1 shows is impossible, or the legacy rewriting that D2 and D3 forbid (MBOD Q5 option C, rejected 3-0). **Until the amendment is signed off, WS-F builds C-7's refusal envtest and C-16's procedure test, and does not start the upgrade E2E job (OPUS-R2-12).** Revision 2 said "D6 as written stands", which told WS-F to build a job this row proves cannot pass. **Proposed (the MBOD Q5 outcome, 3-0):** the baseline is `v1.7.0-rc.0`, cut after WS-E merges (v1alpha1 only, cleaned schema). Install the rc.0 chart and images, create objects of every kind through v1alpha1, upgrade to the v1beta1 build, and run `migrate-storage`. Then assert `storedVersions == [v1beta1]`, group health and a working failover. Once the A-52 rollout-pacing PR lands (WS-E PR3, #244), the job also sets `sidecar.image.{repository, tag}` to the branch build and asserts that PR's requirements (A-52 R-1 to R-7). Revision 4's assertion that a pinned group runs no ordered update is withdrawn: a render-version bump rolls pinned groups too (F-04). **What still covers v1.6.0:** the C-7 refusal envtest on the v1.6.0 CRDs, C-14's envtests, and C-16. **Prerequisite (OPUS-05):** `release.yml` triggers on `v*.*.*` (`.github/workflows/release.yml:5-6`), which matches `v1.7.0-rc.0`, and always adds `type=raw,value=latest` (`:157`). Cutting rc.0 today would move `ghcr.io/shipstream/bloodraven:latest` and `bloodraven-sidecar:latest` to a schema-breaking pre-release. Before the cut, pre-release tags (`-rc.N`) must not push `latest`, and their GitHub release must be marked as a pre-release. The program plan gains a wave between E and F: "cut `v1.7.0-rc.0`". Revision 4 gave the `release.yml` rule to WS-F, which starts only after that cut, so the prerequisite had no owner before it applied (F-45). It is now a precondition of the cut, owned by Barry: the `release.yml` change merges before the rc.0 tag is pushed. | Missing. | WS-F (the job once signed off); Barry (sign-off, the plan update, the `release.yml` pre-release rule before the cut, and the rc.0 cut after WS-E's freeze-critical PRs merge) |
| C-9 | Status writes work through every served version, and destructive operations still require their confirmation through both versions. | Missing. Envtest installs only `config/crd/bases` (`test/envtest/suite_test.go:43-44`). | WS-F |
| C-10 | The redesigns that reshape API semantics have landed (D4): #235, #206, #203. #204 is deferred and additive (A-85 labels its gap break-glass). #205's PR1-PR3 (PR1 is #241) touch the API version `main` serves, so their order relative to WS-E is fixed in the program plan, and whichever lands second applies the A-59 table (revision 4, OPUS-R3-01). PR4 onward targets v1beta1, which under D1 means every served version identically until v1alpha1 is removed (A-59; revision 5, F-27). #240 and #223 have no v1beta1 API impact (A-59). | All open at `2340f6b`. WS-C (#235) and WS-D (#206) are in progress. Proposal 21 (#205) merged at `38e0444`; #241 is open. | WS-C (#235), WS-D (#206, #203), WS-B (#205, #241), Barry (the #241/WS-E order) |
| C-11 | The breaking cleanup (D2) has landed in v1alpha1. | Not started. | WS-E (section 6) |
| C-12 | Docs match the contract. `3.crd-versioning.md` is rewritten for D1 and D5, and adds the confirmation-pattern rule (A-82). The CRD reference, the well-known metadata page (A-79), the Event registry (A-76), the log schema, the doctor skill and the published course are current. The upgrade docs say plainly that there is no in-place upgrade from v1.6.0 and carry the procedure that meets 3.2 (C-16). `10.upgrade-policy.md`, `11.gitops.md`, `5.production-install-examples.md` (revision 5, F-62) and course unit 7 warn that their "CRDs first, then the chart" routine does not apply to v1.6.0 → v1.7.0 (3.2 U-1). `4.install-production.md` applies the CRDs from a release tag, set by a `BLOODRAVEN_VERSION` variable next to the chart version, never from `main` (revision 4, OPUS-R3-08). The site's Vue pages under `site/app/` match the contract too, not only `site/content/` (revision 4, OPUS-R3-10). | `3.crd-versioning.md` still requires a webhook and the storage-version migrator (`:28,33-34,55-112`). `4.install-production.md:39-46` applies CRDs from `raw.githubusercontent.com/ShipStream/bloodraven/main/`. `site/app/pages/compare.vue:156,781` says "v1alpha1, no conversion webhook yet", and `site/app/components/content/HomeFailoverStage.vue:73` shows `primary-candidate`. | WS-E (field docs, doctor, log schema, course, the renamed tokens in `site/app/`, the install-page pin and the release-built site as a program prerequisite (section 6), and the U-1 warnings through E19), WS-F (`3.crd-versioning.md`, upgrade docs, and the API-version text in `compare.vue` when v1beta1 ships) |
| C-13 | Release: `v1.7.0` (D7). A minor version that breaks v1alpha1 and has no in-place upgrade looks routine to anyone who reads only the number, so the release notes open with a breaking-change callout before any other content: no in-place upgrade from v1.6.0, never apply the v1.7.0 CRDs to a v1.6.0 cluster, and a link to the procedure (C-16). They then carry the section 3 table, the Event rename table, the metric, `/deploy/v1`, `/status` and `/ws/status` value changes (A-114), the log-schema breaks and the new Kubernetes floor (A-115). | — | WS-G |
| C-14 | **The operator refuses CRDs older than it was built for (a requirement for WS-F; rewritten in revision 5).** Revision 2 checked only that each CRD serves `v1beta1`, which every CRD does from v1.7.0 on (OPUS-R2-11). Revisions 3 and 4 designed an exact live-schema digest that refused on any mismatch, plus a dry-run object preflight. The ultra review found that an exact match takes the failover controller down during the routine "CRDs first, then the chart" upgrade: once two releases carry the check, the previous operator cannot restart between the two steps (a node drain, an OOM kill), and a description-only schema change refuses as well (merits point 2, F-46). It also found that the preflight does not refuse what ratcheting downgrades (F-07). Under the scope rule this row now states only what the operator must do; WS-F designs how under #243. **(1) Older CRDs refuse.** Before any controller starts, the operator exits non-zero, logging each problem with a pointer to the upgrade docs, when the live CRDs are older than the schema it was built for: a served version it needs is missing, or schema it requires is absent. This catches a chart-only `helm upgrade`, which never upgrades CRDs from `crds/`, in every later release. **(2) Newer CRDs warn.** A live schema newer than the build's, which is the previous operator during "CRDs first, then the chart", or an operator rolled back without its CRDs, starts with a Warning Event and a log line and does not refuse. So the documented upgrade window never takes the failover controller down, and a rollback needs neither an override flag nor older CRDs, which U-13 forbids across a storage-version change (F-08; revision 4 told rollbacks to re-apply the matching CRDs). **Cost:** an older operator against a newer schema that is not compatible with it only warns. The docs bound that to the minutes between the two upgrade steps, and say to roll forward rather than back across a schema change. **(3) Objects.** Before its first write the operator runs the object check of U-10 to U-12 (all five kinds, a check that does not ratchet, conflicts handled) through the dynamic client: the typed client cannot decode a v1.6.0 object under the v1.7.0 CRDs (3.1 item 1; OPUS-R3-10). It writes nothing and refuses rather than adopts, so D3 allows it. **Envtest:** CRDs missing a field the build requires refuse; CRDs with an extra optional field warn and start; U-10's and U-11's fixtures refuse. **RBAC:** `get` and `list` on `customresourcedefinitions` (read-only, which D5 does not object to), plus whatever U-11 adds. Rejected: the exact digest match (above). | Missing. | WS-F (#243 for the mechanism) |
| C-15 | **The supported Kubernetes floor is tested (A-115).** The chart `kubeVersion`, both docs pages, and a CI envtest job all say 1.31. From then on, any change that tightens a spec CEL rule adds a test to that job that stores an object violating the new rule and performs a status write on it, because 1.31 and 1.32 do not ratchet spec CEL rules on status writes (A-115; revision 5, F-40). | Chart `>=1.26.0`, docs 1.27, no job at either. | WS-E (E4: the job, the chart and the docs) |
| C-16 | **The v1.6.0 → v1.7.0 procedure is written and tested.** The upgrade docs carry a procedure that meets every requirement in 3.2 (U-1 to U-8; U-10 to U-13 govern the tools it uses), and WS-F runs it end to end once as U-9 describes. The procedure is tracked as a separate follow-up issue owned by WS-F (#243). | Missing. Revisions 1 and 2 drafted steps here; revision 3 hands them to WS-F as requirements. | WS-F |

The v1 criteria in `3.crd-versioning.md:40-54` stay as they are. Its deprecation table still applies after
v1beta1.

**WS-F items that revision 3 adds.** They come with the C rows above and belong in WS-F's brief:

- Write the v1.6.0 → v1.7.0 procedure in the upgrade docs so that it meets 3.2 U-1 to U-8 and U-10 to U-13, and run it end to end
  once as U-9 describes (C-16). This replaces revision 2's instruction to "verify the 3.2 procedure". It is
  tracked as a separate follow-up issue (#243).
- **Precondition for the upgrade E2E (OPUS-R2-12):** the D6 baseline decision (C-8) is signed off and the program
  plan updated before WS-F starts that job. Until then WS-F builds the C-7 and C-14 envtests and C-16.
- Build C-14's three requirements and U-10 to U-13's object checks, designing the mechanism under #243, with
  their envtests (revision 5; revisions 3 and 4 specified a live-schema digest and a dry-run preflight here).

## 6. WS-E work list

### Delivery as five sequential PRs (revision 5)

Revision 4 put all nineteen commits on one branch: mechanical renames next to new runtime behaviour that the
freeze does not need (A-52's rollout queue, A-54's gate, A-86's ledger, A-81's plugin verbs). Both ultra-review
merits passes found that one stall in the runtime half would block every rename, and with it WS-F, rc.0 and
v1.7.0. Three of the review's eight high findings (F-04, F-05, F-06) were in that runtime half. WS-E is therefore
delivered as five PRs. Each branches from `main` after the previous one merges, and each runs the full gate below.

- **Freeze-critical** means the item changes a schema, a served token (condition or Event reason, key, metric
  label, endpoint value) or a validation rule that v1beta1 freezes, so under D2 it must land in v1alpha1 before
  the freeze. **Only the freeze-critical PRs (PR1 and PR2) gate WS-F and the `v1.7.0-rc.0` cut.** PR3 to PR5 do not.
- The commit numbers E1 to E19 are kept, so the commit references in section 2 still hold. Within a PR, commits
  land in numeric order. Two changes from revision 4: E6 moves into PR2, after E8, because F-01's semantic
  transition rules compare the Duration, Quantity and timestamp types that E8 introduces; and E13 is split into
  E13 (the API shape) and E13b (rollout pacing).
- A field that a non-freeze-critical PR adds after the freeze (A-86's `status.divergentPromotions[]`) is additive,
  and lands in every served version identically (D1, C-4; F-27).

| PR | Scope | Commits | Freeze-critical | Label | Gates |
|---|---|---|---|---|---|
| PR1 | Mechanical renames and reshapes | E1, E2, E3, E4, E5, E7, E8, E9, E10, E11, E12, E13, E14 | **Yes**, every commit | `e2e` (E4, E5, E10, E13 and E14 change failover, restore or sidecar wiring) | WS-F, rc.0 |
| PR2 | Validation and immutability | E6, E16 | **Yes**, both commits | `e2e` (E6) | WS-F, rc.0 |
| PR3 | A-52 rollout pacing | E13b | No: runtime behaviour; A-52's API part is in E13 | `e2e`, its own PR | The first release that upgrades v1.7.0-schema groups to a new default, in practice v1.7.0 (A-52); not WS-F or rc.0 |
| PR4 | A-54 hardened security-context gate | E17 | No: A-54's API shape is the same in both outcomes | `e2e`, its own PR | Nothing. If it has not merged when v1.7.0 is cut, v1.7.0 ships option C (Q6) and records why |
| PR5 | Non-freeze-critical behaviour and docs | E15, E18, E19 | No: E15 adds an additive status field, a key and plugin verbs; E18 and E19 change docs, messages and CI gates | `e2e` (E15) | v1.7.0, through C-12 and C-13; not WS-F or rc.0 |

E14's taint-key length unit test uses A-15's bounds as constants; E6 then enforces them at admission. Until PR3
merges, a change of the sidecar default rolls unpinned groups without pacing. No release is cut in that state
for groups on the new schema except rc.0, whose groups are all new.

### Preconditions and sequencing

- **Program prerequisite, before any incompatible CRD change reaches `main` (revision 5, F-41, F-42).** Revision 4
  pinned the install page's CRD URL in E1. But WS-C (#235), WS-D (#206, #203) and proposal 21's PR1-PR3 (#241)
  merge to `main` before WS-E branches, and each may change the schema incompatibly (A-113 expects #206 to remove
  or retype `allowUnverifiedCrossSite`; A-25 expects #203 to add a root transition rule). Each such merge would
  reach v1.6.0 clusters through the unpinned URL (`3.get-started/4.install-production.md:42-46`) before E1
  existed. And the published site is deployed from `main` on every `site/**` push
  (`.github/workflows/deploy-site.yml:16-21`), so from the first WS-E docs change it would describe fields that
  no installable release has: a v1.6.0 CRD prunes MBK `deletionPolicy: Retain` silently, and then deletes the
  artifact. Two changes therefore land before the first incompatible CRD change merges, by whichever workstream
  merges first, and Barry checks them before that merge:
  1. `4.install-production.md` applies the CRDs from a release tag, set by a `BLOODRAVEN_VERSION` variable next
     to the chart version (3.2 U-1, C-12).
  2. bloodraven.dev is built from the latest release tag, not from `main`, until v1.7.0 ships. Docs changes keep
     landing on `main` with the code. The cost: a docs fix for the released version during that window needs its
     own release-tag build. Rejected: an "unreleased" banner on pages built from `main`, which still serves
     manifests that the installed CRDs prune.
- WS-E branches from `main` after WS-C (#235) and WS-D (#206, then #203) merge, and after #233 and #234. It
  then reshapes on top of their fields instead of colliding with them.
- **Step 0, `plannedFailover` re-audit (revision 3, OPUS-R2-03).** Before E1, WS-E checks `spec/status.plannedFailover`,
  the `AutomaticFailoverHeld` condition, the outcome and reason enums and the #235 annotations against the A-50
  checklist, as #235 merged them. It records each gap in its PR and fixes it in the commit named in A-50 (E5, E7,
  E8, E10, E11, E12, E14 or E15). It does not change claim semantics.
- WS-E does not change the meaning of `pointInTime.sites`, `allowUnverifiedCrossSite` and `archiveProfileName`
  (A-113), or #203's encryption binding. It adds only the bounds that E6's cost budget needs (`sites`:
  `MaxItems=16` and the A-15 site-name item pattern and bound; `archiveProfileName`: `MaxLength=63`) (A-23; revision 4, OPUS-R3-05; revision 3 said "does not touch"), and it
  applies A-25 to whatever #203 adds.
- A-82 (monotonic tokens for declarative destructive operations) is in no E commit. It is a documentation rule
  owned by C-12 in WS-F, as A-50's semantics are owned by #235 (OPUS-R2-14).
- A-97 is written against the PITR fields as #206 leaves them.
- **Proposal 21 PR1-PR3 (revision 4, OPUS-R3-01).** #241 (PR1), PR2 and PR3 change whichever API version `main`
  serves, so they can land before or after WS-E branches. The PRs that are already in WS-E's base go through step 0,
  like #235. WS-E checks their fields, labels, hold, annotations, conditions and reasons against the A-59 table
  and fixes each gap in the commit that applies that rule elsewhere: E7 (`""` phase values), E10 (`…Time` names),
  E11 (`Ready` and `unstableReason` sets), E12 (Event reasons) and E14 (keys and the hold). PRs that merge after
  WS-E use the v1.7.0 rules from the start. WS-E never changes their semantics. The order of #241 relative to
  WS-E is fixed by the program plan (C-10).
- **Re-triage before E1 (OPUS-08; scope widened in revision 5, F-47).** This audit was re-checked at `2340f6b`.
  Before E1, WS-E reviews every file changed between `2340f6b` and its own base, across the whole repository, and
  filters the changes by the surfaces sections 1.2-1.5 govern: CRD fields, condition and Event reasons, metadata
  keys (most are defined in `internal/controller` until E1 moves them), metric names and labels
  (`internal/metrics`), the `/deploy/v1`, `/status` and `/ws/status` payloads (`cmd/bloodraven`,
  `internal/controller/topology.go`, `internal/platform`), the chart (Lua health checks, NetworkPolicy),
  `playground/`, `course/` and `examples/`. Revision 4 diffed a closed path list (`api/`, `config/crd`, `site/`,
  `.agents/`), which missed both #234's `encryption-status-fence` key and #233's
  `bloodraven_restore_reclone_pending{phase}` metric. WS-C and WS-D will have added fields, reasons and keys;
  WS-C's are named in A-50. WS-E triages each one against sections 1-4 (inventory row, finding, migration row,
  status-write row) and records the result in its PR before E1. This is the step that found #233's
  `fencedGtidExecuted`, `RestoreReclonePending` and `RestorePeerHold` for this revision (B.4).
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

The commits below are listed by number. They land in the PR order above (PR1: E1-E5 and E7-E14; PR2: E6, then
E16; PR3: E13b; PR4: E17; PR5: E15, E18, E19), and each one builds and passes on its own in that order.

| # | Commit title | Findings | Areas touched | Tests to add | Docs, doctor, chart and log obligations |
|---|---|---|---|---|---|
| E1 | Export well-known keys, condition types, reasons and Event reasons from the API package | A-69, A-76 (registry scaffold), A-79 (constants and `ReadOnlyTaintKey` with today's values, including the `encryption-status-fence` key), A-73 (the `unstableReason` constants) | new `api/v1alpha1/{well_known,conditions,events}.go`; controller, plugin (including the label literals in `cmd/kubectl-bloodraven/backup.go` and `verify.go`, F-52) and runner use the constants | registry test: every `Eventf` reason and every condition reason is declared; every value `DeploymentUnstableReason` can return is a declared constant (A-73, revision 4) | None of its own. The install-page pin that revision 4 put here is now a program prerequisite that lands before any incompatible CRD change reaches `main` (Preconditions; revision 5, F-42). No behaviour change |
| E2 | Remove MSC fields and status for unimplemented phases | A-60, A-61, A-62, A-63, A-06, A-64 | `mysqlstandbycluster_types.go`, `standbycluster_reconciler.go`, `examples/standby-cluster.yaml` | envtest: `Network` rejected, PVC source unrepresentable; discovery status unit tests | `5.architecture/3.multi-cluster-dr.md`, `crd-reference.md`; chart CRDs (MSC CRD shrinks by about 237 KB) |
| E3 | Remove inert spec and status fields and the legacy DSN mode | A-03, A-56, A-57, A-58, A-59, A-65, A-31 | `types.go`, `backup_types.go`, `mysqlbackupverification_types.go`; credential and env rendering; `cmd/bloodraven/trigger*.go`, plugin `backup.go`/`verify.go`; `newTestFG` and fixtures; `playground/manifests` | envtest: an MFG without `spec.credentials` is rejected, and `spec.secretName` is pruned (OPUS-28); unit: a verification Job without storage | `security-model.md`, `production-hardening.md`, chart `NOTES.txt`, `6.backup-verification.md`; doctor references to `pvcName`/`podName` |
| E4 | Require `spec`; add list-map, set and atomic semantics; explicit site `serverID`; the Kubernetes floor | A-05, A-07, A-08, A-09, A-10, A-11, A-12, A-13, A-14, A-115 | all `*_types.go`; `reconciler.go` (`--server-id` from spec); examples and playground (`serverID: 101…`); `charts/bloodraven/Chart.yaml` `kubeVersion`; a new CI envtest job on 1.31 assets | envtest: a spec-less object of each kind is rejected (moved from E3 because A-05 lands here; R11); two field managers co-own `sites`, `extraContainers` and `conditions`; duplicate `serverID` rejected; `serverID` immutable; unit: `serverID: 4294967295` renders unchanged (no `int32` wrap); every admission test also runs in the 1.31 job | `crd-reference.md`, `failover.md`, `2.getting-started.md` and `4.install-production.md` (1.31); course examples with `serverID`; `e2e` label (`server_id` rendering) |
| E5 | One reference and Secret-reference shape | A-01, A-02, A-04, A-20, A-66, A-67 | types; `mysqldatabase_reconciler.go` index key; `cmd/bloodraven/deploy_api.go`; credential and S3 readers; plugin `backup`; `deployment.yaml` env name for `operatorEndpoint` | envtest: `backupRef: {}` rejected; MDB `failoverGroupRef` immutable; Secret name bounds; unit: A-67's URL becomes `--s3_endpoint` and `--s3_use_https` for `http`/`https`, with and without a port; envtest: `s3EndpointURL: ""` admitted and a scheme-less value rejected (F-50) | every docs manifest; `11.deployment-api.md`; doctor scripts that read `spec.credentials.*`; the MDB `Group` printer column (A-94); course (`operatorSecret`, `groupRef`, `lbIP` via E6) |
| E6 | Name bounds, the string and list bounds the whole-block rules need, site removal refusal, and immutability rules on the kind roots | A-15, A-16, A-17, A-18, A-19, A-22, A-23, A-24, A-25, A-26; the A-92 and A-34 bounds that A-22 and A-23 traverse (revision 4, OPUS-R3-05) | the bounds listed in A-23, added before the rules that traverse them; MFG, MBK and MBV root XValidations; `StorageSpec`/`SiteSpec`; `PVCStorage` union; `backup_schedule.go` (`provisioned`); remove the MBV Job-label workaround | CRD test: every string and list reachable from `initFromBackup`, MBV `spec.pointInTime` and MBV `spec.sanityCheck` has a `maxLength` or `maxItems`; envtest table: every transition rule accepts unchanged updates, rejects changes and passes the CEL cost check; `confirm` with a lowercase `t` or the offset `+25:00` rejected (A-26, F-02); the house rule T-1 envtest: non-canonical Quantity, Duration, offset and explicit-zero values survive the operator's own typed finalizer and annotation-consumer `Update`s, and real changes are still rejected (F-01); root `metadata.name` bounds and grammar (`a.b` and `1orders` rejected, R08); `provisioned` cannot be removed and re-added (R04); unit: `BackupStorageMismatch` on an owned PVC of the wrong class or size | `playground/manifests/failovergroup.yaml` and `failovergroup-3site.yaml` (`lbIP` → `loadBalancerIP`; F-48); `operations.md:381` (resize); `2.operations.md` "Replacing a site" (A-16, F-19); the `initFromBackup` branch of the `RestoreBuildFailed` messages (`restore.go:904-917,1061-1064`), doctor `alert_runbook_matrix.md:57` and `5.backup-encryption.md:250-276`, pointing at the failed-bootstrap procedure (A-23, F-20); `encryption-at-rest.md` (disable is refused); `4.backup-pvc.md`; course (`lbIP`); `e2e` label |
| E7 | CamelCase, typed enums for spec and status; stable metric label values; one vocabulary on the operator's JSON endpoints | A-27, A-28, A-29, A-30, A-32, A-33, A-114 (metrics, `/status`, `/ws/status` role and state) | types and constants; the API-to-internal `state.SiteRole` adapter replacing the cast at `reconciler.go:2383`, and its reverse for the endpoints and metrics (GPT-R3-01); the internal-to-API mappings for `DragonflyRole` (`dragonfly_topology.go:550,567`) and `SiteState` (`runner.go:2448`) (F-21); the PITR verification-mode wire adapter in `backup_verification_job.go` (F-10); `""` removed from the A-28 status enums; `UpdatePhaseComplete` deleted or mapped to `""` in the snapshot (A-29); literal sweep across the controller, plugin, `internal/playground`, `playground/manifests` (the `role:` values in `failovergroup.yaml` and `failovergroup-3site.yaml`; F-48), doctor `triage.sh`/`gtid-audit.sh`; `internal/metrics` label-value functions; the role and state adapter for `/status` (`topology.go:1124-1140`) and `/ws/status` (`:2136-2140`); the playground dashboard's comparisons and CSS classes (`playground/dashboard/index.html:17-18,131-150`) | unit: enum round-trip; unit: every Dragonfly classifier value and every internal site state maps into the API enum (F-21); unit: a verification Job rendered for each `PointInTimeVerificationMode` feeds a mode the download planner accepts (F-10); unit: each API role taken through the topology configuration into `EvalCrossSite`, with a writable `PrimaryCandidate` left unfenced (GPT-R3-01); unit: every enum value of every enum-valued metric label (`role`, `state`, the keyring `phase`, the restore-reclone `phase`, the source `state`, `from`, `to`) maps to its documented value (OPUS-R3-06, F-17); unit: a snapshot taken between `setPhase(Complete)` and the reset carries no `updatePhase` (OPUS-R3-07); payload test: `/status` and `/ws/status` serialize every role and state as the CR value; envtest: old lowercase values and A-33's invalid reasons rejected | docs that show `primary-candidate` or `writable`; `3.monitoring-prometheus.md` and `2.monitoring.md` (PromQL, and the `/status` and `/ws/status` examples at `:543-651`), `examples/prometheusrule.yaml`; the `BloodravenRestoreReclonesHeld` alert and the restore-reclone metric docs (`2.monitoring.md:70,212-213`, `3.monitoring-prometheus.md:134`, `2.backup-restore.md:790`, doctor `SKILL.md:171`; F-17); doctor references; log-schema fields that carry state values; course (`primary-candidate`, `dr-only`) |
| E8 | Timers, timestamps and numeric types: the Duration rule, bounds, zero semantics and `stopTime` | A-35, A-36, A-37, A-38, A-41, A-34, A-39 (R11, OPUS-03) | types; `topology.go` (drop the 0 → 5m mapping); `site_helpers.go` (drop the 0 → 300 mapping); the profile `VerificationSpec` TTL mirror and its two creators (`trigger_verification.go`, plugin `verify.go`; F-11); retry and escrow readers; `restore_script.py` stop-time conversion and the verification Job's timestamp argument (A-39) | CRD test: reflects over the Go Duration fields and asserts both CEL bounds (OPUS-10); envtest bound checks; zero-value unit tests; an omitted `ttlSecondsAfterFinished` stays nil and an explicit 0 stays 0 through the scheduled trigger and through the plugin (F-11); envtest: `maxMemory` of `-1Gi` or `0` rejected, and a unit test pins its byte rendering (F-12); envtest: `stopTime` without a zone, with a lowercase `t` or `z`, with a fraction, or with the offsets `+25:00`, `+99:00` or `+05:99` rejected (OPUS-R3-03, F-02), and every admitted value decodes with `metav1.Time.UnmarshalJSON`, `mode: Timestamp` needs `stopTime`, `mode: Latest` forbids it; CRD test: both `stopTime` properties carry the RFC 3339 pattern | `crd-reference.md` timer table; `production-hardening.md`; PITR docs (`stopTime`); course (`stopDatetime`) |
| E9 | One retention shape | A-55 | `backup_types.go`, `mysqlbackupverification_types.go`; `resolveRetention`, `pruneSuccessful` and the verification sweep | table test of keep-if-any (`keepLast`, `keepWithinDays`, `keepFailed`, explicit 0 meaning keep none, an omitted field meaning the default); the documented mapping of a legacy explicit 0 to omission (F-26) | `2.backup-restore.md` retention section, with the `max(count, minKeep, 1)` mapping and the no-pruning note (R07); doctor playbook entries on retention |
| E10 | Status reshapes: replication state, encryption attribution, failover record, the `…Time` timestamp names, binlog coordinates, artifact identity, provenance; the durable-status markers | A-40 (including `requestTime`, `unsealTime` and `lastSeenTime`, revision 4), A-42, A-43, A-44, A-45, A-46, A-47, A-49, A-96, A-97, A-89 (markers and fail-safe test), A-114 (`replicationRunning`, `/ws/status` shapes) | status types; writers; `cmd/bloodraven/deploy_api.go` (`replicationRunning`); `internal/platform/websocket.go` `TopologyMessage` and its dashboard readers; `internal/dragonfly/info.go` field presence for `master_sync_in_progress` and `master_last_io_seconds_ago` (A-44, F-22); the sidecar's coverage probes and the `/keyring/status` wire types on both sides (`internal/sidecar/mysql.go`, `keyring.go`, `internal/mysql/sidecar_client.go`; A-46, F-13); the `Replicating` readers in `planned_failover.go`, `backup_reconciler.go` and `reconciler.go` (A-43, F-24); `playground/enable-encryption.sh` (A-47, F-25); restore and verification readers of `artifactStorage` (D-12); plugin `status.go`; s31, s37, s48 and s50; the `status.pitr.profileName` readers (`pitr_archive_condition.go:615`, `internal/playground/scenarios/backup_helpers.go:723`; A-49, F-39); `// Durable:` markers | unit: verification and restore use the recorded claim and endpoint; lease gate uses the `replication` enum; unit: A-43's producer mapping (`Unknown` while the healthy streak builds, `Lagging` above `maxLagSeconds`, `Broken` with a stopped thread, the gate refusing `Unknown`); deploy API: `replicationRunning` for each `replication` value; `/ws/status` carries `lastFailover{…}` and `sites[].replication`; unit: INFO without `master_sync_in_progress` leaves `syncInProgress` nil, INFO without `master_last_io_seconds_ago` leaves `lastIOSecondsAgo` nil, and a reported -1 stays -1 (F-22); unit: each coverage flag missing, false and true through the sidecar and the operator (F-13); unit: the planned-failover target check, backup source selection and the `healthy=yes` label each accept a `Lagging` site (F-24); component (`test/component`): the A-89 lost-status and fresh-deploy cases (R11) | doctor `triage.sh`, `SKILL.md`, `sidecar-probe.sh` (F-13), alert matrix (`sealed` → condition); the `/ws/status` meaning change in the release notes (F-23); `8.encryption-at-rest.md`; the MBK `Location` printer column moves here with A-96 (OPUS-26) |
| E11 | Honest generations, the condition vocabulary and the deploy-API token set | A-68, A-70, A-71, A-72, A-73, A-114 (`/deploy/v1`) | `runner.go` (snapshot config generation, advanced on the equal-config path too, F-28; `BootstrapFailed` in `degradedReason`, F-30); `backup_reconciler.go` `pendBackup` escalation (F-29); MBK, MBV and MSC `Ready` and their printer columns; chart ArgoCD Lua (`:66-75,77-87`); `cmd/bloodraven/deploy_api.go`; the bootstrap predicates in s10, s13, s19, s37, s59 and `repair_helpers.go` (F-31) | unit: conditions are stamped with the applied generation; a backup-schedule-only edit brings `observedGeneration` and `Ready.observedGeneration` to `metadata.generation` (F-28); a backup pending past 30m fails with `NoHealthySource` or `PendingTimedOut`, never a pending reason (F-29); `Degraded=True/BootstrapFailed` persists across polls, clears on recovery without a clone, and deployment leases are granted again (F-30); s10, s13, s19, s37 and s59 use the new completion and failure predicates (F-31); Lua test fixtures; course fixtures; deploy API test: every token field, `unstableReason` included, is drawn from the A-69 constants, and every value `DeploymentUnstableReason` returns is declared (F-32) | `crd-reference.md` conditions; `11.deployment-api.md`, with the closed value set of every token field and the `replicationRunning` definition (A-43); `gitops.md`; the ArgoCD Lua in the chart; `playground/chaos-scenarios.md:39,43,545,1479`, `9.failure-mode-matrix.md:51` and the course's bootstrap tokens (`topic-24-provisioning/read.md:287`, `SOURCES.md:228`; F-31) |
| E12 | Event reason naming and types | A-74, A-75, A-76 (generated docs table), A-73 (the `PITRArchiveReady` Event rename, F-34), A-114 (the lease revocation reasons, F-18) | Event call sites, including the six reasons added since `3fa1900` and every Event reason that survives #235, not only its completion Event (F-55); status writers and constants that share a renamed Event token, such as `status.restore.reason` (F-34); `operator_revoked`/`topology_changed` → `OperatorRevoked`/`TopologyChanged` in `deployment_leases.go`, the 409 body and s53 (F-18); s48 assertion | registry test (from E1) extended to doc coverage; unit: every metric `reason` label value written is a declared A-69 constant (A-114) | `2.monitoring.md` (generated), `7.log-schema.md` Event reference, doctor `SKILL.md`/alert matrix/playbooks; release-note rename table, including renamed metric `reason` values |
| E13 | Desired state out of annotations and status; no persisted sidecar default (the API shape) | A-48, A-51, A-52 (the field, the chart values and `default={}`; not pacing), A-53, A-98 | `dragonfly_upgrade.go` (no spec writes; the A-48 level-trigger rules), the Dragonfly StatefulSet renderer (live image kept under `SnapshotRestore`), encryption adoption gate, sidecar image env from the new chart values `sidecar.image.{repository, tag}` (chart `values.yaml`, `deployment.yaml`), MDB DDL, MBK finalizer | envtest: `SnapshotRestore` needs `snapshot.dir`, and `snapshot: {}` gets the rule's message, not an evaluation error; unit: a spec image that differs from the live StatefulSet drives the snapshot upgrade, an unknown live image does nothing (OPUS-18), and the operator never calls `Update` on spec; unit (F-03): a failed SAVE ends in one terminal run and no restart while `spec.dragonfly` is unchanged, a `spec.dragonfly` change re-arms it, a run blocked by a planned failover or a missing `activeSite` waits without the Rolling render moving any site, `sourceImage` comes from the live active site, and a run interrupted after one site resumes after an operator restart; chart test: the env renders from `sidecar.image.*`; a `Retain` backup keeps its artifact | `5.app-integration.md` (Dragonfly upgrade, including the two-step retry); `7.tenant-databases.md`; doctor; course unit 7 (`sidecarImage`); `e2e` label |
| E13b | Rollout pacing for release-driven pod-spec changes (PR3, not freeze-critical) | A-52 (R-1 to R-7) | designed in that PR against #244 | A-52 R-7: the real updater with a replica that stays unready; a combined render-version and sidecar-default change; a `Recreate` group; an operator restart mid-rollout | `10.upgrade-policy.md` (how operator upgrades roll unpinned groups, and that a render-version bump rolls pinned groups too); release notes; `e2e` label |
| E14 | One key prefix, boolean label values, namespace-scoped taint, the hold family, and action-annotation grammar | A-77, A-21, A-78, A-80 | every label, annotation and finalizer constant (E1 makes this a one-file change plus selectors), including the MFG finalizer rename, `encryption-status-fence`, and the verification hold (`backup_verification_hold.go`, `cmd/bloodraven/pitr_retention.go`); any proposal 21 label or restore hold already on WS-E's base, with its `<group name>/<group UID>` value kept (A-59, A-77; revision 4); `tainter.go` (delete `LegacyTaintKey`); the playground object store's tolerations (`playground/manifests/rustfs.yaml:58-66`), which otherwise lose their `NoExecute` toleration after the rename and get evicted on every failover (F-48); selector-mismatch refusal; chart NetworkPolicy; playground and chaos tooling; the doctor escrow helper's keys | envtest and unit: selector-mismatch condition and Event without a retry loop; unit: taint key format and length; the new hold key protects a backup under an active verification and under a Terminating one (retention, cleanup and PITR cutoff); A-78 parsing (both reclone forms with `confirm=` and `uid=`, a divergent form without `confirm=` rejected, the operator's `recloneAnnotationValue` output accepted (F-35), `gtidPrefix=`, values with `:` rejected, basic-form instants, a comma-separated id list, and an operation-id subject accepted without `confirm=`) | `12.well-known-metadata.md` (generated), `5.app-integration.md` tolerations, `architecture.md` labels, doctor scripts' selectors and hold queries (`triage.sh`, `support-bundle.sh`, `keyring-escrow-recover.sh`), `SKILL.md` and playbook 36; course (`shipstream.io/role`); `e2e` label |
| E15 | Divergent-promotion records and acknowledgement; GitOps-safe actions; plugin wrappers (PR5, not freeze-critical) | A-86, A-81 | `gtid_audit.go`, `runner.go`; plugin `ack-divergence`, `rotate-keyring`, `revoke-leases`; the playground reset lists (`cmd/playground-chaos/reset.go:365-370`) gain `divergentPromotions` and `divergentPromotionsOmitted` (F-37) | unit: acknowledging one record keeps the others; a 17th record increments `divergentPromotionsOmitted` and emits `DivergentPromotionRecordsTruncated` (OPUS-22); 17 promotions, 16 acknowledged: the condition stays True until `omitted` is acknowledged with `confirm=` and `uid=` (F-16, F-36); an id taken from another group's status clears nothing (F-14); two promotions of one site in the same second get distinct ids (F-36); 16 records with sets over 8 KiB stay under the size budget and carry `divergentGtidTruncated` (F-49); a set over 1 KiB is cut in the Event and log as documented (F-15); a playground wipe clears both new fields (F-37); plugin tests | `log-schema.md:169`, `crd-reference.md:709`, doctor playbook `troubleshooting_playbooks.md:931-937` (including the step to capture `gtid_executed` before a reclone, F-15), `gitops.md` |
| E16 | Admission validation for my.cnf inputs, cross-references and bounds; kind metadata; doc comments | A-90, A-91, A-92, A-93, A-94, A-95 | types; the D-03 controller fix | envtest: injection strings rejected, dangling profile references rejected, CEL cost passes; `bytesPerChunk: 512k` and `maxBinlogSize: 512m` admitted and `bytesPerChunk: 64m` rejected (F-60); `--replicaof` in `dragonfly.args` rejected (F-38) | `crd-reference.md` |
| E17 | Hardened security-context defaults, gated (PR4, not freeze-critical) | A-54 | one merge helper for MySQL, Dragonfly and Jobs; uid and gid 999; emptyDirs for `/tmp` and `/var/run/mysqld` | the Q6 gate: Restricted-PSS admission, fresh init, existing-volume restart, keyring, PITR, backup and restore, chaos failover, both images, local-path plus one CSI class. If it fails, ship option C and record why. | `production-hardening.md` (drop the FSGroup upgrade procedure); doctor; `e2e` label |
| E18 | Runbooks and operator messages without status writes (PR5, not freeze-critical) | A-83 (rule only; the patch is already gone), A-84, A-85, A-87, A-88, A-89 (docs) | docs and doctor; the `backup_types.go:901-913` comment; the two operator messages (`runner.go:2149-2152`, `internal/state/matrix.go:219-221`; OPUS-02) | unit: neither message contains `--subresource=status`. This is the existing `TestRunner_PendingRecloneRefusalNamesWorkingRemedy` (`internal/controller/restore_reclones_test.go:952-961`) inverted: today it fails unless the `PromotionRefusedPendingReclone` Event contains the token (`:958`), so E18 must change it, not only add a test (OPUS-R3-09). Add the same assertion for the `Degraded=NoPrimary` message. The lint below runs in CI, with its self-test (section 4; GPT-R3-04), and the self-test covers both `--subresource=status` and `--subresource status` (F-67) | `4.runbooks.md`, `8.encryption-at-rest.md` (escrow and keyring-rollback break-glass labels), `2.backup-restore.md`, `2.monitoring.md`, `7.tenant-databases.md`, `7.log-schema.md:268` (checked; `msg` unchanged), `chaos-scenarios.md`, `3.playground.md` (A-88's prerequisite per pass, F-59), doctor `troubleshooting_playbooks.md` and `alert_runbook_matrix.md`; a lint that fails on `--subresource=status` and `--subresource status` outside break-glass blocks in `site/` and `.agents/` (it checks command tokens only; prose status-write instructions are a review item, F-67), and in string literals in production Go code under `internal/` and `cmd/` (excluding `_test.go`, fixtures, the lint's own test inputs and the playground tooling; section 4) |
| E19 | Course sweep, the U-1 warnings, and the old-token gate (PR5, not freeze-critical) | OPUS-06 (no finding of its own); 3.2 U-1 | `course/`; the rebuilt `site/public/courses`; `site/app/` (`HomeFailoverStage.vue:73`, OPUS-R3-10); `10.upgrade-policy.md`, `11.gitops.md`, `5.production-install-examples.md` (F-62) and course unit 7 get the "not for v1.6.0 → v1.7.0" warning (C-12) | **A context-aware CI gate (revision 3; GPT-R2-03, OPUS-R2-04).** Revision 2 grepped bare substrings, so it failed on a correct tree: `operatorSecret` is a prefix of `operatorSecretName`, `shipstream.io/role` is inside `bloodraven.shipstream.io/role`, and A-114 keeps `primary-candidate` as a metric value. The gate scans `course/`, `site/content/`, `site/app/` (revision 4, OPUS-R3-10), `.agents/`, `examples/` and `playground/` manifests and scripts (revision 5, F-48), case-sensitively, matching whole tokens in context only: (1) obsolete field names as YAML or JSON keys or as path segments, a token with no word character on either side followed by `:` or preceded by `.` (`lbIP`, `sidecarImage`, `sidecarResources`, `operatorSecret`, `groupRef`, `stopDatetime`, `sourceSiteOverride`, `mysqlBackupRef`, `maxMemoryMb`, … from the 3.4 table); (2) exact old metadata keys not preceded by a DNS-label character or `.`, so `bloodraven.shipstream.io/…` and the API group `shipstream.io/v1alpha1` never match (`shipstream.io/{role,site,failover-group,healthy,graceful-shutdown,db-readonly…}`, `hold.verification.shipstream.io/`); (3) obsolete CR enum values only as a YAML or JSON field value (`role: primary-candidate`, `"state": "writable"`, `mode: timestamp`, a scalar `updateStrategy:`), plus `healthy=yes\|no`, `dragonfly-traffic=enabled`, and `dr-only` anywhere; (4) the A-74 old Event reasons as whole tokens; (5) (revision 4, OPUS-R3-08) any `raw.githubusercontent.com/ShipStream/bloodraven/main/` URL that points at `config/crd` or `charts/*/crds`, in those paths and in `README.md`. **Allowed:** PromQL label selectors (`role="…"`, `role=~"…"`, `state="…"`), whose kebab-case values A-114 keeps; metric `reason` values, which are the A-69 constants; and the migration mapping on the C-12 upgrade page, inside a marked allow block. Unit test of the gate: one old and one new sample per pattern (`operatorSecret:` fails and `operatorSecretName:` passes; `shipstream.io/role` fails and `bloodraven.shipstream.io/role` passes; `role: primary-candidate` fails and `{role="primary-candidate"}` passes; `…/bloodraven/main/config/crd/…` fails and `…/bloodraven/${BLOODRAVEN_VERSION}/config/crd/…` passes). | the course; `make course` |

After PR2 (the last freeze-critical PR):

- Regenerate the inventory appendix.
- Hand the cleaned schema to Barry for the `v1.7.0-rc.0` cut, once C-8 is signed off and the `release.yml`
  pre-release rule has merged (C-8).

After PR5:

- File the follow-up issues: A-84 re-escrow and the keyring-version rollback action, the D-01…D-14 defects that
  WS-E does not fix itself, and the #204 scope addition (`resume`, and the UID binding of both actions).
- Regenerate the inventory appendix again if PR3 to PR5 changed the schema.

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
| Q5 | Baseline for the D6 upgrade E2E | **A**: `v1.7.0-rc.0`, cut after WS-E (v1alpha1 only, cleaned schema). The E2E goes rc.0 → v1beta1 build → `migrate-storage` → `storedVersions == [v1beta1]`, health and failover. | Round 2, 3-0 (GPT moved from B) | B: a v1.6.0 baseline with a manual-path E2E. C: `migrate-storage` rewrites old field names (a D2/D3 shim) | `migrate-storage` first runs a server-side `dryRun=All` update of every object and refuses before any write, listing each failing object with a migration-table pointer (generic, no legacy names). Envtest pins this with the v1.6.0 CRDs (refusal list, unchanged `resourceVersion`s). v1.6.0 → v1.7.0 is documented as: export manifests, re-apply, never delete the MFG, recreate workloads per Q4, or a fresh install plus logical restore. (Revision 2: the in-place half is impossible (3.1), so only the second half remains, as the new-group procedure, whose requirements are now 3.2. The baseline amends D6 and needs sign-off; see C-8. Revision 5: a no-op dry-run update does not refuse what ratcheting downgrades (F-07), so the refusal condition stands but its mechanism is now requirement U-10, which #243 designs.) | C-7, C-8, section 3 |
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
| A-16 | Site removal and rename refused; replacement by its own same-cluster procedure (revision 5, F-19) | Allow, and implement pruning now; pointing at the v1.6.0 procedure | Settled |
| A-17 | Site storage class, size and zone immutable | Grow-only size rule (expressible at the 1.31 floor, but PVC expansion is not implemented) | Settled |
| A-18 | Backup PVC `claimName \| provisioned` union; presence-checked parent rule plus a reconcile-time `BackupStorageMismatch` whose comparison is defined (nil class matches any; size against the PVC request; larger accepted; revision 3); shared `ProvisionedVolumeSpec` with #205 | CEL forbidding size and class next to `claimName` only; a field-level `self == oldSelf`; a literal or `status.capacity` compare | Settled |
| A-19 | `loadBalancerIP` with `Format=ipv4` | CEL `isIP()` (available at the floor, but it adds CEL cost) | Settled |
| A-20 | `sourceSite`/`targetSite`/`site` naming rule | `sourceSiteOverride` | Settled |
| A-21 | Taint `<ns>.db-readonly.bloodraven.shipstream.io/<group>`; no legacy key | Hashed key (GPT); keep stripping the legacy key | Settled and MBOD Q4 |
| A-22 | MBK/MBV execution fields immutable; cleanup knobs mutable | Defend against edits in the controller | Settled |
| A-23 | `initFromBackup` fully fixed at creation, compared by value (T-1); a never-bootstrapped group is deleted and recreated to fix a failed bootstrap (revision 5, F-20) | Allow edit and removal, forbid only adding (MFGSPEC); allowing edits while status reports a failure | Settled |
| A-24 | Encryption disable refused; keyring paths immutable while enabled | Toggle back and forth | Settled |
| A-25 | MFG transition rules on the kind root | Rules on the shared spec type | Settled |
| A-26 | `confirm` stays a string with `Format=date-time` | `metav1.Time` (breaks token byte equality) | Settled |
| A-27 | CamelCase typed enums; role `DisasterRecovery`; drop `none`; `updateStrategy` object; an exhaustive adapter between the API and internal `state.SiteRole` (revision 4) | `DROnly`; keep `None`; scalar `updateStrategy`; keeping the cast, which fences the writable primary | Settled |
| A-28, A-29 | CamelCase typed status enums with no `""` value (revision 4); `UpdatePhase` in API without `Complete`, which E7 stops persisting (revision 4) | Keep lowercase to match MySQL vocabulary; keep `""` as a second spelling of absence | Settled |
| A-30 | Structured status scope; spec keeps inferring from `includeSchemas`; #205 adds no `kind`, and a PITR run has `provenance.pointInTime` (revision 4) | An explicit spec `scope` (GPT); adopting `kind` `Full\|Schema\|PITR` | Settled |
| A-31, A-32 | Drop unused MBV phases; MDB `Creating` → `Applying` | Freeze never-entered values | Settled |
| A-33 | Typed reason strings with constants, plus reason pattern, length and `sourceConvergenceReason` enum; breaking, status-only (revision 3) | Free strings | Settled |
| A-34 | `sanityCheck{query, minResult, timeout}` and an `outcome` enum | Keep `expect` wrapper and `ran bool` | Settled |
| A-35 | Duration rule | Integer seconds everywhere | MBOD Q2 |
| A-36 | No Go `int` in the API | Add a format only | Settled |
| A-37 | Pointers where zero is valid; no zero-to-default mapping; the MBV TTL's nil-keeps-until-retention is a separate lifecycle decision, applied to the profile mirror too (revision 5, F-11, F-54) | Non-pointer with an `omitempty` default | Settled |
| A-38 | Lower and upper bounds on every Duration; CRD test; cooldown ≥ 30s | Lower bounds only; allow a zero cooldown | MBOD Q2, settled (cooldown) |
| A-39 | `stopTime` as `metav1.Time` everywhere, with an RFC 3339 OpenAPI pattern on every user-written time (revision 4) that bounds the offset and admits no fraction (revision 5, F-02, F-01) | Keep `stopDatetime` with its MySQL form; `format: date-time` alone; a CEL `matches()` (unavailable on a timestamp) | Settled |
| A-40 | `lastFailover{time,target,promotionGtidExecuted}` struct; `*Time` names on every status timestamp, including `requestTime`, `unsealTime` and `lastSeenTime` (revision 4) | Rename to `lastFailoverTime` only; applying the rule to new fields only | Settled |
| A-41, A-42 | `maxMemory` Quantity; shared `BinlogCoordinate` | Keep `maxMemoryMb`; keep two coordinate shapes | Settled |
| A-43 | `replication` enum; `/deploy/v1` `replicationRunning` keeps its name, defined as `Healthy` or `Lagging` on every non-active site (revision 3); `Healthy`/`Lagging` only after the debounce streak, `Lagging` above `maxLagSeconds` (revision 4) | `replicationHealthy *bool` | Settled |
| A-44, A-46 | `*bool` for observed flags | Fill unknowns with `false` | Settled |
| A-45 | `status.encryption{mode, algorithm}`, extended by #203 | `encrypted *bool` | Settled |
| A-47 | Remove `encryptionAtRest.sealed` | Make it `*bool`; keep it | Settled |
| A-48 | `spec.dragonfly.upgradeStrategy`; the operator never writes spec; a terminal run is not restarted until `spec.dragonfly` changes, the ordinary render keeps the live image under `SnapshotRestore`, and the source image is live (revision 5, F-03) | Annotation carrying the image; a retry annotation; re-arming on any generation change | Settled |
| A-49 | Remove `status.pitr.enabled`, `status.pitr.profileName` (revision 5, F-39, which withdraws A-111) and `status.dragonfly.enabled` | Keep the echoes; keep `profileName` as an attribution | Settled |
| A-50 | #235 owns semantics and record structure; WS-C lands the shape against the A-50 checklist, and WS-E's first step re-audits and fixes any gap (revision 3) | "WS-E does not touch this block" (revision 2), which left the must-fix with no owner; WS-E redesigning the claim | Settled (revision 3; orchestrator ruling on OPUS-R2-03) |
| A-51 | `spec.encryptionAtRest.existingData` | Keep the persistent `encryption-adopt` annotation | Settled |
| A-52 | `spec.sidecar.image` with no default (operator-built default from chart values `sidecar.image.{repository, tag}`); `spec.sidecar` stays a value type with `default={}`. Rollout pacing is runtime behaviour: revision 5 removes the queue design and states requirements R-1 to R-7 for WS-E PR3 and #244, which gate v1.7.0, not rc.0 | A persisted CRD default bumped every release; a one-shot `roll-sidecar` action; a required pin; the chart key `sidecarImage.*`; designing the queue inside this audit a fourth time | Settled (revision 5; orchestrator ruling on F-04, F-05, F-06) |
| A-53 | No collation default; breaking (revision 2) | `utf8mb4_0900_ai_ci` default | Settled |
| A-54 | Hardened merged defaults, uid 999, behind a gate; fallback C | As-is semantics (B); C without trying | MBOD Q6 |
| A-55 | `retention{keepLast, keepWithinDays, keepFailed}` on both kinds; migration `keepLast = max(count, minKeep, 1)`; a legacy explicit 0 that means the default migrates to omission (revision 5, F-26); no-pruning has no equivalent | `count`/`minKeep`/`maxFailedKeep`; keep the legacy integer; a finite default silently replacing no-pruning | Settled |
| A-56 to A-61 | Remove inert or unimplemented fields, values and status | Keep them "for compatibility" | Settled |
| A-59 | Proposal 21's planned API is checked against the A-59 table: keys, the hold, `…Time`, typed reasons, no `""`, the A-71 and A-73 sets, no `kind`. PR1-PR3 on WS-E's base go through step 0 (revision 4) | "#205 lands after the freeze and needs four renames" (revision 3) | Settled (revision 4) |
| A-56 | Remove `s3.storageClass` | Wire it to mysqlsh before the freeze | Settled |
| A-62 | Remove MSC `spec.template` | Keep it with CEL excluding activation-owned fields (KINDS) | Settled |
| A-63, A-64 | `source.s3`; structured discovery status | The `BackupStorage` union with a forbidden PVC arm; flat fields | Settled |
| A-65 | `triggeredBy` becomes an annotation | `spec.origin` (OPS) | Settled |
| A-66, A-67 | `operatorEndpoint`; `s3EndpointURL` replacing `s3Endpoint` and `s3UseHTTPS`, adapted to Dragonfly's `host:port` and HTTPS flags | Keep the old names; rename `s3Endpoint` without an adapter | Settled |
| A-68 | `status.observedGeneration` from the applied config generation | The fetched object's generation | Settled |
| A-69, A-70 | Constants in `api/`; `Ready` on every kind now | Defer `Ready` (KINDS) | Settled |
| A-71 to A-73 | Specific `Ready` reasons; polarity fixes; one meaning per token | `TopologyPolled` for both values | Settled |
| A-74, A-75 | Full Event rename table and one type per reason; the rule allows a trailing cause qualifier, and every target has a verb (revision 5, F-33, F-57) | Fix only within-family inconsistencies | Settled |
| A-76 | Generated Event registry with a test | Hand-maintained page | Settled |
| A-77 | One `bloodraven.shipstream.io/` prefix; `"true"`/`"false"` values; hold family `<kind>-hold.bloodraven.shipstream.io/<id>`, with only the prefix changing and the restore-hold value staying `<group name>/<group UID>` (revision 4); MFG finalizer → `failover-group` | Documented split; status quo; dual-key reads for in-flight holds | MBOD Q4; hold and finalizer settled in revision 2 |
| A-78, A-82 | Annotation grammar `<subject>(:k=v)*` with values restricted to `[A-Za-z0-9._/+-]`, basic-form instants, `uid=` binding; both reclone forms carry `confirm=` and `uid=` (revision 5, F-35); an operator-minted operation-id subject satisfies `confirm=` and `uid=`, and a subject may be a comma-separated id list (revision 4); monotonic spec tokens | Positional reclone prefix; `;` as separator (a shell metacharacter) | Settled |
| A-79, A-80 | Exported keys, a generated metadata page, versioned payloads | Copied strings | Settled |
| A-83 | Delete the `Sealed` patch; rely on `KeyringRecovered` | A typed recovery request (GPT) | Settled |
| A-84 | Escrow forging is break-glass; follow-up issue | Build re-escrow before v1beta1 | Settled |
| A-85 | Reclone patches are break-glass until #204, plus `resume` added to its scope | Pull #204 into v1.7.0 | Settled |
| A-86 | `divergentPromotions[]` keyed by a UUID per promotion, 8 KiB per GTID set, a durable `divergentPromotionsOmitted` count with its own bound acknowledgement, and a per-id acknowledgement annotation (revision 5) | A condition-timestamp acknowledgement (STATUS); a time-and-site id; an overflow count kept only in the message | Settled |
| A-87, A-88 | Delete the MDB patch advice; playground resets are break-glass | Keep them documented as supported | Settled |
| A-89 | Durable status stays in status, with a written contract | Move it to annotations or a ConfigMap | Settled |
| A-90 to A-92 | Admission patterns, cross-reference CEL, bounds | Reconcile-time checks only | Settled |
| A-93 to A-95 | `mbk`/`mbv` short names; new printer paths; no line numbers in doc comments | — | Settled |
| A-96 | Fold `storageType` into #232's `artifactStorage`; every consumer reads it | A tagged sub-union; keep the top-level duplicates | Settled |
| A-97 | Shared `provenance` struct, after #206 | Three copies of the fields | Settled |
| A-98 | MBK `deletionPolicy`, default `Delete` | Always delete artifacts | Settled |
| A-99 to A-113 | Leave (section 2.18), except A-111, withdrawn into A-49 in revision 5 | See each row | Settled |
| A-114 | Metric label values are the kebab-case form of the API enums (`dr-only` changes, and in revision 5 the restore-reclone `phase`, F-17), lease revocation reasons become `OperatorRevoked`/`TopologyChanged` (F-18), produced by one mapping function for every enum-valued label, including `phase`, the source `state`, `from` and `to` (revision 4); metric `reason` labels are the A-69 constants; `/deploy/v1`, `/status` and `/ws/status` carry the CR values and shapes (revision 3), documented | Raw CamelCase metric values; freezing `dr-only`; lowercase or kebab-case on the JSON endpoints | Settled (revision 2; extended in revision 3) |
| A-115 | Kubernetes floor 1.31, tested by a CI envtest job | Keep 1.27 with a 1.27 job; an untested floor | Settled (revision 2) |
| T-1 (2.4) | Transition rules compare values, not encodings: Quantity, Duration and timestamps by value, omitted zeros substituted, whole-object equality only on string-only sub-structs; tested against the operator's own typed writes in E6 (revision 5, F-01) | Canonical forms at admission; metadata-only operator patches | Settled (revision 5) |
| A-115 (ratcheting) | Ratcheting downgrades every schema error on an unchanged node; 1.31 and 1.32 do not ratchet spec CEL on status writes, so every later spec CEL tightening ships a 1.31 status-write test (revision 5, F-07, F-40) | Raising the floor to 1.34 | Settled (revision 5) |
| Section 6 | WS-E as five PRs; only the freeze-critical PR1 and PR2 gate WS-F and rc.0; A-52's pacing (PR3) and A-54's gate (PR4) are their own `e2e` PRs; A-86, A-81, E18 and E19 in PR5 (revision 5) | One 19-commit branch | Settled (revision 5; orchestrator ruling on merits point 1) |
| Section 3 | No in-place v1.6.0 → v1.7.0 upgrade; a new group restored from a backup, with a per-field classification of what is dropped. Revision 3 replaces the step-by-step with requirements U-1 to U-9 that WS-F's written and tested procedure must meet. Revision 4 tracks the procedure as a separate WS-F follow-up issue (#243) after a third round of Major findings. Revision 5 states only what is true about ratcheting and `storedVersions`, rewrites U-2, and moves the object-check mechanism into requirements U-10 to U-13 for #243 | The first draft's export/re-apply path; patching the steps a third time; reviewing the procedure inside this proposal again | Settled (revision 2; orchestrator rulings on R02, R03, OPUS-01, and in revisions 3 and 4 the scope rule) |
| C-7 | `migrate-storage` rewrites every object and trims `storedVersions`; its refusal of non-conforming objects is a requirement (U-10 to U-13) that WS-F meets under #243; the v1.6.0 refusal envtest stays (revision 5) | Rewriting legacy fields; a no-op dry-run update as the refusal, which ratcheting defeats (F-07) | MBOD Q5; revision 5 scope rule |
| C-8 | **Proposed amendment to D6:** `v1.7.0-rc.0` baseline, without moving `latest`. Until sign-off WS-F does not start the upgrade E2E (revision 3) | D6 as written (v1.6.0 baseline), which conflicts with D2 | MBOD Q5; **pending Barry and Colin sign-off** |
| C-14 | Requirement for WS-F: the operator refuses CRDs older than its build, warns on newer ones, and runs the U-10 to U-12 object check before its first write; WS-F designs the mechanism under #243 (revision 5) | An exact live-schema digest, which takes the failover controller down between "CRDs first" and "then the chart" (merits point 2, F-46); trusting an annotation (revision 3, GPT-R3-02); "serves `v1beta1`" only | Settled (revision 5; orchestrator ruling on merits point 2) |

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

**Round 3** critiqued revision 3 (`b543405`). The points are cited as GPT-R3-nn and OPUS-R3-nn:

- GPT 6.1 Sol: 4 Major (GPT-R3-01 to GPT-R3-04).
- Claude Opus 5.5: 5 Major and 5 Minor (OPUS-R3-01 to OPUS-R3-10).
- Overlaps: GPT-R3-03 with OPUS-R3-01 (the restore-hold value), and GPT-R3-04 with OPUS-R3-09 (the lint scope).

Every point was checked at `b543405`: the cited code and docs, proposal 21, issues #223, #240 and #241, and the
apiserver's date-time check in `k8s.io/kube-openapi`. None was wrong on its main claim, and none was rejected.

What changed:

- **The v1.6.0 → v1.7.0 procedure is split out (scope rule).** OPUS-R3-08 is the third consecutive round with a
  Major finding in this area: R02, R03 and OPUS-01 in round 1; OPUS-R2-02, GPT-R2-02 and OPUS-R2-07 in round 2;
  OPUS-R3-08 in round 3. Under the program's three-rounds rule, the procedure is no longer reviewed as part of this
  proposal. It is tracked as a separate follow-up issue owned by WS-F (#243), and 3.2 and C-16 say so.
  3.2 stays a list of requirements. Revision 4 adds only OPUS-R3-08's route to U-1: the production install page
  applies CRDs from `main`, so the new schema reaches v1.6.0 clusters as soon as WS-E merges. C-12 and E1 pin
  that URL to a release tag in WS-E's first commit, and E19's gate fails on any `main`-branch CRD URL.
- **Proposal 21** (OPUS-R3-01, OPUS-R3-02, GPT-R3-03). A-59 now has a table of proposal 21's planned fields that
  conflict with the freeze rules, each with the shape to use instead, and A-59 checks every row. It also records
  that PR1-PR3 (PR1 is #241) may land before WS-E, and that #240 and #223 add nothing to the API. Other changes:
  - A-30 decides that #205 adds no `kind`.
  - A-77 keeps the restore-hold value `<group name>/<group UID>`.
  - A-71 adds `RestoreDamagedPrimary`.
  - A-78 accepts an operator-minted operation-id subject in place of `confirm=`.
  - Section 6 gains the PR1-PR3 precondition, and C-10 the ordering.
  - A-28 removes `""` from the eight status enums that keep it, which answers OPUS-R3-01's phase point for every
    enum, not only the two it named.
- **Startup and decoding safety.** C-14 digests the live served schema, and the annotation becomes diagnostic only
  (GPT-R3-02). The object preflight uses the dynamic client (OPUS-R3-10). User-written times carry an RFC 3339
  pattern, because `format: date-time` admits values that `metav1.Time` cannot decode (OPUS-R3-03).
- **The internal role boundary** (GPT-R3-01). A-27 and E7 replace the `state.SiteRole` cast with adapters in both
  directions, and test that a writable `PrimaryCandidate` is not fenced.
- **Commit plan.**
  - E6 adds the bounds its whole-block rules need (OPUS-R3-05), and section 6 now allows bounds on #206-owned
    fields.
  - E18 inverts the existing assertion at `restore_reclones_test.go:958`. The status-write lint covers production
    Go only and has a self-test (GPT-R3-04, OPUS-R3-09).
  - The producer mappings for `updatePhase`, `replication` and `unstableReason` are stated (A-29, A-43, A-73;
    OPUS-R3-07).
- **Conventions.** A-40 renames `requestedAt`, `unsealedSince` and `lastSeen` (OPUS-R3-04). A-114 and section 1.5
  add the `phase`, source `state` and `from`/`to` metric labels and require one mapping function per enum-valued
  label (OPUS-R3-06).
- **Small fixes** (OPUS-R3-10):
  - `serverID` widened to `int64` in the controller (A-08, E4);
  - A-48's `has()` guard;
  - A-78's comma-separated id list for A-86;
  - `site/app/` in C-12 and E19;
  - C-14's dynamic client.
- **Citations corrected:** OPUS-R3-01's line numbers in proposal 21 were off by a few lines. The A-59 table cites
  the right ones: `:173` (phase enum), `:200` and `:222` (`…Since`), `:229`, `:328` and `:376` (reasons), and
  `:327`, `:338`, `:367` and `:371` (`…At` and `since`).
- **Counts:** findings unchanged at 115 (49 must, 50 should, 16 leave); breaking unchanged at 81; migration rows
  106 → 108.

Suggested fix not taken. The point itself was accepted:

| Point | Suggested fix not taken | Why |
|---|---|---|
| OPUS-R3-05 | `excludeGtids` `MaxLength=65536` | Proposal 21 accepts a set of up to 96 KiB in canonical form at reconcile time, so a 64 KiB admission bound would refuse what #205 accepts. `MaxLength=131072` is the Linux limit on one argument, which no longer value could pass. It bounds the CEL cost and leaves #205's semantic limit in place. |

**Ultra review** reviewed revision 4 (`a5f155d`, PR #242) with two model families: GPT 6.1 Sol and Claude Opus 5.5.
A third family was dropped after its slots produced no output. The findings are cited here as F-01 to F-68.

- **Counts:** 185 raw candidates, cross-validated by the other model family where it could run: 131 confirmed,
  deduplicated by root cause into 68 findings (0 critical, 8 high, 40 medium, 20 low), and 54 rejected on
  validation. The review's own
  rejections are recorded so they are not re-raised. Among them: A-82 confirm-token replay on lost status;
  U-8's backups under a foreground delete; `managedFields` size; A-18's `claimName`/`provisioned` switch; the
  unusable-exit variant of A-16 (confirmed separately as F-19); A-90's key-rule cost; C-14's description-only
  refusals (handled as merits point 2); A-92's Abseil flag spellings (the `--replicaof` omission was confirmed as
  F-38); E8's `maxLength` test; A-53's collation migration; A-48's scalar enum; and D-06.
- **Each confirmed finding was checked against the proposal text at `a5f155d` before it was applied**, and where
  the cited code decides the fix, that code was re-read (among others F-03 to F-06, F-10, F-11, F-17, F-18,
  F-21 to F-25, F-35, F-38, F-39, F-52 and kube-openapi's date-time check for F-02). None was wrong on its main
  claim, so none is rejected. The highs F-02 and F-07 and the
  medium F-40 rest on envtest measurements the validators made on 1.31, 1.32, 1.34 and 1.37; their results are
  restated in A-39 and A-115.
- **Merits verdict: sound with reservations (both models).** Both accept the problem framing and the load-bearing
  freeze decisions: an explicit immutable `serverID`, bounded Duration timers, `xxxSecretName`/`xxxSecretKeyRef`,
  one key prefix, the tested 1.31 floor, and the clean-break new-group path (still pending D6 sign-off, C-8). Their
  reservations, and what revision 5 did with each:
  1. **Split WS-E and separate post-freeze behaviour** (both models). Section 6 now delivers WS-E as five PRs and
     marks every commit freeze-critical or not. Only PR1 (renames and reshapes) and PR2 (validation and
     immutability) gate WS-F and rc.0. A-52's rollout pacing (PR3) and A-54's gate (PR4) are their own `e2e` PRs,
     and A-86, A-81, E18 and E19 are in PR5.
  2. **C-14's exact digest is a permanent availability coupling** (both models). C-14 is now a requirement: refuse
     CRDs older than the build, warn on newer ones, and never take the failover controller down in the "CRDs
     first, then the chart" window. The mechanism is WS-F's, under #243.
  3. **Make the normative text unambiguous** (Opus). "How to read this" now declares section 2 and the 3.4 table
     normative, with section 2 winning over sections 6 to 8. Not taken in full: moving the roughly 84 inline
     revision markers into 8.1, which would rewrite every finding between review rounds; the markers are declared
     provenance only instead.
- **Narrowing (program scope rule).** Two areas drew Major findings in every round, so they were narrowed rather
  than patched again:
  - **A-52's rollout queue** (OPUS-07, OPUS-R2-09, then F-04, F-05, F-06). The queue design is removed. A-52 keeps
    the frozen API part (optional `spec.sidecar.image`, no persisted default) and states requirements R-1 to R-7
    for the rollout-pacing PR and its follow-up issue, #244. The C-8 E2E no longer asserts that a
    pinned group never rolls.
  - **Migration mechanics** (round 1 R02, R03, OPUS-01; round 2 OPUS-R2-02, OPUS-R2-05, OPUS-R2-11; round 3
    GPT-R3-02, OPUS-R3-10; then F-07, F-08 and merits point 2). 3.1 item 2, A-115, C-7 and C-14 now state only
    what is true: ratcheting downgrades type and `required` errors on unchanged nodes, so a no-op dry-run update is
    not a reliable refusal, and `storedVersions` rules out re-applying the v1.6.0 CRDs. U-2 is rewritten. The
    remaining mechanism, the object check, its coverage, conflicts and RBAC (F-43, F-44, F-68), is requirements
    U-10 to U-13 for #243, and this audit no longer designs it.
- **What else changed:**
  - **Transition rules and timestamps.** House rule T-1 (2.4) makes every transition rule compare values, not
    encodings, so the operator's own re-encoded Quantity, Duration, `metav1.Time` and omitted zeros pass (F-01).
    It is applied to A-17, A-18, A-22 and A-23 and tested in E6. A-39's pattern bounds the offset and drops the
    fraction (F-02).
  - **Runtime semantics the API change creates:** A-48's level-trigger rules for the Dragonfly snapshot upgrade
    (F-03); A-23's failed-bootstrap remedy (F-20); A-72's `BootstrapFailed` clearing rule (F-30); A-68's applied
    generation on the equal-config path (F-28); A-73's timeout reasons (F-29).
  - **Producer and reader mappings:** the PITR mode wire (F-10), the Dragonfly role and site state casts (F-21),
    every `Replicating` reader (F-24), the `/ws/status` meaning change (F-23), presence for `lastIOSecondsAgo`
    (F-22) and for the sidecar's coverage flags (F-13), `enable-encryption.sh` (F-25), the TTL mirror (F-11) and
    the plugin's label literals (F-52).
  - **A-86's ledger:** a UUID id (F-14), an 8 KiB bound per set (F-49), a durable omitted count with its own
    acknowledgement (F-16, F-36), an honest statement of what Events and logs keep (F-15), and the playground
    reset (F-37).
  - **Served tokens:** the restore-reclone metric `phase` (F-17) and the lease revocation reasons (F-18), each
    with a 3.4 row; the Event rename table follows its own rule (F-33, F-57), includes A-73's rename (F-34), and
    covers every reason that survives #235 (F-55).
  - **Validation:** `maxMemory` sign (F-12), the empty `s3EndpointURL` (F-50), per-field size suffixes (F-60),
    `--replicaof` (F-38), the A-90 cost explanation (F-61), and the retention zeros (F-26).
  - **Sequencing and docs:** the CRD URL pin and a release-built site land before any incompatible CRD change
    reaches `main` (F-41, F-42); `release.yml`'s pre-release rule is Barry's rc.0 precondition (F-45); the re-triage
    reviews the whole repository (F-47); `playground/` joins the sweeps and the E19 gate (F-48); a post-freeze
    field lands in every served version (F-27); A-16 has its own site-replacement procedure (F-19).
  - **Corrections:** A-111's leave is withdrawn into A-49 (F-39); A-37's TTL is a lifecycle decision (F-54); the
    A-43 problem statement (F-23); counts and citations (F-53, F-56, F-58, F-65, F-66); the dump contents (F-64);
    the `excludeSchemas` bound (F-63); the production install examples page (F-62); the lint's real scope (F-67);
    the 1.31 status-write exception (F-40).
  - **Generator** (F-09, F-51). `crd-inventory.py` now matches its docstring: it collapses a resources block by
    shape (`sidecarResources`), types int-or-string map values, prints the bounds on list items and map values and
    the root `required`, and refuses served versions whose schemas differ. The appendix was regenerated; the MFG
    spec count drops from 267 to 262.
- **Counts:** findings unchanged at 115 IDs (49 must, 50 should, 15 leave; A-111 withdrawn); breaking unchanged at
  81; migration rows 108 → 110.

Suggested fixes not taken. The finding itself was accepted in every case:

| Finding | Suggested fix not taken | Why |
|---|---|---|
| F-02 | Keep the fraction in A-39's pattern and bound only the offset | `metav1.Time` encodes at second precision, so an admitted fraction would be lost on the operator's first write and would trip A-23's transition rule (F-01). The pattern rejects it instead. |
| F-07 | Build the C-7 and C-14 refusal from a `dryRun=All` create of each object's copy | The orchestrator's scope ruling: this audit states what is true and the requirements (U-10 to U-13); #243 designs the check. |
| F-08 | A recovery CRD that serves both versions with the old schema and v1alpha1 storage | Same ruling. U-2 states when recovery of the stored objects is possible at all, and #243 designs the steps. |
| F-18 | Declare `operator_revoked` and `topology_changed` permanent exceptions | That freezes a second reason spelling into `/deploy/v1` and the metric. They are renamed to the A-69 constants with a 3.4 row instead. |
| F-46 | Keep the exact digest and document the forward-upgrade window | The orchestrator's ruling on merits point 2: the window must not take the failover controller down, so newer CRDs warn. |
| F-50 | Reject an explicit empty `s3EndpointURL` | Templates that render `""` are common, and the field already means AWS S3 when empty. The pattern admits it. |

## Appendix A. Code defects found during the audit

These are code or docs bugs with no API-shape change. File each as an issue; WS-E fixes those its commits
already touch.

| ID | Defect | Evidence | Recommendation |
|---|---|---|---|
| D-01 (STATUS-20) | `Degraded` gets a new `lastTransitionTime` on every poll while a replication overlay applies. That causes a status write every poll and destroys "degraded since". | The topology reason is written first (`internal/controller/runner.go:1511-1518`), then overlays set it again (`:1539-1617`). `setCondition` (`:2480`) compares against the just-written value. | Issue. Compute the final `Degraded` once, then set it. |
| D-02 (STATUS-21) | `EncryptionAtRestReady` survives disabling encryption. | `encryption_reconcile.go:1136-1139` writes the old condition back when encryption is off. | Issue. Remove the condition when disabled, as `PITRArchiveReady` does. |
| D-03 | `pitr.maxBinlogSize` is written into my.cnf without the CR/LF/NUL check. | `reconciler.go:1079-1084` vs `unsafeMySQLSetting` at `:925`. | Issue, security label. Route it through the same check (admission half: A-90). |
| D-04 | The encryption adoption refusal is advisory: rendering follows spec regardless. | Refusal Event at `encryption_reconcile.go:93`; rendering keyed only on `Spec.EncryptionEnabled()` (`reconciler.go:811,825,854,1363`). | Issue. Rendering must honour the refusal. |
| D-05 | `--maxmemory`, `--proactor_threads` and `--replicaof` are documented as reserved but missing from the operator-owned flags, so a user arg wins (revision 5 adds `--replicaof`, F-38). | `dragonfly_resources.go:316-327`; `dragonfly_types.go:56-60`. | Issue (admission half: A-92). |
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

