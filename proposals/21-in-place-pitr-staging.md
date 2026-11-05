# Proposal: In-Place Point-in-Time Restore Through a Staging Instance

**Issue:** [#205](https://github.com/ShipStream/bloodraven/issues/205)
**Status:** Draft
**Branch:** `docs/in-place-pitr-proposal`

All names introduced here (fields, phases, reasons, Events, annotations, metrics, log messages, scripts) are **proposed**. Citations are `path:line` on `main` at `3fa1900` unless marked `origin/fix/issue-212:` (PR #233, open, head `31429aa`).

## Supported procedure

**Point-in-time recovery into a new failover group (`spec.initFromBackup.pointInTime`) remains the supported production procedure.** It leaves the damaged group untouched until the application cuts over, so a failed or wrong restore costs nothing. In-place PITR, as designed here, is for constrained environments where a second group cannot be created (quota, licensing, fixed endpoints, a single storage pool). It does not merge while the PITR planner can skip another site's transactions (#206), and it always discards every write committed after the stop time, including writes accepted while the staging instance is being built.

## Motivation

`spec.restoreInPlace.pointInTime` exists in the API but every run that sets it is rejected before anything is fenced (`internal/controller/restore_inplace.go:262-274`, `:288-290`; field comment `api/v1alpha1/backup_types.go:802-811`). #197 removed the old behaviour because it was silently wrong: the restore Job piped archived binlogs into the live primary, whose `gtid_executed` already contained every one of those GTIDs (it executed or replicated them), so MySQL skipped the whole replay and the run reported `Succeeded` with the data at the backup. Installing the dump's GTID set first, as new-group PITR does (`internal/controller/restore.go:931-960`), is impossible on a live server: `updateGtidSet:"replace"` requires the dump's set to be disjoint from what the server has executed.

A staging instance removes the conflict. The dump is loaded into an empty, throwaway `mysqld` whose GTID state starts at the dump's own baseline, the archive is replayed there with normal GTID deduplication, the result is proven against the replay plan, and only that verified result is loaded into the live primary through today's full-instance path (fence, peer hold, DROP, unlogged load, peer reclone). The live primary never receives a binlog replay, so there is nothing to skip.

What users get: a rewind of a whole instance to a proven point in time, in the same group, with the outage limited to fence plus DROP plus load (staging runs before the fence), with peers that keep their pre-restore data if the load fails, and with a primary that stays fenced, rather than serving a half-loaded dataset, after a failure past the point of no return.

## Goals

1. Full-instance in-place PITR (`restoreInPlace.pointInTime` with an empty `loadOptions.includeSchemas`) through a staging `mysqld` in an operator-built Job.
2. Every PITR refusal (coverage, cross-site order, GTID proof, versions, volume) lands before the fence, so a refused run causes no outage.
3. The outage is unchanged from today's full-instance restore: staging, replay and the intermediate dump run while the group serves traffic and emergency failover stays available.
4. A failure after the live DROP leaves the primary fenced, the peers held with pre-restore data, and a verified artifact that a new confirm can reuse without re-staging.
5. Archive lineage stays correct: no GTID reuse, a durable restore barrier the planner enforces, and an automatic post-restore full backup.
6. Additive to `shipstream.io/v1beta1`: no renamed or removed fields.

## Non-goals

- **Replacing new-group PITR.** `initFromBackup.pointInTime` stays the supported production procedure. Docs keep pointing there first.
- **Per-schema in-place PITR.** Rejected before fencing with `RestoreInPlaceRejected` (Q5). Cross-schema transactions, routines, events and a binlogged per-schema load need their own design. Follow-up issue.
- **Cross-site chronological replay.** Owned by #206. This proposal consumes its plan and refuses until it exists.
- **Keeping post-stop writes.** Writes after `stopDatetime` are discarded by definition, including writes during staging.
- **Automatic retry after the DROP.** Nothing destructive runs without a new human confirm (Q4).
- **A persistent artifact class in backup storage** (Q1 alternative C, named as the future upgrade path).
- **Compatibility with older archives, objects or in-flight state.** Zero production users (#237 D3): unknown state is refused with a condition or Event and a documented procedure.

## Preconditions (hard merge gates)

The feature (PR6 below) does not merge until all of these are on `main`:

| # | Precondition | Why it gates |
|---|---|---|
| P1 | **#206** chronological cross-site replay with a GTID continuity proof, and a writer-stamped source-archive identity | Without it a window spanning a failback (A to B to A) can skip the other site's transactions. The staged GTID proof needs #206's plan to define the expected and allowed sets across sites, and the reuse fingerprint needs the archive identity. |
| P2 | **#203** encryption identity bound to the prefix, with an operator-owned immutable Secret revision | The staging Job must mount the accepted revision, not the user Secret, so a Secret edit during a multi-hour run cannot change what the Job decrypts. The revision is part of the reuse fingerprint. |
| P3 | **#233** (open): peer `STOP REPLICA IO_THREAD` at Fencing, `fencedGtidExecuted`, Held vs Discarded settlement | Without it the peers replicate the DROPs and a post-DROP failure damages every site. Settlement is the evidence the damaged-primary fence keys on. |
| P4 | **v1.6.0 target-coverage rule** for the staged run: the staging verdict must be `targetCovered=true` (`cmd/bloodraven/pitr_download.go:388-414`) | An in-place restore to an unproven point is a silent partial restore. `allowPartial` and `allowUnverifiedCrossSite` (`api/v1alpha1/backup_types.go:219-234`) are refused for in-place runs at reconcile time. |
| P5 | **Damaged-primary fence** (Q10), shipped as its own PR | Today a failed restore unfences the primary ("Even on failure we unfence", `internal/controller/restore_inplace.go:925-927`). That gap exists for non-PITR full-instance restores too. |
| P6 | **Restore barrier and automatic post-restore backup** (Q3), shipped as its own PR | Today's non-PITR full-instance restores already break lineage silently: a PITR from a pre-restore backup across the restore replays post-restore binlogs onto pre-restore data. |

## API

All additions are optional fields or new enum values on existing types. CEL is used only for structural invariants; every semantic refusal stays reconcile-time so that a stored object with an unsupported spec still accepts unrelated updates (house rule, `internal/controller/restore_inplace.go:276-281`).

### Spec: `spec.restoreInPlace.staging`

```go
// RestoreInPlaceSpec gains one field (api/v1alpha1/backup_types.go:778).
// +kubebuilder:validation:XValidation:rule="has(self.pointInTime) == has(self.staging)",message="staging must be set exactly when pointInTime is set"
type RestoreInPlaceSpec struct {
	// ... existing fields: confirm, source, loadOptions, pointInTime, decryption ...

	// Staging configures the throwaway mysqld that loads the dump, replays
	// the archive to pointInTime and produces the verified artifact the
	// live primary is loaded from. Required with pointInTime, forbidden
	// without it.
	// +optional
	Staging *RestoreStagingSpec `json:"staging,omitempty"`
}

// RestoreStagingCleanupPolicy controls what happens to the staging volume
// when a run ends.
// +kubebuilder:validation:Enum=Delete;RetainOnFailure
type RestoreStagingCleanupPolicy string

const (
	// RestoreStagingDelete deletes the staging volume after any terminal
	// outcome, except while the run's damaged-primary fence is up.
	RestoreStagingDelete RestoreStagingCleanupPolicy = "Delete"
	// RestoreStagingRetainOnFailure deletes it after success and keeps it
	// after a failure so a new confirm can reuse the verified artifact.
	RestoreStagingRetainOnFailure RestoreStagingCleanupPolicy = "RetainOnFailure"
)

type RestoreStagingSpec struct {
	// Image runs mysqld and mysqlsh in the staging Job and mysqlsh in the
	// destructive load Job. Default: the live target pod's resolved image
	// digest when a probe finds mysqlsh in it, else spec.backup.image.
	// Whatever is chosen must report exactly the live target's VERSION().
	// +kubebuilder:validation:MinLength=1
	// +optional
	Image string `json:"image,omitempty"`

	// Volume is the operator-provisioned PersistentVolumeClaim, one per
	// run, that holds the decrypted inputs, the staging datadir and the
	// intermediate dump.
	Volume RestoreStagingVolumeSpec `json:"volume"`

	// Resources are forwarded verbatim to the staging container. When a
	// memory limit is set, innodb_buffer_pool_size is half of it.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// CleanupPolicy controls the staging volume after the run ends.
	// +kubebuilder:default=RetainOnFailure
	// +optional
	CleanupPolicy RestoreStagingCleanupPolicy `json:"cleanupPolicy,omitempty"`

	// ActiveDeadlineSeconds bounds the staging Job (load, replay, proof,
	// intermediate dump). The destructive load Job keeps
	// spec.backup.activeDeadlineSeconds.
	// +kubebuilder:default=21600
	// +kubebuilder:validation:Minimum=600
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// SanityCheck runs one read-only scalar SELECT against the staged
	// instance after the GTID proof, with super_read_only=ON. Same type
	// as MysqlBackupVerification.spec.sanityCheck.
	// +optional
	SanityCheck *SanityCheckSpec `json:"sanityCheck,omitempty"`
}

type RestoreStagingVolumeSpec struct {
	// Size of the staging claim. Required: compressed dump bytes do not
	// predict datadir + indexes + downloaded binlogs + intermediate dump.
	Size resource.Quantity `json:"size"`

	// StorageClassName of the staging claim. Unset uses the cluster
	// default class. An empty string is refused because it means "no
	// class" (static binding), which an operator-provisioned claim
	// cannot use.
	// +kubebuilder:validation:MinLength=1
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}
```

Choices:

- **`staging` is required exactly when `pointInTime` is set.** The pairing is structural (two fields that only make sense together), so it is CEL. A missing `volume.size` cannot be defaulted safely (Q6), so the block must exist; and a `staging` block on a non-PITR restore would be silently ignored, which is worse than an admission error. CRD validation ratcheting (on by default since Kubernetes 1.30) keeps an unchanged stored `restoreInPlace` updatable; with zero production users the residual case of an old object with `pointInTime` and no `staging` is accepted (D3).
- **Everything else is reconcile-time** (`RestoreInPlaceRejected`, confirm not consumed): `allowPartial: true`, `allowUnverifiedCrossSite: true`, `pointInTime` with a non-empty `includeSchemas`, explicit `loadOptions.skipBinlog: false`, a size below a floor (Warning only, see Lifecycle).
- **Access mode is fixed `ReadWriteOnce` and not exposed** (Q6). One pod writes the volume, then one pod reads it, strictly in sequence, which the controller enforces. No use case needs RWX, and RWX classes (NFS) are a poor fit for a mysqld datadir. A future `accessModes` field would be additive.
- **`size` is a plain required `resource.Quantity`**, with no CEL floor. The CEL quantity library is version-dependent; the useful floor depends on the source size, which only the reconciler knows.
- **`storageClassName` is a pointer** because absence (cluster default) and empty string differ.
- **Credentials: no spec field.** The staging `mysqld` is created with `--initialize-insecure` and reached only as `root@127.0.0.1` from inside the pod, exactly as verification does (`internal/controller/verify_script.sh:220-247`). It never talks to the live group, the loader (`util.load_dump` defaults to `loadUsers: false`) loads no accounts, and the intermediate dump is taken with `users: false`. A configurable account would add a Secret to leak and protect nothing loopback does not.
- **Network isolation: no spec field** (Q8). The staging server binds `127.0.0.1` with `--mysqlx=OFF`, the pod has no Service, `automountServiceAccountToken: false`, `hostNetwork: false`, and runs only operator-built containers. No NetworkPolicy: it would add RBAC and a chart mirror, depends on the CNI enforcing it, and cannot isolate containers sharing the pod network namespace, which is the only place loopback is reachable from. The flags are pinned by a unit test.
- **No new pull-secret field.** Staging and probe Jobs use `spec.backup.imagePullSecrets` (`api/v1alpha1/backup_types.go:35-36`). If the live image cannot be pulled with them, the probe fails and selection falls back to `spec.backup.image` (recorded in status).
- **`RetainOnFailure` is the default** because a retained, verified artifact is the fastest repair source after a post-DROP failure. A boolean was rejected (third state), and a TTL was rejected (it deletes the repair source at 3am).

### Status

New `RestoreInPlacePhase` values (enum at `api/v1alpha1/backup_types.go:837-838` becomes `"";Staging;Staged;Preflight;Fencing;Restoring;Resuming;Succeeded;Failed`):

| Phase | Meaning |
|---|---|
| `Staging` | The run is accepted with `pointInTime`. The operator provisions the staging claim, probes the image, and runs the staging Job (mode `Build`) or the re-authentication Job (mode `Reuse`). No deployment hold, no topology freeze, no queue, no fence. |
| `Staged` | The staging Job succeeded and the operator verified its result (sentinel, manifest digest, staged GTID set, versions, verdict). One-step phase; the run moves to `Preflight` on the next reconcile. |

Existing phases keep their meaning; PITR runs reach `Preflight` only from `Staged`. Non-PITR runs still start at `Preflight`.

```go
// status.restoreInPlace.staging: the current run's staging state.
// status.restoreStagingRetained: artifacts that outlive their run.
type RestoreInPlaceStatus struct {
	// ... existing fields ...
	// +optional
	Staging *RestoreStagingStatus `json:"staging,omitempty"`
}

type MysqlFailoverGroupStatus struct {
	// ... existing fields ...
	// RestoreStagingRetained lists staging artifacts kept after their run
	// ended (RetainOnFailure, or a damaged-primary fence). The controller
	// keeps at most one entry outside a replacement window.
	// +listType=map
	// +listMapKey=runID
	// +kubebuilder:validation:MaxItems=4
	// +optional
	RestoreStagingRetained []RestoreStagingStatus `json:"restoreStagingRetained,omitempty"`

	// RestoreBarriers lists restore barriers whose post-restore baseline
	// backup has not completed yet. See "GTID and archive lineage".
	// +listType=map
	// +listMapKey=runID
	// +optional
	RestoreBarriers []RestoreBarrierStatus `json:"restoreBarriers,omitempty"`
}

// +kubebuilder:validation:Enum=Build;Reuse
type RestoreStagingMode string

// +kubebuilder:validation:Enum=Override;LiveTarget;BackupImage
type RestoreStagingImageSource string

// +kubebuilder:validation:Enum=Provisioning;Building;Verified;Consumed;Retained;Deleting;Gone
type RestoreStagingArtifactState string

type RestoreStagingStatus struct {
	RunID          string             `json:"runID"`                  // owning run; never reused
	Mode           RestoreStagingMode `json:"mode,omitempty"`
	ReusedFromRunID string            `json:"reusedFromRunID,omitempty"`
	CleanupPolicy  RestoreStagingCleanupPolicy `json:"cleanupPolicy,omitempty"` // policy the run was accepted with
	PVCName        string             `json:"pvcName,omitempty"`      // written before Create (write-ahead)
	PVCUID         string             `json:"pvcUID,omitempty"`
	LocalNode      string             `json:"localNode,omitempty"`    // set when the bound PV is pinned to one node
	ProbeJobName   string             `json:"probeJobName,omitempty"`
	JobName        string             `json:"jobName,omitempty"`
	JobCreating    bool               `json:"jobCreating,omitempty"`  // write-ahead, as RestoreInPlaceStatus.jobCreating
	Attempts       int32              `json:"attempts,omitempty"`     // staging Jobs re-created after loss
	Image          string             `json:"image,omitempty"`
	ImageDigest    string             `json:"imageDigest,omitempty"`
	ImageSource    RestoreStagingImageSource `json:"imageSource,omitempty"`
	MysqlshVersion string             `json:"mysqlshVersion,omitempty"`
	ServerVersion  string             `json:"serverVersion,omitempty"`      // staging SELECT VERSION()
	DumpServerVersion string          `json:"dumpServerVersion,omitempty"`  // source @.json serverVersion
	TargetServerVersion string        `json:"targetServerVersion,omitempty"` // live target at the last check
	Fingerprint    string             `json:"fingerprint,omitempty"`  // sha256 over the reuse inputs
	SourceIdentity string             `json:"sourceIdentity,omitempty"`
	ArchiveIdentity string            `json:"archiveIdentity,omitempty"`    // #206
	EncryptionRevision string         `json:"encryptionRevision,omitempty"` // #203 Secret revision, or "plaintext"
	ManifestSHA256 string             `json:"manifestSHA256,omitempty"`
	ArtifactBytes  int64              `json:"artifactBytes,omitempty"`
	StagedGtidExecuted string         `json:"stagedGtidExecuted,omitempty"`
	StagedGtidSHA256 string           `json:"stagedGtidSHA256,omitempty"`
	StagingLoadSeconds int64          `json:"stagingLoadSeconds,omitempty"`
	StartTime      *metav1.Time       `json:"startTime,omitempty"`
	StagedAt       *metav1.Time       `json:"stagedAt,omitempty"`
	State          RestoreStagingArtifactState `json:"state,omitempty"`
	RetainedReason string             `json:"retainedReason,omitempty"`
	Reason         string             `json:"reason,omitempty"`   // stable machine reason of the last refusal/failure
	Message        string             `json:"message,omitempty"`
}

// +kubebuilder:validation:Enum=Writing;Written;Archived;Closed
type RestoreBarrierState string

type RestoreBarrierStatus struct {
	RunID       string              `json:"runID"`
	Site        string              `json:"site,omitempty"`
	GTID        string              `json:"gtid,omitempty"` // <server_uuid>:bloodraven_restore:<n>
	State       RestoreBarrierState `json:"state,omitempty"`
	WrittenAt   *metav1.Time        `json:"writtenAt,omitempty"`
	ArchivedAt  *metav1.Time        `json:"archivedAt,omitempty"`
	BaselineBackup *corev1.ObjectReference `json:"baselineBackup,omitempty"` // name + uid
	Message     string              `json:"message,omitempty"`
}

// RestoreOfflineFenceStatus (api/v1alpha1/backup_types.go:987) gains:
type RestoreDamageRecord struct {
	RunID        string       `json:"runID"`
	ConfirmToken string       `json:"confirmToken,omitempty"`
	JobName      string       `json:"jobName,omitempty"`
	Since        metav1.Time  `json:"since"`
	Reason       string       `json:"reason,omitempty"` // e.g. RestoreInPlaceFailed, RestoreInPlaceJobLost
}
// Damage *RestoreDamageRecord `json:"damage,omitempty"`
```

`stagedGtidExecuted` is stored in full, as `sourceGtidExecuted` and `targetGtidExecuted` already are (`api/v1alpha1/backup_types.go:1016-1105`); the digest is what the loader is handed and what the manifest is checked against. A `status.pitr` summary is unchanged.

### Conditions

| Type | Status/Reason | When |
|---|---|---|
| `RestoreInPlaceDamagedPrimaryFenced` (new) | `True` / `RestoreFailedAfterJob` | #233 settlement says `Held` for a run whose Job was created. Message names the runID, the exits and the retained artifact. `False` / `Released`, `Transferred` or `Rebuilt` after an exit. |
| `PITRArchiveReady` (existing, `internal/controller/pitr_archive_condition.go:29`) | `False` / `PITRRestoreBarrier` (new reason) | An entry in `status.restoreBarriers` is not `Closed`. Message names the barrier GTID and the post-restore backup. Precedence: below `ArchiverError`, above `Archiving`. |
| `Ready` | `False` while the damaged fence is up | Same mechanism #233 uses for `Held`. |

### Reasons and Events

Stable reason strings, used in `status.restoreInPlace.staging.reason`, run messages and Events:

| Reason | Where | Confirm consumed |
|---|---|---|
| `RestoreInPlaceRejected` (existing) | reconcile-time spec refusals listed above | no |
| `StagingVersionMismatch` | probe, staged check, Fencing, loader pre-DROP check | no; the run is retried only when the cached pre-check for (image digest, live version) passes |
| `StagingTargetNotCovered` | staging verdict `targetCovered=false` | yes |
| `StagingGtidProofFailed` | staged set outside the plan | yes |
| `StagedGtidsNotInLive` | staged set not a subset of the live `gtid_executed` | yes |
| `StagingKeyringUnavailable` | encrypted tables in the dump, keyring component unavailable | yes |
| `StagingVolumeNotWritable` | in-Job ownership/writability check | yes |
| `StagingVolumeUnavailable` | claim lost, PV node gone or unschedulable before the destructive Job | no if before Fencing, else the fence is released (no Job) |
| `StagingArtifactMismatch` | manifest or file digest mismatch (operator check, re-auth Job or loader) | yes |
| `StagingLoaderDeadlineTooShort` | `spec.backup.activeDeadlineSeconds < 1.5 x stagingLoadSeconds` | no |
| `StagingFailed` | staging Job failed for any other reason | yes |
| `RestoreInPlaceSuperseded` | a newer confirm replaced a run still in `Staging`/`Staged` | n/a (newer confirm runs) |
| `RestoreBarrierNotArchived` | barrier binlog not archived within 10 min at Fencing | yes (fence released, no Job) |
| `PITRRestoreBarrier` | planner refusal and `PITRArchiveReady` reason | n/a |

New Events: `RestoreInPlaceStagingStarted`, `RestoreInPlaceStagingJobCreated`, `RestoreInPlaceStagingVolumeNodeLocal` (Warning), `RestoreInPlaceStagingVolumeSmall` (Warning), `RestoreInPlaceStaged`, `RestoreInPlaceStagingReused`, `RestoreInPlaceStagingFailed` (Warning), `StagingVersionMismatch` (Warning), `RestoreInPlaceWritesDiscarded` (Normal, at Fencing: requested stop time and actual fence time), `RestoreBarrierWritten`, `RestoreBarrierArchived`, `RestoreBarrierClosed`, `PostRestoreBackupCreated`, `PostRestoreBackupFailed` (Warning), `RestoreInPlaceDamagedPrimaryFenced` (Warning), `RestoreInPlaceDamagedPrimaryReleased`, `RestoreInPlaceFenceTransferred`, `RestoreStagingRetained`, `RestoreStagingDeleted`, `RestoreStagingVolumeGone` (Warning), `RestoreStagingCleanupRejected` (Warning), `RestoreInPlaceUnknownPhase` (Warning).

### Annotations (one-shot, on the `MysqlFailoverGroup`, consumed and cleared like `RecloneAnnotation`)

| Annotation | Value | Effect |
|---|---|---|
| `bloodraven.shipstream.io/restore-staging-cleanup` | owning `runID` | Deletes that run's retained artifact. Refused (Event, annotation cleared) if the runID is not retained, is the current run, or is the reuse source of a non-terminal run. Consumption is recorded as `state: Deleting`. RunIDs are UUIDs and never reused, so a stale value cannot hit a later artifact. |
| `bloodraven.shipstream.io/restore-accept-damaged` | failed `runID` | Releases the damaged-primary fence (exit 2). Refused unless the runID matches `status.restoreOfflineFence.damage.runID` and an uncached read shows the Job gone or explicitly terminal and every pod labelled with it `Succeeded`/`Failed` (the #232 `verificationExecutionStopped` rule, `internal/controller/backup_verification_hold.go:62`). The reclone queue stays `Held`; its exits are #233's. |

`kubectl bloodraven restore-staging list|cleanup <group> --run <id>` (new plugin subcommand, `cmd/kubectl-bloodraven/`) reads the retained list and sets the cleanup annotation. `kubectl bloodraven restore accept-damaged <group> --run <id>` sets the other.

A hold annotation is added to the source `MysqlBackup` while a staging or loader Job may read it: `hold.restore.shipstream.io/<runID>` = group name, mirroring the verification hold prefix (`internal/controller/backup_verification_hold.go:29`).

### Metrics

| Metric | Type | Labels |
|---|---|---|
| `bloodraven_restore_staging_duration_seconds` | histogram (buckets to 48h) | `namespace, group, outcome` (`staged`, `failed`, `refused`, `reused`) |
| `bloodraven_restore_staging_retained_artifacts` | gauge | `namespace, group` |
| `bloodraven_restore_staging_retained_bytes` | gauge | `namespace, group` |
| `bloodraven_restore_damaged_primary_fenced` | gauge (1/0) | `namespace, group, site` |
| `bloodraven_pitr_restore_barriers_open` | gauge | `namespace, group` |

`bloodraven_restore_duration_seconds` (`internal/metrics/metrics.go:262-273`) keeps `restore_kind="in_place"` for the destructive Job.

### RBAC

PVC create/delete already exists (`internal/controller/reconciler.go:190`, `config/rbac/role.yaml:7-20`). New: `+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get` on the group reconciler, to read the bound PV's `nodeAffinity`. `charts/bloodraven/templates/clusterrole.yaml` must mirror it (AGENTS.md pre-PR gate 5). The reconciler also `Owns(&corev1.PersistentVolumeClaim{})` for the staging claims; claims carry an owner reference to the group.

### Example

```yaml
apiVersion: shipstream.io/v1beta1
kind: MysqlFailoverGroup
metadata:
  name: orders
spec:
  backup:
    pitr:
      enabled: true
      profileName: nightly-s3
    activeDeadlineSeconds: 14400   # loader deadline; Preflight refuses if < 1.5x the measured staging load
  restoreInPlace:
    confirm: "2026-11-20T03:10:00Z"   # strictly newer than status.restoreInPlace.confirmTokenUsed
    source:
      mysqlBackupRef:
        name: orders-nightly-20261119
    loadOptions:
      threads: 8                       # skipBinlog is forced true; explicit false is rejected
    pointInTime:
      stopDatetime: "2026-11-20T01:42:00Z"
      # allowPartial / allowUnverifiedCrossSite are refused for in-place runs
    staging:
      # image: omitted -> live target digest if it carries mysqlsh, else spec.backup.image
      volume:
        size: 600Gi                    # datadir + indexes + binlogs + intermediate dump
        storageClassName: replicated-encrypted   # node-local classes get a Warning
      resources:
        requests: {cpu: "4", memory: 16Gi}
        limits: {memory: 16Gi}         # innodb_buffer_pool_size = 8Gi
      cleanupPolicy: RetainOnFailure
      activeDeadlineSeconds: 43200
      sanityCheck:
        query: "SELECT COUNT(*) FROM orders.order_header WHERE created_at < '2026-11-20 01:42:00'"
        expect: {minRows: 1}
```

## Lifecycle

```
                   pointInTime set                                 no pointInTime
 spec accepted ───────────────┐                                        │
                              v                                        │
                 ┌──────── Staging ────────┐  (unfenced, no hold,      │
   superseded /  │  probe, PVC, Job        │   failover allowed)       │
   cancelled  <──┤                         ├──> Failed (refused /      │
                 └────────────┬────────────┘     staging failed)       │
                              v                                        │
                           Staged ──(verify outputs)───────┐           │
                                                           v           v
                                                       Preflight <─────┘
                                                           │ hold, arm queue, record fence
                                                           v
                                                        Fencing  label, offline_mode, rechecks,
                                                           │      barrier, peer IO stop, archive wait
                                                           v
                                                       Restoring  loader: verify manifest, DROP, load
                                                       │       │
                                       Job succeeded   │       │ Job failed / lost
                                                       v       v
                                                  Resuming   Failed ──> settlement (#233)
                                                       │                 │ Held -> damaged-primary fence
                                                       v                 │ Discarded -> release fence
                                                  Succeeded ──> post-restore backup closes barrier
```

### 1. Acceptance (entry)

`validateInPlaceRestoreSpec` keeps its checks and replaces the blanket `pointInTime` refusal with the reconcile-time refusals listed under API. A valid PITR run writes `phase=Staging`, `runID`, `runConfirm`, `startTime`, `scope=full` and `staging{runID, mode, cleanupPolicy, state: Provisioning}` in one status write, then emits `RestoreInPlaceStagingStarted`. Nothing else happens on that pass.

If `status.restoreStagingRetained` holds an entry whose fingerprint inputs match (see Q4), `mode=Reuse` and `reusedFromRunID` are set and no new claim is provisioned.

### 2. Staging

No deployment hold, no topology freeze, no reclone queue, no fence, no `offline_mode`. `inPlaceRestoreFreezesTopology` (`internal/controller/restore_inplace.go:70-74`) changes from "in flight and not Preflight" to "Fencing, Restoring or Resuming, or a damaged-primary fence". New deployment leases are refused for the whole run, as today: the unstable-operation check reports any non-terminal in-place phase (`internal/controller/deployment_leases.go:295-296`). A deploy that migrates the schema during staging would be undone by the restore while the new code keeps running, so refusing new leases is safer than allowing them; existing leases are only waited for in Preflight.

Steps, one durable write before each external action:

1. **Source hold.** For a `mysqlBackupRef` source, stamp `hold.restore.shipstream.io/<runID>` on the `MysqlBackup` with an optimistic lock, then re-read it uncached before any Job is created and refuse if it is gone, replaced (UID) or terminating, as `stampVerificationHold` and `launchHoldConfirmed` do (`internal/controller/backup_verification_hold.go:308`, `:347`). Retention (`deleteUnreadBackup`, `:418`), artifact cleanup (`internal/controller/backup_reconciler.go:637`) and the PITR binlog cutoff (`cmd/bloodraven/pitr_retention.go:53`) honour restore holds as they honour verification holds. The hold is released once the run is terminal and no staging, re-auth or loader pod can still read the source.
2. **Image selection and probe.** Candidate = `staging.image` (`Override`), else the live target pod's `status.containerStatuses[].imageID` digest (`LiveTarget`), else `spec.backup.image` (`BackupImage`). A probe Job (`mysql-<fg>-staging-probe-<run8>`, 120 s deadline, no volumes, no token) runs `mysqlsh --version; mysqld --version` and reports through the termination message. A `LiveTarget` candidate without mysqlsh, or with a mysqlsh older than the one that wrote the source dump (`@.json`), falls back once to `BackupImage`. The selected `mysqld --version` must equal the live target's `SELECT VERSION()` and must not be older than the dump's `serverVersion`; otherwise the run ends `Failed`/`StagingVersionMismatch` without consuming the confirm. Probe results are cached in status per digest, so an unchanged mismatch does not re-run Jobs; the run is retried when the image digest or live version changes. **Unverified:** whether the default `mysql:9.7` image ships mysqlsh. Nothing in the repo relies on it (`api/v1alpha1/backup_types.go:9-21` names the community-server image as the mysqlsh carrier), so the design probes and never assumes.
3. **Claim.** Write `pvcName=mysql-<fg>-staging-<run8>` (write-ahead), then create the claim (RWO, `volume.size`, `storageClassName`), owner reference to the group, labels `shipstream.io/restore-run=<runID>`. Once bound, read the PV: `nodeAffinity` on `kubernetes.io/hostname` sets `localNode` and emits `RestoreInPlaceStagingVolumeNodeLocal` (zonal affinity is not node-local). A size below `3 x MysqlBackup.status.sizeBytes` emits `RestoreInPlaceStagingVolumeSmall`; it is a warning, not a refusal, because compression ratios vary.
4. **Staging Job.** Write `jobCreating=true` and `jobName` (write-ahead, as `internal/controller/restore_inplace.go:733-752` does for the destructive Job), then create `mysql-<fg>-staging-<run8>`. Pod: hardened security context (uid/gid 27, read-only root, drop ALL; `internal/controller/backup_job.go:505-560`), `automountServiceAccountToken: false`, no `hostNetwork`, read-only failover tolerations, `backoffLimit: 0`, `activeDeadlineSeconds` from `staging`. Init containers (operator image): `decrypt-download` and `pitr-download` with the #203 revision Secret mounted 0400, writing to the claim instead of size-limited emptyDirs. The claim is mounted at `/var/lib/mysql` (the stock AppArmor profile only allows a datadir under `/var/lib/mysql/**`, `internal/controller/backup_verification_job.go:18-33`); `/run/mysqld` and `/tmp` are emptyDirs.

Staging Job internals (new `staging.sh`, sharing the mysqld boot with `verify_script.sh` and delegating load and replay to `restore.py`):

| Step | What | Fails with |
|---|---|---|
| a | Ownership: `/var/lib/mysql` writable by uid 27; create `<runID>/` owned by the Job; refuse if another runID's marker exists; free-space check against the source size | `StagingVolumeNotWritable` |
| b | Boot mysqld on `<runID>/data` after `--initialize-insecure`: `--bind-address=127.0.0.1 --mysqlx=OFF --skip-log-bin --gtid-mode=ON --enforce-gtid-consistency=ON --skip-replica-start --event-scheduler=OFF --skip-name-resolve --partial-revokes=ON --local-infile=1` (the verification set, `internal/controller/verify_script.sh:191-204`, plus the event scheduler); compare `VERSION()` with `BLOODRAVEN_STAGING_EXPECT_VERSION` before loading anything | `StagingVersionMismatch` |
| c | Keyring: load `component_keyring_file` as verification does (`:95-153`). Unlike verification it fails closed: if the component cannot load and any dump DDL file contains `ENCRYPTION='Y'`, stop | `StagingKeyringUnavailable` |
| d | `util.load_dump` with `updateGtidSet:"replace"`, `skipBinlog:true`, `loadUsers:false` (the verification baseline, `internal/controller/backup_job.go:746-765`); record the load duration | `StagingFailed` |
| e | `_verify_loaded_dump` (post-stop GTIDs already in the dump, lineage; `internal/controller/restore_script.py:581-618`) | `StagingTargetNotCovered` |
| f | Replay through the #206 chronological plan, not one mysqlbinlog run per site | `StagingFailed` |
| g | Staged-GTID proof, below | `StagingGtidProofFailed` |
| h | Verdict: `targetCovered=true`, `partial=false`, `crossSiteUnverified=false` | `StagingTargetNotCovered` |
| i | `SET GLOBAL super_read_only=ON`; optional sanity query inside `START TRANSACTION READ ONLY` | `StagingFailed` (`SanityCheckFailed` detail) |
| j | Delete decrypted inputs and downloaded binlogs; `util.dump_instance('<runID>/artifact', {consistent:true, users:false, events:true, routines:true, triggers:true, compression:'zstd', threads})` | `StagingFailed` |
| k | Re-read `@@gtid_executed`; it must equal the proven set and the artifact's `@.json` `gtidExecuted` | `StagingGtidProofFailed` |
| l | Write `<runID>/artifact-manifest.json`: `{version:1, runID, fingerprint, files:[{path,size,sha256}], stagedGtidExecuted, serverVersion, mysqlshVersion, imageDigest, createdAt}`; fsync | `StagingFailed` |
| m | Stop mysqld, remove `<runID>/data`; print `BLOODRAVEN_STAGING_GTIDS v=1 <set>` and, last, `BLOODRAVEN_STAGING_COMPLETE v=1 runID= manifestSha256= stagedGtidSha256= serverVersion= mysqlshVersion= bytes= loadSeconds=` | |

**Staged-GTID proof.** Let `B` be the dump's `@.json` `gtidExecuted` (installed by `replace`), `X` the user's `excludeGtids`, and from the #206 plan `P` the transactions the plan proves committed at or before the stop and schedules for replay, `W` every GTID in the staged files that the plan places at or before the stop (`P ⊆ W`), and `Q` every staged GTID after the stop. With `S = @@gtid_executed` after replay:

- expected ⊆ staged: `(B ∪ P) − X ⊆ S` (nothing planned was skipped: a short `--stop-datetime` cut, a mid-file stop or a cross-site skip all fail here);
- staged ⊆ allowed: `S ⊆ (B ∪ W) − X` (nothing outside the plan ran);
- no post-stop leak: `S ∩ Q = ∅`.

When #206 proves the exact set, `W = P` and the proof is equality. #206 must emit `P`, `W` and `Q` in `coverage.json`; today's per-site `inWindowGTIDs`/`postStopGTIDs` (`cmd/bloodraven/pitr_download.go:225-274`) cover only the single-replaying-site case. The diffs are written to `<runID>/gtid-proof.json` for diagnosis.

**Result channel.** The termination message (4096 bytes) carries only the short verdict. The GTID set travels in the pod log sentinel (no size limit; the controller already tails `BLOODRAVEN_RESTORE_COMPLETE` the same way, `internal/controller/restore.go:227`) and in the manifest on the claim. The operator records the set, its digest and the manifest digest in status. Status is the trust anchor: only the operator writes it, and every later check compares the claim against it.

### 3. Staged

On Job success the operator parses both sentinels, checks `sha256(set) == stagedGtidSha256`, the versions, and that `stagingLoadSeconds` is present; writes `state=Verified`, `stagedAt`, and `phase=Staged`; emits `RestoreInPlaceStaged`. A previously retained artifact that this run replaces is deleted now, after the replacement is verified (Q9). Next pass: `phase=Preflight`.

In `Reuse` mode, Staging runs a re-authentication Job instead of the staging Job: it mounts the retained claim read-only, re-hashes every file against the manifest, checks the manifest against `manifestSHA256` and the artifact `@.json` set against `stagedGtidExecuted`, and prints the same completion sentinel. No mysqld, no replay. A mismatch ends the run `StagingArtifactMismatch` and marks the retained entry `Gone`.

### 4. Preflight

Today's Preflight unchanged (`internal/controller/restore_inplace.go:437-585`, plus #233's peer checks: `BeginPlanned` hold, writable active site, privileges, previous fence released or owned by a damaged record this run takes over, queue armed, fence recorded), plus for PITR runs:

- live `VERSION()` of the current active site equals `serverVersion` (refuse `StagingVersionMismatch`);
- `S ⊆` live `gtid_executed`, refuse `StagedGtidsNotInLive` otherwise (decision below);
- `spec.backup.activeDeadlineSeconds >= 1.5 x stagingLoadSeconds`, refuse `StagingLoaderDeadlineTooShort` (a loader killed by its deadline after the DROP is a damaged primary);
- claim bound and not terminating; if `localNode` is set, that node exists, is Ready and is not cordoned (`StagingVolumeUnavailable`);
- every staging, probe and re-auth pod of this run has terminated (sequential ownership of the RWO claim);
- privilege check gains `TRANSACTION_GTID_TAG` for the barrier (all full-instance runs).

**Staged set not a subset of live `gtid_executed`: refuse.** `D = S − L` non-empty means the staged data contains transactions the live history does not record, for example an archived branch a lossy failover abandoned, or a dump from another group. The alternative, `SET GLOBAL gtid_purged='+D'` after the load, rewrites the live primary's GTID history behind the archiver's back: the next archived file's `Previous_gtids` would jump by `D` with no archived events, which the predecessor-ordering planner cannot anchor, and it would quietly resurrect a branch the group abandoned. Refusing names `D` in the message and points at the two honest procedures: add `D` to `pointInTime.excludeGtids` and re-run (the fingerprint changes, so it re-stages), or restore into a new group. Consequence, documented: in-place PITR from another group's backup or archive is always refused.

### 5. Fencing

Target site bound at entry (`TopologyGeneration++`, existing). Order:

1. strip the primary role label, wait until fenced (existing);
2. `offline_mode=ON`, evict pre-fence sessions (existing, `internal/controller/restore_offline_fence.go:365-450`);
3. PITR rechecks against the bound target: `VERSION()`, `S ⊆ gtid_executed` (a staging-time failover may have bound a different server), claim and node checks, staging pods gone, `manifestSHA256` present. A refusal releases the fence immediately (no Job, no barrier);
4. **barrier**: write `restoreBarriers[runID]{state: Writing}`, then on the primary `SET GTID_NEXT='AUTOMATIC:bloodraven_restore'; BEGIN; COMMIT; SET GTID_NEXT='AUTOMATIC'; FLUSH BINARY LOGS`, then record the assigned GTID, `state: Written`, Event `RestoreBarrierWritten`. On restart in `Writing`, any `bloodraven_restore` GTID in `gtid_executed` that no other barrier records belongs to this run;
5. #233 peer IO stop with catch-up; `fencedGtidExecuted` now includes the barrier, so a run that stops before its Job still settles `Discarded`;
6. if PITR archiving is enabled, wait up to 10 minutes for the primary's sidecar to report the barrier archived (new `GET /archiver/covers?gtids=<set>` returning whether a sealed archived file covers the set), then `state: Archived`. Timeout ends the run `RestoreBarrierNotArchived` before any Job;
7. Event `RestoreInPlaceWritesDiscarded` (requested stop, fence time), `phase=Restoring`.

### 6. Restoring

Today's destructive path (`internal/controller/restore_inplace.go:689-873`) with a different source and three extra guards:

- source: the staging claim, `subPath: <runID>/artifact`, read-only; image: the staging image digest; no `pitr-download` or `decrypt-download` init containers; `loadOptions.skipBinlog` forced `true`; no `updateGtidSet` (the live-target rule, `internal/controller/restore.go:937-949`, `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline`);
- env `BLOODRAVEN_STAGING_MANIFEST_SHA256`, `BLOODRAVEN_STAGING_GTID_SHA256`, `BLOODRAVEN_EXPECT_TARGET_VERSION`;
- `restore.py` gains `_verify_staging_artifact` before `_require_offline_mode` and the DROP: manifest digest, every file size and SHA-256, no extra files, artifact `@.json` set digest, then target `VERSION()`. Any mismatch exits before the DROP (`BLOODRAVEN_STAGING_ARTIFACT_MISMATCH` / `..._VERSION_MISMATCH`), settlement proves the primary unchanged, and the run ends `StagingArtifactMismatch` / `StagingVersionMismatch` with the fence released.

The full re-hash runs while fenced; for a large artifact it adds minutes to the outage. That is the price of turning a truncated or altered claim into a refusal before the DROP instead of a load failure after it. The point of no return stays `_maybe_preflight_drops` (`internal/controller/restore_script.py:294-331`).

### 7. Resuming and Succeeded

Unchanged: release the queue to `Draining` before writing `Succeeded`; peers reclone from the restored primary. Additionally the hold on the source backup is released, the staging claim is deleted (success always deletes), and `staging.state=Consumed` then `Gone`.

### 8. Post-restore backup and barrier closure

When the run is `Succeeded`, or failed and settled `Discarded`, with PITR archiving enabled, the operator creates `MysqlBackup` `<fg>-post-restore-<run8>` on the PITR profile (Event `PostRestoreBackupCreated`). A barrier is `Closed` when any `Succeeded` backup of the group has a `status.gtidExecuted` containing the barrier GTID (`gtidExecuted` is read before the dump starts, so it is a lower bound that still includes the barrier). Closure removes the entry, emits `RestoreBarrierClosed`, and clears `PITRArchiveReady=False/PITRRestoreBarrier`. A failed backup emits `PostRestoreBackupFailed` and is not retried automatically; any later successful backup closes the barrier. A Held (damaged) run gets no automatic backup until an exit restores the primary: a baseline of a damaged dataset is accurate but useless.

### Where the phase transitions live

| File (proposed unless existing) | Responsibility |
|---|---|
| `internal/controller/restore_inplace.go` (existing) | Entry, phase switch, Preflight/Fencing additions, unknown-phase handling |
| `internal/controller/restore_staging.go` | Staging and Staged: probe, claim, staging and re-auth Jobs, sentinel parsing, fingerprint |
| `internal/controller/restore_staging_job.go` | Job builders, pinned flags, mounts |
| `internal/controller/restore_staging_cleanup.go` | Policy, retained list, cleanup annotation, claim loss |
| `internal/controller/restore_damage_fence.go` | Damage record, exits, fence transfer |
| `internal/controller/restore_barrier.go` | Barrier write, archive wait, post-restore backup, closure, `PITRArchiveReady` reason |
| `internal/controller/restore_hold.go` | Restore holds on `MysqlBackup` (generalizing `backup_verification_hold.go`) |
| `internal/controller/staging.sh`, `restore_script.py` (existing) | Staging script; `_verify_staging_artifact` before the DROP |
| `cmd/bloodraven/pitr_download.go`, `pitr_window.go` (existing) | `PITRRestoreBarrier` refusal, #206 plan sets `P`, `W`, `Q` |
| `internal/sidecar/server.go` (existing) | `GET /archiver/covers` |

## Retry and crash boundaries

Nothing destructive retries automatically. "Durable" lists what status holds at that point.

| Boundary | Outcome | Durable | Admin action |
|---|---|---|---|
| Operator crash before the Staging write | Next reconcile re-validates and accepts | nothing | none |
| Crash in Staging after `pvcName`/`jobCreating` write-ahead | Next pass adopts the claim/Job by name and runID label; a missing claim or Job is created | staging status | none |
| Staging Job lost (deleted, never created after write-ahead) | Re-created automatically, at most 3 times (`attempts`); the Job starts by clearing `<runID>/` except its marker. No live damage | attempts | after 3: bump confirm |
| Staging Job failed | `Failed`/reason; confirm consumed (refusals per the reason table); artifact per `cleanupPolicy` (incomplete artifacts are never reusable) | run status | fix cause, bump confirm |
| Claim lost / node lost during Staging | Job fails or stays Pending; after the Job deadline or a lost claim, `Failed`/`StagingVolumeUnavailable`; no damage | run status | bump confirm (new claim) |
| Failover during Staging | Allowed. Staging continues (it reads only the backup and archive). The target is bound at Fencing; rechecks there catch a version change or a lost transaction (`StagedGtidsNotInLive`) | none extra | none, or re-run with `excludeGtids` |
| Ordered update during Staging | Allowed; a version change is caught at Preflight/Fencing (`StagingVersionMismatch`, retried when the probe passes) | probe cache | wait for the rollout |
| Spec removed in Staging/Staged | `RestoreInPlaceCancelled`; staging Job deleted (foreground); this run's claim deleted; earlier retained artifacts untouched | run status | none |
| Superseding confirm in Staging/Staged | Current run ends `RestoreInPlaceSuperseded`; its Job is deleted; a `Verified` artifact becomes a reuse candidate, an incomplete one is deleted once its pods are gone; the new run starts | run status | none |
| Crash or failure in Preflight | As today: no fence, emergency failover available; artifact kept as `Verified` while the run lives | run status | none |
| Failure in Fencing before the barrier | Fence released, queue discarded (no Job, #233 rule), confirm per reason | run status | per reason |
| Failure in Fencing after the barrier (peer stop timeout, archive wait) | No Job; settlement `Discarded` because `fencedGtidExecuted` includes the barrier; fence released; barrier stays open; automatic backup closes it | barrier, run status | none |
| Crash between Job write-ahead and Create | Rebuilt on the next pass (existing, `:733-752`) | jobCreating | none |
| Loader Job lost after write-ahead | `RestoreInPlaceJobLost`, never rebuilt (existing); settlement decides Held vs Discarded | run, queue | see post-DROP |
| Loader fails before DROP (manifest, version, offline check) | Settlement `Discarded` (unchanged `gtid_executed`); fence released; artifact kept per policy | run status | fix, new confirm (reuses artifact) |
| **Loader fails or is killed after DROP** | see below | damage record | three exits |
| Group deleted | Owner references delete Jobs and claims; holds on backups are released by dead-hold stripping (`internal/controller/backup_verification_hold.go:247-270` generalized) | — | none |
| Operator upgrade mid-run | The new operator resumes from status. Avoid it: an upgrade during Restoring is already discouraged (#233) and staging makes runs hours longer | status | — |
| Operator downgrade mid-run | Unsupported. Operators from PR1 onwards treat an unknown phase as in flight: topology frozen, fence kept if `restoreOfflineFence` is recorded, no Job created or deleted, Warning `RestoreInPlaceUnknownPhase`. An older CRD rejects status writes carrying the new enum values, so an older operator cannot advance the run | status | upgrade back |

**What a failure after the live DROP rolls back to.** Not to the old data on the primary: the DROP and a partial load are not undone.

- Primary: stays fenced (no role label, `offline_mode=ON` re-asserted every Poll) and the topology stays frozen under `status.restoreOfflineFence.damage`, independently of the terminal phase and of whether `spec.restoreInPlace` still exists. `RestoreInPlaceDamagedPrimaryFenced=True`, `Ready=False`, alert.
- Peers: `Held` with pre-restore data and their IO threads stopped (#233), excluded from promotion and reader Services.
- Artifact: retained and still `Verified` (with `RetainOnFailure`, and with `Delete` while the damage record exists).
- Exits, none automatic:
  1. **New confirm.** The new run takes over fence ownership in the same status write that accepts it, with no unfenced gap. With a fingerprint match it reuses the artifact (`Reuse` mode, minutes of re-authentication) and goes straight to Preflight/Fencing. Without one it re-stages first, still fenced.
  2. **Accept and release** with `restore-accept-damaged=<runID>`, after proof the failed Job's pods are gone. The primary serves whatever it holds; the queue stays `Held` for #233's exits.
  3. **Rebuild from a Held peer**: a backup with `sourceSiteOverride` from a held site, then a non-PITR full-instance restore from it, which is exit 1 with that source and transfers the fence explicitly. A first-class "promote a held peer" operation (#233 follow-up) would take the fence over the same way.

**What a failure before the DROP rolls back to.** Nothing was damaged. The fence is released only when #233 settlement proves `Discarded` (every Job pod terminated and `gtid_executed == fencedGtidExecuted`); otherwise it is treated as after-DROP. The barrier, if written, stays until a later backup closes it.

## GTID and archive lineage (Q3)

**Keep the live superset.** The live primary keeps its full `gtid_executed`, including the GTIDs of every discarded post-stop transaction, and the restored data is loaded unlogged (`skipBinlog:true`, forced for every full-instance in-place restore; explicit `false` is rejected). Peers inherit the set through CLONE.

**Why not RESET.** `RESET BINARY LOGS AND GTIDS` followed by `replace` would make the live set equal to the staged set, but the server would then reissue GTID numbers that already sit in the archive for the discarded transactions. The archiver's reuse detector (`internal/sidecar/binlog_reuse.go:115-154`) proves the overlap and records `Epoch.ResetReason = "verified GTID namespace reuse ... use a new manifest prefix and full backup"` (`:184-195`); the archive audit then errors for the site (`internal/sidecar/binlog_history.go:76-77`), retention stops (`internal/sidecar/binlog_archiver.go:378`, `:428`), and the planner refuses every window on that site (`cmd/bloodraven/pitr_window.go:115-116`). RESET would turn every in-place restore into a forced archive migration.

**Restore barrier.** One empty transaction with a tagged GTID, `<server_uuid>:bloodraven_restore:<n>`, committed on the primary at Fencing after the application fence and before the Job exists, and archived before the Job is created when archiving is enabled. It is written for every full-instance in-place restore, PITR or not, and on attempts that later fail. Because it is in the GTID stream itself:

- every archive reader sees it in the per-file GTID coverage, including verification Jobs and other groups restoring from this archive;
- a backup taken before the barrier lacks it in `gtidExecuted`; a backup taken after contains it, even when no other write happened in between (a GTID-set boundary such as `fencedGtidExecuted` alone is ambiguous in exactly that case);
- it needs no time comparison across clocks.

Rejected alternatives: reusing `Epoch.ResetReason` (blocks the whole site forever), and a JSON barrier object in the prefix keyed by time or by `fencedGtidExecuted` (ambiguous at equality, written by a second writer into a single-writer layout). Tagged GTIDs need MySQL 8.3+ and the `TRANSACTION_GTID_TAG` privilege; **unverified against `mysql:9.7` in this repo**, to be checked first in PR2. If it does not hold, PR2 falls back to a binlogged statement on a dedicated operator schema, which the planner identifies by event content instead of tag.

**Planner refusal.** `planPITRArchive` (and #206's replacement) refuses with `PITRRestoreBarrier` when any GTID tagged `bloodraven_restore` lies in the replay window and is not in the base backup's set. The check runs before `excludeGtids` is applied, so excluding the barrier cannot bypass it. The staging Job itself is subject to it: an in-place PITR across an earlier restore is refused.

**Post-restore backup.** See Lifecycle step 8. Until it completes, `PITRArchiveReady=False/PITRRestoreBarrier` and an alert make the PITR gap visible.

## Interaction with the rest of the operator

- **Topology manager.** Not frozen during Staging/Staged; emergency failover works. Frozen from Fencing through Resuming and while a damaged fence exists. #233's restore protection (start gate, re-stop, clone admission, promotion exclusion) is unchanged and applies from Fencing.
- **Planned failover.** Allowed during Staging. Preflight's interplay with planned failover is #233's. A planned failover to a held site stays refused.
- **Ordered updates.** Allowed during Staging; caught by the version rechecks. Preflight waits for them (#233).
- **Backups and retention.** Scheduled backups continue during Staging. The source backup is held. While the damaged fence is up, scheduled backups skip held sites and the fenced primary still accepts the backup account; the docs advise suspending schedules, as #233 does for `Held`.
- **Verification.** Unchanged, except that PITR verifications crossing a barrier are refused. The shared mysqld boot moves into a library both scripts source.
- **Encryption.** Inputs are decrypted with the #203 revision. The intermediate artifact is plaintext on the claim (Q1 A). Live `encryptionAtRest` is unaffected: the logical load recreates `ENCRYPTION='Y'` tables under the live keyring.
- **Deployment API.** `operations.restoreInPlace` (`site/content/docs/4.configuration/11.deployment-api.md:132`, `:153`) reports `Staging` and `Staged`; leases are refused with `ConcurrentOperation` for the whole run and with `RestoreReclonePending` while peers are queued or held (#233).
- **Bootstrap restores.** `initFromBackup` gains the source hold (PR3) but no barrier: a new group has no earlier lineage.

## Security

- **Plaintext at rest.** The staging claim holds decrypted inputs (until step j), the staging datadir (until step m) and, for a retained artifact, a plaintext logical dump of the whole instance. Encrypting only the intermediate dump would protect nothing while the datadir sits next to it during the run. Docs recommend an encrypted, replicated StorageClass and namespace RBAC that limits who can mount claims. Retained artifacts are listed in status, counted in a gauge and alerted on.
- **Keyring.** The staging keyring is ephemeral (`/run/mysqld` emptyDir) and dies with the pod; the datadir it protects is deleted before the pod exits.
- **Loopback root.** Passwordless `root@127.0.0.1` is reachable only from containers in the same pod, all of which are operator-built. No Service, no token, no host network, mysqlx off. Pinned by a unit test of the Job spec and the script flags.
- **Supply chain.** Image digests are recorded and reused for the loader, so the mysqlsh that loads the artifact is the binary that wrote it.

## Observability

- **Structured logs** (camelCase fields; `site/content/docs/8.observability/7.log-schema.md` updated in the PR that adds each): INFO `restore staging started`, INFO `restore staging job created`, WARN `restore staging volume is node-local`, WARN `restore staging refused` (`reason`), INFO `restore staging verified` (`runID, manifestSha256, stagedGtidSha256, bytes`), INFO `restore staging artifact reused`, INFO `restore staging artifact deleted`, INFO `restore barrier written` (`runID, site, gtid`), INFO `restore barrier archived`, INFO `restore barrier closed` (`backup`), ERROR `post-restore backup failed`, ERROR `in-place restore left the primary fenced as possibly damaged` (`runID, site, jobName`), INFO `damaged-primary fence released`, INFO `damaged-primary fence transferred`, WARN `unknown in-place restore phase; holding guards`. No existing `msg` changes.
- **Alerts** (documented in `8.observability/2.monitoring.md` and `5.alert-runbook-map.md`): `BloodravenRestoreDamagedPrimaryFenced` (critical, `bloodraven_restore_damaged_primary_fenced == 1`), `BloodravenRestoreStagingRetained` (warning, retained artifacts for more than 24 h), `BloodravenPITRRestoreBarrierOpen` (warning, open for more than 6 h).
- **bloodraven-doctor** (maintenance contract): `SKILL.md` (staging phases, damaged fence, barrier), `references/alert_runbook_matrix.md` (every new reason, Event and alert; `RestoreInPlaceRejected` row at `:49` rewritten), `references/troubleshooting_playbooks.md` (playbook 19 rewritten from "pointInTime rejected" to the in-place PITR procedure; new playbooks for a damaged fence and its three exits, an open barrier, a retained artifact, `StagedGtidsNotInLive`, `StagingVersionMismatch`), `scripts/triage.sh` (staging status, retained list, damage record, barriers), `scripts/deployment-probe.sh` (new phases), `scripts/sidecar-probe.sh` (`/archiver/covers`), `scripts/support-bundle.sh` (staging and probe Job logs, claims).

## Docs that change when implemented

| Page | Change |
|---|---|
| `site/content/docs/7.backup-and-restore/2.backup-restore.md` | Rewrite "Point-in-time recovery is not supported in place" (`:743-771`) into the in-place PITR procedure with the supported-procedure statement first; full-instance phases, `skipBinlog` forced, the damaged fence and exits, barrier and post-restore backup, sizing formula |
| `site/content/docs/3.get-started/1.known-limitations.md` | Replace "in-place cannot PITR" (`:109-114`) with the remaining limits: full-instance only, no foreign source, plaintext claim |
| `site/content/docs/4.configuration/2.crd-reference.md` | `RestoreInPlaceSpec.staging`, new types, phases, status fields, conditions |
| `site/content/docs/7.backup-and-restore/5.backup-encryption.md` | Staging decryption with the #203 revision; plaintext claim |
| `site/content/docs/7.backup-and-restore/6.backup-verification.md` | `PITRRestoreBarrier` refusal |
| `site/content/docs/5.architecture/3.multi-cluster-dr.md`, `6.durability-and-rpo.md` | Keep new-group PITR as the primary procedure; barrier and RPO gap |
| `site/content/docs/8.observability/2.monitoring.md`, `5.alert-runbook-map.md`, `7.log-schema.md` | Events, metrics, alerts, log messages |
| `site/content/docs/4.configuration/11.deployment-api.md` | New phase values |
| `site/content/docs/6.operations/10.upgrade-policy.md`, `9.failure-mode-matrix.md`, `4.configuration/10.production-hardening.md` | No up/downgrade mid-run; damaged fence; `TRANSACTION_GTID_TAG`; encrypted StorageClass |
| `playground/chaos-scenarios.md` | Sections for scenarios 63-67 |

## Test plan

**DST.** `internal/dst` has no restore model today: it drives the real `TopologyManager` with a fake clock and in-memory MySQL, and models neither CLONE, restores, backups nor the in-place controller (`internal/dst/README.md:138-142`; #233 "Not run"). It cannot test staging, which is controller and Job work. PR1 adds the one piece where DST is the right tool: a restore-guard actor that applies the same guard surfaces the reconciler applies (topology freeze, primary fence, pending/held set, damaged record) through `applyRestoreProtection`, at random points of the fault schedule. New invariants: no promotion of a pending or held site; no `START REPLICA` on a held peer; no failover while frozen; a damaged-fenced primary is never unfenced except by an exit event. Fixed findings are pinned in `test/component` as usual.

**Unit (`internal/controller`, `cmd/bloodraven`).**
- Flip `TestReconcileInPlaceRestore_PointInTimeRejected` (`internal/controller/restore_inplace_test.go:1150`) into acceptance into `Staging`, and add rejection tests for `allowPartial`, `allowUnverifiedCrossSite`, per-schema with `pointInTime`, explicit `skipBinlog:false`.
- Keep `TestBuildInPlaceRestoreJob_RefusesPointInTime` (`:1236`) as a loader invariant (the loader never replays), and `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline` (`internal/controller/restore_test.go:537`).
- Staging Job builder: pinned flags, no token, no host network, no Service, AppArmor paths, claim mount, revision Secret, deadline; probe Job; image selection matrix (override, live with mysqlsh, live without, live too old, fallback mismatch, target changed between staging and Fencing).
- Phase machine: every row of the crash table, including write-ahead adoption, Job loss with the 3-attempt cap, supersede, cancel, reuse, `StagedGtidsNotInLive`, loader deadline refusal, unknown phase.
- Damaged fence: Held settlement keeps label, `offline_mode` and freeze after terminal and after spec removal; fence transfer on new confirm without a gap; accept annotation refused while pods live and for a stale runID.
- Barrier: write-ahead adoption, archive wait timeout, closure by any qualifying backup; planner refusal including the `excludeGtids` bypass attempt.
- Cleanup: policy matrix, replacement-after-verify, cleanup annotation refusals, direct claim deletion.
- Scripts under python3/bash with stub mysqlsh (pattern of `internal/controller/restore_script_offline_test.go`, `pitr_restore_script_test.go`): staged-GTID proof cases, sentinel format, manifest writer, `_verify_staging_artifact` refusing before any DROP statement.

**Component (`test/component`).** Real `TopologyManager` with `FakeReplica` (#233's harness): failover during Staging leaves the run unfrozen and rebinding at Fencing; Held peers plus the damaged fence through operator restarts; fence transfer to a new run; accept-and-release.

**Envtest (`test/envtest`).** CEL pairing rule and enum rejection; status round-trip for every new type; claim owner references and garbage collection on group deletion; damaged record and barriers surviving reconciler restarts; restore holds blocking retention and cleanup.

**Real MySQL, opt-in** (pattern of `TestRestoreFence_RealMySQL`, `internal/mysql/restore_fence_mysql_test.go:20`, and `BLOODRAVEN_PITR_NATIVE_TEST`, `internal/controller/pitr_restore_script_test.go:323`): `BLOODRAVEN_STAGING_NATIVE_TEST=1` runs `staging.sh` against a native mysqld/mysqlsh/mysqlbinlog with a generated dump and binlogs, asserting the proof, the read-only sanity check and that the artifact `@.json` equals the proven set; `BLOODRAVEN_TEST_MYSQL_ADMIN_DSN` covers the tagged-GTID barrier and `TRANSACTION_GTID_TAG`.

**Playground scenarios** (IDs after #233's 62):

| ID | Injects | Asserts | Profile |
|---|---|---|---|
| `63-in-place-pitr-restores-to-stop-time` | marker rows before the stop, rows and a dropped table after it | `Succeeded`; pre-stop rows present, post-stop rows absent, dropped table back; peers recloned and replicating; staging claim deleted; barrier closed by the post-restore backup; a PITR verification from the pre-restore backup across the restore is refused `PITRRestoreBarrier`, and one from the post-restore backup passes | **release** |
| `64-in-place-pitr-fails-after-drop-keeps-fence` | deletes the loader pod once the user schemas are gone | `Failed`; `RestoreInPlaceDamagedPrimaryFenced=True`; primary Service has no endpoints, `offline_mode=ON`, topology frozen; queue `Held`, markers on every peer; artifact retained; then a new confirm reuses it (no staging Job, `mode=Reuse`) and ends `Succeeded` | **release** |
| `65-in-place-pitr-planned-failover-during-staging` | planned failover while `Staging` | staging continues; target bound to the new primary at Fencing; `Succeeded` | full |
| `66-in-place-pitr-operator-restart-each-phase` | operator pod deleted in Staging, Staged, Fencing, Restoring | no duplicate Job or claim, no unfenced gap, `Succeeded` | full |
| `67-in-place-pitr-cross-site-window` | failback A to B to A inside the window (needs #206) | every site's transactions present; staged proof equality | full |

Both release scenarios reset MySQL first. #233's scenario 62 moves to `release` in PR1, because the damaged fence otherwise has no CI signal (#236).

## Phased implementation plan

Each PR passes the AGENTS.md pre-PR gate on its own, ships its own docs, log-schema and doctor updates, and leaves `pointInTime` rejected until PR6.

| PR | Scope | Depends on | Size |
|---|---|---|---|
| PR1 | Damaged-primary fence for every full-instance restore: damage record, condition, gauge, alert, fence transfer on new confirm, accept-and-release annotation and plugin command, unknown-phase fail-safe, DST restore-guard actor, scenario 62 to release | #233 | M (~2k lines with tests) |
| PR2 | Restore barrier (tagged GTID, archive wait, sidecar `/archiver/covers`), planner `PITRRestoreBarrier`, automatic post-restore backup, `PITRArchiveReady` reason, `TRANSACTION_GTID_TAG` preflight, forced `skipBinlog:true` for full-instance restores | #233; coordinate with #206 so its planner keeps the check | M-L |
| PR3 | Restore holds on source `MysqlBackup`s for bootstrap and in-place restores, generalizing #232 (fixes today's gap where retention can prune a backup a restore is reading) | none | S-M |
| PR4 | API: `RestoreStagingSpec`, status types, phases, CEL, generated CRDs copied to the chart, CRD reference; reconcile-time rejection unchanged | v1beta1 types merged | S |
| PR5 | Staging: probe Job, claim, staging Job and `staging.sh`, proof, manifest, sentinels, Staged verification, retention and cleanup, reuse re-auth Job, `persistentvolumes` RBAC and chart mirror. Behind a hidden operator flag `--experimental-in-place-pitr-staging` (off by default); with it on, a run stops at `Staged` and ends `Failed`/`RestoreInPlaceStagingOnly`. No API dry-run mode: rehearsal is what `MysqlBackupVerification` with `pointInTime` is for | #206, #203, PR3, PR4 | L |
| PR6 | Hand-off: Preflight and Fencing rechecks, loader from the claim with `_verify_staging_artifact`, artifact reuse after a post-DROP failure, removal of the flag and the rejection, docs procedure, doctor playbooks, scenarios 63 and 64 in release | PR1, PR2, PR5 | L |
| PR7 | Scenarios 65-67, remaining docs and known-limitations cleanup | PR6 (#206 for 67) | M |

## Decisions

Settled by a many-brain-one-decision debate between GPT 6.1 Sol (paranoid-security lens) and Claude Opus 5.5 (pragmatic-operator lens), three rounds. Q2-Q6, Q8-Q10 were unanimous; Q7 converged in round 3; **Q1 stayed 1-1 and was settled by the host tiebreak, pending Colin's sign-off.**

| Q | Chosen | Rejected, and why |
|---|---|---|
| Q1 data path | **A**: intermediate `util.dump_instance` dump plus authenticated manifest on the operator-provisioned staging claim; the loader is today's `load_dump` path reading it and verifies the manifest before the DROP; the operator checks the status digest before Fencing; node-local Warning; docs recommend a replicated class. Tiebreak: average score A 8.0 vs C 7.5, least-regret minimum A 8 vs C 6. **Dissent (GPT):** C keeps a repair source if the staging volume dies during the post-DROP load, which is one event, not two. **C is the named upgrade path** if node-local staging proves a problem. | B (`util.copyInstance` from a re-booted staging datadir): a new tool and a second mysqld in the riskiest window. C (encrypted operator artifact in backup storage): permanent artifact class with retention, pruning, key-revision references and S3 cleanup on group deletion, which pays off only when backup storage is a separate failure domain. |
| Q2 fence timing | **A**: staging before Preflight, unfenced, failover available; target bound at Fencing; writes during staging discarded and announced | B fence first: hours of outage with no data benefit |
| Q3 lineage | **A**: live superset, forced unlogged load, barrier before destructive execution, planner refusal, automatic post-restore backup, `PITRRestoreBarrier`; applies to non-PITR full-instance restores | B manual backup: a forgotten step is a silent wrong PITR. C RESET + replace: GTID reuse trips the archiver halt |
| Q4 retry after DROP | **A**: no automatic retry; a new confirm reuses a fingerprint-matched, re-authenticated artifact; retry requires the previous Job's pods gone | B always re-stage: prolongs a damaged outage. C auto-retry: destructive action without a human |
| Q5 scope | **A**: full-instance only | B per-schema: needs its own semantics |
| Q6 volume | **A**: required size, optional class, fixed RWO, per-run claim, in-Job ownership check, sequential ownership | B auto-size: compressed bytes do not predict need. C exposed access modes: no use case |
| Q7 image/version | **D**: live digest if it carries mysqlsh, else backup image, override wins; exact `VERSION()` equality, not older than the dump; checked before Fencing, at Fencing and before the DROP; loader uses the staging image | A older-only check: allows a cross-series downgrade. B equality on the backup image: patch churn. C same major.minor: newer-patch dump into an older target is unproven |
| Q8 isolation | **A**: loopback, mysqlx off, no Service, no token, no host network, pinned by test | B NetworkPolicy: RBAC and chart cost, CNI-dependent, cannot isolate the pod's own containers |
| Q9 cleanup | **A'**: `Delete`/`RetainOnFailure` (default retain), success deletes, reuse or delete-after-replacement, runID-valued cleanup annotation plus plugin, tolerated direct deletion, visible retained artifacts | bool: third state. TTL: deletes the repair source at 3am |
| Q10 damaged primary | **A**: durable fence on Held settlement with three exits; separate prerequisite PR | B unfence on failure: splits writes across two inconsistent histories |

Decisions taken in this proposal that the debate did not settle (for review):

1. `staged set ⊄ live gtid_executed`: **refuse** (`StagedGtidsNotInLive`), not `gtid_purged='+diff'`; therefore foreign-source in-place PITR is always refused.
2. The barrier is an **in-band tagged GTID** (`bloodraven_restore`), archived before the Job when archiving is enabled; needs `TRANSACTION_GTID_TAG` for all full-instance restores.
3. `skipBinlog:true` is forced for **all** full-instance in-place restores, not only PITR ones.
4. New deployment leases stay refused during Staging/Staged (existing behaviour), while the `BeginPlanned` wait stays in Preflight.
5. `Delete` policy keeps the artifact while its run's damaged fence exists.
6. Phases `Staging` and `Staged`; a probe Job for image selection; reuse re-authenticated by a separate Job before Fencing.
7. Staging Job loss is re-created automatically up to 3 times (no damage possible); staging Job failure is terminal.
8. Preflight refuses when the loader deadline is below 1.5x the measured staging load.
9. A superseding confirm interrupts a run still in Staging/Staged (today's runs are never interrupted once a Job exists, but staging is not destructive).
10. PR5 lands behind a hidden operator flag rather than an API dry-run mode.
11. Staging data on the claim under `<runID>/`, created by the Job, so root-owned local-path mounts work if they are writable by uid 27.

## Open questions

1. **Q1 sign-off.** A versus C (see Decisions) is pending Colin's review.
2. **Tagged GTIDs on `mysql:9.7`.** `SET GTID_NEXT='AUTOMATIC:<tag>'` and `TRANSACTION_GTID_TAG` are expected from MySQL 8.3 but are not exercised anywhere in the repo. PR2 verifies first; the fallback is described under GTID lineage.
3. **local-path ownership in the playground.** The design assumes the k3d local-path provisioner creates claim directories writable by uid 27 (its default setup script uses mode 0777) so the Job can create its own `<runID>/` directory. Unverified here; PR5 checks it on k3d. If it fails, the playground gets a StorageClass with a setup script that does so; the operator does not gain a root init container.
4. **mysqlsh in `mysql:9.7`.** Unknown; handled by the probe. Recording the measured answer in the docs is a PR5 task.

## Acceptance criteria

1. With `pointInTime` and `staging`, a full-instance in-place restore reaches `Succeeded` with exactly the pre-stop dataset (scenario 63 in release).
2. Every PITR refusal (coverage, proof, versions, volume, `StagedGtidsNotInLive`) happens before the fence, and the reason is in status and an Event.
3. A failure after the DROP leaves the primary fenced and frozen with `RestoreInPlaceDamagedPrimaryFenced=True`, the peers `Held` with their data, and the artifact retained; a new confirm reuses it (scenario 64 in release).
4. Nothing destructive runs without a new confirm; a Job is never rebuilt after its write-ahead without one.
5. No live `RESET`; every full-instance restore writes an archived barrier before its Job; the planner refuses windows across it; `PITRArchiveReady` reports the barrier until a post-restore backup completes.
6. `allowPartial`, `allowUnverifiedCrossSite`, per-schema PITR and `skipBinlog:false` are refused without consuming the confirm.
7. The staging pod has no token, Service or host network and binds loopback with mysqlx off (unit test).
8. Retained artifacts appear in status, a gauge and an alert, and are deleted only by success, replacement after verification, the policy, or the runID-valued annotation.
9. CRDs regenerated and copied to the chart; ClusterRole mirrors `persistentvolumes get`; log-schema, doctor files and the docs listed above updated in the PRs that change them.
10. Merge gated on #206, #203 and #233, and on PR1 and PR2.
