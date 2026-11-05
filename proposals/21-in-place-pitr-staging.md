# Proposal: In-Place Point-in-Time Restore Through a Staging Instance

**Issue:** [#205](https://github.com/ShipStream/bloodraven/issues/205)
**Status:** Draft
**Branch:** `docs/in-place-pitr-proposal`

All names introduced here (fields, phases, reasons, Events, annotations, metrics, log messages, scripts, GTID tags) are **proposed**. Citations are `path:line` on `main` at `418cd4b` unless marked `origin/fix/issue-212:` (PR #233, open, head `4e72753`). MySQL behaviour cited from the MySQL 9.7 Reference Manual is marked **unverified in repo** where nothing in this repository exercises it yet.

## Supported procedure

**Point-in-time recovery into a new failover group (`spec.initFromBackup.pointInTime`) remains the supported production procedure.** It leaves the damaged group untouched until the application cuts over, so a failed or wrong restore costs nothing. In-place PITR, as designed here, is for constrained environments where a second group cannot be created (quota, licensing, fixed endpoints, a single storage pool). It does not merge while the PITR planner can skip another site's transactions (#206), and it always discards every write committed after the stop time, including writes accepted while the staging instance is being built.

## Motivation

`spec.restoreInPlace.pointInTime` exists in the API but every run that sets it is rejected before anything is fenced (`internal/controller/restore_inplace.go:262-274`, `:288-290`; field comment `api/v1alpha1/backup_types.go:802-811`). #197 removed the old behaviour because it was silently wrong: the restore Job piped archived binlogs into the live primary, whose `gtid_executed` already contained every one of those GTIDs (it executed or replicated them), so MySQL skipped the whole replay and the run reported `Succeeded` with the data at the backup. Installing the dump's GTID set first, as new-group PITR does (`internal/controller/restore.go:931-960`), is impossible on a live server: `updateGtidSet:"replace"` requires the dump's set to be disjoint from what the server has executed.

A staging instance removes the conflict. The dump is loaded into an empty, throwaway `mysqld` whose GTID state starts at the dump's own baseline, the archive is replayed there with normal GTID deduplication, the result is proven against the replay plan, and only that verified result is loaded into the live primary through today's full-instance path (fence, peer hold, DROP, unlogged load, peer reclone). The live primary never receives a binlog replay, so there is nothing to skip.

What users get: a rewind of a whole instance to a proven point in time, in the same group. Staging runs before the fence, so the outage is the fence, a few bounded checks, the artifact re-hash, DROP and load. Peers keep their pre-restore data if the load fails. After a failure that may have touched the primary, the primary stays fenced instead of serving a half-loaded dataset.

## Goals

1. Full-instance in-place PITR (`restoreInPlace.pointInTime` with empty `loadOptions.includeSchemas` and `excludeSchemas`, from a `mysqlBackupRef` source) through a staging `mysqld` in an operator-built Job.
2. Every PITR refusal that can be decided from the backup, the archive and the staged result (coverage, cross-site order, GTID proof, versions, volume, subset of live history, loader budget) lands before the fence. The checks repeated after the fence, against the bound target, are bounded (seconds to minutes, plus the archive wait) and release the fence when they refuse.
3. The outage is today's full-instance outage plus bounded additions: the Fencing rechecks, the barrier archive wait (at most 10 minutes), the #233 peer stop (at most 2 minutes) and the loader's artifact re-hash. Staging, replay and the intermediate dump run while the group serves traffic and emergency failover stays available.
4. A loader Job that may have touched the primary leaves it fenced, the peers held with pre-restore data, and a verified artifact that a new confirm can reuse without re-staging. Only a loader that exits through its explicit pre-destruction refusal path counts as harmless.
5. Archive lineage stays correct: no GTID reuse; an in-band barrier before destructive execution and an in-band completion marker after it, both enforced by the planner; an automatic post-restore full backup.
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
| P1 | **#206** chronological cross-site replay with an exact GTID continuity proof, and a writer-stamped source-archive identity the operator can read for a profile without running a plan | Without it a window spanning a failback (A to B to A) can skip the other site's transactions. The staged GTID proof needs #206's plan to emit the exact replay set across sites, and artifact reuse needs the archive identity before it decides. |
| P2 | **#203** encryption identity bound to the prefix, with an operator-owned immutable Secret revision | The staging Job must mount the accepted revision, not the user Secret, so a Secret edit during a multi-hour run cannot change what the Job decrypts. The revision is part of the reuse fingerprint. |
| P3 | **#233** (open): peer `STOP REPLICA IO_THREAD` at Fencing, `fencedGtidExecuted`, Held vs Discarded settlement | Without it the peers replicate the DROPs and a post-DROP failure damages every site. Settlement is the evidence the damaged-primary fence keys on. |
| P4 | **v1.6.0 target-coverage rule** for the staged run: the staging verdict must be `targetCovered=true` (`cmd/bloodraven/pitr_download.go:388-414`) | An in-place restore to an unproven point is a silent partial restore. `allowPartial` and `allowUnverifiedCrossSite` (`api/v1alpha1/backup_types.go:219-234`) are refused for in-place runs at reconcile time. |
| P5 | **Damaged-primary fence** (Q10) with the write-ahead damage boundary below, shipped as its own PR (PR1) | Today a failed restore unfences the primary ("Even on failure we unfence", `internal/controller/restore_inplace.go:925-927`). #233 settlement cannot close the gap alone: an unlogged load into a primary with no user schemas leaves `gtid_executed` unchanged, which #233 itself flags (`origin/fix/issue-212:internal/controller/restore_reclones.go:771-774`). That gap exists for non-PITR full-instance restores too. |
| P6 | **Restore barrier, completion marker and automatic post-restore backup** (Q3), shipped as its own PR (PR2) | Today's non-PITR full-instance restores already break lineage silently: a PITR from a pre-restore backup across the restore replays post-restore binlogs onto pre-restore data. |

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
	// outcome, except while a non-terminal run references it or
	// status.restoreDamage exists (from the loader write-ahead on).
	RestoreStagingDelete RestoreStagingCleanupPolicy = "Delete"
	// RestoreStagingRetainOnFailure deletes it after success and keeps a
	// Verified artifact after any other outcome so a new confirm can reuse
	// it. Incomplete artifacts are deleted under both policies.
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

	// Resources are forwarded verbatim to the staging container.
	// innodb_buffer_pool_size is half the memory limit, else half the
	// memory request, else the mysqld default (128M) with a Warning.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// CleanupPolicy controls the staging volume after the run ends.
	// +kubebuilder:default=RetainOnFailure
	// +optional
	CleanupPolicy RestoreStagingCleanupPolicy `json:"cleanupPolicy,omitempty"`

	// ActiveDeadlineSeconds bounds the staging Job (load, replay, proof,
	// intermediate dump, manifest).
	// +kubebuilder:default=21600
	// +kubebuilder:validation:Minimum=600
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// LoaderActiveDeadlineSeconds bounds the destructive load Job of a
	// PITR run. Unset: the loader Job has no deadline (a kill after the
	// DROP is a damaged primary; a slow load may still finish), and the
	// loader refuses on its own before the DROP if it has not reached it
	// within its pre-destruction budget. spec.backup.activeDeadlineSeconds,
	// which backup Jobs share, is not used for this Job.
	// +kubebuilder:validation:Minimum=600
	// +optional
	LoaderActiveDeadlineSeconds *int64 `json:"loaderActiveDeadlineSeconds,omitempty"`

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
- **Everything else is reconcile-time** (`RestoreInPlaceRejected`, confirm not consumed), checked in the acceptance pass: `allowPartial: true`; `allowUnverifiedCrossSite: true`; `pointInTime` with a non-empty `includeSchemas`; a non-empty `excludeSchemas` on **any** full-instance restore (the DROP removes every user schema, so an excluded schema is destroyed and never reloaded, while the restore reports success); explicit `loadOptions.skipBinlog: false` on any full-instance restore; for PITR runs, `loadIndexes: false` (the artifact's DDL comes from the staged server, so missing secondary indexes would be carried into the live primary); a `source` other than `mysqlBackupRef` (only a referenced backup has the trusted GTID bound PITR requires, `internal/controller/restore.go:161-170`, `:1585-1594`); `validatePITRBackupBefore` against the backup's `completionTime` (`internal/controller/restore.go:1557-1573`); `excludeGtids` that does not parse or that intersects the backup's `status.gtidExecuted` (an exclusion cannot remove a transaction the dump already contains); `excludeGtids` naming any `bloodraven_restore*` GTID. A size below a floor is a Warning only (see Lifecycle).
- **Which load options reach which Job.** The only user field honoured is `loadOptions.threads` (and `resetProgress`, a no-op today). Staging load: `{threads, loadIndexes:true, updateGtidSet:"replace", skipBinlog:true, loadUsers:true, excludeUsers:[root and system accounts]}` (accounts are needed for the replay, see Staging step d). Live loader: `{threads, loadIndexes:true, skipBinlog:true, loadUsers:false}` and no `updateGtidSet`. Accounts, grants and the rest of the `mysql` schema are therefore **not rewound**: the restored primary keeps its current accounts.
- **Access mode is fixed `ReadWriteOnce` and not exposed** (Q6). One pod writes the volume, then one pod reads it, strictly in sequence, which the controller enforces. No use case needs RWX, and RWX classes (NFS) are a poor fit for a mysqld datadir. A future `accessModes` field would be additive.
- **`size` is a plain required `resource.Quantity`**, with no CEL floor. The CEL quantity library is version-dependent; the useful floor depends on the source size, which only the reconciler knows.
- **`storageClassName` is a pointer** because absence (cluster default) and empty string differ.
- **Credentials: no spec field.** The staging `mysqld` is created with `--initialize-insecure` and reached only as `root@127.0.0.1` from inside the pod, exactly as verification does (`internal/controller/verify_script.sh:220-247`). It never talks to the live group. The dump's accounts are loaded into staging only so that account statements in the replay window apply; each loaded account is then `ACCOUNT LOCK`ed before the replay, and none can connect anyway because the server listens on loopback inside a pod that runs only operator-built containers. The intermediate dump is taken with `users: false` and the live loader uses `loadUsers: false`. A configurable account would add a Secret to leak and protect nothing loopback does not.
- **Network isolation: no spec field** (Q8). The staging server binds `127.0.0.1` with `--mysqlx=OFF`, the pod has no Service, `automountServiceAccountToken: false`, `hostNetwork: false`, and runs only operator-built containers. No NetworkPolicy: it would add RBAC and a chart mirror, depends on the CNI enforcing it, and cannot isolate containers sharing the pod network namespace, which is the only place loopback is reachable from. The flags are pinned by a unit test.
- **No new pull-secret field.** Staging and probe Jobs use `spec.backup.imagePullSecrets` (`api/v1alpha1/backup_types.go:35-36`). If the live image cannot be pulled with them, the probe fails and selection falls back to `spec.backup.image` (recorded in status).
- **`RetainOnFailure` is the default** because a retained, verified artifact is the fastest repair source after a post-DROP failure. A boolean was rejected (third state), and a TTL was rejected (it deletes the repair source at 3am).

### Status

New `RestoreInPlacePhase` values (enum at `api/v1alpha1/backup_types.go:837-838` becomes `"";Staging;Staged;Preflight;Fencing;Restoring;Resuming;Succeeded;Failed`):

| Phase | Meaning |
|---|---|
| `Staging` | The run is accepted with `pointInTime`. The operator holds the source, probes the image, decides `Build` or `Reuse`, and runs the staging Job (`Build`, on a new claim) or the re-authentication Job (`Reuse`). No deployment hold, no topology freeze, no queue, no fence. |
| `Staged` | The operator verified the Job's result (termination sentinel, logged sets against their digests, versions, verdict) and certified the artifact. One-step phase; the run moves to `Preflight` on the next reconcile. |

Existing phases keep their meaning; PITR runs reach `Preflight` only from `Staged`. Non-PITR runs still start at `Preflight`.

```go
// status.restoreInPlace gains the run's pinned request and staging state.
type RestoreInPlaceStatus struct {
	// ... existing fields ...
	// Kind is pinned at acceptance: "full", "schema" or "pitr".
	Kind string `json:"kind,omitempty"`
	// RequestSHA256 is the accepted request identity (see "Request
	// identity"). Written in the acceptance write; never changed.
	RequestSHA256 string `json:"requestSHA256,omitempty"`
	// Request is the canonical JSON the digest is computed over.
	Request string `json:"request,omitempty"`
	// +optional
	Staging *RestoreStagingRunStatus `json:"staging,omitempty"`
}

// RestoreStagingRunStatus is per run. Artifacts are not stored here: they
// outlive runs and live in status.restoreStagingArtifacts, so a new run
// replacing status.restoreInPlace never loses track of a claim.
type RestoreStagingRunStatus struct {
	Mode            RestoreStagingMode `json:"mode,omitempty"`            // set in Staging, after the probe
	ArtifactRunID   string             `json:"artifactRunID,omitempty"`   // key into restoreStagingArtifacts (own build or reuse source)
	ProbeJobName    string             `json:"probeJobName,omitempty"`
	JobName         string             `json:"jobName,omitempty"`         // staging, re-auth or result-read Job of the current attempt
	JobCreating     bool               `json:"jobCreating,omitempty"`     // write-ahead
	Attempt         int32              `json:"attempt,omitempty"`         // incremented in the write-ahead; pods carry it as a label
	ProvisioningSince *metav1.Time     `json:"provisioningSince,omitempty"` // durable start of claim/scheduling waits
	Image           string             `json:"image,omitempty"`
	ImageDigest     string             `json:"imageDigest,omitempty"`
	ImageSource     RestoreStagingImageSource `json:"imageSource,omitempty"`
	MysqlshVersion  string             `json:"mysqlshVersion,omitempty"`
	TargetServerVersion string         `json:"targetServerVersion,omitempty"` // live target at the last check
	VersionGate     *RestoreVersionGate `json:"versionGate,omitempty"`    // see StagingVersionMismatch
	Reason          string             `json:"reason,omitempty"`
	Message         string             `json:"message,omitempty"`
}

// RestoreVersionGate records the (candidate image, live version) pair a
// version refusal was made for. A terminal run refused for a version
// mismatch is not restarted by its unconsumed confirm while the pair is
// unchanged.
type RestoreVersionGate struct {
	ImageRef      string `json:"imageRef"`
	TargetVersion string `json:"targetVersion"`
}

type MysqlFailoverGroupStatus struct {
	// ... existing fields ...
	// RestoreStagingArtifacts lists every staging claim the operator owns,
	// from the claim write-ahead until the claim is observed gone.
	// +listType=map
	// +listMapKey=originRunID
	// +kubebuilder:validation:MaxItems=8
	// +optional
	RestoreStagingArtifacts []RestoreStagingArtifact `json:"restoreStagingArtifacts,omitempty"`

	// RestoreBarriers lists restore markers whose lineage gap is still
	// open (see "GTID and archive lineage"). Display and trigger only: the
	// planner reads the markers from the GTID stream, never from here.
	// +listType=map
	// +listMapKey=runID
	// +kubebuilder:validation:MaxItems=32
	// +optional
	RestoreBarriers []RestoreBarrierStatus `json:"restoreBarriers,omitempty"`

	// RestoreDamage is the damaged-primary record. Top-level: it is not
	// part of restoreOfflineFence, which existing code replaces and clears.
	// +optional
	RestoreDamage *RestoreDamageStatus `json:"restoreDamage,omitempty"`
}

// +kubebuilder:validation:Enum=Build;Reuse
type RestoreStagingMode string

// +kubebuilder:validation:Enum=Override;LiveTarget;BackupImage
type RestoreStagingImageSource string

// +kubebuilder:validation:Enum=Provisioning;Building;Verified;Invalid;Deleting
type RestoreStagingArtifactState string

// RestoreStagingArtifact is one claim and the artifact on it. Immutable
// provenance (originRunID, pvcUID, path, certification) is written once.
type RestoreStagingArtifact struct {
	OriginRunID    string `json:"originRunID"`            // run that built it; never reused
	PVCName        string `json:"pvcName"`                // written before Create (write-ahead)
	PVCUID         string `json:"pvcUID,omitempty"`
	Path           string `json:"path,omitempty"`         // "<originRunID>/artifact": manifest.json + dump/
	LocalNode      string `json:"localNode,omitempty"`    // bound PV pinned to one node
	CleanupPolicy  RestoreStagingCleanupPolicy `json:"cleanupPolicy,omitempty"` // policy of the origin run
	State          RestoreStagingArtifactState `json:"state"`
	RequestSHA256  string `json:"requestSHA256,omitempty"`  // origin run's request identity
	CertificationSHA256 string `json:"certificationSHA256,omitempty"` // see "Artifact certification"
	SourceDumpSHA256 string `json:"sourceDumpSHA256,omitempty"` // source @.json
	ArchiveIdentity string `json:"archiveIdentity,omitempty"` // #206
	EncryptionRevision string `json:"encryptionRevision,omitempty"` // #203 revision, or "plaintext"
	ImageDigest    string `json:"imageDigest,omitempty"`
	ServerVersion  string `json:"serverVersion,omitempty"`  // staging SELECT VERSION()
	DumpServerVersion string `json:"dumpServerVersion,omitempty"` // source @.json serverVersion
	FormatVersion  int32  `json:"formatVersion,omitempty"`  // operator's staging/proof/manifest format
	ManifestSHA256 string `json:"manifestSHA256,omitempty"`
	ArtifactBytes  int64  `json:"artifactBytes,omitempty"`
	BaselineGtids  string `json:"baselineGtids,omitempty"`      // B, from the source @.json
	StagedGtidExecuted string `json:"stagedGtidExecuted,omitempty"` // S, in full
	StagedGtidSHA256 string `json:"stagedGtidSHA256,omitempty"`
	StagingLoadSeconds int64 `json:"stagingLoadSeconds,omitempty"`
	HashBytesPerSecond int64 `json:"hashBytesPerSecond,omitempty"` // measured while writing the manifest
	VerifiedAt     *metav1.Time `json:"verifiedAt,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Message        string `json:"message,omitempty"`
}

// +kubebuilder:validation:Enum=Writing;Written;Archived
type RestoreMarkerState string

type RestoreMarker struct {
	GTID  string             `json:"gtid"`  // chosen and recorded before it is committed
	State RestoreMarkerState `json:"state"`
	At    *metav1.Time       `json:"at,omitempty"`
}

type RestoreBarrierStatus struct {
	RunID      string         `json:"runID"`
	Site       string         `json:"site,omitempty"`
	Barrier    RestoreMarker  `json:"barrier"`             // <uuid>:bloodraven_restore:<n>
	Completion *RestoreMarker `json:"completion,omitempty"` // <uuid>:bloodraven_restored:<n>
	Void       *RestoreMarker `json:"void,omitempty"`       // <uuid>:bloodraven_restore_void:<n>
	BaselineBackup *corev1.ObjectReference `json:"baselineBackup,omitempty"` // name + uid
	Message    string         `json:"message,omitempty"`
}

// RestoreDamageStatus: the primary may hold a partial or dropped dataset.
type RestoreDamageStatus struct {
	OriginRunID string      `json:"originRunID"` // first run whose loader may have touched the primary
	RunID       string      `json:"runID"`       // run that owns the record now (origin, or an exit-1 run)
	Site        string      `json:"site"`
	// Jobs are every loader Job (name + UID) that may have touched the
	// primary since originRunID. Exits require all of them stopped.
	// +kubebuilder:validation:MaxItems=8
	Jobs        []corev1.ObjectReference `json:"jobs"`
	Since       metav1.Time `json:"since"`
	Confirmed   bool        `json:"confirmed,omitempty"` // the Job ended without a pre-destruction refusal proof
	Reason      string      `json:"reason,omitempty"`
}
```

`stagedGtidExecuted` is stored in full, as `sourceGtidExecuted` and `targetGtidExecuted` already are (`api/v1alpha1/backup_types.go:1016-1105`). The staging Job may report at most 1 MiB for it (`StagingFailed` above that). A `status.pitr` summary is unchanged.

### Conditions

| Type | Status/Reason | When |
|---|---|---|
| `RestoreInPlaceDamagedPrimaryFenced` (new) | `True` / `RestoreFailedAfterJob` | `status.restoreDamage` exists and its owning run is terminal. Message names the origin run, the loader Jobs, the exits and the Verified artifacts. `False` / `Released`, `Repaired` after an exit. |
| `PITRArchiveReady` (existing, `internal/controller/pitr_archive_condition.go:29`) | `False` / `PITRRestoreBarrier` (new reason) | An entry in `status.restoreBarriers` is open. Message names the barrier and completion GTIDs and the post-restore backup. Precedence: below `ArchiverError`, above `Archiving`. |
| `Ready` | `False` while `status.restoreDamage` exists | Same mechanism #233 uses for `Held`. |

### Reasons and Events

Stable reason strings, used in status messages and Events. "Consumed" follows one rule: a refusal in the acceptance pass never consumes the confirm; any later refusal does, except the version refusals made before a loader Job exists (Q7), which are gated so that the unconsumed confirm cannot restart an identical run (`versionGate`). Without the rule, an unconsumed refusal restarts the run on the next reconcile (`internal/controller/restore_inplace.go:333-351`), which for staging means hours of work repeated in a loop.

| Reason | Where | Confirm consumed |
|---|---|---|
| `RestoreInPlaceRejected` (existing) | acceptance-pass refusals listed above, including a full `restoreStagingArtifacts` list | no |
| `StagingVersionMismatch` | probe, Staged check, Preflight, Fencing, and the operator's check immediately before the loader write-ahead | no; not restarted while `versionGate` matches the current (candidate image, live version) |
| `StagingTargetNotCovered` | staging verdict `targetCovered=false` | yes |
| `StagingGtidProofFailed` | staged set differs from the exact plan, or `excludeGtids` intersects the dump baseline `B` | yes |
| `StagedGtidsNotInLive` | `S − L` non-empty; the message splits `D ∩ B` from `D − B` (see Preflight) | yes |
| `StagingKeyringUnavailable` | encrypted tables in the dump, keyring component unavailable | yes |
| `StagingVolumeNotWritable` | in-Job ownership/writability check | yes |
| `StagingVolumeUnavailable` | provisioning timeout, claim lost or replaced (UID), node gone or unschedulable before the loader write-ahead | yes |
| `StagingArtifactMismatch` | manifest or file digest mismatch (re-auth Job, result-read Job) | yes |
| `StagingLoaderDeadlineTooShort` | `loaderActiveDeadlineSeconds` set and below the loader budget (Preflight) | yes; the Verified artifact stays reusable |
| `StagingFailed` | staging Job failed for any other reason, or its result could not be read after 3 result-read attempts | yes |
| `RestoreInPlaceStagingOnly` | PR5 flag on: the run stops after `Staged` | yes |
| `RestoreInPlaceSuperseded` | the spec's request identity differs from the accepted one before the loader write-ahead (with or without a confirm bump) | no; the next pass starts a run from the current spec |
| `RestoreBarrierNotArchived` | barrier binlog not archived within 10 min at Fencing | yes (no Job) |
| `RestoreInPlacePreDestructionRefused` | the loader exited through its pre-destruction refusal path (manifest, file digests, version, offline fence, local_infile, start deadline, pre-destruction budget); detail in the sentinel | yes (a Job ran) |
| `PITRRestoreBarrier` | planner refusal and `PITRArchiveReady` reason | n/a |

New Events: `RestoreInPlaceStagingStarted`, `RestoreInPlaceStagingJobCreated`, `RestoreInPlaceStagingVolumeNodeLocal` (Warning), `RestoreInPlaceStagingVolumeSmall` (Warning), `RestoreInPlaceStagingResourcesUnset` (Warning), `RestoreInPlaceStaged`, `RestoreInPlaceStagingReused`, `RestoreInPlaceStagingFailed` (Warning), `StagingVersionMismatch` (Warning), `RestoreInPlaceSpecEditDeferred`, `RestoreInPlaceWritesDiscarded` (Normal, at Fencing: requested stop time and actual fence time), `RestoreBarrierWritten`, `RestoreBarrierArchived`, `RestoreCompletionWritten`, `RestoreBarrierVoided`, `RestoreBarrierClosed`, `PostRestoreBackupCreated`, `PostRestoreBackupFailed` (Warning), `RestoreInPlaceDamagedPrimaryFenced` (Warning), `RestoreInPlaceDamagedPrimaryReleased`, `RestoreInPlaceDamageTakenOver`, `ScheduledBackupSkippedDamagedPrimary` (Warning), `RestoreStagingRetained`, `RestoreStagingDeleted`, `RestoreStagingVolumeGone` (Warning), `RestoreStagingCleanupRejected` (Warning), `RestoreInPlaceUnknownPhase` (Warning).

### Annotations (one-shot, on the `MysqlFailoverGroup`, consumed and cleared like `RecloneAnnotation`)

| Annotation | Value | Effect |
|---|---|---|
| `bloodraven.shipstream.io/restore-staging-cleanup` | artifact `originRunID` | Deletes that artifact's claim (UID-conditional delete, state `Deleting`). Refused (Event, annotation cleared) if no such artifact exists or a non-terminal run references it. Allowed while `status.restoreDamage` exists, with a Warning that it removes a repair source. RunIDs are UUIDs and never reused, so a stale value cannot hit a later artifact. |
| `bloodraven.shipstream.io/restore-accept-damaged` | `status.restoreDamage.originRunID` | Releases the damaged-primary fence (exit 2). Refused unless the value matches, no run is in `Preflight` through `Resuming`, and an uncached read shows every Job in `restoreDamage.jobs` gone or explicitly terminal and every pod labelled with it `Succeeded`/`Failed` (the #232 `verificationExecutionStopped` rule, `internal/controller/backup_verification_hold.go:62`). A `Held` queue stays `Held` for #233's exits. If settlement had discarded the queue (the peers resumed, which an unlogged load allows), the same write arms a `Held` queue of every peer: their pre-restore data no longer matches the primary the administrator accepted. |

`kubectl bloodraven restore-staging list|cleanup <group> --run <id>` (new plugin subcommand, `cmd/kubectl-bloodraven/`) reads the artifact list and sets the cleanup annotation. `kubectl bloodraven restore accept-damaged <group> --run <id>` sets the other.

A hold annotation is added to the source `MysqlBackup` from the Staging step 1 write until the run is terminal, no pod of the run can still read the source, and no `status.restoreDamage` names the run (the source is the re-staging input of a repair): `hold.restore.shipstream.io/<runID>` = group name, mirroring the verification hold prefix (`internal/controller/backup_verification_hold.go:29`).

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
  restoreInPlace:
    confirm: "2026-11-20T03:10:00Z"   # strictly newer than status.restoreInPlace.confirmTokenUsed
    source:
      mysqlBackupRef:
        name: orders-nightly-20261119
    loadOptions:
      threads: 8                       # the only honoured field; skipBinlog/loadIndexes forced true
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
      # loaderActiveDeadlineSeconds: unset -> no loader Job deadline; the loader refuses
      # on its own if it has not reached the DROP within its pre-destruction budget
      sanityCheck:
        query: "SELECT COUNT(*) FROM orders.order_header WHERE created_at < '2026-11-20 01:42:00'"
        expect: {minRows: 1}
```

## Lifecycle

```
                   pointInTime set                                 no pointInTime
 spec accepted ───────────────┐  (request identity pinned)             │
                              v                                        │
                 ┌──────── Staging ────────┐  (unfenced, no hold,      │
   superseded /  │ hold, probe, reuse?,    │   failover allowed)       │
   cancelled  <──┤ claim+Job, result       ├──> Failed (refused /      │
                 └────────────┬────────────┘     staging failed)       │
                              v                                        │
                           Staged ──(certify artifact)─────┐           │
                                                           v           v
                                                       Preflight <─────┘
                                                           │ hold, arm queue, record fence
                                                           v
                                                        Fencing  label, offline_mode, rechecks,
                                                           │      barrier, peer IO stop, archive wait
                                                           v
                                                       Restoring  recheck, write-ahead (jobCreating
                                                       │       │   + damage record), loader
                                       Job succeeded   │       │ Job failed / lost
                                                       v       v
                                                  Resuming   Failed ──> damage evaluation + settlement (#233)
                                     completion marker │                 │ refusal proven -> void barrier, release
                                                       v                 │ otherwise -> damaged-primary fence
                                                  Succeeded ──> post-restore backup closes barrier
```

### Request identity and artifact certification

**Request identity** is what the user asked for. At acceptance the operator writes `status.restoreInPlace.request`, the canonical JSON below, and `requestSHA256` (SHA-256, lowercase hex, of its UTF-8 bytes). Canonical means: keys sorted, no insignificant whitespace, no absent-vs-empty ambiguity (every key present, empty string or empty array when unset), defaults resolved at acceptance.

```json
{"v":1,"kind":"pitr",
 "source":{"mysqlBackupRef":{"name":"orders-nightly-20261119","uid":"<MysqlBackup UID>"}},
 "decryption":{"secret":"<name or empty>","key":"<key or empty>"},
 "pointInTime":{"stopDatetime":"2026-11-20T01:42:00Z","sites":["iad","pdx"],
                "archiveProfileName":"nightly-s3","excludeGtids":"<canonical set or empty>"},
 "loadOptions":{"loadIndexes":true,"skipBinlog":true},
 "staging":{"image":"<override or empty>","sanityCheck":{"query":"...","expect":{...}}}}
```

`stopDatetime` is normalized to UTC whole seconds rounded up, as `restore.py` does (`internal/controller/restore_script.py:363-384`); `sites` is sorted and defaults to the group's sites; `archiveProfileName` defaults to `spec.backup.pitr.profileName`; `excludeGtids` is re-serialized by the operator's parser (`internal/mysql/gtid.go:38`, sorted source identifiers, merged intervals). `kind` is `full`, `schema` or `pitr`, so removing `pointInTime`+`staging` changes the identity. Non-identity fields (`threads`, `staging.resources`, `volume`, `cleanupPolicy`, deadlines) are pinned at acceptance as well; an edit applies to the next run (Event `RestoreInPlaceSpecEditDeferred`).

On every pass before the loader write-ahead, and explicitly at Preflight exit, Fencing entry and immediately before the write-ahead, the operator recomputes the identity from the current spec (re-reading the `MysqlBackup` UID):

| Change | Before the loader write-ahead (`Staging` through `Restoring` without a Job) | After it |
|---|---|---|
| Confirm advanced, identity equal | Rebind the confirm to the run, as today (`internal/controller/restore_inplace.go:700-717`) | Deferred, as today (`RestoreInPlaceConfirmDeferred`) |
| Confirm not advanced, changed | `RestoreInPlaceCancelled`, as today (`:399-415`) | Deferred |
| Identity changed (any of source, backup UID, `stopDatetime`, sites, profile, `excludeGtids`, `sanityCheck`, image override, kind), confirm advanced or not | Run ends `RestoreInPlaceSuperseded`, confirm not consumed; fence released if up (unless a damage record exists); the next pass accepts a run from the current spec, which may reuse an artifact whose request matches | Deferred: the Job runs the accepted identity; `RestoreInPlaceRequestChangeDeferred` Event |
| `spec.restoreInPlace` removed | `RestoreInPlaceCancelled` (existing) | Existing `RestoreInPlaceSpecRemoved` |

The loader is built from the pinned request and the artifact record, never from the current spec.

**Artifact certification** is what an artifact proves. `certificationSHA256` is the SHA-256 of the canonical JSON `{v, requestSHA256, sourceDumpSHA256, archiveIdentity, encryptionRevision, imageDigest, serverVersion, mysqlshVersion, formatVersion, baselineGtidSHA256, stagedGtidSHA256, manifestSHA256}`, all taken from the staging result. These are the Q4 fingerprint inputs; `formatVersion` is the operator constant for the staging script, proof and manifest format (the planner/verifier version). An artifact is handed to a loader only if it is `Verified`, its `requestSHA256` equals the run's, its stored certification recomputes from its fields, its `imageDigest` is the run's selected staging image and its `serverVersion` equals the bound target's `VERSION()`.

### Guards by phase

`inPlaceRestoreInFlight` (`internal/controller/restore_inplace.go:56`) gates more than the freeze. Each consumer gets an explicit rule:

| Guard | `Staging`/`Staged` | `Preflight` | `Fencing`/`Restoring`/`Resuming` | terminal with `status.restoreDamage` |
|---|---|---|---|---|
| Topology freeze (`inPlaceRestoreFreezesTopology`, `:70-74`) | no | no (today) | yes | yes |
| Primary label fence + `offline_mode` (`restoreOfflineFenceWanted`, `internal/controller/restore_offline_fence.go:288-300`) | no | no | yes | yes |
| `BeginPlanned` conflict for planned failover and ordered updates (`internal/controller/deployment_leases.go:727-731`) | **no** (new predicate excludes these phases) | yes | yes | yes |
| MysqlDatabase pause (`groupFenced`, `internal/controller/mysqldatabase_reconciler.go:2426-2433`) | **no** | yes | yes | **yes** (today it resumes on a damaged primary) |
| New deployment leases (`deployment_leases.go:295-296`) | refused (decision 4) | refused | refused | refused, reason `RestoreDamagedPrimary` |
| Scheduled backups | run | run | unchanged | skipped with `ScheduledBackupSkippedDamagedPrimary`: the backup account has no `CONNECTION_ADMIN` (`internal/controller/credentials.go:260`), so `offline_mode` refuses it and the primary fallback would fail. An explicit `MysqlBackup` with `sourceSiteOverride` naming a held site still runs |

PR1 adds `inPlaceRestoreMutating` (in flight and not `Staging`/`Staged`) and `restoreDamagePresent`, and switches the four consumers above to them; a unit test pins the table.

### 1. Acceptance (entry)

`validateInPlaceRestoreSpec` keeps its checks and replaces the blanket `pointInTime` refusal with the acceptance-pass refusals listed under API, plus: the backup's `status.gtidExecuted` (a lower bound of the dump baseline `B`) must be a subset of the active site's `gtid_executed` (an early `StagedGtidsNotInLive`, worded as an acceptance refusal), and `restoreStagingArtifacts` must have a free slot. A valid PITR run writes `phase=Staging`, `runID`, `runConfirm`, `startTime`, `kind`, `scope=full`, `request`, `requestSHA256` and `staging{}` in one status write, then emits `RestoreInPlaceStagingStarted`. Nothing else happens on that pass. If `status.restoreDamage` exists, the same write sets `restoreDamage.runID` to the new run (exit 1, Event `RestoreInPlaceDamageTakenOver`); the fence and freeze never lapse because the record keeps them wanted.

### 2. Staging

No deployment hold, no topology freeze, no reclone queue, no fence, no `offline_mode` (guards table). A deploy that migrates the schema during staging would be undone by the restore while the new code keeps running, so new leases stay refused for the whole run; existing leases are only waited for in Preflight. Steps, one durable write before each external action:

1. **Source hold.** Stamp `hold.restore.shipstream.io/<runID>` on the `MysqlBackup` with an optimistic lock, then re-read it uncached before any Job is created and refuse if it is gone, terminating or its UID differs from the request, as `stampVerificationHold` and `launchHoldConfirmed` do (`internal/controller/backup_verification_hold.go:308`, `:347`). Retention (`deleteUnreadBackup`, `:418`), artifact cleanup (`internal/controller/backup_reconciler.go:637`) and the PITR binlog cutoff (`cmd/bloodraven/pitr_retention.go:53`) honour restore holds as they honour verification holds. Release rule: see the hold paragraph under API.
2. **Image selection and probe.** Candidate = `staging.image` (`Override`), else the live target's mysqld container `imageID` when it is a pullable `<repo>@sha256:<digest>` reference, else that container's `image` string (images imported into k3d/kind report a bare ID) (`LiveTarget`), else `spec.backup.image` (`BackupImage`). A probe Job (`mysql-<fg>-staging-probe-<run8>-<attempt>`, 120 s deadline, no volumes, no token) runs `mysqlsh --version; mysqld --version` and reports through the termination message; the digest recorded is the probe pod's own `imageID`. A `LiveTarget` candidate without mysqlsh, or with a mysqlsh older than the one that wrote the source dump (`@.json`), falls back once to `BackupImage`. The probe's parsed `mysqld` `X.Y.Z` must equal the live target's `SELECT VERSION()` numeric prefix and must not be older than the dump's `serverVersion`; otherwise `Failed`/`StagingVersionMismatch` with `versionGate` set (the authoritative comparison is the staged server's own `VERSION()` string at step b). **Unverified:** whether the default `mysql:9.7` image ships mysqlsh (`api/v1alpha1/backup_types.go:9-21` names the community-server image as the mysqlsh carrier), so the design probes and never assumes.
3. **Reuse decision** (here, not at acceptance, because it needs the probe's digest). `mode=Reuse` when a `Verified` artifact has the run's `requestSHA256`, the current #203 `encryptionRevision`, the current #206 `archiveIdentity` for the profile, the probe's `imageDigest`, the operator's `formatVersion`, and a claim whose UID matches `pvcUID`. Else `mode=Build`.
4. **Claim (Build).** Append `restoreStagingArtifacts[{originRunID: runID, pvcName: mysql-<fg>-staging-<run8>, state: Provisioning, cleanupPolicy}]` (write-ahead), then create the claim (RWO, `volume.size`, `storageClassName`, owner reference to the group, label `shipstream.io/restore-run=<runID>`), record `pvcUID`, and set `provisioningSince`. A size below `3 x MysqlBackup.status.sizeBytes` emits `RestoreInPlaceStagingVolumeSmall` (a warning: compression ratios vary). The consumer is created **before** waiting for binding: with `volumeBindingMode: WaitForFirstConsumer` (the playground class, `playground/manifests/storageclass.yaml:16`) the claim binds only once a pod is scheduled. Once bound, the operator reads the PV: `nodeAffinity` on `kubernetes.io/hostname` sets `localNode` and emits `RestoreInPlaceStagingVolumeNodeLocal` (zonal affinity is not node-local). The claim bind, pod scheduling and volume attach together must finish within 30 minutes of `provisioningSince` (durable, so restarts do not reset it), else `StagingVolumeUnavailable`.
5. **Staging Job (Build), re-auth Job (Reuse).** Before every creation, including re-creations, an uncached read must show no earlier Job of this run present and every pod labelled `shipstream.io/restore-run=<runID>` terminal (`Succeeded`/`Failed`; a pod with a deletion timestamp that is not terminal still counts as running). The same proof precedes probe, re-auth and result-read Jobs. Then write `jobCreating=true`, `jobName` and `attempt+1` (write-ahead, as `internal/controller/restore_inplace.go:742-752` does for the destructive Job), then create the Job, whose pods carry `shipstream.io/restore-attempt=<attempt>`. Only after that proof may a pod clear `<runID>/`. Pod: hardened security context (uid/gid 27, read-only root, drop ALL; `internal/controller/backup_job.go:505-560`), `automountServiceAccountToken: false`, no `hostNetwork`, read-only failover tolerations, `backoffLimit: 0`, `activeDeadlineSeconds` from `staging`. Init containers (operator image): `decrypt-download` and `pitr-download` with the #203 revision Secret mounted 0400, writing to the claim instead of size-limited emptyDirs. The claim is mounted at `/var/lib/mysql` (the stock AppArmor profile only allows a datadir under `/var/lib/mysql/**`, `internal/controller/backup_verification_job.go:18-33`); `/run/mysqld` and `/tmp` are emptyDirs. A pod failure carrying the `DisruptionTarget` pod condition (eviction, preemption, drain) counts as a lost attempt, not a failure.

Staging Job internals (new `staging.sh`, sharing the mysqld boot with `verify_script.sh` and delegating load and replay to `restore.py`). Layout on the claim: `<runID>/owner` (runID and attempt), `<runID>/inputs/`, `<runID>/data/`, `<runID>/gtid-proof.json`, and the artifact directory `<runID>/artifact/` holding `manifest.json` and `dump/`.

| Step | What | Fails with |
|---|---|---|
| a | Ownership: `/var/lib/mysql` writable by uid 27; create or clear `<runID>/`, write `owner`; refuse if `<runID>/owner` names another runID; free-space check against the source size. Read the source `@.json`: refuse `X ∩ B ≠ ∅` | `StagingVolumeNotWritable`, `StagingGtidProofFailed` |
| b | Boot mysqld on `<runID>/data` after `--initialize-insecure`: `--bind-address=127.0.0.1 --mysqlx=OFF --skip-log-bin --gtid-mode=ON --enforce-gtid-consistency=ON --skip-replica-start --event-scheduler=OFF --skip-name-resolve --partial-revokes=ON --local-infile=1` (the verification set, `internal/controller/verify_script.sh:191-204`, plus the event scheduler); `VERSION()` must equal `BLOODRAVEN_STAGING_EXPECT_VERSION` exactly before anything is loaded | `StagingVersionMismatch` |
| c | Keyring: load `component_keyring_file` as verification does (`:95-153`). Unlike verification it fails closed: if the component cannot load and any dump DDL file contains `ENCRYPTION='Y'`, stop | `StagingKeyringUnavailable` |
| d | `util.load_dump` with the staging options (API). Accounts are loaded because `--initialize-insecure` creates only root and the replay pipe (`mysqlbinlog | mysql`, no `--force`, `internal/controller/restore_script.py:503-568`) stops on the first `ALTER USER`/`GRANT`/`SET PASSWORD` against a missing account; the operator's own MysqlDatabase statements are binlogged and land in every window. Then `ACCOUNT LOCK` every loaded account. Record the load duration. **Unverified in repo:** mysqlsh `loadUsers`/`excludeUsers` behaviour on 9.7; the native test covers it | `StagingFailed` |
| e | `_verify_loaded_dump` (post-stop GTIDs already in the dump, lineage; `internal/controller/restore_script.py:581-618`) | `StagingTargetNotCovered` |
| f | Replay through the #206 chronological plan, not one mysqlbinlog run per site | `StagingFailed` |
| g | Staged-GTID proof, below | `StagingGtidProofFailed` |
| h | Verdict: `targetCovered=true`, `partial=false`, `crossSiteUnverified=false` | `StagingTargetNotCovered` |
| i | `SET GLOBAL super_read_only=ON`; optional sanity query inside `START TRANSACTION READ ONLY` | `StagingFailed` (`SanityCheckFailed` detail) |
| j | Delete `inputs/`; `util.dump_instance('<runID>/artifact/dump', {consistent:true, users:false, events:true, routines:true, triggers:true, compression:'zstd', threads})` | `StagingFailed` |
| k | Re-read `@@gtid_executed`; it must equal the proven set and the artifact's `@.json` `gtidExecuted` | `StagingGtidProofFailed` |
| l | Write `artifact/manifest.json`: `{version:1, originRunID, requestSHA256, sourceDumpSHA256, archiveIdentity, encryptionRevision, files:[{path,size,sha256}], baselineGtids, stagedGtidExecuted, serverVersion, mysqlshVersion, imageDigest, formatVersion, createdAt}`; fsync. The hashing throughput is measured here (`hashBytesPerSecond`) | `StagingFailed` |
| m | Stop mysqld, remove `<runID>/data`; print the result (below) | |

**Staged-GTID proof.** Let `B` be the dump's `@.json` `gtidExecuted` (installed by `replace`), `X` the user's `excludeGtids` (`X ∩ B = ∅`, step a), and from the #206 plan `P` the exact set of transactions committed at or before the stop that the replay must apply, and `Q` every staged GTID after the stop. In-place runs require #206's exact plan (P1), so with `S = @@gtid_executed` after replay the proof is:

- equality: `S = B ∪ (P − X)` (nothing planned was skipped: a short `--stop-datetime` cut, a mid-file stop or a cross-site skip all fail; nothing outside the plan ran);
- no post-stop leak: `S ∩ Q = ∅`.

#206 must emit `P` and `Q` in `coverage.json`; today's per-site `inWindowGTIDs`/`postStopGTIDs` (`cmd/bloodraven/pitr_download.go:225-274`) cover only the single-replaying-site case. The diffs are written to `<runID>/gtid-proof.json` and printed to the log for diagnosis.

**Result channel.** The termination message (4096 bytes) is authoritative and bounded: `BLOODRAVEN_STAGING_COMPLETE v=1 runID= attempt= manifestSha256= stagedGtidSha256= baselineGtidSha256= sourceDumpSha256= archiveIdentity= serverVersion= mysqlshVersion= bytes= loadSeconds= hashBps=`. The two GTID sets, at most 1 MiB each, travel in the last log lines (`BLOODRAVEN_STAGING_GTIDS v=1 baseline=<B> staged=<S>`). The existing log reader is best-effort: it tails 50 lines with a 16 MiB line limit (`internal/controller/restore.go:413-429`) and never decides a Job's outcome (`:307-308`). The operator therefore accepts the logged sets only when their digests match the termination message. If the log is missing, truncated or mismatched, the operator runs a result-read Job (the re-auth builder in `ReadResult` mode: read-only mount, checks `manifest.json` against `manifestSha256`, prints the sets from it), at most 3 times, then `StagingFailed`. A missing termination message (pod gone) counts as a lost attempt. Status, written only by the operator from these checked values, is the trust anchor.

### 3. Staged

On Job success the operator checks the result as above, the versions and `stagingLoadSeconds`, writes the artifact's certification fields and `state=Verified`, sets `phase=Staged` and emits `RestoreInPlaceStaged`. Next pass: `phase=Preflight`.

In `Reuse` mode, Staging runs a re-authentication Job instead: it mounts the artifact's claim read-only at `subPath: <artifact.path>`, checks `manifest.json` against `manifestSHA256`, re-hashes every file under `dump/` (no missing or extra files), checks the artifact `@.json` set against `stagedGtidExecuted`, and prints the same completion sentinel. A digest mismatch (its own sentinel) makes the artifact `Invalid` and ends the run `StagingArtifactMismatch`. Any other failure (I/O error, eviction, a lost pod) leaves the artifact `Verified` and counts as a lost attempt.

**Artifact lifecycle.** Every staging claim has one entry in `restoreStagingArtifacts` from its write-ahead until it is observed gone, so a run's terminal write never has to hand a claim to another list.

| State | Entered | Leaves to |
|---|---|---|
| `Provisioning` | claim write-ahead | `Building` at the staging Job write-ahead; `Invalid` on timeout, cancel, supersede or failure |
| `Building` | staging Job may write | `Verified` (Staged checks pass); `Invalid` (failure, cancel, supersede, attempts exhausted) |
| `Verified` | certified | `Deleting`: (1) a run that loaded it reached `Succeeded`; (2) policy `Delete` when its origin run is terminal and nothing protects it; (3) replacement: another `Verified` artifact passed the Fencing rechecks against the bound target; (4) cleanup annotation. `Invalid` on a re-auth digest mismatch |
| `Invalid` | never reusable | `Deleting` once no pod of its origin run can mount it |
| `Deleting` | UID-conditional delete issued (`Preconditions{UID}`) | entry removed when the claim is observed gone |

Protection overrides (1)-(3): an artifact referenced by a non-terminal run is never deleted, and while `status.restoreDamage` exists nothing `Verified` or `Invalid` is deleted automatically (a replacement that then fails a hand-off check must not have cost the previous repair source); deferred deletions run when the record clears. A claim deleted directly is tolerated: the entry is removed with `RestoreStagingVolumeGone`, and a run that references it fails `StagingVolumeUnavailable` before the loader write-ahead. `Verified` entries whose origin run is terminal are what the retained gauge counts. `MaxItems=8` cannot be exceeded because acceptance refuses a run without a free slot (repeated failures leave `Invalid` entries only until their pods are gone).

### 4. Preflight

Today's Preflight unchanged (`internal/controller/restore_inplace.go:437-585`, plus #233's peer checks: `BeginPlanned` hold, writable active site, privileges, previous fence released or kept by a damage record this run took over, queue armed, fence recorded), plus:

- for a run that took over a damage record: an uncached read shows every Job in `restoreDamage.jobs` gone or explicitly terminal and every pod labelled with it terminal (the exit-2 rule); a fixed-name loader Job of an earlier run is deleted only after that;
- privileges (all full-instance runs): `RestoreFencePrivileges` (`internal/mysql/restore_fence.go:41-49`) gains `RELOAD` (`FLUSH BINARY LOGS`) and `TRANSACTION_GTID_TAG`; the explicitly assigned tagged `gtid_next` needs `TRANSACTION_GTID_TAG` plus one of `SYSTEM_VARIABLES_ADMIN`, `SESSION_VARIABLES_ADMIN` or `REPLICATION_APPLIER` (MySQL 9.7 Reference Manual, `gtid_next` system variable; tagged GTIDs since 8.3; **unverified in repo**), which the existing `SYSTEM_VARIABLES_ADMIN` requirement covers;
- for PITR runs: live `VERSION()` of the active site equals the artifact's `serverVersion` (`StagingVersionMismatch`); `S ⊆ L` with `L` the live `gtid_executed` (`StagedGtidsNotInLive`, below); claim bound, UID unchanged, not terminating, and if `localNode` is set that node exists, is Ready and is not cordoned (`StagingVolumeUnavailable`); every staging, probe, re-auth and result-read pod of the run terminal;
- loader budget: `estimate = artifactBytes / hashBytesPerSecond + stagingLoadSeconds x max(1, artifactBytes / sourceSizeBytes)` (the load term scales the measured staging load by artifact growth; both sizes are zstd-compressed dumps). With `loaderActiveDeadlineSeconds` set, refuse `StagingLoaderDeadlineTooShort` below `1.5 x estimate`; unset, emit a Normal Event with the estimate. This is a heuristic, not a proof: the live load runs over the network into a busy server. It is evaluated again at Preflight exit, immediately before Fencing.

**Staged set not a subset of live `gtid_executed`: refuse.** `D = S − L` non-empty means the staged data contains transactions the live history does not record, for example an archived branch a lossy failover abandoned, or a dump from another group. The alternative, `SET GLOBAL gtid_purged='+D'` after the load, rewrites the live primary's GTID history behind the archiver's back: the next archived file's `Previous_gtids` would jump by `D` with no archived events, which the predecessor-ordering planner cannot anchor, and it would quietly resurrect a branch the group abandoned. The message splits `D`: for `D − B` (replayed transactions) adding them to `pointInTime.excludeGtids` and re-running helps (the identity changes, so it re-stages); for `D ∩ B` (transactions inside the dump) nothing in this run can remove them, and only a different backup or a restore into a new group helps. Consequence, documented: in-place PITR from another group's backup or archive is always refused.

### 5. Fencing

Target site bound at entry (`TopologyGeneration++`, existing). Order:

1. strip the primary role label, wait until fenced (existing);
2. `offline_mode=ON`, evict pre-fence sessions (existing, `internal/controller/restore_offline_fence.go:365-450`);
3. rechecks against the bound target: request identity, `VERSION()`, `S ⊆ gtid_executed` (a staging-time failover may have bound a different server), claim UID and node, staging pods terminal, artifact hand-off rule. A refusal releases the fence immediately (no Job, no barrier) unless a damage record exists;
4. **barrier** (every full-instance run): choose `n` = 1 + the highest number used by any `bloodraven_restore`, `bloodraven_restored` or `bloodraven_restore_void` GTID across all source UUIDs in the target's `gtid_executed`, and write `restoreBarriers[runID]{barrier: {gtid: <target server_uuid>:bloodraven_restore:<n>, state: Writing}}` **before** committing anything. Then `FLUSH BINARY LOGS`; `SET GTID_NEXT='<uuid>:bloodraven_restore:<n>'; BEGIN; COMMIT; SET GTID_NEXT='AUTOMATIC'`; `FLUSH BINARY LOGS`; `state: Written`, Event `RestoreBarrierWritten`. An empty transaction is logged only under an explicitly assigned GTID; with `AUTOMATIC` (tagged or not) it gets none (MySQL 9.7 Reference Manual, GTID lifecycle; **unverified in repo**). Retry is idempotent: on restart in `Writing`, the recorded GTID already in `gtid_executed` means it was committed; otherwise both statements run again. In `Written` the second flush is re-run unconditionally (an extra rotation is harmless). The flushes put the barrier alone in its own sealed binlog file. Ownership is only ever the recorded GTID; a tag never identifies a run;
5. #233 peer IO stop with catch-up; `fencedGtidExecuted` now includes the barrier;
6. if PITR archiving is enabled, wait up to 10 minutes for the primary's sidecar to report the barrier archived (new `GET /archiver/covers?gtids=<set>`: whether a sealed archived file covers the set), then `state: Archived`. Timeout ends the run `RestoreBarrierNotArchived` before any Job;
7. Event `RestoreInPlaceWritesDiscarded` (requested stop, fence time), `phase=Restoring`.

### 6. Restoring

Today's destructive path (`internal/controller/restore_inplace.go:689-873`) with these changes:

- **Immediately before the write-ahead**, the operator re-reads the target `VERSION()` and the request identity (refusals as in Fencing; a version refusal here does not consume the confirm, Q7's "immediately before the DROP" check).
- **Damage write-ahead (every full-instance run, PR1).** The status update that sets `jobCreating=true` also creates `status.restoreDamage{originRunID: runID, runID, site, jobs: [loader Job name], since}`, or, for a run that took one over, appends this Job to `jobs`. The Job UID is added once the Job exists. From this write on the primary counts as possibly damaged. If the API server refuses the Create (Invalid/Forbidden/BadRequest, which today withdraws the write-ahead, `:753-762`), the record is withdrawn with it when this run created it.
- **Source:** the artifact's claim (`pvcUID` checked uncached before Create), `subPath: <artifact.path>`, read-only at `/staging` (`/staging/manifest.json`, `/staging/dump`); image: the artifact's `imageDigest`; no `pitr-download` or `decrypt-download` init containers; loader options as in API; no `updateGtidSet` (the live-target rule, `internal/controller/restore.go:937-949`, `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline`). `activeDeadlineSeconds` = `loaderActiveDeadlineSeconds` (none when unset) for PITR runs; non-PITR runs keep `spec.backup.activeDeadlineSeconds`.
- **Env:** `BLOODRAVEN_RUN_ID`, `BLOODRAVEN_STAGING_MANIFEST_SHA256`, `BLOODRAVEN_STAGING_GTID_SHA256`, `BLOODRAVEN_EXPECT_TARGET_VERSION`, `BLOODRAVEN_PRE_DESTRUCTION_DEADLINE` (absolute time: Job creation + 10 min + 2 x the estimated hash time).
- **Pre-destruction refusal path (PR1, all full-instance loaders).** `restore.py` sets a module flag immediately before its first mutating statement (`_maybe_reset_replication`, then `_maybe_preflight_drops`, `internal/controller/restore_script.py:880-883`). Before that point every refusal goes through one function, `_refuse_before_destruction(reason)`, which asserts the flag is unset, writes `BLOODRAVEN_PRE_DESTRUCTION_REFUSED v=1 runID=<id> job=<name> reason=<r>` to the termination message and exits with a dedicated code (proposed 70; today every failure exits 2, `:399-402`, `:700-742`, `:887-894`). The refusals routed through it: `_verify_staging_artifact` (manifest digest, every file size and SHA-256, no extra files, artifact `@.json` set digest), target `VERSION()`, `_prepare_local_infile`, `_require_offline_mode`, and the pre-destruction deadline (checked after every hashed file and immediately before the flag is set). After the flag is set the function falls through to exit 2. **Unverified in repo:** that `mysqlsh --py -f` propagates `sys.exit(70)` as the container exit code; PR1 pins it with a native test.

The full re-hash runs while fenced; for a large artifact it adds minutes to the outage. That is the price of turning a truncated or altered claim into a refusal before the DROP instead of a load failure after it.

### 7. Resuming and Succeeded

After the loader Job succeeds: write `restoreBarriers[runID].completion{gtid: <uuid>:bloodraven_restored:<n>, state: Writing}` (same uuid and `n` as the barrier), commit it with the same explicit-GTID statements (idempotent the same way), `Written`, Event `RestoreCompletionWritten`; an unreachable primary is retried with the fence up, since the load already succeeded. Only then release the queue to `Draining` and write `Succeeded`, which clears `status.restoreDamage` (whether this run created it or took it over) in the same write. The completion marker therefore precedes the fence release and every reclone, so recloned peers carry it and held peers do not. The hold on the source backup is released, and the artifact the run loaded goes to `Deleting` (success always deletes).

### 8. After a failed or lost loader Job: damage evaluation

Two independent questions, answered in this order before any fence release:

1. **May the peers resume replication?** #233 settlement, unchanged: Held vs Discarded from the Job pods and `gtid_executed == fencedGtidExecuted`. An unlogged load writes nothing to the binlog, so Discarded is correct for the peers even when the primary was changed.
2. **May the primary serve?** Only on proof. The record clears when the run created it (`originRunID == runID`), the Job is explicitly terminal, and an uncached list shows every pod of the Job terminal with exit code 70 and a termination message naming this run and Job. A pod that was killed, OOM-killed, deadline-killed, evicted, deleted or garbage-collected, or a Job that was lost, provides no proof and leaves the record (`confirmed: true`, condition `True`, alert). A run that took over a record and refuses before destruction only removes its own Job from `jobs`: the origin's damage stays.

A run changed nothing after its own barrier if it ended before its loader write-ahead, or if its loader's refusal proof holds (whether or not it inherited a record). Such a run is **proven undamaged**: if it wrote a barrier, the operator commits `<uuid>:bloodraven_restore_void:<n>` (write-ahead in `restoreBarriers[runID].void`, explicit GTID, idempotent) before the fence is released, Event `RestoreBarrierVoided`. An inherited record keeps the origin run's barrier effective, so the void never hides earlier damage. If MySQL cannot be reached within #233's `restoreSettleBound` (10 minutes, `origin/fix/issue-212:internal/controller/restore_reclones.go:827`), the fence is released anyway and the void is retried afterwards; until it lands the barrier stays effective, which only over-refuses.

### 9. Post-restore backup and barrier closure

After `Succeeded` and the fence release is observed (the backup account cannot connect earlier), with PITR archiving enabled, the operator creates `MysqlBackup` `<fg>-post-restore-<run8>` on the PITR profile (Event `PostRestoreBackupCreated`). A barrier entry closes when (a) a `Succeeded` backup of the group has `status.gtidExecuted` containing the completion marker of this barrier or of any later-numbered barrier (`gtidExecuted` is read before the dump starts, `internal/controller/backup_script.py:317-320`, so the snapshot is after the load), or (b) its void marker is in `gtid_executed`. Closure removes the entry and emits `RestoreBarrierClosed`; with no open entry, `PITRArchiveReady=False/PITRRestoreBarrier` clears. A failed backup emits `PostRestoreBackupFailed` and is not retried automatically; any later qualifying backup closes the barrier. A damaged run's barrier stays open until a later run's completion backup closes it. With PITR archiving disabled the markers are still written (in-band, free), the archive wait and the automatic backup are skipped, and the entry closes when its run ends undamaged or a later run succeeds. At `MaxItems=32` the oldest entry of a terminal run is dropped with an Event: status is display only, so dropping an entry never weakens the planner.

### Where the phase transitions live

| File (proposed unless existing) | Responsibility |
|---|---|
| `internal/controller/restore_inplace.go` (existing) | Entry, request identity, phase switch, Preflight/Fencing additions, unknown-phase handling |
| `internal/controller/restore_guards.go` | `inPlaceRestoreMutating`, `restoreDamagePresent`, the guards table |
| `internal/controller/restore_staging.go` | Staging and Staged: probe, reuse, claim, staging, re-auth and result-read Jobs, result parsing, certification |
| `internal/controller/restore_staging_job.go` | Job builders, pinned flags, mounts |
| `internal/controller/restore_staging_artifacts.go` | Artifact list, lifecycle, policy, cleanup annotation, claim loss |
| `internal/controller/restore_damage_fence.go` | Damage write-ahead, evaluation, exits, takeover |
| `internal/controller/restore_barrier.go` | Barrier, completion and void markers, archive wait, post-restore backup, closure, `PITRArchiveReady` reason |
| `internal/controller/restore_hold.go` | Restore holds on `MysqlBackup` (generalizing `backup_verification_hold.go`) |
| `internal/controller/staging.sh`, `restore_script.py` (existing) | Staging script; pre-destruction refusal path and `_verify_staging_artifact` |
| `cmd/bloodraven/pitr_download.go`, `pitr_window.go` (existing) | `PITRRestoreBarrier` refusal, #206 plan sets `P`, `Q` |
| `internal/sidecar/server.go` (existing) | `GET /archiver/covers` |

## Retry and crash boundaries

Nothing destructive retries automatically. "Durable" lists what status holds at that point.

| Boundary | Outcome | Durable | Admin action |
|---|---|---|---|
| Operator crash before the Staging write | Next reconcile re-validates and accepts | nothing | none |
| Crash in Staging after a claim or Job write-ahead | Next pass adopts the claim (name, UID, run label) or Job (name, run and attempt labels); a claim never created is created | artifact entry, staging status | none |
| Staging, probe, re-auth or result-read Job lost (deleted, never created after write-ahead, pod evicted, result unreadable) | Re-created automatically after the pod-gone proof, at most 3 attempts per Job kind; the new pod clears `<runID>/` only after that proof. No live damage | attempt | after 3: bump confirm |
| Staging Job failed | `Failed`/reason; confirm consumed (reason table); artifact `Invalid` | run status | fix cause, bump confirm |
| Claim lost, node lost or provisioning timeout during Staging | `Failed`/`StagingVolumeUnavailable`; no damage | run status | bump confirm (new claim) |
| Failover during Staging | Allowed. Staging continues (it reads only the backup and archive). The target is bound at Fencing; rechecks there catch a version change or a lost transaction (`StagedGtidsNotInLive`) | none extra | none, or re-run with `excludeGtids` (only for `D − B`) |
| Planned failover or ordered update during Staging | Allowed (guards table); a version change is caught at Preflight/Fencing (`StagingVersionMismatch`, gated by `versionGate`) | versionGate | wait for the rollout, bump confirm or leave it to the gate |
| Spec removed in Staging/Staged | `RestoreInPlaceCancelled`; staging Job deleted (foreground); a `Provisioning`/`Building` artifact goes `Invalid`, a `Verified` one follows its policy, a reused one is untouched | run status | none |
| Request identity changed before the loader write-ahead | `RestoreInPlaceSuperseded`, confirm not consumed; Jobs deleted; the run's own build follows the same rule as spec removal; the next pass accepts a run from the current spec | run status | none |
| Crash or failure in Preflight | As today: no fence, emergency failover available | run status | none |
| Failure in Fencing before the barrier | Fence released unless a damage record exists, queue discarded (no Job, #233 rule), confirm per reason | run status | per reason |
| Failure in Fencing after the barrier (peer stop timeout, archive wait) | No Job; settlement `Discarded`; run proven undamaged, so the barrier is voided; fence released unless a damage record exists | barrier, run status | none |
| Crash before the loader write-ahead | Rebuilt on the next pass (existing) | — | none |
| Crash between the loader write-ahead and Create, or loader Job lost | `RestoreInPlaceJobLost`, never rebuilt (existing, `internal/controller/restore_inplace.go:683-688`); no exit proof, so the damage record stays | run, damage record | three exits |
| Loader refused through the pre-destruction path | Record cleared (if this run created it), barrier voided, settlement `Discarded`, fence released; artifact stays `Verified`; `RestoreInPlacePreDestructionRefused`, confirm consumed | run status | fix, new confirm (reuses the artifact) |
| **Loader failed, killed or lost after its write-ahead without that proof** | see below | damage record | three exits |
| Group deleted | Owner references delete Jobs and claims; holds on backups are released by dead-hold stripping (`internal/controller/backup_verification_hold.go:247-270` generalized) | — | none |
| Operator upgrade mid-run | The new operator resumes from status. Avoid it: an upgrade during Restoring is already discouraged (#233) and staging makes runs hours longer | status | — |
| Operator downgrade mid-run | Unsupported, and unsafe below PR1: an older operator resets an unknown phase to no status (`internal/controller/restore_inplace.go:428-430`), and an older CRD prunes `status.restoreDamage`, so it can unfence a damaged primary. Operators from PR1 onwards treat an unknown phase as in flight: topology frozen, fence kept, no Job created or deleted, Warning `RestoreInPlaceUnknownPhase`. The upgrade-policy page says so | status | upgrade back |

**What a loader without exit proof leaves behind.** Not the old data on the primary: the DROP and a partial load are not undone.

- Primary: stays fenced (no role label, `offline_mode=ON` re-asserted every Poll) and the topology stays frozen under `status.restoreDamage`, independently of the terminal phase and of whether `spec.restoreInPlace` still exists. MysqlDatabase reconciles stay paused. `RestoreInPlaceDamagedPrimaryFenced=True`, `Ready=False`, alert.
- Peers: `Held` with pre-restore data and their IO threads stopped (#233), excluded from promotion and reader Services; or, after an unlogged load that #233 settled `Discarded`, replicating again from a fenced primary that accepts no writes.
- Source backup: still held, with its archived binlogs, so a repair can re-stage. Artifacts: none deleted automatically while the record exists (artifact lifecycle).
- Exits, none automatic:
  1. **New confirm.** The new run takes over the record in its acceptance write (no unfenced gap) and must prove every recorded loader Job stopped at Preflight. It reuses a matching artifact (minutes of re-authentication) or re-stages. Only its `Succeeded` clears the record; any other end of that run (refusal, failure, cancellation, supersession, its own pre-destruction refusal) leaves the record and the fence in place.
  2. **Accept and release** with `restore-accept-damaged=<originRunID>`, after proof that every recorded loader Job's pods are gone. The primary serves whatever it holds; the peers stay (or become) `Held` for #233's exits. Setting that queue to `Draining` reclones the held peers from the accepted primary and destroys what may be the only intact copies: take a backup from a held site first.
  3. **Rebuild from a held peer**: a backup with `sourceSiteOverride` naming a held site, then a full-instance restore from it, which is exit 1 with that source. A first-class "promote a held peer" operation (#233 follow-up) would take the record over the same way.
- **Primary permanently down.** Exits 1 and 3 load into the damaged primary, so they need it running. If it cannot come back: exit 2 (the loader pods on a lost node become provably gone once the node object is deleted), take a backup with `sourceSiteOverride` from a held site, and restore it with `initFromBackup` into a new group, the supported procedure. Backups with `sourceSiteOverride` are allowed while frozen (`internal/controller/backup_reconciler.go:1471-1484`).

## GTID and archive lineage (Q3)

**Keep the live superset.** The live primary keeps its full `gtid_executed`, including the GTIDs of every discarded post-stop transaction, and the restored data is loaded unlogged (`skipBinlog:true`, forced for every full-instance in-place restore; explicit `false` is rejected). Peers inherit the set through CLONE. The DROPs stay binlogged. Consumers of the binlog outside `spec.sites` (an external async standby, which #233 already lists as not held, or a CDC pipeline) therefore receive the DROPs but not the reload, where today an omitted `loadOptions` gives them a binlogged load (`site/content/docs/7.backup-and-restore/2.backup-restore.md:634-638`). Known-limitations and the restore procedure say so.

**Why not RESET.** `RESET BINARY LOGS AND GTIDS` followed by `replace` would make the live set equal to the staged set, but the server would then reissue GTID numbers that already sit in the archive for the discarded transactions. The archiver's reuse detector (`internal/sidecar/binlog_reuse.go:115-154`) proves the overlap and records `Epoch.ResetReason = "verified GTID namespace reuse ... use a new manifest prefix and full backup"` (`:184-195`); the archive audit then errors for the site (`internal/sidecar/binlog_history.go:76-77`), retention stops (`internal/sidecar/binlog_archiver.go:378`, `:428`), and the planner refuses every window on that site (`cmd/bloodraven/pitr_window.go:115-116`). RESET would turn every in-place restore into a forced archive migration.

**Three in-band markers per run**, all empty transactions under explicitly assigned tagged GTIDs on the bound target's `server_uuid`, sharing one number `n` (Lifecycle, Fencing step 4). `n` is above every number any of the three tags has used in the target's `gtid_executed`, which the live superset and CLONE carry forward, so numbers grow with the group's restore history and one run's three markers pair up by `(uuid, n)`:

| Marker | Committed | Means |
|---|---|---|
| barrier `bloodraven_restore:<n>` | Fencing, after the application fence, before the peers stop and before any loader Job; archived before the Job when archiving is enabled | data may change without binlog events after this point |
| completion `bloodraven_restored:<n>` | Resuming, after the loader succeeded, before the fence release and every reclone | the load finished; any snapshot taken after this marker holds the restored dataset |
| void `bloodraven_restore_void:<n>` | after the run is proven undamaged (Lifecycle step 8); the planner then ignores barrier `n` | barrier `n` changed nothing |

Because they are in the GTID stream: every archive reader sees them in the per-file GTID coverage, including verification Jobs and other groups restoring from this archive; a backup's `gtidExecuted` says which side of each marker its snapshot is on, even when no other write happened in between (a GTID-set boundary such as `fencedGtidExecuted` alone is ambiguous in exactly that case); no clock comparison is needed. A backup that contains the barrier proves nothing about its data: a backup of the primary during the archive wait, or of a peer that caught up to the barrier and was then held, contains it and holds pre-restore data. Only the completion marker certifies a post-restore snapshot.

Rejected alternatives: reusing `Epoch.ResetReason` (blocks the whole site forever); a JSON barrier object in the prefix keyed by time or by `fencedGtidExecuted` (ambiguous at equality, written by a second writer into a single-writer layout); `AUTOMATIC:<tag>` with an empty transaction (MySQL assigns no GTID, so nothing is written); adopting "any unrecorded `bloodraven_restore` GTID" after a crash (it can pick up an earlier restore's barrier). Tagged GTIDs need MySQL 8.3+ (the 9.7 line is the only supported one, `site/content/docs/6.operations/10.upgrade-policy.md:24`); the operator already parses tagged GTID sets (`internal/mysql/gtid.go:10-12`). Tagged explicit `gtid_next` against `mysql:9.7` is **unverified in this repo** and is checked first in PR2. If it does not hold, PR2 falls back to binlogged rows in a dedicated operator schema carrying `(kind, n)`, which the planner identifies by event content instead of tag.

**Planner rules** (`planPITRArchive` and #206's replacement; one function used by restore, staging and verification). Let a barrier be *effective* if no void with its `(uuid, n)` appears in the base set or anywhere in the archive.

1. **Base rule.** Let `n*` be the highest-numbered effective barrier in the base backup's `gtidExecuted`. If `n*` exists and the base lacks the completion of `n*`, refuse `PITRRestoreBarrier` ("the backup was taken inside restore `n*`"). A later run's completion supersedes earlier damaged barriers, because its load replaced the whole dataset.
2. **Window rule.** Refuse if an effective barrier not in the base lies at or before the stop (in `P`, or anywhere in a staged file not wholly after the stop). A barrier after the stop (in `Q`) is harmless: `S ∩ Q = ∅` already proves it was not applied. So the supported new-group procedure from a pre-restore backup to a time before the restore still works, and so does an exit-1 re-stage to the same stop.
3. The rules run before `excludeGtids` is applied, and acceptance refuses `excludeGtids` that names any marker, so exclusion cannot bypass them.

**Runs that never complete.** A run proven undamaged (it ended before its loader write-ahead, or its loader exited through the refusal path) gets its void, so readers lose no coverage. A run whose loader may have touched the primary keeps an effective barrier forever: PITR from older backups across that instant is refused, which is correct because the archive cannot replay the unlogged change. A later successful run's completion backup becomes the new base.

**Post-restore backup.** See Lifecycle step 9. Until it completes, `PITRArchiveReady=False/PITRRestoreBarrier` and an alert make the PITR gap visible.

## Interaction with the rest of the operator

The guards table under Lifecycle is the normative list; in prose:

- **Topology manager.** Not frozen during Staging/Staged/Preflight; emergency failover works. Frozen from Fencing through Resuming and while `status.restoreDamage` exists. #233's restore protection (start gate, re-stop, clone admission, promotion exclusion) is unchanged and applies from Fencing.
- **Planned failover and ordered updates.** Allowed during Staging/Staged because `BeginPlanned` stops treating those phases as a concurrent operation; caught by the version and subset rechecks. Preflight waits for them (#233). A planned failover to a held site stays refused.
- **MysqlDatabase.** Not paused during Staging/Staged (account and password management keep working; the restore discards their effect on data, and accounts are not rewound). Paused from Preflight and while a damage record exists, so the operator never runs DDL or account statements against a damaged primary.
- **Backups and retention.** Scheduled backups continue during Staging. The source backup and its binlogs are held. While a damage record exists scheduled backups are skipped with a reason instead of failing against `offline_mode`; explicit backups from held sites run.
- **Verification.** Unchanged, except that PITR verifications are subject to the barrier rules. The shared mysqld boot moves into a library both scripts source. Verification loads no accounts, so a window containing account statements fails there too; that is a separate fix.
- **Encryption.** Inputs are decrypted with the #203 revision. The intermediate artifact is plaintext on the claim (Q1 A). Live `encryptionAtRest` is unaffected: the logical load recreates `ENCRYPTION='Y'` tables under the live keyring.
- **Deployment API.** `operations.restoreInPlace` (`site/content/docs/4.configuration/11.deployment-api.md:132`, `:153`) reports `Staging` and `Staged`; leases are refused with `ConcurrentOperation` for the whole run, with `RestoreReclonePending` while peers are queued or held (#233), and with `RestoreDamagedPrimary` while a damage record exists.
- **Bootstrap restores.** `initFromBackup` gains the source hold (PR3) but no markers: a new group has no earlier lineage.

## Security

- **Plaintext at rest.** The staging claim holds decrypted inputs (until step j), the staging datadir (until step m) and, for a retained artifact, a plaintext logical dump of the whole instance. Encrypting only the intermediate dump would protect nothing while the datadir sits next to it during the run. Docs recommend an encrypted, replicated StorageClass and namespace RBAC that limits who can mount claims. Retained artifacts are listed in status, counted in a gauge and alerted on.
- **Keyring.** The staging keyring is ephemeral (`/run/mysqld` emptyDir) and dies with the pod; the datadir it protects is deleted before the pod exits.
- **Loopback root and loaded accounts.** Passwordless `root@127.0.0.1` and the dump's accounts (locked after the load) are reachable only from containers in the same pod, all of which are operator-built. No Service, no token, no host network, mysqlx off. Pinned by a unit test of the Job spec and the script flags.
- **Supply chain.** Image digests are recorded and reused for the loader, so the mysqlsh that loads the artifact is the binary that wrote it.

## Observability

- **Structured logs** (camelCase fields; `site/content/docs/8.observability/7.log-schema.md` updated in the PR that adds each): INFO `restore staging started`, INFO `restore staging job created`, WARN `restore staging volume is node-local`, WARN `restore staging refused` (`reason`), INFO `restore staging verified` (`runID, manifestSha256, stagedGtidSha256, bytes`), INFO `restore staging artifact reused`, INFO `restore staging artifact deleted`, INFO `restore barrier written` (`runID, site, gtid`), INFO `restore barrier archived`, INFO `restore completion written` (`runID, site, gtid`), INFO `restore barrier voided` (`runID, gtid`), INFO `restore barrier closed` (`backup`), ERROR `post-restore backup failed`, WARN `restore request changed; run superseded` (`runID, requestSHA256`), ERROR `in-place restore left the primary fenced as possibly damaged` (`originRunID, runID, site, jobName`), INFO `in-place restore loader refused before destruction` (`runID, jobName, reason`), INFO `damaged-primary fence released`, INFO `damaged-primary record taken over` (`originRunID, runID`), INFO `scheduled backup skipped: damaged primary`, WARN `unknown in-place restore phase; holding guards`. No existing `msg` changes.
- **Alerts** (documented in `8.observability/2.monitoring.md` and `5.alert-runbook-map.md`): `BloodravenRestoreDamagedPrimaryFenced` (critical, `bloodraven_restore_damaged_primary_fenced == 1`), `BloodravenRestoreStagingRetained` (warning, retained artifacts for more than 24 h), `BloodravenPITRRestoreBarrierOpen` (warning, open for more than 6 h), `BloodravenRestoreInPlaceLoaderSlow` (warning, `Restoring` longer than 2x the loader estimate).
- **bloodraven-doctor** (maintenance contract): `SKILL.md` (staging phases, damaged fence, barrier), `references/alert_runbook_matrix.md` (every new reason, Event and alert; `RestoreInPlaceRejected` row at `:55` rewritten), `references/troubleshooting_playbooks.md` (playbook 19 rewritten from "pointInTime rejected" to the in-place PITR procedure; new playbooks for a damaged fence and its three exits, an open barrier, a retained artifact, `StagedGtidsNotInLive` split by `D ∩ B`, `StagingVersionMismatch`, a permanently down damaged primary), `scripts/triage.sh` (staging status, artifact list, `status.restoreDamage`, barriers and their markers), `scripts/deployment-probe.sh` (new phases), `scripts/sidecar-probe.sh` (`/archiver/covers`), `scripts/support-bundle.sh` (staging and probe Job logs, claims).

## Docs that change when implemented

| Page | Change |
|---|---|
| `site/content/docs/7.backup-and-restore/2.backup-restore.md` | Rewrite "Point-in-time recovery is not supported in place" (`:849-877`) into the in-place PITR procedure with the supported-procedure statement first; full-instance phases, `skipBinlog` forced and `excludeSchemas` refused (also in the phases list, `:632-638`), accounts not rewound, the damage boundary and exits, markers and post-restore backup, sizing formula, loader budget |
| `site/content/docs/3.get-started/1.known-limitations.md` | Replace "in-place cannot PITR" (`:109-114`) with the remaining limits: full-instance only, `mysqlBackupRef` sources only, no foreign source, accounts not rewound, plaintext claim, external binlog consumers see the DROPs but not the reload |
| `site/content/docs/4.configuration/2.crd-reference.md` | `RestoreInPlaceSpec.staging`, new types, phases, status fields, conditions |
| `site/content/docs/7.backup-and-restore/5.backup-encryption.md` | Staging decryption with the #203 revision; plaintext claim |
| `site/content/docs/7.backup-and-restore/6.backup-verification.md` | `PITRRestoreBarrier` refusal |
| `site/content/docs/5.architecture/3.multi-cluster-dr.md`, `6.durability-and-rpo.md` | Keep new-group PITR as the primary procedure; barrier and RPO gap |
| `site/content/docs/8.observability/2.monitoring.md`, `5.alert-runbook-map.md`, `7.log-schema.md` | Events, metrics, alerts, log messages |
| `site/content/docs/4.configuration/11.deployment-api.md` | New phase values |
| `site/content/docs/6.operations/10.upgrade-policy.md`, `9.failure-mode-matrix.md`, `4.configuration/10.production-hardening.md` | No up/downgrade mid-run, and no downgrade below PR1 while a damage record exists; damaged fence; `RELOAD` and `TRANSACTION_GTID_TAG`; encrypted StorageClass |
| `playground/chaos-scenarios.md` | Sections for scenarios 63-67 |

## Test plan

**DST.** `internal/dst` has no restore model today: it drives the real `TopologyManager` with a fake clock and in-memory MySQL, and models neither CLONE, restores, backups nor the in-place controller (`internal/dst/README.md:138-142`; #233 "Not run"). It cannot test staging, which is controller and Job work. PR1 adds the one piece where DST is the right tool: a restore-guard actor that applies the same guard surfaces the reconciler applies (topology freeze, primary fence, pending/held set, damage record) through `applyRestoreProtection`, at random points of the fault schedule. New invariants: no promotion of a pending or held site; no `START REPLICA` on a held peer; no failover while frozen; a primary with a damage record is never unfenced except by an exit event. Fixed findings are pinned in `test/component` as usual.

**Unit (`internal/controller`, `cmd/bloodraven`).**
- Flip `TestReconcileInPlaceRestore_PointInTimeRejected` (`internal/controller/restore_inplace_test.go:1150`) into acceptance into `Staging`, and add acceptance-pass rejection tests for `allowPartial`, `allowUnverifiedCrossSite`, per-schema with `pointInTime`, `excludeSchemas` on any full-instance restore, `skipBinlog:false`, `loadIndexes:false`, an S3/PVC source, a stop before the backup's completion, `excludeGtids` intersecting the backup bound or naming a marker, and a full artifact list.
- Keep `TestBuildInPlaceRestoreJob_RefusesPointInTime` (`:1236`) as a loader invariant (the loader never replays), and `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline` (`internal/controller/restore_test.go:537`).
- Request identity: canonical JSON golden files; every row of the identity table in every pre-Job phase (confirm-only bump rebinds; `stopDatetime`, `excludeGtids`, source, backup UID, `sanityCheck`, image or kind edits supersede; removing `pointInTime`+`staging` mid-Preflight supersedes instead of building a non-PITR loader); a loader is never built from the current spec.
- Guards table: one test per cell, including `BeginPlanned` admitting planned failover and ordered updates during Staging and `groupFenced` pausing MysqlDatabase while a damage record exists.
- Staging Job builder: pinned flags, no token, no host network, no Service, AppArmor paths, claim mount, revision Secret, deadline; probe Job; image selection matrix (override, live digest, live bare ID, live without mysqlsh, live too old, fallback mismatch, target changed between staging and Fencing).
- Phase machine: every row of the crash table; pod-gone proof before each re-creation (a terminating pod blocks it); `DisruptionTarget` counted as lost; WaitForFirstConsumer ordering and the durable provisioning timeout across a restart; result channel (log missing, truncated, digest mismatch, result-read Job); `versionGate` preventing a restart loop; every refusal's confirm consumption.
- Artifacts: the lifecycle table; reuse decided after the probe; reuse mounting `<originRunID>/artifact`; replacement deferred while a damage record exists and while the replacement has not passed Fencing; cancel of a Reuse run leaves the artifact `Verified`; `Invalid` only on a digest mismatch; UID-conditional deletes; direct claim deletion.
- Damage: write-ahead in the same update as `jobCreating`; withdrawn with a refused Create; refusal proof clears only the creator's record; kill, OOM, deadline, eviction, deleted or missing pod and JobLost all leave it; a takeover run's Staging failure, Preflight refusal, loader refusal, cancellation and supersession all leave it; `Succeeded` clears it; accept refused while any recorded Job's pods live and for a stale runID; accept after a Discarded settlement arms a Held queue.
- Markers: `n` selection across UUIDs and tags; idempotent retry from `Writing` and `Written` (no adoption of an earlier barrier); completion before queue release; void after proof; closure by a completion-carrying backup only (a held-peer backup holding the barrier does not close it).
- Planner (`cmd/bloodraven`): base rule and window rule fixtures, including a base inside restore `n`, a base after a later completion, a barrier after the stop, a voided barrier, and the `excludeGtids` bypass attempt.
- Scripts under python3/bash with stub mysqlsh (pattern of `internal/controller/restore_script_offline_test.go`, `pitr_restore_script_test.go`): exact staged-GTID proof cases, `X ∩ B` refusal, sentinel and size bounds, manifest writer, `_verify_staging_artifact`, and that `_refuse_before_destruction` is unreachable once the destruction flag is set and that no refusal before it exits with any other code.

**Component (`test/component`).** Real `TopologyManager` with `FakeReplica` (#233's harness): failover and planned failover during Staging leave the run unfrozen and rebinding at Fencing; held peers plus the damage record through operator restarts; takeover by a new run without an unfenced gap; accept-and-release.

**Envtest (`test/envtest`).** CEL pairing rule and enum rejection; status round-trip for every new type; claim owner references are set (envtest runs no garbage collector, `test/envtest/suite_test.go:42-50`, so cascading deletion is tested in a playground scenario); the damage record, artifact list and barriers surviving reconciler restarts; restore holds blocking retention and cleanup.

**Real MySQL, opt-in** (pattern of `TestRestoreFence_RealMySQL`, `internal/mysql/restore_fence_mysql_test.go:20`, and `BLOODRAVEN_PITR_NATIVE_TEST`, `internal/controller/pitr_restore_script_test.go:323`): `BLOODRAVEN_STAGING_NATIVE_TEST=1` runs `staging.sh` against a native mysqld/mysqlsh/mysqlbinlog with a generated dump and binlogs, asserting the proof, account loading with an `ALTER USER` in the window, the read-only sanity check and that the artifact `@.json` equals the proven set; `BLOODRAVEN_TEST_MYSQL_ADMIN_DSN` covers the three explicit tagged markers (an `AUTOMATIC:<tag>` empty transaction asserted to log nothing), the privilege set and `RELOAD`, and the loader's exit code 70 through `mysqlsh --py -f`.

**Playground scenarios** (IDs after #233's 62):

| ID | Injects | Asserts | Profile |
|---|---|---|---|
| `63-in-place-pitr-restores-to-stop-time` | marker rows before the stop, rows and a dropped table after it, an `ALTER USER` in the window | `Succeeded`; pre-stop rows present, post-stop rows absent, dropped table back; peers recloned and replicating; staging claim deleted; completion marker on every peer; barrier closed by the post-restore backup; a PITR verification from the pre-restore backup across the restore is refused `PITRRestoreBarrier`, one from the post-restore backup passes; deleting the group removes the claims | **release** |
| `64-in-place-pitr-fails-after-drop-keeps-fence` | deletes the loader pod once the user schemas are gone; then a new confirm whose loader pod is deleted before its DROP, then another | `Failed`; `RestoreInPlaceDamagedPrimaryFenced=True`; primary Service has no endpoints, `offline_mode=ON`, topology frozen; queue `Held`, markers on every peer; artifact retained; the second run (no user schemas left, so nothing logged) also ends with the fence up; the third reuses the artifact (`mode=Reuse`) and ends `Succeeded` | **release** |
| `65-in-place-pitr-planned-failover-during-staging` | planned failover while `Staging` | staging continues; target bound to the new primary at Fencing; `Succeeded` | full |
| `66-in-place-pitr-operator-restart-each-phase` | operator pod deleted in Staging, Staged, Fencing (between barrier write-ahead and commit), Restoring | no duplicate Job or claim, one barrier GTID per run, no unfenced gap, `Succeeded` | full |
| `67-in-place-pitr-cross-site-window` | failback A to B to A inside the window (needs #206) | every site's transactions present; staged proof equality | full |

Both release scenarios reset MySQL first. #233's scenario 62 moves to `release` in PR1, because the damage fence otherwise has no CI signal (#236).

## Phased implementation plan

Each PR passes the AGENTS.md pre-PR gate on its own and ships its own docs, log-schema and doctor updates. `pointInTime` stays rejected for real runs until PR6; PR5 accepts it only with its hidden flag on, and then stops after `Staged`.

| PR | Scope | Depends on | Size |
|---|---|---|---|
| PR1 | Damage boundary and damaged-primary fence for every full-instance restore: `status.restoreDamage` written with the loader write-ahead, `restore.py` pre-destruction refusal path and exit code, damage evaluation separate from #233 settlement, takeover by a new confirm with the pod-gone proof, accept-and-release (with Held queue after a Discarded settlement) and plugin command, guards table (`inPlaceRestoreMutating`, `restoreDamagePresent`, `BeginPlanned`, `groupFenced`, lease reason, backup skip), condition, gauge, alert, unknown-phase fail-safe, DST restore-guard actor, scenario 62 to release | #233 | M-L (~2.5k lines with tests) |
| PR2 | Markers (barrier, completion, void; explicit tagged GTIDs, write-ahead, both flushes), archive wait and sidecar `/archiver/covers`, planner base and window rules, automatic post-restore backup, `PITRArchiveReady` reason, `RELOAD` and `TRANSACTION_GTID_TAG` preflight, forced `skipBinlog:true` and refused `excludeSchemas` for full-instance restores | PR1; coordinate with #206 so its planner keeps the rules | L |
| PR3 | Restore holds on source `MysqlBackup`s for bootstrap and in-place restores, generalizing #232 (fixes today's gap where retention can prune a backup a restore is reading), kept while a damage record names the run | none (the damage clause after PR1) | S-M |
| PR4 | API: `RestoreStagingSpec`, status types, phases, CEL, generated CRDs copied to the chart, CRD reference; reconcile-time rejection unchanged | v1beta1 types merged | S |
| PR5 | Staging: request identity, probe Job, reuse decision, claim, staging Job and `staging.sh`, proof, manifest, result channel, Staged certification, artifact lifecycle and cleanup, re-auth and result-read Jobs, `persistentvolumes` RBAC and chart mirror. Behind a hidden operator flag `--experimental-in-place-pitr-staging` (off by default); with it on, a run stops at `Staged` and ends `Failed`/`RestoreInPlaceStagingOnly`. No API dry-run mode: rehearsal is what `MysqlBackupVerification` with `pointInTime` is for | #206, #203, PR3, PR4 | L |
| PR6 | Hand-off: Preflight, Fencing and pre-write-ahead rechecks, loader budget and `loaderActiveDeadlineSeconds`, loader from the claim with `_verify_staging_artifact`, artifact reuse after a damaged failure, removal of the flag and the rejection, docs procedure, doctor playbooks, scenarios 63 and 64 in release | PR1, PR2, PR5 | L |
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
| Q10 damaged primary | **A**: durable fence on possible damage with three exits; separate prerequisite PR | B unfence on failure: splits writes across two inconsistent histories |

How this proposal implements the decisions where the debate left the mechanism open:

- **Q10:** "possibly damaged" is decided by the damage boundary (write-ahead at the loader's `jobCreating`, cleared only by the loader's pre-destruction refusal proof or by `Succeeded`), not by #233's GTID comparison, which cannot see an unlogged load. #233 settlement still decides the peers. This is stricter than "Discarded releases as today": a Discarded settlement releases the fence only when the refusal proof also holds.
- **Q3:** the barrier is joined by a completion marker (certifies a post-restore baseline) and a void marker (returns coverage for runs proven undamaged). Both refine how the barrier is enforced; neither removes it.
- **Q7:** the "immediately before the DROP" check is made by the operator immediately before the loader write-ahead, so its refusal leaves the confirm unconsumed as Q7 requires without creating a second destructive Job; the loader's own in-Job check is an extra guard and, because a Job ran, consumes the confirm.
- **Q9:** "deleted only after the replacement is verified" means after the replacement passes the Fencing rechecks, and not at all while a damage record exists.

Decisions taken in this proposal that the debate did not settle (for review):

1. `staged set ⊄ live gtid_executed`: **refuse** (`StagedGtidsNotInLive`), not `gtid_purged='+diff'`; therefore foreign-source in-place PITR is always refused, and `D ∩ B` can only be fixed with another backup or a new group.
2. Markers are **in-band tagged GTIDs** under explicitly assigned, write-ahead-recorded numbers; they need `RELOAD` and `TRANSACTION_GTID_TAG` for all full-instance restores.
3. `skipBinlog:true` is forced and `excludeSchemas` refused for **all** full-instance in-place restores, not only PITR ones.
4. New deployment leases stay refused during Staging/Staged (existing behaviour), while `BeginPlanned` stops treating those phases as a concurrent operation and MysqlDatabase keeps reconciling.
5. A damage record protects the source hold and every artifact under both cleanup policies.
6. Phases `Staging` and `Staged`; a probe Job for image selection; reuse decided after the probe and re-authenticated by a separate Job before Fencing; in-place PITR needs a `mysqlBackupRef` source.
7. Staging Job loss (including eviction and an unreadable result) is re-created automatically up to 3 times after a pod-gone proof (no damage possible); staging Job failure is terminal.
8. The PITR loader gets its own optional deadline, unset by default, plus an in-script pre-destruction deadline; with a deadline set, Preflight refuses below 1.5x a hash-plus-load estimate (a heuristic).
9. A request-identity change interrupts any run before its loader write-ahead; a confirm-only bump rebinds, as today.
10. Every refusal after the acceptance pass consumes the confirm, except the pre-Job version refusals (Q7), which a `versionGate` keeps from looping.
11. Staging loads the dump's accounts (then locks them) so account statements in the window replay; accounts on the live primary are not rewound.
12. PR5 lands behind a hidden operator flag rather than an API dry-run mode.
13. Staging data on the claim under `<runID>/`, created by the Job, so root-owned local-path mounts work if they are writable by uid 27.

## Open questions

1. **Q1 sign-off.** A versus C (see Decisions) is pending Colin's review.
2. **Tagged explicit GTIDs on `mysql:9.7`.** `SET GTID_NEXT='<uuid>:<tag>:<n>'` with an empty transaction and the privilege set above are expected from MySQL 8.3 but are not exercised anywhere in the repo. PR2 verifies first; the fallback is described under GTID lineage.
3. **mysqlsh behaviour.** `loadUsers` with `excludeUsers` into staging, and `sys.exit(70)` propagating through `mysqlsh --py -f`: both checked by the native tests before PR5 and PR1 merge respectively.
4. **local-path ownership in the playground.** The design assumes the k3d local-path provisioner creates claim directories writable by uid 27 (its default setup script uses mode 0777) so the Job can create its own `<runID>/` directory. Unverified here; PR5 checks it on k3d. If it fails, the playground gets a StorageClass with a setup script that does so; the operator does not gain a root init container.
5. **mysqlsh in `mysql:9.7`.** Unknown; handled by the probe. Recording the measured answer in the docs is a PR5 task.
6. **#206 interface.** The exact plan sets `P` and `Q` and an archive identity readable without a plan are requirements on #206 (P1), not yet agreed there.

## Acceptance criteria

1. With `pointInTime` and `staging`, a full-instance in-place restore reaches `Succeeded` with exactly the pre-stop dataset (scenario 63 in release).
2. Every refusal decidable from the backup, archive and staged result happens before the fence; the rechecks after the fence are bounded and release it (unless a damage record exists); every reason is in status and an Event.
3. A loader Job that did not exit through its pre-destruction refusal path leaves the primary fenced and frozen with `RestoreInPlaceDamagedPrimaryFenced=True`, whatever `gtid_executed` shows; a later run's failure or refusal never releases an inherited record; a new confirm reuses the artifact (scenario 64 in release).
4. Nothing destructive runs without a new confirm or an unconsumed one whose run never created a loader Job; a loader Job is never rebuilt after its write-ahead.
5. No live `RESET`; every full-instance restore writes an explicitly numbered barrier before its loader and a completion marker before its fence release; the planner enforces the base and window rules; `PITRArchiveReady` reports the gap until a backup carrying the completion marker succeeds.
6. `allowPartial`, `allowUnverifiedCrossSite`, per-schema PITR, `excludeSchemas`, `skipBinlog:false`, `loadIndexes:false` and non-`mysqlBackupRef` PITR sources are refused without consuming the confirm.
7. The loader is built only from the accepted request and an artifact whose certification matches it.
8. The staging pod has no token, Service or host network and binds loopback with mysqlx off (unit test).
9. Artifacts appear in status, a gauge and an alert, and are deleted only by success, policy, replacement after the replacement passed Fencing, or the annotation; never automatically while a damage record exists.
10. CRDs regenerated and copied to the chart; ClusterRole mirrors `persistentvolumes get`; log-schema, doctor files and the docs listed above updated in the PRs that change them.
11. Merge gated on #206, #203 and #233, and on PR1 and PR2.
