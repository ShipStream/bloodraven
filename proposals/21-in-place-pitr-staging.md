# Proposal: In-Place Point-in-Time Restore Through a Staging Instance

**Issue:** [#205](https://github.com/ShipStream/bloodraven/issues/205)
**Status:** Draft
**Branch:** `docs/in-place-pitr-proposal`

All names introduced here (fields, phases, reasons, Events, annotations, metrics, log messages, scripts, GTID tags) are **proposed**. Citations are `path:line` on `main` at `7c3626d` (after #233, merged as `d846f9f`, and #234). MySQL behaviour cited from the MySQL 9.7 Reference Manual is marked **unverified in repo** where nothing in this repository exercises it yet.

## Supported procedure

**Point-in-time recovery into a new failover group (`spec.initFromBackup.pointInTime`) remains the supported production procedure.** It leaves the damaged group untouched until the application cuts over, so a failed or wrong restore costs nothing. In-place PITR, as designed here, is for constrained environments where a second group cannot be created (quota, licensing, fixed endpoints, a single storage pool). It does not merge while the PITR planner can skip another site's transactions (#206), and it always discards every write committed after the stop time, including writes accepted while the staging instance is being built.

## Motivation

`spec.restoreInPlace.pointInTime` exists in the API but every run that sets it is rejected before anything is fenced (`internal/controller/restore_inplace.go:262-274`, `:288-290`; field comment `api/v1alpha1/backup_types.go:802-811`). #197 removed the old behaviour because it was silently wrong: the restore Job piped archived binlogs into the live primary, whose `gtid_executed` already contained every one of those GTIDs (it executed or replicated them), so MySQL skipped the whole replay and the run reported `Succeeded` with the data at the backup. Installing the dump's GTID set first, as new-group PITR does (`internal/controller/restore.go:931-960`), is impossible on a live server: `updateGtidSet:"replace"` requires the dump's set to be disjoint from what the server has executed.

A staging instance removes the conflict. The dump is loaded into an empty, throwaway `mysqld` whose GTID state starts at the dump's own baseline, the archive is replayed there with normal GTID deduplication, the result is proven against the replay plan, and only that verified result is loaded into the live primary through today's full-instance path (fence, peer hold, DROP, unlogged load, peer reclone). The live primary never receives a binlog replay, so there is nothing to skip.

What users get: a rewind of a whole instance to a proven point in time, in the same group. Staging runs before the fence, so the outage is the fence, a few bounded checks, the artifact re-hash, DROP and load. Peers keep their pre-restore data if the load fails. After a failure that may have touched the primary, the primary stays fenced instead of serving a half-loaded dataset.

## Goals

1. Full-instance in-place PITR (`restoreInPlace.pointInTime` with empty `loadOptions.includeSchemas` and `excludeSchemas`, from a `mysqlBackupRef` whose dump recorded instance scope and consistency) through a staging `mysqld` in an operator-built Job.
2. Every PITR refusal that can be decided from the backup, the archive and the staged result (source scope, coverage, cross-site order, GTID proof, versions, keyring, volume, subset of live history, loader budget) lands before the fence. The checks repeated after the fence, against the bound target, are bounded (seconds to minutes, plus the archive wait) and release the fence when they refuse, unless a damage record exists.
3. The outage is today's full-instance outage plus bounded additions: the Fencing rechecks, the barrier archive wait (at most 10 minutes), the #233 peer stop (at most 2 minutes) and the loader's artifact re-hash. Staging, replay and the intermediate dump run while the group serves traffic and emergency failover stays available.
4. One damage invariant (Lifecycle, "The damage invariant"): from the loader's write-ahead until one of four named proofs, the primary stays fenced and every damage guard stays on, in every phase of every run. A verified artifact lets a new confirm repair without re-staging.
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
| P3 | **v1.6.0 target-coverage rule** for the staged run: the staging verdict must be `targetCovered=true` (`cmd/bloodraven/pitr_download.go:388-414`) | An in-place restore to an unproven point is a silent partial restore. `allowPartial` and `allowUnverifiedCrossSite` (`api/v1alpha1/backup_types.go:219-234`) are refused for in-place runs at reconcile time. |
| P4 | **Damaged-primary fence** (Q10) under the damage invariant below, shipped as its own PR (PR1) | Today a failed restore unfences the primary ("Even on failure we unfence", `internal/controller/restore_inplace.go:1008-1010`). #233 settlement cannot close the gap alone: an unlogged load into a primary with no user schemas leaves `gtid_executed` unchanged, which #233 itself flags (`internal/controller/restore_reclones.go:771-774`). That gap exists for non-PITR full-instance restores too. |
| P5 | **Restore markers, source-scope recording and automatic post-restore backup** (Q3), shipped as its own PR (PR2) | Today's non-PITR full-instance restores already break lineage silently: a PITR from a pre-restore backup across the restore replays post-restore binlogs onto pre-restore data. They also accept a schema-filtered dump, whose omitted schemas the DROP destroys while the run reports success. |

Satisfied dependency: **#233** (merged as `d846f9f`) supplies the peer `STOP REPLICA IO_THREAD` at Fencing, `fencedGtidExecuted` and Held vs Discarded settlement. Without it the peers would replicate the DROPs and a post-DROP failure would damage every site.

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
- **Everything else is reconcile-time** (`RestoreInPlaceRejected`, confirm not consumed), checked in the acceptance pass. For **every full-instance** restore (PITR or not; the DROP removes every user schema, so anything the source lacks is destroyed while the run reports success): a non-empty `loadOptions.excludeSchemas`; explicit `loadOptions.skipBinlog: false`; a `source` other than `mysqlBackupRef`; a referenced backup that is not `Succeeded`, is terminating, has no `status.location`, has `status.dumpScope` other than `Instance`, has `status.consistent` other than `true`, or has a `status.gtidExecuted` that is empty or does not parse (`validatePITRBackupBound`, `internal/controller/restore.go:1585-1594`, run with the PITR spec or with an empty one; a backup taken before PR2 has no scope and is refused, D3). For **per-schema** restores: any `status.restoreDamage` (the record names the exits). For **PITR** runs, additionally: `allowPartial: true`; `allowUnverifiedCrossSite: true`; a non-empty `includeSchemas`; `loadIndexes: false` (the artifact's DDL comes from the staged server, so missing secondary indexes would be carried into the live primary); `validatePITRBackupBefore` against the backup's `completionTime` (`internal/controller/restore.go:1557-1573`); `excludeGtids` that does not parse, intersects the backup's `status.gtidExecuted` (an exclusion cannot remove a transaction the dump already contains) or names any `bloodraven_restore*` GTID; the backup's `status.gtidExecuted` not a subset of the active site's `gtid_executed` (an early `StagedGtidsNotInLive`). A size below a floor is a Warning only (see Lifecycle).
- **Source scope is recorded, never inferred.** `MysqlBackup` gains `status.dumpScope` (`Instance` when the options the backup script passed to `util.dump_instance` had no `includeSchemas`/`excludeSchemas`, else `Filtered`) and `status.consistent` (the effective `consistent` option, mysqlsh default `true`). The script reports both on its `BLOODRAVEN_DUMP_COMPLETE` sentinel from the options it actually used, so a later profile edit cannot change what an old backup claims. `MysqlBackup` also gains optional `spec.dumpScope` (enum `Instance`): the Job drops the profile's schema filters and forces `consistent: true`. The automatic post-restore backup always sets it, and barrier closure requires `status.dumpScope=Instance` and `consistent=true`.
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
// RestoreInPlaceKind is derived at acceptance from scope and pointInTime
// and pinned. An unknown stored value is handled like an unknown phase.
// +kubebuilder:validation:Enum=Full;Schema;PITR
type RestoreInPlaceKind string

// status.restoreInPlace gains the run's pinned request and staging state.
type RestoreInPlaceStatus struct {
	// ... existing fields ...
	Kind RestoreInPlaceKind `json:"kind,omitempty"`
	// RequestSHA256 is the request identity (see "Request identity"),
	// used for supersession and artifact reuse. Never changed.
	RequestSHA256 string `json:"requestSHA256,omitempty"`
	// Snapshot names the immutable ConfigMap holding the complete accepted
	// request; every later step reads it, never the current spec.
	Snapshot *RestoreSnapshotRef `json:"snapshot,omitempty"`
	// +optional
	Staging *RestoreStagingRunStatus `json:"staging,omitempty"`
}

type RestoreSnapshotRef struct {
	Name   string `json:"name"`   // mysql-<fg>-restore-<run8>
	UID    string `json:"uid"`
	SHA256 string `json:"sha256"` // of the snapshot's canonical JSON
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
	Attempt         int32              `json:"attempt,omitempty"`         // incremented in every Job write-ahead; pods carry it as a label
	LostJobs        int32              `json:"lostJobs,omitempty"`        // lost staging-side Jobs; StagingFailed at 3
	ProvisioningSince *metav1.Time     `json:"provisioningSince,omitempty"` // durable start of claim/scheduling waits
	Image           string             `json:"image,omitempty"`
	ImageDigest     string             `json:"imageDigest,omitempty"`
	ImageSource     RestoreStagingImageSource `json:"imageSource,omitempty"`
	TargetServerVersion string         `json:"targetServerVersion,omitempty"` // live target at the last check
	VersionGate     *RestoreVersionGate `json:"versionGate,omitempty"`    // see StagingVersionMismatch
	Reason          string             `json:"reason,omitempty"`
	Message         string             `json:"message,omitempty"`         // at most 1 KiB
}

// RestoreVersionGate is the key a version refusal was made for. A
// terminal run refused for a version mismatch is not restarted by its
// unconsumed confirm while every field still matches; any change, or a
// newer confirm, is evaluated afresh.
type RestoreVersionGate struct {
	Confirm           string `json:"confirm"`
	RequestSHA256     string `json:"requestSHA256"`
	ImageRef          string `json:"imageRef"`          // candidate as resolved (override, live imageID, backup image)
	ImageDigest       string `json:"imageDigest"`       // probe pod imageID; empty if the probe never ran
	DumpServerVersion string `json:"dumpServerVersion"`
	TargetVersion     string `json:"targetVersion"`
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

	// RestoreMarkerSeq is the highest marker number ever allocated for
	// this group. Never decreases; written in the same update that
	// records a new marker, so an allocated number is never reused.
	// +optional
	RestoreMarkerSeq int64 `json:"restoreMarkerSeq,omitempty"`

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

// RestoreStagingArtifact is one claim and the artifact on it. Everything
// below State is written once, at certification, and is exactly the
// certification input (see "Artifact certification").
type RestoreStagingArtifact struct {
	OriginRunID    string `json:"originRunID"`            // run that built it; never reused
	PVCName        string `json:"pvcName"`                // written before Create (write-ahead)
	PVCUID         string `json:"pvcUID,omitempty"`
	Path           string `json:"path,omitempty"`         // "<originRunID>/artifact": manifest.json + dump/
	LocalNode      string `json:"localNode,omitempty"`    // bound PV pinned to one node
	CleanupPolicy  RestoreStagingCleanupPolicy `json:"cleanupPolicy,omitempty"` // policy of the origin run
	State          RestoreStagingArtifactState `json:"state"`
	GtidSets       *RestoreSnapshotRef `json:"gtidSets,omitempty"` // immutable ConfigMap holding B and S
	CertificationSHA256 string `json:"certificationSHA256,omitempty"`
	RequestSHA256  string `json:"requestSHA256,omitempty"`  // origin run's request identity
	SourceDumpSHA256 string `json:"sourceDumpSHA256,omitempty"` // source @.json
	ArchiveIdentity string `json:"archiveIdentity,omitempty"` // #206
	DumpEncryptionRevision    string `json:"dumpEncryptionRevision,omitempty"`    // #203 revision of the backup prefix, or "plaintext"
	ArchiveEncryptionRevision string `json:"archiveEncryptionRevision,omitempty"` // #203 revision of the archive prefix, or "plaintext"
	ImageDigest    string `json:"imageDigest,omitempty"`
	ServerVersion  string `json:"serverVersion,omitempty"`  // staging SELECT VERSION()
	DumpServerVersion string `json:"dumpServerVersion,omitempty"` // source @.json serverVersion
	MysqlshVersion string `json:"mysqlshVersion,omitempty"`
	FormatVersion  int32  `json:"formatVersion,omitempty"`  // operator's staging/proof/manifest format
	ManifestSHA256 string `json:"manifestSHA256,omitempty"`
	BaselineGtidSHA256 string `json:"baselineGtidSHA256,omitempty"`
	StagedGtidSHA256   string `json:"stagedGtidSHA256,omitempty"`
	EncryptedTables bool  `json:"encryptedTables,omitempty"` // artifact DDL has ENCRYPTION='Y'
	SourceBytes    int64  `json:"sourceBytes,omitempty"`    // downloaded source dump, measured by staging
	ArtifactBytes  int64  `json:"artifactBytes,omitempty"`
	StagingLoadSeconds int64 `json:"stagingLoadSeconds,omitempty"`
	HashBytesPerSecond int64 `json:"hashBytesPerSecond,omitempty"` // measured while writing the manifest
	VerifiedAt     *metav1.Time `json:"verifiedAt,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Message        string `json:"message,omitempty"`          // at most 1 KiB
}

// +kubebuilder:validation:Enum=Writing;Written;Archived
type RestoreMarkerState string

type RestoreMarker struct {
	GTID  string             `json:"gtid"`  // chosen and recorded before it is committed
	State RestoreMarkerState `json:"state"`
	At    *metav1.Time       `json:"at,omitempty"`
}

type RestoreObjectRef struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

type RestoreBarrierStatus struct {
	RunID      string         `json:"runID"`
	N          int64          `json:"n"`                    // allocated from restoreMarkerSeq
	Site       string         `json:"site,omitempty"`
	Barrier    RestoreMarker  `json:"barrier"`             // <uuid>:bloodraven_restore:<n>
	Completion *RestoreMarker `json:"completion,omitempty"` // <uuid>:bloodraven_restored:<n>
	Void       *RestoreMarker `json:"void,omitempty"`       // <uuid>:bloodraven_restore_void:<n>
	BaselineBackup *RestoreObjectRef `json:"baselineBackup,omitempty"`
	Message    string         `json:"message,omitempty"`
}

// RestoreDamageStatus: the primary may hold a partial or dropped dataset.
// No list of Jobs: execution-stopped proof is label-based.
type RestoreDamageStatus struct {
	OriginRunID string      `json:"originRunID"` // run whose loader write-ahead created the record
	RunID       string      `json:"runID"`       // run that owns the record now (origin, or a takeover run)
	Site        string      `json:"site"`
	Since       metav1.Time `json:"since"`
	// Accept is the accept marker <uuid>:bloodraven_restore_accepted:<m>,
	// recorded before it is committed (exit 2).
	Accept      *RestoreMarker `json:"accept,omitempty"`
	Reason      string      `json:"reason,omitempty"`
}
```

**Status size budget.** No full GTID set is added to any status beyond the two that exist today (`sourceGtidExecuted`, `targetGtidExecuted`, `api/v1alpha1/backup_types.go:1039-1128`). The full accepted request, `excludeGtids` included, lives in the run's snapshot ConfigMap; the baseline `B` and staged `S` sets live in the artifact's `manifest.json` on the claim (authenticated by `manifestSHA256`) and in one immutable operator-owned ConfigMap per artifact (`<pvcName>-gtids`, owner reference to the group, deleted with the artifact), which the operator reads uncached and checks against the two digests before each use. Each set may be at most 256 KiB (the staging Job fails `StagingFailed`/`GtidSetTooLarge` above that). Messages are truncated to 1 KiB. What this proposal adds to `MysqlFailoverGroup` status is at most about 40 KiB: 8 artifact entries of about 2 KiB, 32 barrier entries of about 0.5 KiB, the damage record and the run status. A unit test serializes the maximal object and asserts it.

### Conditions

| Type | Status/Reason | When |
|---|---|---|
| `RestoreInPlaceDamagedPrimaryFenced` (new) | `True` / `RestoreFailedAfterJob` | `status.restoreDamage` exists, whatever the phase of any run. Message names the origin and owning runs, the exits and the Verified artifacts. `False` / `Accepted`, `Repaired`, `NotDamaged` after a clearing proof. |
| `PITRArchiveReady` (existing, `internal/controller/pitr_archive_condition.go:29`) | `False` / `PITRRestoreBarrier` (new reason) | An entry in `status.restoreBarriers` is open. Message names the barrier and completion GTIDs and the post-restore backup. Precedence: below `ArchiverError`, above `Archiving`. |
| `Ready` | `False` while `status.restoreDamage` exists | Same mechanism #233 uses for `Held`. |

### Reasons and Events

Stable reason strings, used in status messages and Events. "Consumed" follows one rule: a refusal in the acceptance pass never consumes the confirm; any later refusal does, except the version refusals made before a loader Job exists (Q7), which are gated so that the unconsumed confirm cannot restart an identical run (`versionGate`). Without the rule, an unconsumed refusal restarts the run on the next reconcile (`internal/controller/restore_inplace.go:333-351`), which for staging means hours of work repeated in a loop.

| Reason | Where | Confirm consumed |
|---|---|---|
| `RestoreInPlaceRejected` (existing) | acceptance-pass refusals listed above | no |
| `StagingVersionMismatch` | probe, Staged check, Preflight, Fencing, and the operator's check immediately before the loader write-ahead | no; not restarted while `versionGate` matches |
| `StagingArtifactSlotsFull` | Build decision (Staging step 3) with 8 artifact entries; the message names `kubectl bloodraven restore-staging cleanup` | yes |
| `StagingTargetNotCovered` | staging verdict `targetCovered=false` | yes |
| `StagingGtidProofFailed` | staged set differs from the exact plan, or `excludeGtids` intersects the dump baseline `B` | yes |
| `StagedGtidsNotInLive` | `S − L` non-empty; the message splits `D ∩ B` from `D − B` (see Preflight) | yes |
| `StagingKeyringUnavailable` | encrypted tables in the dump and the staging keyring unavailable; or `encryptedTables` and the live keyring component not active (Preflight, Fencing, loader) | yes |
| `StagingVolumeNotWritable` | in-Job ownership/writability check | yes |
| `StagingVolumeUnavailable` | provisioning timeout, claim lost or replaced (UID), node gone or unschedulable before the loader write-ahead | yes |
| `StagingArtifactMismatch` | manifest, file or GTID-set digest mismatch (re-auth Job, result-read Job, `<pvcName>-gtids` ConfigMap, loader refusal); the artifact becomes `Invalid` | yes |
| `RestoreInPlaceSnapshotLost` | the run's snapshot ConfigMap is missing or fails its digest before the loader write-ahead | yes |
| `StagingLoaderDeadlineTooShort` | `loaderActiveDeadlineSeconds` set and below the loader budget (Preflight) | yes; the Verified artifact stays reusable |
| `StagingFailed` | staging Job failed for any other reason (detail such as `GtidSetTooLarge`), or 3 staging-side Jobs were lost | yes |
| `RestoreInPlaceStagingOnly` | PR5 flag on: the run stops after `Staged` | yes |
| `RestoreInPlaceSuperseded` | the spec's request identity differs from the accepted one before the loader write-ahead (with or without a confirm bump) | no; the next pass starts a run from the current spec |
| `RestoreBarrierNotArchived` | barrier binlog not archived within 10 min at Fencing | yes (no Job) |
| `RestoreInPlacePreDestructionRefused` | the loader exited through its pre-destruction refusal path (manifest, file digests, version, keyring, offline fence, local_infile, pre-destruction deadline); detail in the sentinel | yes (a Job ran) |
| `PITRRestoreBarrier` | planner refusal and `PITRArchiveReady` reason | n/a |

New Events: `RestoreInPlaceStagingStarted`, `RestoreInPlaceStagingJobCreated`, `RestoreInPlaceStagingVolumeNodeLocal` (Warning), `RestoreInPlaceStagingVolumeSmall` (Warning), `RestoreInPlaceStagingResourcesUnset` (Warning), `RestoreInPlaceStaged`, `RestoreInPlaceStagingReused`, `RestoreInPlaceStagingFailed` (Warning), `StagingVersionMismatch` (Warning), `RestoreInPlaceSpecEditDeferred`, `RestoreInPlaceWritesDiscarded` (Normal, at Fencing: requested stop time and actual fence time), `RestoreBarrierWritten`, `RestoreBarrierArchived`, `RestoreCompletionWritten`, `RestoreBarrierVoided`, `RestoreBarrierClosed`, `RestoreAcceptMarkerWritten`, `PostRestoreBackupCreated`, `PostRestoreBackupFailed` (Warning), `RestoreInPlaceDamagedPrimaryFenced` (Warning), `RestoreInPlaceDamagedPrimaryReleased`, `RestoreInPlaceDamageTakenOver`, `ScheduledBackupSkippedDamagedPrimary` (Warning), `RestoreStagingRetained`, `RestoreStagingDeleted`, `RestoreStagingVolumeGone` (Warning), `RestoreStagingCleanupRejected` (Warning), `RestoreInPlaceUnknownPhase` (Warning).

### Annotations (one-shot, on the `MysqlFailoverGroup`, consumed and cleared like `RecloneAnnotation`)

| Annotation | Value | Effect |
|---|---|---|
| `bloodraven.shipstream.io/restore-staging-cleanup` | artifact `originRunID` | Deletes that artifact's claim (UID-conditional delete, state `Deleting`). Refused (Event, annotation cleared) if no such artifact exists or a non-terminal run references it. Allowed while `status.restoreDamage` exists, with a Warning that it removes a repair source. RunIDs are UUIDs and never reused, so a stale value cannot hit a later artifact. |
| `bloodraven.shipstream.io/restore-accept-damaged` | `status.restoreDamage.originRunID` | Exit 2 (see "The damage invariant"). Refused (Event, annotation cleared) unless the value matches, no run is in `Preflight` through `Resuming`, and the execution-stopped proof holds. Then: record `restoreDamage.accept` (fresh number from `restoreMarkerSeq`), commit the accept marker on the primary, clear the record, release the fence, and create the automatic post-restore backup. An unreachable primary leaves the record and fence in place and retries; nothing times out into a release. A `Held` queue stays `Held` for #233's exits. If settlement had discarded the queue (the peers resumed, which an unlogged load allows), the same write arms a `Held` queue of every peer: their pre-restore data no longer matches the primary the administrator accepted. |

`kubectl bloodraven restore-staging list|cleanup <group> --run <id>` (new plugin subcommand, `cmd/kubectl-bloodraven/`) reads the artifact list and sets the cleanup annotation. `kubectl bloodraven restore accept-damaged <group> --run <id>` sets the other.

A hold annotation `hold.restore.shipstream.io/<runID>` = group name (mirroring the verification hold prefix, `internal/controller/backup_verification_hold.go:29`) is added to the source `MysqlBackup` in the Staging step 1 write (non-PITR full-instance runs: in Preflight). It is released when the run is terminal and no pod of the run can still read the source, except that **no restore hold whose value is this group is released while `status.restoreDamage` exists**: every earlier run's source is a possible repair input, and the hold itself, not run status, is what names it.

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

PVC and ConfigMap create/delete already exist (`internal/controller/reconciler.go:205`, `config/rbac/role.yaml:7-20`). New: `+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get` on the group reconciler, to read the bound PV's `nodeAffinity` through the uncached API reader (the `apiReader()` pattern, `internal/controller/backup_verification_hold.go:397-410`; the default cached client would start a PV informer that needs `list`/`watch`). `charts/bloodraven/templates/clusterrole.yaml` must mirror it (AGENTS.md pre-PR gate 5). The reconciler also `Owns(&corev1.PersistentVolumeClaim{})` for the staging claims; claims carry an owner reference to the group.

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
                                                  Resuming   Failed ──> settlement (#233) decides the peers;
                                     completion marker │                 the damage invariant decides the primary
                                                       v
                                                  Succeeded ──> post-restore backup closes barrier
```

### The damage invariant

This paragraph overrides every table and step below. **A damage record (`status.restoreDamage`) is created in the same status update that sets the loader's `jobCreating`, for every full-instance restore. While it exists, every damage guard is on in every phase of every run, terminal or not: the primary's Service label fence, `offline_mode=ON` and `event_scheduler=OFF`, the topology freeze, the MysqlDatabase pause, refused leases and planned failovers, skipped scheduled backups, every restore hold whose value is this group, every `Verified` artifact (only the explicit cleanup annotation deletes one), and no fence release on any timeout. It is cleared only by:**

- **(a) Repaired:** a full-instance run that owns the record (it created it, or took it over in its acceptance write) reaches `Succeeded`: its loader succeeded and its completion marker is committed;
- **(b) Refused:** the loader of the run that created the record exits through its own pre-destruction refusal: an uncached read shows the loader Job explicitly terminal and every pod of it terminal with exit code 70 and a termination message naming this run and Job;
- **(c) Never started:** for a record the run created itself, the same read shows the loader Job explicitly terminal and every pod of it terminal with the main container never started (no `state.running`, no `state.terminated`, no `lastState`; with `restartPolicy: Never` and no init container that connects to MySQL, pinned by a unit test, such a pod cannot have touched the primary), for example a failed `decrypt-download` init container; or the API server refused the Job's Create outright (Invalid/Forbidden/BadRequest), so no pod exists;
- **(d) Accepted:** the `restore-accept-damaged` annotation with the execution-stopped proof, after the accept marker is committed.

Nothing else clears it: not #233 settlement, not a GTID comparison, not a timeout, not an unreachable primary, not a later run's refusal, failure, cancellation or supersession (an inherited record is never cleared by (b) or (c)). While it exists, per-schema restores are refused at acceptance and every full-instance run takes the record over in its acceptance write. **Execution-stopped proof** is label-based: every in-place loader pod carries `shipstream.io/restore-loader=<group>`, and an uncached list shows no pod with that label that is not `Succeeded`/`Failed` (a pod with a deletion timestamp that is not terminal still counts) and no Job with that label lacking an explicit terminal condition (the `jobFinished` rule, `internal/controller/backup_verification_hold.go:62-90`). A pod whose node is lost counts only after the machine is confirmed shut down or externally fenced from MySQL: deleting the Node or force-deleting the pod removes the API object, not the process, and Kubernetes' non-graceful node shutdown procedure (`node.kubernetes.io/out-of-service` after confirmed shutdown) is the supported way to get there. A loader whose pod never runs (unschedulable, `ImagePullBackOff`) keeps the record until it fails or is deleted; deleting it leaves the record (no evidence), and exit 1 reuses the artifact in minutes.

### Request identity and artifact certification

**Accepted execution snapshot.** In the acceptance pass, before the status write, the operator creates an immutable ConfigMap `mysql-<fg>-restore-<run8>` (`immutable: true`, owner reference to the group, label `shipstream.io/restore-run=<runID>`) holding the complete accepted request as canonical JSON: `restoreInPlace` with every default resolved (source with the `MysqlBackup` UID, decryption, `loadOptions` including `threads`, `pointInTime` including `excludeGtids`, and the whole `staging` block: image override, volume, resources, cleanup policy, both deadlines, sanity check), plus the backup's `dumpScope`, `consistent`, `gtidExecuted` digest and `serverVersion` at acceptance. The status write records `snapshot{name, uid, sha256}`. An orphaned ConfigMap from a crash before that write is deleted by its run label. Every later step, including after an operator restart, reads the snapshot uncached and checks its digest; nothing reads the current spec for execution. The snapshot is deleted when the run is terminal and its loader (if any) is stopped. Canonical means: keys sorted, no insignificant whitespace, every key present (empty string or empty array when unset).

**Request identity** is the subset that decides supersession and artifact reuse; `requestSHA256` is the SHA-256 (lowercase hex) of its canonical JSON:

```json
{"v":1,"kind":"PITR",
 "source":{"mysqlBackupRef":{"name":"orders-nightly-20261119","uid":"<MysqlBackup UID>"}},
 "decryption":{"secret":"<name or empty>","key":"<key or empty>"},
 "pointInTime":{"stopDatetime":"2026-11-20T01:42:00Z","sites":["iad","pdx"],
                "archiveProfileName":"nightly-s3","excludeGtids":"<canonical set or empty>"},
 "staging":{"image":"<override or empty>","sanityCheck":{"query":"...","expect":{...}}}}
```

`stopDatetime` is normalized to UTC whole seconds rounded up, as `restore.py` does (`internal/controller/restore_script.py:363-384`); `sites` is sorted and defaults to the group's sites; `archiveProfileName` defaults to `spec.backup.pitr.profileName`. `excludeGtids` is canonicalized by a new `GTIDSet.Canonical()`: tags and UUIDs sorted, each source's intervals sorted, merged where adjacent or overlapping, and deduplicated. Today's `ParseGTIDSet` appends intervals in input order (`internal/mysql/gtid.go:85-90`) and `String()` sorts only the source identifiers (`:321-347`), so `u:1:2` and `u:1-2` would differ; equivalence fixtures pin the new function. Non-identity fields (`threads`, resources, volume, cleanup policy, deadlines) come from the snapshot; an edit to them applies to the next run (Event `RestoreInPlaceSpecEditDeferred`).

On every pass before the loader write-ahead, and explicitly at Preflight exit, Fencing entry and immediately before the write-ahead, the operator recomputes the identity from the current spec (re-reading the `MysqlBackup` UID; a `MysqlBackup` that is gone counts as a changed identity). When the confirm regressed and the identity also changed, the cancel row wins.

| Change | Before the loader write-ahead (`Staging` through `Restoring` without a Job) | After it |
|---|---|---|
| Confirm advanced, identity equal | Rebind the confirm to the run, as today (`internal/controller/restore_inplace.go:783-800`) | Deferred, as today (`RestoreInPlaceConfirmDeferred`) |
| Confirm not advanced, changed | `RestoreInPlaceCancelled`, as today (`:419-435`) | Deferred |
| Identity changed (any of source, backup UID, `stopDatetime`, sites, profile, `excludeGtids`, `sanityCheck`, image override, kind), confirm advanced or not | Run ends `RestoreInPlaceSuperseded`, confirm not consumed; fence released if up (unless a damage record exists); the next pass accepts a run from the current spec, which may reuse an artifact whose request matches | Deferred: the Job runs the accepted snapshot; `RestoreInPlaceSpecEditDeferred` Event |
| `spec.restoreInPlace` removed | `RestoreInPlaceCancelled` (existing) | Existing `RestoreInPlaceSpecRemoved` |

The loader is built from the snapshot and the artifact record, never from the current spec.

**Artifact certification** is what an artifact proves. `certificationSHA256` is the SHA-256 of the canonical JSON of the artifact entry's fields from `requestSHA256` through `encryptedTables` in the struct above (`v` first), all taken from the digest-checked staging result and recorded with it, so a later run recomputes it from the entry alone. These are the Q4 fingerprint inputs; `formatVersion` is the operator constant for the staging script, proof and manifest format (the planner/verifier version). An artifact is handed to a loader only if it is `Verified`, its `requestSHA256` equals the run's, its stored certification recomputes from its fields, its `imageDigest` is the run's selected staging image, its `serverVersion` equals the bound target's `VERSION()`, and its `<pvcName>-gtids` ConfigMap matches both GTID digests.

### Guards by phase

`inPlaceRestoreInFlight` (`internal/controller/restore_inplace.go:56`) gates more than the freeze. Each consumer gets an explicit rule. The last column is the damage invariant and **overrides the phase columns**: each guard is `restoreDamagePresent || <phase rule>`.

| Guard | `Staging`/`Staged` | `Preflight` | `Fencing`/`Restoring`/`Resuming` | `status.restoreDamage` present (any phase, any run) |
|---|---|---|---|---|
| Topology freeze (`inPlaceRestoreFreezesTopology`, `:70-74`) | no | no (today) | yes | yes |
| Primary label fence, `offline_mode`, `event_scheduler=OFF` (`restoreOfflineFenceWanted`, `internal/controller/restore_offline_fence.go:300-312`) | no | no | yes | yes |
| `BeginPlanned` conflict for planned failover and ordered updates (`internal/controller/deployment_leases.go:732-736`) | **no** (new predicate excludes these phases) | yes | yes | yes |
| MysqlDatabase pause (`groupFenced`, `internal/controller/mysqldatabase_reconciler.go:2426-2433`) | **no** | yes | yes | **yes** (today it resumes on a damaged primary) |
| New deployment leases (`deployment_leases.go:295-296`) | refused (decision 4) | refused | refused | refused, reason `RestoreDamagedPrimary` |
| Scheduled backups | run | run | unchanged | skipped with `ScheduledBackupSkippedDamagedPrimary`: the backup account has no `CONNECTION_ADMIN` (`internal/controller/credentials.go:265`), so `offline_mode` refuses it and the primary fallback would fail. An explicit `MysqlBackup` with `sourceSiteOverride` naming a held site still runs |

PR1 adds `inPlaceRestoreMutating` (in flight and not `Staging`/`Staged`) and `restoreDamagePresent`, and switches the consumers above to them; a unit test pins every cell, including a takeover run in `Staging` and in `Preflight` with a record present.

### 1. Acceptance (entry)

`validateInPlaceRestoreSpec` keeps its checks and replaces the blanket `pointInTime` refusal with the acceptance-pass refusals listed under API; `validatePITRBackupBefore` and `validatePITRBackupBound` both run here, against the referenced backup. A valid PITR run creates its snapshot, then writes `phase=Staging`, `runID`, `runConfirm`, `startTime`, `kind`, `scope=full`, `requestSHA256`, `snapshot` and `staging{}` in one status write, and emits `RestoreInPlaceStagingStarted`. Nothing else happens on that pass. If `status.restoreDamage` exists, the same write (for any full-instance run) sets `restoreDamage.runID` to the new run (exit 1, Event `RestoreInPlaceDamageTakenOver`); the guards never lapse because they key on the record, not on the run.

### 2. Staging

No deployment hold, no topology freeze, no reclone queue, no fence, no `offline_mode` (guards table). A deploy that migrates the schema during staging would be undone by the restore while the new code keeps running, so new leases stay refused for the whole run; existing leases are only waited for in Preflight. Steps, one durable write before each external action:

1. **Source hold.** Stamp `hold.restore.shipstream.io/<runID>` on the `MysqlBackup` with an optimistic lock, then re-read it uncached before any Job is created and refuse if it is gone, terminating or its UID differs from the request, as `stampVerificationHold` and `launchHoldConfirmed` do (`internal/controller/backup_verification_hold.go:308`, `:347`). Retention (`deleteUnreadBackup`, `:418`), artifact cleanup (`internal/controller/backup_reconciler.go:637`) and the PITR binlog cutoff (`cmd/bloodraven/pitr_retention.go:53`) honour restore holds as they honour verification holds. Release rule: see the hold paragraph under API.
2. **Image selection and probe.** Candidate = `staging.image` (`Override`), else the live target's mysqld container `imageID` when it is a pullable `<repo>@sha256:<digest>` reference, else that container's `image` string (images imported into k3d/kind report a bare ID) (`LiveTarget`), else `spec.backup.image` (`BackupImage`). A probe Job (`mysql-<fg>-staging-probe-<run8>-<attempt>`, no volumes, no token, `activeDeadlineSeconds: 120` counted from container start by an in-script timer, and a 10-minute durable bound on pull and scheduling) runs `mysqlsh --version; mysqld --version; mysqlbinlog --version; mysql --version` and reports through the termination message; the digest recorded is the probe pod's own `imageID`. A deadline, `ErrImagePull`/`ImagePullBackOff` past the bound, or an eviction is a lost Job, not an answer. Only a completed probe that reports a missing binary, or a mysqlsh older than the one that wrote the source dump (`@.json`), makes a `LiveTarget` candidate fall back once to `BackupImage`. The probe's parsed `mysqld` `X.Y.Z` must equal the live target's `SELECT VERSION()` numeric prefix and must not be older than the dump's `serverVersion`; otherwise `Failed`/`StagingVersionMismatch` with `versionGate` set (the authoritative comparison is the staged server's own `VERSION()` string at step b). **Unverified:** whether the default `mysql:9.7` image ships mysqlsh (`api/v1alpha1/backup_types.go:9-21` names the community-server image as the mysqlsh carrier), so the design probes and never assumes.
3. **Reuse decision** (here, not at acceptance, because it needs the probe's digest). `mode=Reuse` when a `Verified` artifact has the run's `requestSHA256`, the current #203 revisions of both the backup prefix (`dumpEncryptionRevision`) and the archive prefix (`archiveEncryptionRevision`), the current #206 `archiveIdentity` for the profile, the probe's `imageDigest`, the operator's `formatVersion`, a claim whose UID matches `pvcUID`, and, if `localNode` is set, a node that exists, is Ready and is not cordoned. Else `mode=Build`, which needs a free artifact slot (`StagingArtifactSlotsFull` otherwise; a Reuse run needs none).
4. **Claim (Build).** Append `restoreStagingArtifacts[{originRunID: runID, pvcName: mysql-<fg>-staging-<run8>, state: Provisioning, cleanupPolicy}]` (write-ahead), then create the claim (RWO, `volume.size`, `storageClassName`, owner reference to the group, label `shipstream.io/restore-run=<runID>`), record `pvcUID`, and set `provisioningSince`. A size below `3 x MysqlBackup.status.sizeBytes` emits `RestoreInPlaceStagingVolumeSmall` (a warning: compression ratios vary). The consumer is created **before** waiting for binding: with `volumeBindingMode: WaitForFirstConsumer` (the playground class, `playground/manifests/storageclass.yaml:16`) the claim binds only once a pod is scheduled. Once bound, the operator reads the PV: `nodeAffinity` on `kubernetes.io/hostname` sets `localNode` and emits `RestoreInPlaceStagingVolumeNodeLocal` (zonal affinity is not node-local). The claim bind, pod scheduling and volume attach together must finish within 30 minutes of `provisioningSince` (durable, so restarts do not reset it), else `StagingVolumeUnavailable`.
5. **Staging Job (Build), re-auth Job (Reuse).** Before every creation, including re-creations, an uncached read must show no earlier Job of this run present and every pod labelled `shipstream.io/restore-run=<runID>` terminal (`Succeeded`/`Failed`; a pod with a deletion timestamp that is not terminal still counts as running). The same proof precedes probe, re-auth and result-read Jobs. Then write `jobCreating=true`, `jobName` and `attempt+1` (write-ahead, as `internal/controller/restore_inplace.go:825-835` does for the destructive Job), then create the Job, whose pods carry `shipstream.io/restore-attempt=<attempt>`. Pod: hardened security context (uid/gid 27, read-only root, drop ALL; `internal/controller/backup_job.go:505-560`), `automountServiceAccountToken: false`, no `hostNetwork`, read-only failover tolerations, `backoffLimit: 0`, `activeDeadlineSeconds` from the snapshot. Init containers, in order (operator image): `staging-prepare` checks that `/var/lib/mysql` is writable by uid 27, refuses if `<runID>/owner` names another run, clears `<runID>/` (Build attempts only; the pod-gone proof above ran first) and writes `owner` (runID, attempt); then `decrypt-download` and `pitr-download` with the #203 revision Secret mounted 0400, writing to `<runID>/inputs/` on the claim instead of size-limited emptyDirs. The main script only verifies `owner` and never clears. Re-auth and result-read Jobs never clear anything; they mount the claim read-only, get the 30-minute scheduling bound and `activeDeadlineSeconds = 600 + 2 x artifactBytes / hashBytesPerSecond`. The claim is mounted at `/var/lib/mysql` (the stock AppArmor profile only allows a datadir under `/var/lib/mysql/**`, `internal/controller/backup_verification_job.go:18-33`); `/run/mysqld` and `/tmp` are emptyDirs. A pod failure carrying the `DisruptionTarget` pod condition (eviction, preemption, drain) counts as a lost attempt, not a failure.

Staging Job internals (new `staging.sh`, sharing the mysqld boot with `verify_script.sh` and delegating load and replay to `restore.py`). Layout on the claim: `<runID>/owner` (runID and attempt), `<runID>/inputs/`, `<runID>/data/`, `<runID>/gtid-proof.json`, and the artifact directory `<runID>/artifact/` holding `manifest.json` and `dump/`.

| Step | What | Fails with |
|---|---|---|
| a | `<runID>/owner` names this run and attempt (written by `staging-prepare`; `StagingVolumeNotWritable` there); measure `sourceBytes`, the downloaded dump's total size; free-space check against it. Read the source `@.json`: refuse `X ∩ B ≠ ∅` | `StagingVolumeNotWritable`, `StagingGtidProofFailed` |
| b | Boot mysqld on `<runID>/data` after `--initialize-insecure`, with the target site's effective `mysqlConf` (group merged with site and unsafe keys dropped, as `generateMyCnf` does, `internal/controller/reconciler.go:925`, `:1038`) in an option file minus a pinned denylist (networking and admin ports, replication, binlog and GTID options, plugin and component loading, `init_file`, path options), and on the command line, which overrides option files: `--bind-address=127.0.0.1 --mysqlx=OFF --skip-log-bin --gtid-mode=ON --enforce-gtid-consistency=ON --skip-replica-start --event-scheduler=OFF --skip-name-resolve --partial-revokes=ON --local-infile=1` (the verification set, `internal/controller/verify_script.sh:191-204`, plus the event scheduler); `VERSION()` must equal `BLOODRAVEN_STAGING_EXPECT_VERSION` exactly before anything is loaded | `StagingVersionMismatch` |
| c | Keyring: load `component_keyring_file` as verification does (`:95-153`). Unlike verification it fails closed: if the component cannot load and any dump DDL file contains `ENCRYPTION='Y'`, stop. Record `encryptedTables` | `StagingKeyringUnavailable` |
| d | `util.load_dump` with the staging options (API). Accounts are loaded because `--initialize-insecure` creates only root and the replay pipe (`mysqlbinlog | mysql`, no `--force`, `internal/controller/restore_script.py:503-568`) stops on the first `ALTER USER`/`GRANT`/`SET PASSWORD` against a missing account; the operator's own MysqlDatabase statements are binlogged and land in every window. Then `ACCOUNT LOCK` every loaded account. Record the load duration. **Unverified in repo:** mysqlsh `loadUsers`/`excludeUsers` behaviour on 9.7; the native test covers it | `StagingFailed` |
| e | `_verify_loaded_dump` (post-stop GTIDs already in the dump, lineage; `internal/controller/restore_script.py:581-618`) | `StagingTargetNotCovered` |
| f | Replay through the #206 chronological plan, not one mysqlbinlog run per site | `StagingFailed` |
| g | Staged-GTID proof, below | `StagingGtidProofFailed` |
| h | Verdict: `targetCovered=true`, `partial=false`, `crossSiteUnverified=false` | `StagingTargetNotCovered` |
| i | `SET GLOBAL super_read_only=ON`; optional sanity query inside `START TRANSACTION READ ONLY` | `StagingFailed` (`SanityCheckFailed` detail) |
| j | Delete `inputs/`; `util.dump_instance('<runID>/artifact/dump', {consistent:true, users:false, events:true, routines:true, triggers:true, compression:'zstd', threads})` | `StagingFailed` |
| k | Re-read `@@gtid_executed`; it must equal the proven set and the artifact's `@.json` `gtidExecuted` | `StagingGtidProofFailed` |
| l | Write `artifact/manifest.json`: `{version:1, originRunID, requestSHA256, sourceDumpSHA256, archiveIdentity, dumpEncryptionRevision, archiveEncryptionRevision, files:[{path,size,sha256}], baselineGtids, stagedGtidExecuted, serverVersion, dumpServerVersion, mysqlshVersion, imageDigest, formatVersion, encryptedTables, sourceBytes, createdAt}`; fsync. The hashing throughput is measured here (`hashBytesPerSecond` = bytes / max(1, whole seconds)). Either GTID set above 256 KiB fails here | `StagingFailed` |
| m | Stop mysqld, remove `<runID>/data`; print the result (below) | |

**Staged-GTID proof.** Let `B` be the dump's `@.json` `gtidExecuted` (installed by `replace`), `X` the user's `excludeGtids` (`X ∩ B = ∅`, step a), and from the #206 plan `P` the exact set of transactions committed at or before the stop that the replay must apply, and `Q` every staged GTID after the stop. In-place runs require #206's exact plan (P1), so with `S = @@gtid_executed` after replay the proof is:

- equality: `S = B ∪ (P − X)` (nothing planned was skipped: a short `--stop-datetime` cut, a mid-file stop or a cross-site skip all fail; nothing outside the plan ran);
- no post-stop leak: `S ∩ Q = ∅`.

#206 must emit `P` and `Q` in `coverage.json`; today's per-site `inWindowGTIDs`/`postStopGTIDs` (`cmd/bloodraven/pitr_download.go:225-274`) cover only the single-replaying-site case. The diffs are written to `<runID>/gtid-proof.json` and printed to the log for diagnosis.

**Result channel.** The termination message (4096 bytes) is authoritative and bounded: `BLOODRAVEN_STAGING_COMPLETE v=1 runID= attempt= manifestSha256= stagedGtidSha256= baselineGtidSha256= sourceDumpSha256= archiveIdentity= serverVersion= mysqlshVersion= encryptedTables= sourceBytes= bytes= loadSeconds= hashBps=`. The two GTID sets, at most 256 KiB each, travel in the last log lines (`BLOODRAVEN_STAGING_GTIDS v=1 baseline=<B> staged=<S>`). The existing log reader is best-effort: it tails 50 lines with a 16 MiB line limit (`internal/controller/restore.go:413-429`) and never decides a Job's outcome (`:307-308`). The operator therefore accepts the logged sets only when their digests match the termination message. If the log is missing, truncated or mismatched, the operator runs a result-read Job (the re-auth builder in `ReadResult` mode: read-only mount, checks `manifest.json` against `manifestSha256`, prints the sets from it); each such Job counts toward `lostJobs`. A missing termination message (pod gone) is a lost Job. Status and the `<pvcName>-gtids` ConfigMap, written only by the operator from these checked values, are the trust anchor.

### 3. Staged

On Job success the operator checks the result as above, the versions and `stagingLoadSeconds`, creates the `<pvcName>-gtids` ConfigMap, writes the artifact's certification fields and `state=Verified`, sets `phase=Staged` and emits `RestoreInPlaceStaged`. Next pass: `phase=Preflight`.

In `Reuse` mode, Staging runs a re-authentication Job instead: it mounts the artifact's claim read-only at `subPath: <artifact.path>`, checks `manifest.json` against `manifestSHA256`, re-hashes every file under `dump/` (no missing or extra files), checks the artifact `@.json` set against `stagedGtidSHA256`, and prints the same completion sentinel. A digest mismatch (its own sentinel) makes the artifact `Invalid` and ends the run `StagingArtifactMismatch`. Any other failure (I/O error, eviction, a lost pod, the deadline) leaves the artifact `Verified` and is a lost Job.

**Artifact lifecycle.** Every staging claim has one entry in `restoreStagingArtifacts` from its write-ahead until it is observed gone, so a run's terminal write never has to hand a claim to another list.

| State | Entered | Leaves to |
|---|---|---|
| `Provisioning` | claim write-ahead | `Building` at the staging Job write-ahead; `Invalid` on timeout, cancel, supersede or failure |
| `Building` | staging Job may write | `Verified` (Staged checks pass); `Invalid` (failure, cancel, supersede, attempts exhausted) |
| `Verified` | certified | `Deleting`: (1) a run that loaded it reached `Succeeded`; (2) policy `Delete` when its origin run is terminal and nothing protects it; (3) replacement: another `Verified` artifact passed the Fencing rechecks against the bound target; (4) cleanup annotation. `Invalid` on a digest mismatch found by a re-auth Job, the gtids ConfigMap check or a loader refusal whose sentinel reason is a manifest or digest mismatch |
| `Invalid` | never reusable | `Deleting` once no pod of its origin run can mount it |
| `Deleting` | UID-conditional delete issued (`Preconditions{UID}`) | entry removed when the claim is observed gone |

Protection overrides (1)-(3): an artifact referenced by a non-terminal run is never deleted, and while `status.restoreDamage` exists no `Verified` artifact is deleted automatically (a replacement that then fails a hand-off check must not have cost the previous repair source); deferred deletions run when the record clears. `Invalid` artifacts protect nothing and are deleted as usual. A claim deleted directly is tolerated: the entry is removed with `RestoreStagingVolumeGone`, and a run that references it fails `StagingVolumeUnavailable` before the loader write-ahead. `Verified` entries whose origin run is terminal are what the retained gauge counts. `MaxItems=8` cannot be exceeded because a Build needs a free slot (step 3).

### 4. Preflight

Today's Preflight unchanged (`internal/controller/restore_inplace.go:477-641`, plus #233's peer checks: `BeginPlanned` hold, writable active site, privileges, previous fence released or kept by a damage record this run took over, queue armed, fence recorded), plus:

- with a damage record present: the execution-stopped proof (the damage invariant); a fixed-name loader Job of an earlier run is deleted only after that;
- privileges (all full-instance runs): `RestoreFencePrivileges` (`internal/mysql/restore_fence.go:64-72`) gains `RELOAD` (`FLUSH BINARY LOGS`) and `TRANSACTION_GTID_TAG`; the explicitly assigned tagged `gtid_next` needs `TRANSACTION_GTID_TAG` plus one of `SYSTEM_VARIABLES_ADMIN`, `SESSION_VARIABLES_ADMIN` or `REPLICATION_APPLIER` (MySQL 9.7 Reference Manual, `gtid_next` system variable; tagged GTIDs since 8.3; **unverified in repo**), which the existing `SYSTEM_VARIABLES_ADMIN` requirement covers. `SUPER` implies neither `RELOAD` nor `TRANSACTION_GTID_TAG`, so today's "SUPER covers all but PROCESS" rule (`:149`) becomes a per-privilege list; a unit test and the real-MySQL test use a SUPER-only account;
- for PITR runs: live `VERSION()` of the active site equals the artifact's `serverVersion` (`StagingVersionMismatch`); `S ⊆ L` with `L` the live `gtid_executed` (`StagedGtidsNotInLive`, below); if `encryptedTables`, the live keyring component is active (`performance_schema.keyring_component_status`; `StagingKeyringUnavailable`); claim bound, UID unchanged, not terminating, and if `localNode` is set that node exists, is Ready and is not cordoned (`StagingVolumeUnavailable`); every staging, probe, re-auth and result-read pod of the run terminal;
- loader budget, in whole seconds with saturating arithmetic (capped at 30 days): `hash = ceil(artifactBytes / max(1, hashBytesPerSecond))`; `load = ceil(max(1, stagingLoadSeconds) x max(1, artifactBytes / max(1, sourceBytes)))`, where `sourceBytes` is what staging measured, never `MysqlBackup.status.sizeBytes` (0 means unknown for S3 backups, `internal/controller/backup_script.py:223-231`); `preDestruction = 600 + 2 x hash`. With `loaderActiveDeadlineSeconds = D` set, refuse `StagingLoaderDeadlineTooShort` when `D < preDestruction + 1.5 x load`; unset, emit a Normal Event with the estimate. This is a heuristic, not a proof: the live load runs over the network into a busy server. It is evaluated again at Preflight exit, immediately before Fencing.

**Staged set not a subset of live `gtid_executed`: refuse.** `D = S − L` non-empty means the staged data contains transactions the live history does not record, for example an archived branch a lossy failover abandoned, or a dump from another group. The alternative, `SET GLOBAL gtid_purged='+D'` after the load, rewrites the live primary's GTID history behind the archiver's back: the next archived file's `Previous_gtids` would jump by `D` with no archived events, which the predecessor-ordering planner cannot anchor, and it would quietly resurrect a branch the group abandoned. The message splits `D`: for `D − B` (replayed transactions) adding them to `pointInTime.excludeGtids` and re-running helps (the identity changes, so it re-stages); for `D ∩ B` (transactions inside the dump) nothing in this run can remove them, and only a different backup or a restore into a new group helps. Consequence, documented: in-place PITR from another group's backup or archive is always refused.

### 5. Fencing

Target site bound at entry (`TopologyGeneration++`, existing). Order:

1. strip the primary role label, wait until fenced (existing);
2. `offline_mode=ON` and `event_scheduler=OFF`, recording the prior values in the fence record (`priorEventScheduler` beside the existing `priorOfflineMode`; a scheduler `DISABLED` at startup is recorded and left alone), evict pre-fence sessions (existing, `internal/controller/restore_offline_fence.go:377-462`); the fence release restores both, as it restores `offline_mode` today (`:464-505`). Events loaded with the data would otherwise fire into a half-loaded schema and log writes;
3. rechecks against the bound target: request identity, `VERSION()`, `S ⊆ gtid_executed` (a staging-time failover may have bound a different server), live keyring if `encryptedTables`, claim UID and node, staging pods terminal, artifact hand-off rule. A refusal releases the fence immediately (no Job, no barrier) unless a damage record exists;
4. **barrier** (every full-instance run): allocate `n = 1 + max(status.restoreMarkerSeq, every marker number of any `bloodraven_restore*` tag across all source UUIDs in the target's `gtid_executed`)` and, in one status write, set `restoreMarkerSeq = n` and `restoreBarriers[runID]{n, barrier: {gtid: <target server_uuid>:bloodraven_restore:<n>, state: Writing}}`, **before** committing anything. Because the sequence never decreases and an allocated number is recorded before use, no number is ever handed out twice, even when a barrier is never committed. Then `FLUSH BINARY LOGS`; `SET GTID_NEXT='<uuid>:bloodraven_restore:<n>'; BEGIN; COMMIT; SET GTID_NEXT='AUTOMATIC'`; `FLUSH BINARY LOGS`; `state: Written`, Event `RestoreBarrierWritten`. An empty transaction is logged only under an explicitly assigned GTID; with `AUTOMATIC` (tagged or not) it gets none (MySQL 9.7 Reference Manual, GTID lifecycle; **unverified in repo**). Retry is idempotent: on restart in `Writing` while the run is non-terminal, the recorded GTID already in `gtid_executed` means it was committed; otherwise both statements run again. In `Written` the second flush is re-run unconditionally (an extra rotation is harmless). The flushes put the barrier alone in its own sealed binlog file;
5. #233 peer IO stop with catch-up; `fencedGtidExecuted` now includes the barrier;
6. if PITR archiving is enabled, wait up to 10 minutes for the primary's sidecar to report the barrier archived (new `GET /archiver/covers?gtids=<set>`: whether a sealed archived file covers the set), then `state: Archived`. Timeout ends the run `RestoreBarrierNotArchived` before any Job;
7. Event `RestoreInPlaceWritesDiscarded` (requested stop, fence time), `phase=Restoring`.

### 6. Restoring

Today's destructive path (`internal/controller/restore_inplace.go:772-956`) with these changes:

- **Immediately before the write-ahead**, the operator re-reads the target `VERSION()`, the live keyring if `encryptedTables`, and the request identity (refusals as in Fencing; a version refusal here does not consume the confirm, Q7's "immediately before the DROP" check).
- **Damage write-ahead (every full-instance run, PR1).** The status update that sets `jobCreating=true` also creates `status.restoreDamage{originRunID: runID, runID, site, since}`, unless a record already exists (this run then owns it from its acceptance write). From this write on the damage invariant holds. If the API server refuses the Create (Invalid/Forbidden/BadRequest, which today withdraws the write-ahead, `:836-845`), a record this run created in that write is withdrawn with it.
- **Source:** the artifact's claim (`pvcUID` checked uncached before Create), `subPath: <artifact.path>`, read-only at `/staging` (`/staging/manifest.json`, `/staging/dump`); image: the artifact's `imageDigest`; no `pitr-download` or `decrypt-download` init containers; loader options as in API; no `updateGtidSet` (the live-target rule, `internal/controller/restore.go:937-949`, `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline`). Pods carry `shipstream.io/restore-loader=<group>` (every full-instance loader, PR1). `activeDeadlineSeconds` = `loaderActiveDeadlineSeconds` (none when unset) for PITR runs; non-PITR runs keep `spec.backup.activeDeadlineSeconds`.
- **Env:** `BLOODRAVEN_RUN_ID`, `BLOODRAVEN_STAGING_MANIFEST_SHA256`, `BLOODRAVEN_STAGING_GTID_SHA256`, `BLOODRAVEN_EXPECT_TARGET_VERSION`, `BLOODRAVEN_EXPECT_KEYRING` (when `encryptedTables`), `BLOODRAVEN_PRE_DESTRUCTION_DEADLINE`: absolute time `T0 + min(preDestruction, D − 1.5 x load)` (Preflight budget; the second term only when `D` is set), where `T0` is the operator's clock at the write-ahead. `T0` is not later than the Job's creation, and Kubernetes counts `activeDeadlineSeconds` from `status.startTime`, which is not earlier, so the bound is conservative.
- **Pre-destruction refusal path (PR1, all full-instance loaders).** `restore.py` sets a module flag immediately before its first mutating statement (`_maybe_reset_replication`, then `_maybe_preflight_drops`, `internal/controller/restore_script.py:880-883`). Before that point every refusal goes through one function, `_refuse_before_destruction(reason)`, which asserts the flag is unset, writes `BLOODRAVEN_PRE_DESTRUCTION_REFUSED v=1 runID=<id> job=<name> reason=<r>` to the termination message and exits with a dedicated code (proposed 70; today every failure exits 2, `:399-402`, `:700-742`, `:887-894`). The refusals routed through it: `_verify_staging_artifact` (manifest digest, every file size and SHA-256, no extra files, artifact `@.json` set digest; reason `ArtifactMismatch`, which makes the artifact `Invalid`), target `VERSION()`, live keyring active when expected, `_prepare_local_infile`, `_require_offline_mode`, and the pre-destruction deadline (checked after every hashed file and immediately before the flag is set). After the flag is set the function falls through to exit 2. **Unverified in repo:** that `mysqlsh --py -f` propagates `sys.exit(70)` as the container exit code; PR1 pins it with a native test.

The full re-hash runs while fenced; for a large artifact it adds minutes to the outage. That is the price of turning a truncated or altered claim into a refusal before the DROP instead of a load failure after it.

### 7. Resuming and Succeeded

After the loader Job succeeds: write `restoreBarriers[runID].completion{gtid: <uuid>:bloodraven_restored:<n>, state: Writing}` (same uuid and `n` as the barrier), commit it with the same explicit-GTID statements (idempotent the same way), `Written`, Event `RestoreCompletionWritten`; an unreachable primary is retried with the fence up, since the load already succeeded. Only then release the queue to `Draining` and write `Succeeded`, which clears `status.restoreDamage` in the same write (clearing proof (a); the run owns the record, and only full-instance runs can). The completion marker therefore precedes the fence release and every reclone, so recloned peers carry it and held peers do not. The hold on the source backup is released, and the artifact the run loaded goes to `Deleting` (success always deletes).

### 8. After a failed or lost loader Job

Two independent questions:

1. **May the peers resume replication?** #233 settlement: Held vs Discarded from the Job pods and `gtid_executed == fencedGtidExecuted` (`restorePrimaryUnchanged`, `internal/controller/restore_reclones.go:840-891`). PR2 changes that comparison to drop every `bloodraven_restore*` tag from both sets first: markers carry no data, so a void or accept marker committed before or after a restart can never turn Discarded into Held. An unlogged load writes nothing to the binlog, so Discarded is correct for the peers even when the primary was changed.
2. **May the primary serve?** Only under the damage invariant: proofs (b) and (c) are evaluated here, against the run that created the record. A run that took a record over and whose own loader refused or never started leaves the record exactly as it was.

**Void.** A run is *proven undamaged* if it ended before its loader write-ahead, or if its own loader produced the (b) or (c) evidence, whether or not it inherited a record. Every proven-undamaged run that allocated a barrier number (state `Writing` or later) gets `<barrier uuid>:bloodraven_restore_void:<n>`: write-ahead in `restoreBarriers[runID].void`, then committed with an explicit `GTID_NEXT` on the group's current writable primary (legal on any server, so a failover in between does not matter), idempotent like the barrier, Event `RestoreBarrierVoided`. Voiding a barrier that was never committed is harmless and reserves `n` in-band. The void is independent of the fence: without a damage record the fence follows #233 (including its `restoreSettleBound` release, `:827`), and the void is retried until it lands; until then the barrier stays effective, which only over-refuses. With a damage record the fence stays, whatever the void does. An inherited record keeps the origin run's barrier effective, so the void never hides earlier damage.

### 9. Post-restore backup and barrier closure

After `Succeeded` or an accept, once the fence release is observed (the backup account cannot connect earlier), with PITR archiving enabled, the operator creates `MysqlBackup` `<fg>-post-restore-<run8>` on the PITR profile with `spec.dumpScope: Instance` (Event `PostRestoreBackupCreated`). A barrier entry closes when (a) a `Succeeded` backup of the group with `status.dumpScope=Instance` and `status.consistent=true` has a `status.gtidExecuted` containing a *closing marker* (a completion or an accept marker) numbered `n` or higher (`gtidExecuted` is read before the dump starts, `internal/controller/backup_script.py:317-320`, so the snapshot is after the load), or (b) its void is committed. Closure removes the entry and emits `RestoreBarrierClosed`; with no open entry, `PITRArchiveReady=False/PITRRestoreBarrier` clears. A failed backup emits `PostRestoreBackupFailed` and is not retried automatically; any later qualifying backup closes the barrier. With PITR archiving disabled the markers are still written (in-band, free), the archive wait and the automatic backup are skipped, and an entry closes when its void or a closing marker numbered `n` or higher is committed. At `MaxItems=32` the oldest entry whose markers are all `Written` and whose run is terminal is dropped with an Event (never one in `Writing` or with a pending void): status is display only, so dropping an entry never weakens the planner.

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
| Staging, probe, re-auth or result-read Job lost (deleted, never created after write-ahead, pod evicted, result unreadable) | Re-created automatically after the pod-gone proof; `lostJobs` counts them across kinds and the third ends `StagingFailed`; only a Build pod's `staging-prepare` clears `<runID>/`. No live damage | attempt, lostJobs | after 3: bump confirm |
| Staging Job failed | `Failed`/reason; confirm consumed (reason table); artifact `Invalid` | run status | fix cause, bump confirm |
| Claim lost, node lost or provisioning timeout during Staging | `Failed`/`StagingVolumeUnavailable`; no damage | run status | bump confirm (new claim) |
| Failover during Staging | Allowed. Staging continues (it reads only the backup and archive). The target is bound at Fencing; rechecks there catch a version change or a lost transaction (`StagedGtidsNotInLive`) | none extra | none, or re-run with `excludeGtids` (only for `D − B`) |
| Planned failover or ordered update during Staging | Allowed (guards table); a version change is caught at Preflight/Fencing (`StagingVersionMismatch`, gated by `versionGate`) | versionGate | wait for the rollout, bump confirm or leave it to the gate |
| Spec removed in Staging/Staged | `RestoreInPlaceCancelled`; staging Job deleted (foreground); a `Provisioning`/`Building` artifact goes `Invalid`, a `Verified` one follows its policy, a reused one is untouched | run status | none |
| Request identity changed before the loader write-ahead | `RestoreInPlaceSuperseded`, confirm not consumed; Jobs deleted; the run's own build follows the same rule as spec removal; the next pass accepts a run from the current spec | run status | none |
| Crash or failure in Preflight | As today: no fence, emergency failover available | run status | none |
| Failure in Fencing before the barrier | Fence released unless a damage record exists, queue discarded (no Job, #233 rule), confirm per reason | run status | per reason |
| Failure in Fencing after the barrier is allocated (peer stop timeout, archive wait, barrier commit refused) | No Job; settlement `Discarded` (markers ignored); run proven undamaged, so the barrier is voided; fence released unless a damage record exists | barrier, run status | none |
| Crash before the loader write-ahead | Rebuilt on the next pass (existing) | — | none |
| Crash between the loader write-ahead and Create, or loader Job lost | `RestoreInPlaceJobLost`, never rebuilt (existing, `internal/controller/restore_inplace.go:766-771`); no evidence, so the damage record stays | run, damage record | three exits |
| Loader refused through the pre-destruction path, or its main container never started | Proof (b) or (c) clears the record only if this run created it; barrier voided; settlement `Discarded`; fence released only if no record remains; artifact stays `Verified` (`Invalid` on an `ArtifactMismatch` refusal); `RestoreInPlacePreDestructionRefused`, confirm consumed | run status | fix, new confirm (reuses the artifact) |
| **Any other end of a loader after its write-ahead** (failed, killed, OOM, evicted, deleted, lost, deadline) | see below | damage record | three exits |
| Group deleted | Owner references delete Jobs and claims; holds on backups are released by dead-hold stripping (`internal/controller/backup_verification_hold.go:247-270` generalized) | — | none |
| Operator upgrade mid-run | The new operator resumes from status. Avoid it: an upgrade during Restoring is already discouraged (#233) and staging makes runs hours longer | status | — |
| Operator downgrade mid-run | Unsupported, and unsafe below PR1: an older operator resets an unknown phase to no status (`internal/controller/restore_inplace.go:448-450`), and an older CRD prunes `status.restoreDamage`, so it can unfence a damaged primary. Operators from PR1 onwards treat an unknown phase as in flight: topology frozen, fence kept, no Job created or deleted, Warning `RestoreInPlaceUnknownPhase`. The upgrade-policy page says so | status | upgrade back |

**What a loader without exit proof leaves behind.** Not the old data on the primary: the DROP and a partial load are not undone.

- Primary: every damage guard on (the damage invariant), independently of any run's phase and of whether `spec.restoreInPlace` still exists. `RestoreInPlaceDamagedPrimaryFenced=True`, `Ready=False`, alert.
- Peers: `Held` with pre-restore data and their IO threads stopped (#233), excluded from promotion and reader Services; or, after an unlogged load that #233 settled `Discarded`, replicating again from a fenced primary that accepts no writes.
- Sources and artifacts: every restore hold of the group and every `Verified` artifact are kept, so any earlier repair source stays usable.
- Exits, none automatic:
  1. **New confirm** (any full-instance restore). The run takes the record over in its acceptance write and, at Preflight, needs the execution-stopped proof. It reuses a matching artifact (minutes of re-authentication) or re-stages. Only its `Succeeded` clears the record.
  2. **Accept** with `restore-accept-damaged=<originRunID>` and the execution-stopped proof. The accept marker makes the accepted dataset a PITR baseline once a backup carries it. The peers stay (or become) `Held` for #233's exits; setting that queue to `Draining` reclones them from the accepted primary and destroys what may be the only intact copies: take a backup from a held site first.
  3. **Rebuild from a held peer**: a backup with `sourceSiteOverride` naming a held site (allowed while frozen, `internal/controller/backup_reconciler.go:1471-1484`), then a full-instance restore from it, which is exit 1 with that source.
- **Primary permanently down.** Exits 1 and 3 load into the damaged primary, so they need it running, and exit 2 must commit its marker on it. Do not accept: take a backup with `sourceSiteOverride` from a held site and restore it with `initFromBackup` into a new group, the supported procedure. If the lost machine's loader pod is still listed, it must be confirmed shut down or fenced from MySQL before its objects are removed (the damage invariant); that matters for any later exit on this group, not for the new one.

## GTID and archive lineage (Q3)

**Keep the live superset.** The live primary keeps its full `gtid_executed`, including the GTIDs of every discarded post-stop transaction, and the restored data is loaded unlogged (`skipBinlog:true`, forced for every full-instance in-place restore; explicit `false` is rejected). Peers inherit the set through CLONE. The DROPs stay binlogged. Consumers of the binlog outside `spec.sites` (an external async standby, which #233 already lists as not held, or a CDC pipeline) therefore receive the DROPs but not the reload, where today an omitted `loadOptions` gives them a binlogged load (`site/content/docs/7.backup-and-restore/2.backup-restore.md:639-643`). Known-limitations and the restore procedure say so.

**Why not RESET.** `RESET BINARY LOGS AND GTIDS` followed by `replace` would make the live set equal to the staged set, but the server would then reissue GTID numbers that already sit in the archive for the discarded transactions. The archiver's reuse detector (`internal/sidecar/binlog_reuse.go:115-154`) proves the overlap and records `Epoch.ResetReason = "verified GTID namespace reuse ... use a new manifest prefix and full backup"` (`:184-195`); the archive audit then errors for the site (`internal/sidecar/binlog_history.go:76-77`), retention stops (`internal/sidecar/binlog_archiver.go:378`, `:428`), and the planner refuses every window on that site (`cmd/bloodraven/pitr_window.go:115-116`). RESET would turn every in-place restore into a forced archive migration.

**In-band markers**, all empty transactions under explicitly assigned tagged GTIDs. Numbers come from `status.restoreMarkerSeq` and the `gtid_executed` maximum (Fencing step 4), so a number is never handed out twice in a group, even across lossy failovers or a barrier that was never committed. A run's barrier, completion and void share its `n` and the barrier's `server_uuid` (a void committed after a failover still uses that uuid); an accept takes a fresh number:

| Marker | Committed | Means |
|---|---|---|
| barrier `bloodraven_restore:<n>` | Fencing, after the application fence, before the peers stop and before any loader Job; archived before the Job when archiving is enabled | data may change without binlog events after this point |
| completion `bloodraven_restored:<n>` | Resuming, after the loader succeeded, before the fence release and every reclone | closing marker: the load finished; a snapshot after it holds the restored dataset |
| void `bloodraven_restore_void:<n>` | after the run is proven undamaged (Lifecycle step 8), on the current writable primary | barrier `n` changed nothing; the planner ignores it |
| accept `bloodraven_restore_accepted:<m>` | exit 2, on the damaged primary, before its fence release | closing marker: the administrator accepted what the primary holds as the new baseline |

Because they are in the GTID stream: every archive reader sees them in the per-file GTID coverage, including verification Jobs and other groups restoring from this archive; a backup's `gtidExecuted` says which side of each marker its snapshot is on, even when no other write happened in between (a GTID-set boundary such as `fencedGtidExecuted` alone is ambiguous in exactly that case); no clock comparison is needed. A backup that contains the barrier proves nothing about its data: a backup of the primary during the archive wait, or of a peer that caught up to the barrier and was then held, contains it and holds pre-restore data. Only a closing marker certifies a post-restore snapshot.

Rejected alternatives: reusing `Epoch.ResetReason` (blocks the whole site forever); a JSON barrier object in the prefix keyed by time or by `fencedGtidExecuted` (ambiguous at equality, written by a second writer into a single-writer layout); `AUTOMATIC:<tag>` with an empty transaction (MySQL assigns no GTID, so nothing is written); adopting "any unrecorded `bloodraven_restore` GTID" after a crash (it can pick up an earlier restore's barrier). Tagged GTIDs need MySQL 8.3+ (the 9.7 line is the only supported one, `site/content/docs/6.operations/10.upgrade-policy.md:24`); the operator already parses tagged GTID sets (`internal/mysql/gtid.go:10-12`). Tagged explicit `gtid_next` against `mysql:9.7` is **unverified in this repo** and is checked first in PR2. If it does not hold, PR2 stops and this section comes back for review; no fallback is designed here (a marker table would sit in a schema the full-instance DROP, dump and load all handle, which is its own design).

**Planner rules** (`planPITRArchive` and #206's replacement; one function used by restore, staging and verification). Let a barrier be *effective* if no void with its `(uuid, n)` appears in the base set or anywhere in the archive. Marker tags are recognized by tag only; their uuid is irrelevant to numbering.

1. **Base rule.** Let `n*` be the highest number of any effective barrier in the base backup's `gtidExecuted`. If `n*` exists and the base contains no closing marker numbered `n*` or higher, refuse `PITRRestoreBarrier` ("the backup was taken inside restore `n*`"). A later completion or accept closes earlier damaged barriers, because the dataset was replaced or accepted as a whole.
2. **Window rule.** Refuse if an effective barrier not in the base lies at or before the stop (in `P`, or anywhere in a staged file not wholly after the stop). A barrier after the stop (in `Q`) is harmless: `S ∩ Q = ∅` already proves it was not applied. So the supported new-group procedure from a pre-restore backup to a time before the restore still works, and so does an exit-1 re-stage to the same stop.
3. The rules run before `excludeGtids` is applied, and acceptance refuses `excludeGtids` that names any marker, so exclusion cannot bypass them.

**Runs that never complete.** A run proven undamaged gets its void, so readers lose no coverage. A run whose loader may have touched the primary keeps an effective barrier forever: PITR from older backups across that instant is refused, which is correct because the archive cannot replay the unlogged change. A backup carrying a later completion or the accept marker becomes the new base, so PITR forward from the repaired or accepted dataset works again.

**Post-restore backup.** See Lifecycle step 9. Until it completes, `PITRArchiveReady=False/PITRRestoreBarrier` and an alert make the PITR gap visible.

## Interaction with the rest of the operator

The guards table under Lifecycle is the normative list; in prose:

- **Topology manager.** Not frozen during Staging/Staged/Preflight; emergency failover works. Frozen from Fencing through Resuming and while `status.restoreDamage` exists. #233's restore protection (start gate, re-stop, clone admission, promotion exclusion) is unchanged and applies from Fencing.
- **Planned failover and ordered updates.** Allowed during Staging/Staged because `BeginPlanned` stops treating those phases as a concurrent operation; caught by the version and subset rechecks. Preflight waits for them (#233). A planned failover to a held site stays refused.
- **MysqlDatabase.** Not paused during Staging/Staged (account and password management keep working; the restore discards their effect on data, and accounts are not rewound). Paused from Preflight and while a damage record exists, so the operator never runs DDL or account statements against a damaged primary.
- **Backups and retention.** Scheduled backups continue during Staging. The source backup and its binlogs are held, and while a damage record exists so is every restore-held backup of the group. Backups record `dumpScope` and `consistent`; only `Instance`+consistent backups can source a full-instance restore or close a barrier. While a damage record exists scheduled backups are skipped with a reason instead of failing against `offline_mode`; explicit backups from held sites run.
- **Verification.** Unchanged, except that PITR verifications are subject to the barrier rules. The shared mysqld boot moves into a library both scripts source. Verification loads no accounts, so a window containing account statements fails there too; that is a separate fix.
- **Encryption.** Inputs are decrypted with the #203 revision. The intermediate artifact is plaintext on the claim (Q1 A). Live `encryptionAtRest` is unaffected: the logical load recreates `ENCRYPTION='Y'` tables under the live keyring, which Preflight, Fencing and the loader check is active before the DROP when the artifact has encrypted tables.
- **Deployment API.** `operations.restoreInPlace` (`site/content/docs/4.configuration/11.deployment-api.md:132`, `:153`) reports `Staging` and `Staged`; leases are refused with `ConcurrentOperation` for the whole run, with `RestoreReclonePending` while peers are queued or held (#233), and with `RestoreDamagedPrimary` while a damage record exists.
- **Bootstrap restores.** `initFromBackup` gains the source hold (PR3) but no markers: a new group has no earlier lineage.

## Security

- **Plaintext at rest.** The staging claim holds decrypted inputs (until step j), the staging datadir (until step m) and, for a retained artifact, a plaintext logical dump of the whole instance. Encrypting only the intermediate dump would protect nothing while the datadir sits next to it during the run. Docs recommend an encrypted, replicated StorageClass and namespace RBAC that limits who can mount claims. Retained artifacts are listed in status, counted in a gauge and alerted on.
- **Keyring.** The staging keyring is ephemeral (`/run/mysqld` emptyDir) and dies with the pod; the datadir it protects is deleted before the pod exits.
- **Loopback root and loaded accounts.** Passwordless `root@127.0.0.1` and the dump's accounts (locked after the load) are reachable only from containers in the same pod, all of which are operator-built. No Service, no token, no host network, mysqlx off. Pinned by a unit test of the Job spec and the script flags.
- **Supply chain.** Image digests are recorded and reused for the loader, so the mysqlsh that loads the artifact is the binary that wrote it.

## Observability

- **Structured logs** (camelCase fields; `site/content/docs/8.observability/7.log-schema.md` updated in the PR that adds each): INFO `restore staging started`, INFO `restore staging job created`, WARN `restore staging volume is node-local`, WARN `restore staging refused` (`reason`), INFO `restore staging verified` (`runID, manifestSha256, stagedGtidSha256, bytes`), INFO `restore staging artifact reused`, INFO `restore staging artifact deleted`, INFO `restore barrier written` (`runID, site, gtid`), INFO `restore barrier archived`, INFO `restore completion written` (`runID, site, gtid`), INFO `restore barrier voided` (`runID, gtid`), INFO `restore barrier closed` (`backup`), ERROR `post-restore backup failed`, WARN `restore request changed; run superseded` (`runID, requestSHA256`), ERROR `in-place restore left the primary fenced as possibly damaged` (`originRunID, runID, site, jobName`), INFO `in-place restore loader refused before destruction` (`runID, jobName, reason`), INFO `damaged-primary record cleared` (`originRunID, runID, proof`), INFO `restore accept marker written` (`originRunID, gtid`), INFO `damaged-primary record taken over` (`originRunID, runID`), INFO `scheduled backup skipped: damaged primary`, WARN `unknown in-place restore phase; holding guards`. No existing `msg` changes.
- **Alerts** (documented in `8.observability/2.monitoring.md` and `5.alert-runbook-map.md`): `BloodravenRestoreDamagedPrimaryFenced` (critical, `bloodraven_restore_damaged_primary_fenced == 1`), `BloodravenRestoreStagingRetained` (warning, retained artifacts for more than 24 h), `BloodravenPITRRestoreBarrierOpen` (warning, open for more than 6 h), `BloodravenRestoreInPlaceLoaderSlow` (warning, `Restoring` longer than 2x the loader estimate).
- **bloodraven-doctor** (maintenance contract): `SKILL.md` (staging phases, damaged fence, barrier), `references/alert_runbook_matrix.md` (every new reason, Event and alert; `RestoreInPlaceRejected` row at `:55` rewritten), `references/troubleshooting_playbooks.md` (playbook 19 rewritten from "pointInTime rejected" to the in-place PITR procedure; new playbooks for a damaged fence and its three exits, an open barrier, a retained artifact, `StagedGtidsNotInLive` split by `D ∩ B`, `StagingVersionMismatch`, a permanently down damaged primary), `scripts/triage.sh` (staging status, artifact list, `status.restoreDamage`, barriers and their markers), `scripts/deployment-probe.sh` (new phases), `scripts/sidecar-probe.sh` (`/archiver/covers`), `scripts/support-bundle.sh` (staging and probe Job logs, claims).

## Docs that change when implemented

| Page | Change |
|---|---|
| `site/content/docs/7.backup-and-restore/2.backup-restore.md` | Rewrite "Point-in-time recovery is not supported in place" (`:971-999`) into the in-place PITR procedure with the supported-procedure statement first; full-instance phases, `skipBinlog` forced and `excludeSchemas` refused (also in the phases list, `:624-650`), accounts not rewound, the source-scope requirement, the damage invariant and exits, markers and post-restore backup, sizing formula, loader budget |
| `site/content/docs/3.get-started/1.known-limitations.md` | Replace "in-place cannot PITR" (`:116-121`) with the remaining limits: full-instance only, full-instance in-place restores only from an `Instance`+consistent `mysqlBackupRef` (raw S3/PVC sources only per-schema), no foreign source, accounts not rewound, plaintext claim, external binlog consumers see the DROPs but not the reload |
| `site/content/docs/4.configuration/2.crd-reference.md` | `RestoreInPlaceSpec.staging`, new types, phases, status fields, conditions; `MysqlBackup` `spec.dumpScope`, `status.dumpScope`, `status.consistent` |
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
- Flip `TestReconcileInPlaceRestore_PointInTimeRejected` (`internal/controller/restore_inplace_test.go:1150`) into acceptance into `Staging`, and add acceptance-pass rejection tests: for every full-instance restore `excludeSchemas`, `skipBinlog:false`, an S3/PVC source, and a backup that is not `Succeeded`, has no location, is `Filtered`, is not consistent, has no scope recorded, or has an empty or unparseable `gtidExecuted`; a per-schema restore while a damage record exists; for PITR `allowPartial`, `allowUnverifiedCrossSite`, `includeSchemas`, `loadIndexes:false`, a stop before the backup's completion, `excludeGtids` intersecting the bound or naming a marker, and a bound not in live history. Backup side: `dumpScope`/`consistent` taken from the sentinel, not the profile (edit the profile after the backup), and `spec.dumpScope: Instance` overriding a filtered profile.
- Keep `TestBuildInPlaceRestoreJob_RefusesPointInTime` (`:1236`) as a loader invariant (the loader never replays), and `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline` (`internal/controller/restore_test.go:537`).
- Snapshot and identity: the snapshot holds every deferred field and is read after a restart while the spec has changed; a missing or altered snapshot ends `RestoreInPlaceSnapshotLost`; canonical JSON golden files; `GTIDSet.Canonical()` equivalence fixtures (`u:1:2` = `u:1-2`, overlaps, duplicates, tag order); every row of the identity table in every pre-Job phase (confirm-only bump rebinds; `stopDatetime`, `excludeGtids`, source, backup UID, a deleted backup, `sanityCheck`, image or kind edits supersede; confirm regression plus identity change cancels); a loader is never built from the current spec. `versionGate`: an unchanged key blocks the restart, a changed source backup, image, target version or confirm re-evaluates.
- Damage invariant and guards: one test per cell, including a takeover run in `Staging` and `Preflight` with every guard on; each clearing proof (a)-(d) positive and negative: (b) and (c) refused for an inherited record, (c) refused when the main container has `startedAt` or `lastState`, (d) refused while a non-terminal labelled pod or unfinished labelled Job exists; no clear by settlement, timeout or unreachable MySQL; a per-schema `Succeeded` cannot happen while a record exists; restore holds of three successive takeover runs all kept until the record clears.
- Staging Job builder: pinned flags, no token, no host network, no Service, AppArmor paths, claim mount, revision Secret, deadline, `mysqlConf` denylist; init order (`staging-prepare` before the downloads; re-auth and result-read never clear); probe Job (four binaries; deadline, pull failure and eviction are lost Jobs); image selection matrix (override, live digest, live bare ID, live without mysqlsh, live too old, fallback mismatch, target changed between staging and Fencing).
- Phase machine: every row of the crash table; pod-gone proof before each re-creation (a terminating pod blocks it); `DisruptionTarget` counted as lost; WaitForFirstConsumer ordering and the durable provisioning timeout across a restart; result channel (log missing, truncated, digest mismatch, result-read Job); `versionGate` preventing a restart loop; every refusal's confirm consumption.
- Artifacts: the lifecycle table; reuse decided after the probe; reuse mounting `<originRunID>/artifact`; replacement deferred while a damage record exists and while the replacement has not passed Fencing; cancel of a Reuse run leaves the artifact `Verified`; `Invalid` only on a digest mismatch (including a loader `ArtifactMismatch` refusal) and deleted even under a damage record; a slot needed only for Build; reuse skips an artifact on a missing, NotReady or cordoned node; the gtids ConfigMap checked against both digests; UID-conditional deletes; direct claim deletion.
- Damage lifecycle: write-ahead in the same update as `jobCreating`; kill, OOM, deadline, eviction, deleted or missing pod and JobLost all leave it; a takeover run's Staging failure, Preflight refusal, loader refusal, cancellation and supersession all leave it; accept refused for a stale runID; accept after a Discarded settlement arms a Held queue. Loader budget: zero and unknown sizes, zero-duration hashing, saturation, and the deadline-bounded pre-destruction time.
- Markers: allocation from `restoreMarkerSeq` and `gtid_executed` (Opus round-2 sequence: an uncommitted barrier, a later run, and a stale void never collide); idempotent retry from `Writing` and `Written`; void on the current primary after a failover; void for a never-committed barrier; completion before queue release; accept marker before the accept release; settlement `Discarded` with a void or accept marker present, across a restart; closure only by an `Instance`+consistent backup carrying a closing marker numbered `n` or higher; the status size budget with every list at its maximum.
- Planner (`cmd/bloodraven`): base rule and window rule fixtures, including a base inside restore `n`, a base after a later completion, a base after an accept, a barrier after the stop, a voided barrier, and the `excludeGtids` bypass attempt.
- Scripts under python3/bash with stub mysqlsh (pattern of `internal/controller/restore_script_offline_test.go`, `pitr_restore_script_test.go`): exact staged-GTID proof cases, `X ∩ B` refusal, sentinel and size bounds, manifest writer, `_verify_staging_artifact`, and that `_refuse_before_destruction` is unreachable once the destruction flag is set and that no refusal before it exits with any other code.

**Component (`test/component`).** Real `TopologyManager` with `FakeReplica` (#233's harness): failover and planned failover during Staging leave the run unfrozen and rebinding at Fencing; held peers plus the damage record through operator restarts; takeover by a new run without an unfenced gap; accept-and-release.

**Envtest (`test/envtest`).** CEL pairing rule and enum rejection; status round-trip for every new type; claim owner references are set (envtest runs no garbage collector, `test/envtest/suite_test.go:42-50`, so cascading deletion is tested in a playground scenario); the damage record, artifact list and barriers surviving reconciler restarts; restore holds blocking retention and cleanup.

**Real MySQL, opt-in** (pattern of `TestRestoreFence_RealMySQL`, `internal/mysql/restore_fence_mysql_test.go:20`, and `BLOODRAVEN_PITR_NATIVE_TEST`, `internal/controller/pitr_restore_script_test.go:323`): `BLOODRAVEN_STAGING_NATIVE_TEST=1` runs `staging.sh` against a native mysqld/mysqlsh/mysqlbinlog with a generated dump and binlogs, asserting the proof, account loading with an `ALTER USER` in the window, the read-only sanity check and that the artifact `@.json` equals the proven set; `BLOODRAVEN_TEST_MYSQL_ADMIN_DSN` covers the three explicit tagged markers (an `AUTOMATIC:<tag>` empty transaction asserted to log nothing), the privilege set and `RELOAD`, and the loader's exit code 70 through `mysqlsh --py -f`.

**Playground scenarios** (IDs after #233's 62):

| ID | Injects | Asserts | Profile |
|---|---|---|---|
| `63-in-place-pitr-restores-to-stop-time` | marker rows before the stop, rows and a dropped table after it, an `ALTER USER` in the window, an event writing every second | `Succeeded`; no event write between Fencing and the fence release; pre-stop rows present, post-stop rows absent, dropped table back; peers recloned and replicating; staging claim deleted; completion marker on every peer; barrier closed by the post-restore backup; a PITR verification from the pre-restore backup across the restore is refused `PITRRestoreBarrier`, one from the post-restore backup passes; deleting the group removes the claims | **release** |
| `64-in-place-pitr-fails-after-drop-keeps-fence` | deletes the loader pod once the user schemas are gone; then a new confirm whose loader pod is deleted before its DROP, then another | `Failed`; `RestoreInPlaceDamagedPrimaryFenced=True`; primary Service has no endpoints, `offline_mode=ON`, topology frozen; queue `Held`, markers on every peer; artifact retained; the second run (no user schemas left, so nothing logged) also ends with the fence up; the third reuses the artifact (`mode=Reuse`) and ends `Succeeded` | **release** |
| `65-in-place-pitr-planned-failover-during-staging` | planned failover while `Staging` | staging continues; target bound to the new primary at Fencing; `Succeeded` | full |
| `66-in-place-pitr-operator-restart-each-phase` | operator pod deleted in Staging, Staged, Fencing (between barrier write-ahead and commit), Restoring | no duplicate Job or claim, one barrier GTID per run, no unfenced gap, `Succeeded` | full |
| `67-in-place-pitr-cross-site-window` | failback A to B to A inside the window (needs #206) | every site's transactions present; staged proof equality | full |

Both release scenarios reset MySQL first. #233's scenario 62 moves to `release` in PR1, because the damage fence otherwise has no CI signal (#236).

## Phased implementation plan

Each PR passes the AGENTS.md pre-PR gate on its own and ships its own docs, log-schema and doctor updates. `pointInTime` stays rejected for real runs until PR6; PR5 accepts it only with its hidden flag on, and then stops after `Staged`.

| PR | Scope | Depends on | Size |
|---|---|---|---|
| PR1 | The damage invariant for every full-instance restore: `status.restoreDamage` written with the loader write-ahead, the four clearing proofs, `restore.py` pre-destruction refusal path and exit code, `shipstream.io/restore-loader` label and the label-based execution-stopped proof, takeover by any full-instance confirm, per-schema refusal while damaged, accept (with Held queue after a Discarded settlement) and plugin command, `event_scheduler` in the fence, guards (`inPlaceRestoreMutating`, `restoreDamagePresent`, `BeginPlanned`, `groupFenced`, lease reason, backup skip, all group restore holds once PR3 lands), condition, gauge, alert, unknown-phase fail-safe, DST restore-guard actor, scenario 62 to release | #233 (merged) | M-L (~2.5k lines with tests) |
| PR2 | Markers (barrier, completion, void, accept; `restoreMarkerSeq`, explicit tagged GTIDs, write-ahead, both flushes), settlement ignoring marker tags, archive wait and sidecar `/archiver/covers`, planner base and window rules, `MysqlBackup` `status.dumpScope`/`consistent` and `spec.dumpScope`, automatic post-restore backup, `PITRArchiveReady` reason, `RELOAD` and `TRANSACTION_GTID_TAG` preflight (SUPER no longer covering them), and for every full-instance restore: forced `skipBinlog:true`, refused `excludeSchemas`, `mysqlBackupRef` with an `Instance`+consistent, `Succeeded` backup with a bound | PR1; coordinate with #206 so its planner keeps the rules | L |
| PR3 | Restore holds on source `MysqlBackup`s for bootstrap and in-place restores, generalizing #232 (fixes today's gap where retention can prune a backup a restore is reading), all of the group's kept while a damage record exists | none (the damage clause after PR1) | S-M |
| PR4 | API: `RestoreStagingSpec`, status types, phases, CEL, generated CRDs copied to the chart, CRD reference; reconcile-time rejection unchanged | v1beta1 types merged | S |
| PR5 | Staging: execution snapshot, request identity and `GTIDSet.Canonical()`, probe Job, reuse decision, claim, staging Job and `staging.sh`, proof, manifest, result channel, gtids ConfigMap, Staged certification, artifact lifecycle and cleanup, re-auth and result-read Jobs, `persistentvolumes` RBAC and chart mirror. Behind a hidden operator flag `--experimental-in-place-pitr-staging` (off by default); with it on, a run stops at `Staged` and ends `Failed`/`RestoreInPlaceStagingOnly`. No API dry-run mode: rehearsal is what `MysqlBackupVerification` with `pointInTime` is for | #206, #203, PR3, PR4 | L |
| PR6 | Hand-off: Preflight, Fencing and pre-write-ahead rechecks (version, subset, keyring), loader budget and `loaderActiveDeadlineSeconds`, loader from the claim with `_verify_staging_artifact`, artifact reuse after a damaged failure, removal of the flag and the rejection, docs procedure, doctor playbooks, scenarios 63 and 64 in release | PR1, PR2, PR5 | L |
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

- **Q10:** "possibly damaged" is decided by the damage invariant (record written with the loader's `jobCreating`, cleared only by proofs (a)-(d)), not by #233's GTID comparison, which cannot see an unlogged load. #233 settlement still decides the peers. Refinements: "Discarded releases as today" becomes "Discarded releases only when no record remains"; exit 1 is open to any full-instance restore and per-schema restores are refused while damaged; exit 2's proof is label-based and adds an accept marker; exit 3 is exit 1 with a held-site source. Proof (c), a loader that never started, is added so a failed download is not damage.
- **Q3:** the barrier is joined by a completion marker and an accept marker (closing markers, which certify a new baseline) and a void marker (returns coverage for runs proven undamaged), numbered from a never-decreasing group sequence. The automatic post-restore backup is forced to instance scope. These refine how the barrier is enforced; none removes it.
- **Q7:** the "immediately before the DROP" check is made by the operator immediately before the loader write-ahead, so its refusal leaves the confirm unconsumed as Q7 requires without creating a second destructive Job; the loader's own in-Job check is an extra guard and, because a Job ran, consumes the confirm.
- **Q9:** "deleted only after the replacement is verified" means after the replacement passes the Fencing rechecks, and not at all while a damage record exists.

Decisions taken in this proposal that the debate did not settle (for review):

1. `staged set ⊄ live gtid_executed`: **refuse** (`StagedGtidsNotInLive`), not `gtid_purged='+diff'`; therefore foreign-source in-place PITR is always refused, and `D ∩ B` can only be fixed with another backup or a new group.
2. Markers are **in-band tagged GTIDs** under explicitly assigned, write-ahead-recorded numbers from a never-decreasing group sequence (`restoreMarkerSeq`); they need `RELOAD` and `TRANSACTION_GTID_TAG` for all full-instance restores, and #233 settlement ignores them.
3. For **all** full-instance in-place restores, not only PITR ones: `skipBinlog:true` forced, `excludeSchemas` refused, and the source must be a `mysqlBackupRef` whose backup recorded `dumpScope=Instance`, `consistent=true`, a parseable bound and a location (raw S3/PVC sources become per-schema-only). Reading the scope from the source `@.json` was rejected: which options it records on 9.7 is unverified.
4. New deployment leases stay refused during Staging/Staged (existing behaviour), while `BeginPlanned` stops treating those phases as a concurrent operation and MysqlDatabase keeps reconciling.
5. One damage invariant with four clearing proofs overrides every phase rule; it protects every restore hold of the group and every `Verified` artifact, and execution-stopped proof is label-based (no Job list in status). A lost machine counts as stopped only after confirmed shutdown or external fencing.
6. Phases `Staging` and `Staged`; a probe Job for image selection; reuse decided after the probe and re-authenticated by a separate Job before Fencing; in-place PITR needs a `mysqlBackupRef` source.
7. Staging-side Job loss (including eviction and an unreadable result) is re-created automatically after a pod-gone proof, at most 3 lost Jobs per run (no damage possible); staging Job failure is terminal.
8. The PITR loader gets its own optional deadline, unset by default, plus an in-script pre-destruction deadline bounded by it; Preflight refuses when the deadline cannot cover the pre-destruction time plus 1.5x the load estimate (a heuristic from measured source bytes).
9. The complete accepted request lives in an immutable snapshot ConfigMap read on every step; a request-identity change interrupts any run before its loader write-ahead; a confirm-only bump rebinds, as today.
10. Every refusal after the acceptance pass consumes the confirm, except the pre-Job version refusals (Q7), which a `versionGate` keeps from looping.
11. Staging loads the dump's accounts (then locks them) so account statements in the window replay; accounts on the live primary are not rewound.
12. PR5 lands behind a hidden operator flag rather than an API dry-run mode.
13. Staging data on the claim under `<runID>/`, created by the Job, so root-owned local-path mounts work if they are writable by uid 27.
14. The live `event_scheduler` is switched off at Fencing with `offline_mode` and restored at the fence release; the staging mysqld gets the target's `mysqlConf` minus a pinned denylist.
15. Full GTID sets stay out of status: the artifact's `B` and `S` live in its manifest and an immutable ConfigMap, each at most 256 KiB, under a stated status budget.

## Open questions

1. **Q1 sign-off.** A versus C (see Decisions) is pending Colin's review.
2. **Tagged explicit GTIDs on `mysql:9.7`.** `SET GTID_NEXT='<uuid>:<tag>:<n>'` with an empty transaction and the privilege set above are expected from MySQL 8.3 but are not exercised anywhere in the repo. PR2 verifies first; if they fail, the marker design comes back for review (no fallback is designed).
3. **mysqlsh behaviour.** `loadUsers` with `excludeUsers` into staging, and `sys.exit(70)` propagating through `mysqlsh --py -f`: both checked by the native tests before PR5 and PR1 merge respectively.
4. **local-path ownership in the playground.** The design assumes the k3d local-path provisioner creates claim directories writable by uid 27 (its default setup script uses mode 0777) so `staging-prepare` can create `<runID>/`. Unverified here; PR5 checks it on k3d. If it fails, the playground gets a StorageClass with a setup script that does so; the operator does not gain a root init container.
5. **mysqlsh in `mysql:9.7`.** Unknown; handled by the probe. Recording the measured answer in the docs is a PR5 task.
6. **#206 interface.** The exact plan sets `P` and `Q` and an archive identity readable without a plan are requirements on #206 (P1), not yet agreed there.

## Acceptance criteria

1. With `pointInTime` and `staging`, a full-instance in-place restore reaches `Succeeded` with exactly the staged dataset, which the proof ties to the pre-stop history; no event or session writes between the fence and its release (scenario 63 in release).
2. Every refusal decidable from the backup, archive and staged result happens before the fence; the rechecks after the fence are bounded and release it (unless a damage record exists); every reason is in status and an Event.
3. From the loader write-ahead, the primary stays fenced and frozen with `RestoreInPlaceDamagedPrimaryFenced=True` until proof (a), (b), (c) or (d), in every phase of every run and whatever `gtid_executed` shows; a later run's failure or refusal never clears an inherited record; per-schema restores are refused while it exists; a new confirm reuses the artifact (scenario 64 in release).
4. Nothing destructive runs without a new confirm or an unconsumed one whose run never created a loader Job; a loader Job is never rebuilt after its write-ahead.
5. No live `RESET`; every full-instance restore writes a barrier with a never-reused number before its loader and a completion marker before its fence release; an accept writes an accept marker; settlement ignores markers; the planner enforces the base and window rules; `PITRArchiveReady` reports the gap until an `Instance`+consistent backup carrying a closing marker succeeds.
6. `allowPartial`, `allowUnverifiedCrossSite`, per-schema PITR, `excludeSchemas`, `skipBinlog:false`, `loadIndexes:false`, non-`mysqlBackupRef` full-instance sources, and backups that are filtered, inconsistent, unbounded or not `Succeeded` are refused without consuming the confirm.
7. The loader is built only from the accepted snapshot and an artifact whose certification matches it; the status additions stay within the stated budget (unit test).
8. The staging pod has no token, Service or host network and binds loopback with mysqlx off (unit test).
9. Artifacts appear in status, a gauge and an alert, and are deleted only by success, policy, replacement after the replacement passed Fencing, or the annotation; no `Verified` artifact is deleted automatically while a damage record exists.
10. CRDs regenerated and copied to the chart; ClusterRole mirrors `persistentvolumes get`; log-schema, doctor files and the docs listed above updated in the PRs that change them.
11. Merge gated on #206 and #203, and on PR1 and PR2 (#233 is merged).
