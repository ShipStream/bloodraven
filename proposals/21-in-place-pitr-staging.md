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

What users get: a rewind of a whole instance to a proven point in time, in the same group. Staging runs before the fence, so the outage is the fence, a few bounded checks, the artifact re-hash, DROP and load. Peers keep their pre-restore data if the load fails. Once a loader Job may exist, any failure leaves the primary fenced instead of serving a half-loaded dataset, until a successful repair or a human accept.

## Goals

1. Full-instance in-place PITR (`restoreInPlace.pointInTime` with empty `loadOptions.includeSchemas` and `excludeSchemas`, from a `mysqlBackupRef` whose dump recorded instance scope and consistency) through a staging `mysqld` in an operator-built Job.
2. Every PITR refusal that can be decided from the backup, the archive and the staged result (source scope, coverage, cross-site order, GTID proof, versions, keyring, volume, subset of live history, loader budget) lands before the fence. The checks repeated after the fence, against the bound target, are bounded (seconds to minutes, plus the archive wait) and release the fence when they refuse, unless a damage record exists.
3. The outage is today's full-instance outage plus bounded additions: the Fencing rechecks, the event-worker drain (at most 60 seconds), the barrier archive wait (at most 10 minutes), the #233 peer stop (at most 2 minutes) and the loader's artifact re-hash. Staging, replay and the intermediate dump run while the group serves traffic and emergency failover stays available.
4. One damage invariant (Lifecycle, "The damage invariant"): from the loader's write-ahead until a repair succeeds or an administrator accepts, the primary stays fenced and every damage guard stays on, in every phase of every run. A verified artifact lets a new confirm repair without re-staging.
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
	// Delete: after any terminal outcome, unless a non-terminal run
	// references it or status.restoreDamage exists.
	RestoreStagingDelete RestoreStagingCleanupPolicy = "Delete"
	// RetainOnFailure: delete after success, keep a Verified artifact
	// otherwise for reuse. Incomplete artifacts are always deleted.
	RestoreStagingRetainOnFailure RestoreStagingCleanupPolicy = "RetainOnFailure"
)

type RestoreStagingSpec struct {
	// Image runs mysqld and mysqlsh in the staging Job and mysqlsh in the
	// destructive load Job. Default: the live target pod's image when its
	// runtime reports a pullable digest reference and a probe finds mysqlsh
	// in it, else spec.backup.image. The probe Job runs the candidate (no
	// volumes, no token); every later staging-side and loader pod runs the
	// pinned <repo>@sha256:<digest> reference resolved by the probe, which
	// must report exactly the live target's VERSION().
	// +kubebuilder:validation:MinLength=1
	// +optional
	Image string `json:"image,omitempty"`

	// Volume is the operator-provisioned PersistentVolumeClaim, one per
	// run, that holds the decrypted inputs, the staging datadir and the
	// intermediate dump.
	Volume RestoreStagingVolumeSpec `json:"volume"`

	// Resources are forwarded verbatim to the staging container.
	// innodb_buffer_pool_size is half the memory limit, else half the
	// memory request, else the mysqld default (128M) with a Warning. It is
	// set on the command line and replaces any buffer-pool setting in the
	// target's effective mysqlConf (Staging step b).
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

- **`staging` is required exactly when `pointInTime` is set.** The pairing is structural (two fields that only make sense together), so it is CEL. A missing `volume.size` cannot be defaulted safely (Q6), so the block must exist; and a `staging` block on a non-PITR restore would be silently ignored, which is worse than an admission error. CRD validation ratcheting (beta and on by default since Kubernetes 1.30) keeps an unchanged stored `restoreInPlace` updatable there. On 1.27-1.29, which the operator still supports (`site/content/docs/3.get-started/2.getting-started.md:13`), it does not: a stored object with `pointInTime` and no `staging` (today always rejected, so nothing runs) refuses every later update until `spec.restoreInPlace` is removed or completed. The upgrade-policy page tells users on those versions to do that before upgrading (D3: zero production users, so no migration).
- **Everything else is reconcile-time** (`RestoreInPlaceRejected`, confirm not consumed), checked in the acceptance pass. For **every full-instance** restore (PITR or not; the DROP removes every user schema, so anything the source lacks is destroyed while the run reports success): a non-empty `loadOptions.excludeSchemas`; explicit `loadOptions.skipBinlog: false`; a `source` other than `mysqlBackupRef`; a referenced backup that is not `Succeeded`, is terminating, has no `status.location`, has `status.dumpScope` other than `Instance`, has `status.consistent` other than `true`, has no `status.serverVersion` or `status.mysqlshVersion`, or has a `status.gtidExecuted` that is empty or does not parse (a new `validateFullInstanceBound` that parses the set with `mysql.ParseGTIDSet` and names the backup in a non-PITR message; the existing `validatePITRBackupBound`, `internal/controller/restore.go:1585-1594`, only checks non-emptiness and only for PITR specs without `allowPartial`; a backup taken before PR2 has no scope and is refused, D3). For **per-schema** restores: any `status.restoreDamage` (the record names the exits). For **PITR** runs, additionally: `allowPartial: true`; `allowUnverifiedCrossSite: true`; a non-empty `includeSchemas`; `loadIndexes: false` (the artifact's DDL comes from the staged server, so missing secondary indexes would be carried into the live primary); `validatePITRBackupBefore` against the backup's `completionTime` (`internal/controller/restore.go:1557-1573`); `excludeGtids` that does not parse, is longer than 96 KiB in canonical form (the shared replay passes it as one `mysqlbinlog` argument, and Linux caps one argument or environment string at 128 KiB), intersects the backup's `status.gtidExecuted` (an exclusion cannot remove a transaction the dump already contains) or names any `bloodraven_restore*` GTID; the backup's `status.gtidExecuted` not a subset of the active site's `gtid_executed` (reported as `RestoreInPlaceRejected` with detail `StagedGtidsNotInLive`, so not consumed like every acceptance refusal; the later checks report `StagedGtidsNotInLive` itself). A size below a floor is a Warning only (see Lifecycle).
- **Source scope is recorded, never inferred.** `MysqlBackup` gains `status.dumpScope` (`Instance` when the options the backup script passed to `util.dump_instance` had no `includeSchemas`/`excludeSchemas`, else `Filtered`) and `status.consistent` (the effective `consistent` option, mysqlsh default `true`). The script reports both on its `BLOODRAVEN_DUMP_COMPLETE` sentinel from the options it actually used, so a later profile edit cannot change what an old backup claims. The same sentinel carries two measured versions, recorded as `status.serverVersion` (the source's `SELECT VERSION()`, read on the dump connection) and `status.mysqlshVersion` (`mysqlsh --version` of the Job's binary); today the status has only the configured `mysqlImage` (`api/v1alpha1/mysqlbackup_types.go:142-151`) and the sentinel only GTIDs and coordinates (`internal/controller/backup_script.py:189-220`). Encrypted backups need a second path: there the script runs as the dump init container (`internal/controller/backup_job.go:360-368`), the reconciler reads the sentinel only from the main `bloodraven encrypt-upload` container (`internal/controller/backup_reconciler.go:1157-1178`), and that container rebuilds it from `BLOODRAVEN_DUMP_META.json`, which carries three fields (`backup_script.py:329-341`, `dumpMeta` at `cmd/bloodraven/encrypt_upload.go:711-719`, emitted at `:168-172`). PR2 adds the four fields to that JSON, to `dumpMeta` and to `backupmeta.DumpComplete` (`internal/backupmeta/completion.go:67`), so both flows report them. They are what acceptance snapshots and the probe compares against, before anything is downloaded; staging step a checks them against the downloaded `@.json`. `MysqlBackup` also gains optional `spec.dumpScope` (enum `Instance`): the Job drops the profile's schema filters and forces `consistent: true`. The automatic post-restore backup always sets it, and barrier closure requires `status.dumpScope=Instance` and `consistent=true`.
- **Which load options reach which Job.** The only user field honoured is `loadOptions.threads` (and `resetProgress`, a no-op today). Staging load: `{threads, loadIndexes:true, updateGtidSet:"replace", skipBinlog:true, loadUsers:true, excludeUsers:[root and system accounts]}` (accounts are needed for the replay, see Staging step d). Live loader: `{threads, loadIndexes:true, skipBinlog:true, loadUsers:false}` and no `updateGtidSet`. Accounts, grants and the rest of the `mysql` schema are therefore **not rewound**: the restored primary keeps its current accounts.
- **Access mode is fixed `ReadWriteOnce` and not exposed** (Q6). One pod writes the volume, then one pod reads it, strictly in sequence, which the controller enforces. No use case needs RWX, and RWX classes (NFS) are a poor fit for a mysqld datadir. A future `accessModes` field would be additive.
- **`size` is a plain required `resource.Quantity`**, with no CEL floor. The CEL quantity library is version-dependent, and the useful floor depends on the source size, known at reconcile time only when the backup recorded `status.sizeBytes` (0, unknown, for S3 outputs, `internal/controller/backup_script.py:223-231`) and otherwise only inside the Job (Staging step 4).
- **`storageClassName` is a pointer** because absence (cluster default) and empty string differ.
- **Credentials: no spec field.** The staging `mysqld` is created with `--initialize-insecure` and reached only as `root@127.0.0.1` from inside the pod, exactly as verification does (`internal/controller/verify_script.sh:220-247`). It never talks to the live group. The dump's accounts are loaded into staging only so that account statements in the replay window apply; each loaded account is then `ACCOUNT LOCK`ed before the replay, and none can connect anyway because the server listens on loopback inside a pod that runs only operator-built containers. The intermediate dump is taken with `users: false` and the live loader uses `loadUsers: false`. A configurable account would add a Secret to leak and protect nothing loopback does not.
- **Network isolation: no spec field** (Q8). The staging server binds `127.0.0.1` with `--mysqlx=OFF`, the pod has no Service, `automountServiceAccountToken: false`, `hostNetwork: false`, and runs only operator-built containers. No NetworkPolicy: it would add RBAC and a chart mirror, depends on the CNI enforcing it, and cannot isolate containers sharing the pod network namespace, which is the only place loopback is reachable from. The flags are pinned by a unit test.
- **No new pull-secret field.** Staging and probe Jobs use `spec.backup.imagePullSecrets` (`api/v1alpha1/backup_types.go:35-36`). If the live image cannot be pulled with them, the probe Job is lost (Staging step 2) and three losses end the run `StagingFailed`; setting `staging.image` avoids it.
- **`RetainOnFailure` is the default** because a retained, verified artifact is the fastest repair source after a post-DROP failure. A boolean was rejected (third state), and a TTL was rejected (it deletes the repair source at 3am).

### Status

New `RestoreInPlacePhase` values (enum at `api/v1alpha1/backup_types.go:837-838` becomes `"";Staging;Staged;Preflight;Fencing;Restoring;Resuming;Succeeded;Failed`):

| Phase | Meaning |
|---|---|
| `Staging` | The run is accepted with `pointInTime`. The operator holds the source, probes the image, decides `Build` or `Reuse`, and runs the staging Job (`Build`, on a new claim) or the re-authentication Job (`Reuse`). No deployment hold, no topology freeze, no queue, no fence. |
| `Staged` | The operator verified the Job's result (termination sentinel, logged sets against their digests, versions, verdict) and certified the artifact (Build) or matched it to its stored certification (Reuse). One-step phase; the run moves to `Preflight` on the next reconcile. |

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
	// PreflightSince is the Preflight entry write, which turns the
	// MysqlDatabase pause on; Preflight exits no earlier than 65 s later.
	PreflightSince *metav1.Time `json:"preflightSince,omitempty"`
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
	ImageCandidate  string             `json:"imageCandidate,omitempty"`  // as configured or read from the live pod; the probe pod runs it
	ImageRef        string             `json:"imageRef,omitempty"`        // pinned <repo>@sha256:<digest>; every pod after the probe runs it
	ImageRuntimeID  string             `json:"imageRuntimeID,omitempty"`  // probe pod's imageID; identity only, never executed
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
	ImageCandidate    string `json:"imageCandidate"`    // override, live digest reference or backup image
	ImageRef          string `json:"imageRef"`          // pinned reference; empty if the probe never ran
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
	// this group. Written in the same update that records a new marker,
	// so an allocated number is never reused. The transition rule makes
	// the API server reject a write that lowers it (a rule does not run
	// when the field is dropped; "Status writers" covers that case).
	// +kubebuilder:validation:XValidation:rule="self >= oldSelf",message="restoreMarkerSeq never decreases"
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

// RestoreStagingArtifact is one claim and the artifact on it. The fields
// from RequestSHA256 through HashBytesPerSecond are written once, at
// certification, and are exactly the certification input (see "Artifact
// certification"); nothing outside that range is hashed. Strings carry
// MaxLength markers (digests 64, names 253, imageRef 512, versions 64,
// archiveIdentity 256) so the status budget is enforced by the schema.
type RestoreStagingArtifact struct {
	OriginRunID    string `json:"originRunID"`            // run that built it; never reused
	PVCName        string `json:"pvcName"`                // written before Create (write-ahead)
	PVCUID         string `json:"pvcUID,omitempty"`
	Path           string `json:"path,omitempty"`         // "<originRunID>/artifact": manifest.json + dump/
	LocalNode      string `json:"localNode,omitempty"`    // bound PV pinned to one node
	CleanupPolicy  RestoreStagingCleanupPolicy `json:"cleanupPolicy,omitempty"` // policy of the origin run
	State          RestoreStagingArtifactState `json:"state"`
	GtidSets       *RestoreSnapshotRef `json:"gtidSets,omitempty"` // ConfigMap copy of B and S; rebuilt if lost
	CertificationSHA256 string `json:"certificationSHA256,omitempty"` // over the range below, never itself
	RequestSHA256  string `json:"requestSHA256,omitempty"`  // origin run's request identity
	SourceDumpSHA256 string `json:"sourceDumpSHA256,omitempty"` // source @.json
	ArchiveIdentity string `json:"archiveIdentity,omitempty"` // #206
	DumpEncryptionRevision    string `json:"dumpEncryptionRevision,omitempty"`    // #203 revision of the backup prefix, or "plaintext"
	ArchiveEncryptionRevision string `json:"archiveEncryptionRevision,omitempty"` // #203 revision of the archive prefix, or "plaintext"
	ImageRef       string `json:"imageRef,omitempty"`       // pinned <repo>@sha256:<digest> that built it
	ServerVersion  string `json:"serverVersion,omitempty"`  // staging SELECT VERSION()
	DumpServerVersion string `json:"dumpServerVersion,omitempty"` // source @.json serverVersion
	MysqlshVersion string `json:"mysqlshVersion,omitempty"`
	FormatVersion  int32  `json:"formatVersion,omitempty"`  // operator's staging/proof/manifest format
	ManifestSHA256 string `json:"manifestSHA256,omitempty"`
	BaselineGtidSHA256 string `json:"baselineGtidSHA256,omitempty"`
	StagedGtidSHA256   string `json:"stagedGtidSHA256,omitempty"`
	EncryptedTables bool  `json:"encryptedTables,omitempty"` // artifact DDL has ENCRYPTION='Y' (scanned at step l)
	SourceBytes    int64  `json:"sourceBytes,omitempty"`    // downloaded source dump, measured by staging
	ArtifactBytes  int64  `json:"artifactBytes,omitempty"`
	StagingLoadSeconds int64 `json:"stagingLoadSeconds,omitempty"`
	HashBytesPerSecond int64 `json:"hashBytesPerSecond,omitempty"` // measured while writing the manifest
	// End of the certification input. LoaderHashBytesPerSecond is the
	// slowest re-hash rate a loader of this artifact reported (sentinel);
	// later budgets use the lower of the two rates.
	LoaderHashBytesPerSecond int64 `json:"loaderHashBytesPerSecond,omitempty"`
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
	// LoaderWriteAhead is set in the status update that sets the run's
	// jobCreating (the damage write-ahead). Set: never voided. It lives
	// here because run status is replaced by later runs.
	LoaderWriteAhead bool     `json:"loaderWriteAhead,omitempty"`
	Completion *RestoreMarker `json:"completion,omitempty"` // <uuid>:bloodraven_restored:<n>
	Void       *RestoreMarker `json:"void,omitempty"`       // <uuid>:bloodraven_restore_void:<n>
	BaselineBackup *RestoreObjectRef `json:"baselineBackup,omitempty"`
	Message    string         `json:"message,omitempty"`    // at most 256 bytes
}

// RestoreDamageStatus: the primary may hold a partial or dropped dataset.
// No list of Jobs: execution-stopped proof is label-based.
type RestoreDamageStatus struct {
	OriginRunID string      `json:"originRunID"` // run whose loader write-ahead created the record
	RunID       string      `json:"runID"`       // run that owns the record now (origin, or a takeover run)
	Site        string      `json:"site"`
	Since       metav1.Time `json:"since"`
	// AcceptPeersHeldAt: exit 2 armed a Held queue after a Discarded
	// settlement and is waiting for every peer receiver to stop. Cleared
	// in the write that refuses the accept.
	AcceptPeersHeldAt *metav1.Time `json:"acceptPeersHeldAt,omitempty"`
	// Accept is the accept marker <uuid>:bloodraven_restore_accepted:<m>,
	// recorded before it is committed (exit 2, from PR2). Once recorded it
	// is resumed, never reallocated.
	Accept      *RestoreMarker `json:"accept,omitempty"`
	Reason      string      `json:"reason,omitempty"`
}

// RestoreOfflineFenceStatus (existing, api/v1alpha1/backup_types.go:1010) gains
// the scheduler beside priorOfflineMode, under the same rules (Fencing step 2).
type RestoreOfflineFenceStatus struct {
	// ... existing fields: site, confirmToken, runID, priorOfflineMode, sessionsEvicted ...
	PriorEventScheduler *string `json:"priorEventScheduler,omitempty"` // ON, OFF or DISABLED; unset until read
	EventWorkersDrained bool    `json:"eventWorkersDrained,omitempty"` // no event worker thread remains
}
```

**Status size budget.** No full GTID set is added to any status. `status.restoreInPlace` keeps the two it has (`sourceGtidExecuted`, `targetGtidExecuted`, `api/v1alpha1/backup_types.go:1099-1109`), and the group status keeps its existing ones (`fencedGtidExecuted`, `:941`; `promotionGtidExecuted`, `api/v1alpha1/types.go:586`). The full accepted request, `excludeGtids` included, lives in the run's snapshot ConfigMap; the baseline `B` and staged `S` sets live in the artifact's `manifest.json` on the claim (authenticated by `manifestSHA256`) and in one immutable operator-owned ConfigMap per artifact (`<pvcName>-gtids`, owner reference to the group, deleted with the artifact), which the operator reads uncached and checks against the two digests before each use. Each set may be at most 256 KiB (the staging Job fails `StagingFailed`/`GtidSetTooLarge` above that); neither travels through a Job's argv or environment (Staging step f). Artifact and run messages are truncated to 1 KiB, barrier messages to 256 bytes. With the MaxLength bounds above, the additions to `MysqlFailoverGroup` status are at most about 75 KiB: 8 artifact entries of at most 3 KiB, 32 barrier entries of at most 1.2 KiB (three markers of about 150 bytes, ids, a backup reference, the message), a damage record under 1.5 KiB, and run status under 6 KiB (three image strings, the version gate, the message). A unit test serializes the object with every list full and every string at its bound, and asserts the total.

**Status writers.** Every new group-status field (`restoreDamage`, `restoreMarkerSeq`, `restoreBarriers`, `restoreStagingArtifacts`) and every new `status.restoreInPlace` field is reconciler-owned and written only through a fresh-read mutate under `RetryOnConflict` (the `mutateRestoreOfflineFence` pattern, `internal/controller/restore_offline_fence.go:328`). The topology runner's status write replays a snapshot taken before it polled (`updateCRStatus`, `internal/controller/runner.go:1409`) and re-reads only an allowlist from the fresh object (`:1751-1807`: `plannedFailover`, `restore`, `restoreInPlace`, `restoreReclones`, `restoreOfflineFence`, encryption, Dragonfly, `pitr`, and the conditions in `reconcilerOwnedConditions`, `:2478`). The new `status.restoreInPlace` and `restoreOfflineFence` fields ride on the first two of those. The four new top-level fields join the allowlist, and `RestoreInPlaceDamagedPrimaryFenced` joins `reconcilerOwnedConditions`. Otherwise a stale runner write erases `restoreDamage` (unfencing a damaged primary on the next sync), lowers `restoreMarkerSeq`, or drops a barrier or an artifact entry and leaks its claim. Each PR that adds one of these fields adds it to the allowlist and adds a stale-write interleaving test: the runner snapshots, the reconciler writes the field, the runner writes, and the field survives (PR1 `restoreDamage` and the condition, PR2 `restoreMarkerSeq` and `restoreBarriers`, PR5 `restoreStagingArtifacts`). The `Ready` override below is computed inside the same retry, from the fresh object's `restoreDamage`.

### Conditions

| Type | Status/Reason | When |
|---|---|---|
| `RestoreInPlaceDamagedPrimaryFenced` (new) | `True` / `RestoreInProgress` | `status.restoreDamage` exists and its owning run is in `Restoring` or `Resuming` (a healthy load looks like this). |
| | `True` / `RestoreFailedAfterJob` | `status.restoreDamage` exists in any other phase of any run. Message names the origin and owning runs, the exits and the Verified artifacts. `False` / `Repaired` or `Accepted` after a clearing proof. |
| `PITRArchiveReady` (existing, `internal/controller/pitr_archive_condition.go:29`) | `False` / `PITRRestoreBarrier` (new reason) | An entry in `status.restoreBarriers` is open. Message names the barrier and completion GTIDs and the post-restore backup. Full precedence: `ArchiveStoreInitFailed`, `ArchiverError`, `ArchiverDisabled`, `SidecarUnreachable` (the poller's False reasons, `:84-89`), then `PITRRestoreBarrier`, then `AwaitingRollout`/`AwaitingFirstPoll`, then `True`/`Archiving`. Both runner writers derive it in `applyPITRArchiveStatus` (`:535-560`) from the `restoreBarriers` of the object they fetched, so neither replays a stale barrier reason. |
| `Ready` | `False` / `RestoreDamagedPrimary` while `status.restoreDamage` exists | New explicit override in the runner's `Ready` derivation (`internal/controller/runner.go:1468-1512` derives it from writable-primary and replication health only), so it holds also after a `Discarded` settlement with every peer replicating. |

### Reasons and Events

Stable reason strings, used in status messages and Events. "Consumed" follows one rule: a refusal in the acceptance pass never consumes the confirm; any later refusal does, except the version refusals made before a loader Job exists (Q7), which are gated so that the unconsumed confirm cannot restart an identical run (`versionGate`). Without the rule, an unconsumed refusal restarts the run on the next reconcile (`internal/controller/restore_inplace.go:333-351`), which for staging means hours of work repeated in a loop.

| Reason | Where | Confirm consumed |
|---|---|---|
| `RestoreInPlaceRejected` (existing) | acceptance-pass refusals listed above | no |
| `StagingVersionMismatch` | probe, Staged check, Preflight, Fencing, and the operator's check immediately before the loader write-ahead | no; not restarted while `versionGate` matches |
| `StagingArtifactSlotsFull` | Build decision (Staging step 3) with 8 artifact entries; the message names `kubectl bloodraven restore-staging cleanup` | yes |
| `StagingTargetNotCovered` | staging verdict `targetCovered=false` | yes |
| `StagingGtidProofFailed` | staged set differs from the exact plan, or `excludeGtids` intersects the dump baseline `B` | yes |
| `StagedGtidsNotInLive` | `S − L` non-empty at Preflight, Fencing or before the write-ahead; the message splits `D ∩ B` from `D − B` (see Preflight). The acceptance-pass form is `RestoreInPlaceRejected` | yes |
| `StagingKeyringUnavailable` | encrypted tables in the dump and the staging keyring unavailable; or `encryptedTables` and the live keyring component not active (Preflight, Fencing, before the write-ahead) | yes |
| `StagingVolumeNotWritable` | in-Job ownership/writability check | yes |
| `StagingVolumeUnavailable` | provisioning timeout, claim lost or replaced (UID), node gone or unschedulable before the loader write-ahead | yes |
| `StagingArtifactMismatch` | manifest, file or GTID-set digest mismatch found on the claim by a re-auth or result-read Job; the artifact becomes `Invalid`. A missing or mismatched `<pvcName>-gtids` ConfigMap is metadata loss, not this reason: it is rebuilt (Staged) | yes |
| `RestoreInPlaceSnapshotLost` | the run's snapshot ConfigMap is missing or fails its digest before the loader write-ahead | yes |
| `StagingLoaderDeadlineTooShort` | `loaderActiveDeadlineSeconds` set and below the loader budget (Preflight) | yes; the Verified artifact stays reusable |
| `StagingFailed` | staging Job failed for any other reason (detail such as `GtidSetTooLarge`), or 3 staging-side Jobs were lost | yes |
| `RestoreInPlaceStagingOnly` | PR5 flag on: the run stops after `Staged` | yes |
| `RestoreInPlaceSuperseded` | the spec's request identity differs from the accepted one before the loader write-ahead (with or without a confirm bump) | no; the next pass starts a run from the current spec |
| `RestoreBarrierNotArchived` | barrier binlog not archived within 10 min at Fencing | yes (no Job) |
| `RestoreBarrierSlotsFull` | Fencing step 4 with 32 barrier entries and none evictable; refused before allocating | yes (no Job) |
| `RestoreInPlaceDuplicateLoader` | the loader Job is `Complete` but its counters are not `succeeded=1, failed=0` (a second pod ran); not Repaired, the record stays | yes (a Job ran) |
| `RestoreEventWorkersRemain` | an event worker thread survived the scheduler stop and its kill for 60 s (Fencing step 2, every full-instance run); refused before the barrier | yes (no Job) |
| `StagingImageNotPinned` | neither the candidate nor the probe pod's `imageID` yields a pullable `<repo>@sha256:<digest>` reference (Staging step 2), and no fallback remains | yes |
| `RestoreInPlacePreDestructionRefused` | the loader exited through its pre-destruction refusal path. This is the run's status reason for every such exit; the sentinel's reason (`ArtifactMismatch`, `VersionMismatch`, `KeyringUnavailable`, `OfflineModeOff`, `EventWorkersRunning`, `LocalInfile`, `ExecutionClaimHeld`, `PreDestructionDeadline`) is detail in the message and the log's `detail` field. Diagnosis only: the damage record stays | yes (a Job ran) |
| `PITRRestoreBarrier` | planner refusal and `PITRArchiveReady` reason | n/a |
| `RestoreDamagedPrimary` | while a damage record exists: deployment lease and `BeginPlanned` refusals (deployment API), `Ready` reason, backup `Pending`/`Skipped` reason | n/a |

New Events: `RestoreInPlaceStagingStarted`, `RestoreInPlaceStagingJobCreated`, `RestoreInPlaceStagingVolumeNodeLocal` (Warning), `RestoreInPlaceStagingVolumeSmall` (Warning), `RestoreInPlaceStagingResourcesUnset` (Warning), `RestoreInPlaceStaged`, `RestoreInPlaceStagingReused`, `RestoreInPlaceStagingFailed` (Warning), `StagingVersionMismatch` (Warning), `RestoreInPlaceSpecEditDeferred`, `RestoreInPlaceWritesDiscarded` (Normal, at Fencing: requested stop time and actual fence time), `RestoreInPlaceLoaderEstimate` (Normal, Preflight: the loader budget), `RestoreInPlaceWaitingForBackups` (Normal, Preflight), `RestoreBarrierWritten`, `RestoreBarrierEntryDropped` (Warning, a closed-out entry evicted at `MaxItems`), `RestoreBarrierArchived`, `RestoreCompletionWritten`, `RestoreBarrierVoided`, `RestoreBarrierClosed`, `RestoreAcceptMarkerWritten`, `PostRestoreBackupCreated`, `PostRestoreBackupFailed` (Warning), `RestoreInPlaceDamagedPrimaryFenced` (Warning), `RestoreInPlaceDamagedPrimaryReleased`, `RestoreAcceptPeersNotStopped` (Warning), `RestoreInPlaceDamageTakenOver`, `ScheduledBackupSkippedDamagedPrimary` (Warning), `RestoreStagingRetained`, `RestoreStagingDeleted`, `RestoreStagingVolumeGone` (Warning), `RestoreStagingCleanupRejected` (Warning), `RestoreInPlaceUnknownPhase` (Warning).

### Annotations (one-shot, on the `MysqlFailoverGroup`, consumed and cleared like `RecloneAnnotation`)

| Annotation | Value | Effect |
|---|---|---|
| `bloodraven.shipstream.io/restore-staging-cleanup` | artifact `originRunID` | Deletes that artifact's claim (UID-conditional delete, state `Deleting`). Refused (Event, annotation cleared) if no such artifact exists or a non-terminal run references it. Allowed while `status.restoreDamage` exists, with a Warning that it removes a repair source. RunIDs are UUIDs and never reused, so a stale value cannot hit a later artifact. |
| `bloodraven.shipstream.io/restore-accept-damaged` | `status.restoreDamage.originRunID` | Exit 2 (see "The damage invariant"), a resumable sub-state machine keyed on `restoreDamage`. **Admission:** refused (Event, annotation cleared) unless the value matches, no in-place run is non-terminal (a takeover run in `Staging`/`Staged` is cancelled first by removing `spec.restoreInPlace`, so an accepted dataset is never rewound later under an already-bound confirm), and the execution-stopped proof holds. The annotation is cleared once admission is recorded in status; from then on the status fields drive the steps, and while `acceptPeersHeldAt` or `accept` is set, acceptance of a new run waits (confirm not consumed). **Peers first:** if settlement discarded the queue (the peers resumed, which an unlogged load allows), one write arms a `Held` queue of every peer and sets `acceptPeersHeldAt`, and each peer's receiver is stopped and verified with #233's `StopRestorePeerReplication` without catch-up (`internal/controller/restore_peer_replication.go:302`, the call Fencing uses); a peer not verified stopped within `restorePeerStopTimeout` (2 minutes, `:49`) of `acceptPeersHeldAt` refuses the accept: one write clears `acceptPeersHeldAt` and the annotation (Event `RestoreAcceptPeersNotStopped`), the queue stays `Held`, and a new annotation starts a fresh wait. Otherwise a peer could replicate post-accept writes (and, from PR2, the accept marker) over data that is not the accepted dataset. **Then:** (PR2) record `restoreDamage.accept` with a fresh number from `restoreMarkerSeq`, or resume the one already recorded, and commit it on the primary, idempotent like the barrier; clear the record, release the fence, and create the automatic post-restore backup. An operator restart at any step resumes from these fields. An unreachable primary leaves the record and fence in place and retries; nothing times out into a release. The `Held` queue stays for #233's exits, except that resuming replication (removing `status.restoreReclones`) is unsafe after an accept: the peer would receive post-accept writes without the accepted data. Only `Draining` (reclone) brings it back. |

`kubectl bloodraven restore-staging list|cleanup <group> --run <id>` (new plugin subcommand, dispatched beside the existing verbs in `cmd/kubectl-bloodraven/main.go:79-88`) reads the artifact list and sets the cleanup annotation. `kubectl bloodraven restore accept-damaged <group> --run <id>` sets the other. Both require `--run` and write exactly the annotation key and value above; the plugin reference page (`site/content/docs/6.operations/3.kubectl-plugin.md`) documents them.

A hold annotation `hold.restore.shipstream.io/<runID>` = `<group name>/<group UID>` (mirroring the verification hold prefix, `internal/controller/backup_verification_hold.go:29`, which also tells same-name successors apart by UID, `:112-124`) is added to the source `MysqlBackup` in the Staging step 1 write (non-PITR full-instance runs: in Preflight). It is released when the run is terminal and no pod of the run can still read the source, except that **no restore hold whose group UID is this group's is released while `status.restoreDamage` exists**: every earlier run's source is a possible repair input, and the hold itself, not run status, is what names it. A hold whose group UID no longer exists (the group was deleted, or replaced by a same-name successor) is dead and is stripped once no pod of its run runs.

### Metrics

| Metric | Type | Labels |
|---|---|---|
| `bloodraven_restore_staging_duration_seconds` | histogram (buckets to 48h) | `namespace, group, outcome` (`staged`, `failed`, `refused`, `reused`) |
| `bloodraven_restore_staging_retained_artifacts` | gauge | `namespace, group` |
| `bloodraven_restore_staging_retained_bytes` | gauge | `namespace, group` |
| `bloodraven_restore_damaged_primary_fenced` | gauge (1/0) | `namespace, group, site` |
| `bloodraven_pitr_restore_barriers_open` | gauge | `namespace, group` |
| `bloodraven_restore_in_place_loader_started_timestamp_seconds` | gauge (Unix time of the loader write-ahead) | `namespace, group, site` |
| `bloodraven_restore_in_place_loader_estimate_seconds` | gauge (`preDestruction + load` from the Preflight budget) | `namespace, group, site` |

The two loader gauges are set in the damage write-ahead and their series are deleted when the owning run leaves `Restoring`/`Resuming` (and on operator start for a run that is not there), so they exist exactly while a loader may be healthy. `bloodraven_restore_duration_seconds` (`internal/metrics/metrics.go:262-273`) keeps `restore_kind="in_place"` for the destructive Job; it observes only finished restores.

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
      stopDatetime: "2026-11-20T01:42:00Z"   # allowPartial / allowUnverifiedCrossSite refused in place
    staging:                           # image omitted: live digest reference with mysqlsh, else spec.backup.image
      volume:
        size: 600Gi                    # datadir + indexes + binlogs + intermediate dump
        storageClassName: replicated-encrypted   # node-local classes get a Warning
      resources:
        requests: {cpu: "4", memory: 16Gi}
        limits: {memory: 16Gi}         # innodb_buffer_pool_size = 8Gi
      cleanupPolicy: RetainOnFailure
      activeDeadlineSeconds: 43200    # loaderActiveDeadlineSeconds unset: no loader Job deadline
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

This paragraph overrides every table and step below. **A damage record (`status.restoreDamage`) is created in the same status update that sets the loader's `jobCreating`, for every full-instance restore. While it exists, every damage guard is on in every phase of every run, terminal or not: the primary's Service label fence, `offline_mode=ON` and `event_scheduler=OFF`, the topology freeze, the MysqlDatabase pause, refused leases and planned failovers, skipped scheduled backups, every restore hold that names this group's UID, every `Verified` artifact (only the explicit cleanup annotation deletes one), and no fence release on any timeout. It is cleared only by one of two proofs:**

- **Repaired:** a full-instance run that owns the record (it created it, or took it over in its acceptance write) reaches `Succeeded`: its loader Job carries an explicit `JobComplete=True` condition (not the `status.succeeded > 0` fallback of `jobPhase`, `internal/controller/backup_reconciler.go:1087-1104`), its counters read exactly `succeeded=1, failed=0`, the execution-stopped proof holds, and (from PR2) its completion marker is committed after all three;
- **Accepted:** the `restore-accept-damaged` annotation with the execution-stopped proof and no non-terminal run, after the peers are held and (from PR2) the accept marker is committed.

**One Job is not one execution.** A Job with `parallelism=1`, `completions=1`, `backoffLimit=0` and `restartPolicy: Never` (`internal/controller/restore.go:1512-1518`) may still start its program twice (Kubernetes Job documentation, "Handling Pod and container failures"; **unverified in repo**). So the loader takes a per-run execution claim on the target as its last check before the destruction flag: `GET_LOCK('bloodraven_restore_exec_<run8>', 0)` on the mysqlsh session that runs the load, held until the process exits; a concurrent second pod gets 0 and refuses through `_refuse_before_destruction(ExecutionClaimHeld)`. A second pod that starts after the first ended finds the lock free, which is why Repaired also needs the counters: Job tracking with finalizers (GA since Kubernetes 1.26) counts every pod before it is removed, so any second execution shows as `succeeded=2` or `failed>=1`. `JobComplete` with other counters ends the run `RestoreInPlaceDuplicateLoader` with the record kept. `JobComplete` also means the Job controller starts no further pod, and the execution-stopped proof waits out one still running.

Nothing else clears it: not #233 settlement, not a GTID comparison, not a timeout, not an unreachable primary, not the loader's exit code or termination message, not a pod that never started, not a refused Job Create, and not a run's refusal, failure, cancellation or supersession. While it exists, per-schema restores are refused at acceptance and every full-instance run takes the record over in its acceptance write. PR1 ships the record and both proofs without their marker conditions, and PR2 adds the markers and those conditions (Phased plan); each PR is complete on its own. **Execution-stopped proof** (needed by Repaired, by exit 1 at Preflight and by exit 2) is label-based: every in-place loader pod carries `shipstream.io/restore-loader=<group>`, and an uncached list shows no pod with that label that is not `Succeeded`/`Failed` (a pod with a deletion timestamp that is not terminal still counts) and no Job with that label lacking an explicit terminal condition (the `jobFinished` rule, `internal/controller/backup_verification_hold.go:62-90`). It proves only that nothing runs now, never that nothing ran, which is why it gates the human exits and clears nothing alone. A pod whose node is lost counts only after the machine is confirmed shut down or externally fenced from MySQL: deleting the Node or force-deleting the pod removes the API object, not the process, and Kubernetes' non-graceful node shutdown procedure (`node.kubernetes.io/out-of-service` after confirmed shutdown) is the supported way to get there.

**Cost, stated plainly.** A loader that refuses before its first destructive statement (artifact digest, version, keyring, `offline_mode`, `local_infile`, pre-destruction deadline), never starts (failed init container, `ImagePullBackOff`, unschedulable, deleted) or whose Create is refused leaves the primary fenced until a human acts, although it changed nothing: exit 1 reuses the Verified artifact and takes minutes of re-authentication plus the load, and exit 2 accepts the unchanged data. The loader still writes its `BLOODRAVEN_PRE_DESTRUCTION_REFUSED` sentinel and exit code (Lifecycle step 6), for diagnosis and to mark an artifact `Invalid`, never to clear. The operator's own checks before the write-ahead keep this rare: versions, keyring, request identity and snapshot, artifact certification and claim, the loader budget, and a server-side dry-run Create of the exact loader Job (`dryRun=All`), so refusals by Job validation and Job admission land before the record exists. Pod admission is not covered: a ResourceQuota or Pod Security refusal of the loader's pod happens after the Job exists (`internal/controller/backup_reconciler.go:727-732` already reports such Jobs), and the operator has no Pod `create` permission to dry-run one (`config/rbac/role.yaml:39-49`). Such a loader never starts and is handled like any other (below). **A loader that never starts** stays in `Restoring` while it has no Job deadline, where a new confirm is deferred and accept is refused. The documented way out: fix the cause, then delete the loader Job with foreground propagation (`kubectl delete job <name> --cascade=foreground`). The run then ends `RestoreInPlaceJobLost` (never rebuilt, `internal/controller/restore_inplace.go:766-771`) with the record kept, the execution-stopped proof holds, and exits 1 and 2 open. **Follow-up [#240](https://github.com/ShipStream/bloodraven/issues/240): automatic release after a proven pre-destruction refusal.** Three review rounds drew Major findings on proofs that a loader changed nothing (exit-code and never-started evidence over terminal pods that Kubernetes can delete, garbage-collect or mark failed from the control plane). Those proofs need complete, kubelet-sourced evidence for every execution of the Job, and are designed and reviewed separately.

### Request identity and artifact certification

**Accepted execution snapshot.** In the acceptance pass, before the status write, the operator creates an immutable ConfigMap `mysql-<fg>-restore-<run8>` (`immutable: true`, owner reference to the group, label `shipstream.io/restore-run=<runID>`) holding the complete accepted request as canonical JSON: `restoreInPlace` with every default resolved (source with the `MysqlBackup` UID, decryption, `loadOptions` including `threads`, `pointInTime` including `excludeGtids`, and the whole `staging` block: image override, volume, resources, cleanup policy, both deadlines, sanity check), plus the backup's `dumpScope`, `consistent`, `gtidExecuted` digest, `serverVersion` and `mysqlshVersion` at acceptance, plus the resolved storage of the backup's profile and of the archive profile (backend, bucket and prefix, endpoint, region, credentials Secret name or #203 revision, PVC claim name). Today the restore Job builder reads those from the current group spec (`findProfile`, `internal/controller/restore.go:1031-1099`); staging-side Jobs, including re-creations after a lost attempt, are built from the snapshot instead. The status write records `snapshot{name, uid, sha256}`. An orphaned ConfigMap from a crash before that write is deleted by its run label. Every later step, including after an operator restart, reads the snapshot uncached and checks its digest; nothing reads the current spec for execution. The snapshot is deleted when the run is terminal and its loader (if any) is stopped. Canonical means: keys sorted at every level (so `v` has no fixed position), no insignificant whitespace, every key present (empty string or empty array when unset).

**Request identity** is the subset that decides supersession and artifact reuse; `requestSHA256` is the SHA-256 (lowercase hex) of its canonical JSON (shown unsorted for reading):

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
| Identity changed (any of source, backup UID, `decryption.secret` or `decryption.key`, `stopDatetime`, sites, profile, `excludeGtids`, `sanityCheck`, image override, kind), confirm advanced or not | Run ends `RestoreInPlaceSuperseded`, confirm not consumed; fence released if up (unless a damage record exists); the next pass accepts a run from the current spec, which may reuse an artifact whose request matches | Deferred: the Job runs the accepted snapshot; `RestoreInPlaceSpecEditDeferred` Event |
| `spec.restoreInPlace` removed | `RestoreInPlaceCancelled` (existing) | Existing `RestoreInPlaceSpecRemoved` |

The loader is built from the snapshot and the artifact record, never from the current spec.

**Artifact certification** is what an artifact proves. Its input is one object: `{"v":1}` plus the artifact entry's fields from `requestSHA256` through `hashBytesPerSecond` in the struct above, under their JSON names, canonical as above. It excludes `certificationSHA256` itself, the claim fields, `state`, `loaderHashBytesPerSecond`, `verifiedAt`, `reason` and `message`. `certificationSHA256` is the SHA-256 of that JSON, so a later run recomputes it from the entry alone. Each field has one checked source: `requestSHA256`, `imageRef`, both encryption revisions and `formatVersion` (the operator constant for the staging script, proof and manifest format, i.e. the planner/verifier version) come from the run's own status and snapshot, and the Job's result must echo the same values; every other field comes from the digest-checked result (Result channel). These are the Q4 fingerprint inputs, plus the measurements the loader budget uses (`sourceBytes`, `artifactBytes`, `stagingLoadSeconds`, `hashBytesPerSecond`). An artifact is handed to a loader only if it is `Verified`, its `requestSHA256` equals the run's, its stored certification recomputes from its fields, its `imageRef` is the run's pinned staging image, its `serverVersion` equals the bound target's `VERSION()`, and its `<pvcName>-gtids` ConfigMap matches both GTID digests (rebuilt first if it is missing or mismatched, Staged).

### Guards by phase

`inPlaceRestoreInFlight` (`internal/controller/restore_inplace.go:56`) gates more than the freeze. Each consumer gets an explicit rule. The last column is the damage invariant and **overrides the phase columns**: each guard is `restoreDamagePresent || <phase rule>`.

| Guard | `Staging`/`Staged` | `Preflight` | `Fencing`/`Restoring`/`Resuming` | `status.restoreDamage` present (any phase, any run) |
|---|---|---|---|---|
| Topology freeze (`inPlaceRestoreFreezesTopology`, `:70-74`) | no | no (today) | yes | yes |
| Primary Service label fence (`inPlaceRestoreFencesPrimaryService`, `internal/controller/restore_inplace.go:83-104`, consumed by `syncPodLabels`, `internal/controller/reconciler.go:2101-2107`) | no | no | yes (full-instance) | yes |
| `offline_mode` and `event_scheduler=OFF` (`restoreOfflineFenceWanted`, `internal/controller/restore_offline_fence.go:300-312`) | no | no | yes (full-instance) | yes |
| `BeginPlanned` conflict for `PlannedFailover` and `OrderedUpdate` (`internal/controller/deployment_leases.go:732-735`); the restore's own `BeginPlanned("RestoreInPlace")` at Preflight (`restore_inplace.go:489`) stays exempt in every column, so exit 1 is never blocked by its own record | **no** (new predicate excludes these phases) | yes (`ConcurrentOperation`) | yes (`ConcurrentOperation`) | yes, reason `RestoreDamagedPrimary`, checked before `ConcurrentOperation` |
| MysqlDatabase pause (`groupFenced`, `internal/controller/mysqldatabase_reconciler.go:2426-2433`) | **no** | yes, and Preflight quiesces it (below) | yes | **yes** (today it resumes on a damaged primary) |
| New deployment leases (`DeploymentUnstableReason`, `deployment_leases.go:270-296`) | refused, `RestoreInPlace` (decision 4) | refused, `RestoreInPlace` | refused, `RestoreInPlace` | refused, `RestoreDamagedPrimary`, checked first after `DeploymentRevocationPending` so no health or reclone reason hides it |
| Backups whose source would be the target site | run | new Jobs wait `Pending`; Preflight waits for running ones (below) | new Jobs wait `Pending` | scheduled: `Skipped`; explicit without override: `Pending`; with `sourceSiteOverride`: run as requested |

PR1 adds `inPlaceRestoreMutating` (in flight and not `Staging`/`Staged`) and `restoreDamagePresent`, and switches the consumers above to them; a unit test pins every cell, including a takeover run in `Staging` and in `Preflight` with a record present, whose own `BeginPlanned` passes.

**The reconcile pass during Staging.** Changing the predicates is not enough. Today `reconcileInPlaceRestore` returning a requeue ends the pass (`internal/controller/reconciler.go:472-491`); only a planned failover already in flight is driven there. The fence release, the reclone queue and `reconcilePlannedFailover`, which takes fresh planned-failover annotations (`internal/controller/planned_failover_reconciler.go:92-119`), run after it (`reconciler.go:501-531`). PR1 changes this: in `Staging`/`Staged` the in-place requeue goes into `deferredRequeue` and the pass continues, so planned-failover intake, a previous run's reclone queue and fence release, label sync and the rest of the reconcile keep running for the hours staging takes. The early return stays for `Preflight` through `Resuming`. A test sets a fresh planned-failover annotation during `Staging` and asserts it is accepted and completes, and that a `Draining` queue of an earlier run drains.

**Backups and the fence.** Preferring replicas is not a guarantee: backups fall back to the primary, and without a dedicated backup Secret they use the operator credentials (`EffectiveBackupSecretName`, `api/v1alpha1/credentials_helpers.go:23-31`), whose sessions the fence never evicts (`internal/mysql/restore_fence.go:242-247`). So from Preflight until the run is terminal, the backup reconciler leaves a new backup `Pending` (reason `RestoreInPlace`) when its selected source is the run's target site (after `selectSourceSite`, `internal/controller/backup_reconciler.go:219`). Preflight lists the group's `Running` backups with `status.sourceSite` equal to the active site, waits for them (Event `RestoreInPlaceWaitingForBackups`, no bound, like leases; deleting the backup ends the wait), and only then exits. While a damage record exists, a scheduled backup (label `shipstream.io/backup-schedule`) without an override is not started: it gets a new terminal phase `Skipped` (`BackupPhase` enum, `api/v1alpha1/mysqlbackup_types.go:69`, gains it) with reason `RestoreDamagedPrimary` and Event `ScheduledBackupSkippedDamagedPrimary`, so the schedule rollup reports it (`internal/controller/backup_schedule.go:451-466`), the retry path, which acts only on `Failed`, leaves it, and retention prunes it under the same bound as failed backups (`pruneByPhase`, `internal/controller/backup_reconciler.go:1334`). An explicit backup without an override waits `Pending`; one with `sourceSiteOverride` runs as requested (exit 3 needs this).

### 1. Acceptance (entry)

`validateInPlaceRestoreSpec` keeps its checks and replaces the blanket `pointInTime` refusal with the acceptance-pass refusals listed under API; `validatePITRBackupBefore` and `validatePITRBackupBound` run here for PITR runs, and the new `validateFullInstanceBound` for every full-instance run, against the referenced backup. A valid PITR run creates its snapshot, then writes `phase=Staging`, `runID`, `runConfirm`, `startTime`, `kind`, `scope=full`, `requestSHA256`, `snapshot` and `staging{}` in one status write, and emits `RestoreInPlaceStagingStarted`. Nothing else happens on that pass. If `status.restoreDamage` exists, the same write (for any full-instance run) sets `restoreDamage.runID` to the new run (exit 1, Event `RestoreInPlaceDamageTakenOver`); the guards never lapse because they key on the record, not on the run.

### 2. Staging

No deployment hold, no topology freeze, no reclone queue, no fence, no `offline_mode`, and the reconcile pass continues past the restore (guards table). A deploy that migrates the schema during staging would be undone by the restore while the new code keeps running, so new leases stay refused for the whole run; existing leases are only waited for in Preflight. Steps, one durable write before each external action:

1. **Source hold.** Stamp `hold.restore.shipstream.io/<runID>` on the `MysqlBackup` with an optimistic lock, then re-read it uncached before any Job is created and refuse if it is gone, terminating or its UID differs from the request, as `stampVerificationHold` and `launchHoldConfirmed` do (`internal/controller/backup_verification_hold.go:308`, `:347`). Retention (`deleteUnreadBackup`, `:418`), artifact cleanup (`internal/controller/backup_reconciler.go:637`) and the PITR binlog cutoff (`cmd/bloodraven/pitr_retention.go:53`) honour restore holds as they honour verification holds. Release rule: see the hold paragraph under API.
2. **Image selection and probe.** Two values are kept apart. `imageRuntimeID` is the runtime's identity of an image (`ContainerStatus.imageID`; for images imported into k3d/kind a bare `sha256:<id>`, which can name an image configuration rather than a registry manifest), recorded for diagnosis only. `imageRef` is what every pod after the probe runs: a string that parses as `<repo>@sha256:<digest>` (distribution reference grammar, after stripping a `docker-pullable://` prefix); it is never assembled from a repository and a runtime ID. Candidate = `staging.image` (`Override`), else the live target's mysqld container when its `imageID` parses as such a reference (`LiveTarget`; a live image without one is skipped, never guessed), else `spec.backup.image` (`BackupImage`). A probe Job (`mysql-<fg>-staging-probe-<run8>-<attempt>`, no volumes, no token, image `imageCandidate`, the one pod that runs an unpinned reference) runs `mysqlsh --version; mysqld --version; mysqlbinlog --version; mysql --version` under a 120-second `timeout` inside the container and reports through the termination message; its Job `activeDeadlineSeconds` is 720, because Kubernetes counts it from the Job's start, so it must also cover the 10-minute durable bound on scheduling and pull. `imageRef` is the candidate when it already is a digest reference, else the probe pod's `imageID` when that parses as a digest reference of the candidate's normalized repository; otherwise `Failed`/`StagingImageNotPinned` before any claim exists. A deadline, `ErrImagePull`/`ImagePullBackOff` past the bound, or an eviction is a lost Job, not an answer. Only a completed probe that reports a missing binary, or a mysqlsh older than the backup's `status.mysqlshVersion`, makes a `LiveTarget` candidate fall back once to `BackupImage`. The probe's parsed `mysqld` `X.Y.Z` must equal the live target's `SELECT VERSION()` numeric prefix and must not be older than the backup's `status.serverVersion`; otherwise `Failed`/`StagingVersionMismatch` with `versionGate` set (the authoritative comparison is the staged server's own `VERSION()` string at step b). **Unverified:** whether the default `mysql:9.7` image ships mysqlsh (`api/v1alpha1/backup_types.go:9-21` names the community-server image as the mysqlsh carrier), so the design probes and never assumes.
3. **Reuse decision** (here, not at acceptance, because it needs the probe's pinned reference). `mode=Reuse` when a `Verified` artifact has the run's `requestSHA256`, the current #203 revisions of both the backup prefix (`dumpEncryptionRevision`) and the archive prefix (`archiveEncryptionRevision`), the current #206 `archiveIdentity` for the profile, the run's `imageRef`, the operator's `formatVersion`, a claim whose UID matches `pvcUID`, and, if `localNode` is set, a node that exists, is Ready and is not cordoned. Else `mode=Build`, which needs a free artifact slot (`StagingArtifactSlotsFull` otherwise; a Reuse run needs none).
4. **Claim (Build).** Append `restoreStagingArtifacts[{originRunID: runID, pvcName: mysql-<fg>-staging-<run8>, state: Provisioning, cleanupPolicy}]` (write-ahead), then create the claim (RWO, `volume.size`, `storageClassName`, owner reference to the group, label `shipstream.io/restore-run=<runID>`), record `pvcUID`, and set `provisioningSince`. When the backup recorded its size, a claim below `3 x MysqlBackup.status.sizeBytes` emits `RestoreInPlaceStagingVolumeSmall` (a warning: compression ratios vary). When it did not (`sizeBytes` 0, e.g. S3), the same Warning is emitted at Staged from the measured `sourceBytes` (informing reuse and the next run), and step a's free-space check is the only early guard. The consumer is created **before** waiting for binding: with `volumeBindingMode: WaitForFirstConsumer` (the playground class, `playground/manifests/storageclass.yaml:16`) the claim binds only once a pod is scheduled. Once bound, the operator reads the PV: `nodeAffinity` on `kubernetes.io/hostname` sets `localNode` and emits `RestoreInPlaceStagingVolumeNodeLocal` (zonal affinity is not node-local). The claim bind, pod scheduling and volume attach together must finish within 30 minutes of `provisioningSince` (durable, so restarts do not reset it), else `StagingVolumeUnavailable`.
5. **Staging Job (Build), re-auth Job (Reuse).** Before every creation, including re-creations, an uncached read must show every earlier Job of this run finished (an explicit terminal condition, `jobFinished`) and every pod labelled `shipstream.io/restore-run=<runID>` terminal (`Succeeded`/`Failed`; a pod with a deletion timestamp that is not terminal still counts as running). The same proof precedes probe, re-auth and result-read Jobs. A finished Job is deleted (foreground) once its result or loss is recorded in status, so completed predecessors never block progress. Then write `jobCreating=true`, `jobName` and `attempt+1` (write-ahead, as `internal/controller/restore_inplace.go:825-835` does for the destructive Job), then create the Job, whose pods carry `shipstream.io/restore-attempt=<attempt>`. Pod: hardened security context (uid/gid 27, read-only root, drop ALL; `internal/controller/backup_job.go:505-560`), `automountServiceAccountToken: false`, no `hostNetwork`, read-only failover tolerations, `backoffLimit: 0`, `activeDeadlineSeconds` from the snapshot. Init containers, in order (operator image): `staging-prepare` checks that `/var/lib/mysql` is writable by uid 27, refuses if `<runID>/owner` names another run, clears `<runID>/` (Build attempts only; the pod-gone proof above ran first) and writes `owner` (runID, attempt); then `decrypt-download` and `pitr-download` with the #203 revision Secret mounted 0400, writing to `<runID>/inputs/` on the claim instead of size-limited emptyDirs. The main script only verifies `owner` and never clears. Re-auth and result-read Jobs never clear anything; they mount the claim read-only and get the 30-minute scheduling bound, and because a Job's deadline counts from its start, which includes that wait, `activeDeadlineSeconds = 1800 + 600 + ceil(2 x artifactBytes / max(1, hashBytesPerSecond))` (pinned by a builder test). The staging Job's deadline from the snapshot likewise includes its scheduling wait. The claim is mounted at `/var/lib/mysql` (the stock AppArmor profile only allows a datadir under `/var/lib/mysql/**`, `internal/controller/backup_verification_job.go:18-33`); `/run/mysqld` and `/tmp` are emptyDirs. A pod failure carrying the `DisruptionTarget` pod condition (eviction, preemption, drain) counts as a lost attempt, not a failure.

Staging Job internals (new `staging.sh`, sharing the mysqld boot with `verify_script.sh` and delegating load and replay to `restore.py`). Layout on the claim: `<runID>/owner` (runID and attempt), `<runID>/inputs/`, `<runID>/data/`, `<runID>/gtid-proof.json`, and the artifact directory `<runID>/artifact/` holding `manifest.json` and `dump/`.

| Step | What | Fails with |
|---|---|---|
| a | `<runID>/owner` names this run and attempt (written by `staging-prepare`; `StagingVolumeNotWritable` there); measure `sourceBytes`, the downloaded dump's total size; free-space check against it. Read the source `@.json`: its `serverVersion` and the mysqlsh version in its `dumper` string must equal the snapshot's backup `serverVersion` and `mysqlshVersion` (key names on 9.7 **unverified in repo**; the native test pins them); refuse `X ∩ B ≠ ∅` | `StagingVolumeNotWritable`, `StagingFailed` (`SourceVersionMismatch`), `StagingGtidProofFailed` |
| b | Boot mysqld on `<runID>/data` after `--initialize-insecure`, with the target site's effective `mysqlConf` (group merged with site and unsafe keys dropped, as `generateMyCnf` does, `internal/controller/reconciler.go:925`, `:1038`) in an option file minus a pinned denylist (networking and admin ports, replication, binlog and GTID options, plugin and component loading, `init_file`, path options, and the buffer-pool keys `innodb_buffer_pool_size`, `innodb_buffer_pool_chunk_size`, `innodb_buffer_pool_instances`), and on the command line, which overrides option files: `--bind-address=127.0.0.1 --mysqlx=OFF --skip-log-bin --gtid-mode=ON --enforce-gtid-consistency=ON --skip-replica-start --event-scheduler=OFF --skip-name-resolve --partial-revokes=ON --local-infile=1` (the verification set, `internal/controller/verify_script.sh:191-204`, plus the event scheduler) and `--innodb-buffer-pool-size=<from resources>` (API); `VERSION()` must equal `BLOODRAVEN_STAGING_EXPECT_VERSION` exactly before anything is loaded | `StagingVersionMismatch` |
| c | Keyring: load `component_keyring_file` as verification does (`:95-153`). Unlike verification it fails closed: if the component cannot load and any source dump DDL file contains `ENCRYPTION='Y'`, stop. (`encryptedTables` is computed later, from the artifact, at step l) | `StagingKeyringUnavailable` |
| d | `util.load_dump` with the staging options (API). Accounts are loaded because `--initialize-insecure` creates only root and the replay pipe (`mysqlbinlog | mysql`, no `--force`, `internal/controller/restore_script.py:503-568`) stops on the first `ALTER USER`/`GRANT`/`SET PASSWORD` against a missing account; the operator's own MysqlDatabase statements are binlogged and land in every window. Then `ACCOUNT LOCK` every loaded account. Record the load duration. **Unverified in repo:** mysqlsh `loadUsers`/`excludeUsers` behaviour on 9.7; the native test covers it | `StagingFailed` |
| e | `_verify_loaded_dump` (post-stop GTIDs already in the dump, lineage; `internal/controller/restore_script.py:581-618`) | `StagingTargetNotCovered` |
| f | Replay through the #206 chronological plan, not one mysqlbinlog run per site. `BLOODRAVEN_SOURCE_GTID_EXECUTED` is left unset: `B` is already in `gtid_executed` after `replace`, so GTID deduplication skips it, and today's join of it into one `--exclude-gtids=` argument (`internal/controller/restore_script.py:521-530`) cannot carry a 256 KiB set. Only `X` (at most 96 KiB, acceptance) is passed, as today (`internal/controller/pitr.go:546`) | `StagingFailed` |
| g | Staged-GTID proof, below | `StagingGtidProofFailed` |
| h | Verdict: `targetCovered=true`, `partial=false`, `crossSiteUnverified=false` | `StagingTargetNotCovered` |
| i | `SET GLOBAL super_read_only=ON`; optional sanity query inside `START TRANSACTION READ ONLY` | `StagingFailed` (`SanityCheckFailed` detail) |
| j | Delete `inputs/`; `util.dump_instance('<runID>/artifact/dump', {consistent:true, users:false, events:true, routines:true, triggers:true, compression:'zstd', threads})` | `StagingFailed` |
| k | Re-read `@@gtid_executed`; it must equal the proven set and the artifact's `@.json` `gtidExecuted` | `StagingGtidProofFailed` |
| l | Compute `encryptedTables` by scanning every DDL file of the artifact dump for `ENCRYPTION='Y'` (replay can create or alter such a table); write `artifact/manifest.json`: `{version:1, originRunID, requestSHA256, sourceDumpSHA256, archiveIdentity, dumpEncryptionRevision, archiveEncryptionRevision, files:[{path,size,sha256}], baselineGtids, stagedGtidExecuted, serverVersion, dumpServerVersion, mysqlshVersion, imageRef, formatVersion, encryptedTables, sourceBytes, loadSeconds, verdict:{targetCovered, partial, crossSiteUnverified}, createdAt}`; fsync. The hashing throughput is measured here (`hashBytesPerSecond` = bytes / max(1, whole seconds)). Either GTID set above 256 KiB fails here | `StagingFailed` |
| m | Stop mysqld, remove `<runID>/data`; print the result (below) | |

**Staged-GTID proof.** Let `B` be the dump's `@.json` `gtidExecuted` (installed by `replace`), `X` the user's `excludeGtids` (`X ∩ B = ∅`, step a), and from the #206 plan `P` the exact set of transactions committed at or before the stop that the replay must apply, and `Q` every staged GTID after the stop. In-place runs require #206's exact plan (P1), so with `S = @@gtid_executed` after replay the proof is:

- equality: `S = B ∪ (P − X)` (nothing planned was skipped: a short `--stop-datetime` cut, a mid-file stop or a cross-site skip all fail; nothing outside the plan ran);
- no post-stop leak: `S ∩ Q = ∅`.

#206 must emit `P` and `Q` in `coverage.json`; today's per-site `inWindowGTIDs`/`postStopGTIDs` (`cmd/bloodraven/pitr_download.go:225-274`) cover only the single-replaying-site case. The diffs are written to `<runID>/gtid-proof.json` and printed to the log for diagnosis.

**Result channel.** The termination message (4096 bytes) is the one authoritative result and is bounded: `BLOODRAVEN_STAGING_COMPLETE v=1 runID= attempt= requestSha256= imageRef= formatVersion= dumpEncRev= archiveEncRev= targetCovered= partial= crossSiteUnverified= manifestSha256= stagedGtidSha256= baselineGtidSha256= sourceDumpSha256= archiveIdentity= serverVersion= dumpServerVersion= mysqlshVersion= encryptedTables= sourceBytes= bytes= loadSeconds= hashBps=` (fixed-width values plus `imageRef` and `archiveIdentity` at their MaxLength fit in under 2 KiB). It carries every certification field and the verdict. `restore.py` writes its own `BLOODRAVEN_PITR_VERDICT` and termination text to the same file (`internal/controller/restore_script.py:387-394`, `:628-645`), so `staging.sh` redirects both, as verification does (`BLOODRAVEN_PITR_VERDICT_PATH` and `BLOODRAVEN_TERMINATION_LOG` to temp files, `internal/controller/verify_script.sh:249-252`), reads the verdict from there for step h, and writes the termination message last. The two GTID sets, at most 256 KiB each, travel in the last log lines (`BLOODRAVEN_STAGING_GTIDS v=1 baseline=<B> staged=<S>`). The existing log reader is best-effort: it tails 50 lines with a 16 MiB line limit (`internal/controller/restore.go:413-429`) and never decides a Job's outcome (`:307-308`). The operator therefore accepts the logged sets only when their digests match the termination message. If the log is missing, truncated or mismatched, the operator runs a result-read Job (the re-auth builder in `ReadResult` mode: read-only mount, checks `manifest.json` against `manifestSha256`, prints the sets and the manifest's fields from it); each such Job counts toward `lostJobs`. A missing termination message (pod gone) is a lost Job. Status and the `<pvcName>-gtids` ConfigMap, written only by the operator from these checked values, are the trust anchor; the claim's `manifest.json`, authenticated by `manifestSHA256`, is the durable copy.

### 3. Staged

**Build.** On Job success the operator checks the result as above (every echoed field equal to the run's own value, the verdict `targetCovered=true, partial=false, crossSiteUnverified=false`, the versions), creates the `<pvcName>-gtids` ConfigMap, writes the artifact's certification fields and `state=Verified`, sets `phase=Staged` and emits `RestoreInPlaceStaged`. Next pass: `phase=Preflight`.

**Reuse.** Staging runs a re-authentication Job instead: it mounts the artifact's claim read-only at `subPath: <artifact.path>`, checks `manifest.json` against `manifestSHA256`, re-hashes every file under `dump/` (no missing or extra files), checks the artifact `@.json` set against `stagedGtidSHA256`, and prints the same completion sentinel. On success the operator checks that every certified field in the sentinel equals the stored entry, changes neither the entry nor its ConfigMap (it only checks the ConfigMap against both digests), sets `phase=Staged` and emits `RestoreInPlaceStagingReused`. A digest mismatch on the claim (its own sentinel) makes the artifact `Invalid` and ends the run `StagingArtifactMismatch`. Any other failure (I/O error, eviction, a lost pod, the deadline) leaves the artifact `Verified` and is a lost Job.

**Lost `<pvcName>-gtids` ConfigMap.** A ConfigMap that is missing or fails either digest is metadata loss, not artifact damage, in every phase and also while a damage record exists: the operator deletes a mismatched one, runs a result-read Job against the claim (whose `manifest.json` carries both sets under `manifestSHA256`) and recreates the ConfigMap from the digest-checked sets. The artifact becomes `Invalid` only if that Job finds the claim itself fails authentication.

**Artifact lifecycle.** Every staging claim has one entry in `restoreStagingArtifacts` from its write-ahead until it is observed gone, so a run's terminal write never has to hand a claim to another list.

| State | Entered | Leaves to |
|---|---|---|
| `Provisioning` | claim write-ahead | `Building` at the staging Job write-ahead; `Invalid` on timeout, cancel, supersede or failure |
| `Building` | staging Job may write | `Verified` (Staged checks pass); `Invalid` (failure, cancel, supersede, attempts exhausted) |
| `Verified` | certified | `Deleting`: (1) a run that loaded it reached `Succeeded`; (2) policy `Delete` when its origin run is terminal and nothing protects it; (3) replacement: another `Verified` artifact passed the Fencing rechecks against the bound target; (4) cleanup annotation. `Invalid` on a claim digest mismatch found by a re-auth or result-read Job, or a loader refusal whose sentinel reason is `ArtifactMismatch` (never on ConfigMap loss alone) |
| `Invalid` | never reusable | `Deleting` once no pod of its origin run can mount it |
| `Deleting` | UID-conditional delete issued (`Preconditions{UID}`) | entry removed when the claim is observed gone |

Protection overrides (1)-(3): an artifact referenced by a non-terminal run is never deleted, and while `status.restoreDamage` exists no `Verified` artifact is deleted automatically (a replacement that then fails a hand-off check must not have cost the previous repair source); deferred deletions run when the record clears. `Invalid` artifacts protect nothing and are deleted as usual. A claim deleted directly is tolerated: the entry is removed with `RestoreStagingVolumeGone`, and a run that references it fails `StagingVolumeUnavailable` before the loader write-ahead. `Verified` entries whose origin run is terminal are what the retained gauge counts. `MaxItems=8` cannot be exceeded because a Build needs a free slot (step 3).

### 4. Preflight

Today's Preflight unchanged (`internal/controller/restore_inplace.go:477-641`, plus #233's peer checks: `BeginPlanned` hold, writable active site, privileges, previous fence released or kept by a damage record this run took over, queue armed, fence recorded), plus:

- with a damage record present: the execution-stopped proof (the damage invariant); a fixed-name loader Job of an earlier run is deleted only after that;
- privileges (all full-instance runs): `RestoreFencePrivileges` (`internal/mysql/restore_fence.go:64-72`) gains `RELOAD` (`FLUSH BINARY LOGS`) and `TRANSACTION_GTID_TAG`; the explicitly assigned tagged `gtid_next` needs `TRANSACTION_GTID_TAG` plus one of `SYSTEM_VARIABLES_ADMIN`, `SESSION_VARIABLES_ADMIN` or `REPLICATION_APPLIER` (MySQL 9.7 Reference Manual, `gtid_next` system variable; tagged GTIDs since 8.3; **unverified in repo**), which the existing `SYSTEM_VARIABLES_ADMIN` requirement covers. `SUPER` implies neither `RELOAD` nor `TRANSACTION_GTID_TAG`, so today's "SUPER covers all but PROCESS" rule (`:149`) becomes a per-privilege list; a unit test and the real-MySQL test use a SUPER-only account;
- for PITR runs: live `VERSION()` of the active site equals the artifact's `serverVersion` (`StagingVersionMismatch`); `S ⊆ L` with `L` the live `gtid_executed` (`StagedGtidsNotInLive`, below); if `encryptedTables`, the live keyring component is active (`performance_schema.keyring_component_status`; `StagingKeyringUnavailable`); claim bound, UID unchanged, not terminating, and if `localNode` is set that node exists, is Ready and is not cordoned (`StagingVolumeUnavailable`); every staging, probe, re-auth and result-read pod of the run terminal;
- loader budget, in whole seconds with saturating arithmetic (capped at 30 days): `rate = min(hashBytesPerSecond, loaderHashBytesPerSecond)` over the non-zero ones; `hash = ceil(artifactBytes / max(1, rate))`; `load = ceil(max(1, stagingLoadSeconds) x max(1, artifactBytes / max(1, sourceBytes)))`, where `sourceBytes` is what staging measured, never `MysqlBackup.status.sizeBytes`; `preDestruction = 600 + 2 x hash`. With `loaderActiveDeadlineSeconds = D` set, refuse `StagingLoaderDeadlineTooShort` when `D < preDestruction + 1.5 x load`; unset, emit `RestoreInPlaceLoaderEstimate` with the estimate. This is a heuristic, not a proof: the live load runs over the network into a busy server. It is evaluated again at Preflight exit, immediately before Fencing. A loader that refused on its pre-destruction deadline reports its measured re-hash rate in its sentinel, and the operator lowers `loaderHashBytesPerSecond` to it, so the next confirm on the same artifact either fits or is refused here instead of repeating the refusal after the write-ahead;
- quiesce (all full-instance runs): Preflight exits no earlier than 65 s after `preflightSince`, so a MysqlDatabase reconcile that passed its `groupFenced` check (`internal/controller/mysqldatabase_reconciler.go:263`) just before the pause has spent its 60-second MySQL budget (`mysqlDatabaseReconcileBudget`, `:63`, applied at `:433`) before the fence; and it waits for running backups sourced on the active site (Guards, "Backups and the fence").

**Staged set not a subset of live `gtid_executed`: refuse.** `D = S − L` non-empty means the staged data contains transactions the live history does not record, for example an archived branch a lossy failover abandoned, or a dump from another group. The alternative, `SET GLOBAL gtid_purged='+D'` after the load, rewrites the live primary's GTID history behind the archiver's back: the next archived file's `Previous_gtids` would jump by `D` with no archived events, which the predecessor-ordering planner cannot anchor, and it would quietly resurrect a branch the group abandoned. The message splits `D`: for `D − B` (replayed transactions) adding them to `pointInTime.excludeGtids` and re-running helps (the identity changes, so it re-stages); for `D ∩ B` (transactions inside the dump) nothing in this run can remove them, and only a different backup or a restore into a new group helps. Consequence, documented: in-place PITR from another group's backup or archive is always refused.

### 5. Fencing

Target site bound at entry (`TopologyGeneration++`, existing). Order:

1. strip the primary role label, wait until fenced (existing);
2. **Application fence** (every full-instance restore, PR1). So that neither sessions nor events loaded with the data write into a half-loaded schema, `offline_mode=ON` and `event_scheduler=OFF` share the existing protocol (`internal/controller/restore_offline_fence.go:377-462`): (i) both prior values are read and recorded (`priorOfflineMode`, `priorEventScheduler`) before either `SET`, and the site reaches the topology manager only once both are durable (`restoreOfflineFenceSite`, `:318-326`, gains the second condition), so a crash between record and `SET` loses nothing; (ii) a fence taken over on the same site keeps the earlier run's priors (`recordRestoreOfflineFence`, `:359-366`, carries both), so a takeover run restores the original values, not the `OFF` its predecessor applied; (iii) the topology manager re-asserts both on every Poll while the fence is wanted, which PR1 extends to "a damage record exists" (`reassertRestoreOfflineFence`, `:193-219`, which today only reads and sets `offline_mode`), because a restarted mysqld comes back with its configured scheduler; whenever a Poll finds the scheduler not `OFF`, it sets `OFF` and then kills every event worker thread (below), on every Poll until none remains, in every phase where the fence or a damage record is wanted; (iv) the release restores both (`:464-505`). A scheduler `DISABLED` at startup is recorded and left alone. `SET PERSIST` was rejected: it edits the user's persisted configuration in the datadir, which outlives the operator, and restoring a value the user had persisted needs a second record. Its cost: after a mysqld restart, events that commit before the next Poll (seconds) keep their writes, behind the Service fence; an event still running at that Poll is killed and rolled back. Then pre-fence sessions are evicted (existing), and **event workers are drained**: `OFF` stops scheduling but leaves running events executing (MySQL `Event_scheduler::stop()`), and the eviction exempts the operator's own account, which can be an event's definer (`internal/mysql/restore_fence.go:232-247`). The operator kills every thread in `performance_schema.threads` with `NAME = 'thread/sql/event_worker'`, whatever its account (with the `PROCESS` and `CONNECTION_ADMIN` the fence already requires, `internal/mysql/restore_fence.go:64-72`), waits until none remains (at most 60 seconds) and records `eventWorkersDrained` (it means "drained at Fencing"; the Poll kill above and the loader's own check, Restoring, cover later restarts); otherwise, for example for a `SYSTEM_USER` definer it cannot kill, the run ends `RestoreEventWorkersRemain` before the barrier and the fence is released unless a damage record exists. The thread name is **unverified in repo**; the real-MySQL test pins it;
3. rechecks against the bound target: request identity, `VERSION()`, `S ⊆ gtid_executed` (a staging-time failover may have bound a different server), live keyring if `encryptedTables`, claim UID and node, staging pods terminal, artifact hand-off rule. A refusal releases the fence immediately (no Job, no barrier) unless a damage record exists;
4. **barrier** (every full-instance run, PR2): if `restoreBarriers` holds 32 entries, evict one under the rule in step 9, or refuse `RestoreBarrierSlotsFull` before allocating (fence released unless a damage record exists); allocate `n = 1 + max(status.restoreMarkerSeq, every marker number of any `bloodraven_restore*` tag across all source UUIDs in the target's `gtid_executed`)` and, in one status write, set `restoreMarkerSeq = n` and `restoreBarriers[runID]{n, barrier: {gtid: <target server_uuid>:bloodraven_restore:<n>, state: Writing}}`, **before** committing anything. Because the sequence never decreases and an allocated number is recorded before use, no number is ever handed out twice, even when a barrier is never committed. Then `FLUSH BINARY LOGS`; `SET GTID_NEXT='<uuid>:bloodraven_restore:<n>'; BEGIN; COMMIT; SET GTID_NEXT='AUTOMATIC'`; `FLUSH BINARY LOGS`; `state: Written`, Event `RestoreBarrierWritten`. An empty transaction is logged only under an explicitly assigned GTID; with `AUTOMATIC` (tagged or not) it gets none (MySQL 9.7 Reference Manual, GTID lifecycle; **unverified in repo**). Retry is idempotent: on restart in `Writing` while the run is non-terminal, the recorded GTID already in `gtid_executed` means it was committed; otherwise both statements run again. In `Written` the second flush is re-run unconditionally (an extra rotation is harmless). The flushes put the barrier alone in its own sealed binlog file;
5. #233 peer IO stop with catch-up; `fencedGtidExecuted` now includes the barrier;
6. if PITR archiving is enabled, wait up to 10 minutes for the primary's sidecar to report the barrier archived (new `GET /archiver/covers?gtids=<set>`: whether a sealed archived file covers the set), then `state: Archived`. Timeout ends the run `RestoreBarrierNotArchived` before any Job;
7. Event `RestoreInPlaceWritesDiscarded` (requested stop, fence time), `phase=Restoring`.

### 6. Restoring

Today's destructive path (`internal/controller/restore_inplace.go:772-956`) with these changes:

- **Immediately before the write-ahead**, the operator re-reads the target `VERSION()`, the live keyring if `encryptedTables`, and the request identity (refusals as in Fencing; a version refusal here does not consume the confirm, Q7's "immediately before the DROP" check), then submits the exact loader Job as a server-side dry-run Create (`dryRun=All`; refusal ends the run like a Fencing refusal, before any record). It covers Job validation and admission only, not the pod's (damage invariant).
- **Damage write-ahead (every full-instance run, PR1).** The status update that sets `jobCreating=true` also creates `status.restoreDamage{originRunID: runID, runID, site, since}`, unless a record already exists (this run then owns it from its acceptance write), and (PR2) sets `restoreBarriers[runID].loaderWriteAhead`. From this write on the damage invariant holds. The loader gauges are set with it (Metrics). If the API server still refuses the real Create (Invalid/Forbidden/BadRequest; today that withdraws `jobCreating`, `:836-845`, which stays), the run ends `Failed` and the record stays.
- **Source:** the artifact's claim (`pvcUID` checked uncached before Create), `subPath: <artifact.path>`, read-only at `/staging` (`/staging/manifest.json`, `/staging/dump`); image: the artifact's `imageRef` (a digest reference, so the kubelet runs exactly the bytes that built the artifact); no `pitr-download` or `decrypt-download` init containers; loader options as in API; no `updateGtidSet` (the live-target rule, `internal/controller/restore.go:937-949`, `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline`). Pods carry `shipstream.io/restore-loader=<group>` (every full-instance loader, PR1). `activeDeadlineSeconds` = `loaderActiveDeadlineSeconds` (none when unset) for PITR runs; non-PITR runs keep `spec.backup.activeDeadlineSeconds`.
- **Env:** `BLOODRAVEN_RUN_ID`, `BLOODRAVEN_STAGING_MANIFEST_SHA256`, `BLOODRAVEN_STAGING_GTID_SHA256`, `BLOODRAVEN_EXPECT_TARGET_VERSION`, `BLOODRAVEN_EXPECT_KEYRING` (when `encryptedTables`), `BLOODRAVEN_PRE_DESTRUCTION_DEADLINE`, only when `D` is set: absolute time `T0 + D − 1.5 x load` (Preflight budget), where `T0` is the operator's clock at the write-ahead. With no loader deadline there is no pre-destruction deadline either: a slow re-hash then only lengthens the outage, while a refusal would keep the fence up anyway and need a human. `T0` is not later than the Job's creation, and Kubernetes counts `activeDeadlineSeconds` from `status.startTime`, which is not earlier, so the bound is conservative.
- **Pre-destruction refusal path (PR1, all full-instance loaders).** `restore.py` sets a module flag immediately before its first mutating statement (`_maybe_reset_replication`, then `_maybe_preflight_drops`, `internal/controller/restore_script.py:880-883`). Before that point every refusal goes through one function, `_refuse_before_destruction(reason)`, which asserts the flag is unset, writes `BLOODRAVEN_PRE_DESTRUCTION_REFUSED v=1 runID=<id> job=<name> reason=<r>` to the termination message and exits with a dedicated code (proposed 70; today every failure exits 2, `:399-402`, `:700-742`, `:887-894`). The refusals routed through it: `_verify_staging_artifact` (manifest digest, every file size and SHA-256, no extra files, artifact `@.json` set digest; reason `ArtifactMismatch`, which makes the artifact `Invalid`), target `VERSION()`, live keyring active when expected, `_prepare_local_infile`, `_require_offline_mode`, `event_scheduler` `OFF` or `DISABLED` with no `thread/sql/event_worker` thread (`EventWorkersRunning`, which covers a mysqld restart after the Fencing drain), the execution claim (`ExecutionClaimHeld`, damage invariant), and the pre-destruction deadline (checked after every hashed file and immediately before the flag is set; its sentinel carries the bytes hashed and seconds spent). After the flag is set the function falls through to exit 2. The run's status reason is `RestoreInPlacePreDestructionRefused` with the sentinel reason as detail; `ArtifactMismatch` also makes the artifact `Invalid`; nothing clears a damage record. **Unverified in repo:** that `mysqlsh --py -f` propagates `sys.exit(70)` as the container exit code; PR1 pins it with a native test. The full re-hash runs while fenced and adds minutes to the outage for a large artifact: the price of turning a truncated or altered claim into a refusal before the DROP instead of a load failure after it.

### 7. Resuming and Succeeded

The loader is done only when the Repaired conditions hold: `JobComplete=True`, counters `succeeded=1, failed=0`, and the execution-stopped proof (the damage invariant; today the run moves to `Resuming` on `jobPhase` `Succeeded`, `internal/controller/restore_inplace.go:958-997`, which PR1 tightens). Then (PR2) write `restoreBarriers[runID].completion{gtid: <uuid>:bloodraven_restored:<n>, state: Writing}` (same uuid and `n` as the barrier), commit it with the same explicit-GTID statements (idempotent the same way), `Written`, Event `RestoreCompletionWritten`; an unreachable primary is retried with the fence up, since the load already succeeded. Only then release the queue to `Draining` and write `Succeeded`, which clears `status.restoreDamage` in the same write (the Repaired proof; the run owns the record, and only full-instance runs can). The completion marker therefore precedes the fence release and every reclone, so recloned peers carry it and held peers do not. The hold on the source backup is released, and the artifact the run loaded goes to `Deleting` (success always deletes).

### 8. After a failed or lost loader Job

Two independent questions:

1. **May the peers resume replication?** #233 settlement: Held vs Discarded from the Job pods and `gtid_executed == fencedGtidExecuted` (`restorePrimaryUnchanged`, `internal/controller/restore_reclones.go:840-891`). PR2 changes that comparison to drop every `bloodraven_restore*` tag from both sets first: markers carry no data, so a void or accept marker committed before or after a restart can never turn Discarded into Held. An unlogged load writes nothing to the binlog, so Discarded is correct for the peers even when the primary was changed.
2. **May the primary serve?** No. Every run that reached its loader write-ahead has a damage record, and no end of a loader clears it (the damage invariant); exit 1 or exit 2 does.

**Void.** Eligibility is read from the barrier entry alone, never from run status, which the next run replaces (`internal/controller/restore_inplace.go:400-410`) and a failure rewrites without the run ID (`:1139-1149`): an entry with `loaderWriteAhead` unset whose run is terminal gets `<barrier uuid>:bloodraven_restore_void:<n>`, whether or not the run inherited a record. Its write-ahead (`restoreBarriers[runID].void`, state `Writing`) is recorded in the same status update that writes the run's terminal phase, so the intent survives any later replacement; it is then committed with an explicit `GTID_NEXT` on the group's current writable primary (legal on any server, so a failover in between does not matter), idempotent like the barrier, Event `RestoreBarrierVoided`. Voiding a barrier that was never committed is harmless and reserves `n` in-band. The void is independent of the fence: without a damage record the fence follows #233 (including its `restoreSettleBound` release, `:827`), and the void is retried until it lands; until then the barrier stays effective, which only over-refuses. With a damage record the fence stays, whatever the void does. A run that reached its write-ahead is never voided: its barrier stays effective until a closing marker numbered `n` or higher (a later repair's completion or an accept) supersedes it under the base rule. An inherited record keeps the origin run's barrier effective, so a void never hides earlier damage.

### 9. Post-restore backup and barrier closure

After `Succeeded` or an accept, once the fence release is observed (an ordering rule, independent of which credentials backups use), with PITR archiving enabled, the operator creates `MysqlBackup` `<fg>-post-restore-<run8>` on the PITR profile with `spec.dumpScope: Instance` and `sourceSiteOverride` set to the bound target, the site that holds the restored or accepted data (Event `PostRestoreBackupCreated`). Without the override, source selection prefers any replica whose last poll shows it replicating (`selectSourceSite`, `internal/controller/backup_reconciler.go:1465-1509`), which after an accept can be a peer stopped seconds earlier that lacks the accept marker. A barrier entry closes when (a) a `Succeeded` backup of the group with `status.dumpScope=Instance` and `status.consistent=true` has a `status.gtidExecuted` containing a *closing marker* (a completion or an accept marker) numbered `n` or higher (`gtidExecuted` is read before the dump starts, `internal/controller/backup_script.py:317-320`, so the snapshot is after the load), or (b) its void is committed. Closure removes the entry and emits `RestoreBarrierClosed`; with no open entry, `PITRArchiveReady=False/PITRRestoreBarrier` clears. A failed backup, or a `Succeeded` one whose `gtidExecuted` carries no closing marker numbered `n` or higher, emits `PostRestoreBackupFailed` and is not retried automatically; any later qualifying backup closes the barrier. With PITR archiving disabled the markers are still written (in-band, free), the archive wait and the automatic backup are skipped, and an entry closes when its void or a closing marker numbered `n` or higher is committed. At `MaxItems=32` the oldest entry whose run is terminal, whose every allocated marker is committed (`Written` or `Archived`) and which has no pending void is dropped with `RestoreBarrierEntryDropped`: status is display only, so dropping an entry never weakens the planner. With no such entry, the next run refuses `RestoreBarrierSlotsFull` before allocating (Fencing step 4); reaching that takes about 32 runs that passed the write-ahead with no repair, accept or qualifying backup in between.

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
| `internal/controller/runner.go`, `reconciler.go` (existing) | Preserved-field allowlist and `Ready` override (`updateCRStatus`); the reconcile pass continuing during `Staging`/`Staged` |
| `internal/controller/backup_reconciler.go`, `cmd/bloodraven/encrypt_upload.go` (existing) | Backup admission while fenced or damaged, `Skipped`; sentinel fields through `BLOODRAVEN_DUMP_META.json` |

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
| Failure in Fencing after the barrier is allocated (peer stop timeout, archive wait, barrier commit refused) | No Job; settlement `Discarded` (markers ignored); no loader write-ahead, so the barrier is voided; fence released unless a damage record exists | barrier, run status | none |
| Crash before the loader write-ahead | Rebuilt on the next pass (existing) | — | none |
| Loader Job exists but its pod never starts (no loader deadline) | Stays `Restoring`; a new confirm is deferred and accept refused | run, damage record | fix the cause, foreground-delete the Job: `RestoreInPlaceJobLost`, then the exits (damage invariant) |
| Loader Job `Complete` with a second execution (`succeeded` ≠ 1 or `failed` > 0) | `Failed`/`RestoreInPlaceDuplicateLoader`; not Repaired | run, damage record | three exits |
| Crash or refusal during an accept | Resumes from `acceptPeersHeldAt`/`accept`; a refusal clears `acceptPeersHeldAt` and keeps the `Held` queue | damage record | re-annotate |
| `<pvcName>-gtids` ConfigMap missing or mismatched | Rebuilt from the claim by a result-read Job; the artifact stays `Verified` unless the claim fails authentication | artifact entry | none |
| Crash between the loader write-ahead and Create, or loader Job lost | `RestoreInPlaceJobLost`, never rebuilt (existing, `internal/controller/restore_inplace.go:766-771`); no evidence, so the damage record stays | run, damage record | three exits |
| **Any end of a loader after its write-ahead other than success** (pre-destruction refusal, never started, real Create refused, failed, killed, OOM, evicted, deleted, lost, deadline) | see below; a pre-destruction refusal also sets `RestoreInPlacePreDestructionRefused` and, on `ArtifactMismatch`, makes the artifact `Invalid`; confirm consumed | damage record | three exits (exit 1 reuses a `Verified` artifact) |
| Group deleted | Owner references delete Jobs and claims; holds on backups name the group UID and are released by dead-hold stripping (`internal/controller/backup_verification_hold.go:247-270` generalized), also when a same-name group is created | — | none |
| Operator upgrade mid-run | The new operator resumes from status. Avoid it: an upgrade during Restoring is already discouraged (#233) and staging makes runs hours longer | status | — |
| Operator downgrade mid-run | Unsupported, and unsafe below PR1: an older operator resets an unknown phase to no status (`internal/controller/restore_inplace.go:448-450`), and an older CRD prunes `status.restoreDamage`, so it can unfence a damaged primary. Operators from PR1 onwards treat an unknown phase as in flight: topology frozen, fence kept, no Job created or deleted, Warning `RestoreInPlaceUnknownPhase`. The upgrade-policy page says so | status | upgrade back |

**What a loader that did not succeed leaves behind.** Possibly not the old data on the primary: a DROP and a partial load are not undone, and a loader that changed nothing is treated the same way (see the cost under the damage invariant).

- Primary: every damage guard on (the damage invariant), independently of any run's phase and of whether `spec.restoreInPlace` still exists. `RestoreInPlaceDamagedPrimaryFenced=True`, `Ready=False`, alert.
- Peers: `Held` with pre-restore data and their IO threads stopped (#233), excluded from promotion and reader Services; or, after an unlogged load that #233 settled `Discarded`, replicating again from a fenced primary that accepts no writes.
- Sources and artifacts: every restore hold of the group and every `Verified` artifact are kept, so any earlier repair source stays usable.
- Exits, none automatic:
  1. **New confirm** (any full-instance restore). The run takes the record over in its acceptance write and, at Preflight, needs the execution-stopped proof. It reuses a matching artifact (minutes of re-authentication) or re-stages. Only its `Succeeded` clears the record.
  2. **Accept** with `restore-accept-damaged=<originRunID>`, the execution-stopped proof and no non-terminal run. The accept marker (PR2) makes the accepted dataset a PITR baseline once a backup carries it. The peers stay (or, after a `Discarded` settlement, become) `Held` with receivers verified stopped before the marker is committed (Annotations). Setting that queue to `Draining` reclones them from the accepted primary and destroys what may be the only intact copies: take a backup from a held site first. Resuming them without a reclone is unsafe after an accept.
  3. **Rebuild from a held peer**: a backup with `sourceSiteOverride` naming a held site (allowed while frozen, `internal/controller/backup_reconciler.go:1471-1484`) and `dumpScope: Instance`, then a full-instance restore from it, which is exit 1 with that source. Without `dumpScope: Instance` a profile with schema filters or `consistent: false` records a backup that full-instance acceptance refuses, after an hours-long dump. Manifest: `kind: MysqlBackup`, `spec: {failoverGroupRef: {name: orders}, profileName: nightly-s3, sourceSiteOverride: pdx, dumpScope: Instance}`.
- **Primary permanently down.** Exits 1 and 3 load into the damaged primary, so they need it running, and exit 2 must commit its marker on it. Do not accept: take a backup with `sourceSiteOverride` from a held site and `dumpScope: Instance` (as in exit 3) and restore it with `initFromBackup` into a new group, the supported procedure. If the lost machine's loader pod is still listed, it must be confirmed shut down or fenced from MySQL before its objects are removed (the damage invariant); that matters for any later exit on this group, not for the new one.

## GTID and archive lineage (Q3)

**Keep the live superset.** The live primary keeps its full `gtid_executed`, including the GTIDs of every discarded post-stop transaction, and the restored data is loaded unlogged (`skipBinlog:true`, forced for every full-instance in-place restore; explicit `false` is rejected). Peers inherit the set through CLONE. The DROPs stay binlogged. Consumers of the binlog outside `spec.sites` (an external async standby, which #233 already lists as not held, or a CDC pipeline) therefore receive the DROPs but not the reload, where today an omitted `loadOptions` gives them a binlogged load (`site/content/docs/7.backup-and-restore/2.backup-restore.md:639-643`). Known-limitations and the restore procedure say so.

**Why not RESET.** `RESET BINARY LOGS AND GTIDS` followed by `replace` would make the live set equal to the staged set, but the server would then reissue GTID numbers that already sit in the archive for the discarded transactions. The archiver's reuse detector (`internal/sidecar/binlog_reuse.go:115-154`) proves the overlap and records `Epoch.ResetReason = "verified GTID namespace reuse ... use a new manifest prefix and full backup"` (`:184-195`); the archive audit then errors for the site (`internal/sidecar/binlog_history.go:76-77`), retention stops (`internal/sidecar/binlog_archiver.go:378`, `:428`), and the planner refuses every window on that site (`cmd/bloodraven/pitr_window.go:115-116`). RESET would turn every in-place restore into a forced archive migration.

**In-band markers**, all empty transactions under explicitly assigned tagged GTIDs. Numbers come from `status.restoreMarkerSeq` and the `gtid_executed` maximum (Fencing step 4), so a number is never handed out twice in a group, even across lossy failovers or a barrier that was never committed. A run's barrier, completion and void share its `n` and the barrier's `server_uuid` (a void committed after a failover still uses that uuid); an accept takes a fresh number:

| Marker | Committed | Means |
|---|---|---|
| barrier `bloodraven_restore:<n>` | Fencing, after the application fence, before the peers stop and before any loader Job; archived before the Job when archiving is enabled | data may change without binlog events after this point |
| completion `bloodraven_restored:<n>` | Resuming, after the loader succeeded, before the fence release and every reclone | closing marker: the load finished; a snapshot after it holds the restored dataset |
| void `bloodraven_restore_void:<n>` | after a run ends before its loader write-ahead (Lifecycle step 8), on the current writable primary | barrier `n` changed nothing; the planner ignores it |
| accept `bloodraven_restore_accepted:<m>` | exit 2, on the damaged primary, before its fence release | closing marker: the administrator accepted what the primary holds as the new baseline |

Because they are in the GTID stream: every archive reader sees them in the per-file GTID coverage, including verification Jobs and other groups restoring from this archive; a backup's `gtidExecuted` says which side of each marker its snapshot is on, even when no other write happened in between (a GTID-set boundary such as `fencedGtidExecuted` alone is ambiguous in exactly that case); no clock comparison is needed. A backup that contains the barrier proves nothing about its data: a backup of the primary during the archive wait, or of a peer that caught up to the barrier and was then held, contains it and holds pre-restore data. Only a closing marker certifies a post-restore snapshot.

Rejected alternatives: reusing `Epoch.ResetReason` (blocks the whole site forever); a JSON barrier object in the prefix keyed by time or by `fencedGtidExecuted` (ambiguous at equality, written by a second writer into a single-writer layout); `AUTOMATIC:<tag>` with an empty transaction (MySQL assigns no GTID, so nothing is written); adopting "any unrecorded `bloodraven_restore` GTID" after a crash (it can pick up an earlier restore's barrier). Tagged GTIDs need MySQL 8.3+ (the 9.7 line is the only supported one, `site/content/docs/6.operations/10.upgrade-policy.md:24`); the operator already parses tagged GTID sets (`internal/mysql/gtid.go:10-12`). Tagged explicit `gtid_next` against `mysql:9.7` is **unverified in this repo** and is checked first in PR2. If it does not hold, PR2 stops and this section comes back for review; no fallback is designed here (a marker table would sit in a schema the full-instance DROP, dump and load all handle, which is its own design).

**Planner rules** (`planPITRArchive` and #206's replacement; one function used by restore, staging and verification). Let a barrier be *effective* if no void with its `(uuid, n)` appears in the base set or anywhere in the archive. Marker tags are recognized by tag only; their uuid is irrelevant to numbering.

1. **Base rule.** Let `n*` be the highest number of any effective barrier in the base backup's `gtidExecuted`. If `n*` exists and the base contains no closing marker numbered `n*` or higher, refuse `PITRRestoreBarrier` ("the backup was taken inside restore `n*`"). A later completion or accept closes earlier damaged barriers, because the dataset was replaced or accepted as a whole.
2. **Window rule.** Refuse if an effective barrier not in the base lies at or before the stop (in `P`, or anywhere in a staged file not wholly after the stop). A barrier after the stop (in `Q`) is harmless: `S ∩ Q = ∅` already proves it was not applied. So the supported new-group procedure from a pre-restore backup to a time before the restore still works, and so does an exit-1 re-stage to the same stop.
3. The rules run before `excludeGtids` is applied, and acceptance refuses `excludeGtids` that names any marker, so exclusion cannot bypass them.

**Runs that never complete.** A run that ended before its loader write-ahead gets its void, so readers lose no coverage. A run that reached its write-ahead and did not succeed keeps an effective barrier forever, even if its loader changed nothing (the cost under the damage invariant): PITR from older backups across that instant is refused, which is correct when the loader changed data the archive cannot replay and only over-refuses otherwise. A backup carrying a later completion or the accept marker becomes the new base, so PITR forward from the repaired or accepted dataset works again.

**Post-restore backup.** See Lifecycle step 9. Until it completes, `PITRArchiveReady=False/PITRRestoreBarrier` and an alert make the PITR gap visible.

## Interaction with the rest of the operator

The guards table under Lifecycle is the normative list; in prose:

- **Topology manager.** Not frozen during Staging/Staged/Preflight; emergency failover works. Frozen from Fencing through Resuming and while `status.restoreDamage` exists. #233's restore protection (start gate, re-stop, clone admission, promotion exclusion) is unchanged and applies from Fencing.
- **Planned failover and ordered updates.** Allowed during Staging/Staged because `BeginPlanned` stops treating those phases as a concurrent operation and the reconcile pass no longer ends at the restore (guards table); caught by the version and subset rechecks. Preflight waits for them (#233). A planned failover to a held site stays refused.
- **MysqlDatabase.** Not paused during Staging/Staged (account and password management keep working; the restore discards their effect on data, and accounts are not rewound). Paused from Preflight and while a damage record exists; once Preflight's 65-second quiesce has passed, the operator runs no DDL or account statement against the target until the restore ends or the record clears. The pause is a predicate read at the start of a reconcile with no drain, which is why the quiesce is needed. **Declarations are not rewound:** the `MysqlDatabase` objects and their status stay as they are while the schemas go back in time, so a schema created after the stop comes back empty on the next reconcile (`CREATE DATABASE IF NOT EXISTS`, `internal/controller/mysqldatabase_sql.go:171-172`), and a restored schema whose object was deleted since is unmanaged. The procedure requires a schema-to-object audit before applications reopen.
- **Backups and retention.** Scheduled backups continue during Staging. The source backup and its binlogs are held, and while a damage record exists so is every restore-held backup of the group. Backups record `dumpScope` and `consistent`; only `Instance`+consistent backups can source a full-instance restore or close a barrier. From Preflight, backups that would read the target wait and running ones are waited for; while a damage record exists scheduled backups end `Skipped` and explicit ones without an override wait (guards table).
- **Verification.** Unchanged, except that PITR verifications are subject to the barrier rules. The shared mysqld boot moves into a library both scripts source. Verification loads no accounts, so a window containing account statements fails there too; that is a separate fix.
- **Encryption.** Inputs are decrypted with the #203 revision. The intermediate artifact is plaintext on the claim (Q1 A). Live `encryptionAtRest` is unaffected: the logical load recreates `ENCRYPTION='Y'` tables under the live keyring, which Preflight, Fencing and the loader check is active before the DROP when the artifact has encrypted tables.
- **Dragonfly.** Not rewound. A co-managed Dragonfly holds non-durable cache and session state (`api/v1alpha1/dragonfly_types.go:8-12`) that may be derived from discarded writes. The operator does not flush it; the procedure requires application-specific cache and session reconciliation (or a flush the application tolerates) before traffic resumes.
- **Deployment API.** `operations.restoreInPlace` (`site/content/docs/4.configuration/11.deployment-api.md:132`, `:153`) reports `Staging` and `Staged`; leases are refused with `RestoreInPlace` for the whole run (`DeploymentUnstableReason`, `internal/controller/deployment_leases.go:295-296`; `ConcurrentOperation` is `BeginPlanned`'s reason, `:732-735`), with `RestoreReclonePending` while peers are queued or held (#233), and with `RestoreDamagedPrimary`, which takes precedence over every reason except `DeploymentRevocationPending`, while a damage record exists.
- **Bootstrap restores.** `initFromBackup` gains the source hold (PR3) and writes no barrier. A new group can still inherit a barrier: fresh-target PITR installs the source GTID baseline (`internal/controller/restore.go:931-959`), and the planner may have honoured a void that sits elsewhere in the source archive, outside the replay window. The new group's own backups would then carry the barrier without the void and fail the base rule. So after a successful bootstrap whose restored `gtid_executed` contains a barrier, the operator commits on the new primary, before the group's first backup, every void the planner consulted for those barriers (same `<uuid>:bloodraven_restore_void:<n>`, explicit `GTID_NEXT`, idempotent; #206's plan lists them in `coverage.json`). Only that case needs `TRANSACTION_GTID_TAG` on a bootstrap.

## Security

- **Plaintext at rest.** The staging claim holds decrypted inputs (until step j), the staging datadir (until step m) and, for a retained artifact, a plaintext logical dump of the whole instance. Encrypting only the intermediate dump would protect nothing while the datadir sits next to it during the run. Docs recommend an encrypted, replicated StorageClass and namespace RBAC that limits who can mount claims. Retained artifacts are listed in status, counted in a gauge and alerted on.
- **Keyring.** The staging keyring is ephemeral (`/run/mysqld` emptyDir) and dies with the pod; the datadir it protects is deleted before the pod exits.
- **Loopback root and loaded accounts.** Passwordless `root@127.0.0.1` and the dump's accounts (locked after the load) are reachable only from containers in the same pod, all of which are operator-built. No Service, no token, no host network, mysqlx off. Pinned by a unit test of the Job spec and the script flags.
- **Supply chain.** The probe pod runs the candidate (no volumes, no token, nothing to read but its own binaries); every later staging-side and loader pod runs the pinned `imageRef` (`<repo>@sha256:<digest>`), and the loader runs the artifact's, so the mysqlsh that loads the artifact is the binary that wrote it. A bare runtime ID is never used as an image; without a pullable digest reference the run falls back to `spec.backup.image` or refuses before any claim (`StagingImageNotPinned`).

## Observability

- **Structured logs** (camelCase fields; `site/content/docs/8.observability/7.log-schema.md` updated in the PR that adds each): INFO `restore staging started`, INFO `restore staging job created`, WARN `restore staging volume is node-local`, WARN `restore staging refused` (`reason`), INFO `restore staging verified` (`runID, manifestSha256, stagedGtidSha256, bytes`), INFO `restore staging artifact reused`, INFO `restore staging artifact deleted`, INFO `restore barrier written` (`runID, site, gtid`), INFO `restore barrier archived`, INFO `restore completion written` (`runID, site, gtid`), INFO `restore barrier voided` (`runID, gtid`), INFO `restore barrier closed` (`backup`), ERROR `post-restore backup failed`, WARN `restore request changed; run superseded` (`runID, requestSHA256`), ERROR `in-place restore left the primary fenced as possibly damaged` (`originRunID, runID, site, jobName`), INFO `in-place restore loader refused before destruction` (`runID, jobName, reason`), INFO `damaged-primary record cleared` (`originRunID, runID, proof`), INFO `restore accept marker written` (`originRunID, gtid`), INFO `damaged-primary record taken over` (`originRunID, runID`), INFO `scheduled backup skipped: damaged primary`, WARN `in-place restore loader ran twice` (`runID, jobName, succeeded, failed`), INFO `staging gtids configmap rebuilt` (`runID, pvcName`), WARN `event workers killed under the restore fence` (`site, count`), WARN `unknown in-place restore phase; holding guards`. The pre-destruction refusal message carries the sentinel reason as `detail`. No existing `msg` changes.
- **Alerts** (documented in `8.observability/2.monitoring.md` and `5.alert-runbook-map.md`): `BloodravenRestoreDamagedPrimaryFenced` (critical, `bloodraven_restore_damaged_primary_fenced == 1 unless on(namespace, group) bloodraven_restore_in_place_loader_started_timestamp_seconds`, for 5 minutes: the gauge stays 1 for the record's whole life, but a healthy loader does not page), `BloodravenRestoreStagingRetained` (warning, retained artifacts for more than 24 h), `BloodravenPITRRestoreBarrierOpen` (warning, open for more than 6 h), `BloodravenRestoreInPlaceLoaderSlow` (warning, `time() - bloodraven_restore_in_place_loader_started_timestamp_seconds > 2 * bloodraven_restore_in_place_loader_estimate_seconds` where the estimate is above 0; PITR runs only, since only they have a budget).
- **bloodraven-doctor** (maintenance contract): `SKILL.md` (staging phases, damaged fence, barrier), `references/alert_runbook_matrix.md` (every new reason, Event and alert; `RestoreInPlaceRejected` row at `:62` rewritten), `references/troubleshooting_playbooks.md` (playbook 19 rewritten from "pointInTime rejected" to the in-place PITR procedure; new playbooks for a damaged fence and its three exits, an open barrier, a retained artifact, `StagedGtidsNotInLive` split by `D ∩ B`, `StagingVersionMismatch`, a permanently down damaged primary), `scripts/triage.sh` (staging status, artifact list, `status.restoreDamage`, barriers and their markers), `scripts/deployment-probe.sh` (new phases), `scripts/sidecar-probe.sh` (`/archiver/covers`), `scripts/support-bundle.sh` (staging and probe Job logs, claims). New playbooks also cover a loader that never starts (foreground Job deletion, then the exits) and `RestoreInPlaceDuplicateLoader`.

## Docs that change when implemented

| Page | Change |
|---|---|
| `site/content/docs/7.backup-and-restore/2.backup-restore.md` | Rewrite "Point-in-time recovery is not supported in place" (`:971-999`) into the in-place PITR procedure with the supported-procedure statement first; full-instance phases, `skipBinlog` forced and `excludeSchemas` refused (also in the phases list, `:624-650`), accounts not rewound, the source-scope requirement, the damage invariant and exits (including foreground deletion of a loader that never starts), markers and post-restore backup, sizing formula, loader budget; before reopening: Dragonfly cache and session reconciliation and a schema-to-`MysqlDatabase` audit |
| `site/content/docs/3.get-started/1.known-limitations.md` | Replace "in-place cannot PITR" (`:116-121`) with the remaining limits: full-instance only, full-instance in-place restores only from an `Instance`+consistent `mysqlBackupRef` (raw S3/PVC sources only per-schema), no foreign source, accounts not rewound, plaintext claim, external binlog consumers see the DROPs but not the reload |
| `site/content/docs/4.configuration/2.crd-reference.md` | `RestoreInPlaceSpec.staging`, new types, phases, status fields, conditions; `MysqlBackup` `spec.dumpScope`, `status.dumpScope`, `status.consistent`, `status.serverVersion`, `status.mysqlshVersion`, phase `Skipped` |
| `site/content/docs/6.operations/3.kubectl-plugin.md` | `restore-staging list`, `restore-staging cleanup` and `restore accept-damaged` |
| `site/content/docs/7.backup-and-restore/5.backup-encryption.md` | Staging decryption with the #203 revision; plaintext claim |
| `site/content/docs/7.backup-and-restore/6.backup-verification.md` | `PITRRestoreBarrier` refusal |
| `site/content/docs/5.architecture/3.multi-cluster-dr.md`, `6.durability-and-rpo.md` | Keep new-group PITR as the primary procedure; barrier and RPO gap |
| `site/content/docs/8.observability/2.monitoring.md`, `5.alert-runbook-map.md`, `7.log-schema.md` | Events, metrics, alerts, log messages |
| `site/content/docs/4.configuration/11.deployment-api.md` | New phase values; lease reason `RestoreInPlace` during a run and `RestoreDamagedPrimary` (with its precedence) |
| `site/content/docs/6.operations/10.upgrade-policy.md`, `9.failure-mode-matrix.md`, `4.configuration/10.production-hardening.md` | No up/downgrade mid-run, and no downgrade below PR1 while a damage record exists; on Kubernetes 1.27-1.29, remove a stored `restoreInPlace` with `pointInTime` and no `staging` before upgrading (no CRD ratcheting); damaged fence; `RELOAD` and `TRANSACTION_GTID_TAG`; encrypted StorageClass |
| `playground/chaos-scenarios.md` | Sections for scenarios 63-67 |

## Test plan

**DST.** `internal/dst` has no restore model today: it drives the real `TopologyManager` with a fake clock and in-memory MySQL, and has no restore, backup or in-place controller in its model at all; its README lists CLONE/bootstrap among the explicit exclusions (`internal/dst/README.md:138-143`; #233 "Not run"). It cannot test staging, which is controller and Job work. PR1 adds the one piece where DST is the right tool: a restore-guard actor that applies the guard surfaces (topology freeze, primary fence, pending/held set, damage record) at random points of the fault schedule. The harness lives in package `dst` and drives `controller.TopologyManager` (`internal/dst/harness.go:57`), while `applyRestoreProtection` is an unexported runner method (`internal/controller/restore_peer_replication.go:548`). PR1 therefore splits it: an exported pure `RestoreProtectionFor(fg)` computes the protection from status, and an exported `(*TopologyManager).ApplyRestoreProtection` applies it through the existing exported setters (`SetTopologyFrozen`, `topology.go:2947`; `SetRestoreOfflineFence`, `restore_offline_fence.go:154`; `SetRestoreReclonePending`/`SetRestoreRecloneBlocked`, `restore_reclone_promotion.go:29`, `:50`). The runner and the DST actor both call them. New invariants: no promotion of a pending or held site; no `START REPLICA` on a held peer; no failover while frozen; a primary with a damage record is never unfenced except by an exit event. Fixed findings are pinned in `test/component` as usual.

**Unit (`internal/controller`, `cmd/bloodraven`).**
- Flip `TestReconcileInPlaceRestore_PointInTimeRejected` (`internal/controller/restore_inplace_test.go:1150`) into acceptance into `Staging`, and add acceptance-pass rejection tests: for every full-instance restore `excludeSchemas`, `skipBinlog:false`, an S3/PVC source, and a backup that is not `Succeeded`, has no location, is `Filtered`, is not consistent, has no scope or no measured versions recorded, or has an empty or unparseable `gtidExecuted`; a per-schema restore while a damage record exists; for PITR `allowPartial`, `allowUnverifiedCrossSite`, `includeSchemas`, `loadIndexes:false`, a stop before the backup's completion, `excludeGtids` intersecting the bound or naming a marker, and a bound not in live history. Backup side: `dumpScope`/`consistent` taken from the sentinel, not the profile (edit the profile after the backup), `serverVersion`/`mysqlshVersion` parsed from the sentinel, and `spec.dumpScope: Instance` overriding a filtered profile.
- Keep `TestBuildInPlaceRestoreJob_RefusesPointInTime` (`:1236`) as a loader invariant (the loader never replays), and `TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline` (`internal/controller/restore_test.go:537`).
- Snapshot and identity: the snapshot holds every deferred field and is read after a restart while the spec has changed; a missing or altered snapshot ends `RestoreInPlaceSnapshotLost`; canonical JSON golden files; `GTIDSet.Canonical()` equivalence fixtures (`u:1:2` = `u:1-2`, overlaps, duplicates, tag order); every row of the identity table in every pre-Job phase (confirm-only bump rebinds; `stopDatetime`, `excludeGtids`, source, backup UID, a deleted backup, `decryption.secret`, `decryption.key`, `sanityCheck`, image or kind edits supersede; confirm regression plus identity change cancels); a loader is never built from the current spec; a profile storage edit (bucket, endpoint, credentials, PVC claim) between a lost staging Job and its re-creation does not reach the new Job. `versionGate`: an unchanged key blocks the restart, a changed source backup, image, target version or confirm re-evaluates.
- Damage invariant and guards: one test per cell, including a takeover run in `Staging` and `Preflight` with every guard on; Repaired and Accepted positive and negative (Accepted refused while a non-terminal labelled pod or unfinished labelled Job exists, or a lost-node pod without confirmed shutdown); a loader that never starts, then foreground deletion, then each exit; an exit-70 loader, a loader whose main container never started, a terminal Job with no pods, and a refused real Create all leave the record; no clear by settlement, timeout or unreachable MySQL; a per-schema `Succeeded` cannot happen while a record exists; restore holds of three successive takeover runs all kept until the record clears.
- Staging Job builder: pinned flags, no token, no host network, no Service, AppArmor paths, claim mount, revision Secret, deadline, `mysqlConf` denylist; init order (`staging-prepare` before the downloads; re-auth and result-read never clear); probe Job (four binaries; deadline, pull failure and eviction are lost Jobs); probe deadline 720 s with the 120-second in-container timeout; image selection matrix (override by tag and by digest, live digest reference, live bare ID skipped to the backup image, live without mysqlsh, live too old, fallback mismatch, target changed between staging and Fencing) and an imported-image fixture (k3d/kind, probe `imageID` `sha256:<id>`) asserting `imageRef` is never `repo@<runtime ID>` and the run falls back or ends `StagingImageNotPinned`, that the probe pod runs `imageCandidate` and every later pod spec uses `imageRef`; staging step a refusing an `@.json` whose versions differ from the backup's status.
- Phase machine: every row of the crash table; pod-gone proof before each re-creation (a terminating pod blocks it); `DisruptionTarget` counted as lost; WaitForFirstConsumer ordering and the durable provisioning timeout across a restart; result channel (log missing, truncated, digest mismatch, result-read Job); `versionGate` preventing a restart loop; every refusal's confirm consumption.
- Artifacts: the lifecycle table; reuse decided after the probe; reuse mounting `<originRunID>/artifact`; replacement deferred while a damage record exists and while the replacement has not passed Fencing; cancel of a Reuse run leaves the artifact `Verified`; `Invalid` only on a digest mismatch (including a loader `ArtifactMismatch` refusal) and deleted even under a damage record; a missing or mismatched gtids ConfigMap rebuilt by a result-read Job, also while damaged, with the artifact kept `Verified`; a slot needed only for Build; reuse skips an artifact on a missing, NotReady or cordoned node; the gtids ConfigMap checked against both digests; UID-conditional deletes; direct claim deletion.
- Damage lifecycle: write-ahead in the same update as `jobCreating`; kill, OOM, deadline, eviction, deleted or missing pod and JobLost all leave it; a takeover run's Staging failure, Preflight refusal, loader refusal, cancellation and supersession all leave it; accept refused for a stale runID; accept after a Discarded settlement with a peer receiver still running arms a Held queue, never commits the marker before every peer is verified stopped, and refuses after 2 minutes; the dry-run Create refusal ends the run before any record. Application fence: crash between recording the priors and the `SET`s; takeover carry-over (R1 prior scheduler `ON`, R2 succeeds, scheduler ends `ON`); Poll re-assertion after a mysqld restart, also with only a damage record present; an event worker under the operator's own account killed; a worker that survives 60 s refuses before the barrier. Loader budget: zero and unknown sizes, zero-duration hashing, saturation, and the deadline-bounded pre-destruction time.
- Markers: allocation from `restoreMarkerSeq` and `gtid_executed` (Opus round-2 sequence: an uncommitted barrier, a later run, and a stale void never collide); idempotent retry from `Writing` and `Written`; void on the current primary after a failover; void for a never-committed barrier; completion before queue release; accept marker before the accept release; settlement `Discarded` with a void or accept marker present, across a restart; closure only by an `Instance`+consistent backup carrying a closing marker numbered `n` or higher; the status size budget with every list at its maximum.
- Status writers and the reconcile pass: for each new top-level field and the damage condition, a stale `updateCRStatus` write after a reconciler write leaves the field intact; `Ready=False/RestoreDamagedPrimary` with every peer replicating after a `Discarded` settlement; a write lowering `restoreMarkerSeq` rejected (envtest); a fresh planned-failover annotation during `Staging` accepted and completed, and an earlier run's `Draining` queue drained, on passes that also drive the restore.
- Loader execution and Repaired: `succeeded > 0` without `JobComplete` does not reach `Resuming`; `JobComplete` with `succeeded=2` or `failed=1` ends `RestoreInPlaceDuplicateLoader` with the record kept; two pods of one Job, the second refused `ExecutionClaimHeld` while the first holds the lock (script test with a stub lock); a labelled pod still running after `JobComplete` blocks the completion marker; PR1-only build: Repaired and Accepted clear the record with no marker, and a successful non-PITR full-instance restore leaves no record.
- Accept: a refusal clears `acceptPeersHeldAt` and a new annotation waits a fresh 2 minutes; an operator restart after the accept marker is recorded resumes it without allocating another; accept refused while a takeover run is in `Staging`, and accepted after it is cancelled; a new confirm waits while an accept is in progress.
- Backups: a scheduled backup during a damage record ends `Skipped` with no Job, the schedule rollup shows it and no retry follows; an explicit backup without override waits; one with `sourceSiteOverride` runs; from Preflight a backup sourced on the target waits, and Preflight waits for a running one (operator-credential fallback); the post-restore backup carries `sourceSiteOverride`, and a `Succeeded` one without a closing marker emits `PostRestoreBackupFailed`; encrypted backups: `dumpScope`, `consistent` and both versions go sentinel to `BLOODRAVEN_DUMP_META.json` to `encrypt-upload` to status to acceptance and barrier closure.
- Fences and quiesce: Preflight does not exit before 65 s after `preflightSince`, with a MysqlDatabase reconcile suspended after its `groupFenced` check; a mysqld restart after the Fencing drain with a long-running event: the next Poll kills the worker and the loader refuses `EventWorkersRunning` while one runs.
- Staging result and budget: `encryptedTables` true for an unencrypted source whose replay creates an `ENCRYPTION='Y'` table; the result sentinel carries the verdict and every certification field, and a `restore.py` verdict never overwrites it; certification golden file excludes `certificationSHA256`, diagnostics and `loaderHashBytesPerSecond`, and includes `sourceBytes`; a buffer-pool key in `mysqlConf` overridden by the resources value; a near-96 KiB `excludeGtids` passes to `mysqlbinlog`, a larger one is refused at acceptance; re-auth and result-read deadlines include 1,800 s; a completed probe Job does not block the staging Job; no pre-destruction deadline without `D`; a deadline refusal lowers `loaderHashBytesPerSecond` and the next Preflight refuses or fits; the size Warning from `sourceBytes` when `sizeBytes` is 0.
- Barriers: `loaderWriteAhead` decides voiding after run status is replaced by a later run or a failure; eviction of 32 `Archived` closed-out entries; 32 open entries refuse `RestoreBarrierSlotsFull`; `PITRArchiveReady` precedence for every pair of reasons, from either runner writer; a bootstrap whose resolving void is only in the source archive commits that void and its first backup passes the base rule.
- Holds and plugin: a same-name successor group neither keeps nor is protected by its predecessor's holds; plugin dispatch of both commands, `--run` required, exact annotation keys and values (pattern of `cmd/kubectl-bloodraven/reclone_test.go`).
- Planner (`cmd/bloodraven`): base rule and window rule fixtures, including a base inside restore `n`, a base after a later completion, a base after an accept, a barrier after the stop, a voided barrier, and the `excludeGtids` bypass attempt.
- Scripts under python3/bash with stub mysqlsh (pattern of `internal/controller/restore_script_offline_test.go`, `pitr_restore_script_test.go`): exact staged-GTID proof cases, `X ∩ B` refusal, sentinel and size bounds, manifest writer, `_verify_staging_artifact`, and that `_refuse_before_destruction` is unreachable once the destruction flag is set and that no refusal before it exits with any other code.

**Component (`test/component`).** Real `TopologyManager` with `FakeReplica` (#233's harness): failover and planned failover during Staging leave the run unfrozen and rebinding at Fencing; held peers plus the damage record through operator restarts; takeover by a new run without an unfenced gap; accept-and-release; the stale runner write against the damage record with a real runner.

**Sidecar (`internal/sidecar`).** `GET /archiver/covers`: a set only in an unsealed (current) file is not covered; a sealed file covering it is; a set split across a sealed and an unsealed file, or partly in no file, is not; an archive write or listing failure returns an error, never `covered`.

**Envtest (`test/envtest`).** CEL pairing rule and enum rejection; status round-trip for every new type; claim owner references are set (envtest runs no garbage collector, `test/envtest/suite_test.go:42-50`, so cascading deletion is tested in a playground scenario); the damage record, artifact list and barriers surviving reconciler restarts; restore holds blocking retention and cleanup.

**Real MySQL, opt-in** (pattern of `TestRestoreFence_RealMySQL`, `internal/mysql/restore_fence_mysql_test.go:20`, and `BLOODRAVEN_PITR_NATIVE_TEST`, `internal/controller/pitr_restore_script_test.go:323`): `BLOODRAVEN_STAGING_NATIVE_TEST=1` runs `staging.sh` against a native mysqld/mysqlsh/mysqlbinlog with a generated dump and binlogs, asserting the proof, account loading with an `ALTER USER` in the window, the read-only sanity check, that the artifact `@.json` equals the proven set, and the source `@.json` version keys; `BLOODRAVEN_TEST_MYSQL_ADMIN_DSN` covers the three explicit tagged markers (an `AUTOMATIC:<tag>` empty transaction asserted to log nothing), the privilege set and `RELOAD`, the loader's exit code 70 through `mysqlsh --py -f`, and the scheduler fence: an event sleeping inside its body when `event_scheduler=OFF` is set is still listed as `thread/sql/event_worker` until killed, and none remains after the drain; after a mysqld restart with the scheduler configured `ON`, the Poll re-assertion kills a running worker; and the loader's execution claim (`GET_LOCK` on the mysqlsh global session) stays held through `util.load_dump` and refuses a second session.

**Playground scenarios** (IDs after #233's 62):

| ID | Injects | Asserts | Profile |
|---|---|---|---|
| `63-in-place-pitr-restores-to-stop-time` | marker rows before the stop, rows and a dropped table after it, an `ALTER USER` in the window, an event writing every second, and an event (definer: the operator account) already running a long `SLEEP` loop when Fencing begins | `Succeeded`; no event write and no event worker between the barrier and the fence release; `event_scheduler` back to its prior value after release; pre-stop rows present, post-stop rows absent, dropped table back; peers recloned and replicating; staging claim deleted; completion marker on every peer; barrier closed by the post-restore backup; a PITR verification from the pre-restore backup across the restore is refused `PITRRestoreBarrier`, one from the post-restore backup passes; deleting the group removes the claims | **release** |
| `64-in-place-pitr-fails-after-drop-keeps-fence` | deletes the loader pod once the user schemas are gone; then a new confirm whose loader pod is deleted before its DROP, then another | `Failed`; `RestoreInPlaceDamagedPrimaryFenced=True`; primary Service has no endpoints, `offline_mode=ON`, topology frozen; queue `Held`, markers on every peer; artifact retained; the second run (no user schemas left, so nothing logged) also ends with the fence up; the third reuses the artifact (`mode=Reuse`) and ends `Succeeded` | **release** |
| `65-in-place-pitr-planned-failover-during-staging` | planned failover while `Staging` | staging continues; target bound to the new primary at Fencing; `Succeeded` | full |
| `66-in-place-pitr-operator-restart-each-phase` | operator pod deleted in Staging, Staged, Fencing (between barrier write-ahead and commit), Restoring | no duplicate Job or claim, one barrier GTID per run, no unfenced gap, `Succeeded` | full |
| `67-in-place-pitr-cross-site-window` | failback A to B to A inside the window (needs #206) | every site's transactions present; staged proof equality | full |

Both release scenarios reset MySQL first. #233's scenario 62 moves to `release` in PR1, because the damage fence otherwise has no CI signal (#236). Its cleanup today removes the spec, the Jobs and `status.restoreReclones` (`internal/playground/scenarios/s62_full_restore_fails_after_drop.go:363-401`), which leaves a damage record behind. PR1 changes it to leave through a supported exit: foreground-delete the loader Job, set `restore-accept-damaged`, wait for the record to clear, then set the `Held` queue to `Draining` (resuming without a reclone is unsafe after an accept). PR1 also adds `restoreDamage`, `restoreBarriers`, `restoreStagingArtifacts`, `restoreOfflineFence` and `restoreReclones` to the destructive reset's status clear (`mfgStatusFieldsToClear`, `cmd/playground-chaos/reset.go:365-371`), which runs with the operator scaled down and storage wiped (`:110-127`), and keeps `restoreMarkerSeq` while the archive prefix survives so marker numbers are not reused there. A test resets a damaged group and asserts a healthy, unfenced baseline.

## Phased implementation plan

Each PR passes the AGENTS.md pre-PR gate on its own and ships its own docs, log-schema and doctor updates. `pointInTime` stays rejected for real runs until PR6; PR5 accepts it only with its hidden flag on, and then stops after `Staged`. PR1-PR3 change whichever API version `main` serves when they merge (`api/v1alpha1` today, `api/v1alpha1/groupversion_info.go:9`); every field they add is optional, so the v1beta1 migration carries them unchanged whichever lands first. PR4 onward targets v1beta1 only. The damaged-primary fence (PR1) is tracked in [#241](https://github.com/ShipStream/bloodraven/issues/241) and gets its own design review when it is implemented; this proposal fixes its requirements, not its final code shape.

| PR | Scope | Depends on | Size |
|---|---|---|---|
| PR1 | The damage invariant for every full-instance restore: `status.restoreDamage` written with the loader write-ahead; the two clearing proofs without their marker conditions (Repaired: `JobComplete`, counters `succeeded=1, failed=0`, execution-stopped proof; Accepted: annotation, execution-stopped proof, no non-terminal run, peers held); the execution claim and `RestoreInPlaceDuplicateLoader`; the dry-run Create before the write-ahead; `restore.py` pre-destruction refusal path and exit code (diagnosis only, event-worker check included); `shipstream.io/restore-loader` label and the label-based execution-stopped proof; takeover by any full-instance confirm; per-schema refusal while damaged; accept (resumable, with the peers Held and verified stopped after a Discarded settlement) and plugin command; `event_scheduler` in the fence record (prior value, carry-over, Poll re-assertion with the worker kill, release) and the event-worker drain; guards (`inPlaceRestoreMutating`, `restoreDamagePresent`, both fence predicates, `BeginPlanned`, `groupFenced` and the Preflight quiesce, lease reason and precedence, backup admission and `Skipped`, all group restore holds once PR3 lands); the reconcile pass continuing during `Staging`/`Staged`; runner allowlist and `Ready` override; condition, gauges, alerts, unknown-phase fail-safe, DST restore-guard actor, scenario 62 to release with its cleanup and the reset change | #233 (merged) | L (~3k lines with tests) |
| PR2 | Markers (barrier, completion, void, accept; `restoreMarkerSeq` with its transition rule, `loaderWriteAhead`, explicit tagged GTIDs, write-ahead, both flushes, barrier slots), and with them the marker conditions of both clearing proofs (completion before Repaired, accept marker before Accepted); settlement ignoring marker tags; archive wait and sidecar `/archiver/covers`; planner base and window rules; bootstrap void copy; `MysqlBackup` `status.dumpScope`/`consistent`/`serverVersion`/`mysqlshVersion` (also through `BLOODRAVEN_DUMP_META.json` and `encrypt-upload`) and `spec.dumpScope`; automatic post-restore backup with its source override; `PITRArchiveReady` reason and precedence; `RELOAD` and `TRANSACTION_GTID_TAG` preflight (SUPER no longer covering them), and for every full-instance restore: forced `skipBinlog:true`, refused `excludeSchemas`, `mysqlBackupRef` with an `Instance`+consistent, `Succeeded` backup with a parseable bound | PR1; coordinate with #206 so its planner keeps the rules | L |
| PR3 | Restore holds on source `MysqlBackup`s for bootstrap and in-place restores, generalizing #232 (fixes today's gap where retention can prune a backup a restore is reading), all of the group's kept while a damage record exists | none (the damage clause after PR1) | S-M |
| PR4 | API: `RestoreStagingSpec`, status types, phases, CEL, generated CRDs copied to the chart, CRD reference; reconcile-time rejection unchanged | v1beta1 types merged | S |
| PR5 | Staging: execution snapshot, request identity and `GTIDSet.Canonical()`, probe Job, reuse decision, claim, staging Job and `staging.sh`, proof, manifest, result channel, gtids ConfigMap, Staged certification, artifact lifecycle and cleanup, re-auth and result-read Jobs, gtids ConfigMap rebuild, `persistentvolumes` RBAC and chart mirror. Behind a hidden operator flag `--experimental-in-place-pitr-staging` (off by default); with it on, a run stops at `Staged` and ends `Failed`/`RestoreInPlaceStagingOnly`. No API dry-run mode: rehearsal is what `MysqlBackupVerification` with `pointInTime` is for | #206, #203, PR3, PR4 | L |
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

- **Q10:** "possibly damaged" is decided by the damage invariant (record written with the loader's `jobCreating`, cleared only by the Repaired or Accepted proof), not by #233's GTID comparison, which cannot see an unlogged load. #233 settlement still decides the peers. Refinements: "Discarded releases as today" becomes "Discarded releases only when no record remains"; exit 1 is open to any full-instance restore and per-schema restores are refused while damaged; exit 2's proof is label-based and (PR2) adds an accept marker; exit 3 is exit 1 with a held-site source. Q10's "proof the failed Job's pods are gone" is the execution-stopped proof, which gates exits and Repaired and never clears alone.
- **Q3:** the barrier is joined by a completion marker and an accept marker (closing markers, which certify a new baseline) and a void marker (returns coverage for runs that ended before their loader write-ahead), numbered from a never-decreasing group sequence. The automatic post-restore backup is forced to instance scope. These refine how the barrier is enforced; none removes it.
- **Q7:** the "immediately before the DROP" check is made by the operator immediately before the loader write-ahead, so its refusal leaves the confirm unconsumed as Q7 requires without creating a second destructive Job; the loader's own in-Job check is an extra guard and, because a Job ran, consumes the confirm.
- **Q9:** "deleted only after the replacement is verified" means after the replacement passes the Fencing rechecks, and not at all while a damage record exists.

Decisions taken in this proposal that the debate did not settle (for review):

1. `staged set ⊄ live gtid_executed`: **refuse** (`StagedGtidsNotInLive`), not `gtid_purged='+diff'`; therefore foreign-source in-place PITR is always refused, and `D ∩ B` can only be fixed with another backup or a new group.
2. Markers are **in-band tagged GTIDs** under explicitly assigned, write-ahead-recorded numbers from a never-decreasing group sequence (`restoreMarkerSeq`); they need `RELOAD` and `TRANSACTION_GTID_TAG` for all full-instance restores, and #233 settlement ignores them.
3. For **all** full-instance in-place restores, not only PITR ones: `skipBinlog:true` forced, `excludeSchemas` refused, and the source must be a `mysqlBackupRef` whose backup recorded `dumpScope=Instance`, `consistent=true`, a parseable bound and a location (raw S3/PVC sources become per-schema-only). Reading the scope from the source `@.json` was rejected: which options it records on 9.7 is unverified.
4. New deployment leases stay refused during Staging/Staged (existing behaviour), while `BeginPlanned` stops treating those phases as a concurrent operation and MysqlDatabase keeps reconciling.
5. One damage invariant with two clearing proofs (Repaired, Accepted) overrides every phase rule; it protects every restore hold of the group and every `Verified` artifact, and execution-stopped proof is label-based (no Job list in status). A lost machine counts as stopped only after confirmed shutdown or external fencing.
6. Phases `Staging` and `Staged`; a probe Job for image selection; reuse decided after the probe and re-authenticated by a separate Job before Fencing; in-place PITR needs a `mysqlBackupRef` source.
7. Staging-side Job loss (including eviction and an unreadable result) is re-created automatically after a pod-gone proof, at most 3 lost Jobs per run (no damage possible); staging Job failure is terminal.
8. The PITR loader gets its own optional deadline, unset by default, plus, only when it is set, an in-script pre-destruction deadline bounded by it; Preflight refuses when the deadline cannot cover the pre-destruction time plus 1.5x the load estimate (a heuristic from measured source bytes).
9. The complete accepted request lives in an immutable snapshot ConfigMap read on every step; a request-identity change interrupts any run before its loader write-ahead; a confirm-only bump rebinds, as today.
10. Every refusal after the acceptance pass consumes the confirm, except the pre-Job version refusals (Q7), which a `versionGate` keeps from looping.
11. Staging loads the dump's accounts (then locks them) so account statements in the window replay; accounts on the live primary are not rewound.
12. PR5 lands behind a hidden operator flag rather than an API dry-run mode.
13. Staging data on the claim under `<runID>/`, created by the Job, so root-owned local-path mounts work if they are writable by uid 27.
14. For every full-instance restore the live `event_scheduler` is part of the fence record exactly like `offline_mode` (prior recorded before the `SET`, carried across takeovers, re-asserted every Poll while the fence or a damage record exists, with any event worker killed again, restored at release), and running event workers are killed and proven gone before the barrier; `SET PERSIST` rejected. The staging mysqld gets the target's `mysqlConf` minus a pinned denylist.
15. Full GTID sets stay out of status: the artifact's `B` and `S` live in its manifest and an immutable ConfigMap, each at most 256 KiB, under a stated status budget.
16. **Scope narrowed (program rule: Major findings on the damage area in three consecutive rounds).** The round-2 proofs "own pre-destruction refusal" (exit 70) and "never started" are removed; only Repaired and Accepted clear a record, and only runs that ended before their loader write-ahead are voided. Cost: a loader that changed nothing still leaves the primary fenced until exit 1 (minutes, with artifact reuse) or exit 2; operator-side checks and a dry-run Create keep it rare. Automatic release after a proven pre-destruction refusal is follow-up [#240](https://github.com/ShipStream/bloodraven/issues/240).
17. Accept after a `Discarded` settlement holds every peer and verifies its receiver stopped (#233's stop, bounded at 2 minutes) before the accept marker is committed.
18. Images: a runtime `imageID` is identity only; the probe runs the candidate and every later pod runs a validated `<repo>@sha256:<digest>` `imageRef`, else the run falls back to `spec.backup.image` or refuses before any claim. Source versions come from `MysqlBackup` status measured by the backup Job and are checked against the downloaded `@.json`.
19. **Ultra-review round (49 confirmed findings).** Repaired needs the loader Job's `JobComplete` condition, counters `succeeded=1, failed=0` and the execution-stopped proof, because one Job can run its program twice; the loader also takes a per-run `GET_LOCK` execution claim. A second execution keeps the record (`RestoreInPlaceDuplicateLoader`).
20. PR1 ships both clearing proofs without their marker conditions, and PR2 adds the markers together with those conditions, so neither PR ships a proof it cannot satisfy and the fence fix does not wait on the unverified tagged-GTID question. The damaged-primary fence (PR1) is tracked in [#241](https://github.com/ShipStream/bloodraven/issues/241) and gets its own design review when it is implemented.
21. Every new reconciler-owned status field joins the topology runner's preserved allowlist, and the damage condition joins `reconcilerOwnedConditions`; `restoreMarkerSeq` also carries a CEL `self >= oldSelf` rule. `Ready` gets an explicit damage override.
22. `Staging`/`Staged` do not end the reconcile pass, so planned-failover intake and reclone housekeeping keep running for hours of staging.
23. Backups are coordinated by source, not by credentials: from Preflight, backups that would read the target wait and running ones are waited for; while damaged, scheduled backups end in a new `Skipped` phase. The post-restore backup is pinned to the target site.
24. Accept is refused while any in-place run is non-terminal, and it is a resumable sub-state machine. The event scheduler fence kills workers again after a mysqld restart, and the loader refuses while one runs. A lost gtids ConfigMap is rebuilt from the claim, never treated as artifact damage. A bootstrap that inherits a barrier copies the voids the planner consulted.

## Open questions

1. **Q1 sign-off.** A versus C (see Decisions) is pending Colin's review.
2. **Tagged explicit GTIDs on `mysql:9.7`.** `SET GTID_NEXT='<uuid>:<tag>:<n>'` with an empty transaction and the privilege set above are expected from MySQL 8.3 but are not exercised anywhere in the repo. PR2 verifies first; if they fail, the marker design comes back for review (no fallback is designed).
3. **mysqlsh behaviour.** `loadUsers` with `excludeUsers` into staging, `sys.exit(70)` propagating through `mysqlsh --py -f` (diagnosis only), and the global session keeping its `GET_LOCK` through `util.load_dump`: both checked by the native tests before PR5 and PR1 merge respectively.
4. **local-path ownership in the playground.** The design assumes the k3d local-path provisioner creates claim directories writable by uid 27 (its default setup script uses mode 0777) so `staging-prepare` can create `<runID>/`. Unverified here; PR5 checks it on k3d. If it fails, the playground gets a StorageClass with a setup script that does so; the operator does not gain a root init container.
5. **mysqlsh in `mysql:9.7`.** Unknown; handled by the probe. Recording the measured answer in the docs is a PR5 task.
6. **#206 interface.** The exact plan sets `P` and `Q`, the list of voids the planner consulted (bootstrap), and an archive identity readable without a plan are requirements on #206 (P1), not yet agreed there.

## Acceptance criteria

1. With `pointInTime` and `staging`, a full-instance in-place restore reaches `Succeeded` with exactly the staged dataset, which the proof ties to the pre-stop history; no event or session writes and no event worker between the barrier and the fence release when mysqld does not restart in that window (after a restart, events that commit before the next Poll are the stated residual, a running worker is killed at that Poll, and the loader refuses to start destruction while one runs), and `event_scheduler` restored (scenario 63 in release).
2. Every refusal decidable from the backup, archive and staged result happens before the fence; the rechecks after the fence are bounded and release it (unless a damage record exists); every reason is in status and an Event.
3. From the loader write-ahead, the primary stays fenced and frozen with `RestoreInPlaceDamagedPrimaryFenced=True` until the Repaired proof (`JobComplete`, exactly one execution, execution stopped, completion marker) or the Accepted proof, in every phase of every run and whatever `gtid_executed` shows, and no stale status write erases the record; the critical alert pages only when no loader is running; no loader exit, pre-destruction refusal, never-started pod or run failure clears it; an accept after a `Discarded` settlement commits its marker only after every peer receiver is verified stopped; per-schema restores are refused while it exists; a new confirm reuses the artifact (scenario 64 in release).
4. Nothing destructive runs without a new confirm or an unconsumed one whose run never created a loader Job; a loader Job is never rebuilt after its write-ahead, and a second execution of one Job is refused by the execution claim or detected by the Job's counters.
5. No live `RESET`; every full-instance restore writes a barrier with a never-reused number before its loader and a completion marker before its fence release; an accept writes an accept marker; settlement ignores markers; the planner enforces the base and window rules; `PITRArchiveReady` reports the gap until an `Instance`+consistent backup carrying a closing marker succeeds.
6. `allowPartial`, `allowUnverifiedCrossSite`, per-schema PITR, `excludeSchemas`, `skipBinlog:false`, `loadIndexes:false`, non-`mysqlBackupRef` full-instance sources, and backups that are filtered, inconsistent, unbounded or not `Succeeded` are refused without consuming the confirm.
7. The loader is built only from the accepted snapshot and an artifact whose certification matches it; the status additions stay within the stated budget (unit test).
8. The staging pod has no token, Service or host network and binds loopback with mysqlx off (unit test).
9. Artifacts appear in status, a gauge and an alert, and are deleted only by success, policy, replacement after the replacement passed Fencing, or the annotation; no `Verified` artifact is deleted automatically while a damage record exists.
10. CRDs regenerated and copied to the chart; ClusterRole mirrors `persistentvolumes get`; log-schema, doctor files and the docs listed above updated in the PRs that change them.
11. Merge gated on #206 and #203, and on PR1 and PR2 (#233 is merged).
12. During `Staging`/`Staged`, planned failovers are accepted and completed and reclone queues drain; from Preflight on no backup starts against the target, and Preflight waits for running ones.
