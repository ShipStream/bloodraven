# Proposal: DR Drill Automation / Backup Verification

**Wishlist item:** [#8](../WISHLIST.md)
**Status:** Draft
**Branch:** `wishlist/backup-verification`

## Motivation

Bloodraven can produce backups on a schedule (`spec.backup.schedules[]` → `MysqlBackup` CRs → Jobs running `util.dumpInstance`) and can restore from one (`spec.initFromBackup`, `spec.restoreInPlace`). Nothing today *exercises* a backup between the moment it's written and the moment an operator needs it in anger. A backup that fails to load — corrupt dump, missing binlog segment, forgotten credential rotation, storage class regression — is silently unusable, and the failure surfaces during the incident that made us reach for it.

The goal of this proposal is to close that loop: restore the latest backup of each profile periodically, replay the latest archived binlog range when PITR is enabled, run a configurable sanity check, record the result, and expose a `bloodraven_backup_verified_timestamp_seconds` gauge so alerting can fire on stale verification just like it fires on stale backups.

## Goals

1. Automated, periodic verification of every active backup profile.
2. Exercise the *full* restore path: dump load + optional PITR binlog replay against an isolated MySQL instance.
3. Record per-attempt status on a CR so `kubectl describe` tells the whole story.
4. Export Prometheus metrics that match the existing `bloodraven_backup_*` family (same label set, same `_timestamp_seconds` / `_total` / `_duration_seconds` conventions).
5. Clean up all artifacts (Pods, PVCs, Jobs, verification CRs beyond retention) without operator intervention.
6. No impact on the live cluster: verification reads backups, never touches a primary-candidate or replica pod.

## Non-goals

- **Logical equivalence checking** between verified dump and live primary. That belongs to a data-consistency tool; verification's contract is "this backup loads and responds to SQL", not "this backup matches the primary right now".
- **Replay validation against application semantics.** Sanity query is a single configurable SELECT (or NULL for "just load successfully"); we don't ship application-aware checks.
- **In-cluster disaster recovery rehearsal** (promoting the verified instance to serve traffic). Covered separately by Wishlist #7 / #11.
- **Backup encryption** (Wishlist #13). Verification will decrypt if/when that lands; it doesn't block this work.

## API

### New CRD: `MysqlBackupVerification`

Lives in `api/v1alpha1/mysqlbackupverification_types.go`, mirrors the shape of `MysqlBackup`.

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlBackupVerification
metadata:
  name: orders-nightly-20260420-0200
  namespace: shipstream
spec:
  # Required: which failover group + profile to verify.
  failoverGroupRef:
    name: orders
  profileName: nightly

  # Which backup to verify. Default: latest Succeeded MysqlBackup for (group, profile).
  # Optional explicit ref for ad-hoc drills ("verify last week's backup").
  backupRef:
    name: orders-nightly-20260413-0200   # optional

  # PITR replay settings. Default: no replay.
  # "latest" → replay every archived binlog event through newest manifest entry.
  # An RFC3339 timestamp → replay up to that point.
  pointInTime:
    mode: latest            # one of: none | latest | timestamp
    timestamp: ""           # RFC3339, required if mode=timestamp

  # Sanity check. Runs after dump load (and PITR replay, if requested).
  # If empty, success = "dump loaded without error".
  sanityCheck:
    query: "SELECT COUNT(*) FROM orders.orders WHERE created_at > NOW() - INTERVAL 7 DAY"
    expect:
      minRows: 1            # optional; fail if scalar result < minRows
      # maxDurationSeconds defaults to 60
      maxDurationSeconds: 60

  # Ephemeral storage sizing. Must be >= backup artifact size; defaults to
  # 1.5 × the referenced backup's status.sizeBytes (rounded up to the nearest
  # 10 GiB) if not specified.
  storage:
    storageClassName: fast-ssd    # optional; defaults to same class used by the MysqlFailoverGroup primary PVC
    size: 750Gi                   # optional
    # subPath omitted; verification PVCs are always dedicated per-run.

  # What to keep on failure. On success, everything is always cleaned up.
  keepOnFailure: true             # default: true — leave PVC + Pod around for inspection
  ttlSecondsAfterFinished: 3600   # default: 3600 on success, never on failure when keepOnFailure=true

status:
  phase: Succeeded                # Pending | Provisioning | Restoring | Replaying | Checking | Cleaning | Succeeded | Failed
  startTime: "2026-04-20T02:00:14Z"
  completionTime: "2026-04-20T02:11:52Z"
  durationSeconds: 698
  backupRef:                      # resolved concrete reference
    name: orders-nightly-20260420-0200
    uid: 7c1e...
  restoredGtidExecuted: "abc-...:1-9182731"
  replayedThroughBinlog:          # null if pointInTime.mode=none
    file: mysql-bin.000412
    position: 9183001
    timestamp: "2026-04-20T01:59:57Z"
  sanityCheck:
    ran: true
    durationMs: 140
    resultRow: "42"
  jobName: verify-orders-nightly-20260420-0200
  podName: verify-orders-nightly-20260420-0200-p2vf4
  pvcName: verify-orders-nightly-20260420-0200-data
  conditions:
    - type: Verified
      status: "True"
      reason: Succeeded
      lastTransitionTime: "2026-04-20T02:11:52Z"
      message: "dump loaded, 412 binlogs replayed, sanity query returned 1 row"
```

### Scheduling integration

Add an optional field to each `BackupProfile` in `MysqlFailoverGroup.spec.backup.profiles[]`:

```yaml
spec:
  backup:
    profiles:
      - name: nightly
        storage: { ... }
        retentionPolicy: { count: 14, minKeep: 3 }
        verification:
          enabled: true
          schedule: "0 5 * * *"          # after the 02:00 nightly backup finishes
          concurrencyPolicy: Forbid      # Forbid | Replace (never Allow)
          pointInTime: { mode: latest }
          sanityCheck:
            query: "SELECT 1"
          keepOnFailure: true
          storage:                       # same shape as MysqlBackupVerification.spec.storage
            storageClassName: fast-ssd
```

The `MysqlFailoverGroupReconciler` renders this to a Kubernetes `CronJob` (same pattern as `BuildBackupCronJob` in `internal/controller/backup_schedule.go`) that runs `bloodraven trigger-verification --group=orders --profile=nightly`. That subcommand creates a `MysqlBackupVerification` CR — exactly analogous to how `trigger-backup` creates a `MysqlBackup`. Manual / ad-hoc verifications bypass the CronJob and create the CR directly (`kubectl create -f verification.yaml`).

### RBAC

New markers on `MysqlBackupVerificationReconciler` (following the `MysqlBackupReconciler` pattern in `internal/controller/backup_reconciler.go:56-62`):

```go
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;persistentvolumeclaims;services;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
```

Helm chart ClusterRole mirror required (per AGENTS.md pre-PR gate #5).

## Verification lifecycle

```
  Pending ──► Provisioning ──► Restoring ──► Replaying ──► Checking ──► Cleaning ──► Succeeded
                   │                │             │             │            │
                   └─ fail ─────────┴─ fail ──────┴─ fail ──────┴──── fail ──┴──► Failed
                                                                                    │
                                                                       keepOnFailure=false ──► Cleaning
```

1. **Pending** — CR reconciled for the first time; resolve `backupRef` (either explicit or "latest Succeeded `MysqlBackup` with matching group+profile"), set owner reference back to the `MysqlBackup` (so GC of the backup doesn't strand a verification record), finalizer added.

2. **Provisioning** — create a dedicated `PersistentVolumeClaim`, a `Service` (headless, for the verification Pod's DNS), and a verification `Pod`:
   - Image: same `backup.image` + `mysql.image` used by the operator (reuse `internal/controller/image_resolver.go`). Sidecar image not needed — no replication, no archiving.
   - Init containers (in order):
     - `download-backup` — fetches dump artifact to the PVC from backup storage (S3 or PVC reuse); reuses `internal/controller/backup_job.go` credential-mount patterns.
     - `download-binlogs` — only if `pointInTime.mode != none`; reuses `bloodraven pitr-download` from `cmd/bloodraven/main.go:40`, downloads into `/binlogs`.
   - Main container: `mysqld --skip-networking=OFF --bind-address=127.0.0.1 --server-id=4294967290 --log-bin=OFF --skip-replica-start` on the PVC, no replication configured, no external networking.

3. **Restoring** — a sibling `Job` (reused `BuildRestoreJob`-style builder) runs `mysqlsh util.loadDump('/data/dump', { threads: N, updateGtidSet: 'replace', loadUsers: false })` against the verification Pod. `loadUsers: false` because grant statements would conflict with the throwaway instance's privilege set.

4. **Replaying** — if `pointInTime.mode != none`, `mysqlbinlog /binlogs/*.<n> | mysql` through the requested `--stop-datetime` or through the last archived event. Report `replayedThroughBinlog` coordinates.

5. **Checking** — if `sanityCheck.query` is non-empty, run it with `maxDurationSeconds` as a client-side timeout. A single scalar row is captured into `status.sanityCheck.resultRow`. Fails if `minRows` is set and the scalar `< minRows`, or if the query errors.

6. **Cleaning** — on success, delete Pod/Job/Service/PVC; on failure with `keepOnFailure: true`, delete only the Job (whose logs we've already captured into status/events). On failure with `keepOnFailure: false`, full cleanup.

7. **Finalizer removal** — only after cleanup observed complete.

State-machine ownership: mirror the patterns in `internal/controller/restore_inplace.go:658-690` (phase transitions gated by RFC-3339 `confirm` token monotonicity) minus the in-place confirmation token — a verification CR is idempotent by virtue of being single-shot and owner-referenced.

## Isolation model

**Same namespace as the operator, dedicated resources per run.** No new namespace is created. Rationale:

- Creating namespaces from a controller requires elevated RBAC (`namespaces,create`) that the operator shouldn't need for day-to-day work.
- The verification Pod never exposes a service outside itself; it binds `127.0.0.1` and the Pod IP only. No Service of type ClusterIP/NodePort is created for external reach. The headless Service exists purely so the restore Job's DNS resolves.
- Pod-level `NetworkPolicy` (templated optionally) can deny ingress from everything except the paired restore Job if the cluster enforces policy.
- Labels `bloodraven.shipstream.io/verification=<cr-name>` on everything created, so cleanup is a single label selector.

**Resource limits:** default `requests.memory` = 2× backup size capped at 8 GiB; `limits.memory` = 2× requests. These are defaults; `spec.resources` exposes the full Kubernetes resource shape for override.

## Metrics

Added to `internal/metrics/metrics.go` alongside the existing `bloodraven_backup_*` family (lines 72-119):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `bloodraven_backup_verified_timestamp_seconds` | gauge | `group`, `profile` | Unix time of last `Succeeded` verification. **Named in the wishlist item.** |
| `bloodraven_backup_verification_last_attempt_timestamp_seconds` | gauge | `group`, `profile` | Unix time of last attempt, regardless of outcome. |
| `bloodraven_backup_verifications_total` | counter | `group`, `profile`, `result` (`success`\|`failure`) | Lifetime verification attempts. |
| `bloodraven_backup_verification_duration_seconds` | histogram | `group`, `profile` | End-to-end wall-clock duration. |
| `bloodraven_backup_verification_replay_lag_seconds` | gauge | `group`, `profile` | `now() − replayedThroughBinlog.timestamp` at success; measures PITR freshness at verification time. |

A scrape-stale gauge is already the Prometheus convention — alerting becomes `time() - bloodraven_backup_verified_timestamp_seconds > <your SLO>`. This is the explicit deliverable the wishlist item asks for.

## Retention

Per-profile retention, mirroring `spec.backup.profiles[].retentionPolicy`:

```yaml
profiles:
  - name: nightly
    verification:
      retentionPolicy:
        count: 30           # keep last 30 MysqlBackupVerification CRs per profile
        keepFailures: 10    # always retain last N failed runs even if older
```

The reconciler GCs old verification CRs the same way `MysqlBackupReconciler` does (see `internal/controller/backup_reconciler.go`'s retention logic). Retaining failed runs on top of count is a deliberate bias: failures are the data you want to investigate.

## Security

- Reuse the file-mounted credentials pattern from `internal/controller/backup_job.go` — MySQL + cloud storage credentials land under `/run/bloodraven/{mysql,aws}-creds`, never env vars.
- The verification MySQL instance uses a freshly-generated `root` password written to an ephemeral Secret. The restore Job, sanity-check Job, and verification Pod share this Secret; it's deleted during cleanup.
- No service account tokens mounted in the verification Pod beyond the default `automountServiceAccountToken: false`.
- Backup credentials are **read-only**: the verification's cloud-storage credentials should carry only `s3:GetObject`/`s3:ListBucket` on the backup prefix. Surface this in docs; don't silently reuse the backup-writing credential if a narrower one exists on the profile.

## Edge cases

- **Large backups exceed storage class quotas.** Verification PVCs auto-size from `status.sizeBytes`; if the requested size is rejected by the CSI driver, surface a `PVCProvisioningFailed` condition and fail fast rather than looping on `Provisioning`.
- **No Succeeded backup exists yet.** Verification goes straight to `Failed` with `reason: NoBackupAvailable`. The `_last_attempt_timestamp_seconds` metric still moves; the `_verified_timestamp_seconds` gauge stays at its previous value.
- **Concurrent verifications for the same profile.** CronJob `concurrencyPolicy: Forbid` prevents the scheduler from stacking. Manual CRs are rejected by an admission-time check if another verification for the same `(group, profile)` is in a non-terminal phase. (Use a CEL validation rule if feasible; fall back to reconciler-side rejection with a `BlockedByActiveVerification` condition.)
- **Backup referenced by an in-flight verification is deleted.** Owner reference from verification → MysqlBackup ensures cascade; we detect this, flip the verification to `Failed` with `reason: BackupDeleted`, and cleanup.
- **PITR manifest gap.** If `pointInTime.mode: latest` but archived binlogs don't form a contiguous range starting at the backup's `binlogFile`/`binlogPos`, the reconciler surfaces `BinlogGap` with the missing range and fails the verification. This is itself a valuable signal — a silent PITR regression we'd otherwise only discover during a real restore.
- **mysqld refuses to start on the dump's MySQL version mismatch.** Pin the verification `mysql.image` to the same tag used by the active site primary at the time of the referenced backup (captured into `MysqlBackup.status` at dump time — needs a new `status.mysqlImage` field on `MysqlBackup`, small migration).
- **Verification itself takes longer than the schedule interval.** `concurrencyPolicy: Forbid` handles it; document that nightly verification of 500 GB backups is realistic at ~30–60 min on NVMe and cronjob cadence must respect that.
- **Cluster is under capacity pressure.** Verification Pod uses `PriorityClass` below the operator's normal pods (configurable via `spec.priorityClassName`). Eviction during verification transitions to `Failed` with `reason: Preempted`; cleanup runs as normal.

## Testing

Following the AGENTS.md pre-PR gate:

1. Unit tests under `internal/controller/backup_verification_*_test.go`:
   - Phase-transition table tests (Pending → ... → Succeeded / Failed paths).
   - PVC size defaulting from `status.sizeBytes`.
   - Admission-time concurrency rejection.
   - Retention GC.
   - Owner-reference cascade on MysqlBackup deletion.
2. Envtest coverage for CRD validation (CEL rules, defaulting, status subresource).
3. E2E scenario in `test/e2e/backup_verification_test.go`: run a backup against the playground-style two-site cluster, trigger a verification, assert the CR reaches `Succeeded` and the gauge advances.
4. Playground integration: add `playground/verify-backup.sh` that pokes the demo cluster and displays status.

## Phased rollout

- **Phase 1** (this PR, or a small series):
  - CRD types + deepcopy + CRD YAML (config + Helm).
  - Reconciler with dump-load-only verification (no PITR replay, no sanity check). Metrics emitted.
  - Docs page `site/content/docs/7.backup-and-restore/6.backup-verification.md` linked from `backup-restore.mdx` and `monitoring.mdx`.
- **Phase 2**:
  - PITR replay mode.
  - Sanity-check query with scalar comparison.
  - Playground integration + E2E.
- **Phase 3**:
  - `kubectl` plugin integration (Wishlist #18): `kubectl bloodraven verify-backup <name>`.
  - Grafana dashboard panel for verification freshness (part of Wishlist #16).

## Open questions

1. **Reference model for `backupRef`.** Should verification resolve to a `MysqlBackup` CR, or directly to a backup location (so we can verify backups whose CRs have been retention-GC'd)? Proposal: CR only in Phase 1; a `backupLocation` passthrough in Phase 2 for archaeological drills.
2. **Where does the verification MySQL live?** Today: same namespace, dedicated Pod. Alternative: let users target a dedicated DR cluster (pairs with Wishlist #7). Punt to Phase 3.
3. **Retention for the PVCs themselves.** With `keepOnFailure: true` and a flood of failures, PVCs accumulate. Cap with a hard ceiling (`retentionPolicy.maxFailedPVCs = 5`) or reuse the `keepFailures` count? Proposal: reuse `keepFailures` and always delete PVCs of CRs GC'd by retention — so the user's retention knob governs storage too.
4. **Are we verifying that the PITR archive is *complete*, or just that what's archived replays cleanly?** Proposal: surface both. "Complete" requires correlating archived manifest coverage against the primary's current `@@gtid_executed`, which is a separate check; file as follow-up.
5. **Wiring the `MysqlBackup.status.mysqlImage` migration.** Needed so verification pins the right mysqld version. Small existing-CR backfill: reconciler populates it on next observation if empty. Worth a one-paragraph doc note.

## Acceptance criteria

- `MysqlBackupVerification` CRD installs via Helm and `config/crd/bases/`.
- A failover group with `profiles[].verification.enabled: true` emits a CronJob that creates verification CRs on schedule.
- A manually-created verification CR for the playground cluster reaches `Succeeded` on the happy path.
- `bloodraven_backup_verified_timestamp_seconds{group,profile}` advances on success; `bloodraven_backup_verifications_total{result="success"}` increments.
- Failure of any phase leaves either a clean cluster (success-path cleanup) or an inspectable verification Pod + PVC (failure path with `keepOnFailure: true`).
- Pre-PR gate (AGENTS.md): `make generate && make manifests` clean, `make vet`, `make lint`, `make test`, `make test-envtest` all pass.
- Helm chart ClusterRole mirrors the new kubebuilder RBAC markers.
- Docs added: `site/content/docs/7.backup-and-restore/6.backup-verification.md` + updates to `backup-restore.mdx`, `monitoring.mdx`, `production-hardening.mdx`.
