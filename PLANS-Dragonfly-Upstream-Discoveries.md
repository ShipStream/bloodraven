# Dragonfly Operator Upstream Discoveries

## Implementation Checklist Audit

Audit date: 2026-05-02. Scope assumption: Bloodraven supports only Dragonfly `v1.38.0+`; version-specific issues fixed before `v1.38.0` are treated as not relevant except where they imply a lasting config or documentation policy.

### Closed Actions

- [x] Make the `v1.38.0+` support floor explicit in user-facing Dragonfly docs and examples, and update stale examples/tests that still name older images such as `v1.25.5`.
- [x] Decide whether to enforce the `v1.38.0+` floor in CRD validation/admission or leave it documented-only because image tags are operator-supplied strings. Decision: documented-only. The CRD rejects empty images and `:latest`, but does not parse arbitrary registry/tag/digest strings as semantic versions. User-facing docs and playground examples state the supported `v1.38.0+` floor.
- [x] Document Bloodraven's opinionated Dragonfly config contract: ephemeral cache/session state, no operator-managed snapshots, no operator-supported `DFLY LOAD` on live masters, no tiered storage unless explicitly accepted by the operator, and user `spec.dragonfly.args` are escape hatches.
- [x] Extend `status.dragonfly.sites[].ready` and the chaos baseline to include `master_last_io_seconds_ago != -1`, matching the planned-failover `CandidateSyncReady` gate.
- [x] Add an alert or documented metric guidance for unexpected/repeated full resyncs (`master_sync_in_progress=1` frequency/duration). `docs/docs/monitoring.mdx` now documents Dragonfly reachability/promotion/panic metrics and recommends alerting from CR status when `status.dragonfly.sites[].syncInProgress` persists or repeats unexpectedly.
- [x] Document topology-change cost: every Dragonfly promotion/re-parent can force full resync and can cause master latency spikes, so planned failovers/rollouts should be scheduled accordingly.
- [x] Decide whether `spec.dragonfly.args` should block or warn on known-unsafe flags for Bloodraven-managed topologies, especially tiered-storage flags, snapshot scheduling, ACL files, Lua compatibility flags, and unsupported TLS-replication combinations. Decision: block only operator-owned safety flags in code; document the rest as an explicit escape hatch requiring deployment-specific operational acceptance.

### Accounted For Or Not Relevant

- [x] Startup readiness-gate deadlock: not relevant to current design. Bloodraven does not use Dragonfly readiness gates for master election; `DragonflyManager` polls independently.
- [x] Readiness condition requeue dependence: accounted for by the independent Dragonfly poll loop (`DragonflyManager.Run`, default 2s), not by pod events.
- [x] Concurrent master-election race: not relevant as an arbitrary Dragonfly election. The Dragonfly master follows MySQL `status.activeSite` or the in-flight planned-failover target.
- [x] Rolling update deletes all replicas at once: not relevant to custom Dragonfly logic. Per-site Dragonfly is a one-replica StatefulSet and Bloodraven does not loop over stale Dragonfly pods for deletion.
- [x] New pod not configured after restart: accounted for. `reconcileReplication` handles `RoleUnconfigured`, wrong-master replicas, and link-down replicas by issuing or re-issuing `REPLICAOF`.
- [x] `CONFIG SET snapshot_cron ""` during `LOADING`: not relevant to current implementation. Bloodraven does not manage Dragonfly snapshot cron and issues no CONFIG after `REPLICAOF`.
- [x] Snapshot cron restore on promotion: not relevant to current implementation for the same reason; Dragonfly is treated as ephemeral cache/session state.
- [x] Service update alone is insufficient during failover: accounted for. The active Service selector AND-gates `dragonfly-role=master` and `dragonfly-traffic=enabled`, and planned/emergency promotion strips/restores traffic explicitly.
- [x] Existing clients need forced reconnect: accounted for. Planned and emergency promotion issue best-effort `CLIENT KILL TYPE NORMAL` against the old source.
- [x] Replica promotion stability checks: partially accounted for in the critical path. `CandidateSyncReady` checks `master_link_status`, `master_sync_in_progress`, `master_last_io_seconds_ago`, persistence loading, and offset catch-up before planned promotion.
- [x] Foreground deletion reconcile loop: accounted for at the main reconciler entry via the `DeletionTimestamp` early path.
- [x] Spurious Service updates: largely accounted for through controller-runtime `CreateOrUpdate` for resources and equality checks before Dragonfly status patches.
- [x] IPv6 values in labels: not relevant. Dragonfly role/traffic labels do not carry IP addresses; addresses are Service DNS names or status fields.
- [x] `--break_replication_on_master_restart`: accounted for and protected. `buildDragonflyArgs` always emits it and filters user args that target operator-owned flags.
- [x] Orphaned downstream replicas after topology changes: accounted for for Bloodraven's two-site topology. Bloodraven does not use chained replication and re-issues `REPLICAOF` when replicas point wrong or have link down.
- [x] Always-full resync on new master: accounted for operationally by bounded planned-failover sync wait and best-effort Dragonfly continuity; documentation still needs the explicit cost callout above.
- [x] v1.35.0 chained-replication regression: not relevant with the `v1.38.0+` support floor.
- [x] `DFLY LOAD` cloud-path bug fixed in v1.37+: not relevant with the `v1.38.0+` support floor.
- [x] S3 snapshot regression in v1.36.0: not relevant with the `v1.38.0+` support floor.
- [x] Tiered-storage snapshot/journal race fixed in v1.37+: not relevant with the `v1.38.0+` support floor.
- [x] Tiered-storage RDB crash/RSS issues: not used by Bloodraven by default. Remaining action is policy/documentation for user-supplied args.
- [x] `CLUSTER SLOTS` / `CLUSTER SHARDS` return `LOADING`: accounted for by design. Bloodraven does not expose replicas through the active Service and uses `INFO`/persistence state, not CLUSTER commands, for operator health.
- [x] CONFIG commands fail during `LOADING`: not relevant because Bloodraven does not run CONFIG commands in the replication path.
- [x] Silent key loss when Dragonfly replicates from a Redis master: not relevant. Bloodraven only manages Dragonfly-to-Dragonfly replication.
- [x] Maxmemory during startup snapshot load can start empty: not relevant to default Bloodraven semantics because Dragonfly data is ephemeral `emptyDir` cache/session state and emergency continuity is best-effort.
- [x] Set-member expiry replication bug fixed in v1.37+: not relevant with the `v1.38.0+` support floor.
- [x] Empty hash snapshot crash fixed in v1.37+: not relevant with the `v1.38.0+` support floor.
- [x] v1.38 `--replica_delete_expired=true` default: acceptable default for cache/session semantics; no compatibility shim needed.
- [x] `INFO replication` field names: accounted for in `internal/dragonfly.ParseInfoReplication`.
- [x] AUTH model: accounted for at the operator level via `--requirepass` from Secret and authenticated operator connections.
- [x] ACL key/pubsub DSL differences: not directly relevant. Bloodraven does not manage `aclfile`; user-supplied args policy remains open above.
- [x] TLS replication options: not directly relevant. Bloodraven currently uses plaintext in-cluster Service DNS and does not expose a TLS replication API; user-supplied args policy remains open above.
- [x] Snapshot/persistence differences: not used by default. Bloodraven provisions `emptyDir` and treats Dragonfly as non-durable cache/session state.
- [x] Lua scripting differences: not operator-managed. If users enable compatibility flags through `spec.dragonfly.args`, that falls under the open config-policy documentation item.
- [x] Unsupported/different commands (`WAIT`, `COMMAND DOCS`, search, cluster slot management, modules/functions): not used by Bloodraven's operator path.

Findings from three sources: the dragonflydb/dragonfly-operator PR/issue history, the main
dragonflydb/dragonfly engine issue tracker, and the official Dragonfly documentation. Referenced
before starting our own implementation to avoid repeating known mistakes.

---

## Part 1: Operator-Level Gotchas (dragonflydb/dragonfly-operator)

### Replication State Machine (Most Critical)

**Deadlock during startup (Issue #497, PR #511):**
With a readiness gate, master election can deadlock — pods need to pass the readiness gate to
be a healthy master candidate, but the readiness gate needs a healthy master to be configured.
Always requeue when no healthy candidate exists rather than returning `nil`.

**Readiness gate never transitions to `True` (Issue #503, PR #511):**
If a condition check finds a pod not ready, return `RequeueAfter: 5s` explicitly — don't rely
on pod events to re-trigger the reconcile.

**Race in master election (PR #437):**
Multiple concurrent reconciles can all try to promote themselves. Always sort pod names and
deterministically pick the first eligible candidate.

---

### Rolling Updates

**All replicas deleted simultaneously (Issue #504, PR #507):**
If you loop over stale pods and delete them all in one pass, all replicas drop at once — defeats
PDB and creates data loss risk. Pattern: sort pods, check if any are already terminating,
delete exactly one, then `RequeueAfter`.

**New pod never configured after rolling restart (Issue #508):**
A pod can come up with no role label if the reconcile that handled the *old* pod's removal also
handled cluster reconfiguration — the new pod gets skipped because the cluster "looks correct."
Replication setup must handle both initial cluster setup *and* new-pod-joining-existing-cluster
explicitly; never assume the cluster is fully configured after one reconcile.

---

### Snapshot / CONFIG Ordering (Critical Gotcha)

**`CONFIG SET snapshot_cron ""` fails during LOADING (Issue #498, PR #505):**
After `REPLICAOF <master>`, the replica enters `LOADING` state. If you try to clear the
snapshot cron after that, it fails with "Dragonfly is loading the dataset in memory" and the
role label never gets applied — rolling updates stall completely.
**Clear snapshot schedule *before* issuing `REPLICAOF`.**

**Snapshot cron not restored on failover promotion (PR #513):**
When a replica is promoted to master, the snapshot cron must be explicitly restored — it was
cleared when the pod became a replica. Master failover must restore all master-specific
configuration.

---

### Traffic / Client Connections

**Service update isn't enough during failover (PR #455):**
Use a `traffic=enabled/disabled` label on top of the role label. Service selector requires both
`role=master` AND `traffic=enabled`. Sequence: disable traffic on old master → promote new
master → enable traffic → disconnect old master clients.

**Active clients need forced reconnect (PR #436):**
Just removing the pod from the service isn't enough. Existing connections get `READONLY` errors
until they reconnect. Terminate connections on the demoted master to force client reconnects.

---

### Replica Stability Detection

Don't rely on pod readiness alone. Check all three `INFO REPLICATION` fields:
- `master_sync_in_progress != 1`
- `master_link_status == "up"`
- `master_last_io_seconds_ago != -1` (value of `-1` means never synced)

---

### Kubernetes Mechanics

**Foreground deletion loop (PR #415):**
Add an early `if obj.DeletionTimestamp != nil { return }` check or the reconciler loops
destroying and recreating resources during `kubectl delete --cascade=foreground`.

**Spurious service updates (Issue #334):**
The operator kept re-updating a service that hadn't changed, hitting cloud provider rate limits.
Compare desired vs actual state before patching.

**IPv6 in labels (PR #269):**
IPv6 addresses don't fit in label values (255-char limit) — store in annotations instead.

---

### Dragonfly-Specific Flag

**`--break_replication_on_master_restart` (PR #386):**
Set this flag to prevent replicas from silently auto-reconnecting to a stale master after it
restarts. Without it you can get silent data divergence after a master restart.

---

## Part 2: Engine-Level Bugs (dragonflydb/dragonfly main repo)

### REPLICAOF Behavior

**Orphaned downstream replicas on topology change (Issue #2044, OPEN):**
When a master that has replicas receives `REPLICAOF <new-host>` to become a replica itself, its
existing downstream replicas are orphaned. `REPLICAOF NO ONE` does NOT restore them. Each
downstream replica must explicitly re-issue `REPLICAOF <master>` after any topology change.
This is an architectural limitation, not a transient bug.

**Always full resync on new master (Issue #1958, OPEN):**
Dragonfly does not preserve the replication offset (LSN) when reissuing `REPLICAOF` to a new
master. Every topology change triggers a full resync, regardless of how recently the replica
was in sync. Design accordingly: assume topology changes are expensive.

**Frequent unexpected full resyncs (Issue #4760, OPEN):**
Even stable, fully-connected replicas can perform full resyncs multiple times per day. Each
resync blocks command execution for its duration. Alert on `master_sync_in_progress:1` exceeding
expected frequency; it is not safe to assume a replica will only fully sync once at startup.

**v1.35.0 breaks chained replication (Issue #6091, FIXED in patch):**
v1.35.0 added a check rejecting replication if the master is itself a replica, breaking
cascaded/chained topologies. This was a regression. Do not use v1.35.0.

---

### DFLY LOAD and Snapshot Interaction with Replication

**`DFLY LOAD` does not replicate to existing replicas (Issue #6739, OPEN — architectural):**
When you load a snapshot via `DFLY LOAD` on the master, the loaded data is written directly to
`db_slice` without journaling. Replicas only see journal-based changes and will NOT have the
loaded keys. After any `DFLY LOAD` on a live master with replicas, you must force full resync:
```
REPLICAOF NO ONE   # on each replica
REPLICAOF <master> <port>
```
**Alternatively: load snapshots before replicas attach.**

**`DFLY LOAD` from cloud fails when `--dir` is local (Issue #7074, FIXED in v1.37+):**
`DFLY LOAD s3://bucket/file.dfs` fails with "File not found" if `--dir` is a local path. The
loader treats the S3 URL as a local path. Fixed in v1.37+.

**S3 snapshot regression in v1.36.0 (Issue #6345, FIXED in patch):**
v1.36.0 broke S3-compatible endpoint snapshot storage. v1.35.1 works; avoid v1.36.0 for S3.

---

### Data Race in Tiered Storage + Replication (CRITICAL for v1.36 and earlier)

**Snapshot/journal race with tiered storage (Issue #6816, FIXED in v1.37+):**
With tiered storage enabled, mutations during snapshot serialization could race with journal
writes, causing entries without their mutations to appear in the replication stream. Result:
**silent data corruption on replicas**. Fixed in v1.37+ (PRs #7150, #6824). Do not use tiered
storage with replication on versions before v1.37.

**Tiered storage RDB crash on startup (Issue #6521, OPEN as of research date):**
Starting Dragonfly with tiered storage enabled and an existing RDB containing non-string keys
(hashes, lists, sets, streams) crashes with `Unsupported tag for GetRawString(): 17`. Workaround:
start without tiered storage, then enable it. Or ensure RDB contains only string keys.

**Tiered storage RSS exceeds maxmemory (Issue #5645, OPEN):**
With tiered storage enabled, process RSS grows far beyond the configured maxmemory cap (e.g.,
666 GB RSS against 540 GB maxmemory). The process OOMs despite available SSD capacity.
**Tiered storage is not safe for production at scale until this is resolved.**

---

### LOADING State Behavior

**CLUSTER commands return LOADING during initial sync (Issue #5561, OPEN):**
During initial sync (INITIAL_SYNC), `CLUSTER SLOTS` and `CLUSTER SHARDS` return:
`LOADING Dragonfly is loading the dataset in memory`. Cluster-aware clients cannot resolve
topology during replica sync. Operator must remove replicas from service discovery during sync,
or use a health check endpoint that doesn't rely on CLUSTER commands.
*Fixed in v1.32.0 for partial sync — partial sync no longer enters LOADING state.*

**CONFIG commands fail during LOADING:**
As already noted in the operator section, `CONFIG SET` fails while a replica is loading.
This is confirmed engine behavior, not an operator bug. Clear all config before `REPLICAOF`.

---

### Data Loss Scenarios

**Silent key loss when replicating from Redis master (Issue #5508, OPEN):**
Dragonfly replica of a Redis master can silently lose millions of keys with no error. In one
report, 9M keys missing (407M vs. 416M on master) after replication reported success. Implement
explicit DBSIZE validation post-replication; alert on count mismatch.

**Maxmemory reached during snapshot load causes silent empty start (Issue #5845, CLOSED):**
If Dragonfly reaches maxmemory while loading a snapshot at startup, the load fails and the
instance starts empty. No error is surfaced to the operator. Set maxmemory at least 20-30%
higher than the expected data size to account for serialization buffers.

**Set member expiry not replicated to replicas (Issue #6994, FIXED in v1.37+):**
Before v1.37+, when set members expire via lazy expiry, the deletion is not propagated to
replicas. Replica sets retain expired members indefinitely. Fixed in v1.37+.

**Empty hash crash during snapshot (Issue #7137, FIXED in v1.37+):**
When FIELDTTL expires the last field in a hash, the key becomes empty but is not deleted.
Subsequent `SAVE` hits DFATAL because empty hashes are invalid in RDB. Fixed in v1.37+.

---

### Performance: Full Sync Latency Impact

**80x write latency increase during full sync (Issue #4809, OPEN):**
During full resync from a large replica, `SET` command latency increases from ~0.2 µs to
~16 µs on the master. At scale: p99 spikes from 4 ms to 50 ms under moderate load (132K ops/sec)
with a 32M-key dataset (Issue #6131). Plan for this when scheduling rolling updates or
operator-driven topology changes. Consider read-only replicas and query routing to avoid master
during active syncs.

---

### Version Safety Matrix

| Version | Status | Notes |
|---------|--------|-------|
| v1.31.x | Avoid | Introduced partial sync but had partial-sync data loss bug (#5297) |
| v1.32.x | Usable | `CLUSTER` cmds work during LOADING; partial sync improvements |
| v1.33.x | Usable | Snapshot deadlock fixes |
| v1.34.0–v1.34.1 | **AVOID** | Cache-mode eviction causes catastrophic key loss (#5891, #5899) |
| v1.34.2 | Usable | Patch release; last safe pre-v1.35 |
| v1.35.0 | **AVOID** | Blocks chained replication (#6091) |
| v1.35.1 | Minimum safe baseline | Fixes replica-of-self loop and chained replication |
| v1.36.0 | **AVOID** | S3 snapshot regression, memory RSS escapes (#6545), tiering data races |
| v1.37.x | Recommended | Fixes tiering data races, set expiry replication, empty hash crash, DFLY LOAD cloud |
| v1.38.x | Latest stable | Proactive replica expired-key deletion (on by default), XREAD BLOCK crash fixed |

**Supported minimum: v1.38.0.** Bloodraven-managed deployments support only v1.38.0+; earlier-version notes are retained as research context only.

**New in v1.38: `--replica_delete_expired=true` (default ON).** Replicas now proactively delete
expired keys. If you have custom TTL tracking logic or audit expired-but-not-yet-deleted keys,
set `--replica_delete_expired=false` explicitly.

---

## Part 3: Redis Compatibility and Protocol Gaps

### INFO REPLICATION Field Names (Exact Strings for Parsing)

Master instance:
- `role:master`
- `master_replid` — replication ID
- `connected_slaves` — count of connected replicas
- `slave0:id=...,ip=...,port=...,state=...,lag=...` — per-replica line
- `master_repl_offset` — current position

Replica instance:
- `role:slave`
- `master_host` — primary hostname or IP
- `master_port` — primary port
- `master_link_status` — `up` or `down`
- `master_last_io_seconds_ago` — seconds since last interaction; `-1` means never
- `master_sync_in_progress` — `1` during active sync
- `slave_repl_offset` — replica's current position

Prometheus metric for lag: `dragonfly_connected_replica_lag_records`

---

### AUTH / ACL Differences from Redis

- Default user allows **any password** (unlike Redis where `requirepass` sets a strict password).
- **ACL key and pub/sub DSL are not supported** — `aclfile` must not contain key patterns or
  pub/sub channel rules; they are silently ignored or may cause parse errors.
- No special replication user required. Authentication between master and replica uses
  `--requirepass` / `--password`.
- TLS replication: `--tls_replication=true`; requires `.pem` format certs. Can disable TLS on
  admin port with `--no_tls_on_admin_port=true` for plaintext internal replication.

---

### Snapshot / Persistence Differences from Redis

- **Forkless snapshotting:** uses per-shard serialization and epoch versioning, not `fork()`.
  No memory spike on snapshot, but also no COW isolation — mutations during snapshot are included
  (relaxed point-in-time). Point-in-time is default since v1.32.
- **Multi-file format:** Dragonfly native snapshots are one file per shard. This is different from
  Redis's single `dump.rdb`. Redis-compatible single-file RDB export is also supported.
- **`DFLY LOAD` vs startup load:** `DFLY LOAD` bypasses the journal; startup RDB load is the
  safe path for pre-populating instances that will have replicas attached.
- **Automatic RDB import:** Dragonfly auto-loads Redis RDB files at startup (`--dir`, `--dbfilename`).

---

### Lua Scripting Differences

- Default behavior **blocks access to undeclared keys** — set `--default_lua_flags=allow-undeclared-keys`
  to allow arbitrary key access. When enabled, Dragonfly serializes all operations during script
  execution (significant throughput impact under concurrent load).
- Uses Lua 5.4 (supports integer types). Scripts using Lua 5.1 features only should be fine.

---

### Commands That Behave Differently or Are Unsupported

- `CLUSTER SLOTS` / `CLUSTER SHARDS` — return `LOADING` during initial sync (see above).
- `WAIT` — behavior differs from Redis; do not rely on it for synchronous replication confirmation.
- `COMMAND DOCS` — returns an error; not implemented.
- Search commands (`FT.*`) — partial support; `FT.INFO` missing vector parameters before v1.37.
- Cluster slot management (`ASKING`, `READONLY`, `READWRITE`) — not supported.
- Module loading, `FUNCTION` management — not supported.

---

## Key Patterns Summary

1. **Requeue aggressively** — any polling or state machine needs explicit `RequeueAfter`, not reliance on external events.
2. **Deterministic selection** — master/replica selection must use sorted ordering to prevent concurrent conflicts.
3. **One operation per reconcile** — rolling updates, scaling, and transitions should move one step per iteration.
4. **Clear config before REPLICAOF** — `CONFIG SET snapshot_cron ""` must happen before `REPLICAOF`, not after; the LOADING state blocks CONFIG commands.
5. **Restore master config on promotion** — anything cleared on a replica (snapshot cron, flags) must be restored when promoted.
6. **Traffic coordination** — failovers need role changes *and* traffic/service label updates, sequenced correctly.
7. **Replica stability = three checks** — `master_sync_in_progress`, `master_link_status`, `master_last_io_seconds_ago`, not just pod readiness.
8. **Explicit deletion handling** — check `DeletionTimestamp` to avoid loops during foreground deletion.
9. **Bootstrap vs join** — initial cluster setup and new-pod-joining-existing-cluster are different code paths; handle both.
10. **Topology changes always force full resync** — no partial-sync offset preservation across master changes; treat them as expensive.
11. **Validate replica data after replication** — silent key loss is a known engine bug; check DBSIZE after initial sync.
12. **Set maxmemory headroom** — at least 20-30% above expected data size; snapshot load fails silently at the cap.
13. **Avoid DFLY LOAD on masters with live replicas** — journal bypass means replicas miss the loaded data; force resync afterward or load before replicas attach.
14. **Pin to v1.38.0+ minimum** — Bloodraven-managed deployments support only the latest Dragonfly baseline; earlier-version issues are research context, not compatibility targets.
