# The card you keep

Everything from here is reference. Nothing in this topic is new; all of it is spread across the six
units behind you, which is exactly the problem — at 03:00 nobody reads a course. Print this page, or
keep the tab open. It is the one screen the rest of the course was building toward.

Two rules for using it. **Numbers marked *shipped* are CRD defaults; numbers marked *playground* are
what the local group overrides them to.** And anything with a date on it belongs to the
[version appendix](../sources.html#version-appendix) rather than to this card — check there before you
quote a version, an issue number or a documentation defect.

## Timings and thresholds

| Setting | Shipped default | Playground | What it actually controls |
|---|---|---|---|
| `pollInterval` | `2s` | same | The poll tick. One `SELECT @@read_only` per site, in parallel, each under a 5 s ceiling. |
| `failureThreshold` | `3` | same | Consecutive failed polls before `unreachable`. **Detection = `pollInterval` × `failureThreshold` = 6 s.** |
| `recoveryThreshold` | `2` | same | Consecutive writable polls before `writable`. **Not a term in the detection sum.** |
| — | adaptive | same | Undocumented backoff: past `failureThreshold` the whole loop's interval doubles per extra failure, exponent capped at 4, hard cap 30 s. One site down ⇒ a second fault takes 30 × 3 = 90 s to detect. |
| `failoverCooldown` | `5m` | `30s` | Gates **automatic promotion only**, at one call site, immediately before the promotion call. |
| `connectionDrainTimeout` | `30s` | same | Bounds the post-promotion retry window for evicting sessions from a fenced former primary — only while it is reachable. |
| `replication.maxLagSeconds` | `300` | `30` | The `ReplicationLagging` Degraded reason. **Never a promotion gate.** Skipped entirely for `role: read-only` sites. |
| `replication.readOnlyMaxLagSeconds` | *(nil — inherits `maxLagSeconds`)* | `10` | Reader-endpoint membership only. An explicit `0` means zero reported lag, and is not the same as unset. |
| `dns.ttl` | `60` | `10` | `recordTTL` on the `DNSEndpoint`. The floor on how long a stale answer survives. |
| `sidecar.leaseTimeout` | `20s` | same | Rule #2 self-fence window. CEL: ≥ 3 s, and ≥ 3 × `peerCheckInterval`. |
| `sidecar.peerCheckInterval` | `5s` | same | Sidecar tick. CEL: ≥ 1 s. 20 / 5 = **4 ticks** of silence before a lease fence. |
| relay-log drain | `30s` | same | Step 3 of the promotion. Non-fatal on timeout. |
| source convergence | `20s` | same | A separate poll stage, not a failover step. |
| `plannedFailover.maxLagWait` | `5m` | same | The GTID-superset gate. Rollback lives here. |
| `plannedFailover.drainTimeout` | `30s` | same | Connection drain before the write endpoint moves. Exhaustion proceeds anyway. |
| `plannedFailover.onCooldown` | `reject` | same | `defer` parks the request instead of refusing it. |
| `dragonfly.plannedFailover.maxSyncWait` | `30s` | same | Also the `REPLTAKEOVER` timeout argument; the client adds 5 s of I/O grace. |
| Dragonfly emergency budget | `10s` | same | Hard-coded. MySQL failover is never delayed by cache. |
| `backup.maxLagSecondsForSource` | `300` | — | Picks the backup source. A different setting from `maxLagSeconds`, with the same default. |
| `backup.pitr.archivePollInterval` | `60s` | — | Archiver ticker beside inotify. |
| `backup.pitr.maxBinlogSize` | `100M` | — | Written *before* `spec.mysqlConf`, so your override wins. |
| `backup.retention` | `7` | — | Backups kept per profile. |
| Measured failover, clean | **12.0 s** | — | Nine-plus recorded runs, 12.004–12.02 s, one outlier at 13.008 s. |
| Measured failover, full drain | **36.0 s** | — | 6 s detection + a 30 s drain that spent its whole budget. |

## The five condition reasons

There are exactly five, they come straight from `EvalCrossSite`, and **`Failover` is not one of them.**

| Reason | The topology it describes |
|---|---|
| `Healthy` | Exactly one core site writable, none unreachable. `Degraded` is `False`. |
| `Degraded` | Everything else that is not one of the three below — including *the promotion is about to happen* and *a writable non-promotable site needs fencing*. |
| `SplitBrain` | More than one **core** site writable. Readers are excluded from the tally. |
| `NoPrimary` | No core site writable and none unreachable. Will not self-heal; the matrix refuses to auto-elect. |
| `TotalLoss` | Every core site unreachable. |

Replication adds its own reasons to `Degraded`: `ReplicationLagging`, `ReplicationBroken`,
`ReplicationError`, `ReplicationSourceMismatch`. All four skip `role: read-only` sites entirely.

**Evaluation order** (a row fires only if every row above declined):
fence-first early return → `TotalLoss` → `SplitBrain` → failover → `NoPrimary` → degraded-with-peer-down → `Healthy`.

## The nine promotion steps, with fatality

`FailoverController.Execute`, in order. **Fatal** means the promotion aborts; everything else logs and
carries on.

| # | Step | On error |
|---|---|---|
| 1 | `SET GLOBAL super_read_only = ON` on the old primary | warns only |
| 2 | Kill application connections on the old primary | warns only |
| 3 | Relay-log drain on the candidate, 30 s budget | warns, promotes anyway |
| 4 | `STOP REPLICA` on the candidate | **fatal** |
| 5 | `RESET REPLICA ALL` on the candidate | **fatal** |
| 6 | `SELECT @@global.gtid_executed` → `promotionGtidExecuted` | warns only |
| 7 | `SET GLOBAL super_read_only = OFF` | **fatal** |
| 8 | `SET GLOBAL read_only = OFF` | **fatal** |
| 9 | Writable confirmation, synchronously | logs, skips the DNS flip, returns |

Then, outside the sequence: the durable failover record and the counter are stamped **before** DNS.
Node taints and source convergence are poll neighbours, not steps.

## The four Service kinds

`2 × len(sites) + 2` objects. Eight for a three-site group.

| Service | Selector | Application use |
|---|---|---|
| `mysql-<group>-primary` | instance + `role=primary` | **writes** |
| `mysql-<group>-replicas` | instance + `role=replica` + `healthy=yes` | **reads** |
| `mysql-<group>-<site>` | name + instance + site, **plus `healthy=yes` on a `read-only` site** | site-pinned tooling only |
| `mysql-<group>-<site>-internal` | name + instance + site, no health gate, `publishNotReadyAddresses: true` | never yours — sidecar and peer traffic |

`role="fenced"` matches neither shared Service. A `read-only` site earns `healthy=yes` only when
**all five** hold: converged source, replicating, non-nil lag, canonical *direct* source host, and lag
within `readOnlyMaxLagSeconds`.

## Metrics, and their label sets

Label sets are **not uniform**. Read them before you copy a selector between rules.

| Metric | Labels |
|---|---|
| `bloodraven_site_state` | `site`, `state` — a one-hot over `writable`/`read-only`/`unreachable`/`unknown` |
| `bloodraven_replication_lag_seconds` | `site` only — **`-1` means not replicating**, not a small lag |
| `bloodraven_replication_running` | `site`, `thread` (`io`/`sql`) |
| `bloodraven_state_transitions_total` | `site`, `from`, `to` |
| `bloodraven_poll_latency_seconds` | `site` — its `_count` is the loop's heartbeat |
| `bloodraven_failovers_total` | `target_site` |
| `bloodraven_planned_failovers_total` | `target_site`, `result` |
| `bloodraven_divergent_transactions` | `site` |
| `bloodraven_primary_reassert_total` | `site` |
| `bloodraven_split_brain_auto_resolve_total` | `prefer_site` |
| `bloodraven_dns_flips_total` | `site` |
| `bloodraven_replication_source_state` | `namespace`, `group`, `site`, `state` |
| `bloodraven_archiver_backlog_files` | `namespace`, `group`, `site` |
| `bloodraven_backup_last_success_timestamp_seconds` | `group`, `profile` |
| `bloodraven_backup_verified_timestamp_seconds` | `group`, `profile` |
| `bloodraven_keyring_phase` | `mysql_namespace`, `failover_group`, `site`, `phase` — one-hot |

Four words for two concepts: `namespace` / `mysql_namespace`, and `group` / `failover_group`. That is
not a typo in this table.

## Annotations you apply by hand

| Key | Value | Notes |
|---|---|---|
| `bloodraven.shipstream.io/planned-failover` | `<site>` or `<site>:maxLagWait=10m` | Consumed and cleared. Unknown override keys are rejected. |
| `bloodraven.shipstream.io/reclone-site` | `<site>:<divergentGtid prefix ≥8 chars>` | Cold form, when nothing is recorded: `<site>:confirm=<group>`. |
| `bloodraven.shipstream.io/rotate-keyring` | `<site>` | Refused on the active primary. |

Written **by the operator**, never by you: `bloodraven.shipstream.io/last-failover` and
`…/last-failover-target`, RFC3339 at second precision, as a pair.

## Roles, in one line each

- **`primary-candidate`** — the only promotable role. Counted in every tally.
- **`dr-only`** — never promoted, but **counted** in `coreCount` and the tallies. A full topology participant that cannot win.
- **`read-only`** — never promoted, invisible to the matrix, never tainted, refused as a backup source, and skipped by every replication condition. Not a spare.

## Glossary

**Active site** — the one site `status.activeSite` names as the writable authority. One name, or
empty; empty means authority is ambiguous, and every endpoint is shed.

**Anti-flap cooldown** — `failoverCooldown`. Suppresses automatic promotion and nothing else.

**Core site** — any site whose role is not `read-only`. What the matrix counts.

**Divergent GTID set** — `GTID_SUBTRACT(old primary, new primary)`: the transactions the old primary
holds that the new one never saw. Its cardinality is your lost-transaction count.

**Fencing** — `SET GLOBAL super_read_only = ON`. Blocks writes even from `SUPER`. **Closes no sockets**,
so surviving sessions keep serving stale reads. At the Service layer, a separate thing: stamping the
pod `role=fenced`, which matches neither shared selector.

**GTID set** — the transaction history a server has executed, as `uuid:interval[:interval…]`,
comma-separated across UUIDs. In MySQL 9.x a set may carry a user tag, `uuid:tag:interval`, and the tag
is part of the identity — `uuid:A:1-3` and `uuid:B:1-3` are six transactions, not three.

**Ordered update** — the operator-driven rollout of a spec change: standby first, then a real failover,
then the old active. Triggered by spec-hash drift; not cooldown-gated.

**Promotability** — exactly `role == primary-candidate`. Not earned by having the freshest data.

**RPO / RTO** — how much recently committed data you accept losing / how long you accept being unable
to write. Bloodraven's RPO on sudden primary loss is not zero, by design; its whole engineering budget
goes to RTO.

**Self-fence** — the sidecar setting `super_read_only=ON` on its own MySQL without asking anyone.
Rule #1: the known authoritative active site is somebody else. Rule #2: the operator *and every peer*
have been silent past `leaseTimeout`.

**Safety net** — a *different* thing from self-fencing: a one-shot at sidecar startup that fences
first and asks afterwards, and completes before the fencing monitor exists. `safety net:` in a log
means a pod that has never been allowed to write; `SELF-FENCED:` means one that was writing and lost
the argument.

**Sealed** — the steady-state keyring phase: the keyring file is projected read-only from the escrow
Secret, so mysqld physically cannot add a key. `Unsealed` is mid-flight, and rotation re-enters it
*from* `Sealed`, so the phase string alone is ambiguous — read it beside `unsealReason`.

**Source convergence** — the independent poll stage that repoints replicas at the current authority.
Demands a **direct** source; a replica chained off another replica does not count as converged.

**Split brain** — more than one core site writable at once. `sitePriorities` does not prevent it and
merges nothing; it is a standing decision about whose unreplicated writes you will discard.

**Star topology** — every replica replicates directly from the active site. Bloodraven does not accept
chains.

**`super_read_only` vs `read_only`** — `super_read_only` also blocks users holding `CONNECTION_ADMIN`
or `SUPER`, which plain `read_only` does not. Setting `super_read_only=ON` implicitly forces
`read_only=ON`; clearing `read_only` implicitly clears `super_read_only`.

## Where this leaves you

You have a card. Use it to stop looking things up, and use the version appendix to check anything on
it that carries a date. What you can now say about a group you are handed is what the whole course was
for: what it will do next, what it will refuse to do, what it will cost you when it does, and which of
those numbers you measured yourself.
