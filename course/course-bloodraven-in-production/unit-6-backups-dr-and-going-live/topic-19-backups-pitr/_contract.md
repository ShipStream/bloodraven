# Backups and the binlog archiver

**Unit:** 6 — Backups, disaster recovery, and going live
**Objectives (unit-numbered):**
1. Choose S3 or PVC storage for `playground` and say what PVC-local costs you   [obj 1]
2. Say which site a backup runs from and why a `read-only` reader is never eligible   [obj 2]
3. Trace a sealed binlog from rotation to object storage, and name what can never be archived   [obj 3]

## Topic generation prompt

Open on `playground`: three sites, a counter application reading and writing through it, and a failover story that is now complete. None of it survives losing the cluster or a bad `DELETE` at 14:02 last Tuesday. That is what backups and PITR are for, and they are a separate subsystem with separate failure modes.

Teach storage choice first: S3-style object storage or a PVC, and be blunt that PVC-local backups are not durable — a backup that shares a failure domain with the data it protects is an assumption, not a backup. Then source selection, because it is the part operators get wrong. `selectSourceSite` picks a replica first and falls back to the primary, and it records exactly which happened via three reason strings you will see in status: `"override"`, `"replica-preferred"`, `"primary-fallback"`. Sites with `role: read-only` are excluded as backup sources outright, and an explicit `sourceSiteOverride` naming one is **rejected** with an error — the same non-promotable logic from Unit 1 shows up here as non-sourceable. Give `maxLagSecondsForSource`, default 300, as the gate that sends a stale replica's job to the primary instead.

Then the binlog archiver, which lives in the sidecar. Teach one mechanism deeply, because the reason behind it is the teachable part: the archiver watches the **directory**, not the `.index` file, because MySQL rewrites the index atomically — write to `.index.tmp`, then rename — so a file-level watch would lose its watch after the very first rotate. Say that inotify is an optimisation and not the only path: a poll ticker runs alongside it and a best-effort initial scan runs at startup, so rotation is detected with worse latency but never missed. Give `archivePollInterval`, default 60s. Then the role gate: only the primary archives, gated on `@@read_only`, so after a failover the former replica simply takes over archiving with no extra wiring. Then the rule that surprises people: only **sealed** binlogs upload. The last entry in the index is the one MySQL is writing to right now, and it is dropped. Give `maxBinlogSize`, default `100M`, applied only when PITR is enabled and written **before** user overrides, so `spec.mysqlConf` can still win — smaller files mean a shorter unarchived tail. Name the `/pitr-cutoff` endpoint the archiver calls with `namespace`, `group` and `profile`. Cover pruning honestly: it is rate-limited, it fails silently by design so a transient 503 from the operator does not leak into archiver status, and without operator wiring there is no pruning at all.

Close on the hard limits, which are the point of the whole topic. On PVC loss the previously-active binlog lived on the destroyed PVC and is gone forever. PITR cannot reach back past the async-replication cutoff: transactions the old primary committed but never shipped are not in the replica's binlog stream and therefore not in PITR's replay material. And the operational sting — a backup storage failure has **no** data-plane impact at all. MySQL keeps serving reads and writes while your PITR RPO silently drifts. That is the definition of a silent degradation, and it is exactly why topic 4 exists. Do NOT cover verification, restore, or the confirmation token — topic 2 owns them.

## Requested activities

- READ: 900-1100 words. Open on what a failover group does not protect. Cover S3 vs PVC and the durability cost, source selection with the three reason strings and the reader rejection, `maxLagSecondsForSource: 300`, the directory-watch reason, inotify-plus-ticker, `archivePollInterval: 60s`, the `@@read_only` role gate and post-failover handover, sealed-only upload, `maxBinlogSize: 100M` and the before-overrides ordering, `/pitr-cutoff`, silent pruning, and the two hard limits. End on the silent-degradation framing. Use one `flow` widget tracing a binlog from `ROTATE` through sealed, uploaded, and pruned; optionally one `compare` widget on S3 versus PVC storage across durability, failure domain, and what survives cluster loss.
- FLASHCARDS: `"override"` / `"replica-preferred"` / `"primary-fallback"`, `maxLagSecondsForSource` 300, `archivePollInterval` 60s, `maxBinlogSize` 100M, sealed vs active binlog, the directory-watch reason, the `@@read_only` archive gate, `/pitr-cutoff`, pruning-without-wiring. 10-12 cards.
- QUIZ: 5 questions. Which site a backup runs from given a lagging replica and a healthy primary; what happens when `sourceSiteOverride` names the reader; which binlog file is not in the bucket right now and why; what PITR can recover after PVC loss; and what a user changes to shrink the unarchived tail.

## Handoff

**Inherits:** From Unit 5 — the learner can explain every fence they see, and no read-only site surprises them.
**Leaves:** The learner can configure backups and PITR for `playground`, name the backup source and its reason string, and state exactly what is not recoverable.
**Do not cover:** Verification, restore, the RFC 3339 confirm token (topic 2), encryption at rest (topic 3), alerting (topic 4).
