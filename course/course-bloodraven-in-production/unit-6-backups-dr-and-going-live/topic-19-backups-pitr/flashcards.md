# Flashcards — Backups and the binlog archiver

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Backup source reason string `"replica-preferred"` — what had to be true for the operator to record it?

**Back:** The chosen site's observed state was `read-only`, it was actually replicating, and its `secondsBehindSource` was at or below `maxLagSecondsForSource`.

---

**Front:** Backup source reason string `"override"` — what does it tell you?

**Back:** The MysqlBackup carried a `sourceSiteOverride` that named a configured, observed, non-read-only site, so replica-first selection never ran.

---

**Front:** A non-primary site of `playground` is replicating and well inside `maxLagSecondsForSource`, but the operator observes it as `writable`. Is it eligible as a backup source?

**Back:** No. `"replica-preferred"` requires the observed state to be `read-only`; a writable non-primary is skipped and selection falls through.

---

**Front:** `spec.backup.maxLagSecondsForSource` — default value and what it gates

**Back:** 300 seconds; above it a replica loses the dump job and selection falls back to the primary.

---

**Front:** Which 300-second default drives only the `ReplicationLagging` Degraded condition and never chooses a backup source?

**Back:** `spec.replication.maxLagSeconds` — a different field from `spec.backup.maxLagSecondsForSource`, which shares the same default.

---

**Front:** `spec.backup.pitr.archivePollInterval` — default and purpose

**Back:** 60s; the belt-and-braces ticker that runs alongside inotify so a rotation is detected late rather than missed.

---

**Front:** `spec.backup.pitr.maxBinlogSize` — default value and when it is applied

**Back:** `100M`, written into the generated my.cnf only when PITR is enabled.

---

**Front:** Is `max-binlog-size` written before or after `spec.mysqlConf` is merged, and what does that mean for you?

**Back:** Before — so a `spec.mysqlConf` override still wins, unlike `gtid-mode` or `log-bin`, which are written after and cannot be weakened.

---

**Front:** Why does the binlog archiver watch the binlog *directory* rather than `mysql-bin.index`?

**Back:** MySQL rewrites the index atomically — write `.index.tmp`, then rename — so a file-level watch would be lost after the very first rotate.

---

**Front:** What does the archiver do when the binlog index holds fewer than two entries?

**Back:** Nothing — either no binlogs exist yet or only the active one does, so there is nothing sealed to upload.

---

**Front:** What gates whether a sidecar's archiver uploads anything at all, and what does that buy you after a failover?

**Back:** A `@@read_only` check each cycle: only the primary archives, so a promoted replica starts archiving on its next scan with no extra wiring.

---

**Front:** `/pitr-cutoff` — who calls it, with what, and what happens if the operator is not wired up?

**Back:** The sidecar archiver GETs it on the operator with `namespace`, `group` and `profile`; with no retention config wired in there is no pruning at all.
