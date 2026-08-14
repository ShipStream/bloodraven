# Quiz — Backups and the binlog archiver

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A scheduled backup of `playground` fires. `iad` is the active site and is `writable`; `pdx` is `read-only` and replicating but reports `secondsBehindSource` of 900, well past the 300-second default; `reader` is idle with `role: read-only`. Which site runs the dump, and what reason string lands in status?

- `pdx`, reason `"replica-preferred"` — replica-first is the whole point of the setting
- `iad`, reason `"primary-fallback"`
- `reader`, reason `"replica-preferred"` — an idle read-only site is exactly what you want to dump from
- No site; selection errors with no healthy source until `pdx` catches up

**Correct option index:** 1

**Explanation:**

Replica-first is conditional, not unconditional: a replica qualifies only while `secondsBehindSource` is at or below `maxLagSecondsForSource` (default 300). At 900 seconds `pdx` fails the gate, so option 1 is wrong. `reader` carries `role: read-only` and is excluded from the replica pool outright — the tempting 'spare site' is the one site that can never source a backup, so option 3 is wrong. Option 4 inverts the design: the fallback exists precisely so a stale replica does not block the backup, and `iad` is `writable` and promotable, so it takes the job and status records `"primary-fallback"` (objective 2).

## Question 2

**Type:** MULTIPLE_CHOICE

To keep dump load off both primary candidates, you set `sourceSiteOverride: reader` on a MysqlBackup for `playground`. What happens?

- The dump runs on `reader` and status records `"override"` — an explicit override outranks the role check
- The dump runs on `reader` but status records `"replica-preferred"`, since the reader is a replica
- Source selection returns an error saying the override names a read-only site, which cannot be a backup source
- The override is silently ignored and selection falls through to replica-first on `pdx`

**Correct option index:** 2

**Explanation:**

Options 1 and 2 assume the override is a trump card over the role; it is not. Role is checked *first*, and a `read-only` reader is rejected with an explicit error rather than being dumped from — the same non-promotable-therefore-not-authoritative logic from Unit 1, showing up here as non-sourceable. Option 4 is the quieter and more dangerous misconception: rejection is loud and no Job is created, so you find out immediately instead of silently getting a source you did not ask for. A reader is a legitimate site to read from with a client, but never a site Bloodraven will hand a backup job to (objective 2).

## Question 3

**Type:** MULTIPLE_CHOICE

A clean archiver scan has just completed on the primary of `playground`. `mysql-bin.index` lists `mysql-bin.000041` through `mysql-bin.000045`. Which file is not in object storage, and why?

- `mysql-bin.000041` — the oldest file, already removed by the retention sweep
- `mysql-bin.000045` — it is the active binlog, the tail of the index, and the archiver drops it
- None of them; a clean scan uploads every entry the index lists
- `mysql-bin.000044` and `mysql-bin.000045` — the archiver keeps one sealed file in reserve to avoid racing MySQL

**Correct option index:** 1

**Explanation:**

The archiver reads the index and drops the last entry, because that is the file MySQL is writing to right now — only sealed binlogs upload. Option 1 confuses archival with pruning: pruning is driven by a cutoff timestamp fetched from `/pitr-cutoff`, is rate-limited, and would remove the object from storage *and* the manifest, not leave it un-uploaded. Option 3 is the assumption that makes people over-trust their RPO — there is always an unarchived tail on a live primary. Option 4 invents a safety margin the archiver does not have; sealed files are safe to read the moment they are sealed, and holding one back would only widen the gap (objective 3).

## Question 4

**Type:** TRUE_FALSE

PITR is enabled on `playground` and archival is healthy. The primary's node and its PVC are destroyed. A PITR restore can still replay right up to the last transaction the primary committed.

**Correct answer:** false

**Explanation:**

The reversal: healthy archival does not mean complete archival. The previously-active binlog — everything written since the last rotate — lived on the destroyed PVC and is gone forever, so the replay material stops at the last sealed file that reached storage. The second limit compounds it: restoring from the surviving replica cannot reach past the async-replication cutoff, because transactions the old primary committed but never shipped were never in the replica's binlog stream. PITR narrows RPO to the rotation cadence only when the tail survives, which is one of the real costs of any backup that shares, or fails with, the data's own storage (objectives 1, 3).

## Question 5

**Type:** SHORT_ANSWER

You want to shrink the unarchived tail on `playground` — the window of committed transactions that PITR would lose if the primary's PVC were destroyed right now. What do you change, and what does it cost you?

**Sample answer:**

Lower `spec.backup.pitr.maxBinlogSize` below its `100M` default. It is forwarded to MySQL as `max_binlog_size`, so smaller files rotate more often; each rotation seals the current file and makes it eligible for upload, so the active, never-uploaded file holds less. The cost is many more objects in storage and more upload churn. It is written before `spec.mysqlConf` is merged, so a `spec.mysqlConf` entry for the same key would override it.

**A full-credit answer shows:**

A strong answer names `maxBinlogSize` (default `100M`) as the control and explains the mechanism: only sealed binlogs upload, the active file is dropped, so faster rotation means a smaller unarchived tail. It should name a cost — object count, upload churn — and should not reach for `archivePollInterval` or `maxLagSecondsForSource` instead. Bonus credit for noting the before-overrides ordering, or that no setting removes the tail entirely.

**Explanation:**

`archivePollInterval` (60s) only changes how soon a *sealed* file is noticed, not how much data sits in the unsealed one, and inotify usually beats the ticker anyway. `maxLagSecondsForSource` (300) chooses which site runs a full dump and has nothing to do with binlogs. Rotation cadence is the only lever on the tail, and it never reaches zero — there is always a file MySQL is currently writing to (objective 3).
