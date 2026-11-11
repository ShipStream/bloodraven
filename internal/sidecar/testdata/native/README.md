# Native MySQL 8.4 replay fixtures

These files were captured from a disposable `mysql:8.4` server, not generated
wire events. `server-version.txt` and `version.txt` record the actual versions.
`catalog.tsv` records chronological capture labels, original basenames and the
server's `gtid_purged`. The physical CLONE first file resets its basename and
has empty Previous_gtids despite a nonempty purged donor baseline. XA prepare
and commit deliberately span files. LOAD DATA, compressed transactions,
checksum-off files and empty fresh/FLUSH/restart rotations exercise valid
encodings outside strict retention coverage.

Run `bash internal/sidecar/testdata/native/capture.sh` to recapture. It uses
Docker, creates no host listener, uses no existing MySQL/Kubernetes context,
and destroys its disposable datadirs. Timestamps and server UUIDs vary, so
recapture is an intentional fixture update. The utility Dockerfile supplies
the official 8.4.11 mysqlbinlog client absent from the minimal server image.

The fixtures are consumed directly; there is no generated oracle to
regenerate after a recapture. Ordinary `go test ./internal/sidecar` needs no
Docker: `TestInspectTransactionsNativeFixtures`,
`TestOrderingSupportsNativeXAAndChecksumOff`,
`TestArchiveCapturesNativeClonePurgedBase`,
`TestNativeCommitIdentityDistinguishesRelogging` and the replay-prefix tests
parse these captured bytes (identity, parse acceptance, ordering and the
clone's purged base). The mysqlbinlog stop/exclusion window oracle that the
removed `TestPITRNativeMysqlbinlog` checked is gone with that test, and so
are its goldens; mysqlbinlog equivalence is now exercised by the failback
replay below. Playground scenario 37 exercises reclone/promote and safe
cross-site replay refusal.


## Session-compressed captures

`compressed-session.bin` and `compressed-session-idle.bin` are a separate,
genuine MySQL 8.4.11 capture pair. The first has a single-statement transaction
and a multi-statement transaction with SAVEPOINT/ROLLBACK TO SAVEPOINT; both use
`SET SESSION binlog_transaction_compression=ON`. Random test BLOBs ensure
compressed payloads exceed 250 bytes and exercise MySQL's length-encoded size
fields. Its SQL, catalog and server/client version files are committed; `-`
in this pair's catalog means an empty purged GTID set. The two
short pauses preserve both a real stop-prefix boundary and a following idle
barrier. Reproduce just this pair without rewriting the older oracle chain:

```sh
bash internal/sidecar/testdata/native/capture.sh compression
```

The older `compressed.bin` in the original oracle chain is retained for causal
fixture compatibility. That historical capture used SET GLOBAL in the same
session and contains no compressed payload; compression proof tests use the
new session pair. The full capture path now also uses SET SESSION.

`compressed-inner-cut.bin` is an untouched 507-byte MySQL 8.4.11 capture.
SHA-256:
`c13ba1b4fbe14e478eacc0e94e5428306022e8384bdf6ef290a63258d1546fda`.
Its transaction `b5e6ac37-d371-11f1-8b67-65731305df52:9` has outer GTID/payload
and inner XID at 09:00 UTC, but inner BEGIN/table-map/write-rows at 09:30 UTC
on 2026-04-15, created using SET TIMESTAMP inside a compressed transaction.
The official client at a 09:30 stop exits zero and prints COMMIT without the
body. The regression keeps these native bytes unchanged, constructs only the
idle successor, and refuses the cut under both restore flags. A separate
native-shaped payload puts XID at the stop and demonstrates mysqlbinlog's
added ROLLBACK. `TestNativeCutCapturesMatchCatalog` checks these bytes and
their decoded SQL against `compressed-cut-catalog.tsv` without Docker.


`compressed-excluded-cut.bin` is an untouched 772-byte native MySQL 8.4.11
straddling-file reproduction. SHA-256:
`8fe0c4785b0af740db890a970cf9224a933aed5dd5a374d953ca8d928373949f`.
Transaction `7f4a1927-d377-11f1-a983-f6bbc6ad1253:3` has an inner cut but is
already covered by the trusted `:1-3` backup bound; required transaction `:4`
is fully before the target. The regression covers that bound and a `:1-2`
control, plus a later non-excluded inner cut in the same file. Authentication
and decoding still run for every transaction; only the dump-covered inner-time
veto is omitted, matching the bound passed to mysqlbinlog's GTID exclusion.

Both cut captures predate `capture.sh`'s reproducible modes and are kept as
untouched bytes. Their decoded SQL (`compressed-inner-cut.sql`,
`compressed-excluded-cut.sql`) comes from the official 8.4.11 client in the
utility image. `compressed-cut-catalog.tsv` records each file's label, server
version (from its FORMAT_DESCRIPTION event), server_id, SHA-256 and
Previous_gtids. The client version matches
`compressed-session-client-version.txt`. Regenerate the SQL with:

```sh
for f in compressed-inner-cut compressed-excluded-cut; do
  docker run --rm --network none -e TZ=UTC -v "$PWD:/fixtures:ro" bloodraven-pitr-mysqlbinlog:8.4 \
    --no-defaults --verify-binlog-checksum /fixtures/$f.bin > $f.sql
done
```


## Mixed-file failback (MySQL 9.7)

`failback/` is a genuine MySQL 9.7 capture of two servers in one network-less
container (`bash capture.sh failback`, script `failback/capture-inner.sh`):
a planned failover A→B and a planned failback B→A without reclone, with no
rotation at either role change, `binlog_transaction_compression=ON` and
tagged GTIDs (`SET gtid_next='AUTOMATIC:fixture'`). a's
`mysql-bin.000003` is the mixed file of design §4.15: a's era-1 tail, b's
era 2 as replica copies, and a's era 3. b's own file is never archived.
`mysql-bin.000001` is the backup's history. `targets.tsv` records, at each
target, the primary's `gtid_executed`, a row checksum (and one without the
excluded `skip` row, whose GTID is in `skip.tsv`) and `UTC_TIMESTAMP(6)`;
`records.tsv` holds the promotion records a sidecar would write.

`go test ./cmd/bloodraven -run TestPITRNativeFailbackPlans` checks every plan
against those sets without Docker. With Docker,
`BLOODRAVEN_PITR_NATIVE_TEST=1 go test ./cmd/bloodraven -run TestPITRNativeSegmentReplay`
replays each plan's segments with the mysqlsh image's `mysqlbinlog
--include-gtids` into one `mysql` session on a fresh `mysql:9.7` and compares
the rows and `gtid_executed` with the captured source. Recapturing changes
UUIDs and times; the tests read them from the TSV files.


## Effect-digest pairs (MySQL 9.7, and 8.4 to 9.7)

`effect/` is a genuine MySQL 9.7 capture of a source `s` (server_id 101) and
its replica `r` (102, `log_replica_updates`) in one network-less container
(`bash capture.sh effect`, script `effect/capture-inner.sh`). `r` replicates
everything from `s`'s first transaction, so every GTID is archived twice: as
an origin copy in `s-mysql-bin.*` and as a replica copy in `r-mysql-bin.*`.
The two servers cut rows events at different sizes
(`binlog_row_event_max_size` 2048 and 512) and only `r` compresses
transactions, except for a few transactions where only `s` does or neither
does. Some tables are defined differently on `r` (changed with
`sql_log_bin=0`, as an administrator does in a rolling schema change). Both
servers rotate at the same transactions:

| File | Holds |
|---|---|
| `mysql-bin.000001` | The schema: the backup's history. |
| `mysql-bin.000002` | The faithful workload (below); then what is not comparable: statement and mixed format, `CREATE TABLE … SELECT`, a `SAVEPOINT`, a `JSON` table and a partial JSON update. |
| `mysql-bin.000003` | `drift`: `r`'s row was changed with `sql_log_bin=0`, so its copy of `s`'s next update of that row has another before image. GTID, commit time, origin server and emptiness agree. |
| `mysql-bin.000004` | `injected`: a repair transaction run on `r` under `s`'s GTID (`SET gtid_next`) that changes another table; `r` then skips `s`'s original. |
| `mysql-bin.000005` | `injected-values`: the same with a repair that changes the same table, with other values. |

The faithful workload, by what the two copies must be:

- **Equal.** Multi-row statements larger than an event, several tables in
  one transaction, updates and deletes, every common column type (and BLOB,
  GEOMETRY, VECTOR), generated columns, a partitioned table, upserts,
  foreign-key cascades (update, delete, set null), a trigger, MyISAM, a
  tagged GTID, a 301-column table, multi-table `UPDATE` and `DELETE`,
  `INSERT … SELECT`, `LOAD DATA` (row format), a 300 KB value, `ROWS_QUERY`
  events. Tables **without a primary key**, or with only a nullable unique
  key (`nopk-*`, `uniqnull-*`): `r` applies an `UPDATE` or `DELETE` of such a
  table by a scan and logs the rows in the order its scan finds them, so
  the two copies hold the same row images in another order (the same
  statements on a table with a primary key, `pk-*`, keep the order). Tables
  that differ on `r` without changing the row images: more indexes, a
  functional index, another default, a table partitioned on `s` only.
- **Layout** (the same tables, encoded differently; not comparable):
  `SET SESSION binlog_row_image = MINIMAL` or `NOBLOB` on `s` (`r` logs
  `FULL` images), a column added on `r` only, a column widened on `r` only
  (`INT` to `BIGINT`, `replica_type_conversions=ALL_NON_LOSSY`), and a
  generated invisible primary key on `r` only
  (`REQUIRE_TABLE_PRIMARY_KEY_CHECK=GENERATE`). The data is identical on
  both servers in every one of these.

`effect-mixed/` is the same kind of pair with a MySQL 8.4 source and a 9.7
replica, each in its own container on an internal network
(`bash capture.sh effect-mixed`, script `effect-mixed/capture.sh`): plain
changes, foreign-key cascades (an 8.4 source does not log the rows a cascade
changes, and the 9.7 replica does not add them), a trigger, a partitioned
table, and a table without a primary key whose rows `r` logs in another
order.

`cases.tsv` lists each labelled statement's GTIDs and how the two copies'
effect digests (`pitrplan.EffectDigest`, issue #268) must compare:

| Expectation | Meaning | The planner |
|---|---|---|
| `equal` | The same tables, layout and row bytes. | accepts |
| `layout` | The same tables, encoded differently. | does not compare the rows |
| `tables` | Other tables, or the same in another way or order. | refuses |
| `rows` | The same tables and layout, other row bytes. | refuses |
| `uncomparable` | A copy has no digest. | does not compare |

`phases.tsv` records `s`'s `gtid_executed` and `UTC_TIMESTAMP(6)` at each
rotation (in `effect-mixed`, after the schema and at the end).

`go test ./internal/sidecar -run TestEffectDigestNativeFixtures` checks every
GTID that occurs in more than one fixture file against `cases.tsv`, and that
the capture is what it claims (different cuts, one-sided compression, rows
in another order on the tables without a primary key, shorter row images
under `MINIMAL` and `NOBLOB`).
`go test ./cmd/bloodraven -run TestPITRNativeEffectPlans` plans over both
sites: the faithful files plan (9.7, and 8.4 to 9.7), the drifted copy
refuses `PITRArchiveNamespaceReuse` on its row data alone, the repair of
another table on commit time, origin server and "they change different
tables", and the repair of the same table on "the same tables with
different row data". With Docker,
`BLOODRAVEN_PITR_NATIVE_TEST=1 go test ./cmd/bloodraven -run 'TestPITRNativeEffect(Mixed)?Capture'`
runs the capture scripts again and checks the fresh captures the same way
(the mixed one is skipped when the `mysql:8.4` image is not present).

What the capture and one-off runs of the same workload established (MySQL
9.7.4; the one-off captures are not committed):

- An origin copy and a replica copy have equal digests for every comparable
  case above, with the events cut differently and with compression on
  either side only. So does a server that applied the source's binlog
  through `mysqlbinlog | mysql`.
- A replica does not keep the order of the rows of an `UPDATE` or `DELETE`
  on a table without a primary key or NOT NULL unique key (9.7.4, and 9.7.4
  replicating from 8.4.11): the first version of the digest hashed the row
  images in order and refused these healthy archives (review R3). The rows
  part of the digest is therefore a multiset of bytes.
- A session's `binlog_row_image` needs `SESSION_VARIABLES_ADMIN`; an
  unprivileged user gets error 1227 (measured in review R3). The sidecar
  audits only `@@global.binlog_row_image`.
- `JSON` row images are the stored binary document. They stayed equal
  between a source and a replica that replicated from the start, including
  after in-place `JSON_SET`/`JSON_REMOVE`/`JSON_REPLACE` and after partial
  JSON diffs were applied. After the replica's table was reloaded logically
  (`INSERT … SELECT CAST(CAST(doc AS CHAR) AS JSON)`, as a dump and load
  does), the same document was stored without the slack an in-place update
  leaves (`JSON_STORAGE_FREE` 0 instead of 196), and the next update of
  another column of that row had different row images on the two servers.
  A table with a `JSON` column is therefore not comparable.
- A partial JSON update is a `PARTIAL_UPDATE_ROWS` event on the origin and a
  full `UPDATE_ROWS` event on a replica without the option.
- A generated invisible primary key
  (`sql_generate_invisible_primary_key` on the source only) is replicated
  with the table, so both copies hold the column.
