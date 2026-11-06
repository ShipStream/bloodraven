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
