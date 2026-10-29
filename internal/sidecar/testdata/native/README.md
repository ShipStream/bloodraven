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

Then regenerate the native oracle from the repository root:

```sh
BLOODRAVEN_PITR_NATIVE_TEST=1 BLOODRAVEN_PITR_NATIVE_UPDATE=1 go test ./cmd/bloodraven -run '^TestRound2NativeMysqlbinlog$' -count=1 -v
```

Without UPDATE, that opt-in test checks the committed oracle using actual
mysqlbinlog. It compares full versus selected input under UTC for ten stop
and exclusion windows. Two windows deterministically advance one captured
file's outer timestamps and recalculate CRCs, modeling a backward successor
clock without changing transaction bodies or GTIDs. The full/selected SQL
outputs and their hashes are committed, alongside each file's decoded SQL.

Ordinary `go test ./cmd/bloodraven ./internal/sidecar` needs no Docker: it
checks the captured goldens, exact causal downloader order, prefix length,
identity/parse acceptance and conservative retention coverage. The native
oracle proves mysqlbinlog output equivalence, not application restore health;
playground scenario 37 exercises reclone/promote and safe cross-site replay
refusal.


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

`compressed-inner-cut.bin` is the untouched 507-byte MySQL 8.4.11 capture
retained in GPT re-review 5. SHA-256:
`c13ba1b4fbe14e478eacc0e94e5428306022e8384bdf6ef290a63258d1546fda`.
Its transaction `b5e6ac37-d371-11f1-8b67-65731305df52:9` has outer GTID/payload
and inner XID at 09:00 UTC, but inner BEGIN/table-map/write-rows at 09:30 UTC
on 2026-04-15, created using SET TIMESTAMP inside a compressed transaction.
The official client at a 09:30 stop exits zero and prints COMMIT without the
body. The regression keeps these native bytes unchanged, constructs only the
idle successor, and refuses the cut under both restore flags. A separate
native-shaped payload puts XID at the stop and demonstrates mysqlbinlog's
added ROLLBACK. Run the official-client checks with:

```sh
BLOODRAVEN_PITR_NATIVE_TEST=1 go test ./cmd/bloodraven -run 'TestRound2NativeMysqlbinlog|TestRound5NativeMysqlbinlogPayloadCuts' -count=1 -v
```


`compressed-excluded-cut.bin` is GPT re-review 6's untouched 772-byte native
MySQL 8.4.11 straddling-file reproduction. SHA-256:
`8fe0c4785b0af740db890a970cf9224a933aed5dd5a374d953ca8d928373949f`.
Transaction `7f4a1927-d377-11f1-a983-f6bbc6ad1253:3` has an inner cut but is
already covered by the trusted `:1-3` backup bound; required transaction `:4`
is fully before the target. The regression covers that bound and a `:1-2`
control, plus a later non-excluded inner cut in the same file. Authentication
and decoding still run for every transaction; only the dump-covered inner-time
veto is omitted, matching the bound passed to mysqlbinlog's GTID exclusion.
