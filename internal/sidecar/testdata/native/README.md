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
playground scenario 37 verifies reclone/promote/PITR application markers.
