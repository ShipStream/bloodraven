# Proposal: PITR Archiving of Binary Logs Encrypted by MySQL (`binlog_encryption=ON`)

**Issue:** none yet (PR #280, "Known limitations and found, not fixed", item 3)
**Status:** Draft. The recommendation is **pending Colin's sign-off**; nothing here is implemented.
**Branch:** work package WP3 of the PR #280 follow-ups

All names introduced here (reasons, Events, log messages, fields) are **proposed**. Citations are `path:line` at `56f7b2fb` (PR #280's head plus PR #281). "Measured" means observed on a `mysql:9.7` (9.7.4) server in a disposable container on 2026-11-11; the commands and the test that pins each fact are named.

## Summary

A group with `spec.encryptionAtRest` and `spec.backup.pitr` archives no binary log. `spec.encryptionAtRest` renders `binlog_encryption=ON` unless `coverage.binaryLog` is `false`, the PITR archiver reads binlog files from the data volume, and it has no code that decrypts them. Admission accepts the combination, the primary reports `PITRArchiveReady=False`/`ArchiverError` with the text `invalid binlog magic`, and until this branch the encryption-at-rest page said archived binlogs are "encrypted by MySQL on disk *and* re-encrypted by the archiver".

Three options are described: decrypt in the sidecar (1), refuse the combination loudly (2), and avoid encrypted files, either by turning binary log encryption off for PITR groups (3a) or by reading through the replication protocol (3b). The recommendation is option 1 as the fix, with the status half of option 2 as an interim step in the next release.

## Verified facts

### What the code does

| Step | Code | With an encrypted file |
|---|---|---|
| Setting | `encryptionMySQLSettings` renders `binlog-encryption` from `coverage.binaryLog` (`internal/controller/encryption.go:204`), default `true` (`api/v1alpha1/encryption_helpers.go:98`) | `binlog_encryption=ON` on every site |
| Admission | CRD CEL only, no webhook. The rules on `spec.encryptionAtRest` require `spec.tls`, forbid turning it off and pin the keyring paths (`api/v1alpha1/types.go:19-20`, `:57-58`). None mentions `spec.backup.pitr`. `PITRMysqlConfAccepted` checks `spec.mysqlConf` only (`internal/controller/pitr_mysqlconf.go:28-43`) | Accepted |
| Upload | `archivePinned` parses the whole file before the first `Put` (`internal/sidecar/binlog_upload.go:66`); `inspectBinlog` compares the first 4 bytes with `FE 62 69 6E` (`internal/sidecar/binlog_stream.go:115-117`) | `invalid binlog magic`; nothing is uploaded, no manifest row. Ciphertext never reaches the archive |
| Scan result | `scanFiles` reports each file through `fail` (`internal/sidecar/binlog_scan.go:165-171`) | `lastError: archive mysql-bin.NNNNNN: invalid binlog magic`, `backlogFiles` = sealed files, `uploadFailures` and `consecutiveFailedScans` rise |
| Condition | The tracker turns a ready store with `lastError` on 2 consecutive failed scans into `ArchiverError` (`internal/controller/pitr_archive_condition.go:577-590`), with a Warning `PITRArchiveNotReady` (`:978-980`) | `PITRArchiveReady=False`, reason `ArchiverError`, message `<site>: archive mysql-bin.NNNNNN: invalid binlog magic` |
| Header reads | `localBinlogHeader` skips the magic and hands the rest to the go-mysql parser (`internal/sidecar/binlog_scan.go:38-78`) | `get event err EOF, need 1768714341 but got <n>`: the parser reads the key name `MySQLReplicationKey…` as an event header (`0x696C7065` is "epli") |
| Promotion set | `promotionSet` ends on a header it cannot read (`internal/sidecar/promotion_record.go:558-560`, `:661-664`) | The record is written without `promotionGtidExecuted`; INFO `promotion set not recorded` |
| Tail drain | `findTail` → `examineOrigin` → `tailPrevious` → `localBinlogHeader` (`internal/sidecar/binlog_tail.go:458-466`, `:532-538`); the outcome is `tailUndecided` (`:300-303`) | `tail.state: Blocked`, reason `read the header of mysql-bin.NNNNNN: …`, WARN `binlog tail not archived while read-only`; no `lastError` of its own |
| Origin walk | `errEncryptedBinlog` (`internal/sidecar/binlog_origin.go:71`, `:106-107`) | Never reached on a real file: the header read before it fails first. The message quoted in PR #280 ("the binlog file is encrypted") therefore does not appear in any status or log |
| Reclone | The operator waits up to 2 minutes for the recipient's tail (`internal/controller/reclone_archive_tail.go`) | The tail of a read-only site never reports `Drained`, so every reclone waits its 2 minutes and raises Warning `PITRArchiveTailNotArchived` (from the code; not run) |
| Restore | The archive holds `archive.json`, manifests with promotion records, and no binlog | A point-in-time target after the backup has nothing to replay. No restore ever sees an encrypted file. (`mysqlbinlog` on one says `ERROR: Reading encrypted log files directly is not supported.`, measured) |

### What a real server shows (docker)

`TestPITRNativeEncryptedBinlogNotArchivedToday` (`internal/sidecar/binlog_encrypted_native_test.go`, opt-in: `BLOODRAVEN_PITR_NATIVE_TEST=1 go test ./internal/sidecar -run TestPITRNativeEncr -v`) starts `mysql:9.7` with `component_keyring_file` configured as the operator renders it (the manifest and component config of `internal/controller/encryption.go:129-135`, `:175-177`) and the settings of `encryptionMySQLSettings`, and runs the sidecar's archiver (`BinlogArchiver` with the sidecar's `LiveMysql` client as its role probe) over the server's data directory. Its output on this branch:

```
localBinlogHeader: get event err EOF, need 1768714341 but got 670
read-only: primary=false filesArchived=0 backlogFiles=0 uploadFailures=0 consecutiveFailedScans=0 lastError="" tail={Blocked pending=0 active=mysql-bin.000004 reason="read the header of mysql-bin.000003: get event err EOF, need 1768714341 but got 1427"}
records: [{… GTIDExecuted:<uuid>:1-9 … ReadOnlyGTIDExecuted:<uuid>:1-8 PromotionGTIDExecuted: …}]
writable: primary=true filesArchived=0 backlogFiles=6 uploadFailures=22 consecutiveFailedScans=4 lastError="archive mysql-bin.000006: invalid binlog magic" tail={Blocked pending=6 active=mysql-bin.000007 reason="archive mysql-bin.000006: invalid binlog magic"}
demoted: primary=false filesArchived=0 backlogFiles=6 uploadFailures=22 consecutiveFailedScans=6 lastError="archive mysql-bin.000006: invalid binlog magic" tail={Blocked pending=0 active=mysql-bin.000007 reason="read the header of mysql-bin.000006: get event err EOF, need 1768714341 but got 1013"}
decrypted 6 sealed files (the largest 3001429 bytes): <uuid>:1-13
after rotation: key MySQLReplicationKey_<uuid>_2, same bytes false, same size true, keyring [INNODBKey-<uuid>-1 MySQLReplicationKey_<uuid>_2 MySQLReplicationKey_<uuid> MySQLReplicationKey_<uuid>_last_purged]
binlog_encryption off again: primary=true filesArchived=2 backlogFiles=8 … lastError="archive mysql-bin.000008: invalid binlog magic" …, uploads "mysql-bin.000009,mysql-bin.000010"
```

The test asserts each of these. What they establish:

1. **A writable site archives nothing and says so.** Zero uploads, zero manifest rows, the error on every scan. A demoted primary keeps reporting the error and its backlog (the drain cannot decide, so the standing upload failure stays), so `PITRArchiveReady` stays `False` after a failover too.
2. **A read-only site archives no tail**, quietly: `Blocked`, no `lastError`.
3. **A promotion record has no promotion set**, so the #269 anchor is never available on such a group.
4. **File format.** Every binlog starts with `FD 62 69 6E`, then a version byte `01` and three tagged fields in a 512-byte header: field 1, one length byte and the keyring key id `MySQLReplicationKey_<server_uuid>_<seqno>`; field 2, the 32-byte file password, encrypted; field 3, a 16-byte IV. The rest of the file is the plaintext binlog, magic included, as an AES-256-CTR stream: the file is exactly 512 bytes longer than its plaintext.
5. **Keyring.** `component_keyring_file`'s data file is JSON: `{"version":"1.0","elements":[{"data_id":…,"data_type":"AES","data":"<hex>"}]}`. It holds `MySQLReplicationKey_<uuid>_<seqno>` (32 bytes, the binary log master key), `MySQLReplicationKey_<uuid>` (the current sequence number) and the InnoDB key.
6. **Decryption is small and checkable.** The file password is `AES-256-CBC-decrypt(master key, header IV)` of field 2, without padding. The stream key is the first 32 bytes of `SHA-512(file password)`, the nonce the next 8; the counter is a big-endian block number in the low 8 bytes of the IV, starting at 0 after the header. About 40 lines of standard-library Go (`decryptEncNativeBinlog`, test-only). Its output parses to the end with checksums verified in the archiver's own `InspectBinlog`, for every sealed file, one of them a 3 MB file (more than 2^16 cipher blocks). In the exploratory run one decrypted file was also byte-identical to what the server sends over the replication protocol (`mysqlbinlog --read-from-remote-server --raw`, compared with `cmp`).
7. **`ALTER INSTANCE ROTATE BINLOG MASTER KEY` rewrites the header of every existing binlog file in place** (new key id, new encrypted password, same size) and removes the old key from the keyring. The bytes of a sealed encrypted file are therefore not stable; its plaintext is.
8. **Turning the setting off later leaves a gap.** After `binlog_encryption=OFF` the new files are plaintext and are uploaded, the encrypted ones before them keep failing until MySQL purges them (`binlog_expire_logs_seconds`, 14 days as rendered, `internal/controller/reconciler.go:1258`), and the error stands that long.
9. **With `binlog_encryption=OFF` and everything else on, archiving works** (`TestPITRNativeEncryptionAtRestWithoutBinlogEncryptionArchives`: an encrypted table, plaintext binlogs, every sealed file archived).

### Not verified on a cluster

The cluster run could not be done on this host: `kind create cluster` with one control plane and three workers failed twice with `Failed to create control group inotify object: Too many open files` in a node's systemd (`fs.inotify.max_user_instances` is 128 and about 90 containers were running). Raising a host sysctl needs root on a shared machine, so the run stopped there and the cluster was deleted. Everything above the kubelet is therefore established from code (the tracker rules, the Events, the reclone wait) and everything below it from the docker run. The steps for the cluster run are in [Open verification](#open-verification).

## Constraints the options share

- **Existing groups.** A group may already run with both features. An upgrade must not make its object unwritable. On the Kubernetes 1.31 floor a status-subresource write is validated without ratcheting (`AGENTS.md`, A-115), so a new CEL rule that a stored object violates would stop the operator from writing that group's status. Any admission refusal has to be a transition rule; the rest is reconcile-time. This is also the house rule (`internal/controller/restore_inplace.go:296-298`).
- **Rolls.** The mysql launcher and `mysqlServerArgs` are hashed into every site's spec hash. None of the options edits them. A change to `my.cnf` content (option 3a for an existing group) rolls that group; a new sidecar image rolls every group once, as every release does.
- **The sidecar already mounts the keyring.** In both renderings the keyring volume is mounted read-only into the sidecar at `keyring.dataFileDir` and its path is passed as `BLOODRAVEN_KEYRING_FILE` (`internal/controller/encryption.go:255-263`): a projection of the escrow Secret with mode 0444 when sealed (`:273-285`), a memory-backed `emptyDir` when unsealed (`:291-300`). The escrow agent reads it to hash and escrow it (`internal/sidecar/keyring.go:582`). It is the same file mysqld uses.
- **The archive has its own encryption.** `spec.backup.profiles[].encryption` wraps every archive object in client-side AES-256-GCM (`encryptedStore`, bound to the prefix by the #203 binding). The API comment allows relying on the store instead (bucket SSE, volume encryption; `api/v1alpha1/backup_types.go:355-361`). Dumps of an encrypted group already depend on this, not on MySQL's keyring.

## Option 1: decrypt in the sidecar

The archiver opens local binlog files through one function that looks at the magic. For `FD 62 69 6E` it reads the header, finds the named key in the keyring file and returns the decrypted stream; everything behind it (parser, content identity, upload, transaction index, tail drain, promotion set) sees a plaintext binlog. The archive holds plaintext binlogs, wrapped by the profile's `encryption` when that is set, exactly as for a group without `binlog_encryption`.

**Key access.** No new mount, Secret, RBAC or pod-template change: the sidecar reads `BLOODRAVEN_KEYRING_FILE`, which it already hashes.

- *Sealed:* the keyring is read-only and cannot change, and MySQL rejects key rotations (scenario 48 asserts this for the InnoDB key). Every binlog header names a key in that file.
- *Unsealed (bootstrap, clone, rotation):* mysqld may add a key. The sidecar reads the live file, so a key mysqld has written is readable at once; a reader that does not find the named key retries on the next scan (an error like today's, transient).
- *Clone:* since PR #280 a clone ends with a new `server_uuid` and a new `MySQLReplicationKey_<new uuid>_1`. The clone replaced the data directory, so the files encrypted under the old uuid's key are gone; the tail drain runs before the clone starts (the 2-minute wait) with the old key still in the keyring.
- *`ROTATE BINLOG MASTER KEY`:* the operator does not issue it, and a sealed site refuses it. An administrator can run it on an unsealed site. All headers are then re-encrypted in place (fact 7); the keyring and the headers stay consistent, and a file caught mid-rewrite fails its stability check (`stableBinlogFile`) and is retried.
- *Escrow versions:* not needed. The archiver never reads a superseded keyring; it only decrypts files that exist beside the live one.

**Format.** AES-256-CTR with a per-file password wrapped by the master key (fact 6). The vendored `github.com/go-mysql-org/go-mysql` v1.16.0 has no reader for it (searched for the key name, the magic and "encrypt" in its `replication` package), and no other maintained Go implementation was found, so it has to be written: about 100 lines with a seekable `io.ReaderAt` (CTR allows random access, which the origin walk and `--start-position` indexing need). The format is version 1 since MySQL 8.0.14 and unchanged in 9.7.4. An unknown version byte, an unknown field or a missing key must fail closed with a clear error. It is tested against real files: committed fixtures (an encrypted binlog and the throwaway keyring it was written under) for every `go test`, and the native test for the image CI pulls.

**What changes in the sidecar.** Every local read: `archivePinned` (parse and upload stream), `parseBinlogMetadata`, `localBinlogHeader`, `originWalk`, `fileContentID` and the history task's hashing in `scanFiles`. The content identity must be the SHA-256 of the *plaintext*: the object key is the identity, restores verify downloaded bytes against it, and the ciphertext is not stable (fact 7). The manifest `size` is the plaintext size (file size minus 512). The identity cache is keyed on inode, size and mtime and survives a header rewrite only if mtime is unchanged; a rewrite costs one re-hash per file. `errEncryptedBinlog` goes away. The native test's "today" assertions flip to "archived".

**Restore side.** Nothing. `pitr-download`, the planner, `restore_script.py` and `mysqlbinlog` see plaintext files with the same offsets as before.

**Threat model.**

- The sidecar process holds the binary log master key and file keys in memory while it reads. It can already read the whole keyring file, and a live process in the MySQL pod is out of scope of the encryption-at-rest threat model (`site/content/docs/4.configuration/8.encryption-at-rest.md`, "Out of scope"). No new exposure on the pod.
- Row changes leave the encrypted volume for the archive. That is what a PITR archive is, and it is what backups of an encrypted group already do. The archive's protection is the profile's `encryption` or the store's. **Decision for the maintainer (D1):** when `spec.encryptionAtRest` is on and the PITR profile has no `encryption`, (a) only document it, (b) report it (a Warning Event and a line in `status.encryptionAtRest`), or (c) refuse to archive. Proposed: (b). (c) would refuse bucket-level SSE, which the API explicitly allows, and dumps are not held to it.
- Rejected variant, *upload ciphertext plus the wrapped key*: the archive would then be readable only with the site's keyring. A restore into a new group after the source group is gone (the main PITR procedure) would need the old escrow Secret; `mysqlbinlog` cannot read the files, so the restore would have to decrypt anyway; the object bytes change at a master key rotation; and the planner could not index transactions without the key. It adds key custody to the archive and removes nothing.

**Costs and risks.**

- A dependency on a MySQL-internal file format. It is documented by MySQL and stable across 8.0–9.7, and the native test fails on a change, but it is one more thing a MySQL upgrade can break. Failing closed means "not archived, `ArchiverError`", today's state.
- Crypto code in the sidecar. It is decryption of data the pod owns with standard primitives (`crypto/aes`, `crypto/cipher`, `crypto/sha512`), no key generation or storage.
- CPU: one AES-CTR pass per file per read, small beside the SHA-256 and the parse the archiver already does.
- Existing groups: binlogs still on the volume are archived after the upgrade (up to 14 days back); older ones are purged and stay a gap. Their promotion records already written have no promotion set. A full backup after the upgrade is the documented step.

**Rolls.** The sidecar image only (the normal release roll). No launcher, flag, CRD or `my.cnf` change.

**Effort.** About 4–6 days: the reader and its fixtures (1), threading it through the read paths with the plaintext identity (2), tests including the flipped native test and a playground scenario with both features on (1–2), docs and doctor skill (0.5).

**What a user does.** Upgrade. Take a full backup once `PITRArchiveReady` is `True`. Decide on the profile's `encryption` (D1).

## Option 2: refuse the combination loudly

Keep the limitation and make it impossible to miss.

- **Status (2a).** The operator knows from the spec alone that `EffectiveEncryptionCoverage().BinaryLog` is true and PITR is enabled. It sets `PITRArchiveReady=False` with a new reason `BinaryLogEncrypted` (ahead of `ArchiverError` in the reason order) and the message "spec.encryptionAtRest encrypts the binary log (coverage.binaryLog) and the PITR archiver cannot read encrypted binary logs: nothing is archived; see Known limitations", and a Warning Event `PITRBinaryLogEncrypted` on the edge. The sidecar reports the cause instead of the parser's text: `localBinlogHeader` and `inspectBinlog` return `errEncryptedBinlog` for the `FD` magic, so `lastError`, the tail reason and `promotion set not recorded` all read "the binlog file is encrypted (binlog_encryption=ON)". The `status.pitr.message` text changes; no documented `msg` string changes. New: one reason, one Event (`api/v1alpha1/events.go`, `make events-doc`), doc and doctor updates.
- **Admission (2b).** A CEL transition rule on the group refuses creating a group with both, and refuses an update that turns the combination on, while accepting any update of an object that already has it. Sketch, with `optionalOldSelf` as `api/v1alpha1/types.go:59` uses it: the new object has `encryptionAtRest.enabled`, `coverage.binaryLog` unset or true and `backup.pitr.enabled`, only if the old object had all three. Existing groups keep working, including their status writes on 1.31; they get 2a. It needs envtest on current and on 1.31, the regenerated CRD and the chart copy.
- **Existing groups.** Not rejected, not changed: condition `False` with the new reason and one Warning Event. Their archivers keep running and failing; alternatively the operator stops rendering the archiver for them, which rolls them and is not proposed.
- **Docs.** The limitation as written on this branch, plus the new reason and Event.

**Costs and risks.** It ships no capability: a user who needs both still has to choose between PITR and an encrypted binary log (option 3a). 2b adds a rule that option 1 removes again, and CEL budget on a large CRD. 2a alone is small and has no upgrade risk. **Rolls:** a sidecar image for the clearer error text; nothing else. **Effort:** 2a about 1 day; 2b about 1 more with envtest on both versions. **What a user does:** set `coverage.binaryLog: false` on new groups that need PITR, or run without PITR.

## Option 3a: no binary log encryption for PITR groups

`spec.encryptionAtRest.coverage.binaryLog: false` exists today. It renders `binlog-encryption=OFF`; tablespaces, redo and undo logs and the system tablespace stay encrypted. `TestMySQLLauncherNative` runs this rendering ("encryption at rest, binary log not encrypted"), and the control test here shows the archiver works on it (fact 9). The option is to make it the supported way: document it (done on this branch as a fact), and with option 2b make admission point to it.

- **What stays unencrypted on the volume:** binary logs and, on replicas, relay logs. With `binlog_format=ROW` and `binlog_row_image=FULL`, which PITR requires, they hold the full before and after image of every changed row for the binlog retention (14 days as rendered). A stolen volume or snapshot then exposes two weeks of row changes, although not the tables. That is most of what the feature's first threat ("a stolen or decommissioned MySQL data PVC") is about, so the security claim is materially narrower. `status.encryptionAtRest` reports the coverage per site.
- **New groups:** works today, no code.
- **Existing groups:** changing the field changes `my.cnf`, so the group runs an ordered update (followers, a handoff, the old primary). The encrypted files written before keep failing until purged (fact 8), `PITRArchiveReady` stays `False` for up to 14 days, and the archive starts after them. Verified with `SET GLOBAL` on one server, not through an operator roll.
- **Costs and risks:** the narrowed claim above; nothing else. **Rolls:** only groups whose owner changes the field. **Effort:** none beyond docs (and 2b's message if chosen). **What a user does:** set the field at creation; for an existing group, accept the roll, wait out the purge and take a full backup.

## Option 3b: read through the replication protocol

The server decrypts for a replication client: `mysqlbinlog --read-from-remote-server --raw` returned the plaintext file, byte-identical to the decryption of fact 6 (measured). go-mysql v1.16.0, already a dependency, has a dump client (`BinlogSyncer.StartBackup*`). The sidecar would fetch each sealed file through `COM_BINLOG_DUMP` instead of opening it.

- **For it:** no key material and no file-format knowledge in the sidecar; the bytes and offsets are the same as option 1's, so the archive and the restore side are unchanged.
- **Against it:** every read becomes a network dump through mysqld. The archiver reads files often and under tight bounds: header reads for the promotion set have 1 s for up to 64 files (`promotion_record.go:511-515`), the tail drain's origin walk reads windows at offsets, and the history audit re-hashes files in 64 MiB / 2 s slices. A dump is sequential from a position, holds a dump thread and a connection (the sidecar's pool is 3), and costs the server CPU on the primary. The sidecar's MySQL account would need `REPLICATION SLAVE`. Reads would depend on mysqld accepting connections; today's file reads do not. Finding where a sealed file ends in a stream needs the non-blocking dump flag and rotate-event handling. It is a second read path for every reader, used only by encrypted groups, and much more code than option 1.
- **Rolls:** sidecar image; a grant change for the sidecar account on every group. **Effort:** 2–3 weeks. Not recommended.

## Comparison

| | 1 Decrypt in the sidecar | 2 Refuse loudly | 3a `binaryLog: false` | 3b Replication protocol |
|---|---|---|---|---|
| PITR with an encrypted binary log | Yes | No | No (log not encrypted) | Yes |
| Binary log encrypted on the volume | Yes | Yes (no PITR) | No | Yes |
| Archive protection | Profile `encryption` or the store | n/a | Same | Same |
| Key material used by the sidecar | Yes (already mounted) | No | No | No |
| Depends on MySQL's file format | Yes, version 1, fail closed | No | No | No |
| Existing groups | Archive resumes for files still on disk; full backup after | Condition and Event; nothing breaks | Roll, gap until purge | As 1 |
| Rolls | Release roll (sidecar) | Release roll (sidecar) | Groups that change the field | Release roll, grant change |
| CRD / admission | None | Reason, Event; CEL rule in 2b | None | None |
| Effort | 4–6 days | 1–2 days | Docs | 2–3 weeks |

## Recommendation (pending Colin's sign-off)

1. **Fix: option 1.** It is the only option that gives a user both features with the claim intact, it needs no new mount or privilege because the sidecar already has the keyring file, the format is small and was decrypted here to a byte-identical result, and the restore side does not change. Its real cost is a dependency on a MySQL file format, bounded by failing closed and by the native test in CI.
2. **Interim, in the next release: option 2a** (the status reason, the Event and the clear sidecar error). Today's signal is `ArchiverError: invalid binlog magic`, which reads like a corrupt file. One day of work, no upgrade risk, and its reason and Event are removed when option 1 lands.
3. **Option 2b (admission) only if option 1 is not scheduled for the release after next.** A rule that is added and then removed is churn in a CRD that is heading for `v1beta1`.
4. **Option 3a stays documented as the way to run PITR today**, with what it leaves unencrypted stated. It is not a fix.
5. **Option 3b is rejected**: more code and more load on the primary for the one advantage of keeping format knowledge out of the sidecar.

Decisions for the maintainer:

| # | Question | Proposed |
|---|---|---|
| D1 | With option 1, a PITR profile without `encryption` on a group with encryption at rest | Report (Warning Event and status line), do not refuse |
| D2 | Interim 2a in the next release | Yes |
| D3 | Admission refusal 2b | Only if option 1 slips |
| D4 | With option 1, archive the encrypted files still on disk of an existing group, or only files sealed after the upgrade | Archive them: they are this server's history, the planner refuses gaps, and a full backup after the upgrade is documented either way |

## Open verification

To run on a host where kind can start four nodes (`sudo sysctl fs.inotify.max_user_instances=1024`), with a private kubeconfig:

```bash
export KUBECONFIG=$PWD/.wp3-kubeconfig
cat <<EOF | kind create cluster --name bloodraven-wp3 --kubeconfig "$KUBECONFIG" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes: [{role: control-plane}, {role: worker}, {role: worker}, {role: worker}]
EOF
BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh
# PITR on the in-cluster RustFS (create the bucket first; scenario 31 does it with internal/playground/rustfs)
kubectl -n bloodraven-playground patch mfg playground --type merge -p '{"spec":{"backup":{"profiles":[{"name":"rustfs","storage":{"type":"S3","s3":{"bucket":"bloodraven","prefix":"wp3","endpointURL":"http://rustfs.bloodraven-playground.svc.cluster.local:9000","credentialsSecretName":"dragonfly-s3-credentials","region":"us-east-1"}}}],"pitr":{"enabled":true,"profileName":"rustfs"}}}}'
kubectl -n bloodraven-playground wait mfg/playground --for=condition=PITRArchiveReady --timeout=10m   # plaintext baseline
./playground/enable-encryption.sh                      # in place; --fresh for encrypted from birth
.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n bloodraven-playground playground archiver
kubectl -n bloodraven-playground get mfg playground -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.status} {.reason}: {.message}{"\n"}{end}'
kubectl -n bloodraven-playground get events --field-selector reason=PITRArchiveNotReady
kubectl bloodraven promote playground pdx -n bloodraven-playground       # then read the new primary's manifest: promotion record without promotionGtidExecuted
kubectl -n bloodraven-playground annotate mfg playground bloodraven.shipstream.io/reclone-site=iad   # expect PITRArchiveTailNotArchived after 2 minutes
kind delete cluster --name bloodraven-wp3 --kubeconfig "$KUBECONFIG"
```

Expected, from the docker run and the code: after `enable-encryption.sh` the binlogs sealed before the roll stay archived, every one after it fails with `invalid binlog magic`, and the condition turns `False`/`ArchiverError` within two scans. Still unverified anywhere: the kubelet's Secret projection of the keyring as the sidecar would read it for decryption (the escrow agent reads the same file today), and whether an in-place header rewrite changes a file's mtime.

## Docs that change when implemented

- Option 1: remove the limitation from `3.get-started/1.known-limitations.md`, `7.backup-and-restore/2.backup-restore.md` ("Encryption at rest and PITR") and the interaction table of `4.configuration/8.encryption-at-rest.md`; state the archive-protection rule (D1); retire doctor playbook 49 or reduce it to the fail-closed errors; update `8.observability/7.log-schema.md` for any new message.
- Option 2a: add the reason and Event to the same pages, `2.crd-reference.md`, the events reference and playbooks 31 and 48.
