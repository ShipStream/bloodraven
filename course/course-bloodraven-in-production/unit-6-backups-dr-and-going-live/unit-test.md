# Unit 6 test — Ready for the rotation

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you say which site a backup ran from and why, what a `Succeeded` verification did not prove, which site in `playground` cannot have its keyring rotated right now, and why an alert on `bloodraven_replication_lag_seconds` might page you for a reader doing exactly what it was designed to do?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

The `nightly` profile on `playground` fires at 02:00 with no `sourceSiteOverride` and `maxLagSecondsForSource` left at its default. At that moment: `iad` (`primary-candidate`) is the active site, `writable`; `pdx` (`primary-candidate`) is `read-only`, both replication threads running, `secondsBehindSource: 412`; `reader` (`role: read-only`) is `read-only`, replicating, `secondsBehindSource: 3`. Which site runs the dump, and what reason string lands in status?

- `reader`, `"replica-preferred"` — it is the freshest replica and the one site carrying no promotion duty
- `pdx`, `"replica-preferred"` — replica-first is the rule, and the lag figure only drives the `ReplicationLagging` condition
- `iad`, `"primary-fallback"` — `pdx` is past the 300 s source gate and `reader` is excluded from the replica pool by role
- No site: with every replica ineligible the profile errors rather than dumping from a writable primary

**Correct option index:** 2

**Explanation:**

`selectSourceSite` prefers a replica and falls back to the primary. A replica qualifies only if it is `read-only`, actually replicating, and at or below `maxLagSecondsForSource` — default **300**. `pdx` at 412 s fails that gate, so the job goes to the active site as `"primary-fallback"`, which itself requires the primary to be `writable` and promotable. `reader` looks like the obvious host — freshest, least loaded — but `role: read-only` sites are excluded from the replica pool outright; had you named it in `sourceSiteOverride` you would have got a hard rejection, not a silent fallback: `sourceSiteOverride "reader" names a read-only site, which cannot be a backup source`. Option two confuses two settings that both default to 300: `maxLagSecondsForSource` picks the backup source, `spec.replication.maxLagSeconds` drives only the `ReplicationLagging` condition and never selects anything. Option four invents a failure mode — the primary fallback exists precisely so an ineligible replica does not cost you the night's backup. (objective 2)

## Question 2

**Type:** TRUE_FALSE

You add `pointInTime.stopDatetime` under `spec.restoreInPlace` on `playground` while `spec.backup.pitr.enabled` is still `false`. Your `kubectl apply` will be rejected by the API server, carrying the message `pointInTime is set but spec.backup.pitr.enabled=false; PITR restore requires the failover group to have continuous binlog archival configured on the source`.

**Correct answer:** false

**Explanation:**

The message is exactly right; where it arrives is not. This is a **reconciler error, not an admission rejection** — your `kubectl apply` succeeds and prints `configured`, and the failure shows up seconds later in the CR's status and the operator log. Watch there, not at the exit code of the apply. Contrast this with the CEL rule behind `spec.encryptionAtRest.enabled requires spec.tls`, which genuinely is a hard admission rejection: the two look similar in a doc and behave nothing alike at the terminal. The substance of the rejection is not a style preference either — replay material only exists if something archived it, and with PITR off nothing did. Both restore entry points share one builder, so `spec.initFromBackup.pointInTime` fails identically. (objective 6)

## Question 3

**Type:** MULTIPLE_CHOICE

A `MysqlBackupVerification` for last night's `nightly` artifact reads `phase: Succeeded`, `sanityCheck.ran: true`, `sanityCheck.resultRow: 1`. The configured check was `SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'counter_db'` with `minRows: 1`. In the incident review, what may you state that this run proved?

- That the artifact is byte-consistent with the live `playground` primary as it stood when the nightly dump began
- That the artifact loads into a real mysqld and the `playground` schema was in the copy that loaded — nothing more
- That the incident restore path is rehearsed end to end: the dump loads and application traffic can be cut over to it
- Only that the verify pod started — `resultRow: 1` is what an empty result set reports, so the assertion carries no information

**Correct option index:** 1

**Explanation:**

A `Succeeded` proves two things: the artifact loads into a real mysqld, and your chosen scalar assertion held. That is the whole claim. Option one asks for logical equivalence with the live primary, which verification explicitly does not establish — a different tool's job. Option three is the dangerous one, because it is the sentence people reach for at 3am: verification restores into an **ephemeral, throwaway** instance on its own PVC, with `mysqld` bound to `127.0.0.1` and **no Service**, so nothing can be pointed at it and no traffic cutover is rehearsed. Verification is not a DR drill. Option four inverts the semantics: an empty result set is treated as **scalar 0**, precisely so a silently-empty restore fails the `minRows: 1` floor instead of passing; `resultRow: 1` therefore means a row genuinely came back. The best argument for running verification anyway is this project's own history — chaos scenario 31 failed with `ERROR 1062 Duplicate entry` because the verify `mysqld` ran `gtid_mode=OFF`, defeating GTID dedup on replay. A broken verifier is discovered by running it and by nothing else. (objective 4)

## Question 4

**Type:** MULTIPLE_CHOICE

An in-place restore of `playground` completed on 20 May with `spec.restoreInPlace.confirm: "2026-05-20T14:32:00Z"`, and `status.restoreInPlace.confirmTokenUsed` now records that value. The manifest lives in Git and Argo CD re-syncs it every night, unchanged. What happens on tonight's sync?

- The restore re-runs — `restoreInPlace` is the re-runnable field, which is exactly what 'no teardown-and-rename cycle' means
- The sync fails at admission: the API server rejects any `confirm` that is not strictly greater than `confirmTokenUsed`
- The operator clears the terminal status, re-enters `Preflight`, and emits a `RestoreInPlaceRejected` event on every sync
- Nothing. The token is not greater than `confirmTokenUsed`, so the reconciler returns on an already-matching status

**Correct option index:** 3

**Explanation:**

`confirm` is required, must parse as an **RFC 3339 timestamp**, and must be **strictly greater** than `status.restoreInPlace.confirmTokenUsed`. Re-applying an unchanged manifest therefore carries a token that is no longer greater, and the reconciler returns on a terminal status already reflecting that value — GitOps cannot replay a destructive restore, which is the entire point of the design. Option one gets 're-runnable' right but reads it as 'runs again on its own': it means you may run another restore later, by bumping the token, not that every apply triggers one. Option two puts the check in the wrong layer — like the `pointInTime` rejection this is reconciler-side, so the apply succeeds. Option three confuses the rejection path: an *invalid* token on a re-arm emits `RestoreInPlaceRejected` and leaves the previous terminal status **visible**; it does not tear the status down, and an unchanged valid token emits nothing at all. Programmatic callers simply send `now()`. (objective 5)

## Question 5

**Type:** MULTIPLE_CHOICE

You are asked to rotate every keyring in `playground` this evening. Right now `kubectl get mfg playground -o jsonpath='{.status.encryptionAtRest.sites}'` shows `iad Sealed` (and `iad` is the active primary), `pdx Unsealed` with `unsealReason: Rotation`, `reader Sealed`. What is the correct read of that state and the correct plan?

- `iad` reads `Sealed`, so rotate it first while the group is quiet — the rotation refusal applies only to sites sitting in `Unsealed`
- `pdx` is mid-rotation and unprotected until it reads `Sealed`; then rotate `reader`, planned-failover to `pdx`, and rotate `iad` last
- `pdx` reading `Unsealed` means that site was never sealed at all, so re-bootstrap `pdx` first and leave the other two alone
- Annotate all three targets now; escrow Secrets are versioned per site, so concurrent rotations cannot interfere with each other

**Correct option index:** 1

**Explanation:**

`Sealed` is the steady state and the only phase that means done — the keyring is projected read-only from that site's escrow Secret, so mysqld physically cannot add a key. `Unsealed` means a writable memory-backed keyring, and rotation re-enters `Unsealed` **from** `Sealed`, so the string alone is ambiguous; `unsealReason: Rotation` is what tells you `pdx` is a protected site opened deliberately rather than one that was never protected. Option three misreads exactly that ambiguity and would destroy a healthy site. Option one has the refusal backwards: the operator refuses to rotate the **active primary**, because rotation necessarily runs with a writable keyring and that is the only window in which a keyring can be lost — lose a replica's and you reclone it, lose the primary's and you have lost data. The site you cannot rotate is not a fixed name, it is whichever site is active, so `iad` becomes rotatable by ceasing to be primary, via the planned failover you already run at RPO 0 by construction. Option four ignores that rotation is one site at a time and is also refused while a planned failover or ordered update is in flight. (objectives 7, 9)

## Question 6

**Type:** TRUE_FALSE

`playground` runs with `spec.encryptionAtRest.enabled: true` and every site at `Sealed`. Your cluster has no API-server encryption at rest configured for Secrets. The operator will surface this: at least one site will report keyring phase `Failed`, because the escrow Secret cannot be protected.

**Correct answer:** false

**Explanation:**

The opposite is true, and the silence is the hazard. `Failed` means something Bloodraven can actually observe — escrow timed out, the escrow Secret is missing, or the live digest does not match escrow. API-server encryption is not in that list, because nothing in the operator checks it and no status field will ever tell you that you skipped it. The docs are blunt about the consequence: the live keyring is projected from a Kubernetes Secret, and **Kubernetes stores Secrets unencrypted in etcd by default**, so without API-server encryption at rest you have not protected your keys — you have moved them from the MySQL data disk to the control-plane disk, and etcd is now part of your key custody. KMS-backed API-server encryption, restricted Secret RBAC in the namespace, and swap on the worker nodes are all prerequisites you satisfy outside Bloodraven: 'None of these are optional. Bloodraven cannot verify them for you.' Exactly one prerequisite *is* enforced, and it is a different one — the CEL rule requiring `spec.tls`, because a secure connection is genuinely required to clone encrypted data and `CLONE INSTANCE` is how a diverged site is reseeded. (objective 8)

## Question 7

**Type:** MULTIPLE_CHOICE

At 03:12 `BloodravenReplicationLagging` pages on-call for `playground`. The firing series is `bloodraven_replication_lag_seconds{site="reader"}`, the rule is `bloodraven_replication_lag_seconds > 30 for: 5m`, and `reader` is `role: read-only`, both threads running against the correct source, already shed from the `mysql-playground-replicas` endpoints. What is the correct fix?

- Add `role!="read-only"` to the matcher — the operator stamps each site's role onto the lag series for exactly this purpose
- Raise the threshold past `readOnlyMaxLagSeconds`, so the reader sheds from the replicas Service before the rule can fire
- Add `site!="reader"` — the gauge carries a `site` label and nothing else, so you exclude by name and maintain it by hand
- Keep the rule and lower the threshold: a lagging reader is the group's last-resort promotion candidate, so the page is legitimate

**Correct option index:** 2

**Explanation:**

A converged-but-slow reader is designed behaviour, not a fault: chaos scenario 42 soaks a reader past three times `maxLagSeconds` and asserts that `Ready` stays `True`, `activeSite` never changes, `lastFailover` is unchanged so no cooldown is consumed, and the only reaction anywhere is the reader leaving `mysql-playground-replicas` once it passes `readOnlyMaxLagSeconds`. So the rule must exclude it. Option one is the exclusion everyone reaches for and the metric cannot support: `bloodraven_replication_lag_seconds` carries a `site` label **only** — no `role` label, no `group` label — which is why the exclusion is by name and grows a maintenance burden with every reader you add. Option two inverts cause and effect: the shed already happened at 03:12 and the alert fired anyway, because endpoint membership has no bearing on whether a gauge crosses a threshold. Option four is wrong on the role model from Unit 1 — a `role: read-only` site is never promotable and cannot even source a backup; it is not a spare. Note what the exclusion does not buy you either: `> 30` never fires when the gauge reads `-1`, which is what a site that has stopped replicating entirely reports, so `BloodravenReplicationDown` on `bloodraven_replication_running{site,thread}` has to be a separate rule. (objectives 10, 12)

## Question 8

**Type:** SHORT_ANSWER

02:41 — `BloodravenPITRArchiveLagging` fires for `playground` on `min_over_time(bloodraven_archiver_backlog_files[5m]) > 0`. The counter app is writing normally, `iad` is `writable`, and the group reads `Ready: True`. At 03:10, while you are still working the page, the `iad` node loses its PVC entirely and `pdx` is promoted. Answer three things: what that alert meant for the data plane, the first command you run, and precisely what PITR can and cannot give you for the window between 02:41 and 03:10.

**Sample answer:**

The alert meant nothing for the data plane. A backup storage failure has no data-plane impact at all: MySQL kept serving reads and writes, the counter kept counting, `playground` stayed `Healthy` and `Ready: True`, and my PITR RPO drifted backwards in silence. That is the textbook silent degradation, and the reason the alert exists is that nothing else would have told me.

First command: `kubectl -n bloodraven-playground logs deploy/mysql-playground-iad -c sidecar`. The archiver runs inside the per-site sidecar, not the operator, so the default first command `kubectl bloodraven status` would have shown me a perfectly healthy group and taught me nothing.

For 02:41-03:10: whatever sealed binlogs made it into the bucket before the backlog stalled are still replayable. Everything after that is gone in two separate ways. First, only **sealed** binlogs upload — the last entry in the index is the file MySQL is writing to right now and the archiver drops it, so the active binlog was never a candidate for upload, and it lived on the destroyed PVC. It is gone forever, along with every transaction in it. Second, even restoring from `pdx` cannot conjure it back: PITR cannot reach past the async-replication cutoff, because transactions `iad` committed but never shipped are not in `pdx`'s binlog stream and therefore not in PITR's replay material. Neither of those is a bug. If I want a shorter unarchived tail next time the lever is `maxBinlogSize` (default `100M` when PITR is enabled), which is written into the generated my.cnf before `spec.mysqlConf` is merged, so my override still wins.

**A full-credit answer shows:**

A strong answer covers: (1) zero data-plane impact — MySQL keeps serving, the group stays Ready/Healthy, and the degradation is silent, which is the whole point of having the alert; (2) a first command that goes to the **sidecar** logs for the site, explicitly noting that the archiver lives in the sidecar so the default `kubectl bloodraven status` is the wrong first move; (3) both hard limits, distinctly — the active (unsealed) binlog is never uploaded by design and died with the PVC, and PITR cannot reach past the async-replication cutoff, so writes `iad` never shipped to `pdx` are not in the replay material anywhere. Credit a mention of `maxBinlogSize` as the tail-shortening lever, or of the manifest-after-upload ordering. Do not credit an answer that claims the alert indicates writes were failing, that a restore from `pdx` recovers the unshipped tail, or that the operator log is the place to look for the archiver.

**Explanation:**

The three parts map to three separate facts the unit teaches. Backup and PITR are a subsystem with no data-plane coupling, so their failures are invisible unless you alert on them — that is why the alert-to-runbook map sends `BloodravenPITRArchiveLagging` and `BloodravenPITRUploadFailures` to the sidecar logs rather than to the default `kubectl bloodraven status`; the archiver is the one component in the alert set that does not live in the operator. And the loss splits cleanly: the sealed-only upload rule plus PVC destruction accounts for the tail on `iad`, while the async cutoff accounts for everything `iad` committed and never shipped. Confusing the two leads people to believe a healthy replica plus a bucket is a complete recovery story. (objectives 3, 10, 11)

## Question 9

**Type:** MULTIPLE_CHOICE

The cluster hosting `playground` is in `us-west`. Your `kubectl --context=west` calls have been timing out for eleven minutes, and the `MysqlStandbyCluster` in `us-east` reads `BucketReadable: True`, `SourceConfigKnown: True`. A colleague on the office VPN reports the west counter application is still incrementing and still reporting a writable site. What do you do next?

- Declare the source dead and start the DR bootstrap — an unreachable API server is the authoritative liveness signal for a whole cluster
- Wait. You have one signal, and a second observation contradicts it; control plane and data plane fail separately
- Activate the standby cluster — `BucketReadable: True` means it has validated the dump and can restore and promote on request from you
- Create the DR group now and let the operator's `DNSEndpoint` move `playground.example.com` to `us-east` once the new group reports `Ready`

**Correct option index:** 1

**Explanation:**

The checklist demands at least **two of three** independent signals before you declare a source dead, and here you have one — an unreachable API server — actively contradicted by a third-vantage observation that MySQL is serving writes. Option one treats that single signal as authoritative, and it is a known false positive: Kubernetes will not even delete pods on an unreachable node, the containers keep running and keep writing to the PV. The bar is high because **Bloodraven v1 does not automatically detect or resolve cross-cluster split brain** — no operator watches both clusters and the sidecar fencing layer only knows peers in its own group, so the human fencing decision *is* the safety mechanism. GitHub's October 2018 incident cost over 24 hours of reconciliation for a 43-second partition; you would be doing that by hand across two clusters. Option three overstates the standby cluster, which is **observability only**: it re-scans the bucket and publishes `BucketReadable` and `SourceConfigKnown` — no MySQL contact, no restore Jobs, no activation. It tells you a DR bootstrap would be possible and roughly how far back it could reach; it has not proven the dump restores. Option four gets the bootstrap right in shape — a new `MysqlFailoverGroup` in the DR cluster with `spec.initFromBackup` pointed at the same bucket — but hands the operator a job it does not have: the operator owns one per-cluster `A` record and cannot accelerate DNS propagation, so the global application-facing name is yours to flip by weight, CNAME or GSLB. (objectives 13, 14)

## Question 10

**Type:** SHORT_ANSWER

You are the sign-off for taking `playground` live tomorrow. Give a verdict — **block**, or **accept with a named owner** — on each of these five, and say why in one line each. (a) The only backup profile is `storage.type: PVC`, on a claim in the same storage class as the data PVCs. (b) `spec.mysqlConf` is set by another team and nobody has read `sync_binlog` off the running instance. (c) No backup has ever been verified. (d) The runbook's failover timings were measured in the playground. (e) `replication.maxLagSeconds` is set, and the runbook says 'the operator will not promote a replica more than `maxLagSeconds` behind'.

**Sample answer:**

(a) **Block.** A PVC-local backup shares a failure domain with the data it protects — same cluster, same storage class, sometimes the same node — so the event that takes the PVCs takes the copy with it. It does not survive loss of the cluster, which is the case the backup exists for. That is an assumption, not a backup. Fine as a fast local artefact; never as the thing that saves us. Move the profile to S3 with credentials and bucket outside the cluster.

(b) **Block until somebody reads it off the running instance.** `sync_binlog=1` is an overridable default: it is written into the base my.cnf *before* `spec.mysqlConf` is merged, so another team's override wins silently. Contrast `gtid_mode`, `log_bin` and `log_replica_updates`, which are written *after* overrides precisely so nothing can weaken them. Confirming this costs one query.

(c) **Block.** An unverified backup is a schrödinger backup — a file of about the right size. GitLab, January 2017: five mechanisms, all of which would have passed a config review, none of which had ever been read back. Run a `MysqlBackupVerification` and get a `Succeeded`, understanding it proves the artifact loads and my scalar assertion held, and nothing more.

(d) **Accept, owner re-measures on real config.** The playground overrides the shipped defaults — `failoverCooldown: 30s` against 5m, `maxLagSeconds: 30` against 300, `dns.ttl: 10` against 60 — so no timing measured there transfers unchanged. Not a launch blocker, but the runbook is currently fiction and needs an owner with a date.

(e) **Accept the setting, block the runbook sentence — it is false.** `maxLagSeconds` drives only the `ReplicationLagging` Degraded condition and is **not** a promotion gate: if the primary dies while the replica is beyond the threshold, Bloodraven still promotes it, because no writable site at all is almost always worse. An on-call engineer who believes that sentence will wait for a promotion that already happened. Correct the text and name the owner.

I would add a sixth: nothing in the shipped alert set fires for 'the application is still broken after a successful failover' — `BloodravenFailoverOccurred` says the operator finished, not that traffic recovered. Block until somebody owns that alert by name.

**A full-credit answer shows:**

A strong answer blocks on (a), (b) and (c) and accepts (d) and (e) with a named owner — but the reasoning matters more than the label, and a defensible block on (d) or (e) is creditable if the *why* is right. Required substance: (a) PVC-local backups share a failure domain and do not survive cluster loss; (b) `sync_binlog=1` is an overridable default written before `spec.mysqlConf`, so it must be read off the running instance rather than assumed; (c) an unverified backup is an assumption, ideally with the GitLab 2017 point that all five mechanisms would have passed a config review; (d) the playground overrides `failoverCooldown`, `maxLagSeconds` and `dns.ttl`, so no measured timing transfers; (e) `maxLagSeconds` is not a promotion gate — a beyond-threshold replica is still promoted — so the runbook sentence is wrong. Give credit for spotting that (e) is really a documentation defect rather than a configuration one. Give extra credit for adding the unowned application-side write gap, or for noting that a `role: read-only` reader can neither be promoted nor source a backup and is therefore not a spare. Do not credit blanket 'block everything' or 'accept everything' answers with no stated reason.

**Explanation:**

The gate is a decision exercise, not a checklist recital, and every row turns on something the course established rather than on general prudence. (a) is the storage choice from the first topic of this unit — S3 or PVC is the whole choice, and PVC-local is not durable. (b) and (e) are both cases where a name implies a guarantee the code does not give: `sync_binlog` is written before overrides and can be silently weakened, while `maxLagSeconds` never gates promotion and is consulted only for the `ReplicationLagging` condition. (c) is the schrödinger-backup argument. (d) is the trap that catches every team that rehearses in the playground: `failoverCooldown: 30s`, `maxLagSeconds: 30` and `dns.ttl: 10` are playground overrides of shipped defaults of 5m, 300 and 60, so the numbers in a playground-derived runbook are wrong before anyone reads them. The sixth item — the unalerted application-side write gap — is the one no tool will raise for you, which is exactly why it needs an owner by name. (objectives 1, 15)
