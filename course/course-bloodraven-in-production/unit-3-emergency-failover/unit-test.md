# Unit 3 test — Failover, measured

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you name which failover steps abort on error and which only warn, say why the same promotion takes 12 seconds one day and 36 the next, and read two GTID sets to decide whether a returning primary rejoins or blocks?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

You need a completed emergency failover on `playground` that stays completed: `iad` must go down and stay down while you walk an auditor through `status.activeSite`, the failover record and the counter application. Which injection gives you that, deterministically?

- `kubectl delete pod -l shipstream.io/site=iad --grace-period=0 --force`
- `kubectl scale deployment mysql-playground-iad --replicas=0`
- SQL `SHUTDOWN` inside the mysql container, leaving the pod and PVC in place
- Killing the mysqld process so the pod crashes with its PVC intact

**Correct option index:** 1

**Explanation:**

Scale-to-0 removes the pod and keeps the site down, which is exactly why scenario 01 uses it — for determinism, not because the other injections fail. Force-delete *does* trigger a failover (scenario 09b hard-waits for `activeSite` to flip and `lastFailover` to stamp after a `--grace-period=0 --force` delete, and passes), so the widespread "a pod delete does not fail over" superstition is wrong; but it races the sub-five-second Deployment respawn, and that race can restore the original topology through split-brain recovery instead of completing the failover — useless for a demonstration. SQL `SHUTDOWN` restarts the container in place with pod, PVC and IP surviving; scenario 16 explicitly accepts either outcome, because the kubelet's restart speed may beat the ~6 s detection window (`pollInterval` 2 s x `failureThreshold` 3). A pod crash with the PVC intact is the genuine no-failover case: RPO 0, the primary comes back writable and the operator keeps it. (objective 1)

## Question 2

**Type:** MULTIPLE_CHOICE

Four separate incidents on `playground`, each with a single error at a different point of `Execute()`. In which one does `Execute()` return early, leaving `pdx` still read-only and no promotion recorded?

- `SET GLOBAL super_read_only = ON` against the old primary returns a connection error
- The relay-log drain on `pdx` hits its 30 s budget without catching up
- `RESET REPLICA ALL` on `pdx` returns an error
- `SELECT @@global.gtid_executed` on `pdx` returns an error

**Correct option index:** 2

**Explanation:**

Only the four statements that change the candidate's own replication and read-only state are fatal: `STOP REPLICA`, `RESET REPLICA ALL`, and the two promotion statements. `RESET REPLICA ALL` failing returns the error immediately and nothing is promoted. Fencing the old primary is warn-only by design — the old primary is usually unreachable, which is the whole reason the failover is running, so a failed fence must not block promotion. The drain is non-fatal: a timeout logs `relay log drain did not complete cleanly, proceeding with promotion` and the operator promotes with whatever the candidate had applied. Recording the promotion GTID is also warn-only; promotion continues, you simply lose the record you would later have used for the divergence comparison. (objective 2)

## Question 3

**Type:** TRUE_FALSE

During the failover you watch a `shipstream.io/db-readonly-*:NoExecute` taint land on `iad`'s node before `pdx` is writable. You conclude the taint is one of the ordered steps of the promotion sequence.

**Correct answer:** false

**Explanation:**

Ordering is not membership, and this is the inference the timing invites. Taints are a pure function of per-site state transitions, applied earlier in the same poll by a different code path: writable-to-anything-else adds the taint, anything-else-to-writable removes it, read-only to unreachable does nothing. The taint appears around a failover because the same transition drives both, not because the promotion applies it. Source convergence is the docs' other phantom step — an independent poll stage with its own 20 s budget. Both are neighbours in the poll, not links in the chain. The genuinely undocumented step is the one the doc list omits: step 2, killing application connections on the old primary. (objective 2)

## Question 4

**Type:** MULTIPLE_CHOICE

Scenario 14 pauses `pdx`'s SQL applier, seeds five seconds of writes on `iad`, then kills `iad`; `activeSite` flips at 36.0 s. You re-run the same kill with the applier running normally. What do you expect on the stopwatch, and why?

- About 6 s — with no relay backlog there is no detection debounce left to pay
- About 12 s — the drain hits its early-exit condition and returns almost at once
- About 36 s — the drain always spends its full 30 s budget before promoting
- About 30 s — the drain is skipped, leaving only the fixed 30 s promotion budget

**Correct option index:** 1

**Explanation:**

A clean primary kill measures 12.0 s to the `activeSite` flip, reproducible across nine-plus independent runs. The whole difference from 36.005 s is the drain: 36.005 - 12.0 = 24.0 s of extra wall clock spent applying relay logs. Both runs pay the same 6 s of detection (`pollInterval` 2 s x `failureThreshold` 3), so 6 s is not the answer — detection is unaffected by how caught-up the candidate is. The drain is not a fixed cost: it exits the moment the SQL thread is running and `Seconds_Behind_Source` reads 0, which is what makes the clean case fast. And 30 s is the drain's ceiling, not a promotion budget — there is no such fixed budget, and the drain is non-fatal when it expires. (Ignore the docs' 30-45 s figure; it is unsourced and contradicted by the recorded runs.) (objective 3)

## Question 5

**Type:** SHORT_ANSWER

A tenant's incident review asks you to write down the RPO commitment for the emergency path on `playground`, and specifically whether `sync_binlog=1` is part of that commitment. Answer both, and say how you would verify the second on this group rather than asserting it.

**Sample answer:**

The contract is one sentence: an emergency failover can lose every transaction that committed on the dying primary but had not yet replicated to the surviving site. Replication between sites is asynchronous and nothing in the CRD closes that window on the emergency path. `sync_binlog=1` is a default, not a guarantee: it is written into the base my.cnf map before `spec.mysqlConf` overrides are applied, so a tenant who sets `sync_binlog: "0"` gets 0, silently, with no warning and no admission rejection. The test is ordering — written after the overrides it is a guarantee, written before them it is a default. The un-weakenable invariants are the block written last: `gtid_mode=ON`, `enforce_gtid_consistency=ON`, `log_replica_updates=ON`, `log_bin`, `skip_replica_start=ON`, the clone plugin load, plus `skip-log-bin`/`disable-log-bin` deleted outright. So I would not trust the layer diagram; I would read the rendered per-site ConfigMap: `kubectl -n bloodraven-playground get configmap mysql-playground-iad-config -o jsonpath='{.data.bloodraven\.cnf}' | grep -E 'sync-binlog|gtid-mode|flush-log'` and report what is actually in the file. I would also flag `innodb_flush_log_at_trx_commit=2`, which Bloodraven ships for throughput: the MySQL manual attributes the loss to any unexpected mysqld process exit, not only power loss, and says it can erase up to N seconds of transactions.

**A full-credit answer shows:**

A strong answer covers: (1) the contract sentence, stated as loss of transactions committed on the dying primary but not yet replicated; (2) `sync_binlog=1` identified as an overridable default, with the before/after-overrides ordering test as the reason; (3) at least one named invariant from the last-written block as the contrast; (4) verification by reading the rendered per-site ConfigMap rather than the documentation or diagram. Credit but do not require the `innodb_flush_log_at_trx_commit=2` point or the 14-day `binlog-expire-logs-seconds` as a second overridable default. Reject any answer that claims `spec.mysqlConf` cannot weaken `sync_binlog`, or that `maxLagSeconds` bounds the RPO.

**Explanation:**

The RPO story survives contact with a tenant only if you can separate the layer that a tenant beats from the layer that beats the tenant. `sync_binlog=1`, `innodb_flush_log_at_trx_commit=2` and `binlog-expire-logs-seconds=1209600` are written first and are therefore overridable; the GTID and binlog invariants are written last and overwrite whatever the tenant put there. The only trustworthy evidence is the rendered ConfigMap. (objective 4)

## Question 6

**Type:** SHORT_ANSWER

`iad` is back and read-only. The operator logs:

```json
{"level":"WARN","msg":"divergence detected","site":"iad","divergentTransactions":3,
 "divergentGtid":"f52a03db-45e5-11f1-944b-a6b4a989ea09:1-3",
 "oldPrimaryGtid":"f4d07a53-45e5-11f1-8706-dabe2399558e:1-15, f52a03db-45e5-11f1-944b-a6b4a989ea09:1-3",
 "newPrimaryGtid":"f4d07a53-45e5-11f1-8706-dabe2399558e:1-15"}
```

State the exact number of transactions the outage cost, show the comparison the operator made to reach it, and say what `iad` does next and why.

**Sample answer:**

Three transactions. The operator asks one question — does the new primary's set contain the old primary's? `newPrimaryGtid` is `f4d07a53...:1-15`; `oldPrimaryGtid` is `f4d07a53...:1-15` plus `f52a03db...:1-3`. Containment fails, so the operator computes the set difference old minus new — `GTID_SUBTRACT(old, new)` — which yields `f52a03db-45e5-11f1-944b-a6b4a989ea09:1-3`. Its cardinality is 3 - 1 + 1 = 3, published as `status.sites[].divergentGtid` and `divergentTransactionCount`, and as `bloodraven_divergent_transactions{site="iad"} 3`. `iad` does not rejoin: it lands in `RecoveryBlocked`, with the `RecoveryPending` condition True and reason `DivergentTransactions`, and the message naming the count and the exact reclone annotation. The comparison is trustworthy because the operator fenced `iad` with `SET GLOBAL super_read_only = ON` first and only then re-read `@@global.gtid_executed`, so the set could not grow underneath the comparison. The blocked report is re-verified every 30 s.

**A full-credit answer shows:**

A strong answer covers: (1) the count 3, derived from the interval `1-3` in `divergentGtid` rather than merely quoted from the log field; (2) the direction of the containment test — new contains old — and the failure of it here; (3) the subtraction old minus new (`GTID_SUBTRACT`) as the divergence primitive; (4) the outcome `RecoveryBlocked` / condition reason `DivergentTransactions`, not auto-rejoin. Credit the fence-then-re-read ordering and the 30 s re-verification cadence as depth. Reject any answer that reverses the containment direction, or that reads the shared `f4d07a53...:1-15` range as loss.

**Explanation:**

The shared `f4d07a53...:1-15` range is what both sites hold and is not loss; the loss is the range under `iad`'s own UUID that `pdx` never received. `GTID_SUBSET` answers "did it catch up", `GTID_SUBTRACT` answers "by how much", and the cardinality of the subtraction is your lost-transaction count — an exact number for the incident ticket, not an estimate. That same failed containment is what decides the site's fate, so one reading of the status answers both the accounting question and the rejoin question. (objectives 5, 7)

## Question 7

**Type:** MULTIPLE_CHOICE

`pdx`'s SQL applier was paused with roughly five seconds of writes behind it when `iad` was killed; the failover measured 36.0 s. The review wants the RPO. Which row do you quote?

- RPO 0 — no failover happened at all; the primary came back writable and was kept
- Near zero, not guaranteed zero — the candidate was caught up, so only in-flight work is at risk
- Whatever was in flight — the drain applies what it can reach, the rest is gone
- The worst row — the previously-active binlog died with the PVC and PITR cannot replay the tail

**Correct option index:** 2

**Explanation:**

The 36.0 s is the tell: 24.0 s more than the measured 12.0 s clean kill, which is the drain spending its budget on relay logs that had to be applied. That is the "primary kill with unapplied relay logs" row — the 30 s drain applies what it can reach and everything beyond it is gone. RPO 0 with no failover is the pod-crash-with-PVC-intact row, and a failover demonstrably happened here. "Near zero" is the clean-kill row and is exactly what the 36 s run rules out: a caught-up candidate is the case that returns on the drain's early exit at about 12 s. The worst row requires the PVC to be destroyed with the primary; the pod was killed, the PVC survived, so PITR's material is not the issue. Note also what does *not* bound this: `spec.replication.maxLagSeconds` drives only the `ReplicationLagging` Degraded condition and is not a promotion gate. (objectives 3, 6)

## Question 8

**Type:** TRUE_FALSE

`pdx` is the confirmed writable primary. `iad` returns read-only with no active replication. The operator auto-rejoins `iad` as a replica when `iad`'s GTID set contains `pdx`'s set.

**Correct answer:** false

**Explanation:**

The direction is the other way: the test is whether the **new** primary's set contains the **old** primary's — new contains old. The stated test is the inverted one, and it fails in both directions of usefulness. It goes false the moment `pdx` accepts its first write after promotion, so it would block every rejoin that ought to be automatic; and it passes precisely when the old primary is *ahead*, the one case that must never proceed. Get this backwards and you predict every outcome backwards. Note too what the gate is not: recovery is deliberately not gated on `lastFailoverTarget`, because a primary can change hands with no failover recorded at all — safety comes from a unique, directly confirmed, promotable writable primary observed right now. (objective 7)

## Question 9

**Type:** MULTIPLE_CHOICE

`iad` is blocked with `divergentGtid` starting `589f4b67`. You annotate `bloodraven.shipstream.io/reclone-site=iad:deadbeef`, and moments later `kubectl get mfg playground -o yaml` shows no such annotation at all. What happened?

- The reconciler accepted it and cleared the annotation as the clone started
- It was rejected; the operator emitted a `RecloneRejected` warning event and deleted the annotation
- The CRD's CEL validation rejected the value at admission, so it was never persisted
- The operator dropped it because `iad`'s `RecoveryState` was transiently unset during a reconcile

**Correct option index:** 1

**Explanation:**

A rejected reclone annotation emits a `RecloneRejected` warning event and is then deleted from the CR, precisely so a bad annotation cannot spam the reconciler — `kubectl -n bloodraven-playground get events` carries the reason, verbatim: the prefix `deadbeef` does not match the observed `divergentGtid`, re-read `status.sites[].divergentGtid`. An accepted hot reclone needs a prefix of at least 8 characters that is a true prefix of that set; `deadbeef` is 8 characters but matches nothing. It was not accepted, so no clone started. It was not admission validation: the annotation is a free-form string that reaches the reconciler and is rejected there, which is why you get an event rather than an API error at `kubectl annotate` time. And `RecoveryState` is irrelevant — the interlock keys on the presence of `divergentGtid` only, because `RecoveryBlocked` is a downstream UX field that can be transiently unset. (objective 8)

## Question 10

**Type:** MULTIPLE_CHOICE

`iad` is `RecoveryBlocked` with three divergent transactions. You have extracted them and applied them to `pdx`, so `pdx`'s executed set now contains everything `iad` holds. What is the correct next action?

- Annotate `reclone-site=iad:<8-char prefix>` to rebuild `iad` from `pdx` anyway
- Nothing — containment now holds, and rejoin proceeds at the next 30 s re-verification
- Clear `divergentGtid` on the CR so the interlock stops blocking the rejoin
- Repoint `iad` at `pdx` by hand with `SOURCE_AUTO_POSITION=1` and start replication

**Correct option index:** 1

**Explanation:**

Replay is the elegant exit precisely because it needs no annotation: once `pdx` contains `iad`'s set, the containment test passes on the next 30 s re-verification and the normal five-statement rejoin runs on its own. Recloning now would still work but throws away a full datadir's worth of time and bytes to solve a problem you have already solved — reclone is the right exit when the divergent set is expendable or already extracted *and you are not replaying it*. Hand-editing `divergentGtid` out of status is not an exit at all: you would be deleting the operator's evidence rather than the divergence, and the next re-verification recomputes it. Hand-repointing is never sanctioned: `SOURCE_AUTO_POSITION=1` would ask `pdx` for transactions it never had, and the position-based workarounds that appear to fix that work by discarding the correctness the GTID model provides — replication "starts working" and you have merely lost the ability to detect divergence. (objectives 7, 9)
