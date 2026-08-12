# What it cost you

**Unit:** 3 — Emergency failover, end to end
**Objectives (unit-numbered):**
4. State the RPO contract in one sentence and say which settings are guarantees and which are merely defaults   [obj 4]
5. Read `promotionGtidExecuted` and `divergentGtid` to get an exact lost-transaction count   [obj 5]
6. Pick the right row of the per-failure-mode RPO matrix for an outage in front of you   [obj 6]

## Topic generation prompt

The failover in topic 1 worked. This topic answers the question the incident review will actually ask: how many transactions did we lose? Teach the learner to answer with a measured number rather than an estimate.

**Start with the contract, verbatim:** an emergency failover can lose every transaction that committed on the dying primary but had not yet replicated to the surviving site. One sentence, memorised. Everything else in the topic is either an amplifier or a mitigation of that sentence.

**Then the distinction the documentation flattens, and make it the spine of the topic.** Bloodraven writes MySQL configuration in three layers. `sync_binlog=1` and `innodb_flush_log_at_trx_commit=2` are set in the base my.cnf map — applied **before** `spec.mysqlConf` overrides. They are defaults, not guarantees. A tenant can set `sync_binlog=0` in `spec.mysqlConf` and it wins, silently breaking the durability story the RPO documentation tells. `binlog-expire-logs-seconds = 1209600` (14 days) is in the same overridable layer. By contrast the invariants are written **after** user overrides and cannot be weakened: `gtid_mode=ON`, `enforce_gtid_consistency=ON`, `log_replica_updates=ON`, `log_bin`, `skip_replica_start=ON`, and the clone plugin load; the `skip-log-bin` and `disable-log-bin` aliases are deleted outright. Give the learner the test: if it is written after the overrides it is a guarantee, if before it is a default. Have them check their own group's rendered ConfigMap rather than trusting the layer diagram.

Sharpen the `innodb_flush_log_at_trx_commit=2` point with the upstream manual, which is harsher than Bloodraven's docs: the manual attributes the loss window to **any unexpected mysqld process exit**, not only power loss, and it recommends `=1` alongside `sync_binlog=1`. Bloodraven ships `=2` for throughput. That is a defensible trade, but the learner should know they are one `spec.mysqlConf` line away from the stricter setting and should know what the shipped default costs them — up to a second of committed transactions on a crash, on the site that just crashed.

**Then the arithmetic.** `SELECT @@global.gtid_executed` is what the operator records at promotion, into `promotionGtidExecuted`. The divergence path computes the set difference of the old primary's set minus the new primary's, and a transaction count from it, surfaced as `status.sites[].divergentGtid` and as the `bloodraven_divergent_transactions` gauge. Teach the two MySQL primitives underneath so the learner can do the arithmetic by hand on any pair of sets: `GTID_SUBSET(set1, set2)` returns true when every GTID in set1 is also in set2, and `GTID_SUBTRACT(set1, set2)` returns only those GTIDs from set1 that are not in set2 — that subtraction is the divergence primitive, and its cardinality is the lost-transaction count. Add the MySQL 9.x wrinkle that will bite anyone eyeballing sets: GTID sets can carry user-defined **tags**, and a tag is part of the UUID's identity for set purposes, so `uuid:Domain_1:1-3` and `uuid:Domain_2:1-3` are different transactions.

**Make `maxLagSeconds` the distractor**, because it is a top-ranked misconception. Its default is 300 and it drives **only** the `ReplicationLagging` Degraded condition. It is **not** a promotion gate: nothing in the candidate-selection path consults it, and if the primary dies while the replica is beyond the threshold, Bloodraven still promotes — because no writable site at all is almost always worse. Say plainly that a learner who believes `maxLagSeconds` bounds their RPO has an RPO of whatever the lag happened to be. What actually bounds RPO on the planned path is a true GTID-superset test, and that is why planned failover is RPO 0 by construction — mention it in one sentence as the contrast, do not teach the planned path here.

**Finish with the per-failure-mode matrix** so objective 6 is exercised: pod crash with the PVC intact is RPO 0 and no failover at all; a clean primary kill with a caught-up replica is near-zero but not guaranteed zero; a kill with unapplied relay logs loses whatever was in flight; PVC destruction is the worst row, because the previously-active binlog lived on the destroyed PVC and PITR cannot recover the tail — transactions the old primary committed but never shipped are not in the replica's binlog stream and therefore not in PITR's replay material. Give the learner an outage description and have them pick a row.

Do NOT cover how the old primary rejoins, `RecoveryBlocked`, or reclone (topic 3). Do NOT teach PITR mechanics or restore (Unit 6) beyond the one sentence about what PITR cannot reach.

## Requested activities

- READ: 1000-1200 words. The contract sentence; the three-layer config model with the before/after test and the overridable-versus-invariant lists; the upstream manual's harsher framing of `innodb_flush_log_at_trx_commit=2`; `promotionGtidExecuted`, `divergentGtid`, the gauge, and `GTID_SUBSET`/`GTID_SUBTRACT` with a worked count; the GTID tag wrinkle; `maxLagSeconds` as a condition and not a gate; the per-failure-mode rows. Use one `compare` widget with two columns — overridable defaults versus un-weakenable invariants — listing the actual variable names on each side. Optionally one `terminal` widget showing a `GTID_SUBTRACT` worked example producing a count.
- FLASHCARDS: The contract sentence; `sync_binlog=1` and `innodb_flush_log_at_trx_commit=2` as overridable; the five invariants; `binlog-expire-logs-seconds` 14 days; `maxLagSeconds` 300 and what it drives; `GTID_SUBSET` versus `GTID_SUBTRACT`; `promotionGtidExecuted`; `divergentGtid`; `bloodraven_divergent_transactions`. 10-12 cards.
- QUIZ: 5 questions discriminating: whether setting `sync_binlog=0` in `spec.mysqlConf` takes effect (it does) against whether setting `gtid_mode=OFF` does (it does not); whether a replica 400 seconds behind will be promoted when the primary dies (yes — `maxLagSeconds` is not a gate); which expression yields the lost-transaction count from two GTID sets; what `innodb_flush_log_at_trx_commit=2` can lose according to the MySQL manual; and picking the RPO matrix row for a described outage.

## Handoff

**Inherits:** The learner has driven an emergency failover on `orders` and knows the promotion sequence and its timing.
**Leaves:** The learner can state the RPO contract, separate durability guarantees from durability defaults in a rendered config, and produce an exact lost-transaction count for the failover they just ran.
**Do not cover:** Old-primary rejoin, divergence handling, reclone (topic 3); PITR and restore (Unit 6); planned failover mechanics (Unit 5).
