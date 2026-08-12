# Quiz — What it cost you

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A tenant adds both sync_binlog: "0" and gtid_mode: "OFF" to spec.mysqlConf on the orders group. What does the rendered per-site config contain after the next reconcile?

- sync-binlog=0 and gtid-mode=OFF — spec.mysqlConf is the last layer applied
- sync-binlog=0 and gtid-mode=ON
- sync-binlog=1 and gtid-mode=ON — the operator ignores overrides of any durability setting
- Neither is rendered — CEL validation rejects the group before it is admitted

**Correct option index:** 1

**Explanation:**

The two settings sit in different layers. sync-binlog=1 is in the base map, written before spec.mysqlConf, so the tenant's 0 wins and the durability story quietly changes. gtid-mode=ON is in the invariant block written after user overrides, so it is stamped back to ON. Option 0 is the common flattening of the model — it is right about sync_binlog and wrong about gtid_mode. Option 2 assumes the operator classifies settings by what they mean rather than by write order; it does not, which is exactly why sync_binlog is beatable. Option 3 imagines admission-time protection that does not exist: the override is accepted and silently overwritten at render time, with no rejection and no event. (objective 4)

## Question 2

**Type:** TRUE_FALSE

With spec.replication.maxLagSeconds set to 300, a replica reporting 400 seconds of lag is excluded from promotion when the primary dies.

**Correct answer:** false

**Explanation:**

The reversal: Bloodraven promotes it anyway. maxLagSeconds drives exactly one thing — the ReplicationLagging reason on the Degraded condition — and nothing in candidate selection consults it, because no writable site at all is almost always worse than a stale one. The practical consequence is the point: if you believe maxLagSeconds bounds your RPO, your RPO is whatever the lag happened to be when the primary died. A true GTID-superset test is what actually bounds loss, and that gate belongs to the planned path. (objective 4)

## Question 3

**Type:** MULTIPLE_CHOICE

You hold the old primary's gtid_executed set (O) and the value recorded in status.promotionGtidExecuted for the new primary (N). Which expression gives the number of transactions lost?

- GTID_SUBSET(O, N)
- The cardinality of GTID_SUBTRACT(O, N)
- The cardinality of GTID_SUBTRACT(N, O)
- The difference between the highest sequence numbers in O and N

**Correct option index:** 1

**Explanation:**

GTID_SUBTRACT(O, N) returns only those GTIDs from O that are not in N — the transactions the dying primary committed and never shipped — and counting them gives the loss. Option 0 returns a boolean: it tells you whether divergence exists, not how much. Option 2 reverses the operands and yields what the new primary has that the old one lacks, which is normal post-promotion drift, not loss. Option 3 is the eyeball heuristic that breaks on real sets: intervals are sparse, several UUIDs can appear, and a MySQL 9.x tag makes uuid:Domain_1 and uuid:Domain_2 distinct identities, so comparing top sequence numbers can be wrong in either direction. (objective 5)

## Question 4

**Type:** MULTIPLE_CHOICE

Bloodraven's base config sets innodb-flush-log-at-trx-commit=2. According to the MySQL manual, what can that cost you?

- Nothing durable — logs are written at every commit, so committed transactions always survive
- Up to a second of transactions, but only on a power failure or operating system crash
- Up to a second of transactions on any unexpected mysqld process exit
- Only transactions that had not yet been sent to the replica; local commits are unaffected

**Correct option index:** 2

**Explanation:**

The manual is blunter than the operator's docs: with a setting of 2 logs are written at commit but flushed once per second, and any unexpected mysqld process exit can erase up to N seconds of transactions. It recommends 1 alongside sync_binlog=1. Option 0 confuses 'written' with 'flushed' — that gap is the whole loss window. Option 1 is the softer framing worth unlearning: an OOM kill or a crashing mysqld costs you the same second as a power cut, and it is the site that just crashed. Option 3 describes the replication window, which is a separate loss window that adds to this one rather than replacing it. (objective 4)

## Question 5

**Type:** SHORT_ANSWER

The node hosting the active primary iad suffers a disk failure: the pod and its PVC are destroyed together. Nightly backups and PITR binlog archival were enabled. Which row of the per-failure-mode RPO matrix is this, and what specifically cannot be recovered?

**Sample answer:**

This is the worst row — PVC destruction, not a pod crash. It is not the RPO-0 row, because that one requires the PVC to survive so the same primary comes back writable and no failover happens at all. Here the loss is everything iad committed but had not replicated to pdx: those transactions were only ever in the binlog on the destroyed PVC, they were never shipped, so they are not in the replica's binlog stream and therefore not in PITR's replay material. Backups plus PITR get you back to the last archived event, not to the tail. The exact count is read from divergentGtid — except that here the site holding it is gone, so quote the replication window, not a measured number.

**A full-credit answer shows:**

A strong answer covers: (1) identifying the PVC-destruction row rather than the pod-crash row, and saying why the pod-crash row is RPO 0 (PVC intact, primary returns writable, no failover); (2) naming the previously-active binlog on the destroyed PVC as the thing that is gone; (3) the reason PITR cannot cover it — unshipped transactions are not in the replica's binlog stream and so are not in the replay material; (4) bonus credit for noticing that the usual divergentGtid measurement is unavailable because the diverged site no longer exists.

**Explanation:**

The discrimination is between two rows that both start with 'the primary died': with the PVC intact there is no failover and no loss, while with the PVC destroyed you lose the unreplicated tail permanently. The tempting wrong answer is that PITR closes the gap — it does not, because PITR can only replay events that were actually archived, and the tail never left the dead primary. (objective 6)
