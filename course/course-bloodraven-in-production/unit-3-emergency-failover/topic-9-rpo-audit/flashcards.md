# Flashcards — What it cost you

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** State the RPO contract for an emergency failover in one sentence.

**Back:** An emergency failover can lose every transaction that committed on the dying primary but had not yet replicated to the surviving site.

---

**Front:** You are handed a rendered bloodraven.cnf. What single test tells you whether a setting in it is a guarantee or merely a default?

**Back:** Ask where the operator writes it relative to your overrides: written after spec.mysqlConf it is an un-weakenable invariant, written before it is a default a tenant can beat.

---

**Front:** Which two durability settings in a Bloodraven-rendered config are defaults rather than guarantees?

**Back:** sync-binlog=1 and innodb-flush-log-at-trx-commit=2 — both live in the base map, applied before spec.mysqlConf overrides.

---

**Front:** Name the settings Bloodraven writes after user overrides, so no spec.mysqlConf entry can weaken them.

**Back:** gtid-mode=ON, enforce-gtid-consistency=ON, log-replica-updates=ON, log-bin, skip-replica-start=ON, and plugin-load-add=mysql_clone.so.

---

**Front:** binlog-expire-logs-seconds — shipped value, and which layer is it in?

**Back:** 1209600 seconds (14 days), in the overridable base map — a tenant can shorten it and silently shrink your PITR reach.

---

**Front:** GTID_SUBSET(set1, set2) — what does it return?

**Back:** True when every GTID in set1 is also in set2; it answers 'has this site caught up', not 'by how much'.

---

**Front:** GTID_SUBTRACT(set1, set2) — what does it return?

**Back:** Only those GTIDs from set1 that are not in set2 — the divergence primitive, whose cardinality is the lost-transaction count.

---

**Front:** status.promotionGtidExecuted — where does its value come from?

**Back:** SELECT @@global.gtid_executed run on the candidate at promotion, before it accepts any write.

---

**Front:** status.sites[].divergentGtid — what does it contain?

**Back:** The GTID set of transactions held by that site which the current primary never received.

---

**Front:** bloodraven_divergent_transactions — what does this gauge report?

**Back:** The number of divergent transactions on a site pending recovery; 0 when healthy.

---

**Front:** spec.replication.maxLagSeconds — default value and the one behaviour it drives.

**Back:** Default 300; it sets the ReplicationLagging reason on the Degraded condition and does nothing else.

---

**Front:** In a MySQL 9.x GTID set, how many transactions are uuid:Domain_1:1-3 and uuid:Domain_2:1-3 together?

**Back:** Six — a user-defined tag is treated as part of the UUID's identity, so the two ranges are distinct transactions.
