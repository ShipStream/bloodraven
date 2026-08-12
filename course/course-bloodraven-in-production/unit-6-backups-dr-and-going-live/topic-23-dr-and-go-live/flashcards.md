# Flashcards — Losing a whole cluster, and the go-live gate

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The bar for declaring a DR source dead

**Back:** At least two of three independent signals: the source operator's /active-site returns 5xx, the source cluster's API server is unreachable, and source MySQL is TCP-unreachable from a third vantage point outside both clusters.

---

**Front:** Cross-cluster split brain in Bloodraven v1

**Back:** Nothing detects or resolves it. No operator watches both clusters, so the human fencing decision is the safety mechanism.

---

**Front:** `BucketReadable=True` on a MysqlStandbyCluster

**Back:** The DR cluster listed the source bucket prefix and could read it.

---

**Front:** `SourceConfigKnown=True` on a MysqlStandbyCluster

**Back:** The source dump metadata parsed, so status.discovered now carries the dump name, location, GTID set and archived binlog window.

---

**Front:** What a MysqlStandbyCluster does when its source cluster dies

**Back:** Nothing. Phase 1 is observability only: no MySQL contact, no restore Jobs, no activation.

---

**Front:** `spec.restoreInPlace` (as distinct from the DR-bootstrap field)

**Back:** A re-runnable restore into the already-live active primary, gated by an RFC 3339 confirm token — not the path that stands up a new group in a DR cluster.

---

**Front:** The seven `kubectl bloodraven` subcommands

**Back:** status, promote, reclone, backup, verify-backup, version, help.

---

**Front:** Why `kubectl bloodraven promote` obeys every gate the annotation obeys

**Back:** The plugin only writes resources the operator already reads, and never talks to MySQL directly — there is no back door in it.

---

**Front:** `sync_binlog=1` in a Bloodraven group

**Back:** An overridable default written into the base my.cnf before spec.mysqlConf is applied, so a user override wins silently — read the value off the running instance.

---

**Front:** `spec.replication.maxLagSeconds` (default 300)

**Back:** It drives only the ReplicationLagging Degraded condition; a replica beyond the threshold is still promoted.

---

**Front:** The three playground values that are not the shipped defaults

**Back:** failoverCooldown 30s (default 5m), replication.maxLagSeconds 30 (default 300), and dns.ttl 10 (default 60).

---

**Front:** The DNS limit during a DR cutover

**Back:** The operator cannot accelerate DNS propagation, and it owns only the per-cluster record — the global application-facing name is flipped by you.
