# Flashcards — The nine steps of a promotion

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The step of the failover sequence that appears nowhere in the documentation

**Back:** Step 2 — kill application connections on the old primary: SELECT id FROM information_schema.processlist WHERE id != CONNECTION_ID() AND command NOT IN ('Binlog Dump', 'Binlog Dump GTID'), then KILL each id.

---

**Front:** Which threads does the connection-kill query deliberately spare, and why?

**Back:** Binlog Dump and Binlog Dump GTID — the replication dump threads, so killing application sessions does not tear down replication to the surviving sites.

---

**Front:** The four steps of Execute() whose error aborts the failover

**Back:** STOP REPLICA, RESET REPLICA ALL, SET GLOBAL super_read_only = OFF, SET GLOBAL read_only = OFF — the four that change the candidate's own replication and read-only state.

---

**Front:** Relay-log drain: budget, first wait, wait ceiling

**Back:** 30 s budget; the first wait is 500 ms and doubles each round to a 4 s ceiling.

---

**Front:** The relay-log drain's early-exit condition

**Back:** The SQL thread is running and Seconds_Behind_Source reads 0 — the drain returns immediately.

---

**Front:** A site reports super_read_only = ON. What does that tell you?

**Back:** It has been fenced — super_read_only is what the sidecar and the operator fence with, and it blocks writes even from CONNECTION_ADMIN or SUPER.

---

**Front:** Measured time for status.activeSite to flip after a clean primary kill on the playground

**Back:** 12.0 s, reproducible across nine-plus independent recorded runs (12.004 s to 12.02 s, one outlier at 13.008 s).

---

**Front:** What scenario 14 does to the replica before it kills the primary

**Back:** Pauses the replica's SQL applier and seeds five seconds of writes, so the candidate has unapplied relay logs when the drain starts.

---

**Front:** What the operator stamps before it flips DNS, and why

**Back:** The durable failover record and the failover counter — deliberately, so a DNS-provider outage cannot erase the fact that a promotion happened.

---

**Front:** Which code path applies the node taint around a failover?

**Back:** The per-site transition handler, earlier in the same poll — taints are a pure function of the state transition, not a step of the failover sequence.

---

**Front:** Source convergence: budget and where it runs

**Back:** An independent poll stage with its own 20 s budget, repointing replicas at the authoritative site — not a link in the promotion chain.

---

**Front:** Why does scenario 01 hold the site down with scale-to-0 instead of deleting the pod?

**Back:** Determinism — a pod-delete races the ~5 s Deployment respawn, and that race can restore the original topology through split-brain recovery instead of completing the failover.
