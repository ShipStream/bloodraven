# Flashcards — Connection pools that survive a promotion

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The `-primary` Service selector flips from `iad` to `pdx`. Which of your application's connections does that change?

**Back:** Only new ones. kube-proxy chooses a backend at connection establishment; an established flow is never re-evaluated and never reset.

---

**Front:** What does `SET GLOBAL super_read_only = ON` do to the sockets already open on that instance?

**Back:** Nothing. Fencing is a variable change, not a disconnection — every existing session stays connected.

---

**Front:** How long does a session that survived a fence keep serving stale reads?

**Back:** Until that site is next promoted or demoted.

---

**Front:** Which sessions does `KillAppConnections` deliberately spare?

**Back:** Its own connection and the binlog dump threads — everything else in `information_schema.processlist` is killed.

---

**Front:** In v0.9.1, what is the status of Bloodraven's own fix for the stale-connection gap?

**Back:** Still open: issue #123 is unresolved and PR #137 is unmerged.

---

**Front:** Which Bloodraven path actually drains application connections rather than making one best-effort pass?

**Back:** Planned failover. Nothing else does.

---

**Front:** What does the `BloodravenFailoverOccurred` alert observe?

**Back:** The operator's own `bloodraven_failovers_total` counter — that a promotion happened. It observes nothing about your application or its pool.

---

**Front:** Why does a pool's validation query succeed against a demoted primary?

**Back:** Because the node is alive and answering — it is merely read-only, and a validation `SELECT` is not a write.

---

**Front:** Which MySQL errors does a write hit on a demoted primary?

**Back:** ERROR 1290 (server running with the --read-only option) and ERROR 1792.

---

**Front:** `rejectReadOnly` — what problem did drivers add it to solve?

**Back:** A demoted primary looks healthy to every liveness check, so the driver has to treat a read-only connection as unusable itself.

---

**Front:** Why is a short DNS TTL not enough on the JVM?

**Back:** The JVM's default DNS cache can be infinite for the process lifetime; AWS documents forcing `networkaddress.cache.ttl` to 60 s or less.

---

**Front:** Name the three parts of the pool fix that only work together.

**Back:** A bounded connection lifetime, retry scoped to the read-only error class, and a read/write split across the `-primary` and `-replicas` Services.
