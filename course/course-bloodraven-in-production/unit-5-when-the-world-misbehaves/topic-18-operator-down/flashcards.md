# Flashcards — When the operator is down

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The operator pod for `playground` is deleted while both MySQL sites are healthy. What do applications see?

**Back:** Nothing. Reads and writes keep flowing — the operator is on the failure-detection and promotion path, not the request path.

---

**Front:** Which layer preserves correctness while the operator is gone?

**Back:** The sidecar fencing layer — separate processes with their own timers, so no site accepts writes it is not authorised to accept, however long the outage lasts.

---

**Front:** Where is the RPO of an emergency failover actually decided?

**Back:** At the instant the primary died — by what had already replicated to the survivor. Nothing that happens afterwards moves that number.

---

**Front:** The single observable change on the CR during an operator outage.

**Back:** `status.*` stops updating — `status.sites[].lastSeen` freezes at the last poll.

---

**Front:** `replicaCount: 3` on the Bloodraven chart buys you what?

**Back:** A faster handover after the leader crashes. The extra replicas are idle standbys, not parallel workers.

---

**Front:** The detection floor you cannot buy your way past with more operator replicas.

**Back:** `pollInterval × failureThreshold` — 2 s × 3 = 6 s on the shipped defaults.

---

**Front:** `CooldownViolated(restart+stateLost)`

**Back:** The deterministic simulator's name for a restart that lost both durable anti-flap copies and promoted earlier than `failoverCooldown` allowed — a documented inherent finding class, not a queued bug.

---

**Front:** The cooldown failure direction you actually meet on call.

**Back:** It blocks a second failover you genuinely need, for the rest of `spec.failoverCooldown` (default 5m), regardless of how justified you believe this one is.

---

**Front:** What `kubectl bloodraven promote playground pdx` actually does.

**Back:** Writes the `bloodraven.shipstream.io/planned-failover` annotation the operator reads. It never touches MySQL, so it needs a live operator to execute.

---

**Front:** Cloudflare, November 2023 — the operational lesson for a Bloodraven operator.

**Back:** Control plane and data plane fail separately: the data plane kept serving for roughly two days of control-plane outage.
