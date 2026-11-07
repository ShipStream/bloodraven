# Flashcards — Planned failover: moving the primary on purpose

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Which annotation triggers a planned switchover, and what may its value contain?

**Back:** `bloodraven.shipstream.io/planned-failover`. The value is a bare site name, or a site name plus `:key=value` overrides — `maxLagWait` is the only supported key, and an unknown key is rejected.

---

**Front:** Recite the planned-failover phases in order.

**Back:** `Admitting`, `Deferred`, `Fencing`, `Draining`, `WaitingForLag`, `WaitingForDragonflySync`, `PromotingDragonfly`, `Promoting`, `Publishing`, `RollingBack`, `Succeeded`, `Failed`. There is no idle phase: a group with no run has no block.

---

**Front:** `status.plannedFailover.fence.gtidSet` — what is it?

**Back:** The source primary's `GTID_EXECUTED`, recorded immediately after `super_read_only=ON` took effect on it and was read back (`fence.state: Applied`).

---

**Front:** Why does the operator fence the source before snapshotting its GTID, rather than after?

**Back:** Because a fenced primary accepts no new client writes, so the GTID set the target must catch up to cannot grow underneath the gate.

---

**Front:** After a successful planned switchover, what shows that nothing acknowledged was lost?

**Back:** `result.outcome: Succeeded`, and `status.lastFailover.promotionGtidExecuted` equal to `status.plannedFailover.fence.gtidSet`. There is no lost-transaction counter on this path: the gate makes the number zero by construction.

---

**Front:** `spec.plannedFailover.maxLagWait` — default value and what it bounds.

**Back:** 5m. It bounds time spent in `WaitingForLag` before the state machine rolls back to the source. Admission accepts 1s to 1h, for the field and for the annotation's `maxLagWait=` override alike.

---

**Front:** `spec.plannedFailover.drainTimeout` — default value and what it bounds.

**Back:** 30s. It bounds how long the fenced source gets to shed application connections during `Draining`.

---

**Front:** `spec.plannedFailover.onCooldown` — default value and its effect.

**Back:** `Reject`: a cooldown hit in admission ends the run `Failed{NotStarted, CooldownActive}` and clears the annotation, so an admin must re-annotate after the cooldown expires.

---

**Front:** What changes if you set `onCooldown: Defer`?

**Back:** The request enters `Deferred` with `admission.deferReason: CooldownActive` and `admission.retryTime` stamped, keeps the annotation, and admission is retried automatically at cooldown expiry.

---

**Front:** Verbatim refusal when a planned failover targets a `role: ReadOnly` site.

**Back:** `only PrimaryCandidate sites may be promoted`.

---

**Front:** What happens to the source's `-primary` Service endpoint once the run fences it?

**Back:** From `Fencing` on, the source's role label is stripped to `fenced`, which matches neither the `-primary` nor the `-replicas` selector, so the write Service sheds its endpoint. Established connections are left to `Draining`.

---

**Front:** Confusable pair: `maxLagSeconds` versus `maxLagWait`.

**Back:** `spec.replication.maxLagSeconds` (default 300) drives only the `ReplicationLagging` Degraded condition; `spec.plannedFailover.maxLagWait` (default 5m) is the timeout on the GTID superset gate.
