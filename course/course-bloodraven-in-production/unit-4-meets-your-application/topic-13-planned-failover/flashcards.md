# Flashcards — Planned failover: moving the primary on purpose

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Which annotation triggers a planned switchover, and what may its value contain?

**Back:** `bloodraven.shipstream.io/planned-failover`. The value is a bare site name, or a site name plus `:key=value` overrides — `maxLagWait` is the only supported key, and an unknown key is rejected.

---

**Front:** Recite the planned-failover phases in order.

**Back:** `""`, `Pending`, `Deferred`, `Validating`, `Draining`, `WaitingForLag`, `WaitingForDragonflySync`, `PromotingDragonfly`, `Promoting`, `Resuming`, `Succeeded`, `Failed`.

---

**Front:** `status.plannedFailover.sourceGtidAtFence` — what is it?

**Back:** The source primary's `GTID_EXECUTED`, recorded immediately after `super_read_only=ON` took effect on it.

---

**Front:** Why does the operator fence the source before snapshotting its GTID, rather than after?

**Back:** Because a fenced primary accepts no new client writes, so the GTID set the target must catch up to cannot grow underneath the gate.

---

**Front:** What does `status.plannedFailover.transactionsLost` read after a successful planned switchover?

**Back:** 0, by construction. The field is retained for symmetry with the emergency path's data-loss accounting.

---

**Front:** `spec.plannedFailover.maxLagWait` — default value and what it bounds.

**Back:** 5m. It bounds time spent in `WaitingForLag` before the state machine rolls back to the source.

---

**Front:** `spec.plannedFailover.drainTimeout` — default value and what it bounds.

**Back:** 30s. It bounds how long the fenced source gets to shed application connections during `Draining`.

---

**Front:** `spec.plannedFailover.onCooldown` — default value and its effect.

**Back:** `reject`: a cooldown hit at `Validating` stamps `Failed{CooldownActive}` and clears the annotation, so an admin must re-annotate after the cooldown expires.

---

**Front:** What changes if you set `onCooldown: defer`?

**Back:** The request enters the `Deferred` phase with `retryAfter` stamped, keeps the annotation, and validation is retried automatically at cooldown expiry.

---

**Front:** Verbatim refusal when a planned failover targets a `role: read-only` site.

**Back:** `only primary-candidate sites may be promoted`.

---

**Front:** What does entering `Draining` do to the source's `-primary` Service endpoint?

**Back:** The source's role label is stripped to `fenced`, which matches neither the `-primary` nor the `-replicas` selector, so the write Service sheds its endpoint.

---

**Front:** Confusable pair: `maxLagSeconds` versus `maxLagWait`.

**Back:** `spec.replication.maxLagSeconds` (default 300) drives only the `ReplicationLagging` Degraded condition; `spec.plannedFailover.maxLagWait` (default 5m) is the timeout on the GTID superset gate.
