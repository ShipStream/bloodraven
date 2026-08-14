# Flashcards — Cooldown, history, and the one exception

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `spec.failoverCooldown` — default, and what the playground uses

**Back:** `5m` by default (CRD default and the operator's own fallback when the field is nil). The playground manifest overrides it to `30s` for fast experimentation.

---

**Front:** Where in the poll cycle is the anti-flap cooldown actually enforced?

**Back:** In exactly one place: an `if` immediately before the promotion call, after the cross-site table has already chosen a candidate. Nothing else in the poll consults it.

---

**Front:** The log line the operator emits when the cooldown blocks a promotion

**Back:** `failover blocked by anti-flap cooldown`, at INFO, with fields `lastFailover` and `cooldown`.

---

**Front:** Name three mutating subsystems that keep running while the cooldown is ticking

**Back:** Source convergence, old-primary recovery, and reclone — each runs from its own poll call site and never reads `failoverCooldown`. (DNS reconcile and both fencing paths are in the same group.)

---

**Front:** The two annotation keys that carry the failover history on the object itself

**Back:** `bloodraven.shipstream.io/last-failover` (the instant, RFC3339 UTC at second precision) and `bloodraven.shipstream.io/last-failover-target` (the promoted site name), written together in one JSON merge patch.

---

**Front:** Why is the failover record written to both status and annotations?

**Back:** Status is a subresource: its writes travel a separate API path with their own RBAC rule (`mysqlfailovergroups/status`) and admission chain, so one path can be broken or denied while the other still records the promotion.

---

**Front:** `FailoverClockSkewGrace`

**Back:** `5 * time.Minute` — a durable copy stamped more than five minutes ahead of local time is discarded rather than installed, because the cooldown gate reads negative elapsed time as still active.

---

**Front:** The verbatim `msg` the operator logs when it restores writability on a fenced promoted primary

**Back:** `re-asserting fenced promoted primary: no site is writable and the last failover target is GTID-complete; restoring writability`, at WARN, with field `site`.

---

**Front:** Which metric counts primary re-asserts, and what does a climbing value mean?

**Back:** `bloodraven_primary_reassert_total{site}` — a steadily increasing counter means something keeps fencing the promoted primary, so investigate sidecar connectivity to the operator.

---

**Front:** `spec.updateStrategy`

**Back:** `OrderedUpdate` (default) leaves existing site Deployments untouched so the runner sees spec drift and rolls one site at a time; `Recreate` clears the drift list and patches every site Deployment in one pass, so pod restarts may overlap.
