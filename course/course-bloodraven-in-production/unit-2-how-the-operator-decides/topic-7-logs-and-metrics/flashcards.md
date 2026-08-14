# Flashcards — Reading the operator's mind from logs and metrics

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The `state transition` log event — which four fields does it carry?

**Back:** `site`, `from`, `to`, `fg`. `from`/`to` are one of `unknown`, `unreachable`, `read-only`, `writable`.

---

**Front:** Why is changing a `msg` string in Bloodraven a breaking change?

**Back:** The `msg` strings in the Event reference are a published stability contract that downstream log pipelines filter on by exact match; they change only with a deprecation note in `CHANGELOG.md`.

---

**Front:** You see `bloodraven_site_state{site="pdx",state="read-only"} 1`. What are the other three `pdx` series doing?

**Back:** They are all `0`. It is a state-set: every poll writes all four states for every site, exactly one of them `1`.

---

**Front:** `bloodraven_replication_lag_seconds{site="pdx"} -1` — what happened?

**Back:** `Seconds_Behind_Source` came back NULL, meaning the site is not replicating at all. It is a sentinel, not a lag value.

---

**Front:** `spec.replication.readOnlyMaxLagSeconds` is nil versus explicitly `0` — what is the difference?

**Back:** Nil inherits `maxLagSeconds` (default 300). An explicit `0` is meaningful and demands zero reported lag before the reader may serve.

---

**Front:** Which metric increments exactly once per `state transition` log line, with the same labels?

**Back:** `bloodraven_state_transitions_total{site, from, to}`.

---

**Front:** Which single series tells you whether the operator's poll loop is still completing cycles?

**Back:** `bloodraven_poll_latency_seconds_count` — its rate going to zero means no cycle has finished, so every other gauge is stale.

---

**Front:** What does `bloodraven_primary_reassert_total{site}` count?

**Back:** Times the operator restored writability on the last failover target after finding it fenced with no writable site remaining.

---

**Front:** Which label does `bloodraven_failovers_total` carry?

**Back:** `target_site` — the site that was promoted, not the one that died.

---

**Front:** Two JSON streams arrive on the operator's stdout. How do you keep only the contractual one?

**Back:** Keep records with `time` and `msg` (operational `slog`); drop records with `ts` and `logger` (controller-runtime `zap`, not a stable interface).
