# Flashcards — The poll loop and per-site state

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `spec.pollInterval` — default, and what the operator does when the field is unset

**Back:** Default `2s`; the operator also hard-defaults to 2 s in Go when the pointer is nil, so an omitted `pollInterval` still ticks every 2 s.

---

**Front:** `spec.failureThreshold` — what it counts, and its default

**Back:** Consecutive failed probes before a site becomes `unreachable`. Default 3 — and one successful probe resets `failCount` to 0, so they must be consecutive.

---

**Front:** `spec.recoveryThreshold` — which transition it gates, and its default

**Back:** Only the transition **to** `writable`: that many consecutive `read_only=0` answers. Default 2.

---

**Front:** You need a dead primary noticed faster. Which of `failureThreshold` and `recoveryThreshold` do you touch?

**Back:** `failureThreshold` — it is the only one of the two in the detection-delay sum. `recoveryThreshold` gates the opposite transition, back to `writable`.

---

**Front:** The SQL statement the poll runs against each site

**Back:** `SELECT @@read_only` — one server variable, nothing else.

---

**Front:** The per-site probe timeout, and how sites are probed

**Back:** 5 s (`context.WithTimeout(ctx, 5*time.Second)`), with every site probed in parallel.

---

**Front:** The four per-site state constants

**Back:** `StateUnknown`, `StateWritable` (`read_only=0`), `StateReadOnly` (`read_only=1`), `StateUnreachable` (connection failed).

---

**Front:** When does the adaptive poll backoff start, and what is the doubling rule?

**Back:** Once a site's `failCount` goes past `failureThreshold`; the interval then doubles per extra failure via `interval := base * time.Duration(1<<uint(backoffFails))`.

---

**Front:** The two ceilings on the adaptive poll backoff

**Back:** `maxPollBackoffExponent = 4` on the exponent, and a 30 s hard cap on the resulting interval.

---

**Front:** Whose `failCount` sets the poll interval — the failing site's, or the whole group's?

**Back:** There is one loop, and its interval comes from the worst `failCount` across all sites, so one site's outage slows the probing of every healthy site too.
