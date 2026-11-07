# Flashcards — Stand it up and read its status

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `status.activeSite`

**Back:** The single site the operator currently treats as the writable authority — one site name, or empty when no site holds authority.

---

**Front:** How a site's `status.sites[].state` is decided

**Back:** From one query per poll, `SELECT @@read_only`: `0` gives `Writable`, `1` gives `ReadOnly`, a failed connection gives `Unreachable`, and a site not yet polled is `Unknown`.

---

**Front:** `status.sites[].replication`

**Back:** The operator's verdict on that site's replication: `Healthy` or `Lagging` (past `spec.replication.maxLagSeconds`) once both threads have run with a source set for consecutive polls, `Broken` when a thread is stopped or no source is set, `Unknown` when it could not be read or the healthy streak is still building, and `NotApplicable` on the writable primary.

---

**Front:** `status.sites[].gtidExecuted`

**Back:** The executed GTID set read from that follower's replication status: the transaction history it has actually applied.

---

**Front:** A site entry that reads `replication: NotApplicable` and carries no `secondsBehindSource` and no `gtidExecuted` key at all

**Back:** It is the writable primary. The operator only probes replication on `ReadOnly` sites, so it marks the primary `NotApplicable` and leaves the lag and GTID keys absent rather than zero.

---

**Front:** Condition reason `Healthy`

**Back:** Exactly one core site is writable and none is unreachable; the `Degraded` condition reads `False`.

---

**Front:** Condition reason `Degraded`

**Back:** Any shape short of healthy that is not split brain, no primary or total loss — including a live primary with an unreachable peer.

---

**Front:** Condition reason `SplitBrain`

**Back:** More than one core site is writable at the same time.

---

**Front:** Condition reason `NoPrimary`

**Back:** No core site is writable and none is unreachable — every core site is read-only.

---

**Front:** Condition reason `TotalLoss`

**Back:** Every core site is unreachable.
