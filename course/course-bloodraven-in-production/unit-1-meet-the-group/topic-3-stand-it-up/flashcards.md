# Flashcards — Stand it up and read its status

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `status.activeSite`

**Back:** The single site the operator currently treats as the writable authority — one site name, or empty when no site holds authority.

---

**Front:** How a site's `status.sites[].state` is decided

**Back:** From one query per poll, `SELECT @@read_only`: `0` gives `writable`, `1` gives `read-only`, a failed connection gives `unreachable`, and a site not yet polled is `unknown`.

---

**Front:** `status.sites[].replicating`

**Back:** The operator's verdict on whether replication is healthy on that follower — populated only for sites currently in `read-only` state, never for the writable primary.

---

**Front:** `status.sites[].gtidExecuted`

**Back:** The executed GTID set read from that follower's replication status: the transaction history it has actually applied.

---

**Front:** A site entry that carries no `replicating`, no `secondsBehindSource` and no `gtidExecuted` key at all

**Back:** It is the writable primary. The operator only probes replication on `read-only` sites, so those keys are absent rather than zero or false.

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
