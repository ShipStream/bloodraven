# Flashcards — The bet Bloodraven makes

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Asynchronous replication

**Back:** A write commits and is acknowledged on the primary before any other site has seen it — nothing waits for a peer.

---

**Front:** RPO (recovery point objective)

**Back:** How much recently committed data you accept losing. Bloodraven's RPO on sudden primary loss is not zero.

---

**Front:** RTO (recovery time objective)

**Back:** How long you accept being unable to write. This is where Bloodraven spends its engineering budget.

---

**Front:** Failover group

**Back:** One `MysqlFailoverGroup` resource: two to sixteen MySQL sites, exactly one of them writable at a time.

---

**Front:** Active site

**Back:** The one site in the group currently allowed to accept writes; `mysql-orders-primary` points at it.

---

**Front:** Split brain

**Back:** Two sites accepting writes at the same time, so each accumulates transactions the other never saw.

---

**Front:** Fencing

**Back:** Forcing an instance to stop accepting writes — in MySQL, setting `super_read_only=ON` on it.

---

**Front:** Why `super_read_only` rather than `read_only` for fencing?

**Back:** `read_only` still permits updates from `CONNECTION_ADMIN` (or the deprecated `SUPER`); `super_read_only` blocks even those.

---

**Front:** GTID

**Back:** A global transaction identifier — a per-transaction ID that makes two servers' executed-transaction sets directly comparable.

---

**Front:** `status.sites[].divergentGtid`

**Back:** The set of transactions the old primary executed that the new primary never received — the exact, countable data loss.
