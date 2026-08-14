# Flashcards — The moving parts

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The `mysql-playground-primary` Service selects on which pod labels?

**Back:** Two: `app.kubernetes.io/instance=playground` and `shipstream.io/role=primary`. No health label.

---

**Front:** The `mysql-playground-replicas` Service selects on which pod labels?

**Back:** Three: `app.kubernetes.io/instance=playground`, `shipstream.io/role=replica`, and `shipstream.io/healthy=yes`.

---

**Front:** How many Service objects does the operator create for a group, and what is the formula?

**Back:** `2 × len(sites) + 2` — two per-site kinds plus two group-wide kinds. For three-site `playground`: eight.

---

**Front:** A pod is stamped `shipstream.io/role=fenced`. What changes at the Service layer?

**Back:** It matches neither the `-primary` nor the `-replicas` selector, so it drops out of both group endpoints while still running.

---

**Front:** What does the operator do that no sidecar can?

**Back:** Decide across sites — it is the only component that sees every site and picks which one is primary.

---

**Front:** What does the sidecar do that the operator cannot?

**Back:** Fence its own MySQL — set `super_read_only=ON` locally — with the operator dead or unreachable.

---

**Front:** Why can the binlog archiver not run centrally in the operator?

**Back:** It needs the MySQL data PVC mounted, and a `ReadWriteOnce` PVC is bound to one node — so it must run in the pod that has it.

---

**Front:** Which binlog files does the archiver upload?

**Back:** Only sealed ones — it drops the last entry of the index, which is the binlog MySQL is currently writing.

---

**Front:** Role: `primary-candidate`

**Back:** The only role that may be promoted. Promotability is exactly `role == primary-candidate`.

---

**Front:** Role: `dr-only`

**Back:** Counted in the topology tallies like any core site, but never eligible for promotion.

---

**Front:** Role: `read-only`

**Back:** Excluded from `coreCount` and all three state tallies; never taints a node and cannot be a backup source.

---

**Front:** Active site versus primary candidate — what is the difference?

**Back:** `primary-candidate` is a static role you declare in `spec.sites`; the active site is the one site currently holding writable authority.
