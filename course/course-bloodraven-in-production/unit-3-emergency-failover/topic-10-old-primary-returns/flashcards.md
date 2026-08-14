# Flashcards — The old primary comes back

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** A returning old primary: what single test decides whether it rejoins automatically?

**Back:** Whether the NEW primary's GTID set contains the OLD primary's. Contains it — auto-rejoin. Does not — RecoveryBlocked.

---

**Front:** Why the operator fences the returning site and then re-reads its GTID set, instead of using the read taken on arrival

**Back:** The fence guarantees the set can no longer grow, so the post-fence read is the authoritative set the divergence comparison runs against.

---

**Front:** The five statements the operator runs on a returning site when containment holds, in order

**Back:** SET GLOBAL super_read_only = ON, STOP REPLICA, RESET REPLICA ALL, CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1, START REPLICA.

---

**Front:** The upstream MySQL rule that forces STOP REPLICA in front of the rejoin's CHANGE REPLICATION SOURCE TO

**Back:** Both the receiver thread and the applier thread must be stopped before a CHANGE REPLICATION SOURCE TO that employs SOURCE_AUTO_POSITION = 1.

---

**Front:** RecoveryPending is True. What separates reason RecoveryInProgress from reason DivergentTransactions?

**Back:** RecoveryInProgress means the STOP/RESET/CHANGE/START rejoin sequence is running; DivergentTransactions means it is blocked, and the message names the divergent count and the reclone annotation to apply.

---

**Front:** Why RecoveryInProgress is persisted to status before the rejoin SQL runs, not after

**Back:** It is the durable handoff for an operator restart that lands in the middle of the STOP/RESET/CHANGE/START sequence.

---

**Front:** How often a RecoveryBlocked report is re-verified

**Back:** Every 30 seconds — so a site that diverges further gets a refreshed set, and one whose divergence you resolved externally rejoins on the next pass.

---

**Front:** The reclone annotation key, and its two accepted value forms

**Back:** bloodraven.shipstream.io/reclone-site, valued either <siteName> or <siteName>:<divergentGtidPrefix>.

---

**Front:** Minimum length of the divergent-GTID prefix in a hot reclone annotation

**Back:** 8 characters, and it must be a true prefix of the observed status.sites[].divergentGtid.

---

**Front:** What the operator does with a reclone annotation it rejects

**Back:** Emits a RecloneRejected warning event naming the exact fix, then deletes the annotation so a bad value cannot spam the reconciler.

---

**Front:** The one status field the reclone interlock keys on

**Back:** The presence of divergentGtid — never RecoveryState, which is a downstream UX field that can be transiently unset during a reconcile.

---

**Front:** How the operator decides a returning site's datadir is genuinely empty

**Back:** By whether its GTID UUIDs are shared with the new primary's history — not by whether user schemas exist.
