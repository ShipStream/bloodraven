# Flashcards — Verify, and restore

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Schrödinger backup

**Back:** A backup nothing has ever read back. Until something loads it, the artifact is not known to be a backup — it is a file of about the right size.

---

**Front:** Where does the ephemeral verification mysqld listen, and what Service fronts it?

**Back:** It binds 127.0.0.1 only, and no Service is created — the instance is unreachable from outside its own Pod network namespace.

---

**Front:** Minimum size of the ephemeral verification datadir PVC

**Back:** 10 GiB. Auto-sizing is max(10Gi, 1.5 × the backup's sizeBytes rounded up to the nearest 10Gi).

---

**Front:** Verification terminal reason: SanityCheckFailed

**Back:** The sanity query returned a scalar below expect.minRows (or errored outright) — the data is wrong.

---

**Front:** Verification terminal reason: SanityCheckTimeout

**Back:** The sanity query exceeded expect.maxDurationSeconds (default 60) — the instance is wedged rather than wrong.

---

**Front:** A verification sanity query runs cleanly but returns zero rows. What scalar does the check compare against minRows?

**Back:** 0. An empty result set is deliberately treated as scalar 0, so a silently-empty restore fails instead of passing.

---

**Front:** spec.initFromBackup

**Back:** The one-shot restore entry point: it gates normal bootstrap of a brand-new group and is skipped on later reconciles once it has succeeded, even if the field is left in place.

---

**Front:** spec.restoreInPlace

**Back:** The re-runnable restore entry point: it loads a dump into the currently-active primary of a live group, with no teardown-and-rename cycle.

---

**Front:** The in-place restore phases, in order

**Back:** Preflight, Fencing, Restoring, Resuming, then the terminal Succeeded or Failed.

---

**Front:** Format and acceptance rule for spec.restoreInPlace.confirm

**Back:** A required RFC 3339 timestamp that must be strictly greater than the value recorded in status.restoreInPlace.confirmTokenUsed.

---

**Front:** You set pointInTime on a restore while spec.backup.pitr.enabled=false

**Back:** Rejected, identically for both spec.initFromBackup and spec.restoreInPlace: PITR restore requires continuous binlog archival configured on the source.

---

**Front:** spec.keepOnFailure on a MysqlBackupVerification — default, and what it preserves

**Back:** Defaults to true; it leaves the verification Pod and its ephemeral PVC in place after a Failed run so you can exec in and inspect why the load failed.
