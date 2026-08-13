# Flashcards — Upgrading without an incident

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The six ordered-update phases, in order

**Back:** `UpdateReplica`, `WaitReplica`, `Failover`, `UpdateOldPrimary`, `WaitOldPrimary`, `Complete`. They appear verbatim in `status.updatePhase`, which is empty whenever no rollout is running.

---

**Front:** Why the standby is upgraded first

**Back:** MySQL's rolling-upgrade contract: a replica may run a newer version than its source, a source may not run a newer version than its replica. Standby-first keeps the newer MySQL on the replica side for the whole window.

---

**Front:** What triggers an ordered update

**Back:** Spec-hash drift. Under `OrderedUpdate` the reconciler deliberately leaves existing site Deployments untouched, so the desired hash and the live Deployment annotation diverge and the runner hands the rollout to the updater.

---

**Front:** The two preconditions that refuse to start a rollout

**Back:** `precondition: standby <site> is writable; refusing to start ordered update` and `precondition: standby <site> is not replicating`. Both run before the updater takes its lock, so a refused attempt leaves no state behind.

---

**Front:** The mid-rollout abort

**Back:** `standby is writable but replication is not running; aborting ordered update`. A restarted pod comes up writable for a few seconds and cross-site recovery is suppressed during an update, so nothing will start replication for it — waiting the full five minutes would tell you nothing.

---

**Front:** Why the abort counter is not a strict streak

**Back:** A probe error leaves it alone rather than resetting it. Alternating dial errors and 'writable, no source' reads are exactly what a stale pool produces, and a strict streak would let that mask a genuinely broken standby until the outer deadline.

---

**Front:** `updateStrategy: Recreate`

**Back:** Clears the drift list and patches every site Deployment in one pass, so pod restarts may overlap and both sites can be down at once. Never use it for a MySQL version bump: a stalled in-place upgrade leaves no healthy primary and no rollback.

---

**Front:** What a routine image bump does to your dashboards

**Back:** The `Failover` phase is a real promotion: it stamps `lastFailover`, increments `bloodraven_failovers_total`, fires `BloodravenFailoverOccurred`, flips DNS, and moves `activeSite`. It is **not** cooldown-gated, and the fresh `lastFailover` suppresses the next automatic failover for the whole cooldown window.

---

**Front:** The cheapest 'is this us?' check during an alert

**Back:** `kubectl get mysqlfailovergroup <group> -o jsonpath='{.status.updatePhase}'`. Non-empty for exactly the duration of a rollout.

---

**Front:** Helm and CRDs

**Back:** Helm installs CRDs from a chart's `crds/` directory on first install and **never upgrades them**. `helm upgrade` moves the operator binary and silently leaves the CRD schema behind, so new fields are pruned by the API server with no error anywhere. Apply CRDs explicitly, first.

---

**Front:** `spec.sidecarImage`

**Back:** Ships as its own image and is referenced by the group, not the chart. Moving it restarts every site's pod, so it goes through the same ordered update — with the same failover. Bloodraven tolerates one minor of operator/sidecar skew in either direction while pods roll.

---

**Front:** The three one-way doors of upgrading

**Back:** MySQL does not support data downgrade (`MysqlBackup.status.mysqlImage` records what produced each dump); a steady-state per-site version split is not supported, because `spec.image` is one field per group with no `SiteSpec` override; and there is no version admission check at all.
