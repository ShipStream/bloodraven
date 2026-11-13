# Flashcards — Upgrading without an incident

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** The five ordered-update phases, in order

**Back:** `UpdateReplica`, `WaitReplica`, `Failover`, `UpdateOldPrimary`, `WaitOldPrimary`. They appear verbatim in `status.updatePhase`, which is empty whenever no rollout is running: a finished rollout clears it, and there is no `Complete` value.

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

**Back:** `standby is writable but replication is not running; aborting ordered update`. A standby that is writable after its restart (it boots `read_only=ON`, so something cleared that) with cross-site recovery suppressed during the update has nothing to start replication for it — waiting the full five minutes would tell you nothing.

---

**Front:** Why the abort counter is not a strict streak

**Back:** A probe error leaves it alone rather than resetting it. Alternating dial errors and 'writable, no source' reads are exactly what a stale pool produces, and a strict streak would let that mask a genuinely broken standby until the outer deadline.

---

**Front:** `updateStrategy.type: Recreate`

**Back:** Clears the drift list and patches every site Deployment in one pass, so pod restarts may overlap and both sites can be down at once. Never use it for a MySQL version bump: a stalled in-place upgrade leaves no healthy primary and no rollback.

---

**Front:** What a routine image bump does to your dashboards

**Back:** The `Failover` phase is a real promotion: it stamps `lastFailover`, increments `bloodraven_failovers_total`, fires `BloodravenFailoverOccurred`, flips DNS, and moves `activeSite`. It is **not** cooldown-gated, and the fresh `lastFailover` suppresses the next automatic failover for the whole cooldown window.

---

**Front:** The cheapest 'is this us?' check during an alert

**Back:** `kubectl get mysqlfailovergroup <group> -o jsonpath='{.status.updatePhase}'`. Non-empty for exactly the duration of a rollout.

---

**Front:** Helm and CRDs

**Back:** Helm installs CRDs from a chart's `crds/` directory on first install and **never upgrades them**. `helm upgrade` moves the operator binary and silently leaves the CRD schema behind, so new fields are pruned by the API server with no error anywhere. Apply CRDs explicitly, first, with `kubectl apply --server-side --force-conflicts` — the `MysqlFailoverGroup` CRD is too large for client-side apply.

---

**Front:** `spec.sidecar.image`

**Back:** Optional, with no stored default. Unset, the group runs the sidecar its operator release ships with (the chart's `sidecar.image.repository` and `sidecar.image.tag`), so a `helm upgrade` that ships a new sidecar changes the group's pod spec and runs an ordered update — with the same failover — when the group has its turn in the paced rollout. Set it only to pin a build: a pin stays put, and a pin from another release gets a `SidecarVersionSkew` Warning. A pin holds the sidecar image only; a release that changes the pod in another way rolls pinned groups too. Bloodraven tolerates one minor of operator/sidecar skew in either direction while pods roll.

---

**Front:** The three one-way doors of upgrading

**Back:** MySQL does not support data downgrade (`MysqlBackup.status.mysqlImage` records what produced each dump); a steady-state per-site version split is not supported, because `spec.image` is one field per group with no `SiteSpec` override; and there is no version admission check at all.

---

**Front:** Paced rollout

**Back:** A pod change that an operator release causes (a new launcher, a pod render change, a new default sidecar image) is rolled **one failover group at a time**. A group that is not admitted keeps its pods; a group whose roll started and cannot finish keeps the only slot, so a bad release stops at the first group. `status.rollout.phase` is `Pending`, `Admitted`, `Rolling`, `Stalled` or `Complete`. On by default; `rollout.pacing=false` rolls every group at once.

---

**Front:** What waits with a group that is `Pending` in the paced rollout

**Back:** Its pods stay on the previous release, **and so do spec changes that restart its pods**: an edit to `spec.image` is accepted and waits until the group is admitted. `bloodraven.shipstream.io/rollout-admit=true` admits one group out of turn; remove it afterwards, because the operator reads the key, not the value, and every later release would roll that group without waiting.

---

**Front:** An operator upgrade against an old CRD, with paced rollouts

**Back:** The API server drops `status.rollout`, no admission can be stored, and **no group is rolled at all**. Nothing restarts. The signs are a `RolloutRecordNotStored` Warning Event and `bloodraven_rollout_record_not_stored` above zero. Apply the release's CRDs; the rollout starts within seconds.
