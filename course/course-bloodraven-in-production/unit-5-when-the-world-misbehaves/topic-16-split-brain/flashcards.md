# Flashcards — Split brain, and what sitePriorities really buys you

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `spec.splitBrainPolicy.sitePriorities`

**Back:** An ordered list of site names. During split-brain auto-resolution the operator promotes the first entry that is currently writable and `primary-candidate`, and fences every other writable site.

---

**Front:** `spec.splitBrainPolicy.preferSite`

**Back:** A field that exists only in the documentation. It is not in the CRD schema, so the API server prunes it on apply — no error, no event, no effect.

---

**Front:** You want to confirm that a field the docs describe actually exists on this cluster. What do you run?

**Back:** `kubectl explain mysqlfailovergroup.spec.splitBrainPolicy` to read the live schema, or grep the shipped CRD under `config/crd/bases/` and `charts/bloodraven/crds/`.

---

**Front:** Tier 1's guard — the three conditions the recorded `lastFailoverTarget` must satisfy

**Back:** The site must exist (`keepSite != nil`), be currently `StateWritable`, and be `isPromotable()`. All three, as a conjunction.

---

**Front:** `state.ResolveSplitBrain(writable, sitePriorities)`

**Back:** Walks `sitePriorities` in order and returns the earliest entry that is currently writable and `primary-candidate` as `winner`; every other writable site is returned as a loser for the caller to fence.

---

**Front:** Why is GTID freshness deliberately not consulted when picking a split-brain winner?

**Back:** Because every writable side may carry unique writes, so no GTID set contains the other and there is no "freshest" that is safe to promote. Winner selection is therefore policy-driven.

---

**Front:** The CEL rule guarding `sitePriorities`

**Back:** Every entry must match the name of a site whose `role` is `primary-candidate`; the group is rejected at admission otherwise.

---

**Front:** The two log keys carried by the split-brain auto-resolve `Warn`

**Back:** `winner` and `fencedSite` — on the message "split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities".

---

**Front:** Why does the split-brain fence retry count writable candidates directly instead of trusting `action.SplitBrain`?

**Back:** Because the matrix reports `SplitBrain=false` whenever a writable non-promotable site needs fencing first, and a stable split brain emits no further transitions — so a fence that failed once would never be retried.

---

**Front:** What becomes of the loser's unreplicated writes after a priority-based resolution?

**Back:** They are isolated on the loser's PVC — reported as `status.sites[].divergentGtid` and the `bloodraven_divergent_transactions` gauge, and recoverable only by recloning that site.
