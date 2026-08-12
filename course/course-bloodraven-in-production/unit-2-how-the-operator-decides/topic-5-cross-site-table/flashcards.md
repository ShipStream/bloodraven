# Flashcards — The six-row table that decides everything

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Any site is writable while its role is not primary-candidate. What does EvalCrossSite do?

**Back:** Fills action.FenceSites, sets Alert `writable non-promotable site requires fencing (…)` and Reason "Degraded", and returns immediately — before TotalLoss, SplitBrain, failover or anything else is evaluated.

---

**Front:** Exact trigger condition for Reason "TotalLoss"

**Back:** len(unreachable) == coreCount — every core site unreachable. Alert: `TOTAL LOSS: all sites are unreachable`.

---

**Front:** Exact trigger condition for Reason "SplitBrain"

**Back:** len(writable) > 1 among core sites — strictly more than one, so exactly one writable never qualifies.

---

**Front:** The three conjuncts the failover row needs, all at once

**Back:** len(writable) == 0 AND len(unreachable) > 0 AND len(readOnly) > 0. Only then are PromotionCandidates emitted.

---

**Front:** Which single row of EvalCrossSite sets no action.Alert at all?

**Back:** The failover row. It fills PromotionCandidates and sets Reason "Degraded" but never assigns Alert; every other non-healthy row sets one.

---

**Front:** The two distinct NoPrimary alert messages and what selects between them

**Back:** Exactly two read-only sites with zero unreachable gives `NO PRIMARY: both sites are read-only`; every other no-writable case gives `NO PRIMARY: no writable site available`.

---

**Front:** One site writable, one or more unreachable — what is emitted?

**Back:** Alert `<site> unreachable while <site> is primary` with Reason "Degraded". This row is absent from the published docs table.

---

**Front:** The five Reason strings that actually reach status.conditions

**Back:** Healthy, Degraded, SplitBrain, NoPrimary, TotalLoss. "Failover" is not among them.

---

**Front:** A group's only surviving reachable site is `dr-only` and read-only; the primary-candidate peer is unreachable. Which tally holds the dr-only site, and what is emitted?

**Back:** The readOnly tally — dr-only is not excluded, so the failover row's three conjuncts all hold; but RankPromotionCandidates keeps primary-candidates only, returns empty, and the code falls through to Reason "NoPrimary".

---

**Front:** Selector order once the table has emitted PromotionCandidates

**Back:** pickFreshestCandidate reads GTID_EXECUTED and takes the freshest set; sitePriorities only orders the list to break ties or incomparable sets.

---

**Front:** "EvalCrossSite is pure" — what does that exclude, concretely?

**Back:** Any history or policy beyond the supplied priorities: no prior-failover record, no clock, no MySQL access. Split-brain auto-resolution is layered on by the caller.

---

**Front:** How often is the matrix evaluated, versus how often cross-site actions mutate MySQL?

**Back:** The matrix runs on every poll so status always carries the current condition; the mutating cross-site actions remain transition-driven.
