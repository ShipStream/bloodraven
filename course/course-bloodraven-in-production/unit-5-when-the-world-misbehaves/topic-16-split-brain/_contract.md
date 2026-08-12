# Split brain, and what sitePriorities really buys you

**Unit:** 5 — When the world misbehaves
**Objectives (unit-numbered):**
4. Walk the three tiers of split-brain response and say which one your group is on today   [obj 4]
5. Explain why `sitePriorities` is a policy, not a safety feature, and what it silently discards   [obj 5]
6. Recover a split brain: pick a winner, fence, audit, reclone   [obj 6]

## Topic generation prompt

Open with the alert the learner has already seen in the matrix: `SPLIT BRAIN: 2 sites are writable`. Then the question this topic answers — what, if anything, the operator does about it.

Start with the field, and be blunt about the documentation. **`spec.splitBrainPolicy.preferSite` does not exist.** The real field is `spec.splitBrainPolicy.sitePriorities`, an ordered list of site names, and the operator promotes the first entry that is currently writable. The published documentation page describes `preferSite` with a copy-pasteable YAML example; applying that YAML silently no-ops, because the field is not in the CRD schema and unknown fields are pruned. Teach `sitePriorities`, show the learner how to confirm a field exists (`kubectl explain`, or grep the shipped CRD), and say explicitly that documentation can drift from the CRD. That is the real lesson: an hour lost at 3am to a setting that was never applied.

Then the three tiers, in the order the operator evaluates them.

Tier 1 — prior failover history. If `lastFailoverTarget` names a site that is **itself live, writable and promotable**, the operator fences everything else immediately, regardless of policy. All three conditions matter; a recorded target that is unreachable or has been demoted does not win by memory alone.

Tier 2 — no usable history, plus a non-empty `sitePriorities`. `state.ResolveSplitBrain` picks the winner, the operator fences the losers, and the winner is re-promoted through the standard path. `ResolveSplitBrain` refuses sites that are not primary candidates and refuses to guess with an empty list; it never falls back to the order sites were declared in.

Tier 3 — no history and no priorities. Alert only. Manual resolution, by design and by field documentation.

Make the sharpest contrast in the unit explicit: split-brain winner selection deliberately does **not** consult GTID, because every writable side may carry unique writes and there is no "freshest" that is safe. Normal promotion, which the learner met in Unit 2, is GTID-freshest precisely to minimise loss. Same system, opposite selector, for a good reason. Say why.

Cover the operational details that show up in logs and manifests: CEL validation rejects priority entries that do not name `primary-candidate` sites; the emitted log is a `Warn` naming `spec.splitBrainPolicy.sitePriorities` and carrying `winner` and `fencedSite` keys; and fencing is retried on non-transition polls by counting writable candidates directly rather than trusting `action.SplitBrain`, so a fence that failed once gets another attempt without waiting for a state change.

Then the honest paragraph, which the project docs themselves flag with a danger admonition: priority-based resolution makes split-brain resolution fast and deterministic at the cost of silently losing the loser's unreplicated writes. The loss is surfaced loudly but not prevented. `sitePriorities` is a policy decision about which writes you are willing to discard. It is not a safety feature and it does not prevent split brain.

Teach one concrete source of split brain the learner will actually hit: a freshly created or freshly cloned MySQL pod comes up **writable** for several seconds before anything fences it. Recorded runs show a new pod Running and writable at T+22s and `ALERT: SPLIT BRAIN` at T+33s. Restarting a pod is enough to produce this.

Ground it in the wider world. GitHub's October 2018 incident: a 43-second partition left East and West each holding writes the other never saw, and recovery took over 24 hours — the partition was seconds, the cleanup was a day. Orchestrator issue #854: a *graceful* takeover still split-brained, because the new master was made writable before the old one was set read-only, leaving the demoted master holding transactions the cluster never got. And the Pacemaker position on quorum: loss of quorum can take an unbounded time to detect and react to, and the ultimate cure is fencing, locking the other side out. Bloodraven agrees with that last one — that is what the whole fencing layer is.

Finish with objective 6 as a procedure the learner can run on `orders`: pick a winner (by policy or by hand), fence the loser, audit the divergence with `status.sites[].divergentGtid` and the `bloodraven_divergent_transactions` gauge, then reclone with the `bloodraven.shipstream.io/reclone-site` annotation the learner already met. Keep the reclone step as a callback, not a re-teach.

Do NOT cover the partition shapes or their test coverage (topic 3), or what happens when the operator itself is down (topic 4).

## Requested activities

- READ: 1000-1200 words. Lead with the `preferSite` doc error and how to verify a field exists, then the three tiers in evaluation order, then the deliberate absence of GTID from winner selection contrasted with GTID-freshest normal promotion, then CEL validation and the log keys and the non-transition retry, then the policy-not-safety paragraph, then the freshly-cloned-pod split-brain source, then the three external incidents, then the four-step recovery. Use one `terminal` widget showing the split-brain alert and the auto-resolve `Warn` line with its `winner` and `fencedSite` fields, and optionally one `anatomy` widget on a `splitBrainPolicy.sitePriorities` YAML block. Two widgets maximum.
- FLASHCARDS: The split-brain vocabulary and its traps — `sitePriorities` vs the non-existent `preferSite`, the three tiers and their trigger conditions, the tier 1 live/writable/promotable conjunction, why GTID is not consulted, the CEL rule, `winner`/`fencedSite` log keys, what the loser loses. 8-10 cards.
- QUIZ: 5 questions on discriminations: which tier applies given a status dump with and without `lastFailoverTarget`; what happens when `sitePriorities` is empty; whether the freshest site wins a split brain; what applying the documented `preferSite` YAML actually does; whether `sitePriorities` prevents data loss or chooses which data to lose.

## Handoff

**Inherits:** The learner can attribute a fence to a specific sidecar rule and knows fencing blocks rather than cuts off writers.
**Leaves:** The learner can state which split-brain tier `orders` is on, justify or change its `sitePriorities`, and run the pick-fence-audit-reclone recovery.
**Do not cover:** Partition shapes and their coverage (topic 3), operator downtime and cooldown behaviour (topic 4), backup or DR recovery (Unit 6).
