# Planned failover: moving the primary on purpose

**Unit:** 4 — Where failover meets your application
**Objectives (unit-numbered):**
7. Trigger a planned failover with the annotation and follow the phases through to `Succeeded`   [obj 7]
8. Explain why planned failover is RPO 0 by construction and what the lag gate actually compares   [obj 8]
9. Choose the rollback behaviour for a lag gate that never closes   [obj 9]

## Topic generation prompt

Frame this against topic 2's finding. The emergency path from Unit 3 cannot drain your application's connections — in the modes that matter, the old primary is not there to drain. Planned failover **does** drain, and that is exactly why it is the one path on which a well-configured application crosses a primary move cleanly. Make that connection early and explicitly; it is the reason this topic sits in this unit rather than beside emergency failover.

Teach the trigger first: the annotation key is `bloodraven.shipstream.io/planned-failover`. Then the phase list, in order and complete: `""`, `Pending`, `Deferred`, `Validating`, `Draining`, `WaitingForLag`, `WaitingForDragonflySync`, `PromotingDragonfly`, `Promoting`, `Resuming`, `Succeeded`, `Failed`. Have the learner watch `orders` walk it with the counter app still writing.

Then the mechanism that earns the RPO claim, and be exact, because "planned failover is safe" is the kind of sentence people repeat without knowing why. It is RPO 0 **by construction**, not by luck and not by a lag threshold: fence the source, snapshot the source's `GTID_EXECUTED` at that fence, then promote only once the target's `GTID_EXECUTED` **contains** that snapshot. The gate is a true GTID-set superset test, not a lag-seconds heuristic — contrast it directly with `maxLagSeconds`, which the learner met in Unit 2 as a Degraded condition and which is never a promotion gate. Because the source is fenced before the snapshot is taken, the set the target must catch up to cannot grow underneath it. That is the whole argument. `status.transactionsLost` is 0 on a successful switchover by construction.

Give the defaults from `spec.plannedFailover`: `maxLagWait` 5m, `drainTimeout` 30s, `onCooldown` `reject`. Teach `drainTimeout` carefully, because its failure mode is a decision the learner is inheriting: when the drain budget is exhausted with connections still open, the operator logs that it is proceeding **and proceeds**. Draining is best-effort with a deadline, not a barrier. Tie this straight back to topic 2 — connections that outlive the drain are exactly the ones that will serve stale reads, so the drain budget and your pool's connection lifetime are two halves of one setting.

Then rollback, which is the sharpest operational fact in this topic: **rollback exists only in `WaitingForLag`**. Failures there — `LagTimeout`, `InvalidGTID` — unfence the source and put you back where you started with nothing lost. Failures in `Promoting` or `Resuming` fail **without unfencing**, which means a failure late in the sequence leaves you needing a human. Draw the practical conclusion for obj 9: the lag gate that never closes is the *good* failure. If `maxLagWait` expires against a replica that is genuinely behind, letting the rollback fire and fixing the lag first is nearly always right; raising `maxLagWait` to force the gate closed only lengthens the window during which your source is fenced and your writes are refused.

Cover two admission refusals. A planned failover targeting a `read-only` site is hard-refused with `only primary-candidate sites may be promoted` — the same promotability rule from Unit 1, enforced again at this entry point. And the anti-flap cooldown gates planned failover admission too, with reason `CooldownActive`; `onCooldown` defaults to `reject`, so a planned failover soon after an emergency one is refused rather than queued by default.

Do NOT cover the Dragonfly phases in depth — name `WaitingForDragonflySync` and `PromotingDragonfly` as members of the list and hand them to topic 4. Do NOT re-teach the emergency sequence; reference it.

## Requested activities

- READ: 900-1100 words. Trigger, the full phase list, the GTID superset gate and why it is RPO 0 by construction, the three defaults, the proceed-on-exhausted-drain behaviour tied back to topic 2, the `WaitingForLag`-only rollback, the two refusals, and the emergency-vs-planned contrast as the closing argument. Use one `flow` widget for the phase progression, marking the single phase from which rollback unfences the source and the point after which it does not. Optionally one `terminal` widget showing the annotation being applied to `orders` and the phase field being watched.
- FLASHCARDS: The annotation key; the phase list in order; the gate is a GTID superset test not a lag threshold; fence-then-snapshot ordering; RPO 0 by construction; `maxLagWait` 5m, `drainTimeout` 30s, `onCooldown` reject; drain proceeds when exhausted; rollback only in `WaitingForLag`; the reader refusal string; `CooldownActive`. 10-12 cards.
- QUIZ: 5 questions on discriminations — what the lag gate compares (GTID sets, not seconds) and why `maxLagSeconds` is irrelevant to it; what happens to open connections when `drainTimeout` expires; which phases can roll back and which leave the source fenced; what the right response is to a `WaitingForLag` that will not close; and why a planned failover was refused with `CooldownActive` right after an emergency one.

## Handoff

**Inherits:** The learner knows why the emergency path leaves connections stale and can state the three-part pool fix.
**Leaves:** The learner can move `orders`' primary on purpose, follow it to `Succeeded`, explain the RPO 0 argument, and reason about a stuck lag gate.
**Do not cover:** Dragonfly promotion mechanics, `REPLTAKEOVER`, `sessionsPreserved` (topic 4); backup, PITR, or restore (Unit 6).
