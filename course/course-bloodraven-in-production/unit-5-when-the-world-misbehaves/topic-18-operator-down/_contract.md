# When the operator is down

**Unit:** 5 — When the world misbehaves
**Objectives (unit-numbered):**
10. Say what keeps working and what stops when the operator pod is gone   [obj 10]
11. Explain why operator downtime costs write availability but not RPO   [obj 11]
12. Decide whether to wait for the operator or promote by hand   [obj 12]

## Topic generation prompt

Scale the operator to zero on the live `orders` playground and watch the counter app keep incrementing. Start there, because the surprise is that nothing happens. A healthy primary and replica keep serving reads and writes with zero operator involvement: the operator is on the failure-detection and promotion path, not the request path.

Teach the distinction that makes the rest of the topic tractable: **availability and correctness fail separately**. Correctness — no split brain, no silent divergence — is preserved by the sidecar fencing layer regardless of how long the operator is gone. Availability is not. If the primary dies while the operator is down, nothing promotes the replica and `orders` has no writable site until the operator returns. Objective 11 is exactly this: downtime costs write availability, not RPO, because RPO is set by what had replicated at the moment the primary died and the operator's absence does not change that number. Say it in one line the learner can repeat.

Teach the deployment shape and its consequence: the chart ships a single replica with leader election. Leader election means extra replicas are standbys, not parallel workers — adding replicas shortens the gap between a crashed leader and a new one taking over, but it cannot shorten the poll loop, which is the actual detection bottleneck the learner measured in Unit 2. Do not let the learner leave believing three operator replicas make failover faster.

Then the anti-flap cooldown as a two-sided thing. It is durable across restarts only if at least one of its two persistence paths worked. `CooldownViolated(restart+stateLost)` is a documented inherent finding class in the deterministic simulator, not a bug queued for a fix: when a restart loses the durable state, the cooldown can be violated, and that is understood and accepted. The other side is more common on call: the cooldown will also **block a genuinely needed second failover**, and it does not care that you believe this one is justified. Manual promotion is the break-glass — the `kubectl bloodraven promote` subcommand exists for this, and the plugin only writes resources the operator already reads, so it is not a back door around the operator's logic. Objective 12 becomes a decision rule: if the operator is coming back within the cooldown and there is no writable site, waiting usually wins; if the operator is not coming back and writes are down, promote by hand and accept that you have taken over the decision.

Close with the best distributed-systems beat in the repository, and give it room. A `SET GLOBAL` that returns an error may still have landed. Cancelling the context tears down the client connection; it does not roll back a write the server already applied. Bloodraven shipped a bug from exactly this: treating the error as a failure made the monitor re-fence a site it had just promoted. Generalise it properly — a timeout or cancellation tells you that *you stopped waiting*, not that the remote side did not act. Every retry, every rollback, and every "it failed so I'll try the other one" decision inherits that ambiguity. Then the wider-world echo: control plane and data plane fail separately in practice, as Cloudflare's November 2023 incident showed, with the data plane serving for roughly two days of control-plane outage.

Do NOT cover backups, PITR, verification, restore or disaster recovery — Unit 6 owns all of it.

## Requested activities

- READ: 900-1100 words. Open on the operator scaled to zero with the counter app unaffected, then what keeps working and what stops, then availability versus correctness and the RPO line, then single replica with leader election and why replicas do not shorten the poll loop, then the cooldown's two sides with the DST finding class, then the wait-or-promote decision rule with `kubectl bloodraven promote`, then the returned-error-that-landed lesson and the Cloudflare echo. Use one `terminal` widget showing the operator at zero replicas, the counter still writing, and the group status frozen at its last observation. No second widget.
- FLASHCARDS: What survives operator downtime, what does not, availability vs correctness, why downtime costs no RPO, leader election vs parallelism, the two cooldown failure directions, `CooldownViolated(restart+stateLost)`, and the cancelled-write ambiguity. 8-10 cards.
- QUIZ: 5 questions on discriminations: which symptoms are caused by operator absence and which are not; whether a two-hour operator outage worsens RPO; whether adding operator replicas speeds detection; when to break glass with a manual promotion; what a `SET GLOBAL` that returned a context-cancelled error tells you about server state.

## Handoff

**Inherits:** The learner can attribute fences to rules, walk the split-brain tiers, and identify a partition shape.
**Leaves:** The learner can survive an operator outage deliberately, knows the cooldown cuts both ways, and treats a failed write as an unknown rather than a rollback.
**Do not cover:** Backups, PITR, verification, restore or multi-cluster DR — all of Unit 6.
