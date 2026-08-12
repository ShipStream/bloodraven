# Cache and sessions that follow the primary

**Unit:** 4 — Where failover meets your application
**Objectives (unit-numbered):**
10. Say what Bloodraven guarantees about Dragonfly and what it explicitly does not   [obj 10]
11. Trace a Dragonfly promotion through `REPLTAKEOVER` and its fallback, and read `sessionsPreserved`   [obj 11]
12. Explain how the active Dragonfly Service sheds an endpoint atomically during a takeover   [obj 12]

## Topic generation prompt

Open with the scope boundary and hold it for the whole topic: Dragonfly here is **cache and session state, never durable application data**. If losing it costs you a transaction, it was in the wrong place. Everything else in this topic follows from that one sentence — it is why the emergency budget is small, why the fallback is allowed to lose sessions, and why nothing about Dragonfly is ever permitted to affect MySQL durability. For `orders`, the concrete framing is the counter app's session store: nice to keep across a promotion, never the record of truth.

Teach the Service mechanism first, because it is the cleanest idea in the topic. The active Dragonfly Service AND-gates two labels — a role label and a traffic label. Shedding an endpoint **deletes** the traffic key rather than stamping a disabled value, and the reason is precise: the active-Service selector is an exists-and-equals check on `enabled`, so removing the key removes the endpoint atomically, whereas writing some "disabled" value would depend on the selector agreeing about what disabled means. Have the learner derive the consequence: during a takeover there is no window in which both instances match the active selector.

Then the two promotion paths, and keep them clearly separate. The **emergency** path tries `REPLTAKEOVER` first and falls back to `REPLICAOF NO ONE`, which promotes the target but loses sessions — the operator logs that outcome in those words. The **planned** path has **no** `REPLICAOF NO ONE` fallback: it is `REPLTAKEOVER` or nothing, because a planned move that cannot preserve sessions can simply be retried, whereas an emergency one cannot. The emergency path runs under a hard-coded 10 s budget and, by explicit design, never returns an error to its caller, never blocks longer than that bounded budget, and never leaves Dragonfly in a state that affects MySQL durability. Say what that means operationally: MySQL failover is never delayed by cache.

Then `sessionsPreserved`, and insist on the tri-state. It is a `*bool`: true when sessions were preserved, false when they were lost, and **nil when the field is unknown**. Nil is not false. A learner who reads nil as failure will chase incidents that did not happen; a learner who reads it as success will miss ones that did. Give the settings that shape the outcome: `maxSyncWait` defaults to 30 s and doubles as the timeout argument passed to `REPLTAKEOVER`, with the client adding 5 s of I/O grace on top; `onSyncTimeout` is an enum of `proceed` or `fail`, defaulting to `proceed`. Spell out what `proceed` means — the promotion goes ahead and the cache outcome is whatever it is.

Add the upstream fact honestly: `REPLTAKEOVER` is real but undocumented. It is an ADMIN-port, `GLOBAL_TRANS` command taking a timeout in seconds, introduced in Dragonfly v1.5.0, with no official documentation page. The reference is the Dragonfly source and the release notes, not a manual. Then close on two pieces of straight talk. First, the "Dragonfly v1.38.0 or later" requirement is a **support policy, not a guardrail** — nothing in the API types, the controller, or the chart enforces or checks a version. The only CEL rules are that an image is required when Dragonfly is enabled, and that the image may not be `:latest`. Run an older build and Bloodraven will not stop you; it will simply fail in ways nobody has characterised. Second, the pinned playground image has drifted two minors behind current stable. Both facts are the same lesson: version discipline here is yours, not the operator's.

Do NOT cover backup, PITR, or snapshot paths — Unit 6 owns those. Do NOT re-teach the planned failover phase list; reference `WaitingForDragonflySync` and `PromotingDragonfly` as already-known members of it.

## Requested activities

- READ: 900-1100 words. Scope boundary first, then the two-label AND-gate and why shedding deletes the key, then emergency vs planned promotion paths, then the 10 s budget and its never-blocks-MySQL guarantee, then `sessionsPreserved` as a tri-state with `maxSyncWait`/`onSyncTimeout`, then the `REPLTAKEOVER` provenance, then the version-policy and image-drift honesty. Use one `compare` widget setting the emergency path against the planned path on three questions: what is tried, what happens on failure, and what happens to sessions. Optionally one `anatomy` widget on the traffic label `shipstream.io/dragonfly-traffic=enabled` showing why deletion and not a disabled value.
- FLASHCARDS: Dragonfly is cache and session state, never durable data; the two AND-gated labels; shedding deletes the traffic key; emergency = `REPLTAKEOVER` then `REPLICAOF NO ONE` with sessions lost; planned has no fallback; 10 s hard-coded emergency budget; never blocks MySQL; `sessionsPreserved` tri-state including nil; `maxSyncWait` 30 s plus 5 s client grace; `onSyncTimeout` proceed/fail default proceed; `REPLTAKEOVER` is ADMIN-port, `GLOBAL_TRANS`, Dragonfly v1.5.0, undocumented; the version floor is policy with only two CEL rules. 10-12 cards.
- QUIZ: 5 questions on discriminations — what a `sessionsPreserved` of nil does and does not tell you; which promotion path can fall back and what that costs; whether a slow Dragonfly can delay a MySQL promotion; why the traffic label is deleted rather than set to a disabled value; and what actually happens if you run a Dragonfly build below the stated minimum.

## Handoff

**Inherits:** The learner can move `orders`' primary on purpose and knows the planned phase list, including the two Dragonfly phases.
**Leaves:** The learner can state Bloodraven's Dragonfly guarantee and its limits, read `sessionsPreserved` correctly, and explain the atomic endpoint shed. The application half of failover is complete.
**Do not cover:** Backup, PITR, verification, or restore (Unit 6); operator observability and blindness (Unit 5).
