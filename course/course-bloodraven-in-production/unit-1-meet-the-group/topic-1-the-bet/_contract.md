# The bet Bloodraven makes

**Unit:** 1 — Meet the group
**Objectives (unit-numbered):**
1. Say when Bloodraven is the right tool by naming the two-site, non-zero-RPO deployment it targets   [obj 1]
2. Name three things Bloodraven refuses to do, from its own non-goals list   [obj 2]
3. Explain why asynchronous replication with automatic promotion was chosen over Group Replication   [obj 3]

## Topic generation prompt

The learner already has a live group and can name its parts. Open on that running state, then teach the deployment target, the RPO contract sentence verbatim, the six non-goals, and the Group Replication trade. Use one `compare` widget. Do NOT carry the licence question here. Do NOT explain the state machine or the failover sequence.

## Requested activities

- READ: 700-900 words. Open on the live group. Cover the deployment target, the RPO contract, the six non-goals, and the Group Replication trade.
- FLASHCARDS: asynchronous replication, RPO, RTO, failover group, active site, split brain, fencing, GTID.
- QUIZ: 5 questions on whether Bloodraven fits a given requirement.

## Handoff

**Inherits:** A running `playground`, status literacy, and named parts.
**Leaves:** The learner can say what deployment Bloodraven targets and what it will refuse to do.
**Do not cover:** The poll loop or state machine (Unit 2), anything about failover timing.
