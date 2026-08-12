# The bet Bloodraven makes

**Unit:** 1 — Meet the group
**Objectives (unit-numbered):**
1. Say when Bloodraven is the right tool by naming the two-site, non-zero-RPO deployment it targets   [obj 1]
2. Name three things Bloodraven refuses to do, from its own non-goals list   [obj 2]
3. Explain why asynchronous replication with automatic promotion was chosen over Group Replication   [obj 3]

## Topic generation prompt

Open on the situation: `orders` is a MySQL failover group serving a warehouse system across two sites, and the question is not whether a site will be lost but what happens in the ninety seconds afterwards. Teach the deployment target Bloodraven is built for — two or more sites, asynchronous replication, accepted non-zero RPO on unplanned loss — and be concrete that this is a *choice*, not a limitation nobody got round to fixing. Use the RPO contract sentence verbatim from the grounded facts. Then teach the non-goals honestly: no synchronous replication, no zero RPO after sudden primary loss, no reconciliation of divergent writes, no failover-aware connection pools, no durability for PVC-local backups. Land the Group Replication contrast: quorum systems trade write availability for consistency, Bloodraven trades consistency-on-unplanned-loss for write availability and operational simplicity, and both are defensible. State plainly that Bloodraven is public source with no licence file — it is not open-source software, and a reader planning to depend on it should know that. Do NOT explain the state machine, the failover sequence, or any component internals; Unit 1 topic 2 owns the components and Unit 2 owns the decision loop.

## Requested activities

- READ: 700-900 words. Open on `orders` and the site-loss question. Cover the deployment target, the RPO contract in one sentence, the six non-goals, the Group Replication trade, and the licence fact. End by naming the four things that make up a failover group so the next topic can pick them up. Use one `compare` widget contrasting Bloodraven against synchronous/quorum replication on three questions: what happens to writes when a site is lost, what can be lost, and who decides.
- FLASHCARDS: The vocabulary this course will use throughout — asynchronous replication, RPO, RTO, failover group, active site, split brain, fencing, GTID. 8-10 cards.
- QUIZ: 5 questions on whether Bloodraven fits a given requirement (a team demanding zero RPO on sudden loss; a team that needs writes to survive losing one of two sites; a team wanting automatic merge of divergent writes), and on what the non-goals imply for an architecture.

## Handoff

**Inherits:** Nothing — this is the first topic. The learner knows they run MySQL and fear losing a site.
**Leaves:** The learner can say what deployment Bloodraven targets and can name a failover group's parts by name, without yet knowing what any of them do.
**Do not cover:** Component responsibilities (topic 2), the poll loop or state machine (Unit 2), anything about failover timing.
