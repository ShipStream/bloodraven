# The moving parts

**Unit:** 1 — Meet the group
**Objectives (unit-numbered):**
4. Trace a write from the application through `mysql-orders-primary` to the pod that currently owns it   [obj 4]
5. Say what the sidecar does that the operator cannot, and why the binlog archiver lives there   [obj 5]
6. Tell `primary-candidate`, `dr-only` and `read-only` sites apart by what each is allowed to become   [obj 6]

## Topic generation prompt

Teach the four moving parts of `orders` in the order a write meets them. Start with the Services: there are four *kinds* per group, and for a three-site group that is eight objects — be precise, because 'four Services' is a common misreading. Give the `-primary` selector as two labels and `-replicas` as three, from the grounded facts, and draw the consequence: a pod stamped `role=fenced` matches neither selector, which is how fencing works at the Service layer. Then the operator: a single-replica Deployment with leader election that polls every site and decides. Then the sidecar, and make its independence the point — it fences its own MySQL with the operator dead, and the binlog archiver lives there rather than in the operator because inotify plus direct binlog reads need the data PVC mounted, and a ReadWriteOnce PVC is bound to one node. Finish with the three roles and the single rule that distinguishes them: promotability is exactly `role == primary-candidate`. Use an `anatomy` widget on the string `mysql-orders-primary` breaking it into prefix, group name and role suffix. Do NOT explain how the operator decides to fail over, what the sidecar's fencing rules are, or the failover sequence — Unit 2 and Unit 5 own those.

## Requested activities

- READ: 900-1100 words. Trace one write end to end. Cover the four Service kinds with their real selectors, the object-count arithmetic, the operator's single-replica-plus-leader-election shape, the sidecar's independence and the PVC argument for the archiver, and the three roles. End with the learner able to name every pod's role in `orders`. One `anatomy` widget on `mysql-orders-primary`; optionally one `tree` widget showing the group's objects.
- FLASHCARDS: Confusable pairs — `-primary` vs `-replicas` selector, operator vs sidecar responsibility, `primary-candidate` vs `dr-only` vs `read-only`, active site vs primary candidate. 10-12 cards.
- QUIZ: 5 questions on which Service an application should use for a given workload, what a `role=fenced` pod is reachable through, why the archiver is in the sidecar, and what a `dr-only` site may become.

## Handoff

**Inherits:** The learner knows what deployment Bloodraven targets and can name a failover group's parts.
**Leaves:** The learner can trace a write to a specific pod and state each site's role and promotability in `orders`.
**Do not cover:** The poll loop, the state machine, fencing rules, the failover sequence, DNS steering mechanics (Unit 4).
