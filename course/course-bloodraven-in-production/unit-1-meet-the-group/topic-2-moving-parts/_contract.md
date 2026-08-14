# The moving parts

**Unit:** 1 — Meet the group
**Objectives (unit-numbered):**
4. Trace a write from the application through `mysql-playground-primary` to the pod that currently owns it   [obj 4]
5. Say what the sidecar does that the operator cannot, and why the binlog archiver lives there   [obj 5]
6. Tell `primary-candidate`, `dr-only` and `read-only` sites apart by what each is allowed to become   [obj 6]

## Topic generation prompt

The learner already has a live cluster. Teach the four moving parts in the order a write meets them. Start with the Services: four kinds, eight objects for a three-site group. Give the `-primary` and `-replicas` selectors. Then the operator (not on the request path), then the sidecar (fences locally; archiver lives there because the PVC will not travel), then the three roles. Use one `anatomy` widget on `mysql-playground-primary`, one `tree` of the eight Services, and one write-path `figure`. Do NOT explain how the operator decides, what the sidecar's fencing rules are, or the failover sequence.

## Requested activities

- READ: 900-1100 words. Trace one write. Cover Services, operator, sidecar, roles.
- FLASHCARDS: Confusable pairs.
- QUIZ: 5 questions on Services, fencing-by-label, the archiver, and `dr-only`.

## Handoff

**Inherits:** A running `playground` and status literacy from topic 1 of this unit.
**Leaves:** The learner can trace a write to a specific pod and state each site's role.
**Do not cover:** The poll loop, fencing rules, the failover sequence, DNS steering (Unit 4), the RPO contract (next topic).
