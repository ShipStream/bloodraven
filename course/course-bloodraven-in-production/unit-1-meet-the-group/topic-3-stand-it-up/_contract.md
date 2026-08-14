# Stand it up and read its status

**Unit:** 1 — Meet the group
**Objectives (unit-numbered):**
7. Bring up a three-site group with `./playground/setup.sh` and confirm every site reports Ready   [obj 7]
8. Read a `MysqlFailoverGroup` status and name the active site, each site's state, and the replica's lag   [obj 8]
9. Watch the counter application write through `mysql-playground-primary` and find the same row on the replica   [obj 9]

## Topic generation prompt

This is now the first topic. Open on the running example, not the vocabulary. Give the prerequisites, the k3d creation command, and `./playground/setup.sh`. Then spend most of the reading on status literacy: `status.activeSite`, `status.sites[].state`, `.replicating`, `.secondsBehindSource`, `.gtidExecuted`, and the five condition reasons. Show port-forwarding the counter app, incrementing it, and finding the row on the replica. Include the playground-overrides warning. Close with a first-hour glossary of eleven words. Use one `terminal` widget on the status output and one topology `figure`. Do NOT interpret what the operator will *do* with any of these states; that is Unit 2.

## Requested activities

- READ: 900-1100 words. Prerequisites, cluster creation, setup, then status literacy, then the counter app, then the playground-overrides warning, then the first-hour glossary.
- FLASHCARDS: Status fields and condition reasons.
- QUIZ: 5 questions reading a given status dump.

## Handoff

**Inherits:** Nothing — this is the first topic.
**Leaves:** `playground` is running; the learner can read its status and has a first-hour glossary.
**Do not cover:** Component internals, the poll loop, the bet / RPO contract in depth (topic 3 of this unit).
