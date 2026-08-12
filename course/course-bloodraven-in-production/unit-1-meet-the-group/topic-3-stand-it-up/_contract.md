# Stand it up and read its status

**Unit:** 1 — Meet the group
**Objectives (unit-numbered):**
7. Bring up a three-site group with `./playground/setup.sh` and confirm every site reports Ready   [obj 7]
8. Read a `MysqlFailoverGroup` status and name the active site, each site's state, and the replica's lag   [obj 8]
9. Watch the counter application write through `mysql-orders-primary` and find the same row on the replica   [obj 9]

## Topic generation prompt

This is the hands-on topic that creates the running example every later unit touches. Give the prerequisites (Docker or Podman, kubectl, helm, a cluster with at least three worker nodes — the third is dedicated to the reader so storage-loss testing is deterministic), the k3d creation command, and `./playground/setup.sh`. Then spend most of the reading on status literacy, because it is the skill the whole course rests on: `status.activeSite`, `status.sites[].state`, `.role`, `.replicating`, `.secondsBehindSource`, `.gtidExecuted`, and the group conditions with their reason strings — the five real reasons are `Healthy`, `Degraded`, `SplitBrain`, `NoPrimary` and `TotalLoss`, and they come straight from the decision matrix. Show port-forwarding the dashboard and counter app, incrementing the counter, and finding the row on the replica. Include one honest warning, from the grounded facts: the playground overrides shipped defaults — `failoverCooldown: 30s` against a default of 5m, `maxLagSeconds: 30` against 300, `dns.ttl: 10` against 60 — so a timing observed here is not the shipped default. Use one `terminal` widget on the status output whose values the learner predicts before revealing. Do NOT interpret what the operator will *do* with any of these states; that is Unit 2's entire subject.

## Requested activities

- READ: 900-1100 words. Prerequisites, cluster creation, setup, then status literacy in depth, then the counter app and the replica check, then the playground-overrides warning. End with the learner staring at a healthy `orders` and able to describe it precisely. One `terminal` widget on the status read.
- FLASHCARDS: Status fields and condition reasons — `activeSite`, `state`, `replicating`, `secondsBehindSource`, `gtidExecuted`, and the five condition reasons. 8-10 cards.
- QUIZ: 5 questions reading a given status dump: which site is active, which is a reader, whether the group is healthy, and what a `secondsBehindSource` of null means as against zero.

## Handoff

**Inherits:** The learner can name the parts of `orders` and each site's role.
**Leaves:** `orders` is running with three sites and a counter application writing to it; the learner can read its status and describe the current state precisely.
**Do not cover:** What the operator does with these states, debounce, failover, or any chaos injection.
