# Five partitions, five answers

**Unit:** 5 — When the world misbehaves
**Objectives (unit-numbered):**
7. Match an observed symptom to one of the five documented partition scenarios   [obj 7]
8. Explain why a broken MySQL-to-MySQL link is not a failover   [obj 8]
9. Work the on-call partition checklist without guessing   [obj 9]

## Topic generation prompt

"The network is partitioned" is not one failure. Teach the five shapes Bloodraven documents in `docs/docs/network-partitions.mdx` and use that page's own labels and numbering — do not rename, renumber, or invent shapes. For each, give the symptom the learner sees in `playground`, what the operator does, and what the sidecar does. Two anchors you can name directly: the operator-to-site partition, which the deterministic simulator injects as `partitionOperatorSite`; and the site-to-site pair partition, injected as `partitionPair`.

Then do the thing most courses skip: say how well each shape is actually tested, because knowing which failure modes are exercised is part of trusting a system. Scenario A is exercised by chaos scenarios 09 and 06 plus DST. Scenario B is exercised by chaos 17. Scenario C is **DST only**. Scenario D, asymmetric reachability, is **analysis only** — the DST fault model cannot even express it, because its pair key is symmetric by construction, so one-way reachability is unrepresentable in the simulator. Scenario E is covered only indirectly, by chaos 11. Present this as a coverage table, not as an apology. An untested failure mode is not a bug; an untested failure mode you believed was tested is.

Teach objective 8 as its own beat. A broken cross-site replication link triggers **no** automatic action, and the reason is epistemic rather than lazy: from the operator's point of view that mode is indistinguishable from "the replica fell behind because of I/O pressure". Human judgement decides. Connect this to what the learner already knows — the primary is still writable, so the matrix has no failover row to reach, and lag drives only the `ReplicationLagging` Degraded condition, never a promotion. The operator declining to act here is the system working.

Then the credibility section: why naive partition tests are no-ops. Host-netns iptables rules on a k3d node do not partition Kubernetes Service traffic, because kube-proxy's DNAT happens in different chains — the operator keeps reaching MySQL through the ClusterIP while you believe you have severed it. And a NetworkPolicy can be silently ineffective: chaos scenario 33 found a CNI evaluating the policy post-DNAT, so the canary kept resolving DNS through the entire 45-second hold. State the general rule hard: a chaos experiment that injects nothing produces a confident false pass, and a green run of a broken experiment is worse than no experiment. Every partition test needs an independent check that the partition exists. Add one short callback to the frozen-poll incident the learner already dissected in Unit 2 as evidence that partitions surface as *believable* status, not as errors — one sentence, no re-teach.

Then the Kubernetes statefulness traps that manufacture partitioned writable primaries, which is where partitions stop being a networking topic and become a data-integrity one. Kubernetes will not delete pods merely because a node is unreachable; the pod sits `Terminating` or `Unknown` indefinitely, deliberately, to protect at-most-one identity. Force-deleting it breaks at-most-one: the API object disappears while the process may still be running and still writing on the partitioned node. `Terminating` is set by the API server, not the kubelet, so on an unreachable node the container keeps running and keeps writing to the PV. And `ReadWriteOnce` means one **node**, not one pod — storage attach is not fencing. Draw the conclusion for `playground`: the reason a partitioned site must fence itself is that nothing above it can be relied on to stop it.

Finish with objective 9 as a real on-call checklist the learner works in order: confirm the partition actually exists before believing any symptom, identify the shape from the symptom, read the group status and the sidecar logs to see which side fenced itself, decide whether the operator has an action to take at all, and only then decide whether to intervene.

Do NOT cover operator downtime, leader election, or the cooldown (topic 4), and do not cover backup or DR responses (Unit 6).

## Requested activities

- READ: 1000-1200 words. Open on "the network is partitioned" being five different questions, walk the five shapes with symptom and response, present the coverage honestly including the analysis-only shape D, teach the broken-replication-link non-action, then the two no-op injection traps and the general rule, then the four Kubernetes at-most-one facts, then the checklist. Use one `compare` widget across the five scenarios with rows for observed symptom, operator response, sidecar response, and test coverage. Optionally one `terminal` widget showing the independent check that a partition is real before trusting a result. Two widgets maximum.
- FLASHCARDS: The five shapes by label, their coverage status, why a broken replication link is no action, the two injection traps, and the four Kubernetes facts — unreachable node does not delete pods, force-delete breaks at-most-one, `Terminating` is API-server-set, `ReadWriteOnce` is per node. 10-12 cards.
- QUIZ: 5 questions matching a symptom to a shape, deciding whether the operator will act on a broken cross-site replication link, judging whether a described injection actually partitioned anything, and saying what a force-deleted pod on a partitioned node may still be doing.

## Handoff

**Inherits:** The learner can attribute fences to sidecar rules and can walk the split-brain tiers.
**Leaves:** The learner can name the partition shape from a symptom, knows which shapes are tested and which are argued, and will verify an injection before trusting its result.
**Do not cover:** Operator downtime, leader election, the anti-flap cooldown or manual promotion (topic 4); backups, PITR and DR (Unit 6).
