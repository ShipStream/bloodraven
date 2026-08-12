# Unit 5 test — Fencing under uncertainty

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you say which rule fenced a site, which split-brain tier `orders` is on, and why a broken cross-site replication link produces no automatic action?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

A log bundle from `orders`. The `pdx` sidecar logs `SELF-FENCING:` and then `SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore`. From the same bundle: the operator pod was Running and answering `/healthz` throughout, and both `iad` and the reader answered `/peer/ping` on every tick in the minute before the fence. Which rule fired, and how do you know from that evidence alone?

- Rule #2, lease expiry — a `SELF-FENCED:` line is only ever emitted after `leaseTimeout` elapses, so the peers must have been silent for 20 s whatever the ping log says.
- Rule #1, topology mismatch — the cached authoritative `activeSite` was non-empty and named a site other than `pdx`, and rule #1 fires and returns before the lease is ever consulted.
- The startup safety net — the pod never got permission to write, and the operator answered with a different site.
- Neither rule — with the operator reachable the monitor takes no action, so this read-only state came from the operator's own fence step during a failover.

**Correct option index:** 1

**Explanation:**

The monitor evaluates two rules per tick, in a fixed order: rule #1 topology mismatch, then rule #2 lease expiry. Rule #1 fires and *returns*, so a site that has learned the active site is somebody else fences without consulting the lease at all — which is exactly this bundle, where operator and peers were reachable the whole time. Option 1 (rule #2) inverts the evidence: rule #2 requires the operator **and every peer** to be silent for the full `leaseTimeout`, and the ping log rules that out; the `SELF-FENCED:` line is emitted by either rule. Option 3 (safety net) is the wrong log prefix — a `safety net:` line is a pod that has never been allowed to write since boot, while `SELF-FENCING:` / `SELF-FENCED:` is a pod that was writing and lost the argument. Option 4 confuses the sidecar with the operator: the operator's fence step writes no `SELF-FENCED:` line, and the monitor does take action with the operator up — that is precisely what rule #1 is for. (objective 1)

## Question 2

**Type:** MULTIPLE_CHOICE

You added a `role: read-only` reader to `orders` for reporting. Months later the active site `iad` loses its network path to the operator and to `pdx`, but the reader sits in the same rack and its sidecar keeps answering `/peer/ping` on every tick. Defaults are unchanged (`leaseTimeout` 20 s, `peerCheckInterval` 5 s). Twenty-five seconds into the isolation, what is `iad` doing?

- Fenced at T+20 s: the lease expired because a majority of the parties it tracks — the operator and `pdx` — went silent, and two out of three is a quorum loss.
- Fenced at T+20 s: a `role: read-only` site is excluded from the peer set, so only the operator and `pdx` counted, and both were silent for the whole window.
- Still accepting writes: rule #2 fences only when the operator **and every peer** are silent for the full window, and a reader that answers `/peer/ping` is a peer, so it suppresses the fence.
- Still accepting writes, but only until the next `peerCheckInterval` tick at T+30 s, when the fourth consecutive miss trips the fence.

**Correct option index:** 2

**Explanation:**

The lease rule is an all-parties rule, not a vote. One reachable peer keeps the primary writable, and a `role: read-only` reader counts as a peer — it relays topology and answers `/peer/ping`, and a reachable peer without fresh authoritative topology can still suppress the lease-only fence. Adding a reader therefore makes the lease fence *less* likely to fire, which is worth knowing before you size a group. Options 1 and 2 both smuggle in counting: option 1 assumes a majority rule, but this is documented as retained compatibility behaviour, not a quorum guarantee — there is no counting, no majority, no tie-break; option 2 assumes readers are excluded from the peer set, which is true of the matrix's writable/read-only/unreachable tallies but not of sidecar peering. Option 4 invents a consecutive-miss counter: the rule is a single continuous silence window of `leaseTimeout`, and the reader is never silent, so no number of ticks changes the answer. Note also what this does *not* weaken — rule #1 still fences `iad` the moment anyone tells it the active site has moved. (objective 2)

## Question 3

**Type:** TRUE_FALSE

The operator for `orders` has been at zero replicas for twenty minutes. During that window the `pdx` MySQL pod is rescheduled onto another node. True or false: the rescheduled pod comes back up writable and stays writable until the operator returns to fence it.

**Correct answer:** false

**Explanation:**

False. `Server.RunSafetyNet` is a one-shot that completes before the `FencingMonitor` is even constructed, and it fences first and asks afterwards: it sets `super_read_only=ON` on boot, queries the operator for the active site, and clears the fence only if the answer names this site. With the operator at zero replicas there is no answer, so it logs `safety net: could not query active site, staying fenced` and the pod stays fenced for the whole outage. The grain of truth in the claim is that mysqld itself does come up writable for a few seconds before anything fences it — that is where split brains actually come from when the operator is alive, with a recorded run showing a new `pdx` pod writable at T+22s and `ALERT: SPLIT BRAIN` at T+33s. But the safety net is exactly what closes that window when nobody is watching. This is the correctness half of the operator-downtime split: correctness is preserved by the sidecar layer regardless of how long the operator is gone; only write availability is lost. (objectives 3, 10)

## Question 4

**Type:** MULTIPLE_CHOICE

`orders` has two core sites, `iad` and `pdx`, plus a `role: read-only` reader. You restart the `pdx` pod and the group goes to `SPLIT BRAIN: 2 sites are writable (iad, pdx)`. `spec.splitBrainPolicy` is omitted from the manifest entirely. `status.lastFailoverTarget` is `iad`, and `iad` is right now writable and still `role: primary-candidate`. What does the operator do?

- Nothing but alert. With no `sitePriorities` there is no policy to apply, so this is tier 3 and resolution is manual by design.
- Falls back to the order the sites are declared in under `spec.sites` and keeps whichever is listed first.
- Compares the two GTID sets and keeps the freshest side, exactly as it does when picking a promotion candidate.
- Fences every site except `iad` immediately — tier 1 applies because the recorded failover target is currently live, writable and promotable, and tier 1 runs regardless of policy.

**Correct option index:** 3

**Explanation:**

The three tiers are evaluated in order, and tier 1 — prior failover history — comes first. Its trigger is a conjunction: the recorded `lastFailoverTarget` must name a site that is live, writable **and** promotable. All three hold here, so the operator fences everything else immediately and the missing policy never matters. Option 1 is the trap: the absence of `sitePriorities` only puts you on tier 3 when there is *no usable history*, and here there is. Option 2 names a fallback that explicitly does not exist — `ResolveSplitBrain` never falls back to declared order; an empty list returns nothing and the operator refuses to guess. Option 3 imports the wrong selector: split-brain winner selection deliberately does not consult GTID, because every writable side may carry unique writes and neither set contains the other, so there is no freshest that is safe. Restarting a pod is the everyday source of this state — it is worth knowing which tier your group lands on before you cause one. (objective 4)

## Question 5

**Type:** SHORT_ANSWER

Tier 2 has just fired on `orders`. The operator log reads `split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities` with `winner=iad` and `fencedSite=pdx`, and the incident channel says "auto-resolved, nothing to do". Explain why that reading is wrong, and give the steps you run next.

**Sample answer:**

The resolution is a policy decision, not a repair. `sitePriorities` does not prevent split brain and merges nothing — it makes resolution fast and deterministic at the cost of silently losing the loser's unreplicated writes, and the loss is surfaced loudly but not prevented. The winner was picked from my ordered list, not from GTID freshness, so `pdx` may well hold original writes `iad` never saw; that is a standing decision I made in advance about which writes I am willing to discard. What is left is the audit and the rejoin. Pick a winner is already done by policy. The loser is already fenced with `SET GLOBAL super_read_only = ON` — remember that fencing blocks rather than cuts off, so surviving sessions on `pdx` can still serve stale reads until it is next demoted or promoted. Then audit the divergence: read `status.sites[].divergentGtid` for the exact set `pdx` has and `iad` never saw, and the `bloodraven_divergent_transactions` gauge for the count; the condition reason will be `DivergentTransactions` and its message names the annotation. Then reclone `pdx` with `bloodraven.shipstream.io/reclone-site=pdx:<divergentGtidPrefix>`, at least 8 characters, matched against the observed `divergentGtid` so a fat finger is rejected. Do not manually re-attach `pdx` as a replica while it carries divergent GTIDs.

**A full-credit answer shows:**

A strong answer covers: (a) that priority-based resolution is a policy, not a safety feature — it discards the loser's unreplicated writes rather than preventing loss, and that loss is surfaced but not prevented; (b) that the winner was chosen by the ordered list, not by GTID freshness, so the fenced side may hold unique writes; (c) the recovery steps in order — winner already picked by policy, loser fenced with `super_read_only=ON`, audit via `status.sites[].divergentGtid` and the `bloodraven_divergent_transactions` gauge (condition reason `DivergentTransactions`), then reclone via the `bloodraven.shipstream.io/reclone-site=<site>:<gtidPrefix>` annotation with a prefix of at least 8 characters. Credit mentioning that a fenced site still serves stale reads because fencing closes no sockets, or that manually re-attaching a divergent site is the wrong move. Do not credit an answer that treats the auto-resolve as having reconciled or merged both sides.

**Explanation:**

The auto-resolve is the operator executing your standing decision about which writes to throw away, and the project's own docs flag it with a danger admonition for that reason. The `winner` and `fencedSite` keys are alertable precisely because a human still owes the group an audit and a reclone: the fenced site is read-only, but its divergent transactions are still sitting on its disk and it cannot rejoin as a replica until they are gone. Treating the log line as the end of the incident leaves a site that will never converge. (objectives 5, 6)

## Question 6

**Type:** MULTIPLE_CHOICE

A page on `orders`: "the network is partitioned". You look. `status.activeSite` is `iad`, `iad` is `writable`, `pdx` is `read-only`, `lastSeen` is fresh on every site including the reader, no site is `unreachable`, `pdx`'s IO thread is stopped and `secondsBehindSource` is climbing, and `bloodraven_failovers_total` has not moved. Which of the five documented shapes is this?

- A — the operator cannot reach `iad` while `pdx` stays reachable.
- B — the replica is isolated while the primary stays reachable.
- C — the MySQL-to-MySQL link is broken while the operator still reaches both sites.
- D — asymmetric peer reachability, where `iad` reaches `pdx` but `pdx` cannot reach `iad`.

**Correct option index:** 2

**Explanation:**

Fresh `lastSeen` on every site is the discriminator: both sites poll cleanly, so nothing is blind to the operator and only the binlog stream is severed — shape C. Option 1 (A) requires `iad` to go `unreachable` after 6 s and `pdx` to be promoted; neither happened. Option 2 (B) requires `pdx` to be unreachable to the operator, or at least for the isolation to be the replica's link to Bloodraven rather than to MySQL — here `pdx` polls fine and is merely not replicating. Option 4 (D) is a peer-reachability shape, visible as replication or peer checks failing one way only, and it is the one shape with no test of any kind — it is argued from the code, not observed, because the simulator's `pairKey` sorts the two site names and so cannot express one-way reachability at all. Shape C itself is DST-only, injected as `partitionPair`, with no live chaos scenario; knowing which shapes are exercised and which are merely argued is part of trusting the answer you just gave. (objective 7)

## Question 7

**Type:** MULTIPLE_CHOICE

Twelve minutes into that same incident, `pdx` is 400 s behind, `spec.replication.maxLagSeconds` is 30 on this playground group, the `Degraded` condition reads `ReplicationLagging`, and nothing has promoted. Which reading is correct?

- The anti-flap cooldown is suppressing the promotion; it will fire once `spec.failoverCooldown` expires.
- Nothing is stalled. The failover row needs zero writable sites plus at least one unreachable and at least one read-only, and `iad` is still writable, so that row is unreachable by construction — and lag drives only the `ReplicationLagging` condition, never a promotion.
- Lag past `maxLagSeconds` is a promotion gate: the operator is refusing to promote a replica that far behind, and raising the threshold would release it.
- The operator is waiting for `pdx`'s sidecar to self-fence before it is allowed to promote anything.

**Correct option index:** 1

**Explanation:**

A broken cross-site replication link triggers no automatic action, and the reason is epistemic rather than lazy: from the operator's point of view it is indistinguishable from "the replica fell behind because of I/O pressure". The evidence is identical, so human judgement decides — keep serving writes on `iad`, or wait for `pdx` to catch up and run a planned failover, which is RPO 0 by construction. Option 1 blames the cooldown, but the cooldown gates a failover the matrix has already decided to run; here the matrix never reaches the failover row at all. Option 3 is the most attractive wrong answer and is backed by nothing: `maxLagSeconds` drives only the `ReplicationLagging` Degraded condition and is explicitly not a promotion gate — if the primary dies while the replica is far behind, Bloodraven still promotes it, because no writable site is almost always worse. Option 4 inverts the fencing rules: `pdx` is already read-only, and a read-only instance never self-fences because there is nothing left to fence. The operator declining to act here is the system working. (objective 8)

## Question 8

**Type:** MULTIPLE_CHOICE

You inject a partition against `iad` with `iptables -A INPUT -p tcp --dport 3306 -j DROP` on its k3d node. Forty-five seconds later the group status still reads `activeSite=iad, state=writable, Ready=True`. What do you conclude first?

- That you have proven nothing yet. Host-netns iptables does not partition Kubernetes Service traffic — kube-proxy's DNAT runs in different chains — so verify from an independent vantage point, against a disposable canary, that the partition exists before you treat any status as a symptom.
- That the partition is real and detection simply has not completed: 6 s of detection plus the 30 s relay drain is a 36 s worst case, and you are barely past it.
- That the partition is real and the poll has frozen, exactly as in the incident where the operator reported a believable status for two minutes under a deny-all policy — so restart the operator.
- That the partition is real and the response is correct: the operator holds `iad` writable while its sidecar counts down `leaseTimeout`, so wait for the self-fence.

**Correct option index:** 0

**Explanation:**

Step one of the checklist is to confirm the partition is real before believing any symptom, and the operator's own status can never be that check. This injection is a known no-op — kube-proxy's DNAT happens in different chains, so the operator keeps reaching MySQL through the ClusterIP while you believe you have severed it. A NetworkPolicy is not automatically safer: chaos 33 found a CNI evaluating one post-DNAT, so its DNS exception never matched and the canary kept resolving for the entire 45 s hold. Options 2, 3 and 4 all share the same defect — each takes the injection on faith and reasons forward from it, and each would have you wait or act on a partition that may not exist. Option 3 is the subtlest, because a frozen poll really does produce a believable status; that is why status is evidence about the operator, never evidence about the network. A chaos experiment that injects nothing produces a confident false pass, and a green run of a broken experiment is worse than none. (objective 9)

## Question 9

**Type:** TRUE_FALSE

The `orders` primary dies at 02:00. The operator happens to be down and is not restored until 04:00, at which point it promotes `pdx`. True or false: the two-hour delay widens the RPO — more committed transactions are lost than if the operator had been up at 02:00.

**Correct answer:** false

**Explanation:**

False, and the line is worth memorising: operator downtime costs write availability, never RPO. An emergency failover can lose every transaction that committed on the dying primary but had not yet reached the survivor, and that set is sealed at the instant of death — a dead primary produces no further writes to fall behind on. An operator that turns up two hours later promotes the same replica across the same GTID gap and loses exactly the same transactions. What the two hours cost you is two hours with no writable site, because nothing is authorised to promote while the operator is gone. This is the same split as always: availability and correctness fail separately, and the sidecar fencing layer holds correctness for the whole outage. (objective 11)

## Question 10

**Type:** MULTIPLE_CHOICE

It is 02:10 in that outage. `iad` is gone, `pdx` is read-only, healthy and caught up, writes are down, and the operator is in CrashLoopBackOff on a bad image you pushed — the rollback will take about five minutes. Which action actually gets writes back?

- Run `kubectl bloodraven promote orders pdx` now — the plugin talks to MySQL directly, so it promotes without waiting for the operator.
- `SET GLOBAL read_only = OFF` on `pdx` by hand — fastest path, and with the operator down there is nothing to disagree with it.
- Roll the operator image back and let it promote. The promotion path *is* the operator: the plugin only writes resources the operator already reads, so `promote` is a request to that logic, not a substitute for it.
- Wait for `spec.failoverCooldown` to expire — the cooldown is what is blocking the promotion, and it clears on its own.

**Correct option index:** 2

**Explanation:**

Break-glass promotion is not a back door. `kubectl bloodraven promote orders pdx` writes the `bloodraven.shipstream.io/planned-failover` annotation and returns; it never talks to MySQL, so annotating a group nobody is watching only queues an intent — which kills option 1. Option 2 is the one that looks fastest and fails on its own terms: clearing `read_only` makes `pdx` writable while its sidecar's cached authoritative `activeSite` still names `iad`, so rule #1 sees a topology mismatch and fences it straight back, without ever consulting the lease. Option 4 misreads the blocker — a crashlooping operator is the reason nothing promotes, and the cooldown gates a decision that no one is currently making; it is also the trap in the other direction, since a live cooldown would reject a hand-driven promotion outright unless `spec.plannedFailover.onCooldown` is set to `defer`. Fix the operator first: it is the only thing that can promote, and once it is back it promotes within roughly 6 s of detection plus the drain. If the operator were coming back inside the cooldown with no writable site, waiting would win for the same reason — the hand-driven path cannot start sooner. (objectives 1, 12)
