# Quiz — Five partitions, five answers

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

`playground` pages you. `iad` is still writable and the counter app is still committing, but `pdx` has stopped answering the operator's polls entirely and its site state has gone to `unreachable`. Which documented partition scenario is this?

- Scenario A — the operator cannot reach one site while the other is reachable
- Scenario B — the replica site is isolated while the primary stays reachable
- Scenario C — the MySQL-to-MySQL link is broken while the operator reaches both sites
- Scenario E — both sites are unreachable to the operator

**Correct option index:** 1

**Explanation:**

The isolated site is the replica and the primary is untouched, which is scenario B: no failover, and no self-fence either, because a read-only instance has no writability to take away. Scenario A is tempting because it is also 'operator cannot reach a site' — but A is the shape where the unreachable site is the one holding writes, which is what makes a promotion available at all; here promoting would move writes away from a healthy primary. Scenario C requires the operator to still poll both sites cleanly, and `pdx` has stopped answering polls, so the discriminator rules it out. Scenario E needs every site unreachable; `iad` is answering. (objective 7)

## Question 2

**Type:** MULTIPLE_CHOICE

Different page, same group. Both `iad` and `pdx` answer the operator's polls on every tick: `iad` reads writable, `pdx` reads read-only. But `pdx`'s replication IO thread has stopped and `secondsBehindSource` is climbing. Which scenario is this?

- Scenario B — the replica site is isolated from the operator and from replication
- Scenario C — the MySQL-to-MySQL link is broken while the operator reaches both
- Scenario D — asymmetric peer reachability between the two sidecars
- Scenario A — the operator cannot reach one of the two sites at all

**Correct option index:** 1

**Explanation:**

B and C share the visible replication symptom, so the discriminator is the operator's own reachability: in C the operator polls both sites cleanly and only the binlog stream is severed, which is exactly what is described. Scenario B is the near-miss — it looks identical from the replication metrics alone, but B means the replica site is isolated, and a site that answers `SELECT @@read_only` every two seconds is not isolated. Scenario D is about one-way *peer* reachability between sidecars and would not stop the replication stream on its own. Scenario A requires a site to be `unreachable` to the operator, and neither is. (objectives 7, 9)

## Question 3

**Type:** TRUE_FALSE

A cross-site replication link stays broken for twenty minutes while `iad` remains writable. Once `secondsBehindSource` passes `spec.replication.maxLagSeconds`, Bloodraven will promote `pdx` to recover replication.

**Correct answer:** false

**Explanation:**

The reversal: crossing the lag threshold moves a condition, never a primary. `spec.replication.maxLagSeconds` drives only the `ReplicationLagging` Degraded condition — it is not a promotion gate and it is not a promotion trigger. The failover row of the decision matrix needs zero writable sites, and `iad` is still writable, so there is no path to a promotion no matter how far `pdx` falls behind. This is deliberate rather than lazy: from the operator's point of view a broken link and a replica starved of I/O look identical, so acting on the difference would mean guessing. Human judgement decides, and the operator declining to act is the system working. (objective 8)

## Question 4

**Type:** MULTIPLE_CHOICE

To test scenario A you SSH to the k3d node hosting `iad` and add an iptables DROP rule for port 3306 in the host network namespace. You then watch the operator report `iad` as `writable` and `Ready=True` for the whole test window. What have you actually learned?

- That the operator correctly declined to act on a replica-side problem, which is the documented scenario B response
- That this is scenario D: the operator can still read from `iad` even though `iad` can no longer be written to
- Nothing about partitions — the injection never landed, since kube-proxy's DNAT is in different chains
- That the sidecar suppressed its self-fence because at least one peer stayed reachable, which is the documented lease rule

**Correct option index:** 2

**Explanation:**

Host-netns iptables rules on a k3d node do not partition Kubernetes Service traffic, so the run is a confident false pass: an experiment that injects nothing looks exactly like an experiment that proved the system healthy. The first option treats a green result as evidence of correct behaviour, which is the specific mistake — you cannot grade a response to a stimulus that never arrived. The second reads the operator's own status as ground truth, and status is precisely what a partition makes believable rather than wrong. The fourth invokes a real rule to explain a result the rule never produced, since nothing here ever reached the sidecar's fencing path. The fix is an independent check that the partition exists — a disposable canary, run before the real target is touched. (objective 9)

## Question 5

**Type:** SHORT_ANSWER

During a partition, `iad`'s MySQL pod is stuck `Terminating` because its node is unreachable. A colleague proposes `kubectl delete pod --force --grace-period=0` to "clear it out" so the promotion can settle. What may that pod still be doing after the command returns, and what does that imply about where the fence has to come from?

**Sample answer:**

It may still be running and still writing. Force-delete only removes the API object; `Terminating` was set by the API server, not the kubelet, so on an unreachable node the container never gets the message and keeps writing to its PV. That breaks at-most-one identity: the cluster now believes the pod is gone while a second writable MySQL may exist. `ReadWriteOnce` does not save you either — it restricts access to one node, not one pod, so storage attach is not fencing. Since nothing above MySQL can be relied on to stop a partitioned `iad` from accepting writes, the fence has to be a decision the site makes about itself: the sidecar's self-fence.

**A full-credit answer shows:**

A strong answer covers: (1) the process may still be running and still writing to the PV; (2) force-delete removes only the API object and therefore breaks at-most-one; (3) `Terminating` is set by the API server, not the kubelet, which is why an unreachable node's container never stops; (4) `ReadWriteOnce` is per node, not per pod, so storage attach is not fencing; (5) the conclusion — a partitioned site must self-fence because no layer above it can be relied on to stop it. An answer that only says "the pod object disappears" has missed the data-integrity consequence.

**Explanation:**

Kubernetes refuses to delete pods on unreachable nodes on purpose, to protect at-most-one identity; forcing the delete trades that protection for tidiness in the API and buys a possible second writer. All four Kubernetes facts point the same way, and together they are the argument for the sidecar's self-fence being the real barrier rather than a backstop. (objective 9)
