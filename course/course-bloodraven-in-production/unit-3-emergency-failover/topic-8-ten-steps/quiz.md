# Quiz — The nine steps of a promotion

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

The operator begins a failover on `orders` and the very first statement — SET GLOBAL super_read_only = ON against the dead `iad` primary — fails with a connection error. What happens next?

- The failover aborts and is retried on the next poll, because promoting without fencing risks split brain
- The operator logs a warning and carries straight on to the next step
- The candidate is promoted but left read-only until the fence can be confirmed
- The operator skips the relay-log drain and promotes immediately, to shorten the unfenced window

**Correct option index:** 1

**Explanation:**

Fencing the old primary is a warn-only step: the code logs 'failed to fence old primary (may be unreachable)' and continues. That is the design, not an oversight — the old primary is usually unreachable, which is the whole reason a failover is running. Option 1 is the common belief that fencing gates promotion; it does not, and if it did, an unreachable primary would block every failover you actually need. Option 3 invents a half-promoted state that does not exist in Execute(): the two read-only clears are unconditional and fatal on error. Option 4 confuses two independent things — the drain's 30 s budget is about the candidate's relay logs and is never shortened by what happened on the old primary. Only STOP REPLICA, RESET REPLICA ALL and the two read-only clears abort the sequence. (objective 2)

## Question 2

**Type:** TRUE_FALSE

A `kubectl delete pod --grace-period=0 --force` against the primary will not trigger a failover, because the Deployment recreates the pod in under five seconds.

**Correct answer:** false

**Explanation:**

The reversal: a force-delete does trigger failover, and the fast respawn does not save the primary. Scenario 09b force-deletes the primary and hard-waits for activeSite to flip and lastFailover to stamp before it even reaches its real assertion — and it passes. The debounce never looks at pod objects; it looks at whether mysqld answers CheckReadOnly, and a cold container start plus InnoDB recovery comfortably exceeds the 6 s detection window (pollInterval 2 s x failureThreshold 3). The genuinely marginal injection is a container restart in place — scenario 16 issues SQL SHUTDOWN, keeps the pod, PVC and IP, and explicitly accepts either outcome depending on how fast the kubelet restarts the container against that same ~6 s window. Scale-to-0 is preferred in scenario 01 for determinism, not because delete fails. (objective 1)

## Question 3

**Type:** MULTIPLE_CHOICE

Promotion clears `super_read_only` and then `read_only`. Given the candidate is about to accept writes either way, why does the sequence care about `super_read_only` specifically?

- It is the variable the sidecar fences with, and it blocks writes even from CONNECTION_ADMIN or SUPER, which `read_only` alone does not
- MySQL refuses `SET GLOBAL read_only = OFF` while `super_read_only` is ON, so the order is forced by the server
- Clearing it is what lets the new primary stop applying replicated transactions from its old source
- `super_read_only` is the only one of the two that persists across a mysqld restart, so clearing it is what makes the promotion durable

**Correct option index:** 0

**Explanation:**

super_read_only is the real barrier — the MySQL manual is explicit that it prohibits updates even from users holding CONNECTION_ADMIN or SUPER, while read_only does not — and it is exactly what the sidecar's fencing sets. A candidate arriving at promotion may already be fenced, so the promotion has to clear that specific variable. Option 2 is backwards: MySQL couples the two the other way round, and setting read_only=OFF implicitly forces super_read_only=OFF. Option 3 confuses fencing with replication control: replication threads keep applying under super_read_only, which is precisely why STOP REPLICA and RESET REPLICA ALL are separate, fatal steps. Option 4 invents persistence — neither variable survives a restart on its own, and durability of the promotion comes from the failover record stamped before the DNS flip. (objective 2)

## Question 4

**Type:** MULTIPLE_CHOICE

Which of these is actually a step inside the failover sequence in `internal/controller/failover.go`?

- Applying the NoExecute taint to the old primary's node
- Repointing the surviving replicas at the newly promoted site
- Confirming the candidate is writable before the DNS record is flipped
- Recording the old primary's divergent GTID set

**Correct option index:** 2

**Explanation:**

Writable confirmation runs synchronously in the same call stack as the promotion — not deferred to the next poll — and on failure the operator logs that promotion succeeded but DNS was not flipped, and returns. Option 1 is the most common misreading: taints are a pure function of per-site state transitions and are applied earlier in the same poll by a different code path, so they merely appear alongside a failover. Option 2 is source convergence, an independent poll stage with its own 20 s budget; the published sequence lists it as a step, and that is a documentation error. Option 4 belongs to old-primary recovery, which runs later and only once the old primary comes back. Neighbours in the poll are not links in the chain. (objective 2)

## Question 5

**Type:** SHORT_ANSWER

A failover on `orders` measures 36 s from primary kill to `activeSite` flip, where your previous runs all landed at 12 s. Account for the extra 24 seconds, and say what you would check to confirm your explanation.

**Sample answer:**

Both runs pay the same 6 s of detection (pollInterval 2 s x failureThreshold 3), so detection is not the difference. In the 12 s runs the candidate is already caught up, so the relay-log drain hits its early-exit condition — SQL thread running, Seconds_Behind_Source 0 — and returns almost immediately. In the 36 s run the candidate had relay logs it had fetched but not applied, so the drain spent essentially its whole 30 s budget before the operator promoted anyway (the drain is non-fatal on timeout): 36.005 s - 12.0 s = 24.0 s of extra wall clock. To confirm it, I would look for the warning 'relay log drain did not complete cleanly, proceeding with promotion' in the operator log, and for a stopped or lagging SQL applier on the candidate in the period before the kill — scenario 14 produces exactly this by pausing the applier and seeding five seconds of writes.

**A full-credit answer shows:**

A strong answer: (a) names detection as the constant 6 s and derives it from pollInterval x failureThreshold; (b) attributes the whole difference to the relay-log drain, not to detection, DNS or the promotion statements; (c) states the drain's 30 s budget and its early-exit condition (SQL thread running and Seconds_Behind_Source = 0) as the reason the fast case is fast; (d) notes the drain is non-fatal, so the promotion proceeded anyway; (e) offers a checkable artefact — the drain warning in the operator log, or an unapplied relay-log backlog on the candidate. Answers that blame DNS propagation, the anti-flap cooldown, or a slower promotion have missed it.

**Explanation:**

The 12.0 s and 36.005 s figures are both measured, and the gap is entirely the drain: the same 6 s detection, then either an instant early exit or a full 30 s budget spent applying relay logs. This is also why the worst case is around 37 s (6 s detect + 30 s drain) rather than the documentation's unsourced 30-45 s. (objectives 2, 3)
