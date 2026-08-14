# Quiz — Cooldown, history, and the one exception

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** TRUE_FALSE

`playground` failed over to `pdx` ninety seconds ago and `spec.failoverCooldown` is the default `5m`. A stale `iad` now comes back writable, giving two writable sites. The operator will leave both writable until the cooldown expires.

**Correct answer:** false

**Explanation:**

The reversal: the cooldown gates promotion and nothing else, so split-brain fencing is not delayed by it at all. `iad` is fenced on the next poll, from its own cross-site call site, which never reads `failoverCooldown`. The tempting model — cooldown as a five-minute freeze on the whole group — also predicts wrongly for source convergence, old-primary recovery, reclone and DNS reconcile, all of which keep running. (objective 7)

## Question 2

**Type:** MULTIPLE_CHOICE

During a rolling image change on `playground`, `bloodraven_failovers_total` increments for `pdx`. The last emergency failover was ninety seconds earlier, `spec.failoverCooldown` is `5m`, and no `failover blocked by anti-flap cooldown` line appears anywhere in the operator log. What happened?

- The ordered-update handoff promoted `pdx`; that path calls `recordFailover` and increments the counter but is never cooldown-gated, so the guard was never reached
- The operator restarted during the update and rehydrated an empty `lastFailover`, so the cooldown evaluated as expired
- The counter increments on promotion attempts rather than completed promotions, and attempts are not gated
- `spec.updateStrategy: Recreate` was set, which disables anti-flap for the duration of the rollout

**Correct option index:** 0

**Explanation:**

The ordered-update handoff is a separate promotion path with no cooldown check anywhere in it, yet its completion callback stamps the durable failover record and increments `bloodraven_failovers_total` — so the counter moves without the guard ever being consulted, which is also why there is no log line. Option 2 would produce a rehydration warning and requires a restart you did not observe; rehydration also prefers the later of the two durable copies rather than emptying them. Option 3 is wrong: the counter is incremented after a successful promotion, not on attempts. Option 4 invents a coupling that does not exist: `spec.updateStrategy` decides whether a spec change rolls one site at a time or all at once, and touches nothing about anti-flap. (objective 7)

## Question 3

**Type:** MULTIPLE_CHOICE

The operator restarts. `status.lastFailover` reads `10:04:00Z`, the `bloodraven.shipstream.io/last-failover` annotation reads `10:06:00Z`, and the local clock is `10:07:00Z`. Which record does the restarted operator install as its cooldown baseline?

- The status copy, because status is the authoritative subresource and the annotations are only a fallback
- The annotation copy at `10:06:00Z`, because rehydration takes whichever copy is stamped later
- Neither — a disagreement between the two copies is treated as corrupt and the cooldown restarts empty
- Neither copy directly: the operator averages them to bound clock skew between the two write paths

**Correct option index:** 1

**Explanation:**

Rehydration takes the later of the two, because the two paths fail independently: the annotation is ahead precisely when the status write was rejected. `10:06:00Z` is only one minute ahead of local time, well inside `FailoverClockSkewGrace` of `5m`, so it is plausible and gets installed. Option 1 inverts the design — neither copy outranks the other, status only wins an exact tie. Option 3 describes the failure the duplication exists to prevent: discarding history is what resets a cooldown and lets a promotion happen inside the window. Option 4 is invented; there is no averaging, only a later-wins comparison with a future-date guard. (objective 8)

## Question 4

**Type:** SHORT_ANSWER

Every site in `playground` is read-only, no site is unreachable, and `lastFailoverTarget` names `pdx`. All the re-assert preconditions hold except one: the promotion GTID recorded in status was hand-edited during an incident and no longer parses. What does the operator do, and why is that the right behaviour?

**Sample answer:**

It refuses the re-assert. It logs `primary re-assert refused: recorded promotion GTID set failed to parse — status corrupted or manually edited?` at WARN and returns without touching MySQL, leaving the group on its NoPrimary alert for a human. It does not treat the unparseable value as absent and skip the GTID gate. The whole safety argument for re-asserting a fenced primary is that the target's GTID_EXECUTED provably contains the recorded promotion GTID set and every peer's set; if the recorded invariant cannot be read, it cannot have been verified, so proceeding would restore writability on a site that might be missing transactions.

**A full-credit answer shows:**

A strong answer covers: (1) refuse, not skip — the operator returns without mutating MySQL; (2) the reason is that the operator itself wrote that value from MySQL, so a parse failure means corruption or manual tampering; (3) the safety argument depends on the recorded invariant being trustworthy, so an unreadable invariant is not a satisfied one; (4) the consequence is that the group stays wedged and alerting until a human intervenes. Bonus: naming the log msg, or noting that skipping the gate would be the dangerous inversion.

**Explanation:**

The refusal is the point. A parse failure is evidence that status has been corrupted or edited, and the re-assert is only safe because the recorded promotion GTID set can be checked against the target's live `GTID_EXECUTED`. Treating an unreadable value as 'no constraint' would convert the strongest gate into no gate at exactly the moment the data is least trustworthy. (objective 9)

## Question 5

**Type:** TRUE_FALSE

With `spec.failoverCooldown` at the shipped `5m`, `playground` failed over to `pdx` two minutes ago and every site is now read-only. A primary re-assert cannot fire until the five minutes are up.

**Correct answer:** false

**Explanation:**

The reversal: the re-assert can fire right now. It reuses the `failoverCooldown` duration as its rate limit, but measures it against a separate timer, `lastReassert`, which is never compared against `lastFailover`. Two minutes after a failover, `lastReassert` is still zero, so the rate limit is satisfied even though an automatic promotion would be blocked for another three minutes. Assuming one shared clock is the common wrong model, and it makes the re-assert look impossible exactly when it is the thing rescuing the group. (objectives 7, 9)
