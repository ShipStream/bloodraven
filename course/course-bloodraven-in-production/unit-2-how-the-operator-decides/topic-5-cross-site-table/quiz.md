# Quiz — The six-row table that decides everything

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

In `playground`, `iad` (primary-candidate) is writable and `reader` (role: read-only) comes up writable after a pod restart. `pdx` is read-only. What does EvalCrossSite return?

- Reason "SplitBrain" with alert `SPLIT BRAIN: 2 sites are writable (iad, reader)`
- Reason "Degraded" with alert `writable non-promotable site requires fencing (reader)`, returning before any other row is evaluated
- Reason "Healthy", because a read-only role site is invisible to the matrix and iad is the sole core writable
- Reason "Degraded" with alert `reader unreachable while iad is primary`

**Correct option index:** 1

**Explanation:**

The tally loop routes any writable site whose role is not primary-candidate into FenceSites and `continue`s, so `reader` never joins the writable tally; the fence-first guard then fires and returns immediately. SplitBrain is wrong because len(writable) counts core sites only and equals 1 here — and even with two writable candidates the fence-first return would preempt it. Healthy is the trap version of the same fact: readers are excluded from coreCount and the tallies, but a writable one is not merely ignored, it is an anomaly that produces an alert. The last option invents an unreachable site that does not exist. (objectives 4, 6)

## Question 2

**Type:** MULTIPLE_CHOICE

`playground` has `iad` and `pdx` both reachable and both read-only, and no site unreachable. What does the operator do?

- Promotes whichever of `iad` or `pdx` has the freshest GTID set, since both are eligible primary-candidates
- Promotes `iad`, because it is first in `spec.splitBrainPolicy.sitePriorities`
- Emits Reason "NoPrimary" with alert `NO PRIMARY: both sites are read-only` and takes no automatic action
- Emits Reason "Degraded" with promotion candidates, then waits for a human to confirm

**Correct option index:** 2

**Explanation:**

The failover row needs three conjuncts and one of them is missing: there is no unreachable peer. Without one, the source refuses to auto-elect because all-read-only is a startup or recovery state that needs human input. Options one and two both assume the operator elects from a fully reachable all-read-only set — it never does, and neither GTID freshness nor sitePriorities is even consulted, because PromotionCandidates is never populated. The fourth option confuses this with the failover row, which sets Reason "Degraded" and PromotionCandidates only when at least one peer is unreachable. Change `pdx` to unreachable and the same states produce exactly that. (objectives 4, 5)

## Question 3

**Type:** MULTIPLE_CHOICE

`iad` is writable and serving the counter app; `pdx` has just gone unreachable. Which Reason lands on the `Degraded` condition in `status.conditions`?

- "Failover", the outcome named in the published cross-site evaluation table
- "Degraded", with message `pdx unreachable while iad is primary`
- "Healthy", because exactly one site is writable and the group is still serving writes
- "NoPrimary", because the group has lost its promotion target

**Correct option index:** 1

**Explanation:**

One writable plus at least one unreachable hits the primary-up-peer-down row: Reason "Degraded", alert `<site> unreachable while <site> is primary`. "Failover" is the most dangerous distractor because the published docs table does name that outcome — but no code path emits it; the only five reasons that reach status.conditions are Healthy, Degraded, SplitBrain, NoPrimary and TotalLoss, so an alert rule matching a `Failover` reason never fires. "Healthy" requires exactly one writable AND zero unreachable, and the second clause fails. "NoPrimary" requires zero writable, and `iad` is still writable. (objective 4)

## Question 4

**Type:** TRUE_FALSE

Because `dr-only` sites are non-promotable, EvalCrossSite excludes them from `coreCount` and from the writable/read-only/unreachable tallies, exactly as it excludes `role: read-only` sites. True or false?

**Correct answer:** false

**Explanation:**

The opposite is true: only `role: read-only` is excluded. The tally loop increments coreCount for every site whose role is not read-only, so a `dr-only` site counts and still lands in the readOnly or unreachable tally. Promotability and matrix visibility are two separate properties, and conflating them is expensive: in a group of two primary-candidates plus one dr-only site, treating dr-only as invisible would make you predict `len(unreachable) == coreCount` at two unreachable sites when the real threshold is three, so you would expect TotalLoss a whole site early. (objective 4)

## Question 5

**Type:** SHORT_ANSWER

`spec.splitBrainPolicy.sitePriorities` is `[iad, pdx]`. The table has emitted PromotionCandidates and `pdx` holds a strictly fresher GTID set than `iad`. Which site is promoted, and why?

**Sample answer:**

`pdx`. EvalCrossSite only orders the candidate list — it puts `iad` first because sitePriorities names it first — but the caller then runs pickFreshestCandidate, which reads GTID_EXECUTED from every candidate and takes the most caught-up set. GTID freshness is the primary selector because it minimises data loss on promotion; the priority list is consulted only to break ties or genuinely incomparable sets. So `pdx` wins despite being second in the list.

**A full-credit answer shows:**

A strong answer names `pdx`, states that GTID freshness is the primary selector and gives the reason (minimising data loss on promotion), and states that sitePriorities acts only as a tiebreaker for equal or incomparable sets. Answering `iad` on the grounds that priority ordering is authoritative is the misconception being tested. Credit an answer that also notes EvalCrossSite itself never picks the winner — it is pure and has no MySQL access, so the GTID read happens in the caller.

**Explanation:**

Ordering the candidate list is not the same as choosing from it. The matrix is pure and cannot read GTID_EXECUTED at all, so the freshness comparison necessarily happens in the caller, after the table has spoken. (objective 4)

## Question 6

**Type:** MULTIPLE_CHOICE

The matrix reports Reason "NoPrimary" for `playground` on one poll and "Degraded" on the next, without any promotion or fencing having run in between. What best explains it?

- A bug: the reason can only change when the operator has taken a mutating cross-site action
- The matrix is evaluated on every poll so status always carries the current condition, while the mutating cross-site actions remain transition-driven — a site's state changed, and that alone moves the reason
- The matrix consulted its record of the previous poll and escalated the severity after a repeat observation
- Two operator replicas evaluated the group concurrently and wrote conflicting conditions

**Correct option index:** 1

**Explanation:**

The reason string is recomputed from scratch on every poll and written to status.conditions, so it tracks the live topology whether or not anything was mutated; only the actions that change MySQL are gated on transitions. The first option inverts that relationship. The third contradicts the function's stated purity — it has no memory of prior polls and no notion of escalation. The fourth is not the mechanism: the chart ships a single replica with leader election, so there is no concurrent second evaluator to blame. (objective 4)
