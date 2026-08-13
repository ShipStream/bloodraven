# Quiz — Cache and sessions that follow the primary

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A planned switchover of `playground` reports Succeeded. You dump the status and `sessionsPreserved` is absent — the key is not in the YAML at all. What have you learned about the counter app's session store?

- Nothing about the outcome — the field is unknown, and you have to look at the promotion method, the log line and the event to find out
- Sessions were lost; an absent boolean serialises the same as false
- Sessions were preserved; the operator only writes the field when something went wrong
- The switchover silently skipped the Dragonfly phases, so the session store is still on the old master

**Correct option index:** 0

**Explanation:**

`sessionsPreserved` is a `*bool` and its three states are true, false and nil — nil means unknown, for example because Dragonfly was disabled for that attempt. Reading absent as false makes you chase an incident that did not happen; reading it as success makes you miss one that did. "Sessions were lost" is the classic misread of a nil pointer as its zero value: `omitempty` drops nil, not false. "Only written on failure" is backwards — a clean REPLTAKEOVER explicitly stamps true. And an absent field is not evidence about routing: the traffic-label shed and the role flip are what move the active Service, and they leave their own traces. (objective 11)

## Question 2

**Type:** MULTIPLE_CHOICE

Your primary dies. The Dragonfly target's REPLTAKEOVER fails. What does the operator do next, and what does it cost?

- Retries REPLTAKEOVER until maxSyncWait expires, then aborts the whole failover
- Falls back to REPLICAOF NO ONE, promoting the target as an empty master — sessions are lost, and it logs exactly that
- Rolls back the MySQL promotion so cache and database stay consistent with each other
- Leaves both Dragonfly pods as replicas so no site can serve stale session data

**Correct option index:** 1

**Explanation:**

The emergency path is REPLTAKEOVER first, REPLICAOF NO ONE second, and the operator logs "target promoted via REPLICAOF NO ONE (sessions lost)". It buys availability with sessions because an emergency promotion cannot be retried — that trade is only acceptable because Dragonfly holds cache, never durable data. Retrying until maxSyncWait misreads the budget: the emergency attempt is bounded at 10 s regardless. Rolling back MySQL inverts the whole design — no Dragonfly outcome is ever permitted to affect MySQL. Leaving both as replicas would mean the cache serves nothing at all, which is worse than an empty master. (objectives 10, 11)

## Question 3

**Type:** TRUE_FALSE

A Dragonfly pod is wedged and answers nothing. Your MySQL primary then dies. The emergency MySQL promotion will be delayed while the operator waits on Dragonfly.

**Correct answer:** false

**Explanation:**

The opposite is true: MySQL failover is never delayed by cache. The Dragonfly attempt runs after MySQL has already been promoted, under a hard-coded 10 s budget, and by explicit design it never returns an error to its caller, never blocks longer than that budget, and never leaves Dragonfly in a state that affects MySQL durability. The tempting reasoning — "co-managed subsystems must be waited on" — is exactly what the 10 s bound and the best-effort contract exist to refuse. (objective 10)

## Question 4

**Type:** SHORT_ANSWER

During a takeover the operator sheds an endpoint from the active Dragonfly Service by deleting the `shipstream.io/dragonfly-traffic` key rather than setting it to a disabled value. Explain why deletion is the safer mechanism, and what property of the takeover it buys.

**Sample answer:**

The active Service selector is an exists-and-equals check on the value "enabled", so removing the key removes the endpoint immediately and unambiguously. Writing something like traffic=disabled would only work if the selector and the writer agreed on what "disabled" means — the selector does not test for a disabled value, it tests for enabled, so any other value happens to work by accident rather than by contract. Because the sequence strips the source's key before promoting the target and stamps the target's key only afterwards, and an absent key can never match, there is no window in which both instances satisfy the active selector. No moment where the Service load-balances session writes across two masters.

**A full-credit answer shows:**

A strong answer covers: (1) the selector is an exists-and-equals check on "enabled", so a missing key cannot match; (2) a written "disabled" value would depend on selector and writer agreeing on a magic string, i.e. correctness by convention rather than by mechanism; (3) the consequence — strip-before-promote plus an unmatched absent key means no window where both pods are endpoints of the active Service. Credit an answer that names the role label as the other half of the AND-gate. Do not credit an answer that only says "deletion is atomic" without saying what the selector actually tests.

**Explanation:**

The mechanism is the lesson: the AND-gate of role and traffic, plus deletion rather than devaluation, is what makes the endpoint shed atomic and gives the takeover no dual-master window. (objective 12)

## Question 5

**Type:** MULTIPLE_CHOICE

You deploy `playground` with a Dragonfly image older than the stated v1.38.0 minimum. What actually happens?

- Admission rejects the MysqlFailoverGroup — a CEL rule enforces the minimum version
- The operator accepts it but falls back to REPLICAOF NO ONE on every promotion, so sessions are never preserved
- Nothing stops you. The version floor is a support policy, not a guardrail; the only CEL rules are that an image is required and that it may not be `:latest`
- The Helm chart refuses to install because the chart pins a minimum Dragonfly appVersion

**Correct option index:** 2

**Explanation:**

Nothing in the API types, the controller or the chart enforces or checks a Dragonfly version, so an older build is admitted and will simply fail in ways nobody has characterised. The two CEL rules that do exist are narrower than people assume: image required when Dragonfly is enabled, and no `:latest`. "Admission rejects it" and "the chart refuses" are the same wrong instinct — treating a documented minimum as an enforced one. "Falls back cleanly to REPLICAOF NO ONE" is a more subtle trap: the operator has no version knowledge to make that decision with, so you get undefined behaviour rather than a graceful degradation. Note too that the shipped playground pin has itself drifted two minors behind current stable — version discipline is yours. (objective 10)
