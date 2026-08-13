# Unit 2 test — Predicting the operator

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you compute the 6s detection delay from `pollInterval` and `failureThreshold`, name the `Reason` string for a group with one writable and one unreachable site, and say what the anti-flap cooldown will not stop?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

`playground` runs at defaults — `pollInterval: 2s`, `failureThreshold: 3`, `recoveryThreshold: 2`. `pdx` is currently recorded as `unreachable`. Its next three probes all return `read_only=0`. What does the operator record, and when?

- `writable` on the first success — a successful probe zeroes `failCount`, and a site with no failures is writable
- `writable` on the second consecutive success; after the first success it still reads `unreachable`
- `writable` on the second consecutive success; after the first success it reads `read-only`, because a writable answer cannot be published until `recoveryThreshold` is met
- `writable` on the third consecutive success — recovery mirrors `failureThreshold`, so it takes the same three polls to come back as it took to go away

**Correct option index:** 1

**Explanation:**

`computeState` zeroes `failCount` on any successful poll, then handles the writable answer separately: because the site is not already `StateWritable` it increments `recoveryCount` and, until that reaches `RecoveryThreshold` (2), it `return site.state` — the state the site already had. So poll one leaves `pdx` reading `unreachable`, poll two makes it `writable`. Option one confuses the two counters: clearing `failCount` stops the site being *newly* unreachable, it does not grant writability, which is the debounced direction. Option three invents a state the code never returns on that path — the fall-through is the previous state, not `read-only`; `read-only` is returned only for a `read_only=1` answer. Option four applies `failureThreshold` to the wrong transition: 3 gates the way in to `unreachable`, 2 gates the way back to `writable`. (objectives 1, 3)

## Question 2

**Type:** MULTIPLE_CHOICE

You are tailing the operator log for `playground`. The last successful poll of `reader` was at `12:04:10`; at `12:04:12` you see WARN `failed to check replica status`, and at `12:04:16` INFO `state transition` with `site=reader`, `from=read-only`, `to=unreachable`. At `12:05:02` the `reader` pod is back and answering `read_only=1`. What is the next INFO line for that site, and how long after it starts answering?

- INFO `state transition` `from=unreachable` `to=read-only`, on the first successful poll
- INFO `state transition` `from=unreachable` `to=read-only`, six seconds later — the same `pollInterval` × `failureThreshold` sum runs in both directions
- INFO `state transition` `from=unreachable` `to=read-only`, after two polls (4 s), because `recoveryThreshold` is 2
- Nothing at INFO — recoveries are per-poll bookkeeping and the operator's slog handler is set to INFO, so the return is only visible at DEBUG

**Correct option index:** 0

**Explanation:**

A `read_only=1` answer returns `StateReadOnly` immediately, with no counter in the way, so the return transition costs exactly one poll. The six-second gap you can read off the timestamps (`12:04:10` to `12:04:16`) is `pollInterval` 2 s × `failureThreshold` 3 — three consecutive failed probes — and that sum applies only to the way in to `unreachable`. Option two makes the failure debounce symmetric; it is not, deliberately, because read-only is the safe direction and costs nothing to believe early. Option three drags in `recoveryThreshold`, which gates only the transition **to `writable`** and is never consulted here. Option four is wrong about which level carries the event: `state transition` is an INFO event in the log schema's Event reference, with fields `site`, `from`, `to`, `fg`; it is DEBUG-level bookkeeping that stays invisible. (objectives 2, 10)

## Question 3

**Type:** MULTIPLE_CHOICE

A four-site `playground`: `iad` (`primary-candidate`) writable, `pdx` (`primary-candidate`) read-only, `dr` (`role: dr-only`) just restarted and is answering `read_only=0`, `reader` (`role: read-only`) read-only. What does `EvalCrossSite` return on this poll?

- `SplitBrain` — `dr-only` sites count toward `coreCount`, so two core sites are writable and `len(writable) > 1` holds
- `Healthy` — `dr-only` is non-promotable, so like `role: read-only` it is dropped before the tallies and only `iad` is visible
- `FenceSites = [dr]`, Alert `writable non-promotable site requires fencing (dr)`, `Reason = "Degraded"`, returned before any other row is evaluated
- `Reason = "Degraded"` with `PromotionCandidates = [pdx]` — the failover row, because the group no longer has a single trustworthy primary

**Correct option index:** 2

**Explanation:**

The pre-tally loop routes any site that is writable while its role is not `primary-candidate` into `action.FenceSites` and `continue`s, so `dr` never joins a tally at all; `len(FenceSites) > 0` then fires the fence-first early return, which preempts every row below it. Option one gets `coreCount` right — `dr-only` genuinely is counted, unlike `role: read-only` — but misses that the writable `dr` was diverted before the `writable` tally was built, so that tally holds only `iad` and `len(writable) > 1` is false. Option two makes the opposite error, treating `dr-only` as invisible to the matrix; only `role: read-only` is excluded from `coreCount`, and in any case a writable non-candidate is never `Healthy`. Option four fails the failover row's three conjuncts: it needs zero writable **and** at least one unreachable **and** at least one read-only, and here `iad` is writable and nothing is unreachable. (objectives 4, 6)

## Question 4

**Type:** TRUE_FALSE

The `reader` pod in `playground` restarts and comes up writable for a few seconds before anything fences it. The operator records `reader` as `writable` on that single poll rather than waiting for `recoveryThreshold`, and fences it on that same poll without waiting for a state transition.

**Correct answer:** true

**Explanation:**

Both halves hold, and they are two separate deliberate bypasses. A writable observation on a **non-promotable** site skips `recoveryThreshold` entirely — the comment in the poll loop calls it an immediate safety fact and says authority invalidation must not be debounced behind the normal recovery threshold. And fencing a writable non-promotable site runs on **every** poll rather than on a transition, so a failed fence is retried without waiting for the state to change again. The tempting wrong answer is that the reader must prove itself over two polls like any other site: that reasoning is right for a `primary-candidate`, where being slow to believe a new primary is prudence, and exactly backwards for a reader, where being slow to notice it taking writes is not. (objectives 3, 6)

## Question 5

**Type:** SHORT_ANSWER

`playground` failed over to `pdx` two minutes ago. `spec.failoverCooldown` is the shipped `5m`. Now every core site reads `read-only` and none is unreachable. Describe what the cross-site table produces, whether the operator can restore writability before the five minutes are up, and what must hold for it to do so.

**Sample answer:**

The failover row needs three conjuncts — zero writable, at least one unreachable, at least one read-only — and the unreachable conjunct fails, so the operator refuses to auto-elect: all-read-only is indistinguishable from a cluster that has not finished starting, so it is treated as a startup or recovery state needing human input. The table falls through to `NoPrimary`, and with exactly two read-only core sites and zero unreachable the alert is `NO PRIMARY: both sites are read-only`. But the operator holds history the pure table refuses to consult, and it can still restore writability by re-asserting the fenced promoted primary — `pdx`, the recorded `lastFailoverTarget`. The 5 m cooldown does not stand in the way: the re-assert rate limit reuses the `failoverCooldown` duration but measures it against a separate `lastReassert` timer that is never compared against `lastFailover`, and here `lastReassert` is still zero. Every one of these must hold: no subsystem gate active (bootstrap blocking cross-site, ordered update, topology frozen, planned failover in flight), the re-assert rate limit satisfied with no promotion pending confirmation, every non-target peer `read-only` (not writable, not unreachable, not unknown), the target `read-only` and promotable (`role: primary-candidate`), and the target's `GTID_EXECUTED` containing both the recorded promotion GTID set and every peer's `GTID_EXECUTED`. On success you get WARN `re-asserting fenced promoted primary: no site is writable and the last failover target is GTID-complete; restoring writability` with field `site`, and `bloodraven_primary_reassert_total{site}` increments. If the recorded promotion GTID fails to parse the operator refuses rather than skipping the gate, warning `primary re-assert refused: recorded promotion GTID set failed to parse — status corrupted or manually edited?`.

**A full-credit answer shows:**

A strong answer covers: (a) the failover row's missing unreachable conjunct and the design reason — all-read-only is a startup/recovery state needing human input; (b) the fall-through to `NoPrimary` with the two-site message `NO PRIMARY: both sites are read-only`; (c) that a re-assert of `lastFailoverTarget` can still restore writability inside the 5 m window because the rate limit uses a separate `lastReassert` timer never compared against `lastFailover`; (d) at least three of the re-assert preconditions, and credit for naming the GTID containment one (recorded promotion GTID plus every peer's GTID) since it carries the safety argument. Credit the verbatim re-assert log line or `bloodraven_primary_reassert_total`. Mark down an answer that says the group must wait out the cooldown, or that the operator promotes the freshest read-only site.

**Explanation:**

The point of the question is the seam between a pure function and a stateful operator. `EvalCrossSite` has no clock and no memory, so with nothing unreachable it can only alert — and `failoverCooldown` gates exactly one thing, the promotion call, which is not what is happening here anyway. The re-assert is the single path that restores writability inside the cooldown window, and it is safe precisely because it restores authority to the site the operator already made authoritative, only when that site's GTID set proves nothing can be lost. (objectives 5, 7, 9)

## Question 6

**Type:** MULTIPLE_CHOICE

Ninety seconds after `playground` failed over to `pdx`, the old primary `iad` comes back: reachable, read-only, still configured to replicate from nothing. `spec.failoverCooldown` is `5m`. Which statement describes what the operator does before those five minutes elapse?

- Source convergence repoints `iad` at `pdx` and old-primary recovery runs its `STOP REPLICA` / `RESET REPLICA ALL` / `CHANGE REPLICATION SOURCE` rejoin; only a second promotion is blocked
- Nothing mutating touches `iad` — the cooldown freezes cross-site action for the whole group until it expires
- The `DNSEndpoint` is left pointing at the old target until the cooldown expires, because DNS reconcile is part of the failover path the cooldown gates
- A `bloodraven.shipstream.io/reclone-site` annotation on `iad` is accepted but queued, and only acted on once the cooldown has run out

**Correct option index:** 0

**Explanation:**

The cooldown is one `if` immediately before the promotion call: if `lastFailover` is non-zero and `clock.Since(lastFailover) < failoverCooldown`, log `failover blocked by anti-flap cooldown` and return. Nothing else consults it. Option two is the mental model the word 'cooldown' invites and it is wrong — split-brain fencing, non-promotable fencing, source convergence, old-primary recovery, reclone and DNS reconcile each run from their own poll call site with no reference to `tm.failoverCooldown`. Option three inverts DNS behaviour: the `DNSEndpoint` is an idempotent server-side apply on every poll, re-derived from live topology, which is what self-heals a rejected write. Option four invents a queue; a reclone annotation is honoured while the cooldown ticks. (objective 7)

## Question 7

**Type:** TRUE_FALSE

The `mysqlfailovergroups/status` RBAC rule was dropped from the operator's ClusterRole during an upgrade, so status writes have been failing silently while ordinary object patches keep succeeding. `playground` fails over to `pdx`, then the operator pod restarts a minute later. The restarted operator has no failover history and will promote again immediately if the table asks it to.

**Correct answer:** false

**Explanation:**

It still has the history, and that is exactly why the record is written twice. Every promotion stamps `lastFailover` and `lastFailoverTarget` onto the CR status subresource **and** onto the object's own metadata as `bloodraven.shipstream.io/last-failover` and `bloodraven.shipstream.io/last-failover-target`, at RFC3339 second precision, written as a pair by JSON merge patch. Status is a subresource: its writes travel a separate API path with their own RBAC rule and their own admission chain, so one path can be denied or broken for hours while the other keeps recording the fact. On restart the operator reads both and installs the **later** copy — ties go to status, and a copy stamped more than `FailoverClockSkewGrace` (5 m) ahead of local time is discarded rather than installed, because the cooldown gate treats negative elapsed time as still active and a future-dated record would wedge promotion indefinitely. Here the status copy is missing and the annotation copy is present, so the annotation wins and the cooldown survives the restart. (objective 8)

## Question 8

**Type:** MULTIPLE_CHOICE

You scrape the operator's `/metrics` for `playground` and see: `bloodraven_site_state{site="iad",state="writable"} 1`; `{site="pdx",state="unreachable"} 1`; `{site="reader",state="read-only"} 1` (the other three series for each site are `0`); `bloodraven_replication_lag_seconds{site="pdx"} 4`; `{site="reader"} 0`; `bloodraven_state_transitions_total{site="pdx",from="read-only",to="unreachable"} 1`; and `rate(bloodraven_poll_latency_seconds_count[1m])` non-zero for all three sites. What is the group doing?

- `iad` is the primary and `pdx` is replicating four seconds behind it, so the group is healthy with a small amount of lag; the `unreachable` series is a transient the state-set has not cleared yet
- `reader` is not replicating — its `0` is the sentinel the operator writes when `Seconds_Behind_Source` is NULL — while `pdx` is the only working replica
- `iad` is writable and serving; `pdx` has gone unreachable and its `4` is a stale reading left on the series from before it went away; `reader` is replicating and caught up as far as MySQL can tell
- The group has no primary — `bloodraven_site_state` is a counter, so a `1` only means one transition into that state since the process started, not the state it is in now

**Correct option index:** 2

**Explanation:**

`bloodraven_site_state` is a state-set gauge: exactly one of the four series per site is `1` and the other three are `0`, so you read four series and find the `1`. That gives `iad` writable, `pdx` unreachable, `reader` read-only — corroborated by the transition counter, which increments once per `state transition` line and shows `pdx` going `read-only` to `unreachable`. Option one reads the lag gauge as current: when a site goes unreachable the operator neither updates nor deletes its lag gauge, so the last value it published stays on the series indefinitely. A lag gauge is only as fresh as the poll loop, which is why the non-zero `poll_latency_seconds_count` rate matters — it is the series that tells you the loop is still completing cycles at all. Option two misplaces the sentinel: `-1` means the lag was NULL and there is no replication stream; `0` means caught up as far as MySQL can tell. Option four mistakes the gauge for a counter — a state-set publishes the current state every poll. (objective 11)

## Question 9

**Type:** MULTIPLE_CHOICE

A second failover group — not `playground`, which overrides both — leaves `spec.replication` almost alone: it sets `maxLagSeconds: 300` and does not set `readOnlyMaxLagSeconds` at all. `bloodraven_replication_lag_seconds{site="pdx"}` and `{site="reader"}` both read `120`; `pdx` is `role: primary-candidate`, `reader` is `role: read-only`. What follows?

- Neither breaches: `pdx` is judged against `maxLagSeconds` 300, and a nil `readOnlyMaxLagSeconds` inherits `maxLagSeconds`, so `reader` is judged against 300 too
- `reader` breaches: with `readOnlyMaxLagSeconds` unset the reader gate defaults to zero reported lag, so `reader` drops out of the `-replicas` endpoint
- You cannot tell the two apart from this metric until you add a `role` label to the scrape, since `bloodraven_replication_lag_seconds` is otherwise ambiguous
- `pdx` at 120 s is already excluded from promotion, because a candidate beyond half the lag threshold is no longer a safe failover target

**Correct option index:** 0

**Explanation:**

`readOnlyMaxLagSeconds` has no default: when it is nil it inherits `maxLagSeconds`, so both sites are measured against 300 and both stay where they are. Option two is the trap the field is built around — an explicit `0` is meaningful and demands zero reported lag, making it the strictest possible reader gate, but *unset* is not `0`, it is inheritance. Option three gives up too early: the metric genuinely carries only a `site` label, and the join against role is one you perform yourself against the group spec — that is the whole discrimination, not a reason it cannot be made. Option four invents a promotion gate; `maxLagSeconds` drives only the `ReplicationLagging` Degraded condition and is never consulted when picking a candidate, so a lagging candidate stays promotable while a lagging reader merely sheds its reader endpoint. (objective 12)

## Question 10

**Type:** MULTIPLE_CHOICE

`playground` failed over to `pdx` ninety seconds ago; `spec.failoverCooldown` is `5m`. Now `pdx` goes unreachable and `iad` is read-only. What do you see on this poll?

- `Reason = "Degraded"` with `PromotionCandidates = [iad]` and no alert, plus INFO `failover blocked by anti-flap cooldown` with fields `lastFailover` and `cooldown` — and no promotion
- `Reason = "CooldownBlocked"` on the `Degraded` condition, so you can alert on the block directly
- `Reason = "Failover"`, as the cross-site evaluation table in `docs/docs/failover.mdx` publishes it, with the promotion deferred until the cooldown expires
- No condition update at all — the matrix is skipped while the cooldown is running, so `status.conditions` keeps whatever it last wrote

**Correct option index:** 0

**Explanation:**

The three conjuncts of the failover row hold — zero writable, one unreachable, one read-only — so the matrix fills `PromotionCandidates` and sets `Reason = "Degraded"`; it is the one acting row that sets no alert. The cooldown then bites at the single guard immediately before the promotion call and returns after logging `failover blocked by anti-flap cooldown` with `lastFailover` and `cooldown`. Option two invents a reason string: exactly five reach `status.conditions` — `Healthy`, `Degraded`, `SplitBrain`, `NoPrimary`, `TotalLoss`. Option three is the published docs table, and it is wrong in the code's favour: there is no `Failover` reason, so an alerting rule matching on one never fires. Option four contradicts the every-poll evaluation — the matrix runs on every poll so status always carries the current topology condition, and only the *mutating* cross-site actions are transition-driven. (objectives 4, 7)
