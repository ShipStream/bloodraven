# Quiz — Planned failover: moving the primary on purpose

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

`playground` has been sitting in `WaitingForLag` for two minutes. `pdx` reports `Seconds_Behind_Source = 0` and is well inside `spec.replication.maxLagSeconds`. What is the operator actually waiting for before it will promote?

- Replication lag on `pdx` to fall below `spec.replication.maxLagSeconds`, whose default is 300.
- `pdx`'s `GTID_EXECUTED` to contain the source's `GTID_EXECUTED` snapshot taken at the fence.
- `Seconds_Behind_Source` to read 0 across three consecutive polls, matching `failureThreshold`.
- The relay-log drain on `pdx` to finish inside its 30-second budget before promotion runs.

**Correct option index:** 1

**Explanation:**

The gate is a true GTID-set superset test: the reconciler polls the target's `GTID_EXECUTED` and advances only when it contains `status.plannedFailover.fence.gtidSet`. `maxLagSeconds` is not it — that field drives exactly one thing, the `ReplicationLagging` Degraded condition, and is never a promotion gate on either the planned or the emergency path. A streak of `Seconds_Behind_Source = 0` is not it either, and is a bad signal on its own merits: it compares last-executed against last-downloaded relay event, so it reads 0 when the IO thread has stalled. The relay-log drain belongs to the emergency sequence, not to planned failover's lag gate. (objective 8)

## Question 2

**Type:** TRUE_FALSE

`drainTimeout` is a barrier: when it expires with application connections still open on the source, the planned failover aborts and the source is unfenced.

**Correct answer:** false

**Explanation:**

The reversal: the drain is a deadline, not a barrier. When the budget is exhausted with connections remaining, the operator logs `drain budget exhausted after %s with %d connection(s) remaining on %q; proceeding` and proceeds to promotion, so a stuck client cannot block a switchover indefinitely. Nothing is unfenced and nothing aborts. That is why your pool's maximum connection lifetime has to be shorter than the drain budget — connections that outlive the drain are the ones that go on serving stale reads against a demoted primary. (objective 7)

## Question 3

**Type:** MULTIPLE_CHOICE

Four planned failovers on `playground` failed in four different ways. Which one cannot be rolled back, leaving the source fenced for a human to put right?

- `LagTimeout` in `WaitingForLag`, after `maxLagWait` expired against a lagging target.
- `FenceFailed` in `Fencing`, when the fence could not be applied and proven within 30 seconds.
- `PromotionUnconfirmed` in `Promoting`, after `super_read_only=OFF` may have reached the target and the promotion could not be confirmed.
- `CooldownActive` in `Admitting`, when the anti-flap window had not yet elapsed.

**Correct option index:** 2

**Explanation:**

Rollback reaches as far as the first possible write to the target. `LagTimeout` and `FenceFailed` both fail before anything was written to the target, so the run goes through `RollingBack` and, with no other site writable, unfences the source: outcome `RolledBack`, nothing lost. `CooldownActive` fires in admission, before the source was ever fenced: outcome `NotStarted`, nothing to undo. `PromotionUnconfirmed` is past the boundary: `super_read_only=OFF` may have reached the target, so unfencing the source could leave two writable primaries. The run ends `PromotedUnconfirmed` with the source fenced, and a human follows the runbook. A result the run cannot explain at all goes further, to `ManualRecoveryRequired` and an automation hold. (objective 9)

## Question 4

**Type:** SHORT_ANSWER

A planned failover of `playground` from `iad` to `pdx` has rolled back twice with `LagTimeout`; `pdx` is genuinely behind because of a long-running batch job. A colleague proposes annotating with `pdx:maxLagWait=30m` so the gate has time to close. Argue the call.

**Sample answer:**

Do not raise it — fix the lag first. The rollback is the good outcome: `LagTimeout` in `WaitingForLag` unfences `iad`, which stays the active primary with nothing lost, so both failed attempts cost only the fenced window. Raising `maxLagWait` to 30m does not make `pdx` apply relay logs any faster; it only extends the period during which `iad` is fenced with `super_read_only=ON` and the counter app's writes are being refused. Kill or wait out the batch job, confirm `pdx` is applying, then re-annotate with the default 5m and the gate will close on its own.

**A full-credit answer shows:**

A strong answer covers: (1) rollback in `WaitingForLag` unfences the source, so the failure is safe and costs no data; (2) `maxLagWait` is a timeout, not a throttle — it does not accelerate catch-up; (3) the real cost of a longer wait is a longer fenced source with writes refused; (4) the correct action is to remove the source of the lag and retry.

**Explanation:**

The lag gate that never closes is the failure mode you want, because it is the only one that hands the cluster back intact. Lengthening `maxLagWait` trades a safe, reversible refusal for a longer write outage on a primary that is already fenced, without changing whether the target can catch up. (objective 9)

## Question 5

**Type:** MULTIPLE_CHOICE

Ninety seconds after an emergency failover promoted `pdx`, you annotate `playground` to plan a switchover back to `iad`. It lands in `Failed` with reason `CooldownActive`. What happened, and what is the correct response?

- The annotation value was malformed; re-apply it as `iad:maxLagWait=5m` and it will be accepted.
- The anti-flap cooldown gates planned admission too, and `onCooldown` defaults to `Reject`.
- `iad` has not finished rejoining as a replica; the gate refuses targets that are not caught up.
- One failover is allowed per cooldown window; the second is queued and will fire at expiry.

**Correct option index:** 1

**Explanation:**

The same anti-flap cooldown that gates automatic promotion is evaluated in admission for planned failover, against the same durable failover record. With the default `onCooldown: Reject` the run ends `Failed{NotStarted, CooldownActive}` and the annotation is cleared — so wait out the cooldown and re-annotate, or set `onCooldown: Defer` to have it wait in `Deferred` and retry itself at `admission.retryTime`. The annotation was well-formed; a malformed one is rejected with a `PlannedFailoverRejected` Event and never becomes a run. A target that is unreachable, writable or not replicating is refused with `TargetUnhealthy`, a different reason. And nothing is queued by default — that is precisely what `Defer` opts you into. (objective 7)
