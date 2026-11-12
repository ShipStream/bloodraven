# Planned failover: moving the primary on purpose

`playground` is healthy. `iad` is writable, `pdx` and `reader` are read-only, and the counter app is still
writing through the `-primary` Service with the bounded-lifetime pool you fixed in the last topic.
Nothing is broken and nobody is paged — but `iad`'s nodes are being rebuilt on Tuesday and the primary
has to move. This is the one primary move you get to schedule, and the only one where Bloodraven shuts
your application's connections down *before* it takes the write endpoint away.

That is why this topic sits here rather than beside emergency failover. The emergency path does try to
kill application connections on the old primary — it is step 2 of the sequence — but it is best-effort,
single-pass, and skipped when the old primary is unreachable, which is the failure mode that made you
open this unit in the first place. An autonomous sidecar self-fence has no operator-side drain at all.
Only planned failover actually drains. Nothing else does.

## The trigger

One annotation, `bloodraven.shipstream.io/planned-failover`. Its value is a bare site name, or a site
name followed by `:key=value` overrides; `maxLagWait` is the only supported key and takes the spec
field's 1s–1h range, and an unknown or repeated key, or an out-of-range value, is rejected outright
(a `PlannedFailoverRejected` Event, no run) so a typo cannot quietly run with defaults. A valid
request is given a `runID` and the annotation is removed as the run enters `Admitting`; only a
`Deferred` request keeps it.

```widget
{
  "type": "terminal",
  "title": "Moving playground' primary from iad to pdx",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground annotate mysqlfailovergroup playground bloodraven.shipstream.io/planned-failover=pdx",
      "out": "mysqlfailovergroup.shipstream.io/playground annotated"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{.status.plannedFailover.phase}'",
      "out": "WaitingForLag"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{.status.plannedFailover.phase} {.status.plannedFailover.result.outcome}'",
      "out": "Succeeded Succeeded"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

Leave the counter app running while you do this. That is the point of the exercise.

## The phases

Every transition is one durable status write, and every side effect runs only after the record that
announces it has committed, so an operator restart always lands on a record whose side effect is either
done or provably not started. The complete list:

`Admitting`, `Deferred`, `Fencing`, `Draining`, `WaitingForLag`, `WaitingForDragonflySync`,
`PromotingDragonfly`, `Promoting`, `Publishing`, `RollingBack`, `Succeeded`, `Failed`.

There is no idle phase: a group that never had a planned failover has no `status.plannedFailover` block
at all. `Admitting` and `Deferred` own nothing. `Deferred` is entered when the cooldown blocks you and
`onCooldown` is `Defer`, or when a deployment holds a failover-hold lease. From `Fencing` on, the run
holds the group's **claim**: emergency failover, split-brain resolution, ordered updates, recovery and
DNS reconciliation are deferred until the promotion is confirmed, recorded and published.
`WaitingForDragonflySync` and `PromotingDragonfly` are skipped entirely when Dragonfly was not enabled
when the request was accepted — the next topic takes those two apart.

```widget
{
  "type": "flow",
  "title": "Planned failover phases, and where rollback stops",
  "steps": [
    {
      "label": "Admitting",
      "detail": "runID stamped, annotation removed. Owns nothing. Re-validates and waits for the claim; ends Failed{NotStarted, AdmissionTimeout} after 2 minutes."
    },
    {
      "label": "Deferred",
      "detail": "Cooldown with onCooldown: Defer, or a deployment hold. Owns nothing; annotation kept; retried at admission.retryTime."
    },
    {
      "label": "Fencing",
      "detail": "Claim granted. super_read_only=ON on the source, read back, GTID_EXECUTED recorded in fence.gtidSet. Primary role label stripped."
    },
    {
      "label": "Draining",
      "detail": "Application connections on the source killed every second. drainTimeout 30s, then proceeds regardless."
    },
    {
      "label": "WaitingForLag",
      "detail": "Polls the target until its GTID_EXECUTED contains fence.gtidSet. LagTimeout, DivergentTarget or InvalidGTID roll back. maxLagWait 5m."
    },
    {
      "label": "WaitingForDragonflySync",
      "detail": "Skipped unless Dragonfly was enabled at acceptance."
    },
    {
      "label": "PromotingDragonfly",
      "detail": "Skipped unless Dragonfly was enabled at acceptance."
    },
    {
      "label": "Promoting",
      "detail": "Before super_read_only=OFF can reach the target, failures still roll back. AFTER IT, NO ROLLBACK: PromotedUnconfirmed or ManualRecoveryRequired."
    },
    {
      "label": "Publishing",
      "detail": "Promotion confirmed, recorded and published to DNS; claim released. Only status bookkeeping remains."
    },
    {
      "label": "RollingBack",
      "detail": "Nothing was written to the target: unfence the source if no other site is writable and the fence is provably the run's."
    },
    {
      "label": "Succeeded / Failed",
      "detail": "Terminal. result.outcome says what the cluster was left in; result.reason says why."
    }
  ]
}
```

## Why it is RPO 0 — by construction

"Planned failover is safe" is a sentence people repeat without knowing why, so be exact. It is not luck
and it is not a lag threshold. It is an ordering.

At `Fencing` the operator sets `super_read_only = ON` on `iad`, reads it back, and *then* reads `iad`'s
`GTID_EXECUTED`, recording it in `status.plannedFailover.fence.gtidSet`. Fence first, snapshot second. A
fenced primary accepts no new client writes, so the set `pdx` must catch up to cannot grow underneath the
gate. That is the whole argument.

At `WaitingForLag` the operator polls `pdx`'s `GTID_EXECUTED` and asks one question: does it *contain*
`fence.gtidSet`? The check is a genuine GTID-set superset test. Not seconds. And immediately before it
clears `super_read_only` on `pdx`, a pre-write gate demands that `pdx` hold exactly the fenced set, with
no channel left and every other promotable site answering read-only. On success
`status.lastFailover.promotionGtidExecuted` equals `fence.gtidSet`: nothing acknowledged was lost, by construction.
There is no "transactions lost" counter on this path, because a clean switchover cannot produce a
number.

You met `spec.replication.maxLagSeconds` in Unit 2 and it looks like it belongs here. It does not. It
drives exactly one thing: the `ReplicationLagging` Degraded condition. It is not a promotion gate
anywhere — the emergency path promotes a replica that is past the threshold anyway, because no writable
site at all is nearly always worse — and `WaitingForLag` never consults it. Seconds are a bad gate on
their own merits: `Seconds_Behind_Source` compares last-executed against last-*downloaded* relay event,
so it reads 0 when the IO thread has stalled.

## Draining is a deadline, not a barrier

The defaults on `spec.plannedFailover`: `maxLagWait` 5m, `drainTimeout` 30s, `onCooldown` `Reject`.
Admission bounds the two timers, `maxLagWait` to 1s–1h and `drainTimeout` to 1s–10m; a `0` is refused,
not quietly replaced by the default.

Two things take the write endpoint away. At `Fencing` the source's primary role label is stripped to
`fenced`, which matches neither the `-primary` selector nor the `-replicas` selector, so the write
Service sheds the endpoint and new connections stop arriving. At `Draining` the operator kills the
connections already open, every second.

Then the sharp edge. When the budget runs out with connections still on the source, the operator logs
`drain budget exhausted after %s with %d connection(s) remaining on %q; proceeding` — and proceeds,
recording the count in `drain.remainingConnections`. A stuck client is not allowed to block a switchover
indefinitely. That is a defensible choice, and it is a decision you have just inherited: the connections
that outlive the drain are exactly the ones from the last topic, open against a demoted primary, passing
every validation query, serving stale reads until something tries to write. Your `drainTimeout` and your
pool's maximum connection lifetime are two halves of one setting. A pool that holds connections for ten
minutes against a thirty-second drain makes the drain decoration.

## Rollback stops at the first possible write

`result.outcome` says what the cluster was left in. The line that matters is whether
`super_read_only=OFF` may have reached the target.

| Outcome | When | Source afterwards | Who resolves it |
| --- | --- | --- | --- |
| `NotStarted` | Refused or timed out in admission (`CooldownActive`, `TargetNotPromotable`, `AdmissionTimeout`) | Never fenced | Nobody — re-annotate later |
| `RolledBack` | Failed before any write to the target (`LagTimeout`, `DivergentTarget`, `FenceFailed`, `TargetLost`, `PromotionAborted`) | Unfenced, still the primary | Nobody — nothing was lost |
| `SourceLeftFenced` | Nothing was written to the target, but unfencing was unsafe (another site writable, a fence it cannot prove is its own, a lost source) | Still fenced | Often the operator's re-assert; sometimes a human |
| `PromotedUnconfirmed` | A write may have reached the target and it could not be confirmed | Fenced; the target may be serving | A human, with the runbook |
| `ManualRecoveryRequired` | A write may have reached the target and the result cannot be explained. Also, whatever the run sent: its rollback found the source writable without transactions it held when it was fenced (`SourceUnfenced`) | Unknown | A human; automation is **held** until the hold is acknowledged |

One case crosses that line from the other side. A rollback that finds the source already writable
normally ends `RolledBack` with nothing to undo. If that source is also missing transactions it held
when it was fenced (it restarted on older data and was then made writable), it has begun a second
history that reuses the lost GTIDs. The run ends `ManualRecoveryRequired` with reason `SourceUnfenced`
and the same automation hold, although it may never have written to the target; the message names the
lost transactions, nothing is sent to the source, and the target is not pointed at it.

Draw the operational conclusion. A lag gate that never closes is the *good* failure: `iad` comes back
writable, the counter app resumes, and you have lost nothing but the fenced window. So when `maxLagWait`
expires against a `pdx` that is genuinely behind, let the rollback fire and fix the lag first. Raising
`maxLagWait` to force the gate closed does not make `pdx` catch up any faster — it only lengthens the
window in which your source is fenced and your writes are refused.

Two refusals worth recognising before you meet them. A planned failover aimed at `reader` ends
`Failed{NotStarted, TargetNotPromotable}` with `only PrimaryCandidate sites may be promoted` — the
promotability rule from Unit 1, enforced again at this entry point. And the anti-flap cooldown gates
planned admission just as it gates the automatic path, with reason `CooldownActive`; because
`onCooldown` defaults to `Reject`, a planned failover attempted soon after an emergency one ends
`NotStarted` and the annotation is cleared, not queued. Set `onCooldown: Defer` if you would rather it
wait in `Deferred` and retry itself at `admission.retryTime`. One more: planned failover is strict. If a
`PrimaryCandidate` or `DisasterRecovery` site does not answer, admission waits with `waitReason: PeerUnreadable`
and ends `AdmissionTimeout` after two minutes. A site it cannot read might be writable.

## What you have now

You can move `playground`' primary on purpose, follow `status.plannedFailover.phase` to `Succeeded`,
check that `status.lastFailover.promotionGtidExecuted` is the fenced set, and argue the superset gate to anyone who
thinks it is a lag threshold. You can also say which outcomes hand you the cluster back intact and which
hand you a fenced primary, an automation hold and a pager. Everything above assumed MySQL was the only
thing moving. Next: what happens when there is a Dragonfly beside it, and what Bloodraven does and does
not promise about it.
