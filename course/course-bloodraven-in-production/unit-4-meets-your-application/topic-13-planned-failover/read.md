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
name followed by `:key=value` overrides; `maxLagWait` is the only supported key, and an unknown key is
rejected outright so a typo cannot quietly run with defaults. The annotation is consumed and cleared on
the next reconcile, like `reclone-site`.

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
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{.status.plannedFailover.phase} {.status.plannedFailover.transactionsLost}'",
      "out": "Succeeded 0"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

Leave the counter app running while you do this. That is the point of the exercise.

## The phases

The reconciler advances the state machine by one step per reconcile, so an operator restart always lands
on a well-defined observable state. The complete list, in order:

`""`, `Pending`, `Deferred`, `Validating`, `Draining`, `WaitingForLag`, `WaitingForDragonflySync`,
`PromotingDragonfly`, `Promoting`, `Resuming`, `Succeeded`, `Failed`.

`Deferred` is only entered when the cooldown blocks you and `onCooldown` is `defer`.
`WaitingForDragonflySync` and `PromotingDragonfly` are skipped entirely when `spec.dragonfly` is unset or
disabled — the Dragonfly topics later in this unit take those two apart.

```widget
{
  "type": "flow",
  "title": "Planned failover phases, and where rollback stops",
  "steps": [
    {
      "label": "\"\"",
      "detail": "Idle. No planned failover in flight."
    },
    {
      "label": "Pending",
      "detail": "Annotation observed; about to validate."
    },
    {
      "label": "Deferred",
      "detail": "Only on cooldown with onCooldown: defer. Annotation kept; retried at cooldown expiry."
    },
    {
      "label": "Validating",
      "detail": "Site exists, is primary-candidate, is read-only and replicating; no restore, update or planned run in flight; cooldown clear."
    },
    {
      "label": "Draining",
      "detail": "super_read_only=ON on the source, GTID_EXECUTED snapshotted into sourceGtidAtFence, primary role label stripped, connections killed. drainTimeout 30s."
    },
    {
      "label": "WaitingForLag",
      "detail": "ROLLBACK LIVES HERE. Polls target GTID_EXECUTED until it contains sourceGtidAtFence. LagTimeout or InvalidGTID unfence the source and stamp Failed. maxLagWait 5m."
    },
    {
      "label": "WaitingForDragonflySync",
      "detail": "Skipped unless spec.dragonfly is enabled."
    },
    {
      "label": "PromotingDragonfly",
      "detail": "Skipped unless spec.dragonfly is enabled."
    },
    {
      "label": "Promoting",
      "detail": "PAST THE POINT OF NO UNFENCE. Failure stamps Failed{ExecuteFailed}: manual recovery required."
    },
    {
      "label": "Resuming",
      "detail": "activeSite, lastFailover, lastFailoverTarget and promotionGtidExecuted written. Failure here also leaves the source fenced."
    },
    {
      "label": "Succeeded / Failed",
      "detail": "Terminal. Status stays populated so kubectl describe explains the outcome."
    }
  ]
}
```

## Why it is RPO 0 — by construction

"Planned failover is safe" is a sentence people repeat without knowing why, so be exact. It is not luck
and it is not a lag threshold. It is an ordering.

At `Draining` the operator sets `super_read_only = ON` on `iad` and *then* reads `iad`'s
`GTID_EXECUTED`, recording it in `status.plannedFailover.sourceGtidAtFence`. Fence first, snapshot
second. A fenced primary accepts no new client writes, so the set `pdx` must catch up to cannot grow
underneath the gate. That is the whole argument.

At `WaitingForLag` the operator polls `pdx`'s `GTID_EXECUTED` and asks one question: does it *contain*
`sourceGtidAtFence`? The check is a genuine GTID-set superset test — `gtidContains(targetGtid,
cur.SourceGtidAtFence)`, which resolves to `super.Contains(sub)`. Not seconds. Promotion runs only after
that returns true, so `status.plannedFailover.transactionsLost` is 0 on a successful switchover by
construction. The field exists for symmetry with the emergency path's accounting, not because a clean
switchover can produce a number.

You met `spec.replication.maxLagSeconds` in Unit 2 and it looks like it belongs here. It does not. It
drives exactly one thing: the `ReplicationLagging` Degraded condition. It is not a promotion gate
anywhere — the emergency path promotes a replica that is past the threshold anyway, because no writable
site at all is nearly always worse — and `WaitingForLag` never consults it. Seconds are a bad gate on
their own merits: `Seconds_Behind_Source` compares last-executed against last-*downloaded* relay event,
so it reads 0 when the IO thread has stalled.

## Draining is a deadline, not a barrier

The defaults on `spec.plannedFailover`: `maxLagWait` 5m, `drainTimeout` 30s, `onCooldown` `reject`.

Two things happen at `Draining`. The source's primary role label is stripped to `fenced`, which matches
neither the `-primary` selector nor the `-replicas` selector, so the write Service sheds the endpoint and
new connections stop arriving. And the reconciler repeatedly kills the connections already open.

Then the sharp edge. When the budget runs out with connections still on the source, the operator logs
`drain budget exhausted after %s with %d connection(s) remaining on %q; proceeding` — and proceeds. A
stuck client is not allowed to block a switchover indefinitely. That is a defensible choice, and it is a
decision you have just inherited: the connections that outlive the drain are exactly the ones from the
last topic, open against a demoted primary, passing every validation query, serving stale reads until
something tries to write. Your `drainTimeout` and your pool's maximum connection lifetime are two halves
of one setting. A pool that holds connections for ten minutes against a thirty-second drain makes the
drain decoration.

## Rollback exists in exactly one phase

| Failure | Phase | Source afterwards | Who resolves it |
| --- | --- | --- | --- |
| `CooldownActive` | `Validating` | Never fenced | Nobody — re-annotate later |
| `LagTimeout`, `InvalidGTID` | `WaitingForLag` | Unfenced, still the primary | Nobody — nothing was lost |
| `ExecuteFailed` | `Promoting` | Still fenced | A human |
| status write failure | `Resuming` | Still fenced | A human |

Draw the operational conclusion. A lag gate that never closes is the *good* failure: `iad` comes back
writable, the counter app resumes, and you have lost nothing but the fenced window. So when `maxLagWait`
expires against a `pdx` that is genuinely behind, let the rollback fire and fix the lag first. Raising
`maxLagWait` to force the gate closed does not make `pdx` catch up any faster — it only lengthens the
window in which your source is fenced and your writes are refused.

Two refusals worth recognising before you meet them. A planned failover aimed at `reader` is hard-refused
with `only primary-candidate sites may be promoted` — the promotability rule from Unit 1, enforced again
at this entry point. And the anti-flap cooldown gates planned admission just as it gates the automatic
path, with reason `CooldownActive`; because `onCooldown` defaults to `reject`, a planned failover
attempted soon after an emergency one is refused and the annotation cleared, not queued. Set
`onCooldown: defer` if you would rather it wait in `Deferred` and fire itself at cooldown expiry.

## What you have now

You can move `playground`' primary on purpose, follow `status.plannedFailover.phase` to `Succeeded`, read
`transactionsLost: 0`, and argue the superset gate to anyone who thinks it is a lag threshold. You can
also say which failures hand you the cluster back intact and which hand you a fenced primary and a
pager. Everything above assumed MySQL was the only thing moving. Next: what happens when there is a
Dragonfly beside it, and what Bloodraven does and does not promise about it.
