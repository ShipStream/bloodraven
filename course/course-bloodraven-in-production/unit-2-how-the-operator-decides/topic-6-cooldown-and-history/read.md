# Cooldown, history, and the one exception

`orders` has just failed over. `iad` went unreachable, `pdx` was promoted, the counter app is
writing again. The cross-site table you ran in the last topic is still evaluating every poll, and it
will happily select another promotion the second `pdx` looks unreachable — a flap that can walk the
group across every site in under a minute. The thing that stops it is memory: `spec.failoverCooldown`.
Almost everyone overestimates its reach.

## One guard, and only one

`spec.failoverCooldown` defaults to `5m`, both in the CRD and in the operator's own fallback when the
pointer is nil. The playground overrides it to `30s` so you can experiment without waiting.

It is enforced in exactly one place: immediately before the promotion call, after the table has
already chosen a candidate.

```go
if !lastFailover.IsZero() && tm.clock.Since(lastFailover) < tm.failoverCooldown {
    tm.logger.Info("failover blocked by anti-flap cooldown",
        "lastFailover", lastFailover, "cooldown", tm.failoverCooldown)
    return
}
```

One `if`, one `return`, one `msg` with two fields. That is the entire mechanism. The table still
evaluated, the candidate was still ranked, the status condition still says `Degraded` — the operator
simply declined to promote.

## What it does not suppress

Here is where wrong mental models get built. Operators read "cooldown" and picture a five-minute
freeze on the group. It is not that. Every other mutating action in the poll cycle runs from its own
call site, and none of them consults `tm.failoverCooldown`.

| Still runs during the cooldown | Consequence for `orders` |
| --- | --- |
| Split-brain fencing | A second writable site is fenced on the next poll, not five minutes later |
| Non-promotable fencing | A writable `reader` is fenced every poll, unconditionally |
| Source convergence | `iad`, when it returns, is repointed at `pdx` immediately |
| Old-primary recovery | The `STOP REPLICA` / `RESET REPLICA ALL` / `CHANGE REPLICATION SOURCE` rejoin proceeds |
| Reclone | A `reclone-site` annotation is honoured while the cooldown ticks |
| DNS reconcile | The `DNSEndpoint` is re-applied every poll regardless |

Then the sharp edge. The **ordered-update handoff promotion is not cooldown-gated at all** — grep
`updater.go` for `cooldown` and you get nothing. Yet its completion callback calls `recordFailover`
and increments `bloodraven_failovers_total`. So during a rolling spec change your failover counter
can move, your dashboards can page, and the cooldown was never consulted, never logged, never
relevant. If you alert on that counter, alert on the ordered-update log lines too.

## The history, written twice

`lastFailover` and `lastFailoverTarget` go to two durable places on every promotion: the CR **status
subresource**, and two annotations on the object's own metadata, written together in a single JSON
merge patch.

```widget
{"type":"anatomy","title":"bloodraven.shipstream.io/last-failover-target","parts":[{"text":"bloodraven.shipstream.io/","label":"domain prefix","note":"Namespaces the key to this operator. Everything Bloodraven stamps on an object — planned failover, reclone, chaos markers — shares it."},{"text":"last-failover","label":"record noun","note":"Names the promotion event. On its own this is the sibling key bloodraven.shipstream.io/last-failover, whose value is the promotion instant as RFC3339 UTC at second precision."},{"text":"-target","label":"target qualifier","note":"Switches the value from the instant to the promoted site name, verbatim. Never written alone — both keys go in one JSON merge patch, so a reader never sees a timestamp without its target."}]}
```

The duplication is deliberate, and the code says so in a comment. Status is a **subresource**: writes
to it travel a separate API path, with their own RBAC rule and their own admission plugins. A broken
webhook or a missing `mysqlfailovergroups/status` grant can silence status writes for hours while
ordinary object patches keep succeeding — and vice versa. Two independently-failing paths mean a
promotion has to lose both before the cooldown forgets it happened.

```widget
{
  "type": "compare",
  "title": "Two durable copies of the same fact",
  "rows": [
    {
      "aspect": "Which API path writes it?",
      "cells": [
        "The status subresource — a separate endpoint from the object itself",
        "The object's own metadata, via a JSON merge patch on the parent resource"
      ]
    },
    {
      "aspect": "What RBAC does that need?",
      "cells": [
        "mysqlfailovergroups/status",
        "mysqlfailovergroups (patch on the resource itself)"
      ]
    },
    {
      "aspect": "Which wins on rehydration?",
      "cells": [
        "Wins a tie — equal timestamps mean the same promotion",
        "Wins only when strictly later than the status copy"
      ]
    }
  ],
  "columns": [
    {
      "label": "status.lastFailover"
    },
    {
      "label": "bloodraven.shipstream.io/last-failover"
    }
  ]
}
```

On restart the operator reads both and installs **the later one**, guarded by
`FailoverClockSkewGrace = 5 * time.Minute`: a copy stamped more than five minutes ahead of local time
is discarded rather than installed, because the cooldown gate treats negative elapsed time as still
active and a future-dated record would wedge promotion indefinitely. Ties go to status. That tie rule
is why the annotation is written at second precision — matching what `metav1.Time` serialises — so
the same promotion produces an exact tie rather than an annotation that always looks newer.

## The one exception: re-asserting a fenced primary

There is a wedge the pure table cannot escape. Every site is read-only, none is unreachable, so the
table refuses to elect and raises `NoPrimary`. But the operator holds history the table refuses to
consult: `lastFailoverTarget` names the site it already made authoritative. If that site is still
GTID-complete, restoring its writability cannot lose a transaction or create a second primary.

Every one of these must hold:

- No subsystem gate active — bootstrap blocking cross-site, ordered update, topology frozen, planned
  failover in flight.
- The re-assert rate limit is satisfied, and no promotion is still pending confirmation.
- Every non-target peer is `read-only` — not writable, not unreachable, not unknown.
- The target is `read-only` **and** promotable (`role: primary-candidate`).
- The target's `GTID_EXECUTED` contains the recorded promotion GTID set **and** every peer's
  `GTID_EXECUTED`.

Fail the GTID parse and the operator does not fall through — it **refuses**:

```
primary re-assert refused: recorded promotion GTID set failed to parse — status corrupted or manually edited?
```

The safety argument rests on that recorded invariant being trustworthy. An unreadable invariant is
not a satisfied one, so skipping the gate would be exactly backwards. If you see this line, someone
edited status by hand.

On success, verbatim, at `WARN`:

```
re-asserting fenced promoted primary: no site is writable and the last failover target is GTID-complete; restoring writability
```

with field `site`, followed by `bloodraven_primary_reassert_total{site}`. A steadily climbing counter
means something keeps fencing your primary — look at sidecar connectivity, not at the operator.

## The timing that surprises people

The re-assert rate limit reuses the `failoverCooldown` **duration**, but measures it against a
**separate timer**, `lastReassert`, which is never compared against `lastFailover`. Concretely: on
the shipped 5 m default, `pdx` is promoted at T+0, the promotion takes the measured 12.0 s, and at
T+2m the whole group goes read-only. A second automatic promotion is blocked — 2 m is less than 5 m.
A re-assert is not, because `lastReassert` is still zero. On the playground's `30s` cooldown the
same asymmetry compresses: automatic promotion is blocked until T+30s (leaving 30 − 12.0 = 18.0 s of
block after a 12.0 s failover), while the first re-assert is available immediately.

One last trap while you are here: `spec.updateStrategy` exists in the CRD and the docs describe it as
a control, but no Go code outside the type definition reads it. Ordered update triggers on spec drift
alone. Setting `Recreate` changes nothing.

## Where that leaves you

You can now look at a decision the table selected and say whether it will actually execute right now:
promotion is gated, everything else is not, and the ordered-update handoff sidesteps the gate while
still moving the counter. You can find the failover history in both durable places and say which one
a restarted operator believed. What you cannot yet do is watch it happen — the next topic follows one
poll cycle through the structured log, using the `msg` strings the operator promises not to change.
