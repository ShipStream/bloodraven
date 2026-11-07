# Cache and sessions that follow the primary

You can move `playground`' primary on purpose, and you know the phase list it walks. Two of those phases —
`WaitingForDragonflySync` and `PromotingDragonfly` — have gone unexplained. They exist because the
counter app does not only write to MySQL. It keeps a session store in Dragonfly, one pod per site, and
when the primary moves that store must move with it or every logged-in user is bounced.

## The boundary, and everything that follows from it

Dragonfly here is **cache and session state, never durable application data**. If losing it costs you
a transaction, it was in the wrong place. The CRD says so itself: Dragonfly is "treated as non-durable
cache/session state: emergency failover never blocks on it" (`api/v1alpha1/dragonfly_types.go`).

Hold that sentence; the rest of this topic is derived from it. It is why the emergency budget is small,
why one promotion path may lose sessions outright, and why nothing Dragonfly does may ever affect MySQL
durability. For `playground`: the counter app's session store is nice to keep across a promotion, never the
record of truth.

## One Service, two labels, AND-gated

Bloodraven creates one app-facing Service, `playground-dragonfly`, whose selector AND-gates two labels the
operator stamps on Dragonfly pods: `bloodraven.shipstream.io/dragonfly-role=master` and
`bloodraven.shipstream.io/dragonfly-traffic=true`. A pod is an endpoint only when **both** match. Role says
which pod is master; traffic is the canonical "this pod serves writes" gate.

Now the part worth slowing down for. To shed an endpoint the operator **deletes** the traffic key. It
does not stamp a disabled value. The code says why: "Removing the label (set=false) is preferred over
stamping a 'disabled' value because the active-Service selector is an exists-and-equals check on
'true'" (`internal/controller/dragonfly_labels.go`).

```widget
{"type":"anatomy","title":"bloodraven.shipstream.io/dragonfly-traffic=true","parts":[{"text":"bloodraven.shipstream.io/","label":"prefix","note":"Bloodraven's own label domain; every Bloodraven key uses it. Nothing outside the operator writes this label."},{"text":"dragonfly-traffic","label":"key","note":"The gate. The active Service selector is an exists-and-equals check on this key: absent key, no match, no endpoint."},{"text":"true","label":"the only value that means anything","note":"A boolean label value is always true or false, and this one is only ever true: there is no 'false' value written. Shedding deletes the key, because a deleted key cannot be misread — a written value depends on the selector agreeing what it means."}]}
```

Derive the consequence yourself. The takeover sequence strips the source's traffic key *before* it
promotes the target, and stamps the target's key only after. An absent key cannot match. So there is no
window in which both instances match the active selector — no moment where the Service load-balances
your session writes across two masters. With `dragonfly-traffic=disabled` instead, correctness would
depend on selector and writer agreeing on a magic string. Deletion needs no agreement. The steady-state
label sweep honours the same window: mid-takeover it skips re-stamping the source, because that would
"re-attach the old master to the active Service mid-takeover, which is exactly the bug the strip is
preventing".

## Two promotion paths, and only one of them can fall back

Both paths reach for the same primitive first: `REPLTAKEOVER`, Dragonfly's atomic replica takeover. What
differs is what happens when it fails.

```widget
{
  "type": "compare",
  "title": "Emergency vs planned Dragonfly promotion",
  "rows": [
    {
      "aspect": "What is tried?",
      "cells": [
        "REPLTAKEOVER on the target with a 2 s budget, only if the old master answers a 1 s probe",
        "REPLTAKEOVER on the target, with maxSyncWait as the timeout argument"
      ]
    },
    {
      "aspect": "What happens on failure?",
      "cells": [
        "Falls back to REPLICAOF NO ONE, promoting the target as an empty master",
        "No REPLICAOF NO ONE fallback. onSyncTimeout decides: Proceed (default) promotes MySQL anyway with sessions Lost, or Fail rolls back the MySQL fence"
      ]
    },
    {
      "aspect": "What happens to sessions?",
      "cells": [
        "Preserved on REPLTAKEOVER; lost on the fallback — logged as 'target promoted via REPLICAOF NO ONE (sessions lost)'",
        "Preserved after a caught-up REPLTAKEOVER; Lost under Proceed; under Fail the move does not happen. Retry it"
      ]
    }
  ],
  "columns": [
    {
      "label": "Emergency (after MySQL failover)"
    },
    {
      "label": "Planned (PromotingDragonfly phase)"
    }
  ]
}
```

The asymmetry is the same boundary again. An emergency promotion cannot be retried — MySQL has already
failed over and a cache that refuses to promote serves nothing — so it takes the empty master and says
so in the log. A planned switchover *can* be retried, so it refuses to buy availability with sessions.

## Bounded steps, and why MySQL never waits

MySQL does not run the emergency Dragonfly promotion at all. Once MySQL has promoted, its poll loop only
*queues* a request for the new active site and carries on; the group's Dragonfly manager runs it on its
own goroutine. The contract, written in the source, is three promises: it "never returns an error to the
caller, every step is bounded, and it never leaves Dragonfly in a state that affects MySQL durability"
(`internal/controller/dragonfly_topology.go`).

There is no single budget; each step has its own:

- A 1 s dial and `PING` of the old master. A takeover is coordinated by the master, so if it does not
  answer, `REPLTAKEOVER` is skipped.
- `REPLTAKEOVER` with a 2 s budget, which the client waits up to 5 s for (the budget plus 3 s for the reply).
- The `REPLICAOF NO ONE` fallback, on a fresh connection: up to 14 s after a failed takeover, because
  Dragonfly holds the replica's `REPLICAOF` lock for the takeover budget plus 10 s, or 5 s when the takeover
  was skipped.
- An outcome that stays unconfirmed (the fallback could not be sent, or its reply was lost) is settled by
  reading the target's role once 13 s have passed since the takeover was sent (1 s after `REPLICAOF NO ONE`
  when no takeover was sent).
- Every other step, from the preliminary reads to the label stamps, gets 5 s.

Operationally that is one sentence: **MySQL failover is never delayed by cache.** A wedged Dragonfly, an
unreachable target, a takeover that hangs — none of it extends your measured 12.0 s emergency promotion.
The attempt runs *after* MySQL has already succeeded, on a goroutine MySQL never waits for.

## Reading `dragonfly.sessions` — absent is not `Lost`

`status.plannedFailover.dragonfly.sessions` has three states, and the third is load-bearing:

| Value | Meaning |
| --- | --- |
| `Preserved` | `REPLTAKEOVER` succeeded on a caught-up replica. Sessions survived. |
| `Lost` | Sessions were lost: a sync timeout or a source offset that could not be captured under `Proceed`, or a failed `REPLTAKEOVER` (`dragonfly.reason` says which). |
| absent | **Unknown.** Not `Lost`. |

Absent is not failure. Read it as failure and you will chase incidents that did not happen; read it as
success and you will miss ones that did. Absent is what you get when the field was never written —
Dragonfly not enabled when the request was accepted, or a run that ended before its Dragonfly phases,
for instance. Note too that it lives on the *planned*-failover status. An
emergency promotion never writes it; that outcome lives in the log line and the event.

Two settings shape which of the three you get:

- `maxSyncWait`, default `30s`. It bounds `WaitingForDragonflySync` **and** doubles as the timeout
  argument passed to `REPLTAKEOVER`, rounded up to whole seconds. The target lets its master run 10 s past
  that budget, and the client waits 2 s more for the reply so it cannot give up before the server has —
  30 s + 12 s = a 42 s client read deadline at the default.
- `onSyncTimeout`, enum `Proceed;Fail` (CamelCase: the CRD rejects the lower-case spellings older
  releases used), default `Proceed`. Spell `Proceed` out: the promotion goes ahead and the cache outcome
  is whatever it is. `Fail` aborts before MySQL promotion and rolls the source fence back.

`playground` pins both at their defaults:

```yaml
# playground/manifests/failovergroup.yaml  (unchanged from earlier topics)
  dragonfly:
    enabled: true
    image: docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0
    # … port, memory, resources elided …
    plannedFailover:
      maxSyncWait: 30s
      onSyncTimeout: Proceed
```

A clean switchover leaves `dragonfly.takeover.method: ReplTakeover` and `dragonfly.sessions: Preserved`.
The same move with a lagging replica under `Proceed` leaves `dragonfly.sessions: Lost` with
`dragonfly.reason: DragonflySyncTimeout` — and a MySQL promotion that still lands at RPO 0. The cache outcome does not touch the MySQL guarantee. That is the whole design.

## Where `REPLTAKEOVER` actually comes from

Be honest about the primitive. It is real, and it is undocumented: an ADMIN-port,
`GLOBAL_TRANS` command taking a timeout in seconds, introduced in Dragonfly **v1.5.0** (2023-07-03),
with no official documentation page. The reference is the Dragonfly source
(`src/server/server_family.cc`) and the v1.5.0 release notes — "Support atomic replica takeover" — not
a manual. If you audit this path, that is where you read.

## Version discipline is yours, not the operator's

"Dragonfly v1.38.0 or later" is a **support policy, not a guardrail**. Nothing in the API types, the
controller, or the chart enforces or checks a Dragonfly version. The only CEL rules on `spec.dragonfly`
are that an image is required when Dragonfly is enabled, and that it may not be `:latest`. Run a build
older than `REPLTAKEOVER` itself and Bloodraven will not stop you; it will fail in ways nobody has
characterised, on a path that by design never returns an error to its caller.

That is the durable half. The perishable half is which numbers are current — the repo's pin, and
upstream's latest stable, both move — so it lives in the
[version appendix](../sources.html#version-appendix), row C2, along with the command that re-checks
both. When this course was written those two numbers were two minors apart, which is the ordinary
condition of a pin nobody is forced to update.

## Where this leaves you

You can state the guarantee and its limits: best-effort cache continuity, never at the expense of MySQL,
with a shed-then-promote label sequence that has no dual-master window. You can read `dragonfly.sessions`
without misreading an absent value, and say which path may trade sessions for availability. Services, DNS, taints,
pools, planned moves, cache: the operator's side of the application boundary is covered.

One thing on your side of that boundary is still unaccounted for. The deploy pipeline changes the schema
on the same primary the operator moves, on its own calendar, with no idea a switchover is booked. The
last topic in this unit gives it a way to ask first — and a fence for when the answer changes underneath
it.
