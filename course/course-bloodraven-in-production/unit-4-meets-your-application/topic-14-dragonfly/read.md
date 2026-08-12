# Cache and sessions that follow the primary

You can move `orders`' primary on purpose, and you know the phase list it walks. Two of those phases —
`WaitingForDragonflySync` and `PromotingDragonfly` — have gone unexplained. They exist because the
counter app does not only write to MySQL. It keeps a session store in Dragonfly, one pod per site, and
when the primary moves that store must move with it or every logged-in user is bounced.

## The boundary, and everything that follows from it

Dragonfly here is **cache and session state, never durable application data**. If losing it costs you
a transaction, it was in the wrong place. The CRD says so itself: Dragonfly is "treated as non-durable
cache/session state: emergency failover never blocks on it" (`api/v1alpha1/dragonfly_types.go`).

Hold that sentence; the rest of this topic is derived from it. It is why the emergency budget is small,
why one promotion path may lose sessions outright, and why nothing Dragonfly does may ever affect MySQL
durability. For `orders`: the counter app's session store is nice to keep across a promotion, never the
record of truth.

## One Service, two labels, AND-gated

Bloodraven creates one app-facing Service, `orders-dragonfly`, whose selector AND-gates two labels the
operator stamps on Dragonfly pods: `shipstream.io/dragonfly-role=master` and
`shipstream.io/dragonfly-traffic=enabled`. A pod is an endpoint only when **both** match. Role says
which pod is master; traffic is the canonical "this pod serves writes" gate.

Now the part worth slowing down for. To shed an endpoint the operator **deletes** the traffic key. It
does not stamp a disabled value. The code says why: "Removing the label (set=false) is preferred over
stamping a 'disabled' value because the active-Service selector is an exists-and-equals check on
'enabled'" (`internal/controller/dragonfly_labels.go`).

```widget
{"type":"anatomy","title":"shipstream.io/dragonfly-traffic=enabled","parts":[{"text":"shipstream.io/","label":"prefix","note":"Bloodraven's own label namespace. Nothing outside the operator writes it."},{"text":"dragonfly-traffic","label":"key","note":"The gate. The active Service selector is an exists-and-equals check on this key: absent key, no match, no endpoint."},{"text":"enabled","label":"the only value that means anything","note":"There is no 'disabled' value. Shedding deletes the key, because a deleted key cannot be misread — a written value depends on the selector agreeing what it means."}]}
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
        "REPLTAKEOVER on the target, inside a hard-coded 10 s budget",
        "REPLTAKEOVER on the target, with maxSyncWait as the timeout argument"
      ]
    },
    {
      "aspect": "What happens on failure?",
      "cells": [
        "Falls back to REPLICAOF NO ONE, promoting the target as an empty master",
        "No fallback. onSyncTimeout decides: proceed flagged unpreserved, or fail and roll back the MySQL fence"
      ]
    },
    {
      "aspect": "What happens to sessions?",
      "cells": [
        "Preserved on REPLTAKEOVER; lost on the fallback — logged as 'target promoted via REPLICAOF NO ONE (sessions lost)'",
        "Preserved, or the move did not happen. Retry it"
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

## The 10 s budget, and why MySQL never waits

The emergency path wraps everything in `const budget = 10 * time.Second`. Its contract, written in the
source, is three nevers: it "never returns an error to the caller, never blocks longer than a small
bounded budget, and never leaves Dragonfly in a state that affects MySQL durability"
(`internal/controller/dragonfly_topology.go`).

Operationally that is one sentence: **MySQL failover is never delayed by cache.** A wedged Dragonfly, an
unreachable target, a takeover that hangs — none of it extends your measured 12.0 s emergency promotion.
The attempt runs *after* MySQL has already succeeded, and its deadline fires at 10 s regardless of what
timeout the takeover was handed.

## Reading `sessionsPreserved` — it is tri-state

`status.plannedFailover.dragonfly.sessionsPreserved` is a `*bool`, and the pointer is load-bearing:

| Value | Meaning |
| --- | --- |
| `true` | Promotion completed cleanly, replica caught up. Sessions survived. |
| `false` | Sessions were lost: sync timeout with `proceed`, `REPLTAKEOVER` failure, or empty-master fallback. |
| absent (nil) | **Unknown.** Not false. |

Nil is not failure. Read it as failure and you will chase incidents that did not happen; read it as
success and you will miss ones that did. Nil is what you get when the field was never written — Dragonfly
disabled for that attempt, for instance. Note too that it lives on the *planned*-failover status. An
emergency promotion never writes it; that outcome lives in the log line and the event.

Two settings shape which of the three you get:

- `maxSyncWait`, default `30s`. It bounds `WaitingForDragonflySync` **and** doubles as the timeout
  argument passed to `REPLTAKEOVER`. The client adds 5 s of I/O grace on top so it cannot give up before
  the server has spent its full drain budget — 30 s + 5 s = a 35 s client read deadline at the default.
- `onSyncTimeout`, enum `proceed;fail`, default `proceed`. Spell `proceed` out: the promotion goes ahead
  and the cache outcome is whatever it is. `fail` aborts before MySQL promotion and rolls the source
  fence back.

`orders` pins both at their defaults:

```yaml
# playground/manifests/failovergroup.yaml  (unchanged from earlier topics)
  dragonfly:
    enabled: true
    image: docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0
    # … port, memory, resources elided …
    plannedFailover:
      maxSyncWait: 30s
      onSyncTimeout: proceed
```

A clean switchover leaves `promotionMethod: REPLTAKEOVER` and `sessionsPreserved: true`. The same move
with a lagging replica under `proceed` leaves `sessionsPreserved: false` — and a MySQL promotion that
still lands at RPO 0. The cache outcome does not touch the MySQL guarantee. That is the whole design.

## Where `REPLTAKEOVER` actually comes from

Be honest about the primitive. It is real, and it is undocumented: an ADMIN-port,
`GLOBAL_TRANS` command taking a timeout in seconds, introduced in Dragonfly **v1.5.0** (2023-07-03),
with no official documentation page. The reference is the Dragonfly source
(`src/server/server_family.cc`) and the v1.5.0 release notes — "Support atomic replica takeover" — not
a manual. If you audit this path, that is where you read.

## Two pieces of version honesty

"Dragonfly v1.38.0 or later" is a **support policy, not a guardrail**. Nothing in the API types, the
controller, or the chart enforces or checks a Dragonfly version. The only CEL rules on `spec.dragonfly`
are that an image is required when Dragonfly is enabled, and that it may not be `:latest`. Run a build
older than the takeover command itself and Bloodraven will not stop you; it will fail in ways nobody has
characterised.

And the pin has drifted: the repo pins `v1.38.0` (2026-04-14) against a current stable of `v1.40.1`
(2026-08-06) — two minors. Same lesson twice. Version discipline here is yours, not the operator's.

## Where this leaves you

You can state the guarantee and its limits: best-effort cache continuity, never at the expense of MySQL,
with a shed-then-promote label sequence that has no dual-master window. You can read `sessionsPreserved`
without misreading nil, and say which path may trade sessions for availability. The application half of
failover is complete — services, DNS, taints, pools, planned moves, cache.

What you cannot yet do is see any of it from outside. Everything in this unit was checked by reading
status and logs by hand. The next unit asks what the operator exports on its own, where it goes blind,
and what you should be alerting on before an incident makes you look.
