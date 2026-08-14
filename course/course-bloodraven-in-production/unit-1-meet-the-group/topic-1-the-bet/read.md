# The bet Bloodraven makes

You have a live group. `iad` is taking writes. `pdx` and `reader` are copying them. The
counter increments. The question this topic answers is not how any of that is wired. It is
what Bloodraven is willing to lose when `iad` goes away.

## The deployment it is built for

Bloodraven targets one shape and is explicit about it: **two or more MySQL sites, ordinary
asynchronous replication, exactly one site writable at a time, and a non-zero RPO accepted on
unplanned loss.** `spec.sites` takes between 2 and 16 entries. `playground` uses three. That
is the whole design point.

*Asynchronous* means a write commits on the primary and is acknowledged to your application
before any other site has seen it. Nothing waits for a peer. That buys you the write-latency
of a standalone MySQL. It costs you exactly this:

> An emergency failover can lose every transaction that committed on the dying primary but had
> not yet replicated to the surviving site.

**RPO** — recovery point objective — is how much recently committed data you accept losing.
Bloodraven's RPO on sudden primary loss is not zero.

**RTO** — recovery time objective — is how long you accept being unable to write. Bloodraven
spends its whole engineering budget there: it promotes a survivor without waking anyone.

This is a choice, not an unfinished feature. Two sites is a legitimate topology for Bloodraven.
For a quorum system it is a pathological one — lose one of two members and the survivor goes
read-only, so you invent a witness node and inherit its problems.

## The six things it refuses to do

Bloodraven publishes its own non-goals. Learn them before you write a manifest:

- It does not provide synchronous replication or zero RPO after sudden primary loss.
- It does not replace external-dns, cert-manager, Prometheus, Grafana, or your object store.
- It does not make application connection pools failover-aware automatically.
- It does not reconcile divergent writes for you after split-brain.
- It does not make PVC-local backups durable after cluster or storage loss.
- It does not treat Dragonfly as durable application storage; managed Dragonfly is for
  cache/session continuity.

Two of those bite hardest. The connection-pool one, because a pool will hand your application
a connection to a demoted primary: the node is alive, the health check passes, and only the
next `INSERT` fails. And the PVC one, because when a PVC is destroyed the active binlog that
lived on it is gone forever.

## Against quorum

```widget
{
  "type": "compare",
  "title": "Two defensible bets",
  "rows": [
    {
      "aspect": "A site is lost. What happens to writes?",
      "cells": [
        "The surviving site is promoted and takes writes. Two sites is enough to tolerate one loss.",
        "Only a majority side stays writable. Lose one of two members and the survivor is read-only."
      ]
    },
    {
      "aspect": "What can be lost?",
      "cells": [
        "Every transaction that committed on the dying primary but had not yet replicated to the surviving site.",
        "Nothing on the majority side — the cost is paid instead on every single commit, as a cross-node round trip."
      ]
    },
    {
      "aspect": "Who decides?",
      "cells": [
        "An external operator arbitrates, and a sidecar on each MySQL pod fences itself when it cannot confirm it is still authoritative.",
        "The group decides for itself by majority vote. No external decider, and no answer at all without a quorum."
      ]
    }
  ],
  "columns": [
    {
      "label": "Bloodraven — async + external promotion"
    },
    {
      "label": "Synchronous / quorum — e.g. Group Replication"
    }
  ]
}
```

Group Replication is not the loser of that table. It is the other end of it. A quorum system
trades write availability for consistency: it refuses writes rather than accept ones it might
have to throw away. Bloodraven trades the other way — write availability and a mental model
one person can hold at 3am.

If "we lost a second of writes" is a customer-visible failure, run Group Replication. If
losing the ability to write while a cross-site link misbehaves is the worse outcome, run this.

## The starting state

Nothing has changed since you stood `playground` up. This is the shape, excerpted:

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlFailoverGroup
metadata:
  name: playground
spec:
  image: mysql:9.7            # the one MySQL baseline Bloodraven supports — pin it, never mysql:9
  sites:                      # MinItems 2, MaxItems 16
    - name: iad
      role: primary-candidate
    - name: pdx
      role: primary-candidate
    - name: reader
      role: read-only
  dns:
    hostname: playground-db.example.local
  # ... splitBrainPolicy, replication, sidecar, storage elided
```

Those three `role:` values are doing the work you named in the last topic.

## What the bet buys

Against the old posture — page a human, promote by hand, guess the RPO — the bet pays out
three ways.

Unplanned promotion happens with nobody logged in.

A *planned* switchover is RPO 0 **by construction**: the source is fenced, its GTID position
snapshotted, and the target promoted only once its own executed set covers that snapshot.

And when transactions really are lost you get a number, not a shrug: the operator computes
the set difference, publishes the count, and records the set in `status.sites[].divergentGtid`.

All of it is exercised by 51 registered chaos scenarios against real clusters.

## Where this leaves you

You can now say what deployment Bloodraven targets, and what it will refuse to do for you.
You have a running group whose parts you can name, and a contract for what an emergency
failover is allowed to lose.

Then a site stops answering. The fields you learned start moving — `state` to `unreachable`,
the reason off `Healthy`. The operator sees exactly what you see. What it decides to do
about that, and how long it waits first, is the next unit.
