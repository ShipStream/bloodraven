# The bet Bloodraven makes

`playground` is a `MysqlFailoverGroup` — the name the shipped manifest gives it, and the name every
command in this course uses with no edit. Read it as the group you will actually run: three sites,
`iad` and `pdx` plus a third called `reader`, holding the MySQL behind a warehouse system. A counter
application is pointed at it through the `mysql-playground-primary` Service; its page reads the
counter every two seconds and its one button writes. One of those sites will go away — a node, a
rack, a region, a drain run at the wrong hour. The question is not whether, but what `playground`
does in the ninety seconds afterwards, and how many committed transactions you can still account for
once it is over.

## The deployment Bloodraven is built for

Bloodraven targets one shape and is explicit about it: **two or more MySQL sites, ordinary
asynchronous replication, exactly one site writable at a time, and a non-zero RPO accepted on
unplanned loss.** `spec.sites` takes between 2 and 16 entries; `playground` uses three. That is the
whole design point.

*Asynchronous* means a write commits on the primary and is acknowledged to your application before
any other site has seen it. Nothing waits for a peer. That buys you the write-latency profile of a
standalone MySQL and it costs you exactly this, which is the contract in one line:

> An emergency failover can lose every transaction that committed on the dying primary but had not
> yet replicated to the surviving site.

Two terms you will use for the rest of the course. **RPO** — recovery point objective — is how much
recently committed data you accept losing. Bloodraven's RPO on sudden primary loss is not zero.
**RTO** — recovery time objective — is how long you accept being unable to write. Bloodraven spends
its whole engineering budget there: it promotes a survivor without waking anyone.

This is a choice, not an unfinished feature. Two sites is a legitimate topology for Bloodraven. For
a quorum system it is a pathological one — lose one of two members and the survivor goes read-only,
so you invent a witness node and inherit its problems.

## The six things it refuses to do

Bloodraven publishes its own non-goals. Learn them before you write a manifest:

- It does not provide synchronous replication or zero RPO after sudden primary loss.
- It does not replace external-dns, cert-manager, Prometheus, Grafana, or your object store.
- It does not make application connection pools failover-aware automatically.
- It does not reconcile divergent writes for you after split-brain.
- It does not make PVC-local backups durable after cluster or storage loss.
- It does not treat Dragonfly as durable application storage; managed Dragonfly is for
  cache/session continuity.

Two of those bite hardest. The connection-pool one, because a pool will hand your application a
connection to a demoted primary: the node is alive, the validation query passes, and only the next
`INSERT` fails — a live upstream complaint against HikariCP, and the reason drivers grew explicit
`rejectReadOnly` handling. And the PVC one, because when a PVC is destroyed the active binlog that
lived on it is gone forever, so point-in-time recovery has nothing to replay from.

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

Group Replication is not the loser of that table. It is the other end of it. A quorum system trades
write availability for consistency: it refuses writes rather than accept ones it might have to throw
away. Bloodraven trades consistency-on-unplanned-loss for write availability and a mental model one
person can hold at 03:00 — one primary, everyone else replicating from it, no view change to reason
about. If "we lost a second of writes" is a customer-visible failure, run Group Replication. If
losing the ability to write while a cross-site link misbehaves is the worse outcome, run this.

## The starting state

Nothing has changed yet; this is `playground` as it ships, excerpted:

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

Those three `role:` values are doing real work. What each one is allowed to become is a later topic.

## What the bet buys

Against the old posture — page a human, promote by hand, guess the RPO — the bet pays out three
ways. Unplanned promotion happens with nobody logged in. A *planned* switchover is RPO 0 **by
construction**: the source is fenced, its GTID position snapshotted, and the target promoted only
once its own executed set covers that snapshot. And when transactions really are lost you get a
number, not a shrug: the operator computes the set difference between the old primary's GTIDs and
the new one's, publishes the count as `bloodraven_divergent_transactions`, and records the set in
`status.sites[].divergentGtid`. All of it is exercised by 49 registered chaos scenarios against real
clusters.

## Where this leaves you

You can now say what deployment Bloodraven targets, and what it will refuse to do for you. You can
also name a failover group's four parts, without yet knowing what any of them do:

```widget
{
  "type": "tree",
  "title": "What a failover group is made of",
  "root": {
    "name": "MysqlFailoverGroup playground",
    "children": [
      {
        "name": "sites — iad, pdx, reader (each a MySQL pod plus a sidecar container)"
      },
      {
        "name": "the operator — one Deployment, watching the CR"
      },
      {
        "name": "Services — mysql-playground-primary, mysql-playground-replicas, and one per site"
      },
      {
        "name": "a DNS record — a DNSEndpoint object named bloodraven-playground"
      }
    ]
  }
}
```

Next: which of those four does the work, and which one is only there for the moment the operator
cannot be reached?
