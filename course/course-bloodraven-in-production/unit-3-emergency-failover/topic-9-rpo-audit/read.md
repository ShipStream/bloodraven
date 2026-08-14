# What it cost you

`playground` is back. `pdx` took writes 12.0 seconds after you took `iad` down, and the counter
application is incrementing again. The incident review will not ask how fast that was. It will ask
how many writes you lost — and "some" is not an answer you can put in a ticket.

## The contract

Memorise this sentence, because everything else in the topic is either an amplifier or a mitigation
of it:

> An emergency failover can lose every transaction that committed on the dying primary but had not
> yet replicated to the surviving site.

Replication between sites is asynchronous. Nothing holds your commit open on `iad` until `pdx` has
acknowledged it. The window is real, it is bounded only by how far behind `pdx` happened to be, and
no setting in the CRD closes it on the emergency path.

## Guarantees versus defaults

The mitigation people reach for is durability settings, and this is where the documentation flattens
a distinction that decides whether your RPO story survives contact with a tenant. Bloodraven renders
each site's config in three passes. First a base map of sensible settings. Then your overrides —
`spec.mysqlConf`, and then a site's own `mysqlConf`, which beats the group's. Then a short block of
operator-owned invariants, written **after** your overrides, which simply overwrite whatever you put
there. Finally `skip-log-bin` and `disable-log-bin` are deleted outright, because MySQL honours the
last occurrence and a sorted render would place either alias after `log-bin`.

That ordering is the whole test. **Written after the overrides, it is a guarantee. Written before
them, it is a default.**

```widget
{
  "type": "compare",
  "title": "Two layers of the same file",
  "rows": [
    {
      "aspect": "Which settings?",
      "cells": [
        "sync-binlog=1, innodb-flush-log-at-trx-commit=2, binlog-expire-logs-seconds=1209600 (14 days)",
        "gtid-mode=ON, enforce-gtid-consistency=ON, log-replica-updates=ON, log-bin, skip-replica-start=ON, plugin-load-add=mysql_clone.so"
      ]
    },
    {
      "aspect": "Can spec.mysqlConf beat it?",
      "cells": [
        "Yes — silently, with no warning and no admission rejection",
        "No — the invariant block overwrites your key after your override is applied"
      ]
    },
    {
      "aspect": "What does losing it cost?",
      "cells": [
        "The durability and retention story the RPO documentation tells",
        "GTID identity, binlog continuity, and the ability to clone or replicate at all"
      ]
    },
    {
      "aspect": "Where do you check?",
      "cells": [
        "The rendered per-site ConfigMap, not the layer diagram",
        "The same place — the invariant should be present regardless of what you set"
      ]
    }
  ],
  "columns": [
    {
      "label": "Overridable defaults (written first)"
    },
    {
      "label": "Un-weakenable invariants (written last)"
    }
  ]
}
```

So: a tenant who sets `sync_binlog=0` in `spec.mysqlConf` gets `sync_binlog=0`, and your written RPO
promise quietly becomes fiction. A tenant who sets `gtid_mode=OFF` gets `gtid_mode=ON` anyway. Do not
trust the diagram — read the file `playground` actually rendered:

```console
$ kubectl -n bloodraven-playground get configmap mysql-playground-iad-config \
    -o jsonpath='{.data.bloodraven\.cnf}' | grep -E 'sync-binlog|gtid-mode|flush-log'
gtid-mode=ON
innodb-flush-log-at-trx-commit=2
sync-binlog=1
```

Sharpen the middle line, because the MySQL manual is harsher than Bloodraven's docs. With
`innodb_flush_log_at_trx_commit=2`, logs are written at commit but flushed once per second, and the
manual attributes the loss to **any unexpected mysqld process exit** — not just power loss — saying
plainly that it "can erase up to N seconds of transactions". Its own recommendation is
`innodb_flush_log_at_trx_commit=1` alongside `sync_binlog=1`, the setting it calls the safest, which
guarantees no transaction is lost from the binary log. Bloodraven ships `2` for throughput. That is a
defensible trade, and it is one `spec.mysqlConf` line from the stricter one. Just know the price: up
to a second of committed transactions, on the site that has just crashed, on top of the replication
window.

## The arithmetic

Now count what you actually lost. At step 6 of the failover sequence the operator runs
`SELECT @@global.gtid_executed` on the candidate — before it accepts a single write — and records the
answer in `status.promotionGtidExecuted`. That is the high-water mark of everything `pdx` had
received. When the old primary comes back and is compared against it, the operator subtracts one set
from the other and publishes the difference as `status.sites[].divergentGtid`, with its cardinality on
the `bloodraven_divergent_transactions` gauge.

Two MySQL functions let you do that arithmetic by hand on any pair of sets. `GTID_SUBSET(set1, set2)`
returns true when every GTID in `set1` is also in `set2` — that is the "did it catch up" question.
`GTID_SUBTRACT(set1, set2)` returns only those GTIDs from `set1` that are not in `set2`. The
subtraction is the divergence primitive; its cardinality is your lost-transaction count.

One wrinkle before you eyeball a set. MySQL 9.x GTID sets can carry user-defined tags, written
`uuid:tag:interval`, and a tag is treated as part of the UUID's identity. `uuid:Domain_1:1-3` and
`uuid:Domain_2:1-3` are six different transactions, not three. Let MySQL do the subtraction.

## The measurement

```figure
{
  "src": "assets/img/g5-gtid-loss.svg",
  "alt": "Two GTID bars. IAD holds transactions 1 through 23. PDX holds 1 through 19. The range 20-23 is highlighted amber and labelled 4 lost transactions.",
  "caption": "What the failover cost. `20-23` is four transactions, not an estimate.",
  "width": 960,
  "height": 320
}
```

Here is a real `playground` status after an emergency promotion to `pdx`, with the two fields that matter
and everything else elided:

```yaml
status:
  activeSite: pdx                                     # was iad
  lastFailoverTarget: pdx
  promotionGtidExecuted: |-                           # new since topic 1: pdx's set at promotion
    a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:1-19,
    a3c3f9e8-5f9d-11f1-bf37-568bfb8d0365:1-7
  sites:
  - name: iad
    divergentGtid: a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:20-23
    divergentTransactionCount: 4
```

Read it off. `a2cc879c…` is `iad`'s own server UUID — the transactions it originated as primary. `pdx`
had `1-19` of them at the moment of promotion. The difference is `20-23`, so `iad` had run to `1-23`
and four transactions committed on `iad` never reached `pdx`. The count is `23 − 20 + 1 = 4`, which is
exactly what the gauge reports (`bloodraven_divergent_transactions{site="iad"} 4`) and what the status
condition says in words: `Old primary iad has 4 divergent transactions`. Four counter increments. Not
an estimate.

```widget
{
  "type": "terminal",
  "title": "Doing the subtraction yourself",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{.status.sites[?(@.name==\"iad\")].divergentGtid}'",
      "out": "a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:20-23"
    },
    {
      "cmd": "mysql -N -e \"SELECT GTID_SUBTRACT('a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:1-23', 'a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:1-19,a3c3f9e8-5f9d-11f1-bf37-568bfb8d0365:1-7')\"",
      "out": "a2cc879c-5f9d-11f1-9fae-8e47bc2a4544:20-23"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

## The distractor: `maxLagSeconds`

`spec.replication.maxLagSeconds` defaults to 300, and the playground manifest for `playground` sets 30. It
drives exactly one thing: a `ReplicationLagging` reason on the `Degraded` condition when a site's
reported lag exceeds it. It is **not** a promotion gate. Nothing in candidate selection consults it.
If `iad` dies while `pdx` is 400 seconds behind, Bloodraven promotes `pdx` anyway — because no writable
site at all is almost always worse. If you believe `maxLagSeconds` bounds your RPO, your RPO is
whatever the lag happened to be at the moment of the crash. What does bound it is a true GTID-superset
test, which is why a planned switchover is RPO 0 by construction; that path is Unit 4.

## Pick your row

| Failure mode | RPO | Why |
| --- | --- | --- |
| Container/pod crash, PVC intact | 0 — no failover at all | The primary returns writable and the operator keeps it |
| Clean primary kill, replica caught up | Near zero, not guaranteed zero | The contract still applies to anything in flight |
| Primary kill with unapplied relay logs | Whatever was in flight | The 30 s drain applies what it can reach; the rest is gone |
| PVC destroyed with the primary | Worst row | The previously-active binlog lived on the destroyed PVC — PITR cannot replay a tail that was never shipped |

Given an outage, find the row before you quote a number.

You can now state the RPO contract, tell a durability guarantee from a durability default by reading a
rendered ConfigMap, and produce an exact lost-transaction count for the failover you just ran. Which
leaves the four transactions sitting on `iad`. `iad` is back, it is read-only, and it is holding
writes `pdx` has never seen. What happens when it tries to rejoin is the next topic.
