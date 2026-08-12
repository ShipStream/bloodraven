# Connection pools that survive a promotion

Unit 3 left you standing in front of a wall. You held `iad` down, the operator promoted `pdx` in
**12.0 seconds** measured from the kill, the `DNSEndpoint` A record moved, the
`mysql-orders-primary` selector moved to the surviving site, and every status field on `orders` said
exactly what it should. And the counter application carried on serving successful reads out of the
demoted site, with no alert anywhere.

Nothing in that list was broken. The failover was correct. Your application was broken, and the
mechanism is small enough to state in three sentences.

## Three sentences

**One: a Service selector flip changes routing for new connections only.** The `-primary` Service
selects on two labels — instance and `role: primary`. When the operator relabels, kube-proxy starts
sending *new* connections to the new backend. It picked the backend for your existing flows at
connection establishment, and it does not re-evaluate them and does not reset them. Every pooled
connection your application opened before the promotion still points at the same pod it always did.

**Two: `super_read_only` blocks writes but closes no sockets.** Fencing is `SET GLOBAL
super_read_only = ON` — a variable change, not a disconnection. MySQL's own manual is blunt about
what the statement does: it *blocks* while other clients have an ongoing statement or commit, and
waits for them; it never cuts a writer off. So a surviving session keeps serving reads until the
site is next promoted or demoted. Those reads succeed, return plausible data, and are wrong by
however far the demoted site has drifted.

**Three: the operator has exactly one mitigation, and it did not run.** Step 2 of the failover
sequence calls `KillAppConnections` on the old primary:
`SELECT id FROM information_schema.processlist WHERE id != CONNECTION_ID() AND command NOT IN
('Binlog Dump', 'Binlog Dump GTID')`, then `KILL` on each id. It kills everything except itself and
the binlog dump threads.

You are already objecting: *but the operator kills app connections.* It does — and knowing its
limits precisely is the difference between an assumption and a control. It is best-effort,
single-pass, and never retried. Failures are logged as a warning and the sequence carries on. And it
runs **only if the old primary is reachable** — the whole block is guarded on the operator having a
working handle to it. In the Unit 3 scenario the site was held down, so the kill could not run at
all. In a partition it will not run either, which is the case that stings: `iad`'s mysqld is up and
answering your application, and unreachable only from the operator.

```widget
{
  "type": "sequence",
  "title": "The connection that never moves",
  "actors": [
    "counter-app",
    "mysql-orders-primary",
    "iad",
    "pdx"
  ],
  "messages": [
    {
      "from": 0,
      "to": 1,
      "label": "TCP connect — once, when the pool fills",
      "note": "The pool opens a handful of connections at startup and keeps them."
    },
    {
      "from": 1,
      "to": 2,
      "label": "kube-proxy picks a backend at establishment",
      "note": "This flow is now pinned to `iad`. kube-proxy chose once and does not re-evaluate."
    },
    {
      "from": 0,
      "to": 2,
      "label": "`SELECT value FROM counter_db.counters` — on that socket"
    },
    {
      "from": 1,
      "to": 3,
      "label": "selector `role=primary` now matches `pdx` — for NEW connections only",
      "note": "Meanwhile the operator fenced `iad` with `SET GLOBAL super_read_only = ON`, which closes no sockets, and its `KillAppConnections` step never ran because `iad` is unreachable. `pdx` was promoted 12.0 s after the kill."
    },
    {
      "from": 0,
      "to": 2,
      "label": "`SELECT ...` — same socket, still `iad`. Succeeds. Stale.",
      "note": "This is the dangerous one. It returns data and nothing anywhere reports a problem."
    },
    {
      "from": 0,
      "to": 2,
      "label": "`UPDATE ...` — ERROR 1290, the first thing that actually fails",
      "note": "A write is the only operation that surfaces the fence."
    }
  ],
  "caption": "Six of the nine steps involve a socket that was correct when it was opened and wrong ever after. Only the last one tells you."
}
```

## The project says so itself

This is not a lesson invented for the course. Issue **#123 is open** and PR **#137 is unmerged** in
v0.9.1 — a live, acknowledged gap, not history. The project's own wording is that an autonomous
sidecar self-fence has no operator-side connection drain, and that only **planned failover** actually
drains. Nothing else does.

The observability half is worse. No alert in Bloodraven's alert-to-runbook map fires for this.
`BloodravenFailoverOccurred` watches the operator's own counter, `bloodraven_failovers_total` — it
tells you a promotion happened, and its "first checks" column in `docs/docs/alert-runbook-map.mdx`
already lists **app writes** as something a human goes and looks at. Nothing is watching your pool.

## Four ecosystems, one shape

Widen the frame and the Bloodraven-specific feeling disappears.

HikariCP carries an open issue titled *"got a read-only connection from the connection pool after the
db failover"*. Pool validation queries **pass** against a demoted primary — the node is alive, it is
merely read-only — which is exactly why drivers grew `rejectReadOnly` handling, and why the first
thing that actually fails is a write, with **ERROR 1290** or **1792**, not a health check. The JVM's
default DNS cache can be infinite for the process lifetime, which is why a short TTL alone does not
save you and AWS documents forcing `networkaddress.cache.ttl` to 60 s or less. And a proxy in front
does not move existing sessions either: with ProxySQL's `fast_forward=1`, connections keep talking to
the old master and hit read-only errors.

## The fixes that are not fixes

Each of these is reached for first, and each is wrong for a specific reason.

```widget
{
  "type": "compare",
  "title": "What each change actually does to an open socket",
  "rows": [
    {
      "aspect": "What it changes",
      "cells": [
        "The number of sockets, all established the same way",
        "Behaviour after a connection has already broken",
        "How long any socket may live before it is retired"
      ]
    },
    {
      "aspect": "Does it move an established connection?",
      "cells": [
        "No — it creates more of them",
        "No — nothing broke, so nothing reconnects",
        "Yes — each is closed and reopened within the bound"
      ]
    },
    {
      "aspect": "Effect on the stale-read window",
      "cells": [
        "Widens it: more connections pinned to the demoted site, held longer",
        "None: a stale read succeeds, so the reconnect path never runs",
        "Bounds it: no connection outlives the promotion by more than the lifetime"
      ]
    }
  ],
  "columns": [
    {
      "label": "Raise the pool size"
    },
    {
      "label": "reconnect=true"
    },
    {
      "label": "Bounded connection lifetime"
    }
  ]
}
```

Shortening the DNS TTL belongs in the same bin. `spec.dns.ttl` defaults to 60 and the playground
runs it at 10, and both numbers are irrelevant to an established socket — a resolver TTL governs the
*next* lookup, and against a caching runtime it may govern nothing at all.

The real fix is three parts and none of them works alone:

1. **A bounded connection lifetime**, so no connection outlives a promotion by much.
2. **Retry on the right error class** — the read-only write errors, 1290 and 1792 — not blanket
   retry-everything, which will happily replay a statement that failed for an entirely different
   reason.
3. **A read/write split**, so writes resolve through `mysql-orders-primary` and only reads go to
   `mysql-orders-replicas`.

## The artifact

The playground's counter application already carries parts one and three, and it carries the
diagnostic that makes the failure visible:

```go
// playground/counter-app/main.go:89-90
conn.SetMaxOpenConns(5)
conn.SetConnMaxLifetime(30 * time.Second)

// playground/counter-app/main.go:179-186 — asked on the same connection the read came from
conn.QueryRow(`SELECT @@global.read_only`).Scan(&readOnly)
conn.QueryRow(`SELECT @@hostname`).Scan(&host)
```

That is why the counter's `/api/counter` response carries `readOnly` and `dbHost` beside `value`.
Hit it during a failover and you are not guessing: the read succeeds, `readOnly` is `true`, and
`dbHost` still names the demoted site. Three fields, one connection, the whole mechanism on screen.

## Choosing a strategy

`SetConnMaxLifetime` is a pool setting, not a Bloodraven feature — and so are all three of these.
Bloodraven moves labels, records and taints; the strategy is yours.

| Strategy | How the pool gets refreshed | Choose it when |
|---|---|---|
| **Taint-based** | The `shipstream.io/db-readonly-<group>` `NoExecute` taint from topic 11 evicts the non-tolerating app pod at the demoted site; the restart guarantees a fresh pool | The app is co-located with MySQL and restarting it is cheap |
| **Service-based** | The app stays up; bounded lifetime plus error-class retry carries it across | Restarts are expensive, or the app is not site-pinned |
| **Site-local warm standby** | An instance runs at every site; only the one co-located with the current primary takes writes | Cross-site write latency is the binding constraint |

## Measure your own gap

There is no wall-clock recovery number to quote here, and the course will not invent one. Detection
is 6 s, promotion lands at 12.0 s — those are Bloodraven's, and they are measured. How long *your*
writes fail afterwards depends on pool configuration, driver behaviour and DNS caching, none of which
Bloodraven controls. Any number this course handed you would be a number about somebody else's
application.

So you measure. You can now explain the stale-read mechanism from first principles, name Bloodraven's
one mitigation and its exact limits, choose between the three strategies, and state the three-part
pool fix. What you cannot yet state is your own write-gap in seconds — the interval between the last
write the counter app completed against `iad` and the first it completed against `pdx`. That is the
unit project, and it is the only number about your application that is worth anything.
