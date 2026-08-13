# Connection pools that survive a promotion

Unit 3 left you standing in front of a wall. You held `iad` down, the operator promoted `pdx` in
**12.0 seconds** measured from the kill, the `DNSEndpoint` A record moved, the
`mysql-playground-primary` selector moved to the surviving site, and every status field on `playground` said
exactly what it should. And the counter application went on serving successful, stale reads out of the
demoted site — while the first write anyone attempted came back `ERROR 1290` — with nothing anywhere
paging.

Nothing in that list was broken. The failover was correct. Your application was broken, in two
different ways at once, and the mechanism behind both is small enough to state in three sentences.

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

**Three: the operator's mitigations all need a reachable old primary, and yours was not.** Step 2 of
the failover sequence calls `KillAppConnections`:
`SELECT id FROM information_schema.processlist WHERE id != CONNECTION_ID() AND command NOT IN
('Binlog Dump', 'Binlog Dump GTID')`, then `KILL` on each id. It kills everything except itself and
the binlog dump threads. After promotion, the operator keeps going: each topology poll makes one more
bounded eviction pass against the fenced former primary until a pass finds no sessions or
`spec.connectionDrainTimeout` (default `30s`) expires.

You are already objecting: *but the operator kills app connections.* It does — and knowing the limit
precisely is the difference between an assumption and a control. The limit is not the retry count. It
is the word *reachable*: every one of those passes is a SQL statement, issued over a connection to the
site being drained. In the Unit 3 scenario `iad` was scaled to zero, so there was nothing to connect
to and every pass was a no-op. The case that really stings is the partition, where `iad`'s mysqld is up
and answering your application perfectly well, and unreachable only from the operator.

Set `spec.connectionDrainTimeout` against your own pool, not against a feeling. It bounds how long the
operator keeps trying; your pool's maximum connection lifetime bounds how long a survivor can last. The
larger of the two is your real stale-read window.
```widget
{
  "type": "sequence",
  "title": "The connection that never moves",
  "actors": [
    "counter-app",
    "mysql-playground-primary",
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
  "caption": "Every message after the promotion travels a socket that was correct when it was opened and wrong ever after. Only the write says so."
}
```

## This is a known gap, and it is only half closed

None of this is a lesson invented for the course. The project has tracked it as a defect, narrowed it,
and left the part you just met open — the dated record is in the
[version appendix](../sources.html#version-appendix), row A1, which is where to look before you quote
a version.

What is settled is the shape. There are exactly three connection-drain behaviours, and only one of
them is unconditional:

| Path | Drains connections? |
|---|---|
| **Planned failover** | Yes. Repeatedly, inside `drainTimeout`, before the write endpoint moves. It is the only path that drains *ahead* of the switch. |
| **Emergency failover** | Best-effort during the sequence, then retried per poll inside `spec.connectionDrainTimeout` — but only while the fenced site answers. |
| **Autonomous sidecar self-fence** | The sidecar kills what it can and does not retry, because it cannot safely tell an application session from the operator's own. |

The observability half is worse, and no release has changed it. No shipped alert fires for "the
application is still broken after a successful failover." `BloodravenFailoverOccurred` watches the
operator's own counter, `bloodraven_failovers_total` — it tells you a promotion happened, and its
"first checks" column already lists **app writes** as something a human goes and looks at. Nothing is
watching your pool. That alert is yours to write, and Unit 6 makes you write it.

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
3. **A read/write split**, so writes resolve through `mysql-playground-primary` and only reads go to
   `mysql-playground-replicas`.

## The artifact

The playground's counter application already carries parts one and three, and it carries the
diagnostic that makes the failure visible:

```go
// connectLoop, in playground/counter-app/main.go — part one of the fix, already applied
conn.SetMaxOpenConns(5)
conn.SetConnMaxLifetime(30 * time.Second)

// handleCounter — asked on the same connection the read came from
conn.QueryRow(`SELECT @@global.read_only`).Scan(&readOnly)
conn.QueryRow(`SELECT @@hostname`).Scan(&host)
```

That is why the counter's `/api/counter` response carries `readOnly` and `dbHost` beside `value`.
Hit it inside thirty seconds of a failover and you are not guessing: the read succeeds, `readOnly` is
`true`, and `dbHost` still names the demoted site. Three fields, one connection, the whole mechanism
on screen. Wait longer than thirty seconds and it has healed itself — because `SetConnMaxLifetime(30s)`
is doing exactly what part one of the fix is supposed to do. The bug is easiest to see in an
application that has already been half-fixed.

## Choosing a strategy

`SetConnMaxLifetime` is a pool setting, not a Bloodraven feature — and so are all three of these.
Bloodraven moves labels, records and taints; the strategy is yours.

| Strategy | How the pool gets refreshed | Choose it when |
|---|---|---|
| **Taint-based** | The `shipstream.io/db-readonly-<group>` `NoExecute` taint from the previous topic evicts the non-tolerating app pod at the demoted site; the restart guarantees a fresh pool | The app is co-located with MySQL and restarting it is cheap |
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
write *your writer* completed against `iad` and the first it completed against `pdx`. That is the
unit project, and it is the only number about your application that is worth anything.
