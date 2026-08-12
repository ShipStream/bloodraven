# The nine steps of a promotion

`orders` is healthy. `iad` is writable, `pdx` and `reader` are read-only, and the counter application
is committing through `mysql-orders-primary` without pausing. You can already predict which matrix row
fires and which site becomes the candidate. Stop predicting and pull the plug.

```bash
kubectl -n bloodraven-playground scale deployment mysql-orders-iad --replicas=0
kubectl -n bloodraven-playground get mysqlfailovergroup orders \
  -o jsonpath='{.status.activeSite}{"\n"}'
```

Watch `status.activeSite` go from `iad` to `pdx`. Keep the counter running — the interesting part of
this unit is what the application experiences.

## Getting the injection right

A widespread superstition says a pod delete does not trigger failover. It does. Scenario 09b
force-deletes the primary with `--grace-period=0 --force`, hard-waits for `activeSite` to flip and
`lastFailover` to stamp before it even reaches its real assertion, and passes. The sub-five-second
Deployment respawn does not save the primary because the debounce never watches pod objects: it watches
whether mysqld answers `CheckReadOnly`, and a cold container start plus InnoDB recovery comfortably
exceeds the six-second detection window.

Scenario 01 uses scale-to-0 for **determinism**, not because delete fails: a pod-delete races the
respawn, and that race can restore the original topology through split-brain recovery instead of
completing the failover. The genuine no-failover cases are different in kind:

| Injection | What survives | Does it fail over? |
| --- | --- | --- |
| `scale deployment --replicas=0` | nothing | Yes, and the site stays down |
| `delete pod --grace-period=0 --force` | PVC, Service, node | Yes, but it races the ~5 s respawn |
| SQL `SHUTDOWN`, restart in place (scenario 16) | pod, PVC, IP | Maybe — 16 accepts either outcome, depending on the kubelet's restart speed against the ~6 s window |
| Pod crash, PVC intact | data | No — RPO 0; the primary returns writable and is kept |

## The sequence the code actually runs

The documentation's ten-step list does not match `internal/controller/failover.go`: it omits a step,
misstates fatality, and counts two of the promotion's poll neighbours as links in the chain. This
course teaches the code. There are nine steps, and what matters about each is whether an error aborts
the failover or merely produces a log line.

```widget
{
  "type": "order",
  "title": "Order the nine steps of Execute()",
  "items": [
    "Fence the old primary — SET GLOBAL super_read_only = ON — Warns only. The old primary is usually unreachable — that is the whole reason you are here.",
    "Kill application connections on the old primary — Warns only. Undocumented. SELECT id FROM information_schema.processlist WHERE id != CONNECTION_ID() AND command NOT IN ('Binlog Dump', 'Binlog Dump GTID'), then KILL per row.",
    "Relay-log drain on the candidate, 30 s budget — Non-fatal. On timeout the operator logs a warning and promotes anyway.",
    "STOP REPLICA — Fatal. Execute returns the error and no promotion happens.",
    "RESET REPLICA ALL — Fatal. Returns immediately on error.",
    "Record the promotion GTID — SELECT @@global.gtid_executed — Non-fatal. A warning only; promotion continues without the record.",
    "SET GLOBAL super_read_only = OFF — Fatal.",
    "SET GLOBAL read_only = OFF — Fatal.",
    "Writable confirmation — Synchronous, in the same call stack. On failure the operator logs that promotion succeeded but DNS was not flipped, and returns."
  ]
}
```

Read the fatality column as a design statement. Fencing failure does **not** block promotion; neither
does a failed connection kill, an exhausted drain, or a GTID read that errors. The only statements the
operator refuses to proceed past are the four that change the candidate's own replication and
read-only state. Note too what the kill query's `WHERE` clause protects: `Binlog Dump` threads are
excluded, so killing application sessions does not tear down replication.

## Promotion is two statements, not one

If you learned `read_only=0` as *the* promotion command, you have a wedged-primary gap.
`super_read_only` is the actual barrier: it prohibits updates even from `CONNECTION_ADMIN` or `SUPER`,
which plain `read_only` does not, and it is the variable the sidecar fences with. The operator clears
it first and `read_only` second, and a failure of either is fatal. MySQL couples the two — setting
`read_only=OFF` implicitly forces `super_read_only=OFF` — so the point of knowing both is not the
typing. It is that a site reporting `super_read_only=ON` has been *fenced*, and reading that as "just a
replica" is how people go looking for a wedged primary in the wrong place.

Then the ordering fact that shows the design's paranoia. Once promotion and writable confirmation both
succeed, the operator stamps the durable failover record and increments the failover counter **before**
it touches DNS, so a DNS-provider outage cannot erase the fact that a promotion happened. A failed DNS
flip is logged and heals on the next poll; a forgotten promotion would not.

```widget
{
  "type": "terminal",
  "title": "The completion line",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground logs -l app.kubernetes.io/name=bloodraven | grep 'failover complete'",
      "out": "{\"time\":\"2026-04-30T20:55:52.912585929Z\",\"level\":\"INFO\",\"msg\":\"failover complete\",\"fg\":\"bloodraven-playground/orders\",\"promotedSite\":\"pdx\",\"promotionGtid\":\"0e29fbce-44d6-11f1-b93f-2e1a52f79466:1-9\"}"
    }
  ]
}
```

## Two things that are not steps

**Node taints are not part of this sequence.** They are a pure function of per-site state transitions,
applied earlier in the same poll by a different code path — writable-to-anything-else adds the taint,
anything-else-to-writable removes it, read-only ↔ unreachable does nothing. A taint appears around a
failover because the same transition triggers both.

**Source convergence is not part of it either.** It is an independent poll stage with its own 20 s
budget, repointing replicas at whichever site is now authoritative. The docs present both as ordered
steps. They are neighbours in the poll, not links in the chain.

## Inside the 30-second drain

The drain lets the candidate apply the relay logs it already fetched before it takes writes. It exits
early the moment the SQL thread is running and `Seconds_Behind_Source` reads 0. Internally it waits
500 ms, doubling to a 4 s ceiling; if the SQL thread is stopped with unapplied relay logs it restarts
the thread once and retries. A timed-out drain logs `relay log drain did not complete cleanly,
proceeding with promotion` and promotes with whatever the candidate had applied.

## Twelve seconds, and thirty-six

A clean primary kill flips `activeSite` in **12.0 s**, reproducible across nine-plus independent runs
(12.004 s to 12.02 s, one outlier at 13.008 s). Scenario 14 pauses the replica's SQL applier, seeds
five seconds of writes, kills the primary, and measures **36.005 s**.

The difference is entirely the drain. Both runs pay the same 6 s of detection — `pollInterval` 2 s ×
`failureThreshold` 3. In the 12 s case the candidate is already caught up and the drain returns at once
on its early-exit condition. In the 36 s case it has relay logs to apply and spends its whole budget:
36.005 s − 12.0 s = 24.0 s of extra wall clock. Learn 12 s typical and roughly 37 s worst case (6 s
detect + 30 s drain), and be able to say which you are looking at; the documentation's 30–45 s figure
is unsourced and contradicted by the recorded runs. One caveat before you trust your own stopwatch: the
playground overrides `failoverCooldown` to 30 s against a shipped default of 5 m. The timings here are
real. The cooldown is not.

> **You did everything right, and your application never noticed. That is the problem.**
>
> `iad` is scaled to zero and staying down. The operator promoted `pdx` in about twelve seconds, the
> DNS record flipped, the `-primary` Service selector moved. And the counter application kept working
> — no errors, no alerts, reads succeeding.
>
> Then you check which site it is reading from. It is `iad`. The site you just demoted.
>
> This is not a mistake you made and it is not a misconfiguration. `super_read_only` blocks writes but
> closes no sockets, so a session that was already open keeps right on serving stale reads. The
> operator's one mitigation, `KillAppConnections`, runs only when the old primary is reachable and is
> never retried — and you have held the site down, so it cannot run at all. Issue #123 is open and PR
> #137 is unmerged: a live, acknowledged gap. Nothing in the alert map fires for it.
>
> The mental model to carry out of here: the operator's job ends at a label selector and a DNS record,
> and a socket that was already open is outside its reach. Unit 4 is where you close it. Not yet.

## Where this leaves you

You have driven a real emergency failover on `orders`, you can recite the nine steps with fatality per
step, and you can look at a 36 s run and name the 24 s the drain spent. You have also seen something
that should bother you. Before you fix it you need the other half of the story: not how long the
failover took, but which transactions did not make it. That count is in the group's status right now,
and the next topic reads it.
