# When the operator is down

`orders` is healthy on the three-site playground: `iad` writable, `pdx` read-only and replicating, the reader tracking behind them, the counter app writing continuously through `mysql-orders-primary`. Scale the operator to zero and watch what breaks. The lesson is that nothing does.

```widget
{
  "type": "terminal",
  "title": "Operator at zero replicas",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground scale deploy/bloodraven --replicas=0",
      "out": "deployment.apps/bloodraven scaled"
    },
    {
      "cmd": "curl -s localhost:8090/api/counter",
      "out": "{\"value\":<n>,\"dbHost\":\"mysql-orders-iad-…\",\"readOnly\":false}"
    },
    {
      "cmd": "# sixty seconds later, operator still at 0\ncurl -s localhost:8090/api/counter",
      "out": "{\"value\":<n+k, k>0>,\"dbHost\":\"mysql-orders-iad-…\",\"readOnly\":false}   # still climbing"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mfg orders -o jsonpath='{.status.sites[*].lastSeen}'",
      "out": "<the last poll before the scale-down — identical sixty seconds later>"
    }
  ]
}
```

The counter keeps incrementing, `readOnly` stays `false`, and the only thing that changes is the custom resource: `status.sites[].lastSeen` freezes at the final poll. That is the whole shape of operator downtime. A healthy primary and replica keep serving reads and writes with zero operator involvement — the operator sits on the failure-detection and promotion path, not the request path.

## Availability and correctness fail separately

**Correctness** — no split brain, no silent divergence — is preserved by the sidecar fencing layer regardless of how long the operator is gone. The sidecars are separate processes with their own timers, and a dead operator is exactly what their lease rule exists for. A MySQL pod that restarts mid-outage comes up fenced and stays fenced: the startup safety net cannot get an authoritative answer and refuses to guess.

**Availability** is not. If the primary dies while the operator is down, nothing promotes the replica, and `orders` has no writable site until the operator returns.

| Behaviour | At zero replicas |
| --- | --- |
| Reads and writes on a healthy primary | Unaffected — not the request path |
| Replication to `pdx` and the reader | Unaffected — MySQL to MySQL |
| Sidecar self-fencing | Unaffected — separate processes, own timers |
| Failure detection and promotion | Stopped — nothing is authorised to promote |
| Service selector, `DNSEndpoint`, taints | Stopped — all three are operator writes |
| `status.*` on the CR | Frozen, and stale status is not a symptom of anything else |

Objective 11 is the line worth memorising: **operator downtime costs write availability, never RPO.** RPO is fixed by what had replicated at the instant the primary died — an emergency failover can lose every transaction that committed on the dying primary but had not yet reached the survivor, and that set is sealed at the moment of death. An operator that shows up two hours later promotes the same replica across the same GTID gap and loses the same transactions. The outage is longer; the data loss is not larger.

## One replica, leader election, and what it does not buy

The chart ships `replicaCount: 1` with leader election enabled, and engineers reach for `replicaCount: 3` the first time they read the paragraph above. Be precise about what that buys.

Leader election means extra replicas are *standbys, not parallel workers*. One replica holds the lease and is the sole writer of status, DNS, and promotion commands; the others sit idle. Adding them shortens the gap between a crashed leader and a new one taking over. It cannot shorten the poll loop — the detection bottleneck you measured in Unit 2: `pollInterval × failureThreshold`, 2 s × 3 = 6 s on defaults, before promotion even begins. Three replicas do not make failover faster; they make an operator *crash* cheaper.

## The cooldown cuts both ways

The anti-flap cooldown (`spec.failoverCooldown`, default `5m`, `30s` in the playground) fails in two opposite directions.

The rarer one is that the cooldown *is not there when it should be*. It is persisted twice on every promotion — in `status` and in out-of-band annotations — so losing it across a restart takes both paths rejecting writes at once. Then a restarted operator can promote earlier than you configured. The deterministic simulator names this `CooldownViolated(restart+stateLost)` and classifies it as an *inherent* finding class, not a bug queued for a fix: understood, bounded by the GTID gates, accepted.

The direction you meet on call is the opposite. The cooldown **blocks a second failover you genuinely need**, and it does not care that you believe this one is justified. Your judgement is not an input.

## Wait, or promote by hand

Objective 12 turns on one fact about the break-glass tool. `kubectl bloodraven promote orders pdx` writes the `bloodraven.shipstream.io/planned-failover` annotation and returns. The plugin only writes resources the operator already reads; it never talks to MySQL. It is not a back door around the operator's logic — it is a request *to* that logic, and it needs a live operator to execute it. Inside the cooldown it hits the same gate: `spec.plannedFailover.onCooldown` defaults to `reject`, so the request fails outright unless you set `defer`, which parks it until cooldown expiry.

The rule that falls out:

- **Operator returning inside the cooldown, no writable site?** Wait. It promotes within roughly 6 s of detection plus the drain once it is back, and the hand-driven path cannot start sooner.
- **Operator not coming back — crashlooping image, broken RBAC, deleted namespace?** Fix the operator first. That *is* the promotion path; annotating a group nobody is watching only queues an intent.
- **Editing MySQL directly?** Know what you are taking on. Clear `read_only` on `pdx` while the operator is down and the sidecar's topology-mismatch rule sees a writable site disagreeing with the last `activeSite` it cached — and fences it straight back.

## The error that already happened

A `SET GLOBAL` that returns an error may still have landed. Cancelling the context tears down the *client* connection; it does not roll back a write the server already applied. Bloodraven shipped a bug from exactly this: treating the returned error as a failure made the monitor re-fence a site it had just promoted.

Generalise it properly, because it is not a MySQL fact. A timeout or a cancellation tells you that **you stopped waiting**, not that the remote side did not act. Every retry, every rollback, and every "that failed, so I will try the other one" inherits that ambiguity. The only honest response to a cancelled mutation is to re-read the state.

And the wider echo: control plane and data plane fail separately in practice, not only in design documents. Cloudflare's November 2023 incident kept the data plane serving for roughly two days of control-plane outage — your counter app climbing against a scaled-to-zero operator is the same phenomenon at playground scale.

## Where this leaves you

You can take an operator outage deliberately now, and say which guarantees went with it. The cooldown protects you in one direction and traps you in the other; break-glass promotion is a request to the operator, not a substitute for it; a failed write is an unknown, not a rollback. What none of it tells you is what to do when the data itself is gone — when the question stops being "who should be primary" and becomes "which bytes are still recoverable". That is the next unit.
