# Reading the operator's mind from logs and metrics

`orders` has three sites: `iad` and `pdx` as `primary-candidate`, `reader` as `role: read-only`. The counter application is still writing. You can now run the cross-site table in your head and say whether cooldown will let the operator act on it. What you cannot do yet is check your answer against the live group. Two surfaces make that possible, and neither is a dashboard: the structured log, and the operator's `/metrics` endpoint on `:8080`.

## The log is an interface

Bloodraven's operational log is not debug output you grep in a panic. It is a published contract. The `msg` strings and the field names beside them are versioned in `docs/docs/log-schema.mdx`, and the stability table there is explicit: `msg` strings in the Event reference "will not change without a deprecation note in `CHANGELOG.md`", and fields listed alongside a stable `msg` may gain siblings but will never be renamed or removed silently. Downstream pipelines filter on those exact strings — `msg = "initiating failover"` is a Loki alert, not a regex guess. Changing one is a breaking change for every consumer you cannot see.

Two things follow. First, there are two JSON streams on stdout and only one is contractual: the operational `slog` stream carries `time`, `level`, `msg`, and you split it from the controller-runtime `zap` stream with `$.time && $.msg` versus `$.ts && $.logger`. Bloodraven does not redefine the `zap` stream's shape, so do not build on it. Second, keys are `camelCase`, not `snake_case` — `activeSite`, `promotionGtid`, `divergentTransactions`. That convention is part of the contract too.

The anchor event for this unit is one line:

**INFO `state transition`**, with fields `site`, `from`, `to`, `fg`. The `from` and `to` values are the four site states you already know: `unknown`, `unreachable`, `read-only`, `writable`.

It is mirrored exactly once by the counter `bloodraven_state_transitions_total{site, from, to}` — same event, same labels, one increment per line. It is deliberately *not* turned into a Kubernetes Event; the log schema's correlation table lists the Event column for this row as "(none — too noisy for events)". Transitions are cheap and frequent. Events are for humans you want to page.

## One poll cycle

`DEBUG` covers per-poll bookkeeping and is off by default, because the operator's slog handler is set to `INFO`. So a healthy poll loop is *silent*. Silence is the steady state, not a broken log shipper. Here is what a benign disturbance looks like — restart the `reader` pod while the counter keeps writing:

```widget
{
  "type": "terminal",
  "title": "reader goes away and comes back",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground logs -l app.kubernetes.io/name=bloodraven -f | jq -c 'select(.fg==\"bloodraven-playground/orders\")'",
      "out": "{\"time\":\"2026-08-12T12:04:12.114Z\",\"level\":\"WARN\",\"msg\":\"failed to check replica status\",\"site\":\"reader\",\"error\":\"dial tcp 10.43.62.14:3306: connect: connection refused\",\"fg\":\"bloodraven-playground/orders\"}\n{\"time\":\"2026-08-12T12:04:16.118Z\",\"level\":\"INFO\",\"msg\":\"state transition\",\"site\":\"reader\",\"from\":\"read-only\",\"to\":\"unreachable\",\"fg\":\"bloodraven-playground/orders\"}"
    },
    {
      "cmd": "# the pod is back and answering. What is the next INFO line?",
      "out": "{\"time\":\"2026-08-12T12:05:02.340Z\",\"level\":\"INFO\",\"msg\":\"state transition\",\"site\":\"reader\",\"from\":\"unreachable\",\"to\":\"read-only\",\"fg\":\"bloodraven-playground/orders\"}"
    }
  ]
}
```

Read the first block carefully. The last successful poll was at `12:04:10`; the transition lands at `12:04:16`, six seconds later — `pollInterval` 2 s × `failureThreshold` 3, exactly the sum you computed earlier. The `WARN` above it is *not* in the Event reference: ad-hoc retry warnings are best-effort, and the `error` field is a Go error string passed through verbatim. Useful for forensics, unsafe for alerts.

The line you predicted takes a single poll. There is no six-second wait on the way back and no `recoveryThreshold` involved, because `recoveryThreshold` gates only the transition **to `writable`**. `read-only` is entered on one successful poll.

## The seven metrics

| Metric | Type | Labels | What it says |
|---|---|---|---|
| `bloodraven_site_state` | gauge (state-set) | `site`, `state` | `1` on the current state, `0` on the other three |
| `bloodraven_replication_lag_seconds` | gauge | `site` | seconds behind source; `-1` when lag is NULL |
| `bloodraven_state_transitions_total` | counter | `site`, `from`, `to` | one increment per `state transition` line |
| `bloodraven_failovers_total` | counter | `target_site` | promotions completed, by the site promoted |
| `bloodraven_poll_latency_seconds` | histogram | `site` | per-site probe duration; its `_count` is the loop's heartbeat |
| `bloodraven_divergent_transactions` | gauge | `site` | transactions a site holds that the new primary never saw; `0` when healthy |
| `bloodraven_primary_reassert_total` | counter | `site` | times the operator restored writability on a primary its own sidecar had fenced |

`bloodraven_site_state` is a state-set, so you never read one series — you read four and find the `1`:

```widget
{"type":"anatomy","title":"bloodraven_site_state{site=\"iad\",state=\"writable\"} 1","parts":[
 {"text":"bloodraven_site_state","label":"the state-set","note":"Emitted every poll for every site, one series per state. Exactly one of the four is 1."},
 {"text":"site=\"iad\"","label":"which site","note":"The bare name from spec.sites[].name — the same value the log's site field carries."},
 {"text":"state=\"writable\"","label":"which of four","note":"writable, read-only, unreachable, unknown. The other three series for iad are 0 at this instant."}]}
```

## `-1` is not a small lag

`bloodraven_replication_lag_seconds` is set only for replicating sites, and the operator writes **`-1` when `Seconds_Behind_Source` is NULL — that is, when the site is not replicating at all**. This matters more than any threshold you might pick. A reading of `0` means "caught up as far as MySQL can tell". A reading of `-1` means "there is no replication stream here". A dashboard that renders both as "low lag, everything green" inverts the most important signal on the page. Sort your queries so `-1` is never averaged with real seconds.

One more trap: when a site goes `unreachable`, the operator neither updates nor deletes its lag gauge (`internal/controller/topology.go:1157-1182` only deletes for a `writable` site). The last value it published stays on the series. A lag gauge is fresh only as long as the poll loop is turning.

## A lagging replica versus a lagging reader

`bloodraven_replication_lag_seconds` carries only `site`. There is no `role` label, so telling a lagging replica apart from a lagging reader is a join you perform yourself, against the group spec. In `orders` that join is trivial and worth doing consciously:

| | `pdx` (`primary-candidate`) | `reader` (`read-only`) |
|---|---|---|
| Counted in `coreCount`? | yes | no — excluded from every tally |
| Can it be promoted? | yes | never |
| Lag judged against | `maxLagSeconds` (default 300) | `readOnlyMaxLagSeconds` |
| Consequence of breaching it | the group's `ReplicationLagging` Degraded condition | the site drops out of the `-replicas` reader endpoint |

`readOnlyMaxLagSeconds` has **no default**. When it is nil it inherits `maxLagSeconds`; but an explicit `0` is meaningful and demands zero reported lag. Setting it to `0` is not "unset" — it is the strictest possible reader gate, and it is one of five conjuncts the reader endpoint requires (converged source, replicating, non-nil lag, canonical direct source host, and lag within the threshold). So a reader at 45 s with `readOnlyMaxLagSeconds: 30` sheds its endpoint and changes nothing about the group's health or its failover decision. A candidate at 45 s with `maxLagSeconds: 300` does the opposite: it stays in the endpoint and stays promotable.

## Forensics: two minutes of confident nonsense

**This is a captured case study, not something to reproduce.** Issue #93 needs Calico; on k3d with kube-router it is masked, because kube-router flushes conntrack on policy change.

The artefacts. Under a deny-all NetworkPolicy, the operator reports `activeSite=iad`, the site condition `state=writable`, and `Ready=True` — for two full minutes. Meanwhile the sidecar at `iad` has already self-fenced. On `/metrics`: `bloodraven_site_state{site="iad",state="writable"}` is pinned at `1`, `bloodraven_state_transitions_total` is perfectly flat, and `rate(bloodraven_poll_latency_seconds_count[1m])` is `0` for **every** site, not just `iad`.

Stop and answer before reading on: what broke?

Not MySQL, and not the network detection logic. `Poll()` froze. A deny-all policy blackholes an established connection, an in-flight read blocks with no response, and `database/sql` context cancellation does not reliably abort a read already parked on such a socket. `Poll` waits on every site's probe before it does anything else — so one frozen probe freezes the entire loop. Every gauge in the table above is written *after* that wait returns. Nothing transitioned, nothing was re-evaluated, and the operator went on publishing the last state it knew, forever, with full confidence. That is why the tell is `poll_latency_seconds_count` flatlining across all sites: it is the only series that reports whether the loop is still completing cycles at all. Confidence is not freshness.

The wrong first diagnosis is its own lesson. The maintainers initially blamed conntrack and reached for `SetConnMaxLifetime(10s)`. It could not help: a connection parked in a blocked read is never returned to the pool, so it is never recycled. A pool-level fix cannot reach a connection the pool no longer holds. The real fix was a hard driver-level I/O deadline on the probe path, so a blackholed read always returns and the site trips `failureThreshold` normally.

## Where this leaves you

You can now watch `orders` decide in real time — the `state transition` line, the state-set gauges, the transition counter — and, just as importantly, tell a genuine reading from a frozen one. Everything is in place to stop predicting and start measuring. Unit 3 holds a site down for real and puts a clock on the result.
