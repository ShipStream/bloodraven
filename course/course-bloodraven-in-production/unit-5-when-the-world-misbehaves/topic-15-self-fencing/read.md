# Self-fencing: the sidecar's two rules

Scale the operator to zero and watch `playground` carry on:

```bash
kubectl -n bloodraven-playground scale deployment/bloodraven --replicas=0
```

Press **+ Increment**: the write still lands on `iad`. Nothing pages, nothing promotes, nothing fences. That is
correct and it is the point — "A healthy primary and replica keep serving reads and writes with zero
operator involvement. The operator is on the failure-detection and promotion path, not the request
path." So ask the opposite question. With the operator gone, what would ever stop `iad` accepting
writes if it could no longer confirm it is still the active site? Not the operator. The sidecar.

## Two rules, evaluated in order

Every MySQL pod runs a `bloodraven-sidecar` container, and inside it a `FencingMonitor`. Each tick it
pings the operator's `/healthz`, reads the operator's `/active-site`, pings every peer sidecar's
`/peer/ping`, reads every peer's `/peer/active-site` — and then evaluates. Evaluation runs **two**
rules, in `FencingMonitor.evaluate` (`internal/sidecar/fencing.go`):

- **Rule #1 — topology mismatch.** The cached operator-authoritative active site is known and is
  *not* this site. Fence immediately, regardless of lease timing.
- **Rule #2 — lease expiry.** The operator **and every peer** have been silent for longer than
  `leaseTimeout`. Fence.

Order is the lesson. Rule #1 fires and *returns*. A site that has learned the active site is somebody
else fences without ever consulting the lease — which is exactly the stale-primary case, where peers
are perfectly reachable and rule #2 would stay quiet forever.

Before either rule, the monitor reads `@@read_only` and returns early if it is set. A read-only
instance never self-fences, because there is nothing left to fence. That single line is why the
replica's monitor is a permanent no-op, and why "the sidecar fenced it" is never the explanation for
a site that was already read-only.

```widget
{
  "type": "order",
  "title": "One FencingMonitor tick",
  "items": [
    "1. Read @@read_only — Already read-only? Return. A read-only instance has nothing left to fence, so the rest of the tick is skipped entirely.",
    "2. Rule #1 — topology mismatch — Cached authoritative activeSite is non-empty and != mySite? Fence and return. The lease is never consulted.",
    "3. Rule #2 — lease expiry — Operator silent beyond leaseTimeout AND every peer silent beyond leaseTimeout? Fence. Either one answering suppresses it.",
    "4. Otherwise, nothing — No third rule exists. The monitor takes no action and waits for the next peerCheckInterval tick."
  ]
}
```

## The thing that is not a rule

`Server.RunSafetyNet` (`internal/sidecar/server.go`) is a separate one-shot, called from
`cmd/sidecar/main.go`. It completes **before** the `FencingMonitor` is constructed, so it is not a
third rule and never runs again. Anything that presents it beside the two rules is describing the
sidecar's startup, not its steady state.

It fences first and asks afterwards. On boot it sets `super_read_only=ON`, then queries the operator
for the active site, and only clears the fence if the answer names this site. So it fails closed by
*staying* fenced rather than by actively fencing — and it has three distinct exits, each with its own
log line:

| Log line (verbatim) | What actually happened |
|---|---|
| `safety net: could not query active site, staying fenced` | The operator was unreachable or answered badly. The sidecar refuses to guess. |
| `safety net: no active site reported by operator, staying fenced` | The operator answered, and admitted it has no active site yet. |
| `safety net: confirmed standby site, staying fenced` | The operator answered with a *different* site. This pod is a standby and stays that way. |

Learn the prefixes, because in a log bundle they mean different things. A `safety net:` line is a pod
that has **never been allowed to write** since it started. A `SELF-FENCING:` / `SELF-FENCED:` line
from the monitor is a pod that **was writing and lost the argument**. Same read-only outcome, opposite
incident.

## Where the belief comes from: `Adopt` versus `Set`

The monitor's `TopologyCache` has two writers with deliberately different tempers
(`internal/sidecar/topology_cache.go`):

- **`Set`** — used for values read straight from the operator. Unconditional overwrite, no timestamp
  comparison, because "the operator is always authoritative".
- **`Adopt`** — used for peer-relayed views. Returns `false` and changes nothing unless the peer's
  `observedAt` is **strictly newer** than the cached value. A stale peer can never drag you backwards.

That asymmetry is the entire trust model, expressed in two method names. Peers are a relay, not an
authority.

## The lease numbers, and the trap inside them

`spec.sidecar.leaseTimeout` defaults to `20s`; `spec.sidecar.peerCheckInterval` defaults to `5s`.
Three CEL rules guard them at admission: `peerCheckInterval >= 1s`, `leaseTimeout >= 3s`, and
`leaseTimeout >= 3 × peerCheckInterval`. The shipped pair sits exactly on that floor — 3 × 5 s = 15 s,
which 20 s clears with 5 s to spare — so a lease covers 20 / 5 = **4 consecutive ticks** of silence.
Raise `peerCheckInterval` to 10 s alone and 3 × 10 s = 30 s > 20 s: the API server rejects the object.
You must move both.

Now the hard part. Rule #2 requires the operator **and every peer** to be silent for the whole window.
One reachable peer keeps the primary writable. That is documented as "retained compatibility
behavior, not a quorum guarantee" — no counting, no majority, no tie-break. And a `role: read-only`
reader counts as a peer: it relays topology and answers `/peer/ping`, and "A reachable peer without
fresh authoritative topology can still suppress the lease-only all-peers-unreachable fence."

So **adding a reader makes the lease fence less likely to fire.** On `playground` today, `iad` has two
peers — `pdx` and `reader` — which means rule #2 needs three parties (operator + 2 peers) silent for
the full 20 s, up from two before the reader existed. Sit with that before you size your next group.
A reader you added for read scaling has quietly widened the window in which an isolated primary keeps
accepting writes. It has not made rule #1 weaker — but rule #1 needs someone to tell it the truth.

## What fencing actually does to MySQL

`SET GLOBAL super_read_only = ON` is not a switch that cuts writers off.

- It **blocks** while other clients have an ongoing statement, an active `LOCK TABLES WRITE`, or an
  ongoing commit — so a fence call can sit there for seconds. It **fails outright** if the issuing
  session holds explicit locks or a pending transaction.
- An error is not proof it did nothing: "cancelling the context tears down the client connection, it
  does not roll back a write the server already applied."
- `super_read_only` is the real barrier: it "prohibits client updates even from users who have
  `CONNECTION_ADMIN` or `SUPER`", which `read_only` alone does not. Setting `super_read_only=ON`
  implicitly forces `read_only=ON`; setting `read_only=OFF` implicitly forces `super_read_only=OFF`.
- Replication threads keep writing under it. That is precisely why a fenced site is still a working
  replica.
- It does not close sockets. A session that survives the fence can serve **stale reads** until the
  site is next promoted or demoted.

You can now look at a read-only site and name the cause: rule #1 if a live operator disagrees about
the active site, rule #2 only if operator *and* every peer went quiet, and the startup safety net if
the pod never got permission in the first place. Next question: what happens when two sites both
believe they are right at the same time?
