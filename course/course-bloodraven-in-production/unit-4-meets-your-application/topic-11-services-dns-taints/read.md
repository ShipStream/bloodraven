# Services, DNS steering, and taints

`playground` has failed over. pdx is writable, iad is fenced, you have the exact count of transactions the
promotion cost you — and the counter application is still reading from iad, the demoted site, while any
write it attempts comes back `ERROR 1290`, with nothing paging anyone. The next topic answers *why that
connection survived*. This one answers the other half: what Bloodraven actually moves when it promotes
a site, and where each mechanism stops.

There are three surfaces — Services, one DNS object, one node taint.

## Four Services, eight objects

Bloodraven reconciles four *kinds* of Service per group — two per-site, two shared — so the object
count is `2 × len(sites) + 2` — for three-site `playground` (iad, pdx, reader), eight objects.

| Service | Selector | Who may use it |
| --- | --- | --- |
| `mysql-playground-<site>` | name + instance + site (plus `healthy=yes` on a `read-only` site) | site-pinned tooling, debugging |
| `mysql-playground-<site>-internal` | name + instance + site | sidecar and peer traffic only |
| `mysql-playground-primary` | instance + `shipstream.io/role=primary` | writes |
| `mysql-playground-replicas` | instance + `role=replica` + `healthy=yes` | reads |

Applications get two of those four: `-primary` for writes, `-replicas` for reads (objective 1). The
internal per-site Service is never yours. It publishes the sidecar port alongside MySQL, sets
`publishNotReadyAddresses: true` so peers reach a pod that is not serving yet, and carries the
canonical replication source host name. Point an application at it and you have opted out of every
guarantee below.

Count the labels on the two shared Services; the counts are the lesson. `-primary` is **two**:
`app.kubernetes.io/instance=playground` and `shipstream.io/role=primary`. There is no `healthy` key on it,
and `publishNotReadyAddresses` is `false`. `-replicas` is **three**: instance, `role=replica`,
and `shipstream.io/healthy=yes`. The operator stamps `role` and `healthy` onto the pods on each
reconcile; the Services never change, only the labels underneath them.

For a `role: read-only` reader site, that `healthy=yes` stamp is expensive. Five conjuncts must **all**
hold: source convergence is `converged`; `replicating` is true; `secondsBehindSource` is non-nil; the
reported source host canonically matches the active site's internal per-site Service — a *direct*
source, so a replication chain does not count; and the lag is within
`EffectiveReadOnlyMaxLagSeconds()`. That last one has no default: nil inherits `maxLagSeconds`
(300 shipped, 30 in the playground); an explicit `0` demands zero reported lag. Four out of five is
`healthy=no`, and the reader leaves `-replicas` without a word.

Two consequences fall out of the selectors.

**A fenced pod matches neither.** An in-place restore and a planned failover both stamp `role="fenced"`
on the primary's pod. `"fenced"` is neither `"primary"` nor `"replica"`, so the pod drops out of both
shared Services at once while its own per-site Service still selects it. That is what fencing looks
like at the Service layer: unreachable for application writes and reads, still reachable for you.

**Invalid authority sheds every endpoint.** When authority is invalid or incomplete — no confirmed
writable active site — the operator deliberately leaves every site non-primary and every reader
non-serving. Both shared Services keep existing and select nothing. Someone will file this as a
bug: shedding endpoints is the design choice. A refused connection is a failure your
retry logic can see. A successful read of yesterday's data is not.

## One DNS object

Outside the cluster, Bloodraven writes one object: a `DNSEndpoint` on
`externaldns.k8s.io/v1alpha1`, named `bloodraven-<group>`, always an `A` record, with `recordTTL`
taken from `spec.dns.ttl` (default 60; the playground overrides it to 10).

```widget
{
  "type": "terminal",
  "title": "The DNS object after the promotion",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground get dnsendpoint bloodraven-playground -o yaml",
      "out": "apiVersion: externaldns.k8s.io/v1alpha1\nkind: DNSEndpoint\nmetadata:\n  name: bloodraven-playground\nspec:\n  endpoints:\n  - dnsName: playground-db.example.local   # spec.dns.hostname\n    recordType: A\n    recordTTL: 10                       # spec.dns.ttl (default 60)\n    targets:\n    - 10.96.100.20                        # CHANGED: was 10.96.100.10"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

The write model surprises people. There is **no create/update split**. The write is a single idempotent
server-side apply with `FieldOwner("bloodraven")` and forced ownership — one call creates the object,
corrects a hand-edited target, and reclaims a field someone else took. Around it,
`reconcileDNS` runs on **every poll**: it re-derives the desired target from live topology, reads the
live record back, and applies only on a real divergence. Nothing is memoized to replay later. So an
apply that a webhook or an RBAC rule rejects needs no human — it heals on a later poll, it survives an
operator restart, and it can never publish a superseded target at a site that has since gone read-only
(objective 2).

`v1alpha1` is still external-dns's current group version, and an approved proposal targets `v1beta1`
with no date attached — plan for the move, do not wait for it.

Then the boundary. Writing the record is where Bloodraven's authority ends: **the operator cannot
accelerate DNS propagation.** That is external-dns's job, then your resolver's, then your client's
cache, and your TTL is the floor on how long a stale answer survives. A stuck external-dns is a write
outage that begins *after* the operator logged that it finished. Chaos scenario 38 demonstrates that:
deny the operator write verbs on `dnsendpoints`, kill the primary, and the CR promotes correctly while
the DNS target stays stale — a perfect failover and an unreachable database at the same instant.

## The taint

```widget
{"type":"anatomy","title":"The taint Bloodraven applies to a demoted site's nodes","parts":[{"text":"shipstream.io/db-readonly-","label":"key prefix","note":"Fixed constant TaintKeyPrefix. Namespaced to ShipStream so it cannot collide with your own taints."},{"text":"playground","label":"group suffix","note":"The failover group name. Two groups on one node taint independently — this is why the key is per-group."},{"text":"=true","label":"value","note":"Constant TaintValue. It never varies, so a toleration on the key alone with operator Exists is the normal way to tolerate it."},{"text":":NoExecute","label":"effect","note":"Not NoSchedule. This one reaches pods that are already running on the node."}]}
```

Recall from Unit 2: the taint is a pure function of a per-site state transition,
applied earlier in the same poll than any cross-site action. It is **not** a step in the failover
sequence, and `role: read-only` sites are never tainted at all — a reader is already read-only, so
there is nothing to demote.

`NoExecute` is upstream Kubernetes behaviour, and stronger than people expect. Pods that do
not tolerate the taint are **evicted immediately**. Pods that tolerate it with a `tolerationSeconds`
stay bound for exactly that long, after which the node lifecycle controller evicts them. That is why
this belongs to failover (objective 3): an application pod pinned to the demoted site is *removed*
rather than left pointing at a read-only MySQL, and whatever replaces it starts with a fresh
connection pool. Chaos scenario 21 verifies the chain — the old primary's node taint evicts
a non-tolerating canary while a canary tolerating the same taint stays `Running`.

## Where this leaves you

You can now name every surface Bloodraven moves on a promotion — pod labels behind four Services, one
`DNSEndpoint`, one node taint — and where each stops: at the endpoint list, at external-dns's front
door, at the node boundary. Which makes the counter application far more interesting than it was. Its pod was never on iad's node,
so nothing evicted it. It never re-resolved a hostname, so the TTL never applied. And it never asked
`-primary` for an endpoint at all, because it already had one. It is holding an open socket to a MySQL
that has been read-only since the promotion. Every mechanism in this topic fired correctly, and every
one of them missed — because all three act on *routing*, and nothing here routes a connection that has
already been established.
