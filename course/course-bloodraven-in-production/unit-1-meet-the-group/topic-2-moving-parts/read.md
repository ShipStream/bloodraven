# The moving parts

`playground` is running. Three sites — `iad`, `pdx` and `reader` — and a counter application whose page
reads the counter every two seconds and whose one button writes to it. What you cannot yet say is
*which pod* that button's next `UPDATE` lands on, or who decided it should be that one. Four things
stand between the application and a MySQL data directory. Meet them in the order a write meets them.

## First contact: the Services

The counter does not connect to a pod. It connects to `mysql-playground-primary`.

```widget
{"type":"anatomy","title":"mysql-playground-primary","parts":[
 {"text":"mysql-","label":"prefix","note":"Fixed. Every Service the operator creates for a group starts here, so one label selector or one NetworkPolicy can scope the whole group."},
 {"text":"playground","label":"group name","note":"The metadata.name of the MysqlFailoverGroup. Substituted into every object name the operator owns."},
 {"text":"-primary","label":"role suffix","note":"One of four suffix shapes: -primary, -replicas, -<site>, and -<site>-internal. This one selects whichever pod currently carries shipstream.io/role=primary."}]}
```

Bloodraven creates **four kinds** of Service per group. Four *kinds*, not four objects — a common
misreading, and the arithmetic matters when you write NetworkPolicies or audit what the operator
owns. Two kinds are per-site, two are group-wide, so the count is `2 × len(sites) + 2`. For `playground`:
`2 × 3 + 2` = **eight Services**.

| Service | Scope | What it is for |
| --- | --- | --- |
| `mysql-playground-primary` | group | The write endpoint. Exactly one pod behind it, or none. |
| `mysql-playground-replicas` | group | The read endpoint. Every replica currently fit to serve. |
| `mysql-playground-<site>` | per site | Site-local access — `mysql-playground-iad`, `mysql-playground-pdx`, `mysql-playground-reader`. |
| `mysql-playground-<site>-internal` | per site | The stable in-cluster address replication and the sidecars point at. |

The two group-wide Services differ in exactly the way that matters. `-primary` selects on **two**
labels: `app.kubernetes.io/instance=playground` and `shipstream.io/role=primary`. `-replicas` selects on
**three**: instance, `shipstream.io/role=replica`, and `shipstream.io/healthy=yes`. Both set
`publishNotReadyAddresses: false`.

That third label is not decoration. For a `read-only` reader site the operator stamps `healthy=yes`
only when five conditions hold together: source convergence is `Converged`, the site is actually
replicating, its reported lag is non-nil, its source host is the canonical `-internal` address of the
active site, and that lag is at or under `readOnlyMaxLagSeconds`. Fail one and the pod silently
leaves the read endpoint. The write endpoint has no such gate — nothing for it to lag behind.

The per-site pair is where people misremember the rule, so be exact. Neither of them looks at `role`.
`mysql-playground-<site>` selects on name, instance and `shipstream.io/site` — **plus `healthy=yes`, but
only when that site's role is `read-only`**. So `mysql-playground-reader` carries the same staleness gate
`-replicas` does, while `mysql-playground-iad` and `mysql-playground-pdx` carry none. The `-internal` Service
carries no gate on any site and sets `publishNotReadyAddresses: true`, which is the point of it: peers
and sidecars must reach a pod that is not serving yet.

Now the consequence worth carrying into every later unit. Those selectors are *label matches on the
pod*, and the operator owns the labels. During an in-place restore or the draining phase of a planned
failover it stamps the affected pod `shipstream.io/role=fenced` — a value matching neither selector.
The pod keeps running, keeps its PVC, keeps its IP, and appears behind neither endpoint. Fencing at
the Service layer is one label write. When authority is invalid or incomplete the operator does this
to every site at once, shedding all endpoints rather than serving from a topology it cannot vouch
for.

```widget
{
  "type": "tree",
  "title": "Eight Services for a three-site group",
  "root": {
    "name": "playground",
    "children": [
      {
        "name": "group-wide (2)",
        "children": [
          {
            "name": "mysql-playground-primary"
          },
          {
            "name": "mysql-playground-replicas"
          }
        ]
      },
      {
        "name": "iad",
        "children": [
          {
            "name": "mysql-playground-iad"
          },
          {
            "name": "mysql-playground-iad-internal"
          }
        ]
      },
      {
        "name": "pdx",
        "children": [
          {
            "name": "mysql-playground-pdx"
          },
          {
            "name": "mysql-playground-pdx-internal"
          }
        ]
      },
      {
        "name": "reader",
        "children": [
          {
            "name": "mysql-playground-reader"
          },
          {
            "name": "mysql-playground-reader-internal"
          }
        ]
      }
    ]
  }
}
```

Confirm the count rather than trusting the arithmetic:

```
kubectl get svc -n bloodraven-playground \
  -l app.kubernetes.io/instance=playground -o name
```

Eight lines. Six of them carry a site name.

## Second: the operator

Something has to decide which pod wears `role=primary`. That is the operator: a single Deployment,
`replicaCount: 1`, with leader election enabled. It polls every site, evaluates what it sees, and
writes labels, status and DNS. Unit 2 takes the poll loop apart; for now, one fact about its shape.

The operator is **not on the request path**. A healthy primary and replica keep serving reads and
writes with zero operator involvement — it sits on the failure-detection and promotion path only.
Kill it and your application does not notice. That is why one replica is enough: the data plane does
not depend on it, and leader election guarantees two operators never both decide.

The bill comes due elsewhere. Availability *of failover* is not preserved while the operator is down;
nothing gets promoted. Correctness still is — and that is not the operator's doing.

## Third: the sidecar

Every MySQL pod runs a second container beside `mysqld`. Its independence is the whole point.

The sidecar can set `super_read_only=ON` on its own MySQL with the operator dead, unreachable or
mid-crash-loop. It does not ask permission and it does not need a quorum. That is the thing the
operator structurally cannot do: an operator that cannot reach a site cannot fence it, and the sites
you most need fenced are exactly the ones you cannot reach. Unit 5 covers the rules that fire it.
Here, only the division of labour — the operator *decides across sites*, the sidecar *enforces
locally*.

The binlog archiver lives there for a physically different reason. Point-in-time recovery needs
sealed binlog files, and finding them means watching `/var/lib/mysql` for changes to
`mysql-bin.index` with inotify, then reading the binlogs straight off disk. Both need the MySQL data
PVC mounted — the operator mounts it read-only into the sidecar for exactly this. That PVC is
`ReadWriteOnce`, which means one *node*, not one pod. A central operator on some other node cannot
mount it. The archiver has to run where the data is. Two behaviours follow: it archives only when
`@@read_only` is off, so only the primary uploads, and it drops the last index entry, so only
*sealed* binlogs ship — the active one is still being written.

## Fourth: the roles

Each site declares a role. The enum has three values, it defaults to `primary-candidate`, and one
rule separates them: **promotability is exactly `role == primary-candidate`.**

| Role | May be promoted? | Counted in the topology tallies? | In `playground` |
| --- | --- | --- | --- |
| `primary-candidate` | Yes — the only role that may | Yes | `iad`, `pdx` |
| `dr-only` | Never | **Yes** — it counts, it just cannot win | none |
| `read-only` | Never | No — excluded from `coreCount` and all three tallies | `reader` |

`dr-only` is the one people get wrong. It is a full participant in the topology view and a valid
replication target; it simply cannot win a promotion. `read-only` goes further: invisible to the
decision, never taints a node, refused as a backup source. Both share one enforcement — any
non-`primary-candidate` site found *writable* is routed straight to fencing, and a planned failover
naming one is hard-refused with `only primary-candidate sites may be promoted`.

## Where you now stand

You can trace the counter's write: `mysql-playground-primary` → the pod labelled `role=primary` →
whichever of `iad` or `pdx` is the active site. You can name every pod's role in `playground` without
guessing: `iad` and `pdx` are promotable, `reader` never is, and only one is writable at any moment. And you can say what each layer buys you — the operator decides, the sidecar
enforces, the archiver rides along because the PVC will not travel.

What you cannot yet say is how the operator knows a site has gone. It polls. What does it ask, how
often, and how many bad answers before it acts? That is Unit 2.
