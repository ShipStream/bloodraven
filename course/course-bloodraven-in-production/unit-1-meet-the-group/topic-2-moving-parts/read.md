# The moving parts

`playground` is running. Three sites — `iad`, `pdx` and `reader` — and a counter whose page reads
every two seconds and whose one button writes. What you cannot yet say is *which pod* that
button's next `UPDATE` lands on, or who decided it should be that one.

Four things stand between the application and a MySQL data directory. Meet them in the order a
write meets them.

## First contact: the Services

The counter does not connect to a pod. It connects to `mysql-playground-primary`.

```figure
{
  "src": "assets/img/g2-write-path.svg",
  "alt": "A write as four numbered arrows: counter app, to the -primary Service, to the IAD pod labelled role=primary, to disk. A small sidecar box sits on the pod.",
  "caption": "The write path. The Service is a name. The operator owns the label that makes one pod match it.",
  "width": 960,
  "height": 360
}
```

```widget
{"type":"anatomy","title":"mysql-playground-primary","parts":[
 {"text":"mysql-","label":"prefix","note":"Fixed. Every Service the operator creates for a group starts here."},
 {"text":"playground","label":"group name","note":"The metadata.name of the MysqlFailoverGroup."},
 {"text":"-primary","label":"role suffix","note":"One of four suffix shapes: -primary, -replicas, -<site>, and -<site>-internal. This one selects whichever pod currently carries shipstream.io/role=primary."}]}
```

Bloodraven creates **four kinds** of Service per group. Four *kinds*, not four objects. Two kinds
are per-site, two are group-wide, so the count is `2 × len(sites) + 2`. For `playground` that is
**eight Services**.

| Service | Scope | What it is for |
| --- | --- | --- |
| `mysql-playground-primary` | group | The write endpoint. Exactly one pod behind it, or none. |
| `mysql-playground-replicas` | group | The read endpoint. Every replica currently fit to serve. |
| `mysql-playground-<site>` | per site | Site-local access — `mysql-playground-iad`, and so on. |
| `mysql-playground-<site>-internal` | per site | The stable in-cluster address replication and the sidecars point at. |

`-primary` selects on **two** labels: `app.kubernetes.io/instance=playground` and
`shipstream.io/role=primary`. `-replicas` selects on **three**: instance,
`shipstream.io/role=replica`, and `shipstream.io/healthy=yes`.

That third label is not decoration. For a `read-only` reader the operator stamps `healthy=yes`
only when the site is actually replicating from the active primary and is not too far behind.
Fail the check and the pod silently leaves the read endpoint.

The per-site Services do not look at `role`. `mysql-playground-<site>` selects on name, instance
and site — plus `healthy=yes`, but only when that site's role is `read-only`. The `-internal`
Service has no health gate on any site, which is the point of it: peers and sidecars must reach
a pod that is not serving yet.

During an in-place restore or the draining phase of a planned failover the operator stamps the
affected pod `shipstream.io/role=fenced`. That value matches neither shared selector. The pod
keeps running, keeps its disk, keeps its IP, and appears behind neither endpoint. Fencing at
the Service layer is one label write.

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
          { "name": "mysql-playground-primary" },
          { "name": "mysql-playground-replicas" }
        ]
      },
      {
        "name": "iad",
        "children": [
          { "name": "mysql-playground-iad" },
          { "name": "mysql-playground-iad-internal" }
        ]
      },
      {
        "name": "pdx",
        "children": [
          { "name": "mysql-playground-pdx" },
          { "name": "mysql-playground-pdx-internal" }
        ]
      },
      {
        "name": "reader",
        "children": [
          { "name": "mysql-playground-reader" },
          { "name": "mysql-playground-reader-internal" }
        ]
      }
    ]
  }
}
```

Confirm the count:

```
kubectl get svc -n bloodraven-playground \
  -l app.kubernetes.io/instance=playground -o name
```

Eight lines. Six of them carry a site name.

## Second: the operator

Something has to decide which pod wears `role=primary`. That is the operator: a single
Deployment, one replica, with leader election enabled. It polls every site, evaluates what it
sees, and writes labels, status and DNS. Unit 2 takes the poll loop apart.

The operator is **not on the request path**. A healthy primary and replica keep serving with
zero operator involvement. Kill it and your application does not notice.

The bill comes due elsewhere. While the operator is down, nothing gets promoted.

## Third: the sidecar

Every MySQL pod runs a second container beside `mysqld`. Its independence is the whole point.

The sidecar can set `super_read_only=ON` on its own MySQL with the operator dead, unreachable
or mid-crash-loop. It does not ask permission. That is the thing the operator structurally
cannot do: an operator that cannot reach a site cannot fence it, and the sites you most need
fenced are exactly the ones you cannot reach.

The binlog archiver lives there for a physical reason. Point-in-time recovery needs sealed
binlog files, and finding them means watching the data directory on disk. That disk is
`ReadWriteOnce` — one *node*, not one pod. A central operator on some other node cannot mount
it. The archiver has to run where the data is.

## Fourth: the roles

Each site declares a role. The enum has three values. It defaults to `primary-candidate`. One
rule separates them: **promotability is exactly `role == primary-candidate`.**

| Role | May be promoted? | Counted in the topology tallies? | In `playground` |
| --- | --- | --- | --- |
| `primary-candidate` | Yes — the only role that may | Yes | `iad`, `pdx` |
| `dr-only` | Never | **Yes** — it counts, it just cannot win | none |
| `read-only` | Never | No — excluded from the tallies | `reader` |

`dr-only` is the one people get wrong. It is a full participant in the topology view. It simply
cannot win a promotion. `read-only` goes further: invisible to the decision, never a backup
source. Any non-candidate found *writable* is routed straight to fencing.

## Where you now stand

You can trace the counter's write: `mysql-playground-primary` → the pod labelled `role=primary`
→ whichever of `iad` or `pdx` is the active site. You can name every pod's role in `playground`
without guessing: `iad` and `pdx` are promotable, `reader` never is, and only one is writable
at any moment.

Next: why this shape, and what Bloodraven will refuse to do for you.
