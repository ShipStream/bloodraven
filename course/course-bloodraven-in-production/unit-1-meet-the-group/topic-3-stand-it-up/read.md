# Stand it up and read its status

You can name the parts of `orders` on paper. Now put it on a cluster and make it tell you what it is
doing. Everything this course looks at later — a promotion nobody triggered, a switchover you did,
an exact count of lost transactions — is read out of one `MysqlFailoverGroup` status block. Learn to
read that block now and every later unit becomes a matter of noticing what changed.

## Before you start

| You need | Why |
| --- | --- |
| **docker** or **podman** | Builds the images locally. Prefer docker — k3d's podman support is experimental. |
| **kubectl** | Everything from here is `kubectl`. |
| **helm** | Installs the operator. Chart `0.9.1`, appVersion `0.9.1`, `kubeVersion: ">=1.26.0"`. |
| **A cluster with at least three worker nodes** | One worker per site. The third is dedicated to the reader so storage-loss testing is deterministic. |

```bash
k3d cluster create bloodraven --agents 3
```

kind and minikube work too — one server plus three agents either way.

## Bring it up

From the repository root, one script does the whole thing (objective 7):

```bash
./playground/setup.sh
```

It labels one worker per site, builds and loads the images, installs the CRDs and the operator by
Helm, creates the `MysqlFailoverGroup` with two `primary-candidate` sites and one `read-only` reader,
seeds the DNS pipeline, and deploys the dashboard and counter app.

One edit first. The shipped manifest names the group `playground`; this course names it `orders`,
because every object derives its name from the group and `mysql-orders-primary` is the Service your
application connects to. Set `metadata.name: orders` in
`playground/manifests/failovergroup.yaml` and substitute the same wherever the scripts hard-code a
group-derived name — the `mysql-playground-<site>` Deployments, the `playground-dragonfly*` objects,
`mysqlfailovergroup playground` lookups, the `shipstream.io/db-readonly-playground` toleration keys.
Prefer not to? Keep the shipped name and read `playground` wherever this course writes `orders`.

Three MySQL pods, each running `mysql` beside its `sidecar` container, so a healthy site reads `2/2`:

```bash
kubectl -n bloodraven-playground get pods -l app.kubernetes.io/name=mysql
```

## Read the status

`status` is the operator's entire report on the group, and a handful of fields carry everything you
will read for the rest of the course (objective 8).

`status.activeSite` is the single site the operator currently treats as the writable authority. One
name, or empty.

`status.sites[]` runs parallel to `spec.sites`. Per site:

- **`.state`** — one of exactly four values: `writable`, `read-only`, `unreachable`, `unknown`. It
  comes from one query per poll, `SELECT @@read_only`: `0` is `writable`, `1` is `read-only`, a
  failed connection is `unreachable`.
- **`.replicating`** — whether the operator currently regards replication on that follower as
  healthy. It is a verdict, not a raw thread flag.
- **`.secondsBehindSource`** — MySQL's `Seconds_Behind_Source` for that follower.
- **`.gtidExecuted`** — the executed GTID set read from that follower's replication status: the
  transaction history it has actually applied.

There is a trap in that list. The operator probes replication only on sites whose state is
`read-only`, so the writable primary's entry carries no `replicating`, no `secondsBehindSource` and
no `gtidExecuted` at all — those keys are absent, not zero. Absence means "not measured here", never
`false`.

And `.role` is not there at all. Role lives in `spec.sites[].role`; status never repeats it. To tell
which of two `read-only` sites is the dedicated reader, read the spec.

```widget
{
  "type": "terminal",
  "title": "Read the healthy group",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup orders -o jsonpath='{.status.activeSite}{\"\\n\"}'",
      "out": "iad"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup orders -o jsonpath='{range .status.sites[*]}{.name}{\"\\t\"}{.state}{\"\\t\"}{.replicating}{\"\\t\"}{.secondsBehindSource}{\"\\n\"}{end}'",
      "out": "iad\twritable\t\t\npdx\tread-only\ttrue\t0\nreader\tread-only\ttrue\t0"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup orders -o jsonpath='{.status.conditions[?(@.type==\"Degraded\")].reason}{\"\\n\"}'",
      "out": "Healthy"
    }
  ]
}
```

Two blank columns on the `iad` row, and they are the point.

## Conditions and the five reasons

The group carries a `Ready` condition — true when a site is writable and replication is healthy — and
a `Degraded` condition carrying the interesting part: a `reason` string. Five reasons come straight
from the decision matrix, and each describes a *shape*, not an action:

| Reason | The shape it describes |
| --- | --- |
| `Healthy` | Exactly one core site writable, none unreachable. `Degraded` is `False`. |
| `Degraded` | Anything short of healthy that is not one of the three below — a live primary with an unreachable peer, or a writable non-promotable site awaiting fencing. |
| `SplitBrain` | More than one core site is writable at the same time. |
| `NoPrimary` | No core site is writable and none is unreachable — every one of them is read-only. |
| `TotalLoss` | Every core site is unreachable. |

Two things surprise people. There is no `Failover` reason: the topology one promotion away from
recovery reports `Degraded`, like everything else. And **`role: read-only` sites are excluded from
those tallies** — the reader is not a core site, so a dead reader never reads as `TotalLoss`. The
replication checks add their own reasons to `Degraded`, `ReplicationLagging` among them when a site
exceeds `spec.replication.maxLagSeconds`.

What the operator *does* about each shape is Unit 2. Here, you only read them.

## Watch a write land on both sides

```bash
kubectl -n bloodraven-playground port-forward svc/dashboard 8091:8091
kubectl -n bloodraven-playground port-forward svc/counter-app 8090:8090
```

The counter app at `localhost:8090` connects through `mysql-orders-primary` and does nothing but
`UPDATE counter_db.counters SET value = value + 1 WHERE id = 1`. Press **+ Increment** a few times,
then go looking for that row on the replica — by name, not through the primary Service (objective 9):

```bash
kubectl -n bloodraven-playground exec deploy/mysql-orders-pdx -c mysql -- \
  env MYSQL_PWD=playground-root-pw mysql -h127.0.0.1 -uroot -Nse \
  "SELECT value, updated_at FROM counter_db.counters WHERE id = 1"
```

Same `value` you just clicked to. That round trip — write through `mysql-orders-primary`, read it
back out of `pdx` by name — is the whole data plane in two commands.

## One honest warning

The playground deliberately overrides shipped defaults so experiments finish while you are still
watching. A timing you observe here is **not** the shipped default:

| Setting | Playground | Shipped default |
| --- | --- | --- |
| `failoverCooldown` | `30s` | `5m` |
| `replication.maxLagSeconds` | `30` | `300` |
| `dns.ttl` | `10` | `60` |

Everything else matches: `pollInterval`, `failureThreshold`, `recoveryThreshold`, `leaseTimeout`,
`peerCheckInterval` and `maxSyncWait` sit at their defaults. MySQL is `mysql:9.7`, the operator
v0.9.1.

## Where you are

`orders` is up: three sites, `iad` writable and named in `activeSite`, `pdx` and `reader` read-only
and replicating at `0` seconds behind, `Degraded` reason `Healthy`, and a counter application writing
through `mysql-orders-primary` while you watch. You can describe that state field by field, and say
which numbers are playground-tuned.

Then a site stops answering, and the fields you just learned start moving — `state` to `unreachable`,
the reason off `Healthy`. The operator sees exactly what you see. What it decides to do about that,
and how long it waits first, is the next unit.
