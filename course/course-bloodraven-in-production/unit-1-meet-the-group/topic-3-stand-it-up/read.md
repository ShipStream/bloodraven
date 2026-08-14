# Stand it up and read its status

This course is about a MySQL that has to survive losing a whole site. The fastest way in is
not a vocabulary list. It is a real group on your laptop, and a status dump you can read.

```figure
{
  "src": "assets/img/g1-topology.svg",
  "alt": "Three MySQL sites — IAD writable, PDX and READER as replicas — with the counter app writing through mysql-playground-primary and a small operator off to the side.",
  "caption": "What you are about to start. One site takes writes. The other two copy from it. The operator is not on the request path.",
  "width": 960,
  "height": 540
}
```

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

One non-technical check while you are here. Bloodraven is **source-available, not open source**.
Whether your organisation may run it in production is a licensing question, not a `git clone`.
The baseline and the two commands that settle it live in section D of the
[version appendix](../sources.html#version-appendix). Nothing in this course depends on the
answer. Your production plan might.

## Bring it up

From the repository root:

```bash
./playground/setup.sh
```

That script labels one worker per site, builds and loads the images, installs the CRDs and the
operator, creates the group, and deploys a dashboard and a counter app. About two minutes.

**Change nothing.** Every command in this course is written against the group exactly as that
script creates it — `metadata.name: playground` in `bloodraven-playground`. The group name is
baked into node labels, Helm flags and a Go constant, so renaming it quietly breaks later tools.

Three MySQL pods. Each runs `mysql` beside a `sidecar` container, so a healthy site reads `2/2`:

```bash
kubectl -n bloodraven-playground get pods -l app.kubernetes.io/name=mysql
```

## Read the status

`status` is the operator's report on the group. A handful of fields carry everything you will
read for the rest of the course.

`status.activeSite` is the site the operator currently treats as writable. One name, or empty.

`status.sites[]` runs parallel to `spec.sites`. Per site:

- **`.state`** — one of four values: `writable`, `read-only`, `unreachable`, `unknown`. It comes
  from one query per poll, `SELECT @@read_only`: `0` is `writable`, `1` is `read-only`, a failed
  connection is `unreachable`.
- **`.replicating`** — whether the operator currently regards replication on that follower as
  healthy. It is a verdict, not a raw thread flag.
- **`.secondsBehindSource`** — MySQL's `Seconds_Behind_Source` for that follower.
- **`.gtidExecuted`** — the transaction history that follower has actually applied.

There is a trap in that list. The operator probes replication only on sites whose state is
`read-only`, so the writable primary's entry carries no `replicating`, no `secondsBehindSource`
and no `gtidExecuted` at all — those keys are absent, not zero. Absence means "not measured
here", never `false`.

And `.role` is not there at all. Role lives in `spec.sites[].role`. To tell which of two
`read-only` sites is the dedicated reader, read the spec.

```widget
{
  "type": "terminal",
  "title": "Read the healthy group",
  "lines": [
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{.status.activeSite}{\"\\n\"}'",
      "out": "iad"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{range .status.sites[*]}{.name}{\"\\t\"}{.state}{\"\\t\"}{.replicating}{\"\\t\"}{.secondsBehindSource}{\"\\n\"}{end}'",
      "out": "iad\twritable\t\t\npdx\tread-only\ttrue\t0\nreader\tread-only\ttrue\t0"
    },
    {
      "cmd": "kubectl -n bloodraven-playground get mysqlfailovergroup playground -o jsonpath='{.status.conditions[?(@.type==\"Degraded\")].reason}{\"\\n\"}'",
      "out": "Healthy"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

Two blank columns on the `iad` row. That is the point.

## Conditions and the five reasons

The group carries a `Ready` condition — true when a site is writable and replication is healthy —
and a `Degraded` condition with a `reason` string. Five reasons come from the decision table.
Each describes a *shape*, not an action:

| Reason | The shape it describes |
| --- | --- |
| `Healthy` | Exactly one core site writable, none unreachable. `Degraded` is `False`. |
| `Degraded` | Anything short of healthy that is not one of the three below — a live primary with an unreachable peer, or a writable non-promotable site awaiting fencing. |
| `SplitBrain` | More than one core site is writable at the same time. |
| `NoPrimary` | No core site is writable and none is unreachable — every one of them is read-only. |
| `TotalLoss` | Every core site is unreachable. |

Two surprises. There is no `Failover` reason: the topology one promotion away from recovery
reports `Degraded`. And **`role: read-only` sites are excluded from those tallies** — a dead
reader never reads as `TotalLoss`.

What the operator *does* about each shape is Unit 2. Here, you only read them.

## Watch a write land on both sides

```bash
kubectl -n bloodraven-playground port-forward svc/dashboard 8091:8091
kubectl -n bloodraven-playground port-forward svc/counter-app 8090:8090
```

The counter app at `localhost:8090` connects through `mysql-playground-primary`. It has exactly
two code paths:

| Path | What it runs | When |
| --- | --- | --- |
| **read** — `GET /api/counter` | `SELECT value, updated_at …`, then `SELECT @@global.read_only` and `SELECT @@hostname` **on the same connection** | the page polls it every two seconds |
| **write** — `POST /api/increment` | `UPDATE counter_db.counters SET value = value + 1 WHERE id = 1` | only when you press **+ Increment** |

Nothing writes on a timer. You decide when a write happens. And because the read path asks the
same connection who it is talking to, the JSON carries `dbHost` and `readOnly` beside `value`.
In Unit 3 those three fields will disagree with the cluster in a way you can see.

Press **+ Increment** a few times, then go looking for that row on the replica — by name, not
through the primary Service:

```bash
kubectl -n bloodraven-playground exec deploy/mysql-playground-pdx -c mysql -- \
  env MYSQL_PWD=playground-root-pw mysql -h127.0.0.1 -uroot -Nse \
  "SELECT value, updated_at FROM counter_db.counters WHERE id = 1"
```

Same `value` you just clicked to. That round trip is the whole data plane in two commands.

## One honest warning

The playground overrides shipped defaults so experiments finish while you are still watching.
A timing you observe here is **not** the shipped default:

| Setting | Playground | Shipped default |
| --- | --- | --- |
| `failoverCooldown` | `30s` | `5m` |
| `replication.maxLagSeconds` | `30` | `300` |
| `dns.ttl` | `10` | `60` |

Everything else matches: `pollInterval`, `failureThreshold`, `recoveryThreshold`, `leaseTimeout`,
`peerCheckInterval` and `maxSyncWait` sit at their defaults. MySQL is `mysql:9.7`, the operator
v0.9.1.

## Words for the first hour

You will meet these on every later page. One sentence each.

| Word | Meaning here |
| --- | --- |
| **Site** | One MySQL plus its sidecar, pinned to one place (`iad`, `pdx`, `reader`). |
| **Primary** | The one site that currently accepts writes. Named in `status.activeSite`. |
| **Replica** | A site that copies from the primary and is not taking writes. |
| **Failover** | Promoting a replica to primary because the old primary is gone or being moved. |
| **Operator** | The controller that polls sites and decides who is primary. Not on the request path. |
| **Sidecar** | A second container in each MySQL pod. It can stop its own MySQL writing without asking the operator. |
| **Service** | A Kubernetes name your app connects to. `-primary` always points at whoever is writable *right now*. |
| **RPO** | How much recently committed data you accept losing. Bloodraven's RPO on a crash is not zero. |
| **RTO** | How long you accept being unable to write. This is where Bloodraven spends its budget. |
| **GTID** | A unique id for every transaction. The set of them is how you count what a failover cost. |
| **Fence** | `SET GLOBAL super_read_only = ON`. Blocks writes. Does **not** close existing connections. |

The full card is Unit 7. This is enough to start.

## Where you are

`playground` is up: three sites, `iad` writable and named in `activeSite`, `pdx` and `reader`
read-only and replicating at `0` seconds behind, `Degraded` reason `Healthy`, and a counter
application reading and writing through `mysql-playground-primary`.

Next: what those pods actually are, and which of them is allowed to become primary.
