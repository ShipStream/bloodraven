# Building a group from nothing

Six units of this course started from a group that already existed. `./playground/setup.sh` created a
namespace, a Secret, three worker labels, a StorageClass and a `MysqlFailoverGroup`, and everything
since has been about *reading* and *breaking* what it built. Now build one yourself, into an empty
namespace, with nobody's script in the way.

The reason to do this last rather than first is that provisioning is where the consequences of
Unit 1's role model, Unit 2's thresholds and Unit 6's storage choice all get written down at once. You
are not learning field names. You are recording decisions you already know how to defend.

## Draw the line first: what is yours, what is the operator's

The single most common day-0 mistake is building things the operator was going to build, and skipping
things it never will.

```widget
{
  "type": "compare",
  "title": "Who creates what, for one failover group",
  "rows": [
    {
      "aspect": "Kubernetes objects",
      "cells": [
        "Namespace. StorageClass. Credential Secret(s). Node labels matching each site's `taintNodeSelector`. The `MysqlFailoverGroup` itself. A cert-manager `Issuer` if you want TLS.",
        "Per-site Deployment, PVC and ConfigMap. Eight Services for a three-site group. A PodDisruptionBudget. The `bloodraven-<group>` `DNSEndpoint`. The init-users ConfigMap."
      ]
    },
    {
      "aspect": "Inside MySQL",
      "cells": [
        "Nothing, on a fresh datadir. Everything, if you point it at a datadir that already exists.",
        "The clone plugin and the operator user, plus the app, read-only, monitor and backup users whose Secrets you set, each with a fixed grant set. Not a dedicated replication user."
      ]
    },
    {
      "aspect": "Outside the cluster",
      "cells": [
        "external-dns, and the DNS zone it writes to. Object storage and its credentials. Prometheus, Grafana, and the alert rules you wrote in Unit 6.",
        "One `DNSEndpoint` object, re-applied every poll. Nothing else leaves the cluster."
      ]
    }
  ],
  "columns": [
    { "label": "Yours to create" },
    { "label": "The operator's" }
  ]
}
```

Read the bottom-left cell twice. Bloodraven's own non-goals list from Unit 1 said it does not replace
external-dns, cert-manager, Prometheus or your object store, and this is where that stops being a
sentence and becomes a work item on somebody's sprint.

## Credentials: one required Secret, four optional ones

`spec.credentials` is the operator's only way in, and it is required: an object without it is refused at
`kubectl apply` with `spec.credentials: Required value`. Releases before v1.7.0 also accepted a single
`spec.secretName` Secret with a `dsn` key; that mode is gone, and the field is no longer part of the
schema.

The playground's operator Secret is worth reading in full because it is the minimum that works:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mysql-credentials
  namespace: bloodraven-playground
type: Opaque
stringData:
  username: "root"                           # the playground runs the operator account as root
  password: "playground-root-pw"
  MYSQL_ROOT_PASSWORD: "playground-root-pw"
  MYSQL_REPLICATION_USER: "replicator"
  MYSQL_REPLICATION_PASSWORD: "repl-pw-playground"
```

You supply up to five Secrets, each with `username` and `password`, and the operator creates each user
with a fixed grant set it will not let you widen:

| Field | Purpose | Grants the operator issues |
|---|---|---|
| `operatorSecretName` *(required)* | operator and sidecar connections | `ALL PRIVILEGES ON *.*` **WITH GRANT OPTION** |
| `appSecretName` | application read-write | DML and DDL on `*.*` — no `GRANT OPTION`, no `SUPER` or other admin privileges |
| `readOnlySecretName` | application read-only | `SELECT, SHOW VIEW, SHOW DATABASES, PROCESS` |
| `monitorSecretName` | Prometheus exporter | `PROCESS, REPLICATION CLIENT`, plus `SELECT` on `performance_schema` |
| `backupSecretName` | backup and restore jobs | `SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER, RELOAD, BACKUP_ADMIN, REPLICATION CLIENT` |

The operator Secret also carries `MYSQL_ROOT_PASSWORD`, and optionally
`MYSQL_REPLICATION_USER` / `MYSQL_REPLICATION_PASSWORD` — omit those and replication simply
reuses the operator's username and password. Name a dedicated replication user and it is yours to
create on every site: the operator does not (the playground's setup script does it for you).

The grant lists are worth one careful read, because they are the security review answer you will be
asked for. The application user cannot grant privileges to anyone. The monitor user cannot read your
data. The backup user can take a consistent dump and nothing else. None of that is configurable, and
that is the point: a fixed grant set is a claim you can make in a meeting.

## The users appear on first boot, and only on first boot

The mechanism is the ordinary MySQL one, and knowing it saves you an outage. The operator renders an
**init-users ConfigMap** and mounts it at `/docker-entrypoint-initdb.d`. The MySQL entrypoint runs
everything in that directory **once, on an empty datadir, before the server accepts external
connections** — and never again.

```widget
{
  "type": "flow",
  "title": "What runs the first time a site's MySQL container starts on an empty PVC",
  "steps": [
    {
      "label": "Entrypoint initialises the datadir",
      "detail": "Standard mysql image behaviour: an empty /var/lib/mysql triggers initialisation and the /docker-entrypoint-initdb.d hook."
    },
    {
      "label": "Install the clone plugin",
      "detail": "INSTALL PLUGIN clone SONAME 'mysql_clone.so' — guarded by a COUNT(*) on information_schema.PLUGINS, so it is idempotent."
    },
    {
      "label": "Create the operator user",
      "detail": "CREATE USER IF NOT EXISTS + ALTER USER (so a password change is applied), then GRANT ALL PRIVILEGES ON *.* WITH GRANT OPTION."
    },
    {
      "label": "Create the other four",
      "detail": "app, readonly, monitor and backup, each created only when its Secret is set, each with the grant list above."
    },
    {
      "label": "MySQL opens for business",
      "detail": "mysqld restarts with the read_only=ON it is always started with, and the operator's first poll finds a read-only, empty site. Nothing in this list ever runs again on this PVC."
    }
  ]
}
```

Two consequences follow, and both are the kind of thing you meet once.

**Adopting an existing datadir means the init script does not run.** If you point a new group at PVCs
that already hold MySQL data, the entrypoint skips initialisation entirely, so there is no clone
plugin and no operator user until you create them by hand with exactly the grants above. `CLONE_ADMIN` and `BACKUP_ADMIN` are the two people forget, and their absence surfaces
much later — as a reclone that will not start, or a backup job that cannot take a consistent dump.

**Rotating a password is the operator's job only for the role users.** Change a role Secret's
`password` and the operator runs `ALTER USER` on the primary for you. `MYSQL_ROOT_PASSWORD` and a
dedicated replication user's password are read only by the first-boot script, so changing them in
the Secret changes what the operator *presents*, not what MySQL *accepts*, and the failure looks like
a site going `Unreachable` for no reason.

## The manifest, decision by decision

Here is a production-shaped group written from nothing. Every line is a decision made earlier in this
course; the comments say which.

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlFailoverGroup
metadata:
  name: ledger
  namespace: ledger-db
spec:
  image: mysql:9.7                        # one supported baseline; pin it, never mysql:9
  credentials:                            # required — per-role users, fixed grants
    operatorSecretName: ledger-operator
    appSecretName: ledger-app
    readOnlySecretName: ledger-readonly
    monitorSecretName: ledger-monitor
    backupSecretName: ledger-backup
  dns:
    hostname: ledger-db.example.com
    ttlSeconds: 60                        # the shipped default; the playground's 10 is not
  splitBrainPolicy:
    sitePriorities: [iad, pdx]            # Unit 5: a standing decision about whose writes to discard
  replication:
    maxLagSeconds: 300                    # alerting only — never a promotion gate
    readOnlyMaxLagSeconds: 30             # reader endpoint membership only
  sites:
    - name: iad
      serverID: 101
      role: PrimaryCandidate
      zone: us-east-1a
      lbIP: "10.20.30.11"                 # required unless role is ReadOnly
      taintNodeSelector:                  # required unless role is ReadOnly
        shipstream.io/failover-group.ledger: "true"
        shipstream.io/site.ledger: iad
      storage:
        storageClassName: fast-ssd-east
        size: 500Gi
      resources:
        requests: { cpu: "2", memory: 8Gi }
        limits:   { cpu: "4", memory: 8Gi }
    - name: pdx
      serverID: 102
      role: PrimaryCandidate
      # … same shape, its own zone, lbIP, node labels and storage class …
    - name: reader
      serverID: 103
      role: ReadOnly                      # no lbIP and no taintNodeSelector: never promoted, never tainted
      storage:
        storageClassName: standard-west
        size: 500Gi
```

Four lines earn a second look.

**`storageClassName` is per site, deliberately.** Sites are in different failure domains and often on
different hardware. Nothing requires them to match, and a reader on cheaper storage is a legitimate
choice — right up until you remember from Unit 6 that a `role: ReadOnly` site can never source a
backup, so cheap reader storage buys you nothing on the recovery path.

**`resources.limits.memory` should equal `requests.memory` for MySQL.** Kubernetes gives a Pod the
`Guaranteed` QoS class only when every container sets equal requests and limits for both CPU and
memory, and `Guaranteed` is the class the kubelet evicts last under node pressure. A primary evicted
for memory pressure is an unplanned failover you did not schedule.

**`taintNodeSelector` labels must already be on the nodes.** The operator applies the
`shipstream.io/db-readonly-<group>` `NoExecute` taint from Unit 4 by selecting nodes with these
labels. Get the label wrong and nothing errors — the taint is simply applied to nothing, and the
eviction half of your failover strategy silently does not exist.

**`lbIP` and `taintNodeSelector` are required unless the role is `ReadOnly`.** That is a CEL rule, so
you find out at `kubectl apply`, which is the good case.

## What admission catches, and what it does not

Learn this split. It decides which of your mistakes cost you seconds and which cost you an incident.

**Rejected at `kubectl apply`, by the CRD schema and its CEL rules:**

- `spec.credentials` is required; a manifest that still carries `spec.secretName` gets
  `unknown field "spec.secretName"` from `kubectl apply` (strict field validation)
- `spec.sites[].name` must be unique (`spec.sites` is a list keyed by name)
- every site has a `serverID` between 1 and 4294967295, unique in the group and immutable once set:
  it is the MySQL `server_id`, so it does not change when sites are reordered
- at least two sites with role `PrimaryCandidate`
- `splitBrainPolicy.sitePriorities` entries must name `PrimaryCandidate` sites
- `taintNodeSelector` and `lbIP` are required unless the role is `ReadOnly`
- `sidecar.peerCheckInterval` between 1s and 1m, `sidecar.leaseTimeout` between 3s and 10m, and
  `leaseTimeout` ≥ 3 × `peerCheckInterval`
- every timer is bounded, and a zero is refused rather than replaced by the default:
  `pollInterval` 1s–5m, `failoverCooldown` 30s–24h, `connectionDrainTimeout` 1s–10m, `cloneTimeout`
  1m–24h, `plannedFailover.maxLagWait` 1s–1h, `plannedFailover.drainTimeout` 1s–10m
- `encryptionAtRest.enabled` requires `spec.tls`
- `serviceTemplate.externalTrafficPolicy` requires an effective type of `NodePort` or `LoadBalancer`
- `spec.sites` has between 2 and 16 entries

**Not caught by anything:**

- a `taintNodeSelector` naming labels no node carries
- a `storageClassName` that does not exist — the PVC simply stays `Pending`
- `spec.image` set to an unsupported MySQL version; there is no version admission check at all, and it
  surfaces as MySQL pod failures rather than an operator error
- a floating tag such as `mysql:9`, which can drift you onto an unsupported version between restarts
- `maxLagSeconds` chosen as though it gated promotion
- an operator Secret whose `username` and `password` name credentials MySQL does not have
- backups configured on a PVC in the same storage class as the data

Every item in that second list is on the Unit 6 go-live gate for a reason.

## Bootstrap: the first poll on an empty cluster

Apply the manifest and every site comes up **read-only and empty**: the operator starts every
`mysqld` with `--read-only=ON`, and each sidecar's startup safety net adds `super_read_only` because
no active site has been named yet. From Unit 2 you know what the matrix does with every core site
read-only and none unreachable: `NoPrimary`, and it refuses to elect. That is not what happens, and
the reason is a separate, deliberately conservative check.

`isFreshDeploy` requires **three** things of *every* site at once: it is reachable (`ReadOnly`, or
`Writable` if its pod predates the boot flag), it has never had replication configured
(`SHOW REPLICA STATUS` returns nothing), and it holds no data — an empty `GTID_EXECUTED`,
cross-checked against user schemas where the probe is available. Only then does the operator seed
the group: pick a site by `sitePriorities`, lift its fence, report it as the active site at once,
and `CLONE INSTANCE` the others from it. Because an all-read-only group produces no further state
transitions, the check also runs on every quiet poll while the group sits at `NoPrimary`. And if the
seed's own sidecar fences it again before the operator's answer reaches it, the operator re-asserts
the seed: it made that site primary itself.

The emptiness requirement is the load-bearing part, and Unit 3 already showed you the failure it
prevents. A populated cluster can reach the no-history, no-metadata state by accident: a failover
whose status write was rejected, an operator restart that rehydrated a CR with no `status.lastFailover.target`,
and an old primary that respawned (writable on a pod that predates the boot flag, read-only after). The promoted primary's own `RESET REPLICA ALL` erased its
channel metadata, so *metadata absence alone cannot tell that cluster from a fresh one*. Treat it as
fresh and the operator would seed by priority order and clone the newer side from the stale one,
destroying every post-failover write. A site with data is never part of a fresh deploy — full stop.

This is also why a failed clone leaves you stuck. Replication metadata survives on the site that
half-succeeded, so `isFreshDeploy` refuses forever after. The way out is to remove the evidence
deliberately — `STOP REPLICA; RESET REPLICA ALL;` on the stuck site, then restart the operator — and
the reason to do it consciously is that you are overriding a safety check, not clearing a glitch.

What a failed clone no longer does is pass for a successful one. A dropped connection is how
`CLONE INSTANCE` ends either way: the recipient restarts when the clone succeeds, and the session dies
when it is killed after MySQL has already dropped the recipient's data. So before it configures
replication the operator reads the recipient's `performance_schema.clone_status` and requires a new
`Completed` row and a non-empty `GTID_EXECUTED`. Anything else fails the bootstrap — the
`Bootstrapping` condition goes to `False/BootstrapFailed`, `Degraded` goes to `True/BootstrapFailed`,
and the operator logs `bootstrap failed` — and the recipient is fenced again. `Bootstrapping` is `True`
only while a clone runs, and `False/NotBootstrapping` both before any clone and after a successful one,
so "the clone finished" means `False/NotBootstrapping` with a `lastTransitionTime` after you asked
*and* the recipient's `replication` reading `Healthy`. Throughout the clone the recipient keeps `read_only=ON`; only
`super_read_only` is cleared, because `CLONE INSTANCE` refuses to run under it.

## Where this leaves you

You can write a `MysqlFailoverGroup` into an empty namespace and say, for every line, which earlier
unit decided it. You can write `spec.credentials` and recite the grants each user gets. You can say
which of your mistakes the API server will catch and which will wait for an incident, and you can
explain why a group of freshly created, all read-only sites is not stuck at `NoPrimary`.

One line in that manifest has been quietly deferred since Unit 6, where the CRD refused to enable
encryption without it. `spec.tls` is next.
