# Losing a whole cluster, and the go-live gate

`orders` has survived everything you have thrown at it *inside* one cluster: a killed primary
promoted in 12.0 s, a split brain resolved by `sitePriorities`, five shapes of partition, the
operator itself going away, a datadir wiped and restored from a bucket. Every one of those had an
operator watching. Now the cluster hosting `orders` is gone — the API server does not answer, the
nodes do not answer, and what you still have is an S3 prefix in another region. There is no
failover group left to fail over.

## Fence the source first, because that is where the danger is

Your instinct is to restore. Resist it for ten minutes. A restore into a DR cluster while the
source is still accepting writes gives you two writable copies of `orders`, and **Bloodraven v1
does not automatically detect or resolve cross-cluster split brain** (objective 13). Inside one
cluster the operator sees every site every 2 s and the decision matrix flags `SPLIT BRAIN` the
instant more than one core site is writable. Across clusters, nothing holds both halves. No
operator watches both. The sidecar fencing layer only knows the peers in its own group. There is no
component that will catch this mistake for you.

So the fencing decision **is** the safety mechanism, and it is yours. The checklist demands at
least **two of three** independent signals before you declare the source dead:

```widget
{
  "type": "compare",
  "title": "The three source-fencing signals — take any two",
  "rows": [
    {
      "aspect": "What it actually proves",
      "cells": [
        "The source operator cannot reach any writable site — or you cannot reach the source operator.",
        "You cannot administer the source cluster right now.",
        "A network location outside both clusters cannot open port 3306 against the source."
      ]
    },
    {
      "aspect": "How it lies on its own",
      "cells": [
        "The operator is not on the request path. It can be dead while MySQL serves writes perfectly.",
        "Control plane and data plane fail separately. Kubernetes will not even delete pods on an unreachable node — the containers keep running and keep writing to the PV.",
        "A one-way partition means your loss of reachability is not your application's loss of reachability."
      ]
    }
  ],
  "columns": [
    {
      "label": "1. Source operator /active-site returns 5xx"
    },
    {
      "label": "2. Source cluster API server unreachable"
    },
    {
      "label": "3. Source MySQL TCP-dead from a third vantage point"
    }
  ]
}
```

Two signals is not bureaucracy. Each one alone is a known false positive. If a signal is
ambiguous — the API server answers but slowly — you wait. Waiting ten minutes costs you ten
minutes. Getting this wrong costs you what it cost GitHub in October 2018: a 43-second partition
left East and West each holding writes the other had never seen, and reconciling them took over
24 hours. That was one company, one tooling stack, one partition. You would be doing it by hand,
across two clusters, with no shared GTID history to reason from.

## `MysqlStandbyCluster` is a dashboard, not a lifeboat

You will find a CRD called `MysqlStandbyCluster` and assume it is the DR mechanism. It is not.
Today it is **observability only**. Its controller re-scans the source bucket on
`spec.freshness.discoveryInterval` (default 5m) and publishes exactly two conditions:

| Condition | True means |
|---|---|
| `BucketReadable` | The DR cluster listed the source prefix and could read it. |
| `SourceConfigKnown` | The dump metadata parsed — `status.discovered` now carries dump name, location, GTID set, and the archived binlog window. |

That is the whole of it: **no MySQL contact, no restore Jobs, no activation.** A standby cluster
sitting at `BucketReadable=True` has told you that a DR bootstrap would be *possible* and roughly
how far back it could reach. It has not proven the dump restores, and it will not lift a finger
when the source dies. Treat it as the pre-flight gauge it is.

## The bootstrap is the restore path you already have

There is no DR-specific machinery to learn (objective 14). You create a *new* `MysqlFailoverGroup`
in the DR cluster, shaped for the DR cluster's own nodes, IPs and zones, and point
`spec.initFromBackup` at the same bucket the dead cluster was writing to:

```yaml
spec:
  sites:                                    # DR-cluster topology, not the source's
    - name: east-1
      role: primary-candidate
      lbIP: 10.1.20.11
    - name: east-2
      role: primary-candidate
      lbIP: 10.1.20.12
  dns:
    hostname: orders-east.example.com
    ttl: 60                                 # shipped default, not the playground's 10
  initFromBackup:                           # the same one-shot restore field from Unit 6
    source:
      s3:
        bucket: shipstream-backups
        prefix: orders/west/orders-nightly-20260520
        region: us-west-2
        credentialsSecret: s3-dr-readonly-creds
    decryption:
      passphraseSecret:
        name: orders-backup-passphrase      # mirrored into the DR namespace in advance
    pointInTime:
      stopDatetime: "2026-05-20T14:32:00Z"  # omit to recover to the dump's GTID
```

`initFromBackup` is one-shot and gates bootstrap: nothing else proceeds until
`status.restore.phase` reads `Succeeded`. Then normal bootstrap — clone, replication, fencing —
runs exactly as it did on day one of this course.

```widget
{
  "type": "terminal",
  "title": "kubectl bloodraven status orders on the DR cluster (excerpted)",
  "lines": [
    {
      "cmd": "kubectl bloodraven status orders --context=dr -n orders",
      "out": "MysqlFailoverGroup: orders/orders\n  Active site: east-1\n  Ready: True\n  DNS: orders-east.example.com (TTL 60s)\n\nSites:\n  NAME    ROLE               ZONE        STATE      REPL  LAG  RECOVERY  LAST-SEEN\n  east-1  primary-candidate  us-east-1a  writable   no    -    -         2s\n  east-2  primary-candidate  us-east-1b  read-only  yes   0s   -         2s\n\nInitial restore (initFromBackup):\n  Phase: Succeeded\n  Target site: east-1"
    }
  ]
}
```

Then DNS, by the same `DNSEndpoint` path from Unit 4: one object named `bloodraven-orders`,
server-side-applied every poll, one `A` record at `spec.dns.ttl`. The catch is the same catch —
**the operator cannot accelerate DNS propagation**, and here it is worse, because the operator only
owns the per-cluster record. The global application-facing name is yours to flip, by weight,
CNAME or GSLB. A perfect restore behind a stale CNAME is still an outage.

## The day-2 surface you can hand to on-call

`kubectl bloodraven` has exactly seven subcommands: `status`, `promote`, `reclone`, `backup`,
`verify-backup`, `version`, `help`. Nothing else. The design property that makes it safe to put in
a runbook is stated in the plugin's own header: it only writes resources the operator already
reads — annotations on `MysqlFailoverGroup`, plus `MysqlBackup` and `MysqlBackupVerification`
CRs — and **it never talks to MySQL directly**. There is no back door. `kubectl bloodraven promote`
behaves identically to the annotation, obeys the cooldown, the reader refusal, the lag gate, every
gate you already know. An on-call engineer cannot use the plugin to do something the operator
would have refused.

## The go-live gate

Now commit to a verdict on each of these. Not "noted" — block or accept-with-a-named-owner
(objective 15).

| Item | Why it is not what you assumed | My verdict |
|---|---|---|
| `sync_binlog=1` | An **overridable default**, written into the base my.cnf *before* your `spec.mysqlConf`. An override wins silently. | **Block** until you have read it off the running instance. |
| Backups on a PVC only | PVC-local backups are not durable; the failure that takes the PVC takes them. | **Block.** |
| No backup ever verified | An unverified backup is an assumption. Schrödinger backups. | **Block.** |
| The application-side write gap | No shipped alert covers it. Unowned, it is invisible until an incident. | **Block** until somebody owns it by name. |
| `maxLagSeconds: 300` | Drives only the `ReplicationLagging` condition. It is **not** a promotion gate — a replica beyond the threshold is still promoted. | Accept, owner must know this. |
| Runbook timings from the playground | The playground overrides the shipped defaults: `failoverCooldown: 30s` (vs 5m), `maxLagSeconds: 30` (vs 300), `dns.ttl: 10` (vs 60). No timing you measured there transfers. | Accept, owner re-measures on real config. |
| A `role: read-only` reader in the group | It can neither be promoted nor source a backup. It is not a spare. | Accept, owner must know this. |

Disagree with any of my verdicts if you can say *why*. That is the point of the exercise.

## What you can now say about `orders`

`orders` began as three sites and a counter application on a laptop. It is now a group whose
failure modes you can enumerate, whose alerts do not lie, whose backups have been restored at least
once, and which you could hand to an on-call rotation tonight — with an honest statement attached:
it promotes unattended in about 12 seconds, it switches over on purpose at RPO 0 by construction,
it reports the exact lost-transaction count in `divergentGtid`, and it will not save you from a
DNS record you forgot to flip or a second cluster you fenced by guesswork. That statement — not the
failover, not the backup — is what you take away from this course.
