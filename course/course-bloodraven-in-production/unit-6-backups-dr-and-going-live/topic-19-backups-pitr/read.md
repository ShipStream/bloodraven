# Backups and the binlog archiver

`orders` runs on three sites — `iad`, `pdx`, and `reader` — with a counter application writing
every second. The failover story is complete: a primary dies, a candidate is writable about 12
seconds later, and you can read the exact loss from `divergentGtid`. None of that survives losing
the cluster. None of it survives a bad `DELETE` at 14:02 last Tuesday either — that statement
replicates to every site in milliseconds, and every fence you built preserves it faithfully.
Backups and point-in-time recovery are a separate subsystem with separate failure modes.

## Storage first, and be blunt about it

`spec.backup.profiles[].storage` is a tagged union with exactly two arms, `S3` and `PVC`, and a
CEL rule that rejects a body which does not match the tag. That is the whole choice, and it is the
most consequential one in the block (objective 1).

```widget
{
  "type": "compare",
  "title": "Backup storage for orders",
  "rows": [
    {
      "aspect": "Where do the objects land?",
      "cells": [
        "A bucket you name, optionally behind endpointURL for MinIO, Ceph or Wasabi",
        "A PersistentVolumeClaim — one you name, or one the operator provisions per profile"
      ]
    },
    {
      "aspect": "Shared failure domain with the data?",
      "cells": [
        "No — a separate service with its own durability contract",
        "Yes — the same cluster, often the same storage class, sometimes the same node"
      ]
    },
    {
      "aspect": "Survives loss of the cluster?",
      "cells": [
        "Yes, if the credentials and the bucket are outside it",
        "No"
      ]
    },
    {
      "aspect": "Reasonable use",
      "cells": [
        "Production protection for orders",
        "Playground, staging a dump you are about to move elsewhere"
      ]
    }
  ],
  "columns": [
    {
      "label": "storage.type: S3"
    },
    {
      "label": "storage.type: PVC"
    }
  ]
}
```

A PVC-local backup is not durable. A backup that shares a failure domain with the data it protects
is an assumption, not a backup — the event that takes your PVCs takes the copy with it. Choose
`PVC` when you want a fast local artefact, never as the thing that saves you.

## Which site the job runs on

This is the part operators get wrong. `selectSourceSite` prefers a replica and falls back to the
primary, and it records which happened as one of three reason strings you will see in status:
`"override"`, `"replica-preferred"`, `"primary-fallback"` (objective 2).

`"replica-preferred"` is not a preference for "anything that is not the primary". A candidate
replica qualifies only if its observed state is `read-only`, it is actually replicating, and its
`secondsBehindSource` is at or below `maxLagSecondsForSource` — default **300**. Above that gate,
the stale replica loses and the active site takes the job as `"primary-fallback"`, which itself
requires the primary to be `writable` and promotable. Do not confuse the gate with
`spec.replication.maxLagSeconds`, which also defaults to 300 but drives only the
`ReplicationLagging` condition and never picks a backup source.

Sites with `role: read-only` are excluded from the replica pool outright. An explicit
`sourceSiteOverride` naming one is not silently ignored — it is **rejected**:

> `sourceSiteOverride "reader" names a read-only site, which cannot be a backup source`

The reader is the obvious place to put a dump, and it is the one site you cannot. This is the same
logic you met in Unit 1 wearing different clothes: a site that is not a `primary-candidate` is not
authoritative, so it is not promotable — and here, not sourceable.

## The artifact

Add to `orders`:

```yaml
spec:
  backup:
    maxLagSecondsForSource: 300          # default, shown for clarity
    profiles:
      - name: nightly
        storage:
          type: S3                       # not PVC — see above
          s3:
            bucket: orders-backups
            prefix: orders
            endpointURL: http://minio.bloodraven-playground:9000
            credentialsSecret: orders-backup-s3
        retention: 7                     # default
    pitr:
      enabled: true                      # NEW — turns the archiver on
      profileName: nightly
      maxBinlogSize: 100M                # default when PITR is enabled
      archivePollInterval: 60s           # default
```

## The archiver

Enabling PITR starts a binlog archiver inside the per-site sidecar. One mechanism is worth
teaching properly, because the reason is the lesson: the archiver watches the **directory**, not
`mysql-bin.index`. MySQL rewrites that index atomically — write `.index.tmp`, rename over the top —
so an inode-level watch on the index file would survive exactly zero rotations. Watch the
directory and the rename is an event you receive rather than a watch you lose.

inotify is an optimisation, not the mechanism. A ticker runs alongside it at
`archivePollInterval` (default **60s**), and a best-effort scan runs once at startup to sweep up
binlogs produced while the sidecar was offline. If inotify is unavailable — a FUSE-mounted volume,
say — the archiver drops to poll-only. Rotation is then detected with worse latency, up to 60
seconds, but never missed.

Every cycle starts with a role gate: the archiver reads `@@read_only` and, on a replica, clears its
error, zeroes its backlog and returns. Only the primary archives. That single check is the whole
post-failover story — when `pdx` is promoted, its archiver's next scan finds `read_only=0` and
starts uploading. No extra wiring, no operator intervention.

Then the rule that surprises people: only **sealed** binlogs upload. The last entry in the index is
the file MySQL is writing to right now, and the archiver drops it. Fewer than two entries means
nothing is sealed and the cycle is a no-op. So the unarchived tail is exactly the current file's
contents, bounded by `maxBinlogSize` — default `100M`, applied only when PITR is enabled and
written into the generated my.cnf **before** `spec.mysqlConf` is merged, so your override still
wins. Contrast that with `gtid-mode`, `log-bin` and `log-replica-updates`, which are written
*after* overrides precisely so nothing can weaken them.

```widget
{
  "type": "flow",
  "title": "One binlog, rotation to prune",
  "steps": [
    {
      "label": "ROTATE",
      "detail": "MySQL seals mysql-bin.000041, opens .000042, and rewrites mysql-bin.index by writing .index.tmp and renaming it."
    },
    {
      "label": "Trigger",
      "detail": "The directory watch delivers the rename. The archivePollInterval ticker (60s) would have caught it anyway."
    },
    {
      "label": "Role gate",
      "detail": "Read @@read_only. On a replica: clear error, backlog 0, return. Only the primary continues."
    },
    {
      "label": "Seal",
      "detail": "Read the index, drop the last entry — .000042 is the active file — and diff the rest against the site manifest."
    },
    {
      "label": "Upload",
      "detail": "Put .000041 under binlogs/<site>/, then append the manifest entry. Manifest after upload, so a failure never leaves a row pointing at a 404."
    },
    {
      "label": "Prune",
      "detail": "GET /pitr-cutoff. Entries whose lastEventTime precedes the cutoff are removed from the manifest and deleted from storage."
    }
  ]
}
```

Pruning deserves honesty. The archiver asks the operator for the cutoff over
`/pitr-cutoff?namespace=&group=&profile=`, at most once per sweep interval (default one hour),
piggybacked on an archive scan rather than a second ticker. Errors are logged and deliberately
**not** raised into archiver status, so a transient 503 from the operator does not turn your PITR
health red. And if the retention config is absent — no operator address wired into the sidecar —
`maybeRunRetention` returns immediately and there is no pruning at all. Archived binlogs accumulate
until you notice the bill.

## What you cannot recover

Two hard limits, and neither is a bug (objective 3). On **PVC loss**, the previously-active binlog
lived on the destroyed PVC. It is gone forever, along with every transaction in it — PITR narrows
your RPO to the rotation cadence only if the tail survives. And PITR **cannot reach past the
async-replication cutoff**: transactions the old primary committed but never shipped are not in the
replica's binlog stream, and therefore not in PITR's replay material. Restoring from the survivor
cannot conjure writes the survivor never saw.

Now the sting. A backup storage failure has **no data-plane impact whatsoever**. MySQL keeps
serving reads and writes, the counter keeps counting, `orders` stays `Healthy`, and your PITR RPO
drifts backwards in silence for as long as nobody looks. That is the textbook silent degradation,
and it is exactly why Unit 6 has an alerting topic.

You can now configure backups and PITR for `orders`, name the site a backup ran from and the reason
string that explains it, and state precisely what is not recoverable. What you cannot yet do is
prove any of it works — the artefact in the bucket is untested until something loads it. That is
the next question.
