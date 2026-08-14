# Upgrading without an incident

There are three things you will upgrade, they are upgraded by three different mechanisms, and only
one of them is automatic. MySQL moves by an ordered rollout the operator drives. The operator moves by
`helm upgrade`. The CRDs move by neither, and that is the one that catches people.

Everything else in this course has been about failure arriving unannounced. This is the opposite
problem: you are the one causing the disturbance, on a Tuesday, on purpose, and the only question is
whether anybody notices.

## Upgrading MySQL: the standby goes first

Change `spec.image` and apply. What follows is not a Deployment rollout — the operator deliberately
takes that away from Kubernetes and runs it itself.

The mechanism is the spec hash from the last topic. Under `updateStrategy: OrderedUpdate` (the
default) the reconciler **leaves existing site Deployments untouched**, so the desired hash and the
live Deployment annotation diverge. The runner notices that drift on its next pass and hands the
rollout to the ordered updater, which does this:

```widget
{
  "type": "flow",
  "title": "Ordered update — status.updatePhase, one value per step",
  "steps": [
    {
      "label": "UpdateReplica",
      "detail": "Patch the standby's Deployment only. Its pod restarts on the new image; the primary is untouched and still taking writes."
    },
    {
      "label": "WaitReplica",
      "detail": "Poll every 5 s for up to 5 minutes until the standby is read-only, both replication threads are running, and it is inside 5 s of its source. This is the gate, and it can abort."
    },
    {
      "label": "Failover",
      "detail": "Promote the freshly-upgraded standby through the ordinary nine-step sequence. Your primary moves here. This records a failover and increments bloodraven_failovers_total."
    },
    {
      "label": "UpdateOldPrimary",
      "detail": "Patch the old active's Deployment. It is now a standby, so restarting it costs no write availability."
    },
    {
      "label": "WaitOldPrimary",
      "detail": "Wait up to 5 minutes for it to come back. A timeout here only warns — the rollout is already complete in the sense that matters."
    },
    {
      "label": "Complete",
      "detail": "updatePhase clears. Both sites are on the new image, and the primary is on the site that was your standby."
    }
  ]
}
```

Two things about that sequence are worth being able to defend.

**The order is MySQL's requirement, not a preference.** A replica may run a newer MySQL than its
source; a source may not run a newer MySQL than its replica. Upgrading the standby first keeps the
newer version on the replica side of replication for the whole window, which is the supported
direction. Upgrading the primary first would put an older replica behind a newer source — unsupported,
and the failure is not always immediate or obvious.

**Your primary moves, and it does not move back.** Fail-back is current-state-driven, not
identity-driven: nothing in the operator remembers that `iad` "should" be primary. After an image
bump your active site is whichever one used to be the standby. If your application has any site
affinity — a warm standby per site, a taint toleration, a latency assumption — that is a change you
scheduled without noticing. Run the upgrade in the direction you want to end up.

## The refusals, and the one that will surprise you

The updater refuses to start at all if the standby is not genuinely a standby:

```
precondition: standby <site> is writable; refusing to start ordered update
precondition: standby <site> is not replicating
```

Both run **before** the updater takes its lock, so a refused attempt leaves nothing behind and you can
simply fix the standby and re-apply.

Then the interesting one, which fires mid-rollout. `WaitReplica` is not just a timeout — it watches for
a specific shape and aborts on it:

> `standby is writable but replication is not running; aborting ordered update`

That is the restart hazard made explicit. A MySQL pod that restarts comes up writable for a few seconds
before anything fences it — you met that in Unit 5 as the ordinary source of split brains — and during
an ordered update, cross-site recovery is suppressed, so **nothing is going to start replication for
you**. A standby that comes back writable with no working replication is not a slow standby; it is a
stuck one, and waiting the full five minutes before saying so would leave the group in the rollout for
no reason. So the updater counts writable observations and gives up early.

The counter is deliberately not a strict streak — a probe error leaves it alone rather than resetting
it — because an alternating pattern of dial errors and "writable, no source" reads is exactly what a
stale connection pool produces, and a strict streak would let that mask a genuinely broken standby
until the outer deadline.

One more nicety worth knowing when you are reading the logs: a standby pod restart preserves
replication metadata, but the operator runs mysqld with `--skip-replica-start`, so the threads come
back stopped with `SourceHost` still populated. The updater owns that window, recognises the shape and
issues `START REPLICA` itself rather than waiting for something else to.

## `Recreate` is a decision about write availability

`spec.updateStrategy` takes `OrderedUpdate` or `Recreate`, and the difference is not cosmetic. Under
`Recreate` the runner clears the drift list entirely and the reconciler patches every site Deployment
in one pass — so their pod restarts may overlap, and a group with two sites can have both of them down
at the same moment. That is the total-loss window `OrderedUpdate` exists to prevent.

Use `Recreate` only when you can afford to lose the primary and the standby simultaneously, and never
for a MySQL major-version bump: if one pod's in-place upgrade stalls, you have no healthy primary and
no rollback path.

## What an image bump does to your dashboards

Say this out loud before you run one, because otherwise your on-call will say it at 02:00.

The `Failover` phase performs a **real promotion**, through the same code path as an emergency
failover, and its completion callback stamps the durable failover record and increments
`bloodraven_failovers_total`. And it is **not cooldown-gated** — `failoverCooldown` guards the
automatic path only, and nothing in the update controller consults it.

So a routine image bump produces: a `BloodravenFailoverOccurred` alert, a moved `activeSite`, a fresh
`lastFailover` stamp that will suppress the *next* automatic failover for the whole cooldown window,
and a `DNSEndpoint` flip. None of it is a fault. All of it looks exactly like one.

Two mitigations, and you want both. Alert on the ordered-update log lines —
`ordered update: updating standby`, `ordered update: failing over to updated standby`,
`ordered update complete` — so a failover with a rollout around it is visibly different from a failover
without one. And watch `status.updatePhase`, which is non-empty for exactly the duration of a rollout
and is the cheapest possible "is this us?" check:

```bash
kubectl -n bloodraven-playground get mysqlfailovergroup playground \
  -o jsonpath='{.status.updatePhase}{"\n"}'
```

## Upgrading the operator, and the CRDs it does not bring with it

The operator is one Deployment with `replicaCount: 1` and leader election. Upgrading it is
`helm upgrade`, and the blast radius is genuinely small — from Unit 5 you know the operator is not on
the request path, so a restart costs failover cover for a few seconds and costs your data plane
nothing.

The CRDs are the trap, and it is a Helm one rather than a Bloodraven one:

> **Helm installs CRDs from a chart's `crds/` directory on first install, and never upgrades them.**

So `helm upgrade` moves the operator binary and silently leaves the CRD schema at whatever version you
first installed. A new operator against an old CRD is the worst shape available: the fields the new
version wants are pruned by the API server exactly as `preferSite` was in Unit 5 — admitted, dropped,
no error anywhere — and you get an operator behaving as though you never configured the thing you
configured.

Apply the CRDs explicitly, and do it first:

```widget
{
  "type": "order",
  "title": "Upgrading Bloodraven, in order",
  "items": [
    "1. Read the release notes for CRD changes and for the sidecar image tag. These are the only two things that can require action.",
    "2. kubectl apply -f the CRDs from config/crd/bases/ (or the chart's crds/ directory) — Helm will not do this for you, on upgrade or on rollback.",
    "3. helm upgrade the operator chart. One Deployment, one replica; leader election means the new pod takes the lease when the old one releases it.",
    "4. Bump spec.sidecarImage to the matching release and apply. This is an ordered update: it restarts pods, moves your primary, and increments the failover counter.",
    "5. Run a backup verification against the new version, because a restore path you have not exercised since the upgrade is an assumption again."
  ]
}
```

Step 4 catches people out because the sidecar looks like part of the operator and is not. It ships as
its own image, referenced by `spec.sidecarImage` on the group, and moving it is a pod restart on every
site — so it goes through the same ordered update as an image bump, with the same failover. Bloodraven
tolerates a **one-minor** skew between operator and sidecar in either direction while pods roll, because
both sides of the HTTP surface between them are additive-only. Beyond one minor is untested.

And if the same release bumps `spec.image` and `spec.sidecarImage` together, do it in one apply: the
ordered updater restarts the sidecar as part of the pod restart it was already performing, so you pay
for one rollout instead of two.

## The one-way doors

Three things you cannot undo, collected in one place because each is discovered late.

**MySQL does not support downgrade of data.** Once a datadir has been opened by a newer MySQL, an
older mysqld may refuse it outright or corrupt it. `MysqlBackup.status.mysqlImage` records the image
tag that produced each dump precisely so you can check before restoring; restore onto the same major
version that produced the backup, never onto an older one.

**A steady-state per-site version split is not supported.** `spec.image` is one field per group and
`SiteSpec` has no override. Edit a site's Deployment out of band to pin a different tag and the next
reconcile reverts it. Transient skew during a rollout is fine and expected; permanent skew is not a
configuration, it is a fight with the reconciler.

**There is no version admission check.** Setting `spec.image` to something Bloodraven does not support
produces no CRD validation error. It surfaces as MySQL pods failing, which is a much worse place to
find out. The supported baseline, and what to run to re-check it, is in the
[version appendix](../sources.html#version-appendix), row C1.

## Where this leaves you

You can roll a MySQL image change and name the phase you are in from `status.updatePhase`. You can say
why the standby goes first, what the updater refuses to start on, and which mid-rollout shape makes it
abort early rather than wait. You can predict the alert your dashboards will show and tell it apart
from a real failover. And you can upgrade the operator without letting Helm leave your CRDs behind.

That is day 2 complete. What is left is the part you take away from the screen.
