# Deployments that ask first: the lease API

You can move `playground`'s primary on purpose now, and you know what the counter app owes you on the
other side of that move. There is one more thing in your organisation that touches the primary on a
schedule, and it is not on call: the deploy pipeline. Tuesday's release runs `ALTER TABLE` against
`playground`. So does the release that was retried from a second runner because the first one looked
stuck. And the planned failover from the last topic is booked for the same window, because nobody
looks at two calendars.

Bloodraven's answer is the `/deploy/v1` API, and it is deliberately narrow. It gives a deployment two
things: a group-wide **migration mutex**, and a renewable **hold** on planned disruption. It does not
run your migrations, it does not hand out MySQL credentials, and it never delays an emergency failover.
Hold those three refusals in mind for the whole topic; every design choice below follows from them.

## Who is allowed to ask

The API is off by default. Turning it on is a chart value, `auxiliary.deployAPI.enabled`, and it needs
`auxiliary.escrowTLS.enabled` with a certificate Secret, because the API shares the operator's TLS
listener on `:8443` and there is no plaintext fallback. The operator's plain HTTP port, `8082`, never
serves it. Only the leader answers; a follower returns `503 {"error":"not_leader"}`.

A caller identifies itself with a projected ServiceAccount token whose audience is `bloodraven-deploy`,
and the operator checks it with a TokenReview. That gets you authenticated, not authorized. Authorization
lives on the `MysqlDatabase`: its `spec.deploymentClients[]` lists one `namespace` and `serviceAccount`
pair per namespace, and the database's `spec.groupRef.name` decides which group that pair may
coordinate. The lease's `instance` is the `MysqlDatabase` **resource name**, not the SQL schema name.

Look at what the pipeline does *not* need. No permission to patch the `MysqlFailoverGroup`. No read on
any Secret. No MySQL account from Bloodraven at all — the application keeps whatever credential path it
already has. The authority to edit `deploymentClients` is the authority to grant coordination, and that
is the only new authority in the system.

```widget
{
  "type": "terminal",
  "title": "One deploy, from observation to a held primary",
  "lines": [
    {
      "cmd": "curl -s --cacert ca.crt -H \"Authorization: Bearer $(cat /var/run/deploy/token)\" https://bloodraven.bloodraven.svc:8443/deploy/v1/groups/playground | jq '{activeSite, topologyGeneration, stable, leases}'",
      "out": "{\n  \"activeSite\": \"iad\",\n  \"topologyGeneration\": 17,\n  \"stable\": true,\n  \"leases\": []\n}"
    },
    {
      "cmd": "curl -s --cacert ca.crt -H \"Authorization: Bearer $(cat /var/run/deploy/token)\" -X POST https://bloodraven.bloodraven.svc:8443/deploy/v1/groups/playground/leases -d '{\"kind\":\"migration\",\"operationId\":\"a1c3f0e2-…\",\"instance\":\"counter-app\",\"ttlSeconds\":30}' -w ' %{http_code}'",
      "out": "{\"kind\":\"migration\",\"operationId\":\"a1c3f0e2-…\",\"token\":\"…\",\"expiresAt\":\"…\",\"topologyGeneration\":17} 201"
    },
    {
      "cmd": "curl -s --cacert ca.crt -H \"Authorization: Bearer $(cat /var/run/deploy/token)\" -X POST https://bloodraven.bloodraven.svc:8443/deploy/v1/groups/playground/leases -d '{\"kind\":\"failover-hold\",\"operationId\":\"a1c3f0e2-…\",\"instance\":\"counter-app\",\"ttlSeconds\":30}' -w ' %{http_code}'",
      "out": "{\"kind\":\"failover-hold\",\"operationId\":\"a1c3f0e2-…\",\"token\":\"…\",\"expiresAt\":\"…\",\"topologyGeneration\":17} 201"
    }
  ],
  "caption": "Recorded output. **Run** reveals what is already on the page — nothing executes, and no cluster is contacted."
}
```

## Two leases, in one order

`GET /deploy/v1/groups/{group}` is an observation: the active site, `topologyGeneration`, the health
block, every in-flight operation phase, and `stable` with an `unstableReason`. `stable` is false during
any operation phase — including a planned failover sitting in `Deferred` — and whenever the group is
Degraded, SplitBrain or NoPrimary, recovery is pending, or replication is not running. Lag alone does
not make it false; your lag threshold is your policy, so read `health.replicationLagSeconds` and decide.

Then two `POST`s to `/leases`, and the order is not optional. The `migration` lease is the mutex: one
per group, full stop. A second pipeline asking while it is held gets `409 held` with the holder's
`operationId`, `instance` and `expiresAt`, which is everything it needs to wait politely. The
`failover-hold` lease is granted only to whoever currently holds the migration mutex, with the same
`operationId` and `instance`; anyone else gets `403`. Migration first, then hold.

Both carry a `ttlSeconds` clamped to `[5, 120]`, and the client renews each with a `PUT` every
`ttl/3`. A successful renewal returns the new `expiresAt` and the `topologyGeneration` — compare it with
the one you were granted under, every time. If the group is between confirmed states, a renewal can
answer `423 unstable` with a `retryAfterSeconds`; that is not a renewal, and it never extends the
deadline you last had confirmed. Repeating the original `POST` for the same `operationId` is allowed and
returns `200`, but it rotates the token and invalidates the old one, so serialize it with your renewals.

The leases themselves are `coordination.k8s.io/Lease` objects, separate from the operator's own
leader-election lease. Restarting the operator releases nothing and resets nothing. Expiry is judged on
the operator's clock, so keep its nodes synchronised.

## What a hold holds, and what it never touches

An unexpired `failover-hold` does exactly one thing: it defers *planned* disruption. A planned failover
that arrives while the hold is live lands in the `Deferred` phase you met last topic, with reason
`DeploymentHold`, a `retryAfter` equal to the earliest hold expiry, and a message naming the blocking
`operationId` and instance. Every renewal of the hold pushes that `retryAfter` along. When the hold is
released or lapses, the switchover revalidates and proceeds on its own. Ordered updates and
restore-in-place defer behind the same hold. A `migration` lease on its own defers nothing.

```widget
{
  "type": "compare",
  "title": "Where a live failover-hold has authority",
  "columns": [
    {
      "label": "Defers until the hold is gone"
    },
    {
      "label": "Never consults a lease"
    }
  ],
  "rows": [
    {
      "aspect": "Planned failover",
      "cells": [
        "Phase `Deferred`, reason `DeploymentHold`, `retryAfter` = earliest hold expiry. Revalidates when the hold lapses.",
        "—"
      ]
    },
    {
      "aspect": "Ordered update, restore-in-place",
      "cells": [
        "Both wait behind the hold in the same way.",
        "—"
      ]
    },
    {
      "aspect": "Emergency failover",
      "cells": [
        "—",
        "Promotes on the same detection budget you worked out in Unit 2. The hold is not read."
      ]
    },
    {
      "aspect": "Returning-primary fencing, primary reassertion",
      "cells": [
        "—",
        "Safety paths. A deployment cannot buy a delay on either."
      ]
    }
  ],
  "caption": "The right-hand column is the whole reason the API is safe to expose to a pipeline: nothing it can hold makes the group less able to survive losing a site."
}
```

That right-hand column is the design, not a gap. If a hold could pause emergency promotion, a stuck
pipeline would be a way to keep a dead primary nominally in charge. So the emergency path does not read
leases at all, and the API has to answer a different question instead: what happens to the migration
that was running when the primary moved anyway?

## The generation is the fence

`status.topologyGeneration` is a durable counter that increments on every authoritative change of active
site — planned promotion, emergency failover, the failover inside an ordered update, restore-in-place
promotion, bootstrap completion. It is not the CR's `metadata.generation` and it is not a restart
counter. Every lease is stamped with the generation it was granted under, and a generation change
revokes every lease in the group with reason `topology_changed`, raising a `DeploymentLeaseRevoked`
event on the `MysqlFailoverGroup`.

The pipeline finds out on its next heartbeat. `PUT` comes back `409 revoked` with the new generation; a
lease that simply expired comes back `404`; a wrong or stale token comes back `403 token_mismatch`. All
three mean the same thing: **you no longer hold authority.** Stop the heartbeat. Do not reconnect to
the new primary and keep going. Whether the DDL had started is the only question left. Before it, run
the application's own fenced abort. At or after it, stop writing and require a human to verify the
schema — a healthy new primary does not prove a half-applied migration is safe. Release the hold on every
failure path; release the migration mutex only after the deploy's completion or failure state is
safely recorded. And the sentence to keep: a TTL is not an exactly-once DDL guarantee.

The operator keeps a revocation tombstone per operation until the group is deleted, so a client that
survived a revocation and retries its `POST` still gets `409 revoked` even after another operation has
taken the mutex. Tombstones hold token hashes, never tokens; budget for them when you size the API
server.

## Revoking by hand

When you need every lease gone — a pipeline that will not die, a hold you must clear before a
switchover — annotate the group with `bloodraven.shipstream.io/revoke-deployment-leases`. Renewals
then report `operator_revoked`, and the same fenced-abort rules apply to the client. What you never do is
delete the leader-election Lease or edit `topologyGeneration` to clear a hold; both are ways of lying to
the fence. `bloodraven_deploy_leases_active` and `bloodraven_planned_failovers_deferred_total` tell you
whether a hold is live and how often it has cost a switchover, and every request logs `msg="deploy api"`
with `handler`, `group`, `instance`, `operationId` and `status`.

## Where this leaves you

The application half of failover is now complete in both directions: the operator moving the primary
under your app, and your pipeline moving the schema under the operator. You can authorize a runner
without giving it the cluster, take the two leases in order, read `Deferred/DeploymentHold` for what it
is, and answer a `409 revoked` with a stop rather than a reconnect.

Everything in this unit was checked by reading status and logs by hand. The next unit asks what the
operator exports on its own, where it goes blind, and what you should be alerting on before an incident
makes you look.
