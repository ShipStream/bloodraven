# Quiz — Deployments that ask first: the lease API

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A migration is running under a live `failover-hold` when `iad`'s node dies. Three polls later the operator promotes `pdx`. What happened to the hold, and what does the pipeline see on its next `PUT`?

- The hold deferred the promotion; the pipeline keeps renewing with `200` until it releases the hold, then `pdx` is promoted.
- The hold was not consulted; the promotion bumped `topologyGeneration` and revoked both leases, so the `PUT` returns `409 revoked` with reason `topology_changed`.
- The hold was honoured for its remaining TTL; when it expired the promotion ran and the `PUT` returns `404` because the lease had lapsed.
- The operator waited for `retryAfter`, then promoted; the `PUT` returns `423 unstable` until the pipeline re-POSTs the migration lease.

**Correct option index:** 1

**Explanation:**

Emergency failover never reads deployment leases — a hold that could pause it would let a stuck pipeline keep a dead primary in charge. The promotion increments `status.topologyGeneration`, which revokes every lease in the group with `topology_changed`, and the client learns it on the next heartbeat as `409 revoked`. (objectives 14, 15)

## Question 2

**Type:** TRUE_FALSE

A pipeline POSTs `failover-hold` first, then `migration`. The hold is refused with `403` and the migration is granted `201`. It POSTs the hold again and now gets `201`. This is the intended ordering: the hold can only be granted to the current migration holder.

**Correct answer:** true

**Explanation:**

A `failover-hold` is granted only to the operation that currently holds the `migration` mutex, with the same `operationId` and `instance`; any other caller is refused with `403`. The documented order is migration first, then hold. (objective 13)

## Question 3

**Type:** MULTIPLE_CHOICE

Your renewal loop gets `423 unstable` with `retryAfterSeconds` on a `PUT`, while the last confirmed `expiresAt` is 9 seconds away. What may the client do?

- Treat it as a successful renewal and extend its local deadline by the requested TTL, since the lease was not revoked.
- Re-POST the migration lease for the same `operationId` to get a fresh 30-second grant and continue.
- Keep observing within the 9 seconds it already had confirmed, and stop if no `200` arrives before that deadline — the `423` never extends the deadline.
- Ignore it: `423` is advisory, and DDL already in progress can run to completion under the old token.

**Correct option index:** 2

**Explanation:**

A `423 unstable` on renewal is a transient refusal, not a renewal. It never permits work beyond the last confirmed deadline, so the client may only continue observing inside that window. Re-POSTing to bypass an uncertain state is exactly what the docs forbid. (objectives 13, 15)

## Question 4

**Type:** SHORT_ANSWER

The pipeline's `PUT` returns `409 revoked` with `topologyGeneration: 18`. The migration's second `ALTER TABLE` of three had already run against `iad`. `pdx` is now writable and healthy. Argue what the pipeline does next.

**Sample answer:**

Stop the heartbeat and do not reconnect to `pdx` to finish the remaining DDL — `pdx` being healthy says nothing about whether the partially applied change replicated intact or is safe to continue. Because DDL had started, stop all database writes from the deploy and require a human to verify the schema before any retry. Release the `failover-hold` now; release the `migration` mutex only after the deploy's failure state is recorded. A TTL is not an exactly-once DDL guarantee, and the tombstone means this `operationId` will be refused with `409` if it tries again.

**A full-credit answer shows:**

A strong answer covers: (1) `409 revoked` means lost authority — stop the heartbeat, never continue on the new primary; (2) the decision turns on the DDL boundary, and here DDL had started, so stop writes and require manual schema verification rather than the fenced abort; (3) a healthy new primary does not prove a half-applied migration is safe; (4) release the hold on the failure path and the mutex after the terminal state is recorded; (5) optionally, the revocation tombstone refuses a retry of the same operation.

**Explanation:**

The fence is the generation, and losing it is terminal for that operation. Before DDL the client runs its fenced abort; at or after DDL it stops writing and hands the schema to a human. Reconnecting to the new primary to 'finish up' is the failure the API exists to prevent. (objective 15)

## Question 5

**Type:** MULTIPLE_CHOICE

During a deploy the operator pod is restarted. When the new leader comes up, is the pipeline's `failover-hold` still deferring the planned failover that was queued behind it?

- No — leases live in operator memory, so the restart cleared them and the planned failover proceeds on the next reconcile.
- Yes, until its unrenewed TTL expires — leases are `coordination.k8s.io/Lease` objects in the API server, and an operator restart neither releases nor resets them.
- No — a restart increments `topologyGeneration`, which revokes every lease with `topology_changed`.
- Yes, but only if the pipeline re-POSTs the hold within `retryAfterSeconds`, because the new leader has no record of it.

**Correct option index:** 1

**Explanation:**

Deployment leases are durable Lease objects, separate from leader election; restarting the operator does not release or reset them, and `topologyGeneration` is a persisted count of active-site changes, not a restart counter. The hold keeps deferring until it expires on the operator's clock or is released. (objectives 13, 14)
