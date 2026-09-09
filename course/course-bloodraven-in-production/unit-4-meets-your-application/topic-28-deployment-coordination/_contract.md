# Deployments that ask first: the lease API

**Unit:** 4 — Where failover meets your application
**Objectives (unit-numbered):**
13. Authorize a deploy pipeline through `MysqlDatabase.spec.deploymentClients` and take the `migration` mutex and a `failover-hold` in the required order   [obj 13]
14. Say exactly which operations a live hold defers, and which paths never consult a lease   [obj 14]
15. Answer a `409 revoked`, `404` or `403 token_mismatch` renewal correctly: stop the heartbeat, decide by the DDL boundary, never reconnect to the new primary   [obj 15]

## Topic generation prompt

Open on the scheduling collision the rest of the unit has been building toward: the deploy pipeline is the other thing that touches `playground`'s primary on a calendar, and it is not on call. Two runners retrying the same migration, and a planned failover booked for the same window. Then state the API's scope in one breath and hold it for the whole topic — a group-wide migration mutex and a renewable hold on planned disruption; it does not run migrations, does not grant MySQL credentials, and never delays emergency failover. Every design choice follows from those three refusals.

Teach access first, because the security posture is the selling point. Off by default; `auxiliary.deployAPI.enabled` on the chart, requiring the shared TLS listener on `:8443` — no plaintext fallback, `8082` never serves it, followers answer `503 not_leader`. Identity is a projected ServiceAccount token with audience `bloodraven-deploy`, checked by TokenReview; authorization is `MysqlDatabase.spec.deploymentClients[]` (one `namespace`/`serviceAccount` pair per namespace) on a database whose `groupRef` names the group. The `instance` is the `MysqlDatabase` resource name, not the schema. Spell out what the caller does **not** need: no patch on the `MysqlFailoverGroup`, no Secrets, no MySQL account from Bloodraven.

Then the two leases and their order. `GET` is an observation: `stable`/`unstableReason` semantics (false during any operation phase including `Deferred`, Degraded/SplitBrain/NoPrimary, recovery pending, replication not running; lag alone never makes it false). `migration` is the mutex — one per group, `409 held` with the holder's `operationId`, `instance`, `expiresAt`. `failover-hold` is granted only to the current migration holder with the same `operationId`/`instance`, `403` otherwise. TTL clamped `[5, 120]`, renew at `ttl/3`, compare `topologyGeneration` on every renewal. `423 unstable` on renewal is not a renewal and never extends the last confirmed deadline. Same-operation `POST` re-grant returns `200` and rotates the token. Durable `coordination.k8s.io/Lease` objects; operator restart releases nothing; expiry on the operator clock.

Then the hold, and be exact about its authority. It defers planned failover into `Deferred`/`DeploymentHold` — the phase the learner met in topic 3 — with `retryAfter` equal to the earliest hold expiry, moving with each renewal; ordered update and restore-in-place defer the same way; a `migration` lease alone defers nothing. Emergency failover, returning-primary fencing and primary reassertion never consult leases. Make the argument that the never-consults column is the reason the API is safe to expose at all: a hold that could pause emergency promotion would let a stuck pipeline keep a dead primary in charge.

Then the fence, which is the sharpest fact in the topic: `status.topologyGeneration` increments on every authoritative active-site change (planned, emergency, ordered-update failover, restore-in-place promotion, bootstrap completion); it is neither `metadata.generation` nor a restart counter. A bump revokes every lease with `topology_changed` and raises `DeploymentLeaseRevoked`. Teach the client contract as a rule: `409 revoked`, `404`, `403 token_mismatch` all mean lost authority — stop the heartbeat, do not reconnect to the new primary; before DDL run the fenced abort, at or after DDL stop writes and require manual schema verification; release the hold on every failure path and the mutex only after the deploy's terminal state is recorded. "A TTL is not an exactly-once DDL guarantee." Tombstones per revoked operation until group deletion, holding hashes not tokens.

Close with the manual lever — `bloodraven.shipstream.io/revoke-deployment-leases` → `operator_revoked` — and the two things never to do (delete the leader-election Lease, edit `topologyGeneration`), plus the two metrics and the `msg="deploy api"` log line.

Do NOT re-teach the planned-failover phase machine; reference `Deferred`. Do NOT teach the NetworkPolicy template or the chart's escrow TLS plumbing beyond naming the values. Do NOT teach ordered updates or restore-in-place mechanics (Units 6 and 7); name them as things a hold defers.

## Requested activities

- READ: 1000-1200 words. The collision, the three refusals, access (TLS, token audience, `deploymentClients`, what the caller does not need), the two leases and their order with the status codes, TTL/renew/generation-compare, the hold's exact authority as a two-column contrast, the generation fence and the client's stop rule, the manual revoke and the two never-dos. One `terminal` widget showing GET then the two POSTs against `playground`. One `compare` widget with "defers until the hold is gone" against "never consults a lease".
- FLASHCARDS: chart value and port; audience string; where authorization lives and what `instance` means; the three refusals; migration is one per group and what `409 held` carries; hold only to the migration holder; TTL clamp and renew cadence; what `stable=false` covers and what it excludes; `Deferred`/`DeploymentHold`/`retryAfter`; what never consults a lease; what bumps `topologyGeneration`; the revoke annotation and the two never-dos. 10-12 cards.
- QUIZ: 5 questions on discriminations — why a hold cannot stop an emergency promotion and what the client sees instead; which lease to take first and why the hold was refused; what a `423 unstable` on renewal does to the deadline; the correct action on `409 revoked` after DDL has started; and whether an operator restart clears a hold.

## Handoff

**Inherits:** The learner can trigger a planned failover, read `Deferred`, and knows the emergency path promotes on its own detection budget.
**Leaves:** The learner can authorize a pipeline for `playground` without cluster-level RBAC, take both leases in order, explain what a hold does and does not defer, and stop correctly on a revoked lease.
**Do not cover:** Ordered updates (Unit 7), restore-in-place (Unit 6), the escrow TLS certificate lifecycle (Unit 7), alerting on deploy metrics (Unit 6).
