# Flashcards — Deployments that ask first: the lease API

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Which chart value turns on the deployment API, and what must already be on for it to work?

**Back:** `auxiliary.deployAPI.enabled`. It requires `auxiliary.escrowTLS.enabled` with a certificate Secret, because `/deploy/v1` is served only on the shared TLS listener on `:8443` — never on plain HTTP `8082`.

---

**Front:** What audience must a deployment client's projected ServiceAccount token carry?

**Back:** `bloodraven-deploy`. The operator validates the token with a TokenReview against that audience; the Kubernetes API's default audience is not accepted.

---

**Front:** Where does authorization for the deployment API live, and what is the lease's `instance`?

**Back:** On the `MysqlDatabase`: `spec.deploymentClients[]` lists one `namespace` + `serviceAccount` pair per namespace, and `spec.groupRef.name` names the group. `instance` is the `MysqlDatabase` resource name, not the SQL schema.

---

**Front:** Name the three things the deployment API refuses to do.

**Back:** It does not run migrations, it does not grant MySQL credentials, and it never delays emergency failover.

---

**Front:** A second pipeline POSTs a `migration` lease while one is held. What comes back?

**Back:** `409 held`, with the holder's `operationId`, `instance` and `expiresAt` — one migration lease per group, no exceptions.

---

**Front:** Who can be granted a `failover-hold`?

**Back:** Only the current `migration` holder, using the same `operationId` and `instance`. Anyone else gets `403`. Take `migration` first, then `failover-hold`.

---

**Front:** What is the TTL clamp, and how often should a client renew?

**Back:** `ttlSeconds` is clamped to `[5, 120]`. Renew each lease with `PUT` every `ttl/3`, and compare the returned `topologyGeneration` with the grant generation every time.

---

**Front:** What makes `stable=false` in a `GET` — and what does not?

**Back:** Any operation phase (including a planned failover in `Deferred`), Degraded, SplitBrain or NoPrimary, recovery pending, or replication not running. Replication lag alone does not; the lag threshold is the caller's policy.

---

**Front:** A planned failover is requested while a `failover-hold` is live. What does status show?

**Back:** `plannedFailover.phase: Deferred`, `reason: DeploymentHold`, `retryAfter` equal to the earliest hold expiry, and a message naming the blocking `operationId` and instance. The `retryAfter` moves with every hold renewal.

---

**Front:** Which paths never consult a deployment lease?

**Back:** Emergency failover, returning-old-primary fencing, and primary reassertion. A hold defers only planned failover, ordered update and restore-in-place; a `migration` lease alone defers nothing.

---

**Front:** What increments `status.topologyGeneration`, and what does a bump do to leases?

**Back:** Every authoritative active-site change — planned promotion, emergency failover, ordered-update failover, restore-in-place promotion, bootstrap completion. A bump revokes every lease in the group with `topology_changed` and raises a `DeploymentLeaseRevoked` event.

---

**Front:** How do you revoke every deployment lease by hand, and what must you never do instead?

**Back:** Annotate the group with `bloodraven.shipstream.io/revoke-deployment-leases`; renewals then report `operator_revoked`. Never delete the leader-election Lease or edit `topologyGeneration` to clear a hold.
