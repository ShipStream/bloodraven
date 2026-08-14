# Flashcards — Building a group from nothing

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** What you have to create yourself for a new failover group

**Back:** Namespace, StorageClass, credential Secret(s), node labels matching each site's `taintNodeSelector`, the `MysqlFailoverGroup` itself, and a cert-manager `Issuer` if you want TLS. Also external-dns, object storage and Prometheus — Bloodraven's non-goals list said it replaces none of them.

---

**Front:** What the operator creates for you

**Back:** Per-site Deployment, PVC and ConfigMap; eight Services for three sites; a PodDisruptionBudget; the `bloodraven-<group>` `DNSEndpoint`; and the init-users ConfigMap that creates the MySQL users.

---

**Front:** `spec.secretName` versus `spec.credentials`

**Back:** Mutually exclusive, enforced by CEL: *exactly one of secretName or credentials must be set*. `secretName` is the legacy single-Secret mode and the operator reads a `dsn` key from it; `credentials` names up to five per-role Secrets and the operator creates a user per role with a fixed grant set.

---

**Front:** Grants the replication user receives

**Back:** `REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN ON *.*`. The last two are the ones people forget when adopting an existing datadir, and their absence surfaces as a reclone that will not start or a backup that cannot take a consistent dump.

---

**Front:** Grants the `app` user receives in credentials mode

**Back:** `ALL PRIVILEGES ON *.*` — **without** `GRANT OPTION` and without `SUPER`. It cannot grant privileges to anyone. The grant sets are fixed and not configurable, which is what makes them an answer you can give a security review.

---

**Front:** Grants the `monitor` and `readonly` users receive

**Back:** `monitor`: `PROCESS, REPLICATION CLIENT` plus `SELECT` on `performance_schema` — it cannot read your data. `readonly`: `SELECT, SHOW VIEW, SHOW DATABASES, PROCESS`.

---

**Front:** Where the MySQL users come from, and when

**Back:** An operator-rendered init-users ConfigMap mounted at `/docker-entrypoint-initdb.d`. The MySQL entrypoint runs it **once, on an empty datadir, before the server accepts external connections** — and never again.

---

**Front:** What adopting an existing datadir costs you

**Back:** The entrypoint skips initialisation, so the init script never runs: no clone plugin, no replication user, and no app/readonly/monitor/backup users. You create them by hand with exactly the documented grants.

---

**Front:** Does rotating a password in the credential Secret change MySQL?

**Back:** No. The `ALTER USER` line only runs on a fresh datadir. Changing the Secret changes what the operator *presents*, not what MySQL *accepts* — and the site goes `unreachable` for no visible reason. It does roll the pods, because the spec hash includes credential Secret data.

---

**Front:** `lbIP` and `taintNodeSelector`

**Back:** Required by a CEL rule unless the site's role is `read-only`: *taintNodeSelector and lbIP are required unless role is read-only*. A reader is never promoted and never tainted, so it needs neither.

---

**Front:** Why MySQL pods should set `requests` equal to `limits`

**Back:** Guaranteed QoS is granted only when every container sets equal CPU and memory requests and limits, and Guaranteed is the class the kubelet evicts last under node pressure. A primary evicted for memory pressure is an unplanned failover you did not schedule.

---

**Front:** `isFreshDeploy` — the three conditions, all required

**Back:** Every site is `writable`, no site has ever had replication configured (`SHOW REPLICA STATUS` returns nothing), and no site holds data (empty `GTID_EXECUTED`, cross-checked against user schemas). Only then does the operator seed and clone.

---

**Front:** Why emptiness is load-bearing in the fresh-deploy check

**Back:** A populated cluster can reach the all-writable, no-metadata state by restart amnesia — the promoted primary's own `RESET REPLICA ALL` erased its channel metadata. Treating that as fresh would seed by priority order and clone the newer side from the stale one, destroying every post-failover write.
