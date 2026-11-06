# Troubleshooting Playbooks for Bloodraven Doctor

Step-by-step diagnostic and remediation playbooks for common Bloodraven failure modes.

---

## 1. Split-Brain / Dual-Writable Recovery

### Symptoms
- `BloodravenSplitBrainDetected` alert firing.
- Multiple sites report `.readOnly=false`.
- Applications write to both sites simultaneously.

### Diagnostic Steps
1. Identify all writable sites:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.sites[*]}{.name}{": readOnly="}{.readOnly}{", reachable="}{.reachable}{"\n"}{end}'
   ```
2. Determine which site has the latest authoritative writes and highest GTID.
3. Check DNS routing and that the published name matches the spec:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.spec.dns.hostname}{"\n"}'
   kubectl get dnsendpoint -n <ns> -o jsonpath='{range .items[*]}{.spec.endpoints[0].dnsName}{" -> "}{.spec.endpoints[0].targets[0]}{"\n"}{end}'
   ```

### Safe Remediation Steps
1. **Immediate Fencing**: Immediately set `read_only=ON` and `super_read_only=ON` on the non-authoritative site:
   ```bash
   kubectl exec -n <ns> <loser-pod> -c mysql -- mysql -u root -e "SET GLOBAL super_read_only=ON; SET GLOBAL read_only=ON;"
   ```
2. **Confirm Single Primary**:
   Ensure only the winning site is writable.
3. **Reconcile Divergent Data**:
   - Compare GTIDs using `./.agents/skills/bloodraven-doctor/scripts/gtid-audit.sh -n <ns> <group>`.
   - If divergent transactions exist on the loser, evaluate if they need manual data extraction (via `mysqldump` or binlog inspection).
4. **Reclone Loser**:
   ```bash
   kubectl bloodraven reclone <group> <loser-site> -n <ns>
   ```

---

## 2. Divergent Transactions & Errant GTID on Replica

### Symptoms
- Old primary fails to rejoin as replica after emergency failover (`status.sites[].divergenceDetectedBy=OldPrimaryRecovery`, Event `DataLossDetected`).
- `BloodravenDivergentTransactions` alert firing.
- `SHOW REPLICA STATUS` shows replication stopped with GTID error (e.g. error 1236).
- A replica that is still replicating normally can be divergent too (`divergenceDetectedBy=ReplicaAudit`): use playbook 33.

Before treating a site stuck in `RecoveryInProgress` (or an old primary that has not rejoined) as a stall, check whether a handoff owns the sites. Old-primary recovery, its connection drain and replication-source convergence stand down while a planned failover or ordered update is in flight (`.status.plannedFailover.phase`, `.status.updatePhase`) and resume, with a fresh drain budget, once it ends. A Dragonfly snapshot upgrade defers them only intermittently, so it does not explain a stall that lasts the whole upgrade. A planned failover that ends with `reason: RecoveryInFlight` hit its promotion deadline while such a step was still running on the sites; nothing was promoted, and the source was unfenced unless another site was already writable (message ends `left fenced`; see that matrix row). After an operator config or credential change restarts a group's topology manager mid-update, the new manager takes no failover, re-assert, recovery or update until the old manager's ordered update exits. A planned failover that reaches `Promoting` in that window fails closed: `reason: ExecuteFailed` with `a replaced topology manager's ordered update is still finishing on the sites` in the message, nothing sent to the target and the source left fenced (see the matrix row). Meanwhile `status.updatePhase` is empty. The operator log brackets this window with `topology manager deferring to its predecessor's ordered update; no failover, recovery or update until it exits` and `predecessor's ordered update exited; topology manager resuming` (`heldFor`).

### Diagnostic Steps
1. Inspect GTID executed sets:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/gtid-audit.sh -n <ns> <group>
   ```
2. Confirm active primary is healthy and taking production traffic.

### Safe Remediation Steps
1. **Explain the trade-off to the operator**:
   - Recloning will wipe the divergent replica and rebuild it from the active primary using MySQL Clone plugin.
   - Any transactions committed *only* to the old primary will be lost.
2. **Execute Reclone**:
   ```bash
   kubectl bloodraven reclone <group> <divergent-site> -n <ns>
   ```
3. **Monitor Reclone Progress**:
   ```bash
   kubectl get mfg <group> -n <ns> -w
   ```

---

## 3. Keyring & Encryption-at-Rest Troubleshooting

### Critical Alert: `KeyringEscrowMissing` or `KeyringEscrowCorrupt`
> [!CAUTION]
> **DO NOT RESTART THE MYSQL POD!**
> If the keyring escrow Secret in Kubernetes is deleted or corrupt while the pod is running with an in-memory or unsealed keyring, restarting the pod will cause total loss of the tablespace encryption master key, rendering the database files unrecoverable.

### Diagnostic Steps
1. Check sidecar keyring status (metadata and digests only; `/keyring/status` never returns key material):
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n <ns> <group> keyring
   ```
2. Find the site's escrow Secret through the CR, not by guessing its name. Escrow Secrets are named `mysql-<group>-<site>-keyring-v<N>` (immutable, one per keyring version):
   ```bash
   SECRET=$(kubectl get mysqlfailovergroup <group> -n <ns> \
     -o jsonpath='{.status.encryptionAtRest.sites[?(@.name=="<site>")].keyringSecret}')
   VERSION=$(kubectl get mysqlfailovergroup <group> -n <ns> \
     -o jsonpath='{.status.encryptionAtRest.sites[?(@.name=="<site>")].keyringVersion}')
   DIGEST=$(kubectl get mysqlfailovergroup <group> -n <ns> \
     -o jsonpath='{.status.encryptionAtRest.sites[?(@.name=="<site>")].keyringDigest}')
   kubectl get secret -n <ns> "$SECRET"
   kubectl get secret -n <ns> -l shipstream.io/failover-group=<group>,app.kubernetes.io/name=mysql-keyring
   ```

### Safe Remediation Steps
The only surviving copy of a sealed site's keys is the keyring file in its running pod's `mysql` container (`$KEYRING_DIR/keyring`; `KEYRING_DIR` is `spec.encryptionAtRest.keyring.dataFileDir`, default `/run/mysql-keyring`). Do not delete, restart, evict or drain that pod, and do not force an unseal or rollout.

1. `KeyringEscrowMissing` (the Secret is gone): recreate it from the running pod with the helper. It runs only on a site that is `Sealed` or `Failed` with no `unsealReason` (a rotation, bootstrap or clone in progress owns its Secret). It reads the pod's keyring and checks it against `status…keyringDigest` before writing anything, refuses if the Secret exists, never deletes a Secret, and re-reads the group before writing, stopping if its UID or the site's phase, unseal reason, Secret, version or digest changed. It then adds the labels (including `shipstream.io/keyring-version`), the digest annotation, the ownerReference and `immutable: true`. It never writes the group's status: the operator returns the site to `Sealed` itself once MySQL confirms it runs that keyring (Event `KeyringRecovered`), and the script waits up to `KEYRING_RECOVER_WAIT` seconds (default 120) to report it:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/keyring-escrow-recover.sh -n <ns> <group> <site>
   ```
   It reads from the site's one Running, Ready MySQL pod, selected as its Deployment selects it, and refuses if there is not exactly one; `--pod <name>` names it explicitly, and also accepts a terminating pod (with a warning): with Recreate, a pod in its grace period may hold the last copy, so read it before the grace period ends. It runs with GNU or macOS/BSD tools (`sha256sum` or `shasum`).
2. `KeyringEscrowCorrupt` (the Secret exists but no longer hashes to the recorded digest): the helper refuses by default and changes nothing. Replacing the Secret is a separate, explicit step: re-run it with `--replace-corrupt <secret-name>`, repeating the exact Secret name. It then confirms the existing Secret really fails the digest check and the pod's keyring passes it, writes a backup to `./keyring-escrow-backup-<secret>-<timestamp>.yaml`, and only then deletes and recreates that one Secret. Get the human operator's go-ahead before passing `--replace-corrupt`.
   The canonical procedure is "Keyring escrow lost" in `site/content/docs/6.operations/4.runbooks.md`; "Disaster recovery" in `site/content/docs/4.configuration/8.encryption-at-rest.md` points to it. Neither, nor this helper, writes the site's phase: the operator re-seals it.
3. Verify the operator returns the site to `Sealed` (Event `KeyringRecovered`) and the `KeyringEscrowMissing` / `KeyringEscrowCorrupt` events stop. Do not patch the phase by hand: a rotation started meanwhile would be overwritten.

---

## 4. Dragonfly Cache & Session Degradation

### Symptoms
- `status.dragonfly.phase=Degraded` or `DragonflyPromotionFailed`.
- `<group>-dragonfly` service has 0 endpoints.

### Diagnostic Steps
1. Check Dragonfly pods and endpoints:
   ```bash
   kubectl get pods -n <ns> -l app.kubernetes.io/name=dragonfly
   kubectl get endpoints <group>-dragonfly -n <ns>
   ```
2. Check Dragonfly replication status:
   ```bash
   kubectl exec -n <ns> <dragonfly-pod> -- redis-cli -p 6379 info replication
   ```
3. After an emergency failover, find which promotion path ran: `kubectl logs -n bloodraven deploy/bloodraven | grep 'dragonfly emergency'`.
   - `REPLTAKEOVER succeeded`: sessions were preserved.
   - `old master unreachable; skipping REPLTAKEOVER`, or `REPLTAKEOVER failed; falling back`, followed by `target promoted via REPLICAOF NO ONE (sessions lost)`: the cache is writable but empty.
   - `REPLICAOF NO ONE failed` or `re-dial for REPLICAOF NO ONE failed`: an unconfirmed promotion, not the outcome. A takeover can complete on the server after its client gave up, and a timed-out fallback may still have applied. The operator waits for the commands to settle, reads the target's role and logs the result:
     - `promotion outcome unconfirmed; target observed as master`: recorded as promoted (labels, status), with `DragonflySessionsLost`.
     - `promotion failed; target not observed as master`: not promoted; the old source's traffic label was restored. The manager's next poll promotes again if MySQL and Dragonfly still disagree.
     If neither line follows within about 30s, confirm the target's role with `kubectl exec -n <ns> <target-dragonfly-pod> -- redis-cli -p 6379 info replication` and check the active Service's endpoints.
   - `dragonfly promotion: ... skipping` or `request dropped` (coalesced, manager stopping, yielding to another owner, planned failover in flight, target already promoted): the request was superseded or made unnecessary. Not a failure on its own.

   A fallback after a failed takeover can take up to about 14s, and settling an unconfirmed outcome up to about 13s more. Dragonfly holds the replica's `REPLICAOF` lock until the takeover's own wait ends, so a slow promotion is not by itself a failure.
4. A planned failover or rollout logging `dragonfly gate busy; retrying` is waiting for the group's Dragonfly gate, held by the Dragonfly manager's current poll or promotion, or by a stopping manager's last step (WARN `timed out waiting for the Dragonfly manager to stop; its last step finishes under the group's Dragonfly gate`). It requeues every second. If it repeats for minutes, capture a goroutine dump of the operator (the holder is stuck in a Dragonfly or API call) before restarting it. A planned failover waiting this way emits Warning `DragonflyPromotionWaiting` and sets `status.plannedFailover.dragonfly.message`; MySQL writes stay paused on the fenced source meanwhile (about 12-13s typically, about 40s worst case).
5. Snapshot upgrade `status.dragonfly.upgrade.phase=Failed` with `reason=ActiveSiteChanged`: a promotion moved the Dragonfly master off the site the upgrade retained, so the upgrade stopped without touching routing. Confirm `status.dragonfly.activeSite` and the active Service endpoints, then re-annotate to upgrade against the new active site.

### Safe Remediation Steps
1. Inspect if network policies are blocking port 6379 or Dragonfly admin port 9999.
2. Check if auth secret (`requirepass`) matches across sites.
3. If a stale master was isolated, allow Dragonfly to sync from the promoted site. Note: MySQL remains authoritative for all durable persistent application state.

---

## 5. Planned Failover Preflight Checklist

Before executing a planned failover with `kubectl bloodraven promote <group> -n <ns> --to=<target-site>`:

- [ ] Target site is reachable and `status.sites[target].reachable == true`.
- [ ] Replication lag on target site is 0 seconds (`SecondsBehindSource == 0`).
- [ ] Target site has received all GTIDs from active primary.
- [ ] Target node has sufficient CPU/memory capacity and no scheduling taints.
- [ ] Dragonfly replica is caught up and `status.dragonfly.replTakeoverSupported == true`.
- [ ] DNS provider / `external-dns` is healthy.
- [ ] No unexpired deployment `failover-hold` is active; otherwise expect `Deferred/DeploymentHold` and inspect the named operation, instance, and `retryAfter`.

## 6. Deployment lease lost or planned operation deferred

### Diagnostic steps

1. Run `bash ./.agents/skills/bloodraven-doctor/scripts/deployment-probe.sh <namespace> <group>`. An `unstableReason` of `RestoreReclonePending` means a full-instance in-place restore still has peers to reclone (or holds them after a failure); follow [playbook 20](#20-peers-not-yet-recloned-after-a-full-instance-in-place-restore). Collect the deployment operation ID, grant generation, current `status.topologyGeneration`, lease expiry/state/reason, and planned-operation status. The helper projects public fields; never collect raw ownership hashes.
2. Inspect Events for `DeploymentLeaseRevoked` and `PlannedFailoverDeferred`, metrics `bloodraven_deploy_leases_active`, `bloodraven_deploy_lease_age_seconds`, `bloodraven_deploy_lease_revocations_total`, `bloodraven_deploy_lease_expirations_total`, and `bloodraven_planned_failovers_deferred_total`. A restart does not clear durable Leases.
3. Correlate `msg="deploy api"` logs by `group`, `instance`, `namespace`, and `operationId`. HTTP `status` and `duration_ms` identify authorization failures and slow requests without credential dumps. `401` means authentication failure; `403 forbidden` means the caller is not declared for the requested group; `403 token_mismatch` can indicate a stale token after same-operation POST rotation.
4. For network failures, verify the shared TLS listener and Secret, projected token audience, and client CA/SAN validation. Check both deployment pod and namespace selectors. Preserve sidecar `/active-site` and `/pitr-cutoff` access on 8082 and escrow on 8443 when changing policies.
5. Compare the deployment's DDL boundary and heartbeat with topology changes. Use `gtid-audit.sh`; it reports generation before and after its non-atomic site observations. A changed generation requires another observation pass, not permission to resume DDL.

### Invalid Lease duration blocks expiry

**Symptom:** `Deferred/DeploymentHold` persists beyond expiry, with `deployment lease housekeeping failed` or reconciler errors containing `spec.leaseDurationSeconds: Invalid value: 0: must be greater than 0`.

**Cause:** The affected operator writes zero after rounding the remaining lifetime down. Kubernetes rejects the Lease write when `spec.leaseDurationSeconds` becomes `0`; this affects expiry writes, release/revocation in the final fractional second, and revocation tombstone creation after expiry. Stored state remains unchanged, so retries fail again. Correct TLS trust does not fix this API-server validation error.

1. Collect sanitized state/expiry/reason with `deployment-probe.sh` and correlate operator errors. Do not dump raw Lease annotations or ownership hashes. Stop deployments whose confirmed lease expired; post-DDL uncertainty requires manual schema verification.
2. Propose an approved operator upgrade or rebuild containing the positive-duration persistence fix. Coordinate deployment owners and review queued maintenance before rollout, since it can resume when expiry processing succeeds. No MySQL data reset or Lease edit is required. Restarting the same affected binary and requesting revocation do not bypass the invalid write.
3. Verify errors stop, expired holds disappear from active observations, and the planned operation re-enters normal preflight once all holds end. Inspect any remaining blocker rather than forcing promotion. Revoked operation IDs must remain fenced after replacement and restart.

The fixed Kubernetes spec has a minimum one-second duration even for terminal records. Authority still uses the annotation's state, exact `expiresAt` (`now > expiresAt`), and topology generation; the floor adds no grace period. Do not treat the spec duration or holder identity alone as evidence of an active deployment.

### Remediation boundaries

A renewal `404`, `409`, or changed generation means stop the deployment. Post-DDL uncertainty requires manual schema/ledger verification; do not reconnect to the new primary and continue. Same-operation POST rotates the ownership token and must never be used to evade revocation. Release a hold through the owning deployment client's cleanup, not by editing API-server records.

For approved administrative interruption, first coordinate with all deployment owners, stop/fence their clients, and preserve failure evidence. Then annotate the group:

```bash
kubectl annotate mysqlfailovergroup <group> -n <namespace> \
  bloodraven.shipstream.io/revoke-deployment-leases=true --overwrite
```

Verify `DeploymentLeaseRevoked` and renewals reporting `operator_revoked`. Revocation does not roll back DDL, kill the client, or itself change topology generation. Planned failover, ordered update, and restore-in-place can proceed after holds end and normal preflight passes. Emergency failover, returning-primary fencing, and primary reassertion never wait on a hold. Never delete the leader-election Lease.

For an isolated writable site, probe `/status` (`self_fenced`) and `/peer/active-site`, not `/fencing`. Keep the effective timeout `max(configured leaseTimeout, 3s, 3 * max(peerCheckInterval, 1s)) < 60s` for a 60s DNS TTL and allow additional margin for monitor ticks, network timeouts, and SQL fence completion. A reachable peer can prevent lease-expiry fencing, but a fresh conflicting authoritative topology triggers immediate fencing without waiting for expiry.

---

## 7. Tenant database adoption refused (`MysqlDatabase`)

### Symptoms
- `status.phase=Failed` with `reason` `DatabasePreExists`, `PreExistingOwnerUser`, `PreExistingUser`, or `UserClaimedBySibling`.
- A `deletionPolicy: Delete` teardown left an account behind, with `OwnerUserDropSkipped`, `UserDropSkipped`, `OwnerUserReservedSkipped`, `UserReservedSkipped`, or `UserTransferred` in Events.
- `status.pendingOwnerUser` or an `appliedUsers[].pendingUsername` has been set for longer than one reconcile interval.

### Diagnostic steps
1. Read the phase, the `Ready` condition's reason, and the message — the message names the first refused `user@host`, which is the whole diagnosis:
   ```bash
   kubectl get mysqldatabase <name> -n <ns> -o jsonpath='{.status.phase}{" "}{.status.message}{"\n"}'
   ```
2. Dump the write-ahead records and compare them **per `user@host`** with what exists in MySQL:
   ```bash
   kubectl get mysqldatabase <name> -n <ns> -o jsonpath='{.status.databaseCreated}{" owner="}{.status.ownerUser}{.status.ownerHosts}{" pendingOwner="}{.status.pendingOwnerUser}{.status.pendingOwnerHosts}{"\n"}{range .status.appliedUsers[*]}{"  entry "}{.secretName}{" user="}{.username}{.hosts}{" pending="}{.pendingUsername}{.pendingHosts}{"\n"}{end}'
   ```
   `pendingOwnerHosts` / `appliedUsers[].pendingHosts` belong to the rotation *target*; `ownerHosts` / `hosts` belong to the settled name. An empty pending host list with a pending username set is an older operator's shape and falls back to the settled list. A record naming `user@H1` is not a claim on `user@H2`.
3. Identify the other claimant. `UserClaimedBySibling`, `DatabaseNameConflict` and `OwnerConflict` name the peer CR in the message; `PreExisting*` means the account exists in MySQL and **no** record of this CR names it on that host — usually a schema or account created out of band, or a `databaseName` / Secret username collision.
4. For a skipped drop, read the event text: it says which claim vetoed it (a sibling's owner, `grants[]` or ledger; a group-level principal; or this CR's own `grants[]`).

### Safe remediation
1. **Never hand-edit `status`.** These fields are the operator's record of which accounts it created. Clearing `ownerUser`, `pendingOwnerUser`, `ownerHosts`, `appliedUsers[]` or their `pending*` counterparts strands a live privileged account that no later reconcile or delete can find; inventing an entry authorizes a `DROP USER` on an account Bloodraven did not create. There is no supported `kubectl patch` remedy here.
2. Resolve the collision in the *spec* instead: pick an unused `databaseName`, point the owner or `users[]` Secret at a tenant-unique username, or remove the duplicate CR. One refused `user@host` parks the whole CR, so the fix is one edit, not a per-entry cleanup.
3. To adopt an existing schema or account deliberately, drop and recreate it under a name this CR creates — adoption is refused by design and cannot be granted after the fact.
4. A skipped drop is correct behaviour, not a leak: the account is still claimed by something. It has already lost its rights on the deleted database. If it genuinely should not outlive the CR, remove the remaining claim (the sibling's `grants[]`/`users[]` entry, or this CR's own `grants[]`) and drop the account by hand with the group's operator credential.
5. A stuck `pending*` record almost always means the apply keeps failing earlier. Look at the `Ready` condition reason first (`PrimaryUnavailable` is transient and self-heals; `MySQLError`, `GrantUserMissing` and `InvalidSpec` are verdicts about the CR).

---

## 8. Sidecar restarting, unready, or not fencing during a MySQL stall

### Symptoms
- The `sidecar` container restarts or sits in `CrashLoopBackOff` while `mysqld` is hung, in crash recovery, or restarting after CLONE.
- Pods go unready during a MySQL stall.
- After a stall or a sidecar restart, a site that is not the active one is still `super_read_only=OFF`.

### Diagnostic steps
1. Check which paths the probes hit. Liveness must be `/peer/ping` and readiness `/health`:
   ```bash
   kubectl get deploy -n <ns> -l app.kubernetes.io/instance=<group> \
     -o jsonpath='{range .items[*]}{.metadata.name}{" live="}{.spec.template.spec.containers[?(@.name=="sidecar")].livenessProbe.httpGet.path}{" ready="}{.spec.template.spec.containers[?(@.name=="sidecar")].readinessProbe.httpGet.path}{"\n"}{end}'
   ```
   A liveness path of `/health` means the operator predates the fix, so any MySQL stall longer than about 30s restarts the fencing agent. Upgrading the operator rolls the new probe out through the ordered update. That fixes liveness even for a pinned older `spec.sidecarImage`, since every sidecar serves `/peer/ping`.
2. Unready with liveness passing is the intended state while MySQL is down. Confirm the process is alive and the fencing loop is making progress:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n <ns> <group> livez
   ```
   `secondsSinceProgress` should stay well under `stallThresholdSeconds`. A 503 here means the loop is wedged on a call that ignored its timeout. Capture `support-bundle.sh` output before anyone restarts the container. A sidecar older than this endpoint returns 404 for it.
3. Read the sidecar's `safety net:` log lines (see `site/content/docs/8.observability/7.log-schema.md`):
   - `could not set initial super_read_only; the fencing monitor will retry` and `MySQL was unreachable long enough to have restarted; re-arming` mean a fence is still owed. On the first tick where MySQL answers, the monitor fences it unless the operator confirms this site on that tick.
   - `set super_read_only=ON after MySQL became reachable`, `super_read_only=ON confirmed; staying fenced`, and `operator confirms this is the active site; ...` mean the check completed.
   - `could not set super_read_only; will retry` repeating means MySQL answers reads but rejects the fence write. Check the operator user's grants and MySQL's error log.
4. Compare `@@super_read_only` with the operator's `activeSite` (`/peer/active-site`, or `status.activeSite` on the CR). The topology-mismatch rule fences a writable non-active site within one `peerCheckInterval` whenever an authoritative view exists.

### Safe remediation
1. Do not restart the sidecar to "re-run the safety net". The monitor applies the same rule on its own once MySQL answers, and a restart only adds a window with no fencing agent.
2. If a non-active site is writable and the sidecar logs show no pending `safety net:` work and no topology view, fence it by hand (playbook 1, step 1) and investigate why neither the operator nor a peer reported an active site.
3. A short stall (under 30s) does not re-arm the safety net, by design: a slow query on the active primary must not fence it. A `mysqld` that restarts that quickly comes back `read_only=ON` (every `mysqld` is started with `--read-only=ON`), so application users cannot write; it is super-fenced by the topology-mismatch rule only if someone makes it writable, and the operator re-asserts it if it was the primary. A pod still on a pre-`--read-only` spec (mid-upgrade) comes back writable and is fenced only by the topology-mismatch rule or the operator.

## 9. `app` user's mysql schema restriction pending, or a replica stopped on a partial revoke

The operator partially revokes the `app` user's write and DDL privileges on the `mysql` system schema. That needs `partial_revokes=ON`, and the revoke replicates, so the operator first turns it ON on every site and applies the revoke only once all of them confirm.

### Symptoms
- `CredentialReconcileFailed` events containing `partial_revokes not confirmed on every site`, naming a site.
- Or, on a replica: `Replica_SQL_Running: No` with the failed transaction being `REVOKE ... ON mysql.* FROM 'app'@'%'`. This happens only when a site was switched back to `partial_revokes=OFF` by hand, or a site outside the group's spec replicates from it.

### Diagnostic steps
1. Check each site:
   ```bash
   kubectl exec -n <ns> <pod> -c mysql -- mysql -N -e "SELECT @@GLOBAL.partial_revokes"
   ```
2. Check whether the restriction landed on the primary:
   ```bash
   kubectl exec -n <ns> <primary-pod> -c mysql -- mysql -N -e \
     "SELECT User_attributes FROM mysql.user WHERE User='<app-user>'"
   ```
   It should list a `Restrictions` entry for `"Database": "mysql"`. The group's `shipstream.io/system-schema-restriction` annotation equals `shipstream.io/credential-hash` once it has.

### Safe remediation
1. Pending restriction: bring the named site back. The operator retries every minute and turns `partial_revokes` ON itself. Nothing to run by hand.
2. Stopped replica: `SET GLOBAL partial_revokes = ON; START REPLICA;` on that replica. Do not skip the transaction; skipping it leaves the replica without the restriction, so after a failover the `app` user could write the grant tables there.

### Tenant Pending with `PartialRevokesOff`, or Ready with escaped grants

**Symptom:** a `MysqlDatabase` is `Pending` with Ready reason `PartialRevokesOff` and a Warning event. Its message identifies a failed setting read or a refused `SET GLOBAL partial_revokes = ON` on the primary. Transient connection errors instead use `PrimaryUnavailable` and retry. Tenant SQL and the write-ahead stamp have not run; existing grants and accounts remain intact. Cleanup and Delete-policy deletion do not wait on this setting.

1. Read the tenant status and events without reading its credential Secrets:
   ```bash
   kubectl describe mysqldatabase <name> -n <namespace>
   ```
2. Discover the primary's MySQL container and query `SELECT @@GLOBAL.partial_revokes` through the approved admin connection. Check the operator account's ability to read that variable and its `SYSTEM_VARIABLES_ADMIN` privilege. Use the event's error to distinguish missing privileges from connectivity problems.
3. With approval, restore the operator's setting access. The controller retries automatically, enables the setting and applies literal schema grants. Do not work around this by disabling partial revokes; the group's `app` restriction on `mysql.*` depends on it.

**Older false-Ready state:** intermediate development builds and the former SET-refused escaped fallback could leave an escaped row such as `acme\_wms` after the mode became ON. That spelling then matches no schema. Through an approved admin connection, inspect `SELECT Host, Db, User FROM mysql.db WHERE User = '<tenant-user>'` and compare the exact stored spellings. Upgrading to a release containing the escaped-grant reapplication fix reapplies affected Ready tenants once and restores the literal row before removing the escaped row. Ordinary v1.4.0 → v1.5.0 release upgrades already invalidated tenant hashes and are not this unchanged-hash case. If the setting was changed manually after migration, make an approved spec edit to reapply; restarting the operator or editing metadata alone does not invalidate an unchanged Ready hash. Never patch status to manufacture ownership or clear the applied hash.


## 10. Shared principal loses global access to a tenant schema

**Symptom:** a principal listed in `grants[]` loses global DDL, INSERT, SHOW VIEW or backup access on one tenant schema. Older tenant surplus/removal/deletion revokes could create partial restrictions instead of only removing explicit grant rows. If the principal is the operator account, subsequent tenant apply or deletion can fail too. Restrictions can persist after the CR is deleted or return through restore/PITR.

1. Through an approved admin connection, inspect restrictions without collecting passwords or password hashes:
   ```sql
   SELECT User, Host, JSON_EXTRACT(User_attributes, '$.Restrictions') AS Restrictions
   FROM mysql.user
   WHERE JSON_LENGTH(JSON_EXTRACT(User_attributes, '$.Restrictions')) > 0;
   ```
   Compare them with the affected account's explicit schema grants in `information_schema.SCHEMA_PRIVILEGES`. Independent global privileges and role grants are a separate source of access. The `app` account's restriction on `mysql` is intentional; other entries require investigation, not automatic removal.
2. Upgrade to the row-derived tenant revoke fix. Narrowing, removal, retirement and Delete-policy cleanup then revoke only privileges present in each schema grant row, including on retry. Ready objects reapply once. `PartialRevokeLifted` records repair of restrictions on impossible escaped schema names for accounts already recorded by the CR. `TenantPrivilegeRestriction` reports real-schema restrictions, a restricted connecting grantor, or a repair error. Escaped-name repair is best-effort and does not gate normal apply/deletion. If the connecting grantor is restricted on an escaped spelling, repair for every principal on that spelling is skipped; the warning names the grantor and this playbook. These escaped restrictions are inert and can remain until an unrestricted administrator lifts them. Grant-only schema rows (`USAGE` with `IS_GRANTABLE=YES`) retain independently managed GRANT OPTION and do not block cleanup retries.
3. Ask the privilege owner to confirm which real-schema restrictions were accidental. Their JSON contains no provenance. Desired tenant GRANTs can lift restrictions on their own requested privileges, but the operator does not grant surplus privileges on a real schema merely to clear a restriction.
4. After approval, use an unrestricted administrator to lift only the confirmed accidental privileges. For example, to restore an independently granted global INSERT on `acme_wms` for `cdc`:
   ```sql
   GRANT INSERT ON `acme_wms`.* TO 'cdc'@'%';
   ```
   If the operator account restricted itself, use a separate administrator such as root; do not retry the repair through the restricted operator account. Group credential reapplication can restore an authoritative group role's global set, but it must also retain/reapply the intended `app` restriction on `mysql.*`. Avoid broad global re-grants for external/shared principals whose intended policy is unknown.
5. Check the remaining restriction JSON and explicit schema rows, then verify the intended operation through a fresh connection as the affected principal. Retry the tenant reconcile/delete after admin recovery. Do not patch status, skip replicated transactions, disable partial revokes, or remove finalizers to hide a remaining access failure.

---

## 11. Restore or verification reports missing BRV1 magic

### Symptoms

- Restore or verification fails with `ciphertext tamper/downgrade detected` and `missing BRV1 magic` after profile encryption changed.
- A completed backup has `EncryptionAttributionCorrected` in Events.

### Diagnostic steps

1. Read the backup's recorded encryption choice and producing Job name:
   ```bash
   kubectl get mysqlbackup <backup> -n <ns> -o jsonpath='{.status.jobName}{" encrypted="}{.status.encrypted}{" algorithm="}{.status.encryptionAlgorithm}{"\n"}'
   ```
2. Inspect that Job's owner reference, `shipstream.io/backup-encrypted` label, container names, and the uploader's `BLOODRAVEN_ENCRYPTION_ALGORITHM`. The `backup-encrypt-upload` main container identifies an encrypted launch. A `mysqlsh` main container without the encryption label identifies a plaintext launch. Use the original Job configuration rather than today's profile. Do not collect Secret contents or passphrases.
3. If the owned Job is gone, preserve the audit record and verify the artifact's format through an approved storage inspection. Missing BRV1 magic on an encrypted launch can indicate tampering or a wrong source location.

### Safe remediation

1. Run an operator containing the attribution fix. On reconciliation it repairs completed backups from their owned Jobs and emits `EncryptionAttributionCorrected` once per correction. Profile edits affect newly created Jobs.
2. Confirm the corrected status, then retry restore or verification. Keep the original passphrase Secret for backups whose Jobs actually encrypted them.
3. If status and Job already agree, investigate storage identity and artifact integrity. Do not enable plaintext fallback to hide an unexplained mismatch.

---

## 12. Backup or restore reports Access denied with valid credentials

### Symptoms

- A backup or restore Job reports MySQL `Access denied` while the operator connects successfully.
- The password includes surrounding spaces, tabs, or line endings, often from a Secret created with `--from-file`.

### Diagnostic steps

1. Confirm the Job mounts the intended derived MySQL credential Secret and uses the expected backup scripts ConfigMap. Check the group's credential references without decoding or collecting password contents.
2. Inspect the `dump.py` or `restore.py` code in that ConfigMap. Fixed scripts use `_read_mysql_cred_file` to read bytes and decode UTF-8 without stripping or newline translation. Older scripts call the whitespace-normalizing `_read_cred_file` for MySQL credentials. The Go credential producer already preserves those contents.
3. Check file presence and readability without printing values. A readable empty password file intentionally overrides the legacy environment password. A readable empty username fails validation. Missing or unreadable files can use the legacy environment fallback.

### Safe remediation

1. Upgrade to an operator containing the reader fix and confirm it has refreshed the shared scripts ConfigMap before retrying the Job. Do not trim or rotate valid credentials to compensate for the old reader.
2. Retry a failed backup with `kubectl bloodraven backup <group> -n <ns>`. For a failed restore, follow the relevant restore runbook and its data handling precautions before retrying.
3. If authentication still fails with the corrected scripts, investigate the selected MySQL user, host grants, and Secret references. Preserve intended whitespace; AWS configuration normalization is separate from MySQL credentials.

---

## 13. Succeeded backup cannot be found at its recorded location

### Symptoms

- Restore or verification cannot load a dump from a Succeeded MysqlBackup.
- An older backup's `status.location` contains spaces where the storage prefix used underscores, or recorded binlog names and GTID tags lost underscores.
- Cleanup reports an artifact absent while the original objects remain in storage.

### Diagnostic steps

1. Compare the backup's recorded location with the producing Job's main-container `BLOODRAVEN_OUTPUT_URL` and `BLOODRAVEN_STORAGE_TYPE`. Use `mysqlsh` for plaintext Jobs and `backup-encrypt-upload` for encrypted Jobs; the encrypted dump init container points at temporary staging, not the stored artifact. Do not consult today's mutable profile to reconstruct an older destination.
2. Account for the S3 display convention: plaintext Jobs record a bucket-relative key; encrypted Jobs record `s3://<BLOODRAVEN_S3_BUCKET>/<output-prefix>/`. PVC Jobs record the original absolute output path.
3. Inspect the completion log format. `v=2` percent-encodes token values and preserves underscores, percent signs, and whitespace. Unversioned logs used the same underscore for both original spaces and literal underscores, so the original value cannot be recovered from that log alone. Legacy tagged GTID and binlog metadata may also be damaged in already-stored status.

### Safe remediation

1. Upgrade to an operator and dump scripts containing the versioned codec for future completions. Older encrypted operator images can still finish with legacy lines; the fixed parser preserves their literal identity fields but cannot infer originally encoded spaces.
2. Establish the actual artifact using the original Job inputs and storage inspection. If those are unavailable, take a replacement backup. Keep old artifacts until their identity is verified; do not release cleanup finalizers based on a guessed path.
3. Do not globally substitute spaces and underscores in persisted status or infer original GTID tags. Future correct parsing does not reconstruct metadata already corrupted by an older controller.

---

## 14. Backup completed with incomplete metadata

### Symptoms

- `BackupMetadataIncomplete` Event or `MetadataComplete=False` on a MysqlBackup.
- A Succeeded backup has a location but missing size, GTID, or binlog observations.
- An older Succeeded record has an empty or underscore-damaged location.

### Diagnostic steps

1. Run `triage.sh` or collect `support-bundle.sh`. Their backup projections include phase, producing Job, location and metadata condition; Job projections include the owner UID and main-container storage/output fields. They do not collect credential or passphrase contents.
2. Inspect `MetadataComplete`: `SentinelUnavailable` means a pod-list, missing completed Pod, log-stream, scanner, or bounded-read failure; `SentinelMissing` means readable logs lacked a usable completion sentinel. Check access to the completed Pod's main-container logs and API/kubelet availability. `LocationMismatch` means the Job destination was used instead of a different sentinel value.
3. Compare `status.location` with the owned Job's effective main-container output using playbook 13's S3/PVC conventions. Kubernetes expands `$(NAME)` from earlier known environment values and reduces `$$` once; a raw env template may differ from the producer's path. Runtime Secret/ConfigMap, downward-API and service values unavailable from the Job cannot be inferred; a valid producer-reported location is retained instead. A legacy record without `MetadataComplete` keeps its producer-reported path, even if it contains literal `$(NAME)`. Only a recorded Job fallback permits raw-template repair. Consumer Jobs double literal dollars in their env values, so apply one kubelet expansion before comparing their paths with status. Do not collect those Secrets or use encrypted staging paths or a changed profile to guess a destination.
4. `DestinationUnavailable` leaves `Ready=False`. On first completion it returns a reconcile error; on an already-Succeeded record it records a terminal repair outcome without error so retention can continue. Successful metadata repair preserves a True Ready condition; cleanup-only location recovery does not make a Running or Failed backup ready. Deletion of an unrecoverable record emits `ArtifactCleanupSkipped` naming the Job and releases the finalizer; inspect storage separately for any unlocated artifact.
5. Deferred positive size observations can refresh the last-size gauge when the repaired backup is not older than the latest observed success. They do not repeat terminal counters, completion timestamps or Events, and older repairs do not replace newer sizes.

### Safe remediation

1. Restore pod-log reachability while the operator retries every 15 seconds for up to ten minutes from completion. The recovery state is persisted, so restarting the operator does not lose the retry. An empty GTID or zero size in a collected sentinel is valid and does not trigger repeated collection.
2. If logs are gone permanently, use the recorded Job-derived location for restore or verification. Optional observations can remain absent; the backup's successful phase still reflects its completed Job and known artifact destination. Repeated repairs do not repeat terminal counters or completion Events.
3. For historical missing or underscore-damaged locations, reconcile with the fixed operator while the original owned Job is retained. The operator verifies the original destination and repairs the record, including before deletion. If that Job is gone, establish the artifact through approved storage inspection or take a replacement backup. Never invent location substitutions or remove a cleanup finalizer based on a guessed path.

---

## 15. Scheduled backup fails without the expected retry

### Symptoms

- A scheduled MysqlBackup failed with configured retry attempts remaining, but no successor appears.
- A retry's `status.attempt` is temporarily zero or lower than its launch attempt after a failed status patch.
- `BackupRetryConflict` warns that an existing retry backup has different lineage.

### Diagnostic steps

1. Check `spec.backup.retry.maxAttempts`, the failed run's completion time, and `status.backupSchedules[].nextRetryTime`. Backoff doubles per attempt up to the configured cap; ad-hoc backups do not participate.
2. Use the backup projections in `triage.sh` or `support-bundle.sh`. Match the failed run's UID to a successor's `shipstream.io/backup-retry-of` label. The root name/UID annotations identify the original scheduled run, and `shipstream.io/backup-attempt` is durable launch metadata. A retained higher attempt from another run does not prove that this run has a successor.
3. Compare status and launch attempts. Retry limits use the greater value; a lost status patch does not reset a chain to attempt 1. The backup reconciler restores status from the annotation before launch or terminal observation.
4. New names use `backup-retry-<predecessor UID hash>-r<N>`. Repeated reconciles and stale lists reuse that name and verify predecessor, root, attempt, schedule, group and profile. `BackupRetryConflict` means a name collision or altered metadata; inspect those public fields without changing credential Secrets. `status.backupSchedules[].retryConflict` names the blocking object. The operator emits the warning and log when that persisted marker changes, clears it when the conflict is no longer observed, and warns again if a resolved conflict recurs. Reconciles and operator restarts do not repeat an unchanged conflict warning. Only that retry is skipped; other schedules and group status rollups continue. An AlreadyExists/cache-NotFound race requests a five-second wake-up instead of group-wide error backoff; genuine API read failures still propagate.

### Safe remediation

1. If running an affected older operator, upgrade to the per-run lineage fix. Do not delete successful or failed history to make retries work; each new run receives its own chain.
2. If backoff is pending, allow the scheduled wake-up. If a legitimate successor exists, investigate that backup's own phase and Job instead of forcing another retry of its predecessor.
3. For a conflicting identity, establish the source of the conflicting object or metadata edit before acting. Do not rewrite lineage annotations or attempt counters to bypass `maxAttempts`. If an immediate backup is needed, use `kubectl bloodraven backup <group> -n <ns>` for an explicit new run.

---

## 16. Verification source does not match the expected backup

### Symptoms

- A newer successful backup appears while a verification is still running.
- `BackupNotAvailable` after a selected source was deleted or recreated before Job launch.
- `BackupIdentityMismatch` on a running legacy verification, or disagreement between status and the Job's source.

### Diagnostic steps

1. Collect the verification name, phase, Job name and `status.backupRef` name/UID/location/storage type. The triage and support-bundle projections include these public fields. Compare the selected UID rather than relying on a reused backup name or today's latest successful backup.
2. Inspect the producing Job's pod-template `shipstream.io/verification-backup` JSON annotation. New Jobs pin the same tuple in immutable Job spec; the operator checks it against the source env and restores missing or drifted status from it. Existing Jobs are observed without re-fetching the source CR or current profile/derived credentials.
3. For plaintext, compare the main container's `BLOODRAVEN_INPUT_URL` with the pinned location using the normal S3/PVC trailing-slash convention. For encrypted sources, inspect `decrypt-download` storage, bucket and `BLOODRAVEN_SOURCE_PREFIX`; its target and the main input are temporary decrypted paths. Do not collect passphrases, Secrets or arbitrary env.
4. A legacy Job without the snapshot can continue only with a complete status reference matching its source inputs. If the status claims a different location, or its identity is incomplete, the fixed operator fails it with `BackupIdentityMismatch` instead of certifying another backup. Historical terminal records produced by an older operator still require comparison with retained Job evidence.

### Safe remediation

1. For a running Job whose original source is still recorded, allow it to finish. Newer backups or edits to `spec.backupRef` do not change its execution identity. Create another verification to test the newer backup.
2. If the pinned source disappeared or its UID changed before launch, create a new verification for the intended source. The operator does not silently re-pin the old run, even when its spec requested latest selection.
3. For identity mismatch, preserve the recorded reference and Job inputs for investigation, then start a new verification with an explicit intended source. Do not patch the audit record to make a different backup appear verified. Pinning identifies the source but does not hold stored artifacts against retention or external deletion.

## 17. Restore Job failed on `Read-only file system` / `load-progress`

Restore Jobs mount the dump read-only: source PVCs at `/restore` or `/backups`, and decrypted encrypted backups at `/restore-decrypted`. Operators built before load-progress tracking was disabled for restores let mysqlsh try to write `load-progress.<uuid>.json` next to the dump. Every PVC-sourced or encrypted restore then failed.

### Symptoms
- Restore Job logs show `BLOODRAVEN_LOAD_FAILED` mentioning `load-progress` and `Read-only file system` (EROFS).
- For an in-place restore, the Job logs `BLOODRAVEN_DROP_SCHEMA` / `BLOODRAVEN_DROP_ALL_USER_SCHEMAS_DONE` before the load failure, so the target schemas are already gone. `status.restoreInPlace.phase` is `Failed`.

### Diagnostic steps
1. Confirm the Job's main container disables progress tracking. On a fixed operator the variable is present with an empty value:
   ```bash
   kubectl get job -n <ns> <restore-job> -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="BLOODRAVEN_LOAD_PROGRESS_FILE")]}'
   ```
   No output means the Job was built by an older operator.
2. For in-place, list what survived on the primary: `SHOW DATABASES`.

### Safe remediation
1. Upgrade the operator. `loadOptions.resetProgress` has no effect on restores, and changing it does not help.
2. Bootstrap: delete the failed restore Job; the reconciler rebuilds it. Restore Jobs no longer resume a partial load. If the failed Job's log shows `BLOODRAVEN_LOAD_START`, empty the target first (delete the new group's MySQL PVCs, or `./playground/reset-mysql.sh`), or the reload stops on objects the first attempt created.
3. In-place: bump `spec.restoreInPlace.confirm` to a newer RFC 3339 timestamp to re-run the restore. It drops and reloads the same scope, which is what is needed when the earlier attempt dropped the schemas and never loaded them.

## 18. Bootstrap PITR restore fails with duplicate-key or missing-row errors

A PITR replay pipes archived binlogs, with their original GTIDs, into the new primary. The archive includes transactions the dump already contains. MySQL skips them only when the dump's GTID set is in the target's `gtid_purged`. The operator loads bootstrap PITR restores with `updateGtidSet: "replace"` and `skipBinlog: true` to install that set. Operators built before that change did not, so replay re-executed pre-dump transactions.

### Symptoms
- Restore Job logs `BLOODRAVEN_LOAD_COMPLETE`, then `BLOODRAVEN_PITR_FAILED` with MySQL error 1062 (duplicate entry), 1032 (row not found), 1050 (table exists) or 1396 (user exists).
- `status.restore.phase` is `Failed`.

### Diagnostic steps
1. Check the Job's load options:
   ```bash
   kubectl get job -n <ns> <restore-job> -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="BLOODRAVEN_LOAD_OPTIONS")].value}'
   ```
   A fixed operator shows `"updateGtidSet":"replace"` and `"skipBinlog":true` when `initFromBackup.pointInTime` is set.
2. On the target, compare `SELECT @@GLOBAL.gtid_purged` with the dump's `gtidExecuted` (`@.json` in the backup, or `status.gtidExecuted` on the MysqlBackup). After a fixed load they match.

### Safe remediation
1. Upgrade the operator.
2. The target was fresh, so nothing outside it was changed. Reset it and run the restore again:
   - Playground: `./playground/reset-mysql.sh`.
   - Production: scale the new group's MySQL Deployments to 0 (or delete their pods), then delete their PVCs, or recreate the group.
   - Delete the failed restore Job (`kubectl delete job -n <ns> <restore-job>`). While it exists the reconciler keeps reporting `Failed` instead of rebuilding it.
3. Error 1146 or 1049 during replay rather than 1062/1032 means the replay touched a schema the dump or load left out (`dumpSchemas`, `loadOptions.includeSchemas`/`excludeSchemas`). Restore from an instance dump, without load-time schema filters.
4. Do not work around it with `pointInTime.excludeGtids` set to the dump's GTID set. That skips the in-dump transactions but leaves `gtid_purged` without the dump's history.

## 19. In-place restore with `pointInTime` rejected, or an earlier one stopped at the backup

In-place PITR cannot work against a live primary. The replay keeps the archived transactions' original GTIDs, and the primary has already executed every one of them, so MySQL skips them all. Current operators reject `spec.restoreInPlace.pointInTime` in Preflight. Earlier operators ran the replay anyway, logged `BLOODRAVEN_PITR_COMPLETE` and reported `Succeeded`, with the data left at the backup.

### Symptoms
- `status.restoreInPlace.phase: Failed`, `confirmTokenUsed` unchanged (the confirm of the last restore that ran, empty if none), message `restoreInPlace.pointInTime is not supported: ...`, and a `RestoreInPlaceRejected` warning event. No Job exists and nothing was fenced.
- Or, after an older operator ran in-place PITR: rows written between the backup and `stopDatetime` are missing, although the restore reported success.

### Diagnostic steps
1. Confirm the spec: `kubectl get mfg <group> -n <ns> -o jsonpath='{.spec.restoreInPlace.pointInTime}'`.
2. For a past run, compare the expected post-backup rows with what the primary holds. An older operator's `status.restoreInPlace.pitrReplayedBinlogCount > 0` does not mean anything was applied (the field is gone in v2.0.0).

### Safe remediation
1. To restore to the backup itself, remove `pointInTime`; the same `confirm` re-runs.
2. To reach a point in time, restore into a new failover group with `spec.initFromBackup.pointInTime` (fresh target, so the dump's GTID set is installed and the replay applies). Then move the data or traffic across. For a single tenant schema, dump it from the recovered group with `mysqlsh util.dumpSchemas` and run a per-schema in-place restore from that dump.
3. Do not try to force replay on the live primary with `RESET BINARY LOGS AND GTIDS` or `mysqlbinlog --skip-gtids`. The first destroys unarchived binlogs and archive continuity. The second re-applies in-dump and cross-site duplicate transactions.

## 20. Peers not yet recloned after a full-instance in-place restore

A full-instance in-place restore replaces the primary's data. The other sites still have the old data until each is recloned from the restored primary. The operator tracks them in `status.restoreReclones`. The queue is `Armed` while the restore runs and `Draining` after it succeeds. It is `Held` when a later restore that replaced it ran on the primary and failed: nothing is cloned from that primary. The operator reclones the sites one at a time through the `reclone-site` annotation and removes each one only after its clone succeeds. `status.restoreInPlace.phase: Succeeded` does not mean the peers are done.

### Symptoms
- `status.restoreReclones.pending` is non-empty long after the restore succeeded.
- `RestoreRecloneFailed` events, or `status.restoreReclones.failures` lists a site.
- `status.restoreReclones.phase: Held` and a `RestoreReclonesHeld` event: a full-instance restore ran its Job on the primary, failed, and changed the primary. Every pending site keeps its replication receiver stopped and the data it had before the restore; nothing is cloned or replicated into it. A reclone annotation for a held (or `Armed`) site is refused with `RecloneRejected` and removed.
- Every queued site shows `Replica_IO_Running: No` (or `performance_schema.replication_connection_status.SERVICE_STATE = OFF`), `sourceConvergenceReason: RestorePeerHold`, `Degraded=RestoreReclonePending`, and `bloodraven_replication_running{thread="io"}=0`. This is by design: the restore stopped the receivers before its Job dropped anything, and only each site's reclone restarts replication.
- A full-instance restore waits in Preflight with `waiting for the running clone of <site> to finish before restoring`: a clone was running when the restore was accepted. It proceeds once the clone ends.
- `PromotionRefusedPendingReclone` events, or `Degraded=NoPrimary` with a `pending post-restore reclone` message after the primary failed.
- Readers return old data from a site that is still queued. A queued site is labelled `shipstream.io/healthy=no`, which takes it out of the `-replica` Service and a read-only site's Service; the per-site Service of a primary-candidate or dr-only site does not select on it, so check for clients that connect there directly.

### Diagnostic steps
1. Queue state:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.status.restoreReclones}' | jq .
   kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/reclone-site}'
   ```
   `requested` is the site whose clone is running, and the annotation should name it. If the annotation names a different site, someone else's reclone request is pending. The queue waits for it and never overwrites it.
2. Clone progress and failures: the `Bootstrapping` condition, and operator logs `msg="starting bootstrap" source="reclone"`, `bootstrap failed`, `bootstrap completed successfully`.
3. Events: `kubectl get events -n <ns> --field-selector involvedObject.name=<group> | grep -E 'RestoreReclone|PromotionRefusedPendingReclone|RecloneRequested'`.

### Safe remediation
1. A failing clone: fix the cause (donor reachability, the clone plugin, recipient disk space, a sealed keyring) and let the queue retry. Do not set your own reclone annotation for a queued site. The queue would wait for it, and a clone it did not request does not remove the site.
2. The primary failed while only queued sites remain: prefer bringing the primary back; the queue then resumes. Promoting a queued site means serving its pre-restore data:
   - With the primary up there is no in-place promotion: a queued site's receiver is stopped, so it cannot catch up and a planned failover to it is refused (`TargetUnhealthy`).
   - With the primary down: clear the queue with `kubectl patch mfg <group> -n <ns> --subresource=status --type=json -p '[{"op":"remove","path":"/status/restoreReclones"}]'`, after which automatic failover proceeds. Then reclone the remaining sites by hand from whichever primary you keep.
3. `RestoreReclonesDiscarded` after a failed restore means the restore left the primary unchanged: it never created its Job, or the primary's `gtid_executed` still equals `status.restoreReclones.fencedGtidExecuted` (for example the Job failed before its `DROP`). The peers resume replicating and are eligible for promotion again. If the failed restore had replaced an earlier restore's queue, a `RestoreReclonesRestored` event means that queue is draining again. If the event says the primary is unchanged but the Job ran, check the primary: a load without binlogging into a primary that had no user schemas changes data without GTIDs.
4. A `Held` queue (`RestoreReclonesHeld`): the primary may hold the failed restore's dropped or half-loaded data, and the held sites keep their pre-restore data with replication stopped. Do not `START REPLICA` on a held site and do not clone from the primary until its data is checked. Scheduled backups skip the held sites (they do not replicate) and fall back to the primary: suspend schedules whose retention could age out the last good backup. A `RestorePeerReplicationIncomplete` event names a held site that is missing its last pre-fence transactions. Then choose one:
   - Restore the held data into the primary: create a `MysqlBackup` with `spec.sourceSiteOverride: <held-site>`, then run a new full-instance restore from it (it reclones every peer once it succeeds).
   - Keep the primary's data once it is verified good: `kubectl patch mfg <group> -n <ns> --subresource=status --type=merge -p '{"status":{"restoreReclones":{"phase":"Draining"}}}'`; the held sites are recloned from it.
   - Remove `status.restoreReclones`: this resumes the held sites' replication from the current primary, which replicates whatever the failed restore did to it.
   A `Held` queue keeps a site removed from `spec.sites`, so re-adding it does not resume its replication.

## 21. In-place restore stays fenced after `spec.restoreInPlace` was removed, or ends "cancelled"

Once a restore is accepted, `status.restoreInPlace` drives it, not the spec. Removing the field never interrupts a running restore Job. The `-primary` Service fence, the topology freeze (no failover or reclone), and the planned-failover, deployment and `MysqlDatabase` guards stay up until that Job finishes. A run removed before its Job existed is cancelled.

### Symptoms
- `spec.restoreInPlace` is gone, but `status.restoreInPlace.phase` is still `Restoring` and the message reads `spec.restoreInPlace was removed; holding the fence until restore Job ... finishes`. There is a `RestoreInPlaceSpecRemoved` event.
- Or `phase: Failed` with `cancelled: spec.restoreInPlace was removed ...` (`RestoreInPlaceCancelled`): nothing was restored.
- Or `phase: Failed` with `RestoreInPlaceJobLost`: the run's Job vanished before its outcome was seen. `confirmTokenUsed` is that run's confirm, because its Job may have run. This also applies when `jobName` is empty but `jobCreating: true`: the operator had started creating the Job when its status write or the process was lost.

### Diagnostic steps
1. `kubectl get mfg <group> -n <ns> -o jsonpath='{.status.restoreInPlace}' | jq .` for the phase, `jobName`, `scope`, `confirmTokenUsed` and, while a run is in flight, `runConfirm` (that run's confirm).
2. `kubectl get job -n <ns> <jobName>` and its pod logs (`BLOODRAVEN_DROP_*`, `BLOODRAVEN_LOAD_*` sentinels) show how far the restore got.

### Safe remediation
1. A held fence clears by itself when the Job finishes; the run then completes as `Succeeded` or `Failed`. Do not delete the Job to "unstick" it: that kills the loader mid-DROP or mid-load and leaves a partially restored primary.
2. After `RestoreInPlaceJobLost`, verify the primary's data (the restore may have partly run) before relying on it. Then re-run the restore with a newer `confirm`, or restore from backup.
3. Do not edit `status.restoreInPlace` to clear it. `confirmTokenUsed` is what stops an old manifest from re-running a restore that already ran.

## 22. Full-instance restore stuck in Fencing, or applications locked out after a restore (`offline_mode`)

A full-instance in-place restore fences the primary with `SET GLOBAL offline_mode = ON`. MySQL then disconnects and refuses every session without `CONNECTION_ADMIN`/`SUPER`, with error 3032 `ER_SERVER_OFFLINE_MODE`. The fence is recorded in `status.restoreOfflineFence` (`site`, `confirmToken`, `runID`, `priorOfflineMode`, `sessionsEvicted`). The restore Job starts only after no client session from before the fence remains. The operator puts the prior value back when the restore ends.

### Symptoms
- `Failed` with `restoreInPlace rejected: the operator MySQL account lacks <privileges>` (any of `CONNECTION_ADMIN`, `SYSTEM_VARIABLES_ADMIN`, `PROCESS`, `REPLICATION_SLAVE_ADMIN`): preflight refused the restore (nothing fenced, confirm not consumed).
- Stuck in `Fencing` with a message such as `waiting to read offline_mode`, `waiting to apply the offline_mode fence`, or `offline_mode is ON on <site>; waiting for N application session(s) to disconnect`.
- After the restore, applications get `ERROR 3032 ... offline mode`, with `RestoreOfflineFenceReleaseFailed` events and `status.restoreOfflineFence` still set.
- During the restore every peer's replication receiver is stopped (`RestorePeerReplicationStopped`), whatever the replication account, and non-admin monitoring of the primary has gaps. Both are expected.
- Stuck in `Fencing` with `waiting to stop replication on peer <site>: ...` (unreachable peer, refused `STOP REPLICA IO_THREAD`), `receiving the primary's last transactions` (a running receiver catching up, at most 30 s), or `waiting for the running ordered update / planned failover / clone`. After two minutes the run ends `Failed` with `RestoreInPlacePeerStopFailed`: nothing was restored, the confirm is consumed, and `offline_mode` is released.
- Stuck in `Preflight` with `waiting for peer <site> to be reachable: ...`: a peer is unreachable. Nothing is fenced yet. `Failed` with `... lacks REPLICATION_SLAVE_ADMIN on <peer>` rejects the restore without consuming the confirm.
- A new `confirm` does not start a new run while `status.restoreReclones` is still `Armed` from the previous restore: the previous queue is settled first (up to ten minutes while that restore's Job pods are still running), with a `RestoreInPlaceWaitingForPreviousQueue` event. `status.restoreInPlace` keeps showing the previous run meanwhile, and `offline_mode` stays enforced on the primary (re-asserted after a `mysqld` restart) until the settlement finishes.
- `waiting for the planned failover to finish` in Preflight: a planned failover already in flight is still driven to completion; the restore starts afterwards.
- Stuck in `Preflight` or `Fencing` with `waiting for the clone admission of <site> to finish` or `waiting for the running clone of <site> to finish`: a clone was starting or running when the restore began. It finishes (or is refused with `BootstrapRefusedRestoreHold`) first.
- `status.restoreInPlace` is `Failed` but `offline_mode` stays ON and `status.restoreReclones` stays `Armed`: a pod of the failed restore's Job has not terminated (check `kubectl get pods -l batch.kubernetes.io/job-name=<job>`), or the primary is briefly unreadable. The operator waits up to ten minutes (two for an unreadable primary) before holding the peers, because the pod could still drop schemas. Do not lift `offline_mode` by hand meanwhile; delete a stuck pod only once you know it is not mid-restore.
- A `RestoreOfflineFenceSessionsKept` Warning: the operator account lacks `SYSTEM_USER` and left the named accounts' sessions connected.

### Diagnostic steps
1. `kubectl get mfg <group> -n <ns> -o jsonpath='{.status.restoreOfflineFence}{"\n"}{.status.restoreInPlace.phase} {.status.restoreInPlace.message}'`
2. On the primary: `SELECT @@GLOBAL.offline_mode;` and `SELECT ID, USER, HOST, COMMAND, TIME FROM information_schema.PROCESSLIST;`. Sessions that keep coming back belong to accounts with `CONNECTION_ADMIN`/`SUPER`; they are not fenced and do not block the wait. A session that will not die (still `Query` after KILL) does block it.
3. Operator account privileges: `SHOW GRANTS FOR '<operator-user>'@'%';` must include `ALL PRIVILEGES ON *.*`, or `CONNECTION_ADMIN`, `SYSTEM_VARIABLES_ADMIN`, `PROCESS` and `REPLICATION_SLAVE_ADMIN` (`SUPER` stands in for all but `PROCESS`).

### Safe remediation
1. Missing privilege: grant the ones the message names, e.g. `GRANT CONNECTION_ADMIN, SYSTEM_VARIABLES_ADMIN, PROCESS, REPLICATION_SLAVE_ADMIN ON *.* TO '<operator-user>'@'%';` (the restore also needs DDL and DML). The operator re-checks about once a minute with the same `confirm`; nothing else is needed.
2. Fencing waits on a stuck session: find it in the processlist and `KILL <id>`. Do not lift `offline_mode` while the restore is in `Fencing`, `Restoring` or `Resuming`; the operator re-asserts it every poll anyway.
3. Locked out after a terminal restore because the release keeps failing: fix operator reachability to the primary. If applications must come back now, run `SET GLOBAL offline_mode = OFF;` on the primary. If `priorOfflineMode` was `true`, the primary was already in offline mode before the restore; leave it ON. The operator clears the record once its release succeeds.
4. Applications running as accounts with `CONNECTION_ADMIN`/`SUPER` are not fenced at all. Remove those privileges from application accounts.
5. After an operator downgrade during a full-instance restore, nothing releases the fence (older operators do not know `status.restoreOfflineFence`). Once the restore is terminal, run `SET GLOBAL offline_mode = OFF;` on the primary unless `priorOfflineMode` was `true`.
6. Fencing waits on a peer, or the run ended `RestoreInPlacePeerStopFailed`: restore the operator's connectivity to the peer (or its `REPLICATION_SLAVE_ADMIN` grant), or remove the peer from `spec.sites`, then bump `spec.restoreInPlace.confirm`. Do not stop the peer's replication by hand to get past the check and then start it again: while a full-instance restore runs or its queue is `Armed`/`Held`, a running receiver pulls the restore's `DROP` statements. Do not downgrade the operator while `status.restoreReclones` is `Armed` or `Held`: an older operator restarts the held receivers.

## 23. Ordered update stalls on the active site (`maintenance handoff aborted`)

With `updateStrategy: OrderedUpdate`, the operator rolls drifted followers first. It then hands the primary role to a healthy standby, and only after that rolls the old active site. The handoff promotes the standby only once its `GTID_EXECUTED` contains everything the fenced active site committed. If that cannot be proven, the handoff aborts. Nothing is lost when it aborts, but the active site keeps its old spec until a later attempt succeeds.

### Symptoms
- Operator log: WARN `maintenance handoff refused; source not fenced` (refused before any write pause, usually a lagging standby) or `maintenance handoff aborted; source unfenced` (aborted after the fence), followed by `ordered update failed`, repeating on later reconciles. The active site's Deployment hash stays stale.
- Or ERROR `maintenance handoff: rollback failed: source may be left read-only`: the abort could not give the active site its writes back.
- Or, after a successful handoff, ERROR `ordered update: old active has transactions the new primary lacks; not repointing it`, or an `ordered update failed` error containing `before repointing`. The new primary is serving; the old active is fenced (`super_read_only=ON`, unless the error is `fence old active ... before repointing`) and is not replicating.

### Diagnostic steps
1. Read the reason from the `error` field:
   - `did not reach the source GTID set before fencing` (on the `refused` line): the standby is lagging; no write pause happened.
   - `did not reach the fenced source GTID set ... N transactions missing`: the standby is still behind after the 30s fenced budget.
   - `active primary became unreachable during ordered update`: the active site stopped answering. Emergency failover takes over; see the failover runbooks.
   - `no longer contains the fenced source set` (after `STOP REPLICA` or `RESET REPLICA ALL`): the standby's server lost history after the catch-up proof, for example a replaced pod. Treat it as a suspect replica and compare its data before anything else.
   - `transactions the source lacks` (on WARN `maintenance handoff refused; candidate has transactions the source lacks`, or in an `aborted` error after `STOP REPLICA` / `RESET REPLICA ALL`): the standby is divergent. Never promote it to get the update through: use playbook 33.
   - `is divergent (N transactions the primary lacks); reclone or reconcile before the active site can be updated` (on `ordered update failed`, or WARN `ordered update: active-site update stalled; standby is divergent`): the operator has recorded the divergence and does not pick the standby. Playbook 33.
   - `is not Ready` or `did not become Ready` (on `ordered update failed`, or WARN `ordered update: stalled; site pod not Ready`): a pod, not MySQL, holds the update. Playbook 34.
   - `candidate SQL thread error`: the applier stopped.
   - `unparseable GTID set`, or a read error.
2. Compare the sets. The standby's set should contain the active site's:
   ```bash
   kubectl exec -n <ns> <active-pod> -c mysql -- mysql -N -e "SELECT @@GLOBAL.gtid_executed"
   kubectl exec -n <ns> <standby-pod> -c mysql -- mysql -N -e "SHOW REPLICA STATUS\G" | grep -E 'Running|Seconds_Behind|Last_.*Error|Gtid_Set'
   ```
   `scripts/gtid-audit.sh -n <ns> <group>` prints every site's set in one pass.
3. After a rollback failure, check the active site: `SELECT @@GLOBAL.super_read_only, @@GLOBAL.read_only`.

### Safe remediation
1. Lagging standby: let it catch up. Reduce the write load or fix the receiver's network path. The handoff retries on its own, and nothing needs to be run.
2. Applier error: treat it as a broken replica (playbook 2). Do not skip transactions to make the handoff pass.
3. Rollback failed and the active site is read-only, and no other site is writable: clear the fence on the active site only (`SET GLOBAL read_only = OFF;`, which also clears `super_read_only`). Before you do, confirm that the standby is still read-only and still replicating from the active site.
4. Standby left without replication after the abort (WARN `failed to restore candidate replication channel after abort` or `failed to restart replication on candidate after abort`; `SHOW REPLICA STATUS` empty or threads stopped). Nothing re-points it automatically.
   - Confirm the standby is read-only.
   - Confirm its `@@GLOBAL.gtid_executed` is a subset of the active site's: `SELECT GTID_SUBSET('<standby set>', '<active set>')` on the active site returns 1.
   - If it is, run on the standby: `CHANGE REPLICATION SOURCE TO SOURCE_HOST='<active internal host>', SOURCE_USER='<repl user>', SOURCE_PASSWORD='<repl password>', SOURCE_AUTO_POSITION=1; START REPLICA;`. Add `SOURCE_SSL=1, SOURCE_SSL_CA='/etc/mysql/tls/ca.crt'` when `spec.tls` is set, otherwise `GET_SOURCE_PUBLIC_KEY=1`.
   - If it is not a subset, the standby has errant transactions: reclone it (`kubectl bloodraven reclone <group> <site> -n <ns>`) instead, or see playbook 33.
5. Old active not repointed after the handoff (`not repointing it` or `before repointing`). Leave it fenced and do not run `CHANGE REPLICATION SOURCE` on it by hand. Old-primary recovery picks it up on a later poll:
   - If the fence failed or a set was only unreadable, recovery fences the site itself, compares the sets, and rejoins it as a replica when nothing is missing from the new primary. Until then a site whose fence failed has `read_only=ON` but may lack `super_read_only`: check `SELECT @@GLOBAL.super_read_only, @@GLOBAL.read_only` on it.
   - If it really holds transactions the new primary lacks, recovery reports them in `status.sites[].divergentGtid` (`divergenceDetectedBy=OldPrimaryRecovery`) and blocks: follow playbook 2 to extract or replay them before recloning the site.
   - If the old active came back on an empty datadir (a replaced or wiped PVC), its set holds only server-init transactions under a new `server_uuid`. The rollout still refuses and pages, but recovery treats the site as empty and leaves it to auto-clone: no `divergentGtid` appears. Confirm with `SELECT @@server_uuid, @@GLOBAL.gtid_executed` on it (no server UUID shared with the new primary; GTID tags are ignored, so `uuid:tag` and `uuid` count as shared. A site that shares a server UUID but holds transactions the primary lacks is blocked as divergent, not cloned) and let the clone run.

## 24. One group stopped updating (no running topology manager)

A healthy operator pod does not guarantee that every group has a live topology manager. The runner restarts a group's manager when its spec or connection material changes. If the restart fails, the group has no manager, and so no polling, no status writes and no failover, until the runner's 30s retry succeeds.

### Symptoms
- One group's `status.sites[]` and conditions stop changing while other groups keep updating.
- Losing the primary does not trigger failover.
- Operator log: ERROR `failed to start topology manager` for that `fg`, repeating every 30s.
- Warning Event `TopologyManagerStartFailed` on the group, carrying the same error. It is rate-limited (at once when a running manager was torn down, otherwise after one 30s retry, then at most every 5 minutes), so read the log for the latest attempt. A Normal `TopologyManagerStarted` Event marks a recovery the same operator process saw.
- These Events are history, not current state, in either direction. A group that recovered and broke again within 5 minutes can still show `TopologyManagerStarted` last. The operator forgets a failure episode when it restarts, so a group fixed after an operator restart never gets `TopologyManagerStarted` and can still show `TopologyManagerStartFailed` last. Before declaring the manager absent, confirm it from current evidence: the ERROR repeating every 30s in the last couple of minutes of operator log, and `status.sites[].lastSeen` no longer advancing.

### Diagnostic steps
1. Read the error:
   ```bash
   kubectl get events -n <ns> --field-selector involvedObject.name=<group>,reason=TopologyManagerStartFailed
   kubectl logs -n bloodraven deploy/bloodraven | grep -E 'failed to start topology manager|topology manager exited|connection material unreadable' | grep '<ns>/<group>'
   # Current state: a line every 30s here means no manager right now.
   kubectl logs -n bloodraven deploy/bloodraven --since=2m | grep 'failed to start topology manager' | grep '<ns>/<group>'
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{range .status.sites[*]}{.name}{" lastSeen="}{.lastSeen}{"\n"}{end}'
   ```
2. Typical causes:
   - The operator Secret is missing (`get operator secret ... not found`).
   - It has no `dsn` key (DSN mode).
   - The `spec.tls.secretName` Secret is missing or its material does not load.
3. A WARN `connection material unreadable; keeping the running topology manager` is not this failure. The manager is still running on its existing connections while the Secret is being replaced.

### Safe remediation
1. Restore or fix the Secret named in the error. The next sync starts a fresh manager. You do not need to restart the operator.
2. Do not promote by hand while the group has no manager unless the primary is actually down. Once the manager is back, it re-evaluates topology on its first poll.

## 25. Encrypted PITR sidecar fails to init archive store

### Symptoms
- Sidecar logs repeat `failed to init archive store` with `permission denied` for the passphrase file.
- Current sidecar images keep running: the pod stays Ready and in its Services, HTTP and self-fencing work, and `/archiver/status` reports `enabled:true`, `storeReady:false`, `lastError: archive store init: ... permission denied`. The group shows `PITRArchiveReady=False` with reason `ArchiveStoreInitFailed` and one Warning `PITRArchiveNotReady` Event. Archival is stopped on that site.
- Sidecar images that predate that behaviour exit instead: the container crash-loops, HTTP probes fail, the pod is unready, and archival and sidecar fencing are unavailable. The operator then reports `SidecarUnreachable`.

### Cause
Older operators project `pitr-passphrase` as root-owned mode `0400`, but PITR sidecars default to UID 999 and MySQL pods have no fsGroup by default. Archive-store initialization cannot read the file. A current sidecar retries it with backoff (5 s doubling to 5 min) and keeps every other service running; an older sidecar exits before starting its HTTP server or fencing monitor.

### Diagnostic steps
1. Read the sidecar log (add `--previous` for an older, crash-looping image):
   ```bash
   kubectl logs -n <ns> <pod> -c sidecar --tail=100 | grep 'archive store'
   ```
2. Inspect the pod's Secret mode and security contexts without reading Secret contents:
   ```bash
   kubectl get pod -n <ns> <pod> -o jsonpath='{.spec.volumes[?(@.name=="pitr-passphrase")].secret.defaultMode}{"\n"}{.spec.securityContext}{"\n"}{.spec.containers[?(@.name=="sidecar")].securityContext}{"\n"}'
   ```
   JSON reports decimal modes: `256` is `0400`; `292` is `0444`. Without fsGroup, a non-root UID cannot read the root-owned `0400` projection. On a current sidecar `/archiver/status` names the cause in `lastError`; on an older one it is unavailable while the container crash-loops, so use logs.

### Safe remediation
Upgrade to the operator that projects the live sidecar passphrase as `0444`, matching PITR S3 credentials. Its PITR render-version change triggers ordered updates for existing deployments. Verify the new pod's projection mode, readiness, and `/archiver/status` (`storeReady:true`) after the rollout, and that `PITRArchiveReady` returns to `True`. Backup and restore Job projections keep their separate fsGroup-based permissions. Preserve the existing passphrase; adding fsGroup to an existing MySQL PVC can trigger a recursive ownership change and is unnecessary for this fix.

## 26. PITR archival stops after an encryption change

### Symptoms
- The sidecar still has the previous passphrase reference/key while an ordered rollout is pending, or on an older operator that omitted encryption from drift detection.
- After rollout, `/archiver/status` and sidecar logs report a manifest-read error, missing BRV1 magic, or authentication failure; archive coverage stops advancing.

### Cause
The sidecar loads its passphrase at startup. The deployment fingerprint includes the active PITR profile's effective encryption settings and a per-group keyed HMAC of its selected passphrase bytes in both credential modes. Updating projected Secret files alone cannot reload the store. A completed rollout applies the new settings but cannot read an archive encrypted under a different passphrase; strict readers also reject old plaintext manifests after enabling encryption.

### Diagnostic steps
1. Compare the active PITR profile's Secret name and key with the Deployment's `pitr-passphrase` projection and `BLOODRAVEN_PITR_PASSPHRASE_FILE`. Inspect `shipstream.io/spec-hash` and the group's update status to distinguish pending rollout from archive incompatibility. Never print Secret data or an unkeyed hash of it.
2. Read sidecar logs and probe the archiver:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n <ns> <group> archiver
   ```
   If HTTP is unavailable, inspect current and previous startup logs as in playbook 25.

### Safe remediation
Preserve the archive and its original passphrase. Restore access to the original settings and key before attempting to use that recovery history. Do not delete the manifest, overwrite old objects, or enable plaintext fallback as a drift repair. Automatic archive migration and multiple key generations are unsupported; plan any encryption transition separately. For a stale rendered configuration, use the operator with encryption-aware drift detection and verify the normal ordered rollout completes.

## 27. PITR history after a site reclone or data rebuild

### Symptoms
- A former primary is rebuilt, becomes primary again, and reuses `mysql-bin.000001` or other retained binlog names.
- Older archivers may report `backlogFiles=0` while missing the new files, because they recognized only the basename.

### Diagnostic steps
1. Record sidecar, operator, and backup/restore image versions. Confirm all archive readers and writers understand the additive manifest `id` field before using new archives with older images.
2. Inspect `/archiver/status`, upload logs, and the relevant site manifest through the configured archive reader. New rows use `(name, id)` where `id` is a SHA-256 of sealed plaintext bytes for plaintext stores, or a passphrase-keyed HMAC-SHA256 digest for encrypted stores, and new keys end in `<name>.<id>`. Two rows may share a name. Legacy rows without IDs retain plain keys and persist a verified `contentId` after archive inspection. A surviving rollback-stripped row recovers its ID only from an exact confined `remotePath` key.
3. Check that sealed files created after rebuild have corresponding objects and rows. New/changed rotations preempt historical work, including known names. A newest-first cursor yields after 64 MiB/two seconds, including mid-file. Verified legacy IDs survive restart; local fingerprints do not persist. Inspection/encryption/upload need no full-file scratch space. Inspect `archive binlog` errors, backlog and actual storage throughput if it cannot catch up.
4. Take a full backup after the rebuild becomes primary and verify an isolated restore with application markers: a `mode: timestamp` verification to a marker written after the rebuild proves the archive replays across it. If the rebuilt site was recloned after a lossy failover and then promoted, follow playbook 39 first. A successful upload count or zero backlog cannot prove history that an older writer already omitted.

### Safe remediation
Preserve old manifests, objects, and encryption material. Do not delete a site manifest, purge local binlogs, or overwrite retained objects to make names appear new. Use compatible ID-aware readers and writers. If an older archiver already missed files, preserve any surviving local binlogs and investigate the recovery gap before declaring PITR coverage restored. Older writers discard IDs, coverage, purged bases, quarantine/reset guards and pending deletes on rewrite; their pruning can drop every same-name row. Re-upgrade repairs surviving stripped IDs but cannot recover lost rows or pending records. Stop the older writer and restore preserved manifests with compatible recovery images, then verify coverage. All rows use GTID predecessor order, including unique-name numbering resets. Empty frontiers/rotations are valid; XA, LOAD DATA and checksum-off logs use observed ordering GTIDs independently of strict retention coverage. Clone `baseGtids` bridges donor GTIDs missing from the first log header. Divergent boundaries fail before SQL. For missing objects with unknown digests or abandoned tails, follow [Repair PITR history safely](../../../../site/content/docs/7.backup-and-restore/7.pitr-history-repair.md): pause writers, preserve objects/manifests, verify a canonical full backup, quarantine the exact row with a reason/resume bound, then verify replay. Quarantine never deletes data and can be undone. Divergence is evaluated only with established server history: empty execution, running clone and read-only/no-channel bootstrap cannot flag the archive. Eligible audits clear recovered foreign-source flags. Known loss from the current server UUID remains unresolved on numeric return; verify recovery or explicitly acknowledge a canonical backup, or quarantine the abandoned row. Surviving flags block affected transactions, including last rows and discovered aliases. Legacy overlap verification runs even without retention; unresolved issuance blocks unsafe compatibility and retirement. A true reuse guard compares conflicting issuance of the same GTIDs; names, retries and publication order are not evidence. Verified reuse still requires a new generation even after quarantine. For an obsolete upload-order guard, `pitr-quarantine --acknowledge-history` verifies every active stream is covered by a canonical full-backup bound, compares overlapping issuance and records prior evidence; it removes no rows and adds no global backup cutoff. Quarantine bounds restrict only windows needing excluded rows; earlier windows with a proven stop before them remain available. Prefer a new unused selected profile `storage.s3.prefix`/`storage.pvc.subPath` and full backup after RESET. Follow the repair guide for the exact procedure.

### Known limitations

- PITR can refuse ambiguous forks/empty stop frontiers, missing or corrupt
  objects, and unknown issuance provenance. Preserve evidence; recover exact
  bytes or use the canonical acknowledgement/quarantine procedure. Verified
  reuse requires a new prefix/full backup. Same-UUID numeric return cannot
  clear known loss before replacement bytes are sealed and verified.
- Original commit stamps or usable statement Query events are needed for
  issuance proof; missing observations and unrecorded historical reuse cannot
  always be reconstructed. Statement rewrites can trigger a conservative
  guard. Per-site planning does not prove cross-site completeness; verify all
  required sites rather than dropping a failing site without evidence.
- Older writers may already have lost rows, clone bases or recovery provenance.
  Preserve manifests, objects, original passphrases, compatible images and
  generation/backup pairing. Downgrades can discard those fields again; use
  the documented compatible recovery images or a new prefix/full backup.
- Legacy streaming costs storage reads/S3 egress. Restarts rebuild in-memory
  comparisons and cooldowns, repeating I/O and deferring pruning. Scheduling
  budgets do not bound network latency; reverse-ordered GTID histories still
  incur quadratic interval-index insertion. Watch archive capacity while any
  unresolved issuance blocks site-wide retirement.

## 28. PITR retention deferred because GTID coverage is unknown

### Symptoms
- `retention deferred` warnings with `backupMetadataUnknown`, `backupGTIDCoverageUnknown` or `binlogGTIDCoverageUnknown`, or `retention: coverage unavailable` with a read/identity error.
- Archive storage grows while uploads and restores can remain healthy. Unresolved issuance can pause all new retirements for the site; `archiveIssuanceUnverified` at WARN names the failed row and remedy, and `lastError` retains the failure during cooldown.

### Diagnostic steps
1. Inspect every successful, non-deleting backup matching the group's spec, across all profiles, including CRs without labels. Read `status.startTime` and `status.gtidExecuted`; the latter must be a trustworthy pre-dump lower bound. Completion time alone is not a replay boundary. A newer replica backup may cover fewer GTIDs than the oldest backup. Pending, Running, or unknown-phase group backups pause new retirements, including overlapping dumps with older snapshots.
2. From the protected pod network, inspect `/pitr-cutoff?namespace=<ns>&group=<group>&profile=<profile>&coverage=gtid-v1`. A proof includes the matching `coverage` marker, `retainedGTIDs` for all backups, and `cutoffTime` from the minimum known start. Missing metadata yields no cutoff and `retentionDeferredReason`. `noRetainedBackups` and `backupInFlight` are normal Info waits. The informer-backed handler validates the active PITR group/profile, coalesces lookups and permits one uncached lookup per transport source IP per 30 seconds. Junk query keys allocate no admission state. HTTP 429 carries no cutoff, logs `fetch retention cutoff` at Warn and retries after 30 seconds. Use direct Service access preserving pod source IPs; proxy/NAT callers share a limit and forwarding headers are ignored. Safe deferrals alone are cached; positive proofs and failed lookups are not. Older requests deliberately receive no cutoff; a new sidecar also rejects time-only responses from older operators.
3. Inspect affected manifest rows' `gtidCoverage.complete` and `gtidCoverage.gtids`. Every file transaction must be contained in every backup bound. Previous GTIDs and same-name successor files do not establish coverage. Anonymous transactions, truncation, disabled checksums, and unknown events remain retained. Complete empty files and valid ZSTD payloads immediately after GTIDs are supported.
4. The proof's `gtidCoverage.contentId` must match the row's ID or verified legacy `contentId`. Older/unbound proofs are re-inspected from streamed archived plaintext; both ID-aware and verified legacy rows must match their content ID. Each batch reads at most eight objects, with soft 64 MiB/two-second limits, persists IDs/proofs together, and retries eligible remaining work after 30 seconds with Info progress logs. Failed identities retry only after the normal interval (one hour by default); their process-local cooldown cannot delay unrelated backfill and clears on identity change/removal or sidecar restart. Actual failed attempts and incomplete unsupported coverage remain Warn. Check archive read access and the original passphrase; no full-file inspection scratch is used. Unknown/unreadable objects remain retained. Capture `support-bundle.sh`; `backups.json` contains projected backup names, profiles, phases, times, and GTID bounds without reading Secrets.

5. Distinguish coverage backfill from issuance verification. A healthy pending issuance batch logs `archiveIssuanceUnverified` at Info. A failed row logs that reason at Warn with `error`, site/name/ID and the repair/quarantine remedy; `lastError` stays set throughout cooldown. One unresolved legacy identity can defer all new retirements for that site even while unrelated coverage backfill progresses. Repair its archive access/original bytes and allow verification to succeed, or preserve it and explicitly quarantine the exact identity behind a verified canonical backup. Do not leave a growing PVC unattended; observe free space. An unreadable manifest must retry on normal scan triggers, without a completed job causing an immediate scan loop.

### Safe remediation
Preserve files until coverage is proved. Repair missing backup metadata only from the original dump's trustworthy metadata; never invent GTIDs or replace the lower bound with an after-dump sample. Upgrade the operator before sidecars to suppress old time-only retention during rollout. Resolve archive-read failures, then allow the next sweep to retry. Unsupported or incomplete files remain retained; do not delete backups, manifests, or local binlogs to force cleanup.

## 29. Retention object deletion or acknowledgement keeps failing

### Symptoms
- Repeated `retention: delete object` warnings, or `retention: save delete acknowledgements` after a successful object delete.
- An object's live `files` row is absent while a `pruned` retirement record has `deleted: false`. The object may still exist, or may already be absent after an acknowledgement failure.

### Diagnostic steps
1. Read sidecar logs and inspect the site manifest through the configured archive reader. Active `files` rows are used for restore; `pruned` rows are retired and excluded. Pending records retain their name, original archive ID/key, and header metadata. Legacy records also capture a verified `contentId` (keyed on encrypted stores) before deletion.
2. Check delete permissions/backend errors for `retention: delete object`, and manifest write access for `retention: save delete acknowledgements`. The support bundle already captures sidecar logs. The existing `sidecar-probe.sh ... archiver` reports active archival status, not a pending-delete count.
3. A pending record retries at the next retention sweep, including after process restart and before querying the operator. A successful retry writes `deleted: true`; deleting a missing object is idempotent. If a live `files` row still references the same key, the sweeper refuses physical deletion until a new proof retires that live reference.
4. Inspect compatible sidecar versions before a downgrade. Older writers drop retirement records on manifest rewrite. A missing pending record from an older release cannot be recovered by this retry mechanism; preserve evidence before considering any separate orphan audit.

### Safe remediation
Restore archive delete/write access and let the sweeper retry. Do not clear retirement records, toggle `deleted`, purge local binlogs, or remove live manifest rows to force cleanup. Stored `key` and `RemotePath` fields are not deletion authority: the sidecar derives the target from validated identity fields under its configured prefix/site. An already retired object can be deleted without a fresh backup proof; that does not authorize new retirements while coverage is unknown.

## 30. Retired local binlogs or growing retirement metadata

### Symptoms
- Older sidecars upload the same local bytes after a sweep retires them, wasting bandwidth and storage. Retirement-aware sidecars leave those bytes retired across restart and operator outages.
- Acknowledged retirement records remain while local files are still indexed/present, or `retention: compact retirement records` reports a manifest save failure.

### Diagnostic steps
1. Compare the local file's content identity (SHA-256 for plaintext storage; passphrase-keyed HMAC-SHA256 for encrypted storage) with the site's retirement record: `id` for new archive keys, `contentId` for legacy keys. Both pending and acknowledged records suppress matching bytes. A different content ID under the same name is new data and must remain eligible for archival; header timestamps or previous GTIDs alone are insufficient.
2. Inspect `/archiver/status` and the live `files` list. Retired records do not count toward backlog, active file count, bytes, or coverage timestamps. Zero backlog is expected for retired local bytes and still does not prove all historical coverage (playbook 27).
3. Verify the site's sidecar is running and can access its archive. Retention is independent of primary/replica status, role-query success, or sealed-file availability. A removed site without a sidecar has no remaining owner to maintain its archive.
4. Before expecting compaction, check `deleted: true`, a readable local binlog index without the name, and physical absence of the file. An unindexed file still on disk protects the record. Missing/unreadable indexes or filesystem errors defer compaction; pending deletes remain durable regardless of local-file state.
5. For a compaction save warning, check manifest write access and sidecar logs. The old durable records remain and a later scan retries the update. Existing support-bundle logs and archiver probes capture the relevant status; they do not expose a retirement-record count.

### Safe remediation
Upgrade older sidecars that ignore retirement records, restore archive access or local-index readability, and let maintenance retry. Do not clear records or issue `PURGE BINARY LOGS` to make bookkeeping disappear: local files support replication catch-up. The operator defaults local expiry to 14 days; remote retention can be shorter. Preserve a separate archive-lifecycle plan before removing the last sidecar for a site.

## 31. PITR enabled, pods Ready, `PITRArchiveReady=False`

### Symptoms
- `kubectl describe mfg <group>` shows `PITRArchiveReady=False` and a Warning `PITRArchiveNotReady` Event, while `Ready=True`, every MySQL pod is Ready, and applications are unaffected.
- `status.pitr.message` lists one or more `site: error` entries; `lastArchivedTime` / `newestArchivedTime` stop advancing for that site.

### Cause
Binlog archiving has stopped on at least one site. Archiving state is deliberately not coupled to pod readiness or liveness: an archive problem never removes a pod from Services or stops self-fencing. The condition reason names the class:
- `ArchiveStoreInitFailed`: the sidecar cannot build its archive store (unreadable passphrase or S3 credentials file, invalid S3 endpoint URL, empty PVC mount path, PVC mount path that is not a mount point or not writable). Immediate; the sidecar retries on its own with backoff from 5 s up to 5 min.
- `ArchiverError`: the store is up but uploads or manifest reads fail on 2 consecutive archiver scans (`consecutiveFailedScans` ≥ 2 in `/archiver/status`), or the same error stays for 10 minutes without a scan completing. A single failed scan never sets it.
- `ArchiverDisabled`: the sidecar reports PITR disabled on 2 consecutive polls while the spec enables it, and no ordered update is pending for the site, or the PITR spec cannot render an archiver (empty or unknown `profileName`), or the rollout has been pending for 30 minutes. While a valid rollout is pending, for up to 30 minutes, the condition is `Unknown`/`AwaitingRollout` instead, with no Event; the same applies for the first sync after a topology manager starts, before its drift set is computed.
- `SidecarUnreachable`: the operator's poll of `/archiver/status` failed 3 times over at least 60 s, or over at least 5 minutes for a sidecar that has not answered since the operator process started. With a sidecar image older than this behaviour, this is how an archive init crash loop shows up.
- Archive identity refusals (`ArchiveForeign`, `ArchiveCopy`, `ArchiveLegacy`, `ArchiveDescriptorMissing`, `ArchiveEncryptionMismatch`, `ArchiveKeyMismatch`, `ArchiveVersionUnsupported`, `ArchiveStoreUnsupported`): the sidecar refused the archive's `archive.json` descriptor or the store's atomic create. Immediate, and they do not clear by retrying. See playbook 37.
- `ArchiveUnsafeHistory`: a site persisted its permanent history-integrity marker, or `spec.mysqlConf` would make it (previewed before any sidecar reports, with a Warning `PITRArchiveUnsafeHistory`). See playbook 38.

When sites disagree, the reason is the first that applies in the order `ArchiveStoreInitFailed`, the archive refusals in the order above, `ArchiverError`, `ArchiverDisabled`, `SidecarUnreachable`; the message lists every affected site. `status.pitr.archive` summarizes the identity: `state` is `AwaitingDescriptor`, `Verified` or `Refused` with `reason`.

Expected, self-clearing Warning `PITRArchiveNotReady` → Normal `PITRArchiveReady` pairs (no action unless the Normal never follows):
- a pod taking more than 60 s to come back in a rollout or node move (`SidecarUnreachable`);
- a new group or bootstrapped site taking more than 5 minutes to answer (`SidecarUnreachable`);
- enabling PITR with `updateStrategy: Recreate` (`ArchiverDisabled` until the pods restart);
- two failed scans in a row followed by a good one (`ArchiverError`);
- MySQL taking more than about a minute to accept connections after a pod start, for example a long InnoDB crash recovery (`ArchiverError` with `role check: ...`);
- a valid ordered update that is still progressing 30 minutes after a site first reported archiving disabled, for example a slow multi-site rollout after enabling PITR (`ArchiverDisabled` with `ordered update still pending after 30m`, then a Normal once the site rolls).

The PVC store checks its volume when it is built: the mount path must be a mount point in the sidecar's `/proc/self/mountinfo`, and the sidecar must be able to create a file under the archive prefix. A PVC volume left out of the pod spec therefore fails as `ArchiveStoreInitFailed` with `pvc archive store: <path> is not a mount point (it does not exist)` (the image has no such directory) or `... is not a mount point (it is inside <mount>, fstype <type>)`; an unwritable volume fails with `... is not writable`. A declared volume whose claim cannot be bound or mounted leaves the pod Pending, which shows as `SidecarUnreachable`. `PITRArchiveReady=True` still does not prove the archive is on a PVC: any volume mounted at the path passes (an `emptyDir` too), and the check runs only when the store is built, so check the pod's volume source and claim.

### Diagnostic steps
1. Read the condition and the per-site message:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.status} {.reason}: {.message}{"\n"}{end}{.status.pitr.message}{"\n"}'
   ```
2. Probe the sidecars: `scripts/sidecar-probe.sh -n <ns> <group> archiver`. `storeReady:false` with `lastError: archive store init: ...` confirms `ArchiveStoreInitFailed`; `uploadFailures` rises once per failed attempt. For `ArchiverError`, `consecutiveFailedScans` and `lastScanAt` show whether scans keep failing or have stopped completing.
3. Read the sidecar log for `failed to init archive store` (ERROR when the cause changes, WARN while it repeats, with `attempt` and `retryIn`) or `archive binlog` errors.
4. If `PITRArchiveReady` does not change while `status.pitr.message` does, look for the operator log `PITR archive condition read failed; leaving the condition as persisted`: the operator cannot read the group uncached and keeps the persisted condition until it can.
5. For `SidecarUnreachable`, check `kubectl get pod <pod> -n <ns>` restart counts and `kubectl logs <pod> -c sidecar --previous`, and the sidecar image tag.
6. For an `Archive*` reason, read `status.pitr.archive` and each site's `/archiver/status` `archive` block (`scripts/triage.sh` prints both), then go to playbook 37 or 38.

### Safe remediation
- `ArchiveStoreInitFailed`: fix what the error names (Secret projection mode or content, endpoint URL, PVC mount path). The next retry picks it up; look for `archive store initialized after retries`. No pod restart is needed for Secret or endpoint fixes. For `is not a mount point`, compare the pod's `volumes`/`volumeMounts` with the Deployment's pod template: the operator always renders the `pitr-archive` volume, so a missing one was removed by a mutating webhook, policy engine or manual edit. Fix that; restoring the volume changes the pod spec and restarts the pod, which also restarts MySQL, so on the primary site run a planned failover away from it first (`kubectl bloodraven promote <group> <other-site>`). For `is not writable`, the error ends with the cause: `permission denied` means ownership or fsGroup (compare the volume's owner with the sidecar UID, 999 unless overridden); `read-only file system` means the claim's access mode or a filesystem the kernel remounted read-only after an I/O error (check the node's `dmesg`); `no space left on device` means the volume has no free inodes (expand or prune it). The probe creates an empty file, so a volume that is only out of data blocks does not fail init; it shows as `ArchiverError` while retention keeps freeing space. For passphrase `permission denied`, see playbook 25. Do not change an existing archive's passphrase to make the error go away; see playbook 26.
- `ArchiverError`: follow the error text (storage reachability, credentials, playbook 26 for authentication or BRV1 errors).
- `AwaitingRollout`: let the ordered update finish; check `status.updatePhase`. If it stays, the update is blocked (operator log `ordered update: deferring active-site update`, for example no healthy promotable standby, or `ordered update: deferring follower update; no drifted follower is a healthy direct replica` when only followers are drifted and none replicates directly from the active site yet, such as an old primary recovery has not rejoined). After 30 minutes the condition turns `False`/`ArchiverDisabled` with `ordered update still pending after 30m`; unblock the update (restore the standby) rather than restarting pods by hand.
- `ArchiverDisabled`: if it persists after the rollout, check that `spec.backup.pitr.profileName` names an existing profile (look for a `BackupPITRInvalid` Event). With an empty or unknown profile the operator renders no PITR configuration into the sidecar, so it reports archiving disabled indefinitely.
- `ArchiveStoreInitFailed` with `archive store init: … s3:ListBucket`: grant `s3:ListBucket` on the bucket (and `s3:PutObject`/`s3:DeleteObject` on the prefix); the sidecar lists the prefix before creating the descriptor. An inconclusive conditional-write probe (persistent `409 ConditionalRequestConflict`) also shows here and is retried on the next cycle.
- Archive identity reasons and `ArchiveUnsafeHistory`: playbooks 37 and 38.
- `SidecarUnreachable` on an older sidecar image: upgrade the sidecar image; the operator rolls it through the ordered update. The ordered update stops on a rolled pod that is not Ready (playbook 34), but a current sidecar stays Ready through archive failures, so watch `PITRArchiveReady` site by site instead of assuming a finished rollout means archiving works.
- Recovery to `True` needs 2 consecutive clean poll rounds (about a minute) and emits a Normal `PITRArchiveReady` Event. Binlogs sealed during the outage are uploaded once the archiver runs, as long as MySQL still has them; check `backlogFiles` and the coverage fields afterwards.

---

## 32. PITR restore or verification refused (archive identity, dump, target, coverage)

### Symptoms
- `status.restore.reason`, or the verification's `Verified` condition reason, is a `PITR*` token, and the message starts with it. The failing step may be the `dump-meta` or `pitr-download` init container (before MySQL Shell starts) or `restore.py` in the main container.
- A restore with `allowPartial: true` completes with reason `PointInTimePartial` and a Warning Event: the archive held nothing at or after the target, so it replayed to the archive head (`status.restore.pointInTime.outcome: Partial`).
- `PITRArchiveSiteOmitted`, `PITRCrossSiteOrder`, `PITRBackupBoundUnknown`, `PointInTimeCrossSiteUnverified` and `PITRSharedArchivePrefix` no longer occur. `PITRReplayUnsupported` now means only XA in the replay, an event the planner cannot certify `mysqlbinlog` replays (`LOAD DATA`, an incident, an unknown event type) inside a replayed transaction, an anonymous GTID, a transaction without an original commit time, or no `mysqlbinlog`.

### Diagnostic steps
1. Read `kubectl get mfg <group> -n <namespace> -o jsonpath='{.status.restore}'` or `kubectl get mysqlbackupverification <verification> -n <namespace> -o yaml`, the Events, and every container's termination message:
   ```bash
   kubectl get pod <job-pod> -n <namespace> -o jsonpath='{range .status.initContainerStatuses[*]}{.name}: {.state.terminated.message}{"\n"}{end}{range .status.containerStatuses[*]}{.name}: {.state.terminated.message}{"\n"}{end}'
   ```
   The earliest failing container's message is the cause. A successful main container records `BLOODRAVEN_PITR_VERDICT v=2 …` and `BLOODRAVEN_PITR_RESULT v=1 …`. `kubectl logs <job-pod> -c dump-meta` (`dump metadata`, or `read dump metadata` with the token) and `-c pitr-download` (`pitr plan`, or `download site binlogs` with the token, plus `BLOODRAVEN_PITR_DIVERGENT_SET gtids=` with the full set to exclude) add detail. `scripts/triage.sh` prints `status.restore.pointInTime` and each verification's `status.pointInTime`.
2. **Reconcile-time refusals** (`RestoreBuildFailed`, no Job): `PITRTargetInvalid` (unparsable target or more than six fractional digits), `PITRBackupAfterTarget` (target before the backup's `completionTime`, rounded up), `PITRBackupFiltered` (`loadOptions.includeSchemas`/`excludeSchemas` with `pointInTime`), `PITRArchiveDestinationShared` (`archiveProfileName` resolves to the group's own active PITR destination, compared normalized).
3. **Archive identity** (`pitr-download`, checked before any planning, so they name the real problem; `dump-meta` refusals in step 4 come earlier still, because that container runs first):
   - `PITRArchiveNotFound`: no descriptor and no manifests. Wrong `archiveProfileName`, or the archive is empty.
   - `PITRArchiveLegacy`: the prefix predates v2.0.0. This build cannot read it; restore with a pre-v2.0.0 image if the target needs it.
   - `PITRArchiveDescriptorMissing`: version 2 manifests but no `archive.json`. For a replicated copy, wait for replication or copy `archive.json` from the source. Never write one by hand.
   - `PITRArchiveIdentityMismatch`: a manifest names another archive, or the dump's stamp names another archive or source group than the archive read. Point at the archive the backup was taken against (`MysqlBackup.status.pitrArchiveID`).
   - `PITRArchiveEncryptionMismatch` / `PITRArchiveKeyMismatch`: the reading profile's encryption mode or passphrase does not match the descriptor's `keyCheck`.
   - `PITRArchiveVersionUnsupported`: written by a newer release. Upgrade.
   - `PITRArchiveHistoryReset`: a reset epoch (including verified GTID reuse) refuses the whole archive. New prefix plus full backup; older targets need the pre-reset generation. See playbook 39 for the reclone cause.
   - `PITRArchiveListFailed`: repair list permission on the prefix (S3 `ListObjectsV2`).
4. **The dump** (`dump-meta`): `PITRBackupIdentityUnknown` (no backup stamp: the dump predates v2.0.0 or was not taken by a Bloodraven backup Job; take a new backup), `PITRBackupIdentityMismatch` (the stamp's backup or source UID differs from the `mysqlBackupRef`, or `status.gtidExecuted` is not in the dump's `@.json`), `PITRBackupInconsistent` (`@.json` has no `gtidExecuted`, or is marked `gtidExecutedInconsistent`), `PITRBackupFiltered` (schema-filtered dump), `PITRBackupIncomplete` (no `@.json` or `@.done.json`, or the stamp has no `serverClock.end`: the dump did not finish).
5. **The target** (`pitr-download`):
   - `PITRExcludeIntersectsBackup`: `excludeGtids` overlaps the backup. Use a backup taken before the excluded transaction, or on a site that never executed it.
   - `PITRBackupAfterTarget`: the dump finished after T, the backup holds a commit at or after T, or (when some backup transactions are no longer archived) T is earlier than the dumped server's `serverClock.end + 1 s`. The message names the bound. Use an earlier backup, or a later target.
   - `PITRTargetNotCovered`: nothing outside the backup and `excludeGtids` is archived at or after T. The active binlog is archived only after rotation: run `FLUSH BINARY LOGS` on the source, wait for the upload, and retry; or set `allowPartial` on a restore.
   - `PITRStopAmbiguous`: two candidates at the same microsecond on different histories ("choose a target ≠ …"), clock skew around T, or a witness archived only as a replica copy whose predecessors are not proven on its origin (the message names the GTID to exclude, or the promotion-record window and the safe targets). Pick a target the message offers, or exclude the named errant GTID.
   - `PITRBackupNotOnLineage`: the backup holds transactions that are not on the target history, or an injected empty transaction's GTID that the backup server executed with effects (take the backup on the site that injected it).
   - `PITRArchiveDivergentHistory`, `PITRArchiveSplitBrain`, `PITRArchiveGap`, `PITRArchiveOrderConflict`, `PITRArchiveOrderInversion`, `PITRArchiveNamespaceReuse`: playbook 40.
   - `PITRArchiveUnsafeHistory`: the plan needs a site with a history-integrity marker. Playbook 38.
   - `PITRPlanTooLarge`: raise the Job's memory limit (`spec.backup.resources`) or use a backup closer to the target.
   - `PITRArchiveObjectInvalid`: an archived object does not match its digest, hints or parse. Preserve it and inspect.
6. **The replay** (`restore.py`): `PITRCoverageInvalid` (`coverage.json`, the staged files or a plan check do not match the request; also "pod is gone" when the Job's pod was deleted before the operator read its verdict, which is final), `PITRNoArchivedReplay` (latest or partial mode with nothing beyond the backup: rotate on the source and retry), `PITRReplayMismatch` (the server's `gtid_executed` after replay is not the planned target history: preserve the Job and its `coverage.json` and report it), `BLOODRAVEN_PITR_FAILED: mysqlbinlog|mysql exit=<rc> segment=<k>` (read the main container log around `BLOODRAVEN_PITR_SEGMENT k=<k>`), `PITRReplayUnsupported` (XA in the replay or no `mysqlbinlog`; choose a target before the XA transaction), `PITRArchiveIdentityMismatch` (the staged plan's archive does not belong to the dump's source group or stamp; a Job rendered by another operator version, or a tampered staging directory), `PITRExcludeIntersectsBackup` (the server's own GTID functions found `excludeGtids` inside the baseline).
7. For `PointInTimePartial`, compare `status.restore.pointInTime.stopTime` with `newestReplayedCommitTime`, and confirm the earlier point meets the recovery objective before routing traffic.

### Safe remediation
Correct the cause and retry through the normal restore/verification workflow. A failed bootstrap restore has no automatic retry. `restore.py` refusals after the load (`PITRCoverageInvalid` from R2, `PITRReplayMismatch`, `BLOODRAVEN_PITR_FAILED`) leave the target loaded: empty it (or recreate its PVC) before deleting the failed restore Job to retry. Refusals in `dump-meta` and `pitr-download` leave the target untouched. `allowPartial` relaxes only the missing-witness case. Do not delete manifest rows, rewrite `archive.json`, rewrite archived timestamps, or point a restored group's own PITR profile at the source archive.

**Upgrading:** a Job rendered by an older operator writes no version 2 verdict and fails `PITRCoverageInvalid`; delete it and retry. Backups taken before v2.0.0 refuse `PITRBackupIdentityUnknown`.

---

## 33. A healthy replica reports divergent transactions, or an ordered update/planned failover refuses a divergent target

A replica can apply every new transaction from the primary while holding transactions the primary never had: an earlier lossy promotion (issue #202, v1.5.0 ordered updates), a write made on the replica without `sql_log_bin=0`, a split-brain loser repointed by hand, or a restore of stale history. Its IO and SQL threads stay healthy until a conflicting row stops it. Since v1.6.0 the operator audits every read-only follower with a replication channel against the confirmed primary (follower read first, then the primary; about every 30s and on the first poll after start or a primary change), records the extra set, and will not promote that site in an ordered update or a planned failover. Emergency failover is never refused: it ranks divergent sites last and promotes one only when no other candidate remains.

### Symptoms
- `Degraded=True`, reason `DivergentTransactions`, message naming the site, count and set; `Ready` stays `True` and the deployment API keeps granting leases (an old primary's divergence, `divergenceDetectedBy=OldPrimaryRecovery`, still makes it answer `423 unstable`).
- `status.sites[<site>]`: `recoveryState=RecoveryBlocked`, `divergenceDetectedBy=ReplicaAudit`, `divergentGtid`, `divergentTransactionCount`, `divergenceComparedTo=<primary>`; `replicating` often `true`.
- Warning Event `DivergentReplicaDetected`; WARN `divergence detected` with `detectedBy=ReplicaAudit`; `bloodraven_divergent_transactions{site} > 0`.
- Ordered update: `ordered update failed` with `<site> is divergent (N transactions the primary lacks); reclone or reconcile before the active site can be updated`, WARN `ordered update: active-site update stalled; standby is divergent`, or WARN `maintenance handoff refused; candidate has transactions the source lacks`. The active site keeps serving, unfenced, with its old spec. During the v1.6.0 upgrade itself this stall is expected on installs damaged by #202.
- Planned failover: `status.plannedFailover.reason=DivergentTarget`.
- After an emergency: `DivergentPromotion=True` condition and Warning Event `DivergentSitePromoted` (a divergent site is now primary).

### Diagnostic steps
1. Confirm the set independently, follower first:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/gtid-audit.sh -n <ns> <group>   # exit 2 = DIVERGENT, 3 = UNKNOWN
   ```
   It reads the follower's `@@GLOBAL.gtid_executed`, then runs `SELECT GTID_SUBTRACT('<follower set>', @@GLOBAL.gtid_executed)` on the confirmed primary, and reports UNKNOWN (not clean) on any failed read, a non-writable primary, or a `server_uuid` change between the reads.
2. Decode what the transactions are, on the divergent replica, before anything destroys them:
   ```bash
   kubectl exec -n <ns> <replica-pod> -c mysql -- sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -e "SHOW BINARY LOGS"'
   kubectl exec -n <ns> <replica-pod> -c mysql -- sh -c 'mysqlbinlog --include-gtids="<divergentGtid>" --base64-output=DECODE-ROWS -vv /var/lib/mysql/<binlog files>' > errant-<site>.sql
   ```
   Keep `errant-<site>.sql` with the incident. A GTID UUID equal to the replica's own `@@server_uuid` means a local write on the replica; the old primary's UUID means writes the primary lost in a promotion (#202).
3. Ask the data owner which outcome is right. Treat the transactions as real data until proven otherwise: a GTID set cannot tell a stray `CREATE USER` from an order.

### Safe remediation
Pick one. Both end with the audit clearing the block on its own (INFO `divergence cleared: replica holds no transactions the primary lacks`, Event `DivergenceResolved`, `Degraded` back to `Healthy`) within about 30s, and a stalled ordered update then resumes without further action. Re-annotate a refused planned failover.

1. **Discard** (the transactions are not needed, or have been re-applied by hand): reclone the replica from the primary. The plugin fills in the required `divergentGtid` prefix:
   ```bash
   kubectl bloodraven reclone <group> <site> -n <ns>
   # equivalent annotation: bloodraven.shipstream.io/reclone-site=<site>:<first 8+ characters of divergentGtid>
   ```
   (`reclone-site=<site>:confirm=<group>` is the cold form for a site with no recorded `divergentGtid`.) The replica is unavailable for the clone and the group has one copy less until it finishes.
2. **Keep** (the transactions belong in the primary's history): make the primary contain the set. Either replay the decoded events on the primary (review for conflicts first), or, when the transactions must not change the primary's data, commit one empty transaction per GTID on the primary:
   ```sql
   SET GTID_NEXT='<uuid>:<n>'; BEGIN; COMMIT;
   -- repeat for every GTID in divergentGtid, then:
   SET GTID_NEXT='AUTOMATIC';
   ```
   Empty transactions make the sets agree without moving the rows: the replica keeps rows the primary lacks. Use them only for transactions whose effects are absent from both, harmless, or already reconciled.

If an emergency failover promoted a clean site over a divergent one and the new primary was then fenced back by its own sidecar, the operator re-asserts it without holding the divergent peer's recorded `divergentGtid` against it (those transactions stay on the peer, which keeps its report). Anything the peer holds beyond its recorded set still blocks the re-assert: compare with `gtid-audit.sh` and resolve as above.

Do not:
- Force the update or the planned failover through (for example by editing status or repointing replication by hand): promoting the replica would make its extra history authoritative and the primary's newer writes collide with it.
- Expect replication restarts to fix it: thread health does not clear the block, and there is no setting to ignore divergence.
- Run maintenance on a replica with binlogging on. Use `SET SESSION sql_log_bin=0` for replica-local work.

### After an emergency promotion of a divergent site (`DivergentPromotion=True`)
The site was the only candidate left, and its extras are now the primary's history. A later divergent promotion is appended to the same condition (one record per line, newest last, the five newest kept); each also has its own `DivergentSitePromoted` Event. Review every set named in the condition with the data owner (step 2 above, on the new primary), then remove the condition:
```bash
kubectl get mysqlfailovergroup <group> -n <ns> -o json | jq '.status.conditions | map(.type) | index("DivergentPromotion")'
kubectl patch mysqlfailovergroup <group> -n <ns> --subresource=status --type=json -p '[{"op":"remove","path":"/status/conditions/<index>"}]'
```
The operator does not set it again for the same promotion.

---

## 34. Ordered update stalled on a pod that is not Ready (`OrderedUpdateStalled`)

The ordered update reaches MySQL through each site's `-internal` Service, which publishes not-ready addresses, so MySQL can answer and replicate while the pod is out of every application Service. A rolled site therefore counts as updated only when its new pod is also `Ready` (every container) and has been continuously for 30s, counted from the later of the pod's last transition to `Ready` and its newest container start (native sidecar init containers included). A crash-looping container passes its readiness probe between crashes, so a pod that is `Ready` at one moment is not enough. Before it rolls another site or hands off the active site, the operator checks again, by the same rule, that every site already running the new pod template is `Ready`, including the site it just rolled and one an earlier attempt rolled. A template that breaks pods stops at the first site it broke.

### Symptoms
- `Updating=False` with reason `OrderedUpdateStalled`; the message names the site, the pod and why it is not Ready, for example `site iad was updated but its pod mysql-<group>-iad-<hash> did not become Ready within 5m0s (sidecar not ready: crash looping, last exit code 1 (Error))` or `site iad already runs the new pod template but its pod ... is not Ready (...); not handing off the active site pdx`. A restarting container reads `crash looping, last exit code …` in every phase of its loop (back-off, running, `Ready` again for a few seconds, terminated), so the message stays the same across retries while the cause does.
- A Warning Event `OrderedUpdateStalled` with the same text, once per stall; operator log WARN `ordered update: stalled; site pod not Ready` (fields `site`, `pod`, `reason`, `error`) on every retry, about every 30s, sometimes after an `ordered update failed` carrying the same text.
- Argo CD shows the group **Degraded**. `Ready` may still be `True`: the active site keeps serving.
- The stalled site's pod is `NotReady` or crash-looping and, while not Ready, out of its Services. With a broken sidecar it also has no self-fencing. A crash-looping pod can show `Ready` for a few seconds at a time; that does not satisfy the update.

### Diagnostic steps
1. Read the cause from the condition message (`scripts/triage.sh` prints it), then look at the pod it names:
   ```bash
   kubectl get mysqlfailovergroup <group> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="Updating")].message}{"\n"}'
   kubectl describe pod -n <ns> <pod>
   kubectl logs -n <ns> <pod> -c sidecar --previous   # or -c mysql, or the init container the reason names
   ```
2. `pod readiness unknown: ...` means the operator could not read the Deployment or its pods; check the API server and the operator's RBAC for pods and deployments. It does not roll on an unknown.
3. `pod not scheduled: ...` is a scheduling problem (zone selector, capacity, taints), not the template's containers.
4. `pod Ready for less than 30s` means the pod is `Ready` but has not stayed so for 30s and no container restarted recently: it just started, or its readiness probe flaps. A stall with this reason clears on the next retry once the pod holds `Ready` for 30s. If it persists, watch the pod's `Ready` condition (`kubectl get pod <pod> -n <ns> -w`) for a flapping readiness probe. If the pod has been `Ready` for minutes, compare the node's clock with the operator's: the 30s are measured from times the node records, so a node clock ahead of the operator's delays the verdict by the difference.
5. Compare the rendered template with the previous one (`kubectl rollout history deploy/mysql-<group>-<site> -n <ns> --revision=<n>`) to find what the spec change altered: image, Secret references, probe paths, resources.

### Safe remediation
1. Fix the cause the pod reports: a missing or unreadable Secret, a wrong image tag, an invalid setting in the spec. A crash-looping container picks a fixed Secret up on its next restart; deleting the pod skips the back-off. The operator retries every 30s and resumes on its own once the pod has been Ready for 30s; the condition then returns to `NotUpdating` (INFO `ordered update: stalled site pod is Ready` when nothing was left to roll). A crash-looping pod that is briefly Ready between crashes does not clear it.
2. If the template itself is wrong, fix or revert the spec. The broken site then drifts again and the update rolls it like any other site.
3. Leave the active site alone: it was not rolled or fenced, and it keeps serving.

Do not:
- Restart the operator to push the update through: the new process repeats the same check on its first attempt.
- Change the update strategy to `Recreate` to get past it: that rolls every site at once onto the template that broke one.
- Promote the stalled site by hand: its pod is out of the application Services and, with a broken sidecar, cannot self-fence.

## 35. Deleted `MysqlBackup` stuck Terminating (artifact cleanup)

### Symptoms
- A `MysqlBackup` deleted by hand or by retention stays Terminating with the `shipstream.io/mysqlbackup` finalizer.
- `ArtifactCleanupFailed` or `ArtifactCleanupBlocked` Warning Events; `bloodraven_backup_artifact_cleanup_failures_total` rising.
- After an upgrade from an operator whose cleanup ran `cleanup.py`: old Terminating S3 backups with Failed `mysqlbackup-<name>-cleanup` Jobs (new cleanup Jobs are named `mysqlcleanup-<name>`; an old-named Job counts only if the backup owns it). That script needed `util.rmdump`, which MySQL Shell does not have, so S3 cleanup never worked. The new operator replaces those Jobs on its own; let it.

### Diagnostic steps
1. Read the cause and the recorded destination (`scripts/triage.sh` section 8 prints both):
   ```bash
   kubectl get mysqlbackup <name> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="ArtifactCleanup")]}{"\n"}{.status.location}{"\n"}{.status.artifactStorage}{"\n"}'
   kubectl get pods -n <ns> -l shipstream.io/resource=backup-cleanup,shipstream.io/mysqlbackup=<name> \
     -o jsonpath='{range .items[*]}{.metadata.name}{" exit="}{.status.containerStatuses[0].state.terminated.exitCode}{" "}{.status.containerStatuses[0].state.terminated.message}{"\n"}{end}'
   ```
2. Reasons:
   - `CleanupFailed`: exit 75 (storage error), a crash, deadline, eviction or no recorded exit. A new Job runs 5 minutes after the failure.
   - `CleanupRejected`: exit 64. The message says why: missing credential keys, a refused path, a cross-bucket location, an invalid endpoint. A new Job runs 30 minutes after the failure.
   - `CleanupPending`: the pod cannot start; the message carries the waiting reason (`ImagePullBackOff`: the operator image must be pullable in the backup namespace with `spec.backup.imagePullSecrets`), or since when it has been unscheduled or `ContainerCreating` (check `kubectl describe pod` for `FailedMount`/`FailedAttachVolume`, typically a ReadWriteOnce claim attached on another node).
   - `ForegroundDeletion` / `OrphanDeletion`: deleted with `--cascade=foreground` (Argo CD's default prune) or `--cascade=orphan`; cleanup starts once the garbage collector has processed the backup's dependents.
   - `CleanupJobInvalid`: the recorded destination cannot be turned into a Job (unknown storage type, no bucket or claim recorded). No Job ran; treat like `DestinationUnverified`.
   - `CredentialsUnavailable`: the profile no longer has an S3 `credentialsSecret`.
   - `OperatorImageUnset`: the operator cannot name its own image; check its Deployment.
   - `DestinationUnverified`: no recorded `status.artifactStorage.location` (a backup from an older operator) and it cannot be recovered from the producing Job (gone, owned by something else, or its destination depends on runtime-only values), so the exact artifact is unknown.
   - `DestinationMismatch`: `status.location` is not exactly `status.artifactStorage.location`, does not end in the backup name, is outside the recorded bucket or `/backups`, or differs from the producing Job's destination.
   - `CleanupJobConflict`: a Job named `mysqlcleanup-<name>` exists but is not owned by this backup.
   - A new backup stuck `Pending` with `Ready=False/BackupJobConflict` (not a cleanup problem): a `mysqlbackup-<name>` Job it does not own exists, usually orphaned by an earlier same-name backup deleted with `--cascade=orphan`. Delete that Job if nothing owns it; the backup fails after 30 minutes otherwise.
   - `HeldByVerification`: a verification is still reading the artifact; see [playbook 36](#36-backup-kept-past-retention-or-deletion-waiting-on-a-verification).
3. Cleanup deletes from `status.artifactStorage` (captured when the backup ran), never from the live profile. A changed profile bucket, endpoint or claim does not move the delete; only credentials come from the current profile.

### Safe remediation
1. For `CleanupFailed`, `CleanupRejected` and `CredentialsUnavailable`, fix the cause (Secret keys, IAM `s3:ListBucket`/`s3:DeleteObject` on the prefix, endpoint reachability, the operator image pull) and wait. Retries never stop, and a credential rotated inside the same Secret is picked up by the next Job. A replacement starts only after the failed Job's pods are gone.
2. For `DestinationUnverified` and `DestinationMismatch`, find the artifact using the original Job inputs and storage inspection (see [playbook 13](#13-succeeded-backup-cannot-be-found-at-its-recorded-location)). Delete it by hand, then remove the finalizer:
   ```bash
   kubectl patch mysqlbackup <name> -n <ns> --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
   ```
3. `ArtifactCleanupNothingFound` releases the finalizer without deleting anything. If the artifact should have existed, inspect the bucket or claim the backup actually used.

Do not:
- Remove the finalizer while the artifact is still in storage: nothing will clean it up later.
- Edit `status.artifactStorage` or `status.location` to point cleanup somewhere else: the recursive delete would follow it.
- Delete a running cleanup Job to hurry it: the operator replaces only failed Jobs, after their pods are gone.

## 36. Backup kept past retention, or deletion waiting on a verification

### Symptoms
- A successful `MysqlBackup` outside its profile's `count`/`maxAgeDays` window is not pruned, with `RetentionDeferred` Events.
- A deleted `MysqlBackup` stays Terminating with `ArtifactCleanup=False/HeldByVerification` and `ArtifactCleanupDeferred` Events.
- A deleted `MysqlBackupVerification` stays Terminating.

### Diagnostic steps
1. List the holds on the backup and the verifications that reference it:
   ```bash
   kubectl get mysqlbackup <backup> -n <ns> -o json | jq '.metadata.annotations // {} | with_entries(select(.key | startswith("hold.verification.shipstream.io/")))'
   kubectl get mysqlbackupverifications -n <ns> -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,BACKUP_UID:.status.backupRef.uid,JOB:.status.jobName
   ```
2. For each holding verification, check whether anything still runs:
   ```bash
   kubectl get job mysqlverify-<verification> -n <ns> -o jsonpath='{.status.conditions[*].type}{"\n"}'
   kubectl get pods -n <ns> -l shipstream.io/mysqlbackup-verification=<verification> -o custom-columns=POD:.metadata.name,PHASE:.status.phase,DELETING:.metadata.deletionTimestamp
   ```
3. A hold is live while the verification is not finished, or while its Job is unfinished, or while any labelled pod is `Pending` or `Running`. A hold whose verification is gone counts only while that verification's pods still run. An unfinished verification that is not yet pinned holds every successful backup of its group and profile.

### Safe remediation
1. During a run this is expected; retention and cleanup resume on their own after the run stops (the hold release triggers a backup reconcile).
2. If a finished run still holds, it is usually a failed run kept by `keepOnFailure: true` whose pod is still running, or a pod stuck terminating on an unreachable node. Delete the verification (its finalizer waits for its pods) or resolve the stuck pod.
3. A verification that stays Terminating is waiting for its pods to be gone; it releases its hold and finalizer once they are.

Do not:
- Remove a `hold.verification.shipstream.io/*` annotation by hand while a pod of that verification runs: retention or cleanup could then delete the artifact under it.
- Remove the verification finalizer to hurry deletion. The hold then stays on the backup; it stops counting once the pods are gone, but retention only notices on its next pass.

---

## 37. PITR archive identity refused (`ArchiveLegacy`, `ArchiveForeign`, `ArchiveCopy`, `ArchiveDescriptorMissing`, encryption, store)

### Symptoms
- `PITRArchiveReady=False` with an `Archive*` reason and a Warning `PITRArchiveNotReady`; `status.pitr.archive.state: Refused` with the same `reason`.
- `/archiver/status` on the refusing site: `archive.state: Refused`, `lastError` starting with the token (`PITRArchiveLegacy: archive … at s3://…: …; <procedure>`). The site uploads nothing; other sites may still be `Verified` or `AwaitingDescriptor`.
- Sidecar WARN `archive descriptor changed` when a descriptor that verified earlier no longer does.

### Cause
Every archive has a write-once plaintext descriptor `<prefix>/binlogs/archive.json` naming the source group (`namespace`, `name`, `uid`), an archive ID derived from that UID and the normalized destination, and the encryption mode with a passphrase `keyCheck`. A writer accepts it only when it names its own group's UID, an ID derived for its own destination, and its profile's encryption and key:

| Reason | Meaning |
|---|---|
| `ArchiveLegacy` | The prefix predates v2.0.0: version 1 manifests or other objects, no descriptor. Expected on the first upgrade to v2.0.0. |
| `ArchiveForeign` | The descriptor names another group's UID: another group archives here, or this group was deleted and re-created under the same name (new UID). |
| `ArchiveCopy` | This group's UID but an archive ID for another destination: the profile points at a copy (a replicated bucket, a moved prefix, a changed endpoint spelling). Copies are read-only. |
| `ArchiveDescriptorMissing` | Version 2 manifests but no descriptor: it was deleted, or a replicated copy has not received it yet. |
| `ArchiveEncryptionMismatch` | The profile's encryption mode or algorithm differs from the descriptor's (encryption was enabled or disabled on an existing archive). |
| `ArchiveKeyMismatch` | The mounted passphrase does not verify the `keyCheck` (the passphrase or Secret key changed). |
| `ArchiveVersionUnsupported` | Descriptor or manifest from a newer release (a downgrade). |
| `ArchiveStoreUnsupported` | The store does not enforce atomic create: the probe's second `If-None-Match: *` create succeeded or the header was rejected, or a PVC filesystem lacks `link(2)`. |

### Diagnostic steps
1. Read the observation and the per-site reports:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{.status.pitr.archive}{"\n"}'
   scripts/sidecar-probe.sh -n <ns> <group> archiver
   ```
2. Read the descriptor (plaintext on every archive; it holds no secret):
   ```bash
   aws s3 cp s3://<bucket>/<prefix>/binlogs/archive.json - | jq .
   kubectl get mfg <group> -n <ns> -o jsonpath='{.metadata.uid}{"\n"}'
   ```
   Compare `source.uid` with the group's UID and `archiveID` with `status.pitr.archive.archiveID` and the sidecar's `BLOODRAVEN_PITR_ARCHIVE_ID`.
3. For `ArchiveStoreUnsupported`, check the store's conditional-write support (AWS S3, MinIO and RustFS 1.0+ support it; active-active multi-zone buckets do not certify it).
4. For `ArchiveKeyMismatch` / `ArchiveEncryptionMismatch`, compare the active profile's `encryption` block and Secret reference with what the archive was written with (playbook 26). Do not print the passphrase.

### Safe remediation
- Most reasons need **a new prefix and a full backup**: change the active profile's `storage.s3.prefix` (or `pvc.subPath`), or select another profile in `spec.backup.pitr.profileName`, wait for the rollout, then take and verify a full backup. The first upload creates a new descriptor.
- `ArchiveLegacy` after upgrading to v2.0.0: that is the procedure; keep the old prefix, backups, passphrase and a pre-v2.0.0 image for older targets.
- `ArchiveDescriptorMissing`: restore `archive.json` from a bucket backup or, for a copy, from the source; never write a descriptor by hand (the ID and `keyCheck` are derived and a wrong one makes the archive foreign).
- `ArchiveKeyMismatch` / `ArchiveEncryptionMismatch`: revert the profile's encryption settings to what the archive was written with, or start a new prefix. There is no in-place re-encryption.
- `ArchiveStoreUnsupported`: move the archive to a store that enforces conditional writes.
- `ArchiveVersionUnsupported`: run the release that wrote the archive.

Do not delete `archive.json`, rewrite manifests, or point two groups at one prefix. A restored group must archive into its own prefix (`PITRArchiveDestinationShared` refuses the restore otherwise).

---

## 38. PITR unsafe history marker (`ArchiveUnsafeHistory`)

### Symptoms
- `PITRArchiveReady=False` / `ArchiveUnsafeHistory` and a Warning `PITRArchiveUnsafeHistory` (edge-triggered, besides `PITRArchiveNotReady`).
- A site's `/archiver/status` has a non-null `historyIntegrity{violation, detail, at}` and `lastError: PITRArchiveUnsafeHistory: <site> <violation> <detail>; this site's binlogs cannot certify history; start a new archive prefix and take a full backup`. Sidecar WARN `archive history unsafe`.
- Or no sidecar has reported yet, and the condition message names a `spec.mysqlConf` key (the operator's preview).

### Cause
PITR certifies history from binlog order and row images. At store init and every audit cycle each site checks: no replication filters (`performance_schema.replication_applier_filters` / `_global_filters`), no binlog filters (`Binlog_Do_DB`, `Binlog_Ignore_DB`), `binlog_format=ROW`, `binlog_row_image=FULL`, `replica_preserve_commit_order=ON`. On the first violation (`ReplicationFilter`, `BinlogFilter`, `BinlogFormat`, `BinlogRowImage`, `CommitOrder`) it persists the marker in its own manifest and stops uploading. The marker is permanent for that prefix; removing the setting never clears it. `replica-preserve-commit-order` is operator-owned (`ON`), so `CommitOrder` means something changed it at runtime (`SET GLOBAL`). When the check itself fails, `lastError` reads `history integrity: …` and no marker is set; fix the probe (privileges, connectivity).

### Diagnostic steps
1. Find the marked sites and the offending key:
   ```bash
   kubectl get mfg <group> -n <ns> -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.reason}: {.message}{"\n"}{end}'
   kubectl get mfg <group> -n <ns> -o json | jq '{group: .spec.mysqlConf, sites: [.spec.sites[] | {name, mysqlConf}]}'
   scripts/sidecar-probe.sh -n <ns> <group> archiver
   ```
2. On the site, confirm the live values:
   ```sql
   SELECT @@global.binlog_format, @@global.binlog_row_image, @@global.replica_preserve_commit_order;
   SELECT * FROM performance_schema.replication_applier_filters;
   SELECT * FROM performance_schema.replication_applier_global_filters;
   SHOW BINARY LOG STATUS;
   ```

### Safe remediation
1. Remove the setting from `spec.mysqlConf` (group and site); the preview clears once nothing offending remains, but a persisted marker does not.
2. Point `spec.backup.pitr` at a new prefix and wait for the rollout.
3. Take and verify a full backup in the new archive.
4. Keep the marked prefix and its manifest as evidence. Do not edit the manifest to remove `historyIntegrity`.

---

## 39. Reissue risk after a reclone (`PITRArchiveReissueRisk`)

### Symptoms
- Warning `PITRArchiveReissueRisk` on the group: "site `<s>` was promoted while its archive manifest holds N divergent/quarantined rows. If `<s>` was recloned after a lossy failover, it reissues the lost GTIDs, and every restore from this archive refuses PITRArchiveHistoryReset. Start a new archive prefix and take a full backup."
- Later, restores from the archive fail `PITRArchiveHistoryReset`.

### Cause
A recloned server keeps its `server_uuid`. After a lossy failover the old primary's unreplicated transactions are flagged `divergent` (or were quarantined) in its manifest. Once that site is recloned and promoted again, it issues the same GTID numbers with different transactions. The archive then holds two transactions under one GTID, and every reader refuses the whole archive. The Warning fires when the active site changes (failover, planned failover, re-promotion) to a site whose last archiver status reports `divergentRows + quarantinedRows > 0`; it is a heuristic and fires before any reissued GTID is archived.

### Diagnostic steps
1. Read the promoted site's counts and whether it was recloned since the failover:
   ```bash
   scripts/sidecar-probe.sh -n <ns> <group> archiver   # divergentRows, quarantinedRows, promotionRecordedAt
   kubectl logs -n <operator-ns> deploy/bloodraven | jq -c 'select(.msg=="starting bootstrap" and .source=="reclone")'
   ```
2. Compare the reclone time with the lossy failover (`status.lastFailover`, `DataLossDetected` Events).

### Safe remediation
1. If the site was recloned after the lossy failover: point `spec.backup.pitr` at a new prefix, wait for the rollout, then take and verify a full backup. Keep the old prefix and its backups for targets before the reissue.
2. If it was not recloned (the rows come from something else), the Warning is a false positive for reissue; continue with playbook 27 for the divergent rows.
3. Quarantine and acknowledgement cannot clear a verified reuse. Rotating `server_uuid` on reclone is a planned follow-up.

---

## 40. PITR refused: divergent history, lost at promotion, split brain, gap, order conflict or inversion, reuse

### Symptoms
A restore or verification fails with one of `PITRArchiveDivergentHistory`, `PITRArchiveSplitBrain`, `PITRArchiveGap`, `PITRArchiveOrderConflict`, `PITRArchiveOrderInversion` or `PITRArchiveNamespaceReuse`. The archive, the dump and the target were accepted; the archive does not prove a single history from the backup to the target.

### How the planner decides
- Λ is the history of the witness (the first archived transaction at or after T outside the backup B and `excludeGtids` X), or the archive head in latest mode.
- **D ⊆ X.** D is every archived transaction (active or quarantined row) committed after the backup's newest transaction and before T that is outside Λ ∪ B ∪ X. A non-empty D refuses `PITRArchiveDivergentHistory` and prints D (1 KiB; the full set is in the `pitr-download` log line `BLOODRAVEN_PITR_DIVERGENT_SET` and in `coverage.json`). Transactions before the backup's newest transaction never count.
- **Frontier.** The backup's newest commit is read from archived bytes; a member of B archived with a commit time after the dump finished means the GTID was reissued (`PITRArchiveNamespaceReuse`).
- **Anchors.** A witness archived only as a replica copy is accepted when its predecessors are proven on its origin by a promotion record that does not contain it, or by an archived origin file.
- **Loss evidence (latest mode).** A head transaction committed (within δ, default 5 s) before a site was promoted without it was lost at that promotion; so are the divergence audit's `divergent` rows.
- **The marker.** Any row of a site with a history-integrity marker refuses `PITRArchiveUnsafeHistory` when the plan, witness or anchor needs it (playbook 38).

### Diagnostic steps
1. Read the refusal message in full; it names the sets, sites and files. Get the full D from the pitr-download log: `kubectl logs <job-pod> -c pitr-download | grep BLOODRAVEN_PITR_DIVERGENT_SET` (or the `pitr plan`/`download site binlogs` record).
2. Correlate with failovers: `kubectl get mfg <group> -n <ns> -o jsonpath='{.status.lastFailover} {.status.lastFailoverTarget} {.status.promotionGtidExecuted}'`, operator Events, and each site's promotion records and divergent rows (`scripts/gtid-audit.sh -n <ns> <group>` prints them and the `excludeGtids` candidates).
3. Classify:
   - **`PITRArchiveDivergentHistory` (timestamp)**: D was lost at a failover (in the old primary's archived tail, not in the new primary's promotion record), written on a replica (errant), or another branch of a split brain.
   - **`PITRArchiveDivergentHistory` "lost at the promotion of `<site>`" (latest)**: the newest archived state holds transactions the promoted site never had.
   - **`PITRArchiveSplitBrain` (latest)**: two archived heads each hold transactions the other lacks; the message lists each head's exclusive set.
   - **`PITRArchiveGap`**: the history needs transactions no archived file holds: a quarantined row ("it is in quarantined row …"), or a tail that was never archived ("committed on `<site>` and never archived": a failover or reclone before the file was sealed while writable).
   - **`PITRArchiveOrderConflict`**: every transaction is archived but no archived binlog continues from the furthest state; needs commit orders no server produced.
   - **`PITRArchiveOrderInversion`**: two archived binlogs apply the same pair of GTIDs in opposite orders (both named, with their files); usually an errant transaction.
   - **`PITRArchiveNamespaceReuse`**: two copies of a GTID differ, a row that overlaps the history cannot be verified, or a GTID was reissued after the dump (a reclone kept `server_uuid`).

### Safe remediation
- **Divergent history**: if D is history you mean to drop (the lost tail, the errant, the other branch), copy it into `pointInTime.excludeGtids` (restore or verification) and retry. Otherwise choose a target at or before the earliest time in D, or use a backup taken after the latest; a backup taken after a lossy failover needs no exclusion. A successful run reports `excludedDivergentTransactionCount` and a Warning `PointInTimeDivergentHistoryExcluded`.
- **Split brain (latest)**: decide which branch the cluster kept and put the other branch's exclusive set into `excludeGtids`.
- **Gap**: un-quarantine a recovered row, or use a backup taken after the gap, or a target before it, as the message says. An unarchived tail cannot be recovered from the archive; flush binlogs on the primary before planned failovers and reclones (follow-up FU-1 automates this).
- **Order conflict or inversion**: exclude the errant transaction the message names, or use a backup and an archive prefix that start after it. Do not edit binlogs.
- **Reuse**: start a new archive prefix and take a full backup (playbook 39 when a reclone and failback caused it). Never delete or rename archived objects to force a plan.
- After excluding transactions that are on the target history (for example a bad `UPDATE`), the result is not certified for them: check `pointInTime.onHistoryExclusions` and the Warning `PointInTimeOnHistoryExclusions`, and inspect the affected rows.
