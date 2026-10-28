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
   kubectl bloodraven reclone <group> -n <ns> --target=<loser-site>
   ```

---

## 2. Divergent Transactions & Errant GTID on Replica

### Symptoms
- Old primary fails to rejoin as replica after emergency failover.
- `BloodravenDivergentTransactions` alert firing.
- `SHOW REPLICA STATUS` shows replication stopped with GTID error (e.g. error 1236).

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
   kubectl bloodraven reclone <group> -n <ns> --target=<divergent-site>
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
1. Check sidecar keyring status:
   ```bash
   ./.agents/skills/bloodraven-doctor/scripts/sidecar-probe.sh -n <ns> <group> keyring
   ```
2. Check escrow Secret:
   ```bash
   kubectl get secret -n <ns> <group>-keyring-escrow -o yaml
   ```

### Safe Remediation Steps
1. If the pod is still running, query the sidecar `/keyring/status` endpoint to extract the active keyring digest and key metadata.
2. Re-populate the escrow Secret using operator credentials or backup keys.
3. Once the escrow Secret is restored, verify that `status.encryptionAtRest.sites[].phase` settles to `Sealed`.

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

1. Run `bash ./.agents/skills/bloodraven-doctor/scripts/deployment-probe.sh <namespace> <group>`. Collect the deployment operation ID, grant generation, current `status.topologyGeneration`, lease expiry/state/reason, and planned-operation status. The helper projects public fields; never collect raw ownership hashes.
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
2. For a past run, compare the expected post-backup rows with what the primary holds. `status.restoreInPlace.pitrReplayedBinlogCount > 0` does not mean anything was applied.

### Safe remediation
1. To restore to the backup itself, remove `pointInTime`; the same `confirm` re-runs.
2. To reach a point in time, restore into a new failover group with `spec.initFromBackup.pointInTime` (fresh target, so the dump's GTID set is installed and the replay applies). Then move the data or traffic across. For a single tenant schema, dump it from the recovered group with `mysqlsh util.dumpSchemas` and run a per-schema in-place restore from that dump.
3. Do not try to force replay on the live primary with `RESET BINARY LOGS AND GTIDS` or `mysqlbinlog --skip-gtids`. The first destroys unarchived binlogs and archive continuity. The second re-applies in-dump and cross-site duplicate transactions.

## 20. Peers not yet recloned after a full-instance in-place restore

A full-instance in-place restore replaces the primary's data. The other sites still have the old data until each is recloned from the restored primary. The operator tracks them in `status.restoreReclones`. The queue is `Armed` while the restore runs and `Draining` after it succeeds. It is `Held` when a later restore that replaced it ran on the primary and failed: nothing is cloned from that primary. The operator reclones the sites one at a time through the `reclone-site` annotation and removes each one only after its clone succeeds. `status.restoreInPlace.phase: Succeeded` does not mean the peers are done.

### Symptoms
- `status.restoreReclones.pending` is non-empty long after the restore succeeded.
- `RestoreRecloneFailed` events, or `status.restoreReclones.failures` lists a site.
- `status.restoreReclones.phase: Held` and a `RestoreReclonesHeld` event: a later full-instance restore ran its Job on the primary and failed. The pending sites keep older data, and nothing is cloned. A reclone annotation for a held (or `Armed`) site is refused with `RecloneRejected` and removed.
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
   - With the primary up: `kubectl bloodraven promote <group> <site>`. Once the site is the primary, the rest of the queue is abandoned (`RestoreReclonesAbandoned`); a planned failover that rolls back leaves the queue in place.
   - With the primary down: clear the queue with `kubectl patch mfg <group> -n <ns> --subresource=status --type=json -p '[{"op":"remove","path":"/status/restoreReclones"}]'`, after which automatic failover proceeds. Then reclone the remaining sites by hand from whichever primary you keep.
3. `RestoreReclonesDiscarded` after a failed restore is expected: the peers are not recloned from the broken primary. They are not guaranteed to be intact. The restore's `DROP DATABASE` statements are binlogged, so a peer that was replicating may have dropped the same schemas; check its data before promoting it. If the failed restore had replaced an earlier restore's queue, a `RestoreReclonesRestored` event means that queue is draining again (the failed restore never reached its Job).
4. A `Held` queue (`RestoreReclonesHeld`): the primary may hold the failed restore's dropped or half-loaded data, and the held sites may be the only intact copies. Do not clone from the primary until its data is checked. Then choose one: run a new full-instance restore (it reclones every peer once it succeeds); set the phase back to `Draining` once the primary's data is good (`kubectl patch mfg <group> -n <ns> --subresource=status --type=merge -p '{"status":{"restoreReclones":{"phase":"Draining"}}}'`); or remove `status.restoreReclones` to keep the held sites' data as it is.

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
- During the restore, replicas whose replication user lacks `CONNECTION_ADMIN` (legacy `secretName` installs) show their IO thread failing against the primary, and non-admin monitoring has gaps. Both are expected. With `spec.credentials` the replicator is the exempt operator account and keeps replicating.
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

## 23. Ordered update stalls on the active site (`maintenance handoff aborted`)

With `updateStrategy: OrderedUpdate`, the operator rolls drifted followers first. It then hands the primary role to a healthy standby, and only after that rolls the old active site. The handoff promotes the standby only once its `GTID_EXECUTED` contains everything the fenced active site committed. If that cannot be proven, the handoff aborts. Nothing is lost when it aborts, but the active site keeps its old spec until a later attempt succeeds.

### Symptoms
- Operator log: WARN `maintenance handoff refused; source not fenced` (refused before any write pause, usually a lagging standby) or `maintenance handoff aborted; source unfenced` (aborted after the fence), followed by `ordered update failed`, repeating on later reconciles. The active site's Deployment hash stays stale.
- Or ERROR `maintenance handoff: rollback failed: source may be left read-only`: the abort could not give the active site its writes back.

### Diagnostic steps
1. Read the reason from the `error` field:
   - `did not reach the source GTID set before fencing` (on the `refused` line): the standby is lagging; no write pause happened.
   - `did not reach the fenced source GTID set ... N transactions missing`: the standby is still behind after the 30s fenced budget.
   - `active primary became unreachable during ordered update`: the active site stopped answering. Emergency failover takes over; see the failover runbooks.
   - `no longer contains the fenced source set` (after `STOP REPLICA` or `RESET REPLICA ALL`): the standby's server lost history after the catch-up proof, for example a replaced pod. Treat it as a suspect replica and compare its data before anything else.
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
   - If it is not a subset, the standby has errant transactions: reclone it (`kubectl bloodraven reclone <group> -n <ns> --target=<site>`) instead.

## 24. One group stopped updating (no running topology manager)

A healthy operator pod does not guarantee that every group has a live topology manager. The runner restarts a group's manager when its spec or connection material changes. If the restart fails, the group has no manager, and so no polling, no status writes and no failover, until the runner's 30s retry succeeds.

### Symptoms
- One group's `status.sites[]` and conditions stop changing while other groups keep updating.
- Losing the primary does not trigger failover.
- Operator log: ERROR `failed to start topology manager` for that `fg`, repeating every 30s.

### Diagnostic steps
1. Read the error:
   ```bash
   kubectl logs -n bloodraven deploy/bloodraven | grep -E 'failed to start topology manager|topology manager exited|connection material unreadable' | grep '<ns>/<group>'
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
- The sidecar crash-loops with `failed to init archive store` and `permission denied` for the passphrase file.
- Sidecar HTTP probes fail, pods are unready, and archival and sidecar fencing are unavailable.

### Cause
Older operators project `pitr-passphrase` as root-owned mode `0400`, but PITR sidecars default to UID 999 and MySQL pods have no fsGroup by default. The sidecar exits during archive-store initialization, before starting its HTTP server or fencing monitor.

### Diagnostic steps
1. Read the previous sidecar startup log:
   ```bash
   kubectl logs -n <ns> <pod> -c sidecar --previous --tail=100
   ```
2. Inspect the pod's Secret mode and security contexts without reading Secret contents:
   ```bash
   kubectl get pod -n <ns> <pod> -o jsonpath='{.spec.volumes[?(@.name=="pitr-passphrase")].secret.defaultMode}{"\n"}{.spec.securityContext}{"\n"}{.spec.containers[?(@.name=="sidecar")].securityContext}{"\n"}'
   ```
   JSON reports decimal modes: `256` is `0400`; `292` is `0444`. Without fsGroup, a non-root UID cannot read the root-owned `0400` projection. An unavailable `/archiver/status` is expected when initialization fails; use logs to establish the cause.

### Safe remediation
Upgrade to the operator that projects the live sidecar passphrase as `0444`, matching PITR S3 credentials. Its PITR render-version change triggers ordered updates for existing deployments. Verify the new pod's projection mode, readiness, and `/archiver/status` after the rollout. Backup and restore Job projections keep their separate fsGroup-based permissions. Preserve the existing passphrase; adding fsGroup to an existing MySQL PVC can trigger a recursive ownership change and is unnecessary for this fix.

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
4. Verify an isolated restore with application markers written before and after rebuild. A successful upload count or zero backlog cannot prove history that an older writer already omitted.

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
