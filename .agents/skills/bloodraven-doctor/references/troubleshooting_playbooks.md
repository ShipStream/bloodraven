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
