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
