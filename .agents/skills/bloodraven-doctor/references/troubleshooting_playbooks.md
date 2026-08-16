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
3. Check DNS routing:
   ```bash
   kubectl get dnsendpoint -n <ns>
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
