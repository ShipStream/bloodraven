# Bloodraven Alert & Event Runbook Matrix

Quick lookup matrix mapping Prometheus alerts and Kubernetes Events to root causes and verified remediation commands.

| Alert / Event | Impact & Severity | Key Diagnostic Checks | Recommended Remediation Action |
|---|---|---|---|
| `BloodravenNoWritableSite` / `NoPrimary` | 🔴 **CRITICAL** (Total write outage) | Check if all sites have `read_only=ON`, network partition, anti-flap cooldown ticking | If one site is healthy and ahead on GTID: `kubectl bloodraven promote <group> -n <ns> --to=<site> --force` |
| `BloodravenSplitBrainDetected` | 🔴 **CRITICAL** (Multi-primary write divergence) | Multiple sites `read_only=OFF`, app traffic split | 1. Immediately fence loser: `kubectl exec <loser-pod> -c mysql -- mysql -e "SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON;"`<br>2. Update DNS / routing.<br>3. Inspect GTID diff. |
| `BloodravenDivergentTransactions` / `DivergentTransactionsDetected` | 🔴 **CRITICAL** (Replica cannot replicate due to errant transactions) | Events mention divergent GTIDs, `SecondsBehindSource=null`, replication broken | Run GTID audit. If data on old primary can be discarded: `kubectl bloodraven reclone <group> -n <ns> --target=<divergent-site>` |
| `BloodravenOperatorDown` | 🟠 **HIGH** (Failovers and reconciliations suspended) | Operator deployment down, CrashLoopBackOff, or leader lease contention | `kubectl describe deploy bloodraven -n bloodraven`<br>`kubectl logs -n bloodraven deploy/bloodraven` |
| `BloodravenReplicationLagging` | 🟡 **MEDIUM** (High RPO on failover, stale reads) | `SecondsBehindSource` > threshold, large transaction or I/O bottleneck | Check `SHOW PROCESSLIST` and replica I/O load. Reduce write bursts or optimize slow queries. |
| `BloodravenKeyringNotSealed` | 🟠 **HIGH** (Tablespace encryption incomplete/unsealed) | Sidecar `/keyring/status`, `status.encryptionAtRest.sites[].phase` | Verify escrow Secret existence and sidecar file permissions. |
| `KeyringEscrowMissing` / `KeyringEscrowCorrupt` | 🔴 **CRITICAL** (Risk of unrecoverable data loss upon restart) | Escrow Secret deleted or digest mismatch on sealed pod | **DO NOT restart the pod**. Extract active keyring from memory/sidecar `/keyring/status` and re-create escrow Secret. |
| `BloodravenDragonflySiteDown` / `DragonflyDegraded` | 🟡 **MEDIUM** (Cache/Session layer degraded) | `status.dragonfly.phase`, Dragonfly pods, endpoints | Verify Dragonfly pod logs, replication link, and auth Secret alignment. |
| `BloodravenBackupStale` | 🟠 **HIGH** (Backup RPO violation) | Latest `MysqlBackup` failed or missing | Run on-demand backup: `kubectl bloodraven backup <group> -n <ns>` and check backup Job logs. |
| `BloodravenBackupVerificationStale` | 🟡 **MEDIUM** (Untested restore integrity) | Latest `MysqlBackupVerification` failed | Run on-demand verification: `kubectl bloodraven verify-backup <group> -n <ns>`. |
| `BloodravenDNSUpdateFailed` | 🟠 **HIGH** (Clients routing to old/dead site) | `DNSEndpoint` status, `external-dns` controller logs | Check `external-dns` pod logs and cloud DNS provider credentials. |
