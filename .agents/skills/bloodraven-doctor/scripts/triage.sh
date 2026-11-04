#!/usr/bin/env bash
#
# bloodraven-doctor: Comprehensive non-destructive cluster triage script
#
set -euo pipefail

NAMESPACE=""
MFG_NAME=""

usage() {
    echo "Usage: $0 [-n <namespace>] [<mysql-failover-group>]"
    echo ""
    echo "Options:"
    echo "  -n, --namespace  Kubernetes namespace (defaults to current context namespace)"
    echo "  -h, --help       Show this help message"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            if [[ -z "$MFG_NAME" ]]; then
                MFG_NAME="$1"
                shift
            else
                usage
            fi
            ;;
    esac
done

if ! command -v kubectl &>/dev/null; then
    echo "ERROR: kubectl is required but not installed or not in PATH."
    exit 1
fi

# Detect current namespace if not provided
if [[ -z "$NAMESPACE" ]]; then
    NAMESPACE=$(kubectl config view --minify --output 'jsonpath={..namespace}' 2>/dev/null || echo "default")
    [[ -z "$NAMESPACE" ]] && NAMESPACE="default"
fi

echo "======================================================================="
echo "🩺 Bloodraven Doctor: Triage Assessment"
echo "Namespace: $NAMESPACE"
echo "======================================================================="

# List or select MFG
if [[ -z "$MFG_NAME" ]]; then
    GROUPS=$(kubectl get mysqlfailovergroups.shipstream.io -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
    if [[ -z "$GROUPS" ]]; then
        echo "❌ No MySQLFailoverGroup found in namespace '$NAMESPACE'."
        echo "Check all namespaces:"
        kubectl get mysqlfailovergroups.shipstream.io -A 2>/dev/null || echo "No groups found in any namespace."
        exit 1
    fi
    FIRST_GROUP=$(echo "$GROUPS" | awk '{print $1}')
    MFG_NAME="$FIRST_GROUP"
    echo "Target Group: $MFG_NAME (Auto-detected)"
else
    echo "Target Group: $MFG_NAME"
fi

echo ""
echo "--- 1. MySQLFailoverGroup CR Status ---"
CR_JSON=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o json 2>/dev/null || true)
if [[ -z "$CR_JSON" ]]; then
    echo "❌ Failed to retrieve MySQLFailoverGroup '$MFG_NAME' in namespace '$NAMESPACE'."
    exit 1
fi

# kubectl -o jsonpath tolerates missing keys: an unset field yields an empty
# string with exit 0, so the || fallbacks below never fire on their own.
ACTIVE_SITE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.activeSite}' 2>/dev/null || true)
LAST_FAILOVER=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.lastFailover}' 2>/dev/null || true)
TOPOLOGY_GENERATION=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.topologyGeneration}' 2>/dev/null || true)

echo "Active Primary Site: ${ACTIVE_SITE:-Unknown}"
echo "Last Failover:       ${LAST_FAILOVER:-None}"
echo "Topology generation: ${TOPOLOGY_GENERATION:-unavailable}"
RESTORE_RECLONES=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreReclones.phase}{" pending=["}{.status.restoreReclones.pending[*]}{"] requested="}{.status.restoreReclones.requested}{" failures="}{.status.restoreReclones.failures[*].message}' 2>/dev/null || true)
if [[ "$RESTORE_RECLONES" != " pending=[] requested= failures=" && -n "$RESTORE_RECLONES" ]]; then
    echo "Post-restore reclones: $RESTORE_RECLONES (pending sites are not promoted automatically and are out of the -replica and read-only Services; Held = nothing is cloned)"
fi
PITR_ARCHIVE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ -n "$PITR_ARCHIVE" ]]; then
    echo "PITRArchiveReady:    $PITR_ARCHIVE (archiving state only; never affects pod readiness)"
fi
RESTORE_FENCE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreOfflineFence.site}{" priorOfflineMode="}{.status.restoreOfflineFence.priorOfflineMode}{" sessionsEvicted="}{.status.restoreOfflineFence.sessionsEvicted}' 2>/dev/null || true)
if [[ -n "$RESTORE_FENCE" && "$RESTORE_FENCE" != " priorOfflineMode= sessionsEvicted=" ]]; then
    echo "Restore offline_mode fence: $RESTORE_FENCE (applications without CONNECTION_ADMIN cannot connect to that site)"
fi

echo ""
echo "--- 2. Sites & Replication Summary ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.sites[*]}Site: {.name}{"\n"}  Role: {.role}{"\n"}  Reachable: {.reachable}{"\n"}  ReadOnly: {.readOnly}{"\n"}  Replicating: {.replicating}{"\n"}  SecondsBehindSource: {.secondsBehindSource}{"\n"}  ReplicationError: {.replicationError}{"\n"}  RecoveryState: {.recoveryState}{"\n"}  Divergence: {.divergenceDetectedBy}{" vs "}{.divergenceComparedTo}{" "}{.divergentTransactionCount}{" "}{.divergentGtid}{"\n\n"}{end}' 2>/dev/null || echo "Could not parse site status."
DEGRADED=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Degraded")].status}{" "}{.status.conditions[?(@.type=="Degraded")].reason}{": "}{.status.conditions[?(@.type=="Degraded")].message}' 2>/dev/null || true)
echo "Degraded: ${DEGRADED:-unavailable}"
if [[ "$DEGRADED" == *DivergentTransactions* ]]; then
    echo "  A site holds transactions the primary lacks (replication may still be running). Ordered updates and planned failovers will not promote it: run gtid-audit.sh and see playbook 33."
fi
DIVERGENT_PROMOTION=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="DivergentPromotion")].message}' 2>/dev/null || true)
if [[ -n "$DIVERGENT_PROMOTION" ]]; then
    echo "DivergentPromotion: $DIVERGENT_PROMOTION (see playbook 33)"
fi
echo ""

echo "--- 3. Dragonfly Cache Subsystem ---"
DF_PHASE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.dragonfly.phase}' 2>/dev/null || echo "N/A")
DF_TAKEOVER=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.dragonfly.replTakeoverSupported}' 2>/dev/null || echo "N/A")
echo "Dragonfly Phase:              $DF_PHASE"
echo "Replication Takeover Ready:   $DF_TAKEOVER"

echo ""
echo "--- 4. Encryption-at-Rest Keyring ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.encryptionAtRest.sites[*]}Site: {.name} -> Phase: {.phase}, Message: {.message}{"\n"}{end}' 2>/dev/null || echo "Encryption status not available."

echo ""
echo "--- 5. Pod Health & Restarts ---"
kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o wide 2>/dev/null || echo "No pods found."

echo ""
echo "--- 6. Recent Anomalous Events (Last 10) ---"
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp 2>/dev/null | tail -n 10 || echo "No events found."

echo ""
echo "--- 7. Deployment Coordination ---"
bash "$(dirname "${BASH_SOURCE[0]}")/deployment-probe.sh" "$NAMESPACE" "$MFG_NAME" || echo "Deployment evidence unavailable (check jq and read permissions for databases/leases)."

echo ""
echo "--- 8. Backup Completion Metadata ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o 'jsonpath={range .status.backupSchedules[*]}Schedule: {.name}{"\t"}RetryConflict: {.retryConflict}{"\n"}{end}' 2>/dev/null || echo "Backup schedule status unavailable."
# Project public fields only; never dump credential or passphrase env/volumes.
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,ATTEMPT:.status.attempt,LAUNCH_ATTEMPT:.metadata.annotations.shipstream\.io/backup-attempt,RETRY_OF:.metadata.labels.shipstream\.io/backup-retry-of,ROOT:.metadata.annotations.shipstream\.io/backup-retry-root,ROOT_UID:.metadata.annotations.shipstream\.io/backup-retry-root-uid,JOB:.status.jobName,LOCATION:.status.location,METADATA:.status.conditions[?(@.type=="MetadataComplete")].status,REASON:.status.conditions[?(@.type=="MetadataComplete")].reason' 2>/dev/null || echo "Backup status unavailable."
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SUCCEEDED:.status.succeeded,CONTAINERS:.spec.template.spec.containers[*].name,STORAGE:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,OUTPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_OUTPUT_URL")].value,BUCKET:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Backup Job inputs unavailable."
echo ""
echo "--- 9. Verification Execution Source ---"
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,PHASE:.status.phase,JOB:.status.jobName,BACKUP:.status.backupRef.name,BACKUP_UID:.status.backupRef.uid,LOCATION:.status.backupRef.location,STORAGE:.status.backupRef.storageType,VERIFIED:.status.conditions[?(@.type=="Verified")].status,REASON:.status.conditions[?(@.type=="Verified")].reason' 2>/dev/null || echo "Verification status unavailable."
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-verification" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SNAPSHOT:.spec.template.metadata.annotations.shipstream\.io/verification-backup,INPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_INPUT_URL")].value,DECRYPT_STORAGE:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,DECRYPT_PREFIX:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_SOURCE_PREFIX")].value,DECRYPT_BUCKET:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Verification Job inputs unavailable."
echo ""
echo "======================================================================="
echo "✅ Triage scan completed."
echo "======================================================================="
