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
RESTORE_STATUS=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restore}' 2>/dev/null || true)
if [[ -n "$RESTORE_STATUS" ]]; then
    echo "Restore status (requested ceiling and newest replayed event are separate): $RESTORE_STATUS"
fi
RESTORE_RECLONES=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreReclones.phase}{" pending=["}{.status.restoreReclones.pending[*]}{"] requested="}{.status.restoreReclones.requested}{" failures="}{.status.restoreReclones.failures[*].message}' 2>/dev/null || true)
if [[ "$RESTORE_RECLONES" != " pending=[] requested= failures=" && -n "$RESTORE_RECLONES" ]]; then
    echo "Post-restore reclones: $RESTORE_RECLONES (pending sites are not promoted automatically and are out of the -replica and read-only Services; their replication receiver is stopped until their reclone; Held = nothing is cloned or replicated into them after a failed restore)"
    # Replication state of the queued sites as the operator last saw it. A
    # stopped IO thread (sourceConvergenceReason RestorePeerHold) is by design.
    for site in $(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreReclones.pending[*]}' 2>/dev/null || true); do
        SITE_REPL=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{range .status.sites[?(@.name==\"$site\")]}replicating={.replicating} source={.sourceConvergenceState}/{.sourceConvergenceReason}{end}" 2>/dev/null || true)
        echo "  queued $site: ${SITE_REPL:-not observed} (do not START REPLICA on a queued site)"
    done
fi
PITR_ARCHIVE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ -n "$PITR_ARCHIVE" ]]; then
    echo "PITRArchiveReady:    $PITR_ARCHIVE (archiving state only; never affects pod readiness)"
fi
RESTORE_FENCE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreOfflineFence.site}{" priorOfflineMode="}{.status.restoreOfflineFence.priorOfflineMode}{" sessionsEvicted="}{.status.restoreOfflineFence.sessionsEvicted}' 2>/dev/null || true)
if [[ -n "$RESTORE_FENCE" && "$RESTORE_FENCE" != " priorOfflineMode= sessionsEvicted=" ]]; then
    echo "Restore offline_mode fence: $RESTORE_FENCE (applications without CONNECTION_ADMIN cannot connect to that site)"
fi
UPDATING=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="Updating")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ -n "$UPDATING" ]]; then
    echo "Updating:            $UPDATING"
    if [[ "$UPDATING" == *OrderedUpdateStalled* ]]; then
        echo "  The ordered update stopped on a site whose pod is not Ready and rolls nothing else until it is: see playbook 34."
    fi
fi
# A True AutomaticFailoverHeld condition means a planned failover ended
# ManualRecoveryRequired: automatic failover is off for this group until a
# human resolves the split brain and acknowledges the hold.
HOLD=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="AutomaticFailoverHeld")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ "$HOLD" == True* ]]; then
    echo "🔴 AutomaticFailoverHeld: $HOLD"
    echo "  Emergency failover, split-brain resolution, the re-assert, ordered updates, rotations, recovery and DNS reconcile are held for this group."
    echo "  Resolve the split brain by hand, then annotate bloodraven.shipstream.io/ack-planned-failover-hold=<runId> (with approval): see playbook 38."
elif [[ -n "$HOLD" ]]; then
    echo "AutomaticFailoverHeld: $HOLD"
fi
PF_PHASE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.phase}' 2>/dev/null || true)
if [[ -n "$PF_PHASE" ]]; then
    PF_SUMMARY=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='run={.status.plannedFailover.runId} {.status.plannedFailover.source}->{.status.plannedFailover.target} outcome={.status.plannedFailover.result.outcome} reason={.status.plannedFailover.result.reason} warnings={.status.plannedFailover.result.warnings} waitReason={.status.plannedFailover.admission.waitReason} deferReason={.status.plannedFailover.admission.deferReason}' 2>/dev/null || true)
    PF_OUTCOME=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.result.outcome}' 2>/dev/null || true)
    PF_WAIT=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.admission.waitReason}' 2>/dev/null || true)
    PF_MARKER=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/planned-failover-terminal}' 2>/dev/null || true)
    echo "Planned failover:    phase=$PF_PHASE $PF_SUMMARY"
    if [[ -n "$PF_MARKER" ]]; then
        echo "  Terminal marker $PF_MARKER (<runId>/<outcome>/<reason>): status writes stalled and the marker ended the run it names, whatever phase says."
    fi
    case "$PF_OUTCOME" in
        SourceLeftFenced)
            echo "  Nothing was written to the target and the source stays read-only. Check which site is writable before anything else: see playbook 37." ;;
        PromotedUnconfirmed)
            echo "  super_read_only=OFF may have reached the target. Read read_only/super_read_only/gtid_executed on the source and the target: see the PromotedUnconfirmed row of the alert matrix." ;;
        ManualRecoveryRequired)
            echo "  The run may have written to the target and could not explain the result; automation is held: see playbook 38." ;;
    esac
    if [[ "$PF_PHASE" == "Admitting" && "$PF_WAIT" == "PeerUnreadable" ]]; then
        echo "  Waiting because a primary-candidate or dr-only site does not answer. Planned failovers are strict: it ends NotStarted/AdmissionTimeout after 2 minutes unless the site answers."
    fi
fi

echo ""
echo "--- 2. Sites & Replication Summary ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.sites[*]}Site: {.name}{"\n"}  Role: {.role}{"\n"}  Reachable: {.reachable}{"\n"}  ReadOnly: {.readOnly}{"\n"}  Replicating: {.replicating}{"\n"}  SecondsBehindSource: {.secondsBehindSource}{"\n"}  ReplicationError: {.replicationError}{"\n"}  RecoveryState: {.recoveryState}{"\n"}  Divergence: {.divergenceDetectedBy}{" vs "}{.divergenceComparedTo}{" "}{.divergentTransactionCount}{" "}{.divergentGtid}{"\n\n"}{end}' 2>/dev/null || echo "Could not parse site status."
DEGRADED=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Degraded")].status}{" "}{.status.conditions[?(@.type=="Degraded")].reason}{": "}{.status.conditions[?(@.type=="Degraded")].message}' 2>/dev/null || true)
echo "Degraded: ${DEGRADED:-unavailable}"
if [[ "$DEGRADED" == *Initializing* ]]; then
    echo "  The operator just started and has not classified every primary-candidate yet; this is not an outage. Re-run in a few seconds; a site still unclassified after failureThreshold+recoveryThreshold probes is reported as NoPrimary instead."
fi
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
# An aggregated, rate-limited Event can fall out of the last 10: ask for the
# topology manager start history directly. It is history, not current state.
MANAGER_EVENTS=$(kubectl get events -n "$NAMESPACE" --field-selector "involvedObject.name=$MFG_NAME" --sort-by=.lastTimestamp \
    -o 'custom-columns=LAST:.lastTimestamp,TYPE:.type,REASON:.reason,COUNT:.count,MESSAGE:.message' --no-headers 2>/dev/null \
    | grep -E 'TopologyManagerStart(ed|Failed)' || true)
if [[ -n "$MANAGER_EVENTS" ]]; then
    echo "Topology manager start history for $MFG_NAME. History only, not current state, in either direction:"
    echo "  - Warnings are throttled to one per 5 minutes, so a group that recovered and then broke again can still show TopologyManagerStarted last."
    echo "  - The operator forgets a failure episode when it restarts, so a group fixed after an operator restart never gets TopologyManagerStarted and can still show TopologyManagerStartFailed last."
    echo "  Confirm the current state before declaring the manager absent: the operator logs ERROR 'failed to start topology manager' for $NAMESPACE/$MFG_NAME every 30s while it is down"
    echo "  (kubectl logs -n <operator-ns> deploy/bloodraven --since=2m | grep 'failed to start topology manager' | grep '$NAMESPACE/$MFG_NAME'), and status.sites[].lastSeen stops advancing. See playbook 24."
    echo "$MANAGER_EVENTS"
fi

echo ""
echo "--- 7. Deployment Coordination ---"
bash "$(dirname "${BASH_SOURCE[0]}")/deployment-probe.sh" "$NAMESPACE" "$MFG_NAME" || echo "Deployment evidence unavailable (check jq and read permissions for databases/leases)."

echo ""
echo "--- 8. Backup Completion Metadata ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o 'jsonpath={range .status.backupSchedules[*]}Schedule: {.name}{"\t"}RetryConflict: {.retryConflict}{"\n"}{end}' 2>/dev/null || echo "Backup schedule status unavailable."
# Project public fields only; never dump credential or passphrase env/volumes.
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,ATTEMPT:.status.attempt,LAUNCH_ATTEMPT:.metadata.annotations.shipstream\.io/backup-attempt,RETRY_OF:.metadata.labels.shipstream\.io/backup-retry-of,ROOT:.metadata.annotations.shipstream\.io/backup-retry-root,ROOT_UID:.metadata.annotations.shipstream\.io/backup-retry-root-uid,JOB:.status.jobName,LOCATION:.status.location,METADATA:.status.conditions[?(@.type=="MetadataComplete")].status,REASON:.status.conditions[?(@.type=="MetadataComplete")].reason' 2>/dev/null || echo "Backup status unavailable."
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SUCCEEDED:.status.succeeded,CONTAINERS:.spec.template.spec.containers[*].name,STORAGE:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,OUTPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_OUTPUT_URL")].value,BUCKET:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Backup Job inputs unavailable."
echo "Artifact cleanup (Terminating backups show DELETING; CLEANUP is the ArtifactCleanup reason):"
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,DELETING:.metadata.deletionTimestamp,LOCATION:.status.location,RECORDED:.status.artifactStorage.location,BUCKET:.status.artifactStorage.bucket,CLAIM:.status.artifactStorage.claimName,CLEANUP:.status.conditions[?(@.type=="ArtifactCleanup")].reason,MESSAGE:.status.conditions[?(@.type=="ArtifactCleanup")].message' 2>/dev/null || echo "Artifact cleanup status unavailable."
kubectl get pods -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-cleanup" -o 'custom-columns=POD:.metadata.name,BACKUP:.metadata.labels.shipstream\.io/mysqlbackup,PHASE:.status.phase,EXIT:.status.containerStatuses[*].state.terminated.exitCode,VERDICT:.status.containerStatuses[*].state.terminated.message' 2>/dev/null || echo "Cleanup pods unavailable."
if command -v jq >/dev/null 2>&1; then
  echo "Verification holds (backup -> {hold.verification.shipstream.io/<uid>: verification}):"
  kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o json 2>/dev/null | \
    jq -r '.items[] | select(.metadata.annotations // {} | keys | any(startswith("hold.verification.shipstream.io/"))) |
      "\(.metadata.name)\t\(.metadata.annotations | with_entries(select(.key | startswith("hold.verification.shipstream.io/"))) | tojson)"' \
    || echo "Verification holds unavailable."
fi
echo ""
echo "--- 9. Verification Execution Source ---"
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,PHASE:.status.phase,JOB:.status.jobName,BACKUP:.status.backupRef.name,BACKUP_UID:.status.backupRef.uid,LOCATION:.status.backupRef.location,STORAGE:.status.backupRef.storageType,VERIFIED:.status.conditions[?(@.type=="Verified")].status,REASON:.status.conditions[?(@.type=="Verified")].reason' 2>/dev/null || echo "Verification status unavailable."
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-verification" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SNAPSHOT:.spec.template.metadata.annotations.shipstream\.io/verification-backup,INPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_INPUT_URL")].value,DECRYPT_STORAGE:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,DECRYPT_PREFIX:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_SOURCE_PREFIX")].value,DECRYPT_BUCKET:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Verification Job inputs unavailable."
echo ""
echo "======================================================================="
echo "✅ Triage scan completed."
echo "======================================================================="
