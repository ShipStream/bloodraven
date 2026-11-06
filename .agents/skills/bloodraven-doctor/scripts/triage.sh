#!/usr/bin/env bash
#
# bloodraven-doctor: Comprehensive non-destructive cluster triage script
#
set -euo pipefail

# shellcheck source=lib-sidecar-http.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib-sidecar-http.sh"

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
RESTORE_STATUS=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restore.phase}{" reason="}{.status.restore.reason}{" job="}{.status.restore.jobName}{" message="}{.status.restore.message}' 2>/dev/null || true)
if [[ -n "$RESTORE_STATUS" && "$RESTORE_STATUS" != " reason= job= message=" ]]; then
    echo "Restore status:      $RESTORE_STATUS"
    # The certified point-in-time outcome, from the replay's verdict.
    RESTORE_PIT=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restore.pointInTime.mode}{" outcome="}{.status.restore.pointInTime.outcome}{" stop="}{.status.restore.pointInTime.stopTime}{" newest="}{.status.restore.pointInTime.newestReplayedCommitTime}{" archive="}{.status.restore.pointInTime.archive.archiveID}{" segments="}{.status.restore.pointInTime.segmentCount}{" transactions="}{.status.restore.pointInTime.replayedTransactionCount}{" divergentExcluded="}{.status.restore.pointInTime.excludedDivergentTransactionCount}{" onHistoryExcluded="}{.status.restore.pointInTime.onHistoryExclusionCount}{" gtidExecuted="}{.status.restore.pointInTime.gtidExecuted}' 2>/dev/null || true)
    if [[ -n "$RESTORE_PIT" && "$RESTORE_PIT" != " outcome="* ]]; then
        echo "  pointInTime:       $RESTORE_PIT"
        if [[ "$RESTORE_PIT" != *"onHistoryExcluded= "* && "$RESTORE_PIT" != *"onHistoryExcluded=0 "* ]]; then
            echo "  Excluded transactions on the target history precede replayed ones; the result is not certified for them (playbook 40)."
        fi
    fi
    if [[ "$RESTORE_STATUS" == *"reason=PITR"* ]]; then
        echo "  PITR refusal: see playbook 32 (divergent history, gap, order, reuse: playbook 40)."
    fi
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
    if [[ "$PITR_ARCHIVE" == *ArchiveUnsafeHistory* ]]; then
        echo "  A site set its permanent history-integrity marker (or spec.mysqlConf would): see playbook 38."
    elif [[ "$PITR_ARCHIVE" == *" Archive"[A-Z]* && "$PITR_ARCHIVE" != *ArchiveStoreInitFailed* ]]; then
        echo "  A site refused the archive's identity and uploads nothing: see playbook 37."
    fi
fi
# The archive identity the sidecars observed at the active PITR destination.
# The descriptor holds no secret; destination is the normalized s3:// or pvc:// form.
PITR_ARCHIVE_ID=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.pitr.archive.state}{" "}{.status.pitr.archive.reason}{" archiveID="}{.status.pitr.archive.archiveID}{" encryption="}{.status.pitr.archive.encryptionMode}{" destination="}{.status.pitr.archive.destination}{" observed="}{.status.pitr.archive.observedTime}' 2>/dev/null || true)
if [[ -n "$PITR_ARCHIVE_ID" && "$PITR_ARCHIVE_ID" != " archiveID= encryption= destination= observed=" ]]; then
    echo "PITR archive:        $PITR_ARCHIVE_ID"
    # Per-site archive evidence from each sidecar's /archiver/status: the
    # archive block, the last promotion record, the permanent history marker,
    # and the divergent/quarantined rows in the site's own manifest.
    if command -v jq >/dev/null 2>&1; then
        for pod in $(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME,app.kubernetes.io/name=mysql" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
            if ! sidecar_http_get "$NAMESPACE" "$pod" /archiver/status; then
                echo "  $pod: /archiver/status unavailable (tried: $SIDECAR_HTTP_TRIED; sidecar-probe.sh -n $NAMESPACE $MFG_NAME archiver)"
                continue
            fi
            ARCHIVER_JSON="$SIDECAR_HTTP_BODY"
            printf '%s' "$ARCHIVER_JSON" | jq -r --arg pod "$pod" \
                '"  \($pod) site=\(.site // "?") archive=\(.archive.state // "none")/\(.archive.reason // "") id=\(.archive.archiveID // "") promotionRecordedAt=\(.promotionRecordedAt // "none") historyIntegrity=\(if .historyIntegrity then "\(.historyIntegrity.violation) (\(.historyIntegrity.detail)) at \(.historyIntegrity.at)" else "none" end) divergentRows=\(.divergentRows // 0) quarantinedRows=\(.quarantinedRows // 0)"' \
                2>/dev/null || echo "  $pod: /archiver/status is not JSON"
        done
    else
        echo "  Per-site archive, promotionRecordedAt, historyIntegrity, divergentRows and quarantinedRows: sidecar-probe.sh -n $NAMESPACE $MFG_NAME archiver"
    fi
fi
# PITR archive encryption bindings (#203): the identity (mode, key snapshot)
# accepted for each archive destination the group writes or reads. Only
# names are printed; the key snapshots' data is never read.
PITR_BINDING=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveBindingReady")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ -n "$PITR_BINDING" ]]; then
    echo "PITRArchiveBindingReady: $PITR_BINDING"
    # Match on "<status> <reason>" only; the message is free text.
    case "${PITR_BINDING%%:*}" in
        *KeyChangeRefused*|*EncryptionChangeRefused*)
            echo "  A passphrase or encryption change on a profile in archive use was refused; pods still use the accepted key snapshot. See playbook 26 (rotation = new prefix)." ;;
        *KeyMaterialLost*)
            echo "  An accepted key snapshot is missing or does not hold the accepted key; mysqld keeps running, the archiver fails closed. See playbook 26 (key material lost)." ;;
        *BindingLimitReached*)
            echo "  16 bindings exist; release unused ones by id. See playbook 26 (binding limit)." ;;
        *KeyMismatch*|*Archive[A-Z]*)
            echo "  A binding was refused (wrong key, foreign, copy, legacy or missing archive); nothing new renders for it. See playbook 26." ;;
        *Authenticating*|*Establishing*)
            echo "  A binding is being proven (sidecar report or pitr-archive-check Job). A check Job pending > 60 s is retried; see playbook 26 (check Job stuck)." ;;
    esac
fi
PITR_BINDINGS=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.pitrArchiveBindings[*]}{.id}{"\t"}{.state}{"/"}{.reason}{"\t"}{.encryption.mode}{"\t"}{.encryption.keySecretName}{"\t"}{.archiveID}{"\t"}{.destination}{"\n"}{end}' 2>/dev/null || true)
if [[ -n "$PITR_BINDINGS" ]]; then
    echo "PITR archive bindings (status.pitrArchiveBindings; read encryption only when state is Bound):"
    printf '  %s\t%s\t%s\t%s\t%s\t%s\n' "ID" "STATE/REASON" "MODE" "KEY_SECRET" "ARCHIVE_ID" "DESTINATION"
    printf '%s\n' "$PITR_BINDINGS" | sed 's/^/  /'
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
PITR_EVENTS=$(kubectl get events -n "$NAMESPACE" --field-selector "involvedObject.name=$MFG_NAME" --sort-by=.lastTimestamp \
    -o 'custom-columns=LAST:.lastTimestamp,TYPE:.type,REASON:.reason,COUNT:.count,MESSAGE:.message' --no-headers 2>/dev/null \
    | grep -E 'PITRArchive(ReissueRiskDetected|UnsafeHistoryDetected)|PITR(DivergentHistoryExcluded|OnHistoryTransactionsExcluded)|PointInTimePartial|PITRArchive(EncryptionChangeRefused|KeyChangeRefused|BindingRefused|KeySourceRemoved|KeySnapshotRecreated|KeyMaterialLost|KeySnapshotUnreadable|BindingReleaseRefused|BindingReleaseRejected|BindingLimitReached)|BackupScheduleRefused' || true)
if [[ -n "$MANAGER_EVENTS" ]]; then
    echo "Topology manager start history for $MFG_NAME. History only, not current state, in either direction:"
    echo "  - Warnings are throttled to one per 5 minutes, so a group that recovered and then broke again can still show TopologyManagerStarted last."
    echo "  - The operator forgets a failure episode when it restarts, so a group fixed after an operator restart never gets TopologyManagerStarted and can still show TopologyManagerStartFailed last."
    echo "  Confirm the current state before declaring the manager absent: the operator logs ERROR 'failed to start topology manager' for $NAMESPACE/$MFG_NAME every 30s while it is down"
    echo "  (kubectl logs -n <operator-ns> deploy/bloodraven --since=2m | grep 'failed to start topology manager' | grep '$NAMESPACE/$MFG_NAME'), and status.sites[].lastSeen stops advancing. See playbook 24."
    echo "$MANAGER_EVENTS"
fi
# A verification's exclusion and partial-outcome Warnings are recorded on the
# MysqlBackupVerification, not on the group: collect those of the group's
# verifications too.
for verification in $(kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
    VERIFICATION_EVENTS=$(kubectl get events -n "$NAMESPACE" --field-selector "involvedObject.kind=MysqlBackupVerification,involvedObject.name=$verification" --sort-by=.lastTimestamp \
        -o 'custom-columns=LAST:.lastTimestamp,TYPE:.type,REASON:.reason,COUNT:.count,MESSAGE:.message' --no-headers 2>/dev/null \
        | grep -E 'PITR(DivergentHistoryExcluded|OnHistoryTransactionsExcluded)|PointInTimePartial' | sed "s|^|[verification/$verification] |" || true)
    if [[ -n "$VERIFICATION_EVENTS" ]]; then
        PITR_EVENTS+="${PITR_EVENTS:+$'\n'}$VERIFICATION_EVENTS"
    fi
done
if [[ -n "$PITR_EVENTS" ]]; then
    echo "PITR warnings for $MFG_NAME and its verifications (PITRArchiveReissueRiskDetected: playbook 39; PITRArchiveUnsafeHistoryDetected: playbook 38; PITRDivergentHistoryExcluded, PITROnHistoryTransactionsExcluded, PointInTimePartial: playbooks 32 and 40; PITRArchive*Refused/KeyMaterialLost/KeySourceRemoved/BindingLimitReached/BackupScheduleRefused: playbook 26):"
    echo "$PITR_EVENTS"
fi

echo ""
echo "--- 7. Deployment Coordination ---"
bash "$(dirname "${BASH_SOURCE[0]}")/deployment-probe.sh" "$NAMESPACE" "$MFG_NAME" || echo "Deployment evidence unavailable (check jq and read permissions for databases/leases)."

echo ""
echo "--- 8. Backup Completion Metadata ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o 'jsonpath={range .status.backupSchedules[*]}Schedule: {.name}{"\t"}RetryConflict: {.retryConflict}{"\n"}{end}' 2>/dev/null || echo "Backup schedule status unavailable."
# Project public fields only; never dump credential or passphrase env/volumes.
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,ATTEMPT:.status.attempt,LAUNCH_ATTEMPT:.metadata.annotations.shipstream\.io/backup-attempt,RETRY_OF:.metadata.labels.shipstream\.io/backup-retry-of,ROOT:.metadata.annotations.shipstream\.io/backup-retry-root,ROOT_UID:.metadata.annotations.shipstream\.io/backup-retry-root-uid,JOB:.status.jobName,LOCATION:.status.location,FG_UID:.status.failoverGroupUID,PITR_ARCHIVE_ID:.status.pitrArchiveID,METADATA:.status.conditions[?(@.type=="MetadataComplete")].status,REASON:.status.conditions[?(@.type=="MetadataComplete")].reason' 2>/dev/null || echo "Backup status unavailable."
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
echo "Verification point-in-time results (status.pointInTime):"
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,MODE:.status.pointInTime.mode,OUTCOME:.status.pointInTime.outcome,STOP:.status.pointInTime.stopTime,NEWEST:.status.pointInTime.newestReplayedCommitTime,ARCHIVE:.status.pointInTime.archive.archiveID,SITES:.status.pointInTime.replaySites,TXNS:.status.pointInTime.replayedTransactionCount,DIVERGENT_EXCLUDED:.status.pointInTime.excludedDivergentTransactionCount,ON_HISTORY:.status.pointInTime.onHistoryExclusionCount,GTID_EXECUTED:.status.pointInTime.gtidExecuted' 2>/dev/null || echo "Verification point-in-time results unavailable."
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-verification" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SNAPSHOT:.spec.template.metadata.annotations.shipstream\.io/verification-backup,INPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_INPUT_URL")].value,DECRYPT_STORAGE:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,DECRYPT_PREFIX:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_SOURCE_PREFIX")].value,DECRYPT_BUCKET:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Verification Job inputs unavailable."
echo ""
echo "======================================================================="
echo "✅ Triage scan completed."
echo "======================================================================="
