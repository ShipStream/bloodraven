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
LAST_FAILOVER=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.lastFailover.time}{" target="}{.status.lastFailover.target}' 2>/dev/null || true)
TOPOLOGY_GENERATION=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.topologyGeneration}' 2>/dev/null || true)

echo "Active Primary Site: ${ACTIVE_SITE:-Unknown}"
if [[ -z "$LAST_FAILOVER" || "$LAST_FAILOVER" == " target=" ]]; then LAST_FAILOVER=""; fi
echo "Last Failover:       ${LAST_FAILOVER:-None}"
echo "Topology generation: ${TOPOLOGY_GENERATION:-unavailable}"
# status.observedGeneration is the metadata.generation the topology manager
# has applied; the topology conditions carry the same value. Lower than
# metadata.generation means the last spec edit is not applied yet and those
# conditions still describe the previous spec.
SPEC_GENERATION=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
OBSERVED_GENERATION=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
echo "Spec generation:     ${SPEC_GENERATION:-unavailable} (applied by the topology manager: ${OBSERVED_GENERATION:-none})"
if [[ "$SPEC_GENERATION" =~ ^[0-9]+$ && "${OBSERVED_GENERATION:-0}" =~ ^[0-9]+$ && "${OBSERVED_GENERATION:-0}" -lt "$SPEC_GENERATION" ]]; then
    echo "  The topology manager has not applied the latest spec yet; Ready, Degraded, Bootstrapping and Updating describe generation ${OBSERVED_GENERATION:-<none>}. Re-run in a few seconds; a gap that stays usually means no topology manager runs for this group (playbook 24)."
fi
RESTORE_STATUS=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restore.phase}{" reason="}{.status.restore.reason}{" job="}{.status.restore.jobName}{" message="}{.status.restore.message}' 2>/dev/null || true)
if [[ -n "$RESTORE_STATUS" && "$RESTORE_STATUS" != " reason= job= message=" ]]; then
    echo "Restore status:      $RESTORE_STATUS"
    # The certified point-in-time outcome, from the replay's verdict.
    RESTORE_PIT=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restore.provenance.pointInTime.mode}{" outcome="}{.status.restore.provenance.pointInTime.outcome}{" stop="}{.status.restore.provenance.pointInTime.stopTime}{" newest="}{.status.restore.provenance.pointInTime.newestReplayedCommitTime}{" archive="}{.status.restore.provenance.pointInTime.archive.archiveID}{" segments="}{.status.restore.provenance.pointInTime.segmentCount}{" transactions="}{.status.restore.provenance.pointInTime.replayedTransactionCount}{" divergentExcluded="}{.status.restore.provenance.pointInTime.excludedDivergentTransactionCount}{" onHistoryExcluded="}{.status.restore.provenance.pointInTime.onHistoryExclusionCount}{" gtidExecuted="}{.status.restore.provenance.pointInTime.gtidExecuted}' 2>/dev/null || true)
    if [[ -n "$RESTORE_PIT" && "$RESTORE_PIT" != " outcome="* ]]; then
        echo "  pointInTime:       $RESTORE_PIT"
        if [[ "$RESTORE_PIT" != *"onHistoryExcluded= "* && "$RESTORE_PIT" != *"onHistoryExcluded=0 "* ]]; then
            echo "  Excluded transactions on the target history precede replayed ones; the result is not certified for them (playbook 42)."
        fi
    fi
    if [[ "$RESTORE_STATUS" == *"reason=PITR"* ]]; then
        echo "  PITR refusal: see playbook 32 (divergent history, gap, order, reuse: playbook 42)."
    fi
fi
RESTORE_RECLONES=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreReclones.phase}{" pending=["}{.status.restoreReclones.pending[*]}{"] requested="}{.status.restoreReclones.requested}{" failures="}{.status.restoreReclones.failures[*].message}' 2>/dev/null || true)
if [[ "$RESTORE_RECLONES" != " pending=[] requested= failures=" && -n "$RESTORE_RECLONES" ]]; then
    echo "Post-restore reclones: $RESTORE_RECLONES (pending sites are not promoted automatically and are out of the -replica and read-only Services; their replication receiver is stopped until their reclone; Held = nothing is cloned or replicated into them after a failed restore)"
    # Replication state of the queued sites as the operator last saw it. A
    # stopped IO thread (sourceConvergenceReason RestorePeerHold) is by design.
    for site in $(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.restoreReclones.pending[*]}' 2>/dev/null || true); do
        SITE_REPL=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{range .status.sites[?(@.name==\"$site\")]}replication={.replication} source={.sourceConvergenceState}/{.sourceConvergenceReason}{end}" 2>/dev/null || true)
        echo "  queued $site: ${SITE_REPL:-not observed} (do not START REPLICA on a queued site)"
    done
fi
PITR_ARCHIVE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="PITRArchiveReady")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ -n "$PITR_ARCHIVE" ]]; then
    echo "PITRArchiveReady:    $PITR_ARCHIVE (archiving state only; never affects pod readiness)"
    if [[ "$PITR_ARCHIVE" == *ArchiveUnsafeHistory* ]]; then
        echo "  A site set its permanent history-integrity marker (or spec.mysqlConf would): see playbook 40."
    elif [[ "$PITR_ARCHIVE" == *" Archive"[A-Z]* && "$PITR_ARCHIVE" != *ArchiveStoreInitFailed* ]]; then
        echo "  A site refused the archive's identity and uploads nothing: see playbook 39."
    fi
fi
# The archive identity the sidecars observed at the active PITR destination.
# The descriptor holds no secret; destination is the normalized s3:// or pvc:// form.
PITR_ARCHIVE_ID=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.pitr.archive.state}{" "}{.status.pitr.archive.reason}{" archiveID="}{.status.pitr.archive.archiveID}{" encryption="}{.status.pitr.archive.encryptionMode}{" destination="}{.status.pitr.archive.destination}{" observed="}{.status.pitr.archive.observedTime}' 2>/dev/null || true)
if [[ -n "$PITR_ARCHIVE_ID" && "$PITR_ARCHIVE_ID" != " archiveID= encryption= destination= observed=" ]]; then
    echo "PITR archive:        $PITR_ARCHIVE_ID"
    # Per-site archive evidence from each sidecar's /archiver/status: the
    # archive block, the last promotion record, the permanent history marker,
    # the divergent/quarantined rows in the site's own manifest, and the tail
    # block: what the last scan decided about the sealed binlogs the site
    # still has to archive (a read-only site archives the tail of its last
    # writable era). Sidecars that predate the block do not report it.
    if command -v jq >/dev/null 2>&1; then
        for pod in $(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME,app.kubernetes.io/name=mysql" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
            if ! sidecar_http_get "$NAMESPACE" "$pod" /archiver/status; then
                echo "  $pod: /archiver/status unavailable (tried: $SIDECAR_HTTP_TRIED; sidecar-probe.sh -n $NAMESPACE $MFG_NAME archiver)"
                continue
            fi
            ARCHIVER_JSON="$SIDECAR_HTTP_BODY"
            printf '%s' "$ARCHIVER_JSON" | jq -r --arg pod "$pod" \
                '"  \($pod) site=\(.site // "?") archive=\(.archive.state // "none")/\(.archive.reason // "") id=\(.archive.archiveID // "") promotionRecordedAt=\(.promotionRecordedAt // "none") historyIntegrity=\(if .historyIntegrity then "\(.historyIntegrity.violation) (\(.historyIntegrity.detail)) at \(.historyIntegrity.at)" else "none" end) divergentRows=\(.divergentRows // 0) quarantinedRows=\(.quarantinedRows // 0) tail=\(if (.tail | type) == "object" then "\(.tail.state // "?") active=\(.tail.activeFile // "none") newestSealed=\(.tail.newestSealedFile // "none") pending=\(.tail.pendingFiles // 0)\(if (.tail.reason // "") != "" then " reason=\(.tail.reason)" else "" end)\(if .tail.state == "Blocked" and .primary == false then " (read-only site not archiving its tail: playbook 46)" else "" end)" else "not-reported" end)"' \
                2>/dev/null || echo "  $pod: /archiver/status is not JSON"
        done
    else
        echo "  Per-site archive, promotionRecordedAt, historyIntegrity, divergentRows, quarantinedRows and tail: sidecar-probe.sh -n $NAMESPACE $MFG_NAME archiver"
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
# status.plannedFailoverHold (summarised by a True AutomaticFailoverHeld
# condition) means a planned failover ended ManualRecoveryRequired: automatic
# failover is off for this group until a human resolves the split brain and
# acknowledges the hold. A planned-failover-terminal marker with outcome
# ManualRecoveryRequired holds the same way on its own: it is all there is
# when the run ended while status writes were stalled. This mirrors the
# operator's PlannedHoldFrom: the status record wins, a True condition with
# no record is a nameless hold, and otherwise the marker's run is held.
PF_MARKER=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/planned-failover-terminal}' 2>/dev/null || true)
MARKER_RUN=""
MARKER_OUTCOME=""
MARKER_REASON=""
if [[ -n "$PF_MARKER" ]]; then
    # The marker is {"schemaVersion":1,"runID":...,"outcome":...,"reason":...}.
    # A marker the operator cannot parse proves nothing, so neither do we.
    if command -v jq >/dev/null 2>&1; then
        MARKER_FIELDS=$(printf '%s' "$PF_MARKER" | jq -r 'select(type == "object" and .schemaVersion == 1 and (.runID | type) == "string" and .runID != "") | [.runID, (.outcome // ""), (.reason // "")] | join("|")' 2>/dev/null || true)
    elif [[ "$PF_MARKER" =~ \"schemaVersion\":1[,}] ]]; then
        MARKER_FIELDS="$(printf '%s' "$PF_MARKER" | sed -n 's/.*"runID":"\([^"]*\)".*/\1/p')|$(printf '%s' "$PF_MARKER" | sed -n 's/.*"outcome":"\([^"]*\)".*/\1/p')|$(printf '%s' "$PF_MARKER" | sed -n 's/.*"reason":"\([^"]*\)".*/\1/p')"
    else
        MARKER_FIELDS=""
    fi
    IFS='|' read -r MARKER_RUN MARKER_OUTCOME MARKER_REASON <<<"$MARKER_FIELDS" || true
fi
HOLD=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="AutomaticFailoverHeld")]}{.status}{" "}{.reason}{end}' 2>/dev/null || true)
HOLD_RECORD=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailoverHold.runID}{" reason="}{.status.plannedFailoverHold.reason}{" "}{.status.plannedFailoverHold.sourceSite}{"->"}{.status.plannedFailoverHold.targetSite}{" ambiguousSites="}{.status.plannedFailoverHold.ambiguousSites}' 2>/dev/null || true)
HOLD_RUN=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailoverHold.runID}' 2>/dev/null || true)
HOLD_FROM_MARKER=""
if [[ -n "$MARKER_RUN" && "$MARKER_OUTCOME" == "ManualRecoveryRequired" && -z "$HOLD_RUN" && "$HOLD" != True* ]]; then
    HOLD_RUN="$MARKER_RUN"
    HOLD_RECORD="$MARKER_RUN reason=$MARKER_REASON"
    HOLD_FROM_MARKER=1
fi
if [[ "$HOLD" == True* || -n "$HOLD_RUN" ]]; then
    if [[ -n "$HOLD_FROM_MARKER" ]]; then
        echo "🔴 AutomaticFailoverHeld: held by the terminal marker only (status writes stalled, so no condition or status.plannedFailoverHold was written); run=$HOLD_RUN reason=${MARKER_REASON:-<none>}"
    else
        echo "🔴 AutomaticFailoverHeld: ${HOLD:-<no condition>}; plannedFailoverHold: run=${HOLD_RUN:-<none>}${HOLD_RUN:+ ${HOLD_RECORD#"$HOLD_RUN"}}"
    fi
    echo "  Emergency failover, split-brain resolution, the re-assert, ordered updates, rotations, recovery and DNS reconcile are held for this group."
    if [[ -n "$HOLD_RUN" ]]; then
        echo "  Resolve the split brain by hand, then (with approval) kubectl bloodraven ack-planned-failover-hold $MFG_NAME -n $NAMESPACE, or annotate bloodraven.shipstream.io/acknowledge-planned-failover-hold=$HOLD_RUN: see playbook 38."
    else
        echo "  The condition names no run (no status.plannedFailoverHold): no acknowledgement can clear it; see playbook 38."
    fi
elif [[ -n "$HOLD" ]]; then
    echo "AutomaticFailoverHeld: $HOLD"
fi
PF_PHASE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.phase}' 2>/dev/null || true)
if [[ -n "$PF_PHASE" ]]; then
    PF_SUMMARY=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='run={.status.plannedFailover.runID} {.status.plannedFailover.sourceSite}->{.status.plannedFailover.targetSite} outcome={.status.plannedFailover.result.outcome} reason={.status.plannedFailover.result.reason} warnings={.status.plannedFailover.result.warnings} waitReason={.status.plannedFailover.admission.waitReason} deferReason={.status.plannedFailover.admission.deferReason}' 2>/dev/null || true)
    PF_OUTCOME=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.result.outcome}' 2>/dev/null || true)
    PF_WAIT=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.admission.waitReason}' 2>/dev/null || true)
    PF_RUN=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.plannedFailover.runID}' 2>/dev/null || true)
    echo "Planned failover:    phase=$PF_PHASE $PF_SUMMARY"
    if [[ -n "$PF_MARKER" ]]; then
        echo "  Terminal marker $PF_MARKER: status writes stalled and the marker ended the run it names (runID), whatever phase says."
    fi
    # The marker's outcome is the run's outcome when it names this run and
    # the status result has not been written yet.
    if [[ -z "$PF_OUTCOME" && -n "$MARKER_RUN" && "$MARKER_RUN" == "$PF_RUN" ]]; then
        PF_OUTCOME="$MARKER_OUTCOME"
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
        echo "  Waiting because a PrimaryCandidate or DisasterRecovery site does not answer. Planned failovers are strict: it ends NotStarted/AdmissionTimeout after 2 minutes unless the site answers."
    fi
fi

echo ""
echo "--- 2. Sites & Replication Summary ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.sites[*]}Site: {.name}{"\n"}  State: {.state}{"\n"}  Replication: {.replication}{"\n"}  SecondsBehindSource: {.secondsBehindSource}{"\n"}  ReplicationError: {.replicationError}{"\n"}  RecoveryState: {.recoveryState}{"\n"}  Divergence: {.divergenceDetectedBy}{" vs "}{.divergenceComparedTo}{" "}{.divergentTransactionCount}{" "}{.divergentGtid}{"\n\n"}{end}' 2>/dev/null || echo "Could not parse site status."
DEGRADED=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Degraded")].status}{" "}{.status.conditions[?(@.type=="Degraded")].reason}{": "}{.status.conditions[?(@.type=="Degraded")].message}' 2>/dev/null || true)
echo "Degraded: ${DEGRADED:-unavailable}"
if [[ "$DEGRADED" == *Initializing* ]]; then
    echo "  The operator just started and has not classified every PrimaryCandidate site yet; this is not an outage. Re-run in a few seconds; a site still unclassified after failureThreshold+recoveryThreshold probes is reported as NoPrimary instead."
fi
if [[ "$DEGRADED" == *BootstrapFailed* ]]; then
    echo "  The last clone (bootstrap, reclone or auto-clone) failed and its target does not replicate; deployment leases are refused until it replicates or a new clone starts. Fix the cause in the message, then reclone it: see the BootstrapFailed row of the alert matrix."
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
EAR_READY=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.conditions[?(@.type=="EncryptionAtRestReady")]}{.status}{" "}{.reason}{": "}{.message}{end}' 2>/dev/null || true)
if [[ -n "$EAR_READY" ]]; then
    echo "EncryptionAtRestReady: $EAR_READY (True/AllSitesSealed = every site sealed)"
fi

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
    | grep -E 'PITRArchive(ReissueRiskDetected|UnsafeHistoryDetected|TailNotArchived)|PITR(DivergentHistoryExcluded|OnHistoryTransactionsExcluded)|PITRReplayTruncated|PITRArchive(EncryptionChangeRefused|KeyChangeRefused|BindingRefused|KeySourceRemoved|KeySnapshotRecreated|KeyMaterialLost|KeySnapshotReadFailed|BindingReleaseRefused|BindingReleaseRejected|BindingLimitReached)|BackupScheduleRefused' || true)
if [[ -n "$MANAGER_EVENTS" ]]; then
    echo "Topology manager start history for $MFG_NAME. History only, not current state, in either direction:"
    echo "  - Warnings are throttled to one per 5 minutes, so a group that recovered and then broke again can still show TopologyManagerStarted last."
    echo "  - The operator forgets a failure episode when it restarts, so a group fixed after an operator restart never gets TopologyManagerStarted and can still show TopologyManagerStartFailed last."
    echo "  Confirm the current state before declaring the manager absent: the operator logs ERROR 'failed to start topology manager' for $NAMESPACE/$MFG_NAME every 30s while it is down"
    echo "  (kubectl logs -n <operator-ns> deploy/bloodraven --since=2m | grep 'failed to start topology manager' | grep '$NAMESPACE/$MFG_NAME'), and status.sites[].lastSeenTime stops advancing. See playbook 24."
    echo "$MANAGER_EVENTS"
fi
# A verification's exclusion and partial-outcome Warnings are recorded on the
# MysqlBackupVerification, not on the group: collect those of the group's
# verifications too.
for verification in $(kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
    VERIFICATION_EVENTS=$(kubectl get events -n "$NAMESPACE" --field-selector "involvedObject.kind=MysqlBackupVerification,involvedObject.name=$verification" --sort-by=.lastTimestamp \
        -o 'custom-columns=LAST:.lastTimestamp,TYPE:.type,REASON:.reason,COUNT:.count,MESSAGE:.message' --no-headers 2>/dev/null \
        | grep -E 'PITR(DivergentHistoryExcluded|OnHistoryTransactionsExcluded)|PITRReplayTruncated' | sed "s|^|[verification/$verification] |" || true)
    if [[ -n "$VERIFICATION_EVENTS" ]]; then
        PITR_EVENTS+="${PITR_EVENTS:+$'\n'}$VERIFICATION_EVENTS"
    fi
done
if [[ -n "$PITR_EVENTS" ]]; then
    echo "PITR warnings for $MFG_NAME and its verifications (PITRArchiveReissueRiskDetected: playbook 41; PITRArchiveUnsafeHistoryDetected: playbook 40; PITRArchiveTailNotArchived: playbook 46; PITRDivergentHistoryExcluded, PITROnHistoryTransactionsExcluded, PITRReplayTruncated: playbooks 32 and 42; PITRArchive*Refused/KeyMaterialLost/KeySourceRemoved/BindingLimitReached/BackupScheduleRefused: playbook 26):"
    echo "$PITR_EVENTS"
fi

echo ""
echo "--- 7. Deployment Coordination ---"
bash "$(dirname "${BASH_SOURCE[0]}")/deployment-probe.sh" "$NAMESPACE" "$MFG_NAME" || echo "Deployment evidence unavailable (check jq and read permissions for databases/leases)."

echo ""
echo "--- 8. Backup Completion Metadata ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o 'jsonpath={range .status.backupSchedules[*]}Schedule: {.name}{"\t"}RetryConflict: {.retryConflict}{"\n"}{end}' 2>/dev/null || echo "Backup schedule status unavailable."
# Project public fields only; never dump credential or passphrase env/volumes.
kubectl get mysqlbackups -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,ATTEMPT:.status.attempt,LAUNCH_ATTEMPT:.metadata.annotations.bloodraven\.shipstream\.io/backup-attempt,RETRY_OF:.metadata.labels.bloodraven\.shipstream\.io/backup-retry-of,ROOT:.metadata.annotations.bloodraven\.shipstream\.io/backup-retry-root,ROOT_UID:.metadata.annotations.bloodraven\.shipstream\.io/backup-retry-root-uid,JOB:.status.jobName,LOCATION:.status.artifactStorage.location,FG_UID:.status.failoverGroupUID,PITR_ARCHIVE_ID:.status.pitrArchiveID,METADATA:.status.conditions[?(@.type=="MetadataComplete")].status,REASON:.status.conditions[?(@.type=="MetadataComplete")].reason' 2>/dev/null || echo "Backup status unavailable."
kubectl get jobs -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME,bloodraven.shipstream.io/resource=backup" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SUCCEEDED:.status.succeeded,CONTAINERS:.spec.template.spec.containers[*].name,STORAGE:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,OUTPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_OUTPUT_URL")].value,BUCKET:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Backup Job inputs unavailable."
echo "Artifact cleanup (Terminating backups show DELETING; CLEANUP is the ArtifactCleanup reason):"
kubectl get mysqlbackups -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,DELETING:.metadata.deletionTimestamp,POLICY:.spec.deletionPolicy,CLEANUP_START:.status.cleanupStartTime,LOCATION:.status.artifactStorage.location,BUCKET:.status.artifactStorage.bucket,CLAIM:.status.artifactStorage.claimName,CLEANUP:.status.conditions[?(@.type=="ArtifactCleanup")].reason,MESSAGE:.status.conditions[?(@.type=="ArtifactCleanup")].message' 2>/dev/null || echo "Artifact cleanup status unavailable."
kubectl get pods -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME,bloodraven.shipstream.io/resource=backup-cleanup" -o 'custom-columns=POD:.metadata.name,BACKUP:.metadata.labels.bloodraven\.shipstream\.io/mysqlbackup,PHASE:.status.phase,EXIT:.status.containerStatuses[*].state.terminated.exitCode,VERDICT:.status.containerStatuses[*].state.terminated.message' 2>/dev/null || echo "Cleanup pods unavailable."
if command -v jq >/dev/null 2>&1; then
  echo "Verification holds (backup -> {verification-hold.bloodraven.shipstream.io/<uid>: verification}):"
  kubectl get mysqlbackups -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME" -o json 2>/dev/null | \
    jq -r '.items[] | select(.metadata.annotations // {} | keys | any(startswith("verification-hold.bloodraven.shipstream.io/"))) |
      "\(.metadata.name)\t\(.metadata.annotations | with_entries(select(.key | startswith("verification-hold.bloodraven.shipstream.io/"))) | tojson)"' \
    || echo "Verification holds unavailable."
fi
echo ""
echo "--- 9. Verification Execution Source ---"
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,PHASE:.status.phase,JOB:.status.jobName,BACKUP:.status.backupRef.name,BACKUP_UID:.status.backupRef.uid,LOCATION:.status.backupRef.artifactStorage.location,STORAGE:.status.backupRef.artifactStorage.type,VERIFIED:.status.conditions[?(@.type=="Verified")].status,REASON:.status.conditions[?(@.type=="Verified")].reason' 2>/dev/null || echo "Verification status unavailable."
echo "Verification point-in-time results (status.pointInTime):"
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,MODE:.status.pointInTime.mode,OUTCOME:.status.pointInTime.outcome,STOP:.status.pointInTime.stopTime,NEWEST:.status.pointInTime.newestReplayedCommitTime,ARCHIVE:.status.pointInTime.archive.archiveID,SITES:.status.pointInTime.replaySites,TXNS:.status.pointInTime.replayedTransactionCount,DIVERGENT_EXCLUDED:.status.pointInTime.excludedDivergentTransactionCount,ON_HISTORY:.status.pointInTime.onHistoryExclusionCount,GTID_EXECUTED:.status.pointInTime.gtidExecuted' 2>/dev/null || echo "Verification point-in-time results unavailable."
kubectl get jobs -n "$NAMESPACE" -l "bloodraven.shipstream.io/failover-group=$MFG_NAME,bloodraven.shipstream.io/resource=backup-verification" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SNAPSHOT:.spec.template.metadata.annotations.bloodraven\.shipstream\.io/verification-backup,INPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_INPUT_URL")].value,DECRYPT_STORAGE:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,DECRYPT_PREFIX:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_SOURCE_PREFIX")].value,DECRYPT_BUCKET:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' 2>/dev/null || echo "Verification Job inputs unavailable."
echo ""
echo "======================================================================="
echo "✅ Triage scan completed."
echo "======================================================================="
