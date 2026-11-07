#!/usr/bin/env bash
#
# bloodraven-doctor: Support & Incident Bundle Collector
# Captures the MFG (with obvious password/secret/key/token fields redacted),
# resources, events, and operator and pod container logs. Events and logs are
# collected raw: review the bundle before sharing it outside your team.
#
set -euo pipefail

# shellcheck source=lib-sidecar-http.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib-sidecar-http.sh"

NAMESPACE=""
MFG_NAME=""
BUNDLE_NAME="bloodraven-bundle-$(date +%Y%m%d-%H%M%S)"
OUTPUT_FILE=""

usage() {
    echo "Usage: $0 [-n <namespace>] [-o <output.tar.gz>] [<mysql-failover-group>]"
    echo "Gathers a diagnostic bundle for Bloodraven clusters."
    echo "Only the MFG YAML is redacted; events and logs are raw, so review before sharing."
    echo ""
    echo "  -o, --output  Path of the tarball to write. '.tar.gz' is appended if missing."
    echo "                Default: ./${BUNDLE_NAME}.tar.gz. Refuses to overwrite an existing path."
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
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

if [[ -z "$NAMESPACE" ]]; then
    NAMESPACE=$(kubectl config view --minify --output 'jsonpath={..namespace}' 2>/dev/null || echo "default")
    [[ -z "$NAMESPACE" ]] && NAMESPACE="default"
fi

if [[ -z "$MFG_NAME" ]]; then
    MFG_NAME=$(kubectl get mysqlfailovergroups.shipstream.io -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -z "$MFG_NAME" ]]; then
        echo "❌ No MySQLFailoverGroup found."
        exit 1
    fi
fi

if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE="${BUNDLE_NAME}.tar.gz"
elif [[ "$OUTPUT_FILE" != *.tar.gz ]]; then
    OUTPUT_FILE="${OUTPUT_FILE%/}.tar.gz"
fi
if [[ -e "$OUTPUT_FILE" || -L "$OUTPUT_FILE" ]]; then
    echo "❌ Output path already exists: $OUTPUT_FILE (refusing to overwrite)"
    exit 1
fi
OUTPUT_PARENT=$(dirname "$OUTPUT_FILE")
if [[ ! -d "$OUTPUT_PARENT" ]]; then
    echo "❌ Output directory does not exist: $OUTPUT_PARENT"
    exit 1
fi

# Collect into a script-owned staging directory; only this directory is ever removed.
STAGING_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/bloodraven-bundle.XXXXXX")
trap 'rm -rf -- "$STAGING_ROOT"' EXIT
OUTPUT_DIR="$STAGING_ROOT/$BUNDLE_NAME"
mkdir "$OUTPUT_DIR"

echo "📦 Collecting Bloodraven Support Bundle..."
echo "Group: $MFG_NAME | Namespace: $NAMESPACE | Destination: $OUTPUT_FILE"

# 1. Capture CR specification & status (redacting obvious passwords/secrets if any)
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null | \
    sed -E 's/(password|secret|key|token): .*/\1: "[REDACTED]"/g' > "$OUTPUT_DIR/mfg.yaml" || true

# 2. Capture Pods & PVCs
kubectl get pods,pvc,endpoints,services -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o wide > "$OUTPUT_DIR/resources.txt" 2>/dev/null || true

# Project backup status and original main-container destinations without Secrets.
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,ATTEMPT:.status.attempt,LAUNCH_ATTEMPT:.metadata.annotations.shipstream\.io/backup-attempt,RETRY_OF:.metadata.labels.shipstream\.io/backup-retry-of,ROOT:.metadata.annotations.shipstream\.io/backup-retry-root,ROOT_UID:.metadata.annotations.shipstream\.io/backup-retry-root-uid,JOB:.status.jobName,LOCATION:.status.artifactStorage.location,FG_UID:.status.failoverGroupUID,PITR_ARCHIVE_ID:.status.pitrArchiveID,METADATA:.status.conditions[?(@.type=="MetadataComplete")].status,REASON:.status.conditions[?(@.type=="MetadataComplete")].reason' > "$OUTPUT_DIR/backup-status.txt" 2>/dev/null || true
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SUCCEEDED:.status.succeeded,CONTAINERS:.spec.template.spec.containers[*].name,STORAGE:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,OUTPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_OUTPUT_URL")].value,BUCKET:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' > "$OUTPUT_DIR/backup-jobs.txt" 2>/dev/null || true

# Artifact cleanup of deleted backups: recorded destination, condition and pod verdicts.
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,DELETING:.metadata.deletionTimestamp,LOCATION:.status.artifactStorage.location,BUCKET:.status.artifactStorage.bucket,CLAIM:.status.artifactStorage.claimName,CLEANUP:.status.conditions[?(@.type=="ArtifactCleanup")].reason,MESSAGE:.status.conditions[?(@.type=="ArtifactCleanup")].message' > "$OUTPUT_DIR/backup-cleanup-status.txt" 2>/dev/null || true
kubectl get pods -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-cleanup" -o 'custom-columns=POD:.metadata.name,BACKUP:.metadata.labels.shipstream\.io/mysqlbackup,PHASE:.status.phase,EXIT:.status.containerStatuses[*].state.terminated.exitCode,VERDICT:.status.containerStatuses[*].state.terminated.message' > "$OUTPUT_DIR/backup-cleanup-pods.txt" 2>/dev/null || true

# Record verification identity and original inputs; exclude Secrets and arbitrary env.
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,PHASE:.status.phase,JOB:.status.jobName,BACKUP:.status.backupRef.name,BACKUP_UID:.status.backupRef.uid,LOCATION:.status.backupRef.artifactStorage.location,STORAGE:.status.backupRef.artifactStorage.type,VERIFIED:.status.conditions[?(@.type=="Verified")].status,REASON:.status.conditions[?(@.type=="Verified")].reason' > "$OUTPUT_DIR/verification-status.txt" 2>/dev/null || true
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-verification" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SNAPSHOT:.spec.template.metadata.annotations.shipstream\.io/verification-backup,INPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_INPUT_URL")].value,DECRYPT_STORAGE:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,DECRYPT_PREFIX:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_SOURCE_PREFIX")].value,DECRYPT_BUCKET:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' > "$OUTPUT_DIR/verification-jobs.txt" 2>/dev/null || true

# Capture only backup coverage fields; annotations and credential material are excluded.
kubectl get mysqlbackups.shipstream.io -n "$NAMESPACE" -o json 2>/dev/null | \
    jq --arg group "$MFG_NAME" '[.items[] | select(.spec.failoverGroupRef.name == $group) |
      {name: .metadata.name, profile: .spec.profileName, phase: .status.phase,
       deletionTimestamp: .metadata.deletionTimestamp,
       verificationHolds: (.metadata.annotations // {} | with_entries(select(.key | startswith("hold.verification.shipstream.io/")))),
       startTime: .status.startTime, completionTime: .status.completionTime,
       gtidExecuted: .status.gtidExecuted,
       failoverGroupUID: .status.failoverGroupUID, pitrArchiveID: .status.pitrArchiveID}]' > "$OUTPUT_DIR/backups.json" 2> "$OUTPUT_DIR/backup-errors.txt" || true

# PITR archive identity, condition and point-in-time results. The descriptor
# fields and GTID sets hold no secret.
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o json 2>/dev/null | \
    jq '{uid: .metadata.uid, archive: .status.pitr.archive,
         pitrArchiveReady: [.status.conditions[]? | select(.type == "PITRArchiveReady")],
         restore: (.status.restore // null | if . then {phase, reason, message, jobName, pointInTime: .provenance.pointInTime} else null end)}' \
    > "$OUTPUT_DIR/pitr-archive.json" 2> "$OUTPUT_DIR/pitr-archive-errors.txt" || true

# PITR archive encryption bindings (#203): status entries and condition, then
# the operator-made key snapshot Secrets (archive and per-backup dump keys)
# and the pitr-archive-check Jobs. Secrets are projected through
# custom-columns so their data (the passphrase) and the acceptance
# annotation (a passphrase verifier) never enter the bundle.
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o 'custom-columns=ID:.status.pitrArchiveBindings[*].id,STATE:.status.pitrArchiveBindings[*].state,REASON:.status.pitrArchiveBindings[*].reason,MODE:.status.pitrArchiveBindings[*].encryption.mode,KEY_SECRET:.status.pitrArchiveBindings[*].encryption.keySecretName,ARCHIVE_ID:.status.pitrArchiveBindings[*].archiveID,DESTINATION:.status.pitrArchiveBindings[*].destination,CHECK_JOB:.status.pitrArchiveBindings[*].check.jobName,CONDITION:.status.conditions[?(@.type=="PITRArchiveBindingReady")].reason' > "$OUTPUT_DIR/pitr-bindings.txt" 2>/dev/null || true
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o json 2>/dev/null | \
    jq '{bindings: (.status.pitrArchiveBindings // []),
         pitrArchiveBindingReady: [.status.conditions[]? | select(.type == "PITRArchiveBindingReady")]}' \
    > "$OUTPUT_DIR/pitr-bindings.json" 2> "$OUTPUT_DIR/pitr-bindings-errors.txt" || true
for app in mysql-pitr-key mysql-dump-key; do
    kubectl get secrets -n "$NAMESPACE" -l "app.kubernetes.io/name=$app,shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp,IMMUTABLE:.immutable,OWNERS:.metadata.ownerReferences[*].kind,GROUP_UID:.metadata.labels.bloodraven\.shipstream\.io/failover-group-uid,BINDING_ID:.metadata.labels.bloodraven\.shipstream\.io/pitr-binding-id,BACKUP:.metadata.labels.shipstream\.io/mysqlbackup,BACKUP_UID:.metadata.labels.bloodraven\.shipstream\.io/mysqlbackup-uid,DESTINATION:.metadata.annotations.bloodraven\.shipstream\.io/pitr-destination' \
        > "$OUTPUT_DIR/${app}-secrets.txt" 2>/dev/null || true
done
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=pitr-archive-check" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,CREATED:.metadata.creationTimestamp,KEY_SECRET:.metadata.labels.bloodraven\.shipstream\.io/pitr-key-secret,ACTIVE:.status.active,SUCCEEDED:.status.succeeded,FAILED:.status.failed' > "$OUTPUT_DIR/pitr-archive-check-jobs.txt" 2>/dev/null || true
kubectl get pods -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=pitr-archive-check" -o 'custom-columns=POD:.metadata.name,JOB:.metadata.labels.job-name,PHASE:.status.phase,WAITING:.status.containerStatuses[*].state.waiting.reason,EXIT:.status.containerStatuses[*].state.terminated.exitCode,RESULT:.status.containerStatuses[*].state.terminated.message' > "$OUTPUT_DIR/pitr-archive-check-pods.txt" 2>/dev/null || true
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o json 2>/dev/null | \
    jq '[.items[] | {name: .metadata.name, mode: .spec.pointInTime.mode, stopTime: .spec.pointInTime.stopTime,
         excludeGtids: .spec.pointInTime.excludeGtids, phase: .status.phase,
         verified: [.status.conditions[]? | select(.type == "Verified")], pointInTime: .status.pointInTime}]' \
    > "$OUTPUT_DIR/verification-pitr.json" 2> "$OUTPUT_DIR/verification-pitr-errors.txt" || true

# Per-site /archiver/status: archive block, promotionRecordedAt, historyIntegrity,
# divergentRows and quarantinedRows. The endpoint returns no credentials.
# When nothing could fetch it, the file holds an explicit error object
# instead of being empty.
for pod in $(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME,app.kubernetes.io/name=mysql" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
    if sidecar_http_get "$NAMESPACE" "$pod" /archiver/status; then
        printf '%s\n' "$SIDECAR_HTTP_BODY" > "$OUTPUT_DIR/${pod}-archiver-status.json"
    else
        printf '{"error":"/archiver/status unavailable","tried":"%s"}\n' "$SIDECAR_HTTP_TRIED" > "$OUTPUT_DIR/${pod}-archiver-status.json"
    fi
done

# PITR restore and verification Job pods: termination messages (refusals and
# verdicts) and the planner's records. Env and volumes are not captured.
for pod in $(kubectl get pods -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource in (restore,backup-verification)" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
    kubectl get pod "$pod" -n "$NAMESPACE" -o json 2>/dev/null | \
        jq '{name: .metadata.name, phase: .status.phase,
             containers: [(.status.initContainerStatuses // [])[], (.status.containerStatuses // [])[] |
               {name, exitCode: (.state.terminated.exitCode // .lastState.terminated.exitCode),
                message: (.state.terminated.message // .lastState.terminated.message)}]}' \
        > "$OUTPUT_DIR/${pod}-termination.json" 2>/dev/null || true
    for c in dump-meta pitr-download; do
        kubectl logs -n "$NAMESPACE" "$pod" -c "$c" 2>/dev/null | \
            grep -E '"msg":"(pitr plan|dump metadata|read dump metadata|download site binlogs)"|BLOODRAVEN_PITR_DIVERGENT_SET' > "$OUTPUT_DIR/${pod}-${c}-pitr.log" || true
    done
    kubectl logs -n "$NAMESPACE" "$pod" -c mysqlsh 2>/dev/null | \
        grep -E 'BLOODRAVEN_PITR_(START|SEGMENT|COMPLETE|FAILED|PARTIAL|DIVERGENT|ON_HISTORY_EXCLUSIONS|RESULT)' > "$OUTPUT_DIR/${pod}-replay.log" || true
done

# 3. Capture Events
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp > "$OUTPUT_DIR/events.txt" 2>/dev/null || true

# Project known public fields; raw Lease annotations contain ownership hashes.
bash "$(dirname "${BASH_SOURCE[0]}")/deployment-probe.sh" "$NAMESPACE" "$MFG_NAME" > "$OUTPUT_DIR/deployment.txt" 2> "$OUTPUT_DIR/deployment-errors.txt" || true

# 4. Capture Operator Logs (last 500 lines)
OPERATOR_POD=$(kubectl get pods -A -l app.kubernetes.io/name=bloodraven -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
OPERATOR_NS=$(kubectl get pods -A -l app.kubernetes.io/name=bloodraven -o jsonpath='{.items[0].metadata.namespace}' 2>/dev/null || echo "bloodraven")

if [[ -n "$OPERATOR_POD" ]]; then
    kubectl logs -n "$OPERATOR_NS" "$OPERATOR_POD" --tail=500 > "$OUTPUT_DIR/operator.log" 2>/dev/null || true
    kubectl get networkpolicies -n "$OPERATOR_NS" -o yaml > "$OUTPUT_DIR/operator-networkpolicies.yaml" 2>/dev/null || true
fi

# 5. Capture logs for ALL containers in each MySQL pod dynamically
PODS=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
for pod in $PODS; do
    CONTAINERS=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || true)
    for c in $CONTAINERS; do
        kubectl logs -n "$NAMESPACE" "$pod" -c "$c" --tail=300 > "$OUTPUT_DIR/${pod}-${c}.log" 2>/dev/null || true
    done
done

# Promotion records and archive identity events from the sidecar logs.
for pod in $PODS; do
    kubectl logs -n "$NAMESPACE" "$pod" -c sidecar --tail=5000 2>/dev/null | \
        grep -E '"msg":"(promotion recorded|uploads wait for the promotion record|archive descriptor created|archive descriptor changed|archive history unsafe|persist history integrity marker)"' \
        > "$OUTPUT_DIR/${pod}-sidecar-pitr.log" || true
done

# Create tarball (staging directory is removed by the EXIT trap)
tar -czf "$OUTPUT_FILE" -C "$STAGING_ROOT" "$BUNDLE_NAME"

echo "✅ Support bundle saved: $OUTPUT_FILE"
echo "⚠️  Events and container logs are not redacted. Review the bundle before sharing it."
