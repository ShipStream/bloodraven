#!/usr/bin/env bash
#
# bloodraven-doctor: Support & Incident Bundle Collector
# Captures the MFG (with obvious password/secret/key/token fields redacted),
# resources, events, and operator and pod container logs. Events and logs are
# collected raw: review the bundle before sharing it outside your team.
#
set -euo pipefail

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
kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null | \
    sed -E 's/(password|secret|key|token): .*/\1: "[REDACTED]"/g' > "$OUTPUT_DIR/mfg.yaml" || true

# 2. Capture Pods & PVCs
kubectl get pods,pvc,endpoints,services -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o wide > "$OUTPUT_DIR/resources.txt" 2>/dev/null || true

# Project backup status and original main-container destinations without Secrets.
kubectl get mysqlbackups -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,ATTEMPT:.status.attempt,LAUNCH_ATTEMPT:.metadata.annotations.shipstream\.io/backup-attempt,RETRY_OF:.metadata.labels.shipstream\.io/backup-retry-of,ROOT:.metadata.annotations.shipstream\.io/backup-retry-root,ROOT_UID:.metadata.annotations.shipstream\.io/backup-retry-root-uid,JOB:.status.jobName,LOCATION:.status.location,METADATA:.status.conditions[?(@.type=="MetadataComplete")].status,REASON:.status.conditions[?(@.type=="MetadataComplete")].reason' > "$OUTPUT_DIR/backup-status.txt" 2>/dev/null || true
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SUCCEEDED:.status.succeeded,CONTAINERS:.spec.template.spec.containers[*].name,STORAGE:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,OUTPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_OUTPUT_URL")].value,BUCKET:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' > "$OUTPUT_DIR/backup-jobs.txt" 2>/dev/null || true

# Record verification identity and original inputs; exclude Secrets and arbitrary env.
kubectl get mysqlbackupverifications -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME" -o 'custom-columns=NAME:.metadata.name,PHASE:.status.phase,JOB:.status.jobName,BACKUP:.status.backupRef.name,BACKUP_UID:.status.backupRef.uid,LOCATION:.status.backupRef.location,STORAGE:.status.backupRef.storageType,VERIFIED:.status.conditions[?(@.type=="Verified")].status,REASON:.status.conditions[?(@.type=="Verified")].reason' > "$OUTPUT_DIR/verification-status.txt" 2>/dev/null || true
kubectl get jobs -n "$NAMESPACE" -l "shipstream.io/failover-group=$MFG_NAME,shipstream.io/resource=backup-verification" -o 'custom-columns=NAME:.metadata.name,OWNER_UID:.metadata.ownerReferences[*].uid,SNAPSHOT:.spec.template.metadata.annotations.shipstream\.io/verification-backup,INPUT:.spec.template.spec.containers[*].env[?(@.name=="BLOODRAVEN_INPUT_URL")].value,DECRYPT_STORAGE:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_STORAGE_TYPE")].value,DECRYPT_PREFIX:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_SOURCE_PREFIX")].value,DECRYPT_BUCKET:.spec.template.spec.initContainers[?(@.name=="decrypt-download")].env[?(@.name=="BLOODRAVEN_S3_BUCKET")].value' > "$OUTPUT_DIR/verification-jobs.txt" 2>/dev/null || true

# Capture only backup coverage fields; annotations and credential material are excluded.
kubectl get mysqlbackups.shipstream.io -n "$NAMESPACE" -o json 2>/dev/null | \
    jq --arg group "$MFG_NAME" '[.items[] | select(.spec.failoverGroupRef.name == $group) |
      {name: .metadata.name, profile: .spec.profileName, phase: .status.phase,
       deletionTimestamp: .metadata.deletionTimestamp,
       startTime: .status.startTime, completionTime: .status.completionTime,
       gtidExecuted: .status.gtidExecuted}]' > "$OUTPUT_DIR/backups.json" 2> "$OUTPUT_DIR/backup-errors.txt" || true

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

# Create tarball (staging directory is removed by the EXIT trap)
tar -czf "$OUTPUT_FILE" -C "$STAGING_ROOT" "$BUNDLE_NAME"

echo "✅ Support bundle saved: $OUTPUT_FILE"
echo "⚠️  Events and container logs are not redacted. Review the bundle before sharing it."
