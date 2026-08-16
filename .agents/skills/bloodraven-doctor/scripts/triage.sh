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
CR_JSON=$(kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o json 2>/dev/null || true)
if [[ -z "$CR_JSON" ]]; then
    echo "❌ Failed to retrieve MySQLFailoverGroup '$MFG_NAME' in namespace '$NAMESPACE'."
    exit 1
fi

ACTIVE_SITE=$(echo "$CR_JSON" | grep -o '"activeSite":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "Unknown")
LAST_FAILOVER=$(echo "$CR_JSON" | grep -o '"lastFailover":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "None")

echo "Active Primary Site: $ACTIVE_SITE"
echo "Last Failover:       $LAST_FAILOVER"

echo ""
echo "--- 2. Sites & Replication Summary ---"
kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.sites[*]}Site: {.name}{"\n"}  Role: {.role}{"\n"}  Reachable: {.reachable}{"\n"}  ReadOnly: {.readOnly}{"\n"}  Replicating: {.replicating}{"\n"}  SecondsBehindSource: {.secondsBehindSource}{"\n"}  ReplicationError: {.replicationError}{"\n\n"}{end}' 2>/dev/null || echo "Could not parse site status."

echo "--- 3. Dragonfly Cache Subsystem ---"
DF_PHASE=$(kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.dragonfly.phase}' 2>/dev/null || echo "N/A")
DF_TAKEOVER=$(kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.dragonfly.replTakeoverSupported}' 2>/dev/null || echo "N/A")
echo "Dragonfly Phase:              $DF_PHASE"
echo "Replication Takeover Ready:   $DF_TAKEOVER"

echo ""
echo "--- 4. Encryption-at-Rest Keyring ---"
kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{range .status.encryptionAtRest.sites[*]}Site: {.name} -> Phase: {.phase}, Message: {.message}{"\n"}{end}' 2>/dev/null || echo "Encryption status not available."

echo ""
echo "--- 5. Pod Health & Restarts ---"
kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o wide 2>/dev/null || echo "No pods found."

echo ""
echo "--- 6. Recent Anomalous Events (Last 10) ---"
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp 2>/dev/null | tail -n 10 || echo "No events found."

echo ""
echo "======================================================================="
echo "✅ Triage scan completed."
echo "======================================================================="
