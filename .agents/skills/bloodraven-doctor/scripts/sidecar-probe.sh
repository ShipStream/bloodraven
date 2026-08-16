#!/usr/bin/env bash
#
# bloodraven-doctor: Deep Sidecar HTTP Endpoint Prober
# Automatically discovers container names dynamically without assuming fixed names.
#
set -euo pipefail

NAMESPACE=""
MFG_NAME=""
ENDPOINT="status" # status, keyring, fencing, healthz, readyz, metrics

usage() {
    echo "Usage: $0 [-n <namespace>] [<mysql-failover-group>] [status|keyring|fencing|healthz|readyz|metrics]"
    echo "Probes sidecar HTTP endpoints on Bloodraven MySQL pods."
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
        status|keyring|fencing|healthz|readyz|metrics)
            ENDPOINT="$1"
            shift
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

TARGET_PATH="/status"
case "$ENDPOINT" in
    keyring)
        TARGET_PATH="/keyring/status"
        ;;
    fencing)
        TARGET_PATH="/fencing"
        ;;
    healthz)
        TARGET_PATH="/healthz"
        ;;
    readyz)
        TARGET_PATH="/readyz"
        ;;
    metrics)
        TARGET_PATH="/metrics"
        ;;
    status)
        TARGET_PATH="/status"
        ;;
esac

echo "======================================================================="
echo "🔍 Probing Sidecar Endpoint: $TARGET_PATH"
echo "Group: $MFG_NAME | Namespace: $NAMESPACE"
echo "======================================================================="

PODS=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)

if [[ -z "$PODS" ]]; then
    echo "❌ No pods found for instance '$MFG_NAME'."
    exit 1
fi

# Helper to find the best candidate container to reach localhost sidecar port
find_candidate_containers() {
    local pod="$1"
    local ns="$2"
    local all_containers
    all_containers=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || echo "")

    local candidates=()
    # 1. Look for exact "sidecar" or "bloodraven-sidecar"
    for c in $all_containers; do
        if [[ "$c" == "sidecar" || "$c" == "bloodraven-sidecar" ]]; then
            candidates+=("$c")
        fi
    done
    # 2. Look for any container containing "sidecar"
    for c in $all_containers; do
        if [[ "$c" == *"sidecar"* && "$c" != "sidecar" && "$c" != "bloodraven-sidecar" ]]; then
            candidates+=("$c")
        fi
    done
    # 3. Add other containers in pod as fallback (they share localhost netns)
    for c in $all_containers; do
        local already_added=0
        for existing in "${candidates[@]:-}"; do
            if [[ "$c" == "$existing" ]]; then
                already_added=1
                break
            fi
        done
        if [[ $already_added -eq 0 ]]; then
            candidates+=("$c")
        fi
    done

    echo "${candidates[@]}"
}

for pod in $PODS; do
    echo ""
    echo "📍 Pod: $pod"

    CANDIDATES=$(find_candidate_containers "$pod" "$NAMESPACE")
    RESULT=""
    SUCCESSFUL_CONTAINER=""

    for container in $CANDIDATES; do
        # Try wget on 8080 then 8081
        RESULT=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- wget -qO- "http://127.0.0.1:8080${TARGET_PATH}" 2>/dev/null || true)
        if [[ -z "$RESULT" ]]; then
            RESULT=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- wget -qO- "http://127.0.0.1:8081${TARGET_PATH}" 2>/dev/null || true)
        fi

        # If wget was missing, try curl
        if [[ -z "$RESULT" ]]; then
            RESULT=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- curl -s "http://127.0.0.1:8080${TARGET_PATH}" 2>/dev/null || true)
        fi
        if [[ -z "$RESULT" ]]; then
            RESULT=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- curl -s "http://127.0.0.1:8081${TARGET_PATH}" 2>/dev/null || true)
        fi

        if [[ -n "$RESULT" ]]; then
            SUCCESSFUL_CONTAINER="$container"
            break
        fi
    done

    if [[ -n "$RESULT" ]]; then
        echo "   (queried via container: $SUCCESSFUL_CONTAINER)"
        echo "$RESULT"
    else
        echo "⚠️ Unable to fetch $TARGET_PATH from sidecar on pod $pod (tried containers: $CANDIDATES)."
    fi
done
echo ""
