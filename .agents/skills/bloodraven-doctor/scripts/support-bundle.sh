#!/usr/bin/env bash
#
# bloodraven-doctor: Sanitized Support & Incident Bundle Collector
# Automatically captures all pod containers and redacts sensitive data.
#
set -euo pipefail

NAMESPACE=""
MFG_NAME=""
OUTPUT_DIR="bloodraven-bundle-$(date +%Y%m%d-%H%M%S)"

usage() {
    echo "Usage: $0 [-n <namespace>] [-o <output-dir>] [<mysql-failover-group>]"
    echo "Gathers a sanitized diagnostic bundle for Bloodraven clusters."
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
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

mkdir -p "$OUTPUT_DIR"

echo "📦 Collecting Bloodraven Support Bundle..."
echo "Group: $MFG_NAME | Namespace: $NAMESPACE | Destination: $OUTPUT_DIR"

# 1. Capture CR specification & status (redacting obvious passwords/secrets if any)
kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null | \
    sed -E 's/(password|secret|key|token): .*/\1: "[REDACTED]"/g' > "$OUTPUT_DIR/mfg.yaml" || true

# 2. Capture Pods & PVCs
kubectl get pods,pvc,endpoints,services -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o wide > "$OUTPUT_DIR/resources.txt" 2>/dev/null || true

# 3. Capture Events
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp > "$OUTPUT_DIR/events.txt" 2>/dev/null || true

# 4. Capture Operator Logs (last 500 lines)
OPERATOR_POD=$(kubectl get pods -A -l app.kubernetes.io/name=bloodraven -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
OPERATOR_NS=$(kubectl get pods -A -l app.kubernetes.io/name=bloodraven -o jsonpath='{.items[0].metadata.namespace}' 2>/dev/null || echo "bloodraven")

if [[ -n "$OPERATOR_POD" ]]; then
    kubectl logs -n "$OPERATOR_NS" "$OPERATOR_POD" --tail=500 > "$OUTPUT_DIR/operator.log" 2>/dev/null || true
fi

# 5. Capture logs for ALL containers in each MySQL pod dynamically
PODS=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
for pod in $PODS; do
    CONTAINERS=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || true)
    for c in $CONTAINERS; do
        kubectl logs -n "$NAMESPACE" "$pod" -c "$c" --tail=300 > "$OUTPUT_DIR/${pod}-${c}.log" 2>/dev/null || true
    done
done

# Create tarball
tar -czf "${OUTPUT_DIR}.tar.gz" "$OUTPUT_DIR"
rm -rf "$OUTPUT_DIR"

echo "✅ Support bundle saved: ${OUTPUT_DIR}.tar.gz"
