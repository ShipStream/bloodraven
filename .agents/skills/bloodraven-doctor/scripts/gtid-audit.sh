#!/usr/bin/env bash
#
# bloodraven-doctor: GTID Consistency and Divergence Auditor
# Dynamically discovers container names and falls back gracefully.
#
set -euo pipefail

NAMESPACE=""
MFG_NAME=""

usage() {
    echo "Usage: $0 [-n <namespace>] [<mysql-failover-group>]"
    echo "Audits Executed_Gtid_Set across all sites in a Bloodraven MySQL cluster."
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

echo "======================================================================="
echo "🧬 Bloodraven Doctor: GTID Consistency Audit"
echo "Group: $MFG_NAME | Namespace: $NAMESPACE"
echo "======================================================================="

PODS=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$MFG_NAME" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)

if [[ -z "$PODS" ]]; then
    echo "❌ No MySQL pods found for instance '$MFG_NAME'."
    exit 1
fi

find_mysql_container() {
    local pod="$1"
    local ns="$2"
    local all_containers
    all_containers=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || echo "")

    for c in $all_containers; do
        if [[ "$c" == "mysql" ]]; then
            echo "$c"
            return 0
        fi
    done
    for c in $all_containers; do
        if [[ "$c" == *"mysql"* ]]; then
            echo "$c"
            return 0
        fi
    done
    echo "$all_containers" | awk '{print $1}'
}

for pod in $PODS; do
    echo ""
    echo "📍 Probing Pod: $pod"
    SITE_LABEL=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/site}' 2>/dev/null || echo "unknown")
    echo "   Site: $SITE_LABEL"

    MYSQL_CONTAINER=$(find_mysql_container "$pod" "$NAMESPACE")

    GTID_EXECUTED=""
    READ_ONLY=""

    # 1. Try querying mysqld in pod
    GTID_EXECUTED=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$MYSQL_CONTAINER" -- mysql -N -B -e "SELECT @@GLOBAL.gtid_executed;" 2>/dev/null || true)
    READ_ONLY=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$MYSQL_CONTAINER" -- mysql -N -B -e "SELECT @@GLOBAL.read_only;" 2>/dev/null || true)

    # 2. If direct mysql failed (e.g. auth required), query from CR status for this site
    if [[ -z "$GTID_EXECUTED" && -n "$SITE_LABEL" ]]; then
        GTID_EXECUTED=$(kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{.status.sites[?(@.name=='$SITE_LABEL')].gtidExecuted}" 2>/dev/null || true)
        READ_ONLY=$(kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{.status.sites[?(@.name=='$SITE_LABEL')].readOnly}" 2>/dev/null || true)
        if [[ -n "$GTID_EXECUTED" ]]; then
            echo "   (extracted via CR status)"
        fi
    fi

    if [[ -n "$GTID_EXECUTED" ]]; then
        echo "   read_only:       ${READ_ONLY:-unknown}"
        echo "   gtid_executed:   $GTID_EXECUTED"
    else
        echo "   ⚠️ Unable to retrieve GTID coordinates for pod $pod (pod may be restarting or initializing)"
    fi
done

echo ""
echo "======================================================================="
echo "💡 Interpretation Guide:"
echo " - If a replica's gtid_executed contains transaction UUID:sequence intervals"
echo "   NOT present on the active primary, transactions have DIVERGED."
echo " - Divergent replicas cannot replicate without a reclone:"
echo "   kubectl bloodraven reclone $MFG_NAME -n $NAMESPACE --target=<divergent-site>"
echo "======================================================================="
