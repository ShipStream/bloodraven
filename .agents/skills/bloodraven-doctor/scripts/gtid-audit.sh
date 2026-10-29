#!/usr/bin/env bash
#
# bloodraven-doctor: follower-first GTID divergence audit.
#
# For every follower of a Bloodraven MySQL failover group, reports whether it
# holds transactions the confirmed primary lacks:
#
#   1. read the follower's @@server_uuid and @@GLOBAL.gtid_executed FIRST;
#   2. then, ON THE PRIMARY, SELECT GTID_SUBTRACT('<follower set>', @@GLOBAL.gtid_executed).
#
# A follower's set only grows with transactions that are on the primary
# first, so whatever the subtraction leaves is history the primary never had.
# Reading in the other order would report a transaction the follower applied
# between the reads. The result is UNKNOWN (never CLEAN) when a read fails,
# the primary is not confirmed writable, or either server's server_uuid
# changed between the reads.
#
# Healthy replication threads prove nothing here: a divergent replica can
# keep applying the primary's new transactions until a conflicting row
# stops it (issue #202).
#
# Read-only: issues SELECTs only.
#
# Results count only when status.topologyGeneration is readable and
# unchanged across the audit; otherwise every result is UNKNOWN.
#
# Uses the full resource name mysqlfailovergroups.shipstream.io, so it also
# works against a CRD from before the "mfg" short name existed.
#
# Exit status: 0 every follower CLEAN; 2 at least one DIVERGENT; 3 none
# divergent but at least one UNKNOWN; 1 usage or discovery error.
#
set -uo pipefail

NAMESPACE=""
MFG_NAME=""

usage() {
    echo "Usage: $0 [-n <namespace>] [<mysql-failover-group>]"
    echo "Follower-first audit: does any follower hold transactions the primary lacks?"
    echo "Exit: 0 all CLEAN, 2 a follower is DIVERGENT, 3 a follower is UNKNOWN, 1 usage/discovery error."
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)
            [[ $# -ge 2 ]] || usage 1
            NAMESPACE="$2"
            shift 2
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            if [[ -z "$MFG_NAME" ]]; then
                MFG_NAME="$1"
                shift
            else
                usage 1
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
echo "🧬 Bloodraven Doctor: follower-first GTID divergence audit"
echo "Group: $MFG_NAME | Namespace: $NAMESPACE"
GENERATION_BEFORE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.topologyGeneration}' 2>/dev/null || true)
ACTIVE_SITE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.activeSite}' 2>/dev/null || true)
echo "Topology generation before audit: ${GENERATION_BEFORE:-unavailable}"
echo "Active site (status.activeSite): ${ACTIVE_SITE:-none}"
echo "======================================================================="

SITES=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.spec.sites[*].name}' 2>/dev/null || true)
if [[ -z "$SITES" ]]; then
    echo "❌ Could not read spec.sites for $MFG_NAME."
    exit 1
fi

find_mysql_container() {
    local pod="$1"
    local all_containers
    all_containers=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || echo "")
    for c in $all_containers; do
        [[ "$c" == "mysql" ]] && { echo "$c"; return 0; }
    done
    for c in $all_containers; do
        [[ "$c" == *"mysql"* ]] && { echo "$c"; return 0; }
    done
    echo "$all_containers" | awk '{print $1}'
}

# site_pod prints the single Running pod of a site, or nothing when there is
# none or more than one (a rollout in progress: the answer would be ambiguous).
site_pod() {
    local site="$1" pods
    # name<TAB>deletionTimestamp; a terminating pod still reports Running.
    pods=$(kubectl get pods -n "$NAMESPACE" \
        -l "app.kubernetes.io/name=mysql,app.kubernetes.io/instance=$MFG_NAME,shipstream.io/site=$site" \
        --field-selector=status.phase=Running \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.deletionTimestamp}{"\n"}{end}' 2>/dev/null \
        | awk -F'\t' 'NF && $2 == "" {print $1}' || true)
    if [[ $(printf '%s\n' "$pods" | grep -c .) -eq 1 ]]; then
        printf '%s' "$pods"
    fi
}

# run_sql <pod> <sql>: one batch row, tab-separated, as root through the
# pod's own MYSQL_ROOT_PASSWORD. Fails (non-zero) on any error.
run_sql() {
    local pod="$1" sql="$2" container
    container=$(find_mysql_container "$pod")
    kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- \
        sh -c 'MYSQL_PWD="${MYSQL_ROOT_PASSWORD:-}" exec mysql -uroot -N -B -e "$1"' sh "$sql" 2>/dev/null
}

# GTID sets contain only UUIDs, tags, interval numbers and separators.
valid_gtid_set() {
    [[ "$1" =~ ^[A-Za-z0-9_:,\ -]*$ ]]
}

PRIMARY_POD=""
PRIMARY_PROBLEM=""
if [[ -z "$ACTIVE_SITE" ]]; then
    PRIMARY_PROBLEM="status.activeSite is empty: there is no confirmed primary to compare against"
else
    PRIMARY_POD=$(site_pod "$ACTIVE_SITE")
    if [[ -z "$PRIMARY_POD" ]]; then
        PRIMARY_PROBLEM="no single Running pod for active site $ACTIVE_SITE"
    else
        PRIMARY_RO=$(run_sql "$PRIMARY_POD" "SELECT @@GLOBAL.read_only" || true)
        if [[ "$PRIMARY_RO" != "0" ]]; then
            PRIMARY_PROBLEM="active site $ACTIVE_SITE ($PRIMARY_POD) is not confirmed writable (read_only=${PRIMARY_RO:-unreadable})"
        fi
    fi
fi

CLEAN=0
DIVERGENT=0
UNKNOWN=0

for site in $SITES; do
    [[ "$site" == "$ACTIVE_SITE" ]] && continue
    echo ""
    echo "📍 Follower: $site"
    RECORDED_STATE=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{.status.sites[?(@.name=='$site')].recoveryState}" 2>/dev/null || true)
    RECORDED_GTID=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{.status.sites[?(@.name=='$site')].divergentGtid}" 2>/dev/null || true)
    RECORDED_BY=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath="{.status.sites[?(@.name=='$site')].divergenceDetectedBy}" 2>/dev/null || true)
    echo "   Operator record: recoveryState=${RECORDED_STATE:-none} divergenceDetectedBy=${RECORDED_BY:-none} divergentGtid=${RECORDED_GTID:-none}"

    if [[ -n "$PRIMARY_PROBLEM" ]]; then
        echo "   ❔ UNKNOWN: $PRIMARY_PROBLEM"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi
    FOLLOWER_POD=$(site_pod "$site")
    if [[ -z "$FOLLOWER_POD" ]]; then
        echo "   ❔ UNKNOWN: no single Running pod for $site"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi

    # Primary identity before the follower read.
    PRIMARY_UUID_BEFORE=$(run_sql "$PRIMARY_POD" "SELECT @@server_uuid" || true)
    # Follower FIRST.
    FOLLOWER_ROW=$(run_sql "$FOLLOWER_POD" "SELECT @@server_uuid, REPLACE(@@GLOBAL.gtid_executed, '\n', '')" || true)
    FOLLOWER_UUID=$(printf '%s' "$FOLLOWER_ROW" | cut -f1)
    FOLLOWER_GTID=$(printf '%s' "$FOLLOWER_ROW" | cut -f2-)
    if [[ -z "$PRIMARY_UUID_BEFORE" || -z "$FOLLOWER_UUID" ]]; then
        echo "   ❔ UNKNOWN: could not read the primary or the follower (auth, restart, or network)"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi
    if ! valid_gtid_set "$FOLLOWER_GTID"; then
        echo "   ❔ UNKNOWN: follower gtid_executed is not a GTID set: $FOLLOWER_GTID"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi
    # Then the subtraction ON THE PRIMARY.
    PRIMARY_ROW=$(run_sql "$PRIMARY_POD" "SELECT @@server_uuid, @@GLOBAL.read_only, REPLACE(GTID_SUBTRACT('$FOLLOWER_GTID', @@GLOBAL.gtid_executed), '\n', '')" || true)
    PRIMARY_UUID_AFTER=$(printf '%s' "$PRIMARY_ROW" | cut -f1)
    PRIMARY_RO_AFTER=$(printf '%s' "$PRIMARY_ROW" | cut -f2)
    EXTRA=$(printf '%s' "$PRIMARY_ROW" | cut -f3-)
    FOLLOWER_UUID_AFTER=$(run_sql "$FOLLOWER_POD" "SELECT @@server_uuid" || true)

    if [[ -z "$PRIMARY_ROW" ]]; then
        echo "   ❔ UNKNOWN: GTID_SUBTRACT on the primary failed"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi
    if [[ "$PRIMARY_UUID_BEFORE" != "$PRIMARY_UUID_AFTER" || "$FOLLOWER_UUID" != "$FOLLOWER_UUID_AFTER" ]]; then
        echo "   ❔ UNKNOWN: a server was replaced during the audit (server_uuid changed); repeat it"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi
    if [[ "$PRIMARY_RO_AFTER" != "0" ]]; then
        echo "   ❔ UNKNOWN: the primary turned read-only during the audit; repeat it once authority settles"
        UNKNOWN=$((UNKNOWN + 1))
        continue
    fi

    echo "   follower server_uuid: $FOLLOWER_UUID"
    echo "   follower gtid_executed: $FOLLOWER_GTID"
    if [[ -z "$EXTRA" ]]; then
        echo "   ✅ CLEAN: $site holds no transactions the primary ($ACTIVE_SITE) lacks"
        CLEAN=$((CLEAN + 1))
        if [[ "$RECORDED_STATE" == "RecoveryBlocked" ]]; then
            echo "      The operator still records a block; its next audit (within ~30s) clears it."
        fi
    else
        echo "   🔴 DIVERGENT: $site holds transactions the primary ($ACTIVE_SITE) lacks:"
        echo "      $EXTRA"
        DIVERGENT=$((DIVERGENT + 1))
        if [[ "$RECORDED_STATE" != "RecoveryBlocked" ]]; then
            echo "      The operator has not recorded this yet (operator older than v1.6.0, or its first audit is pending)."
        fi
    fi
done

GENERATION_AFTER=$(kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o jsonpath='{.status.topologyGeneration}' 2>/dev/null || true)
echo ""
echo "Topology generation after audit: ${GENERATION_AFTER:-unavailable}"
# Every result was read against one authority only if the topology
# generation is known and unchanged; otherwise nothing above is verified.
if [[ -z "$GENERATION_BEFORE" || -z "$GENERATION_AFTER" ]]; then
    echo "WARNING: Topology stability could not be confirmed (status.topologyGeneration unavailable). Results are UNKNOWN; repeat after stabilization; do not resume deployment DDL."
    UNKNOWN=$((UNKNOWN + CLEAN + DIVERGENT))
    CLEAN=0
    DIVERGENT=0
elif [[ "$GENERATION_BEFORE" != "$GENERATION_AFTER" ]]; then
    echo "WARNING: Topology changed during this audit. Results are UNKNOWN; repeat after stabilization; do not resume deployment DDL."
    UNKNOWN=$((UNKNOWN + CLEAN + DIVERGENT))
    CLEAN=0
    DIVERGENT=0
fi

echo ""
echo "Summary: clean=$CLEAN divergent=$DIVERGENT unknown=$UNKNOWN"
echo "======================================================================="
echo "💡 Interpretation Guide:"
echo " - CLEAN proves only that the follower's set (as read) is contained in the"
echo "   primary's. It says nothing about lag or row contents."
echo " - DIVERGENT: the listed transactions exist only on the follower. A divergent"
echo "   replica can keep replicating with healthy IO/SQL threads; thread health is"
echo "   not a containment proof. Ordered updates and planned failovers will not"
echo "   promote it. Preserve the events first:"
echo "     mysqlbinlog --include-gtids='<set>' <binlog files on the follower>"
echo "   then either discard them:  kubectl bloodraven reclone $MFG_NAME <site> -n $NAMESPACE"
echo "   or keep them: replay the decoded events on the primary."
echo "   Committing empty transactions on the primary (per GTID: SET GTID_NEXT='<uuid>:<n>';"
echo "   BEGIN; COMMIT; then SET GTID_NEXT='AUTOMATIC') only ACCEPTS the divergence: the"
echo "   replica keeps rows the primary lacks and becomes promotable again, and a later"
echo "   promotion makes those rows authoritative. See playbook 33."
echo " - UNKNOWN is not CLEAN. Fix the reason shown and run the audit again."
echo "======================================================================="

if [[ $DIVERGENT -gt 0 ]]; then
    exit 2
fi
if [[ $UNKNOWN -gt 0 ]]; then
    exit 3
fi
exit 0
