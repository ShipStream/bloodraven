#!/usr/bin/env bash
#
# bloodraven-doctor: Deep Sidecar HTTP Endpoint Prober
# Automatically discovers container names dynamically without assuming fixed names.
#
set -euo pipefail

NAMESPACE=""
MFG_NAME=""
ENDPOINT="status" # fencing is the shipped alias for /status (self_fenced)

usage() {
    echo "Usage: $0 [-n <namespace>] [<mysql-failover-group>] [status|keyring|fencing|health|livez|peer|archiver]"
    echo "Probes sidecar HTTP endpoints on Bloodraven MySQL pods."
    echo "  health  readiness: MySQL accepts a connection within 2s (503 when not)"
    echo "  livez   liveness: process up and fencing loop making progress (never touches MySQL)"
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
        status|keyring|fencing|health|livez|peer|archiver)
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
        TARGET_PATH="/status"
        ;;
    health)
        TARGET_PATH="/health"
        ;;
    livez)
        TARGET_PATH="/livez"
        ;;
    peer)
        TARGET_PATH="/peer/active-site"
        ;;
    archiver)
        TARGET_PATH="/archiver/status"
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

# The /keyring/status coverage flags systemTablespaceEncrypted,
# redoLogEncrypted, undoLogEncrypted and binlogEncrypted are omitted when
# MySQL did not report them (no row, NULL, or an unrecognized value). An
# absent flag is unknown, never false: report it as "not reported".
summarize_keyring_coverage() {
    command -v jq >/dev/null 2>&1 || return 0
    local body
    body=$(printf '%s\n' "$1" | sed '/^(HTTP [0-9]*)$/d')
    printf '%s\n' "$body" | jq -r '
        if type != "object" then empty
        elif .coverage == null then "   coverage: not reported (no coverage block)"
        else .coverage as $c |
            "   coverage: " + ([ "systemTablespaceEncrypted", "redoLogEncrypted", "undoLogEncrypted", "binlogEncrypted"]
                | map(. as $k | $k + "=" + (if ($c | has($k)) then ($c[$k] | tostring) else "not-reported" end))
                | join(" ")) + " unencryptedTablespaces=" + ($c.unencryptedTablespaces // 0 | tostring)
        end' 2>/dev/null || true
}

# The /archiver/status tail block says what the last scan decided about the
# sealed binlogs the site still has to archive: Drained, Pending or Blocked
# (with a reason). A read-only site archives only the tail of its last
# writable era. The block is absent until a scan has read the binlog index,
# and from sidecars that predate it: absent is "not reported", never Drained.
summarize_archiver_tail() {
    command -v jq >/dev/null 2>&1 || return 0
    local body
    body=$(printf '%s\n' "$1" | sed '/^(HTTP [0-9]*)$/d')
    printf '%s\n' "$body" | jq -r '
        if type != "object" then empty
        elif .enabled != true then "   tail: not reported (no archiver runs on this pod)"
        elif (.tail | type) != "object" then "   tail: not reported (no scan has read the binlog index yet, or the sidecar predates the tail block)"
        else .tail as $t |
            "   tail: state=" + ($t.state // "?") + " activeFile=" + ($t.activeFile // "none")
            + " newestSealedFile=" + ($t.newestSealedFile // "none")
            + " pendingFiles=" + ($t.pendingFiles // 0 | tostring)
            + (if ($t.reason // "") != "" then " reason=" + $t.reason else "" end)
        end' 2>/dev/null || true
}

for pod in $PODS; do
    echo ""
    echo "📍 Pod: $pod"

    CANDIDATES=$(find_candidate_containers "$pod" "$NAMESPACE")
    RESULT=""
    SUCCESSFUL_CONTAINER=""

    for container in $CANDIDATES; do
        RESULT=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- wget -T 5 -qO- "http://127.0.0.1:8080${TARGET_PATH}" 2>/dev/null || true)

        # If wget was missing or the endpoint answered non-2xx (wget
        # prints no body then), try curl. curl keeps the body and status
        # of a 503: for /health and /livez that is the answer, not a
        # failure to fetch.
        if [[ -z "$RESULT" ]]; then
            RESULT=$(kubectl exec -n "$NAMESPACE" "$pod" -c "$container" -- curl -sS --max-time 5 -w '\n(HTTP %{http_code})' "http://127.0.0.1:8080${TARGET_PATH}" 2>/dev/null || true)
        fi

        if [[ -n "$RESULT" ]]; then
            SUCCESSFUL_CONTAINER="$container"
            break
        fi
    done

    if [[ -n "$RESULT" ]]; then
        echo "   (queried via container: $SUCCESSFUL_CONTAINER)"
        echo "$RESULT"
        if [[ "$ENDPOINT" == "keyring" ]]; then
            summarize_keyring_coverage "$RESULT"
        fi
        if [[ "$ENDPOINT" == "archiver" ]]; then
            summarize_archiver_tail "$RESULT"
        fi
    else
        echo "⚠️ Unable to fetch $TARGET_PATH from sidecar on pod $pod (tried containers: $CANDIDATES)."
    fi
done
echo ""
