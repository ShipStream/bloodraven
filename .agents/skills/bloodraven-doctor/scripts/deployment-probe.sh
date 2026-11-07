#!/usr/bin/env bash
# Read-only deployment coordination evidence; never outputs tokens or hashes.
set -euo pipefail

if [[ $# != 2 ]]; then
    echo "Usage: bash $0 <namespace> <group>" >&2
    exit 1
fi
NAMESPACE="$1"
MFG_NAME="$2"
for tool in kubectl jq; do
    command -v "$tool" >/dev/null || { echo "Required tool missing: $tool" >&2; exit 1; }
done

echo "--- Deployment topology and planned operations ---"
kubectl get mysqlfailovergroups.shipstream.io "$MFG_NAME" -n "$NAMESPACE" -o json | jq '{
    group: .metadata.name, namespace: .metadata.namespace,
    activeSite: .status.activeSite, topologyGeneration: .status.topologyGeneration,
    plannedFailover: (.status.plannedFailover // null | if . == null then null else {
        runID, phase, sourceSite, targetSite,
        outcome: .result.outcome, reason: .result.reason, warnings: .result.warnings,
        waitReason: .admission.waitReason, deferReason: .admission.deferReason,
        retryTime: .admission.retryTime, deadlineTime: .admission.deadlineTime,
        fenceState: .fence.state, promotionState: .promotion.state, rollbackReason: .rollback.reason,
        message} end),
    plannedFailoverTerminalMarker: (.metadata.annotations // {})["bloodraven.shipstream.io/planned-failover-terminal"],
    plannedFailoverHold: .status.plannedFailoverHold,
    automaticFailoverHeld: ([.status.conditions[]? | select(.type == "AutomaticFailoverHeld") | {status, reason, observedGeneration}] | first),
    # The hold the operator enforces (its PlannedHoldFrom): the status record,
    # else a nameless hold from a True condition, else a terminal marker with
    # outcome ManualRecoveryRequired (a run that ended while status writes
    # were stalled). null = no hold.
    plannedHold: (
        ((.metadata.annotations // {})["bloodraven.shipstream.io/planned-failover-terminal"] // "" |
            (try fromjson catch null) |
            if type == "object" and .schemaVersion == 1 and (.runID | type) == "string" and .runID != "" then . else null end) as $marker |
        ([.status.conditions[]? | select(.type == "AutomaticFailoverHeld" and .status == "True")] | length > 0) as $condHeld |
        if .status.plannedFailoverHold != null then
            {runID: .status.plannedFailoverHold.runID, reason: .status.plannedFailoverHold.reason,
             source: (if $marker != null and $marker.outcome == "ManualRecoveryRequired" and $marker.runID == .status.plannedFailoverHold.runID then "status+marker" else "status" end)}
        elif $condHeld then {runID: "", reason: "", source: "condition"}
        elif $marker != null and $marker.outcome == "ManualRecoveryRequired" then {runID: $marker.runID, reason: ($marker.reason // ""), source: "marker"}
        else null end),
    updatePhase: .status.updatePhase,
    restoreInPlace: .status.restoreInPlace, dragonflyUpgrade: .status.dragonfly.upgrade,
    restoreReclones: (.status.restoreReclones // null | if . == null then null else {phase, pending} end),
    degraded: ([.status.conditions[]? | select(.type == "Degraded") | {status, reason}] | first),
    revokeRequested: ((.metadata.annotations // {}) | has("bloodraven.shipstream.io/revoke-deployment-leases"))
}'

echo "Degraded reason RestoreReclonePending (restoreReclones.pending non-empty) keeps the deployment API unstable until the post-restore reclones finish or a Held queue is resolved (playbook 20)."
echo "A non-terminal plannedFailover phase, a planned-failover claim, or a hold (plannedHold non-null: automaticFailoverHeld status=True, status.plannedFailoverHold, or a ManualRecoveryRequired terminal marker) keeps it unstable (unstableReason PlannedFailover); a hold needs a human acknowledgement (playbook 38)."

echo "--- Declared deployment callers ---"
kubectl get mysqldatabases.shipstream.io -n "$NAMESPACE" -o json | jq --arg group "$MFG_NAME" '
    [.items[] | select(.spec.failoverGroupRef.name == $group) |
    {instance: .metadata.name, deploymentClients: (.spec.deploymentClients // [])}]'

echo "--- Durable deployment Lease records (not leader election) ---"
kubectl get leases.coordination.k8s.io -n "$NAMESPACE" -o json | jq --arg group "$MFG_NAME" '
    [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "MysqlFailoverGroup" and .name == $group)) |
    select(.metadata.annotations["bloodraven.shipstream.io/deployment-lease"] != null) |
    {name: .metadata.name, record: (.metadata.annotations["bloodraven.shipstream.io/deployment-lease"] |
        (try fromjson catch {state: "invalid_record"}) | {kind, operationId, instance, namespace, serviceAccount, expiresAt,
                    topologyGeneration, createdAt, state, reason})}]'

echo "Lease records are observations; expiry is enforced by the operator clock. Do not delete or edit them."
