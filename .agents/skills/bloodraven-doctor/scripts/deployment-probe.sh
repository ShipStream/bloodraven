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
kubectl get mfg "$MFG_NAME" -n "$NAMESPACE" -o json | jq '{
    group: .metadata.name, namespace: .metadata.namespace,
    activeSite: .status.activeSite, topologyGeneration: .status.topologyGeneration,
    plannedFailover: .status.plannedFailover, updatePhase: .status.updatePhase,
    restoreInPlace: .status.restoreInPlace, dragonflyUpgrade: .status.dragonfly.upgrade,
    revokeRequested: (.metadata.annotations | has("bloodraven.shipstream.io/revoke-deployment-leases"))
}'

echo "--- Declared deployment callers ---"
kubectl get mysqldatabases.shipstream.io -n "$NAMESPACE" -o json | jq --arg group "$MFG_NAME" '
    [.items[] | select(.spec.groupRef.name == $group) |
    {instance: .metadata.name, deploymentClients: (.spec.deploymentClients // [])}]'

echo "--- Durable deployment Lease records (not leader election) ---"
kubectl get leases.coordination.k8s.io -n "$NAMESPACE" -o json | jq --arg group "$MFG_NAME" '
    [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "MysqlFailoverGroup" and .name == $group)) |
    select(.metadata.annotations["bloodraven.shipstream.io/deployment-lease"] != null) |
    {name: .metadata.name, record: (.metadata.annotations["bloodraven.shipstream.io/deployment-lease"] |
        (try fromjson catch {state: "invalid_record"}) | {kind, operationId, instance, namespace, serviceAccount, expiresAt,
                    topologyGeneration, createdAt, state, reason})}]'

echo "Lease records are observations; expiry is enforced by the operator clock. Do not delete or edit them."
