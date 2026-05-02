#!/usr/bin/env bash
# Reset MySQL state in the playground — scales down, wipes data, and restarts.
# Handles stuck Terminating PVCs, stale taints, and leftover data on k3d nodes.
#
# This script's contract is "wipe everything for this cluster": data dirs,
# PVCs, MFG status fields the operator uses to gate fresh-deploy bootstrap,
# and stale node taints. If you've hand-set status.lastFailoverTarget for a
# debug session, this will erase it.
#
# Usage: ./playground/reset-mysql.sh
set -euo pipefail

NAMESPACE="bloodraven-playground"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FG_NAME="playground"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }

# Refuse to run outside a known-local cluster context (AUDIT M7).
# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"
require_playground_context

# Prefer docker over podman. k3d's podman support is experimental.
# Override with BLOODRAVEN_CONTAINER_RUNTIME=podman if needed. Falls back
# to empty (the data-wipe step then defaults to docker at use-site, which
# matches the legacy behavior on machines with neither runtime installed
# and no live cluster to wipe data from anyway).
if [[ -n "${BLOODRAVEN_CONTAINER_RUNTIME:-}" ]]; then
  RUNTIME="${BLOODRAVEN_CONTAINER_RUNTIME}"
elif command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
elif command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
else
  RUNTIME=""
fi

# ── 0. Scale operator to zero so taint cleanup and status edits stick ─────
info "Scaling operator to zero (no fight over taints/status)..."
kubectl -n "$NAMESPACE" scale deployment bloodraven --replicas=0 2>/dev/null || true
kubectl -n "$NAMESPACE" wait --for=delete pod -l app.kubernetes.io/name=bloodraven --timeout=30s 2>/dev/null || true

# ── 1. Scale down MySQL to release PVC references ─────────────────────────
info "Scaling down MySQL deployments..."
kubectl -n "$NAMESPACE" scale deployment mysql-playground-iad mysql-playground-pdx --replicas=0 2>/dev/null || true

info "Waiting for MySQL pods to terminate..."
kubectl -n "$NAMESPACE" wait --for=delete pod -l app.kubernetes.io/name=mysql --timeout=60s 2>/dev/null || true
# Extra wait in case pods are slow to terminate
sleep 5

# Verify no MySQL pods remain
REMAINING=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=mysql --no-headers 2>/dev/null | wc -l)
if [[ "$REMAINING" -gt 0 ]]; then
  warn "Force-deleting lingering MySQL pods..."
  kubectl -n "$NAMESPACE" delete pods -l app.kubernetes.io/name=mysql --force --grace-period=0 2>/dev/null || true
  sleep 5
fi

# ── 2. Delete PVCs (force if stuck in Terminating) ────────────────────────
info "Deleting PVCs..."
kubectl -n "$NAMESPACE" delete pvc --all --wait=false 2>/dev/null || true
sleep 3

# Check for stuck PVCs
STUCK=$(kubectl -n "$NAMESPACE" get pvc --no-headers 2>/dev/null | wc -l)
if [[ "$STUCK" -gt 0 ]]; then
  warn "PVCs stuck in Terminating — removing finalizers..."
  for pvc in $(kubectl -n "$NAMESPACE" get pvc -o name 2>/dev/null); do
    kubectl -n "$NAMESPACE" patch "$pvc" -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  done
  kubectl -n "$NAMESPACE" delete pvc --all --force --grace-period=0 2>/dev/null || true
  sleep 3
fi

# Also clean up orphaned PVs
for pv in $(kubectl get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="'"$NAMESPACE"'")]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  kubectl patch pv "$pv" -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl delete pv "$pv" --force --grace-period=0 2>/dev/null || true
done
ok "PVCs cleaned up"

# ── 3. Wipe data directories on k3d nodes ─────────────────────────────────
info "Wiping data directories on k3d nodes..."
for node in k3d-bloodraven-agent-0 k3d-bloodraven-agent-1 k3d-bloodraven-server-0; do
  ${RUNTIME:-docker} exec "$node" sh -c 'rm -rf /var/lib/rancher/k3s/storage/pvc-*' 2>/dev/null || true
done
ok "Data wiped"

# ── 3b. Restart local-path-provisioner (B2) ───────────────────────────────
# Resets the 15-failure threshold the provisioner accumulated against the
# PVCs we just deleted. Idempotent and ~3 seconds.
info "Restarting local-path-provisioner..."
if kubectl -n kube-system get deployment local-path-provisioner >/dev/null 2>&1; then
  kubectl -n kube-system rollout restart deployment local-path-provisioner >/dev/null 2>&1 || true
  kubectl -n kube-system rollout status deployment local-path-provisioner --timeout=60s >/dev/null 2>&1 || \
    warn "local-path-provisioner rollout did not complete in 60s (continuing)"
  ok "local-path-provisioner restarted"
else
  warn "local-path-provisioner not found in kube-system (skipping)"
fi

# ── 4. Remove db-readonly taints (operator is down — single-shot) ─────────
info "Removing db-readonly taints..."
for node in $(kubectl get nodes -o name); do
  kubectl taint "$node" shipstream.io/db-readonly-playground- 2>/dev/null || true
  kubectl taint "$node" shipstream.io/db-readonly- 2>/dev/null || true
done
ok "Taints cleared"

# ── 5. Reapply secret ────────────────────────────────────────────────────
info "Reapplying mysql-secret..."
kubectl apply -f "$SCRIPT_DIR/manifests/mysql-secret.yaml"

# ── 6. Clear stale CR status (B3) ────────────────────────────────────────
# isFreshDeploy is gated on status.lastFailover{Target}; without this the
# operator skips bootstrap and stalls on matrix.go's "both read-only" guard.
# Operator is still scaled to 0 here, so the patch isn't immediately
# overwritten. JSON-Patch /status subresource directly.
info "Clearing stale MFG status fields..."
if kubectl -n "$NAMESPACE" get mysqlfailovergroup "$FG_NAME" >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" patch "mysqlfailovergroup/$FG_NAME" \
    --subresource=status --type=json -p='[
      {"op":"remove","path":"/status/lastFailover"},
      {"op":"remove","path":"/status/lastFailoverTarget"},
      {"op":"remove","path":"/status/promotionGtidExecuted"},
      {"op":"remove","path":"/status/plannedFailover"},
      {"op":"remove","path":"/status/recovery"},
      {"op":"remove","path":"/status/dragonfly"}
    ]' 2>/dev/null || true
  ok "MFG status cleared (missing fields ignored)"
else
  warn "MFG $FG_NAME not found — skipping status clear"
fi

# ── 7. Scale MySQL back up ───────────────────────────────────────────────
# With the operator still down, the StatefulSet/Deployment scale-up is
# enough to provision PVCs via local-path-provisioner. The operator will
# come up afterward and reconcile the bootstrap.
info "Scaling MySQL deployments back up..."
kubectl -n "$NAMESPACE" scale deployment mysql-playground-iad mysql-playground-pdx --replicas=1

# ── 8. Wait for pods to become ready ─────────────────────────────────────
info "Waiting for MySQL pods to become ready (up to 120s)..."
READY=0
for i in $(seq 1 24); do
  READY=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=mysql \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[*].ready}{"\n"}{end}' 2>/dev/null \
    | grep -c "true true" || true)
  if [[ "$READY" -ge 2 ]]; then
    ok "Both MySQL pods are ready!"
    break
  fi
  echo "  ... waiting ($i/24) — $READY/2 pods ready"
  sleep 5
done

if [[ "$READY" -lt 2 ]]; then
  # ── 8b. Dump-on-timeout (B4) ───────────────────────────────────────────
  TS=$(date -u +%Y%m%dT%H%M%SZ)
  DUMP_DIR="$REPO_ROOT/playground/chaos-results/reset-$TS"
  mkdir -p "$DUMP_DIR"
  warn "Timed out waiting for MySQL pods. Dumping forensics to:"
  echo "    $DUMP_DIR"
  {
    echo "# reset-mysql.sh wait-loop timeout dump @ $TS"
    echo "# context: $(kubectl config current-context 2>/dev/null || echo unknown)"
    echo "# namespace: $NAMESPACE"
    echo "# ready: $READY/2"
  } > "$DUMP_DIR/README.txt"
  kubectl -n "$NAMESPACE" get pods -o wide > "$DUMP_DIR/pods.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" describe pods -l app.kubernetes.io/name=mysql > "$DUMP_DIR/mysql-pods-describe.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get pvc -o wide > "$DUMP_DIR/pvc.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" describe pvc > "$DUMP_DIR/pvc-describe.txt" 2>&1 || true
  kubectl get pv -o wide > "$DUMP_DIR/pv.txt" 2>&1 || true
  kubectl get nodes -o yaml > "$DUMP_DIR/nodes.yaml" 2>&1 || true
  {
    echo "# node taints"
    kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.taints}{"\n"}{end}' 2>&1 || true
  } > "$DUMP_DIR/node-taints.txt"
  kubectl -n "$NAMESPACE" get events --sort-by='.lastTimestamp' > "$DUMP_DIR/events.txt" 2>&1 || true
  for site in iad pdx; do
    for c in mysql sidecar; do
      kubectl -n "$NAMESPACE" logs "deploy/mysql-playground-$site" -c "$c" --tail=30 \
        > "$DUMP_DIR/mysql-$site-$c.log" 2>&1 || true
    done
  done
  echo ""
  warn "Reset did not converge. Investigate the dump above, then re-run."
  exit 1
fi

# ── 9. Verify data directory is populated ─────────────────────────────────
echo ""
for site in iad pdx; do
  FILES=$(kubectl -n "$NAMESPACE" exec "deploy/mysql-playground-$site" \
    -c mysql -- ls /var/lib/mysql/ 2>/dev/null | wc -l)
  if [[ "$FILES" -gt 0 ]]; then
    ok "$site: data directory has $FILES entries"
  else
    warn "$site: data directory is EMPTY — MySQL may not have initialized properly"
  fi
done

# ── 10. Create replication user ───────────────────────────────────────────
info "Creating replication user on both MySQL sites..."
REPL_USER=$(kubectl -n "$NAMESPACE" get secret mysql-credentials -o jsonpath='{.data.MYSQL_REPLICATION_USER}' 2>/dev/null | base64 -d)
REPL_PASS=$(kubectl -n "$NAMESPACE" get secret mysql-credentials -o jsonpath='{.data.MYSQL_REPLICATION_PASSWORD}' 2>/dev/null | base64 -d)
ROOT_PASS=$(kubectl -n "$NAMESPACE" get secret mysql-credentials -o jsonpath='{.data.MYSQL_ROOT_PASSWORD}' 2>/dev/null | base64 -d)
if [[ -n "$REPL_USER" && -n "$ROOT_PASS" ]]; then
  for site in iad pdx; do
    kubectl -n "$NAMESPACE" exec "deploy/mysql-playground-$site" -c mysql -- \
      mysql "-uroot" "-p${ROOT_PASS}" -e \
      "CREATE USER IF NOT EXISTS '${REPL_USER}'@'%' IDENTIFIED BY '${REPL_PASS}'; \
       GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO '${REPL_USER}'@'%'; \
       FLUSH PRIVILEGES;" 2>/dev/null && ok "Replication user created on $site" || warn "Failed to create replication user on $site"
  done
fi

# ── 11. Bring the operator back ──────────────────────────────────────────
info "Scaling operator back up..."
kubectl -n "$NAMESPACE" scale deployment bloodraven --replicas=1
kubectl -n "$NAMESPACE" rollout status deployment/bloodraven --timeout=60s 2>/dev/null || true
ok "Operator running"

echo ""
kubectl -n "$NAMESPACE" get pods -o wide -l app.kubernetes.io/name=mysql
echo ""
kubectl -n "$NAMESPACE" get pvc 2>/dev/null || true
echo ""
info "Done. Check operator logs with:"
echo "  kubectl -n $NAMESPACE logs -l app.kubernetes.io/name=bloodraven --tail=20"
