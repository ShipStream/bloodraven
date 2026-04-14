#!/usr/bin/env bash
# Reset MySQL state in the playground — scales down, wipes data, and restarts.
# Handles stuck Terminating PVCs, stale taints, and leftover data on k3d nodes.
# Usage: ./playground/reset-mysql.sh
set -euo pipefail

NAMESPACE="bloodraven-playground"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }

# Prefer podman (rootless, no daemon) over docker
if command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
elif command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
else
  RUNTIME=""
fi

# ── 0. Scale operator to zero to prevent taint/PVC race ──────────────────
info "Scaling operator to zero (prevents taint race during reset)..."
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

# ── 4. Remove db-readonly taints ──────────────────────────────────────────
info "Removing db-readonly taints..."
for node in $(kubectl get nodes -o name); do
  kubectl taint "$node" shipstream.io/db-readonly- 2>/dev/null || true
done
ok "Taints cleared"

# ── 5. Reapply secret ────────────────────────────────────────────────────
info "Reapplying mysql-secret..."
kubectl apply -f "$SCRIPT_DIR/manifests/mysql-secret.yaml"

# ── 6. Scale back up ─────────────────────────────────────────────────────
# Remove taints before scale-up (operator is still at zero, so they won't reappear)
for node in $(kubectl get nodes -o name); do
  kubectl taint "$node" shipstream.io/db-readonly- 2>/dev/null || true
done
info "Scaling MySQL deployments back up..."
kubectl -n "$NAMESPACE" scale deployment mysql-playground-iad mysql-playground-pdx --replicas=1

# ── 7. Wait for pods to become ready ─────────────────────────────────────
info "Waiting for MySQL pods to become ready (up to 120s)..."
for i in $(seq 1 24); do
  READY=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=mysql \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[*].ready}{"\n"}{end}' 2>/dev/null \
    | grep -c "true true" || true)
  if [[ "$READY" -ge 2 ]]; then
    ok "Both MySQL pods are ready!"
    break
  fi
  if [[ "$i" -eq 24 ]]; then
    warn "Timed out waiting for MySQL pods. Check logs:"
    echo "  kubectl -n $NAMESPACE logs -l app.kubernetes.io/name=mysql -c mysql --tail=10"
    echo "  kubectl -n $NAMESPACE logs -l app.kubernetes.io/name=mysql -c sidecar --tail=10"
  fi
  # Clear taints each iteration in case operator reapplied them
  for node in $(kubectl get nodes -o name); do
    kubectl taint "$node" shipstream.io/db-readonly- 2>/dev/null || true
  done
  echo "  ... waiting ($i/24) — $READY/2 pods ready"
  sleep 5
done

# ── 8. Verify data directory is populated ─────────────────────────────────
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

# ── 9. Create replication user ────────────────────────────────────────────
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

# ── 10. Scale operator back up ────────────────────────────────────────────
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
