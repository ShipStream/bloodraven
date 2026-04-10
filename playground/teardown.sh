#!/usr/bin/env bash
# Tears down the Bloodraven playground resources from the current cluster.
# Does NOT delete the cluster itself — that's your responsibility.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="bloodraven-playground"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }

info "Uninstalling Bloodraven Helm release..."
helm uninstall bloodraven -n "$NAMESPACE" 2>/dev/null || true

info "Deleting playground manifests..."
kubectl delete -f "$SCRIPT_DIR/manifests/counter-app.yaml" --ignore-not-found
kubectl delete -f "$SCRIPT_DIR/manifests/dashboard.yaml" --ignore-not-found
kubectl delete -f "$SCRIPT_DIR/manifests/failovergroup.yaml" --ignore-not-found
kubectl delete -f "$SCRIPT_DIR/manifests/external-dns.yaml" --ignore-not-found
kubectl delete -f "$SCRIPT_DIR/manifests/dashboard-rbac.yaml" --ignore-not-found
kubectl delete -f "$SCRIPT_DIR/manifests/mysql-secret.yaml" --ignore-not-found
kubectl delete -f "$SCRIPT_DIR/manifests/storageclass.yaml" --ignore-not-found

info "Removing node labels..."
for node in $(kubectl get nodes -l shipstream.io/failover-group=playground -o name); do
  kubectl label "$node" topology.kubernetes.io/zone- shipstream.io/failover-group- shipstream.io/site- 2>/dev/null || true
done

info "Deleting namespace..."
kubectl delete namespace "$NAMESPACE" --ignore-not-found --timeout=60s

ok "Playground torn down. Cluster is still running — delete it yourself if needed."
echo ""
echo "  k3d:      k3d cluster delete bloodraven"
echo "  minikube: minikube delete"
echo "  kind:     kind delete cluster"
echo ""
