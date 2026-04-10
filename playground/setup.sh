#!/usr/bin/env bash
# Bloodraven Playground — deploys external-dns (dry-run), the Bloodraven
# operator, a MysqlFailoverGroup, a counter app, and a real-time dashboard
# into an existing Kubernetes cluster.
#
# Usage: ./playground/setup.sh
#
# Prerequisites:
#   - kubectl pointing at a cluster with at least 2 nodes
#   - helm
#   - docker (images must be loadable into the cluster — see "Image loading" below)
#
# Cluster setup (do once, before running this script):
#
#   k3d:
#     k3d cluster create bloodraven --agents 2
#
#   minikube:
#     minikube start --nodes=2 --cpus=2 --memory=2048 --driver=docker
#
#   kind:
#     cat <<EOF | kind create cluster --config=-
#     kind: Cluster
#     apiVersion: kind.x-k8s.io/v1alpha4
#     nodes:
#       - role: control-plane
#       - role: worker
#       - role: worker
#     EOF
#
# Image loading (after docker build, before running this script):
#   The script builds images locally. You must load them into your cluster:
#
#   k3d:   k3d image import bloodraven:playground bloodraven-sidecar:playground \
#            bloodraven-counter:playground bloodraven-dashboard:playground -c bloodraven
#   minikube: eval $(minikube docker-env) before running this script (images build directly)
#   kind:  kind load docker-image bloodraven:playground bloodraven-sidecar:playground \
#            bloodraven-counter:playground bloodraven-dashboard:playground
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="bloodraven-playground"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }
fail()  { echo -e "\033[1;31mERR\033[0m $*" >&2; exit 1; }

# ── Pre-flight checks ──────────────────────────────────────────────────────
for cmd in kubectl docker helm; do
  command -v "$cmd" >/dev/null || fail "$cmd is required but not found"
done

kubectl cluster-info >/dev/null 2>&1 || fail "No Kubernetes cluster reachable. Set up a cluster first (see script header)."

# ── 1. Verify at least 2 nodes ───────────────────────────────────────────
NODES=($(kubectl get nodes --no-headers -o custom-columns=NAME:.metadata.name))
if [[ ${#NODES[@]} -lt 2 ]]; then
  fail "Need at least 2 nodes, got ${#NODES[@]}. See script header for cluster setup."
fi
ok "Cluster reachable with ${#NODES[@]} nodes"

# ── 2. Label nodes to simulate sites ────────────────────────────────────
info "Labeling nodes as site zones..."
# Pick first two worker nodes (skip control-plane-only if possible)
WORKERS=()
for n in "${NODES[@]}"; do
  role=$(kubectl get node "$n" -o jsonpath='{.metadata.labels.node-role\.kubernetes\.io/control-plane}' 2>/dev/null || true)
  if [[ -z "$role" ]]; then
    WORKERS+=("$n")
  fi
done
# Fall back to all nodes if no pure workers
if [[ ${#WORKERS[@]} -lt 2 ]]; then
  WORKERS=("${NODES[@]}")
fi

kubectl label node "${WORKERS[0]}" topology.kubernetes.io/zone=zone-iad --overwrite
kubectl label node "${WORKERS[0]}" shipstream.io/failover-group=playground --overwrite
kubectl label node "${WORKERS[0]}" shipstream.io/site=iad --overwrite

kubectl label node "${WORKERS[1]}" topology.kubernetes.io/zone=zone-pdx --overwrite
kubectl label node "${WORKERS[1]}" shipstream.io/failover-group=playground --overwrite
kubectl label node "${WORKERS[1]}" shipstream.io/site=pdx --overwrite
ok "Nodes labeled: ${WORKERS[0]}=iad, ${WORKERS[1]}=pdx"

# ── 3. Build images ──────────────────────────────────────────────────────
info "Building operator and sidecar images..."
docker build --target bloodraven -t bloodraven:playground "$PROJECT_ROOT"
docker build --target sidecar -t bloodraven-sidecar:playground "$PROJECT_ROOT"

info "Building counter-app image..."
docker build -t bloodraven-counter:playground "$SCRIPT_DIR/counter-app"

info "Building dashboard image..."
docker build -t bloodraven-dashboard:playground "$SCRIPT_DIR/dashboard"

info "Building dns-webhook image..."
docker build -t bloodraven-dns-webhook:playground "$SCRIPT_DIR/dns-webhook"
ok "All images built"

# ── 4. Auto-detect cluster tool and load images ──────────────────────────
IMAGES=(bloodraven:playground bloodraven-sidecar:playground bloodraven-counter:playground bloodraven-dashboard:playground bloodraven-dns-webhook:playground)

if command -v k3d >/dev/null 2>&1 && k3d cluster list 2>/dev/null | grep -q .; then
  K3D_CLUSTER=$(k3d cluster list -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])" 2>/dev/null || echo "")
  if [[ -n "$K3D_CLUSTER" ]]; then
    info "Loading images into k3d cluster '$K3D_CLUSTER'..."
    k3d image import "${IMAGES[@]}" -c "$K3D_CLUSTER"
    ok "Images loaded (k3d)"
  fi
elif command -v minikube >/dev/null 2>&1 && minikube status >/dev/null 2>&1; then
  info "Minikube detected — images should already be built in minikube's docker."
  info "If not, run: eval \$(minikube docker-env) and re-run this script."
elif command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q .; then
  KIND_CLUSTER=$(kind get clusters 2>/dev/null | head -1)
  info "Loading images into kind cluster '$KIND_CLUSTER'..."
  for img in "${IMAGES[@]}"; do
    kind load docker-image "$img" --name "$KIND_CLUSTER"
  done
  ok "Images loaded (kind)"
else
  warn "Could not auto-detect cluster tool. Make sure images are available in your cluster."
  warn "See script header for image loading instructions."
fi

# ── 5. Install DNSEndpoint CRD (required by external-dns and Bloodraven) ─
info "Installing DNSEndpoint CRD..."
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/external-dns/v0.15.1/docs/contributing/crd-source/crd-manifest.yaml 2>/dev/null || \
  kubectl apply -f - <<'EOF'
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: dnsendpoints.externaldns.k8s.io
spec:
  group: externaldns.k8s.io
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                endpoints:
                  type: array
                  items:
                    type: object
                    properties:
                      dnsName:
                        type: string
                      recordType:
                        type: string
                      targets:
                        type: array
                        items:
                          type: string
                      recordTTL:
                        type: integer
            status:
              type: object
              properties:
                observedGeneration:
                  type: integer
      subresources:
        status: {}
  scope: Namespaced
  names:
    plural: dnsendpoints
    singular: dnsendpoint
    kind: DNSEndpoint
EOF
ok "DNSEndpoint CRD installed"

# ── 6. Install Bloodraven CRDs ───────────────────────────────────────────
info "Installing Bloodraven CRDs..."
kubectl apply -f "$PROJECT_ROOT/charts/bloodraven/crds/"
ok "Bloodraven CRDs installed"

# ── 7. Create namespace and deploy manifests ─────────────────────────────
info "Creating namespace and deploying manifests..."
kubectl apply -f "$SCRIPT_DIR/manifests/namespace.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/mysql-secret.yaml"

# Auto-detect the cluster's default StorageClass provisioner and patch the
# playground StorageClass to use it, so it works on k3d, kind, and minikube.
DEFAULT_PROVISIONER=$(kubectl get storageclass -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].provisioner}' 2>/dev/null | awk '{print $1}')
if [[ -n "$DEFAULT_PROVISIONER" ]]; then
  info "Detected default StorageClass provisioner: $DEFAULT_PROVISIONER"
  sed "s|provisioner: .*|provisioner: $DEFAULT_PROVISIONER|" "$SCRIPT_DIR/manifests/storageclass.yaml" | kubectl apply -f -
else
  warn "No default StorageClass found — applying storageclass.yaml as-is (rancher.io/local-path)"
  kubectl apply -f "$SCRIPT_DIR/manifests/storageclass.yaml"
fi
kubectl apply -f "$SCRIPT_DIR/manifests/external-dns.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/dashboard-rbac.yaml"

# ── 8. Deploy the operator via Helm ──────────────────────────────────────
info "Deploying Bloodraven operator via Helm..."
helm upgrade --install bloodraven "$PROJECT_ROOT/charts/bloodraven" \
  --namespace "$NAMESPACE" \
  --set image.repository=bloodraven \
  --set image.tag=playground \
  --set image.pullPolicy=Never \
  --set installCRDs=false \
  --set 'nodeSelector=null' \
  --set 'tolerations[0].key=node.kubernetes.io/disk-pressure' \
  --set 'tolerations[0].operator=Exists' \
  --set 'tolerations[0].effect=NoSchedule' \
  --set leaderElection.enabled=false \
  --timeout=180s
# Don't use --wait; the operator may take a moment to pass readiness after
# leader election / first reconcile. We wait for it explicitly below.
info "Waiting for operator deployment to roll out..."
kubectl -n "$NAMESPACE" rollout status deployment/bloodraven --timeout=180s
ok "Operator deployed"

# ── 9. Deploy the MysqlFailoverGroup CR ──────────────────────────────────
info "Creating MysqlFailoverGroup CR..."
kubectl apply -f "$SCRIPT_DIR/manifests/failovergroup.yaml"

# ── 10. Deploy counter app and dashboard ─────────────────────────────────
info "Deploying counter app and dashboard..."
kubectl apply -f "$SCRIPT_DIR/manifests/counter-app.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/dashboard.yaml"

# ── 11. Wait and print access info ──────────────────────────────────────
info "Waiting for pods to become ready (this may take a minute)..."
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/external-dns --timeout=120s 2>/dev/null || true
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/dashboard --timeout=120s 2>/dev/null || true

echo ""
echo "=============================================="
echo "  Bloodraven Playground is ready!"
echo "=============================================="
echo ""
echo "  Access the services with kubectl port-forward:"
echo "  (--address 0.0.0.0 makes them reachable over Tailscale / remote access)"
echo ""
echo "    Dashboard:    kubectl -n $NAMESPACE port-forward --address 0.0.0.0 svc/dashboard 8091:8091"
echo "                  then open http://localhost:8091  (or http://<tailscale-ip>:8091)"
echo ""
echo "    Counter App:  kubectl -n $NAMESPACE port-forward --address 0.0.0.0 svc/counter-app 8090:8090"
echo "                  then open http://localhost:8090  (or http://<tailscale-ip>:8090)"
echo ""
echo "  Useful commands:"
echo "    kubectl -n $NAMESPACE get mysqlfailovergroups"
echo "    kubectl -n $NAMESPACE get pods"
echo "    kubectl -n $NAMESPACE get dnsendpoints"
echo "    kubectl -n $NAMESPACE logs -l app.kubernetes.io/name=bloodraven -f"
echo ""
echo "  Chaos monkey:"
echo "    ./playground/chaos.sh kill-site iad"
echo "    ./playground/chaos.sh kill-site pdx"
echo "    ./playground/chaos.sh cordon iad"
echo "    ./playground/chaos.sh network-partition iad"
echo "    ./playground/chaos.sh recover"
echo ""
echo "  Teardown:"
echo "    ./playground/teardown.sh"
echo ""
