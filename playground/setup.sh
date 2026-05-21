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
#   - docker or podman (docker preferred — k3d's podman support is
#     experimental, and the image-load path is faster on docker).
#     Set BLOODRAVEN_CONTAINER_RUNTIME=podman to force podman if both
#     are installed.
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
# Image loading (after building images, before running this script):
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
for cmd in kubectl helm; do
  command -v "$cmd" >/dev/null || fail "$cmd is required but not found"
done

# Refuse to run outside a known-local cluster context (AUDIT M7).
# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"
require_playground_context

HELM_INSTALL_CRDS=false
case "${BLOODRAVEN_SETUP_HELM_INSTALL_CRDS:-}" in
  1|true|TRUE|yes|YES) HELM_INSTALL_CRDS=true ;;
esac
if [[ "$HELM_INSTALL_CRDS" == "true" ]] && helm status bloodraven -n "$NAMESPACE" >/dev/null 2>&1; then
  fail "BLOODRAVEN_SETUP_HELM_INSTALL_CRDS=1 requires a fresh Helm release. Helm installs CRDs from charts/bloodraven/crds only on first install and will not upgrade or repair them on helm upgrade; unset BLOODRAVEN_SETUP_HELM_INSTALL_CRDS to apply CRDs explicitly before upgrading."
fi

# Prefer docker over podman. k3d's podman support is experimental and the
# tar-archive image-load path is slower than docker's native import.
# Override with BLOODRAVEN_CONTAINER_RUNTIME=podman if you actually want
# podman (e.g. on a rootless setup with no docker installed).
if [[ -n "${BLOODRAVEN_CONTAINER_RUNTIME:-}" ]]; then
  RUNTIME="$BLOODRAVEN_CONTAINER_RUNTIME"
  command -v "$RUNTIME" >/dev/null 2>&1 || fail "BLOODRAVEN_CONTAINER_RUNTIME=$RUNTIME but '$RUNTIME' is not on PATH"
elif command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
elif command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
else
  fail "Neither docker nor podman found"
fi
info "Using container runtime: $RUNTIME"

# Podman stores images with a localhost/ prefix that persists through k3d import.
# All image references must include this prefix when podman is the runtime.
IMG_PREFIX=""
if [[ "$RUNTIME" == "podman" ]]; then
  IMG_PREFIX="localhost/"
fi

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
kubectl label node "${WORKERS[0]}" shipstream.io/failover-group.playground=true --overwrite
kubectl label node "${WORKERS[0]}" shipstream.io/site.playground=iad --overwrite

kubectl label node "${WORKERS[1]}" topology.kubernetes.io/zone=zone-pdx --overwrite
kubectl label node "${WORKERS[1]}" shipstream.io/failover-group.playground=true --overwrite
kubectl label node "${WORKERS[1]}" shipstream.io/site.playground=pdx --overwrite
ok "Nodes labeled: ${WORKERS[0]}=iad, ${WORKERS[1]}=pdx"

# ── 3. Build images ──────────────────────────────────────────────────────
if [[ -n "${SKIP_IMAGE_BUILD:-}" ]]; then
  info "SKIP_IMAGE_BUILD is set — skipping image builds (CI mode: images pre-built or pre-loaded)"
else
  info "Building operator and sidecar images..."
  $RUNTIME build --target bloodraven -t bloodraven:playground "$PROJECT_ROOT"
  $RUNTIME build --target sidecar -t bloodraven-sidecar:playground "$PROJECT_ROOT"

  info "Building counter-app image..."
  $RUNTIME build -t bloodraven-counter:playground "$SCRIPT_DIR/counter-app"

  info "Building dashboard image..."
  $RUNTIME build -t bloodraven-dashboard:playground "$SCRIPT_DIR/dashboard"

  info "Building dns-webhook image..."
  $RUNTIME build -t bloodraven-dns-webhook:playground "$SCRIPT_DIR/dns-webhook"
  ok "All images built"
fi

# ── 4. Auto-detect cluster tool and load images ──────────────────────────
IMAGES=(bloodraven:playground bloodraven-sidecar:playground bloodraven-counter:playground bloodraven-dashboard:playground bloodraven-dns-webhook:playground)

podman_save() {
  TARFILE=$(mktemp "${TMPDIR:-/tmp}/bloodraven-images-XXXXXX.tar")
  trap 'rm -f "$TARFILE"' EXIT
  info "Saving ${#IMAGES[@]} image(s) to tar archive..."
  podman save --multi-image-archive -o "$TARFILE" "${IMAGES[@]}"
}

if command -v k3d >/dev/null 2>&1 && k3d cluster list 2>/dev/null | grep -q .; then
  # Prefer the cluster kubectl is currently pointing at (k3d contexts are
  # named "k3d-<cluster>"), so multiple coexisting k3d clusters don't end
  # up importing images into the wrong one. Falls back to the first
  # cluster listed if the context is something other than k3d.
  K3D_CTX=$(kubectl config current-context 2>/dev/null || echo "")
  if [[ "$K3D_CTX" == k3d-* ]]; then
    K3D_CLUSTER="${K3D_CTX#k3d-}"
  else
    K3D_CLUSTER=$(k3d cluster list --no-headers 2>/dev/null | awk 'NR==1 {print $1}' || echo "")
  fi
  if [[ -n "$K3D_CLUSTER" ]]; then
    info "Loading images into k3d cluster '$K3D_CLUSTER'..."
    # --mode direct forces k3d to replace images in each node's
    # containerd. The default ("auto") can no-op when an image with the
    # same tag already exists, leaving the cluster running stale code
    # with no warning.
    if [[ "$RUNTIME" == "podman" ]]; then
      podman_save
      k3d image import --mode direct "$TARFILE" -c "$K3D_CLUSTER"
    else
      k3d image import --mode direct "${IMAGES[@]}" -c "$K3D_CLUSTER"
    fi
    ok "Images loaded (k3d)"
  fi
elif command -v minikube >/dev/null 2>&1 && minikube status >/dev/null 2>&1; then
  info "Minikube detected — images should already be built in minikube's docker."
  info "If not, run: eval \$(minikube docker-env) and re-run this script."
elif command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q .; then
  KIND_CLUSTER=$(kind get clusters 2>/dev/null | head -1)
  info "Loading images into kind cluster '$KIND_CLUSTER'..."
  if [[ "$RUNTIME" == "podman" ]]; then
    podman_save
    kind load image-archive "$TARFILE" --name "$KIND_CLUSTER"
  else
    for img in "${IMAGES[@]}"; do
      kind load docker-image "$img" --name "$KIND_CLUSTER"
    done
  fi
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
if [[ "$HELM_INSTALL_CRDS" == "true" ]]; then
  info "Skipping manual Bloodraven CRD install; fresh Helm install will install chart CRDs from charts/bloodraven/crds"
else
  info "Installing Bloodraven CRDs..."
  kubectl apply -f "$PROJECT_ROOT/charts/bloodraven/crds/"
  ok "Bloodraven CRDs installed"
fi

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
sed "s|image: bloodraven-dns-webhook|image: ${IMG_PREFIX}bloodraven-dns-webhook|" \
  "$SCRIPT_DIR/manifests/external-dns.yaml" | kubectl apply -f -
kubectl apply -f "$SCRIPT_DIR/manifests/dashboard-rbac.yaml"

info "Deploying RustFS S3-compatible snapshot target for Dragonfly..."
kubectl apply -f "$SCRIPT_DIR/manifests/rustfs.yaml"
kubectl -n "$NAMESPACE" rollout status deployment/rustfs --timeout=180s
ok "RustFS ready (scenario 29 creates the dragonfly bucket on demand)"

# ── 8. Deploy the operator via Helm ──────────────────────────────────────
info "Deploying Bloodraven operator via Helm..."
helm upgrade --install bloodraven "$PROJECT_ROOT/charts/bloodraven" \
  --namespace "$NAMESPACE" \
  --set image.repository="${IMG_PREFIX}bloodraven" \
  --set image.tag=playground \
  --set image.pullPolicy=Never \
  --set auxiliary.service.enabled=true \
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
sed "s|sidecarImage: bloodraven-sidecar|sidecarImage: ${IMG_PREFIX}bloodraven-sidecar|" \
  "$SCRIPT_DIR/manifests/failovergroup.yaml" | kubectl apply -f -

info "Seeding DNSEndpoint CR (so external-dns chain works immediately)..."
kubectl apply -f "$SCRIPT_DIR/manifests/dnsendpoint-seed.yaml"

# ── 10. Deploy counter app and dashboard ─────────────────────────────────
info "Deploying counter app and dashboard..."
sed "s|image: bloodraven-counter|image: ${IMG_PREFIX}bloodraven-counter|" \
  "$SCRIPT_DIR/manifests/counter-app.yaml" | kubectl apply -f -
sed "s|image: bloodraven-dashboard|image: ${IMG_PREFIX}bloodraven-dashboard|" \
  "$SCRIPT_DIR/manifests/dashboard.yaml" | kubectl apply -f -

# ── 11. Wait for MySQL pods and create replication user ─────────────────
info "Waiting for MySQL PVCs to bind..."
PVCS_BOUND=0
for i in $(seq 1 180); do
  # Clear taints each second: local-path helper pods do not tolerate the
  # operator's readonly taint, and one eviction can stall provisioning.
  for node in $(kubectl get nodes -o name 2>/dev/null); do
    kubectl taint "$node" shipstream.io/db-readonly-playground- 2>/dev/null || true
    kubectl taint "$node" shipstream.io/db-readonly- 2>/dev/null || true
  done
  PVCS_BOUND=$(kubectl -n "$NAMESPACE" get pvc -l app.kubernetes.io/name=mysql \
    -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null \
    | grep -c '^Bound$' || true)
  if [[ "$PVCS_BOUND" -ge 2 ]]; then
    ok "Both MySQL PVCs are bound"
    break
  fi
  sleep 1
done
if [[ "$PVCS_BOUND" -lt 2 ]]; then
  warn "Timed out waiting for MySQL PVCs to bind"
  kubectl -n "$NAMESPACE" get pvc -o wide 2>/dev/null || true
  kubectl -n "$NAMESPACE" describe pvc 2>/dev/null || true
  exit 1
fi

info "Waiting for MySQL pods to become ready (this may take a few minutes)..."
READY=0
for i in $(seq 1 36); do
  # Clear taints each iteration — operator may apply them before pods are ready
  for node in $(kubectl get nodes -o name 2>/dev/null); do
    kubectl taint "$node" shipstream.io/db-readonly-playground- 2>/dev/null || true
    kubectl taint "$node" shipstream.io/db-readonly- 2>/dev/null || true
  done
  READY=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=mysql \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[*].ready}{"\n"}{end}' 2>/dev/null \
    | grep -c "true true" || true)
  if [[ "$READY" -ge 2 ]]; then
    ok "Both MySQL pods are ready"
    break
  fi
  sleep 5
done
if [[ "$READY" -lt 2 ]]; then
  warn "Timed out waiting for MySQL pods"
  kubectl -n "$NAMESPACE" get pods,pvc -o wide 2>/dev/null || true
  kubectl -n "$NAMESPACE" describe pods -l app.kubernetes.io/name=mysql 2>/dev/null || true
  exit 1
fi

info "Creating replication user on both MySQL sites..."
REPL_USER=$(kubectl -n "$NAMESPACE" get secret mysql-credentials -o jsonpath='{.data.MYSQL_REPLICATION_USER}' | base64 -d)
REPL_PASS=$(kubectl -n "$NAMESPACE" get secret mysql-credentials -o jsonpath='{.data.MYSQL_REPLICATION_PASSWORD}' | base64 -d)
ROOT_PASS=$(kubectl -n "$NAMESPACE" get secret mysql-credentials -o jsonpath='{.data.MYSQL_ROOT_PASSWORD}' | base64 -d)
for site in iad pdx; do
  READ_ONLY=$(kubectl -n "$NAMESPACE" exec "deploy/mysql-playground-$site" -c mysql -- \
    mysql "-uroot" "-p${ROOT_PASS}" -Nse "SELECT @@global.read_only" 2>/dev/null || echo 0)
  SUPER_READ_ONLY=$(kubectl -n "$NAMESPACE" exec "deploy/mysql-playground-$site" -c mysql -- \
    mysql "-uroot" "-p${ROOT_PASS}" -Nse "SELECT @@global.super_read_only" 2>/dev/null || echo 0)
  if kubectl -n "$NAMESPACE" exec "deploy/mysql-playground-$site" -c mysql -- \
    mysql "-uroot" "-p${ROOT_PASS}" -e \
    "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; \
     CREATE USER IF NOT EXISTS '${REPL_USER}'@'%' IDENTIFIED BY '${REPL_PASS}'; \
     GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO '${REPL_USER}'@'%'; \
     FLUSH PRIVILEGES;" 2>/dev/null; then
    kubectl -n "$NAMESPACE" exec "deploy/mysql-playground-$site" -c mysql -- \
      mysql "-uroot" "-p${ROOT_PASS}" -e "SET GLOBAL read_only=${READ_ONLY}; SET GLOBAL super_read_only=${SUPER_READ_ONLY};" 2>/dev/null || true
    ok "Replication user created on $site"
  else
    warn "Failed to create replication user on $site"
    exit 1
  fi
done

# ── 12. Wait for remaining pods and print access info ───────────────────
info "Waiting for remaining pods..."
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/external-dns --timeout=120s 2>/dev/null || true
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/dashboard --timeout=120s 2>/dev/null || true

# ── 13. Wait for Dragonfly StatefulSets (when enabled) ──────────────────
DF_ENABLED=$(kubectl -n "$NAMESPACE" get mysqlfailovergroup playground \
  -o jsonpath='{.spec.dragonfly.enabled}' 2>/dev/null || echo "")
if [[ "$DF_ENABLED" == "true" ]]; then
  info "Waiting for Dragonfly StatefulSets (one per site)..."
  for site in iad pdx; do
    if ! kubectl -n "$NAMESPACE" rollout status statefulset/playground-dragonfly-$site --timeout=120s 2>/dev/null; then
      warn "Dragonfly StatefulSet for $site did not become ready in 120s — check 'kubectl describe statefulset playground-dragonfly-$site'"
    fi
  done

  # Surface the operator's view of which site holds the master. The
  # StatefulSets reach Ready before the operator finishes its first
  # promotion tick, so poll briefly instead of warning on the first miss.
  DF_ACTIVE=""
  for _ in $(seq 1 30); do
    DF_ACTIVE=$(kubectl -n "$NAMESPACE" get mysqlfailovergroup playground \
      -o jsonpath='{.status.dragonfly.activeSite}' 2>/dev/null || echo "")
    [[ -n "$DF_ACTIVE" ]] && break
    sleep 2
  done
  if [[ -n "$DF_ACTIVE" ]]; then
    ok "Dragonfly active site: $DF_ACTIVE (Service: playground-dragonfly:6379)"
  else
    warn "Dragonfly status.activeSite not set after 60s — check operator logs and 'kubectl describe mysqlfailovergroup playground'"
  fi
fi

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
if [[ "$DF_ENABLED" == "true" ]]; then
  echo "  Dragonfly:"
  echo "    kubectl -n $NAMESPACE port-forward --address 0.0.0.0 svc/playground-dragonfly 6379:6379"
  echo "    then: redis-cli -h localhost INFO replication"
  echo "    or per-site: kubectl -n $NAMESPACE port-forward svc/playground-dragonfly-iad 6380:6379"
  echo ""
fi
echo "  Chaos monkey:"
echo "    ./playground/chaos.sh kill-site iad"
echo "    ./playground/chaos.sh kill-site pdx"
echo "    ./playground/chaos.sh cordon iad"
echo "    ./playground/chaos.sh network-partition iad"
echo "    ./playground/chaos.sh kill-dragonfly iad"
echo "    ./playground/chaos.sh dragonfly-status"
echo "    ./playground/chaos.sh recover"
echo ""
echo "  Teardown:"
echo "    ./playground/teardown.sh"
echo ""
