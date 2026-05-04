#!/usr/bin/env bash
# Rebuild playground images, load into cluster, and restart deployments.
# Usage: ./playground/rebuild.sh [component ...]
#
# Components: operator, sidecar, counter, dashboard, dns-webhook
# No arguments = rebuild all. Examples:
#   ./playground/rebuild.sh                  # everything
#   ./playground/rebuild.sh dashboard        # just the dashboard
#   ./playground/rebuild.sh counter dashboard # counter + dashboard
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="bloodraven-playground"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }
fail()  { echo -e "\033[1;31mERR\033[0m $*" >&2; exit 1; }

local_image_ids() {
  local img="$1"
  $RUNTIME image inspect "$img" --format '{{.Id}}' 2>/dev/null | sed 's/^sha256://'

  # Docker BuildKit can leave a local tag pointing at an OCI image
  # index. Kubernetes reports the running container's config digest in
  # status.containerStatuses[*].imageID, so comparing only .Id creates
  # false "stale image" failures. The docker-save manifest exposes the
  # config digest for the same local tag.
  if [[ "$RUNTIME" == "docker" ]]; then
    local tarfile
    tarfile=$(mktemp "${TMPDIR:-/tmp}/bloodraven-image-XXXXXX.tar")
    if docker image save "$img" -o "$tarfile" 2>/dev/null; then
      tar -xOf "$tarfile" manifest.json 2>/dev/null | python3 -c '
import json, sys
try:
    cfg = json.load(sys.stdin)[0].get("Config", "")
except Exception:
    cfg = ""
if cfg:
    print(cfg.rsplit("/", 1)[-1].removesuffix(".json"))
'
    fi
    rm -f "$tarfile"
  fi
}

# Refuse to run outside a known-local cluster context (AUDIT M7).
# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"
require_playground_context

# Prefer docker over podman. k3d's podman support is experimental and the
# tar-archive image-load path is slower than docker's native import.
# Override with BLOODRAVEN_CONTAINER_RUNTIME=podman if you actually want
# podman.
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

# Parse requested components (default: all)
REQUESTED=("$@")
if [[ ${#REQUESTED[@]} -eq 0 ]]; then
  REQUESTED=(operator sidecar counter dashboard dns-webhook)
fi

want() { for r in "${REQUESTED[@]}"; do [[ "$r" == "$1" ]] && return 0; done; return 1; }

IMAGES=()
DEPLOYMENTS=()

# ── Build ───────────────────────────────────────────────────────────────
if want operator; then
  info "Building operator image..."
  $RUNTIME build --target bloodraven -t bloodraven:playground "$PROJECT_ROOT"
  IMAGES+=(bloodraven:playground)
  DEPLOYMENTS+=(bloodraven)
fi

if want sidecar; then
  info "Building sidecar image..."
  $RUNTIME build --target sidecar -t bloodraven-sidecar:playground "$PROJECT_ROOT"
  IMAGES+=(bloodraven-sidecar:playground)
  # Sidecar runs inside MySQL pods — need to restart those
  DEPLOYMENTS+=(mysql-playground-iad mysql-playground-pdx)
fi

if want counter; then
  info "Building counter-app image..."
  $RUNTIME build -t bloodraven-counter:playground "$SCRIPT_DIR/counter-app"
  IMAGES+=(bloodraven-counter:playground)
  DEPLOYMENTS+=(counter-app)
fi

if want dashboard; then
  info "Building dashboard image..."
  $RUNTIME build -t bloodraven-dashboard:playground "$SCRIPT_DIR/dashboard"
  IMAGES+=(bloodraven-dashboard:playground)
  DEPLOYMENTS+=(dashboard)
fi

if want dns-webhook; then
  info "Building dns-webhook image..."
  $RUNTIME build -t bloodraven-dns-webhook:playground "$SCRIPT_DIR/dns-webhook"
  IMAGES+=(bloodraven-dns-webhook:playground)
  DEPLOYMENTS+=(external-dns)
fi

# ── Load into cluster ──────────────────────────────────────────────────
# With podman, images live in podman's store (not a docker daemon), so we
# export to a tar archive and let the cluster tool import from that.
podman_save() {
  TARFILE=$(mktemp "${TMPDIR:-/tmp}/bloodraven-images-XXXXXX.tar")
  trap 'rm -f "$TARFILE"' EXIT
  info "Saving ${#IMAGES[@]} image(s) to tar archive..."
  podman save --multi-image-archive -o "$TARFILE" "${IMAGES[@]}"
}

if command -v k3d >/dev/null 2>&1 && k3d cluster list 2>/dev/null | grep -q .; then
  # Prefer the cluster kubectl is currently pointing at. Without this,
  # multiple coexisting k3d clusters cause images to be imported into
  # whichever cluster JSON parsing returned first — usually not the one
  # being targeted.
  K3D_CTX=$(kubectl config current-context 2>/dev/null || echo "")
  if [[ "$K3D_CTX" == k3d-* ]]; then
    K3D_CLUSTER="${K3D_CTX#k3d-}"
  else
    K3D_CLUSTER=$(k3d cluster list -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])" 2>/dev/null || echo "")
  fi
  if [[ -n "$K3D_CLUSTER" ]]; then
    info "Loading ${#IMAGES[@]} image(s) into k3d cluster '$K3D_CLUSTER'..."
    # tools-node import loads the freshly built image tarball into each
    # node's containerd. Avoid k3d's auto mode here: it can short-circuit
    # when an image with the same tag already exists, leaving the cluster
    # running stale code while reporting success. Direct mode also
    # replaces node images, but it can hang on Docker socket stream
    # failures in this playground environment.
    if [[ "$RUNTIME" == "podman" ]]; then
      podman_save
      k3d image import --mode tools-node "$TARFILE" -c "$K3D_CLUSTER"
    else
      k3d image import --mode tools-node "${IMAGES[@]}" -c "$K3D_CLUSTER"
    fi
    ok "Images loaded (k3d)"
  fi
elif command -v minikube >/dev/null 2>&1 && minikube status >/dev/null 2>&1; then
  info "Minikube detected — images should already be in minikube's docker."
elif command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q .; then
  KIND_CLUSTER=$(kind get clusters 2>/dev/null | head -1)
  info "Loading ${#IMAGES[@]} image(s) into kind cluster '$KIND_CLUSTER'..."
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
  fail "Could not auto-detect cluster tool (k3d/kind/minikube)."
fi

# ── Restart deployments ────────────────────────────────────────────────
# De-duplicate deployment list
UNIQUE_DEPS=($(printf '%s\n' "${DEPLOYMENTS[@]}" | sort -u))
info "Restarting: ${UNIQUE_DEPS[*]}"
for dep in "${UNIQUE_DEPS[@]}"; do
  kubectl -n "$NAMESPACE" rollout restart "deployment/$dep"
done

# Wait for rollouts
for dep in "${UNIQUE_DEPS[@]}"; do
  kubectl -n "$NAMESPACE" rollout status "deployment/$dep" --timeout=120s
done

# Verify each pod is actually running the freshly-built image. Without
# this, a cached/stale image in the cluster's containerd produces a
# successful rollout against unchanged code — see comment on `k3d image
# import --mode direct` above. The expected ID format is "sha256:<hash>"
# in containerStatuses, while $RUNTIME image inspect returns the bare
# hash; we compare the bare-hash forms.
info "Verifying rolled pods are running the freshly-built image..."
verify_failures=()
for img in "${IMAGES[@]}"; do
  expected_ids=$(local_image_ids "$img" | sed '/^$/d' | sort -u)
  expected=$(echo "$expected_ids" | head -1)
  if [[ -z "$expected_ids" ]]; then
    warn "could not read local image ID for $img — skipping verification"
    continue
  fi
  # Map image → label selector. Mirrors the deployment names selected
  # above; "bloodraven" needs the app.kubernetes.io/name selector since
  # the operator pod also has other labels.
  case "$img" in
    bloodraven:playground)               sel="app.kubernetes.io/name=bloodraven" ;;
    bloodraven-sidecar:playground)       sel="app.kubernetes.io/name=mysql" ;;
    bloodraven-counter:playground)       sel="app=counter-app" ;;
    bloodraven-dashboard:playground)     sel="app=dashboard" ;;
    bloodraven-dns-webhook:playground)   sel="app=external-dns" ;;
    *)                                   sel="" ;;
  esac
  [[ -z "$sel" ]] && continue
  pod_ids=$(kubectl -n "$NAMESPACE" get pods -l "$sel" \
    -o jsonpath='{range .items[*].status.containerStatuses[*]}{.image}={.imageID}{"\n"}{end}' 2>/dev/null \
    | grep -F "$img=" \
    | sed 's|^.*=sha256:||' \
    | sort -u)
  if [[ -z "$pod_ids" ]]; then
    warn "no running pods found for $img (selector: $sel)"
    continue
  fi
  match=0
  while read -r got; do
    while read -r expected_id; do
      if [[ "$got" == "$expected_id"* || "$expected_id" == "$got"* ]]; then
        match=1
        break
      fi
    done <<<"$expected_ids"
    [[ $match -eq 1 ]] && break
  done <<<"$pod_ids"
  if [[ $match -eq 1 ]]; then
    ok "$img → ${expected:0:12}"
  else
    verify_failures+=("$img: expected ${expected:0:12}, got $(echo "$pod_ids" | head -1 | cut -c1-12)")
  fi
done

if [[ ${#verify_failures[@]} -gt 0 ]]; then
  echo
  for f in "${verify_failures[@]}"; do
    echo -e "\033[1;31mERR\033[0m image mismatch: $f" >&2
  done
  cat >&2 <<'EOF'

The cluster's containerd is running a stale image. This usually means
"k3d image import" silently no-op'd (despite the script using --mode
direct, which should prevent it) or that a deployment selector is wrong.

To force the new image into k3d:
  kubectl -n bloodraven-playground delete pod -l <selector>   # forces re-pull from local cache
  ./playground/rebuild.sh <component>

If that doesn't help, fully evict the stale image:
  for n in $(k3d node list -o json | python3 -c 'import sys,json; [print(x["name"]) for x in json.load(sys.stdin) if x.get("role")!="loadbalancer"]'); do
    docker exec "$n" crictl rmi <image> 2>/dev/null || true
  done
  ./playground/rebuild.sh <component>
EOF
  exit 1
fi

ok "All done"
