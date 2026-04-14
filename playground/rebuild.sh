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

# Prefer podman (rootless, no daemon) over docker
if command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
elif command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
else
  fail "Neither podman nor docker found"
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
  K3D_CLUSTER=$(k3d cluster list -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])" 2>/dev/null || echo "")
  if [[ -n "$K3D_CLUSTER" ]]; then
    info "Loading ${#IMAGES[@]} image(s) into k3d cluster '$K3D_CLUSTER'..."
    if [[ "$RUNTIME" == "podman" ]]; then
      podman_save
      k3d image import "$TARFILE" -c "$K3D_CLUSTER"
    else
      k3d image import "${IMAGES[@]}" -c "$K3D_CLUSTER"
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
ok "All done"
