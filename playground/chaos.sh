#!/usr/bin/env bash
# Bloodraven Playground — Chaos Monkey
#
# Usage:
#   ./playground/chaos.sh kill-site <iad|pdx>       Kill MySQL pod at a site
#   ./playground/chaos.sh kill-operator              Kill the operator pod
#   ./playground/chaos.sh kill-counter               Kill the counter app pod
#   ./playground/chaos.sh cordon <iad|pdx>           Cordon the node for a site
#   ./playground/chaos.sh uncordon                   Uncordon all nodes
#   ./playground/chaos.sh network-partition <iad|pdx> Simulate network partition (drop MySQL traffic)
#   ./playground/chaos.sh recover                    Undo all chaos (uncordon, restore network)
#   ./playground/chaos.sh status                     Show current state
#   ./playground/chaos.sh watch                      Continuous watch
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="bloodraven-playground"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }

# Refuse to run outside a known-local cluster context (AUDIT M7).
# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"
require_playground_context

usage() {
  echo "Usage: $0 <command> [args]"
  echo ""
  echo "Commands:"
  echo "  kill-site <iad|pdx>          Delete MySQL pod at the given site"
  echo "  kill-operator                Delete the Bloodraven operator pod"
  echo "  kill-counter                 Delete the counter app pod"
  echo "  cordon <iad|pdx>             Cordon the node hosting the given site"
  echo "  uncordon                     Uncordon all playground nodes"
  echo "  network-partition <iad|pdx>  Block MySQL traffic on a site's node via exec into a debug pod"
  echo "  recover                      Undo all chaos actions"
  echo "  status                       Show cluster and failover group status"
  echo "  watch                        Watch pods and failover group continuously"
  exit 1
}

site_zone() {
  case "$1" in
    iad) echo "zone-iad" ;;
    pdx) echo "zone-pdx" ;;
    *) echo "Unknown site: $1" >&2; exit 1 ;;
  esac
}

site_node() {
  local zone
  zone=$(site_zone "$1")
  kubectl get nodes -l "topology.kubernetes.io/zone=$zone" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

cmd_kill_site() {
  local site="${1:?Usage: kill-site <iad|pdx>}"
  info "Killing MySQL pod at site '$site'..."
  kubectl -n "$NAMESPACE" delete pod -l "shipstream.io/site=$site" --grace-period=0 --force 2>/dev/null || \
    kubectl -n "$NAMESPACE" delete pod -l "shipstream.io/site=$site"
  ok "MySQL pod at $site killed"
}

cmd_kill_operator() {
  info "Killing Bloodraven operator pod..."
  kubectl -n "$NAMESPACE" delete pod -l "app.kubernetes.io/name=bloodraven" --grace-period=0 --force 2>/dev/null || \
    kubectl -n "$NAMESPACE" delete pod -l "app.kubernetes.io/name=bloodraven"
  ok "Operator pod killed (will be recreated by deployment)"
}

cmd_kill_counter() {
  info "Killing counter app pod..."
  kubectl -n "$NAMESPACE" delete pod -l "app=counter-app" --grace-period=0 --force 2>/dev/null || \
    kubectl -n "$NAMESPACE" delete pod -l "app=counter-app"
  ok "Counter app pod killed"
}

cmd_cordon() {
  local site="${1:?Usage: cordon <iad|pdx>}"
  local node
  node=$(site_node "$site")
  if [[ -z "$node" ]]; then
    warn "No node found for site $site"
    return 1
  fi
  info "Cordoning node $node (site: $site)..."
  kubectl cordon "$node"
  ok "Node $node cordoned"
}

cmd_uncordon() {
  info "Uncordoning all playground nodes..."
  for site in iad pdx; do
    local node
    node=$(site_node "$site" 2>/dev/null || true)
    if [[ -n "$node" ]]; then
      kubectl uncordon "$node" 2>/dev/null || true
    fi
  done
  ok "All playground nodes uncordoned"
}

cmd_network_partition() {
  local site="${1:?Usage: network-partition <iad|pdx>}"
  info "Simulating network partition on site '$site' (NetworkPolicy deny-all)..."

  # Use a Kubernetes NetworkPolicy to block all ingress and egress traffic
  # to the MySQL pod. This works at the pod network level (unlike host-netns
  # iptables which is bypassed by kube-proxy DNAT).
  kubectl -n "$NAMESPACE" apply -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: chaos-partition-${site}
  namespace: ${NAMESPACE}
  labels:
    app: chaos-partition
spec:
  podSelector:
    matchLabels:
      shipstream.io/site: ${site}
      app.kubernetes.io/name: mysql
  policyTypes:
  - Ingress
  - Egress
  ingress: []
  egress: []
EOF

  ok "Network partition active on site $site (NetworkPolicy deny-all)"
  echo "  To remove: ./playground/chaos.sh recover"
}

cmd_recover() {
  info "Recovering from all chaos..."

  # Uncordon all playground nodes
  cmd_uncordon 2>/dev/null || true

  # Remove chaos NetworkPolicies
  kubectl -n "$NAMESPACE" delete networkpolicy -l app=chaos-partition 2>/dev/null || true

  # Clean up any leftover chaos pods (from older iptables-based partitions)
  for site in iad pdx; do
    kubectl -n "$NAMESPACE" delete pod "chaos-netblock-${site}" --ignore-not-found --grace-period=0 --force 2>/dev/null || true
    kubectl -n "$NAMESPACE" delete pod "chaos-netcleanup-${site}" --ignore-not-found --grace-period=0 --force 2>/dev/null || true
  done

  ok "All chaos recovered (nodes uncordoned, network partitions removed)"
}

cmd_status() {
  echo ""
  info "Nodes:"
  kubectl get nodes -o wide -L topology.kubernetes.io/zone,shipstream.io/site
  echo ""
  info "MysqlFailoverGroup:"
  kubectl -n "$NAMESPACE" get mysqlfailovergroups -o wide 2>/dev/null || echo "  (none found)"
  echo ""
  info "Pods:"
  kubectl -n "$NAMESPACE" get pods -o wide
  echo ""
  info "DNSEndpoints:"
  kubectl -n "$NAMESPACE" get dnsendpoints -o yaml 2>/dev/null | grep -E "dnsName|targets|recordType" || echo "  (none found)"
  echo ""
  info "Node taints:"
  for node in $(kubectl get nodes -l 'shipstream.io/site' -o name 2>/dev/null); do
    taints=$(kubectl get "$node" -o jsonpath='{.spec.taints[*].key}' 2>/dev/null)
    echo "  $node: ${taints:-<none>}"
  done
  echo ""
}

cmd_watch() {
  info "Watching pods and failover group (Ctrl+C to stop)..."
  watch -n2 "kubectl -n $NAMESPACE get mysqlfailovergroups,pods,dnsendpoints -o wide 2>/dev/null"
}

# ── Main dispatch ─────────────────────────────────────────────────────────
case "${1:-}" in
  kill-site)          cmd_kill_site "${2:-}" ;;
  kill-operator)      cmd_kill_operator ;;
  kill-counter)       cmd_kill_counter ;;
  cordon)             cmd_cordon "${2:-}" ;;
  uncordon)           cmd_uncordon ;;
  network-partition)  cmd_network_partition "${2:-}" ;;
  recover)            cmd_recover ;;
  status)             cmd_status ;;
  watch)              cmd_watch ;;
  *)                  usage ;;
esac
