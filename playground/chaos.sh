#!/usr/bin/env bash
# Bloodraven Playground — Chaos Monkey
#
# Usage:
#   ./playground/chaos.sh kill-site <iad|pdx>       Kill MySQL+Dragonfly pods at a site
#   ./playground/chaos.sh kill-operator              Kill the operator pod
#   ./playground/chaos.sh kill-counter               Kill the counter app pod
#   ./playground/chaos.sh kill-dragonfly <iad|pdx>   Kill the Dragonfly pod at a site (StatefulSet recreates)
#   ./playground/chaos.sh dragonfly-status           Show Dragonfly status, pod roles, active endpoints
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
  echo "  kill-site <iad|pdx>          Delete MySQL+Dragonfly pods at the given site"
  echo "  kill-operator                Delete the Bloodraven operator pod"
  echo "  kill-counter                 Delete the counter app pod"
  echo "  kill-dragonfly <iad|pdx>     Delete the Dragonfly pod at the given site"
  echo "  dragonfly-status             Show Dragonfly status, pod roles, and active endpoints"
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
  for node in $(kubectl get nodes -l 'shipstream.io/site.playground' -o name 2>/dev/null); do
    kubectl uncordon "$node" 2>/dev/null || true
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

cmd_kill_dragonfly() {
  local site="${1:?Usage: kill-dragonfly <iad|pdx>}"
  info "Killing Dragonfly pod at site '$site' (StatefulSet will recreate)..."
  # StatefulSet pods are named -<ordinal>; we have replicas=1 so it's always -0.
  local pod="playground-dragonfly-${site}-0"
  kubectl -n "$NAMESPACE" delete pod "$pod" --grace-period=0 --force 2>/dev/null || \
    kubectl -n "$NAMESPACE" delete pod "$pod"
  ok "Dragonfly pod $pod killed"
}

cmd_dragonfly_status() {
  echo ""
  info "Dragonfly status (from MysqlFailoverGroup):"
  kubectl -n "$NAMESPACE" get mysqlfailovergroup playground \
    -o jsonpath='{.status.dragonfly}' 2>/dev/null \
    | python3 -m json.tool 2>/dev/null \
    || kubectl -n "$NAMESPACE" get mysqlfailovergroup playground -o yaml 2>/dev/null \
    | sed -n '/^  dragonfly:/,/^  [a-z]/p' \
    | sed '$d'

  echo ""
  info "Dragonfly pod labels:"
  # Use -o custom-columns instead of -L + positional awk: the default
  # kubectl pod columns include a RESTARTS field that becomes "1 (5m ago)"
  # after the first restart, which silently shifts every column to the
  # right and makes positional parsing print things like "site=ago)".
  kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=dragonfly \
    -o "custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready,SITE:.metadata.labels['shipstream\.io/site'],ROLE:.metadata.labels['shipstream\.io/dragonfly-role'],TRAFFIC:.metadata.labels['shipstream\.io/dragonfly-traffic']" \
    --no-headers 2>/dev/null \
    | awk '{printf "  %-32s ready=%-5s site=%-4s role=%-8s traffic=%s\n", $1, $2, $3, $4, $5}'

  echo ""
  info "Active Service endpoints (writes go here):"
  kubectl -n "$NAMESPACE" get endpointslice \
    -l kubernetes.io/service-name=playground-dragonfly \
    -o jsonpath='{range .items[*].endpoints[*]}  pod={.targetRef.name} ip={.addresses[*]} ready={.conditions.ready}{"\n"}{end}' 2>/dev/null \
    || warn "  (no endpoints — active master may be electing)"
  echo ""
}

cmd_recover() {
  info "Recovering from all chaos..."

  # Uncordon all playground nodes
  cmd_uncordon 2>/dev/null || true

  # Remove chaos NetworkPolicies
  kubectl -n "$NAMESPACE" delete networkpolicy -l app=chaos-partition 2>/dev/null || true

  ok "All chaos recovered (nodes uncordoned, network partitions removed)"
}

cmd_status() {
  echo ""
  info "Nodes:"
  kubectl get nodes -o wide -L topology.kubernetes.io/zone,shipstream.io/site.playground
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
  for node in $(kubectl get nodes -l 'shipstream.io/site.playground' -o name 2>/dev/null); do
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
  kill-dragonfly)     cmd_kill_dragonfly "${2:-}" ;;
  dragonfly-status)   cmd_dragonfly_status ;;
  cordon)             cmd_cordon "${2:-}" ;;
  uncordon)           cmd_uncordon ;;
  network-partition)  cmd_network_partition "${2:-}" ;;
  recover)            cmd_recover ;;
  status)             cmd_status ;;
  watch)              cmd_watch ;;
  *)                  usage ;;
esac
