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

NAMESPACE="bloodraven-playground"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }

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
  local node
  node=$(site_node "$site")
  if [[ -z "$node" ]]; then
    warn "No node found for site $site"
    return 1
  fi
  info "Simulating network partition on $node (blocking port 3306)..."
  info "Deploying a privileged debug pod to run iptables..."

  # Create a privileged debug pod on the target node to manipulate iptables
  local pod_name="chaos-netblock-${site}"
  kubectl -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found --wait=false 2>/dev/null || true
  kubectl -n "$NAMESPACE" run "$pod_name" \
    --image=alpine \
    --restart=Never \
    --overrides='{
      "spec": {
        "hostNetwork": true,
        "nodeName": "'"$node"'",
        "containers": [{
          "name": "chaos",
          "image": "alpine",
          "command": ["sh", "-c", "apk add --no-cache iptables >/dev/null 2>&1 && iptables -A INPUT -p tcp --dport 3306 -j DROP && iptables -A OUTPUT -p tcp --sport 3306 -j DROP && echo BLOCKED && sleep 3600"],
          "securityContext": {"privileged": true}
        }],
        "tolerations": [{"operator": "Exists"}]
      }
    }' 2>/dev/null || {
    warn "Could not create debug pod. Your cluster may not allow privileged pods."
    warn "Alternative: kubectl cordon the node instead."
    return 1
  }

  # Wait for the block to take effect
  kubectl -n "$NAMESPACE" wait --for=condition=ready "pod/$pod_name" --timeout=30s 2>/dev/null || true
  ok "Network partition active on $node (MySQL port 3306 blocked)"
  echo "  To remove: ./playground/chaos.sh recover"
}

cmd_recover() {
  info "Recovering from all chaos..."

  # Uncordon all playground nodes
  cmd_uncordon 2>/dev/null || true

  # Delete any chaos network-block pods (they hold the iptables rules;
  # deleting them + the hostNetwork setup means rules are cleaned on pod exit)
  for site in iad pdx; do
    local pod_name="chaos-netblock-${site}"
    if kubectl -n "$NAMESPACE" get pod "$pod_name" >/dev/null 2>&1; then
      info "Removing network partition pod for $site..."
      # First flush iptables via the pod, then delete it
      kubectl -n "$NAMESPACE" exec "$pod_name" -- sh -c "iptables -F" 2>/dev/null || true
      kubectl -n "$NAMESPACE" delete pod "$pod_name" --grace-period=0 --force 2>/dev/null || true
    fi
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
