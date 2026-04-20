#!/usr/bin/env bash
# Bloodraven Playground — Backup Verification
#
# Triggers a MysqlBackupVerification CR against the playground cluster,
# tails status + Job logs, and cleans up finished runs. Requires that
# a backup profile on the playground failover group has already
# produced at least one Succeeded MysqlBackup (e.g. via the scheduled
# CronJob, or by applying playground/manifests/mysqlbackup-adhoc.yaml).
#
# Usage:
#   ./playground/verify-backup.sh run [profile]        Create a verification and wait for a terminal phase (default profile: minio)
#   ./playground/verify-backup.sh run-pitr [profile]   Same as `run`, but with spec.pointInTime.mode=latest
#   ./playground/verify-backup.sh status               List verifications with phase + age
#   ./playground/verify-backup.sh logs [name]          Tail the Job pod log for the latest (or named) verification
#   ./playground/verify-backup.sh cleanup [--failed]   Delete Succeeded (or Failed with --failed) verifications
#   ./playground/verify-backup.sh schedule-list        Show scheduled verification CronJobs materialized from the profile
set -euo pipefail

NAMESPACE="bloodraven-playground"
GROUP="playground"
DEFAULT_PROFILE="minio"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
warn()  { echo -e "\033[1;33m!!\033[0m $*"; }
fail()  { echo -e "\033[1;31mERR\033[0m $*" >&2; exit 1; }

usage() {
  sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

# Emit a MysqlBackupVerification manifest to stdout. Uses generateName
# so repeated runs don't collide; mirrors the labels the scheduled
# CronJob would stamp so status/cleanup selectors work uniformly.
emit_verification_manifest() {
  local profile="$1" mode="${2:-none}"
  local name_stem="${GROUP}-${profile}-verify-"
  local pit_block=""
  if [[ "$mode" != "none" ]]; then
    pit_block=$(cat <<YAML

  pointInTime:
    mode: ${mode}
YAML
)
  fi
  cat <<YAML
apiVersion: shipstream.io/v1alpha1
kind: MysqlBackupVerification
metadata:
  generateName: ${name_stem}
  namespace: ${NAMESPACE}
  labels:
    shipstream.io/failover-group: ${GROUP}
    shipstream.io/backup-profile: ${profile}
    app.kubernetes.io/managed-by: bloodraven
spec:
  failoverGroupRef:
    name: ${GROUP}
  profileName: ${profile}
  triggeredBy: manual${pit_block}
YAML
}

ensure_succeeded_backup_exists() {
  local profile="$1"
  local count
  count=$(kubectl -n "$NAMESPACE" get mysqlbackups \
    -l "shipstream.io/failover-group=${GROUP},shipstream.io/backup-profile=${profile}" \
    -o json 2>/dev/null \
    | jq '[.items[] | select(.status.phase=="Succeeded")] | length')
  if [[ "${count:-0}" -eq 0 ]]; then
    fail "no Succeeded MysqlBackup found for (group=${GROUP}, profile=${profile}). Run a backup first (e.g. wait for the every-10min CronJob, or apply playground/manifests/mysqlbackup-adhoc.yaml)."
  fi
  info "found ${count} Succeeded MysqlBackup(s) for profile=${profile}"
}

# Wait for a verification CR to reach a terminal phase and print the
# result. Times out after TIMEOUT_SECS to avoid hanging when the
# cluster is wedged.
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"
wait_for_terminal() {
  local name="$1"
  local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
  local phase=""
  info "waiting for ${name} to reach Succeeded|Failed (timeout=${TIMEOUT_SECS}s)"
  while [[ $(date +%s) -lt $deadline ]]; do
    phase=$(kubectl -n "$NAMESPACE" get mysqlbackupverification "$name" \
      -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in
      Succeeded) ok "${name}: Succeeded"; return 0 ;;
      Failed)    warn "${name}: Failed"; return 1 ;;
      "")        printf '.' ;;
      *)         printf '[%s]' "$phase" ;;
    esac
    sleep 5
  done
  echo
  fail "${name} did not reach a terminal phase within ${TIMEOUT_SECS}s"
}

cmd_run() {
  local profile="${1:-$DEFAULT_PROFILE}"
  local mode="${2:-none}"

  ensure_succeeded_backup_exists "$profile"

  local out name
  out=$(emit_verification_manifest "$profile" "$mode" | kubectl create -f -)
  # `kubectl create` prints `mysqlbackupverification.shipstream.io/<name> created`
  name=$(echo "$out" | awk -F'[/ ]' '/created/ {print $2}')
  [[ -n "$name" ]] || fail "failed to extract CR name from: $out"
  ok "created MysqlBackupVerification/${name}"

  if wait_for_terminal "$name"; then
    echo
    kubectl -n "$NAMESPACE" get mysqlbackupverification "$name" -o wide
    return 0
  fi
  echo
  kubectl -n "$NAMESPACE" get mysqlbackupverification "$name" -o yaml \
    | sed -n '/status:/,$p' \
    | head -80
  warn "Job logs (tail -n 200):"
  cmd_logs "$name" || true
  return 1
}

cmd_run_pitr() {
  cmd_run "${1:-$DEFAULT_PROFILE}" "latest"
}

cmd_status() {
  kubectl -n "$NAMESPACE" get mysqlbackupverifications \
    -L shipstream.io/backup-profile \
    -o wide
}

cmd_logs() {
  local name="${1:-}"
  if [[ -z "$name" ]]; then
    name=$(kubectl -n "$NAMESPACE" get mysqlbackupverification \
      --sort-by=.metadata.creationTimestamp \
      -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
    [[ -n "$name" ]] || fail "no MysqlBackupVerification CRs found"
    info "tailing logs for most recent verification: ${name}"
  fi
  local pod
  pod=$(kubectl -n "$NAMESPACE" get pod \
    -l "shipstream.io/mysqlbackup-verification=${name}" \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
  if [[ -z "$pod" ]]; then
    warn "no Job pod yet for ${name} — it may still be provisioning"
    return 0
  fi
  kubectl -n "$NAMESPACE" logs "$pod" --tail=200
}

cmd_cleanup() {
  local phase="Succeeded"
  if [[ "${1:-}" == "--failed" ]]; then
    phase="Failed"
  fi
  local names
  names=$(kubectl -n "$NAMESPACE" get mysqlbackupverifications \
    -o json | jq -r ".items[] | select(.status.phase==\"${phase}\") | .metadata.name")
  if [[ -z "$names" ]]; then
    info "no ${phase} verifications to delete"
    return 0
  fi
  echo "$names" | while read -r n; do
    [[ -n "$n" ]] || continue
    info "deleting ${n} (${phase})"
    kubectl -n "$NAMESPACE" delete mysqlbackupverification "$n" --wait=false
  done
}

cmd_schedule_list() {
  kubectl -n "$NAMESPACE" get cronjobs \
    -l app.kubernetes.io/component=backup-verification-schedule \
    -o wide 2>/dev/null \
    || kubectl -n "$NAMESPACE" get cronjobs | grep -E 'NAME|verify' || true
}

# ── Main dispatch ────────────────────────────────────────────────────────
case "${1:-}" in
  run)            shift; cmd_run "${1:-}" ;;
  run-pitr)       shift; cmd_run_pitr "${1:-}" ;;
  status)         cmd_status ;;
  logs)           shift; cmd_logs "${1:-}" ;;
  cleanup)        shift; cmd_cleanup "${1:-}" ;;
  schedule-list)  cmd_schedule_list ;;
  *)              usage ;;
esac
