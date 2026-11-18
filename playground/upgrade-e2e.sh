#!/usr/bin/env bash
# Upgrade E2E (issue #244, C-8): install the previous published release
# into the playground with one object of every kind, upgrade it to this
# checkout's build, and check that the pod change the new release causes is
# rolled one failover group at a time and that `kubectl bloodraven
# migrate-storage` then moves every stored object to shipstream.io/v1beta1.
#
# Usage:
#   ./playground/upgrade-e2e.sh install   install the previous release (chart
#                                         and images as published) with the
#                                         playground group and apps, and one
#                                         MysqlDatabase, MysqlBackup,
#                                         MysqlBackupVerification and
#                                         MysqlStandbyCluster, created through
#                                         the version that release serves
#   ./playground/upgrade-e2e.sh upgrade   first `helm upgrade` without the
#                                         CRDs and check that the new operator
#                                         refuses to start while the previous
#                                         one keeps the lease (#299); then
#                                         apply this checkout's CRDs and
#                                         `helm upgrade` to its chart and the
#                                         locally built images
#   ./playground/upgrade-e2e.sh run       install, then run scenario
#                                         74-upgrade-paced-rollout (which
#                                         calls `upgrade` itself and watches
#                                         the rollout), scenario
#                                         78-upgrade-storage-migration and one
#                                         failover
#
# Environment:
#   BLOODRAVEN_UPGRADE_FROM           release to upgrade from (default 1.7.0)
#   BLOODRAVEN_UPGRADE_CHART_REPO     Helm repository of the published charts
#   BLOODRAVEN_UPGRADE_ROLLOUT_SOAK   rollout.soak after the upgrade (default
#                                     10s; the chart's default is 1m)
#   BLOODRAVEN_UPGRADE_KUBE_CONTEXT   kubectl/helm context for `upgrade`
#                                     (set by the scenario; default: current)
#
# Prerequisites: an empty playground cluster (see setup.sh) with one node
# more than the playground group's three sites use, for the auxiliary groups
# the scenario creates. The playground group rolls during the upgrade, and
# its db-readonly taint evicts other groups' pods from its sites' nodes.
#   k3d:  k3d cluster create bloodraven --agents 4
#   kind: kind create cluster --config=.github/kind/e2e-upgrade.yaml
# The images of this checkout must be built and loaded as for setup.sh;
# `install` builds and loads them unless SKIP_IMAGE_BUILD is set.
#
# The published chart and images are public: no registry login is needed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="bloodraven-playground"
FROM="${BLOODRAVEN_UPGRADE_FROM:-1.7.0}"
FROM="${FROM#v}"
CHART_REPO="${BLOODRAVEN_UPGRADE_CHART_REPO:-https://raw.githubusercontent.com/shipstream/bloodraven/gh-pages}"
ROLLOUT_SOAK="${BLOODRAVEN_UPGRADE_ROLLOUT_SOAK:-10s}"
# Releases before v1.8.0 serve shipstream.io/v1alpha1 only. The objects of
# the upgrade E2E are therefore created through v1alpha1 and stored as
# v1alpha1, which is what migrate-storage (scenario 78) is there to move.
case "$FROM" in
  0.*|1.[0-7].*) FROM_API_VERSION=v1alpha1 ;;
  *)             FROM_API_VERSION=v1beta1 ;;
esac
SCENARIOS="74-upgrade-paced-rollout,78-upgrade-storage-migration,01-clean-primary-kill"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
fail()  { echo -e "\033[1;31mERR\033[0m $*" >&2; exit 1; }

usage() {
  sed -n '2,36p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 2
}

require_tools() {
  local cmd
  for cmd in kubectl helm; do
    command -v "$cmd" >/dev/null || fail "$cmd is required but not found"
  done
}

# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"

# Podman stores images under localhost/ (see setup.sh).
image_prefix() {
  local runtime="${BLOODRAVEN_CONTAINER_RUNTIME:-}"
  if [[ -z "$runtime" ]]; then
    if command -v docker >/dev/null 2>&1; then runtime=docker; else runtime=podman; fi
  fi
  if [[ "$runtime" == "podman" ]]; then echo "localhost/"; fi
}

install_previous() {
  require_playground_context
  if helm status bloodraven -n "$NAMESPACE" >/dev/null 2>&1; then
    fail "Helm release bloodraven already exists in $NAMESPACE. The upgrade E2E starts from a fresh install of v$FROM: run ./playground/teardown.sh first."
  fi
  # Global, not local: the EXIT trap runs after this function returned.
  UPGRADE_E2E_TMP=$(mktemp -d "${TMPDIR:-/tmp}/bloodraven-upgrade-e2e-XXXXXX")
  trap 'rm -rf "$UPGRADE_E2E_TMP"' EXIT
  local tmp="$UPGRADE_E2E_TMP"
  info "Pulling the published chart bloodraven-$FROM from $CHART_REPO..."
  helm pull bloodraven --repo "$CHART_REPO" --version "$FROM" --untar --untardir "$tmp"
  local app_version
  app_version=$(awk '$1 == "appVersion:" {gsub(/"/, "", $2); print $2}' "$tmp/bloodraven/Chart.yaml")
  [[ "$app_version" == "$FROM" ]] || fail "chart bloodraven-$FROM has appVersion '$app_version'"
  ok "Chart bloodraven-$FROM pulled"

  info "Installing v$FROM (published chart, ghcr.io/shipstream/bloodraven:$FROM and bloodraven-sidecar:$FROM)..."
  BLOODRAVEN_SETUP_CHART_DIR="$tmp/bloodraven" \
  BLOODRAVEN_SETUP_OPERATOR_IMAGE_REPOSITORY="ghcr.io/shipstream/bloodraven" \
  BLOODRAVEN_SETUP_OPERATOR_IMAGE_TAG="$FROM" \
  BLOODRAVEN_SETUP_OPERATOR_PULL_POLICY="IfNotPresent" \
  BLOODRAVEN_SETUP_SIDECAR_IMAGE_REPOSITORY="ghcr.io/shipstream/bloodraven-sidecar" \
  BLOODRAVEN_SETUP_SIDECAR_IMAGE_TAG="$FROM" \
  BLOODRAVEN_SETUP_API_VERSION="$FROM_API_VERSION" \
    "$SCRIPT_DIR/setup.sh"

  local image sidecar
  image=$(kubectl -n "$NAMESPACE" get deployment bloodraven -o jsonpath='{.spec.template.spec.containers[?(@.name=="operator")].image}')
  sidecar=$(kubectl -n "$NAMESPACE" get deployment mysql-playground-iad -o jsonpath='{.spec.template.spec.containers[?(@.name=="sidecar")].image}')
  [[ "$image" == "ghcr.io/shipstream/bloodraven:$FROM" ]] || fail "the operator runs '$image', not the published v$FROM image"
  [[ "$sidecar" == "ghcr.io/shipstream/bloodraven-sidecar:$FROM" ]] || fail "the MySQL pods run sidecar '$sidecar', not the published v$FROM image"
  ok "v$FROM installed: operator $image, sidecar $sidecar"

  create_objects_of_every_kind
}

# One object of every kind, created through the version v$FROM serves and
# so stored as that version (C-8). The group gets the `rustfs` backup
# profile first, which the backup and its verification name.
create_objects_of_every_kind() {
  local objects="$SCRIPT_DIR/manifests/upgrade-e2e-objects.yaml"
  local after_backup="playground.bloodraven.shipstream.io/after-backup"
  objects_manifest() {
    sed "s|^apiVersion: shipstream.io/v1beta1\$|apiVersion: shipstream.io/$FROM_API_VERSION|" "$objects"
  }

  info "Creating the RustFS bucket and adding the rustfs backup profile to the playground group..."
  create_rustfs_bucket bloodraven
  kubectl -n "$NAMESPACE" patch mysqlfailovergroup playground --type merge -p '{
    "spec": {"backup": {
      "image": "container-registry.oracle.com/mysql/community-server:9.7",
      "profiles": [{
        "name": "rustfs",
        "storage": {"type": "S3", "s3": {
          "bucket": "bloodraven", "prefix": "playground", "region": "us-east-1",
          "endpointURL": "http://rustfs.bloodraven-playground.svc.cluster.local:9000",
          "credentialsSecretName": "dragonfly-s3-credentials"}}
      }]
    }}}'

  info "Creating a MysqlDatabase, MysqlBackup and MysqlStandbyCluster through shipstream.io/$FROM_API_VERSION..."
  objects_manifest | kubectl apply -l "!$after_backup" -f -
  kubectl -n "$NAMESPACE" wait mysqldatabase/upgrade-e2e --for=condition=Ready --timeout=120s
  kubectl -n "$NAMESPACE" wait mysqlbackup/upgrade-e2e --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s \
    || { kubectl -n "$NAMESPACE" get mysqlbackup upgrade-e2e -o yaml; fail "the backup taken on v$FROM did not succeed"; }
  info "Creating a MysqlBackupVerification of that backup..."
  objects_manifest | kubectl apply -l "$after_backup" -f -

  local kind
  for kind in mysqlfailovergroups mysqldatabases mysqlbackups mysqlbackupverifications mysqlstandbyclusters; do
    [[ -n "$(kubectl -n "$NAMESPACE" get "$kind.shipstream.io" -o name)" ]] || fail "no object of kind $kind exists before the upgrade"
  done
  ok "One object of every kind exists, stored as shipstream.io/$FROM_API_VERSION"
}

# The playground's RustFS starts without buckets. The request is signed by
# curl (SigV4), through a port-forward, so no S3 client is needed.
create_rustfs_bucket() {
  local bucket="$1" key secret port=19000 pf code="" i
  command -v curl >/dev/null || fail "curl is required to create the RustFS bucket"
  key=$(kubectl -n "$NAMESPACE" get secret dragonfly-s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
  secret=$(kubectl -n "$NAMESPACE" get secret dragonfly-s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
  kubectl -n "$NAMESPACE" rollout status deployment/rustfs --timeout=120s >/dev/null
  kubectl -n "$NAMESPACE" port-forward svc/rustfs "$port:9000" >/dev/null 2>&1 &
  pf=$!
  for i in $(seq 1 30); do
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
      "http://127.0.0.1:$port/$bucket" 2>/dev/null || true)
    # 409: the bucket exists already.
    [[ "$code" == "200" || "$code" == "409" ]] && break
    sleep 1
  done
  kill "$pf" 2>/dev/null || true
  [[ "$code" == "200" || "$code" == "409" ]] || fail "could not create the RustFS bucket $bucket (HTTP $code)"
}

upgrade_to_checkout() {
  local kctx=() hctx=()
  if [[ -n "${BLOODRAVEN_UPGRADE_KUBE_CONTEXT:-}" ]]; then
    kctx=(--context "$BLOODRAVEN_UPGRADE_KUBE_CONTEXT")
    hctx=(--kube-context "$BLOODRAVEN_UPGRADE_KUBE_CONTEXT")
  else
    require_playground_context
  fi
  helm ${hctx[@]+"${hctx[@]}"} status bloodraven -n "$NAMESPACE" >/dev/null 2>&1 \
    || fail "no Helm release bloodraven in $NAMESPACE: run ./playground/upgrade-e2e.sh install first"

  local prefix
  prefix=$(image_prefix)
  # --reuse-values on purpose: it is the upgrade the operations guide and
  # the chart's NOTES.txt show, and it renders the new templates over the
  # old release's values, which have no `rollout` key.
  local upgrade_args=(
    upgrade bloodraven "$PROJECT_ROOT/charts/bloodraven"
    --namespace "$NAMESPACE"
    --reuse-values
    --set "image.repository=${prefix}bloodraven"
    --set image.tag=playground
    --set image.pullPolicy=Never
    --set "sidecar.image.repository=${prefix}bloodraven-sidecar"
    --set sidecar.image.tag=playground
    --set "rollout.soak=$ROLLOUT_SOAK"
    --timeout=180s
  )

  # The mistake first (#299): the chart without its CRDs. Only while the
  # installed CRDs are older than this build needs, which is the case when
  # they do not serve v1beta1 yet.
  local served
  served=$(kubectl ${kctx[@]+"${kctx[@]}"} get crd mysqlfailovergroups.shipstream.io -o jsonpath='{.spec.versions[*].name}')
  if [[ " $served " != *" v1beta1 "* ]]; then
    chart_only_upgrade_is_refused
  fi

  # Helm installs crds/ on the first install only and never upgrades them,
  # so the release's CRDs are applied before the chart, as the upgrade guide
  # says. Without them the API server prunes status.rollout and nothing rolls.
  info "Applying this checkout's CRDs..."
  kubectl ${kctx[@]+"${kctx[@]}"} apply --server-side --force-conflicts -f "$PROJECT_ROOT/charts/bloodraven/crds/"

  info "helm upgrade to this checkout's chart (operator ${prefix}bloodraven:playground, sidecar ${prefix}bloodraven-sidecar:playground, rollout.soak=$ROLLOUT_SOAK)..."
  helm ${hctx[@]+"${hctx[@]}"} "${upgrade_args[@]}"
  # A pod that refused in the step above starts at its next restart, which
  # the kubelet delays by its crash back-off.
  kubectl ${kctx[@]+"${kctx[@]}"} -n "$NAMESPACE" rollout status deployment/bloodraven --timeout=300s
  ok "Operator upgraded"
}

# chart_only_upgrade_is_refused upgrades the chart while the previous
# release's CRDs are still installed and checks what the upgrade guide
# promises for that mistake: the new operator refuses to start, and the
# previous operator keeps running and keeps the leader lease. It uses kctx,
# hctx and upgrade_args of upgrade_to_checkout (bash's dynamic scope).
chart_only_upgrade_is_refused() {
  local selector="app.kubernetes.io/name=bloodraven,app.kubernetes.io/instance=bloodraven"
  local lease="bloodraven.shipstream.io"
  local refusal="CRD check: the operator must not start"
  local k=(kubectl ${kctx[@]+"${kctx[@]}"} -n "$NAMESPACE")

  local previous
  previous=$("${k[@]}" get pods -l "$selector" -o jsonpath='{.items[*].metadata.name}')
  [[ -n "$previous" && "$previous" != *" "* ]] \
    || fail "expected one operator pod before the upgrade, found: '${previous}'"
  local holder
  holder=$("${k[@]}" get lease "$lease" -o jsonpath='{.spec.holderIdentity}')
  [[ "$holder" == "${previous}_"* ]] \
    || fail "the operator pod $previous does not hold the lease $lease before the upgrade (holder: '$holder')"

  info "Chart-only helm upgrade (the CRDs of v$FROM stay): the new operator must refuse to start..."
  helm ${hctx[@]+"${hctx[@]}"} "${upgrade_args[@]}"

  local deadline=$((SECONDS + 180)) refused="" pod logs
  while [[ -z "$refused" ]]; do
    (( SECONDS < deadline )) || fail "no new operator pod logged '$refusal' within 180s of the chart-only upgrade"
    for pod in $("${k[@]}" get pods -l "$selector" -o jsonpath='{.items[*].metadata.name}'); do
      [[ "$pod" == "$previous" ]] && continue
      # A pod in its crash back-off has the line in its previous container.
      logs=$("${k[@]}" logs "$pod" --tail=200 2>/dev/null || true; "${k[@]}" logs "$pod" --previous --tail=200 2>/dev/null || true)
      if [[ "$logs" == *"$refusal"* ]]; then
        refused="$pod"
        break
      fi
    done
    [[ -n "$refused" ]] || sleep 3
  done
  ok "The new operator pod $refused refused to start"

  # The previous operator: still there, not being deleted, Ready, and
  # renewing the lease it held before the upgrade.
  local deleting ready renewed_before renewed_after
  deleting=$("${k[@]}" get pod "$previous" -o jsonpath='{.metadata.deletionTimestamp}') \
    || fail "the previous operator pod $previous is gone after the chart-only upgrade"
  [[ -z "$deleting" ]] || fail "the previous operator pod $previous is being deleted (since $deleting) although the new one refused"
  ready=$("${k[@]}" get pod "$previous" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
  [[ "$ready" == "True" ]] || fail "the previous operator pod $previous is not Ready (Ready=$ready)"
  renewed_before=$("${k[@]}" get lease "$lease" -o jsonpath='{.spec.renewTime}')
  sleep 20
  holder=$("${k[@]}" get lease "$lease" -o jsonpath='{.spec.holderIdentity}')
  renewed_after=$("${k[@]}" get lease "$lease" -o jsonpath='{.spec.renewTime}')
  [[ "$holder" == "${previous}_"* ]] \
    || fail "the lease $lease moved away from the previous operator $previous (holder: '$holder')"
  [[ "$renewed_after" != "$renewed_before" ]] \
    || fail "the previous operator $previous stopped renewing the lease $lease (renewTime $renewed_before for 20s)"
  ok "The previous operator $previous still runs and leads"
}

run_all() {
  install_previous
  info "Building playground-chaos..."
  (cd "$PROJECT_ROOT" && make build-playground-chaos)
  info "Running $SCENARIOS..."
  (cd "$PROJECT_ROOT" && ./bin/playground-chaos run-all --scenarios="$SCENARIOS")
}

case "${1:-}" in
  install) require_tools; install_previous ;;
  upgrade) require_tools; upgrade_to_checkout ;;
  run)     require_tools; run_all ;;
  *)       usage ;;
esac
