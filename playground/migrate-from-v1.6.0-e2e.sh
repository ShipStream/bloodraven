#!/usr/bin/env bash
# End-to-end test of the move off v1.6.0 (issue #243, requirement U-9 of
# proposals/20-api-v1beta1.md): a group that runs the published v1.6.0 is
# moved to this checkout's build as a NEW group in a NEW cluster, restored
# from an encrypted S3 backup. There is no in-place upgrade from v1.6.0;
# this is the procedure the upgrade policy documents
# (site/content/docs/6.operations/10.upgrade-policy.md, "Upgrading from
# v1.6.0 to v1.7.0"), executed step by step.
#
# It needs two kind clusters and several GB of memory, so no CI job runs it.
# Run it by hand when the procedure, the restore path or the migration table
# changes.
#
# Usage:
#   ./playground/migrate-from-v1.6.0-e2e.sh run        every step below, in order
#   ./playground/migrate-from-v1.6.0-e2e.sh old        create the old cluster and install the published v1.6.0
#   ./playground/migrate-from-v1.6.0-e2e.sh seed       add data, a tenant database and an account made by hand
#   ./playground/migrate-from-v1.6.0-e2e.sh backup     stop writes, take the encrypted S3 backup, record what was there
#   ./playground/migrate-from-v1.6.0-e2e.sh new        create the new cluster with this build and bootstrap the group from the backup
#   ./playground/migrate-from-v1.6.0-e2e.sh tenant     create the tenant database and load its schema from the backup
#   ./playground/migrate-from-v1.6.0-e2e.sh accounts   recreate the account made by hand
#   ./playground/migrate-from-v1.6.0-e2e.sh baseline   new baseline backup, PITR archive, verification
#   ./playground/migrate-from-v1.6.0-e2e.sh verify     compare the data, the account and the old prefix
#   ./playground/migrate-from-v1.6.0-e2e.sh teardown   delete both clusters and the v1.6.0 checkout
#
# What it asserts at the end (U-9):
#   - every table of the application schema and of the tenant database has
#     the row count and checksum it had on v1.6.0 when writes stopped (the
#     playground's own counter application starts writing to the new group
#     as soon as the restore succeeds, so its schema is not compared);
#   - the account created outside the operator works on the new group;
#   - the new group is Ready, its tenant database is Ready, its PITR archive
#     is ready on a prefix no v1.6.0 group used, and its baseline backup is
#     verified;
#   - no object under the old group's prefix changed.
#
# Environment:
#   MIGRATE_E2E_DIR          state directory (default: playground/chaos-results/migrate-from-v1.6.0)
#   MIGRATE_E2E_POD_SUBNET   pod CIDR of both clusters (default 10.244.0.0/16; it
#                            must not overlap the Docker network kind uses)
#   MIGRATE_E2E_FROM         the release to move off (default 1.6.0)
#
# Prerequisites: docker, kind, kubectl, helm, curl, yq (mikefarah), git, and
# this repository with the tag v$MIGRATE_E2E_FROM fetched. The published
# images are pulled anonymously from ghcr.io by the cluster nodes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FROM="${MIGRATE_E2E_FROM:-1.6.0}"
FROM="${FROM#v}"
STATE="${MIGRATE_E2E_DIR:-$PROJECT_ROOT/playground/chaos-results/migrate-from-v$FROM}"
POD_SUBNET="${MIGRATE_E2E_POD_SUBNET:-10.244.0.0/16}"
OLD_CLUSTER="bloodraven-v${FROM//./}"
NEW_CLUSTER="bloodraven-new"
OLD_KUBECONFIG="$STATE/old.kubeconfig"
NEW_KUBECONFIG="$STATE/new.kubeconfig"
OLD_SRC="$STATE/src-v$FROM"
NS="bloodraven-playground"
GROUP="playground"

BUCKET="bloodraven"
OLD_PREFIX="old/$GROUP"          # the v1.6.0 group's backup profile
NEW_PREFIX="new/$GROUP"          # the new group's profile: never used by v1.6.0 (U-3)
RUSTFS_NODEPORT=30900
PASSPHRASE_SECRET="backup-passphrase"
TENANT_SCHEMA="acme_wms"
APP_SCHEMA="shop"
HAND_MADE_USER="legacy_app"
HAND_MADE_PASSWORD="legacy-app-password"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m OK\033[0m $*"; }
fail()  { echo -e "\033[1;31mERR\033[0m $*" >&2; exit 1; }

old() { KUBECONFIG="$OLD_KUBECONFIG" "$@"; }
new() { KUBECONFIG="$NEW_KUBECONFIG" "$@"; }

require_tools() {
  local cmd
  for cmd in docker kind kubectl helm curl yq git; do
    command -v "$cmd" >/dev/null || fail "$cmd is required but not found"
  done
  mkdir -p "$STATE"
}

# ── clusters ────────────────────────────────────────────────────────────

# create_cluster NAME KUBECONFIG: a control plane and three workers (one per
# site of the playground group), with Calico as in the E2E jobs.
create_cluster() {
  local name="$1" kubeconfig="$2"
  if kind get clusters 2>/dev/null | grep -qx "$name"; then
    info "kind cluster $name exists"
    kind get kubeconfig --name "$name" >"$kubeconfig"
    return
  fi
  info "Creating kind cluster $name..."
  cat >"$STATE/$name.kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: $name
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
networking:
  disableDefaultCNI: true
  podSubnet: "$POD_SUBNET"
EOF
  # A proxy in the environment would be copied into the nodes, which
  # cannot reach it.
  env -u HTTPS_PROXY -u HTTP_PROXY -u https_proxy -u http_proxy -u NO_PROXY -u no_proxy \
    kind create cluster --config "$STATE/$name.kind.yaml" --image kindest/node:v1.35.0 --wait 0s --kubeconfig "$kubeconfig"
  curl -fsSL -o "$STATE/calico.yaml" https://raw.githubusercontent.com/projectcalico/calico/v3.28.0/manifests/calico.yaml
  echo "93e7f39c3f93f9dc67ee2386aa7240d8d13ff62e54535a2f5f6432e8c6c0f24f  $STATE/calico.yaml" | sha256sum -c - >/dev/null
  sed -i "s|# - name: CALICO_IPV4POOL_CIDR|- name: CALICO_IPV4POOL_CIDR|; s|#   value: \"192.168.0.0/16\"|  value: \"$POD_SUBNET\"|" "$STATE/calico.yaml"
  KUBECONFIG="$kubeconfig" kubectl apply -f "$STATE/calico.yaml" >/dev/null
  KUBECONFIG="$kubeconfig" kubectl -n kube-system rollout status daemonset/calico-node --timeout=300s
  KUBECONFIG="$kubeconfig" kubectl wait nodes --all --for=condition=Ready --timeout=300s
  ok "Cluster $name is ready"
}

# node_image CLUSTER SOURCE TARGET: make SOURCE available on every node
# under the name TARGET, without retagging anything in the host's Docker.
# SOURCE is pulled by the node when it is a registry reference.
node_image() {
  local cluster="$1" source="$2" target="$3" node ref
  for node in $(kind get nodes --name "$cluster"); do
    if [[ "$source" == */* ]]; then
      ref="$source"
      docker exec "$node" crictl pull "$ref" >/dev/null
    else
      ref="docker.io/library/$source"
    fi
    docker exec "$node" ctr -n k8s.io images tag --force "$ref" "docker.io/library/$target" >/dev/null
  done
}

# ── MySQL helpers ───────────────────────────────────────────────────────

root_password() { "$1" kubectl -n "$NS" get secret mysql-credentials -o jsonpath='{.data.MYSQL_ROOT_PASSWORD}' | base64 -d; }

active_site() { "$1" kubectl -n "$NS" get mysqlfailovergroup "$GROUP" -o jsonpath='{.status.activeSite}'; }

# sql old|new SITE STATEMENT...: run SQL as root on a site.
sql() {
  local where="$1" site="$2"
  shift 2
  "$where" kubectl -n "$NS" exec "deploy/mysql-$GROUP-$site" -c mysql -- \
    env MYSQL_PWD="$(root_password "$where")" mysql -h127.0.0.1 -uroot -N -B -e "$*"
}

# fingerprint old|new: "schema.table rows checksum" for every table of the
# schemas the move must carry, on the active site.
fingerprint() {
  local where="$1" site tables t
  site=$(active_site "$where")
  tables=$(sql "$where" "$site" "SELECT CONCAT(table_schema, '.', table_name) FROM information_schema.tables
    WHERE table_type = 'BASE TABLE' AND table_schema IN ('$APP_SCHEMA', '$TENANT_SCHEMA') ORDER BY 1")
  [[ -n "$tables" ]] || fail "no tables found on $where/$site"
  for t in $tables; do
    echo "$t $(sql "$where" "$site" "SELECT COUNT(*) FROM $t") $(sql "$where" "$site" "CHECKSUM TABLE $t" | awk '{print $2}')"
  done
}

# ── object storage ──────────────────────────────────────────────────────

# s3 METHOD PATH: a signed request to the old cluster's RustFS, through a
# port-forward. curl signs it (SigV4), so no S3 client is needed.
s3() {
  local method="$1" path="$2" key secret pf out
  key=$(old kubectl -n "$NS" get secret dragonfly-s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
  secret=$(old kubectl -n "$NS" get secret dragonfly-s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
  old kubectl -n "$NS" port-forward svc/rustfs 19100:9000 >/dev/null 2>&1 &
  pf=$!
  local i
  for i in $(seq 1 30); do
    if out=$(curl -sS -f -X "$method" --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" "http://127.0.0.1:19100$path" 2>/dev/null); then
      kill "$pf" 2>/dev/null || true
      echo "$out"
      return 0
    fi
    sleep 1
  done
  kill "$pf" 2>/dev/null || true
  return 1
}

# list_prefix PREFIX: every object under PREFIX with its size, ETag and
# modification time, one per line, sorted.
list_prefix() {
  local listing
  listing=$(s3 GET "/$BUCKET?list-type=2&max-keys=1000&prefix=$1/") || fail "cannot list s3://$BUCKET/$1/"
  grep -q '<IsTruncated>false</IsTruncated>' <<<"$listing" || fail "s3://$BUCKET/$1/ has more than 1000 objects; the listing is not complete"
  # One <Contents> element per line, then each field on its own: servers
  # order the fields differently.
  local entry
  grep -o '<Contents>.*</Contents>' <<<"$listing" | sed 's|</Contents>|</Contents>\n|g' | while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    echo "$(sed -n 's|.*<Key>\([^<]*\)</Key>.*|\1|p' <<<"$entry") $(sed -n 's|.*<Size>\([^<]*\)</Size>.*|\1|p' <<<"$entry")" \
      "$(sed -n 's|.*<ETag>\([^<]*\)</ETag>.*|\1|p' <<<"$entry") $(sed -n 's|.*<LastModified>\([^<]*\)</LastModified>.*|\1|p' <<<"$entry")"
  done | sort
}

old_node_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$OLD_CLUSTER-worker"
}

shared_endpoint() { echo "http://$(old_node_ip):$RUSTFS_NODEPORT"; }

# ── steps ───────────────────────────────────────────────────────────────

step_old() {
  create_cluster "$OLD_CLUSTER" "$OLD_KUBECONFIG"

  if [[ ! -d "$OLD_SRC" ]]; then
    info "Checking out v$FROM into $OLD_SRC..."
    git -C "$PROJECT_ROOT" worktree add --detach "$OLD_SRC" "v$FROM" >/dev/null
  fi
  info "Chart of v$FROM: comparing the checkout's with the published one..."
  rm -rf "$STATE/published-chart"
  helm pull bloodraven --repo https://raw.githubusercontent.com/shipstream/bloodraven/gh-pages --version "$FROM" --untar --untardir "$STATE/published-chart"
  diff -r -q "$STATE/published-chart/bloodraven/crds" "$OLD_SRC/charts/bloodraven/crds" >/dev/null \
    || fail "the CRDs of the v$FROM tag differ from the published chart's"
  diff -r -q "$STATE/published-chart/bloodraven/templates" "$OLD_SRC/charts/bloodraven/templates" >/dev/null \
    || fail "the templates of the v$FROM tag differ from the published chart's"
  ok "The v$FROM checkout's chart is the published chart"

  info "Images: the published operator and sidecar of v$FROM, and v$FROM's playground apps..."
  node_image "$OLD_CLUSTER" "ghcr.io/shipstream/bloodraven:$FROM" bloodraven:playground
  node_image "$OLD_CLUSTER" "ghcr.io/shipstream/bloodraven-sidecar:$FROM" bloodraven-sidecar:playground
  local app
  for app in counter dashboard dns-webhook; do
    local dir="$OLD_SRC/playground/$app"
    [[ "$app" == "counter" ]] && dir="$OLD_SRC/playground/counter-app"
    docker build -q -t "bloodraven-$app:v${FROM//./}" "$dir" >/dev/null
    kind load docker-image "bloodraven-$app:v${FROM//./}" --name "$OLD_CLUSTER" >/dev/null
    node_image "$OLD_CLUSTER" "bloodraven-$app:v${FROM//./}" "bloodraven-$app:playground"
  done

  info "Installing v$FROM with its own playground setup..."
  # v$FROM's setup.sh loads the host's *:playground images into the first
  # kind cluster it finds, even with SKIP_IMAGE_BUILD: that would replace
  # the published images with this checkout's build. Hide the cluster tools
  # from it; the images are on the nodes already.
  local shims="$STATE/no-cluster-tools"
  mkdir -p "$shims"
  printf '#!/bin/sh\nexit 0\n' >"$shims/kind"
  printf '#!/bin/sh\nexit 0\n' >"$shims/k3d"
  printf '#!/bin/sh\nexit 1\n' >"$shims/minikube"
  chmod +x "$shims/kind" "$shims/k3d" "$shims/minikube"
  (cd "$OLD_SRC" && PATH="$shims:$PATH" KUBECONFIG="$OLD_KUBECONFIG" SKIP_IMAGE_BUILD=1 ./playground/setup.sh)
  # Prove what runs: on every node the names the pods use resolve to the
  # published images.
  local node name
  for node in $(kind get nodes --name "$OLD_CLUSTER"); do
    for name in bloodraven bloodraven-sidecar; do
      [[ "$(docker exec "$node" crictl inspecti -o go-template --template '{{.status.id}}' "docker.io/library/$name:playground")" == \
         "$(docker exec "$node" crictl inspecti -o go-template --template '{{.status.id}}' "ghcr.io/shipstream/$name:$FROM")" ]] \
        || fail "on $node, $name:playground is not the published ghcr.io/shipstream/$name:$FROM"
    done
  done
  ok "The old cluster's operator and sidecars are the published v$FROM images"
  old kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=condition=Ready --timeout=600s
  old kubectl get crd mysqlfailovergroups.shipstream.io -o jsonpath='{.spec.versions[*].name}{"\n"}' | grep -qx v1alpha1 \
    || fail "the old cluster's CRD does not serve v1alpha1 only"
  ok "The old cluster runs v$FROM"
}

step_seed() {
  local site
  site=$(active_site old)
  info "Seeding the old group on $site: an application schema, a tenant database and an account made by hand..."
  sql old "$site" "
    CREATE DATABASE IF NOT EXISTS $APP_SCHEMA;
    CREATE TABLE IF NOT EXISTS $APP_SCHEMA.items (id INT PRIMARY KEY, sku VARCHAR(32) NOT NULL, qty INT NOT NULL);
    INSERT IGNORE INTO $APP_SCHEMA.items
      WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500) SELECT i, CONCAT('SKU-', LPAD(i, 5, '0')), i % 17 FROM n;
    CREATE USER IF NOT EXISTS '$HAND_MADE_USER'@'%' IDENTIFIED BY '$HAND_MADE_PASSWORD';
    GRANT SELECT ON $APP_SCHEMA.* TO '$HAND_MADE_USER'@'%';"

  old kubectl -n "$NS" create secret generic acme-mysql-owner \
    --from-literal=username=acme_owner --from-literal=password=acme-owner-password --dry-run=client -o yaml | old kubectl apply -f -
  # v1.6.0's shape: groupRef, which v1.7.0 renamed to failoverGroupRef.
  old kubectl apply -f - <<EOF
apiVersion: shipstream.io/v1alpha1
kind: MysqlDatabase
metadata:
  name: tenant-acme
  namespace: $NS
spec:
  groupRef:
    name: $GROUP
  databaseName: $TENANT_SCHEMA
  owner:
    secretName: acme-mysql-owner
    privileges: [ALL PRIVILEGES]
EOF
  old kubectl -n "$NS" wait mysqldatabase/tenant-acme --for=condition=Ready --timeout=180s
  sql old "$site" "
    CREATE TABLE IF NOT EXISTS $TENANT_SCHEMA.orders (id INT PRIMARY KEY, customer VARCHAR(64) NOT NULL, total DECIMAL(10,2) NOT NULL);
    INSERT IGNORE INTO $TENANT_SCHEMA.orders
      WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200) SELECT i, CONCAT('customer-', i), i * 1.25 FROM n;"
  ok "Seeded: $APP_SCHEMA.items (500 rows), $TENANT_SCHEMA.orders (200 rows), account $HAND_MADE_USER"
}

step_backup() {
  info "Sharing the old cluster's RustFS with the new cluster (NodePort $RUSTFS_NODEPORT)..."
  old kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: rustfs-shared
  namespace: $NS
spec:
  type: NodePort
  selector:
    app.kubernetes.io/name: rustfs
  ports:
    - port: 9000
      targetPort: 9000
      nodePort: $RUSTFS_NODEPORT
EOF
  s3 PUT "/$BUCKET" >/dev/null || s3 GET "/$BUCKET?list-type=2&max-keys=1" >/dev/null || fail "cannot create the bucket $BUCKET"

  info "Adding an encrypted S3 backup profile to the v$FROM group (prefix $OLD_PREFIX)..."
  old kubectl -n "$NS" create secret generic "$PASSPHRASE_SECRET" \
    --from-literal=passphrase="correct horse battery staple $FROM" --dry-run=client -o yaml | old kubectl apply -f -
  # v1.6.0's field names: credentialsSecret, passphraseSecret.
  old kubectl -n "$NS" patch mysqlfailovergroup "$GROUP" --type merge -p "{
    \"spec\": {\"backup\": {
      \"image\": \"container-registry.oracle.com/mysql/community-server:9.7\",
      \"profiles\": [{
        \"name\": \"s3-enc\",
        \"storage\": {\"type\": \"S3\", \"s3\": {
          \"bucket\": \"$BUCKET\", \"prefix\": \"$OLD_PREFIX\", \"region\": \"us-east-1\",
          \"endpointURL\": \"http://rustfs.$NS.svc.cluster.local:9000\",
          \"credentialsSecret\": \"dragonfly-s3-credentials\"}},
        \"encryption\": {\"algorithm\": \"AES-256-GCM\", \"passphraseSecret\": {\"name\": \"$PASSPHRASE_SECRET\"}}
      }]
    }}}"

  info "Stopping application writes..."
  old kubectl -n "$NS" scale deployment/counter-app --replicas=0
  old kubectl -n "$NS" rollout status deployment/counter-app --timeout=120s
  sleep 5
  fingerprint old >"$STATE/fingerprint.old"
  cat "$STATE/fingerprint.old"

  info "Taking the backup on v$FROM..."
  old kubectl apply -f - <<EOF
apiVersion: shipstream.io/v1alpha1
kind: MysqlBackup
metadata:
  name: before-the-move
  namespace: $NS
spec:
  failoverGroupRef:
    name: $GROUP
  profileName: s3-enc
EOF
  old kubectl -n "$NS" wait mysqlbackup/before-the-move --for=jsonpath='{.status.phase}'=Succeeded --timeout=900s \
    || { old kubectl -n "$NS" get mysqlbackup before-the-move -o yaml; fail "the backup on v$FROM did not succeed"; }
  [[ "$(old kubectl -n "$NS" get mysqlbackup before-the-move -o jsonpath='{.status.encrypted}')" == "true" ]] || fail "the backup is not encrypted"

  # status.location is the dump's own path: s3://<bucket>/<key>, or a bare
  # key in the profile's bucket. It is not the profile prefix (U-4).
  local location
  location=$(old kubectl -n "$NS" get mysqlbackup before-the-move -o jsonpath='{.status.location}')
  [[ -n "$location" ]] || fail "the backup has no status.location"
  location="${location#s3://$BUCKET/}"
  echo "${location%/}" >"$STATE/dump.prefix"
  ok "Backup taken: s3://$BUCKET/$(cat "$STATE/dump.prefix")"

  list_prefix "old" >"$STATE/old-prefix.before"
  [[ -s "$STATE/old-prefix.before" ]] || fail "nothing is stored under s3://$BUCKET/old/"
  ok "$(wc -l <"$STATE/old-prefix.before") objects under s3://$BUCKET/old/ recorded"
}

# new_group_manifest: the playground group of this checkout, plus what the
# move needs. Every name here is today's (the migration table of the
# upgrade policy): nothing is copied from the v1.6.0 manifest.
new_group_manifest() {
  local endpoint dump
  endpoint=$(shared_endpoint)
  dump=$(cat "$STATE/dump.prefix")
  ENDPOINT="$endpoint" DUMP="$dump" BUCKET="$BUCKET" NEW_PREFIX="$NEW_PREFIX" PASSPHRASE_SECRET="$PASSPHRASE_SECRET" TENANT_SCHEMA="$TENANT_SCHEMA" \
  yq '
    .spec.initFromBackup = {
      "source": {"s3": {"bucket": strenv(BUCKET), "prefix": strenv(DUMP), "region": "us-east-1",
                        "endpointURL": strenv(ENDPOINT), "credentialsSecretName": "dragonfly-s3-credentials"}},
      "decryption": {"passphraseSecretKeyRef": {"name": strenv(PASSPHRASE_SECRET)}},
      "loadOptions": {"excludeSchemas": [strenv(TENANT_SCHEMA)]}
    } |
    .spec.backup = {
      "image": "container-registry.oracle.com/mysql/community-server:9.7",
      "profiles": [{
        "name": "s3-new",
        "storage": {"type": "S3", "s3": {"bucket": strenv(BUCKET), "prefix": strenv(NEW_PREFIX), "region": "us-east-1",
                                         "endpointURL": strenv(ENDPOINT), "credentialsSecretName": "dragonfly-s3-credentials"}},
        "encryption": {"algorithm": "AES-256-GCM", "passphraseSecretKeyRef": {"name": strenv(PASSPHRASE_SECRET)}}
      }],
      "pitr": {"enabled": true, "profileName": "s3-new"}
    }
  ' "$PROJECT_ROOT/playground/manifests/failovergroup.yaml"
}

step_new() {
  [[ -s "$STATE/dump.prefix" ]] || fail "no backup recorded: run the backup step first"
  create_cluster "$NEW_CLUSTER" "$NEW_KUBECONFIG"
  new kubectl get crd mysqlfailovergroups.shipstream.io >/dev/null 2>&1 \
    && info "the new cluster already has the Bloodraven CRDs (a rerun)" || ok "The new cluster has never had a Bloodraven CRD (U-1)"

  info "Loading this checkout's images into the new cluster..."
  (cd "$PROJECT_ROOT" && make e2e-images >/dev/null)
  kind load docker-image bloodraven:playground bloodraven-sidecar:playground bloodraven-sidecar:playground-next \
    bloodraven-counter:playground bloodraven-dashboard:playground bloodraven-dns-webhook:playground --name "$NEW_CLUSTER" >/dev/null

  info "Recreating the Secrets the restore reads (U-4): the backup passphrase. The S3 credentials come with the playground."
  new kubectl apply -f "$PROJECT_ROOT/playground/manifests/namespace.yaml"
  old kubectl -n "$NS" get secret "$PASSPHRASE_SECRET" -o jsonpath='{.data.passphrase}' | base64 -d >"$STATE/passphrase"
  new kubectl -n "$NS" create secret generic "$PASSPHRASE_SECRET" --from-file=passphrase="$STATE/passphrase" --dry-run=client -o yaml | new kubectl apply -f -
  rm -f "$STATE/passphrase"

  new_group_manifest >"$STATE/new-group.yaml"
  info "Validating the new manifest with a strict server-side dry run happens when it is applied; its additions:"
  yq '.spec | pick(["initFromBackup", "backup"])' "$STATE/new-group.yaml"

  info "Installing this build and creating the group from the backup..."
  (cd "$PROJECT_ROOT" && KUBECONFIG="$NEW_KUBECONFIG" SKIP_IMAGE_BUILD=1 BLOODRAVEN_SETUP_FAILOVERGROUP_MANIFEST="$STATE/new-group.yaml" ./playground/setup.sh)

  info "Waiting for the bootstrap restore..."
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=jsonpath='{.status.restore.phase}'=Succeeded --timeout=1200s \
    || { new kubectl -n "$NS" get mysqlfailovergroup "$GROUP" -o yaml | yq '.status.restore, .status.conditions'; fail "the bootstrap restore did not succeed"; }
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=condition=Ready --timeout=900s
  ok "The new group is restored and Ready"
}

step_tenant() {
  local site endpoint dump
  endpoint=$(shared_endpoint)
  dump=$(cat "$STATE/dump.prefix")
  site=$(active_site new)
  [[ -z "$(sql new "$site" "SHOW DATABASES LIKE '$TENANT_SCHEMA'")" ]] || fail "$TENANT_SCHEMA was loaded by the bootstrap restore although it was excluded"

  info "Creating the tenant database object: it creates the schema and its owner (U-5)..."
  new kubectl -n "$NS" create secret generic acme-mysql-owner \
    --from-literal=username=acme_owner --from-literal=password=acme-owner-password --dry-run=client -o yaml | new kubectl apply -f -
  new kubectl apply --server-side --validate=strict -f - <<EOF
apiVersion: shipstream.io/v1beta1
kind: MysqlDatabase
metadata:
  name: tenant-acme
  namespace: $NS
spec:
  failoverGroupRef:
    name: $GROUP
  databaseName: $TENANT_SCHEMA
  owner:
    secretName: acme-mysql-owner
    privileges: [ALL PRIVILEGES]
EOF
  new kubectl -n "$NS" wait mysqldatabase/tenant-acme --for=condition=Ready --timeout=300s

  info "Loading $TENANT_SCHEMA from the backup with a per-schema in-place restore..."
  local confirm
  confirm=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  new kubectl -n "$NS" patch mysqlfailovergroup "$GROUP" --type merge -p "{
    \"spec\": {\"restoreInPlace\": {
      \"confirm\": \"$confirm\",
      \"source\": {\"s3\": {\"bucket\": \"$BUCKET\", \"prefix\": \"$dump\", \"region\": \"us-east-1\",
                         \"endpointURL\": \"$endpoint\", \"credentialsSecretName\": \"dragonfly-s3-credentials\"}},
      \"decryption\": {\"passphraseSecretKeyRef\": {\"name\": \"$PASSPHRASE_SECRET\"}},
      \"loadOptions\": {\"includeSchemas\": [\"$TENANT_SCHEMA\"]}
    }}}"
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=jsonpath='{.status.restoreInPlace.phase}'=Succeeded --timeout=1200s \
    || { new kubectl -n "$NS" get mysqlfailovergroup "$GROUP" -o yaml | yq '.status.restoreInPlace'; fail "the tenant restore did not succeed"; }
  new kubectl -n "$NS" patch mysqlfailovergroup "$GROUP" --type json -p '[{"op": "remove", "path": "/spec/restoreInPlace"}]'
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=condition=Ready --timeout=900s
  new kubectl -n "$NS" wait mysqldatabase/tenant-acme --for=condition=Ready --timeout=300s
  ok "$TENANT_SCHEMA is loaded"
}

step_accounts() {
  local site
  site=$(active_site new)
  [[ "$(sql new "$site" "SELECT COUNT(*) FROM mysql.user WHERE user = '$HAND_MADE_USER'")" == "0" ]] \
    || fail "$HAND_MADE_USER exists on the new group before it was recreated: restores are documented not to load users"
  info "Recreating the account that neither the operator nor a MysqlDatabase creates (U-6)..."
  sql new "$site" "
    CREATE USER '$HAND_MADE_USER'@'%' IDENTIFIED BY '$HAND_MADE_PASSWORD';
    GRANT SELECT ON $APP_SCHEMA.* TO '$HAND_MADE_USER'@'%';"
  ok "$HAND_MADE_USER recreated"
}

step_baseline() {
  info "Taking the new group's baseline backup (U-3)..."
  new kubectl apply --server-side --validate=strict -f - <<EOF
apiVersion: shipstream.io/v1beta1
kind: MysqlBackup
metadata:
  name: baseline
  namespace: $NS
spec:
  failoverGroupRef:
    name: $GROUP
  profileName: s3-new
EOF
  new kubectl -n "$NS" wait mysqlbackup/baseline --for=jsonpath='{.status.phase}'=Succeeded --timeout=900s \
    || { new kubectl -n "$NS" get mysqlbackup baseline -o yaml | yq '.status'; fail "the baseline backup did not succeed"; }
  info "Waiting for the PITR archive on the new prefix..."
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=condition=PITRArchiveReady --timeout=900s \
    || { new kubectl -n "$NS" get mysqlfailovergroup "$GROUP" -o yaml | yq '.status.conditions'; fail "PITRArchiveReady is not True"; }
  info "Verifying the baseline backup..."
  new kubectl apply --server-side --validate=strict -f - <<EOF
apiVersion: shipstream.io/v1beta1
kind: MysqlBackupVerification
metadata:
  name: baseline
  namespace: $NS
spec:
  failoverGroupRef:
    name: $GROUP
  profileName: s3-new
  backupRef:
    name: baseline
EOF
  new kubectl -n "$NS" wait mysqlbackupverification/baseline --for=jsonpath='{.status.phase}'=Succeeded --timeout=1200s \
    || { new kubectl -n "$NS" get mysqlbackupverification baseline -o yaml | yq '.status'; fail "the baseline verification did not succeed"; }
  ok "Baseline taken and verified; the PITR archive is ready"
}

step_verify() {
  local site failed=0
  site=$(active_site new)
  info "Group and tenant database (U-8)..."
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=condition=Ready --timeout=300s
  [[ "$(new kubectl -n "$NS" get mysqlfailovergroup "$GROUP" -o jsonpath='{.status.conditions[?(@.type=="Degraded")].status}')" == "False" ]] \
    || { new kubectl -n "$NS" get mysqlfailovergroup "$GROUP" -o yaml | yq '.status.conditions'; fail "the new group is Degraded"; }
  new kubectl -n "$NS" wait mysqldatabase/tenant-acme --for=condition=Ready --timeout=120s
  new kubectl -n "$NS" wait mysqlfailovergroup "$GROUP" --for=condition=PITRArchiveReady --timeout=120s
  new kubectl get crd -o custom-columns='NAME:.metadata.name,STORED:.status.storedVersions' | grep shipstream.io

  info "Data: every table against the frozen source..."
  fingerprint new >"$STATE/fingerprint.new"
  if diff "$STATE/fingerprint.old" "$STATE/fingerprint.new"; then
    ok "$(wc -l <"$STATE/fingerprint.new") tables have the row count and checksum they had on v$FROM"
  else
    failed=1
    echo "the new group's data differs from the frozen source (left: v$FROM, right: new)" >&2
  fi

  info "The account made by hand..."
  local rows
  rows=$(new kubectl -n "$NS" exec "deploy/mysql-$GROUP-$site" -c mysql -- \
    env MYSQL_PWD="$HAND_MADE_PASSWORD" mysql -h127.0.0.1 -u"$HAND_MADE_USER" -N -B -e "SELECT COUNT(*) FROM $APP_SCHEMA.items")
  if [[ "$rows" == "500" ]]; then ok "$HAND_MADE_USER logs in and reads $APP_SCHEMA.items"; else failed=1; echo "$HAND_MADE_USER read '$rows' rows, want 500" >&2; fi

  info "The old prefix..."
  list_prefix "old" >"$STATE/old-prefix.after"
  if diff "$STATE/old-prefix.before" "$STATE/old-prefix.after" >/dev/null; then
    ok "No object under s3://$BUCKET/old/ changed ($(wc -l <"$STATE/old-prefix.after") objects)"
  else
    failed=1
    diff "$STATE/old-prefix.before" "$STATE/old-prefix.after" | head -20 >&2
    echo "objects under the old group's prefix changed" >&2
  fi
  local new_objects
  new_objects=$(list_prefix "new" | wc -l)
  [[ "$new_objects" -gt 0 ]] || { failed=1; echo "nothing was written under s3://$BUCKET/new/" >&2; }
  ok "$new_objects objects under s3://$BUCKET/new/ (the new group's backups and PITR archive)"

  [[ "$failed" == "0" ]] || fail "the move off v$FROM did not verify"
  ok "The move off v$FROM is verified"
}

step_teardown() {
  kind delete cluster --name "$OLD_CLUSTER" 2>/dev/null || true
  kind delete cluster --name "$NEW_CLUSTER" 2>/dev/null || true
  if [[ -d "$OLD_SRC" ]]; then
    git -C "$PROJECT_ROOT" worktree remove --force "$OLD_SRC" 2>/dev/null || true
  fi
  rm -rf "$STATE"
  ok "Clusters and state removed"
}

case "${1:-}" in
  run)
    require_tools
    step_old; step_seed; step_backup; step_new; step_tenant; step_accounts; step_baseline; step_verify
    ;;
  old|seed|backup|new|tenant|accounts|baseline|verify)
    require_tools
    "step_$1"
    ;;
  teardown) step_teardown ;;
  *) sed -n '2,46p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
