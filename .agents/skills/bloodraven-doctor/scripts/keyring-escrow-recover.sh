#!/usr/bin/env bash
#
# bloodraven-doctor: recreate a sealed site's keyring escrow Secret from the
# keyring still mounted in its running pod (KeyringEscrowMissing), or, with a
# separate explicit confirmation, replace an escrow Secret that no longer
# hashes to the recorded digest (KeyringEscrowCorrupt).
#
# Safety rules:
#   - The live keyring is read and checked against the digest recorded in
#     status BEFORE anything is written. A mismatch writes nothing.
#   - An existing Secret is never deleted or overwritten unless
#     --replace-corrupt names that exact Secret, its content really fails
#     the digest check, and a backup of it has been written first.
#   - A Secret this script just created is never deleted automatically.
#   - It never restarts, evicts or rolls the pod: that pod holds the only
#     surviving copy of the keys.
#
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: keyring-escrow-recover.sh -n <namespace> <mysql-failover-group> <site> [--replace-corrupt <secret-name>]

  Without --replace-corrupt: recreate the site's missing escrow Secret from
  the running pod's keyring. Refuses if the Secret exists.

  --replace-corrupt <secret-name>: the Secret exists but no longer hashes to
  status.encryptionAtRest.sites[].keyringDigest. Back it up to
  ./keyring-escrow-backup-<secret-name>-<timestamp>.yaml, delete it and
  recreate it from the running pod. <secret-name> must repeat the exact
  Secret name as confirmation.
EOF
    exit 64
}

NAMESPACE=""
GROUP=""
SITE=""
REPLACE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace) NAMESPACE="${2:-}"; shift 2 ;;
        --replace-corrupt) REPLACE="${2:-}"; [[ -n "$REPLACE" ]] || usage; shift 2 ;;
        -h|--help) usage ;;
        -*) usage ;;
        *)
            if [[ -z "$GROUP" ]]; then GROUP="$1"
            elif [[ -z "$SITE" ]]; then SITE="$1"
            else usage
            fi
            shift ;;
    esac
done
[[ -n "$NAMESPACE" && -n "$GROUP" && -n "$SITE" ]] || usage

die() { echo "keyring-escrow-recover: $*" >&2; exit 1; }

fg_jsonpath() {
    kubectl get mysqlfailovergroup "$GROUP" -n "$NAMESPACE" -o "jsonpath=$1"
}
site_field() {
    fg_jsonpath "{.status.encryptionAtRest.sites[?(@.name==\"$SITE\")].$1}"
}
secret_digest() {
    local data
    data=$(kubectl get secret "$1" -n "$NAMESPACE" -o 'jsonpath={.data.keyring}') || return 1
    echo "sha256:$(printf '%s' "$data" | base64 -d | sha256sum | cut -d' ' -f1)"
}

SECRET=$(site_field keyringSecret) || die "cannot read the failover group"
VERSION=$(site_field keyringVersion)
DIGEST=$(site_field keyringDigest)
KEYRING_DIR=$(fg_jsonpath '{.spec.encryptionAtRest.keyring.dataFileDir}')
KEYRING_DIR=${KEYRING_DIR:-/run/mysql-keyring}
FG_UID=$(fg_jsonpath '{.metadata.uid}')
[[ -n "$SECRET" && -n "$DIGEST" && -n "$FG_UID" ]] || die "site $SITE has no keyringSecret/keyringDigest in status"

POD=$(kubectl get pod -n "$NAMESPACE" \
    -l "shipstream.io/failover-group=$GROUP,shipstream.io/site=$SITE" \
    -o 'jsonpath={.items[0].metadata.name}') || die "cannot list the site's pod"
[[ -n "$POD" ]] || die "no running pod for site $SITE: its keyring is gone; follow the reclone step of the runbook"

# 1. Read and verify the live keyring. Nothing is written before this passes.
umask 077
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
kubectl exec -n "$NAMESPACE" "$POD" -c mysql -- cat "$KEYRING_DIR/keyring" >"$TMP" \
    || die "could not read $KEYRING_DIR/keyring from $POD; nothing was changed"
[[ -s "$TMP" ]] || die "the keyring read from $POD is empty; nothing was changed"
LIVE="sha256:$(sha256sum "$TMP" | cut -d' ' -f1)"
[[ "$LIVE" == "$DIGEST" ]] \
    || die "the running keyring ($LIVE) does not match status ($DIGEST); nothing was changed"

# 2. An existing Secret is only replaced on explicit, separate confirmation.
if kubectl get secret "$SECRET" -n "$NAMESPACE" -o name >/dev/null 2>&1; then
    EXISTING=$(secret_digest "$SECRET") || die "cannot read existing Secret $SECRET; nothing was changed"
    if [[ "$EXISTING" == "$DIGEST" ]]; then
        echo "escrow Secret $SECRET exists and matches the recorded digest; nothing to do"
        exit 0
    fi
    if [[ -z "$REPLACE" ]]; then
        die "Secret $SECRET exists but does not match the recorded digest (KeyringEscrowCorrupt). Nothing was changed. To back it up and replace it from the running pod, re-run with: --replace-corrupt $SECRET"
    fi
    [[ "$REPLACE" == "$SECRET" ]] \
        || die "--replace-corrupt names '$REPLACE', but the site's escrow Secret is '$SECRET'; nothing was changed"
    BACKUP="./keyring-escrow-backup-$SECRET-$(date -u +%Y%m%dT%H%M%SZ).yaml"
    kubectl get secret "$SECRET" -n "$NAMESPACE" -o yaml >"$BACKUP" \
        || die "could not back up $SECRET; nothing was changed"
    [[ -s "$BACKUP" ]] || die "backup of $SECRET is empty; nothing was changed"
    echo "backed up $SECRET to $BACKUP"
    kubectl delete secret "$SECRET" -n "$NAMESPACE" \
        || die "could not delete $SECRET; its backup is $BACKUP"
elif [[ -n "$REPLACE" ]]; then
    die "--replace-corrupt given, but Secret $SECRET does not exist; re-run without it to recreate it"
fi

# 3. Recreate from the verified keyring.
kubectl create secret generic "$SECRET" -n "$NAMESPACE" --from-file="keyring=$TMP" \
    || die "could not create $SECRET; the running pod still holds the keyring, do not restart it"
CREATED=$(secret_digest "$SECRET") || die "created $SECRET but cannot read it back; check it by hand, do not delete it blindly"
[[ "$CREATED" == "$DIGEST" ]] \
    || die "created $SECRET but it reads back as $CREATED, not $DIGEST; check it by hand, do not delete it blindly"

kubectl label secret "$SECRET" -n "$NAMESPACE" \
    app.kubernetes.io/name=mysql-keyring \
    app.kubernetes.io/managed-by=bloodraven \
    "shipstream.io/failover-group=$GROUP" "shipstream.io/site=$SITE" \
    "shipstream.io/keyring-version=$VERSION"
kubectl annotate secret "$SECRET" -n "$NAMESPACE" "shipstream.io/keyring-digest=$DIGEST"
kubectl patch secret "$SECRET" -n "$NAMESPACE" --type=merge -p="{
  \"metadata\":{\"ownerReferences\":[{
    \"apiVersion\":\"shipstream.io/v1alpha1\",\"kind\":\"MysqlFailoverGroup\",
    \"name\":\"$GROUP\",\"uid\":\"$FG_UID\",\"controller\":true,\"blockOwnerDeletion\":true
  }]},\"immutable\":true
}"

# 4. Return the site to Sealed (the JSON patch test guards the index).
read -r -a NAMES <<<"$(fg_jsonpath '{.status.encryptionAtRest.sites[*].name}')"
INDEX=-1
for i in "${!NAMES[@]}"; do
    if [[ "${NAMES[$i]}" == "$SITE" ]]; then INDEX=$i; fi
done
[[ "$INDEX" -ge 0 ]] || die "site $SITE not found in status; set its phase to Sealed by hand"
kubectl patch mysqlfailovergroup "$GROUP" -n "$NAMESPACE" --subresource=status --type=json -p="[
  {\"op\":\"test\",\"path\":\"/status/encryptionAtRest/sites/$INDEX/name\",\"value\":\"$SITE\"},
  {\"op\":\"replace\",\"path\":\"/status/encryptionAtRest/sites/$INDEX/phase\",\"value\":\"Sealed\"}
]"
echo "escrow Secret $SECRET recreated from $POD and site $SITE set back to Sealed; verify it stays Sealed"
