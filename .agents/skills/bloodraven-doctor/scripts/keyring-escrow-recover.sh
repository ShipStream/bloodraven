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
#   - It runs only on a site in a sealed-recovery state (Sealed, or Failed
#     with no unseal reason) and re-reads the group before every write,
#     stopping if the group's UID or the site's phase, unseal reason,
#     Secret, version or digest changed: a rotation or reseal started
#     meanwhile owns the site.
#   - It never writes the failover group's status. The operator returns the
#     site to Sealed itself once the Secret hashes to the recorded digest
#     and MySQL confirms it runs that keyring (Event KeyringRecovered).
#
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: keyring-escrow-recover.sh -n <namespace> <mysql-failover-group> <site> [--pod <pod>] [--replace-corrupt <secret-name>]

  The site's MySQL pod is the one Running, not terminating, with a Ready
  "mysql" container, selected exactly as its Deployment selects it. If there
  is not exactly one, the script refuses; --pod <pod> names it explicitly.
  The named pod must be a Running MySQL pod of that site; it may be
  terminating (with Recreate, a pod in its grace period can hold the last
  copy of the keyring), which is reported as a warning. The digest check
  still runs before anything is written.

  Without --replace-corrupt: recreate the site's missing escrow Secret from
  the running pod's keyring. Refuses if the Secret exists.

  --replace-corrupt <secret-name>: the Secret exists but no longer hashes to
  status.encryptionAtRest.sites[].keyringDigest. Back it up to
  ./keyring-escrow-backup-<secret-name>-<timestamp>.yaml, delete it and
  recreate it from the running pod. <secret-name> must repeat the exact
  Secret name as confirmation.

  Afterwards it waits up to KEYRING_RECOVER_WAIT seconds (default 120) for
  the operator to report the site Sealed again.
EOF
    exit 64
}

NAMESPACE=""
GROUP=""
SITE=""
REPLACE=""
POD=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace) NAMESPACE="${2:-}"; shift 2 ;;
        --pod) POD="${2:-}"; [[ -n "$POD" ]] || usage; shift 2 ;;
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

# Portable digest and base64 decode (GNU coreutils, macOS and BSD).
if command -v sha256sum >/dev/null 2>&1; then
    sha256_of() { sha256sum "$@" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
    sha256_of() { shasum -a 256 "$@" | cut -d' ' -f1; }
else
    die "neither sha256sum nor shasum is installed"
fi
B64_DECODE=""
for flag in --decode -d -D; do
    if [[ "$(printf 'YQ==' | base64 "$flag" 2>/dev/null)" == "a" ]]; then B64_DECODE=$flag; break; fi
done
[[ -n "$B64_DECODE" ]] || die "this base64 cannot decode (tried --decode, -d, -D)"
b64_decode() { base64 "$B64_DECODE"; }

fg_jsonpath() {
    kubectl get mysqlfailovergroup "$GROUP" -n "$NAMESPACE" -o "jsonpath=$1"
}
# One read of everything the recovery depends on, so it cannot mix two
# versions of the object: uid|phase|unsealReason|keyringSecret|keyringVersion|keyringDigest.
site_state() {
    local s="{.status.encryptionAtRest.sites[?(@.name==\"$SITE\")]"
    fg_jsonpath "{.metadata.uid}|${s}.phase}|${s}.unsealReason}|${s}.keyringSecret}|${s}.keyringVersion}|${s}.keyringDigest}"
}
# Stop before a write if the group or the site moved since SNAPSHOT was read.
require_unchanged() {
    local now
    now=$(site_state) || die "cannot re-read the failover group before $1; nothing further was changed"
    [[ "$now" == "$SNAPSHOT" ]] \
        || die "the failover group changed since this script read it (was $SNAPSHOT, now $now); not going on to $1. The operator owns the site now; re-run once it is Failed again"
}
secret_digest() {
    local data
    data=$(kubectl get secret "$1" -n "$NAMESPACE" -o 'jsonpath={.data.keyring}') || return 1
    echo "sha256:$(printf '%s' "$data" | b64_decode | sha256_of)"
}

SNAPSHOT=$(site_state) || die "cannot read the failover group"
IFS='|' read -r FG_UID PHASE UNSEAL_REASON SECRET VERSION DIGEST <<<"$SNAPSHOT"
KEYRING_DIR=$(fg_jsonpath '{.spec.encryptionAtRest.keyring.dataFileDir}')
KEYRING_DIR=${KEYRING_DIR:-/run/mysql-keyring}
[[ -n "$SECRET" && -n "$DIGEST" && -n "$FG_UID" ]] || die "site $SITE has no keyringSecret/keyringDigest in status"
# Only a site that renders sealed against SECRET can be recovered from its
# pod: Sealed (the operator has not noticed yet), or Failed with no unseal
# reason (KeyringEscrowMissing / KeyringEscrowCorrupt). Anything else is a
# lifecycle in progress (rotation, bootstrap, clone) that owns the Secret.
if [[ "$PHASE" != "Sealed" && ! ( "$PHASE" == "Failed" && -z "$UNSEAL_REASON" ) ]]; then
    die "site $SITE is ${PHASE:-<none>}${UNSEAL_REASON:+ ($UNSEAL_REASON)}, not a sealed site whose escrow was lost; nothing was changed"
fi

# The site's MySQL pod, selected as its Deployment selects it (Dragonfly and
# backup Job pods carry the site label too). One line per Running pod:
# name, deletionTimestamp, the mysql container's ready flag.
SELECTOR="app.kubernetes.io/name=mysql,app.kubernetes.io/instance=$GROUP,shipstream.io/site=$SITE"
PODS=$(kubectl get pod -n "$NAMESPACE" -l "$SELECTOR" --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.deletionTimestamp}{"\t"}{.status.containerStatuses[?(@.name=="mysql")].ready}{"\n"}{end}') \
    || die "cannot list the site's pods"
if [[ -n "$POD" ]]; then
    DELETING=$(printf '%s\n' "$PODS" | awk -F'\t' -v p="$POD" '$1 == p {print ($2 == "" ? "no" : "yes"); found=1} END {exit !found}') \
        || die "--pod $POD is not a Running MySQL pod of site $SITE"
    if [[ "$DELETING" == "yes" ]]; then
        echo "keyring-escrow-recover: WARNING: $POD is terminating; reading its keyring before its grace period ends. It may be the last copy." >&2
    fi
else
    READY=$(printf '%s\n' "$PODS" | awk -F'\t' 'NF && $2 == "" && $3 == "true" {print $1}')
    COUNT=$(printf '%s\n' "$READY" | grep -c . || true)
    if [[ "$COUNT" -ne 1 ]]; then
        echo "keyring-escrow-recover: site $SITE has $COUNT Ready MySQL pods; Running pods (name, deleting, mysql ready):" >&2
        printf '%s\n' "$PODS" >&2
        die "refusing to guess. If one of them still holds the keyring, re-run now with --pod <name>; a terminating pod is accepted that way and must be read before its grace period ends. If none runs, the keyring is gone: follow the reclone step of the runbook"
    fi
    POD=$READY
fi

# 1. Read and verify the live keyring. Nothing is written before this passes.
umask 077
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
kubectl exec -n "$NAMESPACE" "$POD" -c mysql -- cat "$KEYRING_DIR/keyring" >"$TMP" \
    || die "could not read $KEYRING_DIR/keyring from $POD; nothing was changed"
[[ -s "$TMP" ]] || die "the keyring read from $POD is empty; nothing was changed"
LIVE="sha256:$(sha256_of "$TMP")"
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
    # Delete and recreate are one step: checked once, here.
    require_unchanged "replace $SECRET"
    CHECKED=1
    kubectl delete secret "$SECRET" -n "$NAMESPACE" \
        || die "could not delete $SECRET; its backup is $BACKUP"
elif [[ -n "$REPLACE" ]]; then
    die "--replace-corrupt given, but Secret $SECRET does not exist; re-run without it to recreate it"
fi

# 3. Recreate from the verified keyring.
[[ -n "${CHECKED:-}" ]] || require_unchanged "create $SECRET"
kubectl create secret generic "$SECRET" -n "$NAMESPACE" --from-file="keyring=$TMP" \
    || die "could not create $SECRET; the running pod still holds the keyring, do not restart it"
CREATED=$(secret_digest "$SECRET") || die "created $SECRET but cannot read it back; check it by hand, do not delete it blindly"
[[ "$CREATED" == "$DIGEST" ]] \
    || die "created $SECRET but it reads back as $CREATED, not $DIGEST; check it by hand, do not delete it blindly"

kubectl label secret "$SECRET" -n "$NAMESPACE" \
    app.kubernetes.io/name=mysql-keyring \
    app.kubernetes.io/managed-by=bloodraven \
    "shipstream.io/failover-group=$GROUP" "shipstream.io/site=$SITE" \
    "shipstream.io/keyring-version=$VERSION" \
    || die "created $SECRET (digest verified) but could not label it; label it by hand as above, do not delete it"
kubectl annotate secret "$SECRET" -n "$NAMESPACE" "shipstream.io/keyring-digest=$DIGEST" \
    || die "created $SECRET (digest verified) but could not annotate it; add shipstream.io/keyring-digest=$DIGEST by hand, do not delete it"
kubectl patch secret "$SECRET" -n "$NAMESPACE" --type=merge -p="{
  \"metadata\":{\"ownerReferences\":[{
    \"apiVersion\":\"shipstream.io/v1alpha1\",\"kind\":\"MysqlFailoverGroup\",
    \"name\":\"$GROUP\",\"uid\":\"$FG_UID\",\"controller\":true,\"blockOwnerDeletion\":true
  }]},\"immutable\":true
}" || die "created $SECRET (digest verified) but could not set its owner reference and immutability; patch it by hand, do not delete it"

# 4. The operator, not this script, returns the site to Sealed: it re-checks
# the Secret against the digest and needs MySQL to confirm the same keyring.
# Writing status here could overwrite a rotation started meanwhile.
WAIT=${KEYRING_RECOVER_WAIT:-120}
deadline=$((SECONDS + WAIT))
while :; do
    NOW=$(site_state) || NOW=""
    IFS='|' read -r NOW_UID NOW_PHASE NOW_REASON NOW_SECRET _ _ <<<"$NOW"
    if [[ "$NOW_UID" == "$FG_UID" && "$NOW_PHASE" == "Sealed" && -z "$NOW_REASON" && "$NOW_SECRET" == "$SECRET" ]]; then
        echo "escrow Secret $SECRET recreated from $POD; the operator reports site $SITE Sealed again"
        exit 0
    fi
    if [[ -n "$NOW" && "$NOW" != "$SNAPSHOT" && "$NOW_PHASE" != "Failed" ]]; then
        echo "escrow Secret $SECRET recreated from $POD; site $SITE has since moved to ${NOW_PHASE:-<none>}${NOW_REASON:+ ($NOW_REASON)} and the operator owns it. Nothing else was changed"
        exit 0
    fi
    [[ $SECONDS -lt $deadline ]] || break
    sleep 5
done
echo "escrow Secret $SECRET recreated from $POD. Site $SITE is not Sealed yet: the operator re-seals it once MySQL confirms it runs that keyring (Event KeyringRecovered). Check the site's message; do not restart the pod"
