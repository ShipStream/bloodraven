#!/usr/bin/env bash
# brprep — day-0 pre-flight for a MysqlFailoverGroup manifest.
#
#   ./brprep.sh <manifest.json> [--target-image <tag>]
#
# Reads a MysqlFailoverGroup as JSON (yq -o json < manifest.yaml, or kubectl
# get -o json) and reports three things:
#
#   REJECT <rule>   the API server will refuse this object — a CEL rule on the
#                   CRD, reproduced here so you find out before you apply
#   WARN <finding>  admission will happily accept this, and it will hurt later
#   the change plan for an ordered update to --target-image
#
# Exit codes:  0 clean or warnings only · 1 one or more REJECTs · 2 bad input
#
# Standard tools only: bash and jq. No cluster, no kubectl, no network.
set -euo pipefail

# ---------------------------------------------------------------- input

MANIFEST=""
TARGET_IMAGE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --target-image) TARGET_IMAGE="${2:-}"; shift 2 ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    -*) echo "brprep: unknown flag $1" >&2; exit 2 ;;
    *) MANIFEST="$1"; shift ;;
  esac
done

[ -n "$MANIFEST" ] || { echo "usage: brprep.sh <manifest.json> [--target-image <tag>]" >&2; exit 2; }
[ -r "$MANIFEST" ] || { echo "brprep: cannot read $MANIFEST" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "brprep: jq is required" >&2; exit 2; }

jq -e '.kind == "MysqlFailoverGroup"' "$MANIFEST" >/dev/null 2>&1 \
  || { echo "brprep: $MANIFEST is not a MysqlFailoverGroup" >&2; exit 2; }

# q <jq-filter> — read one value out of the manifest.
q() { jq -r "$1" "$MANIFEST"; }

# ---------------------------------------------------------------- scaffolding
#
# Everything below this line up to TODO A is written for you.

# secs <duration> — Go duration ("20s", "5m", "1h30m") to whole seconds.
# Prints 0 for an empty or unparsable value, which is never a valid setting
# and therefore always reads as "violates the minimum".
secs() {
  local d="${1:-}" total=0 n unit
  [ -n "$d" ] || { echo 0; return; }
  while [[ "$d" =~ ^([0-9]+)(h|m|s|ms)(.*)$ ]]; do
    n="${BASH_REMATCH[1]}"; unit="${BASH_REMATCH[2]}"; d="${BASH_REMATCH[3]}"
    case "$unit" in
      h) total=$(( total + n * 3600 )) ;;
      m) total=$(( total + n * 60 )) ;;
      s) total=$(( total + n )) ;;
      ms) : ;;   # sub-second precision is never load-bearing for these fields
    esac
  done
  echo "$total"
}

# The nine admission rules, in the fixed order brprep reports them. Most are
# CEL rules on the CRD; the site-count one is a MinItems/MaxItems constraint on
# the schema, the site-names one is the name key of the spec.sites list map, and
# the credentials one is a required field plus kubectl's strict field
# validation. All are refusals at `kubectl apply`, which is all brprep claims.
# The grader matches these tokens exactly, so do not reword them.
RULE_CREDENTIALS="spec.credentials.operatorSecretName is required and spec.secretName is not a field"
RULE_SITE_COUNT="spec.sites must contain between 2 and 16 entries"
RULE_SITE_NAMES="spec.sites[].name must be unique"
RULE_TWO_CANDIDATES="spec.sites must contain at least two sites with role PrimaryCandidate"
RULE_PRIORITIES="splitBrainPolicy.sitePriorities entries must match the names of sites with role PrimaryCandidate"
RULE_SITE_FIELDS="taintNodeSelector and lbIP are required unless role is ReadOnly"
RULE_PEER_INTERVAL="sidecar.peerCheckInterval must be between 1s and 1m"
RULE_LEASE_RANGE="sidecar.leaseTimeout must be between 3s and 10m"
RULE_LEASE_RATIO="sidecar.leaseTimeout must be at least 3x sidecar.peerCheckInterval"

# The six silent findings, in the fixed order brprep reports them.
WARN_FLOATING_TAG="spec.image is a floating tag; pin an immutable one or a restart can drift you onto an unsupported MySQL"
WARN_PVC_BACKUP="backup profile %s uses storage.type PVC; a backup sharing a failure domain with the data is an assumption, not a backup"
WARN_QOS="site %s sets resources.requests != resources.limits; without Guaranteed QoS the kubelet may evict this MySQL first"
WARN_READER_GATE="replication.readOnlyMaxLagSeconds (%s) is above maxLagSeconds (%s); the reader endpoint is now looser than the group threshold"
WARN_UNVERIFIED_ENCRYPTION="encryptionAtRest is enabled but no backup profile is configured; an encrypted group with no verified restore path is one keyring away from unrecoverable"
WARN_RECREATE="updateStrategy.type Recreate patches every site Deployment in one pass; both sites can restart at once"

# ---------------------------------------------------------------- TODO A
#
# check_admission — print one `REJECT <rule>` line per CEL rule this manifest
# violates, in the order the RULE_* constants are declared above. A manifest
# that violates none prints nothing at all.
#
# The nine rules, and what each one actually checks:
#
#   RULE_CREDENTIALS    .spec.credentials.operatorSecretName is present and
#                       non-empty, and .spec.secretName is absent. A missing
#                       credentials block is a required-field error; the
#                       single-Secret secretName mode was removed in v1.7.0,
#                       so kubectl's strict field validation refuses it as
#                       an unknown field.
#   RULE_SITE_COUNT     2 <= (.spec.sites | length) <= 16.
#   RULE_SITE_NAMES     every .spec.sites[].name is distinct.
#   RULE_TWO_CANDIDATES at least two sites have role "PrimaryCandidate".
#                       Remember the CRD default: an omitted role *is*
#                       PrimaryCandidate, so count a missing role as one.
#   RULE_PRIORITIES     every entry of .spec.splitBrainPolicy.sitePriorities
#                       names a site whose effective role is
#                       PrimaryCandidate. An absent list is fine.
#   RULE_SITE_FIELDS    every site whose effective role is NOT "ReadOnly" has
#                       both .lbIP and .taintNodeSelector. A ReadOnly site
#                       needs neither — it is never promoted and never tainted.
#                       This is the one the fixtures try hardest to break.
#   RULE_PEER_INTERVAL  1 <= secs(.spec.sidecar.peerCheckInterval) <= 60,
#                       when set.
#   RULE_LEASE_RANGE    3 <= secs(.spec.sidecar.leaseTimeout) <= 600, when set.
#   RULE_LEASE_RATIO    secs(leaseTimeout) >= 3 * secs(peerCheckInterval),
#                       when both are set.
#
# Return 0 always; the caller counts the lines.
check_admission() {
  : # TODO A — replace this
}

# ---------------------------------------------------------------- TODO B
#
# check_silent — print one `WARN <finding>` line per day-0 mistake that
# admission accepts. Use the WARN_* templates above verbatim, filling the
# printf placeholders. Order is the order they are declared.
#
#   WARN_FLOATING_TAG          .spec.image has no tag at all, or a tag of
#                              "latest", or a tag with no dot in it
#                              ("mysql:9" floats, "mysql:9.7" does not).
#   WARN_PVC_BACKUP            any .spec.backup.profiles[] with
#                              .storage.type == "PVC". One line per profile,
#                              naming it.
#   WARN_QOS                   any site where requests.cpu != limits.cpu or
#                              requests.memory != limits.memory. Compare the
#                              strings; a site that omits resources entirely
#                              is not a finding here.
#   WARN_READER_GATE           .spec.replication.readOnlyMaxLagSeconds is set
#                              and strictly greater than maxLagSeconds.
#   WARN_UNVERIFIED_ENCRYPTION .spec.encryptionAtRest.enabled is true and
#                              .spec.backup.profiles is absent or empty.
#   WARN_RECREATE              .spec.updateStrategy.type == "Recreate".
#
# These are findings, never errors: they must not change the exit code.
check_silent() {
  : # TODO B — replace this
}

# ---------------------------------------------------------------- TODO C
#
# plan_upgrade — given $TARGET_IMAGE, print the ordered-update plan.
#
# When $TARGET_IMAGE is empty, print nothing. When it equals .spec.image,
# print exactly:
#
#     no change
#
# Otherwise print, in this order and one per line:
#
#     plan: mysql:9.7 -> mysql:9.8
#     1. upgrade standby <site> first (a replica may run a newer MySQL than
#        its source; a source may not run a newer MySQL than its replica)
#     2. promote <site> through the ordinary nine-step sequence
#     3. upgrade the former active <site>, now a standby
#     active site after this rollout: <site>
#     expect: bloodraven_failovers_total increments
#     expect: lastFailover is stamped and consumes the anti-flap cooldown
#     expect: the DNSEndpoint A record flips
#
# The standby is the *second* site whose effective role is PrimaryCandidate,
# in `spec.sites` order; the former active is the first. This manifest carries
# no status, so declaration order is the only signal available — say so in
# your own runbook, and read `status.activeSite` when you have a live group.
plan_upgrade() {
  : # TODO C — replace this
}

# ---------------------------------------------------------------- report

echo "brprep: $(q '.metadata.namespace // "default"')/$(q '.metadata.name')"

rejects="$(check_admission || true)"
warns="$(check_silent || true)"

[ -n "$rejects" ] && printf '%s\n' "$rejects"
[ -n "$warns" ] && printf '%s\n' "$warns"

plan="$(plan_upgrade || true)"
[ -n "$plan" ] && printf '%s\n' "$plan"

if [ -n "$rejects" ]; then
  echo "verdict: NOT APPLYABLE ($(printf '%s\n' "$rejects" | wc -l | tr -d ' ') rejection(s))"
  exit 1
fi
echo "verdict: APPLYABLE ($( [ -n "$warns" ] && printf '%s\n' "$warns" | wc -l | tr -d ' ' || echo 0 ) finding(s))"
exit 0
