#!/usr/bin/env bash
# Bloodraven Playground — enable data-at-rest encryption
#
# Turns on spec.encryptionAtRest on a TLS-enabled playground failover
# group. TLS must be present from the initial setup so the encryption
# rollout changes only one restart-sensitive concern at a time.
#
# What it does:
#   1. Verifies the playground started with the expected MySQL TLS
#      Secret and waits for a healthy baseline.
#   2. Enables the operator's keyring escrow TLS listener.
#   3. Patches the MFG with spec.encryptionAtRest.
#   4. Stamps the encryption-adopt annotation, because the playground is
#      already serving and the operator otherwise refuses (existing
#      tablespaces stay plaintext — see the warning below).
#   5. Waits for every site to reach phase=Sealed.
#
# Usage:
#   BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh    # initial setup
#   ./playground/enable-encryption.sh               # convert in place
#   ./playground/enable-encryption.sh --fresh       # wipe MySQL first, so
#                                                   # every tablespace is
#                                                   # encrypted from birth
#   ./playground/enable-encryption.sh --status      # report only
#
# IMPORTANT: without --fresh, tables that already exist stay plaintext.
# MySQL only encrypts what is written after the fact. That is fine for
# exercising the keyring lifecycle, but it is NOT what a production
# adoption looks like — see docs/docs/encryption-at-rest.mdx.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="bloodraven-playground"
FG="playground"
TLS_SECRET="mysql-playground-tls"
ESCROW_TLS_SECRET="bloodraven-escrow-tls"

info() { echo -e "\033[1;34m==>\033[0m $*"; }
ok()   { echo -e "\033[1;32m OK\033[0m $*"; }
warn() { echo -e "\033[1;33m !!\033[0m $*"; }
die()  { echo -e "\033[1;31mERR\033[0m $*" >&2; exit 1; }

wait_for_stable_baseline() {
	local reason="$1"
	local runner="$SCRIPT_DIR/../bin/playground-chaos"
	local deadline healthy_since now output

	if [[ ! -x "$runner" ]]; then
		info "building playground-chaos baseline checker"
		make -C "$SCRIPT_DIR/.." build-playground-chaos
	fi

	info "waiting for a healthy baseline $reason (up to 7 minutes)"
	deadline=$(( $(date +%s) + 420 ))
	healthy_since=0
	while [[ $(date +%s) -lt $deadline ]]; do
		now=$(date +%s)
		if output=$("$runner" check 2>&1); then
			if [[ "$healthy_since" == "0" ]]; then
				healthy_since=$now
			elif (( now - healthy_since >= 20 )); then
				ok "playground baseline remained healthy for 20 seconds"
				return 0
			fi
		else
			healthy_since=0
			warn "$output"
		fi
		sleep 5
	done
	die "playground did not reach a stable healthy baseline $reason"
}

# shellcheck source=playground/_guard.sh
source "$SCRIPT_DIR/_guard.sh"
require_playground_context

FRESH=0
STATUS_ONLY=0
PREPARE_TLS=0
# In CI, prefer a wipe-first enable so every tablespace is encrypted from
# birth and the keyring bootstrap is not racing a multi-site ordered update
# of a live workload. Override with --no-fresh if a job specifically wants
# partial-coverage adoption.
case "${CI:-}" in
1 | true | TRUE | yes | YES) FRESH=1 ;;
esac
for arg in "$@"; do
	case "$arg" in
	--fresh) FRESH=1 ;;
	--no-fresh) FRESH=0 ;;
	--status) STATUS_ONLY=1 ;;
	--prepare-tls) PREPARE_TLS=1 ;;
	-h | --help)
		sed -n '2,29p' "$0"
		exit 0
		;;
	*) die "unknown argument: $arg" ;;
	esac
done

report_status() {
	echo
	info "encryption status"
	kubectl -n "$NAMESPACE" get mysqlfailovergroup "$FG" \
		-o jsonpath='{range .status.encryptionAtRest.sites[*]}{.name}{"\t"}{.phase}{"\t"}{.keyringSecret}{"\t"}{.message}{"\n"}{end}' \
		2>/dev/null || warn "no encryption status yet"
	echo
	info "escrow secrets"
	kubectl -n "$NAMESPACE" get secrets -l app.kubernetes.io/name=mysql-keyring \
		-o custom-columns=NAME:.metadata.name,SITE:.metadata.labels.shipstream\\.io/site,VERSION:.metadata.labels.shipstream\\.io/keyring-version \
		2>/dev/null || warn "no escrow secrets yet"
}

if [[ "$STATUS_ONLY" == "1" ]]; then
	report_status
	exit 0
fi

if [[ "$PREPARE_TLS" != "1" ]]; then
	kubectl -n "$NAMESPACE" get mysqlfailovergroup "$FG" >/dev/null 2>&1 \
		|| die "no MysqlFailoverGroup $FG in $NAMESPACE - run BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh first"
	configured_tls=$(kubectl -n "$NAMESPACE" get mysqlfailovergroup "$FG" \
		-o jsonpath='{.spec.tls.secretName}')
	[[ "$configured_tls" == "$TLS_SECRET" ]] \
		|| die "the playground was not created with MySQL TLS; recreate it with BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh before enabling encryption"
fi

# ---------------------------------------------------------------------
# 2. TLS material
# ---------------------------------------------------------------------
if kubectl -n "$NAMESPACE" get secret "$TLS_SECRET" >/dev/null 2>&1 && \
	kubectl -n "$NAMESPACE" get secret "$ESCROW_TLS_SECRET" >/dev/null 2>&1; then
	ok "TLS secrets $TLS_SECRET and $ESCROW_TLS_SECRET already exist"
else
	if kubectl -n "$NAMESPACE" get secret "$TLS_SECRET" >/dev/null 2>&1 || \
		kubectl -n "$NAMESPACE" get secret "$ESCROW_TLS_SECRET" >/dev/null 2>&1; then
		die "only one playground TLS Secret exists; delete both $TLS_SECRET and $ESCROW_TLS_SECRET, then retry"
	fi
	command -v openssl >/dev/null 2>&1 || die "openssl is required to generate playground TLS material"
	info "generating a self-signed CA and separate MySQL/escrow server certificates"

	TMP="$(mktemp -d)"
	trap 'rm -rf "$TMP"' EXIT

	openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
		-keyout "$TMP/ca.key" -out "$TMP/ca.crt" \
		-subj "/CN=bloodraven-playground-ca" >/dev/null

	# SANs cover every name MySQL is dialled by: the per-site internal
	# Services (used for replication and by the operator), the
	# client-facing Services, and localhost for port-forwarded clients.
	cat > "$TMP/san.cnf" <<EOF
[req]
distinguished_name = dn
[dn]
[v3]
subjectAltName = @alt
extendedKeyUsage = serverAuth, clientAuth
[alt]
DNS.1 = mysql-${FG}-iad
DNS.2 = mysql-${FG}-pdx
DNS.3 = mysql-${FG}-reader
DNS.4 = mysql-${FG}-iad.${NAMESPACE}.svc.cluster.local
DNS.5 = mysql-${FG}-pdx.${NAMESPACE}.svc.cluster.local
DNS.6 = mysql-${FG}-reader.${NAMESPACE}.svc.cluster.local
DNS.7 = mysql-${FG}-iad-internal.${NAMESPACE}.svc.cluster.local
DNS.8 = mysql-${FG}-pdx-internal.${NAMESPACE}.svc.cluster.local
DNS.9 = mysql-${FG}-reader-internal.${NAMESPACE}.svc.cluster.local
DNS.10 = mysql-${FG}-primary.${NAMESPACE}.svc.cluster.local
DNS.11 = mysql-${FG}-replicas.${NAMESPACE}.svc.cluster.local
DNS.12 = localhost
IP.1 = 127.0.0.1
EOF

	openssl req -newkey rsa:2048 -nodes \
		-keyout "$TMP/tls.key" -out "$TMP/tls.csr" \
		-subj "/CN=mysql-${FG}" >/dev/null
	openssl x509 -req -in "$TMP/tls.csr" -days 3650 \
		-CA "$TMP/ca.crt" -CAkey "$TMP/ca.key" -CAcreateserial \
		-extfile "$TMP/san.cnf" -extensions v3 \
		-out "$TMP/tls.crt" >/dev/null

	cat > "$TMP/escrow-san.cnf" <<EOF
[req]
distinguished_name = dn
[dn]
[v3]
subjectAltName = DNS:bloodraven.${NAMESPACE}.svc.cluster.local
extendedKeyUsage = serverAuth
EOF
	openssl req -newkey rsa:2048 -nodes \
		-keyout "$TMP/escrow-tls.key" -out "$TMP/escrow-tls.csr" \
		-subj "/CN=bloodraven.${NAMESPACE}.svc.cluster.local" >/dev/null
	openssl x509 -req -in "$TMP/escrow-tls.csr" -days 3650 \
		-CA "$TMP/ca.crt" -CAkey "$TMP/ca.key" -CAcreateserial \
		-extfile "$TMP/escrow-san.cnf" -extensions v3 \
		-out "$TMP/escrow-tls.crt" >/dev/null

	kubectl -n "$NAMESPACE" create secret generic "$TLS_SECRET" \
		--from-file=ca.crt="$TMP/ca.crt" \
		--from-file=tls.crt="$TMP/tls.crt" \
		--from-file=tls.key="$TMP/tls.key"
	ok "created TLS secret $TLS_SECRET"
	kubectl -n "$NAMESPACE" create secret tls "$ESCROW_TLS_SECRET" \
		--cert="$TMP/escrow-tls.crt" --key="$TMP/escrow-tls.key"
	ok "created escrow TLS secret $ESCROW_TLS_SECRET"
fi

if [[ "$PREPARE_TLS" == "1" ]]; then
	exit 0
fi

# Prefer the chart's escrowTLS secret mount: a prior setup with
# BLOODRAVEN_SETUP_TLS already armed the listener, and a no-op helm
# upgrade still rolls the operator and races the encryption adoption.
if kubectl -n "$NAMESPACE" get deploy bloodraven -o jsonpath='{range .spec.template.spec.volumes[*]}{.secret.secretName}{"\n"}{end}' 2>/dev/null \
	| grep -qx "$ESCROW_TLS_SECRET"; then
	ok "operator already has escrow TLS configured; skipping helm upgrade"
else
	wait_for_stable_baseline "before restarting the operator for escrow TLS"
	info "enabling the operator keyring escrow TLS listener"
	helm upgrade bloodraven "$SCRIPT_DIR/../charts/bloodraven" \
		--namespace "$NAMESPACE" --reuse-values \
		--set auxiliary.escrowTLS.enabled=true \
		--set auxiliary.escrowTLS.existingSecret="$ESCROW_TLS_SECRET" \
		--timeout=180s
	kubectl -n "$NAMESPACE" rollout status deployment/bloodraven --timeout=180s
fi

# ---------------------------------------------------------------------
# 3. Optional wipe, so every tablespace is encrypted from birth
# ---------------------------------------------------------------------
if [[ "$FRESH" == "1" ]]; then
	warn "--fresh: wiping MySQL data so every tablespace is encrypted from birth"
	"$SCRIPT_DIR/reset-mysql.sh"
fi

wait_for_stable_baseline "before enabling encryption"

# ---------------------------------------------------------------------
# 4. Patch the failover group
# ---------------------------------------------------------------------
# JSON Patch, not merge patch: merge and strategic-merge patches drop
# required fields on this CRD (the documented mfg patch trap).
info "enabling spec.encryptionAtRest"
kubectl -n "$NAMESPACE" patch mysqlfailovergroup "$FG" --type=json -p '[
  {"op": "add", "path": "/spec/encryptionAtRest", "value": {
     "enabled": true
  }}
]'

# The operator refuses encryption on any group with an ActiveSite unless
# this annotation is present — including after --fresh, because reset-mysql
# re-establishes a healthy primary before we get here. --fresh still matters
# for data coverage (new tablespaces are encrypted from birth); the
# annotation only acknowledges the timing of the flag flip.
if [[ "$FRESH" == "1" ]]; then
	ok "acknowledging encryption enable after --fresh wipe (annotation required while ActiveSite is set)"
else
	warn "the playground is already serving, so pre-existing tables stay plaintext"
	warn "acknowledging with the encryption-adopt annotation (use --fresh for full coverage)"
fi
kubectl -n "$NAMESPACE" annotate mysqlfailovergroup "$FG" \
	bloodraven.shipstream.io/encryption-adopt=confirm --overwrite

# ---------------------------------------------------------------------
# 5. Wait for every site to seal
# ---------------------------------------------------------------------
# Three-site ordered update + keyring escrow regularly needs more than
# 10 minutes on kind (CI runners are especially slow around the primary
# handoff). 15 minutes still fails fast when the keyring never loads.
info "waiting for every site to reach phase=Sealed (up to 15 minutes)"
deadline=$(( $(date +%s) + 900 ))
while [[ $(date +%s) -lt $deadline ]]; do
	sealed=$(kubectl -n "$NAMESPACE" get mysqlfailovergroup "$FG" \
		-o jsonpath='{.status.encryptionAtRest.sealed}' 2>/dev/null || echo "")
	if [[ "$sealed" == "true" ]]; then
		ok "every site is sealed against an escrowed keyring"
		report_status
		echo
		info "run the keyring chaos scenario with:"
		echo "    make chaos-run SCENARIO=48-keyring-seal-and-rotation"
		exit 0
	fi
	sleep 10
done

warn "sites did not all seal within 15 minutes"
report_status
echo
info "forensics: keyring component files and status on one MySQL pod"
pod=$(kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=mysql,shipstream.io/site" \
	-o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$pod" ]]; then
	kubectl -n "$NAMESPACE" exec "$pod" -c mysql -- sh -c '
		echo "--- mounts ---"
		ls -la /usr/sbin/mysqld.my /var/lib/mysql/mysqld.my /usr/lib64/mysql/plugin/component_keyring_file.cnf /usr/lib64/mysql/plugin/component_keyring_file.so /usr/share/mysql-9.7/english/errmsg.sys /run/mysql-keyring 2>&1 || true
		echo "--- global mysqld.my ---"
		cat /usr/sbin/mysqld.my 2>&1 || true
		echo "--- local datadir mysqld.my ---"
		cat /var/lib/mysql/mysqld.my 2>&1 || true
		echo "--- component_keyring_file.cnf ---"
		cat /usr/lib64/mysql/plugin/component_keyring_file.cnf 2>&1 || true
		echo "--- bloodraven.cnf (encryption lines) ---"
		grep -E "encrypt|keyring" /etc/mysql/conf.d/bloodraven.cnf 2>&1 || true
	' 2>&1 || warn "could not exec into $pod for forensics"
	kubectl -n "$NAMESPACE" logs "$pod" -c sidecar --tail=30 2>&1 || true
fi
echo
warn "check the operator logs and status messages above; see"
warn "docs/docs/runbooks.mdx#keyring-not-sealed"
exit 1
