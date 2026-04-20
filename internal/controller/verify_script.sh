#!/usr/bin/env bash
# Bloodraven verification script. Runs inside a MysqlBackupVerification
# Job. Bootstraps an ephemeral mysqld on a dedicated PVC, waits for it
# to accept connections, delegates the actual loadDump to the shared
# restore.py script (via BLOODRAVEN_MYSQL_HOST=127.0.0.1), then shuts
# mysqld down. Exits with the load script's exit code.
#
# The PVC is always dedicated per verification run; the datadir is
# initialized the first time this script runs (should be every time
# since the PVC is fresh, but we guard for retries). We skip the
# privilege-tables recreate on subsequent runs by checking for the
# mysql directory under the datadir.
#
# Required env:
#   BLOODRAVEN_DATA_DIR     datadir path (must be writable; the PVC is
#                           mounted here).
#   BLOODRAVEN_SCRIPTS_DIR  mounted scripts ConfigMap (restore.py lives
#                           at $BLOODRAVEN_SCRIPTS_DIR/restore.py).
#
# All other env forwarded to restore.py (BLOODRAVEN_INPUT_URL,
# BLOODRAVEN_LOAD_OPTIONS, BLOODRAVEN_MYSQL_CREDS_DIR, BLOODRAVEN_S3_*,
# etc.) are inherited from the container spec.

set -euo pipefail

DATA_DIR="${BLOODRAVEN_DATA_DIR:-/var/lib/mysql-verify}"
SCRIPTS_DIR="${BLOODRAVEN_SCRIPTS_DIR:-/scripts}"
SOCKET="${DATA_DIR}/mysql.sock"
ERRLOG="${DATA_DIR}/mysqld.err"

log() { printf '[verify] %s\n' "$*"; }

cleanup_mysqld() {
    if [[ -n "${MYSQLD_PID:-}" ]] && kill -0 "$MYSQLD_PID" 2>/dev/null; then
        log "shutting down mysqld (pid=$MYSQLD_PID)"
        mysqladmin --socket="$SOCKET" --user=root shutdown >/dev/null 2>&1 || true
        wait "$MYSQLD_PID" 2>/dev/null || true
    fi
}
trap cleanup_mysqld EXIT

mkdir -p "$DATA_DIR"

if [[ ! -d "${DATA_DIR}/mysql" ]]; then
    log "initializing ephemeral datadir at $DATA_DIR"
    mysqld --initialize-insecure \
        --datadir="$DATA_DIR" \
        --user=mysql \
        --log-error="$ERRLOG"
fi

log "starting ephemeral mysqld"
mysqld \
    --datadir="$DATA_DIR" \
    --user=mysql \
    --bind-address=127.0.0.1 \
    --socket="$SOCKET" \
    --log-error="$ERRLOG" \
    --skip-log-bin \
    --skip-slave-start \
    --skip-name-resolve \
    --local-infile=1 &
MYSQLD_PID=$!

log "waiting for mysqld to accept connections"
for _ in $(seq 1 60); do
    if mysqladmin --socket="$SOCKET" --user=root ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
if ! mysqladmin --socket="$SOCKET" --user=root ping >/dev/null 2>&1; then
    log "mysqld did not become ready in 60s; errorlog tail:"
    tail -n 50 "$ERRLOG" || true
    exit 1
fi

# The ephemeral root account has no password after --initialize-insecure.
# Point the shared restore.py at it via BLOODRAVEN_MYSQL_HOST and override
# the creds dir with a tmpfs-backed pair of files holding MYSQL_USER=root
# and an empty password. We can't write into the mounted creds dir (it's
# read-only), so stage a fresh one under /tmp.
VERIFY_CREDS_DIR=/tmp/verify-creds
mkdir -p "$VERIFY_CREDS_DIR"
printf 'root' >"$VERIFY_CREDS_DIR/MYSQL_USER"
: >"$VERIFY_CREDS_DIR/MYSQL_PASSWORD"
chmod 0400 "$VERIFY_CREDS_DIR"/MYSQL_*

export BLOODRAVEN_MYSQL_HOST="127.0.0.1:3306"
export BLOODRAVEN_MYSQL_CREDS_DIR="$VERIFY_CREDS_DIR"
unset BLOODRAVEN_TLS

log "running restore.py against ephemeral mysqld"
set +e
mysqlsh --no-wizard --py -f "$SCRIPTS_DIR/restore.py"
LOAD_RC=$?
set -e

if [[ $LOAD_RC -ne 0 ]]; then
    log "restore.py exited with rc=$LOAD_RC"
    exit "$LOAD_RC"
fi

log "verification succeeded"
exit 0
