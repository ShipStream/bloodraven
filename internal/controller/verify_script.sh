#!/usr/bin/env bash
# Bloodraven verification script. Runs inside a MysqlBackupVerification
# Job. Bootstraps an ephemeral mysqld on a dedicated emptyDir, waits for
# it to accept connections, delegates the actual loadDump to the shared
# restore.py script (via BLOODRAVEN_MYSQL_HOST=127.0.0.1), optionally
# replays archived binlogs on top, optionally runs a scalar sanity
# query, then shuts mysqld down. Exits non-zero on the first failing
# phase.
#
# The emptyDir is always dedicated per verification run; the datadir is
# initialized the first time this script runs (should be every time
# since the emptyDir is fresh, but we guard for retries). We skip the
# privilege-tables recreate on subsequent runs by checking for the
# mysql directory under the datadir.
#
# Required env:
#   BLOODRAVEN_DATA_DIR     datadir path (must be writable; the emptyDir
#                           is mounted here, chowned to this user by the
#                           prep-datadir init container).
#   BLOODRAVEN_SCRIPTS_DIR  mounted scripts ConfigMap (restore.py lives
#                           at $BLOODRAVEN_SCRIPTS_DIR/restore.py).
#
# Optional Phase-2 env:
#   BLOODRAVEN_VERIFY_PITR_MODE           none|latest|timestamp
#   BLOODRAVEN_PITR_LOCAL_DIR             where pitr-download init
#                                         container dropped binlogs
#   BLOODRAVEN_PITR_STOP_DATETIME         replay stop instant (RFC3339)
#                                         for mode=timestamp
#   BLOODRAVEN_VERIFY_SANITY_QUERY        scalar SELECT to run post-load
#   BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS  sanity client-side timeout (s)
#   BLOODRAVEN_VERIFY_SANITY_MIN_ROWS     fail if scalar < this value
#
# All other env forwarded to restore.py (BLOODRAVEN_INPUT_URL,
# BLOODRAVEN_LOAD_OPTIONS, BLOODRAVEN_MYSQL_CREDS_DIR, BLOODRAVEN_S3_*,
# etc.) are inherited from the container spec.

set -euo pipefail

# The emptyDir is mounted at $MOUNT_DIR. mysqld --initialize refuses to
# initialize into a directory that already exists (errno 17 EEXIST), so the
# real datadir is a subdirectory mysqld creates itself; the socket/errlog
# stay on the mount root and need no initialization.
MOUNT_DIR="${BLOODRAVEN_DATA_DIR:-/var/lib/mysql-verify}"
DATA_DIR="${MOUNT_DIR}/data"
SCRIPTS_DIR="${BLOODRAVEN_SCRIPTS_DIR:-/scripts}"
SOCKET="${MOUNT_DIR}/mysql.sock"
ERRLOG="${MOUNT_DIR}/mysqld.err"

log() { printf '[verify] %s\n' "$*"; }

cleanup_mysqld() {
    if [[ -n "${MYSQLD_PID:-}" ]] && kill -0 "$MYSQLD_PID" 2>/dev/null; then
        log "shutting down mysqld (pid=$MYSQLD_PID)"
        mysqladmin --socket="$SOCKET" --user=root shutdown >/dev/null 2>&1 || true
        wait "$MYSQLD_PID" 2>/dev/null || true
    fi
}
trap cleanup_mysqld EXIT

mkdir -p "$MOUNT_DIR"

if [[ ! -d "${DATA_DIR}/mysql" ]]; then
    # Remove any partial datadir left by a prior Job attempt so mysqld can
    # create $DATA_DIR fresh — it refuses a pre-existing directory.
    rm -rf "$DATA_DIR"
    log "initializing ephemeral datadir at $DATA_DIR"
    # Do NOT pass --user=mysql: the pod already pins a fixed non-root
    # identity via RunAsUser/RunAsGroup/FSGroup (uid/gid 27), and the
    # datadir emptyDir is owned by that fsGroup. --user makes mysqld
    # setgid to the image's own "mysql" group (NOT 27 in the verify
    # image), which loses group-write on the emptyDir and fails datadir
    # creation with EACCES. Running as the container identity is correct.
    mysqld --initialize-insecure \
        --datadir="$DATA_DIR" \
        --log-error="$ERRLOG"
fi

log "starting ephemeral mysqld"
# Pid file and mysqlx socket must live on the writable datadir mount;
# the default (/var/run/mysqld/) is not writable in the verification
# Job container image. mysqlx is disabled outright since nothing in
# the verify path uses the X protocol.
mysqld \
    --datadir="$DATA_DIR" \
    --bind-address=127.0.0.1 \
    --socket="$SOCKET" \
    --pid-file="$DATA_DIR/mysqld.pid" \
    --mysqlx=OFF \
    --log-error="$ERRLOG" \
    --skip-log-bin \
    --skip-replica-start \
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

# `--initialize-insecure` only creates root@localhost, which mysqlsh's
# 127.0.0.1 TCP connection doesn't match. Create root@127.0.0.1 with
# no password so restore.py can connect via BLOODRAVEN_MYSQL_HOST.
# Runs via the unix socket, which root@localhost can use.
log "granting root@127.0.0.1 for TCP loopback access"
mysql --socket="$SOCKET" --user=root <<'SQL'
CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY '';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SQL

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
RESTORE_LOG="$(mktemp /tmp/bloodraven-restore.XXXXXX.log)"
mysqlsh --no-wizard --py -f "$SCRIPTS_DIR/restore.py" 2>&1 | tee "$RESTORE_LOG"
LOAD_RC=${PIPESTATUS[0]}
set -e

if [[ $LOAD_RC -ne 0 ]]; then
    log "restore.py exited with rc=$LOAD_RC"
    exit "$LOAD_RC"
fi

# ---- PITR replay -----------------------------------------------------
# restore.py owns PITR replay for both restore and verification jobs.
# Bridge its BLOODRAVEN_RESTORE_COMPLETE metadata into the verification
# sentinel consumed by the reconciler. The restore sentinel does not
# include exact replay position/timestamp, but a non-empty replay mark
# still records that PITR replay ran and which archived binlog was last
# reported by restore.py; the sanity query below proves target semantics.
PITR_MODE="${BLOODRAVEN_VERIFY_PITR_MODE:-none}"
if [[ "$PITR_MODE" != "none" && "$PITR_MODE" != "" ]]; then
    RESTORE_COMPLETE="$(grep 'BLOODRAVEN_RESTORE_COMPLETE' "$RESTORE_LOG" | tail -n 1 || true)"
    LAST_FILE=""
    REPLAY_COUNT=""
    for f in $RESTORE_COMPLETE; do
        case "$f" in
            pitrReplayedBinlogFile=*) LAST_FILE="${f#pitrReplayedBinlogFile=}" ;;
            pitrReplayedBinlogCount=*) REPLAY_COUNT="${f#pitrReplayedBinlogCount=}" ;;
        esac
    done
    if [[ -z "$RESTORE_COMPLETE" ]]; then
        log "PITR mode=$PITR_MODE but restore.py did not report restore completion"
        exit 1
    fi
    if [[ -z "$LAST_FILE" && "$REPLAY_COUNT" != "0" ]]; then
        log "PITR mode=$PITR_MODE but restore.py did not report replayed binlogs"
        exit 1
    fi
    printf 'BLOODRAVEN_VERIFY_REPLAY_COMPLETE file=%s position=%s timestamp=%s\n' \
        "$LAST_FILE" "0" ""
fi

# ---- Sanity check ----------------------------------------------------
# When spec.sanityCheck.query is set the controller passes it through as
# BLOODRAVEN_VERIFY_SANITY_QUERY. Runs against the ephemeral mysqld with
# a client-side timeout. Emits a sentinel so the reconciler can populate
# status.sanityCheck even on success. Underscore-escape is applied to
# preserve whitespace through the reconciler's whitespace-split parser.
SANITY_Q="${BLOODRAVEN_VERIFY_SANITY_QUERY:-}"
if [[ -n "$SANITY_Q" ]]; then
    SANITY_MAX="${BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS:-60}"
    SANITY_MIN="${BLOODRAVEN_VERIFY_SANITY_MIN_ROWS:-0}"

    log "running sanity query (timeout=${SANITY_MAX}s, minRows=${SANITY_MIN})"
    # mysql -B -N renders tab-separated, no header, no pretty borders —
    # ideal for single-column scalar capture. -e escapes `"` in the query
    # via printf %q to avoid tripping on user-quoted literals. timeout
    # wraps mysql so a runaway query can't hold the Job forever.
    set +e
    START_NS=$(date +%s%N)
    SANITY_OUT=$(timeout --preserve-status "${SANITY_MAX}" \
        mysql --socket="$SOCKET" --user=root -B -N -e "$SANITY_Q" 2>&1)
    SANITY_RC=$?
    END_NS=$(date +%s%N)
    set -e
    DURATION_MS=$(( (END_NS - START_NS) / 1000000 ))

    if [[ $SANITY_RC -eq 124 ]]; then
        printf 'BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=%s error=%s\n' \
            "$DURATION_MS" "timeout"
        log "sanity query timed out after ${SANITY_MAX}s"
        exit 1
    fi
    if [[ $SANITY_RC -ne 0 ]]; then
        # Escape spaces so the reconciler's whitespace-split parser can
        # round-trip the error text.
        ERR_ESC="${SANITY_OUT//[[:space:]]/_}"
        printf 'BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=%s error=%s\n' \
            "$DURATION_MS" "${ERR_ESC:-error}"
        log "sanity query failed rc=$SANITY_RC: $SANITY_OUT"
        exit "$SANITY_RC"
    fi

    # Scalar capture: first line, first field. Empty result set → 0 rows.
    SCALAR=$(printf '%s' "$SANITY_OUT" | awk 'NR==1 {print $1}')
    if [[ -z "$SCALAR" ]]; then
        SCALAR="0"
    fi

    # Encode whitespace and equals signs in the scalar so it round-trips
    # through the reconciler's sentinel parser.
    SCALAR_ESC="${SCALAR// /_}"
    printf 'BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=%s resultRow=%s\n' \
        "$DURATION_MS" "$SCALAR_ESC"

    # Only apply the minRows floor when the scalar parses as an integer.
    # Non-integer scalars (strings, decimals) pass through unconditionally.
    if [[ "$SANITY_MIN" -gt 0 ]] && [[ "$SCALAR" =~ ^[0-9]+$ ]]; then
        if [[ "$SCALAR" -lt "$SANITY_MIN" ]]; then
            log "sanity scalar $SCALAR < minRows $SANITY_MIN"
            exit 1
        fi
    fi
fi

log "verification succeeded"
exit 0
