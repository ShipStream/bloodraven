#!/bin/sh
# Bloodraven backup entrypoint. Environment variables are provided by the
# operator-managed CronJob. This script:
#   1. Runs xtrabackup or mysqldump against $MYSQL_HOST
#   2. Streams (gzip-compressed) directly to s3://$S3_BUCKET/$S3_PREFIX/
#   3. Writes a metadata sidecar alongside the backup
#   4. Enforces retention (by count or age)
#
# The script exits non-zero on any failure so that the Kubernetes Job is
# marked Failed.

set -eu

: "${MYSQL_HOST:?missing}"
: "${MYSQL_PORT:=3306}"
: "${MYSQL_DSN:?missing}"
: "${BACKUP_METHOD:=xtrabackup}"
: "${S3_BUCKET:?missing}"
: "${S3_PREFIX:?missing}"
: "${FAILOVER_GROUP:?missing}"
: "${SITE:?missing}"
: "${RETENTION_COUNT:=0}"
: "${RETENTION_DAYS:=0}"

# Parse user:password@tcp(host:port)/db style DSN into discrete pieces for the
# backup tools. Extremely simple parse — assumes the DSN was produced by the
# operator's conventions and contains user:pass@tcp(host:port).
USER=$(printf '%s' "$MYSQL_DSN" | sed -E 's|^([^:]+):.*|\1|')
PASS=$(printf '%s' "$MYSQL_DSN" | sed -E 's|^[^:]+:([^@]*)@.*|\1|')

# aws-cli endpoint flag (optional, for MinIO / non-AWS S3)
S3_FLAGS=""
if [ -n "${S3_ENDPOINT:-}" ]; then
    S3_FLAGS="--endpoint-url $S3_ENDPOINT"
fi

TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
START=$(date -u +%s)

case "$BACKUP_METHOD" in
    xtrabackup)
        EXT="xbstream.gz"
        ;;
    mysqldump)
        EXT="sql.gz"
        ;;
    *)
        echo "ERROR: unsupported BACKUP_METHOD=$BACKUP_METHOD" >&2
        exit 1
        ;;
esac

OBJECT_KEY="$S3_PREFIX/$TS.$EXT"
META_KEY="$S3_PREFIX/$TS.meta.json"
S3_URI="s3://$S3_BUCKET/$OBJECT_KEY"

echo "[bloodraven-backup] method=$BACKUP_METHOD host=$MYSQL_HOST target=$S3_URI"

# Capture GTID_EXECUTED before the backup starts for the metadata sidecar.
GTID=""
if command -v mysql >/dev/null 2>&1; then
    GTID=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$USER" -p"$PASS" \
        -N -B -e "SELECT @@GLOBAL.gtid_executed" 2>/dev/null || true)
fi

case "$BACKUP_METHOD" in
    xtrabackup)
        xtrabackup --backup --stream=xbstream \
            --host="$MYSQL_HOST" --port="$MYSQL_PORT" \
            --user="$USER" --password="$PASS" \
            | gzip -c \
            | aws $S3_FLAGS s3 cp - "$S3_URI"
        ;;
    mysqldump)
        mysqldump --single-transaction --routines --triggers --events \
            --set-gtid-purged=ON \
            -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$USER" -p"$PASS" \
            --all-databases \
            | gzip -c \
            | aws $S3_FLAGS s3 cp - "$S3_URI"
        ;;
esac

END=$(date -u +%s)
DURATION=$((END - START))

# Write metadata sidecar.
META=$(cat <<EOF
{
  "failover_group": "$FAILOVER_GROUP",
  "site": "$SITE",
  "method": "$BACKUP_METHOD",
  "timestamp": "$TS",
  "duration_seconds": $DURATION,
  "gtid_executed": "$GTID",
  "object": "$OBJECT_KEY"
}
EOF
)
printf '%s\n' "$META" | aws $S3_FLAGS s3 cp - "s3://$S3_BUCKET/$META_KEY"

echo "[bloodraven-backup] upload complete duration=${DURATION}s"

# Retention: list all backups in the prefix sorted by timestamp (descending),
# skip the most recent $RETENTION_COUNT, and delete the rest. Age-based
# retention deletes backups older than $RETENTION_DAYS days.
if [ "$RETENTION_COUNT" -gt 0 ] || [ "$RETENTION_DAYS" -gt 0 ]; then
    echo "[bloodraven-backup] enforcing retention count=$RETENTION_COUNT days=$RETENTION_DAYS"

    # List just the backup objects (not meta sidecars) and sort by key desc.
    LIST_TMP=$(mktemp)
    aws $S3_FLAGS s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" \
        | awk '{print $4}' \
        | grep -E '\.(xbstream|sql)\.gz$' \
        | sort -r > "$LIST_TMP" || true

    # Count-based pruning.
    if [ "$RETENTION_COUNT" -gt 0 ]; then
        SKIP=$((RETENTION_COUNT))
        tail -n +$((SKIP + 1)) "$LIST_TMP" | while read -r key; do
            [ -z "$key" ] && continue
            echo "[bloodraven-backup] retention: deleting $key"
            aws $S3_FLAGS s3 rm "s3://$S3_BUCKET/$S3_PREFIX/$key" || true
            base=$(printf '%s' "$key" | sed -E 's/\.(xbstream|sql)\.gz$//')
            aws $S3_FLAGS s3 rm "s3://$S3_BUCKET/$S3_PREFIX/$base.meta.json" 2>/dev/null || true
        done
    fi

    # Age-based pruning.
    if [ "$RETENTION_DAYS" -gt 0 ]; then
        CUTOFF=$(date -u -d "-${RETENTION_DAYS} days" +%Y-%m-%d 2>/dev/null || \
                 date -u -v-"${RETENTION_DAYS}"d +%Y-%m-%d)
        while read -r key; do
            [ -z "$key" ] && continue
            # Key format: <ts>.ext where ts starts with YYYY-MM-DD.
            KEY_DATE=$(printf '%s' "$key" | cut -c1-10)
            if [ "$KEY_DATE" \< "$CUTOFF" ]; then
                echo "[bloodraven-backup] retention: deleting $key (age)"
                aws $S3_FLAGS s3 rm "s3://$S3_BUCKET/$S3_PREFIX/$key" || true
                base=$(printf '%s' "$key" | sed -E 's/\.(xbstream|sql)\.gz$//')
                aws $S3_FLAGS s3 rm "s3://$S3_BUCKET/$S3_PREFIX/$base.meta.json" 2>/dev/null || true
            fi
        done < "$LIST_TMP"
    fi

    rm -f "$LIST_TMP"
fi

echo "[bloodraven-backup] done"
