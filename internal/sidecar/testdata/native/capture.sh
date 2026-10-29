#!/usr/bin/env bash
set -euo pipefail
# Requires Docker and the mysql:8.4 image. No ports, credentials or data leave this container.
mode=${1:-all}
[[ "$mode" == all || "$mode" == compression ]] || { echo "usage: $0 [all|compression]" >&2; exit 2; }
output=$(cd "$(dirname "$0")" && pwd)
docker build -t bloodraven-pitr-mysqlbinlog:8.4 "$output"
docker run --rm -i --network none -e CAPTURE_MODE="$mode" -v "$output:/fixtures" --entrypoint bash mysql:8.4 -s <<'INNER'
set -euo pipefail
start() {
 mysqld --no-defaults --user=root --datadir="$data" --socket=/tmp/capture.sock --pid-file=/tmp/capture.pid --log-error=/tmp/capture.err --skip-networking --secure-file-priv=/tmp --server-id=101 --log-bin="$data/mysql-bin" --gtid-mode=ON --enforce-gtid-consistency=ON --innodb-buffer-pool-size=64M &
 for attempt in {1..60}; do if mysqladmin --socket=/tmp/capture.sock -uroot ping --silent; then return; fi; sleep 1; done
 cat /tmp/capture.err;exit 1
}
query() { mysql --socket=/tmp/capture.sock -uroot --batch --skip-column-names -e "$1"; }
file() { query 'SHOW BINARY LOG STATUS' | cut -f1; }
seal() { local name=$1 old;old=$(file);query 'FLUSH BINARY LOGS';cp "$data/$old" "/fixtures/$name.bin";printf '%s\t%s\t%s\n' "$name" "$old" "$(query 'SELECT @@global.gtid_purged')" >>"$catalog"; }
catalog=/fixtures/catalog.tsv
data=/tmp/source
mkdir "$data";mysqld --no-defaults --user=root --initialize-insecure --datadir="$data" >/tmp/init.log 2>&1
start
if [[ "$CAPTURE_MODE" == compression ]]; then
 catalog=/fixtures/compressed-session-catalog.tsv
 rm -f "$catalog"
 query "CREATE DATABASE replay; CREATE TABLE replay.markers(id INT PRIMARY KEY, value BLOB)"
 query 'FLUSH BINARY LOGS'
 query "SET SESSION binlog_transaction_compression=ON; INSERT INTO replay.markers VALUES(1,RANDOM_BYTES(768)); BEGIN; INSERT INTO replay.markers VALUES(2,RANDOM_BYTES(768)); SAVEPOINT s1; INSERT INTO replay.markers VALUES(3,RANDOM_BYTES(768)); ROLLBACK TO SAVEPOINT s1; INSERT INTO replay.markers VALUES(4,RANDOM_BYTES(768)); COMMIT"
 sleep 2
 seal compressed-session
 sleep 2
 seal compressed-session-idle
 sed -i 's/\t$/\t-/' "$catalog"
 mysqladmin --socket=/tmp/capture.sock -uroot shutdown
 mysqld --version >/fixtures/compressed-session-server-version.txt
 chmod a+r /fixtures/compressed-session*.bin
 exit
fi
rm -f "$catalog"
seal fresh-empty
query "CREATE DATABASE replay; CREATE TABLE replay.markers(id INT PRIMARY KEY, value VARCHAR(40)); INSERT INTO replay.markers VALUES(1,'initial')"
seal initial
seal flush-empty
query "XA START 'capture'; INSERT INTO replay.markers VALUES(2,'xa'); XA END 'capture'; XA PREPARE 'capture'"
seal xa-prepare
query "XA COMMIT 'capture'"
seal xa
printf '6\tload-data\n' > /tmp/capture-load.tsv
query "SET SESSION binlog_format=STATEMENT; LOAD DATA INFILE '/tmp/capture-load.tsv' INTO TABLE replay.markers"
seal load-data
query "SET SESSION binlog_transaction_compression=ON; INSERT INTO replay.markers VALUES(3,REPEAT('z',40))"
seal compressed
# MySQL 8.4 supports changing the checksum dynamically; it may rotate by itself.
query 'SET GLOBAL binlog_checksum=NONE'
query "INSERT INTO replay.markers VALUES(4,'checksum-off')"
seal checksum-off
mysqladmin --socket=/tmp/capture.sock -uroot shutdown
start
seal restart-empty
query "INSTALL PLUGIN clone SONAME 'mysql_clone.so'; GRANT BACKUP_ADMIN, CLONE_ADMIN ON *.* TO 'root'@'localhost'; CLONE LOCAL DATA DIRECTORY='/tmp/recloned'"
mysqladmin --socket=/tmp/capture.sock -uroot shutdown
# A real physical clone excludes binlogs and starts numbering again.
data=/tmp/recloned
start
query "INSERT INTO replay.markers VALUES(5,'after-clone')"
seal recloned
mysqladmin --socket=/tmp/capture.sock -uroot shutdown
mysqld --version > /fixtures/server-version.txt
chmod a+r /fixtures/*.bin
INNER
docker run --rm --network none -e CAPTURE_MODE="$mode" -v "$output:/fixtures" --entrypoint bash bloodraven-pitr-mysqlbinlog:8.4 -c '
if [[ "$CAPTURE_MODE" == compression ]]; then
 mysqlbinlog --version >/fixtures/compressed-session-client-version.txt
 entries=(/fixtures/compressed-session.bin /fixtures/compressed-session-idle.bin)
else
 mysqlbinlog --version >/fixtures/version.txt
 entries=(/fixtures/*.bin)
fi
for entry in "${entries[@]}"; do TZ=UTC mysqlbinlog --no-defaults --verify-binlog-checksum "$entry" > "${entry%.bin}.sql"; done
chmod a+r /fixtures/*'
