# Runs inside mysql:9.7 (see ../capture.sh failback). Writes the mixed-file
# failback fixtures into /fixtures.
set -euo pipefail
out=/fixtures
rm -f "$out"/*.bin "$out"/*.tsv "$out"/server-version.txt
start() { # name server_id port
 mkdir -p /tmp/$1
 mysqld --no-defaults --user=root --initialize-insecure --datadir=/tmp/$1 >/tmp/$1-init.log 2>&1
 mysqld --no-defaults --user=root --datadir=/tmp/$1 --socket=/tmp/$1.sock --pid-file=/tmp/$1.pid --log-error=/tmp/$1.err \
  --port=$3 --bind-address=127.0.0.1 --mysqlx=OFF --server-id=$2 --log-bin=/tmp/$1/mysql-bin --gtid-mode=ON --enforce-gtid-consistency=ON \
  --log-replica-updates=ON --binlog-format=ROW --binlog-row-image=FULL --replica-preserve-commit-order=ON --binlog-transaction-compression=ON \
  --skip-replica-start --innodb-buffer-pool-size=64M --secure-file-priv=/tmp &
 for attempt in {1..90}; do if mysqladmin --socket=/tmp/$1.sock -uroot ping --silent 2>/dev/null; then return; fi; sleep 1; done
 cat /tmp/$1.err; exit 1
}
q() { mysql --socket=/tmp/$1.sock -uroot --batch --skip-column-names -e "$2"; }
executed() { q $1 "SELECT REPLACE(@@global.gtid_executed, CHAR(10), '')"; }
now() { q $1 "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')"; }
checksum() { q $1 "SELECT CONCAT(COUNT(*), '/', COALESCE(SUM(CRC32(CONCAT_WS('|', id, tag, site, SHA2(payload, 256)))), 0)) FROM replay.markers $2"; }
follow() { # replica source_port
 q $1 "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=$2, SOURCE_USER='repl', SOURCE_PASSWORD='repl', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA"
}
catchup() { q $1 "SELECT WAIT_FOR_EXECUTED_GTID_SET('$(executed $2)', 60)" >/dev/null; }
row() { # server tag [tagged]
 local pre=""
 [[ "${3:-}" == tagged ]] && pre="SET gtid_next='AUTOMATIC:fixture'; "
 q $1 "${pre}INSERT INTO replay.markers(tag, site, payload) VALUES('$2', '$1', CONCAT(REPEAT('$2', 64), RANDOM_BYTES(32)))"
}
target() { # label server: the state committed before T
 printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$(executed $2)" "$(checksum $2 '')" "$(checksum $2 "WHERE tag <> 'skip'")" "$(now $2)" >>"$out/targets.tsv"
 sleep 0.05
}
record() { # site server: a promotion record, then more than δ (5 s) before
 # the era's first commit, so every witness w of the era satisfies
 # observedAt + δ < oct(w) and the record can anchor its replica copies
 # (D-43). Transactions after a record can never be loss evidence against
 # it, so the delay is about anchoring, not loss evidence.
 printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$(q $2 'SELECT @@server_uuid')" "$(q $2 'SELECT @@server_id')" "$(executed $2)" "$(now $2)" >>"$out/records.tsv"
 sleep 6
}
start a 101 3306
start b 102 3307
record a a
q a "CREATE USER repl@'127.0.0.1' IDENTIFIED BY 'repl'; GRANT REPLICATION SLAVE ON *.* TO repl@'127.0.0.1'; CREATE DATABASE replay; CREATE TABLE replay.markers(id INT AUTO_INCREMENT PRIMARY KEY, tag VARCHAR(40), site VARCHAR(8), payload BLOB)"
follow b 3306
q a 'FLUSH BINARY LOGS'   # mysql-bin.000001: the backup's history
sleep 2                  # targets after the dump's end rounded up to the second (D-44)
# Era 1 on a. The first FLUSH seals an era-1 file; the era's tail lands in
# the file that later mixes all three eras.
for n in 1 2 3; do row a era1-$n; done
row a era1-tagged tagged
target t1 a
for n in 4 5; do row a era1-$n; done
q a 'FLUSH BINARY LOGS'   # mysql-bin.000002
row a era1-6
row a era1-big
q a "UPDATE replay.markers SET payload = CONCAT(RANDOM_BYTES(1024), RANDOM_BYTES(1024), RANDOM_BYTES(1024), RANDOM_BYTES(1024)) WHERE tag = 'era1-big'"
# A highly compressible transaction: small on disk, 60 kB decompressed
# (the decoder-bound fixture).
q a "INSERT INTO replay.markers(tag, site, payload) VALUES('era1-zeros', 'a', REPEAT('z', 60000))"
catchup b a
# Planned failover A→B: no rotation on either side.
q a 'SET GLOBAL super_read_only = ON'
catchup b a
q b 'STOP REPLICA; RESET REPLICA ALL'
record b b
follow a 3307
# Era 2 on b (archived only as replica copies in a's mixed file).
for n in 1 2; do row b era2-$n; done
before=$(executed b)
row b skip
printf 'skip\t%s\t%s\n' "$before" "$(executed b)" >"$out/skip.tsv"
row b era2-tagged tagged
target t2 b
for n in 3 4; do row b era2-$n; done
catchup a b
# Planned failback B→A.
q b 'SET GLOBAL super_read_only = ON'
catchup a b
q a 'STOP REPLICA; RESET REPLICA ALL; SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF'
record a a
follow b 3306
for n in 1 2; do row a era3-$n; done
target t3 a
for n in 3 4; do row a era3-$n; done
printf 'latest\t%s\t%s\t%s\t-\n' "$(executed a)" "$(checksum a '')" "$(checksum a "WHERE tag <> 'skip'")" >>"$out/targets.tsv"
q a 'FLUSH BINARY LOGS'   # seals mysql-bin.000003: [A era 1 tail, Bu era 2 (replica), A era 3]
for f in mysql-bin.000001 mysql-bin.000002 mysql-bin.000003; do cp /tmp/a/$f "$out/a-$f.bin"; done
mysqld --version >"$out/server-version.txt"
/usr/libexec/mysqlsh/mysqlbinlog --version >>"$out/server-version.txt"
mysqladmin --socket=/tmp/b.sock -uroot shutdown
mysqladmin --socket=/tmp/a.sock -uroot shutdown
chmod a+r "$out"/*
