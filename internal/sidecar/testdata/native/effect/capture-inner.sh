# Runs inside mysql:9.7 (see ../capture.sh effect). Writes the effect-digest
# fixtures into /fixtures: a source s and its replica r (log_replica_updates)
# archive the same transactions, so every GTID occurs as an origin copy in
# s-mysql-bin.* and as a replica copy in r-mysql-bin.*. cases.tsv says, per
# labelled transaction, what the two copies' effect digests must be: equal,
# differ, or uncomparable (README.md, "Effect-digest pairs").
set -euo pipefail
out=/fixtures
rm -f "$out"/*.bin "$out"/*.tsv "$out"/server-version.txt
start() { # name server_id port [mysqld options]
 local name=$1 id=$2 port=$3
 shift 3
 mkdir -p /tmp/$name
 mysqld --no-defaults --user=root --initialize-insecure --datadir=/tmp/$name >/tmp/$name-init.log 2>&1
 mysqld --no-defaults --user=root --datadir=/tmp/$name --socket=/tmp/$name.sock --pid-file=/tmp/$name.pid --log-error=/tmp/$name.err \
  --port=$port --bind-address=127.0.0.1 --mysqlx=OFF --server-id=$id --log-bin=/tmp/$name/mysql-bin --gtid-mode=ON --enforce-gtid-consistency=ON \
  --log-replica-updates=ON --binlog-format=ROW --binlog-row-image=FULL --replica-preserve-commit-order=ON \
  --skip-replica-start --innodb-buffer-pool-size=64M --secure-file-priv=/tmp "$@" &
 for attempt in {1..90}; do if mysqladmin --socket=/tmp/$name.sock -uroot ping --silent 2>/dev/null; then return; fi; sleep 1; done
 cat /tmp/$name.err; exit 1
}
q() { mysql --socket=/tmp/$1.sock -uroot --batch --skip-column-names -e "$2"; }
executed() { q $1 "SELECT REPLACE(@@global.gtid_executed, CHAR(10), '')"; }
now() { q $1 "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')"; }
catchup() { q r "SELECT WAIT_FOR_EXECUTED_GTID_SET('$(executed s)', 60)" >/dev/null; }
tx() { # label expectation sql: one or more transactions on s, recorded by their GTIDs
 local before
 before=$(executed s)
 q s "$3"
 printf '%s\t%s\t%s\n' "$1" "$2" "$(q s "SELECT REPLACE(GTID_SUBTRACT('$(executed s)', '$before'), CHAR(10), '')")" >>"$out/cases.tsv"
}
seal() { # phase: both servers rotate at the same transaction
 catchup
 printf '%s\t%s\t%s\n' "$1" "$(executed s)" "$(now s)" >>"$out/phases.tsv"
 for n in s r; do
  local old
  old=$(q $n 'SHOW BINARY LOG STATUS' | cut -f1)
  q $n 'FLUSH BINARY LOGS'
  cp /tmp/$n/$old "$out/$n-$old.bin"
 done
 sleep 1.1
}
# The two sides cut rows events at different sizes, and only r compresses.
start s 101 3306 --binlog-row-event-max-size=2048 --binlog-transaction-compression=OFF
start r 102 3307 --binlog-row-event-max-size=512 --binlog-transaction-compression=ON
q s "CREATE USER repl@'127.0.0.1' IDENTIFIED BY 'repl'; GRANT REPLICATION SLAVE ON *.* TO repl@'127.0.0.1'"
q r "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='repl', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA"
tx schema uncomparable "CREATE DATABASE e;
CREATE TABLE e.plain (id INT PRIMARY KEY, v VARCHAR(400), n INT);
CREATE TABLE e.other (id INT PRIMARY KEY, v VARCHAR(40));
CREATE TABLE e.types (id INT PRIMARY KEY, d DECIMAL(20,6), f DOUBLE, fl FLOAT, dt DATETIME(6), ts TIMESTAMP(3) NULL, tm TIME(2), y YEAR,
 en ENUM('a','b','c'), st SET('x','y','z'), b BIT(10), vb VARBINARY(40), c CHAR(10), tx TEXT, bl BLOB, g GEOMETRY, big BIGINT UNSIGNED, vec VECTOR(3), n INT NULL);
CREATE TABLE e.gen (id INT PRIMARY KEY, a INT, b INT, s INT AS (a + b) STORED, v INT AS (a * b) VIRTUAL, name VARCHAR(40), INDEX fx ((UPPER(name))));
CREATE TABLE e.part (id INT, k INT, v VARCHAR(100), PRIMARY KEY (id, k)) PARTITION BY HASH (k) PARTITIONS 4;
CREATE TABLE e.nopk (a INT, v VARCHAR(100));
CREATE TABLE e.js (id INT PRIMARY KEY, doc JSON, n INT);
CREATE TABLE e.parent (id INT PRIMARY KEY);
CREATE TABLE e.child (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES e.parent (id) ON DELETE CASCADE);
CREATE TABLE e.trig (id INT PRIMARY KEY);
CREATE TABLE e.audit (id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(40));
CREATE TRIGGER e.trig_ai AFTER INSERT ON e.trig FOR EACH ROW INSERT INTO e.audit (msg) VALUES (CONCAT('ins ', NEW.id));
CREATE TABLE e.my (id INT PRIMARY KEY, v VARCHAR(20)) ENGINE=MyISAM"
seal setup   # mysql-bin.000001: the backup's history

# Phase 1: faithful copies.
rows() { # from to: a multi-row VALUES list of ~250-byte rows
 local i list=""
 for ((i = $1; i <= $2; i++)); do list+="($i, REPEAT('row$i-', 40), $i),"; done
 printf '%s' "${list%,}"
}
tx big-insert equal "INSERT INTO e.plain VALUES $(rows 1 12)"
tx big-update equal "UPDATE e.plain SET v = CONCAT('u', v), n = n + 100 WHERE id <= 12"
tx multi-table equal "BEGIN; INSERT INTO e.other VALUES (1, 'a'), (2, 'b'); UPDATE e.plain SET n = 0 WHERE id = 3; DELETE FROM e.other WHERE id = 1; INSERT INTO e.plain VALUES (100, 'x', 1); COMMIT"
tx same-table-statements equal "BEGIN; INSERT INTO e.plain VALUES (101, 'a', 1); INSERT INTO e.plain VALUES (102, 'b', 2); UPDATE e.plain SET n = 5 WHERE id = 101; UPDATE e.plain SET n = 6 WHERE id = 102; COMMIT"
tx big-delete equal "DELETE FROM e.plain WHERE id BETWEEN 5 AND 12"
tx types equal "INSERT INTO e.types VALUES
 (1, 12345.678901, 1.5e300, 2.5, '2026-01-02 03:04:05.678901', '2026-01-02 03:04:05.678', '-12:34:56.78', 2026, 'b', 'x,z', b'1010101010', x'00ff10', 'pad', REPEAT('t', 300), RANDOM_BYTES(300), ST_GeomFromText('POINT(1 2)'), 18446744073709551615, STRING_TO_VECTOR('[1,2,3]'), NULL),
 (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 7)"
tx types-update equal "UPDATE e.types SET d = d + 1, tx = NULL, n = 1 WHERE id = 1; UPDATE e.types SET bl = x'', c = '' WHERE id = 2"
tx generated equal "INSERT INTO e.gen (id, a, b, name) VALUES (1, 2, 3, 'abc'), (2, 4, 5, NULL); UPDATE e.gen SET a = 10 WHERE id = 1; DELETE FROM e.gen WHERE id = 2"
tx partitioned equal "INSERT INTO e.part VALUES (1, 0, 'p0'), (2, 1, 'p1'), (3, 2, 'p2'), (4, 3, 'p3'), (5, 0, 'p0'), (6, 1, 'p1'); UPDATE e.part SET k = k + 1 WHERE id IN (1, 2); DELETE FROM e.part WHERE id >= 5"
tx no-primary-key equal "INSERT INTO e.nopk VALUES (1, 'a'), (1, 'a'), (2, 'b'), (NULL, NULL); UPDATE e.nopk SET v = 'c' WHERE a = 1; DELETE FROM e.nopk WHERE a = 2"
tx upsert equal "INSERT INTO e.other VALUES (2, 'dup') ON DUPLICATE KEY UPDATE v = 'updated'; REPLACE INTO e.other VALUES (2, 'replaced'), (3, 'new')"
tx cascade equal "INSERT INTO e.parent VALUES (1), (2); INSERT INTO e.child VALUES (1, 1), (2, 1), (3, 2); DELETE FROM e.parent WHERE id = 1"
tx trigger equal "INSERT INTO e.trig VALUES (1), (2)"
tx myisam equal "INSERT INTO e.my VALUES (1, 'a'), (2, 'b'); UPDATE e.my SET v = 'c'"
tx tagged equal "SET gtid_next = 'AUTOMATIC:fx'; INSERT INTO e.other VALUES (10, 'tagged')"
tx source-compressed equal "SET SESSION binlog_transaction_compression = ON; INSERT INTO e.plain VALUES $(rows 200 211)"
# The same with the compression sides swapped: s compresses, r does not.
catchup
q r "STOP REPLICA SQL_THREAD; SET GLOBAL binlog_transaction_compression = OFF; START REPLICA SQL_THREAD"
tx only-source-compressed equal "SET SESSION binlog_transaction_compression = ON; UPDATE e.plain SET n = n + 1 WHERE id >= 200"
# Neither side compresses: the files show s cutting one statement's rows into
# fewer events (2048 bytes) than r (512 bytes).
tx uncompressed-cuts equal "INSERT INTO e.plain VALUES $(rows 300 311)"
catchup
q r "STOP REPLICA SQL_THREAD; SET GLOBAL binlog_transaction_compression = ON; START REPLICA SQL_THREAD"
# Not comparable: statements beside row changes, and JSON.
tx savepoint uncomparable "BEGIN; INSERT INTO e.other VALUES (20, 's'); SAVEPOINT a; INSERT INTO e.other VALUES (21, 's'); ROLLBACK TO SAVEPOINT a; COMMIT"
tx json uncomparable "INSERT INTO e.js VALUES (1, '{\"a\": 1, \"b\": \"$(printf 'x%.0s' {1..80})\"}', 1)"
tx json-partial uncomparable "SET SESSION binlog_row_value_options = PARTIAL_JSON; UPDATE e.js SET doc = JSON_SET(doc, '\$.b', 'short') WHERE id = 1"
tx json-neighbour uncomparable "UPDATE e.js SET n = n + 1 WHERE id = 1"
tx last-faithful equal "INSERT INTO e.other VALUES (30, 'end of phase 1')"
seal faithful   # mysql-bin.000002

# Phase 2: r's data has drifted (a write with sql_log_bin off), so its copy
# of the next change of that row has another before image. GTID, commit
# time, origin server and emptiness all agree.
q r "SET sql_log_bin = 0; UPDATE e.other SET v = 'drifted on r' WHERE id = 3"
tx drift differ "UPDATE e.other SET v = 'changed on s' WHERE id = 3"
tx after-drift equal "INSERT INTO e.other VALUES (40, 'end of phase 2')"
seal drift   # mysql-bin.000003

# Phase 3: a repair transaction injected on r under s's next GTID, with
# other row changes; r then skips s's original.
q r "STOP REPLICA"
before=$(executed s)
tx injected differ "INSERT INTO e.other VALUES (50, 'original on s')"
gtid=$(q s "SELECT REPLACE(GTID_SUBTRACT('$(executed s)', '$before'), CHAR(10), '')")
q r "SET gtid_next = '$gtid'; INSERT INTO e.other VALUES (51, 'repair on r'); SET gtid_next = 'AUTOMATIC'"
q r "START REPLICA"
tx after-injected equal "INSERT INTO e.other VALUES (60, 'end of phase 3')"
seal injected   # mysql-bin.000004
mysqld --version >"$out/server-version.txt"
/usr/libexec/mysqlsh/mysqlbinlog --version >>"$out/server-version.txt"
mysqladmin --socket=/tmp/r.sock -uroot shutdown
mysqladmin --socket=/tmp/s.sock -uroot shutdown
chmod a+r "$out"/*
