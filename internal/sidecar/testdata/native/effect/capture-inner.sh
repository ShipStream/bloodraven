# Runs inside mysql:9.7 (see ../capture.sh effect). Writes the effect-digest
# fixtures into /fixtures: a source s and its replica r (log_replica_updates)
# archive the same transactions, so every GTID occurs as an origin copy in
# s-mysql-bin.* and as a replica copy in r-mysql-bin.*. cases.tsv says, per
# labelled transaction, how the two copies' effect digests must compare
# (README.md, "Effect-digest pairs"):
#   equal         the same tables, layout and row bytes
#   layout        the same tables, encoded differently: not comparable
#   tables        other tables: refused
#   rows          the same tables and layout, other row bytes: refused
#   uncomparable  a copy has no digest
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
wide=$(for i in $(seq 1 300); do printf 'c%d INT, ' "$i"; done)
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
CREATE TABLE e.my (id INT PRIMARY KEY, v VARCHAR(20)) ENGINE=MyISAM;
CREATE TABLE e.heap (a INT, v VARCHAR(40));
CREATE TABLE e.heapidx (a INT, v VARCHAR(40), KEY (a));
CREATE TABLE e.uniqnull (a INT, v VARCHAR(40), UNIQUE KEY (a));
CREATE TABLE e.pk (a INT PRIMARY KEY, v VARCHAR(40));
CREATE TABLE e.img (id INT PRIMARY KEY, v VARCHAR(400), n INT, b BLOB);
CREATE TABLE e.extraidx (id INT PRIMARY KEY, v VARCHAR(40), n INT);
CREATE TABLE e.fidx (id INT PRIMARY KEY, name VARCHAR(40));
CREATE TABLE e.dflt (id INT PRIMARY KEY, v INT DEFAULT 1, w VARCHAR(10));
CREATE TABLE e.extracol (id INT PRIMARY KEY, v VARCHAR(40));
CREATE TABLE e.widen (id INT PRIMARY KEY, n INT);
CREATE TABLE e.wide (id INT PRIMARY KEY, $wide z INT);
CREATE TABLE e.fkp (id INT PRIMARY KEY, k INT, UNIQUE (k));
CREATE TABLE e.fkc (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES e.fkp (k) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE e.fkn (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES e.fkp (k) ON DELETE SET NULL ON UPDATE SET NULL);
CREATE TABLE e.fkg (id INT PRIMARY KEY, c INT, FOREIGN KEY (c) REFERENCES e.fkc (id) ON DELETE CASCADE);
CREATE TABLE e.ts (id INT PRIMARY KEY, v INT, t TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6));
CREATE TABLE e.inv (id INT PRIMARY KEY, v INT, h INT INVISIBLE DEFAULT 7);
CREATE TABLE e.ch (id INT PRIMARY KEY, c CHAR(20), vc VARCHAR(20) CHARACTER SET latin1, d DECIMAL(30,10), en ENUM('a','b'));
CREATE TABLE e.partsrc (id INT PRIMARY KEY, v INT) PARTITION BY HASH (id) PARTITIONS 4;
CREATE TABLE e.lob (id INT PRIMARY KEY, n INT, b LONGBLOB)"
# What an administrator may do on one site only, all with identical data:
# a table created while the replica generates invisible primary keys, and
# (with sql_log_bin off) more indexes, another default, a table that is not
# partitioned, a new column and a wider column.
catchup
q r "STOP REPLICA; CHANGE REPLICATION SOURCE TO REQUIRE_TABLE_PRIMARY_KEY_CHECK = GENERATE; START REPLICA"
tx schema-gipk uncomparable "CREATE TABLE e.gipk (a INT, v VARCHAR(10))"
catchup
q r "STOP REPLICA; CHANGE REPLICATION SOURCE TO REQUIRE_TABLE_PRIMARY_KEY_CHECK = STREAM; SET GLOBAL replica_type_conversions = 'ALL_NON_LOSSY'; START REPLICA"
[[ "$(q r "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'e' AND table_name = 'gipk'")" == 3 ]] || { echo "r generated no invisible primary key" >&2; exit 1; }
q r "SET sql_log_bin = 0;
ALTER TABLE e.extraidx ADD INDEX (v), ADD INDEX (n);
ALTER TABLE e.fidx ADD INDEX fx ((UPPER(name)));
ALTER TABLE e.dflt ALTER v SET DEFAULT 9, MODIFY w VARCHAR(10) NOT NULL DEFAULT 'z';
ALTER TABLE e.partsrc REMOVE PARTITIONING;
ALTER TABLE e.extracol ADD COLUMN x INT DEFAULT 5;
ALTER TABLE e.widen MODIFY n BIGINT"
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
# Tables without a primary key. r applies an UPDATE or DELETE of such a table
# by a scan and logs the rows in the order its scan finds them, which is not
# the order s changed them in: the same row images in another order. Neither
# side compresses here, so the files show it.
fill="(5,'r5'),(3,'r3'),(8,'r8'),(1,'r1'),(7,'r7'),(2,'r2'),(6,'r6'),(4,'r4')"
tx nopk-fill equal "INSERT INTO e.heap VALUES (1,'r1'),(2,'r2'),(3,'r3'),(4,'r4'),(5,'r5'),(6,'r6'),(7,'r7'),(8,'r8'); INSERT INTO e.heapidx VALUES $fill; INSERT INTO e.uniqnull VALUES $fill; INSERT INTO e.pk VALUES $fill"
tx nopk-update-key-order equal "UPDATE e.heapidx SET v = CONCAT(v, 'a')"
tx nopk-update-desc equal "UPDATE e.heap SET v = CONCAT(v, 'b') ORDER BY a DESC"
tx nopk-delete-desc equal "DELETE FROM e.heap ORDER BY a DESC LIMIT 3"
tx nopk-update-index-range equal "UPDATE e.heapidx FORCE INDEX (a) SET v = CONCAT(v, 'b') WHERE a BETWEEN 2 AND 7"
tx nopk-delete-asc equal "DELETE FROM e.heapidx ORDER BY a LIMIT 3"
tx uniqnull-update-desc equal "UPDATE e.uniqnull SET v = CONCAT(v, 'b') ORDER BY v DESC"
tx pk-update-desc equal "UPDATE e.pk SET v = CONCAT(v, 'b') ORDER BY a DESC"
tx pk-delete-desc equal "DELETE FROM e.pk ORDER BY a DESC LIMIT 3"
tx nopk-duplicates equal "INSERT INTO e.heap VALUES (1, 'd'), (1, 'd'), (1, 'd'); UPDATE e.heap SET v = 'e' WHERE v = 'd' LIMIT 2; DELETE FROM e.heap WHERE a = 1 LIMIT 1"
printf 'a,91\nb,92\n' >/tmp/load.csv
tx load-data equal "LOAD DATA INFILE '/tmp/load.csv' INTO TABLE e.heap FIELDS TERMINATED BY ',' (v, a)"
# The same table, encoded differently on the two sites: a session with
# another binlog_row_image on s, while r logs FULL images. The data is
# identical; the bytes are not.
tx img-fill equal "INSERT INTO e.img VALUES (1, 'a', 1, REPEAT('b', 600)), (2, 'b', 2, 'x'), (3, 'c', 3, NULL)"
tx row-image-minimal-update layout "SET SESSION binlog_row_image = MINIMAL; UPDATE e.img SET n = n + 1 WHERE id = 1"
tx row-image-minimal-insert layout "SET SESSION binlog_row_image = MINIMAL; INSERT INTO e.img (id, v) VALUES (10, 'min')"
tx row-image-minimal-delete layout "SET SESSION binlog_row_image = MINIMAL; DELETE FROM e.img WHERE id = 10"
tx row-image-noblob-update layout "SET SESSION binlog_row_image = NOBLOB; UPDATE e.img SET n = n + 1 WHERE id = 1"
catchup
q r "STOP REPLICA SQL_THREAD; SET GLOBAL binlog_transaction_compression = ON; START REPLICA SQL_THREAD"
# More of what a faithful replica re-logs byte for byte.
tx fk-setup equal "INSERT INTO e.fkp VALUES (1, 10), (2, 20), (3, 30); INSERT INTO e.fkc VALUES (1, 10), (2, 10), (3, 20); INSERT INTO e.fkn VALUES (1, 10), (2, 30); INSERT INTO e.fkg VALUES (1, 1), (2, 3)"
tx fk-update-cascade equal "UPDATE e.fkp SET k = 11 WHERE id = 1"
tx fk-delete-cascade equal "DELETE FROM e.fkp WHERE id = 1"
tx fk-set-null equal "DELETE FROM e.fkp WHERE id = 3"
tx fk-multi equal "BEGIN; UPDATE e.fkp SET k = 21 WHERE id = 2; DELETE FROM e.fkp WHERE id = 2; COMMIT"
tx on-update-timestamp equal "INSERT INTO e.ts (id, v) VALUES (1, 1), (2, 2); UPDATE e.ts SET v = v + 1"
tx invisible-column equal "INSERT INTO e.inv VALUES (1, 1); UPDATE e.inv SET v = 2"
tx characters equal "INSERT INTO e.ch VALUES (1, 'pad   ', CONCAT('caf', CHAR(233 USING latin1)), 123456789.0123456789, 'b'), (2, '', '', -0.0000000001, 'a'); UPDATE e.ch SET c = 'x' WHERE id = 1"
tx wide-table equal "INSERT INTO e.wide (id, c1, c300, z) VALUES (1, 1, 300, 9), (2, NULL, NULL, NULL); UPDATE e.wide SET c150 = 150 WHERE id = 1; DELETE FROM e.wide WHERE id = 2"
tx multi-table-update equal "UPDATE e.img i JOIN e.pk p ON p.a = i.id SET i.n = i.n + 1, p.v = CONCAT(p.v, 'j')"
tx multi-table-delete equal "USE e; DELETE i, p FROM img AS i JOIN pk AS p ON p.a = i.id WHERE i.id = 3"
tx insert-select equal "INSERT INTO e.other SELECT id + 100, v FROM e.img"
tx noop-update equal "UPDATE e.img SET n = n WHERE id = 1; INSERT INTO e.other VALUES (500, 'after a statement that changed nothing')"
tx rows-query equal "SET SESSION binlog_rows_query_log_events = ON; INSERT INTO e.other VALUES (600, 'rq')"
# A 300 KB value: one row image far larger than either side's event size.
# s compresses this one too, so the fixture stays small.
tx big-blob equal "SET SESSION binlog_transaction_compression = ON; INSERT INTO e.lob VALUES (1, 1, REPEAT('z', 300000)); UPDATE e.lob SET n = 2 WHERE id = 1"
# Replica-only definitions that leave the row images alone.
tx replica-indexes equal "INSERT INTO e.extraidx VALUES (1, 'a', 1), (2, 'b', 2); UPDATE e.extraidx SET n = 9 WHERE id = 1; DELETE FROM e.extraidx WHERE id = 2"
tx replica-functional-index equal "INSERT INTO e.fidx VALUES (1, 'abc'), (2, NULL); UPDATE e.fidx SET name = 'x' WHERE id = 2; DELETE FROM e.fidx WHERE id = 1"
tx replica-default equal "INSERT INTO e.dflt (id, w) VALUES (1, 'w'); UPDATE e.dflt SET v = 3"
tx source-only-partitioned equal "INSERT INTO e.partsrc VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5); UPDATE e.partsrc SET v = v + 1; DELETE FROM e.partsrc WHERE id > 3"
# The same tables with another definition on r: the data is identical, the
# row images are not.
tx replica-column layout "INSERT INTO e.extracol VALUES (1, 'a'); UPDATE e.extracol SET v = 'b' WHERE id = 1"
tx replica-wider-column layout "INSERT INTO e.widen VALUES (1, 5); UPDATE e.widen SET n = 6 WHERE id = 1"
tx replica-invisible-primary-key layout "INSERT INTO e.gipk VALUES (1, 'a'), (2, 'b'); UPDATE e.gipk SET v = 'c' WHERE a = 1"
# Not comparable: statements beside row changes, and JSON.
tx mixed-format uncomparable "SET SESSION binlog_format = MIXED; INSERT INTO e.other VALUES (700, 'mixed')"
tx statement-format uncomparable "SET SESSION binlog_format = STATEMENT; INSERT INTO e.other VALUES (701, 'stmt')"
tx create-select uncomparable "CREATE TABLE e.ctas AS SELECT * FROM e.other"
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
tx drift rows "UPDATE e.other SET v = 'changed on s' WHERE id = 3"
tx after-drift equal "INSERT INTO e.other VALUES (40, 'end of phase 2')"
seal drift   # mysql-bin.000003

# Phase 3: a repair transaction injected on r under s's next GTID, which
# changes another table; r then skips s's original.
inject() { # label expectation "sql on s" "sql on r under the same GTID"
 local before gtid
 q r "STOP REPLICA"
 before=$(executed s)
 tx "$1" "$2" "$3"
 gtid=$(q s "SELECT REPLACE(GTID_SUBTRACT('$(executed s)', '$before'), CHAR(10), '')")
 q r "SET gtid_next = '$gtid'; $4; SET gtid_next = 'AUTOMATIC'"
 q r "START REPLICA"
}
inject injected tables "INSERT INTO e.other VALUES (50, 'original on s')" "INSERT INTO e.pk VALUES (51, 'repair on r')"
tx after-injected equal "INSERT INTO e.other VALUES (60, 'end of phase 3')"
seal injected   # mysql-bin.000004

# Phase 4: the same with a repair that changes the same table: other values.
inject injected-values rows "INSERT INTO e.other VALUES (70, 'original on s')" "INSERT INTO e.other VALUES (71, 'repair on r')"
tx after-injected-values equal "INSERT INTO e.other VALUES (80, 'end of phase 4')"
seal injected-values   # mysql-bin.000005
mysqld --version >"$out/server-version.txt"
/usr/libexec/mysqlsh/mysqlbinlog --version >>"$out/server-version.txt"
mysqladmin --socket=/tmp/r.sock -uroot shutdown
mysqladmin --socket=/tmp/s.sock -uroot shutdown
chmod a+r "$out"/*
