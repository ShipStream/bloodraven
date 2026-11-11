#!/usr/bin/env bash
# The mixed-version effect capture (../capture.sh effect-mixed): a MySQL 8.4
# source s and a MySQL 9.7 replica r (log_replica_updates), each in its own
# container on an internal network (no route out). Every GTID is archived
# as an origin copy in s-mysql-bin.000001.bin and as a replica copy in
# r-mysql-bin.000001.bin. cases.tsv says how the two copies' effect digests
# must compare, in the words of ../effect/capture-inner.sh; phases.tsv
# records s's gtid_executed and clock after the schema (where a backup is
# taken) and at the end.
#
# Set EFFECT_MIXED_OUT to capture somewhere else than this directory (the
# opt-in test does).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${EFFECT_MIXED_OUT:-$here}
tag=bloodraven-effect-mixed-$$
net=$tag
cleanup() {
 docker rm -f "$tag-s" "$tag-r" >/dev/null 2>&1 || true
 docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create --internal "$net" >/dev/null
docker run -d --name "$tag-s" --network "$net" --network-alias s -v "$here/server.sh:/server.sh:ro" -v "$out:/fixtures" --entrypoint bash mysql:8.4 /server.sh 101 >/dev/null
docker run -d --name "$tag-r" --network "$net" --network-alias r -v "$here/server.sh:/server.sh:ro" -v "$out:/fixtures" --entrypoint bash mysql:9.7 /server.sh 102 >/dev/null
q() { docker exec -i "$tag-$1" mysql --socket=/tmp/m.sock -uroot --batch --skip-column-names -e "$2"; }
for n in s r; do
 up=0
 for attempt in {1..90}; do
  if docker exec "$tag-$n" mysqladmin --socket=/tmp/m.sock -uroot ping --silent 2>/dev/null; then up=1; break; fi
  sleep 1
 done
 [[ $up == 1 ]] || { docker logs "$tag-$n" >&2; exit 1; }
done
executed() { q "$1" "SELECT REPLACE(@@global.gtid_executed, CHAR(10), '')"; }
catchup() { q r "SELECT WAIT_FOR_EXECUTED_GTID_SET('$(executed s)', 60)" >/dev/null; }
now() { q "$1" "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')"; }
phase() { catchup; printf '%s\t%s\t%s\n' "$1" "$(executed s)" "$(now s)" >>"$out/phases.tsv"; sleep 1.1; }
rm -f "$out/cases.tsv" "$out/phases.tsv"
tx() { # label expectation sql: one or more transactions on s, recorded by their GTIDs
 local before
 before=$(executed s)
 q s "$3"
 printf '%s\t%s\t%s\n' "$1" "$2" "$(q s "SELECT REPLACE(GTID_SUBTRACT('$(executed s)', '$before'), CHAR(10), '')")" >>"$out/cases.tsv"
}
q s "CREATE USER repl@'%' IDENTIFIED BY 'repl'; GRANT REPLICATION SLAVE ON *.* TO repl@'%'"
q r "CHANGE REPLICATION SOURCE TO SOURCE_HOST='s', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='repl', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA"
tx mixed-schema uncomparable "CREATE DATABASE m;
CREATE TABLE m.plain (id INT PRIMARY KEY, v VARCHAR(400), n INT, f FLOAT, d DOUBLE, dt DATETIME(6), g INT AS (n + 1) STORED, gv INT AS (n * 2) VIRTUAL);
CREATE TABLE m.parent (id INT PRIMARY KEY, k INT, UNIQUE (k));
CREATE TABLE m.child (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES m.parent (k) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE m.childnull (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES m.parent (k) ON DELETE SET NULL ON UPDATE SET NULL);
CREATE TABLE m.trig (id INT PRIMARY KEY);
CREATE TABLE m.audit (id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(40));
CREATE TRIGGER m.trig_ai AFTER INSERT ON m.trig FOR EACH ROW INSERT INTO m.audit (msg) VALUES (CONCAT('ins ', NEW.id));
CREATE TABLE m.part (id INT PRIMARY KEY, v INT) PARTITION BY HASH (id) PARTITIONS 4;
CREATE TABLE m.heapidx (a INT, v VARCHAR(40), KEY (a))"
phase setup
tx mixed-plain equal "INSERT INTO m.plain (id, v, n, f, d, dt) VALUES (1, 'a', 1, 1.5, 2.5, '2026-01-02 03:04:05.678901'), (2, NULL, NULL, NULL, NULL, NULL); UPDATE m.plain SET n = 5 WHERE id = 1; DELETE FROM m.plain WHERE id = 2"
tx mixed-fk-setup equal "INSERT INTO m.parent VALUES (1, 10), (2, 20), (3, 30); INSERT INTO m.child VALUES (1, 10), (2, 10), (3, 20); INSERT INTO m.childnull VALUES (1, 10), (2, 30)"
# An 8.4 source does not log the rows a cascade changes; the 9.7 replica,
# which would log them for a statement of its own, does not add them.
tx mixed-fk-update-cascade equal "UPDATE m.parent SET k = 11 WHERE id = 1"
tx mixed-fk-delete-cascade equal "DELETE FROM m.parent WHERE id = 1"
tx mixed-fk-set-null equal "DELETE FROM m.parent WHERE id = 3"
tx mixed-trigger equal "INSERT INTO m.trig VALUES (1), (2)"
tx mixed-partitioned equal "INSERT INTO m.part VALUES (1, 1), (2, 2), (3, 3); UPDATE m.part SET v = 9; DELETE FROM m.part WHERE id = 2"
# A table without a primary key: r logs the rows in its scan's order.
tx mixed-nopk-fill equal "INSERT INTO m.heapidx VALUES (5,'r5'),(3,'r3'),(8,'r8'),(1,'r1'),(7,'r7'),(2,'r2'),(6,'r6'),(4,'r4')"
tx mixed-nopk-update-key-order equal "UPDATE m.heapidx SET v = CONCAT(v, 'a')"
tx mixed-end equal "INSERT INTO m.trig VALUES (9)"
phase faithful
[[ "$(q r "SELECT COUNT(*) FROM m.child")" == "$(q s "SELECT COUNT(*) FROM m.child")" ]] || { echo "the cascade left other rows on r" >&2; exit 1; }
for n in s r; do
 old=$(q $n 'SHOW BINARY LOG STATUS' | cut -f1)
 q $n 'FLUSH BINARY LOGS'
 docker exec "$tag-$n" bash -c "cp /tmp/d/$old /fixtures/$n-$old.bin && chmod a+r /fixtures/$n-$old.bin"
done
{ q s "SELECT CONCAT('s: MySQL ', VERSION())"; q r "SELECT CONCAT('r: MySQL ', VERSION())"; } >"$out/server-version.txt"
chmod a+r "$out/cases.tsv" "$out/phases.tsv" "$out/server-version.txt"
