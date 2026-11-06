# Index-pass structure captures (MySQL 9.7.4)

Captured from a disposable `mysql:9.7` server (9.7.4, `--gtid-mode=ON
--enforce-gtid-consistency=ON`, row format, CRC32 checksums) for the planner's
index pass (`InspectTransactions`). The `.sql` files are the bundled
`/usr/libexec/mysqlsh/mysqlbinlog` output of each file.

`structures.bin` holds, in order: DDL statements (no BEGIN), a row
transaction (BEGIN … XID), an atomic `CREATE TABLE … AS SELECT` (BEGIN, the
`CREATE … START TRANSACTION` statement, its rows, XID), a MyISAM write (BEGIN
… COMMIT), a `CREATE PROCEDURE` whose string literal holds a line that starts
`SET @@SESSION.GTID_NEXT=`, an XA transaction (XA START … XA PREPARE, then
XA COMMIT), a compressed row transaction and a compressed atomic CTAS, and
two transactions with the tagged GTIDs `cccccccc-…:tg:1-2` of a UUID with no
untagged GTIDs (an empty one and a row one).

`tagged-only-previous.bin` is the next file: its Previous_gtids is
`<server uuid>:1-11,cccccccc-cccc-cccc-cccc-cccccccccccc:tg:1-2`, a tagged
source whose UUID has no untagged GTIDs, which go-mysql v1.16.0's decoder
attributes to the server UUID. It holds one row transaction compressed at
`binlog_transaction_compression_level_zstd=22` (a single-segment zstd frame
whose content size is the payload's uncompressed size).

The session that wrote them:

    CREATE DATABASE s; CREATE TABLE s.t1(id INT PRIMARY KEY, v VARCHAR(100));
    INSERT INTO s.t1 VALUES (1,'a'),(2,'b');
    CREATE TABLE s.t2 AS SELECT * FROM s.t1;
    CREATE TABLE s.m(id INT) ENGINE=MyISAM; INSERT INTO s.m VALUES (1);
    CREATE PROCEDURE s.p() BEGIN SELECT 'line1
    SET @@SESSION.GTID_NEXT= ''bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:9''/*!*/;
    x'; END
    XA START 'x1'; INSERT INTO s.t1 VALUES (3,'c'); XA END 'x1'; XA PREPARE 'x1'; XA COMMIT 'x1';
    SET SESSION binlog_transaction_compression=ON;
    INSERT INTO s.t1 VALUES (4,'d'); CREATE TABLE s.t3 AS SELECT * FROM s.t1;
    SET SESSION binlog_transaction_compression=OFF;
    SET GTID_NEXT='cccccccc-cccc-cccc-cccc-cccccccccccc:tg:1'; BEGIN; COMMIT;
    SET GTID_NEXT='cccccccc-cccc-cccc-cccc-cccccccccccc:tg:2'; INSERT INTO s.t1 VALUES (5,'e');
    SET GTID_NEXT=AUTOMATIC; FLUSH BINARY LOGS;
    SET SESSION binlog_transaction_compression=ON;
    SET SESSION binlog_transaction_compression_level_zstd=22;
    INSERT INTO s.t1 VALUES (6,'f');
    FLUSH BINARY LOGS;

`xa-shapes.bin` holds a one-phase XA commit (logged as an XA_PREPARE event
with the one-phase flag), an XA PREPARE / XA ROLLBACK pair, a transaction
with `SAVEPOINT` / `ROLLBACK TO SAVEPOINT`, and a MyISAM-plus-InnoDB
transaction (logged as two transactions); an XA ROLLBACK of an unprepared
branch and temporary tables log nothing. The session:

    XA START 'op'; INSERT INTO s.t1 VALUES (50,'x'); XA END 'op'; XA COMMIT 'op' ONE PHASE;
    XA START 'rb'; INSERT INTO s.t1 VALUES (51,'x'); XA END 'rb'; XA PREPARE 'rb'; XA ROLLBACK 'rb';
    XA START 'r1'; INSERT INTO s.t1 VALUES (52,'x'); XA END 'r1'; XA ROLLBACK 'r1';
    BEGIN; INSERT INTO s.t1 VALUES (53,'x'); SAVEPOINT a; INSERT INTO s.t1 VALUES (54,'x');
    ROLLBACK TO SAVEPOINT a; COMMIT;
    CREATE TEMPORARY TABLE s.tmp(id INT); DROP TEMPORARY TABLE s.tmp;
    BEGIN; INSERT INTO s.m VALUES (2); INSERT INTO s.t1 VALUES (55,'x'); COMMIT;
    FLUSH BINARY LOGS;
