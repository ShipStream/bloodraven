# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261106  4:11:55 server id 1  end_log_pos 127 CRC32 0xc2ad2c72 	Start: binlog v 4, server v 9.7.4 created 261106  4:11:55 at startup
ROLLBACK/*!*/;
BINLOG '
i1Ptag8BAAAAewAAAH8AAAAAAAQAOS43LjQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAACLU+1qEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAFyLK3C
'/*!*/;
# at 127
#261106  4:11:55 server id 1  end_log_pos 158 CRC32 0x1d065782 	Previous-GTIDs
# [empty]
# at 158
#261106  4:11:55 server id 1  end_log_pos 235 CRC32 0x883f9886 	GTID	last_committed=0	sequence_number=1	rbr_only=no	original_committed_timestamp=1793938315509493	immediate_commit_timestamp=1793938315509493	transaction_length=176
# original_commit_timestamp=1793938315509493 (2026-11-06 04:11:55.509493 UTC)
# immediate_commit_timestamp=1793938315509493 (2026-11-06 04:11:55.509493 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315509493*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:1'/*!*/;
# at 235
#261106  4:11:55 server id 1  end_log_pos 334 CRC32 0x8fc45231 	Query	thread_id=10	exec_time=0	error_code=0	Xid = 9
SET TIMESTAMP=1793938315/*!*/;
SET @@session.pseudo_thread_id=10/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1168113696/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=255/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255*//*!*/;
/*!80016 SET @@session.default_table_encryption=0*//*!*/;
CREATE DATABASE s
/*!*/;
# at 334
#261106  4:11:55 server id 1  end_log_pos 411 CRC32 0x66d4949d 	GTID	last_committed=1	sequence_number=2	rbr_only=no	original_committed_timestamp=1793938315523032	immediate_commit_timestamp=1793938315523032	transaction_length=211
# original_commit_timestamp=1793938315523032 (2026-11-06 04:11:55.523032 UTC)
# immediate_commit_timestamp=1793938315523032 (2026-11-06 04:11:55.523032 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315523032*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:2'/*!*/;
# at 411
#261106  4:11:55 server id 1  end_log_pos 545 CRC32 0xa13925c7 	Query	thread_id=10	exec_time=0	error_code=0	Xid = 10
SET TIMESTAMP=1793938315/*!*/;
/*!80013 SET @@session.sql_require_primary_key=0*//*!*/;
CREATE TABLE s.t1(id INT PRIMARY KEY, v VARCHAR(100))
/*!*/;
# at 545
#261106  4:11:55 server id 1  end_log_pos 624 CRC32 0x7e26b993 	GTID	last_committed=2	sequence_number=3	rbr_only=yes	original_committed_timestamp=1793938315531479	immediate_commit_timestamp=1793938315531479	transaction_length=285
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793938315531479 (2026-11-06 04:11:55.531479 UTC)
# immediate_commit_timestamp=1793938315531479 (2026-11-06 04:11:55.531479 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315531479*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:3'/*!*/;
# at 624
#261106  4:11:55 server id 1  end_log_pos 695 CRC32 0x13b5bdd2 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 695
#261106  4:11:55 server id 1  end_log_pos 748 CRC32 0x7fc79541 	Table_map: `s`.`t1` mapped to number 101
# has_generated_invisible_primary_key=0
# at 748
#261106  4:11:55 server id 1  end_log_pos 799 CRC32 0x96354bba 	Write_rows: table id 101 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAAOwCAAAAAGUAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/AEGVx38=
i1Ptah4BAAAAMwAAAB8DAAAAAGUAAAAAABEAAgAC/wABAAAAAQBhAAIAAAABAGK6SzWW
'/*!*/;
# at 799
#261106  4:11:55 server id 1  end_log_pos 830 CRC32 0x20c60ac8 	Xid = 11
COMMIT/*!*/;
# at 830
#261106  4:11:55 server id 1  end_log_pos 909 CRC32 0x91249f65 	GTID	last_committed=3	sequence_number=4	rbr_only=no	original_committed_timestamp=1793938315545203	immediate_commit_timestamp=1793938315545203	transaction_length=449
# original_commit_timestamp=1793938315545203 (2026-11-06 04:11:55.545203 UTC)
# immediate_commit_timestamp=1793938315545203 (2026-11-06 04:11:55.545203 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315545203*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:4'/*!*/;
# at 909
#261106  4:11:55 server id 1  end_log_pos 980 CRC32 0x325cac09 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 980
#261106  4:11:55 server id 1  end_log_pos 1144 CRC32 0x975221ea 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
/*!80013 SET @@session.sql_require_primary_key=0*//*!*/;
CREATE TABLE `s`.`t2` (
  `id` int NOT NULL,
  `v` varchar(100) DEFAULT NULL
) START TRANSACTION
/*!*/;
# at 1144
#261106  4:11:55 server id 1  end_log_pos 1197 CRC32 0x3edc23c1 	Table_map: `s`.`t2` mapped to number 103
# has_generated_invisible_primary_key=0
# at 1197
#261106  4:11:55 server id 1  end_log_pos 1248 CRC32 0x1bacfc1a 	Write_rows: table id 103 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAAK0EAAAAAGcAAAAAAAEAAXMAAnQyAAIDDwKQAQIBAQACA/z/AMEj3D4=
i1Ptah4BAAAAMwAAAOAEAAAAAGcAAAAAABEAAgAC/wABAAAAAQBhAAIAAAABAGIa/Kwb
'/*!*/;
# at 1248
#261106  4:11:55 server id 1  end_log_pos 1279 CRC32 0x08b5fcec 	Xid = 12
COMMIT/*!*/;
# at 1279
#261106  4:11:55 server id 1  end_log_pos 1356 CRC32 0x7ca54444 	GTID	last_committed=4	sequence_number=5	rbr_only=no	original_committed_timestamp=1793938315549711	immediate_commit_timestamp=1793938315549711	transaction_length=187
# original_commit_timestamp=1793938315549711 (2026-11-06 04:11:55.549711 UTC)
# immediate_commit_timestamp=1793938315549711 (2026-11-06 04:11:55.549711 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315549711*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:5'/*!*/;
# at 1356
#261106  4:11:55 server id 1  end_log_pos 1466 CRC32 0x7e46ebca 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
/*!80013 SET @@session.sql_require_primary_key=0*//*!*/;
CREATE TABLE s.m(id INT) ENGINE=MyISAM
/*!*/;
# at 1466
#261106  4:11:55 server id 1  end_log_pos 1545 CRC32 0x0a40d0e5 	GTID	last_committed=5	sequence_number=6	rbr_only=yes	original_committed_timestamp=1793938315551993	immediate_commit_timestamp=1793938315551993	transaction_length=306
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793938315551993 (2026-11-06 04:11:55.551993 UTC)
# immediate_commit_timestamp=1793938315551993 (2026-11-06 04:11:55.551993 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315551993*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:6'/*!*/;
# at 1545
#261106  4:11:55 server id 1  end_log_pos 1616 CRC32 0xb8d5a6d5 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 1616
#261106  4:11:55 server id 1  end_log_pos 1660 CRC32 0x6508a194 	Table_map: `s`.`m` mapped to number 104
# has_generated_invisible_primary_key=0
# at 1660
#261106  4:11:55 server id 1  end_log_pos 1700 CRC32 0x711ab59a 	Write_rows: table id 104 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAALAAAAHwGAAAAAGgAAAAAAAEAAXMAAW0AAQMAAQEBAJShCGU=
i1Ptah4BAAAAKAAAAKQGAAAAAGgAAAAAABEAAgAB/wABAAAAmrUacQ==
'/*!*/;
# at 1700
#261106  4:11:55 server id 1  end_log_pos 1772 CRC32 0x42436a65 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
COMMIT
/*!*/;
# at 1772
#261106  4:11:55 server id 1  end_log_pos 1851 CRC32 0x3f19e9fe 	GTID	last_committed=6	sequence_number=7	rbr_only=no	original_committed_timestamp=1793938315560381	immediate_commit_timestamp=1793938315560381	transaction_length=329
# original_commit_timestamp=1793938315560381 (2026-11-06 04:11:55.560381 UTC)
# immediate_commit_timestamp=1793938315560381 (2026-11-06 04:11:55.560381 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315560381*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:7'/*!*/;
# at 1851
#261106  4:11:55 server id 1  end_log_pos 2101 CRC32 0x7be7328a 	Query	thread_id=10	exec_time=0	error_code=0	Xid = 16
SET TIMESTAMP=1793938315/*!*/;
CREATE DEFINER=`root`@`localhost` PROCEDURE `s`.`p`()
BEGIN SELECT 'line1
SET @@SESSION.GTID_NEXT= ''bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:9''/*!*/;
x'; END
/*!*/;
# at 2101
#261106  4:11:55 server id 1  end_log_pos 2180 CRC32 0x3c482b38 	GTID	last_committed=7	sequence_number=8	rbr_only=yes	original_committed_timestamp=1793938315564117	immediate_commit_timestamp=1793938315564117	transaction_length=387
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793938315564117 (2026-11-06 04:11:55.564117 UTC)
# immediate_commit_timestamp=1793938315564117 (2026-11-06 04:11:55.564117 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315564117*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:8'/*!*/;
# at 2180
#261106  4:11:55 server id 1  end_log_pos 2268 CRC32 0xd0175b63 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
XA START X'7831',X'',1
/*!*/;
# at 2268
#261106  4:11:55 server id 1  end_log_pos 2321 CRC32 0x4f4b19c2 	Table_map: `s`.`t1` mapped to number 101
# has_generated_invisible_primary_key=0
# at 2321
#261106  4:11:55 server id 1  end_log_pos 2364 CRC32 0x7fd14554 	Write_rows: table id 101 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAABEJAAAAAGUAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/AMIZS08=
i1Ptah4BAAAAKwAAADwJAAAAAGUAAAAAABEAAgAC/wADAAAAAQBjVEXRfw==
'/*!*/;
# at 2364
#261106  4:11:55 server id 1  end_log_pos 2450 CRC32 0x56ab42a9 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
XA END X'7831',X'',1
/*!*/;
# at 2450
#261106  4:11:55 server id 1  end_log_pos 2488 CRC32 0x1e00a7a6 	XA PREPARE X'7831',X'',1
XA PREPARE X'7831',X'',1
/*!*/;
# at 2488
#261106  4:11:55 server id 1  end_log_pos 2565 CRC32 0x05efda30 	GTID	last_committed=8	sequence_number=9	rbr_only=no	original_committed_timestamp=1793938315566361	immediate_commit_timestamp=1793938315566361	transaction_length=166
# original_commit_timestamp=1793938315566361 (2026-11-06 04:11:55.566361 UTC)
# immediate_commit_timestamp=1793938315566361 (2026-11-06 04:11:55.566361 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315566361*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:9'/*!*/;
# at 2565
#261106  4:11:55 server id 1  end_log_pos 2654 CRC32 0x06078efe 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
XA COMMIT X'7831',X'',1
/*!*/;
# at 2654
#261106  4:11:55 server id 1  end_log_pos 2731 CRC32 0x9724c821 	GTID	last_committed=9	sequence_number=10	rbr_only=yes	original_committed_timestamp=1793938315572718	immediate_commit_timestamp=1793938315572718	transaction_length=240
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793938315572718 (2026-11-06 04:11:55.572718 UTC)
# immediate_commit_timestamp=1793938315572718 (2026-11-06 04:11:55.572718 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315572718*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:10'/*!*/;
# at 2731
#261106  4:11:55 server id 1  end_log_pos 2894 CRC32 0x5b6dfa60 	Transaction_Payload		payload_size=130	compression_type=ZSTD	uncompressed_size=182
# Start of compressed events.
# at 2894
#261106  4:11:55 server id 1  end_log_pos 2894 CRC32 0x1647f7f2 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 2894
#261106  4:11:55 server id 1  end_log_pos 2894 CRC32 0x3ea119c6 	Table_map: `s`.`t1` mapped to number 101
# has_generated_invisible_primary_key=0
# at 2894
#261106  4:11:55 server id 1  end_log_pos 2894 CRC32 0xf7a8bd6b 	Write_rows: table id 101 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAAAAAAAAAAGUAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/AMYZoT4=
i1Ptah4BAAAAKwAAAAAAAAAAAGUAAAAAABEAAgAC/wAEAAAAAQBka72o9w==
'/*!*/;
# at 2894
#261106  4:11:55 server id 1  end_log_pos 2894 CRC32 0xd6555356 	Xid = 23
COMMIT/*!*/;
# End of compressed events.
# at 2894
#261106  4:11:55 server id 1  end_log_pos 2973 CRC32 0x5cfeaa84 	GTID	last_committed=10	sequence_number=11	rbr_only=no	original_committed_timestamp=1793938315587204	immediate_commit_timestamp=1793938315587204	transaction_length=465
# original_commit_timestamp=1793938315587204 (2026-11-06 04:11:55.587204 UTC)
# immediate_commit_timestamp=1793938315587204 (2026-11-06 04:11:55.587204 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315587204*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:11'/*!*/;
# at 2973
#261106  4:11:55 server id 1  end_log_pos 3044 CRC32 0x5c948f23 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 3044
#261106  4:11:55 server id 1  end_log_pos 3208 CRC32 0x79823ee5 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
/*!80013 SET @@session.sql_require_primary_key=0*//*!*/;
CREATE TABLE `s`.`t3` (
  `id` int NOT NULL,
  `v` varchar(100) DEFAULT NULL
) START TRANSACTION
/*!*/;
# at 3208
#261106  4:11:55 server id 1  end_log_pos 3261 CRC32 0xeea910c2 	Table_map: `s`.`t3` mapped to number 108
# has_generated_invisible_primary_key=0
# at 3261
#261106  4:11:55 server id 1  end_log_pos 3328 CRC32 0x1def867a 	Write_rows: table id 108 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAAL0MAAAAAGwAAAAAAAEAAXMAAnQzAAIDDwKQAQIBAQACA/z/AMIQqe4=
i1Ptah4BAAAAQwAAAAANAAAAAGwAAAAAABEAAgAC/wABAAAAAQBhAAIAAAABAGIAAwAAAAEAYwAE
AAAAAQBkeobvHQ==
'/*!*/;
# at 3328
#261106  4:11:55 server id 1  end_log_pos 3359 CRC32 0x787fa91e 	Xid = 24
COMMIT/*!*/;
# at 3359
#261106  4:11:55 server id 1  end_log_pos 3446 CRC32 0x176c9e89 	GTID	last_committed=11	sequence_number=12	rbr_only=no	original_committed_timestamp=1793938315591200	immediate_commit_timestamp=1793938315591200	transaction_length=230
# original_commit_timestamp=1793938315591200 (2026-11-06 04:11:55.591200 UTC)
# immediate_commit_timestamp=1793938315591200 (2026-11-06 04:11:55.591200 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315591200*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= 'cccccccc-cccc-cccc-cccc-cccccccccccc:tg:1'/*!*/;
# at 3446
#261106  4:11:55 server id 1  end_log_pos 3517 CRC32 0xfe8e3a02 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 3517
#261106  4:11:55 server id 1  end_log_pos 3589 CRC32 0xd0524014 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
COMMIT
/*!*/;
# at 3589
#261106  4:11:55 server id 1  end_log_pos 3676 CRC32 0x1d8128ba 	GTID	last_committed=11	sequence_number=13	rbr_only=yes	original_committed_timestamp=1793938315597557	immediate_commit_timestamp=1793938315597557	transaction_length=285
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793938315597557 (2026-11-06 04:11:55.597557 UTC)
# immediate_commit_timestamp=1793938315597557 (2026-11-06 04:11:55.597557 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315597557*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= 'cccccccc-cccc-cccc-cccc-cccccccccccc:tg:2'/*!*/;
# at 3676
#261106  4:11:55 server id 1  end_log_pos 3747 CRC32 0x6f4dc9a5 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793938315/*!*/;
BEGIN
/*!*/;
# at 3747
#261106  4:11:55 server id 1  end_log_pos 3800 CRC32 0xfdfa9fd4 	Table_map: `s`.`t1` mapped to number 101
# has_generated_invisible_primary_key=0
# at 3800
#261106  4:11:55 server id 1  end_log_pos 3843 CRC32 0x36d404ae 	Write_rows: table id 101 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAANgOAAAAAGUAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/ANSf+v0=
i1Ptah4BAAAAKwAAAAMPAAAAAGUAAAAAABEAAgAC/wAFAAAAAQBlrgTUNg==
'/*!*/;
# at 3843
#261106  4:11:55 server id 1  end_log_pos 3874 CRC32 0xc260213d 	Xid = 30
COMMIT/*!*/;
# at 3874
#261106  4:11:55 server id 1  end_log_pos 3918 CRC32 0x466adc8d 	Rotate to binlog.000002  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
