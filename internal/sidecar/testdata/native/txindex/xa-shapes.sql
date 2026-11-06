# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261106  4:43:02 server id 1  end_log_pos 127 CRC32 0x151020a5 	Start: binlog v 4, server v 9.7.4 created 261106  4:43:02
BINLOG '
1lrtag8BAAAAewAAAH8AAAAAAAQAOS43LjQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAGlIBAV
'/*!*/;
# at 127
#261106  4:43:02 server id 1  end_log_pos 242 CRC32 0x29c552e7 	Previous-GTIDs
# 2f1f78ac-d998-11f1-87d6-5aba2ed1f094:1-15,
# cccccccc-cccc-cccc-cccc-cccccccccccc:tg:1-2
# at 242
#261106  4:43:02 server id 1  end_log_pos 321 CRC32 0x6354feee 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793940182355256	immediate_commit_timestamp=1793940182355256	transaction_length=387
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793940182355256 (2026-11-06 04:43:02.355256 UTC)
# immediate_commit_timestamp=1793940182355256 (2026-11-06 04:43:02.355256 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793940182355256*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:16'/*!*/;
# at 321
#261106  4:43:02 server id 1  end_log_pos 409 CRC32 0xd325224f 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
SET @@session.pseudo_thread_id=36/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1168113696/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=255/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255*//*!*/;
XA START X'6f70',X'',1
/*!*/;
# at 409
#261106  4:43:02 server id 1  end_log_pos 462 CRC32 0x427042d4 	Table_map: `s`.`t1` mapped to number 234
# has_generated_invisible_primary_key=0
# at 462
#261106  4:43:02 server id 1  end_log_pos 505 CRC32 0xead17e74 	Write_rows: table id 234 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
1lrtahMBAAAANQAAAM4BAAAAAOoAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/ANRCcEI=
1lrtah4BAAAAKwAAAPkBAAAAAOoAAAAAABEAAgAC/wAyAAAAAQB4dH7R6g==
'/*!*/;
# at 505
#261106  4:43:02 server id 1  end_log_pos 591 CRC32 0x375ddda8 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
XA END X'6f70',X'',1
/*!*/;
# at 591
#261106  4:43:02 server id 1  end_log_pos 629 CRC32 0x5fc3b8bd 	XA PREPARE X'6f70',X'',1
XA COMMIT X'6f70',X'',1 ONE PHASE
/*!*/;
# at 629
#261106  4:43:02 server id 1  end_log_pos 708 CRC32 0x39c3932d 	GTID	last_committed=1	sequence_number=2	rbr_only=yes	original_committed_timestamp=1793940182358234	immediate_commit_timestamp=1793940182358234	transaction_length=387
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793940182358234 (2026-11-06 04:43:02.358234 UTC)
# immediate_commit_timestamp=1793940182358234 (2026-11-06 04:43:02.358234 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793940182358234*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:17'/*!*/;
# at 708
#261106  4:43:02 server id 1  end_log_pos 796 CRC32 0x80f18e8a 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
XA START X'7262',X'',1
/*!*/;
# at 796
#261106  4:43:02 server id 1  end_log_pos 849 CRC32 0xaca390fa 	Table_map: `s`.`t1` mapped to number 234
# has_generated_invisible_primary_key=0
# at 849
#261106  4:43:02 server id 1  end_log_pos 892 CRC32 0x438cabd3 	Write_rows: table id 234 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
1lrtahMBAAAANQAAAFEDAAAAAOoAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/APqQo6w=
1lrtah4BAAAAKwAAAHwDAAAAAOoAAAAAABEAAgAC/wAzAAAAAQB406uMQw==
'/*!*/;
# at 892
#261106  4:43:02 server id 1  end_log_pos 978 CRC32 0x5080e7b6 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
XA END X'7262',X'',1
/*!*/;
# at 978
#261106  4:43:02 server id 1  end_log_pos 1016 CRC32 0x15e856b7 	XA PREPARE X'7262',X'',1
XA PREPARE X'7262',X'',1
/*!*/;
# at 1016
#261106  4:43:02 server id 1  end_log_pos 1093 CRC32 0xaf42ba45 	GTID	last_committed=2	sequence_number=3	rbr_only=no	original_committed_timestamp=1793940182362098	immediate_commit_timestamp=1793940182362098	transaction_length=168
# original_commit_timestamp=1793940182362098 (2026-11-06 04:43:02.362098 UTC)
# immediate_commit_timestamp=1793940182362098 (2026-11-06 04:43:02.362098 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793940182362098*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:18'/*!*/;
# at 1093
#261106  4:43:02 server id 1  end_log_pos 1184 CRC32 0x54c487c7 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
XA ROLLBACK X'7262',X'',1
/*!*/;
# at 1184
#261106  4:43:02 server id 1  end_log_pos 1263 CRC32 0x7121f695 	GTID	last_committed=3	sequence_number=4	rbr_only=yes	original_committed_timestamp=1793940182366957	immediate_commit_timestamp=1793940182366957	transaction_length=356
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793940182366957 (2026-11-06 04:43:02.366957 UTC)
# immediate_commit_timestamp=1793940182366957 (2026-11-06 04:43:02.366957 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793940182366957*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:19'/*!*/;
# at 1263
#261106  4:43:02 server id 1  end_log_pos 1334 CRC32 0x3ceb16ac 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
BEGIN
/*!*/;
# at 1334
#261106  4:43:02 server id 1  end_log_pos 1387 CRC32 0x19f65469 	Table_map: `s`.`t1` mapped to number 234
# has_generated_invisible_primary_key=0
# at 1387
#261106  4:43:02 server id 1  end_log_pos 1430 CRC32 0x122e8b09 	Write_rows: table id 234 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
1lrtahMBAAAANQAAAGsFAAAAAOoAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/AGlU9hk=
1lrtah4BAAAAKwAAAJYFAAAAAOoAAAAAABEAAgAC/wA1AAAAAQB4CYsuEg==
'/*!*/;
# at 1430
#261106  4:43:02 server id 1  end_log_pos 1509 CRC32 0x31b877e7 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
SAVEPOINT `a`
/*!*/;
# at 1509
#261106  4:43:02 server id 1  end_log_pos 1540 CRC32 0xe20324c4 	Xid = 491
COMMIT/*!*/;
# at 1540
#261106  4:43:02 server id 1  end_log_pos 1619 CRC32 0xd1255466 	GTID	last_committed=4	sequence_number=5	rbr_only=yes	original_committed_timestamp=1793940182371102	immediate_commit_timestamp=1793940182371102	transaction_length=306
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793940182371102 (2026-11-06 04:43:02.371102 UTC)
# immediate_commit_timestamp=1793940182371102 (2026-11-06 04:43:02.371102 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793940182371102*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:20'/*!*/;
# at 1619
#261106  4:43:02 server id 1  end_log_pos 1690 CRC32 0x5258c0c7 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
BEGIN
/*!*/;
# at 1690
#261106  4:43:02 server id 1  end_log_pos 1734 CRC32 0x91f3c0b6 	Table_map: `s`.`m` mapped to number 209
# has_generated_invisible_primary_key=0
# at 1734
#261106  4:43:02 server id 1  end_log_pos 1774 CRC32 0x637f1b76 	Write_rows: table id 209 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
1lrtahMBAAAALAAAAMYGAAAAANEAAAAAAAEAAXMAAW0AAQMAAQEBALbA85E=
1lrtah4BAAAAKAAAAO4GAAAAANEAAAAAABEAAgAB/wACAAAAdht/Yw==
'/*!*/;
# at 1774
#261106  4:43:02 server id 1  end_log_pos 1846 CRC32 0xfd1d043e 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
COMMIT
/*!*/;
# at 1846
#261106  4:43:02 server id 1  end_log_pos 1925 CRC32 0x4e6b3d80 	GTID	last_committed=5	sequence_number=6	rbr_only=yes	original_committed_timestamp=1793940182374892	immediate_commit_timestamp=1793940182374892	transaction_length=277
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793940182374892 (2026-11-06 04:43:02.374892 UTC)
# immediate_commit_timestamp=1793940182374892 (2026-11-06 04:43:02.374892 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793940182374892*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:21'/*!*/;
# at 1925
#261106  4:43:02 server id 1  end_log_pos 1996 CRC32 0x5a6d73d2 	Query	thread_id=36	exec_time=0	error_code=0
SET TIMESTAMP=1793940182/*!*/;
BEGIN
/*!*/;
# at 1996
#261106  4:43:02 server id 1  end_log_pos 2049 CRC32 0x3f93e848 	Table_map: `s`.`t1` mapped to number 234
# has_generated_invisible_primary_key=0
# at 2049
#261106  4:43:02 server id 1  end_log_pos 2092 CRC32 0xc0ad967f 	Write_rows: table id 234 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
1lrtahMBAAAANQAAAAEIAAAAAOoAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/AEjokz8=
1lrtah4BAAAAKwAAACwIAAAAAOoAAAAAABEAAgAC/wA3AAAAAQB4f5atwA==
'/*!*/;
# at 2092
#261106  4:43:02 server id 1  end_log_pos 2123 CRC32 0xa09641ed 	Xid = 499
COMMIT/*!*/;
# at 2123
#261106  4:43:02 server id 1  end_log_pos 2167 CRC32 0x142a5f47 	Rotate to binlog.000006  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
