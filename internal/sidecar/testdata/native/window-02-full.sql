# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0xc6f283fe 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35 at startup
ROLLBACK/*!*/;
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAABjVOJqEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAH+g/LG
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 158 CRC32 0x7b101ae1 	Previous-GTIDs
# [empty]
# at 158
#261028 20:00:35 server id 101  end_log_pos 205 CRC32 0xdb195a31 	Rotate to mysql-bin.000002  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0x871a3223 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEjMhqH
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 158 CRC32 0x7b101ae1 	Previous-GTIDs
# [empty]
# at 158
#261028 20:00:35 server id 101  end_log_pos 235 CRC32 0xcae9c55e 	GTID	last_committed=0	sequence_number=1	rbr_only=no	original_committed_timestamp=1793217635582788	immediate_commit_timestamp=1793217635582788	transaction_length=191
# original_commit_timestamp=1793217635582788 (2026-10-28 20:00:35.582788 UTC)
# immediate_commit_timestamp=1793217635582788 (2026-10-28 20:00:35.582788 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635582788*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1'/*!*/;
# at 235
#261028 20:00:35 server id 101  end_log_pos 349 CRC32 0xc9309ae9 	Query	thread_id=11	exec_time=0	error_code=0	Xid = 18
SET TIMESTAMP=1793217635/*!*/;
SET @@session.pseudo_thread_id=11/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1168113696/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=255/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255*//*!*/;
/*!80016 SET @@session.default_table_encryption=0*//*!*/;
CREATE DATABASE replay
/*!*/;
# at 349
#261028 20:00:35 server id 101  end_log_pos 426 CRC32 0x87d510e1 	GTID	last_committed=1	sequence_number=2	rbr_only=no	original_committed_timestamp=1793217635600819	immediate_commit_timestamp=1793217635600819	transaction_length=229
# original_commit_timestamp=1793217635600819 (2026-10-28 20:00:35.600819 UTC)
# immediate_commit_timestamp=1793217635600819 (2026-10-28 20:00:35.600819 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635600819*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:2'/*!*/;
# at 426
#261028 20:00:35 server id 101  end_log_pos 578 CRC32 0xf6a8614a 	Query	thread_id=11	exec_time=0	error_code=0	Xid = 19
SET TIMESTAMP=1793217635/*!*/;
/*!80013 SET @@session.sql_require_primary_key=0*//*!*/;
CREATE TABLE replay.markers(id INT PRIMARY KEY, value VARCHAR(40))
/*!*/;
# at 578
#261028 20:00:35 server id 101  end_log_pos 657 CRC32 0xcad0396a 	GTID	last_committed=2	sequence_number=3	rbr_only=yes	original_committed_timestamp=1793217635616803	immediate_commit_timestamp=1793217635616803	transaction_length=292
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793217635616803 (2026-10-28 20:00:35.616803 UTC)
# immediate_commit_timestamp=1793217635616803 (2026-10-28 20:00:35.616803 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635616803*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:3'/*!*/;
# at 657
#261028 20:00:35 server id 101  end_log_pos 728 CRC32 0x72762180 	Query	thread_id=11	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
BEGIN
/*!*/;
# at 728
#261028 20:00:35 server id 101  end_log_pos 791 CRC32 0xeb86b3e2 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 791
#261028 20:00:35 server id 101  end_log_pos 839 CRC32 0x2c1b920a 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
Y1TiahNlAAAAPwAAABcDAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACAw8CoAACAQEAAgP8
/wDis4br
Y1Tiah5lAAAAMAAAAEcDAAAAAGoAAAAAAAEAAgAC/wABAAAAB2luaXRpYWwKkhss
'/*!*/;
# at 839
#261028 20:00:35 server id 101  end_log_pos 870 CRC32 0xf719b187 	Xid = 20
COMMIT/*!*/;
# at 870
#261028 20:00:35 server id 101  end_log_pos 917 CRC32 0x594dd581 	Rotate to mysql-bin.000003  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0x871a3223 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEjMhqH
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 198 CRC32 0xd2ea4ec6 	Previous-GTIDs
# 3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1-3
# at 198
#261028 20:00:35 server id 101  end_log_pos 245 CRC32 0x9b38a503 	Rotate to mysql-bin.000004  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0x871a3223 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEjMhqH
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 198 CRC32 0xd2ea4ec6 	Previous-GTIDs
# 3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1-3
# at 198
#261028 20:00:35 server id 101  end_log_pos 277 CRC32 0x69582e02 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793217635671221	immediate_commit_timestamp=1793217635671221	transaction_length=422
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793217635671221 (2026-10-28 20:00:35.671221 UTC)
# immediate_commit_timestamp=1793217635671221 (2026-10-28 20:00:35.671221 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635671221*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:4'/*!*/;
# at 277
#261028 20:00:35 server id 101  end_log_pos 375 CRC32 0x27ffdec6 	Query	thread_id=18	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
XA START X'63617074757265',X'',1
/*!*/;
# at 375
#261028 20:00:35 server id 101  end_log_pos 438 CRC32 0xa0132ca7 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 438
#261028 20:00:35 server id 101  end_log_pos 481 CRC32 0x071ab7a9 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
Y1TiahNlAAAAPwAAALYBAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACAw8CoAACAQEAAgP8
/wCnLBOg
Y1Tiah5lAAAAKwAAAOEBAAAAAGoAAAAAAAEAAgAC/wACAAAAAnhhqbcaBw==
'/*!*/;
# at 481
#261028 20:00:35 server id 101  end_log_pos 577 CRC32 0x84268470 	Query	thread_id=18	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
XA END X'63617074757265',X'',1
/*!*/;
# at 577
#261028 20:00:35 server id 101  end_log_pos 620 CRC32 0xb57fdbc2 	XA PREPARE X'63617074757265',X'',1
XA PREPARE X'63617074757265',X'',1
/*!*/;
# at 620
#261028 20:00:35 server id 101  end_log_pos 667 CRC32 0x621dfda1 	Rotate to mysql-bin.000005  pos: 4
ROLLBACK /* added by mysqlbinlog */ /*!*/;
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0x871a3223 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEjMhqH
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 198 CRC32 0x1e404e58 	Previous-GTIDs
# 3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1-4
# at 198
#261028 20:00:35 server id 101  end_log_pos 275 CRC32 0x1bf8f3ac 	GTID	last_committed=0	sequence_number=1	rbr_only=no	original_committed_timestamp=1793217635698607	immediate_commit_timestamp=1793217635698607	transaction_length=176
# original_commit_timestamp=1793217635698607 (2026-10-28 20:00:35.698607 UTC)
# immediate_commit_timestamp=1793217635698607 (2026-10-28 20:00:35.698607 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635698607*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:5'/*!*/;
# at 275
#261028 20:00:35 server id 101  end_log_pos 374 CRC32 0xd4f5e4b1 	Query	thread_id=22	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
XA COMMIT X'63617074757265',X'',1
/*!*/;
# at 374
#261028 20:00:35 server id 101  end_log_pos 421 CRC32 0x7e1cd740 	Rotate to mysql-bin.000006  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0x871a3223 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEjMhqH
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 198 CRC32 0x90cf49bb 	Previous-GTIDs
# 3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1-5
# at 198
#261028 20:00:35 server id 101  end_log_pos 277 CRC32 0xfc820631 	GTID	last_committed=0	sequence_number=1	rbr_only=no	original_committed_timestamp=1793217635728269	immediate_commit_timestamp=1793217635728269	transaction_length=485
# original_commit_timestamp=1793217635728269 (2026-10-28 20:00:35.728269 UTC)
# immediate_commit_timestamp=1793217635728269 (2026-10-28 20:00:35.728269 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635728269*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:6'/*!*/;
# at 277
#261028 20:00:35 server id 101  end_log_pos 357 CRC32 0x1113d0f8 	Query	thread_id=26	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
BEGIN
/*!*/;
# at 357
#261028 20:00:35 server id 101  end_log_pos 396 CRC32 0xc8fef0f4 
#Begin_load_query: file_id: 1  block_len: 12
# at 396
#261028 20:00:35 server id 101  end_log_pos 652 CRC32 0x98e2ec51 	Execute_load_query	thread_id=26	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
LOAD DATA LOCAL INFILE '/tmp/SQL_LOAD_MB-1-0' INTO TABLE `replay`.`markers` FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\' LINES TERMINATED BY '\n' (`id`, `value`)
/*!*/;
# file_id: 1 
# at 652
#261028 20:00:35 server id 101  end_log_pos 683 CRC32 0x7622c2a7 	Xid = 84
COMMIT/*!*/;
# at 683
#261028 20:00:35 server id 101  end_log_pos 730 CRC32 0xa8b289cc 	Rotate to mysql-bin.000007  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 CRC32 0x871a3223 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEjMhqH
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 198 CRC32 0x5c654925 	Previous-GTIDs
# 3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1-6
# at 198
#261028 20:00:35 server id 101  end_log_pos 277 CRC32 0xa317e0d7 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793217635760915	immediate_commit_timestamp=1793217635760915	transaction_length=325
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793217635760915 (2026-10-28 20:00:35.760915 UTC)
# immediate_commit_timestamp=1793217635760915 (2026-10-28 20:00:35.760915 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635760915*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:7'/*!*/;
# at 277
#261028 20:00:35 server id 101  end_log_pos 348 CRC32 0x83d71275 	Query	thread_id=30	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
BEGIN
/*!*/;
# at 348
#261028 20:00:35 server id 101  end_log_pos 411 CRC32 0x4ae9490a 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 411
#261028 20:00:35 server id 101  end_log_pos 492 CRC32 0x5109fa9d 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
Y1TiahNlAAAAPwAAAJsBAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACAw8CoAACAQEAAgP8
/wAKSelK
Y1Tiah5lAAAAUQAAAOwBAAAAAGoAAAAAAAEAAgAC/wADAAAAKHp6enp6enp6enp6enp6enp6enp6
enp6enp6enp6enp6enp6enp6enqd+glR
'/*!*/;
# at 492
#261028 20:00:35 server id 101  end_log_pos 523 CRC32 0x7152067e 	Xid = 101
COMMIT/*!*/;
# at 523
#261028 20:00:35 server id 101  end_log_pos 570 CRC32 0xf097f682 	Rotate to mysql-bin.000008  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
# at 4
#261028 20:00:35 server id 101  end_log_pos 127 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:35
BINLOG '
Y1Tiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAC1Ah3w
'/*!*/;
# at 127
#261028 20:00:35 server id 101  end_log_pos 194 	Previous-GTIDs
# 3acfc321-d30a-11f1-9ef3-7dd2a8d22646:1-7
# at 194
#261028 20:00:35 server id 101  end_log_pos 269 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793217635805092	immediate_commit_timestamp=1793217635805092	transaction_length=256
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793217635805092 (2026-10-28 20:00:35.805092 UTC)
# immediate_commit_timestamp=1793217635805092 (2026-10-28 20:00:35.805092 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217635805092*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3acfc321-d30a-11f1-9ef3-7dd2a8d22646:8'/*!*/;
# at 269
#261028 20:00:35 server id 101  end_log_pos 450 	Transaction_Payload		payload_size=152	compression_type=ZSTD	uncompressed_size=202
# Start of compressed events.
# at 450
#261028 20:00:35 server id 101  end_log_pos 450 	Query	thread_id=35	exec_time=0	error_code=0
SET TIMESTAMP=1793217635/*!*/;
BEGIN
/*!*/;
# at 450
#261028 20:00:35 server id 101  end_log_pos 450 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 450
#261028 20:00:35 server id 101  end_log_pos 450 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
Y1TiahNlAAAAOwAAAAAAAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACAw8CoAACAQEAAgP8
/wA=
Y1Tiah5lAAAAMQAAAAAAAAAAAGoAAAAAAAEAAgAC/wAEAAAADGNoZWNrc3VtLW9mZg==
'/*!*/;
# at 450
#261028 20:00:35 server id 101  end_log_pos 450 	Xid = 121
COMMIT/*!*/;
# End of compressed events.
# at 450
#261028 20:00:35 server id 101  end_log_pos 493 	Rotate to mysql-bin.000010  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
