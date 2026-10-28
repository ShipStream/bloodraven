# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
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
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
