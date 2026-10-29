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
SET @@session.pseudo_thread_id=22/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1168113696/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=255/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255*//*!*/;
XA COMMIT X'63617074757265',X'',1
/*!*/;
# at 374
#261028 20:00:35 server id 101  end_log_pos 421 CRC32 0x7e1cd740 	Rotate to mysql-bin.000006  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
