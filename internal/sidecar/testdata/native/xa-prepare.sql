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
SET @@session.pseudo_thread_id=18/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1168113696/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=255/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255*//*!*/;
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
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
