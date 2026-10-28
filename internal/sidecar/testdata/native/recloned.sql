# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261028 20:00:40 server id 101  end_log_pos 127 CRC32 0xeba8921b 	Start: binlog v 4, server v 8.4.11 created 261028 20:00:40 at startup
ROLLBACK/*!*/;
BINLOG '
aFTiag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAABoVOJqEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAEbkqjr
'/*!*/;
# at 127
#261028 20:00:40 server id 101  end_log_pos 158 CRC32 0x0a06e1a8 	Previous-GTIDs
# [empty]
# at 158
#261028 20:00:41 server id 101  end_log_pos 237 CRC32 0xfaf3df5a 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793217641081993	immediate_commit_timestamp=1793217641081993	transaction_length=296
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793217641081993 (2026-10-28 20:00:41.081993 UTC)
# immediate_commit_timestamp=1793217641081993 (2026-10-28 20:00:41.081993 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793217641081993*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '3f788889-d30a-11f1-9549-8c10aa28c76e:1'/*!*/;
# at 237
#261028 20:00:41 server id 101  end_log_pos 308 CRC32 0x43a656b8 	Query	thread_id=8	exec_time=0	error_code=0
SET TIMESTAMP=1793217641/*!*/;
SET @@session.pseudo_thread_id=8/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1168113696/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=255/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
/*!80011 SET @@session.default_collation_for_utf8mb4=255*//*!*/;
BEGIN
/*!*/;
# at 308
#261028 20:00:41 server id 101  end_log_pos 371 CRC32 0xeaaf190f 	Table_map: `replay`.`markers` mapped to number 85
# has_generated_invisible_primary_key=0
# at 371
#261028 20:00:41 server id 101  end_log_pos 423 CRC32 0x558c9bb0 	Write_rows: table id 85 flags: STMT_END_F

BINLOG '
aVTiahNlAAAAPwAAAHMBAAAAAFUAAAAAAAEABnJlcGxheQAHbWFya2VycwACAw8CoAACAQEAAgP8
/wAPGa/q
aVTiah5lAAAANAAAAKcBAAAAAFUAAAAAAAEAAgAC/wAFAAAAC2FmdGVyLWNsb25lsJuMVQ==
'/*!*/;
# at 423
#261028 20:00:41 server id 101  end_log_pos 454 CRC32 0x42827d04 	Xid = 16
COMMIT/*!*/;
# at 454
#261028 20:00:41 server id 101  end_log_pos 501 CRC32 0xca7789ff 	Rotate to mysql-bin.000002  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
