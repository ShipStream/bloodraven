# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
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
SET @@session.pseudo_thread_id=35/*!*/;
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
