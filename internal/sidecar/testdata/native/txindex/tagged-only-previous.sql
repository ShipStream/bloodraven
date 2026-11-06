# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261106  4:11:55 server id 1  end_log_pos 127 CRC32 0xf2795889 	Start: binlog v 4, server v 9.7.4 created 261106  4:11:55
BINLOG '
i1Ptag8BAAAAewAAAH8AAAAAAAQAOS43LjQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAGJWHny
'/*!*/;
# at 127
#261106  4:11:55 server id 1  end_log_pos 242 CRC32 0x9b45812f 	Previous-GTIDs
# 2f1f78ac-d998-11f1-87d6-5aba2ed1f094:1-11,
# cccccccc-cccc-cccc-cccc-cccccccccccc:tg:1-2
# at 242
#261106  4:11:55 server id 1  end_log_pos 319 CRC32 0x17896e86 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793938315612418	immediate_commit_timestamp=1793938315612418	transaction_length=232
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793938315612418 (2026-11-06 04:11:55.612418 UTC)
# immediate_commit_timestamp=1793938315612418 (2026-11-06 04:11:55.612418 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793938315612418*//*!*/;
/*!80014 SET @@session.original_server_version=90704*//*!*/;
/*!80014 SET @@session.immediate_server_version=90704*//*!*/;
SET @@SESSION.GTID_NEXT= '2f1f78ac-d998-11f1-87d6-5aba2ed1f094:12'/*!*/;
# at 319
#261106  4:11:55 server id 1  end_log_pos 474 CRC32 0x0ee8b947 	Transaction_Payload		payload_size=122	compression_type=ZSTD	uncompressed_size=182
# Start of compressed events.
# at 474
#261106  4:11:55 server id 1  end_log_pos 474 CRC32 0x1647f7f2 	Query	thread_id=10	exec_time=0	error_code=0
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
BEGIN
/*!*/;
# at 474
#261106  4:11:55 server id 1  end_log_pos 474 CRC32 0x3ea119c6 	Table_map: `s`.`t1` mapped to number 101
# has_generated_invisible_primary_key=0
# at 474
#261106  4:11:55 server id 1  end_log_pos 474 CRC32 0x8e39cd6e 	Write_rows: table id 101 flags: STMT_END_F USE_SQL_FOREIGN_KEY_F

BINLOG '
i1PtahMBAAAANQAAAAAAAAAAAGUAAAAAAAEAAXMAAnQxAAIDDwKQAQIBAQACA/z/AMYZoT4=
i1Ptah4BAAAAKwAAAAAAAAAAAGUAAAAAABEAAgAC/wAGAAAAAQBmbs05jg==
'/*!*/;
# at 474
#261106  4:11:55 server id 1  end_log_pos 474 CRC32 0xd7d20ed1 	Xid = 35
COMMIT/*!*/;
# End of compressed events.
# at 474
#261106  4:12:56 server id 1  end_log_pos 518 CRC32 0x862e3f83 	Rotate to binlog.000003  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
