# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#260415  9:00:00 server id 184  end_log_pos 127 CRC32 0x9098f63c 	Start: binlog v 4, server v 8.4.11 created 260415  9:00:00
BINLOG '
kFPfaQ+4AAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAE89piQ
'/*!*/;
# at 127
#260415  9:00:00 server id 184  end_log_pos 198 CRC32 0x2e0e59ba 	Previous-GTIDs
# b5e6ac37-d371-11f1-8b67-65731305df52:1-8
# at 198
#260415  9:00:00 server id 184  end_log_pos 277 CRC32 0x1b875cf3 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793262231815980	immediate_commit_timestamp=1793262231815980	transaction_length=262
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793262231815980 (2026-10-29 08:23:51.815980 UTC)
# immediate_commit_timestamp=1793262231815980 (2026-10-29 08:23:51.815980 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793262231815980*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= 'b5e6ac37-d371-11f1-8b67-65731305df52:9'/*!*/;
# at 277
#260415  9:00:00 server id 184  end_log_pos 460 CRC32 0x458b27c8 	Transaction_Payload		payload_size=150	compression_type=ZSTD	uncompressed_size=231
# Start of compressed events.
# at 460
#260415  9:30:00 server id 184  end_log_pos 460 CRC32 0xef1c414e 	Query	thread_id=10	exec_time=17016831	error_code=0
SET TIMESTAMP=1776245400/*!*/;
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
# at 460
#260415  9:30:00 server id 184  end_log_pos 460 CRC32 0x0caf2312 	Table_map: `probe`.`markers` mapped to number 85
# has_generated_invisible_primary_key=0
# at 460
#260415  9:30:00 server id 184  end_log_pos 460 CRC32 0x4e3c5e3f 	Write_rows: table id 85 flags: STMT_END_F

BINLOG '
mFrfaRO4AAAAPQAAAAAAAAAAAFUAAAAAAAEABXByb2JlAAdtYXJrZXJzAAID/AEEAgEBAAID/P8A
EiOvDA==
mFrfaR64AAAAVAAAAAAAAAAAAFUAAAAAAAEAAgAC/wACAAAAKAAAAHp6enp6enp6enp6enp6enp6
enp6enp6enp6enp6enp6enp6enp6eno/XjxO
'/*!*/;
# at 460
#260415  9:00:00 server id 184  end_log_pos 460 CRC32 0xc0b82cac 	Xid = 24
COMMIT/*!*/;
# End of compressed events.
# at 460
#260415  9:00:00 server id 184  end_log_pos 507 CRC32 0x4249c573 	Rotate to mysql-bin.000005  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
