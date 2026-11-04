# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#260415  9:00:00 server id 606  end_log_pos 127 CRC32 0xd5e3236a 	Start: binlog v 4, server v 8.4.11 created 260415  9:00:00
BINLOG '
kFPfaQ9eAgAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAFqI+PV
'/*!*/;
# at 127
#260415  9:00:00 server id 606  end_log_pos 198 CRC32 0x18d79f18 	Previous-GTIDs
# 7f4a1927-d377-11f1-a983-f6bbc6ad1253:1-2
# at 198
#260415  9:00:00 server id 606  end_log_pos 277 CRC32 0xb16f93b0 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793264565643219	immediate_commit_timestamp=1793264565643219	transaction_length=265
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793264565643219 (2026-10-29 09:02:45.643219 UTC)
# immediate_commit_timestamp=1793264565643219 (2026-10-29 09:02:45.643219 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793264565643219*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '7f4a1927-d377-11f1-a983-f6bbc6ad1253:3'/*!*/;
# at 277
#260415  9:00:00 server id 606  end_log_pos 463 CRC32 0xfd9ae335 	Transaction_Payload		payload_size=153	compression_type=ZSTD	uncompressed_size=229
# Start of compressed events.
# at 463
#260415  9:30:00 server id 606  end_log_pos 463 CRC32 0x65e6e7ac 	Query	thread_id=11	exec_time=17019165	error_code=0
SET TIMESTAMP=1776245400/*!*/;
SET @@session.pseudo_thread_id=11/*!*/;
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
# at 463
#260415  9:30:00 server id 606  end_log_pos 463 CRC32 0x416b3076 	Table_map: `probe`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 463
#260415  9:30:00 server id 606  end_log_pos 463 CRC32 0xa4800649 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
mFrfaRNeAgAAPgAAAAAAAAAAAGoAAAAAAAEABXByb2JlAAdtYXJrZXJzAAIDDwKgAAIBAQACA/z/
AHYwa0E=
mFrfaR5eAgAAUQAAAAAAAAAAAGoAAAAAAAEAAgAC/wACAAAAKHp6enp6enp6enp6enp6enp6enp6
enp6enp6enp6enp6enp6enp6enpJBoCk
'/*!*/;
# at 463
#260415  9:00:00 server id 606  end_log_pos 463 CRC32 0x5bb1879a 	Xid = 24
COMMIT/*!*/;
# End of compressed events.
# at 463
#260415  9:10:00 server id 606  end_log_pos 542 CRC32 0x1db20b18 	GTID	last_committed=1	sequence_number=2	rbr_only=yes	original_committed_timestamp=1793264565654483	immediate_commit_timestamp=1793264565654483	transaction_length=262
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793264565654483 (2026-10-29 09:02:45.654483 UTC)
# immediate_commit_timestamp=1793264565654483 (2026-10-29 09:02:45.654483 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793264565654483*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '7f4a1927-d377-11f1-a983-f6bbc6ad1253:4'/*!*/;
# at 542
#260415  9:10:00 server id 606  end_log_pos 725 CRC32 0x368f5ad3 	Transaction_Payload		payload_size=150	compression_type=ZSTD	uncompressed_size=195
# Start of compressed events.
# at 725
#260415  9:10:00 server id 606  end_log_pos 725 CRC32 0x39c4aabf 	Query	thread_id=13	exec_time=17020365	error_code=0
SET TIMESTAMP=1776244200/*!*/;
BEGIN
/*!*/;
# at 725
#260415  9:10:00 server id 606  end_log_pos 725 CRC32 0x50b81d29 	Table_map: `probe`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 725
#260415  9:10:00 server id 606  end_log_pos 725 CRC32 0xa0696b52 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
6FXfaRNeAgAAPgAAAAAAAAAAAGoAAAAAAAEABXByb2JlAAdtYXJrZXJzAAIDDwKgAAIBAQACA/z/
ACkduFA=
6FXfaR5eAgAALwAAAAAAAAAAAGoAAAAAAAEAAgAC/wADAAAABm5lZWRlZFJraaA=
'/*!*/;
# at 725
#260415  9:10:00 server id 606  end_log_pos 725 CRC32 0x34b19c79 	Xid = 36
COMMIT/*!*/;
# End of compressed events.
# at 725
#260415  9:35:00 server id 606  end_log_pos 772 CRC32 0x1e80cd2f 	Rotate to mysql-bin.000003  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
