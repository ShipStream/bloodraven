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
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
