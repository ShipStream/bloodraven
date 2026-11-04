# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261029  8:34:45 server id 101  end_log_pos 127 CRC32 0x7fbc9076 	Start: binlog v 4, server v 8.4.11 created 261029  8:34:45
BINLOG '
JQXjag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAF2kLx/
'/*!*/;
# at 127
#261029  8:34:45 server id 101  end_log_pos 198 CRC32 0x0ba8f3c1 	Previous-GTIDs
# 94ae26b8-d373-11f1-8ee3-b7be919e6317:1-4
# at 198
#261029  8:34:47 server id 101  end_log_pos 245 CRC32 0x8f31422c 	Rotate to mysql-bin.000004  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
