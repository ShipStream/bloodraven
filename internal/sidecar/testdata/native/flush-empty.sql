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
#261028 20:00:35 server id 101  end_log_pos 245 CRC32 0x9b38a503 	Rotate to mysql-bin.000004  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
