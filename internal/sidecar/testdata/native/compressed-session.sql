# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#261029  8:34:43 server id 101  end_log_pos 127 CRC32 0xdbc06393 	Start: binlog v 4, server v 8.4.11 created 261029  8:34:43
BINLOG '
IwXjag9lAAAAewAAAH8AAAAAAAQAOC40LjExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEwANAAgAAAAABAAEAAAAYwAEGggAAAAAAAACAAAACgoKKioAEjQA
CigAAAGTY8Db
'/*!*/;
# at 127
#261029  8:34:43 server id 101  end_log_pos 198 CRC32 0x54a1af14 	Previous-GTIDs
# 94ae26b8-d373-11f1-8ee3-b7be919e6317:1-2
# at 198
#261029  8:34:43 server id 101  end_log_pos 277 CRC32 0x878da2f0 	GTID	last_committed=0	sequence_number=1	rbr_only=yes	original_committed_timestamp=1793262883618524	immediate_commit_timestamp=1793262883618524	transaction_length=1034
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793262883618524 (2026-10-29 08:34:43.618524 UTC)
# immediate_commit_timestamp=1793262883618524 (2026-10-29 08:34:43.618524 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793262883618524*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '94ae26b8-d373-11f1-8ee3-b7be919e6317:3'/*!*/;
# at 277
#261029  8:34:43 server id 101  end_log_pos 1232 CRC32 0xcfec5d3c 	Transaction_Payload		payload_size=918	compression_type=ZSTD	uncompressed_size=956
# Start of compressed events.
# at 1232
#261029  8:34:43 server id 101  end_log_pos 1232 CRC32 0xd69c486f 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793262883/*!*/;
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
# at 1232
#261029  8:34:43 server id 101  end_log_pos 1232 CRC32 0xfbec115d 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 1232
#261029  8:34:43 server id 101  end_log_pos 1232 CRC32 0x81cce9b1 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
IwXjahNlAAAAPAAAAAAAAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACA/wBAgIBAQACAT9d
Eez7
IwXjah5lAAAAKgMAAAAAAAAAAGoAAAAAAAEAAgAC/wABAAAAAANeLj/LpxzKyPctNS0BMBiut7b4
QWNKhjM2I+A+/9b+xpMLX02nt765nUFO46c4uATRtNVnvqZtoP2A2+RAjOdaZRQY49zwiPeoAcYe
/D7QYI6+FZ4meY3Npc9JESP7/ScoMPq9HfCl8fXhZWZ2vhoj2JouUku8wltmcWPos5946XLtfIY7
cimGl9B7/oMV1mnB7vW3X3eqIxZmN5lCKyc+nqoPjud9TschdVXrOH7O+IsT4f8Hb/DsbkuykDwl
4D1q5I/RQqlfiyAIwXxh23+uVNZosFGk+D/smC0SIsB3WlC9wNpGL2FpPRdnDZMTyB4SEwsh2FJv
5DHw/9VHRnrB9qcWCl82jHLAF8Ou/H+P9/Rl0RNmBFdUaqBXvlFbBPetyZ4V3CR/waKtHN+NPZIj
0dIbZqZgQhk/Q/Zb7fnS69YgXabuDNcc1omWPhA/AnsXQQWPxgtIuA4gqFaZtQILAahVV/PYsaZy
Txk+vF8dqf2R6szpCfK8SRC0iASu2UK+GvlOtvc7IjXZJsCz7UHL0gCUN37ILfqNlB1ahM+W0oU6
UP/BtzGa2mBtjJRT/IfhU+8SZw9KNsWRAyrZQamcR6VmdXdMjDtQ9E5sWOZIF4ZA+Knyy7XUvdyq
Ib6l+VF+g+Ic8IrVD7M0dKQh8ReDNPdXi8NHdN76bc9AZmrhq3hQIyusGkp38oWs6WIgoDuHFH72
aeulGMEfdOyLW+Ul7ZZFNCJjEiQD4+cS2pBE7JFPTk5KPElQvuKx+TydsuK/vU1AWVSpA+o0PvZM
pT+m5Dlrh5wh/QdNwc+R4kw8OFhaZ9qj7H7r0GkXFdpFfujgEJmJjWcFdGOE1s2PhxpVacaxyjAY
Ro/XN7CaxzRF1kqRhMSC0TRTL1EmnMlwRBFcwSY8yl4uahjuvMNXmi+jI4TmXbsBYc/2JZ5plbw5
J75dc0pEleMheC9p43dE+4k5WVW+MrvpWGlRsicqcvvejy38jOmtEjPGy82qmhZW1zwxh6WTHZq2
Ub3u4PAxfx6x6cyB
'/*!*/;
# at 1232
#261029  8:34:43 server id 101  end_log_pos 1232 CRC32 0x343bb6fb 	Xid = 16
COMMIT/*!*/;
# End of compressed events.
# at 1232
#261029  8:34:43 server id 101  end_log_pos 1311 CRC32 0x7100ed98 	GTID	last_committed=1	sequence_number=2	rbr_only=yes	original_committed_timestamp=1793262883622112	immediate_commit_timestamp=1793262883622112	transaction_length=1829
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
# original_commit_timestamp=1793262883622112 (2026-10-29 08:34:43.622112 UTC)
# immediate_commit_timestamp=1793262883622112 (2026-10-29 08:34:43.622112 UTC)
/*!80001 SET @@session.original_commit_timestamp=1793262883622112*//*!*/;
/*!80014 SET @@session.original_server_version=80411*//*!*/;
/*!80014 SET @@session.immediate_server_version=80411*//*!*/;
SET @@SESSION.GTID_NEXT= '94ae26b8-d373-11f1-8ee3-b7be919e6317:4'/*!*/;
# at 1311
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0x5e3ee73b 	Transaction_Payload		payload_size=1713	compression_type=ZSTD	uncompressed_size=1894
# Start of compressed events.
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0xd69c486f 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793262883/*!*/;
BEGIN
/*!*/;
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0xfbec115d 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0xa8bd37db 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
IwXjahNlAAAAPAAAAAAAAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACA/wBAgIBAQACAT9d
Eez7
IwXjah5lAAAAKgMAAAAAAAAAAGoAAAAAAAEAAgAC/wACAAAAAAOEpErxc2v+M8RQr0oyaLCX8iJN
0W/za6ryKkJqtqONai0DbdTtmMZF73Vx5YWhMMrL/mlT5fO7SGjWg9D4S0qJ3To96MneFM63oZrS
Zuo3Fv11FxUjM34DiXaA2ZhYf0zK/6E2kvPrImOhDos3RNlMd7IMoDoiuyerAWudQ7f16Rzk+JRy
HybJTmayBBLEZfhCcBWAVN8JTsJvvr4zyBR3Gwrnfz1K/Ym4Krpw2gGA20AnXjuXNly/yOMMSZIU
EzjLH5RLszjyR677bV1OCEBwRZAvUF3epllDRHQqkx2Pp7GT7UPhbiZK6EmowIVEhEecWBqqUb8i
jOXSbj9KznyuJVT8kJo3ZMz6UKp+Y9cW+ZV9IPAUFgIRBfsBJ0ep4bHj+nBfxGlCldgKGbGe0YTN
0dUZz69f2ZW7aERDWKQlHqZv2WKFDYNypNtAhuWmOSuUEMB6Sp1HUc64ayTO+uwVoigeE6yrW9cf
9+4xVGFRGpR+J16TaKQH6QAAV0OPq2wF9+lOH4Nr758VyGO62phjj98K4cl/C59YAdOJA06XVc5A
XBUd2k1a6ju8yuvax47531jftTXhXEGpmmFsKOGq8wDA7ZqJk/2OxpeX6K9/UU32SMukVNDhxem9
JR8L5iLg0qZVQnTffFo0hKMYo31z3ivmQKf2hChw+2gkJx78zm0aNa4ulKuADLfChfkBjy06Sn+/
qbmYCj+Rvof6gRJ81eIboxsWDx3Zj2EjT6NLiaxqOrSSq56pgH45xiif33vzJ7TITpJE470tyyVj
j6KkXHy7gyW1bIPgZoqQghwmERHtjpFngC72iGKVcXS4mg75X98ovy0H4BJVQJJT2jnc961Cqs4w
jNlO1M3ilrDY0vtH+RdPWbkKPmB013+Ni0Pde0MP2FU7vTP8Ix8qLFV4RNyzIJ/X3E8Eyd6s3Vnw
aR0OYdR9dWESlzp5l5dCzc+4A/8E4uqsZ4hAszfdrJDga07UifMD+cXha98TXAynhVfj4QApCpgr
lrI9AsFXc2nbN72o
'/*!*/;
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0xd209b58c 	Query	thread_id=10	exec_time=0	error_code=0
SET TIMESTAMP=1793262883/*!*/;
SAVEPOINT `s1`
/*!*/;
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0xfbec115d 	Table_map: `replay`.`markers` mapped to number 106
# has_generated_invisible_primary_key=0
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0xe578fb05 	Write_rows: table id 106 flags: STMT_END_F

BINLOG '
IwXjahNlAAAAPAAAAAAAAAAAAGoAAAAAAAEABnJlcGxheQAHbWFya2VycwACA/wBAgIBAQACAT9d
Eez7
IwXjah5lAAAAKgMAAAAAAAAAAGoAAAAAAAEAAgAC/wAEAAAAAAOVkbag971yZksMm+onX3lQUgCz
0bFMK04ANUQiSBlc7kXniDK6a1mhLJfDkrYoscVdVvAHKsHgu9fBJVtlw9fmLhqz5Cc3bLDSa4Uh
oLdv6OvDK5h5H6u644/F4qEKS8BXeOr0u9tG21YhKurrY2gHGBX9NG/HEw3DU8cAswyam218LH3w
+CLXQruFH8Z5Zbcd59jXBIJrSuwR83GVBisp9n8e3stqUfy5a2hRfYo60CY0rPRem5lDrTIq7Q70
9hSnk18pa7sqxoA7hxVbbhnKADT7wUQfh2YJlvGnGeFwixBfE/6TtW2u5DmhJ9BsvYtNZX67O0gw
aGoJ3fgV07FPaDKpcP9NJfjUESzDNlMmoYhlI76TbCp1mPnKWPp/bqRkfjotQohp/BIv/MsciBcp
zqFYn9XHPyaY+xHWRKpGoBdsCwlC8g7NIs5u2cQWz1ivpmYdcbQ4zH0FfsxRfietQYzoIqk9cqQS
vqS4Cfiyavy6NZz2lD6d9x5+dDnomZlgTp1wVlTEqpfwhGQzUm4YT4E6UywNhOEGxqEA3zotKvoU
e7JW9agSRHdVKDi2QWaYfFy8jcrgpfuVP1a5f5F1G5g9iM4dvKMnJhPff+t46d2FSX1HsxFuXV+F
K2F4sYOHD5pvwfFr/wLP/QVMnqBBUrPLH0VoEAYQAmoi8fRVmp5YCDvGOwnc52/A1HcVsEs882KE
c9EELuFezZHla9MyrTgc7VjhrgkiKj81TTIhoeuOscEOs0jOFjoJqnddYODUWRFkVy7rFlf1Fiui
Onbxy6w+A4DMSi2+ej9H+BEgx3ycqzWyMDbBVluqkzRrjrl+/T4zjAAEcU53blutw0uJQGWOquJP
a5vOBO5UWlZQh3Y0piEBQi7/aJGapnLXs+9KngfTE7t9unLh8GQuaoxjRnlPlUudAx6QuX0yasYT
PrlKmAC8hlYrf3dZFxmS+XWQVUWnGnfEQXAD29QJnHnu5wwLXhQFcsfwlW/Gb/IenJZhMeuhr3Vr
ibrF+p9BrNIF+3jl
'/*!*/;
# at 3061
#261029  8:34:43 server id 101  end_log_pos 3061 CRC32 0x761eb186 	Xid = 18
COMMIT/*!*/;
# End of compressed events.
# at 3061
#261029  8:34:45 server id 101  end_log_pos 3108 CRC32 0x030e58db 	Rotate to mysql-bin.000003  pos: 4
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
