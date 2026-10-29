package sidecar

import (
	"encoding/binary"
	"hash/crc32"
	"sort"

	"github.com/go-mysql-org/go-mysql/replication"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// PITRGTIDCoverageProtocol identifies retention clients that require GTID proof.
const PITRGTIDCoverageProtocol = "gtid-v1"

// BinlogGTIDCoverage records observed GTIDs; only Complete authorizes retention.
// Unknown or unsupported events keep Complete false even when replay can parse them.
type BinlogGTIDCoverage struct {
	Complete bool   `json:"complete"`
	GTIDs    string `json:"gtids,omitempty"`
	// ContentID binds this proof to bytes inspected by an identity-aware writer.
	ContentID string `json:"contentId,omitempty"`
}

type binlogCoverageCollector struct {
	bytes          int64
	valid          bool
	format         bool
	sawGTID        bool
	payloadAllowed bool
	gtids          internalmysql.GTIDSet
}

func newBinlogCoverageCollector() *binlogCoverageCollector {
	return &binlogCoverageCollector{bytes: 4, valid: true, gtids: make(internalmysql.GTIDSet)}
}

func (c *binlogCoverageCollector) observe(e *replication.BinlogEvent) {
	c.bytes += int64(len(e.RawData))
	payloadAllowed := c.payloadAllowed
	c.payloadAllowed = false
	var gtid *replication.GTIDEvent
	switch e.Header.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT:
		format, ok := e.Event.(*replication.FormatDescriptionEvent)
		if !ok || format.ChecksumAlgorithm != replication.BINLOG_CHECKSUM_ALG_CRC32 || len(e.RawData) < 4 {
			c.valid = false
		} else {
			body, checksum := e.RawData[:len(e.RawData)-4], e.RawData[len(e.RawData)-4:]
			if binary.LittleEndian.Uint32(checksum) != crc32.ChecksumIEEE(body) {
				c.valid = false
			}
		}
		if c.format {
			c.valid = false
		}
		c.format = true
	case replication.PREVIOUS_GTIDS_EVENT, replication.ROTATE_EVENT, replication.STOP_EVENT:
	case replication.GTID_EVENT:
		gtid, _ = e.Event.(*replication.GTIDEvent)
	case replication.GTID_TAGGED_LOG_EVENT:
		if tagged, ok := e.Event.(*replication.GtidTaggedLogEvent); ok {
			gtid = &tagged.GTIDEvent
		}
	case replication.TRANSACTION_PAYLOAD_EVENT:
		payload, ok := e.Event.(*replication.TransactionPayloadEvent)
		if !payloadAllowed || !ok || payload == nil {
			c.valid = false
			break
		}
		var decodedBytes uint64
		for _, inner := range payload.Events {
			decodedBytes += uint64(len(inner.RawData))
			switch inner.Header.EventType {
			case replication.QUERY_EVENT, replication.XID_EVENT, replication.TABLE_MAP_EVENT,
				replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
				replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
				replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
				replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
				replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT,
				replication.BEGIN_LOAD_QUERY_EVENT, replication.EXECUTE_LOAD_QUERY_EVENT:
			default:
				c.valid = false
			}
		}
		if len(payload.Events) == 0 || decodedBytes != payload.UncompressedSize || uint64(len(payload.Payload)) != payload.Size {
			c.valid = false
		}
	case replication.QUERY_EVENT, replication.XID_EVENT, replication.TABLE_MAP_EVENT,
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
		replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT,
		replication.BEGIN_LOAD_QUERY_EVENT, replication.EXECUTE_LOAD_QUERY_EVENT:
		if !c.sawGTID {
			c.valid = false
		}
	default:
		// Anonymous transactions and future encodings are retained.
		c.valid = false
	}
	if e.Header.EventType != replication.GTID_EVENT && e.Header.EventType != replication.GTID_TAGGED_LOG_EVENT {
		return
	}
	if gtid == nil || gtid.GNO <= 0 {
		c.valid = false
		return
	}
	next, err := gtid.GTIDNext()
	if err != nil {
		c.valid = false
		return
	}
	parsed, err := internalmysql.ParseGTIDSet(next.String())
	if err != nil || parsed.IsEmpty() {
		c.valid = false
		return
	}
	c.sawGTID = true
	c.payloadAllowed = true
	for sid, intervals := range parsed {
		c.gtids[sid] = append(c.gtids[sid], intervals...)
	}
}

func (c *binlogCoverageCollector) result(size int64, parseErr error) *BinlogGTIDCoverage {
	if parseErr != nil || c.bytes != size || !c.valid || !c.format {
		return &BinlogGTIDCoverage{}
	}
	for sid, intervals := range c.gtids {
		sort.Slice(intervals, func(i, j int) bool { return intervals[i].Start < intervals[j].Start })
		merged := intervals[:0]
		for _, iv := range intervals {
			if len(merged) > 0 && (iv.Start <= merged[len(merged)-1].End || iv.Start-1 == merged[len(merged)-1].End) {
				if iv.End > merged[len(merged)-1].End {
					merged[len(merged)-1].End = iv.End
				}
			} else {
				merged = append(merged, iv)
			}
		}
		c.gtids[sid] = merged
	}
	return &BinlogGTIDCoverage{Complete: true, GTIDs: c.gtids.String()}
}
