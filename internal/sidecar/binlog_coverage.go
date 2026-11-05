package sidecar

import (
	"encoding/binary"
	"hash/crc32"
	"sort"
	"strings"

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
	// Manifest v2 content-bound hints, computed in the same inspection
	// pass as GTIDs (D-4). A downloaded row whose verified values differ
	// refuses PITRArchiveObjectInvalid.
	//
	// OriginalCommitMin and OriginalCommitMax are the smallest and largest
	// original commit timestamps (µs since the Unix epoch) of the file's
	// transactions; both 0 when the file has no transactions, or when any
	// transaction carries no original commit timestamp.
	OriginalCommitMin int64 `json:"originalCommitMin,omitempty"`
	OriginalCommitMax int64 `json:"originalCommitMax,omitempty"`
	// EmptyGTIDs are the file's transactions with no data or query events
	// (a filtered replica's placeholders, or injected empty transactions);
	// see binlogDataEvent for what counts.
	EmptyGTIDs string `json:"emptyGtids,omitempty"`
}

type binlogCoverageCollector struct {
	bytes          int64
	valid          bool
	format         bool
	sawGTID        bool
	payloadAllowed bool
	gtids          internalmysql.GTIDSet

	// Hints (D-4). current is the open transaction's GTID, currentData
	// whether it has carried a data or statement event yet.
	current        internalmysql.GTIDSet
	currentData    bool
	empty          internalmysql.GTIDSet
	commitMin      int64
	commitMax      int64
	commitUnknown  bool
	transactionsIn int64
}

func newBinlogCoverageCollector() *binlogCoverageCollector {
	return &binlogCoverageCollector{bytes: 4, valid: true, gtids: make(internalmysql.GTIDSet), empty: make(internalmysql.GTIDSet)}
}

// binlogDataEvent reports whether e carries row data or statement content,
// as opposed to transaction framing. A transaction with none of these is
// empty (emptyGtids): a filtered replica's placeholder or an injected empty
// transaction, which MySQL writes as GTID, BEGIN, COMMIT. The planner's
// emptiness classification must use the same rule.
func binlogDataEvent(e *replication.BinlogEvent) bool {
	switch e.Header.EventType {
	case replication.QUERY_EVENT:
		q, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			return true
		}
		switch strings.ToUpper(strings.TrimSpace(string(q.Query))) {
		case "BEGIN", "COMMIT", "ROLLBACK":
			return false
		}
		return true
	case replication.TABLE_MAP_EVENT,
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
		replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT,
		replication.BEGIN_LOAD_QUERY_EVENT, replication.EXECUTE_LOAD_QUERY_EVENT:
		return true
	}
	return false
}

// closeTransaction records the open transaction as empty if it carried no
// data event.
func (c *binlogCoverageCollector) closeTransaction() {
	if c.current != nil && !c.currentData {
		for sid, intervals := range c.current {
			c.empty[sid] = append(c.empty[sid], intervals...)
		}
	}
	c.current, c.currentData = nil, false
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
		payload, err := decodedTransactionPayload(e)
		if !payloadAllowed || err != nil {
			c.valid = false
			break
		}
		for _, inner := range payload.Events {
			if binlogDataEvent(inner) {
				c.currentData = true
			}
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
		if binlogDataEvent(e) {
			c.currentData = true
		}
	default:
		// Anonymous transactions and future encodings are retained.
		c.valid = false
	}
	if e.Header.EventType != replication.GTID_EVENT && e.Header.EventType != replication.GTID_TAGGED_LOG_EVENT {
		return
	}
	c.closeTransaction()
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
	c.current = parsed
	c.transactionsIn++
	if gtid.OriginalCommitTimestamp == 0 {
		c.commitUnknown = true
	} else {
		oct := int64(gtid.OriginalCommitTimestamp)
		if c.commitMin == 0 || oct < c.commitMin {
			c.commitMin = oct
		}
		if oct > c.commitMax {
			c.commitMax = oct
		}
	}
}

func mergeGTIDIntervals(set internalmysql.GTIDSet) {
	for sid, intervals := range set {
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
		set[sid] = merged
	}
}

func (c *binlogCoverageCollector) result(size int64, parseErr error) *BinlogGTIDCoverage {
	if parseErr != nil || c.bytes != size || !c.valid || !c.format {
		return &BinlogGTIDCoverage{}
	}
	c.closeTransaction()
	mergeGTIDIntervals(c.gtids)
	mergeGTIDIntervals(c.empty)
	coverage := &BinlogGTIDCoverage{Complete: true, GTIDs: c.gtids.String(), EmptyGTIDs: c.empty.String()}
	if c.transactionsIn > 0 && !c.commitUnknown {
		coverage.OriginalCommitMin, coverage.OriginalCommitMax = c.commitMin, c.commitMax
	}
	return coverage
}
