package sidecar

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// BinlogEvidence separates replay observations from stricter retention coverage.
// Evidence is usable only when InspectBinlog reaches an authenticated EOF.
type BinlogEvidence struct {
	Metadata                  BinlogMetadata
	Digest                    string
	GTIDs                     internalmysql.GTIDSet
	StopReached               bool
	NewestEventTime           time.Time
	FirstVerifiedEventTime    time.Time
	FirstTransactionEventTime time.Time
	LastTransactionEventTime  time.Time
	TimestampsComplete        bool
	NewestReplayEventTime     time.Time
	BeforeStopGTIDs           internalmysql.GTIDSet
	CompletedBeforeStopGTIDs  internalmysql.GTIDSet
	ReplayRefusal             string
}
type binlogCountingReader struct {
	io.Reader
	n int64
}

func (r *binlogCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}

// InspectBinlog parses a whole plaintext stream, including unknown valid events.
// Retention may remain incomplete for valid XA, LOAD DATA or checksum-off files.
func InspectBinlog(r io.Reader, stop time.Time) (BinlogEvidence, error) {
	return inspectBinlog(r, stop, nil)
}

// InspectBinlogForReplay retains all parsing checks and excludes dump-covered
// transactions only from the inner timestamp veto, matching mysqlbinlog.
func InspectBinlogForReplay(r io.Reader, stop time.Time, excluded internalmysql.GTIDSet) (BinlogEvidence, error) {
	return inspectBinlog(r, stop, nil, excluded)
}
func inspectBinlog(r io.Reader, stop time.Time, observe func(*replication.BinlogEvent), excluded ...internalmysql.GTIDSet) (BinlogEvidence, error) {
	h := sha256.New()
	count := &binlogCountingReader{Reader: io.TeeReader(r, h)}
	result := BinlogEvidence{GTIDs: make(internalmysql.GTIDSet), TimestampsComplete: true}
	var magic [4]byte
	if _, err := io.ReadFull(count, magic[:]); err != nil {
		return result, err
	}
	if !bytes.Equal(magic[:], replication.BinLogFileHeader) {
		return result, fmt.Errorf("invalid binlog magic")
	}
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	coverage := newBinlogCoverageCollector()
	first := false
	previous := false
	anonymous := false
	beforeStop := newBinlogCoverageCollector()
	currentGTID := "unknown"
	currentExcluded := false
	completed := replayPrefixCollector{committed: make(internalmysql.GTIDSet)}
	err := parser.ParseReader(count, func(event *replication.BinlogEvent) error {
		if observe != nil {
			observe(event)
		}
		if gtid := originGTID(event); gtid != nil {
			currentExcluded = false
			if next, err := gtid.GTIDNext(); err == nil {
				currentGTID = next.String()
				if len(excluded) == 1 {
					transaction, err := internalmysql.ParseGTIDSet(currentGTID)
					currentExcluded = err == nil && excluded[0].Contains(transaction)
				}
			}
		}
		coverage.observe(event)
		switch event.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.ROTATE_EVENT, replication.STOP_EVENT,
			replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.QUERY_EVENT, replication.XID_EVENT, replication.TABLE_MAP_EVENT,
			replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
			replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
			replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT,
			replication.BEGIN_LOAD_QUERY_EVENT, replication.EXECUTE_LOAD_QUERY_EVENT, replication.XA_PREPARE_LOG_EVENT:
		default:
			// Payloads can contain independently timestamped inner events.
			// Outer headers and unknown encodings cannot certify site order.
			result.TimestampsComplete = false
		}
		ts := time.Unix(int64(event.Header.Timestamp), 0).UTC()
		if event.Header.Timestamp > 0 {
			if result.FirstVerifiedEventTime.IsZero() || ts.Before(result.FirstVerifiedEventTime) {
				result.FirstVerifiedEventTime = ts
			}
			if ts.After(result.NewestEventTime) {
				result.NewestEventTime = ts
			}
			switch event.Header.EventType {
			case replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.ROTATE_EVENT, replication.STOP_EVENT:
			default:
				if result.FirstTransactionEventTime.IsZero() || ts.Before(result.FirstTransactionEventTime) {
					result.FirstTransactionEventTime = ts
				}
				if ts.After(result.LastTransactionEventTime) {
					result.LastTransactionEventTime = ts
				}
			}
		} else if event.Header.EventType != replication.PREVIOUS_GTIDS_EVENT {
			// PREVIOUS_GTIDS is untimed metadata. Other missing clocks cannot
			// prove chronological order between independently archived sites.
			result.TimestampsComplete = false
		}
		if !stop.IsZero() && !ts.Before(stop) {
			result.StopReached = true
		}
		if !result.StopReached {
			beforeStop.observe(event)
			payloadRefusal := ""
			if event.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT {
				payload, err := decodedTransactionPayload(event)
				if err != nil {
					payloadRefusal = fmt.Sprintf("compressed transaction %s cannot be decoded before the target; mysqlbinlog replay cannot be verified", currentGTID)
				} else if !stop.IsZero() && !currentExcluded {
					for _, inner := range payload.Events {
						if !time.Unix(int64(inner.Header.Timestamp), 0).Before(stop) {
							payloadRefusal = fmt.Sprintf("compressed transaction %s has events at or after the target; mysqlbinlog would replay it partially", currentGTID)
							break
						}
					}
				}
			}
			if payloadRefusal != "" {
				if result.ReplayRefusal == "" {
					result.ReplayRefusal = payloadRefusal
				}
				completed.pending = nil
			} else {
				completed.observe(event)
			}
			if event.Header.Timestamp > 0 && ts.After(result.NewestReplayEventTime) {
				result.NewestReplayEventTime = ts
			}
		}
		if pg, ok := event.Event.(*replication.PreviousGTIDsEvent); ok {
			previous = true
			result.Metadata.PreviousGTIDs = pg.GTIDSets
		}
		if event.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT && event.Header.EventType != replication.PREVIOUS_GTIDS_EVENT && event.Header.Timestamp > 0 {
			ts := time.Unix(int64(event.Header.Timestamp), 0).UTC()
			if !first {
				result.Metadata.FirstEventTime = ts
				first = true
			}
			result.Metadata.LastEventTime = ts
		}
		if event.Header.EventType == replication.ANONYMOUS_GTID_EVENT {
			anonymous = true
		}
		if event.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			if format, ok := event.Event.(*replication.FormatDescriptionEvent); ok && format.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32 && !coverage.valid {
				return fmt.Errorf("invalid format checksum")
			}
		}
		return nil
	})
	var payloadErr *replication.EventError
	if errors.As(err, &payloadErr) && payloadErr.Header != nil && payloadErr.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT && !result.StopReached && (stop.IsZero() || time.Unix(int64(payloadErr.Header.Timestamp), 0).Before(stop)) && result.ReplayRefusal == "" {
		result.ReplayRefusal = fmt.Sprintf("compressed transaction %s cannot be decoded before the target; mysqlbinlog replay cannot be verified", currentGTID)
	}
	_, drainErr := io.Copy(io.Discard, count)
	if err == nil {
		err = drainErr
	}
	if err == nil && coverage.bytes != count.n {
		err = fmt.Errorf("truncated binlog: parsed %d of %d bytes", coverage.bytes, count.n)
	}
	if err == nil && (!coverage.format || !previous) {
		err = fmt.Errorf("incomplete binlog header")
	}
	if err == nil && anonymous {
		err = fmt.Errorf("anonymous GTID cannot establish replay order")
	}
	result.GTIDs = UnionBinlogGTIDs(coverage.gtids)
	result.BeforeStopGTIDs = UnionBinlogGTIDs(beforeStop.gtids)
	result.CompletedBeforeStopGTIDs = completed.committed
	result.Metadata.Size = count.n
	result.Metadata.GTIDCoverage = coverage.result(count.n, err)
	// Observations remain useful for early divergence diagnostics, never deletion.
	if !result.Metadata.GTIDCoverage.Complete {
		result.Metadata.GTIDCoverage.GTIDs = result.GTIDs.String()
	}
	result.Digest = hex.EncodeToString(h.Sum(nil))
	return result, err
}

// Credit only transaction bodies that finish before mysqlbinlog's first stop
// event. Unsupported encodings, XA and interrupted bodies supply no proof.
// This collector never changes replay selection or retention authorization.
type replayPrefixCollector struct {
	pending, committed internalmysql.GTIDSet
	inTransaction      bool
}

func (c *replayPrefixCollector) observe(event *replication.BinlogEvent) {
	if gtid := originGTID(event); gtid != nil {
		c.pending = nil
		c.inTransaction = false
		if next, err := gtid.GTIDNext(); err == nil {
			c.pending, _ = internalmysql.ParseGTIDSet(next.String())
		}
		return
	}
	finish := func() {
		c.committed = UnionBinlogGTIDs(c.committed, c.pending)
		c.pending = nil
		c.inTransaction = false
	}
	switch event.Header.EventType {
	case replication.XID_EVENT:
		finish()
	case replication.QUERY_EVENT:
		query, ok := event.Event.(*replication.QueryEvent)
		if !ok {
			c.pending = nil
			return
		}
		sql := replayControlQuery(query.Query)
		words := strings.Fields(sql)
		if len(words) == 0 {
			c.pending = nil
			return
		}
		switch words[0] {
		case "BEGIN":
			c.inTransaction = true
		case "COMMIT":
			finish()
		case "ROLLBACK":
			if !c.inTransaction || !isSavepointRollback(words) {
				c.pending = nil
			}
		case "XA":
			c.pending = nil
		case "START":
			if len(words) > 1 && words[1] == "TRANSACTION" {
				c.inTransaction = true
			} else if !c.inTransaction {
				finish()
			}
		default:
			if !c.inTransaction {
				finish()
			}
		}
	case replication.TRANSACTION_PAYLOAD_EVENT:
		if c.payloadCompletes(event) {
			finish()
		} else {
			c.pending = nil
		}

	case replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.ROTATE_EVENT, replication.STOP_EVENT,
		replication.TABLE_MAP_EVENT, replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT, replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT:
	default:
		c.pending = nil
	}
}

// Ignore ordinary leading comments when identifying transaction control. An
// executable version comment is deliberately uncredited rather than parsed.
func replayControlQuery(query []byte) string {
	sql := strings.TrimSpace(string(query))
	for {
		if strings.HasPrefix(sql, "#") || (strings.HasPrefix(sql, "--") && (len(sql) == 2 || sql[2] <= ' ')) {
			end := strings.IndexByte(sql, '\n')
			if end < 0 {
				return ""
			}
			sql = strings.TrimSpace(sql[end+1:])
			continue
		}
		if !strings.HasPrefix(sql, "/*") {
			break
		}
		if strings.HasPrefix(sql, "/*!") {
			return ""
		}
		end := strings.Index(sql, "*/")
		if end < 0 {
			return ""
		}
		sql = strings.TrimSpace(sql[end+2:])
	}
	return strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(sql, ";")))
}

func (c *replayPrefixCollector) payloadCompletes(event *replication.BinlogEvent) bool {
	payload, err := decodedTransactionPayload(event)
	if err != nil || c.pending.IsEmpty() {
		return false
	}
	last := payload.Events[len(payload.Events)-1]
	if last == nil || last.Header == nil {
		return false
	}
	if last.Header.EventType != replication.XID_EVENT {
		query, ok := last.Event.(*replication.QueryEvent)
		if !ok {
			return false
		}
		words := strings.Fields(replayControlQuery(query.Query))
		if len(words) == 0 || words[0] != "COMMIT" {
			return false
		}
	}
	innerProof := replayPrefixCollector{pending: c.pending, committed: make(internalmysql.GTIDSet), inTransaction: c.inTransaction}
	var decodedBytes uint64
	for _, inner := range payload.Events {
		if inner == nil || inner.Header == nil {
			return false
		}
		decodedBytes += uint64(len(inner.RawData))
		switch inner.Header.EventType {
		case replication.QUERY_EVENT:
			query, ok := inner.Event.(*replication.QueryEvent)
			if !ok {
				return false
			}
			words := strings.Fields(replayControlQuery(query.Query))
			if len(words) == 0 || words[0] == "XA" || (words[0] == "ROLLBACK" && (!innerProof.inTransaction || !isSavepointRollback(words))) {
				return false
			}
		case replication.XID_EVENT, replication.TABLE_MAP_EVENT,
			replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
			replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
			replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT:
		default:
			return false
		}
		innerProof.observe(inner)
	}
	return decodedBytes == payload.UncompressedSize && uint64(len(payload.Payload)) == payload.Size && innerProof.committed.Contains(c.pending)
}

func isSavepointRollback(words []string) bool {
	rest := words[1:]
	if len(rest) > 0 && rest[0] == "WORK" {
		rest = rest[1:]
	}
	return len(rest) > 1 && rest[0] == "TO"
}
