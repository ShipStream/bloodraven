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
	// PayloadUndecodable marks a parse error caused by an undecodable
	// compressed payload before the target, as opposed to corrupt framing.
	PayloadUndecodable bool

	// Stop-frontier evidence. mysqlbinlog ends at the first event whose header
	// time reaches the stop, including server-local metadata, so the commit
	// times that follow decide whether that event skipped an older commit.
	StopLatchLocal    bool
	StopLatchServerID uint32
	StopLatchTime     time.Time
	// LastOriginServerID is the originating server of the last transaction
	// before the stop event, or in the whole file when the stop is not reached.
	LastOriginServerID uint32
	// PostStopProof reports a transaction at or after the stop event that
	// committed at or after the stop.
	PostStopProof bool
	// PostStopEarlyCommit names the first non-excluded transaction after the
	// stop event that committed before the stop; replay would skip it.
	PostStopEarlyCommit string
	// PostStopGTIDs are transactions after the stop event committed at or after
	// the stop. LateCommitGTIDs and EarlyCommit cover the whole file instead.
	PostStopGTIDs   internalmysql.GTIDSet
	LateCommitGTIDs internalmysql.GTIDSet
	EarlyCommit     string
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
	return inspectBinlog(r, stop, nil, binlogInspection{})
}

// InspectBinlogForReplay retains all parsing checks. Excluded transactions are
// the backup bound plus explicit excludeGtids, exactly as passed to
// mysqlbinlog --exclude-gtids; they are exempt only from the inner timestamp
// veto, replay-time reporting and skipped-commit refusals.
func InspectBinlogForReplay(r io.Reader, stop time.Time, excluded internalmysql.GTIDSet) (BinlogEvidence, error) {
	return inspectBinlog(r, stop, nil, binlogInspection{replay: true, excluded: excluded})
}

type binlogInspection struct {
	replay   bool
	excluded internalmysql.GTIDSet
}

// binlogCommitTime prefers the originating server's commit timestamp. Files
// without commit timestamps fall back to the GTID event header.
func binlogCommitTime(gtid *replication.GTIDEvent, header *replication.EventHeader) time.Time {
	for _, micros := range []uint64{gtid.OriginalCommitTimestamp, gtid.ImmediateCommitTimestamp} {
		if micros > 0 {
			return time.UnixMicro(int64(micros)).UTC()
		}
	}
	return time.Unix(int64(header.Timestamp), 0).UTC()
}

func binlogLocalEvent(kind replication.EventType) bool {
	switch kind {
	case replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.ROTATE_EVENT, replication.STOP_EVENT:
		return true
	}
	return false
}

func inspectBinlog(r io.Reader, stop time.Time, observe func(*replication.BinlogEvent), opts binlogInspection) (BinlogEvidence, error) {
	h := sha256.New()
	count := &binlogCountingReader{Reader: io.TeeReader(r, h)}
	result := BinlogEvidence{GTIDs: make(internalmysql.GTIDSet), TimestampsComplete: true, PostStopGTIDs: make(internalmysql.GTIDSet), LateCommitGTIDs: make(internalmysql.GTIDSet)}
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
	var currentSet internalmysql.GTIDSet
	currentExcluded := false
	var currentCommit time.Time
	// Archival inspection without a stop never reads the completed prefix.
	collect := opts.replay || !stop.IsZero()
	completed := replayPrefixCollector{committed: make(internalmysql.GTIDSet)}
	observeTime := func(ts time.Time, transaction bool) {
		if result.FirstVerifiedEventTime.IsZero() || ts.Before(result.FirstVerifiedEventTime) {
			result.FirstVerifiedEventTime = ts
		}
		if ts.After(result.NewestEventTime) {
			result.NewestEventTime = ts
		}
		if transaction {
			if result.FirstTransactionEventTime.IsZero() || ts.Before(result.FirstTransactionEventTime) {
				result.FirstTransactionEventTime = ts
			}
			if ts.After(result.LastTransactionEventTime) {
				result.LastTransactionEventTime = ts
			}
		}
	}
	err := parser.ParseReader(count, func(event *replication.BinlogEvent) error {
		if observe != nil {
			observe(event)
		}
		gtid := originGTID(event)
		if gtid != nil {
			currentExcluded = false
			currentSet = nil
			currentGTID = "unknown"
			if next, err := gtid.GTIDNext(); err == nil {
				currentGTID = next.String()
				if parsed, err := internalmysql.ParseGTIDSet(currentGTID); err == nil {
					currentSet = parsed
					currentExcluded = opts.excluded != nil && opts.excluded.Contains(parsed)
				}
			}
			currentCommit = binlogCommitTime(gtid, event.Header)
			if !stop.IsZero() {
				if currentCommit.Before(stop) {
					if !currentExcluded && result.EarlyCommit == "" {
						result.EarlyCommit = currentGTID + " committed " + currentCommit.Format(time.RFC3339Nano)
					}
				} else {
					for sid, intervals := range currentSet {
						result.LateCommitGTIDs[sid] = append(result.LateCommitGTIDs[sid], intervals...)
					}
				}
			}
		}
		coverage.observe(event)
		var payload *replication.TransactionPayloadEvent
		var payloadErr error
		switch event.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.ROTATE_EVENT, replication.STOP_EVENT,
			replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.QUERY_EVENT, replication.XID_EVENT, replication.TABLE_MAP_EVENT,
			replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
			replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
			replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT,
			replication.BEGIN_LOAD_QUERY_EVENT, replication.EXECUTE_LOAD_QUERY_EVENT, replication.XA_PREPARE_LOG_EVENT:
		case replication.TRANSACTION_PAYLOAD_EVENT:
			// Decoded inner headers carry their own times; an undecodable
			// payload leaves the event-time diagnostics incomplete.
			payload, payloadErr = decodedTransactionPayload(event)
			if payloadErr != nil {
				result.TimestampsComplete = false
			} else {
				for _, inner := range payload.Events {
					if inner.Header.Timestamp == 0 {
						result.TimestampsComplete = false
						continue
					}
					observeTime(time.Unix(int64(inner.Header.Timestamp), 0).UTC(), true)
				}
			}
		default:
			// Unknown encodings cannot supply event-time diagnostics.
			result.TimestampsComplete = false
		}
		local := binlogLocalEvent(event.Header.EventType)
		ts := time.Unix(int64(event.Header.Timestamp), 0).UTC()
		if event.Header.Timestamp > 0 {
			observeTime(ts, !local)
		} else if event.Header.EventType != replication.PREVIOUS_GTIDS_EVENT {
			// PREVIOUS_GTIDS is untimed metadata.
			result.TimestampsComplete = false
		}
		latched := result.StopReached
		if !stop.IsZero() && !ts.Before(stop) {
			result.StopReached = true
		}
		if result.StopReached && !latched {
			result.StopLatchLocal = local
			result.StopLatchServerID = event.Header.ServerID
			result.StopLatchTime = ts
			// The stop can fall inside a transaction whose GTID came first;
			// mysqlbinlog then drops that transaction's body.
			if gtid == nil && !local && currentSet != nil {
				if !currentCommit.Before(stop) {
					result.PostStopProof = true
					for sid, intervals := range currentSet {
						result.PostStopGTIDs[sid] = append(result.PostStopGTIDs[sid], intervals...)
					}
				} else if !currentExcluded && result.PostStopEarlyCommit == "" {
					result.PostStopEarlyCommit = currentGTID + " committed " + currentCommit.Format(time.RFC3339Nano)
				}
			}
		}
		if gtid != nil {
			if !result.StopReached {
				result.LastOriginServerID = event.Header.ServerID
			} else if !stop.IsZero() {
				if currentCommit.Before(stop) {
					if !currentExcluded && result.PostStopEarlyCommit == "" {
						result.PostStopEarlyCommit = currentGTID + " committed " + currentCommit.Format(time.RFC3339Nano)
					}
				} else {
					result.PostStopProof = true
					for sid, intervals := range currentSet {
						result.PostStopGTIDs[sid] = append(result.PostStopGTIDs[sid], intervals...)
					}
				}
			}
		}
		if !result.StopReached {
			beforeStop.observe(event)
			payloadRefusal := ""
			if event.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT {
				if payloadErr != nil {
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
			} else if collect {
				completed.observe(event)
			}
			if !local && !currentExcluded {
				newest := ts
				if payload != nil {
					for _, inner := range payload.Events {
						if t := time.Unix(int64(inner.Header.Timestamp), 0).UTC(); t.After(newest) {
							newest = t
						}
					}
				}
				if event.Header.Timestamp > 0 && newest.After(result.NewestReplayEventTime) {
					result.NewestReplayEventTime = newest
				}
			}
		}
		if pg, ok := event.Event.(*replication.PreviousGTIDsEvent); ok {
			previous = true
			result.Metadata.PreviousGTIDs = pg.GTIDSets
		}
		if event.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT && event.Header.EventType != replication.PREVIOUS_GTIDS_EVENT && event.Header.Timestamp > 0 {
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
	var eventErr *replication.EventError
	if errors.As(err, &eventErr) && eventErr.Header != nil && eventErr.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT && !result.StopReached && (stop.IsZero() || time.Unix(int64(eventErr.Header.Timestamp), 0).Before(stop)) {
		result.PayloadUndecodable = true
		if result.ReplayRefusal == "" {
			result.ReplayRefusal = fmt.Sprintf("compressed transaction %s cannot be decoded before the target; mysqlbinlog replay cannot be verified", currentGTID)
		}
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
	result.CompletedBeforeStopGTIDs = UnionBinlogGTIDs(completed.committed)
	result.PostStopGTIDs = UnionBinlogGTIDs(result.PostStopGTIDs)
	result.LateCommitGTIDs = UnionBinlogGTIDs(result.LateCommitGTIDs)
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
	// Appending keeps each commit constant-time; the caller normalizes once.
	finish := func() {
		for sid, intervals := range c.pending {
			c.committed[sid] = append(c.committed[sid], intervals...)
		}
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
	for _, inner := range payload.Events {
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
	// decodedTransactionPayload already checked the decoded and compressed sizes.
	return UnionBinlogGTIDs(innerProof.committed).Contains(c.pending)
}

func isSavepointRollback(words []string) bool {
	rest := words[1:]
	if len(rest) > 0 && rest[0] == "WORK" {
		rest = rest[1:]
	}
	return len(rest) > 1 && rest[0] == "TO"
}
