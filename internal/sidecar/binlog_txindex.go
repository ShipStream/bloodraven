package sidecar

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
)

// IsArchiveObjectMissing reports whether a store error says the object
// does not exist (ENOENT on a PVC, NoSuchKey/404 on S3). The planner's
// fetcher treats only this, and integrity failures, as an unreadable row;
// every other store error is transient (review round 1 P4).
func IsArchiveObjectMissing(err error) bool {
	return errors.Is(err, os.ErrNotExist) || isS3NotFound(err)
}

// IsArchiveIntegrityError reports whether a read error says the stored
// bytes are bad, not the connection: a missing BRV1 magic while encryption
// is required (ErrTamperedOrDowngrade), or bytes that do not decrypt and
// authenticate (errObjectUndecryptable: a BRV1 failure, an AEAD tag
// mismatch, data after the final chunk). encryptedStore.Open marks a
// decryption failure undecryptable only when the store delivered every
// byte without a transport error; a backupcrypto error without that mark
// (a truncated body read as ErrTruncated or as a failed tag) is a
// transport failure and so transient (review F082, F209).
func IsArchiveIntegrityError(err error) bool {
	return errors.Is(err, ErrTamperedOrDowngrade) || errors.Is(err, errObjectUndecryptable)
}

// TxIndexOptions configure InspectTransactions.
type TxIndexOptions struct {
	// Label names the file in refusals ("<site>/<name>").
	Label string
	// Budget is charged for every indexed transaction and, transiently,
	// for each event and decompressed payload. Its MaxEventBytes is the
	// decoder bound (D-42).
	Budget *pitrplan.Budget
}

// Index-pass hooks; tests replace them to assert that oversized events are
// refused before any decoder runs.
var (
	txIndexParse = func(p *replication.BinlogParser, raw []byte) (*replication.BinlogEvent, error) {
		return p.Parse(raw)
	}
	txIndexDecompress = decompressPayload
)

// Minimum bodies (after the common header, before the checksum) of the
// events the index pass hands to go-mysql's decoders, which index their
// input without bounds checks (review F078): FORMAT_DESCRIPTION is
// binlog_version 2 + server_version 50 + create_timestamp 4 +
// header_length 1; a GTID event is flags 1 + SID 16 + GNO 8. A tagged GTID
// event is a serialized message whose own decoder is guarded by
// parseEvent's recover.
const (
	minFormatDescriptionBody = 2 + 50 + 4 + 1
	minGTIDBody              = 1 + 16 + 8
)

// InspectTransactions is the planner's index pass (design §4.10). It reads
// a plaintext binlog stream and returns its per-transaction index:
//
//  1. Framing: it reads each 19-byte event header itself and refuses an
//     event_size above the budget's MaxEventBytes (PITRPlanTooLarge)
//     before reading the body, and verifies every event's CRC32 itself.
//     For a TRANSACTION_PAYLOAD_EVENT it parses the TLV header and refuses
//     an uncompressed_size above the bound before zstd runs; it then
//     decompresses with a decoder capped at that size. Only the
//     FORMAT_DESCRIPTION and GTID events go through go-mysql's decoders
//     (with their panics turned into refusals); query and PREVIOUS_GTIDS
//     events are decoded here, and every other event is classified by its
//     type alone, so no table map or row image is ever decoded or kept.
//  2. Index: GTID, original commit timestamp, originating server_id,
//     emptiness (binlogDataEvent, the rule the emptyGtids hint uses), XA.
//     Every transaction must end with its terminator (XID, COMMIT,
//     ROLLBACK, XA PREPARE, or a statement outside BEGIN) before the next
//     GTID or the end of the file, and no event other than rotation,
//     heartbeat and similar bookkeeping may follow a terminator (review
//     F080).
//  3. The SHA-256 digest of every byte, for the caller to authenticate
//     against the manifest row's ID.
//
// Refusals are *pitrplan.Refusal. Any other error is a read error.
func InspectTransactions(r io.Reader, opts TxIndexOptions) (*pitrplan.FileIndex, error) {
	label := opts.Label
	budget := opts.Budget
	if budget == nil {
		budget = pitrplan.NewBudget(pitrplan.DefaultPlanMemoryBytes)
	}
	maxEvent := budget.MaxEventBytes()
	invalid := func(format string, args ...any) error {
		return &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: label + ": " + fmt.Sprintf(format, args...)}
	}
	h := sha256.New()
	src := io.TeeReader(r, h)
	var magic [4]byte
	if _, err := io.ReadFull(src, magic[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, invalid("truncated binlog header")
		}
		return nil, err
	}
	if !bytes.Equal(magic[:], replication.BinLogFileHeader) {
		return nil, invalid("not a binlog (bad magic)")
	}
	ix := &txIndexer{
		label:  label,
		budget: budget,
		parser: replication.NewBinlogParser(),
		b:      pitrplan.NewIndexBuilder(label, budget),
		// The v4 QUERY_EVENT post-header, until FORMAT_DESCRIPTION says.
		queryPostHeader: 13,
	}
	ix.parser.SetVerifyChecksum(true)
	offset := int64(4)
	var header [replication.EventHeaderSize]byte
	for {
		if _, err := io.ReadFull(src, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, invalid("truncated event header at offset %d", offset)
			}
			return nil, err
		}
		size := int64(binary.LittleEndian.Uint32(header[9:13]))
		kind := replication.EventType(header[4])
		if size < replication.EventHeaderSize {
			return nil, invalid("event at offset %d declares %d bytes", offset, size)
		}
		if size > maxEvent {
			return nil, tooLarge(label, fmt.Sprintf("an event of %d bytes (%s at offset %d)", size, kind, offset), maxEvent)
		}
		if err := budget.Charge(size, "binlog event decode"); err != nil {
			return nil, err
		}
		raw := make([]byte, size)
		copy(raw, header[:])
		if _, err := io.ReadFull(src, raw[replication.EventHeaderSize:]); err != nil {
			budget.Release(size)
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return nil, invalid("truncated event at offset %d", offset)
			}
			return nil, err
		}
		err := ix.event(raw, offset)
		budget.Release(size)
		if err != nil {
			return nil, err
		}
		offset += size
	}
	if !ix.sawFormat {
		return nil, invalid("no FORMAT_DESCRIPTION event")
	}
	if ix.state == txnOpen || ix.state == txnBegun {
		return nil, invalid("the file ends inside transaction %s, before its terminator", ix.gtid)
	}
	return ix.b.Finish(hex.EncodeToString(h.Sum(nil)), offset)
}

// tooLarge is the decoder-bound refusal. The bound is min(64 MiB, budget/8):
// below the fixed cap, more memory raises it; at the cap it cannot (review
// F081).
func tooLarge(label, what string, maxEvent int64) error {
	remedy := "raise the Job's memory limit (the bound is 1/8 of the planner budget, at most 64 MiB)"
	if maxEvent >= pitrplan.MaxEventBytesCap {
		remedy = "this is the planner's fixed 64 MiB bound, which more memory does not raise; choose a target whose plan does not read this file (a target before it, or a backup taken after it)"
	}
	return &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
		"%s: %s exceeds the planner's limit of %d bytes; %s", label, what, maxEvent, remedy)}
}

// txnState is where the index pass is relative to the transaction
// boundaries (review F080).
type txnState uint8

const (
	// txnNone: before the first GTID, or after a transaction's terminator.
	txnNone txnState = iota
	// txnOpen: after a GTID event, before its first statement.
	txnOpen
	// txnBegun: inside BEGIN (or XA START), until COMMIT, ROLLBACK, an XID
	// or XA PREPARE.
	txnBegun
	// txnStatement: after a statement outside BEGIN (DDL, XA COMMIT). The
	// transaction is complete; only rows ending with an XID of its own
	// may follow (a non-atomic CREATE … SELECT).
	txnStatement
)

// txIndexer is one file's index pass.
type txIndexer struct {
	label           string
	budget          *pitrplan.Budget
	parser          *replication.BinlogParser
	b               *pitrplan.IndexBuilder
	sawFormat       bool
	checksum        bool
	queryPostHeader int
	state           txnState
	gtid            pitrplan.GTID
}

func (ix *txIndexer) invalid(format string, args ...any) error {
	return &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: ix.label + ": " + fmt.Sprintf(format, args...)}
}

func (ix *txIndexer) unsupported(format string, args ...any) error {
	return &pitrplan.Refusal{Token: pitrarchive.TokenReplayUnsupported, Message: ix.label + ": " + fmt.Sprintf(format, args...)}
}

// parseEvent runs go-mysql's decoder, turning a decoder panic on bytes it
// does not bounds-check into an error (review F078).
func (ix *txIndexer) parseEvent(raw []byte) (ev *replication.BinlogEvent, err error) {
	defer func() {
		if r := recover(); r != nil {
			ev, err = nil, fmt.Errorf("undecodable event: %v", r)
		}
	}()
	return txIndexParse(ix.parser, raw)
}

// event indexes one outer event.
func (ix *txIndexer) event(raw []byte, offset int64) error {
	kind := replication.EventType(raw[4])
	// FORMAT_DESCRIPTION comes first, whatever the event (review F208).
	if !ix.sawFormat {
		if kind != replication.FORMAT_DESCRIPTION_EVENT {
			return ix.invalid("first event is %s, not FORMAT_DESCRIPTION", kind)
		}
		return ix.formatDescription(raw, offset)
	}
	if kind == replication.FORMAT_DESCRIPTION_EVENT {
		return ix.invalid("second FORMAT_DESCRIPTION event at offset %d", offset)
	}
	body := raw[replication.EventHeaderSize:]
	if ix.checksum {
		if len(body) < 4 {
			return ix.invalid("%s at offset %d is too short for its checksum", kind, offset)
		}
		data, sum := raw[:len(raw)-4], raw[len(raw)-4:]
		if binary.LittleEndian.Uint32(sum) != crc32.ChecksumIEEE(data) {
			return ix.invalid("%s at offset %d fails its CRC32", kind, offset)
		}
		body = body[:len(body)-4]
	}
	serverID := binary.LittleEndian.Uint32(raw[5:9])
	switch kind {
	case replication.PREVIOUS_GTIDS_EVENT:
		if ix.state != txnNone {
			return ix.invalid("PREVIOUS_GTIDS at offset %d inside transaction %s", offset, ix.gtid)
		}
		text, err := decodePreviousGTIDs(body)
		if err != nil {
			return ix.invalid("PREVIOUS_GTIDS at offset %d: %v", offset, err)
		}
		set, err := pitrplan.ParseSet(text)
		if err != nil {
			return ix.invalid("PREVIOUS_GTIDS %q: %v", text, err)
		}
		return ix.b.SetPrevious(set)
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT:
		if kind == replication.GTID_EVENT && len(body) < minGTIDBody {
			return ix.invalid("GTID event at offset %d is %d bytes", offset, len(body))
		}
		ev, err := ix.parseEvent(raw)
		if err != nil {
			return ix.invalid("event %s at offset %d: %v", kind, offset, err)
		}
		gtid := originGTID(ev)
		if gtid == nil {
			return ix.invalid("undecodable GTID event at offset %d", offset)
		}
		g, err := gtidOf(gtid)
		if err != nil {
			return ix.invalid("GTID event at offset %d: %v", offset, err)
		}
		if !ix.b.HasPrevious() {
			return ix.invalid("transaction %s before PREVIOUS_GTIDS", g)
		}
		if ix.state == txnOpen || ix.state == txnBegun {
			return ix.invalid("transaction %s ends without a terminator before %s at offset %d", ix.gtid, g, offset)
		}
		if err := ix.b.BeginTxn(g, int64(gtid.OriginalCommitTimestamp), serverID); err != nil { //nolint:gosec // µs fit int64
			return err
		}
		ix.state, ix.gtid = txnOpen, g
		return nil
	case replication.ANONYMOUS_GTID_EVENT:
		return ix.unsupported("anonymous transaction at offset %d; replay needs GTIDs", offset)
	case replication.ROTATE_EVENT, replication.STOP_EVENT, replication.HEARTBEAT_EVENT, replication.HEARTBEAT_LOG_EVENT_V2,
		replication.IGNORABLE_EVENT, replication.TRANSACTION_CONTEXT_EVENT, replication.VIEW_CHANGE_EVENT:
		return nil
	case replication.TRANSACTION_PAYLOAD_EVENT:
		return ix.payload(body, offset)
	}
	return ix.step(kind, body, fmt.Sprintf("offset %d", offset))
}

// formatDescription reads the first event through go-mysql, which checks
// its checksum against the algorithm the event itself declares.
func (ix *txIndexer) formatDescription(raw []byte, offset int64) error {
	if len(raw)-replication.EventHeaderSize < minFormatDescriptionBody {
		return ix.invalid("FORMAT_DESCRIPTION event at offset %d is %d bytes", offset, len(raw))
	}
	ev, err := ix.parseEvent(raw)
	if err != nil {
		return ix.invalid("event %s at offset %d: %v", replication.FORMAT_DESCRIPTION_EVENT, offset, err)
	}
	fde, ok := ev.Event.(*replication.FormatDescriptionEvent)
	if !ok {
		return ix.invalid("undecodable FORMAT_DESCRIPTION event")
	}
	ix.sawFormat = true
	ix.checksum = fde.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32
	if n := int(replication.QUERY_EVENT) - 1; n < len(fde.EventTypeHeaderLengths) {
		ix.queryPostHeader = int(fde.EventTypeHeaderLengths[n])
	}
	ix.b.SetWriter(binary.LittleEndian.Uint32(raw[5:9]))
	return nil
}

// step applies one in-transaction event (outer, or inside a payload) of
// the given kind; body is its post-header and payload (no checksum). It
// tracks the transaction's terminator and records its flags.
func (ix *txIndexer) step(kind replication.EventType, body []byte, where string) error {
	ev := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: kind}}
	var query string
	if kind == replication.QUERY_EVENT {
		q, err := innerQuery(body, ix.queryPostHeader)
		if err != nil {
			return ix.invalid("query event at %s: %v", where, err)
		}
		ev.Event = &replication.QueryEvent{Query: q}
		query = strings.ToUpper(strings.TrimSpace(string(q)))
	}
	outside := func() error {
		return ix.unsupported("%s at %s outside any GTID transaction", kind, where)
	}
	switch ix.state {
	case txnNone:
		return outside()
	case txnOpen:
		switch kind {
		case replication.QUERY_EVENT:
			if query == "BEGIN" || strings.HasPrefix(query, "XA START") || strings.HasPrefix(query, "XA BEGIN") {
				ix.state = txnBegun
			} else {
				ix.state = txnStatement
			}
		case replication.XID_EVENT:
			ix.state = txnNone
		case replication.XA_PREPARE_LOG_EVENT:
			return ix.invalid("XA PREPARE at %s without XA START in transaction %s", where, ix.gtid)
		}
	case txnBegun:
		switch {
		case kind == replication.QUERY_EVENT && (query == "COMMIT" || query == "ROLLBACK"),
			kind == replication.XID_EVENT, kind == replication.XA_PREPARE_LOG_EVENT:
			ix.state = txnNone
		}
	case txnStatement:
		// A statement without BEGIN is complete. Only its own rows, closed
		// by an XID, may follow it; anything else (an INCIDENT, say)
		// belongs to no transaction.
		switch kind {
		case replication.TABLE_MAP_EVENT,
			replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
			replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT:
			ix.state = txnBegun
		case replication.XID_EVENT:
			ix.state = txnNone
		default:
			return outside()
		}
	}
	classifyEvent(ix.b, ev)
	return nil
}

// classifyEvent records an in-transaction event's data/XA/unsupported flags.
func classifyEvent(b *pitrplan.IndexBuilder, ev *replication.BinlogEvent) {
	if binlogDataEvent(ev) {
		b.MarkData()
	}
	switch ev.Header.EventType {
	case replication.QUERY_EVENT:
		if q, ok := ev.Event.(*replication.QueryEvent); ok {
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(string(q.Query))), "XA ") {
				b.Mark(pitrplan.TxnXA)
			}
		}
	case replication.XA_PREPARE_LOG_EVENT:
		b.Mark(pitrplan.TxnXA)
	case replication.TABLE_MAP_EVENT,
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT, replication.ROWS_QUERY_EVENT,
		replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT, replication.XID_EVENT:
	default:
		// LOAD DATA blocks, incidents and unknown events: mysqlbinlog's
		// replay of them is not certified.
		b.Mark(pitrplan.TxnUnsupported)
	}
}

func gtidOf(e *replication.GTIDEvent) (pitrplan.GTID, error) {
	if len(e.SID) != 16 {
		return pitrplan.GTID{}, fmt.Errorf("source id of %d bytes", len(e.SID))
	}
	sid := formatUUID(e.SID)
	if tag := e.Tag.String(); tag != "" {
		sid += ":" + strings.ToLower(tag)
	}
	return pitrplan.GTID{SID: sid, GNO: e.GNO}, nil
}

// formatUUID spells 16 bytes as a lower-case UUID.
func formatUUID(b []byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
}

// decodePreviousGTIDs decodes a PREVIOUS_GTIDS_EVENT body (no header, no
// checksum) to GTID set text, one "uuid[:tag]:intervals" member per
// encoded source, in MySQL's encoding: an 8-byte source count (in the
// tagged format, byte 0 and byte 7 are the format marker 1 and bytes 1-6
// the count), then per source its 16-byte UUID, in the tagged format a
// one-byte varlen tag length and the tag, an 8-byte interval count, and
// per interval its start and its exclusive end (8 bytes each). go-mysql's
// own decoder appends a tagged source to the previous untagged UUID, so a
// UUID that has only tagged GTIDs is attributed to another server
// (review F079).
func decodePreviousGTIDs(data []byte) (string, error) {
	if len(data) < 8 {
		return "", fmt.Errorf("%d bytes is too short", len(data))
	}
	tagged := data[7] == 1
	var count uint64
	if tagged {
		if data[0] != 1 {
			return "", fmt.Errorf("unknown GTID set format %d", data[0])
		}
		var n [8]byte
		copy(n[:], data[1:7])
		count = binary.LittleEndian.Uint64(n[:])
	} else {
		count = binary.LittleEndian.Uint64(data[:8])
	}
	pos := 8
	need := func(n int) error {
		if len(data)-pos < n {
			return fmt.Errorf("truncated at byte %d", pos)
		}
		return nil
	}
	members := make([]string, 0, min(count, 1024))
	for i := uint64(0); i < count; i++ {
		if err := need(16); err != nil {
			return "", err
		}
		var m strings.Builder
		m.WriteString(formatUUID(data[pos : pos+16]))
		pos += 16
		if tagged {
			if err := need(1); err != nil {
				return "", err
			}
			// A varlen integer: one byte holds n<<1 for n < 128; tags are
			// at most 32 characters.
			lenByte := data[pos]
			pos++
			if lenByte&1 != 0 || lenByte>>1 > 32 {
				return "", fmt.Errorf("bad tag length byte %#x", lenByte)
			}
			if n := int(lenByte >> 1); n > 0 {
				if err := need(n); err != nil {
					return "", err
				}
				m.WriteByte(':')
				m.WriteString(strings.ToLower(string(data[pos : pos+n])))
				pos += n
			}
		}
		if err := need(8); err != nil {
			return "", err
		}
		intervals := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		if intervals == 0 || intervals > uint64(len(data)-pos)/16 {
			return "", fmt.Errorf("%d intervals for one source at byte %d", intervals, pos)
		}
		for j := uint64(0); j < intervals; j++ {
			start := int64(binary.LittleEndian.Uint64(data[pos:])) //nolint:gosec // range-checked below
			end := int64(binary.LittleEndian.Uint64(data[pos+8:])) //nolint:gosec
			pos += 16
			if start < 1 || end <= start || end-1 > pitrplan.MaxGNO {
				return "", fmt.Errorf("bad interval [%d, %d)", start, end)
			}
			m.WriteByte(':')
			m.WriteString(strconv.FormatInt(start, 10))
			if end-1 > start {
				m.WriteByte('-')
				m.WriteString(strconv.FormatInt(end-1, 10))
			}
		}
		members = append(members, m.String())
	}
	if pos != len(data) {
		return "", fmt.Errorf("%d bytes after the set", len(data)-pos)
	}
	return strings.Join(members, ","), nil
}

// previousGTIDsOf decodes a PREVIOUS_GTIDS event that go-mysql framed with
// decodePreviousGTIDs instead of go-mysql's GTIDSets, which misattributes a
// UUID that has only tagged GTIDs (review F079), so the manifest's
// previousGtids agree with the index pass over the same bytes. checksum
// tells whether the file's FORMAT_DESCRIPTION announced CRC32 (RawData
// keeps the trailing checksum).
func previousGTIDsOf(e *replication.BinlogEvent, checksum bool) (string, error) {
	if len(e.RawData) < replication.EventHeaderSize {
		return "", fmt.Errorf("PREVIOUS_GTIDS event is %d bytes", len(e.RawData))
	}
	body := e.RawData[replication.EventHeaderSize:]
	if checksum {
		if len(body) < 4 {
			return "", fmt.Errorf("PREVIOUS_GTIDS event is too short for its checksum")
		}
		body = body[:len(body)-4]
	}
	text, err := decodePreviousGTIDs(body)
	if err != nil {
		return "", fmt.Errorf("PREVIOUS_GTIDS: %w", err)
	}
	return text, nil
}

// payload handles a TRANSACTION_PAYLOAD_EVENT without go-mysql's payload
// decoder: it bounds uncompressed_size before zstd, decompresses under
// that cap, and steps the inner events through the transaction tracker.
// The caller has verified the event's CRC.
func (ix *txIndexer) payload(body []byte, offset int64) error {
	switch ix.state {
	case txnNone:
		return ix.unsupported("transaction payload at offset %d outside any GTID transaction", offset)
	case txnBegun, txnStatement:
		return ix.invalid("transaction payload at offset %d inside the uncompressed transaction %s", offset, ix.gtid)
	}
	var values [4]uint64
	seen := 0
	data := body
	for {
		field, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return ix.invalid("payload header at offset %d: %v", offset, err)
		}
		data = data[n:]
		if field == 0 {
			break
		}
		if field > 3 || seen&(1<<field) != 0 {
			return ix.invalid("payload header at offset %d: unknown or duplicate field %d", offset, field)
		}
		length, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return ix.invalid("payload header at offset %d: %v", offset, err)
		}
		data = data[n:]
		if length > uint64(len(data)) {
			return ix.invalid("payload header at offset %d is truncated", offset)
		}
		value, n, err := payloadLengthEncodedInt(data[:length])
		if err != nil || uint64(n) != length {
			return ix.invalid("payload header at offset %d: bad value for field %d", offset, field)
		}
		values[field] = value
		seen |= 1 << field
		data = data[length:]
	}
	if seen != 14 {
		return ix.invalid("payload header at offset %d is incomplete", offset)
	}
	size, ctype, usize := values[1], values[2], values[3]
	maxEvent := ix.budget.MaxEventBytes()
	if usize > uint64(maxEvent) { //nolint:gosec // maxEvent > 0
		return tooLarge(ix.label, fmt.Sprintf("a compressed transaction payload of %d uncompressed bytes (offset %d)", usize, offset), maxEvent)
	}
	if ctype != uint64(replication.ZSTD) || size != uint64(len(data)) {
		return ix.invalid("payload at offset %d: unsupported compression %d or size %d of %d", offset, ctype, size, len(data))
	}
	charge := payloadDecodeBytes(int64(usize)) //nolint:gosec // usize ≤ maxEvent
	if err := ix.budget.Charge(charge, "transaction payload decode"); err != nil {
		return err
	}
	defer ix.budget.Release(charge)
	plain, err := txIndexDecompress(data, int64(usize)) //nolint:gosec
	if err != nil {
		return ix.invalid("payload at offset %d: %v", offset, err)
	}
	for pos := 0; pos < len(plain); {
		if len(plain)-pos < replication.EventHeaderSize {
			return ix.invalid("payload at offset %d has a truncated inner event", offset)
		}
		h := plain[pos : pos+replication.EventHeaderSize]
		n := int(binary.LittleEndian.Uint32(h[9:13]))
		if n < replication.EventHeaderSize || n > len(plain)-pos {
			return ix.invalid("payload at offset %d has an inner event of %d bytes", offset, n)
		}
		kind := replication.EventType(h[4])
		switch kind {
		case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.ANONYMOUS_GTID_EVENT,
			replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.TRANSACTION_PAYLOAD_EVENT:
			return ix.invalid("payload at offset %d holds a %s", offset, kind)
		}
		// Inner events carry no checksum.
		if err := ix.step(kind, plain[pos+replication.EventHeaderSize:pos+n], fmt.Sprintf("offset %d (inside the payload at %d)", pos, offset)); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

// innerQuery extracts a QUERY_EVENT's statement from its post-header and
// payload (no checksum): post-header, status variables, schema, NUL,
// query.
func innerQuery(data []byte, postHeader int) ([]byte, error) {
	if postHeader < 13 || len(data) < postHeader {
		return nil, fmt.Errorf("short query event")
	}
	schemaLen := int(data[8])
	statusLen := int(binary.LittleEndian.Uint16(data[11:13]))
	start := postHeader + statusLen + schemaLen + 1
	if start > len(data) {
		return nil, fmt.Errorf("query event fields overrun the event")
	}
	return data[start:], nil
}

// zstdMinWindow is the smallest decoder window cap: a frame may declare a
// window larger than its content, up to the zstd format's 8 MiB minimum a
// decoder must support.
const zstdMinWindow = 8 << 20

// payloadDecodeBytes is the transient charge for decompressing a payload
// of n bytes: the output buffer and the decoder's history window, which
// for MySQL's single-segment frames is the content size (frames that
// declare a larger window are refused), plus the decoder's block buffers
// (up to about 1.5n, at most 2 MiB) and state.
// TestDecompressPayloadHeapBound measures it (review F210).
func payloadDecodeBytes(n int64) int64 { return 2*n + min(2*n, 2<<20) + 64<<10 }

// decompressPayload inflates one zstd frame of exactly limit bytes. MySQL
// writes single-segment frames whose content size is the payload's
// uncompressed_size, so the decoder's window is the content itself; a
// frame that declares another size, or a window above max(limit, 8 MiB),
// is refused. The output is read into one buffer of limit bytes; one more
// byte means the frame decompresses past its declared size.
func decompressPayload(src []byte, limit int64) ([]byte, error) {
	var h zstd.Header
	if err := h.Decode(src); err != nil {
		return nil, fmt.Errorf("zstd frame header: %w", err)
	}
	if h.HasFCS && h.FrameContentSize != uint64(limit) { //nolint:gosec // limit ≥ 0
		return nil, fmt.Errorf("zstd frame declares %d bytes, the payload header %d", h.FrameContentSize, limit)
	}
	window := uint64(max(limit, zstdMinWindow)) //nolint:gosec // limit ≥ 0
	if h.WindowSize > window {
		return nil, fmt.Errorf("zstd frame declares a %d-byte window for %d bytes", h.WindowSize, limit)
	}
	dec, err := zstd.NewReader(bytes.NewReader(src), zstd.WithDecoderMaxWindow(window), zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	out := make([]byte, limit)
	if n, err := io.ReadFull(dec, out); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decompresses to %d bytes, header says %d", n, limit)
		}
		return nil, err
	}
	var extra [1]byte
	if n, err := dec.Read(extra[:]); n > 0 {
		return nil, fmt.Errorf("decompresses past its declared %d bytes", limit)
	} else if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return out, nil
}

// planSiteRowBytes is charged per manifest row PlanSite converts: the Row
// it builds and pitrplan.Run's copy of it, and its name and ID.
var planSiteRowBytes = 2 * int64(unsafe.Sizeof(pitrplan.Row{}))

// setChargeBytes estimates a parsed set's heap as pitrplan's budget does:
// per source identifier its map slot (key and slice header, growth
// headroom), its spelling and its interval array; per interval its slot.
func setChargeBytes(s pitrplan.Set) int64 {
	var n int64
	for sid, ivs := range s {
		n += 2*(16+24) + 16 + int64(len(sid)+15)/16*16 + int64(len(ivs))*(int64(unsafe.Sizeof(pitrplan.Interval{}))+8)
	}
	return n
}

// PlanSite converts a loaded version-2 manifest into the planner's view of
// that site (§4.1); see PlanSiteCharged.
func PlanSite(m *Manifest) (pitrplan.SiteManifest, error) {
	return PlanSiteCharged(m, nil)
}

// PlanSiteCharged converts a loaded version-2 manifest into the planner's
// view of that site (§4.1): active rows (Files), quarantined rows, the
// divergence audit's MissingGTIDs per row, promotion records and the
// history marker. Hints are trusted only when the row's coverage is
// Complete and bound to the row's content ID; the planner verifies them
// against the bytes of every row it downloads. With a budget, every row
// and parsed set is charged before the next is built (D-42, review F211).
func PlanSiteCharged(m *Manifest, budget *pitrplan.Budget) (pitrplan.SiteManifest, error) {
	out := pitrplan.SiteManifest{Site: m.Site, Marked: m.HistoryIntegrity, Promotions: m.Promotions}
	charge := func(n int64, what string) error {
		if budget == nil {
			return nil
		}
		return budget.Charge(n, what)
	}
	divergent := map[[2]string]pitrplan.Set{}
	for _, d := range m.Divergent {
		set, err := pitrplan.ParseSet(d.MissingGTIDs)
		if err != nil {
			return out, fmt.Errorf("%s: manifest %s divergent row %s: %v", pitrarchive.TokenArchiveObjectInvalid, m.Site, d.Name, err)
		}
		if err := charge(setChargeBytes(set), "divergence audit of site "+m.Site); err != nil {
			return out, err
		}
		key := [2]string{d.Name, d.ID}
		divergent[key] = divergent[key].Union(set)
	}
	convert := func(e ManifestEntry, state pitrplan.RowState) (pitrplan.Row, error) {
		row := pitrplan.Row{Site: m.Site, Name: e.Name, ID: e.ID, State: state, Created: e.FirstEventTime}
		if e.ID == "" {
			return row, fmt.Errorf("%s: manifest %s row %s has no id; version-2 rows are content-addressed", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name)
		}
		what := "manifest row " + m.Site + "/" + e.Name
		if err := charge(planSiteRowBytes+int64(len(e.Name)+len(e.ID)), what); err != nil {
			return row, err
		}
		base, err := pitrplan.ParseSet(e.BaseGTIDs)
		if err != nil {
			return row, fmt.Errorf("%s: manifest %s row %s baseGtids: %v", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name, err)
		}
		row.Base = base
		prev, err := pitrplan.ParseSet(e.PreviousGTIDs)
		if err != nil {
			return row, fmt.Errorf("%s: manifest %s row %s previousGtids: %v", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name, err)
		}
		previous := prev.Union(base)
		if err := charge(setChargeBytes(base)+setChargeBytes(previous), what); err != nil {
			return row, err
		}
		// Coverage copied from other bytes (no content binding) proves
		// nothing about this row: its hints are unknown, so the planner
		// downloads the row instead of skipping it (review F212).
		if c := e.GTIDCoverage; c != nil && c.Complete && coverageBoundToEntry(e) {
			gtids, err1 := pitrplan.ParseSet(c.GTIDs)
			empty, err2 := pitrplan.ParseSet(c.EmptyGTIDs)
			if err := errors.Join(err1, err2); err != nil {
				return row, fmt.Errorf("%s: manifest %s row %s hints: %v", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name, err)
			}
			if err := charge(setChargeBytes(gtids)+setChargeBytes(empty), what); err != nil {
				return row, err
			}
			row.Hints = pitrplan.Hints{Known: true, Previous: previous, GTIDs: gtids, Empty: empty, Min: c.OriginalCommitMin, Max: c.OriginalCommitMax}
		} else {
			row.Hints = pitrplan.Hints{Previous: previous}
		}
		if d, ok := divergent[[2]string{e.Name, e.ID}]; ok && state == pitrplan.RowActive {
			row.Divergent = d
		}
		return row, nil
	}
	for _, e := range m.Files {
		row, err := convert(e, pitrplan.RowActive)
		if err != nil {
			return out, err
		}
		out.Rows = append(out.Rows, row)
	}
	for _, q := range m.Quarantined {
		row, err := convert(q.Entry, pitrplan.RowQuarantined)
		if err != nil {
			return out, err
		}
		row.QuarantinedAt = q.At
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}
