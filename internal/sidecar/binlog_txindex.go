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
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
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

// IsArchiveIntegrityError reports whether a read error comes from the
// encrypted store's authentication (a BRV1 failure, a missing BRV1 magic
// while encryption is required, an AEAD tag mismatch): the bytes are bad,
// not the connection.
func IsArchiveIntegrityError(err error) bool {
	for _, target := range []error{ErrTamperedOrDowngrade, backupcrypto.ErrMagicMismatch, backupcrypto.ErrTruncated,
		backupcrypto.ErrUnsupportedVersion, backupcrypto.ErrUnsupportedAlgorithm, backupcrypto.ErrChunkTooLarge, backupcrypto.ErrCounterExhausted} {
		if errors.Is(err, target) {
			return true
		}
	}
	// crypto/cipher's AEAD Open error is unexported.
	return err != nil && strings.Contains(err.Error(), "message authentication failed")
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

// InspectTransactions is the planner's index pass (design §4.10). It reads
// a plaintext binlog stream and returns its per-transaction index:
//
//  1. Framing: it reads each 19-byte event header itself and refuses an
//     event_size above the budget's MaxEventBytes (PITRPlanTooLarge)
//     before reading the body. For a TRANSACTION_PAYLOAD_EVENT it parses
//     the TLV header and refuses an uncompressed_size above the bound
//     before zstd runs; it then decompresses with a decoder capped at that
//     size. Every other event goes through BinlogParser.Parse, which
//     verifies its CRC32 (payload events are CRC-checked here).
//  2. Index: GTID, original commit timestamp, originating server_id,
//     emptiness (binlogDataEvent, the rule the emptyGtids hint uses), XA.
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
	unsupported := func(format string, args ...any) error {
		return &pitrplan.Refusal{Token: pitrarchive.TokenReplayUnsupported, Message: label + ": " + fmt.Sprintf(format, args...)}
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
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	// Row images are never needed: skip decoding them.
	parser.SetRowsEventDecodeFunc(func(*replication.RowsEvent, []byte) error { return nil })
	b := pitrplan.NewIndexBuilder(label, budget)
	offset := int64(4)
	sawFormat := false
	checksum := false
	queryPostHeader := 13
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
			return nil, &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
				"%s: an event of %d bytes (%s at offset %d) exceeds the planner's limit of %d bytes; raise the Job's memory limit", label, size, kind, offset, maxEvent)}
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
		serverID := binary.LittleEndian.Uint32(header[5:9])
		var err error
		if kind == replication.TRANSACTION_PAYLOAD_EVENT {
			err = indexPayload(b, raw, checksum, maxEvent, queryPostHeader, budget, label, offset)
		} else {
			err = indexEvent(b, parser, raw, &sawFormat, &checksum, &queryPostHeader, serverID, offset, invalid, unsupported)
		}
		budget.Release(size)
		if err != nil {
			return nil, err
		}
		offset += size
	}
	if !sawFormat {
		return nil, invalid("no FORMAT_DESCRIPTION event")
	}
	return b.Finish(hex.EncodeToString(h.Sum(nil)), offset)
}

func indexEvent(b *pitrplan.IndexBuilder, parser *replication.BinlogParser, raw []byte, sawFormat, checksum *bool, queryPostHeader *int, serverID uint32, offset int64,
	invalid, unsupported func(string, ...any) error) error {
	kind := replication.EventType(raw[4])
	if !*sawFormat && kind != replication.FORMAT_DESCRIPTION_EVENT {
		return invalid("first event is %s, not FORMAT_DESCRIPTION", kind)
	}
	ev, err := txIndexParse(parser, raw)
	if err != nil {
		return invalid("event %s at offset %d: %v", kind, offset, err)
	}
	switch kind {
	case replication.FORMAT_DESCRIPTION_EVENT:
		if *sawFormat {
			return invalid("second FORMAT_DESCRIPTION event at offset %d", offset)
		}
		*sawFormat = true
		fde, ok := ev.Event.(*replication.FormatDescriptionEvent)
		if !ok {
			return invalid("undecodable FORMAT_DESCRIPTION event")
		}
		*checksum = fde.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32
		if n := int(replication.QUERY_EVENT) - 1; n < len(fde.EventTypeHeaderLengths) {
			*queryPostHeader = int(fde.EventTypeHeaderLengths[n])
		}
		b.SetWriter(serverID)
		return nil
	case replication.PREVIOUS_GTIDS_EVENT:
		pg, ok := ev.Event.(*replication.PreviousGTIDsEvent)
		if !ok {
			return invalid("undecodable PREVIOUS_GTIDS event")
		}
		set, err := pitrplan.ParseSet(pg.GTIDSets)
		if err != nil {
			return invalid("PREVIOUS_GTIDS %q: %v", pg.GTIDSets, err)
		}
		return b.SetPrevious(set)
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT:
		gtid := originGTID(ev)
		if gtid == nil {
			return invalid("undecodable GTID event at offset %d", offset)
		}
		g, err := gtidOf(gtid)
		if err != nil {
			return invalid("GTID event at offset %d: %v", offset, err)
		}
		if !b.HasPrevious() {
			return invalid("transaction %s before PREVIOUS_GTIDS", g)
		}
		return b.BeginTxn(g, int64(gtid.OriginalCommitTimestamp), serverID) //nolint:gosec // µs fit int64
	case replication.ANONYMOUS_GTID_EVENT:
		return unsupported("anonymous transaction at offset %d; replay needs GTIDs", offset)
	case replication.ROTATE_EVENT, replication.STOP_EVENT, replication.HEARTBEAT_EVENT, replication.HEARTBEAT_LOG_EVENT_V2,
		replication.IGNORABLE_EVENT, replication.TRANSACTION_CONTEXT_EVENT, replication.VIEW_CHANGE_EVENT, replication.XID_EVENT:
		return nil
	}
	if !b.InTxn() {
		return unsupported("%s at offset %d outside any GTID transaction", kind, offset)
	}
	classifyEvent(b, ev)
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
	var buf [36]byte
	hex.Encode(buf[0:8], e.SID[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], e.SID[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], e.SID[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], e.SID[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], e.SID[10:16])
	sid := string(buf[:])
	if tag := e.Tag.String(); tag != "" {
		sid += ":" + strings.ToLower(tag)
	}
	return pitrplan.GTID{SID: sid, GNO: e.GNO}, nil
}

// indexPayload handles a TRANSACTION_PAYLOAD_EVENT without go-mysql's
// payload decoder: it checks the CRC, bounds uncompressed_size before
// zstd, decompresses under that cap, and classifies the inner events.
func indexPayload(b *pitrplan.IndexBuilder, raw []byte, checksum bool, maxEvent int64, queryPostHeader int, budget *pitrplan.Budget, label string, offset int64) error {
	invalid := func(format string, args ...any) error {
		return &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: label + ": " + fmt.Sprintf(format, args...)}
	}
	body := raw[replication.EventHeaderSize:]
	if checksum {
		if len(body) < 4 {
			return invalid("payload event at offset %d is too short", offset)
		}
		data, sum := raw[:len(raw)-4], raw[len(raw)-4:]
		if binary.LittleEndian.Uint32(sum) != crc32.ChecksumIEEE(data) {
			return invalid("payload event at offset %d fails its CRC32", offset)
		}
		body = body[:len(body)-4]
	}
	if !b.InTxn() {
		return &pitrplan.Refusal{Token: pitrarchive.TokenReplayUnsupported, Message: fmt.Sprintf("%s: transaction payload at offset %d outside any GTID transaction", label, offset)}
	}
	var values [4]uint64
	seen := 0
	data := body
	for {
		field, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return invalid("payload header at offset %d: %v", offset, err)
		}
		data = data[n:]
		if field == 0 {
			break
		}
		if field > 3 || seen&(1<<field) != 0 {
			return invalid("payload header at offset %d: unknown or duplicate field %d", offset, field)
		}
		length, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return invalid("payload header at offset %d: %v", offset, err)
		}
		data = data[n:]
		if length > uint64(len(data)) {
			return invalid("payload header at offset %d is truncated", offset)
		}
		value, n, err := payloadLengthEncodedInt(data[:length])
		if err != nil || uint64(n) != length {
			return invalid("payload header at offset %d: bad value for field %d", offset, field)
		}
		values[field] = value
		seen |= 1 << field
		data = data[length:]
	}
	if seen != 14 {
		return invalid("payload header at offset %d is incomplete", offset)
	}
	size, ctype, usize := values[1], values[2], values[3]
	if usize > uint64(maxEvent) { //nolint:gosec // maxEvent > 0
		return &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
			"%s: a compressed transaction payload of %d uncompressed bytes (offset %d) exceeds the planner's limit of %d bytes; raise the Job's memory limit", label, usize, offset, maxEvent)}
	}
	if ctype != uint64(replication.ZSTD) || size != uint64(len(data)) {
		return invalid("payload at offset %d: unsupported compression %d or size %d of %d", offset, ctype, size, len(data))
	}
	if err := budget.Charge(int64(usize), "transaction payload decode"); err != nil { //nolint:gosec
		return err
	}
	defer budget.Release(int64(usize))                  //nolint:gosec
	plain, err := txIndexDecompress(data, int64(usize)) //nolint:gosec
	if err != nil {
		return invalid("payload at offset %d: %v", offset, err)
	}
	if uint64(len(plain)) != usize {
		return invalid("payload at offset %d decompresses to %d bytes, header says %d", offset, len(plain), usize)
	}
	for pos := 0; pos < len(plain); {
		if len(plain)-pos < replication.EventHeaderSize {
			return invalid("payload at offset %d has a truncated inner event", offset)
		}
		h := plain[pos : pos+replication.EventHeaderSize]
		n := int(binary.LittleEndian.Uint32(h[9:13]))
		if n < replication.EventHeaderSize || pos+n > len(plain) {
			return invalid("payload at offset %d has an inner event of %d bytes", offset, n)
		}
		inner := plain[pos : pos+n]
		kind := replication.EventType(h[4])
		ev := &replication.BinlogEvent{RawData: inner, Header: &replication.EventHeader{EventType: kind}}
		if kind == replication.QUERY_EVENT {
			q, err := innerQuery(inner[replication.EventHeaderSize:], queryPostHeader)
			if err != nil {
				return invalid("payload at offset %d: %v", offset, err)
			}
			ev.Event = &replication.QueryEvent{Query: q}
		}
		switch kind {
		case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.ANONYMOUS_GTID_EVENT,
			replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT, replication.TRANSACTION_PAYLOAD_EVENT:
			return invalid("payload at offset %d holds a %s", offset, kind)
		}
		classifyEvent(b, ev)
		pos += n
	}
	return nil
}

// innerQuery extracts a QUERY_EVENT's statement (payload events carry no
// checksums): post-header, status variables, schema, NUL, query.
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

// decompressPayload inflates a zstd frame, refusing output beyond limit.
func decompressPayload(src []byte, limit int64) ([]byte, error) {
	// The LimitReader bounds the output; the window cap bounds the
	// decoder's own buffer (a frame may declare a window larger than its
	// content, so the cap is not the content limit itself).
	window := uint64(max(limit, 8<<20)) //nolint:gosec // limit > 0
	dec, err := zstd.NewReader(bytes.NewReader(src), zstd.WithDecoderMaxWindow(window), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	out, err := io.ReadAll(io.LimitReader(dec, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > limit {
		return nil, fmt.Errorf("decompresses past its declared %d bytes", limit)
	}
	return out, nil
}

// PlanSite converts a loaded version-2 manifest into the planner's view of
// that site (§4.1): active rows (Files), quarantined rows, the divergence
// audit's MissingGTIDs per row, promotion records and the history marker.
// Hints are trusted only when the row's coverage is Complete; the planner
// verifies them against the bytes of every row it downloads.
func PlanSite(m *Manifest) (pitrplan.SiteManifest, error) {
	out := pitrplan.SiteManifest{Site: m.Site, Marked: m.HistoryIntegrity, Promotions: m.Promotions}
	divergent := map[[2]string]pitrplan.Set{}
	for _, d := range m.Divergent {
		set, err := pitrplan.ParseSet(d.MissingGTIDs)
		if err != nil {
			return out, fmt.Errorf("%s: manifest %s divergent row %s: %v", pitrarchive.TokenArchiveObjectInvalid, m.Site, d.Name, err)
		}
		key := [2]string{d.Name, d.ID}
		divergent[key] = divergent[key].Union(set)
	}
	convert := func(e ManifestEntry, state pitrplan.RowState) (pitrplan.Row, error) {
		row := pitrplan.Row{Site: m.Site, Name: e.Name, ID: e.ID, State: state, Created: e.FirstEventTime}
		if e.ID == "" {
			return row, fmt.Errorf("%s: manifest %s row %s has no id; version-2 rows are content-addressed", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name)
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
		if c := e.GTIDCoverage; c != nil && c.Complete {
			gtids, err1 := pitrplan.ParseSet(c.GTIDs)
			empty, err2 := pitrplan.ParseSet(c.EmptyGTIDs)
			if err := errors.Join(err1, err2); err != nil {
				return row, fmt.Errorf("%s: manifest %s row %s hints: %v", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name, err)
			}
			row.Hints = pitrplan.Hints{Known: true, Previous: prev.Union(base), GTIDs: gtids, Empty: empty, Min: c.OriginalCommitMin, Max: c.OriginalCommitMax}
		} else {
			row.Hints = pitrplan.Hints{Previous: prev.Union(base)}
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
