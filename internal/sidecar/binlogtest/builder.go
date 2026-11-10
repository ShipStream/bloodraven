// Package binlogtest builds small, CRC32-checked MySQL v4 binlog files for
// tests: a format description, previous GTIDs, GTID transactions with
// original commit timestamps and server ids, and a stop event. The layout
// matches internal/sidecar/testdata/generate_binlogs.py, plus commit
// timestamps and empty (BEGIN/COMMIT only) transactions.
package binlogtest

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// Txn is one GTID transaction.
type Txn struct {
	// SID is the GTID source UUID, GNO its number.
	SID string
	GNO int64
	// Commit is the original commit time (µs precision). Zero writes no
	// commit timestamps.
	Commit time.Time
	// ServerID is the originating server id in the event headers.
	ServerID uint32
	// Empty writes BEGIN and COMMIT only (an injected or filtered
	// placeholder); otherwise Statement (default an INSERT) is written.
	Empty     bool
	Statement string
	// Rows, when set, makes this a row transaction instead: BEGIN, a
	// TABLE_MAP of orders.markers (one column), one WRITE_ROWS event whose
	// row images are these bytes, and an XID. The index pass digests such
	// a transaction's row changes (pitrplan.EffectDigest).
	Rows []byte
}

// File is one sealed binlog.
type File struct {
	// ServerID is the writing server, in the format description event.
	ServerID uint32
	// Created is the format description timestamp.
	Created time.Time
	// Previous is the Previous_gtids set ("" for none).
	Previous string
	Txns     []Txn
}

// Bytes encodes the file.
func (f File) Bytes() ([]byte, error) {
	out := []byte{0xfe, 'b', 'i', 'n'}
	event := func(kind byte, ts time.Time, serverID uint32, body []byte) {
		size := 19 + len(body) + 4
		header := make([]byte, 19)
		binary.LittleEndian.PutUint32(header[0:], uint32(ts.Unix()))
		header[4] = kind
		binary.LittleEndian.PutUint32(header[5:], serverID)
		binary.LittleEndian.PutUint32(header[9:], uint32(size))
		binary.LittleEndian.PutUint32(header[13:], uint32(len(out)+size))
		data := append(header, body...)
		out = append(out, data...)
		out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(data))
	}
	created := f.Created
	if created.IsZero() {
		created = time.Unix(1776243600, 0)
	}
	fde := make([]byte, 0, 100)
	fde = binary.LittleEndian.AppendUint16(fde, 4)
	version := make([]byte, 50)
	copy(version, "9.7.0-log")
	fde = append(fde, version...)
	fde = binary.LittleEndian.AppendUint32(fde, uint32(created.Unix()))
	fde = append(fde, 19)
	lengths := make([]byte, 41)
	lengths[1] = 13 // QUERY_EVENT post-header
	// TABLE_MAP and the version-2 rows events, as MySQL 9.7 declares them.
	lengths[18], lengths[29], lengths[30], lengths[31] = 8, 10, 10, 10
	fde = append(fde, lengths...)
	fde = append(fde, 1) // CRC32
	event(15, created, f.ServerID, fde)

	previous, err := previousGTIDsBody(f.Previous)
	if err != nil {
		return nil, err
	}
	event(35, time.Unix(0, 0), f.ServerID, previous)

	last := created
	for i, t := range f.Txns {
		sid, err := sidBytes(t.SID)
		if err != nil {
			return nil, err
		}
		ts := t.Commit
		if ts.IsZero() {
			ts = created
		}
		last = ts
		body := []byte{0x01}
		body = append(body, sid...)
		body = binary.LittleEndian.AppendUint64(body, uint64(t.GNO))
		body = append(body, 0x02)
		body = binary.LittleEndian.AppendUint64(body, uint64(i))
		body = binary.LittleEndian.AppendUint64(body, uint64(i+1))
		if !t.Commit.IsZero() {
			micros := uint64(t.Commit.UnixMicro())
			body = appendUint56(body, micros|1<<55) // immediate, original follows
			body = appendUint56(body, micros)       // original
			body = append(body, 0)                  // transaction_length (unset)
			body = binary.LittleEndian.AppendUint32(body, 90700)
		}
		event(33, ts, t.ServerID, body)
		if t.Empty {
			event(2, ts, t.ServerID, queryBody("BEGIN"))
			event(2, ts, t.ServerID, queryBody("COMMIT"))
			continue
		}
		if t.Rows != nil {
			event(2, ts, t.ServerID, queryBody("BEGIN"))
			event(19, ts, t.ServerID, TableMapBody(rowsTableID, "orders", "markers", 15))
			event(30, ts, t.ServerID, RowsBody(rowsTableID, true, 1, [][]byte{{0x01}}, t.Rows))
			event(16, ts, t.ServerID, make([]byte, 8))
			continue
		}
		stmt := t.Statement
		if stmt == "" {
			stmt = fmt.Sprintf("INSERT INTO markers VALUES ('%s:%d')", t.SID, t.GNO)
		}
		event(2, ts, t.ServerID, queryBody(stmt))
	}
	event(3, last.Add(time.Second), f.ServerID, nil)
	return out, nil
}

// MustBytes is Bytes for fixtures that cannot fail.
func (f File) MustBytes() []byte {
	b, err := f.Bytes()
	if err != nil {
		panic(err)
	}
	return b
}

// rowsTableID is the table id of a Rows transaction's table map.
const rowsTableID = 108

// TableMapBody is a TABLE_MAP event's post-header and payload: the table
// id, flags, the database and table names, and one type byte per column
// (15 is VARCHAR, 245 JSON), with zeroed column metadata and null bitmap.
func TableMapBody(id uint64, db, table string, types ...byte) []byte {
	body := appendUint48(nil, id)
	body = append(body, 1, 0) // flags
	body = append(append(append(body, byte(len(db))), db...), 0)
	body = append(append(append(body, byte(len(table))), table...), 0)
	body = append(body, byte(len(types)))
	body = append(body, types...)
	body = append(body, byte(2*len(types)))                // metadata length
	body = append(body, make([]byte, 2*len(types))...)     // metadata
	return append(body, make([]byte, (len(types)+7)/8)...) // null bitmap
}

// RowsBody is a version-2 rows event's post-header and payload: the table
// id, flags (STMT_END_F when end), an empty extra-data block, the column
// count, the columns-present bitmaps (one, or two for an update) and the
// row images.
func RowsBody(id uint64, end bool, columns int, bitmaps [][]byte, rows []byte) []byte {
	body := appendUint48(nil, id)
	flags := byte(0)
	if end {
		flags = 1
	}
	body = append(body, flags, 0)
	body = append(body, 2, 0) // extra-data length: its own two bytes
	body = append(body, byte(columns))
	for _, b := range bitmaps {
		body = append(body, b...)
	}
	return append(body, rows...)
}

func appendUint48(b []byte, v uint64) []byte {
	for i := 0; i < 6; i++ {
		b = append(b, byte(v>>(8*i)))
	}
	return b
}

func appendUint56(b []byte, v uint64) []byte {
	for i := 0; i < 7; i++ {
		b = append(b, byte(v>>(8*i)))
	}
	return b
}

func queryBody(query string) []byte {
	body := make([]byte, 0, 13+8+len(query))
	body = binary.LittleEndian.AppendUint32(body, 1) // thread id
	body = binary.LittleEndian.AppendUint32(body, 0) // exec time
	body = append(body, 6)                           // schema length
	body = binary.LittleEndian.AppendUint16(body, 0) // error code
	body = binary.LittleEndian.AppendUint16(body, 0) // status vars length
	body = append(body, "orders\x00"...)
	return append(body, query...)
}

func sidBytes(sid string) ([]byte, error) {
	b, err := hex.DecodeString(strings.ReplaceAll(sid, "-", ""))
	if err != nil || len(b) != 16 {
		return nil, fmt.Errorf("binlogtest: bad source uuid %q", sid)
	}
	return b, nil
}

func previousGTIDsBody(previous string) ([]byte, error) {
	set, err := internalmysql.ParseGTIDSet(previous)
	if err != nil {
		return nil, err
	}
	sids := make([]string, 0, len(set))
	for sid := range set {
		if strings.Contains(sid, ":") {
			return nil, fmt.Errorf("binlogtest: tagged previous GTIDs are not supported")
		}
		sids = append(sids, sid)
	}
	sort.Strings(sids)
	body := binary.LittleEndian.AppendUint64(nil, uint64(len(sids)))
	for _, sid := range sids {
		raw, err := sidBytes(sid)
		if err != nil {
			return nil, err
		}
		body = append(body, raw...)
		body = binary.LittleEndian.AppendUint64(body, uint64(len(set[sid])))
		for _, iv := range set[sid] {
			body = binary.LittleEndian.AppendUint64(body, uint64(iv.Start))
			body = binary.LittleEndian.AppendUint64(body, uint64(iv.End+1))
		}
	}
	return body, nil
}
