package sidecar

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// An origin transaction is one the server that wrote a binlog file
// originated: a GTID event (GTID_LOG_EVENT or the tagged GTID event) whose
// event-header server_id equals the server_id of the file's
// FORMAT_DESCRIPTION event. It is the planner's definition of an origin copy
// (pitrplan: Txns[i].Srv == WriterServerID); a replicated transaction keeps
// its origin's server_id in its header. The tail drain uploads a read-only
// site's sealed files up to the last one that holds such a transaction.

// originVerdict is what an origin walk has found so far.
type originVerdict int

const (
	// originUnfinished: the walk ran out of its budget; Walk again resumes.
	originUnfinished originVerdict = iota
	// originNone: the whole file was walked and holds no origin transaction.
	originNone
	// originFound: the file holds an origin transaction.
	originFound
)

// originWalkWindow is how much of the file one read fetches. Events smaller
// than the window are stepped over in memory; larger ones are skipped
// without reading their bodies.
const originWalkWindow = 64 << 10

// originBudget bounds the work of one scan's origin walks, like the history
// audit's 64 MiB / 2 s (scanFiles): the scan goroutine must not stall on a
// large file. A walk that exhausts it resumes on the next scan.
type originBudget struct {
	bytes    int64
	deadline time.Time
}

func newOriginBudget() *originBudget {
	return &originBudget{bytes: 64 << 20, deadline: time.Now().Add(2 * time.Second)}
}

func (b *originBudget) spent() bool {
	return b.bytes <= 0 || !time.Now().Before(b.deadline)
}

// originWalk walks the event headers of one plaintext binlog file: the
// 19-byte common header is timestamp(4) type(1) server_id(4) event_size(4)
// log_pos(4) flags(2), and the next header follows event_size bytes after
// the start of this one. No event body is decoded, so a compressed
// transaction payload is skipped whole: its GTID event stands outside it.
// The walk stops at the first origin transaction. It keeps its position, so
// a walk cut short by the budget resumes where it stopped; the caller drops
// it when the file changes.
type originWalk struct {
	offset int64  // of the next event header; 0 before the magic was read
	writer uint32 // the FORMAT_DESCRIPTION event's server_id
	buf    []byte // the read window
	bufOff int64  // file offset of buf[0]
}

// Walk continues the walk of r, a file of size bytes, within budget. Any
// malformed or truncated framing is an error (fail closed: the caller then
// uploads nothing), and the walk must not be continued after one.
func (w *originWalk) Walk(r io.ReaderAt, size int64, budget *originBudget) (originVerdict, error) {
	for {
		// The budget is checked once per window read, not per event.
		if w.offset < size && !w.holds(max(w.offset, 4), replication.EventHeaderSize) && budget.spent() {
			return originUnfinished, nil
		}
		kind, serverID, ok, err := w.next(r, size, budget)
		if err != nil {
			return originUnfinished, err
		}
		if !ok {
			return originNone, nil
		}
		if (kind == replication.GTID_EVENT || kind == replication.GTID_TAGGED_LOG_EVENT) && serverID == w.writer {
			return originFound, nil
		}
	}
}

// next frames the event at the walk's position and steps past it. ok is
// false at the end of the file. The first call reads the magic and the
// FORMAT_DESCRIPTION event, whose server_id is the file's writer.
func (w *originWalk) next(r io.ReaderAt, size int64, budget *originBudget) (kind replication.EventType, serverID uint32, ok bool, err error) {
	first := w.offset == 0
	if first {
		magic, err := w.window(r, 0, 4, size, budget)
		if err != nil {
			return 0, 0, false, fmt.Errorf("read binlog magic: %w", err)
		}
		// A file MySQL encrypted is read through localBinlogs, as its
		// plaintext; the bytes of one are not a binlog.
		if magic[0] != 0xfe || magic[1] != 'b' || magic[2] != 'i' || magic[3] != 'n' {
			return 0, 0, false, fmt.Errorf("not a binlog file: magic % x", magic)
		}
		if size == 4 {
			return 0, 0, false, errors.New("the file has no FORMAT_DESCRIPTION event")
		}
		w.offset = 4
	}
	if w.offset >= size {
		return 0, 0, false, nil
	}
	header, err := w.window(r, w.offset, replication.EventHeaderSize, size, budget)
	if err != nil {
		return 0, 0, false, fmt.Errorf("event header at offset %d: %w", w.offset, err)
	}
	kind = replication.EventType(header[4])
	serverID = binary.LittleEndian.Uint32(header[5:])
	eventSize := int64(binary.LittleEndian.Uint32(header[9:]))
	logPos := binary.LittleEndian.Uint32(header[13:])
	if eventSize < replication.EventHeaderSize || eventSize > size-w.offset {
		return 0, 0, false, fmt.Errorf("event at offset %d has size %d in a file of %d bytes", w.offset, eventSize, size)
	}
	// In a binlog file every event's log_pos is its own end offset
	// (modulo 2^32); anything else is not an event boundary.
	if logPos != uint32(w.offset+eventSize) {
		return 0, 0, false, fmt.Errorf("event at offset %d (size %d) has log_pos %d", w.offset, eventSize, logPos)
	}
	if first {
		if kind != replication.FORMAT_DESCRIPTION_EVENT {
			return 0, 0, false, fmt.Errorf("the first event has type %d, not FORMAT_DESCRIPTION", kind)
		}
		w.writer = serverID
	}
	w.offset += eventSize
	return kind, serverID, true, nil
}

// holds reports whether the current window has n bytes at off.
func (w *originWalk) holds(off int64, n int) bool {
	return off >= w.bufOff && off+int64(n) <= w.bufOff+int64(len(w.buf))
}

// window returns n bytes at off, reading a new window when they are not in
// the current one. size is the file's size.
func (w *originWalk) window(r io.ReaderAt, off int64, n int, size int64, budget *originBudget) ([]byte, error) {
	if off+int64(n) > size {
		return nil, io.ErrUnexpectedEOF
	}
	if !w.holds(off, n) {
		if w.buf == nil {
			w.buf = make([]byte, 0, originWalkWindow)
		}
		want := min(int64(cap(w.buf)), size-off)
		w.buf = w.buf[:want]
		read, err := r.ReadAt(w.buf, off)
		budget.bytes -= int64(read)
		if int64(read) < int64(n) {
			w.buf, w.bufOff = w.buf[:0], 0
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		w.buf, w.bufOff = w.buf[:read], off
	}
	start := off - w.bufOff
	return w.buf[start : start+int64(n)], nil
}
