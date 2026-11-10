package sidecar

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// walkOrigin runs one origin walk over data with a generous budget.
func walkOrigin(t *testing.T, data []byte) (originVerdict, error) {
	t.Helper()
	return (&originWalk{}).Walk(bytes.NewReader(data), int64(len(data)), newOriginBudget())
}

// originTxns are the transactions sid:from..to as committed on serverID.
func originTxns(sid string, serverID uint32, from, to int64) []binlogtest.Txn {
	var out []binlogtest.Txn
	for gno := from; gno <= to; gno++ {
		out = append(out, binlogtest.Txn{SID: sid, GNO: gno, ServerID: serverID, Commit: time.Unix(1776243600+gno, 0)})
	}
	return out
}

func TestOriginWalkGeneratedFiles(t *testing.T) {
	replica := originTxns(hintSIDA, 101, 1, 3)
	own := originTxns(hintSIDB, 102, 1, 2)
	for _, tc := range []struct {
		name string
		file binlogtest.File
		want originVerdict
	}{
		{"origin file", binlogtest.File{ServerID: 102, Txns: own}, originFound},
		{"pure replica file", binlogtest.File{ServerID: 102, Txns: replica}, originNone},
		{"replica copies, then the writable era", binlogtest.File{ServerID: 102, Txns: append(append([]binlogtest.Txn{}, replica...), own...)}, originFound},
		{"the writable era, then replica copies", binlogtest.File{ServerID: 102, Txns: append(append([]binlogtest.Txn{}, own...), replica...)}, originFound},
		{"no transaction", binlogtest.File{ServerID: 102}, originNone},
		// The writer's own source UUID under another server's id (a
		// server_id that changed) is not an origin copy for the planner.
		{"own uuid, other server_id", binlogtest.File{ServerID: 102, Txns: originTxns(hintSIDB, 7, 1, 2)}, originNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := walkOrigin(t, tc.file.MustBytes())
			if err != nil || got != tc.want {
				t.Fatalf("verdict %d, %v; want %d", got, err, tc.want)
			}
		})
	}
}

// The failback capture is a genuine MySQL 9.7 pair with tagged GTIDs and
// binlog_transaction_compression=ON: a's mysql-bin.000003 mixes a's era
// with b's as replica copies. The walk needs no payload decoding: a
// compressed transaction's GTID event stands outside its payload.
func TestOriginWalkNativeFiles(t *testing.T) {
	for _, name := range []string{"a-mysql-bin.000001.bin", "a-mysql-bin.000002.bin", "a-mysql-bin.000003.bin"} {
		data, err := os.ReadFile(filepath.Join("testdata", "native", "failback", name))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := walkOrigin(t, data); err != nil || got != originFound {
			t.Fatalf("%s as written by a: verdict %d, %v", name, got, err)
		}
		// The same events in a file another server wrote are replica
		// copies, except where the file holds that server's own (b's era
		// in the mixed file).
		copied := append([]byte(nil), data...)
		binary.LittleEndian.PutUint32(copied[4+5:], 102)
		want := originNone
		if name == "a-mysql-bin.000003.bin" {
			want = originFound
		}
		if got, err := walkOrigin(t, copied); err != nil || got != want {
			t.Fatalf("%s as written by server 102: verdict %d, %v; want %d", name, got, err, want)
		}
		binary.LittleEndian.PutUint32(copied[4+5:], 999)
		if got, err := walkOrigin(t, copied); err != nil || got != originNone {
			t.Fatalf("%s as written by server 999: verdict %d, %v", name, got, err)
		}
	}
	// A tagged GTID event (type 42) alone marks an origin transaction.
	tagged, err := os.ReadFile(filepath.Join("testdata", "tagged-transaction.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := walkOrigin(t, tagged); err != nil || got != originFound {
		t.Fatalf("tagged GTID file: verdict %d, %v", got, err)
	}
}

// Every native capture frames to its end: the walk's size and log_pos
// checks hold on real MySQL 8.4 and 9.7 files (checksum off, compressed
// payloads, XA, LOAD DATA, tagged GTIDs, empty rotations).
func TestOriginWalkFramesNativeCaptures(t *testing.T) {
	var files []string
	for _, pattern := range []string{"testdata/native/*.bin", "testdata/native/failback/*.bin", "testdata/native/txindex/*.bin"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	if len(files) < 15 {
		t.Fatalf("found only %d native captures", len(files))
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		w := &originWalk{}
		events := 0
		for {
			_, _, ok, err := w.next(bytes.NewReader(data), int64(len(data)), newOriginBudget())
			if err != nil {
				t.Fatalf("%s: event %d: %v", path, events, err)
			}
			if !ok {
				break
			}
			events++
		}
		if events < 2 || w.offset != int64(len(data)) {
			t.Fatalf("%s: framed %d events through offset %d of %d", path, events, w.offset, len(data))
		}
	}
}

// A truncated or corrupt file is an error, never a verdict of "none" and
// never a panic: a pure replica file cut anywhere but at an event boundary
// fails closed.
func TestOriginWalkTruncatedAndCorruptFiles(t *testing.T) {
	data := binlogtest.File{ServerID: 102, Txns: originTxns(hintSIDA, 101, 1, 3)}.MustBytes()
	boundaries := map[int]bool{}
	w := &originWalk{}
	for {
		_, _, ok, err := w.next(bytes.NewReader(data), int64(len(data)), newOriginBudget())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		boundaries[int(w.offset)] = true
	}
	for cut := 0; cut < len(data); cut++ {
		got, err := walkOrigin(t, data[:cut])
		switch {
		case boundaries[cut]:
			if err != nil || got != originNone {
				t.Fatalf("cut at the event boundary %d: verdict %d, %v", cut, got, err)
			}
		case err == nil:
			t.Fatalf("cut at %d inside an event: verdict %d and no error", cut, got)
		}
	}

	corrupt := func(change func(b []byte)) []byte {
		b := append([]byte(nil), data...)
		change(b)
		return b
	}
	second := 4 + int(binary.LittleEndian.Uint32(data[4+9:])) // the PREVIOUS_GTIDS event
	for name, b := range map[string][]byte{
		"wrong magic":         corrupt(func(b []byte) { b[0] = 0x00 }),
		"event size 0":        corrupt(func(b []byte) { binary.LittleEndian.PutUint32(b[second+9:], 0) }),
		"event size below 19": corrupt(func(b []byte) { binary.LittleEndian.PutUint32(b[second+9:], 18) }),
		"event size past EOF": corrupt(func(b []byte) { binary.LittleEndian.PutUint32(b[second+9:], uint32(len(b))) }),
		"event size off by one": corrupt(func(b []byte) {
			binary.LittleEndian.PutUint32(b[second+9:], binary.LittleEndian.Uint32(b[second+9:])+1)
		}),
		"log_pos elsewhere":     corrupt(func(b []byte) { binary.LittleEndian.PutUint32(b[second+13:], 7) }),
		"no format description": corrupt(func(b []byte) { b[4+4] = 35 }),
		"magic only":            data[:4],
		"empty":                 nil,
	} {
		if got, err := walkOrigin(t, b); err == nil {
			t.Errorf("%s: verdict %d and no error", name, got)
		}
	}
	encrypted := corrupt(func(b []byte) { b[0] = 0xfd })
	if _, err := walkOrigin(t, encrypted); !errors.Is(err, errEncryptedBinlog) {
		t.Errorf("an encrypted binlog: %v", err)
	}
}

// A walk the budget cuts short resumes where it stopped and reaches the
// verdict of an unbounded walk, reading no window twice.
func TestOriginWalkResumesWithinBudget(t *testing.T) {
	// Replica copies beyond one read window, then one origin transaction.
	txns := originTxns(hintSIDA, 101, 1, 900)
	txns = append(txns, originTxns(hintSIDB, 102, 1, 1)...)
	data := binlogtest.File{ServerID: 102, Txns: txns}.MustBytes()
	if len(data) < 2*originWalkWindow {
		t.Fatalf("fixture of %d bytes does not span two windows", len(data))
	}
	for _, last := range []struct {
		name string
		data []byte
		want originVerdict
	}{
		{"origin transaction at the end", data, originFound},
		{"no origin transaction", binlogtest.File{ServerID: 102, Txns: txns[:900]}.MustBytes(), originNone},
	} {
		t.Run(last.name, func(t *testing.T) {
			w := &originWalk{}
			rounds, verdict := 0, originUnfinished
			var read int64
			for verdict == originUnfinished {
				if rounds++; rounds > 100 {
					t.Fatal("the walk does not finish")
				}
				// One window per round.
				budget := &originBudget{bytes: 1, deadline: time.Now().Add(time.Minute)}
				var err error
				if verdict, err = w.Walk(bytes.NewReader(last.data), int64(len(last.data)), budget); err != nil {
					t.Fatal(err)
				}
				read += 1 - budget.bytes
			}
			if verdict != last.want || rounds < 2 {
				t.Fatalf("verdict %d after %d rounds, want %d after several", verdict, rounds, last.want)
			}
			if read > int64(len(last.data))+originWalkWindow {
				t.Fatalf("read %d bytes of a %d-byte file", read, len(last.data))
			}
		})
	}
	// A spent budget reads nothing.
	w := &originWalk{}
	spent := &originBudget{bytes: 0, deadline: time.Now().Add(time.Minute)}
	if verdict, err := w.Walk(bytes.NewReader(data), int64(len(data)), spent); err != nil || verdict != originUnfinished || spent.bytes != 0 {
		t.Fatalf("spent budget: verdict %d, %v, bytes %d", verdict, err, spent.bytes)
	}
	late := &originBudget{bytes: 1 << 20, deadline: time.Now().Add(-time.Second)}
	if verdict, err := w.Walk(bytes.NewReader(data), int64(len(data)), late); err != nil || verdict != originUnfinished {
		t.Fatalf("past deadline: verdict %d, %v", verdict, err)
	}
}
