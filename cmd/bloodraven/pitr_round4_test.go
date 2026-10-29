package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"log/slog"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/klauspost/compress/zstd"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// Rebuild native event positions and CRCs after inserting or replacing queries.
func round4Reframe(events [][]byte) []byte {
	out := []byte("\xfebin")
	for _, event := range events {
		event = bytes.Clone(event)
		binary.LittleEndian.PutUint32(event[9:13], uint32(len(event)))
		binary.LittleEndian.PutUint32(event[13:17], uint32(len(out)+len(event)))
		binary.LittleEndian.PutUint32(event[len(event)-4:], crc32.ChecksumIEEE(event[:len(event)-4]))
		out = append(out, event...)
	}
	return out
}
func round4Events(raw []byte) [][]byte {
	var events [][]byte
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		events = append(events, bytes.Clone(raw[off:off+size]))
		off += size
	}
	return events
}
func round4Query(event []byte) string {
	body := event[19 : len(event)-4]
	start := 13 + int(binary.LittleEndian.Uint16(body[11:13])) + int(body[8]) + 1
	return string(body[start:])
}
func round4SetQuery(event []byte, query string) []byte {
	old := round4Query(event)
	out := bytes.Clone(event[:len(event)-4-len(old)])
	out = append(out, query...)
	return append(out, make([]byte, 4)...)
}
func round4Savepoint(t *testing.T, raw []byte) []byte {
	t.Helper()
	var events [][]byte
	inserted, xid := false, false
	for _, event := range round4Events(raw) {
		events = append(events, event)
		if event[4] == 2 && round4Query(event) == "BEGIN" {
			events = append(events, round4SetQuery(event, "SAVEPOINT `s1`"))
			inserted = true
		}
		xid = xid || event[4] == 16
	}
	if !inserted || !xid {
		t.Fatal("native fixture must contain BEGIN ... XID")
	}
	return round4Reframe(events)
}
func round4MoveClock(raw []byte, at time.Time) []byte {
	events := round4Events(raw)
	for _, event := range events {
		if binary.LittleEndian.Uint32(event[:4]) != 0 {
			binary.LittleEndian.PutUint32(event[:4], uint32(at.Unix()))
		}
	}
	return round4Reframe(events)
}
func TestRound4NativePredecessorCoverage(t *testing.T) {
	for _, savepoint := range []bool{false, true} {
		t.Run(map[bool]string{false: "BEGIN to XID", true: "native SAVEPOINT"}[savepoint], func(t *testing.T) {
			raw := pitrFixture(t, "native/initial.bin")
			if savepoint {
				raw = round4Savepoint(t, raw)
			} else {
				// Verify the control uses the native transactional shape too.
				_ = round4Savepoint(t, raw)
			}
			evidence, err := sidecar.InspectBinlog(bytes.NewReader(raw), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if !evidence.Metadata.GTIDCoverage.Complete {
				t.Fatal("fixture must have complete verified whole-file coverage")
			}
			var sid string
			for key := range evidence.GTIDs {
				sid = key
			}
			at := evidence.NewestEventTime
			idle := round4MoveClock(pitrFixture(t, "native/flush-empty.bin"), at.Add(10*time.Minute))
			store := round1Archive(t, map[string][][]byte{"iad": {raw, idle}})
			for _, partial := range []bool{false, true} {
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, at.Add(5*time.Minute), t.TempDir(), sid+":1", partial, false, "timestamp", slog.Default())
				if err != nil || !result.TargetCovered || result.Partial {
					t.Fatalf("archived native predecessor refused: %+v %v", result, err)
				}
			}
		})
	}
}
func TestRound4AdminAndCompressedPredecessors(t *testing.T) {
	for _, query := range []string{"ANALYZE TABLE replay.markers", "OPTIMIZE TABLE replay.markers", "FLUSH PRIVILEGES", "compressed transaction"} {
		t.Run(query, func(t *testing.T) {
			raw := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
			if query == "compressed transaction" {
				raw = pitrFixture(t, "compressed-transaction.bin")
			} else {
				events := round4Events(raw)
				for i, event := range events {
					if event[4] == 2 {
						events[i] = round4SetQuery(event, query)
					}
				}
				raw = round4Reframe(events)
			}
			evidence, err := sidecar.InspectBinlog(bytes.NewReader(raw), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if !evidence.Metadata.GTIDCoverage.Complete {
				t.Fatal("whole-file coverage must be complete")
			}
			barrier := round1PatchTimestamp(t, round1Stream(t, 11, round1Time(9, 20, 0), false, false), 3, uint32(round1Time(9, 35, 0).Unix()))
			store := round1Archive(t, map[string][][]byte{"iad": {raw, barrier}})
			for _, partial := range []bool{false, true} {
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", slog.Default())
				if err != nil || !result.TargetCovered || result.Partial {
					t.Fatalf("archived predecessor refused: %+v %v", result, err)
				}
			}
		})
	}
}

func round4CompressNativeTransaction(t *testing.T, raw []byte) []byte {
	t.Helper()
	events := round4Events(raw)
	begin, end := -1, -1
	for i, event := range events {
		if event[4] == 2 && round4Query(event) == "BEGIN" {
			begin = i
		}
		if begin >= 0 && event[4] == 16 {
			end = i
			break
		}
	}
	if begin < 0 || end < 0 {
		t.Fatal("native transaction body absent")
	}
	var inner []byte
	for _, event := range events[begin : end+1] {
		event = bytes.Clone(event[:len(event)-4])
		binary.LittleEndian.PutUint32(event[9:13], uint32(len(event)))
		binary.LittleEndian.PutUint32(event[13:17], 0)
		inner = append(inner, event...)
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll(inner, nil)
	encoder.Close()
	body := []byte{}
	for _, field := range []struct {
		key   byte
		value uint64
	}{{1, uint64(len(compressed))}, {2, 0}, {3, uint64(len(inner))}} {
		value := mysql.PutLengthEncodedInt(field.value)
		body = append(body, field.key, byte(len(value)))
		body = append(body, value...)
	}
	body = append(body, 0)
	body = append(body, compressed...)
	payload := bytes.Clone(events[begin][:19])
	payload[4] = 40
	payload = append(payload, body...)
	payload = append(payload, make([]byte, 4)...)
	result := append([][]byte{}, events[:begin]...)
	result = append(result, payload)
	result = append(result, events[end+1:]...)
	return round4Reframe(result)
}

func TestRound4NativeStopPrefixContainment(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "savepoint prefix", true: "decoded compressed prefix"}[compressed], func(t *testing.T) {
			raw := round4Savepoint(t, pitrFixture(t, "native/initial.bin"))
			if compressed {
				raw = round4CompressNativeTransaction(t, raw)
			}
			full, err := sidecar.InspectBinlog(bytes.NewReader(raw), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			var sid string
			for key := range full.GTIDs {
				sid = key
			}
			stop := full.NewestEventTime.Add(5 * time.Minute)
			events := round4Events(raw)
			last := events[len(events)-1]
			if last[4] != 4 {
				last = bytes.Clone(last[:19])
				last[4] = 3
				last = append(last, make([]byte, 4)...)
				events = append(events, last)
			}
			binary.LittleEndian.PutUint32(last[:4], uint32(full.NewestEventTime.Add(10*time.Minute).Unix()))
			barrier := round4Reframe(events)
			prefix, err := sidecar.InspectBinlog(bytes.NewReader(barrier), stop)
			if err != nil {
				t.Fatal(err)
			}
			if !prefix.StopReached || !prefix.CompletedBeforeStopGTIDs.Contains(full.GTIDs) {
				t.Fatalf("decoded native prefix was not credited: %+v", prefix)
			}
			store := round1Archive(t, map[string][][]byte{"iad": {barrier}, "pdx": {raw}})
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, stop, t.TempDir(), sid+":1", false, true, "timestamp", slog.Default())
			if err != nil || !result.TargetCovered || !result.CrossSiteUnverified {
				t.Fatalf("native completed-prefix containment refused: %+v %v", result, err)
			}
		})
	}
}
