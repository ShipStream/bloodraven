package sidecar

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestRound5RealCompressedRetentionAndPrefix(t *testing.T) {
	raw, err := os.ReadFile("testdata/native/compressed-session.bin")
	if err != nil {
		t.Fatal(err)
	}
	payloads, large, multi := 0, false, false
	evidence, err := inspectBinlog(bytes.NewReader(raw), time.Time{}, func(event *replication.BinlogEvent) {
		if payload, ok := event.Event.(*replication.TransactionPayloadEvent); ok {
			payloads++
			large = large || len(payload.Payload) >= 251
			writes := 0
			for _, inner := range payload.Events {
				if inner.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
					writes++
				}
			}
			multi = multi || writes > 1
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if payloads != 2 || !large || !multi {
		t.Fatalf("capture lacks genuine single/multi large payloads: count=%d large=%t multi=%t", payloads, large, multi)
	}
	if !evidence.Metadata.GTIDCoverage.Complete || evidence.GTIDs.TransactionCount() != 2 {
		t.Fatalf("real compressed retention coverage incomplete: %+v", evidence)
	}
	stop := evidence.LastTransactionEventTime.Add(time.Second)
	prefix, err := InspectBinlog(bytes.NewReader(raw), stop)
	if err != nil {
		t.Fatal(err)
	}
	if !prefix.StopReached || !prefix.CompletedBeforeStopGTIDs.Contains(evidence.GTIDs) {
		t.Fatalf("real compressed prefix incomplete: %+v", prefix)
	}
}

func TestRound5PayloadLengthEncodingBoundaries(t *testing.T) {
	for _, tc := range []struct {
		wire []byte
		want uint64
	}{
		{[]byte{250}, 250}, {[]byte{0xfc, 251, 0}, 251}, {[]byte{0xfc, 255, 255}, 65535},
		{[]byte{0xfd, 0, 0, 1}, 65536}, {[]byte{0xfe, 0, 0, 0, 0, 1, 0, 0, 0}, 1 << 32},
	} {
		got, n, err := payloadLengthEncodedInt(tc.wire)
		if err != nil || got != tc.want || n != len(tc.wire) {
			t.Fatalf("length encoding %x: %d/%d %v", tc.wire, got, n, err)
		}
	}
	for _, wire := range [][]byte{nil, {0xfb}, {0xff}, {0xfc, 1}, {0xfd, 1, 2}, {0xfe, 1, 2, 3, 4, 5, 6, 7}} {
		if _, _, err := payloadLengthEncodedInt(wire); err == nil {
			t.Fatalf("invalid encoding accepted: %x", wire)
		}
	}
}
