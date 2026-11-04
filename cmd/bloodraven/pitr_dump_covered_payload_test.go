package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"
)

func TestRound6DumpCoveredInnerCut(t *testing.T) {
	raw := pitrFixture(t, "native/compressed-excluded-cut.bin")
	digest := sha256.Sum256(raw)
	if len(raw) != 772 || hex.EncodeToString(digest[:]) != "8fe0c4785b0af740db890a970cf9224a933aed5dd5a374d953ca8d928373949f" {
		t.Fatal("native review fixture changed")
	}
	sid := "7f4a1927-d377-11f1-a983-f6bbc6ad1253"
	for _, last := range []string{"3", "2"} {
		t.Run("bound through "+last, func(t *testing.T) {
			store := round1Archive(t, map[string][][]byte{"iad": {raw}})
			for _, partial := range []bool{false, true} {
				for _, cross := range []bool{false, true} {
					result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), sid+":1-"+last, partial, cross, "timestamp", slog.Default())
					if last == "3" {
						if err != nil || !result.TargetCovered || result.Partial || len(result.Sites) != 1 || len(result.Sites[0].Staged) != 1 {
							t.Fatalf("dump-covered transaction vetoed needed same-file replay: %+v %v", result, err)
						}
					} else if err == nil || !strings.Contains(err.Error(), "compressed transaction "+sid+":3 has events at or after the target") {
						t.Fatalf("non-excluded inner cut escaped refusal: %+v %v", result, err)
					}
				}
			}
		})
	}
}

func TestRound6LaterRequiredInnerCutStillRefuses(t *testing.T) {
	raw := pitrFixture(t, "native/compressed-excluded-cut.bin")
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	var replacement []byte
	payloads := 0
	err := parser.ParseReader(bytes.NewReader(raw[4:]), func(event *replication.BinlogEvent) error {
		payload, ok := event.Event.(*replication.TransactionPayloadEvent)
		if !ok {
			return nil
		}
		payloads++
		if payloads != 2 {
			return nil
		}
		var body []byte
		for _, inner := range payload.Events {
			data := bytes.Clone(inner.RawData)
			if inner.Header.EventType == replication.XID_EVENT {
				binary.LittleEndian.PutUint32(data[:4], uint32(round1Time(9, 30, 0).Unix()))
			}
			body = append(body, data...)
		}
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			return err
		}
		compressed := encoder.EncodeAll(body, nil)
		encoder.Close()
		header := []byte{}
		for _, field := range []struct {
			key   byte
			value uint64
		}{{1, uint64(len(compressed))}, {2, 0}, {3, uint64(len(body))}} {
			value := mysql.PutLengthEncodedInt(field.value)
			header = append(header, field.key, byte(len(value)))
			header = append(header, value...)
		}
		replacement = append(bytes.Clone(event.RawData[:19]), header...)
		replacement = append(replacement, 0)
		replacement = append(replacement, compressed...)
		replacement = append(replacement, make([]byte, 4)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if payloads != 2 || replacement == nil {
		t.Fatal("native fixture must contain both transactions")
	}
	events := round4Events(raw)
	payloads = 0
	for i, event := range events {
		if event[4] == 40 {
			payloads++
			if payloads == 2 {
				events[i] = replacement
			}
		}
	}
	raw = round4Reframe(events)
	sid := "7f4a1927-d377-11f1-a983-f6bbc6ad1253"
	store := round1Archive(t, map[string][][]byte{"iad": {raw}})
	for _, partial := range []bool{false, true} {
		for _, cross := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), sid+":1-3", partial, cross, "timestamp", slog.Default())
			if err == nil || !strings.Contains(err.Error(), "compressed transaction "+sid+":4 has events at or after the target") {
				t.Fatalf("a later non-excluded cut was lost: %+v %v", result, err)
			}
		}
	}
}
