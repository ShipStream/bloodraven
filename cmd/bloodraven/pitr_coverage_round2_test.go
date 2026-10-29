package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"log/slog"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func round2Retag(t *testing.T, raw []byte, number uint64) []byte {
	t.Helper()
	raw = bytes.Clone(raw)
	sid, _ := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		if raw[off+4] == 33 {
			copy(raw[off+20:off+36], sid)
			binary.LittleEndian.PutUint64(raw[off+36:], number)
			binary.LittleEndian.PutUint32(raw[off+size-4:off+size], crc32.ChecksumIEEE(raw[off:off+size-4]))
		}
		off += size
	}
	return raw
}

func round2Join(t *testing.T, files ...[]byte) []byte {
	t.Helper()
	out := []byte("\xfebin")
	for i, raw := range files {
		for off := 4; off < len(raw); {
			size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
			event := bytes.Clone(raw[off : off+size])
			off += size
			kind := event[4]
			if (i > 0 && (kind == 15 || kind == 35)) || (i < len(files)-1 && kind == 3) {
				continue
			}
			binary.LittleEndian.PutUint32(event[13:17], uint32(len(out)+size))
			binary.LittleEndian.PutUint32(event[size-4:], crc32.ChecksumIEEE(event[:size-4]))
			out = append(out, event...)
		}
	}
	return out
}

func TestRound2StraddlingLineage(t *testing.T) {
	for _, shape := range []string{"mid file", "mid file rotated", "straddling idle successor", "round1 exact idle", "s31 seed backup markers rotation"} {
		t.Run(shape, func(t *testing.T) {
			file := round2Join(t, round1Stream(t, 9, round1Time(8, 0, 0), false, false), round1Stream(t, 10, round1Time(9, 0, 0), false, false), round1Stream(t, 11, round1Time(9, 40, 0), false, false))
			files := [][]byte{file}
			bound := retentionTestSID + ":1-10"
			switch shape {
			case "mid file rotated":
				files = append(files, round1Stream(t, 12, round1Time(9, 45, 0), true, false))
			case "straddling idle successor":
				file = round2Join(t, round1Stream(t, 9, round1Time(9, 0, 0), false, false), round1Stream(t, 10, round1Time(9, 20, 0), false, false))
				file = round1PatchTimestamp(t, file, 3, uint32(round1Time(9, 31, 0).Unix()))
				files = [][]byte{file, round1Stream(t, 11, round1Time(9, 40, 0), true, false)}
			case "round1 exact idle":
				files = [][]byte{round1PatchTimestamp(t, pitrFixture(t, "incarnation-a.bin"), 3, uint32(round1Time(9, 31, 0).Unix()))}
				bound = retentionTestSID + ":1-11"
			}
			store := round1Archive(t, map[string][][]byte{"iad": files})
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), bound, false, false, "timestamp", slog.Default())
			if err != nil || !result.TargetCovered || result.Partial {
				t.Fatalf("valid backup-straddling history refused: %+v %v", result, err)
			}
		})
	}
}

func TestRound2BarrierMustCoverWriter(t *testing.T) {
	for _, nonwriter := range []string{"iad", "zzz"} {
		for _, partial := range []bool{false, true} {
			t.Run(nonwriter+map[bool]string{false: " strict", true: " partial"}[partial], func(t *testing.T) {
				writer := round2Join(t, round2Retag(t, round1Stream(t, 10, round1Time(9, 0, 0), false, false), 1), round2Retag(t, round1Stream(t, 10, round1Time(9, 10, 0), false, false), 2))
				streams := map[string][][]byte{nonwriter: {round1Stream(t, 10, round1Time(10, 0, 0), true, false)}, "pdx": {writer}}
				store := round1Archive(t, streams)
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{nonwriter, "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", slog.Default())
				if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered {
					t.Fatalf("idle stale copy certified writer's missing tail: %+v %v", result, err)
				}
			})
		}
	}
}

func TestRound2InterruptedBarrierOrderRefused(t *testing.T) {
	old := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	interrupted := round1PatchTimestamp(t, old, 2, uint32(round1Time(9, 30, 0).Unix()))
	interrupted = round1PatchTimestamp(t, interrupted, 3, uint32(round1Time(9, 31, 0).Unix()))
	store := round1Archive(t, map[string][][]byte{"east": {interrupted}, "iad": {old}})
	_, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"east", "iad"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", false, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "PITRCrossSiteOrder") {
		t.Fatalf("announced transaction used to prune committed copy: %v", err)
	}
}

func TestRound2ReclonedSparseHistoryRefused(t *testing.T) {
	a := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	b := round2Retag(t, round1Stream(t, 11, round1Time(10, 0, 0), false, false), 1)
	r := round2Join(t, round1Stream(t, 11, round1Time(11, 0, 0), false, false), round1Stream(t, 12, round1Time(11, 40, 0), false, false))
	store := round1Archive(t, map[string][][]byte{"iad": {a, r}, "pdx": {b}})
	// Physical clone carries B in baseGtids, but it does not create B's replay bytes.
	m, err := sidecar.LoadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	m.Files[1].BaseGTIDs = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1"
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Put(context.Background(), sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	for _, partial := range []bool{false, true} {
		_, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(11, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PITRCrossSiteOrder") {
			t.Fatalf("clone checkpoint pruned absent transaction bytes: %v", err)
		}
	}
}
