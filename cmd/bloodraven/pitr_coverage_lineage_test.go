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

func retagGTIDFixture(t *testing.T, raw []byte, number uint64) []byte {
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

func joinBinlogFixtures(t *testing.T, files ...[]byte) []byte {
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

func TestPITRBackupStraddlingLineageAccepted(t *testing.T) {
	for _, shape := range []string{"mid file", "mid file rotated", "straddling idle successor", "exact idle boundary", "s31 seed backup markers rotation"} {
		t.Run(shape, func(t *testing.T) {
			file := joinBinlogFixtures(t, coverageStream(t, 9, coverageTime(8, 0, 0), false, false), coverageStream(t, 10, coverageTime(9, 0, 0), false, false), coverageStream(t, 11, coverageTime(9, 40, 0), false, false))
			files := [][]byte{file}
			bound := retentionTestSID + ":1-10"
			switch shape {
			case "mid file rotated":
				files = append(files, coverageStream(t, 12, coverageTime(9, 45, 0), true, false))
			case "straddling idle successor":
				file = joinBinlogFixtures(t, coverageStream(t, 9, coverageTime(9, 0, 0), false, false), coverageStream(t, 10, coverageTime(9, 20, 0), false, false))
				file = patchEventTimestamp(t, file, 3, uint32(coverageTime(9, 31, 0).Unix()))
				files = [][]byte{file, coverageStream(t, 11, coverageTime(9, 40, 0), true, false)}
			case "exact idle boundary":
				files = [][]byte{patchEventTimestamp(t, pitrFixture(t, "incarnation-a.bin"), 3, uint32(coverageTime(9, 31, 0).Unix()))}
				bound = retentionTestSID + ":1-11"
			}
			store := coverageArchive(t, map[string][][]byte{"iad": files})
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, coverageTime(9, 30, 0), t.TempDir(), bound, false, false, "timestamp", slog.Default())
			if err != nil || !result.TargetCovered || result.Partial {
				t.Fatalf("valid backup-straddling history refused: %+v %v", result, err)
			}
		})
	}
}

// A stop that falls inside a transaction committed before the target drops
// that transaction's body, so the stop cannot certify coverage.
func TestPITRInterruptedEarlyCommitRefused(t *testing.T) {
	old := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	interrupted := patchEventTimestamp(t, old, 2, uint32(coverageTime(9, 30, 0).Unix()))
	interrupted = patchEventTimestamp(t, interrupted, 3, uint32(coverageTime(9, 31, 0).Unix()))
	for _, streams := range []map[string][][]byte{{"east": {interrupted}}, {"east": {interrupted}, "iad": {old}}} {
		var sites []string
		for site := range streams {
			sites = append(sites, site)
		}
		store := coverageArchive(t, streams)
		for _, partial := range []bool{false, true} {
			_, err := downloadPITRArchive(context.Background(), store, "binlogs", sites, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, true, "timestamp", slog.Default())
			if err == nil || !strings.Contains(err.Error(), "replay would not apply it") {
				t.Fatalf("interrupted pre-target commit certified: %v", err)
			}
		}
	}
}

func TestPITRReclonedSparseHistoryRefused(t *testing.T) {
	a := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	b := retagGTIDFixture(t, coverageStream(t, 11, coverageTime(10, 0, 0), false, false), 1)
	r := joinBinlogFixtures(t, coverageStream(t, 11, coverageTime(11, 0, 0), false, false), coverageStream(t, 12, coverageTime(11, 40, 0), false, false))
	store := coverageArchive(t, map[string][][]byte{"iad": {a, r}, "pdx": {b}})
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
		_, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(11, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "archive does not contain transactions between the backup bound and") {
			t.Fatalf("clone checkpoint pruned absent transaction bytes: %v", err)
		}
	}
}
