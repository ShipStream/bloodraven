package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func round1Time(hour, minute, second int) time.Time {
	return time.Date(2026, 4, 15, hour, minute, second, 0, time.UTC)
}

// Preserve native framing and CRCs while varying history, clocks, and idle files.
func round1Stream(t *testing.T, previous uint64, at time.Time, idle, foreign bool) []byte {
	t.Helper()
	raw := pitrFixture(t, "incarnation-a.bin")
	result := []byte("\xfebin")
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		event := bytes.Clone(raw[off : off+size])
		kind := event[4]
		off += size
		if idle && (kind == 33 || kind == 2) {
			continue
		}
		ts := at
		if kind == 33 || kind == 2 {
			ts = ts.Add(time.Second)
		}
		if kind == 3 {
			ts = ts.Add(2 * time.Second)
		}
		if kind != 35 {
			binary.LittleEndian.PutUint32(event[:4], uint32(ts.Unix()))
		}
		if kind == 35 {
			binary.LittleEndian.PutUint64(event[19+40:], previous+1)
		}
		if kind == 33 {
			binary.LittleEndian.PutUint64(event[19+17:], previous+1)
		}
		if foreign && (kind == 35 || kind == 33) {
			sid, _ := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			offset := 19 + 8
			if kind == 33 {
				offset = 19 + 1
			}
			copy(event[offset:offset+16], sid)
		}
		binary.LittleEndian.PutUint32(event[13:17], uint32(len(result)+size))
		binary.LittleEndian.PutUint32(event[size-4:], crc32.ChecksumIEEE(event[:size-4]))
		result = append(result, event...)
	}
	return result
}

func round1PatchTimestamp(t *testing.T, raw []byte, kind byte, ts uint32) []byte {
	t.Helper()
	raw = bytes.Clone(raw)
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		if raw[off+4] == kind {
			binary.LittleEndian.PutUint32(raw[off:off+4], ts)
			binary.LittleEndian.PutUint32(raw[off+size-4:off+size], crc32.ChecksumIEEE(raw[off:off+size-4]))
		}
		off += size
	}
	return raw
}

func round1Archive(t *testing.T, streams map[string][][]byte) sidecar.ArchiveStore {
	t.Helper()
	store, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	for site, files := range streams {
		m := sidecar.Manifest{Site: site}
		for i, data := range files {
			e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			row := sidecar.ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1), ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs}
			key, err := sidecar.ArchiveEntryKey("binlogs", site, row)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatal(err)
			}
			m.Files = append(m.Files, row)
		}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Put(context.Background(), sidecar.ManifestKey("binlogs", site), bytes.NewReader(raw), int64(len(raw))); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func TestRound1IdleCoverage(t *testing.T) {
	for _, shape := range []string{"idle sealed file", "backed-up transaction then idle rotation"} {
		t.Run(shape, func(t *testing.T) {
			idle := round1Stream(t, 11, round1Time(9, 0, 0), true, false)
			idle = round1PatchTimestamp(t, idle, 3, uint32(round1Time(9, 31, 0).Unix()))
			files := [][]byte{idle}
			if shape != "idle sealed file" {
				old := round1PatchTimestamp(t, pitrFixture(t, "incarnation-a.bin"), 3, uint32(round1Time(9, 31, 0).Unix()))
				files = append([][]byte{old}, files...)
			}
			store := round1Archive(t, map[string][][]byte{"iad": files})
			for _, partial := range []bool{false, true} {
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-11", partial, false, "timestamp", slog.Default())
				if err != nil || !result.TargetCovered || result.Partial || len(result.Sites[0].Staged) != 0 {
					t.Fatalf("idle window lost verified coverage: %+v %v", result, err)
				}
			}
		})
	}
}

func TestRound1ChronologicalGuard(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, name := range []string{"ordered failover", "reverse failover", "failback", "overlap", "equal boundary", "foreign barrier", "interrupted transaction", "nontransaction barrier", "missing timestamp"} {
		t.Run(name, func(t *testing.T) {
			early := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
			late := round1Stream(t, 11, round1Time(11, 0, 0), false, false)
			streams := map[string][][]byte{"east": {early}, "iad": {late}}
			switch name {
			case "reverse failover":
				streams = map[string][][]byte{"east": {late}, "iad": {early}}
			case "failback":
				streams = map[string][][]byte{"east": {early, round1Stream(t, 12, round1Time(11, 0, 0), false, false)}, "iad": {round1Stream(t, 11, round1Time(10, 0, 0), false, false)}}
			case "overlap":
				streams["iad"] = [][]byte{round1Stream(t, 11, round1Time(9, 0, 1), false, false)}
			case "equal boundary":
				streams["iad"] = [][]byte{round1Stream(t, 11, round1Time(9, 0, 2), false, false)}
			case "foreign barrier":
				streams["iad"] = [][]byte{early}
				streams["east"] = [][]byte{round1Stream(t, 10, round1Time(12, 0, 0), false, true)}
			case "interrupted transaction":
				streams = map[string][][]byte{"east": {round1PatchTimestamp(t, early, 2, uint32(round1Time(11, 0, 2).Unix()))}, "iad": {early}}
			case "nontransaction barrier":
				streams["iad"] = [][]byte{round1Stream(t, 10, round1Time(11, 0, 0), false, false)}
				streams["east"] = [][]byte{round1Stream(t, 10, round1Time(12, 0, 0), true, true)}
			case "missing timestamp":
				streams["iad"] = [][]byte{round1Stream(t, 10, round1Time(11, 0, 0), false, false)}
				streams["east"] = [][]byte{round1PatchTimestamp(t, early, 15, 0)}
			}
			store := round1Archive(t, streams)
			for _, partial := range []bool{false, true} {
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "east"}, round1Time(11, 0, 2), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", logger)
				if name == "foreign barrier" && result.TargetCovered {
					t.Fatal("foreign barrier certified an uncovered source archive")
				}
				if name == "ordered failover" {
					if err != nil || !result.TargetCovered {
						t.Fatalf("ordered failover refused: %+v %v", result, err)
					}
					for _, site := range result.Sites {
						if len(site.Staged) != 1 || !site.HasReplayTransactions {
							t.Fatalf("ordered failover did not include both transaction prefixes: %+v", result)
						}
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), "PITRCrossSiteOrder") {
					t.Fatalf("unsafe order accepted: %+v %v", result, err)
				}
			}
		})
	}
}

func TestRound1UnanchoredCoverage(t *testing.T) {
	for _, name := range []string{"foreign lineage", "unknown baseline"} {
		t.Run(name, func(t *testing.T) {
			store := round1Archive(t, map[string][][]byte{"iad": {round1Stream(t, 10, round1Time(11, 0, 0), false, name == "foreign lineage")}})
			baseline := retentionTestSID + ":1-10"
			if name == "unknown baseline" {
				baseline = ""
			}
			out := t.TempDir()
			_, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(10, 0, 0), out, baseline, false, false, "timestamp", slog.Default())
			want := "PITRTargetNotCovered"
			if name == "unknown baseline" {
				want = "PITRBackupBoundUnknown"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("unanchored barrier certified: %v", err)
			}
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(10, 0, 0), out, baseline, true, false, "timestamp", slog.Default())
			if err != nil || result.TargetCovered || !result.Partial {
				t.Fatalf("unanchored partial: %+v %v", result, err)
			}
			raw, err := os.ReadFile(filepath.Join(out, "coverage.json"))
			if err != nil {
				t.Fatal(err)
			}
			var saved map[string]any
			if err = json.Unmarshal(raw, &saved); err != nil {
				t.Fatal(err)
			}
			site := saved["sites"].([]any)[0].(map[string]any)
			if anchored, ok := site["anchored"].(bool); !ok || anchored {
				t.Fatalf("unanchored evidence not recorded: %s", raw)
			}
		})
	}
}

func TestRound1OpaquePayloadChronologyRefused(t *testing.T) {
	compressed := pitrFixture(t, "compressed-transaction.bin")
	store := round1Archive(t, map[string][][]byte{"east": {compressed}, "iad": {round1Stream(t, 10, round1Time(11, 0, 0), false, false)}})
	_, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"east", "iad"}, round1Time(11, 0, 1), t.TempDir(), retentionTestSID+":1-10", true, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "PITRCrossSiteOrder") {
		t.Fatalf("outer payload time certified inner event order: %v", err)
	}
	// A single staged site retains the existing native payload replay rules.
	store = round1Archive(t, map[string][][]byte{"iad": {compressed}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 0, 1), t.TempDir(), retentionTestSID+":1-10", false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered {
		t.Fatalf("single-site payload replay changed: %+v %v", result, err)
	}
}

func TestRound1NativeIdleRotationCoverage(t *testing.T) {
	data := pitrFixture(t, "native/flush-empty.bin")
	evidence, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.GTIDs.IsEmpty() || evidence.Metadata.PreviousGTIDs == "" {
		t.Fatal("fixture must model an idle rotation after existing transactions")
	}
	store := round1Archive(t, map[string][][]byte{"iad": {data}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, evidence.NewestEventTime, t.TempDir(), evidence.Metadata.PreviousGTIDs, false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || result.Partial || len(result.Sites[0].Staged) != 0 {
		t.Fatalf("FLUSH BINARY LOGS did not establish idle coverage: %+v %v", result, err)
	}
}

func TestRound1FirstEventAtTarget(t *testing.T) {
	at := round1Time(11, 0, 0)
	store := round1Archive(t, map[string][][]byte{"iad": {round1Stream(t, 10, at, false, false)}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, at, t.TempDir(), retentionTestSID+":1-10", false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || len(result.Sites[0].Staged) != 1 || !result.Sites[0].NewestReplayEventTime.IsZero() {
		t.Fatalf("first-event barrier coverage: %+v %v", result, err)
	}
}
