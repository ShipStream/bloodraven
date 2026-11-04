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
	for _, shape := range []string{"idle sealed file", "backed-up transaction then idle rotation", "backed-up transaction then idle file", "replica-era transaction then idle rotation"} {
		t.Run(shape, func(t *testing.T) {
			idle := round1Stream(t, 11, round1Time(9, 0, 0), true, false)
			idle = round1PatchTimestamp(t, idle, 3, uint32(round1Time(9, 31, 0).Unix()))
			old := round1PatchTimestamp(t, pitrFixture(t, "incarnation-a.bin"), 3, uint32(round1Time(9, 31, 0).Unix()))
			files := [][]byte{idle}
			switch shape {
			case "backed-up transaction then idle rotation":
				files = [][]byte{old, idle}
			case "backed-up transaction then idle file":
				files = [][]byte{pitrFixture(t, "incarnation-a.bin"), idle}
			case "replica-era transaction then idle rotation":
				// The sealed file's transaction came from another server, so
				// the local rotation cannot show that replication reached it.
				events := round4Events(old)
				for _, event := range events {
					if event[4] == 33 || event[4] == 2 {
						binary.LittleEndian.PutUint32(event[5:9], 2)
					}
				}
				files = [][]byte{round4Reframe(events)}
			}
			// A transaction-free chain cannot show which server wrote it,
			// so its server-local stop event proves nothing on its own.
			covered := shape == "backed-up transaction then idle rotation" || shape == "backed-up transaction then idle file"
			store := round1Archive(t, map[string][][]byte{"iad": files})
			for _, partial := range []bool{false, true} {
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-11", partial, false, "timestamp", slog.Default())
				if covered && (err != nil || !result.TargetCovered || result.Partial || len(result.Sites[0].Staged) != 0) {
					t.Fatalf("idle window lost verified coverage: %+v %v", result, err)
				}
				if !covered && (result.TargetCovered || (partial && (err != nil || !result.Partial)) || (!partial && (err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered")))) {
					t.Fatalf("unproven server-local stop certified: %+v %v", result, err)
				}
				if !covered && !strings.Contains(result.Sites[0].AnchorReason, "stop reached only on a server-local event") {
					t.Fatalf("unproven stop lacks its reason: %+v", result.Sites[0])
				}
			}
		})
	}
}

// Under the single-site rule, two sites holding transactions to replay always
// refuse, whatever their event order. A site that replays nothing before the
// target is left out, so it neither refuses nor ends replay early.
func TestRound1TwoReplayingSitesAlwaysRefuse(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, name := range []string{"ordered failover", "reverse failover", "failback", "overlap", "equal boundary", "missing timestamp", "foreign barrier", "nontransaction barrier"} {
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
			case "missing timestamp":
				streams["iad"] = [][]byte{round1Stream(t, 10, round1Time(11, 0, 0), false, false)}
				streams["east"] = [][]byte{round1PatchTimestamp(t, early, 15, 0)}
			case "foreign barrier":
				streams["iad"] = [][]byte{early}
				streams["east"] = [][]byte{round1Stream(t, 10, round1Time(12, 0, 0), false, true)}
			case "nontransaction barrier":
				streams["iad"] = [][]byte{round1Stream(t, 10, round1Time(11, 0, 0), false, false)}
				streams["east"] = [][]byte{round1Stream(t, 10, round1Time(12, 0, 0), true, true)}
			}
			store := round1Archive(t, streams)
			for _, partial := range []bool{false, true} {
				for _, unverified := range []bool{false, true} {
					result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "east"}, round1Time(11, 0, 2), t.TempDir(), retentionTestSID+":1-10", partial, unverified, "timestamp", logger)
					replaying := 0
					for _, site := range result.Sites {
						if site.HasReplayTransactions {
							replaying++
						} else if len(site.Staged) > 0 && err == nil {
							t.Fatalf("site without replay kept in the replay: %+v", result.Sites)
						}
					}
					switch name {
					case "foreign barrier":
						// Only iad replays; it never reaches the target.
						if result.TargetCovered || (partial != (err == nil)) {
							t.Fatalf("foreign site certified iad's uncovered tail: %+v %v", result, err)
						}
						continue
					case "nontransaction barrier":
						// Only iad replays, and its own stop covers the target.
						if err != nil || !result.TargetCovered || result.CrossSiteUnverified || result.ProofSite != "iad" {
							t.Fatalf("single replaying site refused: %+v %v", result, err)
						}
						continue
					}
					if replaying < 2 {
						t.Fatalf("fixture needs two replaying sites: %+v", result.Sites)
					}
					if unverified && err == nil {
						if !result.CrossSiteUnverified {
							t.Fatalf("cross-site replay not labelled unverified: %+v", result)
						}
						continue
					}
					if err == nil || !(strings.Contains(err.Error(), "PITRCrossSiteOrder") || strings.Contains(err.Error(), "PITRTargetNotCovered")) || result.TargetCovered && !unverified {
						t.Fatalf("cross-site window certified: %+v %v", result, err)
					}
					if !unverified && !partial && strings.Contains(err.Error(), "PITRCrossSiteOrder") && !strings.Contains(err.Error(), "allowUnverifiedCrossSite") {
						t.Fatalf("refusal lost its remedy: %v", err)
					}
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

func TestRound1CompressedPayloadTimesAreDiagnostic(t *testing.T) {
	compressed := pitrFixture(t, "compressed-transaction.bin")
	// Decoded inner headers keep event-time diagnostics complete; a
	// compressed transaction alone no longer decides multi-site policy.
	evidence, err := sidecar.InspectBinlog(bytes.NewReader(compressed), time.Time{})
	if err != nil || !evidence.TimestampsComplete {
		t.Fatalf("decoded payload marked timestamps incomplete: %+v %v", evidence, err)
	}
	store := round1Archive(t, map[string][][]byte{"east": {compressed}, "iad": {round1Stream(t, 10, round1Time(9, 0, 0), false, false)}})
	_, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"east", "iad"}, round1Time(11, 0, 1), t.TempDir(), retentionTestSID+":1-10", true, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "PITRCrossSiteOrder") {
		t.Fatalf("two replaying sites certified: %v", err)
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
	// A transaction-free file cannot show which server wrote the history, so
	// its server-local rotation alone does not prove the stop.
	store := round1Archive(t, map[string][][]byte{"iad": {data}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, evidence.NewestEventTime, t.TempDir(), evidence.Metadata.PreviousGTIDs, false, false, "timestamp", slog.Default())
	if err == nil || result.TargetCovered || !strings.Contains(result.Sites[0].AnchorReason, "stop reached only on a server-local event") {
		t.Fatalf("transaction-free rotation certified: %+v %v", result, err)
	}
	// With the writer's file archived, its commits share the rotation's
	// second: they are after a target at that second, yet in the bound.
	store = round1Archive(t, map[string][][]byte{"iad": {pitrFixture(t, "native/initial.bin"), data}})
	_, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, evidence.NewestEventTime, t.TempDir(), evidence.Metadata.PreviousGTIDs, false, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "the backup already contains transactions committed at or after the target") {
		t.Fatalf("post-target bound certified: %v", err)
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
