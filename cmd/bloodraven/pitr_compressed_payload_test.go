package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

func inspectBinlogFixture(t *testing.T, raw []byte) sidecar.BinlogEvidence {
	t.Helper()
	evidence, err := sidecar.InspectBinlog(bytes.NewReader(raw), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}
func idleRotationAfter(t *testing.T, raw []byte, at time.Time) []byte {
	t.Helper()
	evidence := inspectBinlogFixture(t, raw)
	previous, err := internalmysql.ParseGTIDSet(evidence.Metadata.PreviousGTIDs)
	if err != nil {
		t.Fatal(err)
	}
	executed := sidecar.UnionBinlogGTIDs(previous, evidence.GTIDs)
	if len(executed) != 1 {
		t.Fatal("idle fixture expects one source UUID")
	}
	body := binary.LittleEndian.AppendUint64(nil, 1)
	for sid, intervals := range executed {
		uuid, err := hex.DecodeString(strings.ReplaceAll(sid, "-", ""))
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, uuid...)
		body = binary.LittleEndian.AppendUint64(body, uint64(len(intervals)))
		for _, interval := range intervals {
			body = binary.LittleEndian.AppendUint64(body, uint64(interval.Start))
			body = binary.LittleEndian.AppendUint64(body, uint64(interval.End)+1)
		}
	}
	events := splitBinlogEvents(coverageStream(t, 0, at, true, false))
	// A real rotation keeps the writer's server_id.
	server := splitBinlogEvents(raw)[0][5:9]
	for i, event := range events {
		if event[4] == 35 {
			event = append(bytes.Clone(event[:19]), body...)
			events[i] = append(event, make([]byte, 4)...)
		}
		copy(events[i][5:9], server)
	}
	return reframeBinlogEvents(events)
}
func requirePayloadTargetCovered(t *testing.T, files [][]byte, bound string, stop time.Time) {
	t.Helper()
	store := coverageArchive(t, map[string][][]byte{"iad": files})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, stop, t.TempDir(), bound, partial, false, "timestamp", slog.Default())
		if err != nil || !result.TargetCovered || result.Partial {
			t.Fatalf("verified available history refused: %+v %v", result, err)
		}
	}
}
func TestPITRRealCompressedWindows(t *testing.T) {
	raw := pitrFixture(t, "native/compressed-session.bin")
	evidence := inspectBinlogFixture(t, raw)
	t.Run("whole predecessor", func(t *testing.T) {
		stop := evidence.NewestEventTime.Add(time.Second)
		requirePayloadTargetCovered(t, [][]byte{raw, pitrFixture(t, "native/compressed-session-idle.bin")}, evidence.Metadata.PreviousGTIDs, stop)
	})
	t.Run("stop prefix and containment", func(t *testing.T) {
		stop := evidence.LastTransactionEventTime.Add(time.Second)
		if !stop.Before(evidence.NewestEventTime) {
			t.Fatal("capture needs a later sealed rotation")
		}
		store := coverageArchive(t, map[string][][]byte{"iad": {raw}, "pdx": {raw}})
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, stop, t.TempDir(), evidence.Metadata.PreviousGTIDs, false, true, "timestamp", slog.Default())
		if err != nil || !result.TargetCovered || !result.CrossSiteUnverified {
			t.Fatalf("real compressed prefix was not credited: %+v %v", result, err)
		}
	})
}
func TestPITRXAAndChecksumOffPredecessors(t *testing.T) {
	for _, shape := range []string{"XA", "checksum off"} {
		t.Run(shape, func(t *testing.T) {
			files := [][]byte{pitrFixture(t, "native/checksum-off.bin")}
			if shape == "XA" {
				files = [][]byte{pitrFixture(t, "native/xa-prepare.bin"), pitrFixture(t, "native/xa.bin")}
			}
			first := inspectBinlogFixture(t, files[0])
			last := inspectBinlogFixture(t, files[len(files)-1])
			if first.Metadata.GTIDCoverage.Complete {
				t.Fatal("regression needs non-retention-grade predecessor evidence")
			}
			stop := last.NewestEventTime.Add(5 * time.Minute)
			files = append(files, idleRotationAfter(t, files[len(files)-1], stop.Add(5*time.Minute)))
			requirePayloadTargetCovered(t, files, first.Metadata.PreviousGTIDs, stop)
		})
	}
}
func requireCompressedInnerCutRefused(t *testing.T, streams map[string][][]byte, bound, gtid string) {
	t.Helper()
	store := coverageArchive(t, streams)
	sites := []string{"iad"}
	if _, ok := streams["pdx"]; ok {
		sites = append(sites, "pdx")
	}
	for _, partial := range []bool{false, true} {
		for _, cross := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", sites, coverageTime(9, 30, 0), t.TempDir(), bound, partial, cross, "timestamp", slog.Default())
			want := "compressed transaction " + gtid + " has events at or after the target; mysqlbinlog would replay it partially"
			if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || !strings.Contains(err.Error(), want) {
				t.Fatalf("inner-cut payload not refused: %+v %v", result, err)
			}
			found := false
			for _, site := range result.Sites {
				found = found || strings.Contains(site.AnchorReason, want)
			}
			if !found {
				t.Fatalf("inner-cut transaction absent from anchorReason: %+v", result)
			}
		}
	}
}
func TestPITRNativeCompressedInnerCut(t *testing.T) {
	raw := pitrFixture(t, "native/compressed-inner-cut.bin")
	// This is the untouched 507-byte MySQL 8.4.11 capture retained by GPT review 5.
	if len(raw) != 507 {
		t.Fatal("native capture changed")
	}
	before := sha256.Sum256(raw)
	if hex.EncodeToString(before[:]) != "c13ba1b4fbe14e478eacc0e94e5428306022e8384bdf6ef290a63258d1546fda" {
		t.Fatal("native review fixture bytes changed")
	}
	idle := idleRotationAfter(t, raw, coverageTime(9, 35, 0))
	sid := "b5e6ac37-d371-11f1-8b67-65731305df52"
	requireCompressedInnerCutRefused(t, map[string][][]byte{"iad": {raw, idle}}, sid+":1-8", sid+":9")
	if after := sha256.Sum256(raw); after != before {
		t.Fatal("test mutated native capture")
	}
}
func TestPITRCompressedInnerXIDAtTarget(t *testing.T) {
	original := moveBinlogClock(pitrFixture(t, "native/initial.bin"), coverageTime(9, 0, 0))
	evidence := inspectBinlogFixture(t, original)
	var sid string
	for source := range evidence.GTIDs {
		sid = source
	}
	interrupted := patchEventTimestamp(t, original, 16, uint32(coverageTime(9, 30, 0).Unix()))
	compressed := compressNativeTransaction(t, interrupted)
	t.Run("predecessor", func(t *testing.T) {
		requireCompressedInnerCutRefused(t, map[string][][]byte{"iad": {compressed, idleRotationAfter(t, compressed, coverageTime(9, 35, 0))}}, sid+":1", sid+":3")
	})
	t.Run("stop prefix", func(t *testing.T) {
		barrier := patchEventTimestamp(t, compressed, 4, uint32(coverageTime(9, 35, 0).Unix()))
		requireCompressedInnerCutRefused(t, map[string][][]byte{"iad": {barrier}, "pdx": {original}}, sid+":1", sid+":3")
	})
}

func TestPITRDumpCoveredPayloadDoesNotReplay(t *testing.T) {
	// The compressed transaction is in a later site, outside the actual input prefix.
	raw := pitrFixture(t, "native/compressed-inner-cut.bin")
	evidence := inspectBinlogFixture(t, raw)
	first := idleRotationAfter(t, raw, coverageTime(9, 35, 0))
	baseline, err := internalmysql.ParseGTIDSet(evidence.Metadata.PreviousGTIDs)
	if err != nil {
		t.Fatal(err)
	}
	bound := sidecar.UnionBinlogGTIDs(baseline, evidence.GTIDs).String()
	// All its GTIDs are in the dump; neither site supplies staged replay files.
	// iad's own copy shows which server wrote the idle rotation.
	store := coverageArchive(t, map[string][][]byte{"iad": {raw, first}, "pdx": {raw}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), bound, false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered {
		t.Fatalf("dump-covered payload incorrectly treated as replayed: %+v %v", result, err)
	}
}

func TestPITRUnverifiablePrefixDiagnostic(t *testing.T) {
	raw := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	events := splitBinlogEvents(raw)
	for i, event := range events {
		if event[4] == 2 {
			events[i] = setQueryEventSQL(event, "XA PREPARE 'x'")
		}
	}
	opaque := patchEventTimestamp(t, reframeBinlogEvents(events), 3, uint32(coverageTime(9, 35, 0).Unix()))
	// iad ends replay inside an unverifiable S:11 prefix; pdx's history
	// needs S:11, which exists only in that unverifiable form.
	later := coverageStream(t, 11, coverageTime(9, 10, 0), false, false)
	store := coverageArchive(t, map[string][][]byte{"iad": {opaque}, "pdx": {later}})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, true, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), pitrUnverifiableAnchorReason) || result.Sites[1].AnchorReason != pitrUnverifiableAnchorReason || strings.Contains(err.Error(), "archive does not contain transactions") {
			t.Fatalf("unverifiable evidence diagnosed as missing bytes: %+v %v", result, err)
		}
	}
}

func TestPITRUndecodablePayloadRefuses(t *testing.T) {
	for _, shape := range []string{"unknown header", "invalid ZSTD"} {
		t.Run(shape, func(t *testing.T) {
			good := pitrFixture(t, "compressed-transaction.bin")
			evidence := inspectBinlogFixture(t, good)
			events := splitBinlogEvents(good)
			for _, event := range events {
				if event[4] == 40 {
					if shape == "unknown header" {
						event[19] = 99
					} else {
						// Known fixture header: small TLV keys/lengths, followed by ZSTD bytes.
						offset := 19
						for event[offset] != 0 {
							offset += 2 + int(event[offset+1])
						}
						event[offset+1] ^= 0xff
					}
				}
			}
			bad := reframeBinlogEvents(events)
			store := coverageArchive(t, map[string][][]byte{"iad": {good}})
			digest := sha256.Sum256(bad)
			row := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: hex.EncodeToString(digest[:]), PreviousGTIDs: evidence.Metadata.PreviousGTIDs}
			key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.Put(context.Background(), key, bytes.NewReader(bad), int64(len(bad))); err != nil {
				t.Fatal(err)
			}
			saveArchiveManifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{row}})
			for _, partial := range []bool{false, true} {
				for _, cross := range []bool{false, true} {
					result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, cross, "timestamp", slog.Default())
					if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || !strings.Contains(err.Error(), "cannot be decoded before the target") || len(result.Sites) != 1 || !strings.Contains(result.Sites[0].AnchorReason, "compressed transaction "+retentionTestSID+":11") {
						t.Fatalf("undecodable payload not refused explicitly: %+v %v", result, err)
					}
				}
			}
		})
	}
}

func TestPITRUnreadLaterPayloadDoesNotSupplyProof(t *testing.T) {
	original := moveBinlogClock(pitrFixture(t, "native/initial.bin"), coverageTime(9, 0, 0))
	first := patchEventTimestamp(t, original, 4, uint32(coverageTime(9, 35, 0).Unix()))
	interrupted := patchEventTimestamp(t, original, 16, uint32(coverageTime(9, 30, 0).Unix()))
	later := compressNativeTransaction(t, interrupted)
	evidence := inspectBinlogFixture(t, original)
	var sid string
	for key := range evidence.GTIDs {
		sid = key
	}
	store := coverageArchive(t, map[string][][]byte{"iad": {first}, "pdx": {later}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), sid+":1", false, true, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || !result.CrossSiteUnverified {
		t.Fatalf("a payload after the global stop was treated as replayed: %+v %v", result, err)
	}
}

func TestPITRNativeMysqlbinlogPayloadCuts(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("opt-in official MySQL 8.4 payload evidence")
	}
	run := func(raw []byte, stop string) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "input.bin"), raw, 0644); err != nil {
			t.Fatal(err)
		}
		args := []string{"run", "--rm", "--network", "none", "-e", "TZ=UTC", "-v", dir + ":/fixtures:ro", "bloodraven-pitr-mysqlbinlog:8.4", "--no-defaults", "--verify-binlog-checksum"}
		if stop != "" {
			args = append(args, "--stop-datetime="+stop)
		}
		args = append(args, "/fixtures/input.bin")
		output, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("official mysqlbinlog: %v %s", err, output)
		}
		return string(output)
	}
	t.Run("native body cut", func(t *testing.T) {
		raw := pitrFixture(t, "native/compressed-inner-cut.bin")
		full, cut := run(raw, ""), run(raw, "2026-04-15 09:30:00")
		if !strings.Contains(full, "Write_rows") || strings.Contains(cut, "Write_rows") || !strings.Contains(cut, "COMMIT") {
			t.Fatal("official client no longer reproduces the native partial-body output")
		}
	})
	t.Run("inner commit cut", func(t *testing.T) {
		raw := moveBinlogClock(pitrFixture(t, "native/initial.bin"), coverageTime(9, 0, 0))
		raw = patchEventTimestamp(t, raw, 16, uint32(coverageTime(9, 30, 0).Unix()))
		raw = compressNativeTransaction(t, raw)
		cut := run(raw, "2026-04-15 09:30:00")
		if !strings.Contains(cut, "Write_rows") || !strings.Contains(cut, "ROLLBACK /* added by mysqlbinlog */") {
			t.Fatal("official client no longer reproduces the interrupted inner-XID rollback")
		}
	})
	t.Run("genuine fully before target", func(t *testing.T) {
		raw := pitrFixture(t, "native/compressed-session.bin")
		evidence := inspectBinlogFixture(t, raw)
		full, cut := run(raw, ""), run(raw, evidence.LastTransactionEventTime.Add(time.Second).UTC().Format("2006-01-02 15:04:05"))
		if strings.Count(full, "Write_rows") < 3 || strings.Count(full, "Write_rows") != strings.Count(cut, "Write_rows") || strings.Count(full, "COMMIT") != strings.Count(cut, "COMMIT") {
			t.Fatal("fully-before-target compressed capture did not emit all bodies and commits")
		}
	})
}
