package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

type nativePITRCase struct {
	Rollback bool   `json:"rollback,omitempty"`
	Stop     string `json:"stop"`
	Exclude  string `json:"exclude,omitempty"`
	Prefix   int    `json:"prefix"`
	SHA256   string `json:"sha256"`
}

// Move one captured file's outer event clock forward, preserving event bodies
// and CRCs. Its successors now model a backward clock without changing GTIDs.
func nativeCasePayloads(t *testing.T, rows []sidecar.ManifestEntry, payloads [][]byte, rollback bool) ([]sidecar.ManifestEntry, [][]byte) {
	t.Helper()
	rows = append([]sidecar.ManifestEntry(nil), rows...)
	payloads = append([][]byte(nil), payloads...)
	if !rollback {
		return rows, payloads
	}
	data := bytes.Clone(payloads[1])
	for offset := 4; offset < len(data); {
		size := int(binary.LittleEndian.Uint32(data[offset+9 : offset+13]))
		stamp := binary.LittleEndian.Uint32(data[offset : offset+4])
		if stamp > 0 {
			binary.LittleEndian.PutUint32(data[offset:offset+4], stamp+60)
		}
		binary.LittleEndian.PutUint32(data[offset+size-4:offset+size], crc32.ChecksumIEEE(data[offset:offset+size-4]))
		offset += size
	}
	e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	rows[1].ID, rows[1].FirstEventTime = e.Digest, e.Metadata.FirstEventTime
	payloads[1] = data
	return rows, payloads
}

func nativePITRRows(t *testing.T) ([]sidecar.ManifestEntry, [][]byte, []string) {
	t.Helper()
	catalog, err := os.ReadFile("../../internal/sidecar/testdata/native/catalog.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var rows []sidecar.ManifestEntry
	var data [][]byte
	var labels []string
	for _, line := range strings.Split(strings.TrimSpace(string(catalog)), "\n") {
		fields := strings.Split(line, "\t")
		payload, err := os.ReadFile("../../internal/sidecar/testdata/native/" + fields[0] + ".bin")
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := sidecar.InspectBinlog(bytes.NewReader(payload), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, sidecar.ManifestEntry{Name: fields[1], ID: evidence.Digest, Size: evidence.Metadata.Size, PreviousGTIDs: evidence.Metadata.PreviousGTIDs, BaseGTIDs: func() string {
			if len(fields) > 2 {
				return strings.ReplaceAll(fields[2], "\\n", ",")
			}
			return ""
		}(), FirstEventTime: evidence.Metadata.FirstEventTime})
		data = append(data, payload)
		labels = append(labels, fields[0])
	}
	return rows, data, labels
}
func TestRound2CommittedNativeWindows(t *testing.T) {
	encoded, err := os.ReadFile("../../internal/sidecar/testdata/native/windows.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []nativePITRCase
	if err := json.Unmarshal(encoded, &cases); err != nil {
		t.Fatal(err)
	}
	rows, payloads, _ := nativePITRRows(t)
	for caseIndex, tc := range cases {
		full, err := os.ReadFile(fmt.Sprintf("../../internal/sidecar/testdata/native/window-%02d-full.sql", caseIndex))
		if err != nil {
			t.Fatal(err)
		}
		selected, err := os.ReadFile(fmt.Sprintf("../../internal/sidecar/testdata/native/window-%02d-selected.sql", caseIndex))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(full)
		if !bytes.Equal(full, selected) || hex.EncodeToString(sum[:]) != tc.SHA256 {
			t.Fatal("committed native oracle differs")
		}
		t.Run(tc.Stop+"/"+tc.Exclude, func(t *testing.T) {
			rows, payloads := nativeCasePayloads(t, rows, payloads, tc.Rollback)
			ctx := context.Background()
			store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
			if err != nil {
				t.Fatal(err)
			}
			manifest := sidecar.Manifest{Site: "iad"}
			for i := len(rows) - 1; i >= 0; i-- {
				key, err := sidecar.ArchiveEntryKey("binlogs", "iad", rows[i])
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Put(ctx, key, bytes.NewReader(payloads[i]), int64(len(payloads[i]))); err != nil {
					t.Fatal(err)
				}
				manifest.Files = append(manifest.Files, rows[i])
			}
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(encoded), int64(len(encoded))); err != nil {
				t.Fatal(err)
			}
			stop, err := time.Parse(time.RFC3339, tc.Stop)
			if err != nil {
				t.Fatal(err)
			}
			out := t.TempDir()
			n, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", stop, out, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			if n != tc.Prefix {
				t.Fatalf("staged %d files, native oracle requires prefix %d", n, tc.Prefix)
			}
			files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
			if err != nil {
				t.Fatal(err)
			}
			for i, file := range files {
				got, err := os.ReadFile(file)
				if err != nil || !bytes.Equal(got, payloads[i]) {
					t.Fatalf("native causal order differs at %d: %v", i, err)
				}
			}
		})
	}
}

// Opt-in: docker build -t bloodraven-pitr-mysqlbinlog:8.4 internal/sidecar/testdata/native
// BLOODRAVEN_PITR_NATIVE_TEST=1 go test ./cmd/bloodraven -run TestRound2NativeMysqlbinlog
func TestRound2NativeMysqlbinlog(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("opt-in MySQL 8.4 native oracle")
	}
	rows, payloads, labels := nativePITRRows(t)
	root := t.TempDir()
	for i, label := range labels {
		if err := os.WriteFile(filepath.Join(root, label+".bin"), payloads[i], 0644); err != nil {
			t.Fatal(err)
		}
	}
	run := func(stop, exclude string, count int) []byte {
		t.Helper()
		args := []string{"run", "--rm", "--network", "none", "-e", "TZ=UTC", "-v", root + ":/fixtures:ro", "bloodraven-pitr-mysqlbinlog:8.4", "--no-defaults", "--verify-binlog-checksum", "--stop-datetime=" + stop}
		if exclude != "" {
			args = append(args, "--exclude-gtids="+exclude)
		}
		for _, label := range labels[:count] {
			args = append(args, "/fixtures/"+label+".bin")
		}
		cmd := exec.Command("docker", args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("native mysqlbinlog: %v: %s", err, stderr.Bytes())
		}
		return out
	}
	var cases []nativePITRCase
	path := "../../internal/sidecar/testdata/native/windows.json"
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_UPDATE") == "1" {
		times := []time.Time{time.Unix(int64(binary.LittleEndian.Uint32(payloads[0][4:8])), 0).UTC()}
		for _, index := range []int{1, 3, len(payloads) - 1} {
			e, err := sidecar.InspectBinlog(bytes.NewReader(payloads[index]), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			times = append(times, e.Metadata.LastEventTime.Add(time.Second))
		}
		initial, err := sidecar.InspectBinlog(bytes.NewReader(payloads[1]), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		for _, stop := range times {
			for _, exclude := range []string{"", initial.GTIDs.String()} {
				cases = append(cases, nativePITRCase{Stop: stop.Format(time.RFC3339), Exclude: exclude})
			}
		}
		for _, exclude := range []string{"", initial.GTIDs.String()} {
			stamp := binary.LittleEndian.Uint32(payloads[1][4:8])
			cases = append(cases, nativePITRCase{Stop: time.Unix(int64(stamp)+30, 0).UTC().Format(time.RFC3339), Exclude: exclude, Rollback: true})
		}
	} else {
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encoded, &cases); err != nil {
			t.Fatal(err)
		}
	}
	for i := range cases {
		tc := &cases[i]
		_, payloads := nativeCasePayloads(t, rows, payloads, tc.Rollback)
		for index, label := range labels {
			if err := os.WriteFile(filepath.Join(root, label+".bin"), payloads[index], 0644); err != nil {
				t.Fatal(err)
			}
		}
		ts, err := time.Parse(time.RFC3339, tc.Stop)
		if err != nil {
			t.Fatal(err)
		}
		stop := ts.Format("2006-01-02 15:04:05")
		full := run(stop, tc.Exclude, len(labels))
		prefix := len(payloads)
		reached := false
		for fileIndex, data := range payloads {
			for offset := 4; offset+19 <= len(data); {
				if int64(binary.LittleEndian.Uint32(data[offset:offset+4])) >= ts.Unix() {
					prefix = fileIndex + 1
					reached = true
					break
				}
				size := int(binary.LittleEndian.Uint32(data[offset+9 : offset+13]))
				if size < 19 || offset+size > len(data) {
					t.Fatal("invalid captured event frame")
				}
				offset += size
			}
			if reached {
				break
			}
		}
		selected := run(stop, tc.Exclude, prefix)
		if !bytes.Equal(full, selected) {
			t.Fatalf("native suffix omission changed output for %s", tc.Stop)
		}
		if os.Getenv("BLOODRAVEN_PITR_NATIVE_UPDATE") == "1" {
			for label, data := range map[string][]byte{"full": full, "selected": selected} {
				filename := fmt.Sprintf("../../internal/sidecar/testdata/native/window-%02d-%s.sql", i, label)
				if err := os.WriteFile(filename, data, 0644); err != nil {
					t.Fatal(err)
				}
			}
		}

		sum := sha256.Sum256(full)
		digest := hex.EncodeToString(sum[:])
		if os.Getenv("BLOODRAVEN_PITR_NATIVE_UPDATE") == "1" {
			tc.Prefix = prefix
			tc.SHA256 = digest
		} else if tc.Prefix != prefix || tc.SHA256 != digest {
			t.Fatalf("native prefix/golden drift at %s: prefix=%d hash=%s", tc.Stop, prefix, digest)
		}
	}
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_UPDATE") == "1" {
		encoded, err := json.MarshalIndent(cases, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(encoded, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Fprintf(os.Stderr, "validated %d native stop/filter windows\n", len(cases))
}
