package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func TestPITRCoverageRequired(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    bool
		stop    time.Time
		partial bool
		want    string
	}{
		{name: "zero manifests", stop: time.Now(), want: "PITRTargetNotCovered"},
		{name: "tail before target", data: true, stop: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), want: "PITRTargetNotCovered"},
		{name: "partial tail", data: true, stop: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), partial: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := round2Archive(t)
			if tc.data {
				store, _ = round2Archive(t, pitrFixture(t, "incarnation-a.bin"))
			}
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, tc.stop, t.TempDir(), retentionTestSID+":1-10", tc.partial, false, "timestamp", slog.New(slog.NewTextHandler(io.Discard, nil)))
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("want %s, got %v", tc.want, err)
				}
				return
			}
			if err != nil || !result.Partial {
				t.Fatalf("want explicit partial outcome: %+v %v", result, err)
			}
		})
	}
}

func TestPITROmittedArchiveSite(t *testing.T) {
	store, _ := round2Archive(t, pitrFixture(t, "incarnation-a.bin"))
	_, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"renamed"}, time.Now(), t.TempDir(), "", true, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "PITRArchiveSiteOmitted") || !strings.Contains(err.Error(), "manifest-iad.json") {
		t.Fatalf("omitted source accepted: %v", err)
	}
}

func coveragePutSite(t *testing.T, store sidecar.ArchiveStore, site string, data []byte) sidecar.BinlogEvidence {
	t.Helper()
	e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	row := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs}
	key, err := sidecar.ArchiveEntryKey("binlogs", site, row)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(sidecar.Manifest{Site: site, Files: []sidecar.ManifestEntry{row}})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Put(context.Background(), sidecar.ManifestKey("binlogs", site), bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	return e
}

type coverageListErrorStore struct {
	sidecar.ArchiveStore
	opened bool
}

func (s *coverageListErrorStore) List(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("access denied")
}
func (s *coverageListErrorStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.opened = true
	return s.ArchiveStore.Open(ctx, key)
}

func TestPITRCoverageArchiveCases(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, name := range []string{"zero manifests", "zero files", "exact barrier", "native barrier", "forged timestamp", "beyond tail", "partial empty", "renamed source", "omitted removed", "included old", "listing failure", "latest empty", "latest replay", "latest covered by backup", "partial corruption", "partial gap", "partial divergence", "partial quarantine"} {
		t.Run(name, func(t *testing.T) {
			store, _ := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
			sites := []string{"iad"}
			start := retentionTestSID + ":1-10"
			partial := false
			mode := "timestamp"
			want := ""
			out := t.TempDir()
			stop := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
			switch name {
			case "zero manifests":
				want = "PITRTargetNotCovered"
			case "zero files":
				round2Manifest(t, store, sidecar.Manifest{Site: "iad"})
				want = "PITRTargetNotCovered"
			case "partial empty":
				partial = true
			case "latest empty":
				mode = "latest"
				want = "PITRNoArchivedReplay"
			case "listing failure":
				store = &coverageListErrorStore{ArchiveStore: store}
				partial = true
				want = "PITRArchiveListFailed"
			default:
				data := pitrFixture(t, "incarnation-a.bin")
				if name == "native barrier" {
					data = pitrFixture(t, "native/xa.bin")
				}
				e := coveragePutSite(t, store, "iad", data)
				stop = e.NewestEventTime
				start = e.Metadata.PreviousGTIDs
				switch name {
				case "forged timestamp":
					stop = stop.Add(time.Hour)
					round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{{Name: "mysql-bin.000001", ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs, LastEventTime: stop.Add(time.Hour)}}})
					want = "PITRTargetNotCovered"
				case "beyond tail":
					stop = stop.Add(time.Hour)
					want = "PITRTargetNotCovered"
				case "renamed source":
					sites = []string{"iad", "target-renamed"}
				case "omitted removed":
					coveragePutSite(t, store, "removed", data)
					later := coveragePutSite(t, store, "iad", pitrFixture(t, "incarnation-b.bin"))
					stop = later.NewestEventTime
					start = retentionTestSID + ":1-10"
					want = "PITRArchiveSiteOmitted"
					partial = true
				case "included old":
					start = retentionTestSID + ":1-20"
					coveragePutSite(t, store, "old", data)
					sites = append(sites, "old")
					coveragePutSite(t, store, "iad", pitrFixture(t, "incarnation-b.bin"))
					stop = time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)
				case "latest replay":
					mode = "latest"
				case "latest covered by backup":
					mode = "latest"
					start = retentionTestSID + ":1-20"
					want = "PITRNoArchivedReplay"
				case "partial corruption":
					partial = true
					want = "identity mismatch"
					key, _ := sidecar.ArchiveEntryKey("binlogs", "iad", sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: e.Digest})
					other := pitrFixture(t, "incarnation-b.bin")
					if err := store.Put(context.Background(), key, bytes.NewReader(other), int64(len(other))); err != nil {
						t.Fatal(err)
					}
				case "partial gap", "partial divergence", "partial quarantine":
					partial = true
					stop = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
					row := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs}
					m := sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{row}}
					if name == "partial gap" {
						m.Files = append(m.Files, sidecar.ManifestEntry{Name: "mysql-bin.missing"})
						want = "unreadable or missing"
					}
					if name == "partial divergence" {
						m.Divergent = []sidecar.ArchiveDivergence{{Name: row.Name, ID: row.ID, MissingGTIDs: "lost:1"}}
						want = "divergent"
					}
					round2Manifest(t, store, m)
					if name == "partial quarantine" {
						if err := sidecar.QuarantineArchive(context.Background(), store, "binlogs", "iad", row.Name, row.ID, "test", retentionTestSID+":1-20", false); err != nil {
							t.Fatal(err)
						}
						want = "quarantined row"
					}
				}
			}
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", sites, stop, out, start, partial, false, mode, logger)
			if want != "" {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("want %s: result=%+v err=%v", want, result, err)
				}
				if name == "omitted removed" {
					entries, readErr := os.ReadDir(out)
					if readErr != nil || len(entries) != 0 {
						t.Fatalf("binlog download preceded omission preflight: %v %v", entries, readErr)
					}
				}
				if name == "listing failure" && store.(*coverageListErrorStore).opened {
					t.Fatal("download preceded preflight")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "timestamp" && !partial && !result.TargetCovered {
				t.Fatal("missing barrier")
			}
			raw, err := os.ReadFile(filepath.Join(out, "coverage.json"))
			if err != nil {
				t.Fatal(err)
			}
			var saved pitrCoverage
			if err = json.Unmarshal(raw, &saved); err != nil || saved.Version != 1 {
				t.Fatalf("coverage file %s: %v", raw, err)
			}
			if name == "included old" && len(result.Sites[1].Staged) != 0 {
				t.Fatal("lower cut did not skip old site")
			}
		})
	}
}
