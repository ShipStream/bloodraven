package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type retentionReviewReader struct {
	client.Reader
	lists atomic.Int32
}

func (c *retentionReviewReader) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	c.lists.Add(1)
	return c.Reader.List(ctx, obj, opts...)
}
func retentionReviewClient(t *testing.T, backups ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	hasGroup := false
	for _, obj := range backups {
		if g, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && g.Namespace == "ns" && g.Name == "fg" {
			hasGroup = true
		}
	}
	if !hasGroup {
		backups = append(backups, retentionRound2Group())
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlBackup{}).WithObjects(backups...).Build()
}
func retentionReviewRead(t *testing.T, handler http.Handler) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&profile=p&coverage=gtid-v1", nil))
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return w.Code, result
}
func TestReviewPITRCutoffBoundsRepeatedPositiveLookups(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &retentionReviewReader{Reader: retentionReviewClient(t, backup)}
	handler := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	for i := 0; i < 20; i++ {
		status, body := retentionReviewRead(t, handler)
		if status != 200 && status != 429 {
			t.Fatalf("HTTP %d: %v", status, body)
		}
		if status == 429 && body["cutoffTime"] != nil {
			t.Fatal("throttled reply authorizes retirement")
		}
	}
	if got := reader.lists.Load(); got != 1 {
		t.Fatalf("repeated unauthenticated positive requests issued %d LISTs, want 1", got)
	}
}
func TestReviewPITRCutoffIncludesAllGroupProfilesAndInFlightBackups(t *testing.T) {
	start := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	for _, phase := range []v1alpha1.BackupPhase{v1alpha1.BackupPhaseSucceeded, v1alpha1.BackupPhasePending, v1alpha1.BackupPhaseRunning, ""} {
		t.Run(string(phase), func(t *testing.T) {
			first := retentionBoundaryBackup("p", retentionTestSID+":1-20", start)
			other := retentionBoundaryBackup("other", retentionTestSID+":1-10", start.Add(-48*time.Hour))
			other.Spec.ProfileName = "weekly-offsite"
			other.Labels = nil
			other.Status.Phase = phase
			c := retentionReviewClient(t, first, other)
			_, body := retentionReviewRead(t, newAuxMux(nil, platform.NewHub(slog.Default()), c))
			if phase == v1alpha1.BackupPhaseSucceeded {
				sets, ok := body["retainedGTIDs"].([]any)
				if !ok || len(sets) != 2 {
					t.Fatalf("other profile's replay window omitted: %v", body)
				}
			} else if body["cutoffTime"] != nil || body["retentionDeferredReason"] != "backupInFlight" {
				t.Fatalf("in-flight group backup can lose its replay window: %v", body)
			}
		})
	}
}
func TestReviewRunningBackupKeepsReplayWindowThroughCompletion(t *testing.T) {
	start := time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC)
	recent := retentionBoundaryBackup("recent", retentionTestSID+":1-20", start.Add(-48*time.Hour))
	running := retentionBoundaryBackup("overlapping", "", start.Add(-96*time.Hour))
	running.Status.Phase = v1alpha1.BackupPhaseRunning
	running.Status.CompletionTime = nil
	c := retentionReviewClient(t, recent, running)
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join("../../internal/sidecar/testdata", "incarnation-a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(payload)
	id := hex.EncodeToString(hash[:])
	name := "mysql-bin.000001"
	key := "binlogs/iad/" + name + "." + id
	if err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	row := sidecar.ManifestEntry{Name: name, ID: id, LastEventTime: start.Add(-72 * time.Hour), GTIDCoverage: &sidecar.BinlogGTIDCoverage{Complete: true, GTIDs: retentionTestSID + ":11"}}
	data, _ := json.Marshal(sidecar.Manifest{Version: 1, Site: "iad", Files: []sidecar.ManifestEntry{row}})
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	sweep := func() {
		// A new handler models expiry of safe deferrals between hourly sweeps.
		server := httptest.NewServer(newAuxMux(nil, platform.NewHub(slog.Default()), c))
		defer server.Close()
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		mysql, err := sidecar.NewLiveMysqlFromDSN("unused@tcp(127.0.0.1:1)/?timeout=1ms")
		if err != nil {
			t.Fatal(err)
		}
		defer mysql.Close()
		observed := &retentionMaintenanceStore{ArchiveStore: store, cancel: cancel}
		a := sidecar.NewBinlogArchiver(&sidecar.PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), PollInterval: time.Hour}, "iad", retentionReplica{mysql}, observed, slog.New(slog.NewTextHandler(io.Discard, nil)))
		a.SetRetentionClient(strings.TrimPrefix(server.URL, "http://"), "ns", "fg", "p", time.Hour)
		done := make(chan struct{})
		go func() { defer close(done); a.Run(runCtx) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cancel()
			<-done
			t.Fatal("ordinary archiver maintenance did not complete")
		}
		if observed.reads < 3 {
			t.Fatal("retention was not reached through Run")
		}
		if _, exists, err := store.Get(ctx, key); err != nil || !exists {
			t.Fatalf("transaction 11 needed by overlapping backup was retired: exists=%t, %v", exists, err)
		}
	}
	sweep()
	running.Status.Phase = v1alpha1.BackupPhaseSucceeded
	running.Status.GtidExecuted = retentionTestSID + ":1-10"
	if err := c.Status().Update(ctx, running); err != nil {
		t.Fatal(err)
	}
	sweep()
}

// Embed the real adapter for the package-private connectability interface;
// read-only maintenance calls only these overrides and never dials MySQL.
type retentionReplica struct{ *sidecar.LiveMysql }

func (retentionReplica) IsReadOnly(context.Context) (bool, error) { return true, nil }
func (retentionReplica) BinlogState(context.Context) (string, string, error) {
	return retentionTestSID, retentionTestSID + ":1-100", nil
}

type retentionMaintenanceStore struct {
	sidecar.ArchiveStore
	cancel context.CancelFunc
	reads  int
}

func (s *retentionMaintenanceStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	data, exists, err := s.ArchiveStore.Get(ctx, key)
	if key == sidecar.ManifestKey("binlogs", "iad") {
		s.reads++
		// audit, retention, then the aggregate refresh after the sweep has finished.
		if s.reads == 3 {
			s.cancel()
		}
	}
	return data, exists, err
}

func (retentionReplica) BinlogHistoryReady(context.Context) (bool, error) { return true, nil }
