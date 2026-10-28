package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
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
