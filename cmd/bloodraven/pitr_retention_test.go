package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const retentionTestSID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func retentionBoundaryBackup(name, gtids string, start time.Time) *v1alpha1.MysqlBackup {
	s, c := metav1.NewTime(start), metav1.NewTime(start.Add(2*time.Hour))
	return &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{"shipstream.io/failover-group": "fg", "shipstream.io/backup-profile": "p"}}, Spec: v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "fg"}, ProfileName: "p"}, Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, StartTime: &s, CompletionTime: &c, GtidExecuted: gtids}}
}

func TestPITRCutoffRequiresEveryBackupGTIDBoundary(t *testing.T) {
	start := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, secondGTID string
		known            bool
	}{
		{"lagging newer backup", retentionTestSID + ":1-10", true},
		{"missing metadata", "", false},
		{"missing start time", retentionTestSID + ":1-10", false}, {"malformed metadata", "not-a-gtid", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			first := retentionBoundaryBackup("old", retentionTestSID+":1-20", start)
			second := retentionBoundaryBackup("new", tc.secondGTID, start.Add(time.Hour))
			if tc.name == "missing start time" {
				second.Status.StartTime = nil
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, second, retentionRound2Group()).Build()
			mux := newAuxMux(nil, platform.NewHub(slog.Default()), c)
			request := func(query string) map[string]any {
				t.Helper()
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&profile=p"+query, nil))
				if w.Code != http.StatusOK {
					t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
				}
				var body map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				return body
			}
			legacy := request("")
			if legacy["cutoffTime"] != nil {
				t.Errorf("legacy sidecar was authorized for unsafe time-only deletion: %v", legacy)
			}
			proof := request("&coverage=gtid-v1")
			if tc.known {
				if proof["cutoffTime"] != start.Format(time.RFC3339) || proof["coverage"] != "gtid-v1" {
					t.Errorf("snapshot boundary missing: %v", proof)
				}
				raw, ok := proof["retainedGTIDs"].([]any)
				if !ok || len(raw) != 2 {
					t.Fatalf("all retained backup bounds required: %v", proof)
				}
				sets := []string{raw[0].(string), raw[1].(string)}
				slices.Sort(sets)
				want := []string{retentionTestSID + ":1-10", retentionTestSID + ":1-20"}
				if !slices.Equal(sets, want) {
					t.Errorf("backup GTID bounds = %v", sets)
				}
			} else if proof["cutoffTime"] != nil {
				t.Errorf("unknown backup metadata authorized deletion: %v", proof)
			}
			if body := request(""); body["cutoffTime"] != nil {
				t.Errorf("coverage cache leaked a cutoff to legacy caller: %v", body)
			}
		})
	}
}

func TestPITRCutoffDoesNotCachePositiveCoverage(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	first := retentionBoundaryBackup("old", retentionTestSID+":1-20", start)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, retentionRound2Group()).Build()
	mux := newAuxMux(nil, platform.NewHub(slog.Default()), c)
	read := func() map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&profile=p&coverage=gtid-v1", nil))
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	_ = read()
	second := retentionBoundaryBackup("new", retentionTestSID+":1-10", start.Add(time.Hour))
	second.Labels = nil // Safety depends on retained CR specs, including unlabelled CRs.
	if err := c.Create(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if body := read(); body["cutoffTime"] != nil {
		t.Fatalf("rate-limited query reused stale delete authority: %v", body)
	}
	// A fresh admission window must include the newly observed unlabelled CR.
	mux = newAuxMux(nil, platform.NewHub(slog.Default()), c)
	proof := read()
	sets, ok := proof["retainedGTIDs"].([]any)
	if !ok || len(sets) != 2 {
		t.Fatalf("new retained backup was hidden by cached proof or label filter: %v", proof)
	}
}
