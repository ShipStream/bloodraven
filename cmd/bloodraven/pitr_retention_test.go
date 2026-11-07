package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const retentionTestSID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func retentionBoundaryBackup(name, gtids string, start time.Time) *v1alpha1.MysqlBackup {
	s, c := metav1.NewTime(start), metav1.NewTime(start.Add(2*time.Hour))
	return &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{"bloodraven.shipstream.io/failover-group": "fg", "bloodraven.shipstream.io/backup-profile": "p"}}, Spec: v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "fg"}, ProfileName: "p"}, Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, StartTime: &s, CompletionTime: &c, GtidExecuted: gtids}}
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
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, second, cutoffTestGroup()).Build()
			mux := newAuxMux(nil, platform.NewHub(slog.Default()), c)
			request := func(query string) map[string]any {
				t.Helper()
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&archive="+cutoffTestArchive+query, nil))
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
			if legacy["archive"] != cutoffTestArchive || legacy["profile"] != nil {
				t.Errorf("legacy reply does not echo the archive: %v", legacy)
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
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, cutoffTestGroup()).Build()
	mux := newAuxMux(nil, platform.NewHub(slog.Default()), c)
	read := func() map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&archive="+cutoffTestArchive+"&coverage=gtid-v1", nil))
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

// TestPITRCutoffKeepsHeldTerminatingBackup pins #211: a backup deleted
// while a verification reads it keeps its artifact until the run stops, so
// it must keep bounding binlog retention for that long too, including for a
// run started by an operator that did not stamp holds.
func TestPITRCutoffKeepsHeldTerminatingBackup(t *testing.T) {
	start := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	reading := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "v", Namespace: "ns", UID: "uid-v"},
		Spec:       v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "fg"}, ProfileName: "p"},
		Status: v1alpha1.MysqlBackupVerificationStatus{Phase: v1alpha1.VerificationPhaseRestoring,
			BackupRef: &v1alpha1.VerificationBackupRef{Name: "deleting", UID: "uid-deleting", ArtifactStorage: &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStorageS3, Location: "x/deleting"}}},
	}
	for _, tc := range []struct {
		name      string
		held      bool
		reader    bool
		listError bool
		cutoff    time.Time
		wantErr   bool
	}{
		{name: "stamped hold", held: true, cutoff: start},
		{name: "unstamped, verification still reading", reader: true, cutoff: start},
		{name: "not read", cutoff: start.Add(time.Hour)},
		{name: "reader lookup fails closed", listError: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			deleting := retentionBoundaryBackup("deleting", retentionTestSID+":1-10", start)
			deleting.UID = "uid-deleting"
			now := metav1.Now()
			deleting.DeletionTimestamp = &now
			deleting.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
			if tc.held {
				deleting.Annotations = map[string]string{v1alpha1.VerificationHoldAnnotationPrefix + "uid-v": "v"}
			}
			kept := retentionBoundaryBackup("kept", retentionTestSID+":1-20", start.Add(time.Hour))
			objs := []client.Object{deleting, kept}
			if tc.reader {
				objs = append(objs, reading)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			var holdReader client.Reader = c
			if tc.listError {
				holdReader = interceptor.NewClient(c, interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*v1alpha1.MysqlBackupVerificationList); ok {
							return errors.New("injected")
						}
						return c.List(ctx, list, opts...)
					},
				})
			}
			resp, err := resolvePITRCutoff(context.Background(), c, holdReader, "ns", "fg", cutoffTestArchive)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("an unknown reader set produced a cutoff: %v", resp)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if resp["cutoffTime"] != tc.cutoff.Format(time.RFC3339) {
				t.Fatalf("cutoff %v, want %s", resp["cutoffTime"], tc.cutoff.Format(time.RFC3339))
			}
		})
	}
}

// TestPITRCutoffListsVerificationsOnce pins that a lookup with many stuck
// Terminating backups costs one uncached verification List, not one each.
func TestPITRCutoffListsVerificationsOnce(t *testing.T) {
	start := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objs := []client.Object{retentionBoundaryBackup("kept", retentionTestSID+":1-20", start.Add(time.Hour))}
	now := metav1.Now()
	for _, name := range []string{"stuck-a", "stuck-b", "stuck-c", "stuck-d"} {
		b := retentionBoundaryBackup(name, retentionTestSID+":1-10", start)
		b.UID = types.UID("uid-" + name)
		b.DeletionTimestamp = &now
		b.Finalizers = []string{"bloodraven.shipstream.io/mysqlbackup"}
		objs = append(objs, b)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	lists := 0
	holdReader := interceptor.NewClient(c, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1alpha1.MysqlBackupVerificationList); ok {
				lists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	resp, err := resolvePITRCutoff(context.Background(), c, holdReader, "ns", "fg", cutoffTestArchive)
	if err != nil {
		t.Fatal(err)
	}
	if lists != 1 {
		t.Fatalf("verification Lists = %d, want 1 per request", lists)
	}
	if resp["archive"] != cutoffTestArchive {
		t.Fatalf("response echoes archive %v, want %s", resp["archive"], cutoffTestArchive)
	}
	if _, ok := resp["profile"]; ok {
		t.Fatalf("response still echoes a profile: %v", resp)
	}
	if resp["cutoffTime"] != start.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("cutoff %v", resp["cutoffTime"])
	}
}
