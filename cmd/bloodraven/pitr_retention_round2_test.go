package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func retentionRound2Group() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fg"}, Spec: v1alpha1.MysqlFailoverGroupSpec{Backup: &v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p"}}}}
}
func retentionRound2Request(t *testing.T, h http.Handler, caller, query string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&profile=p&coverage=gtid-v1"+query, nil)
	r.RemoteAddr = caller
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return w.Code, body
}
func TestRereviewProbeAdmissionMapStarvesLegitimateGroups(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &retentionReviewReader{Reader: retentionReviewClient(t, retentionRound2Group(), backup)}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	for i := 0; i < pitrCutoffCacheMaxEntries; i++ {
		r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/pitr-cutoff?namespace=x&group=g%d&profile=p&coverage=gtid-v1", i), nil)
		r.RemoteAddr = "192.0.2.1:2000"
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	status, body := retentionRound2Request(t, h, "192.0.2.2:3000", "")
	if status != http.StatusOK || body["cutoffTime"] == nil {
		t.Fatalf("legitimate group refused after junk keys: HTTP %d %v", status, body)
	}
	if reader.lists.Load() != 1 {
		t.Fatalf("junk requests triggered backup LISTs: %d", reader.lists.Load())
	}
}
func TestRound2CutoffCallerCannotReserveAnotherSitesProof(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	c := retentionReviewClient(t, retentionRound2Group(), backup)
	h := newAuxMux(nil, platform.NewHub(slog.Default()), c)
	if status, body := retentionRound2Request(t, h, "192.0.2.1:2000", ""); status != 200 || body["cutoffTime"] == nil {
		t.Fatal(status, body)
	}
	for i := 0; i < 5; i++ {
		retentionRound2Request(t, h, "192.0.2.1:2000", "")
	}
	// A newly observed lagging backup must appear in the other site's fresh proof.
	lagging := retentionBoundaryBackup("lagging", retentionTestSID+":1-10", time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC))
	if err := c.Create(context.Background(), lagging); err != nil {
		t.Fatal(err)
	}
	status, body := retentionRound2Request(t, h, "192.0.2.2:3000", "")
	sets, _ := body["retainedGTIDs"].([]any)
	if status != 200 || len(sets) != 2 {
		t.Fatalf("caller stole another site's admission or hid lagging backup: HTTP %d %v", status, body)
	}
}

type retentionRound2FailReader struct {
	client.Reader
	fail bool
}

func (r *retentionRound2FailReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if r.fail {
		return fmt.Errorf("injected lookup failure")
	}
	return r.Reader.List(ctx, list, opts...)
}
func TestRound2FailedCutoffLookupCannotBlockAnotherCaller(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &retentionRound2FailReader{Reader: retentionReviewClient(t, retentionRound2Group(), backup), fail: true}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	status, _ := retentionRound2Request(t, h, "192.0.2.1:2000", "")
	if status != 500 {
		t.Fatalf("failure became a safe deferral: HTTP %d", status)
	}
	reader.fail = false
	status, body := retentionRound2Request(t, h, "192.0.2.2:3000", "")
	if status != 200 || body["cutoffTime"] == nil {
		t.Fatalf("failed caller blocked healthy lookup: HTTP %d %v", status, body)
	}
}

func TestRound2InvalidPITRProfileAndForwardingHeaderCannotConsumeOtherAdmission(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &retentionReviewReader{Reader: retentionReviewClient(t, backup)}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	request := func(profile, forwarded string) int {
		r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&profile="+profile+"&coverage=gtid-v1", nil)
		r.RemoteAddr = "192.0.2.1:3000"
		r.Header.Set("X-Forwarded-For", forwarded)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if got := request("wrong-profile", ""); got != 404 {
		t.Fatalf("unconfigured profile was admitted: %d", got)
	}
	if got := request("p", ""); got != 200 {
		t.Fatalf("junk profile consumed caller's slot: %d", got)
	}
	if got := request("p", "192.0.2.99"); got != 429 {
		t.Fatalf("forged forwarding header bypassed source-IP throttle: %d", got)
	}
	if reader.lists.Load() != 1 {
		t.Fatalf("invalid or throttled queries reached backup LIST: %d", reader.lists.Load())
	}
}

type retentionCanceledCallerReader struct {
	client.Reader
	cancel context.CancelFunc
}

func (r *retentionCanceledCallerReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.Reader.List(ctx, list, opts...)
}
func TestRound2DisconnectedCallerDoesNotCancelSharedCutoffLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &retentionCanceledCallerReader{Reader: retentionReviewClient(t, backup), cancel: cancel}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&profile=p&coverage=gtid-v1", nil).WithContext(ctx)
	r.RemoteAddr = "192.0.2.1:2000"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("request cancellation poisoned the shared lookup: %d %s", w.Code, w.Body.String())
	}
	if ctx.Err() == nil {
		t.Fatal("test did not disconnect the first caller")
	}
	status, body := retentionRound2Request(t, h, "192.0.2.2:3000", "")
	if status != 200 || body["cutoffTime"] == nil {
		t.Fatalf("disconnected caller disabled a later site: %d %v", status, body)
	}
}
