package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/platform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cutoffTestUID is the test group's UID. Archive IDs are derived from it,
// so /pitr-cutoff only admits archives this group creates (Δ12).
const cutoffTestUID = types.UID("fg-uid")

// cutoffTestDestination is the destination of the test group's one binding.
func cutoffTestDestination() pitrarchive.Destination {
	dest, err := pitrarchive.NewPVCDestination("ns", "backups", "")
	if err != nil {
		panic(err)
	}
	return dest
}

// cutoffTestArchive is the archive ID the test group creates at its bound
// destination: the one its sidecars send to /pitr-cutoff.
var cutoffTestArchive = pitrarchive.CreateArchiveID(cutoffTestUID, cutoffTestDestination())

// cutoffTestBinding is a Bound plaintext binding for dest.
func cutoffTestBinding(dest pitrarchive.Destination) v1alpha1.PITRArchiveBinding {
	sum := sha256.Sum256([]byte("bloodraven-pitr-binding-v1\x00" + dest.String()))
	return v1alpha1.PITRArchiveBinding{
		ID:          "pab-" + hex.EncodeToString(sum[:])[:16],
		Destination: dest.String(),
		Encryption:  v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.PITRArchiveEncryptionNone},
		State:       v1alpha1.PITRArchiveBindingBound,
		Reason:      "Bound",
	}
}

func cutoffTestGroup() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fg", UID: cutoffTestUID},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Backup: &v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p"}}},
		Status:     v1alpha1.MysqlFailoverGroupStatus{PITRArchiveBindings: []v1alpha1.PITRArchiveBinding{cutoffTestBinding(cutoffTestDestination())}},
	}
}
func cutoffRequest(t *testing.T, h http.Handler, caller, query string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&archive="+cutoffTestArchive+"&coverage=gtid-v1"+query, nil)
	r.RemoteAddr = caller
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return w.Code, body
}
func TestPITRCutoffJunkKeysDoNotStarveLegitimateGroups(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &countingBackupReader{Reader: cutoffTestClient(t, cutoffTestGroup(), backup)}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	for i := 0; i < pitrCutoffCacheMaxEntries; i++ {
		r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/pitr-cutoff?namespace=x&group=g%d&archive=%s&coverage=gtid-v1", i, cutoffTestArchive), nil)
		r.RemoteAddr = "192.0.2.1:2000"
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	status, body := cutoffRequest(t, h, "192.0.2.2:3000", "")
	if status != http.StatusOK || body["cutoffTime"] == nil {
		t.Fatalf("legitimate group refused after junk keys: HTTP %d %v", status, body)
	}
	if reader.lists.Load() != 1 {
		t.Fatalf("junk requests triggered backup LISTs: %d", reader.lists.Load())
	}
}
func TestPITRCutoffCallerCannotReserveAnotherSitesProof(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	c := cutoffTestClient(t, cutoffTestGroup(), backup)
	h := newAuxMux(nil, platform.NewHub(slog.Default()), c)
	if status, body := cutoffRequest(t, h, "192.0.2.1:2000", ""); status != 200 || body["cutoffTime"] == nil {
		t.Fatal(status, body)
	}
	for i := 0; i < 5; i++ {
		cutoffRequest(t, h, "192.0.2.1:2000", "")
	}
	// A newly observed lagging backup must appear in the other site's fresh proof.
	lagging := retentionBoundaryBackup("lagging", retentionTestSID+":1-10", time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC))
	if err := c.Create(context.Background(), lagging); err != nil {
		t.Fatal(err)
	}
	status, body := cutoffRequest(t, h, "192.0.2.2:3000", "")
	sets, _ := body["retainedGTIDs"].([]any)
	if status != 200 || len(sets) != 2 {
		t.Fatalf("caller stole another site's admission or hid lagging backup: HTTP %d %v", status, body)
	}
}

// TestPITRCutoffGatesOnOwnArchive pins Δ12: /pitr-cutoff answers only for
// an archive the group itself creates at one of its bound destinations. A
// pod on the old render (profile=) gets 400 until it rolls, so retention
// defers and nothing is deleted.
func TestPITRCutoffGatesOnOwnArchive(t *testing.T) {
	other, err := pitrarchive.NewS3Destination("offsite", "pitr", "")
	if err != nil {
		t.Fatal(err)
	}
	group := cutoffTestGroup()
	group.Status.PITRArchiveBindings = append(group.Status.PITRArchiveBindings, cutoffTestBinding(other))
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	for _, tc := range []struct {
		name, query string
		want        int
	}{
		{"own archive", "archive=" + cutoffTestArchive, http.StatusOK},
		{"own second binding", "archive=" + pitrarchive.CreateArchiveID(cutoffTestUID, other), http.StatusOK},
		{"another group's archive at the same destination", "archive=" + pitrarchive.CreateArchiveID("other-uid", cutoffTestDestination()), http.StatusNotFound},
		{"well-formed unknown archive", "archive=ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa", http.StatusNotFound},
		{"invalid archive", "archive=not-an-archive", http.StatusNotFound},
		{"missing archive", "", http.StatusBadRequest},
		{"old render sends a profile", "profile=p", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newAuxMux(nil, platform.NewHub(slog.Default()), cutoffTestClient(t, group, backup))
			r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&coverage=gtid-v1&"+tc.query, nil)
			r.RemoteAddr = "192.0.2.1:2000"
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("HTTP %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			switch tc.want {
			case http.StatusOK:
				if body["cutoffTime"] == nil || body["archive"] != r.URL.Query().Get("archive") {
					t.Fatalf("own archive refused a cutoff or echoed the wrong archive: %v", body)
				}
			case http.StatusNotFound:
				if body["error"] != "PITR archive is not one of this group's archives" || body["cutoffTime"] != nil {
					t.Fatalf("foreign archive reply: %v", body)
				}
			case http.StatusBadRequest:
				if body["error"] != "namespace, group, and archive query parameters are required" {
					t.Fatalf("missing archive reply: %v", body)
				}
			}
		})
	}
}

// TestPITRCutoffRefusesGroupWithoutUID pins that a group with no UID (so no
// derivable archive ID) never admits an archive.
func TestPITRCutoffRefusesGroupWithoutUID(t *testing.T) {
	group := cutoffTestGroup()
	group.UID = ""
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	h := newAuxMux(nil, platform.NewHub(slog.Default()), cutoffTestClient(t, group, backup))
	if status, body := cutoffRequest(t, h, "192.0.2.1:2000", ""); status != http.StatusNotFound || body["cutoffTime"] != nil {
		t.Fatalf("group without a UID admitted an archive: HTTP %d %v", status, body)
	}
}

type failingBackupListReader struct {
	client.Reader
	fail bool
}

func (r *failingBackupListReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if r.fail {
		return fmt.Errorf("injected lookup failure")
	}
	return r.Reader.List(ctx, list, opts...)
}
func TestPITRFailedCutoffLookupCannotBlockAnotherCaller(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &failingBackupListReader{Reader: cutoffTestClient(t, cutoffTestGroup(), backup), fail: true}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	status, _ := cutoffRequest(t, h, "192.0.2.1:2000", "")
	if status != 500 {
		t.Fatalf("failure became a safe deferral: HTTP %d", status)
	}
	reader.fail = false
	status, body := cutoffRequest(t, h, "192.0.2.2:3000", "")
	if status != 200 || body["cutoffTime"] == nil {
		t.Fatalf("failed caller blocked healthy lookup: HTTP %d %v", status, body)
	}
}

func TestPITRInvalidArchiveAndForwardingHeaderCannotConsumeOtherAdmission(t *testing.T) {
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &countingBackupReader{Reader: cutoffTestClient(t, backup)}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	request := func(archive, forwarded string) int {
		r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&archive="+archive+"&coverage=gtid-v1", nil)
		r.RemoteAddr = "192.0.2.1:3000"
		r.Header.Set("X-Forwarded-For", forwarded)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if got := request("wrong-archive", ""); got != 404 {
		t.Fatalf("unknown archive was admitted: %d", got)
	}
	if got := request(cutoffTestArchive, ""); got != 200 {
		t.Fatalf("junk archive consumed caller's slot: %d", got)
	}
	if got := request(cutoffTestArchive, "192.0.2.99"); got != 429 {
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
func TestPITRDisconnectedCallerDoesNotCancelSharedCutoffLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backup := retentionBoundaryBackup("retained", retentionTestSID+":1-20", time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	reader := &retentionCanceledCallerReader{Reader: cutoffTestClient(t, backup), cancel: cancel}
	h := newAuxMux(nil, platform.NewHub(slog.Default()), reader)
	r := httptest.NewRequest(http.MethodGet, "/pitr-cutoff?namespace=ns&group=fg&archive="+cutoffTestArchive+"&coverage=gtid-v1", nil).WithContext(ctx)
	r.RemoteAddr = "192.0.2.1:2000"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("request cancellation poisoned the shared lookup: %d %s", w.Code, w.Body.String())
	}
	if ctx.Err() == nil {
		t.Fatal("test did not disconnect the first caller")
	}
	status, body := cutoffRequest(t, h, "192.0.2.2:3000", "")
	if status != 200 || body["cutoffTime"] == nil {
		t.Fatalf("disconnected caller disabled a later site: %d %v", status, body)
	}
}
