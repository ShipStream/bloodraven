package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGetArchiverStatusDecodesPayload(t *testing.T) {
	lastUpload := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	oldest := time.Date(2026, 4, 10, 9, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 4, 10, 9, 45, 0, 0, time.UTC)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/archiver/status" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":            true,
			"primary":            true,
			"site":               "dc1",
			"storageType":        "S3",
			"manifestPrefix":     "binlogs",
			"filesArchived":      7,
			"uploadFailures":     2,
			"lastUploadAt":       lastUpload,
			"backlogFiles":       1,
			"manifestFileCount":  3,
			"manifestBytes":      1024,
			"oldestArchivedTime": oldest,
			"newestArchivedTime": newest,
			"lastError":          "transient 503",
		})
	}))
	defer ts.Close()

	c := NewSidecarClient(ts.URL)
	got, err := c.GetArchiverStatus(context.Background())
	if err != nil {
		t.Fatalf("GetArchiverStatus: %v", err)
	}
	if !got.Enabled || !got.Primary {
		t.Errorf("expected enabled+primary, got %+v", got)
	}
	if got.Site != "dc1" || got.StorageType != "S3" {
		t.Errorf("identity wrong: %+v", got)
	}
	if got.UploadFailures != 2 || got.BacklogFiles != 1 {
		t.Errorf("counters wrong: uploads=%d backlog=%d", got.UploadFailures, got.BacklogFiles)
	}
	if got.ManifestFileCount != 3 || got.ManifestBytes != 1024 {
		t.Errorf("manifest aggregates wrong: count=%d bytes=%d", got.ManifestFileCount, got.ManifestBytes)
	}
	if !got.LastUploadAt.Equal(lastUpload) {
		t.Errorf("LastUploadAt = %v, want %v", got.LastUploadAt, lastUpload)
	}
	if !got.OldestArchivedTime.Equal(oldest) || !got.NewestArchivedTime.Equal(newest) {
		t.Errorf("times wrong: oldest=%v newest=%v", got.OldestArchivedTime, got.NewestArchivedTime)
	}
	if got.LastError != "transient 503" {
		t.Errorf("LastError = %q, want transient 503", got.LastError)
	}
}

func TestGetArchiverStatusNon200Errors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	c := NewSidecarClient(ts.URL)
	_, err := c.GetArchiverStatus(context.Background())
	if err == nil {
		t.Fatal("expected error on 503, got nil")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error = %v, want contains 503", err)
	}
}

func TestGetArchiverStatusMalformedJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"enabled": notjson}`))
	}))
	defer ts.Close()

	c := NewSidecarClient(ts.URL)
	_, err := c.GetArchiverStatus(context.Background())
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
}
