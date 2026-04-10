package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/platform"
	"k8s.io/apimachinery/pkg/types"
)

func TestAuxMuxHealthz(t *testing.T) {
	hub := platform.NewHub(slog.Default())
	runner := controller.NewTopologyManagerRunner(nil, nil, hub, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	newAuxMux(runner, hub).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("expected body %q, got %q", "ok", rec.Body.String())
	}
}

func TestActiveSiteMissingParams(t *testing.T) {
	hub := platform.NewHub(slog.Default())
	runner := controller.NewTopologyManagerRunner(nil, nil, hub, slog.Default())

	tests := []struct {
		name string
		url  string
	}{
		{"missing both", "/active-site"},
		{"missing namespace", "/active-site?group=orders"},
		{"missing group", "/active-site?namespace=default"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rec := httptest.NewRecorder()
			newAuxMux(runner, hub).ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
		})
	}
}

func TestActiveSiteNoManagers(t *testing.T) {
	hub := platform.NewHub(slog.Default())
	runner := controller.NewTopologyManagerRunner(nil, nil, hub, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/active-site?namespace=default&group=orders", nil)
	rec := httptest.NewRecorder()
	newAuxMux(runner, hub).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestActiveSiteNotFound(t *testing.T) {
	hub := platform.NewHub(slog.Default())
	runner := controller.NewTopologyManagerRunner(nil, nil, hub, slog.Default())

	// Add a dummy manager for a different group.
	cfg := controller.TopologyConfig{
		Name:  "other",
		Sites: [2]controller.SiteTopologyConfig{{Name: "dc1"}, {Name: "dc2"}},
	}
	fc := controller.NewFailoverController(slog.Default())
	tm := controller.NewTopologyManager(cfg, nil, nil, fc, nil, hub, nil, slog.Default())
	runner.SetManagerForTest(types.NamespacedName{Namespace: "default", Name: "other"}, tm)

	req := httptest.NewRequest(http.MethodGet, "/active-site?namespace=default&group=orders", nil)
	rec := httptest.NewRecorder()
	newAuxMux(runner, hub).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestActiveSiteFound(t *testing.T) {
	hub := platform.NewHub(slog.Default())
	runner := controller.NewTopologyManagerRunner(nil, nil, hub, slog.Default())

	cfg := controller.TopologyConfig{
		Name:  "orders",
		Sites: [2]controller.SiteTopologyConfig{{Name: "iad"}, {Name: "pdx"}},
	}
	fc := controller.NewFailoverController(slog.Default())
	tm := controller.NewTopologyManager(cfg, nil, nil, fc, nil, hub, nil, slog.Default())
	runner.SetManagerForTest(types.NamespacedName{Namespace: "default", Name: "orders"}, tm)

	req := httptest.NewRequest(http.MethodGet, "/active-site?namespace=default&group=orders", nil)
	rec := httptest.NewRecorder()
	newAuxMux(runner, hub).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["namespace"] != "default" {
		t.Errorf("expected namespace=default, got %q", result["namespace"])
	}
	if result["group"] != "orders" {
		t.Errorf("expected group=orders, got %q", result["group"])
	}
	// No polls run, so active_site should be empty.
	if result["active_site"] != "" {
		t.Errorf("expected empty active_site, got %q", result["active_site"])
	}
}

func TestAuxMuxStatusWithoutFailoverGroups(t *testing.T) {
	hub := platform.NewHub(slog.Default())
	runner := controller.NewTopologyManagerRunner(nil, nil, hub, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()

	newAuxMux(runner, hub).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var status map[string]string
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if status["status"] != "no active failover groups" {
		t.Fatalf("expected no active failover groups status, got %#v", status)
	}
}
