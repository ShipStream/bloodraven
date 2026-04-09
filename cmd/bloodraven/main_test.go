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

func TestAuxMuxStatusWithoutPairs(t *testing.T) {
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
	if status["status"] != "no active pairs" {
		t.Fatalf("expected no active pairs status, got %#v", status)
	}
}
