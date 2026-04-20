package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// mockMysqlQuerier implements mysqlQuerier for testing.
type mockMysqlQuerier struct {
	connectable       bool
	readOnly          bool
	superReadOnly     bool
	status            *StatusInfo
	statusErr         error
	readOnlyErr       error
	setReadOnlyErr    error
	clearReadOnlyErr  error
	setReadOnlyCalled   bool
	clearReadOnlyCalled bool
}

func (m *mockMysqlQuerier) queryStatus(_ context.Context) (*StatusInfo, error) {
	if m.statusErr != nil {
		return nil, m.statusErr
	}
	if m.status != nil {
		return m.status, nil
	}
	return &StatusInfo{
		Role:         "primary",
		ReadOnly:     false,
		SuperReadOnly: false,
		ServerID:     101,
		GtidExecuted: "uuid:1-100",
		Uptime:       3600,
	}, nil
}

func (m *mockMysqlQuerier) isConnectable(_ context.Context) bool {
	return m.connectable
}

func (m *mockMysqlQuerier) IsReadOnly(_ context.Context) (bool, error) {
	if m.readOnlyErr != nil {
		return false, m.readOnlyErr
	}
	return m.readOnly, nil
}

func (m *mockMysqlQuerier) SetSuperReadOnly(_ context.Context) error {
	m.setReadOnlyCalled = true
	if m.setReadOnlyErr != nil {
		return m.setReadOnlyErr
	}
	m.superReadOnly = true
	return nil
}

func (m *mockMysqlQuerier) ClearSuperReadOnly(_ context.Context) error {
	m.clearReadOnlyCalled = true
	if m.clearReadOnlyErr != nil {
		return m.clearReadOnlyErr
	}
	m.superReadOnly = false
	m.readOnly = false
	return nil
}

func TestPeerPingReturns200(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true}
	srv := NewServer(mock, ":0", testLogger())

	req := httptest.NewRequest(http.MethodGet, "/peer/ping", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "pong" {
		t.Errorf("expected 'pong', got %q", w.Body.String())
	}
}

func TestPeerActiveSite_ReturnsSnapshot(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true}
	srv := NewServer(mock, ":0", testLogger())

	cache := &TopologyCache{}
	observed := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	cache.Set("pdx", observed)
	srv.SetTopology(cache)

	req := httptest.NewRequest(http.MethodGet, "/peer/active-site", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var snap TopologySnapshot
	if err := json.NewDecoder(w.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.ActiveSite != "pdx" {
		t.Errorf("activeSite = %q, want pdx", snap.ActiveSite)
	}
	if !snap.ObservedAt.Equal(observed) {
		t.Errorf("observedAt = %v, want %v", snap.ObservedAt, observed)
	}
}

func TestPeerActiveSite_EmptyCacheReturns204(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true}
	srv := NewServer(mock, ":0", testLogger())
	srv.SetTopology(&TopologyCache{}) // empty cache

	req := httptest.NewRequest(http.MethodGet, "/peer/active-site", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204 for empty cache, got %d", w.Code)
	}
}

func TestPeerActiveSite_NoTopologyReturns204(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true}
	srv := NewServer(mock, ":0", testLogger())
	// No SetTopology call — topology is nil.

	req := httptest.NewRequest(http.MethodGet, "/peer/active-site", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204 when topology is nil, got %d", w.Code)
	}
}

func TestHealthReturns200WhenMySQLConnectable(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true}
	srv := NewServer(mock, ":0", testLogger())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHealthReturns503WhenMySQLUnreachable(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: false}
	srv := NewServer(mock, ":0", testLogger())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestStatusReturnsValidJSON(t *testing.T) {
	mock := &mockMysqlQuerier{
		connectable: true,
		status: &StatusInfo{
			Role:              "primary",
			ReadOnly:          false,
			SuperReadOnly:     false,
			GtidExecuted:      "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-45839",
			ReplicaIORunning:  false,
			ReplicaSQLRunning: false,
			ServerID:          101,
			Uptime:            7200,
		},
	}
	srv := NewServer(mock, ":0", testLogger())

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var result StatusInfo
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if result.Role != "primary" {
		t.Errorf("expected role 'primary', got %q", result.Role)
	}
	if result.ReadOnly {
		t.Error("expected read_only=false")
	}
	if result.ServerID != 101 {
		t.Errorf("expected server_id=101, got %d", result.ServerID)
	}
	if result.GtidExecuted != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-45839" {
		t.Errorf("unexpected gtid_executed: %q", result.GtidExecuted)
	}
	if result.Uptime != 7200 {
		t.Errorf("expected uptime=7200, got %d", result.Uptime)
	}
}

func TestStatusReturns503WhenMySQLFails(t *testing.T) {
	mock := &mockMysqlQuerier{
		connectable: false,
		statusErr:   context.DeadlineExceeded,
	}
	srv := NewServer(mock, ":0", testLogger())

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

// fakeOperator returns an httptest.Server that responds to /active-site requests.
func fakeOperator(activeSite string, statusCode int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		if statusCode == http.StatusOK {
			json.NewEncoder(w).Encode(map[string]string{"activeSite": activeSite})
		} else {
			json.NewEncoder(w).Encode(map[string]string{"error": "test"})
		}
	}))
}

func safetyNetConfig(operatorAddr, mySite string) *Config {
	return &Config{
		MySite:            mySite,
		PodNamespace:      "default",
		FailoverGroup:     "orders",
		BloodravenAddress: operatorAddr,
	}
}

func TestSafetyNetFencesAndStaysFencedOnNonActiveSite(t *testing.T) {
	op := fakeOperator("site1", http.StatusOK)
	defer op.Close()
	addr := op.Listener.Addr().String()

	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig(addr, "site2"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should set super_read_only on startup")
	}
	if mock.clearReadOnlyCalled {
		t.Error("safety net should NOT clear super_read_only on non-active site")
	}
}

func TestSafetyNetFencesThenClearsOnActiveSite(t *testing.T) {
	op := fakeOperator("site1", http.StatusOK)
	defer op.Close()
	addr := op.Listener.Addr().String()

	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig(addr, "site1"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should set super_read_only on startup even on active site")
	}
	if !mock.clearReadOnlyCalled {
		t.Error("safety net should clear super_read_only after confirming active site")
	}
}

func TestSafetyNetSkipsMissingIdentity(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), &Config{})

	if mock.setReadOnlyCalled {
		t.Error("safety net should not set super_read_only when identity not configured")
	}
}

func TestSafetyNetStaysFencedWhenOperatorUnavailable(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig("127.0.0.1:1", "site2"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should fence on startup even when operator is unreachable")
	}
	if mock.clearReadOnlyCalled {
		t.Error("safety net should NOT clear fence when operator is unreachable")
	}
}

func TestSafetyNetStaysFencedOnOperator503(t *testing.T) {
	op := fakeOperator("", http.StatusServiceUnavailable)
	defer op.Close()
	addr := op.Listener.Addr().String()

	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig(addr, "site2"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should fence on startup")
	}
	if mock.clearReadOnlyCalled {
		t.Error("safety net should NOT clear fence when operator returns 503")
	}
}

func TestSafetyNetStaysFencedOnOperator404(t *testing.T) {
	op := fakeOperator("", http.StatusNotFound)
	defer op.Close()
	addr := op.Listener.Addr().String()

	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig(addr, "site2"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should fence on startup")
	}
	if mock.clearReadOnlyCalled {
		t.Error("safety net should NOT clear fence when operator returns 404")
	}
}

func TestSafetyNetStaysFencedOnEmptyActiveSite(t *testing.T) {
	op := fakeOperator("", http.StatusOK)
	defer op.Close()
	addr := op.Listener.Addr().String()

	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig(addr, "site2"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should fence on startup")
	}
	if mock.clearReadOnlyCalled {
		t.Error("safety net should NOT clear fence when activeSite is empty")
	}
}

func TestSafetyNetStaysFencedOnMalformedJSON(t *testing.T) {
	op := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "not json")
	}))
	defer op.Close()
	addr := op.Listener.Addr().String()

	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), safetyNetConfig(addr, "site2"))

	if !mock.setReadOnlyCalled {
		t.Error("safety net should fence on startup")
	}
	if mock.clearReadOnlyCalled {
		t.Error("safety net should NOT clear fence on malformed JSON")
	}
}
