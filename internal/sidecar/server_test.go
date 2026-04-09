package sidecar

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// mockMysqlQuerier implements mysqlQuerier for testing.
type mockMysqlQuerier struct {
	connectable     bool
	readOnly        bool
	superReadOnly   bool
	status          *StatusInfo
	statusErr       error
	readOnlyErr     error
	setReadOnlyErr  error
	setReadOnlyCalled bool
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

func TestSafetyNetSetsSuperReadOnlyOnNonActiveSite(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), "site2", "site1")

	if !mock.setReadOnlyCalled {
		t.Error("safety net should set super_read_only on non-active site with read_only=OFF")
	}
}

func TestSafetyNetSkipsActiveSite(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), "site1", "site1")

	if mock.setReadOnlyCalled {
		t.Error("safety net should not set super_read_only on active site")
	}
}

func TestSafetyNetSkipsWhenAlreadyReadOnly(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true, readOnly: true}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), "site2", "site1")

	if mock.setReadOnlyCalled {
		t.Error("safety net should not set super_read_only when already read-only")
	}
}

func TestSafetyNetSkipsWhenEnvNotSet(t *testing.T) {
	mock := &mockMysqlQuerier{connectable: true, readOnly: false}
	srv := NewServer(mock, ":0", testLogger())

	srv.RunSafetyNet(context.Background(), "", "")

	if mock.setReadOnlyCalled {
		t.Error("safety net should not set super_read_only when env vars not set")
	}
}
