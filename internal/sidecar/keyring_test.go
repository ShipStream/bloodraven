package sidecar

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// fakeKeyringMySQL is a scripted keyringQuerier.
type fakeKeyringMySQL struct {
	component      *KeyringComponentStatus
	coverage       *KeyringCoverage
	componentErr   error
	coverageErr    error
	componentCalls atomic.Int32
	coverageCalls  atomic.Int32
	rotateErr      error
	rotateCalls    atomic.Int32
	encryptCalls   atomic.Int32
	readOnly       bool
	readOnlyErr    error
	encryptSysErr  error
	encryptSysFunc func() error
}

func (f *fakeKeyringMySQL) KeyringComponentStatus(context.Context) (*KeyringComponentStatus, error) {
	f.componentCalls.Add(1)
	if f.componentErr != nil {
		return nil, f.componentErr
	}
	return f.component, nil
}

func (f *fakeKeyringMySQL) EncryptionCoverage(context.Context) (*KeyringCoverage, error) {
	f.coverageCalls.Add(1)
	return f.coverage, f.coverageErr
}

func (f *fakeKeyringMySQL) RotateInnoDBMasterKey(context.Context) error {
	f.rotateCalls.Add(1)
	return f.rotateErr
}

func (f *fakeKeyringMySQL) EncryptSystemTablespace(context.Context) error {
	f.encryptCalls.Add(1)
	if f.encryptSysFunc != nil {
		return f.encryptSysFunc()
	}
	return f.encryptSysErr
}

func (f *fakeKeyringMySQL) IsReadOnly(context.Context) (bool, error) {
	return f.readOnly, f.readOnlyErr
}

// escrowServer is a stand-in for the operator's /keyring/escrow endpoint.
type escrowServer struct {
	srv      *httptest.Server
	mu       chan struct{}
	received [][]byte
	tokens   []string
	version  int32
	// corruptDigest makes the server echo a digest that does not match
	// what it received, exercising the sidecar's verification.
	corruptDigest bool
	status        int
}

func newEscrowServer(t *testing.T) *escrowServer {
	t.Helper()
	es := &escrowServer{mu: make(chan struct{}, 1), status: http.StatusOK}
	es.mu <- struct{}{}
	es.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req escrowRequest
		_ = json.Unmarshal(body, &req)
		raw, _ := base64.StdEncoding.DecodeString(req.Keyring)

		<-es.mu
		es.received = append(es.received, raw)
		es.tokens = append(es.tokens, r.Header.Get("Authorization"))
		es.version++
		v := es.version
		corrupt := es.corruptDigest
		status := es.status
		es.mu <- struct{}{}

		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
			return
		}
		d := digestOf(raw)
		if corrupt {
			d = digestOf([]byte("something-else"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(escrowResponse{Version: v, Digest: d, Secret: "sec"})
	}))
	t.Cleanup(es.srv.Close)
	return es
}

func (es *escrowServer) url() string {
	return es.srv.URL + "/keyring/escrow"
}

func (es *escrowServer) snapshot() ([][]byte, []string) {
	<-es.mu
	defer func() { es.mu <- struct{}{} }()
	r := make([][]byte, len(es.received))
	copy(r, es.received)
	tk := make([]string, len(es.tokens))
	copy(tk, es.tokens)
	return r, tk
}

type agentFixture struct {
	agent   *KeyringAgent
	dir     string
	keyring string
	escrow  *escrowServer
	mysql   *fakeKeyringMySQL
}

func newAgentFixture(t *testing.T, mutate func(*KeyringConfig)) *agentFixture {
	t.Helper()
	dir := t.TempDir()
	keyring := filepath.Join(dir, "keyring")
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("s3cr3t-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	es := newEscrowServer(t)
	caFile := filepath.Join(dir, "escrow-ca.crt")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: es.srv.Certificate().Raw})
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("write escrow CA: %v", err)
	}
	cfg := &KeyringConfig{
		Path:         keyring,
		EscrowArmed:  true,
		TokenFile:    tokenFile,
		EscrowURL:    es.url(),
		EscrowCAFile: caFile,
		PollInterval: time.Millisecond,
	}
	if mutate != nil {
		mutate(cfg)
	}
	my := &fakeKeyringMySQL{
		component: &KeyringComponentStatus{Name: "component_keyring_file", Status: "Active"},
		coverage:  &KeyringCoverage{SystemTablespaceEncrypted: true},
	}
	agent, err := NewKeyringAgent(cfg, &Config{
		PodNamespace:  "shared-lion",
		FailoverGroup: "lion",
		MySite:        "dc1",
	}, my, discardLogger())
	if err != nil {
		t.Fatalf("new keyring agent: %v", err)
	}

	return &agentFixture{agent: agent, dir: dir, keyring: keyring, escrow: es, mysql: my}
}

// --- status ---------------------------------------------------------

func TestKeyringAgent_MissingFileIsNotAnError(t *testing.T) {
	f := newAgentFixture(t, nil)
	f.agent.tick(context.Background())

	got := f.agent.Snapshot()
	if got.Present {
		t.Error("Present should be false when the keyring does not exist yet")
	}
	if got.LastError != "" {
		t.Errorf("a not-yet-created keyring is normal during bootstrap, got error %q", got.LastError)
	}
	received, _ := f.escrow.snapshot()
	if len(received) != 0 {
		t.Error("nothing should be pushed when there is no keyring")
	}
}

func TestKeyringAgent_EscrowEligibility(t *testing.T) {
	// Escrow must ignore bootstrap placeholders (empty file or empty
	// component document) and only push after real key material exists.
	// The ordered-rollout case also covers a replica that starts first
	// and must wait until a writable site creates a key via ALTER TABLESPACE.
	emptyDoc := []byte("{\"version\":\"1.0\",\"elements\":[]}\n")
	populated := []byte("{\"version\":\"1.0\",\"elements\":[{\"key\":\"master\"}]}\n")

	cases := []struct {
		name            string
		cfg             func(*KeyringConfig)
		setup           func(t *testing.T, f *agentFixture)
		wantEscrowCount int
		wantEscrowBody  string
		checkStatus     func(t *testing.T, got KeyringStatus)
	}{
		{
			name: "empty file is not escrowed",
			setup: func(t *testing.T, f *agentFixture) {
				// MySQL needs the file to exist before it starts; escrowing
				// it would record a useless "version 1" the operator might
				// then seal against.
				if err := os.WriteFile(f.keyring, nil, 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
				f.agent.tick(context.Background())
			},
			wantEscrowCount: 0,
			checkStatus: func(t *testing.T, got KeyringStatus) {
				if !got.Present || got.Size != 0 || got.Digest != "" {
					t.Errorf("status = %+v", got)
				}
			},
		},
		{
			name: "empty bootstrap document is not escrowed",
			setup: func(t *testing.T, f *agentFixture) {
				if err := os.WriteFile(f.keyring, emptyDoc, 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
				f.agent.tick(context.Background())
			},
			wantEscrowCount: 0,
			checkStatus: func(t *testing.T, got KeyringStatus) {
				if !got.Present || got.Size != int64(len(emptyDoc)) || got.Digest != "" {
					t.Errorf("status = %+v", got)
				}
			},
		},
		{
			name: "populated keyring after primary encryption is escrowed",
			cfg:  func(c *KeyringConfig) { c.EncryptSystemTablespace = true },
			setup: func(t *testing.T, f *agentFixture) {
				f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: false}
				f.mysql.readOnly = true
				if err := os.WriteFile(f.keyring, emptyDoc, 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
				// Install the encrypt side-effect before the replica tick so an
				// accidental call would both count and mutate the keyring.
				f.mysql.encryptSysFunc = func() error {
					return os.WriteFile(f.keyring, populated, 0o600)
				}
				// Replica-first during ordered rollout must not encrypt or
				// escrow the bootstrap document before a writable site creates a key.
				f.agent.tick(context.Background())
				if got := f.mysql.encryptCalls.Load(); got != 0 {
					t.Fatalf("read-only replica invoked system-tablespace encryption %d times", got)
				}
				received, _ := f.escrow.snapshot()
				if len(received) != 0 {
					t.Fatalf("escrowed the keyless bootstrap document: %q", received[0])
				}

				f.mysql.readOnly = false
				f.agent.tick(context.Background())
				if got := f.mysql.encryptCalls.Load(); got != 1 {
					t.Fatalf("encryption calls = %d, want 1", got)
				}
			},
			wantEscrowCount: 1,
			wantEscrowBody:  string(populated),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgentFixture(t, tc.cfg)
			tc.setup(t, f)

			received, _ := f.escrow.snapshot()
			if len(received) != tc.wantEscrowCount {
				t.Fatalf("escrow count = %d, want %d (payloads=%q)", len(received), tc.wantEscrowCount, received)
			}
			if tc.wantEscrowCount > 0 && tc.wantEscrowBody != "" {
				if got := string(received[0]); got != tc.wantEscrowBody {
					t.Fatalf("escrowed %q, want %q", got, tc.wantEscrowBody)
				}
			}
			if tc.checkStatus != nil {
				tc.checkStatus(t, f.agent.Snapshot())
			}
		})
	}
}

// --- escrow ---------------------------------------------------------

func TestKeyringAgent_PushesKeyring(t *testing.T) {
	f := newAgentFixture(t, nil)
	raw := []byte("real keyring bytes")
	if err := os.WriteFile(f.keyring, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	received, tokens := f.escrow.snapshot()
	if len(received) != 1 {
		t.Fatalf("expected one push, got %d", len(received))
	}
	if string(received[0]) != string(raw) {
		t.Errorf("pushed %q, want %q", received[0], raw)
	}
	// The token is read from the mounted file with trailing whitespace
	// stripped — a stray newline would otherwise fail the operator's
	// constant-time comparison.
	if tokens[0] != "Bearer s3cr3t-token" {
		t.Errorf("authorization header = %q", tokens[0])
	}

	got := f.agent.Snapshot()
	if got.EscrowedDigest != digestOf(raw) || got.EscrowedVersion != 1 {
		t.Errorf("status = %+v", got)
	}
	if got.LastEscrowAt == nil || got.LastEscrowAt.IsZero() {
		t.Error("LastEscrowAt should be stamped")
	}
}

func TestKeyringStatus_OmitsLastEscrowAtBeforeFirstPush(t *testing.T) {
	f := newAgentFixture(t, nil)
	raw, err := json.Marshal(f.agent.Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "lastEscrowAt") {
		t.Fatalf("initial status contains a zero escrow timestamp: %s", raw)
	}
}

func TestKeyringAgent_DoesNotRepushUnchangedKeyring(t *testing.T) {
	f := newAgentFixture(t, nil)
	if err := os.WriteFile(f.keyring, []byte("stable"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i := 0; i < 5; i++ {
		f.agent.tick(context.Background())
	}
	received, _ := f.escrow.snapshot()
	if len(received) != 1 {
		t.Fatalf("expected one push for an unchanged keyring, got %d", len(received))
	}
}

func TestKeyringAgent_PushesAgainWhenKeyringChanges(t *testing.T) {
	f := newAgentFixture(t, nil)
	if err := os.WriteFile(f.keyring, []byte("v1"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())
	if err := os.WriteFile(f.keyring, []byte("v2-after-rotation"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	received, _ := f.escrow.snapshot()
	if len(received) != 2 {
		t.Fatalf("expected two pushes, got %d", len(received))
	}
	if string(received[1]) != "v2-after-rotation" {
		t.Errorf("second push = %q", received[1])
	}
}

// TestKeyringAgent_RetriesWhenOperatorEchoesWrongDigest is the sidecar
// half of the escrow gate: if the operator did not store exactly what
// was sent, the agent must not record the push as durable, or the
// operator could later seal the site against the wrong bytes.
func TestKeyringAgent_RetriesWhenOperatorEchoesWrongDigest(t *testing.T) {
	f := newAgentFixture(t, nil)
	f.escrow.corruptDigest = true
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	f.agent.tick(context.Background())
	if got := f.agent.Snapshot(); got.EscrowedDigest != "" {
		t.Fatalf("a mismatched echo must not be recorded as escrowed: %+v", got)
	}

	// And it keeps retrying rather than giving up.
	f.agent.tick(context.Background())
	received, _ := f.escrow.snapshot()
	if len(received) != 2 {
		t.Errorf("expected a retry, got %d pushes", len(received))
	}
}

func TestKeyringAgent_RetriesOnServerError(t *testing.T) {
	// push treats every non-200 the same: surface only the HTTP status on
	// LastError, never the response body (which may carry credentials).
	tests := []struct {
		name   string
		status int
	}{
		{name: "forbidden", status: http.StatusForbidden},
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "bad_request", status: http.StatusBadRequest},
		{name: "internal_server_error", status: http.StatusInternalServerError},
		{name: "service_unavailable", status: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newAgentFixture(t, nil)
			f.escrow.status = tt.status
			if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			f.agent.tick(context.Background())

			got := f.agent.Snapshot()
			if got.EscrowedDigest != "" {
				t.Error("a rejected push must not be recorded as escrowed")
			}
			if got.LastError == "" {
				t.Error("the rejection should surface on status so the operator can report it")
			}
			wantStatus := fmt.Sprintf("HTTP status %d", tt.status)
			if !strings.Contains(got.LastError, wantStatus) {
				t.Errorf("LastError = %q, want only the safe %q", got.LastError, wantStatus)
			}
			if strings.Contains(got.LastError, "nope") {
				t.Errorf("LastError exposed the escrow response body: %q", got.LastError)
			}
		})
	}
}

func TestKeyringAgent_NoEscrowWhenDisarmed(t *testing.T) {
	// A sealed pod reports its digest (drift detection) but has no token
	// and nothing to push.
	f := newAgentFixture(t, func(c *KeyringConfig) {
		c.EscrowArmed = false
		c.TokenFile = ""
	})
	if err := os.WriteFile(f.keyring, []byte("sealed-keyring"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	received, _ := f.escrow.snapshot()
	if len(received) != 0 {
		t.Fatal("a sealed pod must not push anything")
	}
	if got := f.agent.Snapshot(); got.Digest != digestOf([]byte("sealed-keyring")) {
		t.Errorf("a sealed pod should still report its digest: %+v", got)
	}
}

// --- rotation -------------------------------------------------------

func TestKeyringAgent_RotatesOnceThenEscrows(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) { c.Rotate = true })
	if err := os.WriteFile(f.keyring, []byte("pre-rotation"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	f.agent.tick(context.Background())
	if got := f.mysql.rotateCalls.Load(); got != 1 {
		t.Fatalf("rotate calls = %d, want 1", got)
	}
	// Repeated ticks must not keep rotating — each rotation rewraps every
	// tablespace key.
	f.agent.tick(context.Background())
	f.agent.tick(context.Background())
	if got := f.mysql.rotateCalls.Load(); got != 1 {
		t.Errorf("rotate calls = %d, want 1 after repeated ticks", got)
	}
	if !f.agent.Snapshot().RotateDone {
		t.Error("RotateDone should be reported")
	}
}

// TestKeyringAgent_EscrowsEvenIfRotationFails covers the dangerous case:
// a rotation that errors may still have written a new key to the
// keyring, so the agent must escrow whatever is on disk rather than
// bailing out.
func TestKeyringAgent_EscrowsEvenIfRotationFails(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) { c.Rotate = true })
	f.mysql.rotateErr = errors.New("boom")
	if err := os.WriteFile(f.keyring, []byte("possibly-new-keys"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	received, _ := f.escrow.snapshot()
	if len(received) != 1 {
		t.Fatalf("keyring must still be escrowed after a failed rotation, got %d pushes", len(received))
	}
	if f.agent.Snapshot().RotateError == "" {
		t.Error("the rotation failure should be reported")
	}
}

func TestKeyringAgent_NoRotationWhenDisarmed(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) {
		c.Rotate = true
		c.EscrowArmed = false
	})
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())
	if got := f.mysql.rotateCalls.Load(); got != 0 {
		t.Errorf("rotate calls = %d: rotation without escrow would strand the new key", got)
	}
}

// --- system tablespace ----------------------------------------------

func TestKeyringAgent_EncryptsSystemTablespaceOnPrimary(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) { c.EncryptSystemTablespace = true })
	f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: false}
	f.mysql.readOnly = false
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	if got := f.mysql.encryptCalls.Load(); got != 1 {
		t.Errorf("encrypt calls = %d, want 1", got)
	}
}

func TestKeyringAgent_BootstrapsMasterKeyOnEmptyKeyring(t *testing.T) {
	// Adoption can leave a valid empty component_keyring_file document
	// with no master key; ROTATE is what seeds it so later DDL works.
	f := newAgentFixture(t, func(c *KeyringConfig) { c.EncryptSystemTablespace = true })
	f.mysql.component = &KeyringComponentStatus{Name: "component_keyring_file", Status: "Active"}
	f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: false}
	f.mysql.readOnly = false
	empty := []byte("{\"version\":\"1.0\",\"elements\":[]}\n")
	if err := os.WriteFile(f.keyring, empty, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	if got := f.mysql.rotateCalls.Load(); got != 1 {
		t.Fatalf("rotate calls = %d, want 1 to seed an empty keyring", got)
	}
	if got := f.mysql.encryptCalls.Load(); got != 1 {
		t.Errorf("encrypt calls = %d, want 1 after bootstrap", got)
	}
}

func TestKeyringAgent_SkipsMasterKeyBootstrapOnReplica(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) { c.EncryptSystemTablespace = true })
	f.mysql.component = &KeyringComponentStatus{Name: "component_keyring_file", Status: "Active"}
	f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: false}
	f.mysql.readOnly = true
	empty := []byte("{\"version\":\"1.0\",\"elements\":[]}\n")
	if err := os.WriteFile(f.keyring, empty, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())
	if got := f.mysql.rotateCalls.Load(); got != 0 {
		t.Errorf("rotate calls = %d, want 0 on a replica", got)
	}
	if got := f.mysql.encryptCalls.Load(); got != 0 {
		t.Errorf("encrypt calls = %d, want 0 on a replica", got)
	}
}

func TestKeyringAgent_SkipsSystemTablespaceOnReplica(t *testing.T) {
	// ALTER TABLESPACE is replicated DDL; running it on a read-only
	// replica just fails, and the primary's statement will reach it.
	f := newAgentFixture(t, func(c *KeyringConfig) { c.EncryptSystemTablespace = true })
	f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: false}
	f.mysql.readOnly = true
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())

	if got := f.mysql.encryptCalls.Load(); got != 0 {
		t.Errorf("encrypt calls = %d, want 0 on a replica", got)
	}
}

func TestKeyringAgent_SkipsSystemTablespaceWhenAlreadyEncrypted(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) { c.EncryptSystemTablespace = true })
	f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: true}
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())
	if got := f.mysql.encryptCalls.Load(); got != 0 {
		t.Errorf("encrypt calls = %d, want 0", got)
	}
}

func TestKeyringAgent_ComponentFailureDoesNotSkipCoverage(t *testing.T) {
	f := newAgentFixture(t, func(c *KeyringConfig) { c.EncryptSystemTablespace = true })
	f.mysql.componentErr = errors.New("component unavailable")
	f.mysql.coverage = &KeyringCoverage{SystemTablespaceEncrypted: false}
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())
	got := f.agent.Snapshot()
	if f.mysql.coverageCalls.Load() != 1 || got.Coverage == nil {
		t.Fatalf("coverage was not preserved after component failure: %+v", got)
	}
	if f.mysql.encryptCalls.Load() != 1 {
		t.Error("system tablespace fix-up was skipped after component failure")
	}
	if !strings.Contains(got.LastError, "keyring component status") {
		t.Errorf("LastError = %q", got.LastError)
	}
}

func TestKeyringAgent_CoverageFailureIsReported(t *testing.T) {
	f := newAgentFixture(t, nil)
	f.mysql.coverageErr = errors.New("coverage unavailable")
	if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.agent.tick(context.Background())
	got := f.agent.Snapshot()
	if f.mysql.componentCalls.Load() != 1 || got.Component == nil {
		t.Fatalf("component sample was not preserved: %+v", got)
	}
	if !strings.Contains(got.LastError, "encryption coverage") {
		t.Errorf("LastError = %q", got.LastError)
	}
}

// --- config ---------------------------------------------------------

func TestKeyringConfigFromEnv(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		cfg, err := keyringConfigFromEnv()
		if err != nil || cfg != nil {
			t.Fatalf("cfg=%v err=%v", cfg, err)
		}
	})

	t.Run("enabled requires a path", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "1")
		if _, err := keyringConfigFromEnv(); err == nil {
			t.Fatal("expected an error when the keyring path is missing")
		}
	})

	t.Run("sealed rendering", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "1")
		t.Setenv("BLOODRAVEN_KEYRING_FILE", "/run/mysql-keyring/keyring")
		cfg, err := keyringConfigFromEnv()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if cfg.EscrowArmed || cfg.Rotate {
			t.Errorf("sealed rendering should arm nothing: %+v", cfg)
		}
	})

	t.Run("escrow requires a token file", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "1")
		t.Setenv("BLOODRAVEN_KEYRING_FILE", "/run/mysql-keyring/keyring")
		t.Setenv("BLOODRAVEN_KEYRING_ESCROW", "1")
		if _, err := keyringConfigFromEnv(); err == nil {
			t.Fatal("expected an error when the token file is missing")
		}
	})

	t.Run("rotation without escrow is rejected", func(t *testing.T) {
		// Silently ignoring this would hide an operator bug: rotating
		// against a sealed keyring fails at the engine level anyway.
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "1")
		t.Setenv("BLOODRAVEN_KEYRING_FILE", "/run/mysql-keyring/keyring")
		t.Setenv("BLOODRAVEN_KEYRING_ROTATE", "1")
		if _, err := keyringConfigFromEnv(); err == nil {
			t.Fatal("expected an error for rotate-without-escrow")
		}
	})

	t.Run("full unsealed rendering", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "1")
		t.Setenv("BLOODRAVEN_KEYRING_FILE", "/run/mysql-keyring/keyring")
		t.Setenv("BLOODRAVEN_KEYRING_ESCROW", "1")
		t.Setenv("BLOODRAVEN_KEYRING_TOKEN_FILE", "/run/bloodraven/keyring-token/token")
		t.Setenv("BLOODRAVEN_KEYRING_ESCROW_URL", "https://bloodraven:8443/keyring/escrow")
		t.Setenv("BLOODRAVEN_KEYRING_ESCROW_CA_FILE", "/etc/mysql/tls/ca.crt")
		t.Setenv("BLOODRAVEN_KEYRING_ROTATE", "1")
		t.Setenv("BLOODRAVEN_KEYRING_ENCRYPT_SYSTEM_TABLESPACE", "1")
		t.Setenv("BLOODRAVEN_KEYRING_POLL_INTERVAL", "2s")
		cfg, err := keyringConfigFromEnv()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !cfg.EscrowArmed || !cfg.Rotate || !cfg.EncryptSystemTablespace {
			t.Errorf("cfg = %+v", cfg)
		}
		if cfg.PollInterval != 2*time.Second {
			t.Errorf("poll interval = %v", cfg.PollInterval)
		}
	})

	t.Run("boolean forms are consistent", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "True")
		t.Setenv("BLOODRAVEN_KEYRING_FILE", "/run/mysql-keyring/keyring")
		t.Setenv("BLOODRAVEN_KEYRING_ENCRYPT_SYSTEM_TABLESPACE", "TRUE")
		cfg, err := keyringConfigFromEnv()
		if err != nil || cfg == nil || !cfg.EncryptSystemTablespace {
			t.Fatalf("cfg=%+v err=%v", cfg, err)
		}
	})

	t.Run("invalid boolean is loud", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "yes")
		if _, err := keyringConfigFromEnv(); err == nil {
			t.Fatal("expected invalid boolean to fail")
		}
	})

	t.Run("bad poll interval is loud", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_KEYRING_ENABLED", "1")
		t.Setenv("BLOODRAVEN_KEYRING_FILE", "/run/mysql-keyring/keyring")
		t.Setenv("BLOODRAVEN_KEYRING_POLL_INTERVAL", "not-a-duration")
		if _, err := keyringConfigFromEnv(); err == nil {
			t.Fatal("expected a parse error")
		}
	})
}

// --- HTTP surface ---------------------------------------------------

func TestServerKeyringStatusEndpoint(t *testing.T) {
	t.Run("no agent reports disabled", func(t *testing.T) {
		srv := NewServer(&fakeMysqlForKeyring{}, ":0", discardLogger())
		rec := httptest.NewRecorder()
		srv.handleKeyringStatus(rec, httptest.NewRequest(http.MethodGet, "/keyring/status", nil))
		var got KeyringStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Enabled {
			t.Error("a sidecar without a keyring agent must report enabled=false")
		}
	})

	t.Run("agent snapshot is served", func(t *testing.T) {
		f := newAgentFixture(t, nil)
		if err := os.WriteFile(f.keyring, []byte("k"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		f.agent.tick(context.Background())

		srv := NewServer(&fakeMysqlForKeyring{}, ":0", discardLogger())
		srv.SetKeyring(f.agent)
		rec := httptest.NewRecorder()
		srv.handleKeyringStatus(rec, httptest.NewRequest(http.MethodGet, "/keyring/status", nil))

		var got KeyringStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !got.Enabled || got.Digest != digestOf([]byte("k")) {
			t.Errorf("status = %+v", got)
		}
		// The endpoint is unauthenticated, so it must never carry key
		// material — only digests.
		if strings.Contains(rec.Body.String(), base64.StdEncoding.EncodeToString([]byte("k"))) {
			t.Error("/keyring/status must not expose keyring bytes")
		}
	})
}

// fakeMysqlForKeyring satisfies mysqlQuerier for server construction.
type fakeMysqlForKeyring struct{}

func (f *fakeMysqlForKeyring) queryStatus(context.Context) (*StatusInfo, error) {
	return &StatusInfo{}, nil
}
func (f *fakeMysqlForKeyring) isConnectable(context.Context) bool       { return true }
func (f *fakeMysqlForKeyring) IsReadOnly(context.Context) (bool, error) { return false, nil }
func (f *fakeMysqlForKeyring) SetSuperReadOnly(context.Context) error   { return nil }
func (f *fakeMysqlForKeyring) ClearSuperReadOnly(context.Context) error { return nil }
