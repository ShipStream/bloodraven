package controller

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

type escrowHarness struct {
	handler http.Handler
	client  client.Client
	token   string
	fg      *v1alpha1.MysqlFailoverGroup
}

func newEscrowHarness(t *testing.T) *escrowHarness {
	t.Helper()
	fg := encTestFG()
	r, c := newReconciler(fg)
	if err := r.ensureEscrowToken(context.Background(), fg, "dc1"); err != nil {
		t.Fatalf("mint token: %v", err)
	}
	var sec corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: fg.Namespace, Name: v1alpha1.KeyringTokenSecretName(fg.Name, "dc1"),
	}, &sec); err != nil {
		t.Fatalf("read token: %v", err)
	}
	return &escrowHarness{
		handler: NewKeyringEscrowHandler(c, slog.New(slog.NewTextHandler(io.Discard, nil))),
		client:  c,
		token:   string(sec.Data[v1alpha1.KeyringTokenKey]),
		fg:      fg,
	}
}

func (h *escrowHarness) post(t *testing.T, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	switch v := body.(type) {
	case string:
		raw = []byte(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		raw = b
	}
	req := httptest.NewRequest(http.MethodPost, "/keyring/escrow", bytes.NewReader(raw))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func (h *escrowHarness) req(keyring []byte, digest string) KeyringEscrowRequest {
	return KeyringEscrowRequest{
		Namespace: h.fg.Namespace,
		Group:     h.fg.Name,
		Site:      "dc1",
		Digest:    digest,
		Keyring:   base64.StdEncoding.EncodeToString(keyring),
	}
}

func TestEscrowHandler_HappyPath(t *testing.T) {
	h := newEscrowHarness(t)
	raw := []byte("live-keyring-bytes")

	rec := h.post(t, h.token, h.req(raw, keyringDigest(raw)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var resp KeyringEscrowResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Version != 1 || resp.Secret != "mysql-lion-dc1-keyring-v1" {
		t.Errorf("response = %+v", resp)
	}
	// The echoed digest is what the sidecar checks before it considers
	// the push durable, so it must be the digest of what was stored.
	if resp.Digest != keyringDigest(raw) {
		t.Errorf("echoed digest = %q", resp.Digest)
	}

	var sec corev1.Secret
	if err := h.client.Get(context.Background(), types.NamespacedName{
		Namespace: h.fg.Namespace, Name: resp.Secret,
	}, &sec); err != nil {
		t.Fatalf("secret not stored: %v", err)
	}
	if !bytes.Equal(sec.Data[v1alpha1.KeyringDataFileName], raw) {
		t.Error("stored bytes differ from what was pushed")
	}
}

func TestEscrowHandler_RejectsMissingToken(t *testing.T) {
	h := newEscrowHarness(t)
	rec := h.post(t, "", h.req([]byte("k"), ""))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestEscrowHandler_RejectsWrongToken(t *testing.T) {
	h := newEscrowHarness(t)
	rec := h.post(t, h.token+"tampered", h.req([]byte("k"), ""))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	// The error must not disclose whether the site or token exists.
	if strings.Contains(rec.Body.String(), "no escrow token issued") {
		t.Errorf("response leaks internal detail: %s", rec.Body)
	}
}

func TestEscrowHandler_RejectsCrossSiteToken(t *testing.T) {
	// dc1's token must not be usable to overwrite dc2's keyring — that
	// would let a compromised replica strand the primary.
	h := newEscrowHarness(t)
	req := h.req([]byte("k"), "")
	req.Site = "dc2"
	rec := h.post(t, h.token, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestEscrowHandler_RejectsDigestMismatch(t *testing.T) {
	// A corrupted upload must never be recorded as authoritative: the
	// operator would then seal a site against a keyring MySQL cannot use.
	h := newEscrowHarness(t)
	rec := h.post(t, h.token, h.req([]byte("actual"), keyringDigest([]byte("claimed"))))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	store, _ := newEscrowStore()
	store.client = h.client
	if _, ok, _ := store.current(context.Background(), h.fg, "dc1"); ok {
		t.Error("a mismatched push must not be stored")
	}
}

func TestEscrowHandler_RejectsMissingDigest(t *testing.T) {
	h := newEscrowHarness(t)
	rec := h.post(t, h.token, h.req([]byte("actual"), ""))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestEscrowHandler_RejectsEmptyKeyring(t *testing.T) {
	h := newEscrowHarness(t)
	req := h.req(nil, "")
	req.Keyring = base64.StdEncoding.EncodeToString([]byte{})
	rec := h.post(t, h.token, req)
	// An empty Keyring field fails the required-fields check before the
	// decode; either way it must not be stored.
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body)
	}
}

func TestEscrowHandler_RejectsOversizedPayload(t *testing.T) {
	h := newEscrowHarness(t)
	huge := bytes.Repeat([]byte("x"), maxKeyringBytes+1024)
	rec := h.post(t, h.token, h.req(huge, ""))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestEscrowHandler_RejectsNonPOST(t *testing.T) {
	h := newEscrowHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/keyring/escrow", nil)
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestEscrowHandler_RejectsMalformedJSON(t *testing.T) {
	h := newEscrowHarness(t)
	rec := h.post(t, h.token, "{not json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestEscrowHandler_RejectsMissingFields(t *testing.T) {
	h := newEscrowHarness(t)
	raw := []byte("k")
	req := h.req(raw, keyringDigest(raw))
	req.Site = ""
	rec := h.post(t, h.token, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestEscrowHandler_RejectsUnknownSite(t *testing.T) {
	h := newEscrowHarness(t)
	// Mint a token for a site name that is not in spec.sites so the
	// request gets past auth and reaches the membership check.
	ctx := context.Background()
	r := &MysqlFailoverGroupReconciler{Client: h.client, Scheme: h.client.Scheme()}
	if err := r.ensureEscrowToken(ctx, h.fg, "ghost"); err != nil {
		t.Fatalf("mint: %v", err)
	}
	var sec corev1.Secret
	if err := h.client.Get(ctx, types.NamespacedName{
		Namespace: h.fg.Namespace, Name: v1alpha1.KeyringTokenSecretName(h.fg.Name, "ghost"),
	}, &sec); err != nil {
		t.Fatalf("get ghost token: %v", err)
	}

	raw := []byte("k")
	req := h.req(raw, keyringDigest(raw))
	req.Site = "ghost"
	rec := h.post(t, string(sec.Data[v1alpha1.KeyringTokenKey]), req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %s)", rec.Code, rec.Body)
	}
}

func TestEscrowHandler_RejectsWhenEncryptionDisabled(t *testing.T) {
	fg := newTestFG() // no spec.encryptionAtRest
	r, c := newReconciler(fg)
	ctx := context.Background()
	if err := r.ensureEscrowToken(ctx, fg, "dc1"); err != nil {
		t.Fatalf("mint: %v", err)
	}
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: fg.Namespace, Name: v1alpha1.KeyringTokenSecretName(fg.Name, "dc1"),
	}, &sec); err != nil {
		t.Fatalf("get token: %v", err)
	}

	h := &escrowHarness{
		handler: NewKeyringEscrowHandler(c, slog.New(slog.NewTextHandler(io.Discard, nil))),
		client:  c,
		token:   string(sec.Data[v1alpha1.KeyringTokenKey]),
		fg:      fg,
	}
	raw := []byte("k")
	rec := h.post(t, h.token, h.req(raw, keyringDigest(raw)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

func TestEscrowHandler_RetryIsIdempotent(t *testing.T) {
	h := newEscrowHarness(t)
	raw := []byte("same-bytes")
	first := h.post(t, h.token, h.req(raw, keyringDigest(raw)))
	second := h.post(t, h.token, h.req(raw, keyringDigest(raw)))
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("codes %d / %d", first.Code, second.Code)
	}
	var a, b KeyringEscrowResponse
	_ = json.Unmarshal(first.Body.Bytes(), &a)
	_ = json.Unmarshal(second.Body.Bytes(), &b)
	if a.Version != b.Version {
		t.Errorf("retry created version %d after %d", b.Version, a.Version)
	}
}
