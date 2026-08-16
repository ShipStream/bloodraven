package licenseroundtrip

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/license"
)

type nodeResult struct {
	Token        string `json:"token"`
	PublicKeyHex string `json:"publicKeyHex"`
	Kid          string `json:"kid"`
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func nodeSign(t *testing.T, seed []byte, kid string, claims map[string]any) nodeResult {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is not on PATH")
	}
	payload, err := json.Marshal(map[string]any{
		"seedB64": base64.StdEncoding.EncodeToString(seed),
		"kid":     kid,
		"claims":  claims,
	})
	if err != nil {
		t.Fatalf("marshal node input: %v", err)
	}
	script := filepath.Join(repoRoot(t), "site", "scripts", "sign-license-token.mjs")
	cmd := exec.Command("node", script)
	cmd.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("node signer failed: %v\nstderr: %s", err, stderr.String())
	}
	var out nodeResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("node signer output: %v\nstdout: %s", err, stdout.String())
	}
	if out.Token == "" || out.PublicKeyHex == "" {
		t.Fatalf("node signer returned empty fields: %+v", out)
	}
	return out
}

func TestNodeTokenVerifiesWithGoVerifier(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	kid := "test-roundtrip-1"
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	claims := map[string]any{
		"iss":          license.Issuer,
		"sub":          "cus_roundtrip",
		"org":          "Roundtrip Org",
		"edition":      license.EditionProduction,
		"issuedFor":    "2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e",
		"iat":          now.Unix(),
		"updatesUntil": now.Add(365 * 24 * time.Hour).Unix(),
	}

	got := nodeSign(t, priv.Seed(), kid, claims)
	wantPub := hex.EncodeToString(pub)
	if got.PublicKeyHex != wantPub {
		t.Fatalf("node derived pub %s, go has %s", got.PublicKeyHex, wantPub)
	}

	result := license.Verify(got.Token, license.StaticKeys(map[string]ed25519.PublicKey{kid: pub}), now)
	if !result.Valid {
		t.Fatalf("go verifier rejected node token: %+v", result)
	}
	if result.Organization != "Roundtrip Org" || result.Edition != license.EditionProduction {
		t.Fatalf("claims: %+v", result)
	}
	if result.Subject != "cus_roundtrip" || result.IssuedFor != "2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e" {
		t.Fatalf("ids: %+v", result)
	}
	if result.Kid != kid {
		t.Fatalf("kid: %q", result.Kid)
	}
	if strings.Contains(got.Token, ".") {
		payloadJSON, err := base64.RawURLEncoding.DecodeString(strings.Split(got.Token, ".")[1])
		if err != nil {
			t.Fatalf("payload: %v", err)
		}
		if bytes.Contains(payloadJSON, []byte(`"exp"`)) {
			t.Fatal("token payload must not contain exp")
		}
	}
}

func TestTamperedNodeTokenFailsSignature(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	kid := "test-roundtrip-1"
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	got := nodeSign(t, priv.Seed(), kid, map[string]any{
		"iss":          license.Issuer,
		"sub":          "cus_roundtrip",
		"org":          "Roundtrip Org",
		"edition":      license.EditionOrganization,
		"issuedFor":    "2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e",
		"iat":          now.Unix(),
		"updatesUntil": now.Add(365 * 24 * time.Hour).Unix(),
	})

	parts := strings.Split(got.Token, ".")
	if len(parts) != 3 {
		t.Fatalf("compact token: %s", got.Token)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != ed25519.SignatureSize {
		t.Fatalf("signature segment: %v len=%d", err, len(sig))
	}
	sig[0] ^= 0x01
	tampered := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(sig)

	result := license.Verify(tampered, license.StaticKeys(map[string]ed25519.PublicKey{kid: pub}), now)
	if result.Valid || result.Reason != license.ReasonBadSignature {
		t.Fatalf("tampered token must be bad signature: %+v", result)
	}
}
