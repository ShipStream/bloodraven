package license

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// testOnlyPublicKeyHex is the committed test-only Ed25519 public key
// documented in site/content/docs/2.license-token.md. The matching
// private key is not in this repository. ProductionKey does not
// contain this kid.
const testOnlyPublicKeyHex = "4cb5abf6ad79fbf5abbccafcc269d85cd2651ed4b885b5869f241aedf0a5ba29"

// docsExampleToken is the compact JWS printed on the signer-contract page.
const docsExampleToken = "eyJhbGciOiJFZERTQSIsImtpZCI6InRlc3Qtb25seS0xIiwidHlwIjoiSldUIn0.eyJlZGl0aW9uIjoib3JnYW5pemF0aW9uIiwiaWF0IjoxNzU1MjE2MDAwLCJpc3MiOiJodHRwczovL2xpY2Vuc2Uuc2hpcHN0cmVhbS5pby9ibG9vZHJhdmVuIiwiaXNzdWVkRm9yIjoib3JkX2V4YW1wbGUiLCJvcmciOiJBY21lIENvcnAiLCJzdWIiOiJjdXNfZXhhbXBsZSIsInVwZGF0ZXNVbnRpbCI6MTc4Njc1MjAwMH0.MlHGkwxyk325K5RWI_rIYCLkFBzmRWD6jTa2OQ2zp9rHgjW6Fy1gT_V93T2WHzQebLnJhkH8eKDe-YwQyLFkDg"

func testAuthority(t *testing.T) (ed25519.PrivateKey, string, KeyLookup) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	kid := "test-only-1"
	return priv, kid, StaticKeys(map[string]ed25519.PublicKey{kid: pub})
}

func validClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss":          Issuer,
		"sub":          "cus_test",
		"org":          "Acme Corp",
		"edition":      EditionOrganization,
		"issuedFor":    "ord_test",
		"iat":          now.Unix(),
		"updatesUntil": now.Add(365 * 24 * time.Hour).Unix(),
	}
}

func mustEncode(t *testing.T, priv ed25519.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	tok, err := Encode(priv, kid, claims)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return tok
}

func TestVerifyTable(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	priv, kid, keys := testAuthority(t)
	otherPub, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("other key: %v", err)
	}
	bothKeys := StaticKeys(map[string]ed25519.PublicKey{
		kid:         priv.Public().(ed25519.PublicKey),
		"other-kid": otherPub,
	})

	type tc struct {
		name  string
		token func() string
		keys  KeyLookup
		now   time.Time
		want  Result
		check func(*testing.T, Result)
	}

	cases := []tc{
		{
			name:  "valid organization",
			token: func() string { return mustEncode(t, priv, kid, validClaims(now)) },
			keys:  keys,
			now:   now,
			check: func(t *testing.T, r Result) {
				if !r.Valid || r.UpdatesExpired || r.Community {
					t.Fatalf("valid org: %+v", r)
				}
				if r.Organization != "Acme Corp" || r.Edition != EditionOrganization {
					t.Fatalf("claims: %+v", r)
				}
				if r.IssuedFor != "ord_test" || r.Kid != kid {
					t.Fatalf("ids: %+v", r)
				}
			},
		},
		{
			name: "valid production",
			token: func() string {
				c := validClaims(now)
				c["edition"] = EditionProduction
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid || r.Edition != EditionProduction {
					t.Fatalf("got %+v", r)
				}
			},
		},
		{
			name: "expired updates period is still valid",
			token: func() string {
				c := validClaims(now)
				c["updatesUntil"] = now.Add(-24 * time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid || !r.UpdatesExpired {
					t.Fatalf("expired updates must stay valid: %+v", r)
				}
			},
		},
		{
			name: "updatesUntil equal now is not expired",
			token: func() string {
				c := validClaims(now)
				c["updatesUntil"] = now.Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid || r.UpdatesExpired {
					t.Fatalf("boundary: %+v", r)
				}
			},
		},
		{
			name: "bad signature",
			token: func() string {
				tok := mustEncode(t, priv, kid, validClaims(now))
				parts := strings.Split(tok, ".")
				sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
				sig[0] ^= 0xff
				parts[2] = base64.RawURLEncoding.EncodeToString(sig)
				return strings.Join(parts, ".")
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonBadSignature),
		},
		{
			name: "alg none",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "none", "kid": kid}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonAlgorithm),
		},
		{
			name: "alg HS256",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "HS256", "kid": kid, "typ": "JWT"}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonAlgorithm),
		},
		{
			name: "alg ES256",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "ES256", "kid": kid, "typ": "JWT"}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonAlgorithm),
		},
		{
			name: "alg Ed25519 is not EdDSA",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "Ed25519", "kid": kid, "typ": "JWT"}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonAlgorithm),
		},
		{
			name:  "unknown kid",
			token: func() string { return mustEncode(t, priv, "no-such-kid", validClaims(now)) },
			keys:  keys,
			now:   now,
			want:  invalid(ReasonUnknownKID),
		},
		{
			name: "empty kid",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "EdDSA", "kid": ""}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonUnknownKID),
		},
		{
			name: "malformed base64 payload",
			token: func() string {
				header := map[string]any{"alg": AlgEdDSA, "kid": kid, "typ": "JWT"}
				hb, err := json.Marshal(header)
				if err != nil {
					t.Fatalf("header: %v", err)
				}
				h64 := base64.RawURLEncoding.EncodeToString(hb)
				payload := "%%%not-base64%%%"
				sig := ed25519.Sign(priv, []byte(h64+"."+payload))
				return h64 + "." + payload + "." + base64.RawURLEncoding.EncodeToString(sig)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonMalformed),
		},
		{
			name:  "not three parts",
			token: func() string { return "only.two" },
			keys:  keys,
			now:   now,
			want:  invalid(ReasonMalformed),
		},
		{
			name:  "empty token",
			token: func() string { return "" },
			keys:  keys,
			now:   now,
			want:  invalid(ReasonMalformed),
		},
		{
			name:  "oversized token",
			token: func() string { return strings.Repeat("a", MaxTokenBytes+1) },
			keys:  keys,
			now:   now,
			want:  invalid(ReasonMalformed),
		},
		{
			name: "wrong issuer",
			token: func() string {
				c := validClaims(now)
				c["iss"] = "https://evil.example"
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonIssuer),
		},
		{
			name: "iat within future leeway",
			token: func() string {
				c := validClaims(now)
				c["iat"] = now.Add(24 * time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid {
					t.Fatalf("future iat within leeway: %+v", r)
				}
			},
		},
		{
			name: "iat in the past (clock ahead)",
			token: func() string {
				c := validClaims(now)
				c["iat"] = now.Add(-30 * 24 * time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid {
					t.Fatalf("past iat: %+v", r)
				}
			},
		},
		{
			name: "iat beyond future leeway",
			token: func() string {
				c := validClaims(now)
				c["iat"] = now.Add(ClockLeeway + time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonIssuedAt),
		},
		{
			name: "nbf within future leeway",
			token: func() string {
				c := validClaims(now)
				c["nbf"] = now.Add(24 * time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid {
					t.Fatalf("nbf within leeway: %+v", r)
				}
			},
		},
		{
			name: "nbf beyond future leeway",
			token: func() string {
				c := validClaims(now)
				c["nbf"] = now.Add(ClockLeeway + time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonNotBefore),
		},
		{
			name: "past exp is ignored",
			token: func() string {
				c := validClaims(now)
				c["exp"] = now.Add(-365 * 24 * time.Hour).Unix()
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid {
					t.Fatalf("past exp must be ignored: %+v", r)
				}
			},
		},
		{
			name: "non-numeric exp is ignored",
			token: func() string {
				c := validClaims(now)
				c["exp"] = "not-a-number"
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			check: func(t *testing.T, r Result) {
				if !r.Valid {
					t.Fatalf("junk exp must be ignored: %+v", r)
				}
			},
		},
		{
			name: "wrong typ",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "EdDSA", "kid": kid, "typ": "at+jwt"}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonMalformed),
		},
		{
			name: "typ null is present and rejected",
			token: func() string {
				return unsignedToken(t, map[string]any{"alg": "EdDSA", "kid": kid, "typ": nil}, validClaims(now))
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonMalformed),
		},
		{
			name:  "unknown kid retains kid",
			token: func() string { return mustEncode(t, priv, "no-such-kid", validClaims(now)) },
			keys:  keys,
			now:   now,
			check: func(t *testing.T, r Result) {
				if r.Valid || r.Reason != ReasonUnknownKID || r.Kid != "no-such-kid" {
					t.Fatalf("want unknown kid retained, got %+v", r)
				}
			},
		},
		{
			name:  "signed with other known kid",
			token: func() string { return mustEncode(t, otherPriv, kid, validClaims(now)) },
			keys:  bothKeys,
			now:   now,
			want:  invalid(ReasonBadSignature),
		},
		{
			name: "missing org",
			token: func() string {
				c := validClaims(now)
				delete(c, "org")
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonClaims),
		},
		{
			name: "empty issuedFor",
			token: func() string {
				c := validClaims(now)
				c["issuedFor"] = ""
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonClaims),
		},
		{
			name: "community edition in token is not a paid edition",
			token: func() string {
				c := validClaims(now)
				c["edition"] = EditionCommunity
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonClaims),
		},
		{
			name: "fractional updatesUntil",
			token: func() string {
				c := validClaims(now)
				c["updatesUntil"] = float64(now.Unix()) + 0.5
				return mustEncode(t, priv, kid, c)
			},
			keys: keys,
			now:  now,
			want: invalid(ReasonClaims),
		},
		{
			name:  "nil keys",
			token: func() string { return mustEncode(t, priv, kid, validClaims(now)) },
			keys:  nil,
			now:   now,
			want:  invalid(ReasonUnknownKID),
		},
		{
			name:  "empty trust store",
			token: func() string { return mustEncode(t, priv, kid, validClaims(now)) },
			keys:  ProductionKey,
			now:   now,
			want:  invalid(ReasonUnknownKID),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Verify(tc.token(), tc.keys, tc.now)
			if tc.check != nil {
				tc.check(t, got)
				return
			}
			if got.Valid != tc.want.Valid || got.Reason != tc.want.Reason || got.Edition != tc.want.Edition {
				t.Fatalf("got valid=%v reason=%q edition=%q, want valid=%v reason=%q edition=%q",
					got.Valid, got.Reason, got.Edition, tc.want.Valid, tc.want.Reason, tc.want.Edition)
			}
			if got.Valid {
				t.Fatal("invalid cases must not be valid")
			}
		})
	}
}

func unsignedToken(t *testing.T, header, claims map[string]any) string {
	t.Helper()
	hb, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("header: %v", err)
	}
	pb, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("claims: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(hb) + "." +
		base64.RawURLEncoding.EncodeToString(pb) + "." +
		base64.RawURLEncoding.EncodeToString([]byte("not-a-real-signature"))
}

func TestTestOnlyPublicKeyHexIsNotProduction(t *testing.T) {
	if _, ok := ProductionKey("test-only-1"); ok {
		t.Fatal("test public key must not be in the production store")
	}
	if testOnlyPublicKeyHex == "" || len(testOnlyPublicKeyHex) != 64 {
		t.Fatalf("test-only public key hex should be 32 bytes / 64 hex chars")
	}
}

func TestDocsExampleTokenVerifiesAgainstCommittedTestKey(t *testing.T) {
	raw, err := hex.DecodeString(testOnlyPublicKeyHex)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		t.Fatalf("committed test public key: %v len=%d", err, len(raw))
	}
	keys := StaticKeys(map[string]ed25519.PublicKey{"test-only-1": raw})
	now := time.Unix(1755216000, 0).UTC()
	got := Verify(docsExampleToken, keys, now)
	if !got.Valid || got.Organization != "Acme Corp" || got.Edition != EditionOrganization {
		t.Fatalf("docs example must verify: %+v", got)
	}
	if got.UpdatesUntil.Unix() != 1786752000 {
		t.Fatalf("updatesUntil = %d", got.UpdatesUntil.Unix())
	}
}
