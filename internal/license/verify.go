package license

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"
	"time"
)

const (
	// Issuer is the only accepted iss value.
	Issuer = "https://license.shipstream.io/bloodraven"

	// AlgEdDSA is the only accepted JWS alg. Checked before signature verify.
	AlgEdDSA = "EdDSA"

	EditionProduction   = "production"
	EditionOrganization = "organization"
	EditionCommunity    = "community"
	MaxTokenBytes       = 8192
	maxHeaderBytes      = 1024
	maxPayloadBytes     = 4096
	ClockLeeway         = 7 * 24 * time.Hour
	ReasonMalformed     = "malformed"
	ReasonAlgorithm     = "algorithm"
	ReasonUnknownKID    = "unknown kid"
	ReasonBadSignature  = "bad signature"
	ReasonIssuer        = "issuer"
	ReasonClaims        = "claims"
	ReasonIssuedAt      = "iat"
	ReasonNotBefore     = "nbf"
)

// KeyLookup returns the public key for kid. A missing or malformed key
// is reported as unknown kid; callers must not panic.
type KeyLookup func(kid string) (ed25519.PublicKey, bool)

// Source identifies where the token string came from.
type Source string

const (
	SourceNone     Source = "none"
	SourceOperator Source = "operator"
	SourceMFG      Source = "mfg"
)

// Result is the offline observation of a license token. It is never an
// error that should fail reconciliation.
type Result struct {
	Valid          bool
	UpdatesExpired bool
	Community      bool
	Reason         string
	Organization   string
	Edition        string
	Subject        string
	IssuedFor      string
	Kid            string
	IssuedAt       time.Time
	UpdatesUntil   time.Time
	Source         Source
}

// Claims is the signed payload. exp is deliberately absent.
type Claims struct {
	Iss          string `json:"iss"`
	Sub          string `json:"sub"`
	Org          string `json:"org"`
	Edition      string `json:"edition"`
	IssuedFor    string `json:"issuedFor"`
	Iat          int64  `json:"iat"`
	Nbf          int64  `json:"nbf,omitempty"`
	UpdatesUntil int64  `json:"updatesUntil"`
}

func invalid(reason string) Result {
	return invalidKid(reason, "")
}

func invalidKid(reason, kid string) Result {
	return Result{
		Valid:     false,
		Community: true,
		Edition:   EditionCommunity,
		Reason:    reason,
		Kid:       kid,
	}
}

// Verify checks a compact JWS offline. An empty token is not handled here;
// call Resolve. This function never panics on attacker-controlled input.
func Verify(token string, keys KeyLookup, now time.Time) Result {
	if token == "" || len(token) > MaxTokenBytes {
		return invalid(ReasonMalformed)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return invalid(ReasonMalformed)
	}

	headerJSON, err := decodeSegment(parts[0], maxHeaderBytes)
	if err != nil {
		return invalid(ReasonMalformed)
	}
	var rawHdr map[string]any
	if err := json.Unmarshal(headerJSON, &rawHdr); err != nil {
		return invalid(ReasonMalformed)
	}
	alg, _ := rawHdr["alg"].(string)
	if alg != AlgEdDSA {
		kid, _ := rawHdr["kid"].(string)
		return invalidKid(ReasonAlgorithm, kid)
	}
	kid, _ := rawHdr["kid"].(string)
	if kid == "" {
		return invalid(ReasonUnknownKID)
	}
	if typVal, present := rawHdr["typ"]; present {
		typ, ok := typVal.(string)
		if !ok || typ != "JWT" {
			return invalidKid(ReasonMalformed, kid)
		}
	}

	if keys == nil {
		return invalidKid(ReasonUnknownKID, kid)
	}
	pub, ok := keys(kid)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return invalidKid(ReasonUnknownKID, kid)
	}

	sig, err := decodeSegment(parts[2], ed25519.SignatureSize)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return invalidKid(ReasonBadSignature, kid)
	}
	signingInput := parts[0] + "." + parts[1]
	if !ed25519.Verify(pub, []byte(signingInput), sig) {
		return invalidKid(ReasonBadSignature, kid)
	}

	payloadJSON, err := decodeSegment(parts[1], maxPayloadBytes)
	if err != nil {
		return invalid(ReasonMalformed)
	}
	var raw map[string]any
	if err := json.Unmarshal(payloadJSON, &raw); err != nil {
		return invalid(ReasonMalformed)
	}

	iss, ok := stringClaim(raw, "iss")
	if !ok || iss != Issuer {
		return invalidKid(ReasonIssuer, kid)
	}
	sub, ok := stringClaim(raw, "sub")
	if !ok || sub == "" {
		return invalidKid(ReasonClaims, kid)
	}
	org, ok := stringClaim(raw, "org")
	if !ok || org == "" {
		return invalidKid(ReasonClaims, kid)
	}
	edition, ok := stringClaim(raw, "edition")
	if !ok || (edition != EditionProduction && edition != EditionOrganization) {
		return invalidKid(ReasonClaims, kid)
	}
	issuedFor, ok := stringClaim(raw, "issuedFor")
	if !ok || issuedFor == "" {
		return invalidKid(ReasonClaims, kid)
	}
	iat, ok := unixClaim(raw, "iat", true)
	if !ok {
		return invalidKid(ReasonClaims, kid)
	}
	nbf, nbfOK := unixClaim(raw, "nbf", false)
	if !nbfOK {
		return invalidKid(ReasonClaims, kid)
	}
	updatesUntil, ok := unixClaim(raw, "updatesUntil", true)
	if !ok {
		return invalidKid(ReasonClaims, kid)
	}

	// exp is ignored if present. A skewed or buggy signer must not turn
	// a perpetual license into an invalid one.
	_ = raw["exp"]

	if now.IsZero() {
		now = time.Now()
	}
	leewayEnd := now.Add(ClockLeeway).Unix()
	if iat > leewayEnd {
		return invalidKid(ReasonIssuedAt, kid)
	}
	if _, hasNBF := raw["nbf"]; hasNBF && nbf > leewayEnd {
		return invalidKid(ReasonNotBefore, kid)
	}

	out := Result{
		Valid:        true,
		Organization: org,
		Edition:      edition,
		Subject:      sub,
		IssuedFor:    issuedFor,
		Kid:          kid,
		IssuedAt:     time.Unix(iat, 0).UTC(),
		UpdatesUntil: time.Unix(updatesUntil, 0).UTC(),
	}
	if updatesUntil < now.Unix() {
		out.UpdatesExpired = true
	}
	return out
}

func decodeSegment(seg string, max int) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, errSegmentTooLarge
	}
	return b, nil
}

type segmentError string

func (e segmentError) Error() string { return string(e) }

const errSegmentTooLarge segmentError = "segment too large"

func stringClaim(raw map[string]any, key string) (string, bool) {
	v, ok := raw[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func unixClaim(raw map[string]any, key string, required bool) (int64, bool) {
	v, ok := raw[key]
	if !ok {
		return 0, !required
	}
	switch n := v.(type) {
	case float64:
		if n != math.Trunc(n) || n < 0 || n >= float64(math.MaxInt64) {
			return 0, false
		}
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil || i < 0 {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}
