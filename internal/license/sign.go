package license

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Encode builds a compact EdDSA JWS. The operator never mints licenses;
// this exists so tests (and the out-of-repo signer, by reading the
// contract) share one encoding. Do not call this from reconcile.
func Encode(priv ed25519.PrivateKey, kid string, claims map[string]any) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("license: invalid private key size")
	}
	if kid == "" {
		return "", fmt.Errorf("license: kid is required")
	}
	header := map[string]any{
		"alg": AlgEdDSA,
		"kid": kid,
		"typ": "JWT",
	}
	hb, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	pb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	h64 := base64.RawURLEncoding.EncodeToString(hb)
	p64 := base64.RawURLEncoding.EncodeToString(pb)
	sig := ed25519.Sign(priv, []byte(h64+"."+p64))
	return h64 + "." + p64 + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
