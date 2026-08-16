package license

import (
	"crypto/ed25519"
	"encoding/hex"
)

// productionKeyHex is the append-only trust store: kid → raw Ed25519
// public key as 64 lowercase hex characters (32 bytes).
//
// The repo owner fills this in. An empty map is the correct shipping
// state until a production public key exists. A build with no entries
// compiles and treats every presented token as unknown kid (community
// behavior, valid=false). It must not panic.
//
// Keys are append-only. An old operator binary can never learn a new
// key, so retired kids stay here forever so already-issued tokens keep
// verifying.
//
// To insert a production key (do this on a machine that will hold the
// private key; never commit the private key):
//
//	openssl genpkey -algorithm ED25519 -out license-ed25519.pem
//	openssl pkey -in license-ed25519.pem -pubout -outform DER | tail -c 32 | xxd -p -c 32
//
// Then add `"br-YYYY-N": "<64 hex chars>"` below. The matching private
// key stays in the signer service, not this repository.
var productionKeyHex = map[string]string{
	// Generated 2026-08-15. Private key is held only by the signer
	// service; sha256 of this public key begins 2a94018110e06c67.
	"br-1": "1b3bea77364fff24dad67d2727c3861c1a93dd3f22faccfac4b0fdecd69f6f02",
}

// ProductionKey looks up a production public key by kid. Unknown kids
// and malformed hex entries return false. The returned key is a copy.
func ProductionKey(kid string) (ed25519.PublicKey, bool) {
	hexStr, ok := productionKeyHex[kid]
	if !ok || hexStr == "" {
		return nil, false
	}
	raw, err := hex.DecodeString(hexStr)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, false
	}
	out := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(out, raw)
	return out, true
}

// StaticKeys returns a lookup over an in-memory map. Used by tests.
// Production code uses ProductionKey. Keys are copied on lookup.
func StaticKeys(keys map[string]ed25519.PublicKey) KeyLookup {
	return func(kid string) (ed25519.PublicKey, bool) {
		pub, ok := keys[kid]
		if !ok || len(pub) != ed25519.PublicKeySize {
			return nil, false
		}
		out := make(ed25519.PublicKey, ed25519.PublicKeySize)
		copy(out, pub)
		return out, true
	}
}
