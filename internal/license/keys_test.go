package license

import (
	"crypto/ed25519"
	"testing"
)

func TestProductionKeyUnknownKidMisses(t *testing.T) {
	// A kid that must never be issued. An unknown kid misses rather than
	// falling back to any other entry in the store.
	pub, ok := ProductionKey("no-such-kid")
	if ok || pub != nil {
		t.Fatalf("unknown kid must miss: ok=%v pub=%v", ok, pub)
	}
	if _, ok := ProductionKey(""); ok {
		t.Fatal("empty kid must miss")
	}
}

// TestProductionKeyEntriesAreWellFormed guards the paste. Every entry must
// decode to a full-size Ed25519 public key, so a truncated or mistyped hex
// string fails here rather than silently rejecting every real license in
// the field as unknown kid.
func TestProductionKeyEntriesAreWellFormed(t *testing.T) {
	for kid := range productionKeyHex {
		pub, ok := ProductionKey(kid)
		if !ok {
			t.Errorf("kid %q is in the store but does not resolve", kid)
			continue
		}
		if len(pub) != ed25519.PublicKeySize {
			t.Errorf("kid %q: got %d bytes, want %d", kid, len(pub), ed25519.PublicKeySize)
		}
	}
}

func TestProductionKeyReturnsCopy(t *testing.T) {
	for kid := range productionKeyHex {
		first, ok := ProductionKey(kid)
		if !ok {
			continue
		}
		first[0] ^= 0xff
		again, _ := ProductionKey(kid)
		if again[0] == first[0] {
			t.Fatalf("kid %q: lookup must return a copy", kid)
		}
		break
	}
}

func TestStaticKeysRejectsWrongLength(t *testing.T) {
	lookup := StaticKeys(map[string]ed25519.PublicKey{
		"short": ed25519.PublicKey{1, 2, 3},
	})
	if _, ok := lookup("short"); ok {
		t.Fatal("short key must not verify")
	}
	if _, ok := lookup("missing"); ok {
		t.Fatal("missing kid")
	}
}

func TestStaticKeysReturnsCopy(t *testing.T) {
	raw := make(ed25519.PublicKey, ed25519.PublicKeySize)
	raw[0] = 7
	lookup := StaticKeys(map[string]ed25519.PublicKey{"k": raw})
	got, ok := lookup("k")
	if !ok {
		t.Fatal("expected key")
	}
	got[0] = 9
	again, _ := lookup("k")
	if again[0] != 7 {
		t.Fatal("lookup must return a copy")
	}
}
