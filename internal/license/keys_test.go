package license

import (
	"crypto/ed25519"
	"testing"
)

func TestProductionKeyEmptyStore(t *testing.T) {
	pub, ok := ProductionKey("br-1")
	if ok || pub != nil {
		t.Fatalf("empty store must miss: ok=%v pub=%v", ok, pub)
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
