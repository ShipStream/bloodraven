package pitrarchive

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

const goldenUID types.UID = "6b1d3c1e-0000-4000-8000-000000000001"

// goldenArchiveIDsV1 pins derivation v1 for each golden destination, in the
// order of goldenDestinations. Cross-checked with Python (hashlib.sha256,
// base64.b32encode). Never change these; add a derivation instead.
var goldenArchiveIDsV1 = []string{
	"ar1-ls5zbywzgarztmrxkprjhhnopk",
	"ar1-dn4tbv6lsnaysaaunvdfbuu7fm",
	"ar1-qs5h75hi62icu5sakrst4pv4fl",
	"ar1-bvcbct46elyh34l5g6kopdrrsk",
	"ar1-vxee5y3gb36wdt6m5kjxp4o5w2",
}

func TestArchiveIDV1Golden(t *testing.T) {
	if len(goldenArchiveIDsV1) != len(goldenDestinations) {
		t.Fatal("golden tables out of step")
	}
	for i, g := range goldenDestinations {
		d, err := g.dest()
		if err != nil {
			t.Fatal(err)
		}
		got := archiveIDV1(goldenUID, d)
		if got != goldenArchiveIDsV1[i] {
			t.Errorf("%s: archiveIDV1 = %q, want golden %q", g.name, got, goldenArchiveIDsV1[i])
		}
		if !ValidArchiveID(got) {
			t.Errorf("%s: %q does not match %s", g.name, got, ArchiveIDPattern)
		}
		if CreateArchiveID(goldenUID, d) != got {
			t.Errorf("%s: CreateArchiveID does not use derivation 0", g.name)
		}
	}
}

func TestArchiveIDDistinguishesInputs(t *testing.T) {
	a, _ := NewS3Destination("b", "x", "")
	b, _ := NewS3Destination("b", "y", "")
	ids := map[string]string{
		"a/uid1": CreateArchiveID("uid1", a),
		"b/uid1": CreateArchiveID("uid1", b),
		"a/uid2": CreateArchiveID("uid2", a),
	}
	seen := map[string]string{}
	for k, id := range ids {
		if prev, dup := seen[id]; dup {
			t.Fatalf("%s and %s share archive ID %s", prev, k, id)
		}
		seen[id] = k
	}
}

func TestValidArchiveID(t *testing.T) {
	for id, want := range map[string]bool{
		"ar1-abcdefghijklmnopqrstuvwxyz":  true,
		"ar1-234567abcdefghijklmnopqrst":  true,
		"ar1-abcdefghijklmnopqrstuvwxy":   false, // 25
		"ar1-abcdefghijklmnopqrstuvwxyza": false, // 27
		"ar1-ABCDEFGHIJKLMNOPQRSTUVWXYZ":  false,
		"ar1-abcdefghijklmnopqrstuvwxy1":  false, // 1 is not base32
		"ar2-abcdefghijklmnopqrstuvwxyz":  false,
		"":                                false,
	} {
		if got := ValidArchiveID(id); got != want {
			t.Errorf("ValidArchiveID(%q) = %v, want %v", id, got, want)
		}
	}
}

// TestIsDerivedArchiveIDAcceptsEverySupportedDerivation pins D-3: a writer
// accepts an ID from any listed derivation, and nothing else.
func TestIsDerivedArchiveIDAcceptsEverySupportedDerivation(t *testing.T) {
	d, _ := NewS3Destination("b", "x", "")
	v1 := CreateArchiveID(goldenUID, d)
	future := "ar1-zzzzzzzzzzzzzzzzzzzzzzzzzz"
	if !IsDerivedArchiveID(v1, goldenUID, d) {
		t.Fatal("v1 ID not accepted")
	}
	if IsDerivedArchiveID(future, goldenUID, d) {
		t.Fatal("an ID from an unsupported derivation was accepted")
	}
	if IsDerivedArchiveID(v1, "other", d) {
		t.Fatal("an ID for another UID was accepted")
	}
	if IsDerivedArchiveID(v1, goldenUID, Destination{}) || IsDerivedArchiveID(v1, "", d) {
		t.Fatal("an empty UID or destination was accepted")
	}

	saved := ArchiveIDDerivations
	t.Cleanup(func() { ArchiveIDDerivations = saved })
	ArchiveIDDerivations = []func(types.UID, Destination) string{
		func(types.UID, Destination) string { return future },
		archiveIDV1,
	}
	if !IsDerivedArchiveID(future, goldenUID, d) || !IsDerivedArchiveID(v1, goldenUID, d) {
		t.Fatal("with two derivations, both IDs must be accepted")
	}
	if CreateArchiveID(goldenUID, d) != future {
		t.Fatal("CreateArchiveID must use derivation 0")
	}
}
