package backupcrypto

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var goldenKeyCheckFields = KeyCheckFields{
	ArchiveID:       "ar1-abcdefghijklmnopqrstuvwxyz",
	SourceUID:       "6b1d3c1e-0000-4000-8000-000000000001",
	SourceNamespace: "db",
	SourceName:      "orders",
	Mode:            "Passphrase",
	Algorithm:       "AES-256-GCM",
}

const (
	goldenKeyCheckPassphrase = "correct horse battery staple, 32+ bytes of it"
	// goldenKeyCheck was computed by this package and cross-checked with an
	// independent Python implementation (hashlib/hmac, RFC 5869 HKDF). It
	// must never change: every archive's descriptor carries a value built
	// this way.
	goldenKeyCheck = "9C6-YDIGpp3n-zj_ii0glfi-z1DW-AuQKJpt5JKiCSY"
)

func TestArchiveKeyCheckGoldenVector(t *testing.T) {
	got, err := ArchiveKeyCheck([]byte(goldenKeyCheckPassphrase), goldenKeyCheckFields)
	if err != nil {
		t.Fatalf("ArchiveKeyCheck: %v", err)
	}
	if got != goldenKeyCheck {
		t.Fatalf("keyCheck = %q, want golden %q (the format changed; every existing archive would refuse)", got, goldenKeyCheck)
	}
	if len(got) != KeyCheckEncodedLen || !ValidKeyCheckEncoding(got) {
		t.Fatalf("keyCheck %q is not a valid encoding", got)
	}
	if !VerifyArchiveKeyCheck([]byte(goldenKeyCheckPassphrase), goldenKeyCheckFields, goldenKeyCheck) {
		t.Fatal("golden keyCheck does not verify")
	}
}

func TestVerifyArchiveKeyCheck(t *testing.T) {
	pass := []byte(goldenKeyCheckPassphrase)
	mutate := func(f func(*KeyCheckFields)) KeyCheckFields {
		c := goldenKeyCheckFields
		f(&c)
		return c
	}
	tests := []struct {
		name       string
		passphrase []byte
		fields     KeyCheckFields
		keyCheck   string
		want       bool
	}{
		{"correct key", pass, goldenKeyCheckFields, goldenKeyCheck, true},
		{"trimmed-equivalent passphrase", []byte(" \t" + goldenKeyCheckPassphrase + "\r\n"), goldenKeyCheckFields, goldenKeyCheck, true},
		{"wrong key", []byte("another passphrase entirely, also long"), goldenKeyCheckFields, goldenKeyCheck, false},
		{"key differs by inner byte", []byte("correct horse battery staple, 32+ bytes of iT"), goldenKeyCheckFields, goldenKeyCheck, false},
		{"vertical tab is not trimmed", []byte(goldenKeyCheckPassphrase + "\v"), goldenKeyCheckFields, goldenKeyCheck, false},
		{"empty passphrase", nil, goldenKeyCheckFields, goldenKeyCheck, false},
		{"whitespace-only passphrase", []byte(" \n"), goldenKeyCheckFields, goldenKeyCheck, false},
		{"archiveID changed", pass, mutate(func(f *KeyCheckFields) { f.ArchiveID = "ar1-bbcdefghijklmnopqrstuvwxyz" }), goldenKeyCheck, false},
		{"source uid changed", pass, mutate(func(f *KeyCheckFields) { f.SourceUID = "other" }), goldenKeyCheck, false},
		{"namespace changed", pass, mutate(func(f *KeyCheckFields) { f.SourceNamespace = "db2" }), goldenKeyCheck, false},
		{"name changed", pass, mutate(func(f *KeyCheckFields) { f.SourceName = "orders2" }), goldenKeyCheck, false},
		{"mode changed", pass, mutate(func(f *KeyCheckFields) { f.Mode = "None" }), goldenKeyCheck, false},
		{"algorithm changed", pass, mutate(func(f *KeyCheckFields) { f.Algorithm = "" }), goldenKeyCheck, false},
		// Length prefixes keep field boundaries: moving a byte between
		// adjacent fields must change the MAC.
		{"field boundary shift", pass, mutate(func(f *KeyCheckFields) { f.SourceNamespace, f.SourceName = "dbo", "rders" }), goldenKeyCheck, false},
		{"malformed keyCheck", pass, goldenKeyCheckFields, "not-base64!", false},
		{"padded keyCheck", pass, goldenKeyCheckFields, goldenKeyCheck + "=", false},
		{"empty keyCheck", pass, goldenKeyCheckFields, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := VerifyArchiveKeyCheck(tt.passphrase, tt.fields, tt.keyCheck); got != tt.want {
				t.Fatalf("VerifyArchiveKeyCheck = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArchiveKeyCheckEmptyPassphrase(t *testing.T) {
	for _, p := range [][]byte{nil, {}, []byte("\r\n \t")} {
		if _, err := ArchiveKeyCheck(p, goldenKeyCheckFields); !errors.Is(err, ErrEmptyPassphrase) {
			t.Fatalf("ArchiveKeyCheck(%q) err = %v, want ErrEmptyPassphrase", p, err)
		}
	}
}

func TestTrimPassphrase(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"secret", "secret"},
		{"secret\n", "secret"},
		{"secret\r\n", "secret"},
		{" \tsecret \t\r\n", "secret"},
		{"in ner", "in ner"},
		{"\n\n", ""},
		{"", ""},
		// Only the four ASCII characters are trimmed (F16): TrimSpace
		// would also strip these, and two trim rules would disagree on
		// which bytes are the key.
		{"secret\v", "secret\v"},
		{"\fsecret", "\fsecret"},
		{"secret\u0085", "secret\u0085"},
		{"secret ", "secret "},
	}
	for _, tt := range tests {
		if got := string(TrimPassphrase([]byte(tt.in))); got != tt.want {
			t.Errorf("TrimPassphrase(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestReadPassphraseFileUsesTrimPassphrase pins parity: the file reader and
// the exported trim agree byte for byte (D-7).
func TestReadPassphraseFileUsesTrimPassphrase(t *testing.T) {
	dir := t.TempDir()
	for i, raw := range []string{"pass\n", " \tpass\r\n", "pass\v\n", "pa ss", "pass "} {
		p := filepath.Join(dir, "p"+string(rune('a'+i)))
		if err := os.WriteFile(p, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := ReadPassphraseFile(p)
		if err != nil {
			t.Fatalf("ReadPassphraseFile(%q): %v", raw, err)
		}
		if want := TrimPassphrase([]byte(raw)); string(got) != string(want) {
			t.Fatalf("ReadPassphraseFile(%q) = %q, TrimPassphrase = %q", raw, got, want)
		}
	}
	p := filepath.Join(dir, "blank")
	if err := os.WriteFile(p, []byte(" \r\n\t"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPassphraseFile(p); err == nil {
		t.Fatal("ReadPassphraseFile accepted a whitespace-only passphrase")
	}
}
