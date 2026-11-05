package pitrarchive

import (
	"strings"
	"testing"
	"time"
)

var (
	testSource     = Source{Namespace: "db", Name: "orders", UID: goldenUID}
	testPassphrase = []byte("correct horse battery staple, 32+ bytes of it")
	testNow        = time.Date(2026, 11, 4, 20, 1, 2, 123456789, time.FixedZone("x", 3600))
	encNone        = EncryptionIdentity{Mode: EncryptionNone}
	encPass        = EncryptionIdentity{Mode: EncryptionPassphrase, Algorithm: AlgorithmAES256GCM}
)

func testDest(t *testing.T) Destination {
	t.Helper()
	d, err := NewS3Destination("backups", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestNewDescriptorCanonicalGolden(t *testing.T) {
	d := testDest(t)
	desc, err := NewDescriptor(testSource, d, encNone, nil, "iad", testNow)
	if err != nil {
		t.Fatal(err)
	}
	got, err := desc.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"kind":"BloodravenPITRArchive","version":1,"archiveID":"ar1-ls5zbywzgarztmrxkprjhhnopk",` +
		`"source":{"namespace":"db","name":"orders","uid":"6b1d3c1e-0000-4000-8000-000000000001"},` +
		`"encryption":{"mode":"None","algorithm":"","keyCheck":""},` +
		`"createdAt":"2026-11-04T19:01:02.123456Z","createdBy":"iad"}`
	if string(got) != want {
		t.Fatalf("canonical descriptor\n got %s\nwant %s", got, want)
	}

	pd, err := NewDescriptor(testSource, d, encPass, testPassphrase, "iad", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if pd.Encryption.KeyCheck == "" || pd.ArchiveID != desc.ArchiveID {
		t.Fatalf("passphrase descriptor = %+v", pd)
	}
	// Siblings racing to create build identical identity fields.
	sib, _ := NewDescriptor(testSource, d, encPass, append([]byte(" "), testPassphrase...), "pdx", testNow.Add(time.Hour))
	if !IdentityEqual(pd, sib) {
		t.Fatalf("sibling descriptors differ in identity: %+v vs %+v", pd, sib)
	}
}

func TestNewDescriptorRefusesBadInput(t *testing.T) {
	d := testDest(t)
	cases := []struct {
		name string
		src  Source
		dest Destination
		enc  EncryptionIdentity
		pass []byte
	}{
		{"no uid", Source{Namespace: "db", Name: "orders"}, d, encNone, nil},
		{"no destination", testSource, Destination{}, encNone, nil},
		{"none with algorithm", testSource, d, EncryptionIdentity{Mode: EncryptionNone, Algorithm: AlgorithmAES256GCM}, nil},
		{"passphrase without algorithm", testSource, d, EncryptionIdentity{Mode: EncryptionPassphrase}, testPassphrase},
		{"passphrase empty key", testSource, d, encPass, []byte(" \n")},
		{"unknown mode", testSource, d, EncryptionIdentity{Mode: "KMS"}, nil},
	}
	for _, c := range cases {
		if _, err := NewDescriptor(c.src, c.dest, c.enc, c.pass, "iad", testNow); err == nil {
			t.Errorf("%s: NewDescriptor succeeded", c.name)
		}
	}
}

func TestParseDescriptorRoundTrip(t *testing.T) {
	for _, enc := range []EncryptionIdentity{encNone, encPass} {
		desc, err := NewDescriptor(testSource, testDest(t), enc, testPassphrase, "iad", testNow)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := desc.Canonical()
		back, err := ParseDescriptor(b)
		if err != nil {
			t.Fatalf("%s: ParseDescriptor: %v", enc, err)
		}
		b2, _ := back.Canonical()
		if string(b) != string(b2) {
			t.Fatalf("%s: canonical bytes changed across a round trip:\n%s\n%s", enc, b, b2)
		}
		if !IdentityEqual(desc, back) || !back.CreatedAt.Equal(desc.CreatedAt) || back.CreatedBy != "iad" {
			t.Fatalf("%s: round trip = %+v, want %+v", enc, back, desc)
		}
	}
}

func TestParseDescriptorRefusals(t *testing.T) {
	pd, _ := NewDescriptor(testSource, testDest(t), encPass, testPassphrase, "iad", testNow)
	valid, _ := pd.Canonical()
	sub := func(old, new string) string {
		s := strings.Replace(string(valid), old, new, 1)
		if s == string(valid) {
			t.Fatalf("substitution %q not applied", old)
		}
		return s
	}
	cases := map[string]string{
		"not json":           `{`,
		"trailing data":      string(valid) + `{}`,
		"wrong kind":         sub(`"kind":"BloodravenPITRArchive"`, `"kind":"Other"`),
		"version 2":          sub(`"version":1`, `"version":2`),
		"version 0":          sub(`"version":1`, `"version":0`),
		"malformed id":       sub(pd.ArchiveID, "ar1-short"),
		"missing uid":        sub(`"uid":"`+string(goldenUID)+`"`, `"uid":""`),
		"missing namespace":  sub(`"namespace":"db"`, `"namespace":""`),
		"unknown mode":       sub(`"mode":"Passphrase"`, `"mode":"KMS"`),
		"bad algorithm":      sub(`"algorithm":"AES-256-GCM"`, `"algorithm":"AES-128-GCM"`),
		"no keyCheck":        sub(`"keyCheck":"`+pd.Encryption.KeyCheck+`"`, `"keyCheck":""`),
		"bad keyCheck":       sub(`"keyCheck":"`+pd.Encryption.KeyCheck+`"`, `"keyCheck":"abc"`),
		"none with keyCheck": sub(`"mode":"Passphrase","algorithm":"AES-256-GCM"`, `"mode":"None","algorithm":""`),
		"none with algorithm": `{"kind":"BloodravenPITRArchive","version":1,"archiveID":"` + pd.ArchiveID +
			`","source":{"namespace":"db","name":"orders","uid":"u"},"encryption":{"mode":"None","algorithm":"AES-256-GCM","keyCheck":""}}`,
	}
	for name, raw := range cases {
		_, err := ParseDescriptor([]byte(raw))
		if tok, ok := TokenOf(err); !ok || tok != TokenArchiveVersionUnsupported {
			t.Errorf("%s: ParseDescriptor err = %v, want %s", name, err, TokenArchiveVersionUnsupported)
		}
	}
}

func TestParseDescriptorToleratesUnknownFields(t *testing.T) {
	pd, _ := NewDescriptor(testSource, testDest(t), encNone, nil, "iad", testNow)
	b, _ := pd.Canonical()
	raw := strings.Replace(string(b), `"createdBy":"iad"`, `"createdBy":"iad","futureField":{"x":1}`, 1)
	got, err := ParseDescriptor([]byte(raw))
	if err != nil || !IdentityEqual(got, pd) {
		t.Fatalf("ParseDescriptor with an unknown field = %+v, %v", got, err)
	}
}

func TestIdentityEqual(t *testing.T) {
	a, _ := NewDescriptor(testSource, testDest(t), encPass, testPassphrase, "iad", testNow)
	b := *a
	b.CreatedAt, b.CreatedBy = testNow.Add(time.Minute), "pdx"
	if !IdentityEqual(a, &b) {
		t.Fatal("createdAt/createdBy must not affect identity")
	}
	for name, mut := range map[string]func(*Descriptor){
		"archiveID": func(d *Descriptor) { d.ArchiveID = "ar1-zzzzzzzzzzzzzzzzzzzzzzzzzz" },
		"uid":       func(d *Descriptor) { d.Source.UID = "x" },
		"name":      func(d *Descriptor) { d.Source.Name = "x" },
		"mode":      func(d *Descriptor) { d.Encryption.Mode = EncryptionNone },
		"keyCheck":  func(d *Descriptor) { d.Encryption.KeyCheck = "x" },
		"version":   func(d *Descriptor) { d.Version = 2 },
	} {
		c := *a
		mut(&c)
		if IdentityEqual(a, &c) {
			t.Errorf("a change to %s kept identity equal", name)
		}
	}
	if IdentityEqual(a, nil) || !IdentityEqual(nil, nil) {
		t.Fatal("nil handling")
	}
}

func TestIsIgnoredArchiveKey(t *testing.T) {
	for key, want := range map[string]bool{
		"binlogs/a/mysql-bin.000001.tmp":                 true,
		"binlogs/manifest-a.json.part":                   true,
		"binlogs/archive.json.tmp-1234":                  true,
		"binlogs/.bloodraven-cas-probe-0123456789abcdef": true,
		"binlogs/archive.json":                           false,
		"binlogs/manifest-a.json":                        false,
		"binlogs/a/mysql-bin.000001":                     false,
	} {
		if got := IsIgnoredArchiveKey(key); got != want {
			t.Errorf("IsIgnoredArchiveKey(%q) = %v, want %v", key, got, want)
		}
	}
	if got := DescriptorKey("team/binlogs"); got != "team/binlogs/archive.json" {
		t.Fatalf("DescriptorKey = %q", got)
	}
}
