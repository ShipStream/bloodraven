package pitrarchive

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestVerifyDescriptor(t *testing.T) {
	dest := testDest(t)
	other, _ := NewS3Destination("backups", "copy", "")
	none, _ := NewDescriptor(testSource, dest, encNone, nil, "iad", testNow)
	pass, _ := NewDescriptor(testSource, dest, encPass, testPassphrase, "iad", testNow)
	// A copy: same group, built for another destination, read at dest.
	copyDesc, _ := NewDescriptor(testSource, other, encNone, nil, "iad", testNow)
	foreign, _ := NewDescriptor(Source{Namespace: "db", Name: "billing", UID: "uid-billing"}, dest, encNone, nil, "iad", testNow)
	future := *none
	future.ArchiveID = "ar1-zzzzzzzzzzzzzzzzzzzzzzzzzz" // an unsupported derivation
	tampered := *pass
	tampered.Source.Name = "renamed" // keyCheck covers the name

	writer := func(enc EncryptionIdentity) Expectation {
		return Expectation{Role: RoleWriter, SourceUID: goldenUID, Destination: dest, Encryption: enc}
	}
	reader := func(uid types.UID, id string, enc EncryptionIdentity) Expectation {
		return Expectation{Role: RoleReader, SourceUID: uid, ArchiveID: id, Destination: dest, Encryption: enc}
	}
	tests := []struct {
		name   string
		desc   *Descriptor
		expect Expectation
		pass   []byte
		want   Token // "" means accepted
	}{
		{"writer none ok", none, writer(encNone), nil, ""},
		{"writer passphrase ok", pass, writer(encPass), testPassphrase, ""},
		{"writer passphrase trimmed-equivalent", pass, writer(encPass), append(append([]byte{}, testPassphrase...), '\n'), ""},
		{"writer foreign", foreign, writer(encNone), nil, TokenArchiveForeign},
		{"writer copy at another destination", copyDesc, writer(encNone), nil, TokenArchiveCopy},
		{"writer unsupported derivation", &future, writer(encNone), nil, TokenArchiveCopy},
		{"writer expects passphrase, archive none", none, writer(encPass), testPassphrase, TokenArchiveEncryptionMismatch},
		{"writer expects none, archive passphrase", pass, writer(encNone), nil, TokenArchiveEncryptionMismatch},
		{"writer wrong key", pass, writer(encPass), []byte("a different passphrase of some length"), TokenArchiveKeyMismatch},
		{"writer no key mounted", pass, writer(encPass), nil, TokenArchiveKeyMismatch},
		{"writer tampered identity", &tampered, writer(encPass), testPassphrase, TokenArchiveKeyMismatch},
		{"writer bad version", &Descriptor{Kind: DescriptorKind, Version: 2}, writer(encNone), nil, TokenArchiveVersionUnsupported},

		{"reader accepts any id", copyDesc, reader("", "", encNone), nil, ""},
		{"reader accepts foreign uid when unchecked", foreign, reader("", "", encNone), nil, ""},
		{"reader expected uid ok", none, reader(goldenUID, none.ArchiveID, encNone), nil, ""},
		{"reader uid mismatch", foreign, reader(goldenUID, "", encNone), nil, TokenArchiveIdentityMismatch},
		{"reader archive id mismatch", copyDesc, reader(goldenUID, none.ArchiveID, encNone), nil, TokenArchiveIdentityMismatch},
		{"reader encryption mismatch", none, reader("", "", encPass), testPassphrase, TokenArchiveEncryptionMismatch},
		{"reader wrong key", pass, reader("", "", encPass), []byte("a different passphrase of some length"), TokenArchiveKeyMismatch},
		{"reader right key", pass, reader(goldenUID, pass.ArchiveID, encPass), testPassphrase, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := VerifyDescriptor(tt.desc, tt.expect, tt.pass)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("VerifyDescriptor = %v, want nil", err)
				}
				return
			}
			tok, ok := TokenOf(err)
			if !ok || tok != tt.want {
				t.Fatalf("VerifyDescriptor = %v, want token %s", err, tt.want)
			}
			if msg := err.Error(); !strings.HasPrefix(msg, string(tt.want)+": archive ") || !strings.Contains(msg, " at "+dest.String()+": ") || !strings.Contains(msg, "; ") {
				t.Fatalf("message %q does not follow '<Token>: archive <id> at <destination>: <detail>; <procedure>'", msg)
			}
		})
	}
}

func TestVerifyDescriptorMessage(t *testing.T) {
	dest := testDest(t)
	foreign, _ := NewDescriptor(Source{Namespace: "db", Name: "billing", UID: "uid-billing"}, dest, encNone, nil, "iad", testNow)
	err := VerifyDescriptor(foreign, Expectation{Role: RoleWriter, SourceUID: goldenUID, Destination: dest, Encryption: encNone}, nil)
	want := "PITRArchiveForeign: archive " + foreign.ArchiveID + " at s3://backups/binlogs?endpoint=aws: " +
		"this prefix belongs to group db/billing (uid uid-billing), not this group (uid " + string(goldenUID) + "); " +
		"point this profile at a new prefix"
	if err == nil || err.Error() != want {
		t.Fatalf("message\n got %v\nwant %s", err, want)
	}
}

func TestVerifyDescriptorInvalidExpectation(t *testing.T) {
	dest := testDest(t)
	none, _ := NewDescriptor(testSource, dest, encNone, nil, "iad", testNow)
	for name, e := range map[string]Expectation{
		"no role":                 {SourceUID: goldenUID, Destination: dest, Encryption: encNone},
		"writer without uid":      {Role: RoleWriter, Destination: dest, Encryption: encNone},
		"writer without dest":     {Role: RoleWriter, SourceUID: goldenUID, Encryption: encNone},
		"bad expected encryption": {Role: RoleReader, Encryption: EncryptionIdentity{Mode: EncryptionPassphrase}},
	} {
		if err := VerifyDescriptor(none, e, nil); !errors.Is(err, ErrInvalidExpectation) {
			t.Errorf("%s: err = %v, want ErrInvalidExpectation", name, err)
		}
	}
	if err := VerifyDescriptor(nil, Expectation{Role: RoleReader, Encryption: encNone}, nil); !errors.Is(err, ErrInvalidExpectation) {
		t.Errorf("nil descriptor: err = %v", err)
	}
}

func TestParseAndVerifyDescriptor(t *testing.T) {
	dest := testDest(t)
	pass, _ := NewDescriptor(testSource, dest, encPass, testPassphrase, "iad", testNow)
	b, _ := pass.Canonical()
	expect := Expectation{Role: RoleWriter, SourceUID: goldenUID, Destination: dest, Encryption: encPass}
	if _, err := ParseAndVerifyDescriptor(b, expect, testPassphrase); err != nil {
		t.Fatal(err)
	}
	_, err := ParseAndVerifyDescriptor([]byte(`{"kind":"BloodravenPITRArchive","version":9}`), expect, testPassphrase)
	if tok, _ := TokenOf(err); tok != TokenArchiveVersionUnsupported || !strings.Contains(err.Error(), " at "+dest.String()) {
		t.Fatalf("parse refusal = %v", err)
	}
}
