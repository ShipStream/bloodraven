package pitrarchive

import (
	"strings"
	"testing"
)

func TestTokenCatalog(t *testing.T) {
	seen := map[Token]bool{}
	for _, tok := range AllTokens {
		if tok == "" || seen[tok] {
			t.Fatalf("token %q is empty or duplicated", tok)
		}
		seen[tok] = true
		if !tok.Known() {
			t.Fatalf("%s not Known", tok)
		}
		if !strings.HasPrefix(string(tok), "PITR") && tok != TokenPITRFailed {
			t.Fatalf("%s does not start with PITR", tok)
		}
	}
	if Token("PITRCrossSiteOrder").Known() || Token("PITRArchiveSiteOmitted").Known() {
		t.Fatal("removed tokens must not be in the catalog")
	}
}

func TestParseToken(t *testing.T) {
	tests := []struct {
		msg  string
		want Token
		ok   bool
	}{
		{"PITRArchiveForeign: archive ar1-x at s3://b: detail; procedure", TokenArchiveForeign, true},
		{"  PITRArchiveGap the target history needs …", TokenArchiveGap, true},
		{"BLOODRAVEN_PITR_FAILED: mysqlbinlog exit=1 segment=0", TokenPITRFailed, true},
		{"PITRUnknownThing: x", "PITRUnknownThing", false},
		{"", "", false},
	}
	for _, tt := range tests {
		got, ok := ParseToken(tt.msg)
		if got != tt.want || ok != tt.ok {
			t.Errorf("ParseToken(%q) = %q, %v; want %q, %v", tt.msg, got, ok, tt.want, tt.ok)
		}
	}
}

func TestTokenReason(t *testing.T) {
	want := map[Token]RefusalReason{
		TokenArchiveForeign:            ReasonArchiveForeign,
		TokenArchiveCopy:               ReasonArchiveCopy,
		TokenArchiveLegacy:             ReasonArchiveLegacy,
		TokenArchiveDescriptorMissing:  ReasonArchiveDescriptorMissing,
		TokenArchiveEncryptionMismatch: ReasonArchiveEncryptionMismatch,
		TokenArchiveKeyMismatch:        ReasonArchiveKeyMismatch,
		TokenArchiveVersionUnsupported: ReasonArchiveVersionUnsupported,
		TokenArchiveStoreUnsupported:   ReasonArchiveStoreUnsupported,
		TokenArchiveUnsafeHistory:      ReasonArchiveUnsafeHistory,
	}
	if len(want) != len(ArchiveReadyReasons) {
		t.Fatal("ArchiveReadyReasons out of step with the table")
	}
	for _, tok := range AllTokens {
		r, ok := tok.Reason()
		if w, has := want[tok]; has {
			if !ok || r != w {
				t.Errorf("%s.Reason() = %q, %v; want %q", tok, r, ok, w)
			}
		} else if ok {
			t.Errorf("%s.Reason() = %q, want none", tok, r)
		}
	}
}
