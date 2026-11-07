package mysql

import (
	"encoding/json"
	"testing"
)

// A-46/F-13: the operator decodes a coverage flag the sidecar omitted as
// nil, not false, and keeps a reported false and true.
func TestKeyringCoverageDecodesOmittedFlagAsUnknown(t *testing.T) {
	var st KeyringStatus
	raw := `{"enabled":true,"present":true,"size":1,"coverage":{"redoLogEncrypted":false,"undoLogEncrypted":true,"unencryptedTablespaces":3}}`
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	c := st.Coverage
	if c == nil {
		t.Fatal("coverage lost")
	}
	if c.SystemTablespaceEncrypted != nil || c.BinlogEncrypted != nil {
		t.Errorf("omitted flags decoded as reported: system=%v binlog=%v", c.SystemTablespaceEncrypted, c.BinlogEncrypted)
	}
	if c.RedoLogEncrypted == nil || *c.RedoLogEncrypted || c.UndoLogEncrypted == nil || !*c.UndoLogEncrypted {
		t.Errorf("reported flags lost: redo=%v undo=%v", c.RedoLogEncrypted, c.UndoLogEncrypted)
	}
	if c.UnencryptedTablespaces != 3 {
		t.Errorf("unencryptedTablespaces = %d", c.UnencryptedTablespaces)
	}
}

// TestKeyringCoverageDecode_EachFlagMissingFalseTrue (F-13): each coverage
// flag the sidecar may send, omitted, false and true, decodes to nil,
// false and true on the operator side, without touching the others.
func TestKeyringCoverageDecode_EachFlagMissingFalseTrue(t *testing.T) {
	flags := []string{"systemTablespaceEncrypted", "redoLogEncrypted", "undoLogEncrypted", "binlogEncrypted"}
	field := func(c *KeyringCoverage, name string) *bool {
		return map[string]*bool{
			"systemTablespaceEncrypted": c.SystemTablespaceEncrypted, "redoLogEncrypted": c.RedoLogEncrypted,
			"undoLogEncrypted": c.UndoLogEncrypted, "binlogEncrypted": c.BinlogEncrypted,
		}[name]
	}
	for _, flag := range flags {
		for _, state := range []string{"missing", "false", "true"} {
			t.Run(flag+"/"+state, func(t *testing.T) {
				cov := map[string]any{"unencryptedTablespaces": 1}
				for _, other := range flags {
					cov[other] = true
				}
				switch state {
				case "missing":
					delete(cov, flag)
				case "false":
					cov[flag] = false
				}
				raw, err := json.Marshal(map[string]any{"enabled": true, "present": true, "coverage": cov})
				if err != nil {
					t.Fatal(err)
				}
				var st KeyringStatus
				if err := json.Unmarshal(raw, &st); err != nil {
					t.Fatal(err)
				}
				if st.Coverage == nil {
					t.Fatal("coverage lost")
				}
				for _, other := range flags {
					want := "true"
					if other == flag {
						want = state
					}
					got := "missing"
					if b := field(st.Coverage, other); b != nil {
						got = map[bool]string{true: "true", false: "false"}[*b]
					}
					if got != want {
						t.Errorf("%s decoded as %s, want %s (%s)", other, got, want, raw)
					}
				}
			})
		}
	}
}
