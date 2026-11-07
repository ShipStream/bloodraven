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
