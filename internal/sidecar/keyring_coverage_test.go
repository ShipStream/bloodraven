package sidecar

import (
	"database/sql"
	"encoding/json"
	"testing"
)

func optionalBoolString(b *bool) string {
	if b == nil {
		return "unset"
	}
	if *b {
		return "true"
	}
	return "false"
}

// A-46/F-13: a coverage flag MySQL did not report stays unknown. NULL, a
// missing row or an unrecognized value is never reported as false.
func TestCoverageFlagParsing(t *testing.T) {
	for _, tc := range []struct {
		in   sql.NullString
		want string
	}{
		{sql.NullString{}, "unset"},
		{sql.NullString{Valid: true, String: ""}, "unset"},
		{sql.NullString{Valid: true, String: "maybe"}, "unset"},
		{sql.NullString{Valid: true, String: "ON"}, "true"},
		{sql.NullString{Valid: true, String: "1"}, "true"},
		{sql.NullString{Valid: true, String: " true "}, "true"},
		{sql.NullString{Valid: true, String: "OFF"}, "false"},
		{sql.NullString{Valid: true, String: "0"}, "false"},
		{sql.NullString{Valid: true, String: "false"}, "false"},
	} {
		if got := optionalBoolString(mysqlOptionalBool(tc.in)); got != tc.want {
			t.Errorf("mysqlOptionalBool(%+v) = %s, want %s", tc.in, got, tc.want)
		}
	}
	for in, want := range map[string]string{"Y": "true", "y": "true", "N": "false", "": "unset", "?": "unset"} {
		if got := optionalBoolString(tablespaceEncrypted(in)); got != want {
			t.Errorf("tablespaceEncrypted(%q) = %s, want %s", in, got, want)
		}
	}
}

// The /keyring/status wire omits an unknown flag and carries false and
// true as reported.
func TestKeyringCoverageWireOmitsUnknownFlags(t *testing.T) {
	f, tr := false, true
	raw, err := json.Marshal(KeyringCoverage{SystemTablespaceEncrypted: nil, RedoLogEncrypted: &f, UndoLogEncrypted: &tr})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["systemTablespaceEncrypted"]; ok {
		t.Errorf("an unknown flag is on the wire: %s", raw)
	}
	if _, ok := got["binlogEncrypted"]; ok {
		t.Errorf("an unknown flag is on the wire: %s", raw)
	}
	if got["redoLogEncrypted"] != false || got["undoLogEncrypted"] != true {
		t.Errorf("reported flags lost: %s", raw)
	}
	if _, ok := got["unencryptedTablespaces"]; !ok {
		t.Errorf("unencryptedTablespaces dropped: %s", raw)
	}
}
