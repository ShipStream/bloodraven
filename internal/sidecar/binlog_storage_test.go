package sidecar

import (
	"testing"
)

// TestPVCStore_FullPath_Sanitizes proves the traversal guard in
// pvcStore.fullPath (called from every Put/Get/PutFile/GetFile/Delete
// path). This is security-critical for restore paths that may be fed
// attacker-controlled keys from object storage. AUDIT M15.
func TestPVCStore_FullPath_Sanitizes(t *testing.T) {
	p := &pvcStore{root: "/mnt/pvc"}

	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"plain", "binlogs/iad/mysql-bin.000001", false},
		{"leading slash normalized", "/binlogs/iad/mysql-bin.000001", false},
		{"dotdot escape rejected", "../escape", true},
		{"dotdot nested rejected", "a/b/../../../escape", true},
		{"single dot rejected", ".", true},
		{"double dot rejected", "..", true},
		{"prefix collision rejected", "../pvc-evil/escape", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.fullPath(tc.key)
			if (err != nil) != tc.wantErr {
				t.Errorf("fullPath(%q) err=%v wantErr=%v", tc.key, err, tc.wantErr)
			}
		})
	}
}
