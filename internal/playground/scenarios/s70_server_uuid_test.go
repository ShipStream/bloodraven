package scenarios

import (
	"strings"
	"testing"

	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

const (
	s70OldUUID = "8d8c6b2a-dd2f-11f1-87db-8e979c620586" // a before its reclone
	s70NewUUID = "abd7f20b-dd32-11f1-bd72-3ac3991a0bea" // a after it
	s70BUUID   = "8d6fed6a-dd2f-11f1-8833-f233a7e69e3b" // b
)

// TestS70DivergentEvidence: after the reclone a's sidecar must report its
// archived lost rows as divergent, with a verdict that nothing is
// reissuable and no archiver error. A reissuable set or a quarantined row
// ends the wait at once.
func TestS70DivergentEvidence(t *testing.T) {
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name     string
		st       *pgsidecar.ArchiverStatusResponse
		wantDone bool
		wantErr  string
	}{
		{name: "no status"},
		{name: "audit has not run", st: &pgsidecar.ArchiverStatusResponse{}},
		{name: "rows, no verdict yet", st: &pgsidecar.ArchiverStatusResponse{DivergentRows: 2}},
		{name: "verdict, rows not recorded yet", st: &pgsidecar.ArchiverStatusResponse{ReissuableGTIDs: str("")}},
		{name: "evidence", st: &pgsidecar.ArchiverStatusResponse{DivergentRows: 2, ReissuableGTIDs: str("")}, wantDone: true},
		{name: "evidence with an archiver error", st: &pgsidecar.ArchiverStatusResponse{DivergentRows: 2, ReissuableGTIDs: str(""), LastError: "upload failed"}},
		{name: "reissuable", st: &pgsidecar.ArchiverStatusResponse{DivergentRows: 2, ReissuableGTIDs: str(s70OldUUID + ":9-15")},
			wantErr: "can reissue " + s70OldUUID + ":9-15"},
		{name: "quarantined", st: &pgsidecar.ArchiverStatusResponse{DivergentRows: 1, QuarantinedRows: 1, ReissuableGTIDs: str("")},
			wantErr: "quarantined rows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done, msg, err := s70DivergentEvidence(tc.st)
			if msg == "" {
				t.Error("no summary for the wait's timeout message")
			}
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || done {
					t.Fatalf("done=%v err=%v, want an error containing %q", done, err, tc.wantErr)
				}
			case err != nil || done != tc.wantDone:
				t.Fatalf("done=%v err=%v, want done=%v and no error", done, err, tc.wantDone)
			}
		})
	}
}

// TestS70NoReissue: the recloned a's executed set after its new era holds
// none of the lost set and numbers the new era under its new server_uuid.
func TestS70NoReissue(t *testing.T) {
	lost := s70OldUUID + ":9-15"
	kept := s70OldUUID + ":1-8," + s70BUUID + ":1-40"
	for _, tc := range []struct {
		name     string
		executed string
		uuid     string
		wantErr  string
	}{
		{name: "new era under the new server_uuid", executed: kept + "," + s70NewUUID + ":1-6", uuid: s70NewUUID},
		// What a clone that kept the server_uuid did: the new era took the
		// lost transactions' numbers.
		{name: "lost GTIDs reissued", executed: s70OldUUID + ":1-12," + s70BUUID + ":1-40", uuid: s70OldUUID,
			wantErr: "executed " + s70OldUUID + ":9-12 of the lost set"},
		{name: "one lost GTID reissued under a new identity too", executed: kept + "," + s70OldUUID + ":9," + s70NewUUID + ":1-6", uuid: s70NewUUID,
			wantErr: "executed " + s70OldUUID + ":9 of the lost set"},
		{name: "no era under the server's identity", executed: kept, uuid: s70NewUUID, wantErr: "holds no transaction under its server_uuid"},
		{name: "identity is the lost set's", executed: kept, uuid: s70OldUUID, wantErr: "is one the lost set"},
		{name: "unparsable set", executed: "not-a-set", uuid: s70NewUUID, wantErr: "parse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := s70NoReissue(tc.executed, lost, tc.uuid)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestScenario70KeepsTheArchiveUsable pins what #265 changed in s70: the
// reclone and failback are followed by verifications that succeed, not by
// the history-reset refusal and the new-prefix remedy, and the scenario no
// longer expects a reissue.
func TestScenario70KeepsTheArchiveUsable(t *testing.T) {
	s70, names := pitrScenarioSteps(t, s70ID)
	reclone := stepIndex(t, names, "reclone a")
	after := stepIndex(t, names, "no lost GTID is reissued")
	if after != reclone+1 || after != len(names)-1 {
		t.Fatalf("s70 steps %q: the verification after the failback must follow the reclone and end the scenario", names)
	}
	for _, want := range []string{"a new server_uuid", "not an archiver error", "no reissue warning"} {
		if !strings.Contains(names[reclone], want) {
			t.Errorf("s70 reclone step %q must state %q", names[reclone], want)
		}
	}
	for _, want := range []string{"refuses DivergentHistory with L ∪ L'", "verifies with both excluded", "β2 verifies without exclusions"} {
		if !strings.Contains(names[after], want) {
			t.Errorf("s70 step %q must state %q", names[after], want)
		}
	}
	for _, n := range names {
		if strings.Contains(n, "remedy") || strings.Contains(n, "new archive prefix") {
			t.Errorf("s70 still has a remedy step: %q", n)
		}
	}
	for _, want := range []string{"gives it a new server_uuid", "emits no PITRArchiveReissueRiskDetected", "no verification refuses PITRArchiveHistoryReset"} {
		if !strings.Contains(s70.Hypothesis, want) {
			t.Errorf("s70 hypothesis must state %q: %s", want, s70.Hypothesis)
		}
	}
}
