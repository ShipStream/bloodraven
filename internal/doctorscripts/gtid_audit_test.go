package doctorscripts

import (
	"strings"
	"testing"
)

// gtid-audit.sh, the follower audit: a follower that lacks transactions it
// issued itself is reported as such, with the set and the remedy, and the
// script says how its own finding relates to what the operator recorded.
func TestGTIDAuditOwnTransactionsLost(t *testing.T) {
	const (
		primaryUUID  = "11111111-0000-11f1-8000-0000000000a1"
		followerUUID = "22222222-0000-11f1-8000-0000000000b2"
		followerSet  = followerUUID + ":1-90"
	)
	cases := []struct {
		name string
		// missing is GTID_SUBTRACT(primary, follower): what the follower
		// lacks, own transactions or not.
		missing string
		// extra is GTID_SUBTRACT(follower, primary).
		extra        string
		recordedLost string
		want, wantNo []string
	}{
		{
			name:    "the follower lacks its own tail, and the operator has not recorded it",
			missing: primaryUUID + ":1-20," + followerUUID + ":91-100",
			want: []string{
				"OWN TRANSACTIONS LOST: dc2 lacks transactions it issued itself that the primary (dc1) holds:",
				"      " + followerUUID + ":91-100\n",
				"kubectl bloodraven reclone lion dc2 -n orders --cold",
				"The operator has not recorded this yet",
				"CLEAN: dc2 holds no transactions the primary (dc1) lacks",
			},
			// The primary's own transactions the follower has not applied
			// yet are lag, not a loss.
			wantNo: []string{primaryUUID + ":1-20\n"},
		},
		{
			name:         "the same, recorded by the operator",
			missing:      followerUUID + ":91-100",
			recordedLost: followerUUID + ":91-100",
			want:         []string{"OWN TRANSACTIONS LOST", "lostGtid=" + followerUUID + ":91-100"},
			wantNo:       []string{"has not recorded this yet"},
		},
		{
			name:         "the operator records a loss the primary does not hold",
			missing:      primaryUUID + ":18-20",
			recordedLost: followerUUID + ":91-100",
			want: []string{
				"The operator records lostGtid for dc2, but the primary holds nothing dc2 issued under its current server_uuid and lacks",
				"another site holds them and the primary does not",
				"CLEAN: dc2 holds",
			},
			wantNo: []string{"OWN TRANSACTIONS LOST"},
		},
		{
			name:    "lost and divergent at once",
			missing: followerUUID + ":91-100",
			extra:   followerUUID + ":101-105",
			want: []string{
				"OWN TRANSACTIONS LOST",
				"DIVERGENT: dc2 holds transactions the primary (dc1) lacks:",
				"      " + followerUUID + ":101-105\n",
			},
		},
		{
			name:   "a complete follower",
			want:   []string{"CLEAN: dc2 holds"},
			wantNo: []string{"OWN TRANSACTIONS LOST", "The operator records lostGtid"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newRuleEnv(t)
			const fg = "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath="
			e.rule(t, fg+"*status.topologyGeneration*", "7", false)
			e.rule(t, fg+"*status.activeSite*", "dc1", false)
			e.rule(t, fg+"*spec.sites*", "dc1 dc2", false)
			e.rule(t, fg+"*dc2*lostGtid*", tc.recordedLost, false)
			e.rule(t, "get pods -n orders *site=dc1 *", "mysql-dc1\t\n", false)
			e.rule(t, "get pods -n orders *site=dc2 *", "mysql-dc2\t\n", false)
			e.rule(t, "get pod mysql-dc? -n orders *", "mysql sidecar", false)
			// The primary: the subtraction with its identity and
			// read_only, the reverse subtraction, read_only, identity.
			e.rule(t, "exec -n orders mysql-dc1 *SELECT @@server_uuid, @@GLOBAL.read_only*", primaryUUID+"\t0\t"+tc.extra+"\n", false)
			e.rule(t, "exec -n orders mysql-dc1 *SELECT REPLACE*", tc.missing+"\n", false)
			e.rule(t, "exec -n orders mysql-dc1 *SELECT @@GLOBAL.read_only", "0\n", false)
			e.rule(t, "exec -n orders mysql-dc1 *SELECT @@server_uuid", primaryUUID+"\n", false)
			// The follower: identity with its set, then identity again.
			e.rule(t, "exec -n orders mysql-dc2 *SELECT @@server_uuid, REPLACE*", followerUUID+"\t"+followerSet+"\n", false)
			e.rule(t, "exec -n orders mysql-dc2 *SELECT @@server_uuid", followerUUID+"\n", false)

			out, _ := e.run(t, scriptPath("gtid-audit.sh"), "-n", "orders", "lion")
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("gtid-audit output lacks %q:\n%s", want, out)
				}
			}
			for _, bad := range tc.wantNo {
				if strings.Contains(out, bad) {
					t.Errorf("gtid-audit output contains %q:\n%s", bad, out)
				}
			}
			if strings.Contains(out, "UNKNOWN:") {
				t.Errorf("the audit could not decide; the fixture misses a call:\n%s", out)
			}
		})
	}
}
