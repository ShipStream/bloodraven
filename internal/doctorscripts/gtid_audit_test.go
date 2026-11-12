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
		// recordedTo is status.sites[].lostComparedTo.
		recordedTo string
		// channel is the follower's SHOW REPLICA STATUS in the vertical
		// format ("" when it has none), primaryLog the primary's SHOW
		// BINARY LOG STATUS row, still what the follower lacks of that
		// row's set when it is read after its applier's position.
		channel, primaryLog, still string
		want, wantNo               []string
	}{
		{
			name:    "the follower lacks its own tail, and the operator has not recorded it",
			missing: primaryUUID + ":1-20," + followerUUID + ":91-100",
			want: []string{
				"OWN TRANSACTIONS LOST: dc2 lacks transactions it issued itself that the primary (dc1) holds:",
				"      " + followerUUID + ":91-100\n",
				"Its replication from the primary is not running, so nothing is delivering them",
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
			recordedTo:   "dc3",
			want: []string{
				"lostComparedTo=dc3",
				"The operator records lostGtid for dc2 (lostComparedTo: dc3), but the primary holds nothing dc2 issued under its current server_uuid and provably lacks",
				"another site holds them and the primary does not",
				"the report ends once every other site answers and none holds them",
				"CLEAN: dc2 holds",
			},
			wantNo: []string{"OWN TRANSACTIONS LOST"},
		},
		{
			// A replay onto the primary of transactions the follower
			// issued: the follower's channel runs and its applier is
			// behind the primary's position. Not a loss.
			name:       "the follower's channel runs and its applier is behind the primary: lag",
			missing:    followerUUID + ":91-100",
			channel:    channelStatus("Yes", "Yes", "mysql-bin.000007", "4000"),
			primaryLog: "mysql-bin.000007\t5000\t\t\t" + primaryUUID + ":1-20,\\n" + followerUUID + ":1-100\n",
			want: []string{
				"BEHIND, NOT LOST (so far): dc2 lacks transactions under its own server_uuid that the primary (dc1) holds:",
				"its applier (mysql-bin.000007:4000) has not reached the primary's position (mysql-bin.000007:5000)",
				"CLEAN: dc2 holds",
			},
			wantNo: []string{"OWN TRANSACTIONS LOST", "--cold"},
		},
		{
			name:       "the follower's applier has passed the primary's position and still lacks them: lost",
			missing:    followerUUID + ":91-100",
			channel:    channelStatus("Yes", "Yes", "mysql-bin.000008", "120"),
			primaryLog: "mysql-bin.000007\t5000\t\t\t" + primaryUUID + ":1-20,\\n" + followerUUID + ":1-100\n",
			still:      followerUUID + ":95-100",
			want: []string{
				"OWN TRANSACTIONS LOST: dc2 lacks transactions it issued itself that the primary (dc1) holds:",
				"      " + followerUUID + ":95-100\n",
				"Its applier has executed the primary's binary log past mysql-bin.000007:5000",
				"kubectl bloodraven reclone lion dc2 -n orders --cold",
			},
			wantNo: []string{"BEHIND, NOT LOST (so far)"},
		},
		{
			name:       "the follower applied them between the reads: not a loss",
			missing:    followerUUID + ":91-100",
			channel:    channelStatus("Yes", "Yes", "mysql-bin.000007", "5000"),
			primaryLog: "mysql-bin.000007\t5000\t\t\t" + followerUUID + ":1-100\n",
			want:       []string{"has applied them since the first read: not a loss", "CLEAN: dc2 holds"},
			wantNo:     []string{"OWN TRANSACTIONS LOST", "BEHIND, NOT LOST (so far)"},
		},
		{
			name:    "the channel is stopped: the single comparison stands",
			missing: followerUUID + ":91-100",
			channel: channelStatus("Yes", "No", "mysql-bin.000007", "4000"),
			want:    []string{"OWN TRANSACTIONS LOST", "Its replication from the primary is not running"},
			wantNo:  []string{"BEHIND, NOT LOST (so far)"},
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
			e.rule(t, fg+"*dc2*lostComparedTo*", tc.recordedTo, false)
			e.rule(t, "get pods -n orders *site=dc1 *", "mysql-dc1\t\n", false)
			e.rule(t, "get pods -n orders *site=dc2 *", "mysql-dc2\t\n", false)
			e.rule(t, "get pod mysql-dc? -n orders *", "mysql sidecar", false)
			// The primary: the subtraction with its identity and
			// read_only, the reverse subtraction, read_only, identity.
			e.rule(t, "exec -n orders mysql-dc1 *SELECT @@server_uuid, @@GLOBAL.read_only*", primaryUUID+"\t0\t"+tc.extra+"\n", false)
			e.rule(t, "exec -n orders mysql-dc1 *SELECT REPLACE*", tc.missing+"\n", false)
			if tc.primaryLog != "" {
				e.rule(t, "exec -n orders mysql-dc1 *SHOW BINARY LOG STATUS*", tc.primaryLog, false)
			}
			if tc.channel != "" {
				e.rule(t, "exec -n orders mysql-dc2 *SHOW REPLICA STATUS*", tc.channel, false)
			} else {
				e.rule(t, "exec -n orders mysql-dc2 *SHOW REPLICA STATUS*", "", false)
			}
			// What the follower lacks of the primary's set at its
			// position, read after the applier's position.
			e.rule(t, "exec -n orders mysql-dc2 *SELECT REPLACE(GTID_SUBTRACT*", tc.still+"\n", false)
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

// channelStatus is the part of SHOW REPLICA STATUS, in the vertical format,
// that the audit reads.
func channelStatus(io, sql, execFile, execPos string) string {
	return "*************************** 1. row ***************************\n" +
		"             Replica_IO_State: Waiting for source to send event\n" +
		"                  Source_Host: mysql-dc1\n" +
		"        Relay_Source_Log_File: " + execFile + "\n" +
		"           Replica_IO_Running: " + io + "\n" +
		"          Replica_SQL_Running: " + sql + "\n" +
		"          Exec_Source_Log_Pos: " + execPos + "\n"
}
