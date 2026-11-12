package dst

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/state"
)

// TestTrial_RestoredOwnLossReportDoesNotWedgeACompletePrimary is the
// history of the planned sweep's WedgedNoPrimary trial with an operator
// restart added: the primary restarts in place without part of its own
// history, its peer holds that part, and the re-assert reports it (a
// witness's report: there is no primary to compare it with). Its pod is
// then replaced onto the data directory that still has the tail, so it is
// complete again, and the operator restarts before the report's review.
// The new manager restores the report from status, which does not say what
// the report rested on. With no writable site only the re-assert compares
// the primary again: it settles the restored report and restores the
// primary. The trial runs the restore as the runner does (restoreSites),
// so the log line of the settled report is there only when the model
// carries a report over a restart.
func TestTrial_RestoredOwnLossReportDoesNotWedgeACompletePrimary(t *testing.T) {
	trial := Trial{
		SiteNames:         []string{"alpha", "beta", "gamma"},
		Roles:             []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleDROnly},
		CooldownSec:       10,
		SeedHistory:       true, // alpha is the recorded failover target, also after the restart
		WarmupPolls:       8,
		HealAt:            40,
		Polls:             40 + 10 + 140,
		SidecarLeasePolls: 15,
		Ops: []Op{
			{At: 20, Kind: OpRegressSite, Site: "alpha", K: 3},
			{At: 22, Kind: OpRegressBoot, Site: "alpha"},
			{At: 27, Kind: OpRestartOperator},
		},
		Actors: Actors{Enabled: true, Ops: []Op{
			{At: 25, Kind: OpReplacePod, Site: "alpha", K: 5},
		}},
	}
	var logs bytes.Buffer
	r := RunTrial(trial, true, slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	for _, v := range r.Violations {
		t.Errorf("violation: %s", v)
	}
	var alpha SiteTruth
	for _, tr := range r.FinalTruth {
		if tr.Name == "alpha" {
			alpha = tr
		}
	}
	if !alpha.Writable() {
		t.Errorf("alpha ended read-only=%v super_read_only=%v executed=%s; want the complete primary restored", alpha.ReadOnly, alpha.SuperReadOnly, alpha.Executed)
	}
	out := logs.String()
	for _, want := range []string{
		`msg="own transactions lost"`, // the report, before the restart
		`msg="own transactions lost cleared: no site holds transactions the site lacks"`,       // the restored report, settled
		`msg="re-asserting fenced promoted primary: no site is writable and the last failover`, // and the primary restored
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the operator log lacks %s; the trial did not take the history this test is about", want)
		}
	}
	if t.Failed() {
		for _, e := range r.Events {
			t.Log(e)
		}
	}
}
