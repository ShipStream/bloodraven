package dst

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/state"
)

// Constructed trials for the two ways the operator's own-loss evidence was
// lost (H1 and H2 of the second review round). Each is a history the
// evidence faults (ownLossEvidenceFaults) can produce, written out so that
// it does not depend on a seed or on the scheduler's draws, and each ends
// with PromotedWithoutOwnTransactions on the code from before its fix.

func evidenceTrial(ops []Op, healAt int, actors ...Op) Trial {
	return Trial{
		SiteNames:         []string{"alpha", "beta", "gamma"},
		Roles:             []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly},
		CooldownSec:       5,
		SeedHistory:       true, // alpha is the recorded failover target
		WarmupPolls:       8,
		HealAt:            healAt,
		Polls:             healAt + 10 + 150,
		SidecarLeasePolls: 8,
		SidecarTopology:   true,
		SidecarAfterPoll:  true,
		Ops:               ops,
		Actors:            Actors{Enabled: true, SchedSeed: 1, Ops: actors},
	}
}

// runEvidenceTrial runs trial and returns its result and the operator log.
func runEvidenceTrial(t *testing.T, trial Trial) (TrialResult, string) {
	t.Helper()
	var logs bytes.Buffer
	r := RunTrial(trial, true, slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return r, logs.String()
}

// requireHistory fails on any violation, and when the operator log lacks
// one of want: the trial then did not take the history its test is about.
func requireHistory(t *testing.T, r TrialResult, logs string, want ...string) {
	t.Helper()
	for _, v := range r.Violations {
		t.Errorf("violation: %s", v)
	}
	for _, w := range want {
		if !strings.Contains(logs, w) {
			t.Errorf("the operator log lacks %s; the trial did not take the history this test is about", w)
		}
	}
	if t.Failed() {
		for _, e := range r.Events {
			t.Log(e)
		}
	}
}

// H1. A witness report must survive an operator restart, and a manager
// replacement, while its witness does not answer.
//
//   - p016: beta crashes. p022: alpha, the primary, restarts on a data
//     directory five ticks old and loses the last transactions it issued;
//     gamma holds them, beta does not.
//   - p027, p028: alpha and beta come back fenced and the operator
//     restarts. No site is writable. The picker compares alpha with gamma
//     and refuses it: a witness report on alpha that rests on gamma.
//   - p033: an external writer makes beta writable. It is adopted as the
//     primary, and alpha, which holds nothing beta lacks, rejoins it.
//   - gamma stops answering the operator, and in the same poll the operator
//     restarts or replaces the group's manager. The new manager starts
//     from status, compares alpha with the primary, and finds nothing lost
//     there. That is not the end of the report: gamma has not answered.
//   - A planned failover to alpha is requested while gamma is silent, and
//     gamma answers again while it would be running.
//
// Before the fix the restored report was cleared by the clean comparison
// with the primary, the run was admitted, and alpha was made writable in
// the poll after gamma came back, holding five transactions fewer than
// gamma under the server_uuid it issues under.
func TestTrial_WitnessReportSurvivesASilentWitness(t *testing.T) {
	for _, tc := range []struct {
		name          string
		at, n, k, pfA int
	}{
		{"the manager is replaced", 37, 5, 1, 39},
		{"the operator restarts", 36, 5, 0, 38},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, logs := runEvidenceTrial(t, evidenceTrial([]Op{
				{At: 16, Kind: OpCrash, Site: "beta", Fenced: true},
				{At: 22, Kind: OpRegressSite, Site: "alpha", K: 5},
				{At: 27, Kind: OpRegressBoot, Site: "alpha", Fenced: true},
				{At: 28, Kind: OpRecover, Site: "beta", Fenced: true},
				{At: 28, Kind: OpRestartOperator},
				{At: 33, Kind: OpRogueWritable, Site: "beta"},
				{At: tc.at, Kind: OpSilentWitness, Site: "gamma", N: tc.n, K: tc.k},
				{At: tc.at + tc.n, Kind: OpSilentWitnessHeal, Site: "gamma"},
			}, 70, Op{At: tc.pfA, Kind: OpPlannedFailover, Site: "alpha"}))
			requireHistory(t, r, logs,
				`msg="own transactions lost" site=alpha`,
				`comparedTo=gamma`)
			if strings.Contains(logs, `msg="planned failover: claim granted"`) {
				t.Error("a planned failover to alpha was admitted while its report stood")
			}
		})
	}
}

// H2. A clone the operator issued into a site and that failed must not
// make the operator treat the identity the site still runs under as one a
// clone superseded.
//
//   - p009: a clone into beta, a replica, is issued and fails. beta keeps
//     its data and its server_uuid.
//   - p012: alpha, the primary, crashes; beta is promoted and issues
//     transactions. p016: alpha returns and rejoins beta. p024: alpha's
//     receiver stalls, so alpha stops receiving what beta issues; gamma
//     keeps receiving it.
//   - p034: beta crashes and alpha is promoted without beta's last
//     transactions. p036: beta's data directory comes back twelve ticks
//     old, without those transactions; p040: it boots and rejoins alpha,
//     which lacks them too.
//   - gamma holds them. The audit finds gamma divergent, and must look at
//     the site that issued those transactions: beta no longer holds them,
//     and promoted it would issue their GTIDs again. beta gets a witness
//     report.
//   - p064: a planned failover to beta is requested, and refused.
//
// Before the fix the clone marker was written when the clone was issued,
// the cross-check filtered on the identities no clone had superseded
// without reading the site, and so never read beta: no report, and the
// planned failover made beta writable.
func TestTrial_FailedCloneDoesNotHideAWitnessReport(t *testing.T) {
	r, logs := runEvidenceTrial(t, evidenceTrial([]Op{
		{At: 9, Kind: OpFailedClone, Site: "beta"},
		{At: 12, Kind: OpCrash, Site: "alpha", Fenced: true},
		{At: 16, Kind: OpRecover, Site: "alpha", Fenced: true},
		{At: 24, Kind: OpStallFetch, Site: "alpha"},
		{At: 34, Kind: OpCrash, Site: "beta", Fenced: true},
		{At: 36, Kind: OpRegressSite, Site: "beta", K: 12},
		{At: 38, Kind: OpHealFetch, Site: "alpha"},
		{At: 40, Kind: OpRegressBoot, Site: "beta", Fenced: true},
	}, 90, Op{At: 64, Kind: OpPlannedFailover, Site: "beta"}))
	requireHistory(t, r, logs,
		`msg="own transactions lost" site=beta`,
		`comparedTo=gamma`)
	if strings.Contains(logs, `msg="reclone requested for a site that lacks transactions it issued itself"`) {
		t.Error("beta was recloned: the trial is about a site that holds everything the primary does")
	}
	if strings.Contains(logs, `msg="planned failover: claim granted"`) {
		t.Error("a planned failover to beta was admitted while its report stood")
	}
	failed := false
	for _, e := range r.Events {
		if e.Kind == EvFailedClone && strings.Contains(e.String(), "recorded") {
			failed = true
		}
	}
	if !failed {
		t.Error("the failed clone was not recorded: the operator could not read beta at p009")
	}
}
