package controller

import (
	"context"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// opsUUIDOld is a server_uuid the source (dc1) ran under before the one it
// runs under now.
const opsUUIDOld = "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a"

// TestRollback_ComparesTheSourceWithEverySiteThatAnswers: a rollback does
// not unfence a source that lacks transactions it issued itself when any
// site that answers holds them, not only the target. The target of a run
// that rolls back is often the site the operator cannot read (that is why
// the run rolled back), while a disaster-recovery site holds the tail the
// source lost when it restarted on older data before the fence was read.
//
// Whose set counts follows own_transactions_lost.go: the fenced set is the
// source's own report, so all of it counts, under whatever server_uuid;
// the target and the other sites are witnesses, which count under the
// source's server_uuids that no clone has superseded, and not at all while
// they await the reclone of an in-place restore or do not answer.
func TestRollback_ComparesTheSourceWithEverySiteThatAnswers(t *testing.T) {
	hold, leftFenced, rolledBack := v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
		v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverOutcomeRolledBack
	notRestored := []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored}
	lag := v1alpha1.PlannedFailoverReasonLagTimeout
	// cloned records that dc1 ran under opsUUIDOld and was then cloned, so
	// that identity is superseded; replaced records the earlier identity
	// without a clone (a data directory replaced by hand), which stays live.
	replaced := func(f *opsFixture) { f.tm.knownSiteServerUUIDs("dc1", opsUUIDOld) }
	cloned := func(f *opsFixture) {
		replaced(f)
		f.tm.markSiteCloned(context.Background(), "dc1")
	}
	withOld := func(n string) string { return opsF + "," + opsUUIDOld + ":1-" + n }
	cases := []rollbackSourceCase{
		{name: "fenced source lacks its own transactions a disaster-recovery site holds, target unreachable",
			third: state.SiteRoleDROnly, reason: lag,
			mutate:  func(f *opsFixture) { f.fenced(); f.tgt.down = true; f.third.gtid = opsUUIDA + ":1-13" },
			outcome: leftFenced, warnings: notRestored},
		{name: "the same with a target that answers and holds no more than the source",
			third: state.SiteRoleDROnly, reason: lag,
			mutate:  func(f *opsFixture) { f.fenced(); f.third.gtid = opsUUIDA + ":1-13" },
			outcome: leftFenced},
		{name: "the same beside a read-only reader that holds them",
			third: state.SiteRoleReadOnly, reason: lag,
			mutate:  func(f *opsFixture) { f.fenced(); f.third.gtid = opsUUIDA + ":1-13" },
			outcome: leftFenced},
		{name: "writable source lacks its own transactions a disaster-recovery site holds",
			third:   state.SiteRoleDROnly,
			mutate:  func(f *opsFixture) { f.third.gtid = opsUUIDA + ":1-13" },
			outcome: hold, outReason: v1alpha1.PlannedFailoverReasonSourceUnfenced, lost: opsUUIDA + ":11-13"},
		{name: "what the fenced set, the target and another site show is one set",
			third: state.SiteRoleDROnly,
			mutate: func(f *opsFixture) {
				f.src.gtid, f.tgt.gtid, f.third.gtid = opsUUIDA+":1-8", opsUUIDA+":1-12", opsUUIDA+":1-14"
			},
			outcome: hold, lost: opsUUIDA + ":9-14"},

		// A site that proves nothing leaves the rollback as it was.
		{name: "a site that does not answer is no evidence",
			third: state.SiteRoleDROnly, reason: lag,
			mutate:  func(f *opsFixture) { f.fenced(); f.third.gtid = opsUUIDA + ":1-13"; f.third.down = true },
			outcome: rolledBack, unfenced: true},
		{name: "a site queued for the reclone of an in-place restore is no evidence",
			third: state.SiteRoleDROnly, reason: lag,
			mutate: func(f *opsFixture) {
				f.fenced()
				f.third.gtid = opsUUIDA + ":1-13"
				f.tm.mu.Lock()
				f.tm.restoreReclonePending = []string{"dc3"}
				f.tm.mu.Unlock()
			},
			outcome: rolledBack, unfenced: true},
		{name: "a site ahead under another server's uuid is no evidence",
			third: state.SiteRoleDROnly, reason: lag,
			mutate:  func(f *opsFixture) { f.fenced(); f.third.gtid = opsF + "," + opsUUIDB + ":1-4" },
			outcome: rolledBack, unfenced: true},

		// Identities the source had before. One a clone superseded: what
		// another site holds of it beyond the source's copy the source gave
		// up at the clone, so that site is divergent and the source is not
		// short. One that changed without a clone stays evidence.
		{name: "another site holds transactions of an identity the source gave up at a clone",
			third: state.SiteRoleDROnly, reason: lag,
			mutate: func(f *opsFixture) {
				cloned(f)
				f.fenced()
				f.src.gtid, f.tgt.gtid, f.third.gtid = withOld("3"), withOld("3"), withOld("5")
			},
			fence: withOld("3"), outcome: rolledBack, unfenced: true},
		{name: "the target holds transactions of an identity the source gave up at a clone",
			reason: lag,
			mutate: func(f *opsFixture) {
				cloned(f)
				f.fenced()
				f.src.gtid, f.tgt.gtid = withOld("3"), withOld("5")
			},
			// The target is divergent: nothing is pointed at the source (its
			// channel runs and is left alone), and the source is not held.
			fence: withOld("3"), outcome: rolledBack, unfenced: true},
		{name: "another site holds transactions of an earlier identity no clone superseded",
			third: state.SiteRoleDROnly, reason: lag,
			mutate: func(f *opsFixture) {
				replaced(f)
				f.fenced()
				f.src.gtid, f.tgt.gtid, f.third.gtid = withOld("3"), withOld("3"), withOld("5")
			},
			fence: withOld("3"), outcome: leftFenced},
		{name: "the target holds transactions of an earlier identity no clone superseded",
			reason: lag,
			mutate: func(f *opsFixture) {
				replaced(f)
				f.fenced()
				f.src.gtid, f.tgt.gtid = withOld("3"), withOld("5")
			},
			fence: withOld("3"), outcome: leftFenced},
		// The fenced set is what the source itself reported: its part under
		// a superseded identity counts like the rest.
		{name: "fenced source lacks part of the fenced set under an identity a clone superseded",
			reason: lag,
			mutate: func(f *opsFixture) {
				cloned(f)
				f.fenced()
				f.src.gtid, f.tgt.gtid = withOld("3"), withOld("3")
			},
			fence: withOld("5"), outcome: leftFenced},
		{name: "writable source lacks part of the fenced set under an identity a clone superseded",
			mutate: func(f *opsFixture) {
				cloned(f)
				f.src.gtid, f.tgt.gtid = withOld("3"), withOld("3")
			},
			fence: withOld("5"), outcome: hold, lost: opsUUIDOld + ":4-5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runRollbackSourceCase(t, tc) })
	}
}

// TestRollback_SourceWhoseSetDoesNotParseStaysFenced: a set that does not
// parse proves nothing about what the source holds, so the source is not
// unfenced on it.
func TestRollback_SourceWhoseSetDoesNotParseStaysFenced(t *testing.T) {
	runRollbackSourceCase(t, rollbackSourceCase{
		name:    "unparseable",
		reason:  v1alpha1.PlannedFailoverReasonLagTimeout,
		mutate:  func(f *opsFixture) { f.fenced(); f.src.gtid = "not-a-gtid-set" },
		outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
	})
}
