package pitrplan

import (
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Known limitations after a server_uuid has issued a GTID twice, on a
// history the operator still permits (site docs, failover, "What this
// cannot see"; review of issue #269 after the operator began to reclone a
// returning site that lacks its own transactions). THE PLANS ASSERTED
// CERTIFIED IN THIS FILE ARE WRONG. They are pinned so that closing one is
// a deliberate change, and because they are why the containment anchor
// (containmentAnchors) stays off. test/component
// TestKnownLimitationPITRPlanAfterInPlaceReissue builds the same history
// with real archivers.

// reassertedOptions vary reassertedInPlace.
type reassertedOptions struct {
	// p2 is the promotion set of the record of the episode that issued
	// A:51-52 again ("": the sidecar came up after the operator had made the
	// server writable again, so it did not see it read-only).
	p2 string
	// sample is that record's gtidExecuted ("": A:1-52, observed at 74.2 s,
	// before A:53 committed); at is when it was observed (0: 74.2 s).
	sample string
	at     float64
	// later adds a's next episode (promoted at 120 s, recorded with its
	// promotion set), archived only as b's copies.
	later bool
	// onHolder: the backup is taken on b at backupAt, holding backup, and
	// retention has retired b1, the file with b's copies of what the backup
	// holds. Otherwise the backup is a's, A:1-45 at 45.5 s, and b1 is
	// archived.
	onHolder bool
	backup   string
	backupAt float64
}

// reassertedInPlace is a primary that lost its last transactions and was
// made writable again with no failover in between:
//
//	a (A, 101) is the primary. A:21-50 are sealed and archived (a1). A:51
//	and A:52 (51 s, 52 s) reach b (B, 102) and are lost on a in a crash at
//	53 s (its volume came back older).
//	No site that holds them answers when the operator decides (b is down or
//	unreachable), so a is made writable again at 72 s. It commits A:51' and
//	A:52' (73 s, 74 s: the lost numbers, other transactions) and A:53-60
//	(75..82 s). b comes back, already holds "A:51-52", skips the new ones
//	and applies A:53-60 (b2).
//	a fails before any binlog of that episode is archived. b is promoted at
//	95 s and commits B:1-10 (b3). With later, a rejoins (its set contains
//	b's), is promoted again at 120 s, commits A:61-70 (121 s..; record at
//	124.2 s holding A:61-63) and fails again before it uploads; b archives
//	its copies (b4).
//
// From 73 s on the origin a had A:1-50, A:51', A:52'. The archive and b's
// backups hold only the lost A:51 and A:52.
func reassertedInPlace(t *testing.T, o reassertedOptions) *synth {
	t.Helper()
	s := newSynth(t)
	sample, at := o.sample, o.at
	if sample == "" {
		sample = "A:1-52"
	}
	if at == 0 {
		at = 74.2
	}
	s.site("a").Promotions = append(s.site("a").Promotions,
		recP("A", 101, 10, "A:1-20", ""),
		recP("A", 101, at, sample, o.p2))
	s.site("b").Promotions = append(s.site("b").Promotions, recP("B", 102, 95.5, "A:1-60", "A:1-60"))
	s.file("a", "a1", 101, "A:1-20", "", run("A", 21, 50, 21, 101))
	if !o.onHolder {
		s.file("b", "b1", 102, "A:1-20", "", run("A", 21, 52, 21, 101))
	}
	s.file("b", "b2", 102, "A:1-52", "", run("A", 53, 60, 75, 101))
	s.file("b", "b3", 102, "A:1-60", "", run("B", 1, 10, 96, 102))
	if o.later {
		s.site("a").Promotions = append(s.site("a").Promotions, recP("A", 101, 124.2, "A:1-63,B:1-10", "A:1-60,B:1-10"))
		s.site("b").Promotions = append(s.site("b").Promotions, recP("B", 102, 140.5, "A:1-70,B:1-10", "A:1-70,B:1-10"))
		s.file("b", "b4", 102, "A:1-60,B:1-10", "", run("A", 61, 70, 121, 101))
		s.file("b", "b5", 102, "A:1-70,B:1-10", "", run("B", 11, 20, 141, 102))
	}
	if o.onHolder {
		s.backup(o.backup, "b", 102, o.backupAt)
	} else {
		s.backup("A:1-45", "a", 101, 45.5)
	}
	return s
}

// wantWrongPlan asserts a certified plan with witness w and Λ = lineage,
// which holds the lost A:51-52: replayed from b/b1 when replays, else
// taken from the backup.
func wantWrongPlan(t *testing.T, what string, p *Plan, err error, w, lineage string, replays bool) {
	t.Helper()
	wantCertified(t, "WRONG, pinned: "+what, p, err, w, lineage)
	if got := strings.Contains(segString(p), "[b/b1: A:46-52]"); got != replays {
		t.Fatalf("%s: segments %s; replays the lost A:51-52 from b/b1: %v, want %v", what, segString(p), got, replays)
	}
}

// The containment anchor certifies the lost transactions in the lag window
// of a LATER episode's record, whose promotion set holds A:51-52 as the
// transactions a issued again, when nothing dates the reissue:
//
//   - the record of the episode that issued them again carries no promotion
//     set (TestWitnessLostTailReissuedInAnEarlierEpisode has the same after
//     a failover; here the server never lost its role, which the operator
//     cannot prevent when no site that holds the lost transactions answers);
//   - every record carries a promotion set, and the only copy of the lost
//     transactions is in the backup: the planner dates archived copies, and
//     does not date the backup against a record's episode;
//   - the same with a backup taken on b after a issued them again: no time
//     tells that backup from one that holds the new transactions.
//
// The planner as it ships refuses each of these targets, as it did before
// issue #269: only the containment anchor certifies them.
func TestKnownLimitationContainmentAnchorAfterReissue(t *testing.T) {
	for _, tc := range []struct {
		what    string
		o       reassertedOptions
		lineage string
		replays bool
	}{
		{"the reissuing episode's record has no promotion set", reassertedOptions{later: true}, "A:1-61,B:1-10", true},
		{"the lost transactions are only in a backup from before the reissuing episode",
			reassertedOptions{p2: "A:1-50", later: true, onHolder: true, backup: "A:1-52", backupAt: 60}, "A:1-61,B:1-10", false},
		{"the lost transactions are only in a backup taken on the replica after the reissue",
			reassertedOptions{p2: "A:1-50", later: true, onHolder: true, backup: "A:1-60", backupAt: 90}, "A:1-61,B:1-10", false},
	} {
		t.Run(tc.what, func(t *testing.T) {
			wantShippedRefusal(t, "lag window of the later episode", reassertedInPlace(t, tc.o), 121.5, "",
				"A:62, is archived only as a replica copy on b. Its predecessors include B:1 ", "or choose a target after 2026-11-01T12:02:09.200000Z")
			withContainmentAnchors(t)
			p, err := reassertedInPlace(t, tc.o).run(ModeTimestamp, 121.5, "", false)
			wantWrongPlan(t, tc.what, p, err, "A:62", tc.lineage, tc.replays)
			if len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#2" {
				t.Fatalf("anchored by %v, want the later episode's record a#2", p.Witness.AnchorRecords)
			}
		})
	}
	// When the reissuing episode's record carries its promotion set and the
	// lost transactions are archived, the same target refuses, with the
	// anchor and without it.
	eitherWay(t, func(t *testing.T) {
		_, err := reassertedInPlace(t, reassertedOptions{p2: "A:1-50", later: true}).run(ModeTimestamp, 129.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun")
	})
}

// A known limitation of the planner as it ships, which the review of issue
// #269 found: a replica copy whose predecessors are all under the witness's
// own server_uuid needs no anchor, so no record is consulted and nothing
// dates the copy's predecessors. In the lag window of the record of the
// episode that issued A:51-52 again, the shipped planner certifies the lost
// transactions although that record carries a promotion set which shows
// them to be older than its episode. (More than δ after the record the same
// record dates them and the target refuses: TestWitnessLostTailReissued.)
//
// With the containment anchor on, a witness that the record or its
// confirmation holds is held to the record and refuses; a witness that
// neither holds (committed within δ after the record, no confirmation) is
// certified as shipped.
func TestKnownLimitationOwnHistoryCopyNeedsNoAnchor(t *testing.T) {
	// The record at 74.2 s holds neither A:53 (75 s) nor a confirmation.
	eitherWay(t, func(t *testing.T) {
		p, err := reassertedInPlace(t, reassertedOptions{p2: "A:1-50"}).run(ModeTimestamp, 74.5, "", false)
		wantWrongPlan(t, "no anchor is needed, so nothing dates A:51-52", p, err, "A:53", "A:1-52", true)
		if len(p.Witness.AnchorRecords) != 0 || p.Witness.GroundTruth != GroundTruthAnchored {
			t.Fatalf("witness %+v, want a replica copy accepted without an anchor", p.Witness)
		}
	})
	// The record, observed at 76.2 s, holds A:53-54.
	held := reassertedOptions{p2: "A:1-50", sample: "A:1-54", at: 76.2}
	p, err := shippedRun(reassertedInPlace(t, held), 74.5, "")
	wantWrongPlan(t, "as shipped, the record that holds the witness is not consulted", p, err, "A:53", "A:1-52", true)
	withContainmentAnchors(t)
	_, err = reassertedInPlace(t, held).run(ModeTimestamp, 74.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"Its copy in b/b2 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#1",
		"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun")
}

// A known limitation of the time anchor that does not need a record without
// a promotion set (compare TestKnownLimitationTimeAnchorCertifiesReissuedGTID):
// the lost A:51-52 are in the backup, taken on the replica that kept them,
// and in no archived row (retention retired the rows that backup holds).
// The planner dates the archived copies of a copy's predecessors against a
// record's episode; it does not date the backup, although this one finished
// (60 s) before the episode that must have committed A:51-52 can have begun
// (69.2 s). Anchored by time by the reissuing episode's record, which
// carries its promotion set, the plan builds on that backup.
func TestKnownLimitationBackupCarriesReissuedGTID(t *testing.T) {
	o := reassertedOptions{p2: "A:1-50", onHolder: true, backup: "A:1-52", backupAt: 60}
	eitherWay(t, func(t *testing.T) {
		p, err := reassertedInPlace(t, o).run(ModeTimestamp, 81.5, "", false)
		wantWrongPlan(t, "anchored by time by a record with a promotion set; the backup holds the lost A:51-52", p, err, "A:60", "A:1-59", false)
		if len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#1" {
			t.Fatalf("anchored by %v, want a#1", p.Witness.AnchorRecords)
		}
	})
	// With the copies still archived the record dates them and the target
	// refuses.
	o.onHolder = false
	eitherWay(t, func(t *testing.T) {
		_, err := reassertedInPlace(t, o).run(ModeTimestamp, 81.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun")
	})
}
