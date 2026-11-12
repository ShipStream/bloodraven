package pitrplan

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// What an anchor says of the origin's history is held against the replica
// copy it accepts (review r5 of issue #269). An anchor proves that the
// origin had executed a set before the witness; the plan certifies the
// copy's whole predecessor set. The two differ after a loss:
//
//   - a server never applies through replication a transaction that
//     carries its own server_id, so an old primary that lost its last
//     transactions and rejoined never gets them back, and its copies of the
//     next primary's transactions follow less than that primary had;
//   - a server that lost transactions issues their GTIDs again, to other
//     transactions, and a replica that holds the lost ones skips the new
//     ones, so its copies of what the server commits next follow the lost
//     transactions, which that server no longer has.
//
// The checks that hold a copy to a time anchor are part of the shipped
// planner; those that hold it to a containment anchor run only with that
// anchor on (containmentAnchors). Each test here says which planner it
// asserts, and what the shipped one answers.

// eitherWay runs f with the containment anchor off, as the planner ships,
// and on: what f asserts does not depend on it.
func eitherWay(t *testing.T, f func(t *testing.T)) {
	t.Helper()
	shipped(func() { t.Run("shipped", f) })
	t.Run("containment anchors", func(t *testing.T) {
		withContainmentAnchors(t)
		f(t)
	})
}

// recP is a promotion record of (uuid, srv) observed at second at, with
// gtid_executed set, promotion set p ("" for none) and a 5 s probe interval.
func recP(uuid string, srv uint32, at float64, set, p string) pitrarchive.PromotionRecord {
	return pitrarchive.PromotionRecord{ServerUUID: uuid, ServerID: srv, GTIDExecuted: set, PromotionGTIDExecuted: p,
		ObservedAt: synthEpoch.Add(time.Duration(at * float64(time.Second))), ProbeInterval: pitrarchive.Duration(synthProbeInterval)}
}

// wantCertified asserts a plan with witness w and Λ ∖ X = lineage.
func wantCertified(t *testing.T, what string, p *Plan, err error, w, lineage string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if p.Witness.GTID.String() != w || !p.Lineage.Subtract(p.Excluded).Equal(MustParseSet(lineage)) {
		t.Fatalf("%s: witness %s Λ=%s X=%s, want %s with Λ∖X=%s", what, p.Witness.GTID, p.Lineage, p.Excluded, w, lineage)
	}
}

// lostTailReissued is a failback after an old primary lost its tail, with
// no clone, no reset and no manual step (MySQL 9.7: a receiver drops the
// events that carry its own server_id, and the server then issues the
// lowest free GTID numbers):
//
//	era 1  a (A, 101) is primary. A:21-50 are sealed and archived (a1).
//	       A:51 and A:52 (51 s, 52 s) reach b and are lost on a in a crash.
//	era 2  b (B, 102) is promoted at 60 s with A:1-52 and commits B:1-10
//	       (61..70 s); its files b1 (copies of A:21-52) and b2 are archived.
//	       a returns holding A:1-50, a subset of b's set, so it rejoins by
//	       replication. Its receiver drops A:51-52; it applies B:1-10.
//	era 3  b fails at 75 s. a is promoted at 80 s with the rotation
//	       acknowledged (promotion set A:1-50,B:1-10) and commits A:51' and
//	       A:52' (81 s, 82 s: the lost numbers, other transactions), then
//	       A:53.. (83 s..). Its record is observed at 84.2 s. b returns,
//	       already holds "A:51-52", skips the new ones and applies A:53-60
//	       (b3).
//	era 4  a fails before it seals anything of era 3; b is promoted at 95 s
//	       and archives b3.
//
// In era 3 the origin a had A:1-50, B:1-10, A:51', A:52'. The archive holds
// only the lost A:51 and A:52 (b1). evidence says whether a's era-3 record
// carries its promotion set.
func lostTailReissued(t *testing.T, evidence bool) *synth {
	s := newSynth(t)
	p := ""
	if evidence {
		p = "A:1-50,B:1-10"
	}
	s.site("a").Promotions = append(s.site("a").Promotions,
		recP("A", 101, 10, "A:1-20", ""),
		recP("A", 101, 84.2, "A:1-54,B:1-10", p))
	s.site("b").Promotions = append(s.site("b").Promotions,
		recP("B", 102, 60.5, "A:1-52", ""),
		recP("B", 102, 95.5, "A:1-60,B:1-10", ""))
	s.file("a", "a1", 101, "A:1-20", "", run("A", 21, 50, 21, 101))
	s.file("b", "b1", 102, "A:1-20", "", run("A", 21, 52, 21, 101))
	s.file("b", "b2", 102, "A:1-52", "", run("B", 1, 10, 61, 102))
	s.file("b", "b3", 102, "A:1-52,B:1-10", "", run("A", 53, 60, 83, 101))
	s.file("b", "b4", 102, "A:1-60,B:1-10", "", run("B", 11, 20, 96, 102))
	s.backup("A:1-45", "a", 101, 45.5)
	return s
}

// Finding 1: the copy of the first transaction after the reissued ones
// follows the lost transactions. With the promotion set every such target
// refuses, in the lag window and after it, and names the transaction that
// dates the loss; the rows of other eras are not read for it. The shipped
// planner refuses the lag-window targets as it did before issue #269 (no
// record anchors them), and the target the record anchors by time through
// the same dating.
func TestWitnessLostTailReissued(t *testing.T) {
	withContainmentAnchors(t)
	targets := []struct {
		what    string
		stop    float64
		witness string
	}{
		{"after a's promotion, before the reissued A:51", 80.5, "A:53"},
		{"after the reissued A:52, before A:53", 82.5, "A:53"},
		{"before A:54, which the record holds", 83.5, "A:54"},
		{"before A:60, anchored by time", 89.5, "A:60"},
	}
	for _, tt := range targets {
		t.Run(tt.what, func(t *testing.T) {
			s := lostTailReissued(t, true)
			_, err := s.run(ModeTimestamp, tt.stop, "", false)
			wantToken(t, err, pitrarchive.TokenStopAmbiguous,
				tt.witness+", is archived only as a replica copy. Its copy in b/b3 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#1 (observed at 2026-11-01T12:01:24.200000Z), so the origin committed it in that record's episode or later",
				"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun (2026-11-01T12:01:19.200000Z, the record's probe interval of 5s before it was observed): the GTID was issued twice",
				"Exclude A:51-52 (the restore then holds none of them)")
			if s.fetches["b/b2"] != 0 || s.fetches["b/b4"] != 0 {
				t.Errorf("rows read: %v; b2 and b4 hold no predecessor to date", s.fetches)
			}
			// Excluding the lost transactions restores the history without
			// either transaction under their GTIDs.
			p, err := lostTailReissued(t, true).run(ModeTimestamp, tt.stop, "A:51-52", false)
			n := mustGTID(t, tt.witness).GNO
			lineage := "A:1-50,B:1-10"
			if n > 53 {
				lineage += fmt.Sprintf(",A:53-%d", n-1)
			}
			wantCertified(t, "lost transactions excluded", p, err, tt.witness, lineage)
			if segs := segString(p); strings.Contains(segs, "A:51") || strings.Contains(segs, "-52]") {
				t.Fatalf("the plan replays a lost transaction: %s", segs)
			}
		})
	}

	// A record without a promotion set anchors nothing in its lag window,
	// as before issue #269.
	for _, tt := range targets[:3] {
		_, err := lostTailReissued(t, false).run(ModeTimestamp, tt.stop, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "does not anchor "+tt.witness+" by containment: it carries no promotion set")
	}

	// As shipped: the first three targets lie in the lag window of a's
	// record, which anchors nothing there, whatever it carries and whatever
	// is excluded.
	for _, tt := range targets[:3] {
		for _, evidence := range []bool{true, false} {
			for _, exclude := range []string{"", "A:51-52"} {
				wantShippedRefusal(t, tt.what, lostTailReissued(t, evidence), tt.stop, exclude, tt.witness+", is archived only as a replica copy on b. Its predecessors include B:1 ",
					"or choose a target after 2026-11-01T12:01:29.200000Z (a's promotion record at 2026-11-01T12:01:24.200000Z plus the 5s clock-skew margin)")
			}
		}
	}
	// The fourth is anchored by time, by a record that carries its
	// promotion set, and the dating refuses it as above.
	_, err := shippedRun(lostTailReissued(t, true), 89.5, "")
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"A:60, is archived only as a replica copy. Its copy in b/b3 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#1 (observed at 2026-11-01T12:01:24.200000Z)",
		"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun", "the GTID was issued twice",
		"Exclude A:51-52 (the restore then holds none of them)")
	p, err := shippedRun(lostTailReissued(t, true), 89.5, "A:51-52")
	wantCertified(t, "as shipped, lost transactions excluded", p, err, "A:60", "A:1-50,A:53-59,B:1-10")
	// Without the promotion set nothing dates them: see
	// TestKnownLimitationTimeAnchorCertifiesReissuedGTID.
}

// A known limitation that is older than issue #269 and that this planner
// still has, with containment anchors off and on: THE PLANS ASSERTED HERE
// ARE WRONG. They are pinned so that closing the gap is a deliberate change
// and so that nobody takes it for closed.
//
// A server_uuid issues a GTID twice when its server loses transactions its
// replica already holds and is made writable again without a clone:
// lostTailReissued. The operator reclones such a server when a site it can
// read holds the lost transactions, so this history now needs one of the
// cases it cannot see (site docs, failover, "What this cannot see"): the
// server rejoined by replication under an operator from before that check
// and was promoted before the replica audit compared it, it was made
// writable while no site that holds the lost transactions answered
// (reassertedInPlace, witness_residual_test.go), or the GTIDs were issued
// again before the upgrade. The archive holds only the first, lost issue of
// A:51-52, on the replica. A target that the promotion record of the
// reissuing episode anchors by time is certified when nothing dates that
// episode: its record carries no promotion set (a sidecar from before issue
// #269, a sidecar that started while the server was already writable,
// binlog_encryption=ON, an unreadable binlog header). The plan then replays
// the lost A:51-52, where the origin had the reissued ones. When the record
// carries a promotion set the same targets refuse
// (TestWitnessLostTailReissued, TestWitnessLostTailReissuedInAnEarlierEpisode).
//
// The operator's reclone of a returning site that lacks its own
// transactions narrows this to those cases; it does not close it, and
// nothing in the archive tells them apart. Closing it here needs the
// planner to refuse a time anchor while a record of the witness's
// server_uuid carries no promotion set between the oldest transaction the
// plan relies on under that server_uuid and the witness, which refuses
// every such target of an archive written before promotion sets (site
// docs, known limitations, "GTIDs issued twice").
func TestKnownLimitationTimeAnchorCertifiesReissuedGTID(t *testing.T) {
	eitherWay(t, func(t *testing.T) {
		// The record of the reissuing episode is the anchor.
		p, err := lostTailReissued(t, false).run(ModeTimestamp, 89.5, "", false)
		wantCertified(t, "WRONG, pinned: anchored by the reissuing episode's record, which has no promotion set", p, err, "A:60", "A:1-59,B:1-10")
		if segs := segString(p); !strings.Contains(segs, "[b/b1: A:46-52]") {
			t.Fatalf("the pinned plan no longer replays the lost A:51-52 from b/b1: %s", segs)
		}
		// The reissue lies an episode back: a later record anchors, it holds
		// A:51-52 in its own promotion set, and the record of the episode
		// that issued them again has none.
		s := laterEpisode(t, "")
		p, err = s.run(ModeTimestamp, 129.5, "", false)
		wantCertified(t, "WRONG, pinned: the reissuing episode's record, an episode back, has no promotion set", p, err, "A:70", "A:1-69,B:1-20")
		if segs := segString(p); !strings.Contains(segs, "[b/b1: A:46-52]") {
			t.Fatalf("the pinned plan no longer replays the lost A:51-52 from b/b1: %s", segs)
		}
	})
}

// laterEpisode is lostTailReissued followed by another round: b is primary
// again (era 4) and a is promoted a third time at 120 s (era 5), commits
// A:61-70 (121 s..) and is recorded at 124.2 s holding A:61-63. era3 is the
// promotion set of the record of era 3, the episode that issued A:51-52
// again ("" for none).
func laterEpisode(t *testing.T, era3 string) *synth {
	s := lostTailReissued(t, true)
	s.site("a").Promotions[1].PromotionGTIDExecuted = era3
	s.site("a").Promotions = append(s.site("a").Promotions, recP("A", 101, 124.2, "A:1-63,B:1-20", "A:1-60,B:1-20"))
	s.file("b", "b5", 102, "A:1-60,B:1-20", "", run("A", 61, 70, 121, 101))
	return s
}

// The reissue may lie an episode back (laterEpisode): the record of a's
// third promotion holds A:51-52 in its promotion set, as the transactions a
// issued in era 3, so that record dates nothing about them. The record of
// era 3 still does: a committed everything under its server_uuid outside
// that record's promotion set from era 3 on, and the archive's A:51 is
// older. That holds for a target the latest record anchors by time, as
// shipped, and for one in its lag window, with containment anchors.
func TestWitnessLostTailReissuedInAnEarlierEpisode(t *testing.T) {
	build := func(era3 string) *synth { return laterEpisode(t, era3) }
	// A:70 commits at 130 s, more than δ after the record of era 5.
	eitherWay(t, func(t *testing.T) {
		_, err := build("A:1-50,B:1-10").run(ModeTimestamp, 129.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous,
			"A:70, is archived only as a replica copy. Its copy in b/b5 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#1 (observed at 2026-11-01T12:01:24.200000Z)",
			"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun",
			"Exclude A:51-52 (the restore then holds none of them)")
		p, err := build("A:1-50,B:1-10").run(ModeTimestamp, 129.5, "A:51-52", false)
		wantCertified(t, "lost transactions excluded", p, err, "A:70", "A:1-50,A:53-69,B:1-20")
	})
	// As shipped, a target in the lag window of the latest record refuses
	// as before issue #269, whatever the record of era 3 carries.
	for _, era3 := range []string{"A:1-50,B:1-10", ""} {
		wantShippedRefusal(t, "lag window of era 5", build(era3), 121.5, "", "A:62, is archived only as a replica copy on b. Its predecessors include B:1 ",
			"or choose a target after 2026-11-01T12:02:09.200000Z (a's promotion record at 2026-11-01T12:02:04.200000Z plus the 5s clock-skew margin)")
	}

	// With containment anchors the lag-window target is held to the record
	// of era 3 in the same way.
	withContainmentAnchors(t)
	_, err := build("A:1-50,B:1-10").run(ModeTimestamp, 121.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"A:62, is archived only as a replica copy. Its copy in b/b5 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#1 (observed at 2026-11-01T12:01:24.200000Z)",
		"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun",
		"Exclude A:51-52 (the restore then holds none of them)")
	p, err := build("A:1-50,B:1-10").run(ModeTimestamp, 121.5, "A:51-52", false)
	wantCertified(t, "lost transactions excluded", p, err, "A:62", "A:1-50,A:53-61,B:1-20")
	// When the record of era 3 has no promotion set nothing dates them, and
	// the plan replays the lost A:51-52. THIS PLAN IS WRONG, and only a
	// containment anchor certifies it (the shipped planner refuses it,
	// above): one of the two reasons the anchor is off (containmentAnchors).
	p, err = build("").run(ModeTimestamp, 121.5, "", false)
	wantCertified(t, "WRONG, pinned: era 3 recorded without a promotion set", p, err, "A:62", "A:1-61,B:1-20")
}

// lostTailOmitted is steps 1-3 of lostTailReissued, but b fails seconds
// after its promotion, before it uploads its own era. a is promoted again
// and archives its file a2, which holds copies of B:1-10 after A:1-50 only,
// and then a's next era, in which it issues A:51.. again. p is the
// promotion set of b's record (observed at 64.2 s, holding B:1-3); sealed
// says whether b's file from before its promotion is archived.
func lostTailOmitted(t *testing.T, p string, sealed bool) *synth {
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions, recP("A", 101, 10, "A:1-20", ""))
	s.site("b").Promotions = append(s.site("b").Promotions, recP("B", 102, 64.2, "A:1-52,B:1-3", p))
	s.file("a", "a1", 101, "A:1-20", "", run("A", 21, 50, 21, 101))
	s.file("a", "a2", 101, "A:1-50", "", run("B", 1, 10, 61, 102), run("A", 51, 60, 80, 101))
	if sealed {
		s.file("b", "b1", 102, "A:1-40", "", run("A", 41, 52, 41, 101))
	}
	s.backup("A:1-45", "a", 101, 45.5)
	return s
}

// Finding 2: the copy does not follow transactions the anchor proves the
// origin had executed. It refuses after the lag window (the whole record
// holds them), also for a record without a promotion set, and that is part
// of the shipped planner; with containment anchors it refuses in the lag
// window too (the promotion set holds them), where the shipped planner
// refuses as before issue #269.
func TestWitnessLostTailOmitted(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(fmt.Sprintf("sealed=%v", sealed), func(t *testing.T) {
			// More than δ after b's record the whole record anchors, and
			// holds the transactions a's copy does not follow.
			eitherWay(t, func(t *testing.T) {
				for _, p := range []string{"A:1-52", ""} {
					_, err := lostTailOmitted(t, p, sealed).run(ModeTimestamp, 69.5, "", false)
					wantToken(t, err, pitrarchive.TokenStopAmbiguous,
						"B:10, is archived only as a replica copy. Its copy in a/a2 does not follow A:51-52, which B:10's origin had executed before it: the promotion record b#0, observed at 2026-11-01T12:01:04.200000Z, before B:10, holds them",
						"an old primary that lost its last transactions and rejoined does not get them back",
						"Exclude A:51-52 (the restore then holds none of them)")
					// Excluding them restores the history without them. When
					// b's file is archived the archive holds both transactions
					// that were issued as A:51, and says so.
					plan, err := lostTailOmitted(t, p, sealed).run(ModeTimestamp, 69.5, "A:51-52", false)
					if sealed {
						wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:51 differs between b/b1 and a/a2", "the GTID was reissued")
						continue
					}
					wantCertified(t, "lost transactions excluded", plan, err, "B:10", "A:1-50,B:1-9")
				}
			})
			// As shipped, b's record anchors nothing in its lag window.
			for _, exclude := range []string{"", "A:51-52"} {
				wantShippedRefusal(t, "lag window", lostTailOmitted(t, "A:1-52", sealed), 61.5, exclude, "B:2, is archived only as a replica copy on a. Its predecessors include A:1 ", "in the window")
			}

			withContainmentAnchors(t)
			_, err := lostTailOmitted(t, "A:1-52", sealed).run(ModeTimestamp, 61.5, "", false)
			wantToken(t, err, pitrarchive.TokenStopAmbiguous,
				"B:2, is archived only as a replica copy. Its copy in a/a2 does not follow A:51-52, which B:2's origin had executed before it: they are in the promotion set (promotionGtidExecuted) of the promotion record b#0, observed at 2026-11-01T12:01:04.200000Z",
				"an old primary that lost its last transactions and rejoined does not get them back",
				"Exclude A:51-52 (the restore then holds none of them)")
			if r, _ := RefusalOf(err); strings.Contains(r.Error(), "clocks disagree") {
				t.Errorf("the refusal blames the clocks: %v", err)
			}
			// Excluding them restores the history without them. When b's
			// file is archived the archive holds both transactions that
			// were issued as A:51, and says so.
			p, err := lostTailOmitted(t, "A:1-52", sealed).run(ModeTimestamp, 61.5, "A:51-52", false)
			if sealed {
				wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:51 differs between b/b1 and a/a2", "the GTID was reissued")
				return
			}
			wantCertified(t, "lost transactions excluded", p, err, "B:2", "A:1-50,B:1")
		})
	}
	// A record without a promotion set refuses in its lag window as before.
	withContainmentAnchors(t)
	_, err := lostTailOmitted(t, "", false).run(ModeTimestamp, 61.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "does not anchor B:2 by containment: it carries no promotion set")
}

// A promotion set that is smaller than what the server had executed when
// it was promoted (the promotion-time rotation was not acknowledged, and
// A:51-52 arrived after the sidecar's last read-only observation) proves
// nothing about A:51-52: the record's sample holds them and B:2, in no
// order. A copy that follows them is refused as unexplained
// (TestWitnessContainmentLag); a copy that does not follow them, as the old
// primary's does after it lost them, may be missing predecessors.
func TestWitnessContainmentUnorderedPredecessors(t *testing.T) {
	withContainmentAnchors(t)
	wantShippedRefusal(t, "lag window", lostTailOmitted(t, "A:1-50", false), 61.5, "", "in the window")
	_, err := lostTailOmitted(t, "A:1-50", false).run(ModeTimestamp, 61.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"The promotion record b#0 (observed at 2026-11-01T12:01:04.200000Z) would anchor it by containment, but its gtidExecuted holds A:51-52 outside its promotion set (promotionGtidExecuted)",
		"nothing orders them against B:2. Its copy in a/a2 does not follow them, so it may be missing predecessors of B:2",
		"Exclude A:51-52 (the restore then holds none of them)")
	p, err := lostTailOmitted(t, "A:1-50", false).run(ModeTimestamp, 61.5, "A:51-52", false)
	wantCertified(t, "excluded", p, err, "B:2", "A:1-50,B:1")

	// The same through a confirmation: the record's sample is the promotion
	// set, and the server, still replicating, applied A:51-52 before the
	// confirmation was read.
	s := newSynth(t)
	s.site("b").Promotions = append(s.site("b").Promotions, cConfirm(recP("B", 102, 60.5, "A:1-50", "A:1-50"), 70.6, "A:51-52,B:1-10"))
	s.file("a", "a2", 101, "A:1-50", "", run("B", 1, 10, 61, 102))
	s.backup("A:1-50", "a", 101, 50.5)
	_, err = s.run(ModeTimestamp, 61.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "would anchor it by containment, but its confirmation holds A:51-52 outside its promotion set")
	wantShippedRefusal(t, "within the margin of the record", s, 61.5, "", "which is not provably before B:2")
	// Without them the confirmation orders nothing but the episode's own
	// transactions, and the record anchors.
	s.site("b").Promotions[0] = cConfirm(recP("B", 102, 60.5, "A:1-50", "A:1-50"), 70.6, "B:1-10")
	p, err = s.run(ModeTimestamp, 61.5, "", false)
	wantCertified(t, "confirmation of the episode's own transactions", p, err, "B:2", "A:1-50,B:1")
	wantShippedRefusal(t, "within the margin of the confirmed record", s, 61.5, "", "which is not provably before B:2")
}

// The sample that holds w was read after w committed, so it holds every
// predecessor of w on its origin. A copy that follows a transaction the
// sample lacks is not that history, whatever its server_uuid: here the
// replica holds A:70, which the origin lost and has not issued again.
func TestWitnessContainmentCopyBeyondSample(t *testing.T) {
	withContainmentAnchors(t)
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions, lagRecord())
	s.file("b", "b1", 102, "A:1-50,A:70,D:1", "", run("A", 51, 60, 51, 101))
	s.backup("A:1-50,A:70,D:1", "b", 102, 50.8)
	_, err := s.run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"Its copy in b/b1 follows A:70, which the gtidExecuted of the promotion record a#0 lacks",
		"the origin had not executed them before A:53", "Exclude A:70 (the restore then holds none of them)")
	wantShippedRefusal(t, "lag window", s, 52.5, "", "predecessors include D:1", "in the window")
	// More than δ after the record nothing samples the witness, and the
	// time rule accepts the copy as before.
	p, err := s.run(ModeTimestamp, 59.5, "", false)
	wantCertified(t, "time anchor", p, err, "A:60", "A:1-59,A:70,D:1")
	wantShippedSame(t, "time anchor", s, 59.5, "", answer(p, nil))
}

// episodeArchive is containmentArchive with the commit times of A:51 and
// A:52 chosen by the test: the predecessors of A:53 (53 s) that the
// promotion set of a's record does not hold.
func episodeArchive(t *testing.T, rec pitrarchive.PromotionRecord, at51, at52 float64) *synth {
	t.Helper()
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions, rec)
	s.file("b", "b1", 102, "A:1-50,D:1", "",
		[]stx{{g: "A:51", oct: sec(at51), srv: 101}, {g: "A:52", oct: sec(at52), srv: 101}}, run("A", 53, 100, 53, 101))
	s.backup("A:1-50,D:1", "b", 102, 40)
	return s
}

// The predecessors of w that the promotion set does not hold are dated
// like w: inside the record's episode, on the record's clock, with no
// margin.
func TestWitnessContainmentEpisodePredecessors(t *testing.T) {
	// The record as a time anchor (for A:60, more than δ after it) dates its
	// episode's transactions, with no upper bound: the shipped planner's
	// check. a's record was observed at 54.2 s with a 5 s probe interval, so
	// its episode began at 49.2 s at the earliest.
	eitherWay(t, func(t *testing.T) {
		_, err := episodeArchive(t, lagRecord(), 49.199999, 52).run(ModeTimestamp, 59.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "A:60, is archived only as a replica copy. Its copy in b/b1 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#0",
			"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:49.199999Z, before that episode can have begun (2026-11-01T12:00:49.200000Z",
			"Exclude A:51 (the restore then holds none of them)")
		p, err := episodeArchive(t, lagRecord(), 49.2, 52).run(ModeTimestamp, 59.5, "", false)
		wantCertified(t, "committed at the episode's start", p, err, "A:60", "A:1-59,D:1")
		// A predecessor committed after the record was observed is in its
		// episode still.
		p, err = episodeArchive(t, lagRecord(), 51, 54.3).run(ModeTimestamp, 59.5, "", false)
		wantCertified(t, "committed after the record", p, err, "A:60", "A:1-59,D:1")
		// A record without a promotion set dates nothing.
		bare := lagRecord()
		bare.PromotionGTIDExecuted = ""
		p, err = episodeArchive(t, bare, 30, 52).run(ModeTimestamp, 59.5, "", false)
		wantCertified(t, "no promotion set", p, err, "A:60", "A:1-59,D:1")
		// A row that may hold a copy and straddles the episode's start must
		// be read; when it cannot be, nothing dates that copy.
		s := episodeArchive(t, lagRecord(), 51, 52)
		lost := s.file("c", "c1", 103, "A:1-49,D:1", "", []stx{{g: "A:50", oct: sec(40), srv: 101}, {g: "A:51", oct: sec(51), srv: 101}})
		s.lost[lost] = true
		_, err = s.run(ModeTimestamp, 59.5, "", false)
		wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "which may hold a copy of A:51 (a predecessor of the witness candidate A:60 whose commit time must lie in the episode of the promotion record a#0)")
	})

	// With containment anchors the same holds in the record's lag window,
	// with the sample that holds the witness as the upper bound.
	withContainmentAnchors(t)
	wantContained(t, "at the episode's start", episodeArchive(t, lagRecord(), 49.2, 52), 52.5, "A:53", "A:1-52,D:1", "a#0")
	_, err := episodeArchive(t, lagRecord(), 49.199999, 52).run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"Its copy in b/b1 follows A:51, which is outside the promotion set (promotionGtidExecuted) of the promotion record a#0",
		"Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:49.199999Z, before that episode can have begun (2026-11-01T12:00:49.200000Z",
		"Exclude A:51 (the restore then holds none of them)")
	// A predecessor committed after the sample that holds w.
	_, err = episodeArchive(t, lagRecord(), 51, 54.3).run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous,
		"Yet the copy of A:52 in b/b1 was committed at 2026-11-01T12:00:54.300000Z, after the sample that holds A:53 (2026-11-01T12:00:54.200000Z)")
	// The shipped planner does not get as far as the dates there.
	for _, at := range [][2]float64{{49.199999, 52}, {51, 54.3}} {
		wantShippedRefusal(t, "lag window", episodeArchive(t, lagRecord(), at[0], at[1]), 52.5, "", "predecessors include D:1", "in the window")
	}
	wantContained(t, "time anchor, episode's start", episodeArchive(t, lagRecord(), 49.2, 52), 59.5, "A:60", "A:1-59,D:1", "a#0")
	// A record without a promotion set dates nothing.
	bare := lagRecord()
	bare.PromotionGTIDExecuted = ""
	wantContained(t, "time anchor without a promotion set", episodeArchive(t, bare, 30, 52), 59.5, "A:60", "A:1-59,D:1", "a#0")

	// Another copy of the predecessor, on a third site, is dated too.
	s := episodeArchive(t, lagRecord(), 51, 52)
	s.file("c", "c1", 103, "A:1-50,D:1", "", []stx{{g: "A:51", oct: sec(51), srv: 101}})
	wantContained(t, "second copy inside", s, 52.5, "A:53", "A:1-52,D:1", "a#0")
	// A row that may hold a copy and straddles the episode's start must be
	// read; when it cannot be, nothing dates that copy.
	s = episodeArchive(t, lagRecord(), 51, 52)
	lost := s.file("c", "c1", 103, "A:1-49,D:1", "", []stx{{g: "A:50", oct: sec(40), srv: 101}, {g: "A:51", oct: sec(51), srv: 101}})
	s.lost[lost] = true
	_, err = s.run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "which may hold a copy of A:51 (a predecessor of the witness candidate A:53 whose commit time must lie in the episode of the promotion record a#0)")
	wantShippedRefusal(t, "lag window, a row that cannot be read", s, 52.5, "", "predecessors include D:1", "in the window")
}

// episodeDates reads the hints first: a row whose hinted commit times lie
// inside the episode is not downloaded, one that straddles its start or has
// no time hints is, and once a copy from before the episode has been read,
// rows that lie wholly before it are counted from their hints.
func TestEpisodeDatesReadsHintsFirst(t *testing.T) {
	build := func() (*synth, *planner, occurrence) {
		s := newSynth(t)
		s.site("a").Promotions = append(s.site("a").Promotions, cRec(54.2, "A:1-60,D:1", "A:1-50,D:1"))
		s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 52, 51, 101))
		s.file("b", "b2", 102, "A:1-52,D:1", "", run("A", 53, 60, 53, 101))
		s.backup("A:1-50,D:1", "b", 102, 40)
		return s, nil, occurrence{}
	}
	plannerOf := func(s *synth) (*planner, occurrence) {
		var a Archive
		for _, name := range s.order {
			a.Sites = append(a.Sites, *s.sites[name])
		}
		s.fetches = map[string]int{}
		fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
			s.fetches[r.Site+"/"+r.Name]++
			return s.idx[r], nil
		})
		p, err := newPlanner(context.Background(), Request{Mode: ModeTimestamp, Stop: time.UnixMicro(sec(53.5)).UTC(), Dump: s.dump, Budget: NewBudget(64 << 20)}, a, fetch)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range p.rows {
			if r.Name == "b2" {
				f, err := p.fetch(r)
				if err != nil {
					t.Fatal(err)
				}
				return p, occurrence{f, 1} // A:54
			}
		}
		t.Fatal("no row b2")
		return nil, occurrence{}
	}
	own := MustParseSet("A:51-53")
	w := mustGTID(t, "A:54")

	s, _, _ := build()
	p, o := plannerOf(s)
	if err := p.episodeDates(w, o, &p.promos[0], own, sec(54.2)); err != nil {
		t.Fatal(err)
	}
	if s.fetches["b/b1"] != 0 {
		t.Fatalf("b1 lies inside the episode by its hints and was downloaded: %v", s.fetches)
	}

	// Further copies: c1 inside (not read), c2 without time hints (read), c3
	// across the episode's start (read; its copy of A:51 is inside).
	s, _, _ = build()
	s.file("c", "c1", 103, "A:1-50,D:1", "", run("A", 51, 53, 51, 101))
	s.file("d", "d1", 104, "A:1-50,D:1", "", run("A", 51, 52, 51, 101))
	s.sites["d"].Rows[0].Hints.Min, s.sites["d"].Rows[0].Hints.Max = 0, 0
	s.file("e", "e1", 105, "A:1-49,D:1", "", []stx{{g: "A:50", oct: sec(40), srv: 101}, {g: "A:51", oct: sec(51), srv: 101}})
	p, o = plannerOf(s)
	if err := p.episodeDates(w, o, &p.promos[0], own, sec(54.2)); err != nil {
		t.Fatal(err)
	}
	if s.fetches["c/c1"] != 0 || s.fetches["d/d1"] != 1 || s.fetches["e/e1"] != 1 {
		t.Fatalf("rows read: %v, want d1 and e1 and not c1", s.fetches)
	}

	// Three rows from before the episode hold copies of A:51 (an earlier
	// transaction under that GTID): the first is read, to name the copy and
	// its commit time, the others are counted from their hints.
	s, _, _ = build()
	for i, site := range []string{"c", "d", "e"} {
		s.file(site, site+"1", uint32(103+i), "A:1-50,D:1", "", []stx{{g: "A:51", oct: sec(20 + float64(i)), srv: 101}, {g: fmt.Sprintf("A:%d", 52+i), oct: sec(21 + float64(i)), srv: 101}})
	}
	p, o = plannerOf(s)
	err := p.episodeDates(w, o, &p.promos[0], own, math.MaxInt64)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "Yet the copy of A:51 in c/c1 was committed at 2026-11-01T12:00:20.000000Z", "Exclude A:51-53 (the restore then holds none of them)")
	if s.fetches["c/c1"] != 1 || s.fetches["d/d1"] != 0 || s.fetches["e/e1"] != 0 {
		t.Fatalf("rows read: %v, want c1 alone of the rows before the episode", s.fetches)
	}
}

// A confirmation is a sample of its record's episode. One that holds w
// although the archived w was committed after it was read saw another
// transaction: the record does not anchor w by time, and w refuses.
//
// The refusal is outright. Setting the record aside alone would leave the
// plan to the other copies and anchors, and they can certify what the
// planner refuses with the record: the last case here has two copies that
// the record shows to disagree, and without the record one of them passes.
// This is part of the shipped planner.
func TestWitnessAnchorConfirmationBeforeWitness(t *testing.T) {
	// a's record at 40 s holds A:1-50 and D:1; its confirmation at
	// confirmedAt holds A:51-52. b's copies follow D:1; with other, site c
	// archived copies too, which do not.
	build := func(confirmedAt float64, other bool) *synth {
		s := newSynth(t)
		s.site("a").Promotions = append(s.site("a").Promotions, cConfirm(recP("A", 101, 40, "A:1-50,D:1", "A:1-50,D:1"), confirmedAt, "A:51-52"))
		s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 100, 51, 101))
		if other {
			s.file("c", "c1", 103, "A:1-50", "", run("A", 51, 100, 51, 101))
		}
		s.backup("A:1-50,D:1", "b", 102, 39)
		return s
	}
	const contradicted = "A:52, is archived only as a replica copy. The confirmation of the promotion record a#0, observed at 2026-11-01T12:00:50.200000Z, holds A:52, yet the archived A:52 was committed at 2026-11-01T12:00:52.000000Z, after it: the archived transaction is not the one A:52's origin had executed then (the GTID was issued twice, or the server's clock was set back in between)"
	eitherWay(t, func(t *testing.T) {
		_, err := build(50.2, false).run(ModeTimestamp, 51.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, contradicted, "If the GTID was issued twice, start a new archive prefix and take a full backup")
		// A confirmation read after w committed is no contradiction.
		wantContained(t, "confirmation after w", build(52, false), 51.5, "A:52", "A:1-51,D:1", "a#0")
		// The next witness is beyond the confirmation, and anchored as ever.
		wantContained(t, "a witness the confirmation does not hold", build(50.2, false), 52.5, "A:53", "A:1-52,D:1", "a#0")

		// A copy that needs no anchor refuses too: here every predecessor
		// is the origin's own, and the record still says that the archived
		// A:52 is not the transaction its server had.
		own := newSynth(t)
		own.site("a").Promotions = append(own.site("a").Promotions, cConfirm(recP("A", 101, 40, "A:1-50", "A:1-50"), 50.2, "A:51-52"))
		own.file("b", "b1", 102, "A:1-50", "", run("A", 51, 100, 51, 101))
		own.backup("A:1-50", "b", 102, 39)
		_, err = own.run(ModeTimestamp, 51.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, contradicted)

		// Two copies. With the record as anchor they disagree about D:1,
		// which the record holds, and the plan refuses. The contradicted
		// record must not turn that into a plan from c's copy alone.
		_, err = build(52, true).run(ModeTimestamp, 51.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "Its copy in c/c1 does not follow D:1, which A:52's origin had executed before it")
		_, err = build(50.2, true).run(ModeTimestamp, 51.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, contradicted)
	})
}

// One sidecar observes a server without a gap. Records of the same
// (server_uuid, server_id) in two sites' manifests are not one such
// observation, so none of them anchors by containment, whichever holds w
// and whenever the other was observed. server_id is assigned per site, so
// the operator's pods cannot write this; a restored volume could.
func TestWitnessContainmentSameServerOnTwoSites(t *testing.T) {
	withContainmentAnchors(t)
	later := recP("A", 101, 57, "A:1-54,E:1", "A:1-50,E:1")
	build := func(other pitrarchive.PromotionRecord) *synth {
		s := newSynth(t)
		s.site("c").Promotions = append(s.site("c").Promotions, other)
		s.site("a").Promotions = append(s.site("a").Promotions, later)
		s.file("b", "b1", 102, "A:1-50", "", []stx{{g: "E:1", oct: sec(52.4), srv: 102}}, run("A", 53, 100, 53, 101))
		s.backup("A:1-50", "b", 102, 50.5)
		return s
	}
	// Without the other site's record a#0 anchors A:53 and explains E:1.
	s := build(pitrarchive.PromotionRecord{})
	s.sites["c"].Promotions = nil
	wantContained(t, "one site", s, 52.5, "A:53", "A:1-50,E:1", "a#0")
	reset := recP("A", 101, 55, "A:1-50,E:1", "")
	reset.Kind = pitrarchive.RecordReset
	for name, other := range map[string]pitrarchive.PromotionRecord{
		"promotion record long before":     recP("A", 101, 40, "A:1-50", ""),
		"promotion record around w":        recP("A", 101, 53.5, "A:1-50", ""),
		"promotion record after the other": recP("A", 101, 62.1, "A:1-50", ""),
		"record that holds w, later":       recP("A", 101, 5000, "A:1-60,E:1", ""),
		"reset record":                     reset,
	} {
		_, err := build(other).run(ModeTimestamp, 52.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include E:1",
			"does not anchor A:53 by containment: c#0, observed at", "is a record of the same server (server_uuid and server_id) on another site")
		wantShippedRefusal(t, name, build(other), 52.5, "", "predecessors include E:1")
		if t.Failed() {
			t.Fatalf("%s", name)
		}
	}
}

// A sidecar appends its records as it observes them. A manifest whose
// records are not in the order of their times around the anchor shows that
// the sidecar's clock was set back in between, and then the record's time
// dates nothing: here the record written second carries the earlier time
// and would otherwise pass for the earliest holder of A:53.
func TestWitnessContainmentRecordOrder(t *testing.T) {
	withContainmentAnchors(t)
	build := func(records ...pitrarchive.PromotionRecord) *synth {
		s := newSynth(t)
		s.site("a").Promotions = append(s.site("a").Promotions, records...)
		s.file("b", "b1", 102, "A:1-50", "", []stx{{g: "E:1", oct: sec(52.4), srv: 102}}, run("A", 53, 100, 53, 101))
		s.backup("A:1-50", "b", 102, 50.5)
		return s
	}
	holder := recP("A", 101, 54.2, "A:1-54,E:1", "A:1-50,E:1")
	wantContained(t, "alone", build(holder), 52.5, "A:53", "A:1-50,E:1", "a#0")
	_, err := build(recP("A", 101, 70, "A:1-54", "A:1-50"), holder).run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include E:1",
		"the promotion record a#1 (observed at 2026-11-01T12:00:54.200000Z) does not anchor A:53 by containment: site a's records are not in the order they were observed (a#0 was observed at 2026-11-01T12:01:10.000000Z)")
	_, err = build(holder, recP("A", 101, 30, "A:1-30", "")).run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "site a's records are not in the order they were observed (a#1 was observed at 2026-11-01T12:00:30.000000Z)")
	for _, s := range []*synth{build(recP("A", 101, 70, "A:1-54", "A:1-50"), holder), build(holder, recP("A", 101, 30, "A:1-30", ""))} {
		wantShippedRefusal(t, "records out of order", s, 52.5, "", "predecessors include E:1")
	}
}

// An earlier episode's record whose confirmation holds w is the earliest
// holder of w: a later record that holds w as well is of a later episode
// and anchors nothing by containment.
func TestWitnessContainmentEarlierHolderByConfirmation(t *testing.T) {
	withContainmentAnchors(t)
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions,
		cConfirm(recP("A", 101, 40, "A:1-50", "A:1-50"), 52, "A:51"),
		recP("A", 101, 55.9, "A:1-52,E:1", "A:1-50,E:1"))
	s.file("b", "b1", 102, "A:1-50", "", []stx{{g: "E:1", oct: sec(50.95), srv: 102}}, run("A", 51, 100, 51, 101))
	s.backup("A:1-50", "b", 102, 49.5)
	msg := wantRefusedE1(t, s, 50.97)
	if strings.Contains(msg, "a#1 (observed at 2026-11-01T12:00:55.900000Z) does not anchor") && !strings.Contains(msg, "because") {
		t.Fatalf("the later holder is in question without a fence: %s", msg)
	}
}

// wantRefusedE1 asserts PITRStopAmbiguous for the unexplained E:1, from
// the planner with containment anchors and from the shipped one.
func wantRefusedE1(t *testing.T, s *synth, stop float64) string {
	t.Helper()
	_, err := s.run(ModeTimestamp, stop, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include E:1")
	wantShippedRefusal(t, "unexplained E:1", s, stop, "", "predecessors include E:1")
	return err.Error()
}

// The residual of the containment anchor, and one of the two reasons it is
// off (containmentAnchors): a transaction and its predecessors that were
// lost and issued again with every commit time inside the record's episode
// window are not told apart from the episode's own. THE PLANS ASSERTED
// HERE ARE WRONG; only a containment anchor certifies them, and the shipped
// planner refuses each of them as before issue #269. Here a's first incarnation committed
// A:51-53 at 51..53 s on a history without E:1; a lost them, replicated b's
// errant E:1, was promoted and committed them again within one probe
// interval, and its record at obs holds E:1 in its promotion set. The
// archive holds the earlier A:51-53, after E:1, on b. As soon as one of
// them lies before the window the record anchors nothing.
func TestWitnessContainmentResidualIssuedTwice(t *testing.T) {
	withContainmentAnchors(t)
	build := func(obs float64, pi time.Duration) *synth {
		s := newSynth(t)
		rec := recP("A", 101, obs, "A:1-54,E:1", "A:1-50,E:1")
		rec.ProbeInterval = pitrarchive.Duration(pi)
		s.site("a").Promotions = append(s.site("a").Promotions, rec)
		s.file("b", "b1", 102, "A:1-50", "", []stx{{g: "E:1", oct: sec(50.9), srv: 102}}, run("A", 51, 100, 51, 101))
		s.backup("A:1-50", "b", 102, 50.5)
		return s
	}
	// The window is [obs − probeInterval, obs]: everything inside it.
	p, err := build(55.9, 5*time.Second).run(ModeTimestamp, 52.5, "", false)
	wantCertified(t, "WRONG, pinned: every commit inside the window", p, err, "A:53", "A:1-52,E:1")
	wantShippedRefusal(t, "every commit inside the window", build(55.9, 5*time.Second), 52.5, "", "predecessors include E:1", "in the window")
	// A:51 (51 s) before the window.
	_, err = build(56.1, 5*time.Second).run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "Yet the copy of A:51 in b/b1 was committed at 2026-11-01T12:00:51.000000Z, before that episode can have begun")
	// The witness itself before the window; no margin widens it (9.9 s
	// after the witness used to pass with the two 5 s margins).
	for _, obs := range []float64{58.1, 62.9, 63} {
		wantRefusedE1(t, build(obs, 5*time.Second), 52.5)
	}
	// A stalled sampler widens the window with the probe interval it
	// records, and with it the residual.
	p, err = build(80.9, 30*time.Second).run(ModeTimestamp, 52.5, "", false)
	wantCertified(t, "WRONG, pinned: a 30 s probe interval", p, err, "A:53", "A:1-52,E:1")
	wantShippedRefusal(t, "a 30 s probe interval", build(80.9, 30*time.Second), 52.5, "", "predecessors include E:1", "in the window")
	_, err = build(81.1, 30*time.Second).run(ModeTimestamp, 52.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "before that episode can have begun")

	// A later transaction under the witness's GTID, committed after the
	// sample that held the earlier one, is refused whatever the distance.
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions, recP("A", 101, 50.2, "A:1-52,D:1", "A:1-50,D:1"))
	s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 100, 51, 101))
	s.backup("A:1-50,D:1", "b", 102, 50.1)
	_, err = s.run(ModeTimestamp, 51.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "does not anchor A:52 by containment: A:52 was committed at 2026-11-01T12:00:52.000000Z, after the record that holds it was observed")
}
