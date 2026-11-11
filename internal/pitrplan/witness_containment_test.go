package pitrplan

import (
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// The containment anchor (issue #269). In every archive here a's server
// (uuid A, server_id 101) is promoted holding A:1-50 and D:1, which reached
// it from another source before the promotion. Its era A:51-100 (A:n
// commits at n s) is archived only as replica copies on b, so only a record
// of a can prove that a had D:1 when it committed a witness.

// cRec is a promotion record of a's server observed at second at, with
// gtid_executed set and promotion set p ("" for none).
func cRec(at float64, set, p string) pitrarchive.PromotionRecord {
	return pitrarchive.PromotionRecord{ServerUUID: "A", ServerID: 101, GTIDExecuted: set, PromotionGTIDExecuted: p,
		ObservedAt: synthEpoch.Add(time.Duration(at * float64(time.Second))), ProbeInterval: pitrarchive.Duration(synthProbeInterval)}
}

// cConfirm adds a confirmation observed at second at.
func cConfirm(rec pitrarchive.PromotionRecord, at float64, added string) pitrarchive.PromotionRecord {
	rec.ConfirmedAt, rec.ConfirmedGTIDAdded = synthEpoch.Add(time.Duration(at*float64(time.Second))), added
	return rec
}

// lagRecord holds A:51-54: it was observed 4.2 s into a's era. Its
// promotion set is the promotion GTID set.
func lagRecord() pitrarchive.PromotionRecord { return cRec(54.2, "A:1-54,D:1", "A:1-50,D:1") }

// pendingRecord was observed before a's first commit; A:51-55 commit
// within δ after it.
func pendingRecord() pitrarchive.PromotionRecord { return cRec(50.5, "A:1-50,D:1", "A:1-50,D:1") }

func containmentArchive(t *testing.T, records ...pitrarchive.PromotionRecord) *synth {
	t.Helper()
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions, records...)
	s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 100, 51, 101))
	s.backup("A:1-50,D:1", "b", 102, 50.8)
	return s
}

// wantContained asserts that the target just before A:n plans with the
// record named anchor as its only record anchor.
func wantContained(t *testing.T, what string, s *synth, stop float64, witness, lineage, anchor string) {
	t.Helper()
	p, err := s.run(ModeTimestamp, stop, "", false)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if p.Witness.GTID.String() != witness || p.Witness.GroundTruth != GroundTruthAnchored || !p.Lineage.Equal(MustParseSet(lineage)) ||
		len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != anchor || p.Witness.AnchorOriginRow != "" {
		t.Fatalf("%s: witness %+v Λ=%s, want %s anchored by %s with Λ=%s", what, p.Witness, p.Lineage, witness, anchor, lineage)
	}
}

// wantUncontained asserts PITRStopAmbiguous for the unexplained D:1, with
// subs in the message.
func wantUncontained(t *testing.T, what string, s *synth, stop float64, subs ...string) string {
	t.Helper()
	_, err := s.run(ModeTimestamp, stop, "", false)
	r, ok := RefusalOf(err)
	if !ok || r.Token != pitrarchive.TokenStopAmbiguous {
		t.Fatalf("%s: got %v, want PITRStopAmbiguous", what, err)
	}
	for _, sub := range append([]string{"predecessors include D:1"}, subs...) {
		if !strings.Contains(r.Error(), sub) {
			t.Errorf("%s: refusal %q lacks %q", what, r.Error(), sub)
		}
	}
	return r.Error()
}

// The lag case: the record already holds w, so no clock orders them; its
// promotion set does not hold w, so the server executed that set first.
func TestWitnessContainmentLag(t *testing.T) {
	wantContained(t, "lag", containmentArchive(t, lagRecord()), 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantContained(t, "last lagged commit", containmentArchive(t, lagRecord()), 53.5, "A:54", "A:1-53,D:1", "a#0")

	// The same record without a promotion set refuses as before, and says
	// what the record lacks, with the safe target.
	bare := lagRecord()
	bare.PromotionGTIDExecuted = ""
	wantUncontained(t, "no promotion set", containmentArchive(t, bare), 52.5,
		"or choose a target after 2026-11-01T12:00:59.200000Z (a's promotion record at 2026-11-01T12:00:54.200000Z plus the 5s clock-skew margin)",
		"in the window", "which records it as already executed",
		"the promotion record a#0 (observed at 2026-11-01T12:00:54.200000Z) does not anchor A:53 by containment: it carries no promotion set (promotionGtidExecuted): the sidecar did not observe the server read-only directly before the episode")

	// A promotion set that holds w proves nothing about w: the server had
	// it before the episode. Nothing is said about containment.
	held := lagRecord()
	held.PromotionGTIDExecuted = "A:1-53,D:1"
	if msg := wantUncontained(t, "w in the promotion set", containmentArchive(t, held), 52.5, "in the window"); strings.Contains(msg, "containment") {
		t.Errorf("a record whose promotion set holds w is not in question: %s", msg)
	}

	// A smaller promotion set (the rotation was not acknowledged and D:1
	// arrived after the last read-only observation) anchors only itself.
	small := lagRecord()
	small.PromotionGTIDExecuted = "A:1-50"
	wantUncontained(t, "promotion set without D:1", containmentArchive(t, small), 52.5,
		"the promotion record a#0 proves by containment only its promotion set (promotionGtidExecuted), which does not hold D:1",
		"the promotion-time binlog rotation was not acknowledged")
	// With D:1 excluded the copy needs no anchor (a backup without D:1).
	s := containmentArchive(t, small)
	s.backup("A:1-50", "b", 102, 50.8)
	p, err := s.run(ModeTimestamp, 52.5, "D:1", false)
	if err != nil || !p.Lineage.Subtract(MustParseSet("D:1")).Equal(MustParseSet("A:1-52")) {
		t.Fatalf("D:1 excluded: %+v %v", p, err)
	}
}

// The pending case: the record lacks w and was observed within δ of it;
// the confirmation of the same episode holds w.
func TestWitnessContainmentPending(t *testing.T) {
	confirmed := cConfirm(pendingRecord(), 60.6, "A:51-60")
	wantContained(t, "pending, confirmed", containmentArchive(t, confirmed), 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantContained(t, "first commit of the era", containmentArchive(t, confirmed), 51.9, "A:52", "A:1-51,D:1", "a#0")

	wantUncontained(t, "no confirmation", containmentArchive(t, pendingRecord()), 52.5,
		"or choose a target after 2026-11-01T12:00:55.500000Z (a's promotion record at 2026-11-01T12:00:50.500000Z, which is not provably before A:53, plus the 5s clock-skew margin)",
		"the promotion record a#0 (observed at 2026-11-01T12:00:50.500000Z) does not anchor A:53 by containment: A:53 is not in it and it has no confirmation (confirmedAt): the sidecar or the server stopped, or the episode ended, within about 10s of the record")

	wantUncontained(t, "confirmation without w", containmentArchive(t, cConfirm(pendingRecord(), 60.6, "A:51-52")), 52.5,
		"does not anchor A:53 by containment: neither it nor its confirmation at 2026-11-01T12:01:00.600000Z holds A:53")

	// A confirmation alone is no anchor: the promotion set is the anchor.
	bare := confirmed
	bare.PromotionGTIDExecuted = ""
	wantUncontained(t, "confirmation without a promotion set", containmentArchive(t, bare), 52.5,
		"does not anchor A:53 by containment: it carries no promotion set (promotionGtidExecuted)")

	// More than δ after the record the time rule anchors with the whole
	// record, as before, whatever the promotion set holds.
	small := confirmed
	small.PromotionGTIDExecuted = "A:1-50"
	wantContained(t, "after the margin", containmentArchive(t, small), 55.7, "A:56", "A:1-55,D:1", "a#0")
	wantUncontained(t, "inside the margin, promotion set without D:1", containmentArchive(t, small), 52.5,
		"which does not hold D:1")
}

// Evidence that does not hold together is ignored: the record plans as a
// record without it does.
func TestWitnessContainmentInvalidEvidence(t *testing.T) {
	tests := []struct {
		name string
		edit func(r *pitrarchive.PromotionRecord)
		why  string
	}{
		{"unparsable promotion set", func(r *pitrarchive.PromotionRecord) { r.PromotionGTIDExecuted = "not-a-set" }, "its promotionGtidExecuted is not a GTID set"},
		{"promotion set beyond gtidExecuted", func(r *pitrarchive.PromotionRecord) { r.PromotionGTIDExecuted = "A:1-50,D:1,E:1" }, "its promotionGtidExecuted holds E:1, which its gtidExecuted lacks"},
		{"addition without confirmedAt", func(r *pitrarchive.PromotionRecord) { r.ConfirmedGTIDAdded = "A:55-60" }, "it carries confirmedGtidAdded without confirmedAt"},
		{"confirmation before the record", func(r *pitrarchive.PromotionRecord) { *r = cConfirm(*r, 54.1, "A:55-60") }, "its confirmedAt 2026-11-01T12:00:54.100000Z precedes its observedAt"},
		{"unparsable addition", func(r *pitrarchive.PromotionRecord) { *r = cConfirm(*r, 64.3, "x") }, "its confirmedGtidAdded is not a GTID set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := lagRecord()
			tt.edit(&rec)
			wantUncontained(t, tt.name, containmentArchive(t, rec), 52.5, "in the window", "does not anchor A:53 by containment: "+tt.why)
			// The time rule still uses the record.
			wantContained(t, tt.name+", after the margin", containmentArchive(t, rec), 59.5, "A:60", "A:1-59,D:1", "a#0")
		})
	}
	// A reset record's promotion set is not read: the record is a fence.
	reset := lagRecord()
	reset.Kind = pitrarchive.RecordReset
	if msg := wantUncontained(t, "reset record", containmentArchive(t, reset), 52.5); strings.Contains(msg, "containment") {
		t.Errorf("a reset record is not in question: %s", msg)
	}
}

// Each fence applies to a containment anchor: the sets prove the order of
// the promotion set and the w the sidecar sampled, and only records and
// clocks say that the archived w is that transaction.
func TestWitnessContainmentFences(t *testing.T) {
	reset := func(at float64, pi time.Duration) pitrarchive.PromotionRecord {
		r := cRec(at, "A:1-50,D:1", "")
		r.Kind, r.ProbeInterval = pitrarchive.RecordReset, pitrarchive.Duration(pi)
		return r
	}
	other := func(at float64, set string) pitrarchive.PromotionRecord {
		r := cRec(at, set, "")
		r.ServerUUID, r.ServerID = "B", 102
		return r
	}
	// A:53 commits at 53 s; δ and the probe intervals are 5 s.
	tests := []struct {
		name   string
		a      []pitrarchive.PromotionRecord // site a's records
		b      []pitrarchive.PromotionRecord // site b's records
		anchor string
		fence  string // "" when a#… anchors
	}{
		{name: "no other record", a: []pitrarchive.PromotionRecord{lagRecord()}, anchor: "a#0"},
		// Later records of the site: the archived w may belong to a later
		// incarnation that issued the GTID again.
		{name: "later reset within w + PI + δ", a: []pitrarchive.PromotionRecord{lagRecord(), reset(62.9, 5*time.Second)},
			fence: "does not anchor A:53 by containment, because a later record of site a, a#1 (an incarnation-reset record observed at 2026-11-01T12:01:02.900000Z"},
		{name: "later reset beyond w + PI + δ", a: []pitrarchive.PromotionRecord{lagRecord(), reset(63.1, 5*time.Second)}, anchor: "a#0"},
		{name: "later record with an unknown probe interval", a: []pitrarchive.PromotionRecord{lagRecord(), reset(5000, 0)}, fence: "a later record of site a, a#1"},
		{name: "later promotion record of the same history", a: []pitrarchive.PromotionRecord{lagRecord(), cRec(60, "A:1-60,D:1", "")}, fence: "a later record of site a, a#1 (a promotion record"},
		// Earlier records of the site: the archived w may have been
		// committed before the change they report.
		{name: "earlier reset within δ before w", a: []pitrarchive.PromotionRecord{reset(48.1, 5*time.Second), lagRecord()},
			fence: "the promotion record a#1 (observed at 2026-11-01T12:00:54.200000Z) does not anchor A:53 by containment, because an earlier record of site a, a#0 (an incarnation-reset record observed at 2026-11-01T12:00:48.100000Z, reporting a change that happened up to 5s before it), is not provably before A:53"},
		{name: "earlier reset more than δ before w", a: []pitrarchive.PromotionRecord{reset(47.9, 5*time.Second), lagRecord()}, anchor: "a#1"},
		{name: "earlier record with an unknown probe interval, more than δ before w", a: []pitrarchive.PromotionRecord{reset(47.9, 0), lagRecord()}, anchor: "a#1"},
		{name: "earlier promotion record within δ before w", a: []pitrarchive.PromotionRecord{cRec(48.1, "A:1-48", ""), lagRecord()}, fence: "an earlier record of site a, a#0 (a promotion record"},
		// Another server's promotion around w that lacks w: the server
		// lost its role and regained it, and its records may be missing.
		{name: "another server promoted within δ after w, without w", a: []pitrarchive.PromotionRecord{lagRecord()}, b: []pitrarchive.PromotionRecord{other(57.9, "A:1-50,B:1")},
			fence: "does not anchor A:53 by containment, because b#0, another server's promotion record observed at 2026-11-01T12:00:57.900000Z"},
		{name: "another server promoted δ after w", a: []pitrarchive.PromotionRecord{lagRecord()}, b: []pitrarchive.PromotionRecord{other(58.1, "A:1-50,B:1")}, anchor: "a#0"},
		{name: "another server promoted within 2δ before w, without w", a: []pitrarchive.PromotionRecord{lagRecord()}, b: []pitrarchive.PromotionRecord{other(43.1, "A:1-43")}, fence: "b#0, another server's promotion record"},
		{name: "another server promoted 2δ before w", a: []pitrarchive.PromotionRecord{lagRecord()}, b: []pitrarchive.PromotionRecord{other(42.9, "A:1-42")}, anchor: "a#0"},
		{name: "another server promoted holding w", a: []pitrarchive.PromotionRecord{lagRecord()}, b: []pitrarchive.PromotionRecord{other(55, "A:1-55,D:1")}, anchor: "a#0"},
		// The earliest record that holds w is the episode's; a later one
		// that claims w too does not replace it.
		{name: "an earlier record already holds w", a: []pitrarchive.PromotionRecord{cRec(53.6, "A:1-53,D:1", ""), lagRecord()}, fence: "the promotion record a#0 (observed at 2026-11-01T12:00:53.600000Z) does not anchor A:53 by containment: it carries no promotion set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := containmentArchive(t, tt.a...)
			s.site("b").Promotions = append(s.site("b").Promotions, tt.b...)
			if tt.fence == "" {
				wantContained(t, tt.name, s, 52.5, "A:53", "A:1-52,D:1", tt.anchor)
				return
			}
			wantUncontained(t, tt.name, s, 52.5, tt.fence)
		})
	}
}

// The cap drops a manifest's oldest records. For a containment anchor a
// dropped record could be an earlier fence, so the oldest record of a full
// manifest anchors nothing by containment (the cap fails closed).
func TestWitnessContainmentAtRecordCap(t *testing.T) {
	build := func(later int) *synth {
		records := []pitrarchive.PromotionRecord{lagRecord()}
		for i := 0; i < later; i++ {
			r := cRec(10000+float64(i), "A:1-100,D:1", "")
			r.Kind = pitrarchive.RecordReset
			records = append(records, r)
		}
		return containmentArchive(t, records...)
	}
	wantContained(t, "one below the cap", build(pitrarchive.MaxPromotionRecords-2), 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantUncontained(t, "at the cap", build(pitrarchive.MaxPromotionRecords-1), 52.5,
		"does not anchor A:53 by containment: it is the oldest of site a's 256 records, the most a manifest keeps")
	// A record that is not the oldest has its predecessors in the manifest.
	records := []pitrarchive.PromotionRecord{cRec(-1000, "", ""), lagRecord()}
	for i := 0; i < pitrarchive.MaxPromotionRecords-2; i++ {
		r := cRec(10000+float64(i), "A:1-100,D:1", "")
		r.Kind = pitrarchive.RecordReset
		records = append(records, r)
	}
	wantContained(t, "at the cap, not the oldest", containmentArchive(t, records...), 52.5, "A:53", "A:1-52,D:1", "a#1")
}

// The record dates its episode: a transaction committed before the episode
// can have begun, or after the sample that holds it, is not the one the
// sidecar sampled (a server that lost a transaction issues its GTID again).
// The record's times and the commit time are readings of one clock (the
// sidecar probes the mysqld of its own pod), so the bounds carry no
// clock-skew margin.
func TestWitnessContainmentEpisodeWindow(t *testing.T) {
	// held is a record observed at second at that holds A:53 (53 s) as its
	// episode's first transaction.
	held := func(at float64) pitrarchive.PromotionRecord { return cRec(at, "A:1-53,D:1", "A:1-52,D:1") }
	wantContained(t, "committed when the record was observed", containmentArchive(t, held(53)), 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantUncontained(t, "committed after the record was observed", containmentArchive(t, held(52.999999)), 52.5,
		"does not anchor A:53 by containment: A:53 was committed at 2026-11-01T12:00:53.000000Z, after the record that holds it was observed: the archived transaction is not the one the record saw (the GTID was issued twice, or the server's clock was set back in between)")
	// The episode began no earlier than the probe interval (5 s) before
	// the record: a commit at that instant is inside, one µs earlier is not.
	wantContained(t, "committed at the window's start", containmentArchive(t, held(58)), 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantUncontained(t, "committed before the window's start", containmentArchive(t, held(58.000001)), 52.5,
		"does not anchor A:53 by containment: A:53 was committed at 2026-11-01T12:00:53.000000Z, more than the record's probe interval (5s) before the record was observed, so before its episode can have begun",
		"the GTID was issued twice")
	wantUncontained(t, "committed long before the episode", containmentArchive(t, held(500)), 52.5, "before its episode can have begun")

	unknown := lagRecord()
	unknown.ProbeInterval = 0
	wantUncontained(t, "unknown probe interval", containmentArchive(t, unknown), 52.5, "does not anchor A:53 by containment: its probe interval is unknown, so it does not date its episode")

	// A witness that only the confirmation holds was committed after the
	// record's own sample, which the server read at most
	// PromotionProbeSpread (1 s) before the record's time, and before the
	// confirmation was read.
	confirmed := func(at, confirmedAt float64) pitrarchive.PromotionRecord {
		return cConfirm(cRec(at, "A:1-52,D:1", "A:1-52,D:1"), confirmedAt, "A:53-60")
	}
	wantContained(t, "confirmation-held, 1 s before the record", containmentArchive(t, confirmed(54, 64.1)), 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantUncontained(t, "confirmation-held, more than 1 s before the record", containmentArchive(t, confirmed(54.000001, 64.1)), 52.5,
		"does not anchor A:53 by containment: A:53 was committed at 2026-11-01T12:00:53.000000Z, more than 1s before the record was observed, yet the record does not hold it and its confirmation does: the archived transaction is not the one the confirmation saw")
	// The reader does not rely on the writer's confirmation delay: a
	// confirmation dated before the witness cannot have seen it.
	wantContained(t, "confirmation-held, committed when it was observed", containmentArchive(t, confirmed(52.5, 53)), 52.6, "A:53", "A:1-52,D:1", "a#0")
	wantUncontained(t, "confirmation-held, committed after it", containmentArchive(t, confirmed(52.5, 52.999999)), 52.6,
		"does not anchor A:53 by containment: A:53 was committed at 2026-11-01T12:00:53.000000Z, after the confirmation that holds it was observed (2026-11-01T12:00:52.999999Z)")
}

// A second promotion of the same server: each record anchors its own
// episode's first transactions, and the first record's transactions are in
// the second's promotion set.
func TestWitnessContainmentSecondPromotion(t *testing.T) {
	second := cConfirm(cRec(80.3, "A:1-80,D:1", "A:1-80,D:1"), 90.4, "A:81-90")
	s := containmentArchive(t, lagRecord(), second)
	wantContained(t, "first episode", s, 52.5, "A:53", "A:1-52,D:1", "a#0")
	wantContained(t, "second episode", s, 80.5, "A:81", "A:1-80,D:1", "a#1")
	// Between the two the first record anchors by time... it holds A:54
	// but not A:60, and was observed more than δ before it.
	wantContained(t, "between", s, 59.5, "A:60", "A:1-59,D:1", "a#0")
}

// N1: an exclusion neither sets a record aside nor makes one an anchor.
func TestWitnessContainmentIgnoresExclusions(t *testing.T) {
	fenced := func() *synth {
		s := containmentArchive(t, lagRecord())
		r := cRec(55, "A:1-50,B:1-3", "")
		r.ServerUUID, r.ServerID = "B", 102
		s.site("b").Promotions = append(s.site("b").Promotions, r)
		return s
	}
	wantUncontained(t, "fenced", fenced(), 52.5, "b#0, another server's promotion record")
	// Excluding everything the fencing record holds beyond the backup
	// does not lift the fence.
	_, err := fenced().run(ModeTimestamp, 52.5, "B:1-3", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include D:1", "b#0, another server's promotion record")
	// Nor does an exclusion disturb an anchor: A:60 is excluded, A:53
	// plans as without it.
	p, err := containmentArchive(t, lagRecord()).run(ModeTimestamp, 52.5, "A:60", false)
	if err != nil || p.Witness.GTID.String() != "A:53" || len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#0" {
		t.Fatalf("unrelated exclusion: %+v %v", p, err)
	}
	// Excluding the record's own episode transaction A:53 moves the
	// witness to A:54, which the same record anchors.
	p, err = containmentArchive(t, lagRecord()).run(ModeTimestamp, 52.5, "A:53", false)
	if err != nil || p.Witness.GTID.String() != "A:54" || len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#0" {
		t.Fatalf("w excluded: %+v %v", p, err)
	}
}

// Records written before issue #269 carry none of the new fields and plan
// exactly as they did: a lag refusal with the window and the safe targets.
func TestWitnessContainmentOldRecordsUnchanged(t *testing.T) {
	old := lagRecord()
	old.PromotionGTIDExecuted = ""
	old.ReadOnlyGTIDExecuted = "A:1-50,D:1" // N5: recorded by older sidecars, never read
	s := containmentArchive(t, old)
	wantUncontained(t, "old record", s, 52.5, "in the window")
	wantContained(t, "old record, after the margin", s, 59.5, "A:60", "A:1-59,D:1", "a#0")
}

// recordEvidence reads the evidence of promotion records only, and all of
// it or none.
func TestRecordEvidence(t *testing.T) {
	set := MustParseSet("A:1-54,D:1")
	proven, confirmed, unusable := recordEvidence(lagRecord(), set)
	if !proven.Equal(MustParseSet("A:1-50,D:1")) || confirmed != nil || unusable != "" {
		t.Fatalf("lag record: %s %s %q", proven, confirmed, unusable)
	}
	proven, confirmed, unusable = recordEvidence(cConfirm(lagRecord(), 64.3, "A:55-60"), set)
	if !proven.Equal(MustParseSet("A:1-50,D:1")) || !confirmed.Equal(MustParseSet("A:1-60,D:1")) || unusable != "" {
		t.Fatalf("confirmed record: %s %s %q", proven, confirmed, unusable)
	}
	// An idle episode's confirmation adds nothing and still confirms.
	proven, confirmed, unusable = recordEvidence(cConfirm(lagRecord(), 64.3, ""), set)
	if proven == nil || !confirmed.Equal(set) || unusable != "" {
		t.Fatalf("idle confirmation: %s %s %q", proven, confirmed, unusable)
	}
	for name, rec := range map[string]pitrarchive.PromotionRecord{
		"reset record":         func() pitrarchive.PromotionRecord { r := lagRecord(); r.Kind = pitrarchive.RecordReset; return r }(),
		"another kind":         func() pitrarchive.PromotionRecord { r := lagRecord(); r.Kind = "Future"; return r }(),
		"no promotion set":     cConfirm(cRec(54.2, "A:1-54,D:1", ""), 64.3, "A:55-60"),
		"old record":           {ServerUUID: "A", ServerID: 101, GTIDExecuted: "A:1-54,D:1", ReadOnlyGTIDExecuted: "A:1-50,D:1"},
		"unusable":             cRec(54.2, "A:1-54,D:1", "A:1-60"),
		"unusable, confirmed":  cConfirm(cRec(54.2, "A:1-54,D:1", "A:1-60"), 64.3, "A:55-60"),
		"usable, bad addition": cConfirm(lagRecord(), 64.3, "A:0"),
	} {
		if proven, confirmed, _ := recordEvidence(rec, set); proven != nil || confirmed != nil {
			t.Errorf("%s: evidence %s / %s, want none", name, proven, confirmed)
		}
	}
}

// A transaction under a's server_uuid that another server committed (an
// explicit gtid_next there) is not a transaction of a's episode: records
// are matched by server_uuid and origin server_id, so a's record anchors
// nothing for it, whatever it holds.
func TestWitnessContainmentOtherOriginServer(t *testing.T) {
	s := newSynth(t)
	s.site("a").Promotions = append(s.site("a").Promotions, lagRecord())
	// A:53 carries server_id 103 in b's binlog: c committed it.
	era := run("A", 51, 100, 51, 101)
	era[2].srv = 103
	s.file("b", "b1", 102, "A:1-50,D:1", "", era)
	s.backup("A:1-50,D:1", "b", 102, 50.8)
	_, err := s.run(ModeTimestamp, 52.5, "", false)
	r, ok := RefusalOf(err)
	if !ok || r.Token != pitrarchive.TokenStopAmbiguous || !strings.Contains(r.Error(), "the server with uuid A (server_id 103)") || strings.Contains(r.Error(), "containment") {
		t.Fatalf("A:53 committed by another server: %v", err)
	}
}

// Tagged GTIDs: a transaction under the server's uuid with a tag is the
// episode's own like any other, and the sets compare whole source
// identifiers.
func TestWitnessContainmentTaggedGTID(t *testing.T) {
	build := func(rec pitrarchive.PromotionRecord) *synth {
		s := newSynth(t)
		s.site("a").Promotions = append(s.site("a").Promotions, rec)
		s.file("b", "b1", 102, "A:1-50,D:1,D:blue:1-2", "",
			[]stx{{g: "A:blue:1", oct: sec(52), srv: 101}, {g: "A:51", oct: sec(53), srv: 101}, {g: "A:blue:2", oct: sec(54), srv: 101}})
		s.backup("A:1-50,D:1,D:blue:1-2", "b", 102, 50.8)
		return s
	}
	rec := cRec(54.2, "A:1-51,A:blue:1-2,D:1,D:blue:1-2", "A:1-50,D:1,D:blue:1-2")
	p, err := build(rec).run(ModeTimestamp, 51.9, "", false)
	if err != nil || p.Witness.GTID.String() != "A:blue:1" || len(p.Witness.AnchorRecords) != 1 || !p.Lineage.Equal(MustParseSet("A:1-50,D:1,D:blue:1-2")) {
		t.Fatalf("tagged witness: %+v %v", p, err)
	}
	p, err = build(rec).run(ModeTimestamp, 53.5, "", false)
	if err != nil || p.Witness.GTID.String() != "A:blue:2" || !p.Lineage.Equal(MustParseSet("A:1-51,A:blue:1,D:1,D:blue:1-2")) {
		t.Fatalf("second tagged witness: %+v %v", p, err)
	}
	// A promotion set that lacks the tagged predecessors of another source
	// does not explain them.
	small := cRec(54.2, "A:1-51,A:blue:1-2,D:1,D:blue:1-2", "A:1-50,D:1")
	_, err = build(small).run(ModeTimestamp, 51.9, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include D:blue:1", "which does not hold D:blue:1")
	// A tagged transaction the promotion set holds is no transaction of
	// the episode.
	held := cRec(54.2, "A:1-51,A:blue:1-2,D:1,D:blue:1-2", "A:1-50,A:blue:1,D:1,D:blue:1-2")
	_, err = build(held).run(ModeTimestamp, 51.9, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include D:1")
}
