package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/shipstream/bloodraven/internal/state"
)

const (
	pickerUUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	pickerErr  = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
)

func blockSite(tm *TopologyManager, site, gtid string, count int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.recovery[site] = &siteRecovery{state: recoveryStateBlocked, divergentGtid: gtid, divergentCount: count,
		detectedBy: divergenceDetectedByAudit, comparedTo: "dc0"}
}

// A divergent candidate that is a GTID superset of a clean one would win on
// freshness alone. Emergency failover prefers the clean candidate: the
// superset's extra history is exactly what is in question.
func TestPickFreshestCandidate_PrefersCleanOverDivergentSuperset(t *testing.T) {
	divergent := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-100," + pickerErr + ":1-5"}
	clean := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-100"}
	tm, _, _ := newTestTopologyManager(divergent, clean)
	blockSite(tm, "dc1", pickerErr+":1-5", 5)

	if winner := tm.pickFreshestCandidate(context.Background(), []string{"dc1", "dc2"}); winner != "dc2" {
		t.Fatalf("winner = %q, want the clean dc2 over the divergent superset dc1", winner)
	}
}

// GTID freshness still orders candidates within each partition: of two
// clean candidates the fresher wins, ahead of a fresher divergent one.
func TestPickFreshestCandidate_FreshnessWithinCleanPartition(t *testing.T) {
	behind := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-90"}
	ahead := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-100"}
	divergent := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-100," + pickerErr + ":1-5"}
	tm := newConvergenceManager(t, threeCandidates(), behind, ahead, divergent)
	blockSite(tm, "follower-b", pickerErr+":1-5", 5)

	if winner := tm.pickFreshestCandidate(context.Background(), []string{"follower-b", "primary", "follower-a"}); winner != "follower-a" {
		t.Fatalf("winner = %q, want the freshest clean candidate follower-a", winner)
	}
}

// The promotion watermark still refuses a regressed candidate, clean or
// not; the partition does not resurrect it.
func TestPickFreshestCandidate_WatermarkStillApplies(t *testing.T) {
	regressed := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-50"}
	divergent := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-50," + pickerErr + ":1-5"}
	tm, _, _ := newTestTopologyManager(regressed, divergent)
	blockSite(tm, "dc2", pickerErr+":1-5", 5)
	tm.promotionGtidExecuted = pickerUUID + ":1-100"

	if winner := tm.pickFreshestCandidate(context.Background(), []string{"dc1", "dc2"}); winner != "" {
		t.Fatalf("winner = %q, want no candidate: both regressed below the watermark", winner)
	}
}

// Emergency failover is never refused for divergence. When the only
// candidate is divergent it is promoted, with an ERROR naming its extras;
// once it is the confirmed primary its block becomes the durable
// DivergentPromotion condition and a Warning Event, instead of vanishing.
func TestEmergencyFailover_PromotesSoleDivergentCandidateWithDurableNote(t *testing.T) {
	var logs bytes.Buffer
	primary := &mockMySQL{readOnly: false, gtidExecuted: pickerUUID + ":1-100"}
	standby := &mockMySQL{readOnly: true, gtidExecuted: pickerUUID + ":1-100," + pickerErr + ":1-5"}
	tm, _, _ := newTestTopologyManager(primary, standby)
	tm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	conditions, events := statusRunner(t, tm)
	pollN(tm, 2)
	blockSite(tm, "dc2", pickerErr+":1-5", 5)

	primary.setError(errDown)
	pollN(tm, 3)
	if standby.readOnly {
		t.Fatal("divergent sole candidate was not promoted: emergency failover must not be refused")
	}
	if !strings.Contains(logs.String(), `msg="failover picker: promoting a divergent candidate; no other candidate remains"`) {
		t.Errorf("no ERROR naming the divergent promotion; logs:\n%s", logs.String())
	}
	pollN(tm, 2)

	if rec := recoveryOf(tm, "dc2"); rec.state != "" {
		t.Errorf("block on the new primary = %+v, want it converted to the durable note", rec)
	}
	note := conditions()["DivergentPromotion"]
	if note.Status != metav1.ConditionTrue || note.Reason != "DivergentSitePromoted" ||
		!strings.Contains(note.Message, "dc2") || !strings.Contains(note.Message, pickerErr+":1-5") {
		t.Fatalf("DivergentPromotion condition = %+v, want True naming dc2 and its set", note)
	}
	found := false
	for len(events.Events) > 0 {
		if e := <-events.Events; strings.Contains(e, "DivergentSitePromoted") {
			found = true
		}
	}
	if !found {
		t.Error("no DivergentSitePromoted Warning Event")
	}

	// The note is written once: later status writes keep it, and nothing
	// re-adds it after an administrator removes it.
	pollN(tm, 2)
	if tm.buildSnapshot(nil).DivergentPromotion != nil {
		t.Error("note still pending after it was persisted")
	}
}

var errDown = errors.New("dial tcp: connection refused")

func threeCandidates() []state.SiteRole {
	return []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate}
}

// The picker can promote a clean dc3 over a divergent superset dc2. If
// dc3's own sidecar then fences it back, the re-assert that heals that
// wedge must not refuse because dc2 holds its recorded extras: those are
// already reported, stay on dc2, and are re-audited against dc3. Anything
// dc2 holds beyond its recorded set still blocks the re-assert.
func TestPrimaryReassert_DivergentPeerRecordedExtrasDoNotWedge(t *testing.T) {
	for _, tc := range []struct {
		name      string
		peerGTID  string
		reasserts bool
	}{
		{"peer holds only its recorded extras", pickerUUID + ":1-100," + pickerErr + ":1-5", true},
		{"peer also holds unrecorded transactions", pickerUUID + ":1-101," + pickerErr + ":1-5", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldPrimary := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: pickerUUID + ":1-100"}
			divergent := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: tc.peerGTID}
			target := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: pickerUUID + ":1-100"}
			tm := newConvergenceManager(t, threeCandidates(), oldPrimary, divergent, target)
			tm.mu.Lock()
			for i := range tm.sites {
				tm.sites[i].state = state.StateReadOnly
			}
			tm.lastFailoverTarget = "follower-b"
			tm.promotionGtidExecuted = pickerUUID + ":1-100"
			tm.mu.Unlock()
			blockSite(tm, "follower-a", pickerErr+":1-5", 5)

			tm.checkPrimaryReassert(context.Background())

			writable := !target.readOnly
			if writable != tc.reasserts {
				t.Fatalf("target writable after re-assert = %v, want %v", writable, tc.reasserts)
			}
			if !divergent.readOnly || !oldPrimary.readOnly {
				t.Fatal("a peer was made writable")
			}
		})
	}
}

// A second divergent promotion before an administrator reviewed the first
// must not erase it: the condition keeps every unreviewed promotion.
func TestUpdateCRStatus_DivergentPromotionKeepsEarlierRecords(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	standby := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(primary, standby)
	conditions, _ := statusRunner(t, tm)
	pollN(tm, 2)

	for _, note := range []DivergentPromotion{
		{Site: "dc2", DivergentGtid: pickerErr + ":1-5", DivergentTransactions: 5, ComparedTo: "dc1"},
		{Site: "dc1", DivergentGtid: pickerErr + ":9", DivergentTransactions: 1, ComparedTo: "dc2"},
	} {
		tm.mu.Lock()
		n := note
		tm.pendingDivergentPromotion = &n
		tm.mu.Unlock()
		tm.StatusCallback(tm.buildSnapshot(nil))
	}

	msg := conditions()["DivergentPromotion"].Message
	if !strings.Contains(msg, pickerErr+":1-5") || !strings.Contains(msg, pickerErr+":9") {
		t.Fatalf("DivergentPromotion message = %q, want both promotions recorded", msg)
	}
}

// The record is bounded: the newest promotions are kept and a marker says
// older ones were dropped (their Events and logs remain).
func TestAppendDivergentPromotion_KeepsNewestRecords(t *testing.T) {
	msg := ""
	for i := 1; i <= maxDivergentPromotionRecords+2; i++ {
		record := "record-" + strconv.Itoa(i)
		if msg == "" {
			msg = record
		} else {
			msg = appendDivergentPromotion(msg, record)
		}
	}
	lines := strings.Split(msg, "\n")
	if len(lines) != maxDivergentPromotionRecords+1 || lines[0] != divergentPromotionOmitted {
		t.Fatalf("record = %q, want the omission marker and %d records", msg, maxDivergentPromotionRecords)
	}
	if lines[1] != "record-3" || lines[len(lines)-1] != "record-"+strconv.Itoa(maxDivergentPromotionRecords+2) {
		t.Fatalf("record = %q, want the newest records in order", msg)
	}
}
