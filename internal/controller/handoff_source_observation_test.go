package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/shipstream/bloodraven/internal/state"
)

// After a maintenance handoff the ordered update replaces the old primary's
// pod at once, so the polls that follow cannot read it. The manager used
// to keep the Writable state of its last poll until FailureThreshold probes
// had failed: status showed two writable sites (a SPLIT BRAIN alert), the
// pending promotion was cleared as superseded by the fenced source, and
// DNS was pointed back at it (#236, scenario 34). The handoff fenced the
// source and read its GTID set under the fence, so the first poll after it
// takes the unanswered source as read-only.
func TestHandoffPromotion_UnreadableFencedSourceIsNotWritable(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(dc1, dc2)
	setRecoveredTopology(tm)
	ctx := context.Background()

	// ExecuteHandoff fenced dc1 and promoted dc2; dc1's pod is replaced.
	dc1.mu.Lock()
	dc1.readOnly, dc1.superReadOnly, dc1.err = true, true, errors.New("dial tcp: connection refused")
	dc1.mu.Unlock()
	dc2.mu.Lock()
	dc2.readOnly = false
	dc2.mu.Unlock()
	tm.recordHandoffPromotion(ctx, "dc1", "dc2", "")

	tm.Poll(ctx)

	if got := dns.getLastIP(); got != "2.2.2.2" {
		t.Fatalf("DNS = %q after the poll that followed the handoff, want the promoted dc2 (2.2.2.2)", got)
	}
	tm.mu.RLock()
	promoted, dc1State := tm.promotedSite, tm.sites[0].state
	tm.mu.RUnlock()
	if dc1State == state.StateWritable {
		t.Fatal("the fenced, unreadable source is still Writable")
	}
	if promoted != "dc2" {
		t.Fatalf("pending promotion = %q, want dc2 kept until dc2 is confirmed writable", promoted)
	}
	if active := tm.Status().ActiveSite; active != "dc2" {
		t.Fatalf("status active site = %q, want dc2", active)
	}

	// dc2 is confirmed writable; dc1 stays unreadable and is never named.
	pollN(tm, 3)
	if active := tm.Status().ActiveSite; active != "dc2" {
		t.Fatalf("status active site = %q after dc2 is confirmed, want dc2", active)
	}
	if got := dns.getLastIP(); got != "2.2.2.2" {
		t.Fatalf("DNS = %q, want 2.2.2.2", got)
	}
}

// A source that answers keeps its real answer: the first poll after the
// handoff does not override a successful probe.
func TestHandoffPromotion_AnsweringSourceKeepsItsAnswer(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(dc1, dc2)
	setRecoveredTopology(tm)
	ctx := context.Background()

	dc2.mu.Lock()
	dc2.readOnly = false
	dc2.mu.Unlock()
	// Something made dc1 writable again after the handoff's fence.
	tm.recordHandoffPromotion(ctx, "dc1", "dc2", "")
	tm.Poll(ctx)

	tm.mu.RLock()
	dc1State := tm.sites[0].state
	tm.mu.RUnlock()
	if dc1State != state.StateWritable {
		t.Fatalf("dc1 state = %s, want Writable: it answered writable after the handoff", dc1State)
	}
}
