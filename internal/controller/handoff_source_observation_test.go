package controller

import (
	"context"
	"errors"
	"sync"
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
	dc1State, dc2State := tm.sites[0].state, tm.sites[1].state
	tm.mu.RUnlock()
	if dc1State == state.StateWritable {
		t.Fatal("the fenced, unreadable source is still Writable")
	}
	if dc2State != state.StateWritable {
		t.Fatalf("promoted dc2 state = %s after a poll that read it writable, want Writable", dc2State)
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

// A promotion this process made and confirmed is not debounced again: the
// first poll that reads the target writable takes it as Writable, so status
// does not read "No site is writable" (NoPrimary) for recoveryThreshold
// polls after every promotion (#236, scenario 49 after a planned
// switchover). Another site answering writable is still debounced.
func TestRecordedPromotion_FirstWritablePollIsNotDebounced(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(dc1, dc2)
	setRecoveredTopology(tm)
	ctx := context.Background()

	// A planned failover fenced dc1 and promoted dc2.
	dc1.mu.Lock()
	dc1.readOnly = true
	dc1.mu.Unlock()
	dc2.mu.Lock()
	dc2.readOnly = false
	dc2.mu.Unlock()
	tm.recordFailover(ctx, tm.clock.Now(), "dc2", "")
	tm.Poll(ctx)
	tm.mu.RLock()
	dc1State, dc2State := tm.sites[0].state, tm.sites[1].state
	tm.mu.RUnlock()
	if dc1State != state.StateReadOnly || dc2State != state.StateWritable {
		t.Fatalf("states dc1=%s dc2=%s after the first poll, want ReadOnly and Writable", dc1State, dc2State)
	}

	// Later, dc1 answers writable without a promotion: debounced as before.
	dc1.mu.Lock()
	dc1.readOnly = false
	dc1.mu.Unlock()
	tm.Poll(ctx)
	tm.mu.RLock()
	dc1State = tm.sites[0].state
	tm.mu.RUnlock()
	if dc1State == state.StateWritable {
		t.Fatal("a writable answer nobody here caused skipped the recoveryThreshold debounce")
	}
}

// firstProbeBlocks blocks the first CheckReadOnly until release is closed,
// after the read it returns was taken.
type firstProbeBlocks struct {
	*mockMySQL
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (f *firstProbeBlocks) CheckReadOnly(ctx context.Context) (bool, error) {
	ro, err := f.mockMySQL.CheckReadOnly(ctx)
	f.once.Do(func() {
		close(f.entered)
		<-f.release
	})
	return ro, err
}

// A poll that read the source writable before the fence and finishes after
// the handoff was recorded must not consume the fenced-source marker: its
// results are discarded at the epoch check, and the next poll, which
// cannot read the source, takes it as read-only. This states the property;
// the ordering that broke it (a poll reaching its epoch check between the
// marker and the epoch move) is closed by construction, both being written
// in one critical section of recordPromotion.
func TestHandoffPromotion_PollFromBeforeTheFenceDoesNotConsumeTheMarker(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(dc1, dc2)
	setRecoveredTopology(tm)
	blocking := &firstProbeBlocks{mockMySQL: dc1, entered: make(chan struct{}), release: make(chan struct{})}
	tm.sites[0].mysql = blocking
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		tm.Poll(ctx) // reads dc1 writable, then waits
	}()
	<-blocking.entered

	dc1.mu.Lock()
	dc1.readOnly, dc1.superReadOnly, dc1.err = true, true, errors.New("dial tcp: connection refused")
	dc1.mu.Unlock()
	dc2.mu.Lock()
	dc2.readOnly = false
	dc2.mu.Unlock()
	tm.recordHandoffPromotion(ctx, "dc1", "dc2", "")
	close(blocking.release)
	<-done

	tm.Poll(ctx)
	tm.mu.RLock()
	dc1State := tm.sites[0].state
	tm.mu.RUnlock()
	if dc1State == state.StateWritable {
		t.Fatal("the fenced, unreadable source is Writable: a poll from before the fence consumed the marker")
	}
}
