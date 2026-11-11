package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// threeSiteLoss is dc1 (the failed primary), dc2 (the candidate that came
// back without its own tail) and dc3, a disaster-recovery site that still
// answers.
func threeSiteLoss(t *testing.T, dc3 mysql.Checker) *TopologyManager {
	t.Helper()
	dc1 := &lossServer{mockMySQL: &mockMySQL{err: errors.New("connection refused")}, uuid: lossUUIDNew}
	dc2 := &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: lossReturnGTID}, uuid: lossUUIDOld}
	cfg := testTopologyConfig()
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3",
		Role: state.SiteRoleDROnly, TaintSelector: taintSelector("dc3"), Host: "mysql-dc3"})
	tm := NewTopologyManager(cfg, []mysql.Checker{dc1, dc2, dc3}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	tm.failoverCooldown = 0
	t.Cleanup(func() { metrics.OwnTransactionsLost.WithLabelValues("dc2").Set(0) })
	return tm
}

func witnessServer(gtid string) *lossServer {
	return &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: gtid}, uuid: lossUUIDRe}
}

// With the primary gone there is nothing to compare a candidate with but
// the sites that still answer. One that holds transactions the candidate
// issued and lacks is the evidence: the candidate is refused and reported.
func TestOwnLoss_PickerRefusesACandidateOnAWitness(t *testing.T) {
	tm := threeSiteLoss(t, witnessServer(lossUUIDOld+":1-100"))
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "" {
		t.Fatalf("picked %q: dc2 lacks %s, which dc3 holds", got, lossLostGTID)
	}
	tm.mu.RLock()
	rec := tm.ownLoss["dc2"]
	refused := append([]string(nil), tm.ownLossPromotionRefused...)
	tm.mu.RUnlock()
	if rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc3" {
		t.Fatalf("own-loss record = %+v, want %s against dc3", rec, lossLostGTID)
	}
	if len(refused) != 1 || refused[0] != "dc2" {
		t.Fatalf("promotion refused = %v, want [dc2]", refused)
	}

	// The control: the same witness without the candidate's lost tail is
	// no evidence, and the candidate is picked.
	tm = threeSiteLoss(t, witnessServer(lossUUIDOld+":1-90"))
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "dc2" {
		t.Fatalf("picked %q, want dc2: nothing shows it lost anything", got)
	}
	// A witness ahead only on another server's transactions is lag, not a
	// loss of the candidate's.
	tm = threeSiteLoss(t, witnessServer(lossUUIDOld+":1-90,"+lossUUIDNew+":1-50"))
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "dc2" {
		t.Fatalf("picked %q, want dc2: the witness is ahead under the dead primary's uuid only", got)
	}
}

// hangingServer never answers a GTID read until its context ends.
type hangingServer struct{ *lossServer }

func (h hangingServer) GetGtidExecuted(ctx context.Context) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

// A witness never holds a promotion for longer than its grace: a site that
// does not answer is not waited for.
func TestOwnLoss_PickerDoesNotWaitForASilentWitness(t *testing.T) {
	tm := threeSiteLoss(t, hangingServer{witnessServer(lossUUIDOld + ":1-100")})
	tm.ownLossWitnessGrace = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	got := tm.pickFreshestCandidate(ctx, []string{"dc2"})
	elapsed := time.Since(start)
	if got != "dc2" {
		t.Fatalf("picked %q, want dc2: a witness that does not answer is no evidence", got)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("the pick took %s: a silent witness held the promotion past its grace", elapsed)
	}
}

// The recorded primary came back read-only without part of its own
// history, and its peer holds it. Re-assert refuses (it always did, for
// the peer's extra transactions); now the status says what is wrong with
// the target.
func TestOwnLoss_RefusedReassertReportsTheTarget(t *testing.T) {
	f := newLossFixture(t, lossUUIDOld+":1-100", lossReturnGTID, false)
	// dc1 holds everything dc2 issued and is a fenced replica; dc2 is the
	// recorded primary.
	f.primary.mockMySQL.readOnly, f.primary.mockMySQL.superReadOnly = true, true
	pollN(f.tm, 3)
	f.tm.mu.Lock()
	f.tm.lastFailoverTarget = "dc2"
	f.tm.mu.Unlock()
	if f.tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert was attempted")
	}
	rec := f.loss()
	if rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc1" {
		t.Fatalf("own-loss record = %+v, want %s against dc1", rec, lossLostGTID)
	}
	f.site.mockMySQL.mu.Lock()
	readOnly := f.site.mockMySQL.readOnly
	f.site.mockMySQL.mu.Unlock()
	if !readOnly {
		t.Fatal("the target was made writable")
	}
}
