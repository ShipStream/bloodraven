package component

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Pins of what the planned DST sweep found around a site that lost its own
// transactions (seeds shift with the schedule generator; these do not).

// holdCloneGate is a keyring gate that keeps a clone from starting until
// ready is set: the request stands, as it does while a recipient's
// keyring is unsealed.
type holdCloneGate struct {
	ready atomic.Bool
}

func (g *holdCloneGate) RequestKeyringUnseal(context.Context, types.NamespacedName, string) (bool, error) {
	return g.ready.Load(), nil
}

func (g *holdCloneGate) NotifyCloneComplete(context.Context, types.NamespacedName, string) error {
	return nil
}

// DST seed 375699 (ModelHole): the reclone of a site that lacked its own
// transactions was requested, could not start for a while, and ran later
// on that old comparison, after the site had taken acknowledged writes
// (an external writer had made it writable while it was cut off). Those
// writes then existed nowhere. The clone must rest on what the site holds
// when it starts: here dc1 gains transactions dc2 lacks while its clone
// waits, and is never cloned over.
func TestOwnTransactionsLost_RecloneIsNotRunOnAnOldComparison(t *testing.T) {
	f := newOwnLossFixture(t)
	f.failoverAndReturn()
	gate := &holdCloneGate{}
	f.tm.SetKeyringGate(gate)
	replicationBeforeClone := false
	f.armClone(&replicationBeforeClone)

	f.poll(8)
	if got := f.statusSite("dc1"); got.RecoveryState != v1alpha1.RecoveryStateRecloneRequired || got.LostGtid != ownLossLost {
		t.Fatalf("setup: dc1 status = %+v, want RecloneRequired for %s", got, ownLossLost)
	}
	if !strings.Contains(f.logs.String(), `msg="reclone requested for a site that lacks transactions it issued itself" site=dc1`) {
		t.Fatalf("setup: no reclone was requested; logs:\n%s", f.logs.String())
	}

	// dc1 takes five writes of its own that dc2 never sees, then the clone
	// could start.
	diverged := gtidWithExtras(ownLossReturn, ownLossUUIDdc1, 101, 105)
	f.dc1.setGtidExecuted(diverged)
	gate.ready.Store(true)
	f.poll(40)

	f.dc1.mu.Lock()
	cloned, set := f.dc1.cloneInstanceCalled, f.dc1.gtidExecuted
	f.dc1.mu.Unlock()
	if cloned || set != diverged {
		t.Fatalf("dc1 was cloned over (cloned=%v, set %s): it held %s:101-105, which dc2 lacks", cloned, set, ownLossUUIDdc1)
	}
	got := f.statusSite("dc1")
	if got.RecoveryState != v1alpha1.RecoveryStateBlocked || got.DivergentGtid != ownLossUUIDdc1+":101-105" || got.LostGtid != ownLossLost {
		t.Fatalf("dc1 status = %+v, want RecoveryBlocked for its five transactions, with the lost set beside it", got)
	}
	if !strings.Contains(f.logs.String(), `msg="automatic reclone not started: the check before the clone does not allow it" site=dc1 donor=dc2`) {
		t.Errorf("the refusal was not logged; logs:\n%s", f.logs.String())
	}
}

// DST seed 337457 (PromotedWithoutOwnTransactions): dc1 was promoted
// without part of its own history while dc2, which holds it, could not be
// read (nothing the operator can know). When dc2 came back it was reported
// divergent for exactly those transactions. Then dc1 restarted read-only,
// and the primary re-assert, which sets a peer's recorded divergence
// aside, made dc1 writable again: it would issue those GTIDs a second
// time, and dc2's report would clear by itself. The re-assert must count a
// peer's recorded divergence when it consists of the target's own
// transactions.
func TestOwnTransactionsLost_ReassertCountsAPeersRecordedDivergence(t *testing.T) {
	f := newOwnLossFixture(t)
	f.poll(3)
	if got := f.tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("setup: active site = %q, want dc1", got)
	}
	// dc1 restarts on a volume ten transactions old while dc2 is cut off:
	// nothing shows the loss, and dc1 is made the primary again.
	cutOff := func(on bool) {
		n := 0
		if on {
			n = 1 << 30
		}
		f.dc2.mu.Lock()
		if f.dc2.failNext == nil {
			f.dc2.failNext = map[string]int{}
		}
		for _, stmt := range []string{"GetGtidExecuted", "ReadGTIDSnapshot"} {
			f.dc2.failNext[stmt] = n
		}
		f.dc2.mu.Unlock()
		if on {
			f.dc2.setError(errors.New("dial tcp: i/o timeout"))
		} else {
			f.dc2.setError(nil)
		}
	}
	cutOff(true)
	lost := ownLossReturn
	f.dc1.replacePod(podReplacement{gtidExecuted: &lost, noChannel: true})
	f.poll(8)
	if f.dc1.isReadOnly() {
		t.Fatalf("setup: dc1 was not made writable again while dc2 could not be read; logs:\n%s", f.logs.String())
	}
	// dc2 answers again: it holds the ten transactions dc1 lacks.
	cutOff(false)
	f.poll(8)
	if got := f.statusSite("dc2"); got.RecoveryState != v1alpha1.RecoveryStateBlocked || got.DivergentGtid != ownLossLost {
		t.Fatalf("setup: dc2 status = %+v, want RecoveryBlocked for %s", got, ownLossLost)
	}

	// dc1 restarts in place (same data, read-only): every site is
	// read-only, and the re-assert looks at it.
	f.dc1.replacePod(podReplacement{noChannel: true})
	f.poll(12)

	if !f.dc1.isReadOnly() {
		t.Fatalf("dc1 was made writable again although dc2 holds %s, which dc1 issued and would issue again", ownLossLost)
	}
	got := f.statusSite("dc1")
	if got.RecoveryState != v1alpha1.RecoveryStateRecloneRequired || got.LostGtid != ownLossLost {
		t.Fatalf("dc1 status = %+v, want RecloneRequired for %s", got, ownLossLost)
	}
	if !strings.Contains(f.logs.String(), `msg="primary re-assert refused: a peer holds transactions the target issued itself and lacks" site=dc1 peer=dc2`) {
		t.Errorf("the refusal was not logged; logs:\n%s", f.logs.String())
	}
}
