package component

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// newAutoCloneRecoveryHarness is newRecoveryHarness with a bootstrap
// controller wired, so auto-clone is live and a wrong fresh-datadir verdict
// would actually clone over the site.
func newAutoCloneRecoveryHarness(t *testing.T, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()
	h := newRecoveryHarness(t, dc1, dc2)
	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  0,
	}
	bootCfg := controller.BootstrapConfig{
		ReplUser:     "repl",
		ReplPassword: "replpass",
		CloneTimeout: 30 * time.Second,
	}
	fc := controller.NewFailoverController(h.logger)
	bc := controller.NewBootstrapController(h.logger)
	h.tm = controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, bc, bootCfg, h.tainter, h.hub, h.dns, h.logger, h.clock)
	return h
}

// TestRecovery_TaggedSharedHistoryBlockedNotCloned runs the full poll loop,
// with auto-clone enabled, against a returning old primary whose only
// overlap with the new primary is tagged history under the cluster's server
// UUID and which has no user schemas. Tags do not change which server wrote
// a transaction, so the site is a returning member: recovery must compare
// its GTIDs and block on the tagged transactions the new primary lacks,
// never treat it as a fresh datadir and clone over them.
func TestRecovery_TaggedSharedHistoryBlockedNotCloned(t *testing.T) {
	const taggedOnly = "uuid1:app:1-5"
	for _, tc := range []struct {
		name     string
		readOnly bool
	}{
		{name: "returns read-only", readOnly: true},
		{name: "returns writable", readOnly: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			noSchemas := false
			dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
			dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
			h := newAutoCloneRecoveryHarness(t, dc1, dc2)

			h.pollN(2) // establish
			dc1.setError(errDown)
			h.pollN(3) // failover to dc2
			h.pollN(2) // confirm
			if dc2.isReadOnly() {
				t.Fatal("setup: dc2 was not promoted")
			}

			dc1.mu.Lock()
			dc1.gtidExecuted = taggedOnly
			dc1.hasUserSchemas = &noSchemas
			dc1.mu.Unlock()
			dc1.respawn(tc.readOnly)

			for i := 0; i < 6; i++ {
				h.pollN(1)
				// Any clone runs on a goroutine; give one a chance to start.
				time.Sleep(5 * time.Millisecond)

				dc1.mu.Lock()
				cloned := dc1.cloneInstanceCalled
				replicating := dc1.replicationSourceSet || dc1.replicaStarted
				dc1.mu.Unlock()
				if cloned {
					t.Fatalf("poll %d: dc1 was cloned over; its tagged transactions would be lost", i+1)
				}
				if replicating {
					t.Fatalf("poll %d: dc1 was rejoined as a replica despite transactions the primary lacks", i+1)
				}
				if phase := h.tm.BootstrapPhase(); phase != controller.BootstrapPhaseNone {
					t.Fatalf("poll %d: bootstrap phase %q, want none", i+1, phase)
				}
			}

			s := h.tm.Status()
			if s.Sites[0].RecoveryState != "RecoveryBlocked" {
				t.Fatalf("dc1 RecoveryState = %q, want RecoveryBlocked", s.Sites[0].RecoveryState)
			}
			if s.Sites[0].DivergentGtid != taggedOnly {
				t.Fatalf("dc1 DivergentGtid = %q, want %q", s.Sites[0].DivergentGtid, taggedOnly)
			}
			if !dc1.isReadOnly() {
				t.Fatal("dc1 is not fenced read-only")
			}
		})
	}
}
