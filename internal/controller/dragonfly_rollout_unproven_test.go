package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// rolloutUnprovenFixture is a group whose replica site has been rolled to
// a new image and whose active site, dc1, is next, with source and target
// as the two sites' Dragonfly.
func rolloutUnprovenFixture(t *testing.T, source, target *fakeDragonflyConn) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, string) {
	t.Helper()
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	r, c := newReconciler(fg)
	r.Recorder = record.NewFakeRecorder(64)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC1: source, syncProofDC2: target})
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	for _, site := range []string{"dc1", "dc2"} {
		if err := c.Create(ctx, dragonflyPodForTest(fg, site, fg.Spec.Dragonfly.Image, true)); err != nil {
			t.Fatalf("create %s pod: %v", site, err)
		}
	}
	oldImage := fg.Spec.Dragonfly.Image
	fg.Spec.Dragonfly.Image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("replica rollout reconcile: %v", err)
	}
	markDragonflyStatefulSetRolledOutForTest(t, c, "dc2")
	return r, c, fg, oldImage
}

func rolloutActiveSite(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) string {
	t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	return got.Status.Dragonfly.ActiveSite
}

// A group whose Dragonfly user or image refuses the position commands can
// never have its replica's sync proven by position. Its rollout does not
// wait for that for good: with the link checks holding it promotes with
// REPLTAKEOVER, which has its own catch-up wait, and says so once. Before,
// the readiness check returned "not ready" on every pass, ahead of the
// promotion whose failures are what end a rollout that cannot promote, and
// the active site kept its old pod indefinitely.
func TestReconcileDragonflyResources_RolloutWithoutAPositionPromotesByTakeover(t *testing.T) {
	refusal := &dragonfly.ServerError{Message: "NOPERM ops has no ACL permissions"}
	for _, tc := range []struct {
		name           string
		source, target func() *fakeDragonflyConn
	}{
		{"the target refuses DEBUG REPLICA OFFSET", func() *fakeDragonflyConn { return syncProofMaster("a", 24, 28) },
			func() *fakeDragonflyConn {
				c := syncProofReplica("a", 1, 1)
				c.positionErr = refusal
				return c
			}},
		{"the source refuses DFLY REPLICAOFFSET", func() *fakeDragonflyConn {
			c := syncProofMaster("a", 24, 28)
			c.positionErr = refusal
			return c
		}, func() *fakeDragonflyConn { return syncProofReplica("a", 1, 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.target()
			r, c, fg, _ := rolloutUnprovenFixture(t, tc.source(), target)
			if _, err := r.reconcileDragonflyResources(context.Background(), fg); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if got := rolloutActiveSite(t, c, fg); got != "dc2" {
				t.Fatalf("dragonfly activeSite = %q, want dc2: the rollout must not stall on a position it can never read", got)
			}
			if target.replTakeoverCalls != 1 || target.replicaOfNoOneCalls != 0 {
				t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want one takeover", target.replTakeoverCalls, target.replicaOfNoOneCalls)
			}
			events := drainEventsCh(r.Recorder.(*record.FakeRecorder).Events, 16, 50*time.Millisecond)
			n := 0
			for _, ev := range events {
				if eventsContain([]string{ev}, v1alpha1.EventDragonflySyncUnproven) {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("DragonflySyncUnproven emitted %d times, want once: %v", n, events)
			}
		})
	}
}

// With the position refused and the link checks not holding, the rollout
// waits like any other whose target is not linked, and the stall is
// reported after two minutes.
func TestReconcileDragonflyResources_RolloutWithoutAPositionWaitsForTheLink(t *testing.T) {
	target := syncProofReplica("a", 1, 1)
	target.positionErr = &dragonfly.ServerError{Message: "NOPERM ops has no ACL permissions"}
	target.info.MasterLinkStatus = "down"
	r, c, fg, oldImage := rolloutUnprovenFixture(t, syncProofMaster("a", 24, 28), target)
	// The link checks come first in a look, so a link that is down is seen
	// before the refusal; a replica that follows another instance is the
	// case only the link proof catches.
	for i, mutate := range []func(){
		func() {},
		func() { target.info.MasterLinkStatus, target.info.MasterReplID = "up", "z" },
	} {
		mutate()
		if requeue, err := r.reconcileDragonflyResources(context.Background(), fg); err != nil || requeue == 0 {
			t.Fatalf("case %d: requeue=%v err=%v, want a requeue", i, requeue, err)
		}
		if got := rolloutActiveSite(t, c, fg); got != "dc1" {
			t.Fatalf("case %d: dragonfly activeSite = %q, want dc1", i, got)
		}
		if target.replTakeoverCalls != 0 || target.replicaOfNoOneCalls != 0 {
			t.Fatalf("case %d: REPLTAKEOVER %d, REPLICAOF NO ONE %d; want nothing sent", i, target.replTakeoverCalls, target.replicaOfNoOneCalls)
		}
	}
	if img := dragonflyStatefulSetImageForTest(t, c, "dc1"); img != oldImage {
		t.Fatalf("active image = %q, want %q", img, oldImage)
	}
}

// A takeover that keeps failing for a target that is ready on its link
// alone is never replaced by REPLICAOF NO ONE. After three failures the
// rollout's last resort applies, as for any rollout that cannot promote:
// the active site's StatefulSet is updated where it is.
func TestReconcileDragonflyResources_RolloutTakeoverOnlyFailuresEndInTheActiveUpdate(t *testing.T) {
	target := syncProofReplica("a", 1, 1)
	target.positionErr = &dragonfly.ServerError{Message: "NOPERM ops has no ACL permissions"}
	target.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}
	r, c, fg, oldImage := rolloutUnprovenFixture(t, syncProofMaster("a", 24, 28), target)
	ctx := context.Background()
	key := dragonflyRolloutKey{fg: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, target: "dc2", source: "dc1"}
	// The active master stops answering right after each takeover fails:
	// the state in which a promotion that may fall back does.
	sent := 0
	dial := r.dragonflyConnector
	r.dragonflyConnector = func(ctx context.Context, addr, password string) (DragonflyConnection, error) {
		if addr == syncProofDC1 && target.replTakeoverCalls > sent {
			return nil, errors.New("connection refused")
		}
		return dial(ctx, addr, password)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		sent = target.replTakeoverCalls
		if img := dragonflyStatefulSetImageForTest(t, c, "dc1"); img != oldImage {
			t.Fatalf("before attempt %d: active image = %q, want %q", attempt, img, oldImage)
		}
		if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if target.replTakeoverCalls != attempt {
			t.Fatalf("after attempt %d: REPLTAKEOVER sent %d times", attempt, target.replTakeoverCalls)
		}
		// Let the promotion backoff pass.
		r.dragonflyRolloutMu.Lock()
		if st, ok := r.dragonflyRolloutBackoff[key]; ok {
			st.lastFailure = time.Now().Add(-time.Hour)
			r.dragonflyRolloutBackoff[key] = st
		}
		r.dragonflyRolloutMu.Unlock()
	}
	if target.replicaOfNoOneCalls != 0 {
		t.Fatalf("REPLICAOF NO ONE sent %d times for a target whose sync was never proven by position", target.replicaOfNoOneCalls)
	}
	if got := rolloutActiveSite(t, c, fg); got != "dc1" {
		t.Fatalf("dragonfly activeSite = %q, want dc1: nothing was promoted", got)
	}
	if img := dragonflyStatefulSetImageForTest(t, c, "dc1"); img == oldImage {
		t.Fatalf("active image still %q after three failed takeovers: the rollout did not end", img)
	}
}
