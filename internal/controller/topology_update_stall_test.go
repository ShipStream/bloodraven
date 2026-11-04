package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

const stallPrimaryHost = "mysql-primary-internal.ns.svc.cluster.local"

// stallHarness is a topology manager with an ordered update wired to fake
// Deployments and pods, a fake clock for the stall back-off, and the
// status snapshots it emits.
type stallHarness struct {
	tm   *TopologyManager
	clk  *clock.FakeClock
	pods map[string]*fakeSitePod

	mu      sync.Mutex
	applied []string
	snaps   []TopologySnapshot
}

func newStallHarness(t *testing.T, roles []state.SiteRole) *stallHarness {
	t.Helper()
	checkers := []*mockMySQL{{readOnly: false}}
	for range roles[1:] {
		checkers = append(checkers, &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
			IORunning: true, SQLRunning: true, SourceHost: stallPrimaryHost,
		}})
	}
	tm := newConvergenceManager(t, roles, checkers...)
	h := &stallHarness{tm: tm, clk: clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), pods: map[string]*fakeSitePod{}}
	tm.clock = h.clk
	for i := range tm.sites {
		h.pods[tm.sites[i].name] = &fakeSitePod{name: "mysql-fg-" + tm.sites[i].name + "-1"}
		if i > 0 {
			tm.sites[i].replicating = true
			tm.sites[i].sourceHost = stallPrimaryHost
		}
	}
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.updater.tickInterval = time.Millisecond
	tm.updater.rolledSiteTimeout = 250 * time.Millisecond
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		h.mu.Lock()
		h.applied = append(h.applied, site)
		h.mu.Unlock()
		return nil
	}
	tm.PodReady = func(ctx context.Context, site string) (PodReadiness, error) { return h.pods[site].probe(ctx) }
	tm.StatusCallback = func(s TopologySnapshot) {
		h.mu.Lock()
		h.snaps = append(h.snaps, s)
		h.mu.Unlock()
	}
	return h
}

func (h *stallHarness) appliedSites() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.applied, ",")
}

func (h *stallHarness) lastStall(t *testing.T) string {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.snaps) == 0 {
		t.Fatal("no status snapshot emitted")
	}
	return h.snaps[len(h.snaps)-1].UpdateStalled
}

func (h *stallHarness) snapshots() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.snaps)
}

func (h *stallHarness) drift() []string {
	h.tm.mu.RLock()
	defer h.tm.mu.RUnlock()
	return append([]string(nil), h.tm.specDriftSites...)
}

// waitIdle waits for the update goroutine and any stall recheck to finish.
func (h *stallHarness) waitIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.tm.mu.RLock()
		busy := h.tm.deploymentUpdatePending || h.tm.deploymentUpdateRunning || h.tm.updateStallChecking
		h.tm.mu.RUnlock()
		if !busy && !h.tm.updater.IsUpdating() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("ordered update did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

// observed stands in for the poll that follows an update attempt: an
// attempt that claimed the update window leaves the deployment change
// awaiting observation, and the next attempt waits for a poll to see it.
func (h *stallHarness) observed() {
	h.tm.mu.Lock()
	defer h.tm.mu.Unlock()
	h.tm.deploymentAwaitingObservation = false
}

func (h *stallHarness) probes(site string) int {
	p := h.pods[site]
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.probes
}

// Issue #209 across attempts: an earlier attempt rolled follower-a and its
// sidecar never came up, so it has no drift left while MySQL still answers
// through the -internal Service. The next attempt must not hand the active
// site to it, must not start the update at all (no lease admission, no
// authority change), and must not retry at poll frequency.
func TestCheckUpdate_StallsOnUpdatedStandbyWhosePodIsNotReady(t *testing.T) {
	h := newStallHarness(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate})
	h.pods["follower-a"].broken = true
	h.tm.SetSpecDriftSites([]string{"primary"})
	epoch := h.tm.authorityEpoch

	if !h.tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate did not try the update")
	}
	h.waitIdle(t)
	want := "site follower-a already runs the new pod template but its pod mysql-fg-follower-a-1 is not Ready (" + sidecarCrashLoop + "); not rolling another site"
	if got := h.lastStall(t); got != want {
		t.Fatalf("UpdateStalled = %q\nwant            %q", got, want)
	}
	if got := h.appliedSites(); got != "" {
		t.Fatalf("applied = %q, want nothing rolled", got)
	}
	if h.tm.authorityEpoch != epoch {
		t.Fatal("the stalled attempt claimed the update window")
	}
	if checkers := h.tm.sites; checkers[1].mysql.(*mockMySQL).promoted || checkers[0].mysql.(*mockMySQL).superReadOnly {
		t.Fatal("the stalled attempt touched MySQL authority")
	}

	// Inside the back-off nothing is probed or started.
	probes := h.probes("follower-a")
	if h.tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate retried inside the stall back-off")
	}
	h.waitIdle(t)
	if h.probes("follower-a") != probes {
		t.Fatal("the stalled site was probed inside the back-off")
	}

	// After it the attempt is retried and stalls again on the same cause,
	// with no new status write.
	snaps := h.snapshots()
	h.clk.Advance(orderedUpdateStallRetry)
	if !h.tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate did not retry after the back-off")
	}
	h.waitIdle(t)
	if h.probes("follower-a") == probes || h.snapshots() != snaps || h.appliedSites() != "" {
		t.Fatalf("retry: probes %d->%d snapshots %d->%d applied %q", probes, h.probes("follower-a"), snaps, h.snapshots(), h.appliedSites())
	}
}

// A follower whose new pod never becomes Ready stops the rollout there:
// the next follower is not rolled, the stalled site leaves the drift set
// (its Deployment has the new template, so a retry stops on its pod rather
// than rolling it again), and once the pod is Ready the rollout resumes and
// the stall clears.
func TestCheckUpdate_RolledFollowerStallStopsRolloutUntilItsPodIsReady(t *testing.T) {
	h := newStallHarness(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly})
	h.tm.ApplyUpdate = func(_ context.Context, site string) error {
		h.mu.Lock()
		h.applied = append(h.applied, site)
		h.mu.Unlock()
		if site == "follower-a" {
			h.pods[site].breakIt()
		}
		return nil
	}
	h.tm.SetSpecDriftSites([]string{"follower-a", "follower-b"})

	if !h.tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate did not start the update")
	}
	h.waitIdle(t)
	if got := h.appliedSites(); got != "follower-a" {
		t.Fatalf("applied = %q, want follower-b left alone", got)
	}
	if got := h.lastStall(t); !strings.HasPrefix(got, "site follower-a was updated but its pod mysql-fg-follower-a-1 did not become Ready within 250ms") {
		t.Fatalf("UpdateStalled = %q", got)
	}
	if got := strings.Join(h.drift(), ","); got != "follower-b" {
		t.Fatalf("drift = %q, want follower-b only", got)
	}
	h.observed()

	// Still broken at the retry: follower-b stays untouched.
	h.clk.Advance(orderedUpdateStallRetry)
	if !h.tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate did not retry after the back-off")
	}
	h.waitIdle(t)
	if got := h.appliedSites(); got != "follower-a" {
		t.Fatalf("applied = %q after a retry with follower-a still not Ready", got)
	}
	if got := h.lastStall(t); !strings.HasPrefix(got, "site follower-a already runs the new pod template") {
		t.Fatalf("UpdateStalled = %q", got)
	}

	p := h.pods["follower-a"]
	p.mu.Lock()
	p.broken = false
	p.mu.Unlock()
	h.clk.Advance(orderedUpdateStallRetry)
	if !h.tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate did not resume")
	}
	h.waitIdle(t)
	if got := h.appliedSites(); got != "follower-a,follower-b" {
		t.Fatalf("applied = %q, want follower-b rolled once follower-a is Ready", got)
	}
	if got := h.lastStall(t); got != "" {
		t.Fatalf("UpdateStalled = %q after the rollout finished", got)
	}
	if d := h.drift(); len(d) != 0 {
		t.Fatalf("drift = %v", d)
	}
}

// When the stalled site was the last to roll, no update attempt follows to
// clear the stall. The recheck does, once that pod is Ready, and not before.
func TestCheckUpdate_RecheckClearsStallOfLastRolledSiteOncePodIsReady(t *testing.T) {
	h := newStallHarness(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate})
	h.tm.ApplyUpdate = func(_ context.Context, site string) error {
		h.pods[site].breakIt()
		return nil
	}
	h.tm.SetSpecDriftSites([]string{"follower-a"})
	h.tm.checkUpdate(context.Background())
	h.waitIdle(t)
	if h.lastStall(t) == "" || len(h.drift()) != 0 {
		t.Fatalf("setup: stall %q drift %v", h.lastStall(t), h.drift())
	}

	// Not due yet, then due but still broken.
	probes := h.probes("follower-a")
	h.tm.checkUpdate(context.Background())
	h.waitIdle(t)
	if h.probes("follower-a") != probes {
		t.Fatal("stall rechecked inside the back-off")
	}
	h.clk.Advance(orderedUpdateStallRetry)
	h.tm.checkUpdate(context.Background())
	h.waitIdle(t)
	if h.probes("follower-a") != probes+1 || h.lastStall(t) == "" {
		t.Fatalf("recheck of a broken pod: probes %d->%d stall %q", probes, h.probes("follower-a"), h.lastStall(t))
	}

	p := h.pods["follower-a"]
	p.mu.Lock()
	p.broken = false
	p.mu.Unlock()
	h.clk.Advance(orderedUpdateStallRetry)
	h.tm.checkUpdate(context.Background())
	h.waitIdle(t)
	if got := h.lastStall(t); got != "" {
		t.Fatalf("UpdateStalled = %q, want it cleared once the pod is Ready", got)
	}
	h.tm.mu.RLock()
	retryAt := h.tm.updateStallRetryAt
	h.tm.mu.RUnlock()
	if !retryAt.IsZero() {
		t.Fatal("the back-off outlived the stall")
	}
}

// A recheck that lands after a newer stall was recorded must not clear it.
func TestRecheckUpdateStallKeepsNewerStall(t *testing.T) {
	h := newStallHarness(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate})
	release := make(chan struct{})
	probed := make(chan struct{})
	h.tm.PodReady = func(context.Context, string) (PodReadiness, error) {
		close(probed)
		<-release
		return PodReadiness{Ready: true, Pod: "p"}, nil
	}
	h.tm.recordUpdateStall(&PodNotReadyError{Site: "follower-a", Reason: "old"}, nil)
	h.clk.Advance(orderedUpdateStallRetry)
	h.tm.recheckUpdateStall(context.Background())
	<-probed
	h.tm.recordUpdateStall(&PodNotReadyError{Site: "follower-a", Reason: "new"}, nil)
	close(release)
	h.waitIdle(t)
	h.tm.mu.RLock()
	stall := h.tm.updateStall
	h.tm.mu.RUnlock()
	if !strings.Contains(stall, "(new)") {
		t.Fatalf("updateStall = %q, want the newer stall kept", stall)
	}
}
