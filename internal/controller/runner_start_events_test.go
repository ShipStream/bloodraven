package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// startEventHarness drives TopologyManagerRunner.sync with a fake clock that
// advances one resync interval per sync, and collects the Events it emits.
type startEventHarness struct {
	t      *testing.T
	r      *TopologyManagerRunner
	c      client.Client
	fg     *v1alpha1.MysqlFailoverGroup
	secret *corev1.Secret
	rec    *record.FakeRecorder
	clock  time.Time
}

func newStartEventHarness(t *testing.T) *startEventHarness {
	t.Helper()
	r, c, fg, secret := lifecycleRunner(t)
	h := &startEventHarness{
		t:      t,
		r:      r,
		c:      c,
		fg:     fg,
		secret: secret,
		rec:    record.NewFakeRecorder(1000),
		clock:  time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
	}
	r.recorder = h.rec
	r.now = func() time.Time { return h.clock }
	return h
}

// sync advances the clock by one resync interval, runs a sync and returns
// the Events it emitted.
func (h *startEventHarness) sync() []string {
	h.t.Helper()
	h.clock = h.clock.Add(30 * time.Second)
	mustSync(h.t, h.r)
	return drainEvents(h.rec)
}

func (h *startEventHarness) setDSN(data map[string][]byte) {
	h.t.Helper()
	ctx := context.Background()
	var cur corev1.Secret
	err := h.c.Get(ctx, client.ObjectKeyFromObject(h.secret), &cur)
	if err != nil {
		fresh := h.secret.DeepCopy()
		fresh.ResourceVersion = ""
		fresh.Data = data
		if err := h.c.Create(ctx, fresh); err != nil {
			h.t.Fatalf("create secret: %v", err)
		}
		return
	}
	cur.Data = data
	if err := h.c.Update(ctx, &cur); err != nil {
		h.t.Fatalf("update secret: %v", err)
	}
}

func (h *startEventHarness) breakSecret() {
	h.setDSN(map[string][]byte{"not-dsn": []byte("x")})
}

func (h *startEventHarness) fixSecret() {
	h.setDSN(map[string][]byte{"dsn": []byte("op:pw@tcp(127.0.0.1:1)/mysql")})
}

func (h *startEventHarness) deleteSecret() {
	h.t.Helper()
	if err := h.c.Delete(context.Background(), h.secret.DeepCopy()); err != nil {
		h.t.Fatalf("delete secret: %v", err)
	}
}

func countReason(events []string, eventType, reason string) int {
	n := 0
	for _, e := range events {
		if strings.HasPrefix(e, eventType+" "+reason+" ") {
			n++
		}
	}
	return n
}

func startFailureWarnings(events []string) int {
	return countReason(events, corev1.EventTypeWarning, ReasonTopologyManagerStartFailed)
}

func (h *startEventHarness) startFailureTracked(nn types.NamespacedName) bool {
	h.r.mu.RLock()
	defer h.r.mu.RUnlock()
	_, failing := h.r.startFailures[nn]
	_, warned := h.r.lastStartWarning[nn]
	return failing || warned
}

// A group that never had a manager is warned about once its start failure
// outlives one resync, and not again inside the 5-minute floor.
func TestStartFailureEvent_NeverStartedWarnsAfterOneResync(t *testing.T) {
	h := newStartEventHarness(t)
	h.breakSecret()

	if ev := h.sync(); len(ev) != 0 {
		t.Fatalf("first failure: want no events, got %v", ev)
	}
	ev := h.sync()
	if startFailureWarnings(ev) != 1 || len(ev) != 1 {
		t.Fatalf("second failure: want one %s Warning, got %v", ReasonTopologyManagerStartFailed, ev)
	}
	if !strings.Contains(ev[0], "no topology manager is running") || !strings.Contains(ev[0], "missing 'dsn' key") {
		t.Fatalf("Warning message lacks the impact or the cause: %q", ev[0])
	}
	for i := 3; i <= 10; i++ {
		if ev := h.sync(); len(ev) != 0 {
			t.Fatalf("sync %d (inside the floor): want no events, got %v", i, ev)
		}
	}
}

// Tearing down a working manager takes the group dark at once, so the first
// failed restart is already worth a Warning.
func TestStartFailureEvent_TornDownManagerWarnsImmediately(t *testing.T) {
	h := newStartEventHarness(t)
	if ev := h.sync(); len(ev) != 0 {
		t.Fatalf("healthy start: want no events, got %v", ev)
	}
	if registeredManager(h.r, FailoverGroupNamespacedName(h.fg)) == nil {
		t.Fatal("setup: no manager started")
	}
	h.breakSecret()
	if ev := h.sync(); startFailureWarnings(ev) != 1 {
		t.Fatalf("failed restart: want one Warning, got %v", ev)
	}
	if ev := h.sync(); len(ev) != 0 {
		t.Fatalf("next failure: want no events, got %v", ev)
	}
}

// Error text that changes every sync must not bypass the floor: the Warning
// budget is shared with every other Warning on the group.
func TestStartFailureEvent_AlternatingErrorsRespectFloor(t *testing.T) {
	h := newStartEventHarness(t)
	h.breakSecret()

	var warnedAt []time.Time
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			h.deleteSecret()
		} else {
			h.breakSecret()
		}
		ev := h.sync()
		for range startFailureWarnings(ev) {
			warnedAt = append(warnedAt, h.clock)
		}
	}
	// 40 syncs span 20 minutes: the first eligible Warning plus at most one
	// per 5 minutes after it.
	if len(warnedAt) == 0 || len(warnedAt) > 4 {
		t.Fatalf("want 1-4 Warnings over 20 minutes of alternating errors, got %d at %v", len(warnedAt), warnedAt)
	}
	for i := 1; i < len(warnedAt); i++ {
		if gap := warnedAt[i].Sub(warnedAt[i-1]); gap < startFailureWarningFloor {
			t.Fatalf("Warnings %d and %d only %s apart, want >= %s", i-1, i, gap, startFailureWarningFloor)
		}
	}
}

// An unchanged failure is re-announced every 10 minutes and not otherwise.
func TestStartFailureEvent_SteadyFailureHeartbeat(t *testing.T) {
	h := newStartEventHarness(t)
	h.breakSecret()

	var warnedAt []time.Duration
	start := h.clock
	for i := 0; i < 50; i++ {
		ev := h.sync()
		for range startFailureWarnings(ev) {
			warnedAt = append(warnedAt, h.clock.Sub(start))
		}
	}
	want := []time.Duration{time.Minute, 11 * time.Minute, 21 * time.Minute}
	if len(warnedAt) != len(want) {
		t.Fatalf("Warnings at %v, want %v", warnedAt, want)
	}
	for i := range want {
		if warnedAt[i] != want[i] {
			t.Fatalf("Warnings at %v, want %v", warnedAt, want)
		}
	}
}

// Recovery after a Warning is announced and clears the episode; a new
// failure soon after still waits for the floor.
func TestStartFailureEvent_RecoveryAnnouncedAndFloorSurvives(t *testing.T) {
	h := newStartEventHarness(t)
	nn := FailoverGroupNamespacedName(h.fg)
	h.breakSecret()
	h.sync()
	if ev := h.sync(); startFailureWarnings(ev) != 1 {
		t.Fatalf("want a Warning, got %v", ev)
	}
	warnedAt := h.clock

	h.fixSecret()
	ev := h.sync()
	if countReason(ev, corev1.EventTypeNormal, ReasonTopologyManagerStarted) != 1 {
		t.Fatalf("recovery: want one Normal %s, got %v", ReasonTopologyManagerStarted, ev)
	}
	if !strings.Contains(ev[0], "after 2 failed attempts") {
		t.Fatalf("recovery message lacks the attempt count: %q", ev[0])
	}
	h.r.mu.RLock()
	_, stillFailing := h.r.startFailures[nn]
	h.r.mu.RUnlock()
	if stillFailing {
		t.Fatal("start-failure episode not cleared after a successful start")
	}

	// Break the running manager: eligible at once, but inside the floor.
	h.breakSecret()
	for h.clock.Add(30*time.Second).Sub(warnedAt) < startFailureWarningFloor {
		if ev := h.sync(); len(ev) != 0 {
			t.Fatalf("at %s after the last Warning: want no events inside the floor, got %v", h.clock.Sub(warnedAt), ev)
		}
	}
	if ev := h.sync(); startFailureWarnings(ev) != 1 {
		t.Fatalf("floor elapsed: want one Warning, got %v", ev)
	}
}

// A failure that clears by the next resync is not worth an Event.
func TestStartFailureEvent_TransientFailureEmitsNothing(t *testing.T) {
	h := newStartEventHarness(t)
	h.breakSecret()
	if ev := h.sync(); len(ev) != 0 {
		t.Fatalf("first failure: want no events, got %v", ev)
	}
	h.fixSecret()
	if ev := h.sync(); len(ev) != 0 {
		t.Fatalf("recovered before any Warning: want no events, got %v", ev)
	}
}

// Deleting the group drops its bookkeeping, and a recreated group starts a
// new episode.
func TestStartFailureEvent_DeletedGroupPruned(t *testing.T) {
	h := newStartEventHarness(t)
	ctx := context.Background()
	nn := FailoverGroupNamespacedName(h.fg)
	h.breakSecret()
	h.sync()
	if ev := h.sync(); startFailureWarnings(ev) != 1 {
		t.Fatalf("want a Warning, got %v", ev)
	}

	var cur v1alpha1.MysqlFailoverGroup
	if err := h.c.Get(ctx, client.ObjectKeyFromObject(h.fg), &cur); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if err := h.c.Delete(ctx, &cur); err != nil {
		t.Fatalf("delete fg: %v", err)
	}
	h.sync()
	if h.startFailureTracked(nn) {
		t.Fatal("start-failure bookkeeping survived the group's deletion")
	}

	recreated := h.fg.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = "test-uid-recreated"
	if err := h.c.Create(ctx, recreated); err != nil {
		t.Fatalf("recreate fg: %v", err)
	}
	if ev := h.sync(); len(ev) != 0 {
		t.Fatalf("recreated group's first failure: want no events, got %v", ev)
	}
	if ev := h.sync(); startFailureWarnings(ev) != 1 {
		t.Fatalf("recreated group's second failure: want one Warning, got %v", ev)
	}
}

// Event messages are readable by anyone who can list Events in the
// namespace, so a DSN parse failure must not carry the password.
func TestStartFailureEvent_MessageOmitsDSNPassword(t *testing.T) {
	h := newStartEventHarness(t)
	h.setDSN(map[string][]byte{"dsn": []byte("op:s3cr3t-pw@tcp(127.0.0.1:1/mysql")})
	h.sync()
	ev := h.sync()
	if startFailureWarnings(ev) != 1 {
		t.Fatalf("want a Warning for the malformed DSN, got %v", ev)
	}
	if !strings.Contains(ev[0], "DSN") {
		t.Fatalf("Warning does not name the DSN failure: %q", ev[0])
	}
	if strings.Contains(ev[0], "s3cr3t-pw") {
		t.Fatalf("Warning leaks the DSN password: %q", ev[0])
	}
}
