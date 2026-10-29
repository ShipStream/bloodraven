package controller

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// Re-review N1 (GPT), N6 (Opus): the listed copy still carries a False
// that has since recovered, the uncached read fails and nothing is
// handed over. The replacement must not write that False back or fire a
// Warning, whether its first round misses a site or is clean.
func TestPITRArchiveFailedSeedReadDoesNotResurrectRecoveredFalse(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	persistedFalse := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiveStoreInitFailed,
		Message: "iad: archive store init: boom", ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0.Add(-time.Hour))}
	clean := pitrRound{iad: pitrClean(), pdx: pitrClean()}
	for _, first := range []string{"missed", "clean"} {
		t.Run("first round "+first, func(t *testing.T) {
			fg := pitrArchiveFG(persistedFalse)
			fg.UID = "uid-1"
			nn := FailoverGroupNamespacedName(fg)
			r, c, rec, oldP := pitrArchiveRunner(t, fg, nil)
			listed := fg.DeepCopy()
			observeAndWrite(t, r, oldP, nn, t0, clean.results())
			observeAndWrite(t, r, oldP, nn, t0.Add(30*time.Second), clean.results())
			if cond, _ := pitrCondition(t, c, nn); cond.Status != cTrue {
				t.Fatalf("old poller did not recover: %+v", cond)
			}
			drainEventsPITR(rec)

			delete(r.managers, nn)
			r.apiReader = failingReader{}
			p := r.newPITRArchiverPoller(context.Background(), listed, nn, make([]*internalmysql.SidecarClient, 2), nil)
			r.managers[nn] = &managedTopology{archiver: p}
			round := clean
			if first == "missed" {
				round = pitrRound{iad: pitrMiss(), pdx: pitrClean()}
			}
			observeAndWrite(t, r, p, nn, t0.Add(60*time.Second), round.results())
			cond, _ := pitrCondition(t, c, nn)
			if cond == nil || cond.Status == cFalse {
				t.Fatalf("condition = %+v, the recovered failure was written back", cond)
			}
			if first == "clean" && cond.Status != cTrue {
				t.Fatalf("condition = %+v, want True after a clean first round", cond)
			}
			if w, n := countPITREvents(drainEventsPITR(rec)); w != 0 || n != 0 {
				t.Fatalf("events: %d Warning, %d Normal; want none", w, n)
			}

			// The uncached read recovers: the tracker anchors on the
			// recovered True and keeps its own observations.
			r.apiReader = nil
			observeAndWrite(t, r, p, nn, t0.Add(90*time.Second), clean.results())
			if cond, _ = pitrCondition(t, c, nn); cond == nil || cond.Status != cTrue {
				t.Fatalf("after the read recovered: %+v, want True", cond)
			}
			if w, n := countPITREvents(drainEventsPITR(rec)); w != 0 || n != 0 {
				t.Fatalf("after the read recovered: %d Warning, %d Normal; want none", w, n)
			}
		})
	}
}

// pitrRolloutServers starts an iad sidecar answering healthy and a pdx
// sidecar answering enabled:false.
func pitrRolloutServers(t *testing.T) []*internalmysql.SidecarClient {
	t.Helper()
	iad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"enabled":true,"storeReady":true,"consecutiveFailedScans":0}`)
	}))
	t.Cleanup(iad.Close)
	pdx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"enabled":false}`)
	}))
	t.Cleanup(pdx.Close)
	return []*internalmysql.SidecarClient{internalmysql.NewSidecarClient(iad.URL), internalmysql.NewSidecarClient(pdx.URL)}
}

// Re-review N2 (GPT): a PITR spec that cannot render an archiver (here a
// profile that does not exist) is never "awaiting rollout", even while
// the site's ordered update stays blocked.
func TestPITRArchiveInvalidProfileIsNotMaskedByBlockedRollout(t *testing.T) {
	fg := pitrArchiveFG(nil)
	fg.Spec.Backup.PITR.ProfileName = "does-not-exist"
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, pitrRolloutServers(t))
	_ = r
	p.awaitingRollout = func() []string { return []string{"pdx"} }
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		p.now = func() time.Time { return at }
		p.round(context.Background())
	}
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRArchiverDisabled {
		t.Fatalf("condition = %+v, want False/%s for a profile that cannot render", cond, reasonPITRArchiverDisabled)
	}
	if w, _ := countPITREvents(drainEventsPITR(rec)); w != 1 {
		t.Fatalf("%d Warnings, want 1", w)
	}
}

// Re-review N1 (Opus), N2 (GPT): a valid PITR rollout that never
// completes stops being AwaitingRollout after 30 minutes.
func TestPITRArchiveBlockedRolloutBecomesDisabledAfterBound(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, pitrRolloutServers(t))
	_ = r
	p.awaitingRollout = func() []string { return []string{"pdx"} }
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for d := time.Duration(0); d <= 31*time.Minute; d += 30 * time.Second {
		at := t0.Add(d)
		p.now = func() time.Time { return at }
		p.round(context.Background())
		cond, _ := pitrCondition(t, c, nn)
		switch {
		case d < 30*time.Minute && (cond == nil || cond.Reason != reasonPITRAwaitingRollout):
			t.Fatalf("t+%v: %+v, want Unknown/%s within the bound", d, cond, reasonPITRAwaitingRollout)
		case d >= 30*time.Minute && (cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRArchiverDisabled):
			t.Fatalf("t+%v: %+v, want False/%s after the bound", d, cond, reasonPITRArchiverDisabled)
		}
	}
	cond, _ := pitrCondition(t, c, nn)
	if !strings.Contains(cond.Message, "ordered update still pending") {
		t.Fatalf("message = %q, want it to say the rollout has not delivered archiving", cond.Message)
	}
	if w, _ := countPITREvents(drainEventsPITR(rec)); w != 1 {
		t.Fatalf("%d Warnings, want 1", w)
	}
}

// Re-review N2 (Opus): archivePollInterval raised while the manager runs.
// A single failed scan stays visible for one 15m scan interval and must
// not reach the stuck bound computed from the old 60s interval.
func TestPITRArchiveStuckBoundFollowsCurrentScanInterval(t *testing.T) {
	listed := pitrArchiveFG(nil)
	fg := listed.DeepCopy()
	fg.Spec.Backup.PITR.ArchivePollInterval = &metav1.Duration{Duration: 15 * time.Minute}
	nn := FailoverGroupNamespacedName(fg)
	r, c, _, _ := pitrArchiveRunner(t, fg, nil)
	p := r.newPITRArchiverPoller(context.Background(), listed, nn, make([]*internalmysql.SidecarClient, 2), nil)
	r.managers[nn] = &managedTopology{archiver: p}

	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	observeAndWrite(t, r, p, nn, t0, pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
	for d := 30 * time.Second; d < 15*time.Minute; d += 30 * time.Second {
		observeAndWrite(t, r, p, nn, t0.Add(d), pitrRound{iad: scanObs(t0, pitrInt(1), "load manifest: 503"), pdx: pitrClean()}.results())
		if cond, _ := pitrCondition(t, c, nn); cond.Status != cTrue {
			t.Fatalf("t+%v: %+v, want True for one failed scan at a 15m scan interval", d, cond)
		}
	}
}

// Re-review N3 (Opus): while a site has been silent since the restart,
// counts and the oldest time stay as recorded, but the newest and last
// archived times follow the sites that answer.
func TestPITRArchiveNewestCoverageAdvancesDuringLongSilence(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	oldest, newest, last := metav1.NewTime(t0.Add(-48*time.Hour)), metav1.NewTime(t0.Add(-time.Minute)), metav1.NewTime(t0.Add(-30*time.Second))
	fg := pitrArchiveFG(nil)
	fg.Status.PITR = &v1alpha1.PITRStatus{Enabled: true, ProfileName: "nightly", ArchivedFileCount: 10, ArchivedBytes: 100,
		OldestArchivedTime: &oldest, NewestArchivedTime: &newest, LastArchivedTime: &last}
	nn := FailoverGroupNamespacedName(fg)
	r, c, _, p := pitrArchiveRunner(t, fg, nil)

	var pdxNewest time.Time
	for h := 0; h <= 6; h++ {
		at := t0.Add(time.Duration(h) * time.Hour)
		pdx := pitrClean().status
		pdx.ManifestFileCount, pdx.ManifestBytes = 3, 30
		pdx.OldestArchivedTime = t0.Add(-time.Hour)
		pdxNewest = at.Add(-10 * time.Second)
		pdx.NewestArchivedTime, pdx.LastUploadAt = pdxNewest, at.Add(-5*time.Second)
		p.now = func() time.Time { return at }
		p.record(at, []archiverPollResult{{site: "iad", err: fmt.Errorf("connection refused")}, {site: "pdx", status: pdx}})
		// The topology writer carries coverage; it writes on every call
		// here because the sites' lastSeen changes.
		snap := TopologySnapshot{ActiveSite: "pdx", Sites: []SiteSnapshot{{Name: "iad", LastSeen: at.Add(-time.Hour)}, {Name: "pdx", LastSeen: at}}}
		if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
			t.Fatal(err)
		}
	}
	_, pitr := pitrCondition(t, c, nn)
	if pitr == nil || pitr.ArchivedFileCount != 10 || pitr.ArchivedBytes != 100 || !pitr.OldestArchivedTime.Equal(&oldest) {
		t.Fatalf("status.pitr = %+v, want the recorded counts and oldest time kept", pitr)
	}
	if pitr.NewestArchivedTime == nil || !pitr.NewestArchivedTime.Time.Equal(pdxNewest) {
		t.Fatalf("newestArchivedTime = %v, want pdx's %v", pitr.NewestArchivedTime, pdxNewest)
	}
	if pitr.LastArchivedTime == nil || pitr.LastArchivedTime.Time.Before(pdxNewest) {
		t.Fatalf("lastArchivedTime = %v, want it to follow pdx", pitr.LastArchivedTime)
	}
	if !strings.Contains(pitr.Message, "iad") {
		t.Fatalf("message = %q, want the silent site named", pitr.Message)
	}
}

// A cached read can be stale too: the first object a writer fetches still
// shows a False that the API server no longer has. Adopting it is undone
// when the retry fetches the current object, back to what the tracker
// itself observed.
func TestPITRArchiveStaleFetchedFalseIsDroppedOnRefetch(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	persistedTrue := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cTrue, Reason: reasonPITRArchiving,
		Message: pitrArchiveArchivingText, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0.Add(-time.Hour))}
	staleFalse := metav1.Condition{Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiveStoreInitFailed,
		Message: "iad: archive store init: boom", ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0.Add(-2 * time.Hour))}
	fg := pitrArchiveFG(persistedTrue)
	fg.UID = "uid-1"
	nn := FailoverGroupNamespacedName(fg)

	staleNext := false
	base := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if mfg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && staleNext {
				staleNext = false
				setCondition(&mfg.Status.Conditions, staleFalse)
			}
			return nil
		},
	})
	rec := record.NewFakeRecorder(100)
	r := &TopologyManagerRunner{client: c, apiReader: failingReader{}, recorder: rec,
		logger: slog.New(slog.DiscardHandler), managers: map[types.NamespacedName]*managedTopology{}}
	p := r.newPITRArchiverPoller(context.Background(), fg, nn, make([]*internalmysql.SidecarClient, 2), nil)
	r.managers[nn] = &managedTopology{archiver: p}
	p.record(t0, pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())

	// The topology writer's first Get is stale, its retry Get is current.
	staleNext = true
	snap := TopologySnapshot{ActiveSite: "iad", Sites: []SiteSnapshot{
		{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly},
	}}
	if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
		t.Fatal(err)
	}
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cTrue {
		t.Fatalf("condition = %+v, want True from the tracker's clean round", cond)
	}
	if w, n := countPITREvents(drainEventsPITR(rec)); w != 0 || n != 0 {
		t.Fatalf("events: %d Warning, %d Normal; want none", w, n)
	}
}
