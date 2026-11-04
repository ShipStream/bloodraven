package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// failingReader stands in for an unavailable uncached API reader.
type failingReader struct{}

func (failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("api server unavailable")
}
func (failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("api server unavailable")
}

func countPITREvents(events []string) (warnings, normals int) {
	for _, e := range events {
		switch {
		case strings.HasPrefix(e, "Warning PITRArchiveNotReady"):
			warnings++
		case strings.HasPrefix(e, "Normal PITRArchiveReady"):
			normals++
		}
	}
	return warnings, normals
}

// Review R1 (GPT): sync lists the group (True), the old poller then
// persists False, and the replacement is built from the listed copy. The
// replacement must neither overwrite that False with Unknown (and later
// re-fire the Warning) nor certify True after one clean round.
func TestPITRArchiveReplacementPollerSeesNewerPersistedFalse(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	persistedTrue := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cTrue, Reason: reasonPITRArchiving,
		Message: pitrArchiveArchivingText, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0.Add(-time.Hour))}
	initFail := pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}
	seeds := []string{"handoff from stopped poller", "authoritative read", "old poller did not stop", "seed read failed, then recovers"}
	for _, seed := range seeds {
		for _, first := range []string{"missed", "clean"} {
			t.Run(seed+"/first round "+first, func(t *testing.T) {
				fg := pitrArchiveFG(persistedTrue)
				fg.UID = "uid-1"
				nn := FailoverGroupNamespacedName(fg)
				r, c, rec, oldP := pitrArchiveRunner(t, fg, nil)
				listed := fg.DeepCopy()

				observeAndWrite(t, r, oldP, nn, t0, initFail.results())
				if w, _ := countPITREvents(drainEventsPITR(rec)); w != 1 {
					t.Fatalf("old poller: %d Warnings, want 1", w)
				}

				prev := r.managers[nn]
				delete(r.managers, nn)
				done := make(chan struct{})
				switch seed {
				case "handoff from stopped poller":
					close(done)
					prev.archiverDone = done
				case "authoritative read":
					prev = nil
				case "old poller did not stop":
					prev.archiverDone = done
				case "seed read failed, then recovers":
					prev = nil
					r.apiReader = failingReader{}
				}
				p := r.newPITRArchiverPoller(context.Background(), listed, nn,
					make([]*internalmysql.SidecarClient, len(listed.Spec.Sites)), prev)
				r.managers[nn] = &managedTopology{archiver: p}

				round := pitrRound{iad: pitrMiss(), pdx: pitrClean()}
				if first == "clean" {
					round = pitrRound{iad: pitrClean(), pdx: pitrClean()}
				}
				observeAndWrite(t, r, p, nn, t0.Add(30*time.Second), round.results())
				cond, _ := pitrCondition(t, c, nn)
				if cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRArchiveStoreInitFailed {
					t.Fatalf("after the replacement's first round: %+v, want the persisted False kept", cond)
				}
				r.apiReader = nil
				observeAndWrite(t, r, p, nn, t0.Add(60*time.Second), initFail.results())
				if first == "clean" {
					observeAndWrite(t, r, p, nn, t0.Add(90*time.Second), pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
					cond, _ = pitrCondition(t, c, nn)
					if cond.Status != cFalse {
						t.Fatalf("one clean round after the failure certified %s", cond.Status)
					}
					observeAndWrite(t, r, p, nn, t0.Add(120*time.Second), pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
					cond, _ = pitrCondition(t, c, nn)
					if cond.Status != cTrue {
						t.Fatalf("two clean rounds: %s, want True", cond.Status)
					}
				}
				w, n := countPITREvents(drainEventsPITR(rec))
				wantNormals := 0
				if first == "clean" {
					wantNormals = 1
				}
				if w != 0 || n != wantNormals {
					t.Fatalf("replacement events: %d Warning, %d Normal; want 0 Warning, %d Normal", w, n, wantNormals)
				}
			})
		}
	}
}

// A stopped poller hands over only to the same group with the same sites.
func TestPITRArchiveHandoffRequiresSameGroupAndSites(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		mutate func(fg *v1alpha1.MysqlFailoverGroup)
	}{
		{"recreated group", func(fg *v1alpha1.MysqlFailoverGroup) { fg.UID = "uid-2" }},
		{"site set changed", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Sites = append(fg.Spec.Sites, fg.Spec.Sites[0])
			fg.Spec.Sites[2].Name = "dfw"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := pitrArchiveFG(nil)
			old.UID = "uid-1"
			r, _, _, oldP := pitrArchiveRunner(t, old, nil)
			oldP.record(t0, pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}.results())
			prev := r.managers[FailoverGroupNamespacedName(old)]
			done := make(chan struct{})
			close(done)
			prev.archiverDone = done

			fg := old.DeepCopy()
			tc.mutate(fg)
			nn := FailoverGroupNamespacedName(fg)
			p := r.newPITRArchiverPoller(context.Background(), fg, nn, make([]*internalmysql.SidecarClient, len(fg.Spec.Sites)), prev)
			if v := p.archiveVerdict(); v.Status != cUnknown {
				t.Fatalf("verdict = %+v, want a fresh Unknown, not the other group's False", v)
			}
		})
	}
}

func scanObs(scanAt time.Time, failed *int64, lastErr string) pitrObs {
	return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(true),
		LastScanAt: scanAt, ConsecutiveFailedScans: failed, LastError: lastErr}}
}

func pitrInt(v int64) *int64 { return &v }

// Review R1 (Opus): debounce ArchiverError on archiver scans. The operator
// polls every 30s and the archiver scans every 60s by default, so one
// failed scan is seen by two polls.
func TestPITRArchiveErrorDebouncesOnScans(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	ok := pitrClean()
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	const errText = "load manifest: StatusCode: 503"
	cases := []struct {
		name   string
		rounds []pitrRound
	}{
		{"one failed scan seen by two polls stays True", []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 10 * time.Second, iad: scanObs(at(0), pitrInt(1), errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 40 * time.Second, iad: scanObs(at(0), pitrInt(1), errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 70 * time.Second, iad: scanObs(at(60*time.Second), pitrInt(0), ""), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{"two failed scans are ArchiverError", []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 10 * time.Second, iad: scanObs(at(0), pitrInt(1), errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 40 * time.Second, iad: scanObs(at(0), pitrInt(1), errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 70 * time.Second, iad: scanObs(at(60*time.Second), pitrInt(2), errText), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiverError},
		}},
		{"older sidecar: a later lastScanAt with an error confirms", []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 10 * time.Second, iad: scanObs(at(0), nil, errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 40 * time.Second, iad: scanObs(at(0), nil, errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 50 * time.Second, iad: scanObs(at(0), nil, errText), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 70 * time.Second, iad: scanObs(at(60*time.Second), nil, errText), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiverError},
		}},
	}
	stuck := []pitrRound{{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving}}
	for d := 30 * time.Second; d <= pitrArchiveErrorStuckAfter+30*time.Second; d += 30 * time.Second {
		want, reason := cTrue, reasonPITRArchiving
		if d-30*time.Second >= pitrArchiveErrorStuckAfter {
			want, reason = cFalse, reasonPITRArchiverError
		}
		stuck = append(stuck, pitrRound{at: d, iad: scanObs(at(0), pitrInt(1), errText), pdx: ok, wantStatus: want, wantReason: reason})
	}
	cases = append(cases, struct {
		name   string
		rounds []pitrRound
	}{"a scan that never completes is ArchiverError after the stuck bound", stuck})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newPITRArchiveTracker(nil)
			for i, r := range tc.rounds {
				tr.observe(t0.Add(r.at), r.results())
				if v := tr.current(); v.Status != r.wantStatus || v.Reason != r.wantReason {
					t.Fatalf("round %d (t+%v): %s/%s %q, want %s/%s", i, r.at, v.Status, v.Reason, v.Message, r.wantStatus, r.wantReason)
				}
			}
		})
	}
}

// Review R3 (Opus): a fresh deploy or a site whose sidecar has never
// answered gets a 5 minute grace before SidecarUnreachable; a crash
// looping old sidecar still surfaces after it.
func TestPITRArchiveNeverAnsweredSiteGetsLongerGrace(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	tr := newPITRArchiveTracker(nil)
	for d := time.Duration(0); d <= pitrArchiveFirstAnswerGrace; d += 30 * time.Second {
		tr.observe(t0.Add(d), pitrRound{iad: pitrClean(), pdx: pitrMiss()}.results())
		v := tr.current()
		if d < pitrArchiveFirstAnswerGrace && v.Status != cUnknown {
			t.Fatalf("t+%v: %s/%s, want Unknown while pdx has never answered", d, v.Status, v.Reason)
		}
		if d == pitrArchiveFirstAnswerGrace && (v.Status != cFalse || v.Reason != reasonPITRSidecarUnreachable) {
			t.Fatalf("t+%v: %s/%s, want False/%s after the grace", d, v.Status, v.Reason, reasonPITRSidecarUnreachable)
		}
	}
}

// Review R3 (Opus): ArchiverDisabled while the site's ordered update is
// still pending is AwaitingRollout, without a Warning; once the rollout is
// no longer pending it is False/ArchiverDisabled.
func TestPITRArchiveDisabledDuringPendingRolloutIsAwaitingRollout(t *testing.T) {
	iad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"enabled":true,"storeReady":true,"consecutiveFailedScans":0}`)
	}))
	defer iad.Close()
	pdx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"enabled":false}`)
	}))
	defer pdx.Close()

	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, []*internalmysql.SidecarClient{
		internalmysql.NewSidecarClient(iad.URL), internalmysql.NewSidecarClient(pdx.URL),
	})
	var mu sync.Mutex
	drift := []string{"pdx"}
	p.awaitingRollout = func() []string {
		mu.Lock()
		defer mu.Unlock()
		return drift
	}
	_ = r
	p.round(context.Background())
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cUnknown || cond.Reason != reasonPITRAwaitingRollout || !strings.Contains(cond.Message, "pdx") {
		t.Fatalf("condition = %+v, want Unknown/%s naming pdx", cond, reasonPITRAwaitingRollout)
	}
	if w, _ := countPITREvents(drainEventsPITR(rec)); w != 0 {
		t.Fatalf("%d Warnings during a pending rollout, want none", w)
	}

	mu.Lock()
	drift = nil
	mu.Unlock()
	p.round(context.Background())
	if cond, _ = pitrCondition(t, c, nn); cond.Status == cFalse {
		t.Fatalf("ArchiverDisabled on the first poll after the rollout cleared: %+v", cond)
	}
	p.round(context.Background())
	cond, _ = pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRArchiverDisabled {
		t.Fatalf("condition = %+v, want False/%s once no rollout is pending", cond, reasonPITRArchiverDisabled)
	}
	if w, _ := countPITREvents(drainEventsPITR(rec)); w != 1 {
		t.Fatalf("%d Warnings, want 1", w)
	}
}

// A known failure is not hidden by a pending rollout on another site.
func TestPITRArchiveAwaitingRolloutKeepsKnownFalse(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	tr := newPITRArchiveTracker(nil)
	tr.observe(t0, pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}.results())
	round := pitrRound{iad: pitrClean(), pdx: pitrDisabled()}.results()
	round[1].awaitingRollout = true
	tr.observe(t0.Add(30*time.Second), round)
	if v := tr.current(); v.Status != cFalse || v.Reason != reasonPITRArchiveStoreInitFailed {
		t.Fatalf("verdict = %+v, want the known False kept", v)
	}
}

// Review R7 (Opus): a round is stamped with the time the poll started, so
// a slow first poll does not shorten the unreachable span.
func TestPITRArchiveRoundStampedWithTickTime(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	now := t0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		now = now.Add(3 * time.Second)
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	fg := pitrArchiveFG(nil)
	_, _, _, p := pitrArchiveRunner(t, fg, []*internalmysql.SidecarClient{
		internalmysql.NewSidecarClient(srv.URL), internalmysql.NewSidecarClient(srv.URL),
	})
	p.onRound = nil
	p.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	p.scan(context.Background())
	p.mu.RLock()
	got := p.tracker.sites["iad"].firstMissAt
	p.mu.RUnlock()
	if !got.Equal(t0) {
		t.Fatalf("first miss stamped %v, want the tick time %v", got, t0)
	}
}

// A handed-over history keeps what the stopped poller knew: the site
// answered before (so the 60s unreachable rule applies, not the
// first-answer grace) and its last good coverage.
func TestPITRArchiveHandoffCarriesSiteHistory(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	fg := pitrArchiveFG(nil)
	fg.UID = "uid-1"
	nn := FailoverGroupNamespacedName(fg)
	r, c, _, oldP := pitrArchiveRunner(t, fg, nil)
	iad := pitrClean().status
	iad.ManifestFileCount = 7
	observeAndWrite(t, r, oldP, nn, t0, []archiverPollResult{{site: "iad", status: iad}, {site: "pdx", status: pitrClean().status}})

	prev := r.managers[nn]
	done := make(chan struct{})
	close(done)
	prev.archiverDone = done
	delete(r.managers, nn)
	p := r.newPITRArchiverPoller(context.Background(), fg, nn, make([]*internalmysql.SidecarClient, 2), prev)
	r.managers[nn] = &managedTopology{archiver: p}

	for i := 1; i <= 3; i++ {
		observeAndWrite(t, r, p, nn, t0.Add(time.Duration(i)*30*time.Second),
			[]archiverPollResult{{site: "iad", err: errors.New("connection refused")}, {site: "pdx", status: pitrClean().status}})
	}
	cond, pitr := pitrCondition(t, c, nn)
	if cond == nil || cond.Reason != reasonPITRSidecarUnreachable {
		t.Fatalf("condition = %+v, want False/%s after 3 misses over 60s", cond, reasonPITRSidecarUnreachable)
	}
	if pitr == nil || pitr.ArchivedFileCount != 7 || !strings.Contains(pitr.Message, "last-known for: iad") {
		t.Fatalf("status.pitr = %+v, want iad's handed-over coverage marked last-known", pitr)
	}
}

// The authoritative read also protects the other direction: a listed
// copy still carrying a False that has since recovered must not bring
// the False (and a Warning) back.
func TestPITRArchiveReplacementIgnoresStaleListedFalse(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	persistedFalse := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiveStoreInitFailed,
		Message: "iad: archive store init: boom", ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0.Add(-time.Hour))}
	fg := pitrArchiveFG(persistedFalse)
	fg.UID = "uid-1"
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, oldP := pitrArchiveRunner(t, fg, nil)
	listed := fg.DeepCopy()
	clean := pitrRound{iad: pitrClean(), pdx: pitrClean()}
	observeAndWrite(t, r, oldP, nn, t0, clean.results())
	observeAndWrite(t, r, oldP, nn, t0.Add(30*time.Second), clean.results())
	if cond, _ := pitrCondition(t, c, nn); cond.Status != cTrue {
		t.Fatalf("old poller did not recover: %+v", cond)
	}
	drainEventsPITR(rec)

	delete(r.managers, nn)
	p := r.newPITRArchiverPoller(context.Background(), listed, nn, make([]*internalmysql.SidecarClient, 2), nil)
	r.managers[nn] = &managedTopology{archiver: p}
	observeAndWrite(t, r, p, nn, t0.Add(60*time.Second), clean.results())
	if cond, _ := pitrCondition(t, c, nn); cond.Status != cTrue {
		t.Fatalf("condition = %+v, want True kept", cond)
	}
	if w, n := countPITREvents(drainEventsPITR(rec)); w != 0 || n != 0 {
		t.Fatalf("events: %d Warning, %d Normal; want none", w, n)
	}
}

// The runner notices spec drift on its own 30s sync, so a poll can see
// enabled:false before the site is marked as awaiting its rollout. That
// first poll must not be ArchiverDisabled.
func TestPITRArchiveDisabledBeforeDriftIsNoticed(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	tr := newPITRArchiveTracker(nil)
	tr.observe(t0, pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
	tr.observe(t0.Add(30*time.Second), pitrRound{iad: pitrClean(), pdx: pitrDisabled()}.results())
	if v := tr.current(); v.Status != cTrue {
		t.Fatalf("first enabled:false poll: %+v, want True kept", v)
	}
	round := pitrRound{iad: pitrClean(), pdx: pitrDisabled()}.results()
	round[1].awaitingRollout = true
	tr.observe(t0.Add(60*time.Second), round)
	if v := tr.current(); v.Status != cUnknown || v.Reason != reasonPITRAwaitingRollout {
		t.Fatalf("after drift is noticed: %+v, want Unknown/%s", v, reasonPITRAwaitingRollout)
	}
}
