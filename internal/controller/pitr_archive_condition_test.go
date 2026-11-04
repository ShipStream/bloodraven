package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/state"
)

// pitrObs is one site's poll outcome in a tracker table case.
type pitrObs struct {
	status *internalmysql.ArchiverStatus
	err    error
}

func pitrBool(v bool) *bool { return &v }

func pitrClean() pitrObs {
	return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(true)}}
}
func pitrMiss() pitrObs { return pitrObs{err: errors.New("connection refused")} }
func pitrInitFailed(msg string) pitrObs {
	return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(false), LastError: "archive store init: " + msg}}
}
func pitrInitPending() pitrObs {
	return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(false)}}
}
func pitrRunError(msg string) pitrObs {
	return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(true), LastError: msg}}
}
func pitrDisabled() pitrObs { return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: false}} }

// pitrOld* are payloads from a sidecar that predates storeReady.
func pitrOldClean() pitrObs { return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true}} }
func pitrOldError(msg string) pitrObs {
	return pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, LastError: msg}}
}

type pitrRound struct {
	at         time.Duration
	iad, pdx   pitrObs
	wantStatus metav1.ConditionStatus
	wantReason string
}

func (r pitrRound) results() []archiverPollResult {
	return []archiverPollResult{
		{site: "iad", status: r.iad.status, err: r.iad.err},
		{site: "pdx", status: r.pdx.status, err: r.pdx.err},
	}
}

const (
	cTrue    = metav1.ConditionTrue
	cFalse   = metav1.ConditionFalse
	cUnknown = metav1.ConditionUnknown
)

func TestPITRArchiveTrackerStateMachine(t *testing.T) {
	persistedTrue := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cTrue, Reason: reasonPITRArchiving}
	persistedFalse := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiveStoreInitFailed, Message: "iad: archive store init: boom"}
	ok := pitrClean()

	cases := []struct {
		name      string
		persisted *metav1.Condition
		rounds    []pitrRound
	}{
		{name: "startup is Unknown, first clean round certifies", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "startup with a missed poll stays Unknown", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: pitrMiss(), wantStatus: cUnknown, wantReason: reasonPITRAwaitingFirstPoll},
			{at: 30 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "one missed poll keeps True", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 60 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "three misses over 60s are SidecarUnreachable", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 60 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 90 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cFalse, wantReason: reasonPITRSidecarUnreachable},
		}},
		{name: "three quick misses are not yet unreachable", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 10 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 20 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 70 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cFalse, wantReason: reasonPITRSidecarUnreachable},
		}},
		{name: "init failure is immediate, on any role", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: ok, pdx: pitrInitFailed("read passphrase: permission denied"), wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
		}},
		{name: "a miss never clears a known init failure", rounds: []pitrRound{
			{at: 0, iad: pitrInitFailed("boom"), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 30 * time.Second, iad: pitrMiss(), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 60 * time.Second, iad: ok, pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 90 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "store init in progress is not a failure", rounds: []pitrRound{
			{at: 0, iad: pitrInitPending(), pdx: ok, wantStatus: cUnknown, wantReason: reasonPITRAwaitingFirstPoll},
			{at: 30 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "single lastError blip keeps True", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: pitrRunError("role check: i/o timeout"), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 60 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "two consecutive errors are ArchiverError, recovery needs two clean rounds", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: pitrRunError("load manifest: 403"), pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 60 * time.Second, iad: pitrRunError("load manifest: 403"), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiverError},
			{at: 90 * time.Second, iad: ok, pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiverError},
			{at: 120 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "a miss or error resets recovery progress", rounds: []pitrRound{
			{at: 0, iad: pitrInitFailed("boom"), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 30 * time.Second, iad: ok, pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 60 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 90 * time.Second, iad: ok, pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 120 * time.Second, iad: pitrRunError("blip"), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 150 * time.Second, iad: ok, pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 180 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "old sidecar without storeReady is not an init failure", rounds: []pitrRound{
			{at: 0, iad: pitrOldClean(), pdx: pitrOldClean(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: pitrOldError("upload: 500"), pdx: pitrOldClean(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 60 * time.Second, iad: pitrOldError("upload: 500"), pdx: pitrOldClean(), wantStatus: cFalse, wantReason: reasonPITRArchiverError},
		}},
		{name: "enabled:false while PITR is on is ArchiverDisabled on the second poll", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 30 * time.Second, iad: ok, pdx: pitrDisabled(), wantStatus: cTrue, wantReason: reasonPITRArchiving},
			{at: 60 * time.Second, iad: ok, pdx: pitrDisabled(), wantStatus: cFalse, wantReason: reasonPITRArchiverDisabled},
		}},
		{name: "restart with persisted True needs a fresh observation", persisted: persistedTrue, rounds: []pitrRound{
			{at: 0, iad: ok, pdx: pitrMiss(), wantStatus: cUnknown, wantReason: reasonPITRAwaitingFirstPoll},
			{at: 30 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "restart with persisted False keeps it until two clean rounds", persisted: persistedFalse, rounds: []pitrRound{
			{at: 0, iad: pitrMiss(), pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 30 * time.Second, iad: ok, pdx: ok, wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
			{at: 60 * time.Second, iad: ok, pdx: ok, wantStatus: cTrue, wantReason: reasonPITRArchiving},
		}},
		{name: "init failure outranks unreachable", rounds: []pitrRound{
			{at: 0, iad: ok, pdx: pitrMiss(), wantStatus: cUnknown, wantReason: reasonPITRAwaitingFirstPoll},
			{at: 30 * time.Second, iad: ok, pdx: pitrMiss(), wantStatus: cUnknown, wantReason: reasonPITRAwaitingFirstPoll},
			{at: 60 * time.Second, iad: pitrInitFailed("boom"), pdx: pitrMiss(), wantStatus: cFalse, wantReason: reasonPITRArchiveStoreInitFailed},
		}},
	}

	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newPITRArchiveTracker(tc.persisted)
			if tc.persisted == nil || tc.persisted.Status != cFalse {
				if v := tr.current(); v.Status != cUnknown || v.Reason != reasonPITRAwaitingFirstPoll {
					t.Fatalf("before any poll: %+v, want Unknown/%s", v, reasonPITRAwaitingFirstPoll)
				}
			}
			for i, r := range tc.rounds {
				tr.observe(t0.Add(r.at), r.results())
				v := tr.current()
				if v.Status != r.wantStatus || v.Reason != r.wantReason {
					t.Fatalf("round %d (t+%v): %s/%s %q, want %s/%s", i, r.at, v.Status, v.Reason, v.Message, r.wantStatus, r.wantReason)
				}
			}
		})
	}
}

func TestPITRArchiveTrackerMessageListsEverySiteSorted(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	tr := newPITRArchiveTracker(nil)
	round := []archiverPollResult{
		{site: "pdx", status: pitrInitFailed("read passphrase: permission denied").status},
		{site: "iad", status: pitrDisabled().status},
		{site: "dfw", status: pitrClean().status},
	}
	for i := 0; i < 3; i++ {
		tr.observe(t0.Add(time.Duration(i)*30*time.Second), round)
	}
	v := tr.current()
	if v.Reason != reasonPITRArchiveStoreInitFailed {
		t.Fatalf("reason = %s, want %s (init failure outranks disabled)", v.Reason, reasonPITRArchiveStoreInitFailed)
	}
	iad, pdx := strings.Index(v.Message, "iad: "), strings.Index(v.Message, "pdx: archive store init: read passphrase: permission denied")
	if iad < 0 || pdx < 0 || iad > pdx || strings.Contains(v.Message, "dfw") {
		t.Fatalf("message = %q, want sorted iad then pdx entries and no healthy site", v.Message)
	}
}

// pitrArchiveRunner builds a runner over a fake client holding fg, with a
// poller for fg's sites wired the way startManager wires it.
func pitrArchiveRunner(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, clients []*internalmysql.SidecarClient) (*TopologyManagerRunner, client.Client, *record.FakeRecorder, *archiverPoller) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).Build()
	rec := record.NewFakeRecorder(100)
	r := &TopologyManagerRunner{
		client:   c,
		recorder: rec,
		logger:   slog.New(slog.DiscardHandler),
		managers: make(map[types.NamespacedName]*managedTopology),
	}
	if clients == nil {
		clients = make([]*internalmysql.SidecarClient, len(fg.Spec.Sites))
	}
	nn := FailoverGroupNamespacedName(fg)
	p := r.newPITRArchiverPoller(context.Background(), fg, nn, clients, nil)
	r.managers[nn] = &managedTopology{archiver: p}
	return r, c, rec, p
}

// pitrArchiveTestPoller builds fg's poller the way startManager does,
// with no sidecar clients.
func pitrArchiveTestPoller(t *testing.T, r *TopologyManagerRunner, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) *archiverPoller {
	t.Helper()
	return r.newPITRArchiverPoller(context.Background(), fg, nn, make([]*internalmysql.SidecarClient, len(fg.Spec.Sites)), nil)
}

func pitrArchiveFG(persisted *metav1.Condition) *v1alpha1.MysqlFailoverGroup {
	fg := pitrEnabledFG("default", "orders")
	fg.Generation = 3
	fg.Spec.Sites = []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}
	// A profile the PITR spec can render, so rollout suppression applies.
	fg.Spec.Backup.Profiles = []v1alpha1.BackupProfile{{Name: "nightly", Storage: v1alpha1.BackupStorage{
		Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{Bucket: "archive"},
	}}}
	if persisted != nil {
		fg.Status.Conditions = []metav1.Condition{*persisted}
	}
	return fg
}

func drainEventsPITR(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func pitrCondition(t *testing.T, c client.Client, nn types.NamespacedName) (*metav1.Condition, *v1alpha1.PITRStatus) {
	t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	return apimeta.FindStatusCondition(got.Status.Conditions, conditionPITRArchiveReady), got.Status.PITR
}

// observeAndWrite feeds one poll round to the poller's tracker and runs
// the poller's status writer, as one Run iteration does.
func observeAndWrite(t *testing.T, r *TopologyManagerRunner, p *archiverPoller, nn types.NamespacedName, at time.Time, round []archiverPollResult) {
	t.Helper()
	p.record(at, round)
	if err := r.updatePITRArchiveStatus(context.Background(), nn); err != nil {
		t.Fatalf("updatePITRArchiveStatus: %v", err)
	}
}

func TestPITRArchiveConditionEventsOnlyOnEdges(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, nil)
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	ok := pitrRound{iad: pitrClean(), pdx: pitrClean()}
	initFail := pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}
	initFail2 := pitrRound{iad: pitrInitFailed("other cause"), pdx: pitrClean()}
	disabled := pitrRound{iad: pitrInitFailed("boom"), pdx: pitrDisabled()}

	steps := []struct {
		round      pitrRound
		wantStatus metav1.ConditionStatus
		wantReason string
		wantEvents []string
	}{
		{ok, cTrue, reasonPITRArchiving, nil},
		{ok, cTrue, reasonPITRArchiving, nil},
		{initFail, cFalse, reasonPITRArchiveStoreInitFailed, []string{"Warning PITRArchiveNotReady"}},
		{initFail, cFalse, reasonPITRArchiveStoreInitFailed, nil},
		{initFail2, cFalse, reasonPITRArchiveStoreInitFailed, nil},
		{disabled, cFalse, reasonPITRArchiveStoreInitFailed, nil},
		{ok, cFalse, reasonPITRArchiveStoreInitFailed, nil},
		{ok, cTrue, reasonPITRArchiving, []string{"Normal PITRArchiveReady"}},
		{ok, cTrue, reasonPITRArchiving, nil},
		{initFail, cFalse, reasonPITRArchiveStoreInitFailed, []string{"Warning PITRArchiveNotReady"}},
	}
	for i, s := range steps {
		observeAndWrite(t, r, p, nn, t0.Add(time.Duration(i)*30*time.Second), s.round.results())
		cond, pitr := pitrCondition(t, c, nn)
		if cond == nil || cond.Status != s.wantStatus || cond.Reason != s.wantReason {
			t.Fatalf("step %d: condition = %+v, want %s/%s", i, cond, s.wantStatus, s.wantReason)
		}
		if cond.ObservedGeneration != fg.Generation {
			t.Fatalf("step %d: observedGeneration = %d, want %d", i, cond.ObservedGeneration, fg.Generation)
		}
		if pitr == nil || !pitr.Enabled {
			t.Fatalf("step %d: status.pitr = %+v", i, pitr)
		}
		events := drainEventsPITR(rec)
		if len(events) != len(s.wantEvents) {
			t.Fatalf("step %d: events = %q, want %q", i, events, s.wantEvents)
		}
		for j, prefix := range s.wantEvents {
			if !strings.HasPrefix(events[j], prefix) {
				t.Fatalf("step %d: event %q, want prefix %q", i, events[j], prefix)
			}
		}
		if s.wantStatus == cFalse && !strings.Contains(cond.Message, "iad: archive store init: ") {
			t.Fatalf("step %d: message = %q, want the iad init error", i, cond.Message)
		}
	}
}

func TestPITRArchiveConditionRestartDoesNotRefire(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	persisted := &metav1.Condition{
		Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiveStoreInitFailed,
		Message: "iad: archive store init: boom", ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0),
	}
	fg := pitrArchiveFG(persisted)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, nil)

	initFail := pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}
	observeAndWrite(t, r, p, nn, t0, initFail.results())
	if ev := drainEventsPITR(rec); len(ev) != 0 {
		t.Fatalf("restart re-fired events for a persisted False: %q", ev)
	}
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse || !cond.LastTransitionTime.Equal(&persisted.LastTransitionTime) {
		t.Fatalf("condition = %+v, want persisted False with its transition time", cond)
	}

	// A restart with a persisted True and a first round that misses a
	// site must not re-certify True, and Unknown fires no Event.
	persistedTrue := persisted.DeepCopy()
	persistedTrue.Status, persistedTrue.Reason, persistedTrue.Message = cTrue, reasonPITRArchiving, "ok"
	fg2 := pitrArchiveFG(persistedTrue)
	r2, c2, rec2, p2 := pitrArchiveRunner(t, fg2, nil)
	observeAndWrite(t, r2, p2, nn, t0, pitrRound{iad: pitrClean(), pdx: pitrMiss()}.results())
	cond, _ = pitrCondition(t, c2, nn)
	if cond == nil || cond.Status != cUnknown || cond.Reason != reasonPITRAwaitingFirstPoll {
		t.Fatalf("condition = %+v, want Unknown/%s", cond, reasonPITRAwaitingFirstPoll)
	}
	if ev := drainEventsPITR(rec2); len(ev) != 0 {
		t.Fatalf("events = %q, want none", ev)
	}
}

func TestPITRArchiveConditionRemovedWhenPITRDisabled(t *testing.T) {
	persisted := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiverError, Message: "iad: x"}
	fg := pitrArchiveFG(persisted)
	fg.Spec.Backup.PITR.Enabled = false
	fg.Status.PITR = &v1alpha1.PITRStatus{Enabled: true, ArchivedFileCount: 4}
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, nil)
	observeAndWrite(t, r, p, nn, time.Now(), pitrRound{iad: pitrDisabled(), pdx: pitrDisabled()}.results())
	cond, pitr := pitrCondition(t, c, nn)
	if cond != nil {
		t.Fatalf("condition = %+v, want removed while PITR is disabled", cond)
	}
	if pitr != nil {
		t.Fatalf("status.pitr = %+v, want nil while PITR is disabled", pitr)
	}
	if ev := drainEventsPITR(rec); len(ev) != 0 {
		t.Fatalf("events = %q, want none", ev)
	}
	if v := p.archiveVerdict(); v.Status != cUnknown {
		t.Fatalf("tracker = %+v, want reset to Unknown while PITR is disabled", v)
	}
}

func TestPITRArchiveUnreachableMarksCoverageLastKnown(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, _, p := pitrArchiveRunner(t, fg, nil)
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	good := pitrClean()
	good.status.ManifestFileCount = 7
	observeAndWrite(t, r, p, nn, t0, []archiverPollResult{{site: "iad", status: good.status}, {site: "pdx", status: pitrClean().status}})
	for i := 1; i <= 3; i++ {
		observeAndWrite(t, r, p, nn, t0.Add(time.Duration(i)*30*time.Second),
			[]archiverPollResult{{site: "iad", err: errors.New("connection refused")}, {site: "pdx", status: pitrClean().status}})
	}
	cond, pitr := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRSidecarUnreachable {
		t.Fatalf("condition = %+v, want False/%s", cond, reasonPITRSidecarUnreachable)
	}
	if pitr == nil || pitr.ArchivedFileCount != 7 {
		t.Fatalf("status.pitr = %+v, want the last-known coverage kept", pitr)
	}
	if !strings.Contains(pitr.Message, "iad: ") || !strings.Contains(pitr.Message, "last-known") {
		t.Fatalf("status.pitr.message = %q, want the unreachable site and a last-known note", pitr.Message)
	}
}

// TestPITRArchiveConditionTopologyWriteDoesNotClobberOrRefire runs the
// topology status writer between poller writes: it must carry the
// poller's current verdict and an edge must fire exactly once whichever
// writer persists it.
func TestPITRArchiveConditionTopologyWriteDoesNotClobberOrRefire(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, nil)
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	snap := TopologySnapshot{ActiveSite: "iad", Sites: []SiteSnapshot{
		{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly},
	}}
	observeAndWrite(t, r, p, nn, t0, pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())

	// The poll round lands, then the topology writer runs before the
	// poller's own writer.
	p.record(t0.Add(30*time.Second), pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}.results())
	if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse {
		t.Fatalf("after topology write: %+v, want False", cond)
	}
	if err := r.updatePITRArchiveStatus(context.Background(), nn); err != nil {
		t.Fatalf("updatePITRArchiveStatus: %v", err)
	}
	snap.Sites[1].State = state.StateUnreachable
	if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}
	var warnings int
	for _, e := range drainEventsPITR(rec) {
		if strings.HasPrefix(e, "Warning PITRArchiveNotReady") {
			warnings++
		}
	}
	if warnings != 1 {
		t.Fatalf("PITRArchiveNotReady events = %d, want exactly 1", warnings)
	}
	cond, _ = pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRArchiveStoreInitFailed {
		t.Fatalf("final condition = %+v", cond)
	}
}

// TestPITRArchiveInitFailureSurfacesThroughPoller drives the real path: a
// real sidecar server whose archive store cannot be built, the operator's
// HTTP client and poller, and the poller's status writer.
func TestPITRArchiveInitFailureSurfacesThroughPoller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The failing site: a real sidecar Server and ArchiverSupervisor. The
	// supervisor logs "failed to init archive store" after recording the
	// failure, which is the test's synchronisation point.
	attempted := make(chan struct{}, 8)
	logger := slog.New(&pitrLogSignal{msg: "failed to init archive store", ch: attempted})
	liveMySQL, err := sidecar.NewLiveMysqlFromDSN("u:p@tcp(127.0.0.1:1)/")
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	defer liveMySQL.Close()
	pitrCfg := &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()},
		ManifestPrefix: "pitr/orders", PassphraseFile: t.TempDir() + "/missing", BinlogDir: t.TempDir(),
		BinlogIndex: "mysql-bin.index", PollInterval: time.Minute}
	sup := sidecar.NewArchiverSupervisor(pitrCfg, "iad", liveMySQL, sidecar.NewArchiveStore, logger)
	srv := sidecar.NewServer(liveMySQL, "127.0.0.1:0", logger)
	srv.SetArchiver(sup)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ctx, ln) }()
	go sup.Run(ctx)
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("archive store init was not attempted")
	}

	// The healthy site answers like a current sidecar with a ready store.
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"enabled":true,"storeReady":true,"site":"pdx","storageType":"PVC","manifestPrefix":"pitr/orders"}`)
	}))
	defer healthy.Close()

	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, []*internalmysql.SidecarClient{
		internalmysql.NewSidecarClient("http://" + ln.Addr().String()),
		internalmysql.NewSidecarClient(healthy.URL),
	})
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		p.now = func() time.Time { return at }
		p.round(ctx)
	}

	cond, pitr := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse || cond.Reason != reasonPITRArchiveStoreInitFailed {
		t.Fatalf("condition = %+v, want False/%s", cond, reasonPITRArchiveStoreInitFailed)
	}
	if !strings.HasPrefix(cond.Message, "iad: archive store init: archive store: read passphrase") {
		t.Fatalf("message = %q", cond.Message)
	}
	if pitr == nil || !strings.Contains(pitr.Message, "iad: archive store init:") {
		t.Fatalf("status.pitr = %+v, want the init error in the message", pitr)
	}
	var warnings []string
	for _, e := range drainEventsPITR(rec) {
		if strings.HasPrefix(e, "Warning PITRArchiveNotReady") {
			warnings = append(warnings, e)
		}
	}
	if len(warnings) != 1 {
		t.Fatalf("Warning events = %q, want exactly one", warnings)
	}
	if !strings.Contains(warnings[0], reasonPITRArchiveStoreInitFailed) {
		t.Fatalf("event = %q, want the reason", warnings[0])
	}
	_ = r
}

// pitrLogSignal is a slog handler that signals when a record with msg is
// logged.
type pitrLogSignal struct {
	msg string
	ch  chan struct{}
}

func (h *pitrLogSignal) Enabled(context.Context, slog.Level) bool { return true }
func (h *pitrLogSignal) Handle(_ context.Context, r slog.Record) error {
	if r.Message == h.msg {
		select {
		case h.ch <- struct{}{}:
		default:
		}
	}
	return nil
}
func (h *pitrLogSignal) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *pitrLogSignal) WithGroup(string) slog.Handler      { return h }
