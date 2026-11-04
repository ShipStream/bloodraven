package controller

import (
	"context"
	"errors"
	"log/slog"
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
	"github.com/shipstream/bloodraven/internal/state"
)

// Review R2 (GPT): after a restart one site answers and the other has
// never answered in this process. The persisted aggregate coverage must
// stay as it was instead of shrinking to the answering site's share.
func TestPITRArchiveCoverageKeptWhenASiteHasNoSampleAfterRestart(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	oldest, newest, last := metav1.NewTime(t0.Add(-48*time.Hour)), metav1.NewTime(t0.Add(-time.Minute)), metav1.NewTime(t0.Add(-30*time.Second))
	fg := pitrArchiveFG(nil)
	fg.Status.PITR = &v1alpha1.PITRStatus{
		Enabled: true, ProfileName: "nightly", ArchivedFileCount: 10, ArchivedBytes: 100,
		OldestArchivedTime: &oldest, NewestArchivedTime: &newest, LastArchivedTime: &last,
	}
	nn := FailoverGroupNamespacedName(fg)
	r, c, _, p := pitrArchiveRunner(t, fg, nil)

	pdx := pitrClean().status
	pdx.ManifestFileCount, pdx.ManifestBytes = 3, 30
	pdx.OldestArchivedTime, pdx.NewestArchivedTime = t0.Add(-time.Hour), t0.Add(-10*time.Minute)
	observeAndWrite(t, r, p, nn, t0, []archiverPollResult{
		{site: "iad", err: errors.New("connection refused")},
		{site: "pdx", status: pdx},
	})
	_, pitr := pitrCondition(t, c, nn)
	if pitr == nil || pitr.ArchivedFileCount != 10 || pitr.ArchivedBytes != 100 {
		t.Fatalf("status.pitr = %+v, want the persisted 10 files / 100 bytes", pitr)
	}
	if pitr.OldestArchivedTime == nil || !pitr.OldestArchivedTime.Equal(&oldest) ||
		pitr.NewestArchivedTime == nil || !pitr.NewestArchivedTime.Equal(&newest) {
		t.Fatalf("coverage bounds = %v..%v, want the persisted %v..%v", pitr.OldestArchivedTime, pitr.NewestArchivedTime, oldest, newest)
	}
	if !strings.Contains(pitr.Message, "iad: archiver status unavailable") {
		t.Fatalf("message = %q, want the unreachable site", pitr.Message)
	}
	if note := pitr.Message[strings.LastIndex(pitr.Message, "; ")+2:]; !strings.Contains(note, "iad") || strings.Contains(note, "pdx") {
		t.Fatalf("coverage note = %q, want it to name iad only", note)
	}
}

// Review R5 (Opus): a persistent error whose text changes every scan
// (S3 RequestID/HostID) must not rewrite the status every poll.
func TestPITRArchiveMessageOnlyChangesAreRateLimited(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, _, p := pitrArchiveRunner(t, fg, nil)
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	s3err := func(id string) pitrObs {
		return pitrRunError("load manifest: operation error S3: GetObject, https response error StatusCode: 403, RequestID: " + id + ", HostID: " + id + "==, api error AccessDenied: Access Denied")
	}
	rv := func() string {
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &got); err != nil {
			t.Fatal(err)
		}
		return got.ResourceVersion
	}
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	step := func(d time.Duration, iad, pdx pitrObs) {
		p.now = func() time.Time { return at(d) }
		observeAndWrite(t, r, p, nn, at(d), pitrRound{iad: iad, pdx: pdx}.results())
	}

	step(0, s3err("A1"), pitrClean())
	step(30*time.Second, s3err("A2"), pitrClean())
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Reason != reasonPITRArchiverError {
		t.Fatalf("condition = %+v, want False/%s", cond, reasonPITRArchiverError)
	}
	written := rv()

	step(60*time.Second, s3err("A3"), pitrClean())
	step(90*time.Second, s3err("A4"), pitrClean())
	if got := rv(); got != written {
		t.Fatal("a message that differs only in RequestID/HostID was written")
	}
	step(120*time.Second, pitrRunError("load manifest: dial tcp: i/o timeout"), pitrClean())
	if got := rv(); got != written {
		t.Fatal("a message-only change was written within 5 minutes of the last write")
	}
	step(30*time.Second+5*time.Minute, pitrRunError("load manifest: dial tcp: i/o timeout"), pitrClean())
	if got := rv(); got == written {
		t.Fatal("a message-only change was still not written after 5 minutes")
	}
	written = rv()
	// A new affected site is not message-only: it is written at once.
	step(6*time.Minute, pitrRunError("load manifest: dial tcp: i/o timeout"), pitrInitFailed("boom"))
	if got := rv(); got == written {
		t.Fatal("a change in the affected-site set was rate-limited")
	}
	cond, _ = pitrCondition(t, c, nn)
	if !strings.Contains(cond.Message, "pdx: archive store init: boom") {
		t.Fatalf("message = %q, want the new site", cond.Message)
	}
}

// Review R2 (Opus): the topology writer runs between its two Gets while
// the poller writer persists a newer condition. It must not replay its
// stale copy, and its Event baseline must be the object it fetched.
func TestPITRArchiveTopologyWriterInterleavedWithPollerWrite(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

	var (
		r            *TopologyManagerRunner
		p            *archiverPoller
		armed        bool
		inHook       bool
		gets         int
		hookFinished bool
	)
	base := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if armed && !inHook {
				if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
					gets++
					if gets == 2 {
						inHook = true
						p.record(t0.Add(30*time.Second), pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}.results())
						if err := r.updatePITRArchiveStatus(ctx, nn); err != nil {
							t.Errorf("poller write: %v", err)
						}
						inHook, hookFinished = false, true
					}
				}
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	rec := record.NewFakeRecorder(100)
	r = &TopologyManagerRunner{client: c, recorder: rec, logger: slog.New(slog.DiscardHandler), managers: map[types.NamespacedName]*managedTopology{}}
	p = pitrArchiveTestPoller(t, r, fg, nn)
	r.managers[nn] = &managedTopology{archiver: p}

	p.record(t0, pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
	if err := r.updatePITRArchiveStatus(context.Background(), nn); err != nil {
		t.Fatal(err)
	}
	drainEventsPITR(rec)

	armed = true
	snap := TopologySnapshot{ActiveSite: "iad", Sites: []SiteSnapshot{
		{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly},
	}}
	if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}
	armed = false
	if !hookFinished {
		t.Fatal("the poller write did not run between the topology writer's Gets")
	}
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Status != cFalse {
		t.Fatalf("after the topology write: %+v, want the poller's False kept", cond)
	}

	p.record(t0.Add(60*time.Second), pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}.results())
	if err := r.updatePITRArchiveStatus(context.Background(), nn); err != nil {
		t.Fatal(err)
	}
	var warnings, normals int
	for _, e := range drainEventsPITR(rec) {
		switch {
		case strings.HasPrefix(e, "Warning PITRArchiveNotReady"):
			warnings++
		case strings.HasPrefix(e, "Normal PITRArchiveReady"):
			normals++
		}
	}
	if warnings != 1 || normals != 0 {
		t.Fatalf("events: %d Warning, %d Normal; want exactly 1 Warning and no Normal", warnings, normals)
	}
}

// Review R1 (Opus), behaviour common to every sidecar version: one
// failed archiver scan stays visible to two 30s polls with the default
// 60s scan interval, and must not be ArchiverError.
func TestPITRArchiveOneFailedScanSeenTwiceStaysTrue(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	failed := func(scanAt time.Time) pitrObs {
		o := pitrRunError("role check: i/o timeout")
		o.status.LastScanAt = scanAt
		return o
	}
	cleanAt := func(scanAt time.Time) pitrObs {
		o := pitrClean()
		o.status.LastScanAt = scanAt
		return o
	}
	tr := newPITRArchiveTracker(nil)
	for i, r := range []pitrRound{
		{at: 0, iad: cleanAt(t0.Add(-time.Minute)), pdx: pitrClean()},
		{at: 10 * time.Second, iad: failed(t0), pdx: pitrClean()},
		{at: 40 * time.Second, iad: failed(t0), pdx: pitrClean()},
		{at: 70 * time.Second, iad: cleanAt(t0.Add(time.Minute)), pdx: pitrClean()},
	} {
		tr.observe(t0.Add(r.at), r.results())
		if v := tr.current(); v.Status != cTrue {
			t.Fatalf("round %d: %s/%s %q, want True for a single failed scan", i, v.Status, v.Reason, v.Message)
		}
	}
}
