package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// persistPlannedSucceeded drives the reconciler's planned-failover engine
// through the Publishing pass of a confirmed run to pdx: its Succeeded
// terminal write takes status.activeSite from the manager's authority
// (generation 6, pdx), the planned path's write of status.activeSite.
func persistPlannedSucceeded(t *testing.T, c client.Client, runner *TopologyManagerRunner, nn types.NamespacedName) {
	t.Helper()
	ctx := context.Background()
	var cur v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	confirmed := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	gtid := "uuid-iad:1-10"
	cur.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 4,
		Phase: v1alpha1.PlannedFailoverPhasePublishing, SourceSite: "iad", TargetSite: "pdx", StartTime: confirmed,
		Fence: &v1alpha1.PlannedFailoverFenceRecord{GtidSet: &gtid},
		Promotion: &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionConfirmed,
			ConfirmationTime: &confirmed}}
	if err := c.Status().Update(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	planned := &MysqlFailoverGroupReconciler{Client: c, APIReader: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(10),
		Runner: runner, PlannedTopology: &pfFakeTopology{authority: &PlannedAuthority{ActiveSite: "pdx", TopologyGeneration: 6}}}
	if _, err := planned.plannedFailoverEngine().Step(ctx, nn); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	if pf := cur.Status.PlannedFailover; pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseSucceeded {
		t.Fatalf("planned failover block = %+v, want Succeeded", pf)
	}
}

// G4 / D-46: status.activeSite has two writers, the topology status writer
// and the planned-failover engine's Succeeded terminal write. Whichever
// persists a planned promotion first, PITRArchiveReissueRiskDetected fires
// exactly once for it when the promoted site's server could reissue GTIDs
// its archive holds, and not at all when its divergent rows are only
// evidence (#271).
func TestPITRArchiveReissueRiskBothWriterOrderings(t *testing.T) {
	for _, tc := range []struct {
		name         string
		plannedFirst bool
		// reissuable: pdx's server kept the server_uuid that issued the
		// transactions it lost. Otherwise it has a new one (a clone).
		reissuable bool
	}{
		{"topology writer first", false, true},
		{"planned failover persists first", true, true},
		{"topology writer first, new server_uuid", false, false},
		{"planned failover persists first, new server_uuid", true, false},
	} {
		plannedFirst := tc.plannedFirst
		t.Run(tc.name, func(t *testing.T) {
			fg := identityFG()
			fg.Status.ActiveSite = "iad"
			fg.Status.TopologyGeneration = 5
			nn := client.ObjectKeyFromObject(fg)
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			rec := record.NewFakeRecorder(50)
			reissuable := ""
			if tc.reissuable {
				reissuable = "bbbbbbbb-0000-4000-8000-00000000000b:9-10"
			}
			runner := newRunnerWithArchiverSnapshots(t, nn, map[string]*internalmysql.ArchiverStatus{
				"pdx": {Enabled: true, DivergentRows: 2, ReissuableGTIDs: &reissuable},
			})
			runner.client = c
			runner.managers[nn].tm = &TopologyManager{}
			runner.recorder = rec
			snap := TopologySnapshot{TopologyGeneration: 6, ActiveSite: "pdx", Sites: []SiteSnapshot{
				{Name: "iad", State: state.StateReadOnly}, {Name: "pdx", State: state.StateWritable},
			}}
			persistPlanned := func() { persistPlannedSucceeded(t, c, runner, nn) }
			persistTopology := func() {
				if err := runner.updateCRStatus(context.Background(), nn, snap); err != nil {
					t.Fatal(err)
				}
			}
			if plannedFirst {
				persistPlanned()
				persistTopology()
			} else {
				persistTopology()
				persistPlanned()
			}
			var got []string
			for _, e := range drainRunnerEvents(rec) {
				if strings.Contains(e, v1alpha1.EventPITRArchiveReissueRiskDetected) {
					got = append(got, e)
				}
			}
			if !tc.reissuable {
				if len(got) != 0 {
					t.Fatalf("reissue warnings = %v, want none: pdx's server cannot reissue its divergent rows", got)
				}
			} else if len(got) != 1 || !strings.Contains(got[0], "site pdx was promoted") || !strings.Contains(got[0], "bbbbbbbb-0000-4000-8000-00000000000b:9-10") {
				t.Fatalf("reissue warnings = %v, want exactly one for pdx naming the reissuable set", got)
			}
			var live v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &live); err != nil {
				t.Fatal(err)
			}
			if live.Status.ActiveSite != "pdx" {
				t.Fatalf("activeSite = %q", live.Status.ActiveSite)
			}
		})
	}
}

// F041: a promotion deferred across a persisted NoPrimary interval
// (activeSite iad -> "" -> pdx on a group that has failed over before)
// still warns once, whichever writer persists it; a first bootstrap of a
// group that never failed over does not.
func TestPITRArchiveReissueRiskAfterPersistedNoPrimary(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failedOver   bool
		plannedFirst bool
		want         int
	}{
		{name: "topology writer", failedOver: true, want: 1},
		{name: "planned failover writer", failedOver: true, plannedFirst: true, want: 1},
		{name: "first bootstrap", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := identityFG()
			fg.Status.ActiveSite = ""
			fg.Status.TopologyGeneration = 5
			if tc.failedOver {
				at := metav1.Now()
				fg.Status.SetLastFailover(&at, "iad", "")
			}
			nn := client.ObjectKeyFromObject(fg)
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			rec := record.NewFakeRecorder(50)
			// No verdict from the sidecar: the row count decides.
			runner := newRunnerWithArchiverSnapshots(t, nn, map[string]*internalmysql.ArchiverStatus{
				"pdx": {Enabled: true, QuarantinedRows: 1},
			})
			runner.client = c
			runner.managers[nn].tm = &TopologyManager{}
			runner.recorder = rec
			snap := TopologySnapshot{TopologyGeneration: 6, ActiveSite: "pdx", Sites: []SiteSnapshot{
				{Name: "iad", State: state.StateReadOnly}, {Name: "pdx", State: state.StateWritable},
			}}
			if tc.plannedFirst {
				persistPlannedSucceeded(t, c, runner, nn)
			}
			if err := runner.updateCRStatus(context.Background(), nn, snap); err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, e := range drainRunnerEvents(rec) {
				if strings.Contains(e, v1alpha1.EventPITRArchiveReissueRiskDetected) {
					n++
				}
			}
			if n != tc.want {
				t.Fatalf("%d reissue warnings, want %d", n, tc.want)
			}
		})
	}
}
