package controller

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// G4 / D-46: status.activeSite has two writers, the topology status writer
// and the planned-failover reconciler. Whichever persists a planned
// promotion first, PITRArchiveReissueRiskDetected fires exactly once for it.
func TestPITRArchiveReissueRiskBothWriterOrderings(t *testing.T) {
	for _, plannedFirst := range []bool{true, false} {
		name := "topology writer first"
		if plannedFirst {
			name = "planned failover persists first"
		}
		t.Run(name, func(t *testing.T) {
			fg := identityFG()
			fg.Status.ActiveSite = "iad"
			fg.Status.TopologyGeneration = 5
			nn := client.ObjectKeyFromObject(fg)
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			rec := record.NewFakeRecorder(50)
			runner := newRunnerWithArchiverSnapshots(t, nn, map[string]*internalmysql.ArchiverStatus{
				"pdx": {Enabled: true, DivergentRows: 2},
			})
			runner.client = c
			runner.managers[nn].tm = &TopologyManager{}
			runner.recorder = rec
			planned := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(10), Runner: runner}

			snap := TopologySnapshot{TopologyGeneration: 6, ActiveSite: "pdx", Sites: []SiteSnapshot{
				{Name: "iad", State: state.StateReadOnly}, {Name: "pdx", State: state.StateWritable},
			}}
			persistPlanned := func() {
				if err := planned.patchFailoverTrackingFields(context.Background(), nn, "pdx", "", metav1.Now()); err != nil {
					t.Fatal(err)
				}
			}
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
				if strings.Contains(e, eventReasonPITRArchiveReissueRisk) {
					got = append(got, e)
				}
			}
			if len(got) != 1 || !strings.Contains(got[0], "site pdx was promoted") {
				t.Fatalf("reissue warnings = %v, want exactly one for pdx", got)
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
				fg.Status.LastFailover, fg.Status.LastFailoverTarget = &at, "iad"
			}
			nn := client.ObjectKeyFromObject(fg)
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			rec := record.NewFakeRecorder(50)
			runner := newRunnerWithArchiverSnapshots(t, nn, map[string]*internalmysql.ArchiverStatus{
				"pdx": {Enabled: true, QuarantinedRows: 1},
			})
			runner.client = c
			runner.managers[nn].tm = &TopologyManager{}
			runner.recorder = rec
			planned := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(10), Runner: runner}
			snap := TopologySnapshot{TopologyGeneration: 6, ActiveSite: "pdx", Sites: []SiteSnapshot{
				{Name: "iad", State: state.StateReadOnly}, {Name: "pdx", State: state.StateWritable},
			}}
			if tc.plannedFirst {
				if err := planned.patchFailoverTrackingFields(context.Background(), nn, "pdx", "", metav1.Now()); err != nil {
					t.Fatal(err)
				}
			}
			if err := runner.updateCRStatus(context.Background(), nn, snap); err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, e := range drainRunnerEvents(rec) {
				if strings.Contains(e, eventReasonPITRArchiveReissueRisk) {
					n++
				}
			}
			if n != tc.want {
				t.Fatalf("%d reissue warnings, want %d", n, tc.want)
			}
		})
	}
}
