package scenarios

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestBootstrapCompletedSince(t *testing.T) {
	request := time.Date(2026, 11, 7, 12, 0, 0, 600_000_000, time.UTC)
	bootstrapping := func(status metav1.ConditionStatus, reason string, at time.Time) *metav1.Condition {
		return &metav1.Condition{
			Type: v1alpha1.ConditionBootstrapping, Status: status, Reason: reason,
			LastTransitionTime: metav1.NewTime(at),
		}
	}
	done := func(at time.Time) *metav1.Condition {
		return bootstrapping(metav1.ConditionFalse, v1alpha1.ReasonNotBootstrapping, at)
	}
	site := func(name string, replication v1alpha1.SiteReplicationState) v1alpha1.SiteStatus {
		return v1alpha1.SiteStatus{Name: name, Replication: replication}
	}
	healthyPDX := []v1alpha1.SiteStatus{site("iad", ""), site("pdx", v1alpha1.SiteReplicationHealthy)}

	cases := []struct {
		name      string
		condition *metav1.Condition
		sites     []v1alpha1.SiteStatus
		want      bool
		wantMsg   string
	}{
		{name: "no Bootstrapping condition", sites: healthyPDX, wantMsg: "no Bootstrapping condition yet"},
		{name: "clone running", condition: bootstrapping(metav1.ConditionTrue, v1alpha1.ReasonCloning, request.Add(time.Minute)), sites: healthyPDX, wantMsg: "status=True"},
		{name: "clone failed", condition: bootstrapping(metav1.ConditionFalse, v1alpha1.ReasonBootstrapFailed, request.Add(time.Minute)), sites: healthyPDX, wantMsg: "reason=BootstrapFailed"},
		{name: "unknown status", condition: bootstrapping(metav1.ConditionUnknown, v1alpha1.ReasonNotBootstrapping, request.Add(time.Minute)), sites: healthyPDX, wantMsg: "status=Unknown"},
		{name: "transition before the request", condition: done(request.Add(-2 * time.Second)), sites: healthyPDX, wantMsg: "predates the request"},
		// Condition times have second precision: a transition in the
		// request's second, written as its truncated value, still counts.
		{name: "same-second transition", condition: done(request.Truncate(time.Second)), sites: healthyPDX, want: true},
		{name: "transition after the request", condition: done(request.Add(30 * time.Second)), sites: healthyPDX, want: true},
		{name: "target lagging", condition: done(request.Add(time.Minute)), sites: []v1alpha1.SiteStatus{site("pdx", v1alpha1.SiteReplicationLagging)}, want: true},
		{name: "target replication unknown", condition: done(request.Add(time.Minute)), sites: []v1alpha1.SiteStatus{site("pdx", v1alpha1.SiteReplicationUnknown)}, wantMsg: "pdx replication=Unknown"},
		{name: "target replication broken", condition: done(request.Add(time.Minute)), sites: []v1alpha1.SiteStatus{site("pdx", v1alpha1.SiteReplicationBroken)}, wantMsg: "pdx replication=Broken"},
		{name: "target replication absent", condition: done(request.Add(time.Minute)), sites: []v1alpha1.SiteStatus{site("pdx", "")}, wantMsg: "pdx replication="},
		{name: "target missing from status", condition: done(request.Add(time.Minute)), sites: []v1alpha1.SiteStatus{site("iad", v1alpha1.SiteReplicationHealthy)}, wantMsg: "pdx not in status.sites"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mfg := &v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{Sites: tc.sites}}
			if tc.condition != nil {
				mfg.Status.Conditions = []metav1.Condition{*tc.condition}
			}
			got, msg := bootstrapCompletedSince(mfg, request, "pdx")
			if got != tc.want {
				t.Fatalf("bootstrapCompletedSince = %v (%s), want %v", got, msg, tc.want)
			}
			if tc.wantMsg != "" && !strings.Contains(msg, tc.wantMsg) {
				t.Fatalf("message %q does not contain %q", msg, tc.wantMsg)
			}
		})
	}
}

func TestBootstrapRunningAndFailed(t *testing.T) {
	cases := []struct {
		name            string
		condition       *metav1.Condition
		running, failed bool
	}{
		{name: "absent"},
		{name: "running", condition: &metav1.Condition{Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonCloning}, running: true},
		{name: "not bootstrapping", condition: &metav1.Condition{Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNotBootstrapping}},
		{name: "failed", condition: &metav1.Condition{Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonBootstrapFailed}, failed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bootstrapRunning(tc.condition); got != tc.running {
				t.Fatalf("bootstrapRunning = %v, want %v", got, tc.running)
			}
			if got := bootstrapFailed(tc.condition); got != tc.failed {
				t.Fatalf("bootstrapFailed = %v, want %v", got, tc.failed)
			}
		})
	}
}
