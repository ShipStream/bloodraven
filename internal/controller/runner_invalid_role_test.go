package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// K073: a running group whose spec changes to a role the operator cannot
// interpret has its manager stopped and unregistered (fail closed), its
// site gauges removed, and a start-failure Warning at once, since it had a
// manager; a second sync starts nothing.
func TestSync_InvalidRoleRetiresRunningManager(t *testing.T) {
	r, c, fg, _ := lifecycleRunner(t)
	rec := record.NewFakeRecorder(16)
	r.recorder = rec
	nn := FailoverGroupNamespacedName(fg)
	ctx := context.Background()

	mustSync(t, r)
	mt := registeredManager(r, nn)
	if mt == nil || managerStopped(mt) {
		t.Fatal("setup: want a running manager")
	}
	site := fg.Spec.Sites[0].Name
	metrics.SiteState.With(prometheus.Labels{"namespace": fg.Namespace, "group": fg.Name, "site": site, "role": "primary-candidate", "state": "writable"}).Set(1)

	var live v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Sites[1].Role = "Bogus"
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		mustSync(t, r)
		if got := registeredManager(r, nn); got != nil {
			t.Fatalf("sync %d left a manager registered for an invalid role", i)
		}
	}
	if !managerStopped(mt) {
		t.Fatal("the running manager was not stopped")
	}
	if n := metricSeriesFor(t, metrics.SiteState, fg.Namespace, fg.Name, site); n != 0 {
		t.Fatalf("%d site-state series left for the retired manager", n)
	}
	warnings := 0
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "Warning") && strings.Contains(e, v1alpha1.EventTopologyManagerStartFailed) {
			warnings++
		}
	}
	if warnings == 0 {
		t.Fatal("no start-failure Warning for the retired manager")
	}
}

func metricSeriesFor(t *testing.T, vec *prometheus.GaugeVec, namespace, group, site string) int {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	vec.Collect(ch)
	close(ch)
	n := 0
	for m := range ch {
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			t.Fatal(err)
		}
		labels := map[string]string{}
		for _, lp := range out.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		if labels["namespace"] == namespace && labels["group"] == group && labels["site"] == site {
			n++
		}
	}
	return n
}
