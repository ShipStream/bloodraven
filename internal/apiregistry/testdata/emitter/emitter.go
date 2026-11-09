// Package emitter is a fixture for TestRegistryCatchesUndeclaredValues and
// TestEventPairsFollowEachCall: it emits one undeclared Event reason through
// a helper, spells one declared condition reason as a literal, records one
// Event whose reason cannot be followed, writes one undeclared metric
// reason label, and records Events through a helper that forwards both the
// Event type and the reason.
package emitter

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

var fixtureFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "fixture_failures_total",
}, []string{"group", "reason"})

func warn(r record.EventRecorder, fg *v1alpha1.MysqlFailoverGroup, reason string) {
	r.Event(fg, corev1.EventTypeWarning, reason, "fixture")
}

// emit forwards both the type and the reason: each call is paired on its own.
func emit(r record.EventRecorder, fg *v1alpha1.MysqlFailoverGroup, eventType, reason string) {
	r.Event(fg, eventType, reason, "fixture")
}

// Emit exercises the failures.
func Emit(r record.EventRecorder, fg *v1alpha1.MysqlFailoverGroup, detail string) metav1.Condition {
	warn(r, fg, "FixtureUndeclaredEvent")
	warn(r, fg, v1alpha1.EventPlannedFailoverStarted)
	r.Event(fg, corev1.EventTypeNormal, fmt.Sprint(detail), "fixture")
	fixtureFailures.WithLabelValues("g", "FixtureUndeclaredMetricReason").Inc()
	fixtureFailures.WithLabelValues("g", v1alpha1.ReasonProfileReadOnly).Inc()
	emit(r, fg, corev1.EventTypeNormal, v1alpha1.EventPlannedFailoverSkipped)
	emit(r, fg, corev1.EventTypeWarning, v1alpha1.EventPlannedFailoverSkipped)
	emit(r, fg, corev1.EventTypeWarning, v1alpha1.EventPlannedFailoverRejected)
	return metav1.Condition{Type: v1alpha1.ConditionReady, Reason: "Bound"}
}
