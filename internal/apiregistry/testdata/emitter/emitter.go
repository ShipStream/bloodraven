// Package emitter is a fixture for TestRegistryCatchesUndeclaredValues: it
// emits one undeclared Event reason through a helper, spells one declared
// condition reason as a literal, and records one Event whose reason cannot
// be followed.
package emitter

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func warn(r record.EventRecorder, fg *v1alpha1.MysqlFailoverGroup, reason string) {
	r.Event(fg, corev1.EventTypeWarning, reason, "fixture")
}

// Emit exercises the three failures.
func Emit(r record.EventRecorder, fg *v1alpha1.MysqlFailoverGroup, detail string) metav1.Condition {
	warn(r, fg, "FixtureUndeclaredEvent")
	warn(r, fg, v1alpha1.EventPlannedFailoverStarted)
	r.Event(fg, corev1.EventTypeNormal, fmt.Sprint(detail), "fixture")
	return metav1.Condition{Type: v1alpha1.ConditionReady, Reason: "Bound"}
}
