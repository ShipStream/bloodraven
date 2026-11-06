package main

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
)

func TestPlannedHoldAckValue(t *testing.T) {
	held := func() *v1alpha1.MysqlFailoverGroup {
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "orders"}}
		fg.Status.PlannedFailoverHold = &v1alpha1.PlannedFailoverHold{RunID: "run-1", HoldTime: metav1.Now()}
		return fg
	}
	markerOnly := func() *v1alpha1.MysqlFailoverGroup {
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "orders",
			Annotations: map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: brcontroller.TerminalMarker{
				RunID: "run-2", Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}.String()}}}
		return fg
	}
	namesNoRun := func() *v1alpha1.MysqlFailoverGroup {
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "orders"}}
		fg.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonPlannedFailoverManualRecoveryRequired}}
		return fg
	}
	for _, tc := range []struct {
		name    string
		fg      *v1alpha1.MysqlFailoverGroup
		runID   string
		want    string
		wantErr string
	}{
		{name: "status hold", fg: held(), want: "run-1"},
		{name: "explicit matching id", fg: held(), runID: "run-1", want: "run-1"},
		{name: "explicit other id", fg: held(), runID: "run-0", wantErr: "does not name the held run"},
		{name: "marker-only hold", fg: markerOnly(), want: "run-2"},
		{name: "no hold", fg: &v1alpha1.MysqlFailoverGroup{}, wantErr: "has no planned-failover automation hold"},
		{name: "condition naming no run", fg: namesNoRun(), wantErr: "names no run"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := plannedHoldAckValue(tc.fg, tc.runID)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("plannedHoldAckValue = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
