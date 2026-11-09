package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func waitFixture(marker string, phase v1alpha1.PlannedFailoverPhase) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "lion"}}
	if marker != "" {
		fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: marker}
	}
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 4, Phase: phase, TargetSite: "pdx", SourceSite: "iad",
		StartTime: metav1.Now()}
	return fg
}

func waitClient(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) *fake.ClientBuilder {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(fg).WithStatusSubresource(fg).
		WithIndex(&corev1.Event{}, "involvedObject.name", func(o client.Object) []string {
			return []string{o.(*corev1.Event).InvolvedObject.Name}
		})
}

// G-m7: a run the terminal marker ended while status still reads Promoting
// is reported from the marker (outcome included), not waited on until the
// timeout.
func TestWaitForPlannedFailover_HonoursTerminalMarker(t *testing.T) {
	cases := []struct {
		name, marker string
		wantErr      string
		wantOut      string
	}{
		{name: "succeeded", marker: `{"schemaVersion":1,"runID":"run-1","outcome":"Succeeded"}`, wantOut: "succeeded (ended by the terminal marker"},
		{name: "left fenced", marker: `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`, wantErr: "SourceLeftFenced:StatusStalled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := waitClient(t, waitFixture(tc.marker, v1alpha1.PlannedFailoverPhasePromoting)).Build()
			var out, errOut bytes.Buffer
			start := time.Now()
			err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, plannedWaitBaseline{site: "pdx"})
			if time.Since(start) > 3*time.Second {
				t.Fatalf("waited %s for a marker-ended run", time.Since(start))
			}
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("err = %v, want nil", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if tc.wantOut != "" && !strings.Contains(out.String(), tc.wantOut) {
				t.Fatalf("stdout = %q, want %q", out.String(), tc.wantOut)
			}
		})
	}
}

// A marker of an earlier run does not end the current one.
func TestPlannedTerminalMarker_OtherRunIgnored(t *testing.T) {
	if _, _, ok := plannedTerminalMarker(waitFixture(`{"schemaVersion":1,"runID":"run-0","outcome":"Succeeded"}`, v1alpha1.PlannedFailoverPhasePromoting)); ok {
		t.Fatal("a marker naming another run ended the current one")
	}
}

// Succeeded prints the run's warnings, and detailed status shows the marker.
func TestPlannedFailoverOutput_WarningsAndMarker(t *testing.T) {
	fg := waitFixture("", v1alpha1.PlannedFailoverPhaseSucceeded)
	fg.Status.PlannedFailover.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded,
		CompletionTime: metav1.Now(), Warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningDNSUpdateFailed}}
	cl := waitClient(t, fg).Build()
	var out, errOut bytes.Buffer
	if err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, plannedWaitBaseline{site: "pdx"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "warnings: DNSUpdateFailed") {
		t.Fatalf("stdout = %q", out.String())
	}
	var detail bytes.Buffer
	printPlannedFailover(&detail, waitFixture(`{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`, v1alpha1.PlannedFailoverPhaseDraining))
	if !strings.Contains(detail.String(), "Ended by terminal marker: SourceLeftFenced:StatusStalled") {
		t.Fatalf("status detail = %q", detail.String())
	}
}
