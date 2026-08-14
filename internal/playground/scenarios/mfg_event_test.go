package scenarios

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMatchMFGEvent(t *testing.T) {
	fg := "mysql-ha"
	notBefore := time.Date(2026, 8, 14, 12, 0, 0, 500*int(time.Millisecond), time.UTC)
	pdxMsg := "refusing to rotate keyring on site pdx: site is the active primary; rotate replicas first"
	iadMsg := "refusing to rotate keyring on site iad: site is the active primary; rotate replicas first"

	fresh := func(msg string) corev1.Event {
		return testMFGEvent(fg, "KeyringRotationRefused", msg, notBefore, time.Time{})
	}

	cases := []struct {
		name    string
		ev      corev1.Event
		snippet string
		want    bool
	}{
		{
			name:    "fresh matching event",
			ev:      fresh(pdxMsg),
			snippet: "refusing to rotate keyring on site pdx: site is the active primary",
			want:    true,
		},
		{
			name:    "stale LastTimestamp",
			ev:      testMFGEvent(fg, "KeyringRotationRefused", pdxMsg, notBefore.Add(-3*time.Second), time.Time{}),
			snippet: "pdx",
			want:    false,
		},
		{
			name:    "2s slack on LastTimestamp",
			ev:      testMFGEvent(fg, "KeyringRotationRefused", pdxMsg, notBefore.Add(-2*time.Second), time.Time{}),
			snippet: "pdx",
			want:    true,
		},
		{
			name:    "EventTime fallback when LastTimestamp is zero",
			ev:      testMFGEvent(fg, "KeyringRotationRefused", pdxMsg, time.Time{}, notBefore),
			snippet: "pdx",
			want:    true,
		},
		{
			name:    "stale EventTime when LastTimestamp is zero",
			ev:      testMFGEvent(fg, "KeyringRotationRefused", pdxMsg, time.Time{}, notBefore.Add(-3*time.Second)),
			snippet: "pdx",
			want:    false,
		},
		{
			name:    "LastTimestamp wins over a fresher EventTime",
			ev:      testMFGEvent(fg, "KeyringRotationRefused", pdxMsg, notBefore.Add(-3*time.Second), notBefore),
			snippet: "pdx",
			want:    false,
		},
		{
			name:    "both timestamps zero",
			ev:      testMFGEvent(fg, "KeyringRotationRefused", pdxMsg, time.Time{}, time.Time{}),
			snippet: "pdx",
			want:    false,
		},
		{
			name:    "wrong reason",
			ev:      testMFGEvent(fg, "KeyringRotated", pdxMsg, notBefore, time.Time{}),
			snippet: "pdx",
			want:    false,
		},
		{
			name:    "wrong InvolvedObject.Name",
			ev:      testMFGEvent("other-fg", "KeyringRotationRefused", pdxMsg, notBefore, time.Time{}),
			snippet: "pdx",
			want:    false,
		},
		{
			name: "wrong InvolvedObject.Kind",
			ev: func() corev1.Event {
				ev := fresh(pdxMsg)
				ev.InvolvedObject.Kind = "Pod"
				return ev
			}(),
			snippet: "pdx",
			want:    false,
		},
		{
			name:    "empty snippet matches any message",
			ev:      fresh(pdxMsg),
			snippet: "",
			want:    true,
		},
		{
			name:    "pdx snippet does not match iad refusal",
			ev:      fresh(iadMsg),
			snippet: "refusing to rotate keyring on site pdx: site is the active primary",
			want:    false,
		},
		{
			name:    "iad snippet matches iad refusal",
			ev:      fresh(iadMsg),
			snippet: "refusing to rotate keyring on site iad: site is the active primary",
			want:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchMFGEvent(tc.ev, fg, notBefore, "KeyringRotationRefused", tc.snippet)
			if got != tc.want {
				t.Fatalf("matchMFGEvent = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchMFGEventMixedListPicksThisRunThisSite(t *testing.T) {
	fg := "mysql-ha"
	notBefore := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	staleIAD := testMFGEvent(fg, "KeyringRotationRefused",
		"refusing to rotate keyring on site iad: site is the active primary; rotate replicas first",
		notBefore.Add(-time.Hour), time.Time{})
	freshPDX := testMFGEvent(fg, "KeyringRotationRefused",
		"refusing to rotate keyring on site pdx: site is the active primary; rotate replicas first",
		notBefore.Add(time.Second), time.Time{})
	events := []corev1.Event{staleIAD, freshPDX}

	snippet := "refusing to rotate keyring on site pdx: site is the active primary"
	var hit *corev1.Event
	for i := range events {
		if matchMFGEvent(events[i], fg, notBefore, "KeyringRotationRefused", snippet) {
			hit = &events[i]
			break
		}
	}
	if hit == nil {
		t.Fatal("expected the fresh pdx event to match")
	}
	if hit.Message != freshPDX.Message {
		t.Fatalf("matched %q, want the fresh pdx refusal", hit.Message)
	}
	if matchMFGEvent(staleIAD, fg, notBefore, "KeyringRotationRefused", "") {
		t.Fatal("stale iad event must not match even with an empty snippet")
	}
}

func testMFGEvent(fg, reason, msg string, last, eventTime time.Time) corev1.Event {
	ev := corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Kind: "MysqlFailoverGroup",
			Name: fg,
		},
		Reason:  reason,
		Message: msg,
	}
	if !last.IsZero() {
		ev.LastTimestamp = metav1.NewTime(last)
	}
	if !eventTime.IsZero() {
		ev.EventTime = metav1.NewMicroTime(eventTime)
	}
	return ev
}
