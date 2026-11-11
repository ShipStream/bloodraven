package scenarios

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestCloneIdentityVerdict(t *testing.T) {
	const a, b = "8d8c6b2a-dd2f-11f1-87db-8e979c620586", "abd7f20b-dd32-11f1-bd72-3ac3991a0bea"
	for _, tc := range []struct {
		name          string
		before, after string
		wantErr       string
	}{
		{name: "rotated", before: a, after: b},
		{name: "kept", before: a, after: a, wantErr: "left its server_uuid " + a + " unchanged"},
		{name: "before unknown", after: b, wantErr: "both must be read"},
		{name: "after unknown", before: a, wantErr: "both must be read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := cloneIdentityVerdict("iad", tc.before, tc.after)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "iad")):
				t.Fatalf("error = %v, want one naming iad and containing %q", err, tc.wantErr)
			}
		})
	}
}

// The Event the check looks for is matched by reason, group, time and the
// start of its message, which names the site.
func TestCloneNotRotatedEventMatch(t *testing.T) {
	since := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	event := func(reason, group, msg string, at time.Time) corev1.Event {
		return corev1.Event{
			InvolvedObject: corev1.ObjectReference{Kind: "MysqlFailoverGroup", Name: group},
			Reason:         reason, Message: msg, LastTimestamp: metav1.NewTime(at),
		}
	}
	msg := fmt.Sprintf(cloneNotRotatedEventPrefix, "iad") + " 8d8c6b2a-dd2f-11f1-87db-8e979c620586: the mysql container's launcher did not remove auto.cnf"
	snippet := fmt.Sprintf(cloneNotRotatedEventPrefix, "iad")
	for _, tc := range []struct {
		name string
		ev   corev1.Event
		want bool
	}{
		{"this site, after the clone", event(v1alpha1.EventCloneServerUUIDNotRotated, "lion", msg, since.Add(time.Minute)), true},
		{"before the clone", event(v1alpha1.EventCloneServerUUIDNotRotated, "lion", msg, since.Add(-time.Minute)), false},
		{"another site", event(v1alpha1.EventCloneServerUUIDNotRotated, "lion", strings.Replace(msg, "site iad ", "site iad2 ", 1), since.Add(time.Minute)), false},
		{"another group", event(v1alpha1.EventCloneServerUUIDNotRotated, "tiger", msg, since.Add(time.Minute)), false},
		{"another reason", event(v1alpha1.EventRecloneRequested, "lion", msg, since.Add(time.Minute)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchMFGEvent(tc.ev, "lion", since, v1alpha1.EventCloneServerUUIDNotRotated, snippet); got != tc.want {
				t.Fatalf("match = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCloneIdentityMirrors: the Event message prefix and the launcher's
// log prefix the scenarios quote are the operator's.
func TestCloneIdentityMirrors(t *testing.T) {
	dir := filepath.Join("..", "..", "controller")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var src strings.Builder
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src.Write(raw)
	}
	code := src.String()
	if !strings.Contains(code, `"`+cloneNotRotatedEventPrefix+" ") {
		t.Errorf("the CloneServerUUIDNotRotated Event message no longer starts %q; update cloneNotRotatedEventPrefix", cloneNotRotatedEventPrefix)
	}
	if !strings.Contains(code, `mysqlLauncherLogPrefix = "bloodraven-launcher:"`) {
		t.Error("the launcher's log prefix is no longer \"bloodraven-launcher:\"; update cloneIdentityVerdict's message")
	}
}
