package scenarios

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgsidecar "github.com/shipstream/bloodraven/internal/sidecar"
)

func TestConfirmTokenAdvances(t *testing.T) {
	base := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	older := newConfirmToken(base, 0)
	newer := newConfirmToken(base, time.Minute)

	// Empty last-used always advances.
	if ok, err := confirmTokenAdvances(newer, ""); err != nil || !ok {
		t.Errorf("empty lastUsed must advance: ok=%v err=%v", ok, err)
	}
	// Strictly newer advances; equal/older does not.
	if ok, err := confirmTokenAdvances(newer, older); err != nil || !ok {
		t.Errorf("newer > older must advance: ok=%v err=%v", ok, err)
	}
	if ok, err := confirmTokenAdvances(older, newer); err != nil || ok {
		t.Errorf("older < newer must NOT advance: ok=%v err=%v", ok, err)
	}
	if ok, err := confirmTokenAdvances(older, older); err != nil || ok {
		t.Errorf("equal token must NOT advance (strictly after): ok=%v err=%v", ok, err)
	}
	// Unparseable spec confirm is an error; unparseable last-used advances.
	if _, err := confirmTokenAdvances("not-a-timestamp", older); err == nil {
		t.Error("unparseable spec confirm must error")
	}
	if ok, err := confirmTokenAdvances(newer, "garbage"); err != nil || !ok {
		t.Errorf("unparseable lastUsed must be treated as advance: ok=%v err=%v", ok, err)
	}
}

func TestNewConfirmTokenIsRFC3339(t *testing.T) {
	tok := newConfirmToken(time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC), 0)
	if _, err := time.Parse(time.RFC3339, tok); err != nil {
		t.Errorf("confirm token %q is not RFC3339: %v", tok, err)
	}
}

func TestSiteLBIPAndStringsContain(t *testing.T) {
	mfg := &v1alpha1.MysqlFailoverGroup{}
	mfg.Spec.Sites = []v1alpha1.SiteSpec{
		{Name: "iad", LBIP: "10.0.0.1"},
		{Name: "pdx", LBIP: "10.0.0.2"},
	}
	if got := siteLBIP(mfg, "pdx"); got != "10.0.0.2" {
		t.Errorf("siteLBIP(pdx) = %q, want 10.0.0.2", got)
	}
	if got := siteLBIP(mfg, "nope"); got != "" {
		t.Errorf("siteLBIP(unknown) = %q, want empty", got)
	}
	if !stringsContain([]string{"10.0.0.1", "10.0.0.2"}, "10.0.0.2") {
		t.Error("stringsContain should find present element")
	}
	if stringsContain([]string{"10.0.0.1"}, "10.0.0.2") {
		t.Error("stringsContain should not find absent element")
	}
}

func TestS35RollbackConverged(t *testing.T) {
	const source, target = "pdx", "iad"

	// Transient "" activeSite right after the terminal Failed/LagTimeout
	// phase (unfence has not yet cleared read_only) must not be treated as
	// done nor as an error — the poll keeps waiting.
	if done, _, err := s35RollbackConverged("", "", source, target); err != nil || done {
		t.Errorf("transient empty activeSite: done=%v err=%v, want done=false err=nil", done, err)
	}

	// Source restored as sole active site, history untouched: done.
	if done, _, err := s35RollbackConverged(source, "", source, target); err != nil || !done {
		t.Errorf("source active, no history: done=%v err=%v, want done=true err=nil", done, err)
	}

	// A stale lastFailoverTarget from a prior, unrelated failover (neither
	// source nor target) must not block convergence once the source is
	// active again.
	if done, _, err := s35RollbackConverged(source, "dc3", source, target); err != nil || !done {
		t.Errorf("source active, unrelated lastFailoverTarget: done=%v err=%v, want done=true err=nil", done, err)
	}

	// The rollback actually leaving the target active is a genuine contract
	// violation: hard error, not a timeout.
	if done, _, err := s35RollbackConverged(target, "", source, target); err == nil || done {
		t.Errorf("target active: done=%v err=%v, want done=false err!=nil", done, err)
	}

	// History advancing to the target is a genuine contract violation even
	// if activeSite has not (yet) followed: hard error.
	if done, _, err := s35RollbackConverged("", target, source, target); err == nil || done {
		t.Errorf("lastFailoverTarget advanced to target: done=%v err=%v, want done=false err!=nil", done, err)
	}
	if done, _, err := s35RollbackConverged(source, target, source, target); err == nil || done {
		t.Errorf("source active but lastFailoverTarget advanced to target: done=%v err=%v, want done=false err!=nil", done, err)
	}
}

func TestRestoreInPlaceStatusChanged(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 7, 11, 1, 13, 12, 0, time.UTC))
	t1 := metav1.NewTime(time.Date(2026, 7, 11, 1, 16, 39, 0, time.UTC))
	// A second metav1.Time built from the same instant as t0, but a
	// distinct object — mirrors re-fetching the identical stale status from
	// a fresh API GET on a later poll.
	t0Again := metav1.NewTime(t0.Time)

	staleFailed := &v1alpha1.RestoreInPlaceStatus{
		Phase:     v1alpha1.RestoreInPlaceFailed,
		Message:   `confirm must be an RFC 3339 timestamp: cannot parse "not-a-timestamp"`,
		StartTime: &t0,
	}

	// No status existed before the patch: any observed status is new.
	if !restoreInPlaceStatusChanged(nil, staleFailed) {
		t.Error("nil stale + non-nil observed must be changed")
	}
	// Both nil: nothing to compare, not changed.
	if restoreInPlaceStatusChanged(nil, nil) {
		t.Error("nil stale + nil observed must not be changed")
	}
	// Stale existed but observed is nil (status cleared): changed.
	if !restoreInPlaceStatusChanged(staleFailed, nil) {
		t.Error("non-nil stale + nil observed must be changed")
	}

	// Re-observing the exact same stale status on a later poll (as happens
	// while the controller has not yet reconciled the freshly-patched spec)
	// must NOT be treated as changed — this is the bug scenario 36 hit live.
	sameAgain := &v1alpha1.RestoreInPlaceStatus{
		Phase:     v1alpha1.RestoreInPlaceFailed,
		Message:   staleFailed.Message,
		StartTime: &t0Again,
	}
	if restoreInPlaceStatusChanged(staleFailed, sameAgain) {
		t.Error("identical re-observed stale status must not be changed")
	}

	// A fresh dispatch always moves to a non-terminal phase first
	// (reconcileInPlaceRestore's cur==nil branch) with a brand new
	// StartTime: must be detected as changed.
	freshPreflight := &v1alpha1.RestoreInPlaceStatus{
		Phase:     v1alpha1.RestoreInPlacePreflight,
		Message:   "validating preconditions",
		StartTime: &t1,
	}
	if !restoreInPlaceStatusChanged(staleFailed, freshPreflight) {
		t.Error("new phase + new startTime must be changed")
	}

	// Same phase and startTime but a different message (e.g. two ticks of
	// the same fresh attempt re-failing) must still be changed — do not
	// rely on startTime alone.
	sameStartDifferentMessage := &v1alpha1.RestoreInPlaceStatus{
		Phase:     staleFailed.Phase,
		Message:   "a materially different failure",
		StartTime: &t0Again,
	}
	if !restoreInPlaceStatusChanged(staleFailed, sameStartDifferentMessage) {
		t.Error("same phase/startTime but different message must be changed")
	}

	// Same phase/message/startTime but a populated jobName (Restoring
	// reached on the same StartTime as a fresh dispatch would carry
	// forward) must be changed.
	withJob := &v1alpha1.RestoreInPlaceStatus{
		Phase:     staleFailed.Phase,
		Message:   staleFailed.Message,
		JobName:   "mysql-fg-iad-inplace-restore",
		StartTime: &t0Again,
	}
	if !restoreInPlaceStatusChanged(staleFailed, withJob) {
		t.Error("different jobName must be changed")
	}
}

func TestManifestNewestLastEvent(t *testing.T) {
	t0 := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	m := &pgsidecar.Manifest{Files: []pgsidecar.ManifestEntry{
		{Name: "a", LastEventTime: t0},
		{Name: "b", LastEventTime: t0.Add(2 * time.Minute)},
		{Name: "c", LastEventTime: t0.Add(time.Minute)},
	}}
	if got := manifestNewestLastEvent(m); !got.Equal(t0.Add(2 * time.Minute)) {
		t.Errorf("newest = %v, want %v", got, t0.Add(2*time.Minute))
	}
	if got := manifestNewestLastEvent(&pgsidecar.Manifest{}); !got.IsZero() {
		t.Errorf("empty manifest newest = %v, want zero", got)
	}
	if got := manifestNewestLastEvent(nil); !got.IsZero() {
		t.Errorf("nil manifest newest = %v, want zero", got)
	}
}

// TestInPlaceRestoreJobNameMirror pins the scenario's mirror of the
// controller's fixed in-place restore Job name. If the controller formula
// (inPlaceRestoreJobName in internal/controller/restore_inplace.go) ever
// changes, the retained-Job sweep in scenario 36 must be updated to match,
// and this guard flags the drift.
func TestInPlaceRestoreJobNameMirror(t *testing.T) {
	cases := []struct {
		fg, site, want string
	}{
		{"playground", "pdx", "mysql-playground-pdx-inplace-restore"},
		{"playground", "iad", "mysql-playground-iad-inplace-restore"},
	}
	for _, c := range cases {
		if got := inPlaceRestoreJobName(c.fg, c.site); got != c.want {
			t.Errorf("inPlaceRestoreJobName(%q,%q) = %q, want %q", c.fg, c.site, got, c.want)
		}
	}
}
