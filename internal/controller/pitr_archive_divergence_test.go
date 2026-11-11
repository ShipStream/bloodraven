package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// #271: divergence evidence in a site's manifest is information. Only the
// sidecar's lastError makes PITRArchiveReady False; the row counts never do.

const pitrReissuableAuditError = "site iad: server_uuid aaaaaaaa-0000-4000-8000-00000000000a no longer has archived transactions aaaaaaaa-0000-4000-8000-00000000000a:9-10 that it issued (row mysql-bin.000002 id 0f) and reuses their GTIDs when it writes; once the reused GTIDs are archived, every restore from this archive refuses PITRArchiveHistoryReset. Reclone this site before it is promoted (a clone starts with a new server_uuid); if it is already the primary, start a new archive prefix and take a full backup"

// pitrDivergent is a clean archiver whose manifest holds divergent and
// quarantined rows that its server cannot reissue.
func pitrDivergent(divergent, quarantined int64) pitrObs {
	none := ""
	o := pitrClean()
	o.status.DivergentRows, o.status.QuarantinedRows, o.status.ReissuableGTIDs = divergent, quarantined, &none
	return o
}

// pitrReissuable is what a sidecar reports for a server that could reissue
// archived GTIDs: the row, the set and the audit's error.
func pitrReissuable(failedScans int64) pitrObs {
	set := "aaaaaaaa-0000-4000-8000-00000000000a:9-10"
	o := pitrRunError(pitrReissuableAuditError)
	o.status.DivergentRows, o.status.ReissuableGTIDs, o.status.ConsecutiveFailedScans = 1, &set, &failedScans
	return o
}

func TestPITRArchiveTrackerDivergentRowsAreNotAFailure(t *testing.T) {
	t0 := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	ok := pitrClean()
	note := " (" + pitrArchiveDivergenceNoteText + ")"
	steps := []struct {
		name        string
		iad, pdx    pitrObs
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
	}{
		{"clean", ok, ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText},
		{"one divergent row appears", pitrDivergent(1, 0), ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText + "; iad: 1 divergent row" + note},
		{"it stays", pitrDivergent(1, 0), ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText + "; iad: 1 divergent row" + note},
		{"both sites, sorted", pitrDivergent(2, 0), pitrDivergent(3, 1), cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText + "; iad: 2 divergent rows; pdx: 3 divergent rows" + note},
		{"quarantined rows alone say nothing", pitrDivergent(0, 4), ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText},
		// The sidecar reports the divergence that needs action as an error.
		{"reissuable: first failed scan is pending", pitrReissuable(1), ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText},
		{"reissuable: confirmed", pitrReissuable(2), ok, cFalse, v1alpha1.ReasonArchiverError, "iad: " + pitrReissuableAuditError},
		// Recloned with a new server_uuid: the rows stay, the error ends.
		{"recovering", pitrDivergent(1, 0), ok, cFalse, v1alpha1.ReasonArchiverError, "iad: " + pitrReissuableAuditError},
		{"recovered", pitrDivergent(1, 0), ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText + "; iad: 1 divergent row" + note},
	}
	tr := newPITRArchiveTracker(nil)
	for i, s := range steps {
		tr.observe(t0.Add(time.Duration(i)*30*time.Second), []archiverPollResult{
			{site: "pdx", status: s.pdx.status, err: s.pdx.err},
			{site: "iad", status: s.iad.status, err: s.iad.err},
		})
		if v := tr.current(); v.Status != s.wantStatus || v.Reason != s.wantReason || v.Message != s.wantMessage {
			t.Fatalf("step %d (%s): %s/%s %q, want %s/%s %q", i, s.name, v.Status, v.Reason, v.Message, s.wantStatus, s.wantReason, s.wantMessage)
		}
	}
}

// Through the poller's status writer: divergent rows change only the True
// condition's message (written at once, since it names a site), fire no
// Event and leave status.pitr.message empty; a reissuable divergence is the
// sidecar's error, with the usual edge Events.
func TestPITRArchiveConditionReportsDivergenceAsInformation(t *testing.T) {
	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	r, c, rec, p := pitrArchiveRunner(t, fg, nil)
	t0 := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	ok := pitrClean()
	steps := []struct {
		iad         pitrObs
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantInCond  string
		wantPITRMsg string
		wantEvents  []string
	}{
		{ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText, "", nil},
		{pitrDivergent(1, 0), cTrue, v1alpha1.ReasonArchiving, "; iad: 1 divergent row (", "", nil},
		{pitrDivergent(1, 0), cTrue, v1alpha1.ReasonArchiving, "; iad: 1 divergent row (", "", nil},
		{pitrReissuable(2), cFalse, v1alpha1.ReasonArchiverError, "iad: site iad: server_uuid", "iad: site iad: server_uuid", []string{"Warning PITRArchiveNotReady"}},
		{pitrDivergent(1, 0), cFalse, v1alpha1.ReasonArchiverError, "iad: site iad: server_uuid", "", nil},
		{pitrDivergent(1, 0), cTrue, v1alpha1.ReasonArchiving, "; iad: 1 divergent row (", "", []string{"Normal PITRArchiveRecovered"}},
		{ok, cTrue, v1alpha1.ReasonArchiving, pitrArchiveArchivingText, "", nil},
	}
	for i, s := range steps {
		round := []archiverPollResult{{site: "iad", status: s.iad.status}, {site: "pdx", status: ok.status}}
		observeAndWrite(t, r, p, nn, t0.Add(time.Duration(i)*30*time.Second), round)
		cond, pitr := pitrCondition(t, c, nn)
		if cond == nil || cond.Status != s.wantStatus || cond.Reason != s.wantReason || !strings.Contains(cond.Message, s.wantInCond) {
			t.Fatalf("step %d: condition = %+v, want %s/%s with %q", i, cond, s.wantStatus, s.wantReason, s.wantInCond)
		}
		if s.wantInCond == pitrArchiveArchivingText && cond.Message != pitrArchiveArchivingText {
			t.Fatalf("step %d: message = %q, want the plain archiving text", i, cond.Message)
		}
		if pitr == nil || (s.wantPITRMsg == "") != (pitr.Message == "") || !strings.Contains(pitr.Message, s.wantPITRMsg) {
			t.Fatalf("step %d: status.pitr.message = %q, want %q", i, pitr.Message, s.wantPITRMsg)
		}
		events := drainEventsPITR(rec)
		if len(events) != len(s.wantEvents) {
			t.Fatalf("step %d: events = %q, want %q", i, events, s.wantEvents)
		}
		for j, prefix := range s.wantEvents {
			if !strings.HasPrefix(events[j], prefix) {
				t.Fatalf("step %d: event %q, want prefix %q", i, events[j], prefix)
			}
		}
	}
}

// The message change that names a newly divergent site is written at once,
// not held by the message-only rate limit.
func TestPITRArchiveWriteNeededForDivergenceNote(t *testing.T) {
	status := func(msg string) *v1alpha1.MysqlFailoverGroupStatus {
		return &v1alpha1.MysqlFailoverGroupStatus{
			PITR:       &v1alpha1.PITRStatus{},
			Conditions: []metav1.Condition{{Type: v1alpha1.ConditionPITRArchiveReady, Status: cTrue, Reason: v1alpha1.ReasonArchiving, Message: msg}},
		}
	}
	sites := []string{"iad", "pdx"}
	plain := pitrArchiveArchivingMessage([]archiverPollResult{{site: "iad", status: &internalmysql.ArchiverStatus{}}})
	one := pitrArchiveArchivingMessage([]archiverPollResult{{site: "iad", status: &internalmysql.ArchiverStatus{DivergentRows: 1}}})
	two := pitrArchiveArchivingMessage([]archiverPollResult{{site: "iad", status: &internalmysql.ArchiverStatus{DivergentRows: 2}}})
	if plain != pitrArchiveArchivingText {
		t.Fatalf("plain message = %q", plain)
	}
	if immediate, _ := pitrArchiveWriteNeeded(status(plain), status(one), sites); !immediate {
		t.Fatal("a site's first divergent row was not written at once")
	}
	if immediate, _ := pitrArchiveWriteNeeded(status(one), status(plain), sites); !immediate {
		t.Fatal("the end of a site's divergence was not written at once")
	}
	if immediate, messageOnly := pitrArchiveWriteNeeded(status(one), status(two), sites); immediate || !messageOnly {
		t.Fatalf("a changed row count: immediate=%t messageOnly=%t, want a rate-limited message write", immediate, messageOnly)
	}
}
