package controller

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
)

// pointInTimePartialMessage is status.message (and the PointInTimePartial
// Event) for a Partial outcome.
func pointInTimePartialMessage(res *v1alpha1.PointInTimeResult) string {
	requested, newest := "none", "none"
	if res.StopTime != nil {
		requested = res.StopTime.UTC().Format(backupmeta.MicroTimeLayout)
	}
	if res.NewestReplayedCommitTime != nil {
		newest = res.NewestReplayedCommitTime.UTC().Format(backupmeta.MicroTimeLayout)
	}
	return fmt.Sprintf("PointInTimePartial: requested %s; the archive holds no transaction at or after it, so replay stopped at the archive head (newest replayed commit %s)", requested, newest)
}

// onHistoryExclusionsMessage is the PITROnHistoryTransactionsExcluded text
// (§7.2, D-48).
func onHistoryExclusionsMessage(res *v1alpha1.PointInTimeResult) string {
	set := res.OnHistoryExclusions
	if set == "" {
		set = fmt.Sprintf("(%d transactions; see the Job log)", res.OnHistoryExclusionCount)
	}
	return fmt.Sprintf("excluded %s precedes replayed transactions on the target history; their GTIDs were skipped but later row images may carry their effects; the result is not certified for them", set)
}

// Event reasons #206 adds follow A-74 (proposals/20): <Subject><PastTenseVerb>
// with PITR as the only acronym. PointInTimePartial predates #206 and is
// renamed by WS-E (E12) together with status.restore.reason.
const (
	eventReasonPITRDivergentHistoryExcluded      = "PITRDivergentHistoryExcluded"
	eventReasonPITROnHistoryTransactionsExcluded = "PITROnHistoryTransactionsExcluded"
)

// emitPointInTimeEvents records the Warnings a certified replay result
// carries (§7.2) on obj, a MysqlFailoverGroup or a MysqlBackupVerification.
// Callers emit them once, after the status write that records res.
func emitPointInTimeEvents(rec record.EventRecorder, obj runtime.Object, res *v1alpha1.PointInTimeResult) {
	if rec == nil || res == nil {
		return
	}
	if res.Outcome == v1alpha1.PointInTimePartial {
		rec.Eventf(obj, corev1.EventTypeWarning, "PointInTimePartial", "%s", pointInTimePartialMessage(res))
	}
	if res.ExcludedDivergentTransactionCount > 0 {
		rec.Eventf(obj, corev1.EventTypeWarning, eventReasonPITRDivergentHistoryExcluded,
			"replay excluded %d transactions committed before the target that are outside the target history", res.ExcludedDivergentTransactionCount)
	}
	if res.OnHistoryExclusionCount > 0 {
		rec.Eventf(obj, corev1.EventTypeWarning, eventReasonPITROnHistoryTransactionsExcluded, "%s", onHistoryExclusionsMessage(res))
	}
}
