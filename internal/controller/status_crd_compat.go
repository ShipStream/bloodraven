package controller

import (
	"errors"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Status writes against a CRD that is older than the operator.
//
// Helm installs the CRDs of a chart once and does not upgrade them, so an
// operator can run against the CRD of an earlier release until someone
// applies the new one. A status field the old CRD does not know is pruned
// without an error. An enum value it does not know is different: the API
// server refuses the whole status update with 422, so one site in a new
// state would stop every status write of its group, the active site, the
// failover record and the conditions included.
//
// The topology status writer therefore retries a write that was refused
// for an unsupported value with every value a released CRD may not know
// replaced by one all of them accept (degradeStatusForOlderCRD). The
// replacement never reads healthier than the truth: it is the nearest
// "needs attention" value, and the conditions (whose reasons are free
// text), the Events and the log keep the real one. For
// crdDegradedRetryInterval the degraded values are written without trying
// the current ones first, then the current ones are tried again, so an
// upgraded CRD is taken up without a restart.
//
// Add a value here in the release that adds it to an enum of
// MysqlFailoverGroupStatus, and extend TestEnvtest_StatusWrite_OlderCRD
// (test/envtest), which writes against the CRD in
// test/envtest/testdata/crd-pre-own-loss.

// crdDegradedRetryInterval is how long status writes use the degraded
// values after the installed CRD refused the current ones.
const crdDegradedRetryInterval = 10 * time.Minute

// degradeStatusForOlderCRD replaces, in st, every enum value that a
// released CRD does not know by one that every released CRD accepts, and
// reports whether it changed anything.
//
//   - sites[].recoveryState RecloneRequired becomes RecoveryBlocked: the
//     site needs attention and is not promoted. RecoveryInProgress would
//     read as a routine rejoin, and an operator that restarts on it
//     resumes that rejoin.
//   - sites[].sourceConvergenceReason OwnTransactionsLost becomes
//     GTIDDiverged: the reason that goes with the same Blocked state.
func degradeStatusForOlderCRD(st *v1alpha1.MysqlFailoverGroupStatus) bool {
	changed := false
	for i := range st.Sites {
		if st.Sites[i].RecoveryState == v1alpha1.RecoveryStateRecloneRequired {
			st.Sites[i].RecoveryState = v1alpha1.RecoveryStateBlocked
			changed = true
		}
		if st.Sites[i].SourceConvergenceReason == v1alpha1.SourceReasonOwnTransactionsLost {
			st.Sites[i].SourceConvergenceReason = v1alpha1.SourceReasonGTIDDiverged
			changed = true
		}
	}
	return changed
}

// statusRefusedForUnsupportedValue reports whether err is the API server
// refusing an object for an enum value its schema does not list.
func statusRefusedForUnsupportedValue(err error) bool {
	if !apierrors.IsInvalid(err) {
		return false
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		if details := status.Status().Details; details != nil {
			for _, cause := range details.Causes {
				if cause.Type == metav1.CauseTypeFieldValueNotSupported {
					return true
				}
			}
		}
	}
	return strings.Contains(err.Error(), "Unsupported value")
}

// noteStatusDegradedForOlderCRD records that the installed CRD refused the
// current values (refused is its error) and accepted the degraded ones:
// the writes of the next crdDegradedRetryInterval use the degraded values
// at once, and the condition is logged once per manager.
func (r *TopologyManagerRunner) noteStatusDegradedForOlderCRD(nn types.NamespacedName, mt *managedTopology, refused error) {
	if mt == nil {
		r.logger.Warn("status written with the values an older CRD accepts; the installed CRD is older than the operator",
			"fg", nn, "error", refused,
			"remedy", "apply the CRDs of this release (helm does not upgrade the crds/ of a chart)")
		return
	}
	mt.crdDegraded.Store(time.Now().Add(crdDegradedRetryInterval).UnixNano())
	if mt.crdDegradedLogged.Swap(true) {
		return
	}
	r.logger.Warn("status written with the values an older CRD accepts; the installed CRD is older than the operator",
		"fg", nn, "error", refused,
		"remedy", "apply the CRDs of this release (helm does not upgrade the crds/ of a chart)")
}
