package controller

import (
	"context"
	"time"

	"github.com/shipstream/bloodraven/api/v1alpha1"
)

// reconcilePITRArchiveBindings runs one pass of the PITR archive binding
// engine (#203, IMPL-203 §4.10): list snapshots, recover, check integrity,
// derive targets, advance each entry (consumer proof, check Jobs,
// eligibility, evaluation), drop unused non-Bound entries, process a
// release, write acceptance annotations, write status, emit Events, delete
// consumed check Jobs. It runs after reconcileEncryptionAtRest and before
// reconcileSiteConfigMaps, and leaves fg.Status.PITRArchiveBindings filled
// in memory for the rendering that follows. The duration is a requeue hint
// (0: none).
//
// Step 0 stub: WP-B implements it.
func (r *MysqlFailoverGroupReconciler) reconcilePITRArchiveBindings(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	_, _ = ctx, fg
	return 0, nil
}

// Referenced until Reconcile calls it (WP-C wires the call), so the unused
// linter accepts the stub.
var _ = (*MysqlFailoverGroupReconciler).reconcilePITRArchiveBindings
