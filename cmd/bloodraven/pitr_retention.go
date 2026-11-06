package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type pitrLookupResult struct {
	status int
	body   map[string]any
}

// resolvePITRCutoff computes the binlog retention cutoff. holdReader is an
// uncached reader used only to decide whether a backup being deleted is
// still read by a verification.
func resolvePITRCutoff(ctx context.Context, reader, holdReader client.Reader, namespace, group, archive string) (map[string]any, error) {
	var list v1alpha1.MysqlBackupList
	// Specs are authoritative: labels and the PITR profile do not delimit which
	// group backups may later be used as PITR restore sources.
	if err := reader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	resp := map[string]any{"namespace": namespace, "group": group, "archive": archive, "coverage": sidecar.PITRGTIDCoverageProtocol}
	var cutoff *time.Time
	var retained []string
	unknown, inFlight := false, false
	// One verification List per request, however many backups are stuck
	// Terminating (a wedged cleanup can leave many).
	var readers *controller.BackupReadChecker
	for i := range list.Items {
		b := &list.Items[i]
		if b.Spec.FailoverGroupRef.Name != group {
			continue
		}
		// A backup being deleted no longer bounds retention, unless a
		// verification may be reading it: its artifact is kept until that
		// run stops, and so must be the binlogs a PITR replay reads after
		// it. A stamped hold is enough; otherwise (a run started by an
		// older operator) ask the API server, and fail closed on errors.
		// Holds are only taken on Succeeded backups, so a deleted backup in
		// any other phase is skipped without asking the API server.
		if !b.DeletionTimestamp.IsZero() && b.Status.Phase != v1alpha1.BackupPhaseSucceeded {
			continue
		}
		if !b.DeletionTimestamp.IsZero() && !controller.BackupHasVerificationHold(b) {
			if readers == nil {
				readers = controller.NewBackupReadChecker(holdReader)
			}
			read, err := readers.BeingRead(ctx, b)
			if err != nil {
				return nil, fmt.Errorf("check verification readers of backup %s: %w", b.Name, err)
			}
			if !read {
				continue
			}
		}
		switch b.Status.Phase {
		case v1alpha1.BackupPhaseFailed:
			continue
		case v1alpha1.BackupPhaseSucceeded:
		default:
			// Pending, Running and unknown phases can still produce an older dump.
			inFlight = true
			continue
		}
		set, err := internalmysql.ParseGTIDSet(b.Status.GtidExecuted)
		if err != nil || set.IsEmpty() || b.Status.StartTime == nil || b.Status.StartTime.IsZero() {
			unknown = true
			continue
		}
		retained = append(retained, set.String())
		start := b.Status.StartTime.Time
		if cutoff == nil || start.Before(*cutoff) {
			c := start
			cutoff = &c
		}
	}
	switch {
	case inFlight:
		resp["retentionDeferredReason"] = "backupInFlight"
	case unknown:
		resp["retentionDeferredReason"] = "backupMetadataUnknown"
	case cutoff == nil:
		resp["retentionDeferredReason"] = "noRetainedBackups"
	default:
		sort.Strings(retained)
		resp["cutoffTime"] = cutoff.UTC().Format(time.RFC3339)
		resp["retainedGTIDs"] = retained
	}
	return resp, nil
}
