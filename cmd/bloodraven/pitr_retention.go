package main

import (
	"context"
	"sort"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type pitrLookupResult struct {
	status int
	body   map[string]any
}

func resolvePITRCutoff(ctx context.Context, reader client.Reader, namespace, group, profile string) (map[string]any, error) {
	var list v1alpha1.MysqlBackupList
	// Specs are authoritative: labels and the PITR profile do not delimit which
	// group backups may later be used as PITR restore sources.
	if err := reader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	resp := map[string]any{"namespace": namespace, "group": group, "profile": profile, "coverage": sidecar.PITRGTIDCoverageProtocol}
	var cutoff *time.Time
	var retained []string
	unknown, inFlight := false, false
	for i := range list.Items {
		b := &list.Items[i]
		if !b.DeletionTimestamp.IsZero() || b.Spec.FailoverGroupRef.Name != group {
			continue
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
