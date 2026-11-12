package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// groupCloneIdentityStore keeps the pre-clone server_uuids of a group's
// clone recipients in CloneServerUUIDAnnotation on the group.
//
// Metadata, not status: the topology status writer rebuilds status from
// its own snapshot, and a status field the installed CRD does not know yet
// is pruned without an error, which would silently drop the record. The
// group's metadata already carries the operator's other must-not-lose
// records (LastFailoverAnnotation, PromotionGtidExecutedAnnotation).
type groupCloneIdentityStore struct {
	client client.Client
	// reader is the uncached reader, when the runner has one: a record
	// written by the manager this one replaces must be seen.
	reader client.Reader
	nn     types.NamespacedName
	logger *slog.Logger
}

// cloneIdentityStore returns the store of nn's group.
func (r *TopologyManagerRunner) cloneIdentityStore(nn types.NamespacedName) CloneIdentityStore {
	return &groupCloneIdentityStore{client: r.client, reader: r.apiReader, nn: nn, logger: r.logger}
}

// parseCloneIdentities decodes CloneServerUUIDAnnotation. A value that
// does not decode (edited by hand) is reported and read as empty: the
// records it held are lost, and the next clone writes a valid one.
func (s *groupCloneIdentityStore) parseCloneIdentities(raw string) map[string]CloneIdentity {
	out := map[string]CloneIdentity{}
	if raw == "" {
		return out
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		if s.logger != nil {
			s.logger.Warn("ignoring an undecodable clone server_uuid record", "fg", s.nn,
				"annotation", v1alpha1.CloneServerUUIDAnnotation, "error", err)
		}
		return map[string]CloneIdentity{}
	}
	for site, rec := range out {
		if rec.ServerUUID == "" {
			delete(out, site)
		}
	}
	return out
}

func (s *groupCloneIdentityStore) Load(ctx context.Context) (map[string]CloneIdentity, error) {
	reader := s.reader
	if reader == nil {
		reader = s.client
	}
	var fg v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, s.nn, &fg); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return s.parseCloneIdentities(fg.Annotations[v1alpha1.CloneServerUUIDAnnotation]), nil
}

// mutate applies change to the stored records with a write conditioned on
// the group's resourceVersion. change reports whether anything changed.
// recipient, when it names a site, adds to that site's server_uuid
// timeline, by the same write, the server_uuid it names.
func (s *groupCloneIdentityStore) mutate(ctx context.Context, recipient [2]string, change func(map[string]CloneIdentity) bool) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := s.client.Get(ctx, s.nn, &fresh); err != nil {
			return err
		}
		base := fresh.DeepCopy()
		historyChanged := false
		if recipient[0] != "" {
			changed, err := addServerUUIDHistory(&fresh, recipient[0], recipient[1], s.logger, s.nn)
			if err != nil {
				return err
			}
			historyChanged = changed
		}
		records := s.parseCloneIdentities(fresh.Annotations[v1alpha1.CloneServerUUIDAnnotation])
		if !change(records) {
			if !historyChanged {
				return nil
			}
			return s.client.Patch(ctx, &fresh, metadataPatch(base))
		}
		annotations := fresh.GetAnnotations()
		if len(records) == 0 {
			delete(annotations, v1alpha1.CloneServerUUIDAnnotation)
		} else {
			raw, err := json.Marshal(records)
			if err != nil {
				return fmt.Errorf("encode clone server_uuid records: %w", err)
			}
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[v1alpha1.CloneServerUUIDAnnotation] = string(raw)
		}
		fresh.SetAnnotations(annotations)
		return s.client.Patch(ctx, &fresh, metadataPatch(base))
	})
}

// Record stores site's record and, in the same write, adds the
// server_uuid it names to the site's server_uuid timeline
// (ServerUUIDHistoryAnnotation): the clone gives the recipient a new
// server_uuid, and the one it gives up is one it issued transactions
// under. A clone does not start unless this write succeeded, so a volume
// that later comes back from before the clone is recognised by the
// server_uuid recorded here.
//
// The clone itself is marked on the timeline when it is known to have
// installed (TopologyManager.finishCloneIdentity), and the record stays
// until that marker is stored: a manager that restarts in between finds
// the record and settles the clone again. A clone that fails or never
// begins leaves no marker, so the identity the site keeps is never taken
// for one a clone replaced.
func (s *groupCloneIdentityStore) Record(ctx context.Context, site string, rec CloneIdentity) error {
	return s.mutate(ctx, [2]string{site, rec.ServerUUID}, func(records map[string]CloneIdentity) bool {
		if cur, ok := records[site]; ok && cur.equal(rec) {
			return false
		}
		records[site] = rec
		return true
	})
}

func (s *groupCloneIdentityStore) Resolve(ctx context.Context, site string, rec CloneIdentity) error {
	err := s.mutate(ctx, [2]string{}, func(records map[string]CloneIdentity) bool {
		if cur, ok := records[site]; !ok || !cur.equal(rec) {
			return false
		}
		delete(records, site)
		return true
	})
	// A group that is gone has no record left to remove.
	return client.IgnoreNotFound(err)
}
