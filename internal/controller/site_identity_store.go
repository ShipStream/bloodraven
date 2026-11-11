package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// groupSiteIdentityStore keeps the server_uuids each site of a group has
// run under in ServerUUIDHistoryAnnotation on the group.
//
// Metadata, not status, for the reasons groupCloneIdentityStore gives: the
// topology status writer rebuilds status from its own snapshot, and a
// status field the installed CRD does not know is pruned without an error.
type groupSiteIdentityStore struct {
	client client.Client
	// reader is the uncached reader, when the runner has one: a uuid
	// recorded by the manager this one replaces must be seen.
	reader client.Reader
	nn     types.NamespacedName
	logger *slog.Logger
}

// siteIdentityStore returns the store of nn's group.
func (r *TopologyManagerRunner) siteIdentityStore(nn types.NamespacedName) SiteIdentityStore {
	return &groupSiteIdentityStore{client: r.client, reader: r.apiReader, nn: nn, logger: r.logger}
}

// parseSiteIdentities decodes ServerUUIDHistoryAnnotation. A value that
// does not decode (edited by hand) is reported and read as empty: the
// history it held is lost, and the uuids the sites run under now are
// recorded again as they are seen.
func (s *groupSiteIdentityStore) parseSiteIdentities(raw string) map[string][]string {
	out := map[string][]string{}
	if raw == "" {
		return out
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		if s.logger != nil {
			s.logger.Warn("ignoring an undecodable server_uuid history", "fg", s.nn,
				"annotation", v1alpha1.ServerUUIDHistoryAnnotation, "error", err)
		}
		return map[string][]string{}
	}
	for site, uuids := range out {
		var kept []string
		for _, u := range uuids {
			if u = normalizeServerUUID(u); u != "" && !slices.Contains(kept, u) {
				kept = append(kept, u)
			}
		}
		if len(kept) == 0 {
			delete(out, site)
		} else {
			out[site] = kept
		}
	}
	return out
}

func (s *groupSiteIdentityStore) Load(ctx context.Context) (map[string][]string, error) {
	reader := s.reader
	if reader == nil {
		reader = s.client
	}
	var fg v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, s.nn, &fg); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return s.parseSiteIdentities(fg.Annotations[v1alpha1.ServerUUIDHistoryAnnotation]), nil
}

// Add appends serverUUID to site's history with a write conditioned on the
// group's resourceVersion, dropping the oldest entries beyond
// serverUUIDHistoryLimit. Sites the group no longer has are dropped with
// the same write.
func (s *groupSiteIdentityStore) Add(ctx context.Context, site, serverUUID string) error {
	serverUUID = normalizeServerUUID(serverUUID)
	if site == "" || serverUUID == "" {
		return nil
	}
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := s.client.Get(ctx, s.nn, &fresh); err != nil {
			return err
		}
		records := s.parseSiteIdentities(fresh.Annotations[v1alpha1.ServerUUIDHistoryAnnotation])
		if slices.Contains(records[site], serverUUID) {
			return nil
		}
		records[site] = append(records[site], serverUUID)
		if n := len(records[site]); n > serverUUIDHistoryLimit {
			records[site] = records[site][n-serverUUIDHistoryLimit:]
		}
		for name := range records {
			if !slices.ContainsFunc(fresh.Spec.Sites, func(sp v1alpha1.SiteSpec) bool { return sp.Name == name }) {
				delete(records, name)
			}
		}
		if len(records[site]) == 0 {
			// The site left the spec meanwhile: nothing to record.
			return nil
		}
		raw, err := json.Marshal(records)
		if err != nil {
			return fmt.Errorf("encode server_uuid history: %w", err)
		}
		base := fresh.DeepCopy()
		annotations := fresh.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[v1alpha1.ServerUUIDHistoryAnnotation] = string(raw)
		fresh.SetAnnotations(annotations)
		return s.client.Patch(ctx, &fresh, metadataPatch(base))
	})
	// A group that is gone has no history left to keep.
	return client.IgnoreNotFound(err)
}
