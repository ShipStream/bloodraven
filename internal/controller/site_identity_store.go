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
// run under in ServerUUIDHistoryAnnotation on the group: per site a
// timeline, oldest first, of server_uuids and of serverUUIDCloneMarker
// where a clone was installed into the site (SiteIdentityStore).
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
	return parseServerUUIDHistory(raw, s.logger, s.nn)
}

// parseServerUUIDHistory decodes the value of ServerUUIDHistoryAnnotation
// (see parseSiteIdentities).
func parseServerUUIDHistory(raw string, logger *slog.Logger, nn types.NamespacedName) map[string][]string {
	out := map[string][]string{}
	if raw == "" {
		return out
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		if logger != nil {
			logger.Warn("ignoring an undecodable server_uuid history", "fg", nn,
				"annotation", v1alpha1.ServerUUIDHistoryAnnotation, "error", err)
		}
		return map[string][]string{}
	}
	for site, entries := range out {
		var kept []string
		for _, e := range entries {
			kept, _ = appendServerUUIDEntry(kept, normalizeServerUUID(e))
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

// Add appends entry to site's timeline with a write conditioned on the
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
		base := fresh.DeepCopy()
		changed, err := addServerUUIDHistory(&fresh, site, serverUUID, s.logger, s.nn)
		if err != nil || !changed {
			return err
		}
		return s.client.Patch(ctx, &fresh, metadataPatch(base))
	})
	// A group that is gone has no history left to keep.
	return client.IgnoreNotFound(err)
}

// addServerUUIDHistory appends entry (a server_uuid, or
// serverUUIDCloneMarker) to site's timeline in fg's
// ServerUUIDHistoryAnnotation, dropping the oldest entries beyond
// serverUUIDHistoryLimit and the sites the group no longer has. It reports
// whether the annotation changed; the caller writes fg. The clone identity
// record uses it too, so that the server_uuid a clone recipient gives up
// and the clone itself are stored by the write the clone waits for
// (groupCloneIdentityStore.Record).
func addServerUUIDHistory(fg *v1alpha1.MysqlFailoverGroup, site, entry string, logger *slog.Logger, nn types.NamespacedName) (bool, error) {
	entry = normalizeServerUUID(entry)
	if site == "" || entry == "" {
		return false, nil
	}
	records := parseServerUUIDHistory(fg.Annotations[v1alpha1.ServerUUIDHistoryAnnotation], logger, nn)
	next, changed := appendServerUUIDEntry(records[site], entry)
	if !changed {
		return false, nil
	}
	records[site] = next
	if n := len(records[site]); n > serverUUIDHistoryLimit {
		records[site] = records[site][n-serverUUIDHistoryLimit:]
	}
	// A marker with nothing before it says nothing.
	for len(records[site]) > 0 && records[site][0] == serverUUIDCloneMarker {
		records[site] = records[site][1:]
	}
	for name := range records {
		if !slices.ContainsFunc(fg.Spec.Sites, func(sp v1alpha1.SiteSpec) bool { return sp.Name == name }) {
			delete(records, name)
		}
	}
	if len(records[site]) == 0 {
		// The site left the spec meanwhile: nothing to record.
		return false, nil
	}
	raw, err := json.Marshal(records)
	if err != nil {
		return false, fmt.Errorf("encode server_uuid history: %w", err)
	}
	annotations := fg.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[v1alpha1.ServerUUIDHistoryAnnotation] = string(raw)
	fg.SetAnnotations(annotations)
	return true, nil
}
