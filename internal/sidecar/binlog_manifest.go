package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"
)

// ManifestVersion is the on-disk schema version for site manifests.
// Bumped only when a reader from an older bloodraven release would
// misinterpret a field written by a newer one. Additive changes (new
// optional fields) do NOT bump this.
const ManifestVersion = 1

// ManifestEntry is a single row in a per-site manifest — one archived
// binlog file.
type ManifestEntry struct {
	Name           string    `json:"name"`
	RemotePath     string    `json:"remotePath"`
	Size           int64     `json:"size"`
	FirstEventTime time.Time `json:"firstEventTime,omitempty"`
	LastEventTime  time.Time `json:"lastEventTime,omitempty"`
	PreviousGTIDs  string    `json:"previousGtids,omitempty"`
	EndGTIDs       string    `json:"endGtids,omitempty"`
	ArchivedAt     time.Time `json:"archivedAt"`
}

// Manifest is the JSON document written to storage at
// <prefix>/manifest-<site>.json. One per site so primary and
// post-failover primary don't race on the same object — the restore
// Job merges across all manifests in the prefix.
type Manifest struct {
	Version int             `json:"version"`
	Site    string          `json:"site"`
	Files   []ManifestEntry `json:"files"`
}

// manifestKey returns the storage-relative key for a site's manifest.
// Site names are a-z0-9 plus dashes (enforced by the CRD), so no
// escaping is needed.
func manifestKey(prefix, site string) string {
	return path.Join(prefix, fmt.Sprintf("manifest-%s.json", site))
}

// binlogObjectKey returns the storage-relative key under which a given
// binlog file will be archived. Partitioned by site so two primaries
// (before/after failover) can coexist without filename collisions.
func binlogObjectKey(prefix, site, filename string) string {
	return path.Join(prefix, site, filename)
}

// loadManifest reads and decodes the current manifest for site, or
// returns an empty Manifest{} if none exists yet. A decode error on an
// existing object is propagated — the caller should surface it loudly
// rather than silently reset the manifest.
func loadManifest(ctx context.Context, store archiveStore, prefix, site string) (*Manifest, error) {
	data, ok, err := store.Get(ctx, manifestKey(prefix, site))
	if err != nil {
		return nil, err
	}
	if !ok {
		return &Manifest{Version: ManifestVersion, Site: site, Files: nil}, nil
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", manifestKey(prefix, site), err)
	}
	// Enforce Version/Site invariants even on legacy writes.
	if m.Version == 0 {
		m.Version = ManifestVersion
	}
	if m.Site == "" {
		m.Site = site
	}
	return &m, nil
}

// saveManifest serializes m and writes it to storage under the site's
// manifest key. The Files slice is sorted lexicographically by Name so
// the output is deterministic and diff-friendly in object storage
// browsers.
func saveManifest(ctx context.Context, store archiveStore, prefix string, m *Manifest) error {
	sort.Slice(m.Files, func(i, j int) bool {
		return m.Files[i].Name < m.Files[j].Name
	})
	m.Version = ManifestVersion
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, manifestKey(prefix, m.Site), bytes.NewReader(buf), int64(len(buf)))
}

// appendManifestEntry is a convenience wrapper that loads, upserts by
// Name, and saves. Idempotent: re-appending the same file just
// overwrites its row (archivedAt is bumped to match).
func appendManifestEntry(ctx context.Context, store archiveStore, prefix, site string, entry ManifestEntry) error {
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return err
	}
	found := false
	for i := range m.Files {
		if m.Files[i].Name == entry.Name {
			m.Files[i] = entry
			found = true
			break
		}
	}
	if !found {
		m.Files = append(m.Files, entry)
	}
	return saveManifest(ctx, store, prefix, m)
}

// pruneManifest removes entries whose Name is in drop. Returns the set
// of remote object keys that were referenced by the removed entries so
// the caller can issue Delete calls. The manifest is only rewritten if
// at least one entry was removed.
func pruneManifest(ctx context.Context, store archiveStore, prefix, site string, drop map[string]struct{}) ([]string, error) {
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return nil, err
	}
	if len(drop) == 0 {
		return nil, nil
	}
	var removed []string
	kept := m.Files[:0]
	for _, f := range m.Files {
		if _, d := drop[f.Name]; d {
			removed = append(removed, f.RemotePath)
			continue
		}
		kept = append(kept, f)
	}
	if len(removed) == 0 {
		return nil, nil
	}
	m.Files = kept
	if err := saveManifest(ctx, store, prefix, m); err != nil {
		return nil, err
	}
	return removed, nil
}
