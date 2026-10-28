package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
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
	GTIDCoverage *BinlogGTIDCoverage `json:"gtidCoverage,omitempty"`
	// ID identifies sealed bytes (keyed for encrypted stores); empty for legacy keys.
	ID        string `json:"id,omitempty"`
	ContentID string `json:"contentId,omitempty"`
	// BaseGTIDs bridges a physical clone's empty Previous_gtids to its donor history.
	BaseGTIDs      string    `json:"baseGtids,omitempty"`
	Name           string    `json:"name"`
	RemotePath     string    `json:"remotePath"`
	Size           int64     `json:"size"`
	FirstEventTime time.Time `json:"firstEventTime,omitempty"`
	LastEventTime  time.Time `json:"lastEventTime,omitempty"`
	PreviousGTIDs  string    `json:"previousGtids,omitempty"`
	ArchivedAt     time.Time `json:"archivedAt"`
}

// PrunedEntry records durable retirement and its object-deletion acknowledgement.
// ID keeps the original archive key layout; legacy rows use ContentID for byte identity.
type ArchiveQuarantine struct {
	Divergent   *ArchiveDivergence     `json:"divergent,omitempty"`
	Reuse       []ArchiveReuseEvidence `json:"reuse,omitempty"`
	Entry       ManifestEntry          `json:"entry"`
	Reason      string                 `json:"reason"`
	ResumeGTIDs string                 `json:"resumeGtids"`
	At          time.Time              `json:"at"`
}
type ArchiveReuseEvidence struct {
	First  ManifestEntry `json:"first"`
	Second ManifestEntry `json:"second"`
	GTIDs  string        `json:"gtids"`
	Proof  string        `json:"proof"`
}
type ArchiveDivergence struct {
	ContentID    string `json:"contentId,omitempty"`
	Name         string `json:"name"`
	ID           string `json:"id,omitempty"`
	MissingGTIDs string `json:"missingGtids"`
}
type ArchiveHistoryAcknowledgement struct {
	Reason           string    `json:"reason"`
	ResumeGTIDs      string    `json:"resumeGtids"`
	PriorResetReason string    `json:"priorResetReason,omitempty"`
	At               time.Time `json:"at"`
}
type ArchiveEpoch struct {
	Reuse []ArchiveReuseEvidence `json:"reuse,omitempty"`
	// Legacy diagnostics are readable for recovery; none authorizes a guard.
	ServerUUID  string `json:"serverUuid"`
	LastName    string `json:"lastName"`
	Executed    string `json:"executed"`
	ResetReason string `json:"resetReason,omitempty"`
}

// Manifest is the JSON document at <prefix>/manifest-<site>.json. Each site's
// sidecar owns its manifest; the restore Job combines their replay streams.
type Manifest struct {
	Divergent    []ArchiveDivergence             `json:"divergent,omitempty"`
	Acknowledged []ArchiveHistoryAcknowledgement `json:"acknowledged,omitempty"`
	Quarantined  []ArchiveQuarantine             `json:"quarantined,omitempty"`
	Epoch        *ArchiveEpoch                   `json:"epoch,omitempty"`

	Version int             `json:"version"`
	Site    string          `json:"site"`
	Files   []ManifestEntry `json:"files"`
}

// ManifestKey returns the storage-relative key for a site's manifest.
// Exported so the cmd/bloodraven pitr-download subcommand can use the
// same key convention as the sidecar archiver without duplicating the
// string layout.
func ManifestKey(prefix, site string) string {
	return manifestKey(prefix, site)
}

// ManifestKeyPrefix is the common prefix every site manifest shares
// (e.g. "<prefix>/manifest-"). List callers use it to filter S3
// ListObjectsV2 results down to just manifest objects.
func ManifestKeyPrefix(prefix string) string {
	return path.Join(prefix, "manifest-")
}

// LoadManifest is the exported read-only counterpart of loadManifest.
// Used by the restore init container to fetch site manifests without
// pulling in the whole archiver package.
func LoadManifest(ctx context.Context, store ArchiveStore, prefix, site string) (*Manifest, error) {
	return loadManifest(ctx, store, prefix, site)
}

// SiteFromManifestKey extracts "us-east-1a" from
// "<prefix>/manifest-us-east-1a.json". Returns "" if the key doesn't
// match the expected shape so callers can ignore stray objects in the
// same prefix.
func SiteFromManifestKey(key string) string {
	base := path.Base(key)
	if !strings.HasPrefix(base, "manifest-") || !strings.HasSuffix(base, ".json") {
		return ""
	}
	return base[len("manifest-") : len(base)-len(".json")]
}

// manifestKey returns the storage-relative key for a site's manifest.
// Site names are a-z0-9 plus dashes (enforced by the CRD pattern on
// spec.sites[].name), so no escaping is needed.
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
func loadManifest(ctx context.Context, store ArchiveStore, prefix, site string) (*Manifest, error) {
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
	// Enforce the Version invariant even on legacy writes.
	if m.Version == 0 {
		m.Version = ManifestVersion
	}
	// The site always comes from the caller, never the stored document:
	// saveManifest writes back to manifestKey(prefix, m.Site), so a
	// tampered "site" field could otherwise redirect that write.
	m.Site = site
	for i := range m.Files {
		m.Files[i] = normalizeArchiveEntry(prefix, site, m.Files[i])
	}
	return &m, nil
}

// saveManifest serializes m and writes it to storage under the site's
// manifest key. The Files slice is sorted lexicographically by Name and ID so
// the output is deterministic and diff-friendly in object storage
// browsers.
func saveManifest(ctx context.Context, store ArchiveStore, prefix string, m *Manifest) error {
	sort.Slice(m.Files, func(i, j int) bool {
		if m.Files[i].Name != m.Files[j].Name {
			return m.Files[i].Name < m.Files[j].Name
		}
		return m.Files[i].ID < m.Files[j].ID
	})
	m.Version = ManifestVersion
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, manifestKey(prefix, m.Site), bytes.NewReader(buf), int64(len(buf)))
}

// appendManifestEntry is a convenience wrapper that loads, upserts by
// (Name, ID), and saves. Idempotent: re-appending the same file just
// overwrites its row (archivedAt is bumped to match).
func appendManifestEntry(ctx context.Context, store ArchiveStore, prefix, site string, entry ManifestEntry) error {
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return err
	}
	found := false
	for i := range m.Files {
		if m.Files[i].identity() == entry.identity() {
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

// pruneManifest removes entries whose (Name, ID) identity is in drop. Returns the
// remote object keys of retired entries, persisted in Pruned before the caller issues
// Delete calls. Keys are rebuilt from prefix/site/Name/ID rather than
// taken from the manifest's RemotePath, so a tampered manifest cannot
// steer deletes outside this site's archive; entries whose Name is not
// a plain file name, or whose ID is invalid, are dropped but not deleted. The
// manifest is only rewritten if at least one entry was removed. Drop values
// must contain the verified content ID (keyed for encrypted stores) for each selected identity.
func pruneManifest(ctx context.Context, store ArchiveStore, prefix, site string, drop map[binlogIdentity]string) ([]string, error) {
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		return nil, err
	}
	if len(drop) == 0 {
		return nil, nil
	}
	var removed []string
	pruned := 0
	kept := m.Files[:0]
	for _, f := range m.Files {
		if _, d := drop[f.identity()]; d {
			pruned++
			if key, err := ArchiveEntryKey(prefix, site, f); err == nil {
				removed = append(removed, key)
			}
			continue
		}
		kept = append(kept, f)
	}
	if pruned == 0 {
		return nil, nil
	}
	m.Files = kept
	if err := saveManifest(ctx, store, prefix, m); err != nil {
		return nil, err
	}
	return removed, nil
}

// isPlainFileName reports whether name is a single path element that
// cannot escape the directory it is joined onto.
func isPlainFileName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		!strings.ContainsAny(name, "/\\")
}
