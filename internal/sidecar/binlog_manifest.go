package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// ManifestVersion is the on-disk schema version for site manifests (D-4).
// Version 2 adds the required ArchiveID, the per-row content hints in
// BinlogGTIDCoverage, Promotions and HistoryIntegrity. Readers refuse every
// other version: a version-1 writer rewrites the whole document and strips
// fields it does not know (F1), so v2 data cannot survive an old writer and
// an old manifest cannot be trusted by a new reader.
const ManifestVersion = 2

// Manifest load and save refusals. Each message starts with its PITR
// refusal token, so it can surface unchanged as a sidecar LastError or a Job
// termination message. Match with errors.Is.
var (
	// ErrManifestLegacy: a manifest with version < 2 (or none), written
	// before archive identity existed.
	ErrManifestLegacy = errors.New(string(pitrarchive.TokenArchiveLegacy))
	// ErrManifestVersionUnsupported: a manifest from a newer release.
	ErrManifestVersionUnsupported = errors.New(string(pitrarchive.TokenArchiveVersionUnsupported))
	// ErrManifestInvalid: a v2 manifest that breaks a v2 invariant (no
	// archiveId, too many promotion records).
	ErrManifestInvalid = errors.New(string(pitrarchive.TokenArchiveObjectInvalid))
)

// errManifestDecode: the stored manifest bytes are not JSON a Manifest
// decodes from (content, not transport). Untokenized, as before.
var errManifestDecode = errors.New("decode manifest")

// PromotionRecord and HistoryIntegrity are defined in pitrarchive so the
// planner and the sidecar status share one definition (D-43, D-34).
type (
	PromotionRecord  = pitrarchive.PromotionRecord
	HistoryIntegrity = pitrarchive.HistoryIntegrity
)

// MaxPromotionRecords is how many promotion records a manifest keeps.
const MaxPromotionRecords = pitrarchive.MaxPromotionRecords

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
type PrunedEntry struct {
	Name           string    `json:"name"`
	ID             string    `json:"id,omitempty"`
	ContentID      string    `json:"contentId,omitempty"`
	PreviousGTIDs  string    `json:"previousGtids,omitempty"`
	FirstEventTime time.Time `json:"firstEventTime,omitempty"`
	Key            string    `json:"key"`
	Deleted        bool      `json:"deleted"`
}

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
// Saves are a plain Put by the site's single writer (F34), followed by a
// read-back compare; there is no CAS.
type Manifest struct {
	Divergent    []ArchiveDivergence             `json:"divergent,omitempty"`
	Acknowledged []ArchiveHistoryAcknowledgement `json:"acknowledged,omitempty"`
	Quarantined  []ArchiveQuarantine             `json:"quarantined,omitempty"`
	Epoch        *ArchiveEpoch                   `json:"epoch,omitempty"`

	Version int `json:"version"`
	// ArchiveID is the archive's descriptor ID (required in v2). A
	// manifest whose ArchiveID differs from the descriptor's belongs to
	// another archive.
	ArchiveID string          `json:"archiveId"`
	Site      string          `json:"site"`
	Files     []ManifestEntry `json:"files"`
	Pruned    []PrunedEntry   `json:"pruned,omitempty"`
	// Promotions are this site's promotion records, append-only, newest
	// last, at most MaxPromotionRecords (D-43).
	Promotions []PromotionRecord `json:"promotions,omitempty"`
	// HistoryIntegrity is set once on the first history-integrity
	// violation and never cleared by any writer (D-34).
	HistoryIntegrity *HistoryIntegrity `json:"historyIntegrity,omitempty"`

	// appendedPromotion is the record AppendPromotionRecord added since
	// the last save: the one record ArchiveSession.SaveManifest may keep
	// although an earlier cap eviction named it. Not serialized.
	appendedPromotion *PromotionRecord
}

// AppendPromotionRecord appends r and drops the oldest records beyond
// MaxPromotionRecords. Dropping a record only removes anchors and loss
// evidence, so the cap fails closed (D-43).
func (m *Manifest) AppendPromotionRecord(r PromotionRecord) {
	m.Promotions = append(m.Promotions, r)
	if n := len(m.Promotions) - MaxPromotionRecords; n > 0 {
		m.Promotions = append([]PromotionRecord(nil), m.Promotions[n:]...)
	}
	rec := r
	m.appendedPromotion = &rec
}

// appendedPromotionKey is the key of the record appended since the last
// save, or "".
func (m *Manifest) appendedPromotionKey() string {
	if m.appendedPromotion == nil {
		return ""
	}
	return promotionRecordKey(*m.appendedPromotion)
}

// checkManifestVersion refuses every manifest version except
// ManifestVersion, and a v2 manifest without an archiveId.
func checkManifestVersion(key string, m *Manifest) error {
	switch {
	case m.Version < ManifestVersion:
		return fmt.Errorf("%w: manifest %s has version %d, written before archive identity; start a new archive prefix and take a full backup",
			ErrManifestLegacy, key, m.Version)
	case m.Version > ManifestVersion:
		return fmt.Errorf("%w: manifest %s has version %d; this release reads only version %d; upgrade bloodraven",
			ErrManifestVersionUnsupported, key, m.Version, ManifestVersion)
	case m.ArchiveID == "":
		return fmt.Errorf("%w: manifest %s is version %d but has no archiveId", ErrManifestInvalid, key, m.Version)
	case len(m.Promotions) > MaxPromotionRecords:
		return fmt.Errorf("%w: manifest %s holds %d promotion records, max %d", ErrManifestInvalid, key, len(m.Promotions), MaxPromotionRecords)
	}
	return nil
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
// returns an empty version-2 Manifest if none exists yet. The empty
// manifest has no ArchiveID; the writer sets it from its verified
// descriptor before saving. A decode error on an existing object is
// propagated — the caller should surface it loudly rather than silently
// reset the manifest. Any version other than ManifestVersion is refused
// (ErrManifestLegacy, ErrManifestVersionUnsupported), as is a v2 manifest
// without an archiveId (ErrManifestInvalid).
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
		return nil, fmt.Errorf("%w %s: %w", errManifestDecode, manifestKey(prefix, site), err)
	}
	if err := checkManifestVersion(manifestKey(prefix, site), &m); err != nil {
		return nil, err
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
// manifest key, with no identity checks. Writers save through
// ArchiveSession.SaveManifest, which adds Reverify, the archive ID and the
// read-back compare; this raw form is for tests and fixtures.
func saveManifest(ctx context.Context, store ArchiveStore, prefix string, m *Manifest) error {
	buf, err := encodeManifest(prefix, m)
	if err != nil {
		return err
	}
	return store.Put(ctx, manifestKey(prefix, m.Site), bytes.NewReader(buf), int64(len(buf)))
}

// encodeManifest stamps the current version and serializes m. The Files
// slice is sorted lexicographically by Name and ID so the output is
// deterministic and diff-friendly in object storage browsers. It refuses a
// manifest its own loader would refuse.
func encodeManifest(prefix string, m *Manifest) ([]byte, error) {
	sort.Slice(m.Files, func(i, j int) bool {
		if m.Files[i].Name != m.Files[j].Name {
			return m.Files[i].Name < m.Files[j].Name
		}
		return m.Files[i].ID < m.Files[j].ID
	})
	sort.Slice(m.Pruned, func(i, j int) bool {
		a, b := m.Pruned[i], m.Pruned[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.ContentID < b.ContentID
	})
	m.Version = ManifestVersion
	if err := checkManifestVersion(manifestKey(prefix, m.Site), m); err != nil {
		return nil, fmt.Errorf("refusing to save: %w", err)
	}
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return buf, nil
}

// pruneManifest removes entries whose (Name, ID) identity is in drop. Returns the
// remote object keys of retired entries, persisted in Pruned before the caller issues
// Delete calls. Keys are rebuilt from prefix/site/Name/ID rather than
// taken from the manifest's RemotePath, so a tampered manifest cannot
// steer deletes outside this site's archive; entries whose Name is not
// a plain file name, or whose ID is invalid, are dropped but not deleted. The
// manifest is only rewritten if at least one entry was removed. Drop values
// must contain the verified content ID (keyed for encrypted stores) for each selected identity.
func pruneManifest(ctx context.Context, session *ArchiveSession, site string, drop map[binlogIdentity]string) ([]string, error) {
	prefix := session.Prefix()
	m, err := session.LoadManifest(ctx, site)
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
		if contentID, d := drop[f.identity()]; d {
			pruned++
			if key, err := ArchiveEntryKey(prefix, site, f); err == nil {
				if f.ID != "" && contentID != f.ID {
					return nil, fmt.Errorf("retirement content identity differs from archive ID for %s", f.Name)
				}
				if _, err := ArchiveEntryKey(prefix, site, ManifestEntry{Name: f.Name, ID: contentID}); err != nil || contentID == "" {
					return nil, fmt.Errorf("missing valid retirement content identity for %s", f.Name)
				}
				retired := PrunedEntry{Name: f.Name, ID: f.ID, Key: key, PreviousGTIDs: f.PreviousGTIDs, FirstEventTime: f.FirstEventTime}
				if f.ID == "" {
					retired.ContentID = contentID
				}
				found := false
				for i := range m.Pruned {
					p := m.Pruned[i]
					if p.Name == retired.Name && p.ID == retired.ID && p.ContentID == retired.ContentID {
						m.Pruned[i] = retired
						found = true
						break
					}
				}
				if !found {
					m.Pruned = append(m.Pruned, retired)
				}
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
	if err := session.SaveManifest(ctx, m); err != nil {
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
