package sidecar

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// File framing can change when transactions are re-logged. Original commit
// stamps and actual query content distinguish issuance from file publication.
// Unknown origins never manufacture a RESET guard.
type binlogOrigins struct {
	stamps, queries        [32]byte
	stampCount, queryCount int64
}

func addOrigin(sum *[32]byte, h hash.Hash) {
	digest := h.Sum(nil)
	for i := range sum {
		sum[i] ^= digest[i]
	}
}
func originGTID(event *replication.BinlogEvent) *replication.GTIDEvent {
	if e, ok := event.Event.(*replication.GTIDEvent); ok {
		return e
	}
	if e, ok := event.Event.(*replication.GtidTaggedLogEvent); ok {
		return &e.GTIDEvent
	}
	return nil
}

// Accumulate order-independent per-GTID identities in constant memory. A
// complete overlap is compared only when both streams supply that evidence.
func (a *BinlogArchiver) archivedOrigins(ctx context.Context, entry ManifestEntry, wanted internalmysql.GTIDSet) (binlogOrigins, string, error) {
	result := binlogOrigins{}
	key, err := ArchiveEntryKey(a.cfg.ManifestPrefix, a.site, entry)
	if err != nil {
		return result, "", err
	}
	r, err := a.store.Open(ctx, key)
	if err != nil {
		return result, "", err
	}
	defer r.Close()
	current := ""
	var queries hash.Hash
	finish := func() {
		if queries != nil {
			addOrigin(&result.queries, queries)
			result.queryCount++
		}
		queries = nil
	}
	evidence, err := inspectBinlog(r, time.Time{}, func(event *replication.BinlogEvent) {
		if g := originGTID(event); g != nil {
			finish()
			current = ""
			next, nextErr := g.GTIDNext()
			if nextErr != nil {
				return
			}
			set, parseErr := internalmysql.ParseGTIDSet(next.String())
			if parseErr != nil || !wanted.Contains(set) {
				return
			}
			current = next.String()
			if g.OriginalCommitTimestamp != 0 {
				h := sha256.New()
				_, _ = fmt.Fprintf(h, "%s:%d", current, g.OriginalCommitTimestamp)
				addOrigin(&result.stamps, h)
				result.stampCount++
			}
		}
		if q, ok := event.Event.(*replication.QueryEvent); ok && current != "" {
			query := strings.TrimSpace(string(q.Query))
			if strings.EqualFold(query, "BEGIN") || strings.EqualFold(query, "COMMIT") {
				return
			}
			if queries == nil {
				queries = sha256.New()
				_, _ = fmt.Fprintf(queries, "%s:", current)
			}
			_, _ = fmt.Fprintf(queries, "%d:%s%d:%s", len(q.Schema), q.Schema, len(q.Query), q.Query)
		}
	})
	finish()
	if err != nil {
		return result, "", err
	}
	id := entry.ID
	if id == "" {
		id = entry.ContentID
	}
	if id != "" && !MatchesBinlogDigest(a.store, id, evidence.Digest) {
		return result, "", fmt.Errorf("identity mismatch checking GTID issuance for %s", entry.Name)
	}
	return result, a.store.BinlogContentID(evidence.Digest), nil
}
func differentOrigins(a, b binlogOrigins, count int64) string {
	if a.stampCount == count && b.stampCount == count && a.stamps != b.stamps {
		return "original commit identities differ"
	}
	if a.queryCount == count && b.queryCount == count && a.queries != b.queries {
		return "transaction query identities differ"
	}
	return ""
}
func (a *BinlogArchiver) detectArchiveReuse(ctx context.Context, m *Manifest, entry ManifestEntry) error {
	candidate, err := internalmysql.ParseGTIDSet(observedArchiveGTIDs(entry))
	if err != nil {
		return err
	}
	// Set intersection already requires the same source UUID and tag.
	if candidate.IsEmpty() {
		return nil
	}
	for _, old := range m.Files {
		if !coverageBoundToEntry(old) {
			continue
		}
		if old.identity() == entry.identity() {
			continue
		}
		if sameArchiveBytes(a.store, old, entry) {
			continue
		}
		oldTX, err := internalmysql.ParseGTIDSet(observedArchiveGTIDs(old))
		if err != nil {
			return err
		}
		overlap := candidate.Subtract(candidate.Subtract(oldTX))
		if overlap.IsEmpty() {
			continue
		}
		proof, err := CheckArchiveIssuance(ctx, a.store, a.cfg.ManifestPrefix, a.site, old, entry, overlap)
		if err != nil {
			return err
		}
		if proof != nil {
			recordArchiveReuse(m, *proof)
			return nil
		}
	}
	return nil
}

// CheckArchiveIssuance verifies both streams before comparing overlapping GTIDs.
// It also protects restore lower cuts when older manifests lack a reuse guard.
func CheckArchiveIssuance(ctx context.Context, store ArchiveStore, prefix, site string, first, second ManifestEntry, overlap internalmysql.GTIDSet) (*ArchiveReuseEvidence, error) {
	a := &BinlogArchiver{cfg: &PITRConfig{ManifestPrefix: prefix}, site: site, store: store}
	before, beforeID, err := a.archivedOrigins(ctx, first, overlap)
	if err != nil {
		return nil, err
	}
	after, afterID, err := a.archivedOrigins(ctx, second, overlap)
	if err != nil {
		return nil, err
	}
	if beforeID == afterID {
		return nil, nil
	}
	if proof := differentOrigins(before, after, overlap.TransactionCount()); proof != "" {
		if first.ID == "" {
			first.ContentID = beforeID
		}
		if second.ID == "" {
			second.ContentID = afterID
		}
		return &ArchiveReuseEvidence{First: first, Second: second, GTIDs: overlap.String(), Proof: proof}, nil
	}
	count := overlap.TransactionCount()
	if (before.stampCount == count && after.stampCount == count) || (before.queryCount == count && after.queryCount == count) {
		return nil, nil
	}
	return nil, fmt.Errorf("overlapping GTID issuance remains unverified for %s", overlap.String())
}
func recordArchiveReuse(m *Manifest, proof ArchiveReuseEvidence) {
	if m.Epoch == nil {
		m.Epoch = &ArchiveEpoch{}
	}
	for _, old := range m.Epoch.Reuse {
		if old.First.identity() == proof.First.identity() && old.Second.identity() == proof.Second.identity() && old.GTIDs == proof.GTIDs {
			return
		}
	}
	m.Epoch.Reuse = append(m.Epoch.Reuse, proof)
	m.Epoch.ResetReason = fmt.Sprintf("verified GTID namespace reuse within %s (%s); use a new manifest prefix and full backup", proof.GTIDs, proof.Proof)
}
