package main

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

type pitrQuarantineScope struct {
	record sidecar.ArchiveQuarantine
	bound  internalmysql.GTIDSet
	valid  bool
}

func mergePITRSafety(dst, src *pitrWindowNode) {
	if src.divergent != "" {
		if dst.divergent == "" {
			dst.divergent = src.divergent
		} else if dst.divergent != src.divergent {
			dst.divergent += "," + src.divergent
		}
	}
	dst.quarantines = append(dst.quarantines, src.quarantines...)
}

type pitrWindowNode struct {
	entry                     sidecar.ManifestEntry
	evidence                  *sidecar.BinlogEvidence
	path                      string
	index                     int
	base, previous, execution internalmysql.GTIDSet
	count                     int64
	active                    bool
	divergent                 string
	quarantines               []pitrQuarantineScope
}

func pitrNodeID(e sidecar.ManifestEntry) string {
	if e.ID != "" {
		return e.ID
	}
	return e.ContentID
}
func pitrExecution(p, tx internalmysql.GTIDSet) internalmysql.GTIDSet {
	return sidecar.UnionBinlogGTIDs(p, tx)
}
func pitrFailure(site string, n *pitrWindowNode, detail string) error {
	return fmt.Errorf("site %s name %s id %s P=%q: %s; preserve objects; use bloodraven pitr-quarantine with a canonical full-backup bound, or a new prefix after RESET", site, n.entry.Name, n.entry.ID, n.entry.PreviousGTIDs, detail)
}
func pitrNodeRank(n *pitrWindowNode) int {
	if n.evidence != nil {
		if n.evidence.GTIDs.IsEmpty() {
			return 0
		}
		return 2
	}
	return 1
}

// pitrNodeQueue revisits a node only when its evidence changes. Parsed GTID
// sets and verified observations stay on the node across provisional reorderings.
type pitrNodeQueue []*pitrWindowNode

func (q pitrNodeQueue) Len() int { return len(q) }
func (q pitrNodeQueue) Less(i, j int) bool {
	a, b := q[i], q[j]
	if a.count != b.count {
		return a.count < b.count
	}
	if a.entry.PreviousGTIDs != b.entry.PreviousGTIDs {
		return a.entry.PreviousGTIDs < b.entry.PreviousGTIDs
	}
	if pitrNodeRank(a) != pitrNodeRank(b) {
		return pitrNodeRank(a) < pitrNodeRank(b)
	}
	if a.entry.Name != b.entry.Name {
		return a.entry.Name < b.entry.Name
	}
	return a.entry.ID < b.entry.ID
}
func (q pitrNodeQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *pitrNodeQueue) Push(v any)   { *q = append(*q, v.(*pitrWindowNode)) }
func (q *pitrNodeQueue) Pop() any     { old := *q; n := old[len(old)-1]; *q = old[:len(old)-1]; return n }

func planPITRWindow(ctx context.Context, store sidecar.ArchiveStore, prefix, site string, stop time.Time, outDir, start string, logger *slog.Logger) (pitrSiteCoverage, error) {
	result := pitrSiteCoverage{Site: site, Staged: []pitrStagedFile{}}
	if !isSafeBasename(site) {
		return result, fmt.Errorf("reject unsafe site name %q", site)
	}

	baseline, err := internalmysql.ParseGTIDSet(start)
	if err != nil {
		return result, fmt.Errorf("invalid START_GTIDS: %w", err)
	}
	m, err := sidecar.LoadManifest(ctx, store, prefix, site)
	if err != nil {
		return result, err
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		return result, fmt.Errorf("site %s: %s", site, m.Epoch.ResetReason)
	}
	// Alias lookups are confined to one basename and constant-size identity keys.
	aliases := make(map[string]map[string]*pitrWindowNode)
	// Acknowledgement removes no rows. It is an audit record, not a global
	// backup-generation cutoff; actual row/window checks still apply.
	var nodes []*pitrWindowNode
	type rowIdentity struct{ name, id string }
	rowNodes := make(map[rowIdentity]*pitrWindowNode)
	aliasNode := func(entry sidecar.ManifestEntry) *pitrWindowNode {
		id := pitrNodeID(entry)
		if id == "" {
			return nil
		}
		byID := aliases[entry.Name]
		if n := byID[id]; n != nil && n.active {
			return n
		}
		if n := byID[store.BinlogContentID(id)]; n != nil && n.active {
			return n
		}
		return nil
	}
	register := func(n *pitrWindowNode) {
		id := pitrNodeID(n.entry)
		if id == "" {
			return
		}
		byID := aliases[n.entry.Name]
		if byID == nil {
			byID = make(map[string]*pitrWindowNode)
			aliases[n.entry.Name] = byID
		}
		byID[id], byID[store.BinlogContentID(id)] = n, n
	}
	entries := append([]sidecar.ManifestEntry(nil), m.Files...)
	for _, q := range m.Quarantined {
		entries = append(entries, q.Entry)
	}
	for i, entry := range entries {
		var scopes []pitrQuarantineScope
		if i >= len(m.Files) {
			q := m.Quarantined[i-len(m.Files)]
			bound, err := internalmysql.ParseGTIDSet(q.ResumeGTIDs)
			scopes = []pitrQuarantineScope{{record: q, bound: bound, valid: err == nil && !bound.IsEmpty()}}
		}
		if !isSafeBasename(entry.Name) {
			return result, fmt.Errorf("unsafe binlog name %q", entry.Name)
		}
		if _, err := sidecar.ArchiveEntryKey(prefix, site, entry); err != nil {
			return result, err
		}
		if existing := aliasNode(entry); existing != nil {
			rowNodes[rowIdentity{entry.Name, entry.ID}] = existing
			existing.quarantines = append(existing.quarantines, scopes...)
			if len(scopes) == 0 && existing.entry.ID == "" && entry.ID != "" {
				existing.entry = entry
			}
			continue
		}
		n := &pitrWindowNode{entry: entry, index: i, active: true, quarantines: scopes}
		nodes = append(nodes, n)
		rowNodes[rowIdentity{entry.Name, entry.ID}] = n
		register(n)
	}
	for _, n := range nodes {
		base, err := internalmysql.ParseGTIDSet(n.entry.BaseGTIDs)
		if err != nil {
			return result, err
		}
		previous, err := internalmysql.ParseGTIDSet(n.entry.PreviousGTIDs)
		if err != nil {
			return result, err
		}
		n.base = base
		n.previous = pitrExecution(previous, base)
		n.count = n.previous.TransactionCount()
		n.entry.PreviousGTIDs = n.previous.String()
	}
	for _, d := range m.Divergent {
		if n := rowNodes[rowIdentity{d.Name, d.ID}]; n != nil {
			mergePITRSafety(n, &pitrWindowNode{divergent: d.MissingGTIDs})
			continue
		}
		if n := aliasNode(sidecar.ManifestEntry{Name: d.Name, ID: d.ID, ContentID: d.ContentID}); n != nil {
			n.divergent = d.MissingGTIDs
			continue
		}
		if d.ID == "" && d.ContentID == "" {
			for _, n := range nodes {
				if n.entry.Name == d.Name && n.entry.ID == "" {
					n.divergent = d.MissingGTIDs
				}
			}
		}
	}

	dir, err := safeJoinUnderRoot(outDir, site)
	if err != nil {
		return result, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return result, err
	}
	old, err := filepath.Glob(filepath.Join(dir, "mysql-bin.*"))
	if err != nil {
		return result, err
	}
	for _, file := range old {
		if err := os.Remove(file); err != nil {
			return result, err
		}
	}
	defer func() {
		for _, n := range nodes {
			if n.path != "" {
				_ = os.Remove(n.path)
			}
		}
	}()
	fetch := func(n *pitrWindowNode, stage bool) error {
		key, err := sidecar.ArchiveEntryKey(prefix, site, n.entry)
		if err != nil {
			return err
		}
		reader, err := store.Open(ctx, key)
		if err != nil {
			return fmt.Errorf("%w: %v", err, pitrFailure(site, n, "object unreadable or missing (digest may be unknown): "+err.Error()))
		}
		defer reader.Close()
		var out *os.File
		var source io.Reader = reader
		part := filepath.Join(dir, fmt.Sprintf(".entry-%d.part", n.index))
		if stage {
			out, err = os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			source = io.TeeReader(reader, out)
		}
		evidence, readErr := sidecar.InspectBinlogForReplay(source, stop, baseline)
		if out != nil {
			closeErr := out.Close()
			if readErr == nil {
				readErr = closeErr
			}
		}
		if readErr != nil {
			_ = os.Remove(part)
			if evidence.ReplayRefusal != "" {
				identity := pitrNodeID(n.entry)
				if identity != "" && !sidecar.MatchesBinlogDigest(store, identity, evidence.Digest) {
					return pitrFailure(site, n, "content identity mismatch")
				}
				result.AnchorReason = evidence.ReplayRefusal
				return fmt.Errorf("PITRTargetNotCovered: site %s: %s", site, result.AnchorReason)
			}
			return pitrFailure(site, n, "invalid replay observations: "+readErr.Error())
		}
		identity := pitrNodeID(n.entry)
		if identity != "" && !sidecar.MatchesBinlogDigest(store, identity, evidence.Digest) {
			_ = os.Remove(part)
			return pitrFailure(site, n, "content identity mismatch")
		}
		if n.evidence != nil && n.evidence.Digest != evidence.Digest {
			_ = os.Remove(part)
			return pitrFailure(site, n, "object changed during planning")
		}
		n.evidence = &evidence
		if evidence.NewestEventTime.After(result.NewestEventTime) {
			result.NewestEventTime = evidence.NewestEventTime
		}
		previous, previousErr := internalmysql.ParseGTIDSet(evidence.Metadata.PreviousGTIDs)
		if previousErr != nil {
			return previousErr
		}
		n.previous = pitrExecution(previous, n.base)
		n.execution = pitrExecution(n.previous, evidence.GTIDs)
		n.count = n.previous.TransactionCount()
		n.entry.PreviousGTIDs = n.previous.String()
		n.entry.FirstEventTime = evidence.Metadata.FirstEventTime
		n.entry.LastEventTime = evidence.Metadata.LastEventTime
		n.entry.Size = evidence.Metadata.Size
		if n.entry.ID == "" {
			n.entry.ContentID = store.BinlogContentID(evidence.Digest)
		}
		if stage {
			n.path = part
		}
		return nil
	}
	// Staging a previously inspected node checks its bytes again without
	// recomputing GTIDs or ordering evidence.
	stageVerified := func(n *pitrWindowNode) error {
		key, err := sidecar.ArchiveEntryKey(prefix, site, n.entry)
		if err != nil {
			return err
		}
		r, err := store.Open(ctx, key)
		if err != nil {
			return pitrFailure(site, n, err.Error())
		}
		defer r.Close()
		part := filepath.Join(dir, fmt.Sprintf(".entry-%d.part", n.index))
		out, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, readErr := io.Copy(io.MultiWriter(out, h), r)
		closeErr := out.Close()
		if readErr == nil {
			readErr = closeErr
		}
		if readErr != nil || hex.EncodeToString(h.Sum(nil)) != n.evidence.Digest {
			_ = os.Remove(part)
			return pitrFailure(site, n, fmt.Sprintf("object changed or unreadable during staging: %v", readErr))
		}
		n.path = part
		return nil
	}
	var issued sidecar.BinlogTransactionIndex
	verifiedNodes := make(map[int]*pitrWindowNode)
	queue := pitrNodeQueue(append([]*pitrWindowNode(nil), nodes...))
	heap.Init(&queue)
	quarantineAllowed := func(n *pitrWindowNode) bool {
		if len(n.quarantines) == 0 {
			return false
		}
		for _, scope := range n.quarantines {
			if !scope.valid || !baseline.Contains(scope.bound) {
				return false
			}
		}
		return true
	}
	warnQuarantine := func(n *pitrWindowNode) {
		for _, scope := range n.quarantines {
			logger.Warn("quarantined binlog excluded", "site", site, "name", scope.record.Entry.Name, "id", scope.record.Entry.ID, "reason", scope.record.Reason)
		}
	}
	var selected []*pitrWindowNode
	var barrier *pitrWindowNode
	var coverageBarrier *pitrWindowNode
	anchors := func(n *pitrWindowNode) bool {
		return n != nil && !baseline.IsEmpty() && (n.previous.Contains(baseline) || (baseline.Contains(n.previous) && n.execution.Contains(baseline)))
	}
	barrierTransactions := 0
	// Only changed predecessor evidence can invalidate the already validated
	// prefix. Correctly hinted histories make one forward pass through the heap.
	resetPrefix := func() {
		for _, n := range selected {
			if n.active {
				heap.Push(&queue, n)
			}
		}
		selected = nil
		barrier = nil
		coverageBarrier = nil
		barrierTransactions = 0
	}
	for queue.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		n := heap.Pop(&queue).(*pitrWindowNode)
		if !n.active {
			continue
		}
		if quarantineAllowed(n) {
			warnQuarantine(n)
			continue
		}
		if barrier != nil && !n.previous.Contains(barrier.previous) {
			return result, pitrFailure(site, n, "branch incomparable with stop barrier")
		}
		if barrier != nil && (baseline.IsEmpty() || anchors(barrier) || coverageBarrier != nil) && n.entry.PreviousGTIDs != barrier.entry.PreviousGTIDs {
			continue
		}

		if n.evidence == nil {
			prior := n.entry.PreviousGTIDs
			if err := fetch(n, barrier == nil); err != nil {
				return result, err
			}
			if other := aliasNode(n.entry); other != nil && other != n {
				changed := n.divergent != "" || len(n.quarantines) > 0
				mergePITRSafety(other, n)
				if n.path != "" {
					_ = os.Remove(n.path)
					n.path = ""
				}
				n.active = false
				if changed {
					resetPrefix()
				}
				continue
			}
			n.entry.GTIDCoverage = n.evidence.Metadata.GTIDCoverage
			binding := n.entry.ID
			if binding == "" {
				binding = n.entry.ContentID
			}
			n.entry.GTIDCoverage.ContentID = binding
			for _, id := range issued.Overlaps(n.evidence.GTIDs) {
				other := verifiedNodes[id]
				if !other.active || other.evidence.Digest == n.evidence.Digest {
					continue
				}
				overlap := n.evidence.GTIDs.Subtract(n.evidence.GTIDs.Subtract(other.evidence.GTIDs))
				proof, err := sidecar.CheckArchiveIssuance(ctx, store, prefix, site, other.entry, n.entry, overlap)
				if err != nil {
					return result, pitrFailure(site, n, "unable to verify overlapping issuance: "+err.Error())
				}
				if proof != nil {
					return result, pitrFailure(site, n, "verified GTID namespace reuse: "+proof.GTIDs+"; "+proof.Proof)
				}
			}
			issued.Add(n.index, n.evidence.GTIDs)
			verifiedNodes[n.index] = n
			register(n)
			if prior != n.entry.PreviousGTIDs {
				resetPrefix()
			}
			heap.Push(&queue, n)
			continue
		}
		if barrier != nil && n.entry.PreviousGTIDs != barrier.entry.PreviousGTIDs {
			if !n.previous.Contains(barrier.execution) {
				return result, pitrFailure(site, n, "successor does not continue replay barrier history")
			}
			if !n.evidence.GTIDs.IsEmpty() || len(n.quarantines) > 0 || n.divergent != "" {
				return result, pitrFailure(site, n, "cannot use an ambiguous transaction-bearing successor to anchor replay")
			}
			if anchors(n) && n.evidence.StopReached {
				coverageBarrier = n
			}
			continue
		}

		if !baseline.IsEmpty() && baseline.Contains(n.execution) {
			if n.evidence.StopReached {
				result.StopReached = true
				if anchors(n) {
					if len(selected) > 0 {
						previous := selected[len(selected)-1]
						if missing := previous.execution.Subtract(n.previous); !missing.IsEmpty() {
							return result, pitrFailure(site, previous, "divergent boundary before idle barrier: "+missing.String())
						}
					}
					coverageBarrier = n
				}
			}
			if n.path != "" {
				if err := os.Remove(n.path); err != nil {
					return result, err
				}
				n.path = ""
			}
			continue
		}
		if len(n.quarantines) > 0 {
			if !n.evidence.BeforeStopGTIDs.IsEmpty() {
				return result, pitrFailure(site, n, "quarantined row is needed by this replay window; canonical backup must cover "+n.quarantines[0].record.ResumeGTIDs)
			}
		}

		if barrier != nil {
			if !n.evidence.GTIDs.IsEmpty() {
				barrierTransactions++
				if barrierTransactions > 1 {
					return result, pitrFailure(site, n, "multiple transaction-bearing files at stop frontier")
				}
				if !n.evidence.BeforeStopGTIDs.IsEmpty() {
					return result, pitrFailure(site, n, "ambiguous empty stop frontier would omit transactions: "+n.evidence.BeforeStopGTIDs.String())
				}
			}
			continue
		}
		if n.divergent != "" && !n.evidence.BeforeStopGTIDs.IsEmpty() {
			return result, pitrFailure(site, n, "audited divergent transactions="+n.divergent)
		}

		overlap := n.evidence.GTIDs.Subtract(n.evidence.GTIDs.Subtract(n.previous))
		if !overlap.IsEmpty() {
			return result, pitrFailure(site, n, "GTID namespace reuse: T intersects P: "+overlap.String())
		}
		if len(selected) > 0 {
			previous := selected[len(selected)-1]
			if missing := previous.execution.Subtract(n.previous); !missing.IsEmpty() {
				return result, pitrFailure(site, previous, "divergent boundary; T/P missing from next="+missing.String())
			}
		}
		if n.path == "" {
			if err := stageVerified(n); err != nil {
				return result, err
			}
		}
		selected = append(selected, n)
		if n.evidence.StopReached {
			barrier = n
			if !n.evidence.GTIDs.IsEmpty() {
				barrierTransactions = 1
			}
		}
	}
	for _, n := range selected {
		if n.divergent != "" && !n.evidence.BeforeStopGTIDs.IsEmpty() {
			return result, pitrFailure(site, n, "audited divergent transactions="+n.divergent)
		}
	}
	if barrier != nil || coverageBarrier != nil {
		result.StopReached = true
		proof := barrier
		if proof == nil || !anchors(proof) {
			proof = coverageBarrier
		}
		result.Anchored = anchors(proof)
		checkpointBarrier := proof
		if checkpointBarrier == nil {
			checkpointBarrier = barrier
		}
		if checkpointBarrier == nil {
			checkpointBarrier = coverageBarrier
		}
		result.checkpoint = sidecar.UnionBinlogGTIDs(checkpointBarrier.previous)
		for _, n := range selected {
			if n == checkpointBarrier {
				break
			}
			if checkpointBarrier.previous.Contains(n.execution) {
				result.checkpoint = sidecar.UnionBinlogGTIDs(result.checkpoint, n.evidence.GTIDs)
			}
		}
		result.CheckpointGTIDs = result.checkpoint.String()
		if !result.Anchored {
			result.AnchorReason = "chain does not straddle or follow the trusted backup bound"
		}
	}
	for ordinal, n := range selected {
		// Keep predecessor gaps visible even without an upper barrier (allowPartial).
		result.checkpoint = sidecar.UnionBinlogGTIDs(result.checkpoint, n.previous)
		result.CheckpointGTIDs = result.checkpoint.String()
		result.replayPrefix = sidecar.UnionBinlogGTIDs(result.replayPrefix, n.evidence.CompletedBeforeStopGTIDs)
		if result.replayRefusal == "" {
			result.replayRefusal = n.evidence.ReplayRefusal
		}
		result.unverifiableGTIDs = sidecar.UnionBinlogGTIDs(result.unverifiableGTIDs, n.evidence.BeforeStopGTIDs.Subtract(n.evidence.CompletedBeforeStopGTIDs))
		// Identity-verified parsed GTIDs supply lower history for whole files.
		// Retention completeness remains a separate, stricter deletion requirement.
		if n.evidence.StopReached {
			result.lowerGTIDs = sidecar.UnionBinlogGTIDs(result.lowerGTIDs, n.evidence.CompletedBeforeStopGTIDs)
		} else {
			result.lowerGTIDs = sidecar.UnionBinlogGTIDs(result.lowerGTIDs, n.evidence.GTIDs)
		}
		name := fmt.Sprintf("mysql-bin.%020d.%s", ordinal, n.entry.Name)
		if n.entry.ID != "" {
			name += "." + n.entry.ID
		}
		if err := os.Rename(n.path, filepath.Join(dir, name)); err != nil {
			return result, err
		}
		n.path = ""
		result.Staged = append(result.Staged, pitrStagedFile{Path: filepath.ToSlash(filepath.Join(site, name)), Digest: n.evidence.Digest,
			FirstEventTime: n.evidence.FirstVerifiedEventTime, LastEventTime: n.evidence.NewestEventTime,
			FirstTransactionEventTime: n.evidence.FirstTransactionEventTime, LastTransactionEventTime: n.evidence.LastTransactionEventTime,
			TimestampsComplete: n.evidence.TimestampsComplete})
		result.StagedStopReached = result.StagedStopReached || n.evidence.StopReached
		result.window = sidecar.UnionBinlogGTIDs(result.window, n.evidence.BeforeStopGTIDs)
		result.InWindowGTIDs = result.window.String()
		if n.evidence.NewestEventTime.After(result.NewestEventTime) {
			result.NewestEventTime = n.evidence.NewestEventTime
		}
		if n.evidence.NewestReplayEventTime.After(result.NewestReplayEventTime) {
			result.NewestReplayEventTime = n.evidence.NewestReplayEventTime
		}
		if !n.evidence.BeforeStopGTIDs.Subtract(baseline).IsEmpty() {
			result.HasReplayTransactions = true
		}
		logger.Info("archived binlog downloaded", "site", site, "name", n.entry.Name, "size", n.entry.Size, "first_event", n.entry.FirstEventTime.Format(time.RFC3339))
	}
	return result, nil
}
