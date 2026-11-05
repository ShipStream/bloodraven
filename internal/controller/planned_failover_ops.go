package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// The planned-failover topology facade: the engine's only path to the
// topology managers and MySQL. Every mutating call checks the run's claim
// immediately before its first statement (requireRunClaim) and sends
// nothing when that check fails. Reads need no claim.
//
// The facade is process-scoped and owns the run's in-process memos: which
// fence it sent (fence attribution) and how far its promotion statements got
// (stage classification). They are never persisted; a cross-process re-run
// has none and falls back to the durable records plus @@server_uuid.

const (
	// plannedDNSBudget bounds the run's DNS apply (3 attempts).
	plannedDNSBudget   = 10 * time.Second
	plannedDNSAttempts = 3
)

// promoteMemoStage is how far this process's promotion statements for a
// run got. Each stage is recorded before its statement is sent, so "sent"
// includes a statement whose reply was lost.
type promoteMemoStage int

const (
	promoteMemoNone promoteMemoStage = iota
	promoteMemoStopSent
	promoteMemoResetSent
	promoteMemoSROClearSent
	promoteMemoROClearSent
)

type fenceMemoEntry struct {
	uuid string
	sent bool
}

// runnerPlannedTopology implements PlannedFailoverTopology over a
// ManagerRegistry.
type runnerPlannedTopology struct {
	reg   ManagerRegistry
	hooks SimHooks

	mu          sync.Mutex
	fenceMemo   map[string]fenceMemoEntry
	promoteMemo map[string]promoteMemoStage
}

// NewPlannedFailoverTopology returns the production facade over reg. The
// runner builds one per process; the simulator builds one per simulated
// process.
func NewPlannedFailoverTopology(reg ManagerRegistry) PlannedFailoverTopology {
	return newRunnerPlannedTopology(reg)
}

func newRunnerPlannedTopology(reg ManagerRegistry) *runnerPlannedTopology {
	return &runnerPlannedTopology{
		reg:         reg,
		fenceMemo:   make(map[string]fenceMemoEntry),
		promoteMemo: make(map[string]promoteMemoStage),
	}
}

var _ PlannedFailoverTopology = (*runnerPlannedTopology)(nil)

func (p *runnerPlannedTopology) leader() context.Context { return p.reg.LeaderContext() }

// manager returns nn's current manager, or errManagerAbsent.
func (p *runnerPlannedTopology) manager(nn types.NamespacedName) (*TopologyManager, error) {
	if tm := p.reg.Snapshot(nn).Current; tm != nil {
		return tm, nil
	}
	return nil, errManagerAbsent
}

func (p *runnerPlannedTopology) fenceMemoFor(runID string) (fenceMemoEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.fenceMemo[runID]
	return e, ok
}

func (p *runnerPlannedTopology) noteFenceSent(runID, uuid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fenceMemo[runID] = fenceMemoEntry{uuid: uuid, sent: true}
}

// promoteMemoFor returns how far this process's promotion of runID got,
// and whether this process has any promotion memo for it at all.
func (p *runnerPlannedTopology) promoteMemoFor(runID string) (promoteMemoStage, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.promoteMemo[runID]
	return s, ok
}

func (p *runnerPlannedTopology) startPromoteMemo(runID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.promoteMemo[runID]; !ok {
		p.promoteMemo[runID] = promoteMemoNone
	}
}

// notePromoteSent advances runID's memo. A process with no memo for the
// run (a cross-process re-run) keeps none: its memo could not prove what an
// earlier process sent.
func (p *runnerPlannedTopology) notePromoteSent(runID string, s promoteMemoStage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.promoteMemo[runID]; ok && cur < s {
		p.promoteMemo[runID] = s
	}
}

// memoProvesNoWrite reports whether this process's memo proves the run
// never sent super_read_only=OFF to the target.
func (p *runnerPlannedTopology) memoProvesNoWrite(runID string) bool {
	s, ok := p.promoteMemoFor(runID)
	return ok && s < promoteMemoSROClearSent
}

// ---------- claim ----------

// ClaimPlannedFailover is §3.2: step 0 (the retiring managers, with no
// lock held), then the current manager's claimPlannedFailover.
func (p *runnerPlannedTopology) ClaimPlannedFailover(ctx context.Context, nn types.NamespacedName, req PlannedClaimRequest) (PlannedClaimDecision, error) {
	if l := p.leader(); l != nil && l.Err() != nil {
		return PlannedClaimDecision{}, errLeaderLost
	}
	snap := p.reg.Snapshot(nn)
	if snap.Current == nil {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerAbsent, "no topology manager is registered"), nil
	}
	for _, old := range snap.Retiring {
		if old.changingSites() || old.orderedUpdateClaimed() {
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager is still changing the sites"), nil
		}
	}
	return snap.Current.claimPlannedFailover(ctx, req, p.hooks)
}

// EnsureClaim is the engine's hydration (§3.4) for an owns-sites pass: it
// returns Granted when the slot holds the run, waits while another run or
// conflicting work holds the sites, and otherwise hydrates the run's
// claim with needsReprove from fg, the pass's uncached read.
func (p *runnerPlannedTopology) EnsureClaim(ctx context.Context, ref PlannedRunRef, fg *v1alpha1.MysqlFailoverGroup) (PlannedClaimDecision, error) {
	if l := p.leader(); l != nil && l.Err() != nil {
		return PlannedClaimDecision{}, errLeaderLost
	}
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked || pf.RunID != ref.RunID || !plannedFailoverOwnsSites(fg) {
		return PlannedClaimDecision{}, errClaimLost
	}
	snap := p.reg.Snapshot(ref.Group)
	slot := snap.Slot
	if held, ok := slot.snapshot(); ok {
		switch {
		case held.mode == claimModeHold:
			return claimDecisionReject(v1alpha1.PlannedFailoverReasonManualRecoveryPending,
				"automatic failover is held after planned failover run %s", held.runID), nil
		case held.runID != ref.RunID:
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitClaimHeld, "planned failover run %s holds the claim", held.runID), nil
		default:
			return claimDecisionGranted(&held), nil
		}
	}
	c, ok := hydratedRunClaim(pf, time.Now())
	if !ok {
		return PlannedClaimDecision{}, errClaimLost
	}
	tm := snap.Current
	if tm == nil {
		// No other topology actor exists without a manager: hold the slot
		// now so the next manager is blocked from its first poll.
		if slot.hydrate(c) {
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerAbsent, "claim hydrated; no topology manager is registered"), nil
		}
		return p.EnsureClaim(ctx, ref, fg)
	}
	for _, old := range snap.Retiring {
		if old.changingSites() || old.orderedUpdateClaimed() {
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager is still changing the sites"), nil
		}
	}
	if tm.predecessorMutating() {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager is still changing the sites"), nil
	}
	d, decided, raced := tm.hydratePlannedClaim(slot, c)
	if raced {
		// Another claim landed between the check above and the lock;
		// decide again against it.
		return p.EnsureClaim(ctx, ref, fg)
	}
	if decided {
		return d, nil
	}
	return claimDecisionGranted(&c), nil
}

// hydratePlannedClaim installs c in slot under tm.mu unless conflicting
// work holds the sites (decided, with the Wait), or the slot is no longer
// empty (raced).
func (tm *TopologyManager) hydratePlannedClaim(slot *PlannedClaimSlot, c plannedClaim) (d PlannedClaimDecision, decided, raced bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	switch {
	case tm.plannedSlot != slot:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerNotReady, "the topology manager is not wired to the group's claim slot"), true, false
	case tm.deploymentChanging > 0:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPromotionInFlight, "a promotion is in flight"), true, false
	case tm.deploymentUpdateRunning || tm.predecessorUpdateInFlight:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitUpdateInFlight, "an ordered update holds the sites"), true, false
	}
	if !slot.hydrate(c) {
		return PlannedClaimDecision{}, false, true
	}
	tm.crossSiteRetry = true
	return PlannedClaimDecision{}, false, false
}

// ReleasePlannedFailoverClaim releases the claim only when runID holds it,
// and records why (§3.6). It takes slot.mu only, then tm.mu to arm the
// manager's cross-site retry, so the deferred decisions run again.
func (p *runnerPlannedTopology) ReleasePlannedFailoverClaim(nn types.NamespacedName, runID string, why ClaimReleaseReason) bool {
	if why == ReleaseToHold {
		return p.ConvertToHold(nn, runID)
	}
	snap := p.reg.Snapshot(nn)
	if !snap.Slot.release(runID, why) {
		return false
	}
	if tm := snap.Current; tm != nil {
		tm.armCrossSiteRetry()
		tm.logger.Info("planned failover: claim released", "fg", tm.cfg.Name, "runId", runID, "why", string(why))
	}
	return true
}

// ConvertToHold turns runID's claim into an automation hold (why=toHold).
func (p *runnerPlannedTopology) ConvertToHold(nn types.NamespacedName, runID string) bool {
	snap := p.reg.Snapshot(nn)
	if !snap.Slot.toHold(runID) {
		return false
	}
	if tm := snap.Current; tm != nil {
		tm.logger.Warn("planned failover: automation held pending acknowledgement", "fg", tm.cfg.Name, "runId", runID, "sites", []string(nil))
	}
	return true
}

// PlannedClaimHeld reports whether nn's slot holds a claim in either mode.
func (p *runnerPlannedTopology) PlannedClaimHeld(nn types.NamespacedName) bool {
	snap := p.reg.Snapshot(nn)
	if snap.Slot.Held() {
		return true
	}
	return snap.Current != nil && snap.Current.plannedClaimHeld()
}

// ActiveSite is the current manager's effective active site.
func (p *runnerPlannedTopology) ActiveSite(nn types.NamespacedName) (string, bool) {
	tm := p.reg.Snapshot(nn).Current
	if tm == nil {
		return "", false
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.effectiveActiveSiteLocked(), true
}

// ---------- probes ----------

// plannedProbe is one site's live read_only answer.
type plannedProbe struct {
	name     string
	role     state.SiteRole
	readOnly bool
	err      error
}

// plannedProbeSites reads read_only on every site but skip, concurrently,
// each bounded by withMySQLSafetyTimeout. Site config is immutable after
// construction, so no lock is held.
func (tm *TopologyManager) plannedProbeSites(ctx context.Context, skip ...string) []plannedProbe {
	out := make([]plannedProbe, 0, len(tm.sites))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range tm.sites {
		site := &tm.sites[i]
		if slices.Contains(skip, site.name) {
			continue
		}
		wg.Add(1)
		go func(site *siteTracker) {
			defer wg.Done()
			var ro bool
			err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				var err error
				ro, err = site.mysql.CheckReadOnly(probeCtx)
				return err
			})
			mu.Lock()
			out = append(out, plannedProbe{name: site.name, role: site.role, readOnly: ro, err: err})
			mu.Unlock()
		}(site)
	}
	wg.Wait()
	slices.SortFunc(out, func(a, b plannedProbe) int { return strings.Compare(a.name, b.name) })
	return out
}

// plannedPeerVerdict classifies a probe set by §4.1's site classes.
type plannedPeerVerdict struct {
	writable          []string // any class, positively writable
	unreadable        []string // primary-candidate or dr-only, no answer
	unreadableReaders []string // read-only-role sites, no answer (allowed)
}

func classifyPlannedPeers(probes []plannedProbe) plannedPeerVerdict {
	var v plannedPeerVerdict
	for _, p := range probes {
		switch {
		case p.err == nil && !p.readOnly:
			v.writable = append(v.writable, p.name)
		case p.err != nil && p.role == state.SiteRoleReadOnly:
			v.unreadableReaders = append(v.unreadableReaders, p.name)
		case p.err != nil:
			v.unreadable = append(v.unreadable, p.name)
		}
	}
	return v
}

func (v plannedPeerVerdict) allUnreadable() []string {
	return append(slices.Clone(v.unreadable), v.unreadableReaders...)
}

// plannedGrantEvidence is §4.1 at the grant: the source writable (and its
// @@server_uuid from a pooled snapshot), the target a healthy read-only
// replica of the source, every other primary-candidate and dr-only site
// positively read-only, and no reader writable. It owns nothing.
type plannedEvidence struct {
	wait       *PlannedClaimDecision
	sourceUUID string
}

func (tm *TopologyManager) plannedGrantEvidence(ctx context.Context, source, target string) plannedEvidence {
	wait := func(reason v1alpha1.PlannedFailoverWaitReason, format string, args ...any) plannedEvidence {
		d := claimDecisionWait(reason, format, args...)
		return plannedEvidence{wait: &d}
	}
	src, tgt := tm.getSite(source), tm.getSite(target)
	if src == nil || tgt == nil {
		return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "source %q or target %q is not in the topology", source, target)
	}
	var (
		wg      sync.WaitGroup
		srcUUID string
		srcErr  error
		repl    *mysql.ReplicaStatus
		replErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcErr = withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			r, ok := src.mysql.(gtidSnapshotReader)
			if !ok {
				return mysql.ErrSessionUnsupported
			}
			snap, err := r.ReadGTIDSnapshot(probeCtx)
			srcUUID = snap.ServerUUID
			return err
		})
	}()
	go func() {
		defer wg.Done()
		replErr = withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var err error
			repl, err = tgt.mysql.ShowReplicaStatus(probeCtx)
			return err
		})
	}()
	probes := tm.plannedProbeSites(ctx)
	wg.Wait()

	for _, pr := range probes {
		switch pr.name {
		case source:
			switch {
			case pr.err != nil:
				return wait(v1alpha1.PlannedFailoverWaitPeerUnreadable, "the source %q did not answer: %v", source, pr.err)
			case pr.readOnly:
				return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "the source %q reads read-only", source)
			}
		case target:
			switch {
			case pr.err != nil:
				return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q did not answer: %v", target, pr.err)
			case !pr.readOnly:
				return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "the target %q reads writable", target)
			}
		default:
			switch {
			case pr.err == nil && !pr.readOnly:
				return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "site %q reads writable beside the source", pr.name)
			case pr.err != nil && pr.role != state.SiteRoleReadOnly:
				return wait(v1alpha1.PlannedFailoverWaitPeerUnreadable, "site %q did not answer: %v", pr.name, pr.err)
			}
		}
	}
	switch {
	case srcErr != nil || srcUUID == "":
		return wait(v1alpha1.PlannedFailoverWaitPeerUnreadable, "could not read the source's server_uuid: %v", srcErr)
	case replErr != nil:
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "could not read the target's replication status: %v", replErr)
	case repl == nil:
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q has no replication channel", target)
	case !repl.IORunning || !repl.SQLRunning:
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q's replication threads are not both running", target)
	case repl.LastSQLError != "":
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q's applier reports an error: %s", target, repl.LastSQLError)
	case canonicalSourceHost(repl.SourceHost) != canonicalSourceHost(src.host):
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q replicates from %q, not the source %q", target, repl.SourceHost, source)
	}
	return plannedEvidence{sourceUUID: srcUUID}
}

// sourceIdentity is the source's live read_only, super_read_only and
// @@server_uuid.
type sourceIdentity struct {
	readOnly, superReadOnly bool
	uuid                    string
	gtid                    string
}

func readSourceIdentity(ctx context.Context, s interface {
	CheckReadOnly(context.Context) (bool, error)
	CheckSuperReadOnly(context.Context) (bool, error)
	ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error)
}) (sourceIdentity, error) {
	var id sourceIdentity
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		if id.readOnly, err = s.CheckReadOnly(probeCtx); err != nil {
			return err
		}
		if id.superReadOnly, err = s.CheckSuperReadOnly(probeCtx); err != nil {
			return err
		}
		snap, err := s.ReadGTIDSnapshot(probeCtx)
		id.uuid, id.gtid = snap.ServerUUID, snap.GtidExecuted
		return err
	})
	return id, err
}

// pooledIdentity adapts a Checker for readSourceIdentity; the optional
// methods fail with ErrSessionUnsupported when the checker lacks them.
type pooledIdentity struct{ c mysql.Checker }

func (p pooledIdentity) CheckReadOnly(ctx context.Context) (bool, error) {
	return p.c.CheckReadOnly(ctx)
}
func (p pooledIdentity) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	if c, ok := p.c.(superReadOnlyChecker); ok {
		return c.CheckSuperReadOnly(ctx)
	}
	return false, mysql.ErrSessionUnsupported
}

// ReadGTIDSnapshot falls back to GetGtidExecuted with an unknown identity
// (ServerUUID "") for a checker without the combined read; an unknown
// identity never matches a recorded @@server_uuid, so attribution then
// fails safe.
func (p pooledIdentity) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	if c, ok := p.c.(gtidSnapshotReader); ok {
		return c.ReadGTIDSnapshot(ctx)
	}
	gtid, err := p.c.GetGtidExecuted(ctx)
	return mysql.GTIDSnapshot{GtidExecuted: gtid}, err
}

// ---------- fence ----------

// Fence is §4.2 plus §4.3 on one pinned source session: re-prove
// authority, check the source is still the intent's server and writable,
// SET super_read_only=ON, read it back, snapshot the fenced set, then run
// the post-fence proof. The fence is not transactional: nothing is undone
// on error; the run's records and the rollback decide.
func (p *runnerPlannedTopology) Fence(ctx context.Context, ref PlannedRunRef, sourceServerUUID string) (FenceProof, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return FenceProof{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return FenceProof{}, err
	}
	src := tm.getSite(ref.Source)
	if src == nil {
		return FenceProof{}, errSiteNotFound
	}
	// §4.2: the source must still be the unique writable site.
	probes := tm.plannedProbeSites(ctx)
	p.hooks.yield("fence.afterProbe")
	var srcProbe plannedProbe
	var others []plannedProbe
	for _, pr := range probes {
		if pr.name == ref.Source {
			srcProbe = pr
		} else {
			others = append(others, pr)
		}
	}
	if srcProbe.err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	v := classifyPlannedPeers(others)
	switch {
	case srcProbe.readOnly || len(v.writable) > 0:
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceWritable: !srcProbe.readOnly}, nil
	case len(v.unreadable) > 0:
		// Strict evidence: an unreadable primary-candidate or dr-only
		// peer may be a writable site the operator cannot see. Nothing
		// is sent; the engine retries until fenceDeadline.
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonPeerUnreadable, UnreadablePeers: v.unreadable}, nil
	}

	sess, err := src.mysql.PinSession(ctx)
	if err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	defer func() { _ = sess.Close() }()
	id, err := readSourceIdentity(ctx, sess)
	if err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	switch {
	case id.uuid != sourceServerUUID:
		// The source was replaced since the grant; it is untouched.
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged,
			SourceWritable: !id.readOnly, SourceServerUUID: id.uuid}, nil
	case id.readOnly:
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceServerUUID: id.uuid}, nil
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return FenceProof{}, err
	}
	p.hooks.yield("fence.beforeSet")
	// Recorded before the statement: a SET whose reply is lost may still
	// have fenced the source.
	p.noteFenceSent(ref.RunID, id.uuid)
	setErr := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return sess.SetSuperReadOnly(c, true) })
	p.hooks.yield("fence.afterSet")
	if setErr != nil {
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonFenceFailed, SourceServerUUID: id.uuid}, nil
	}
	after, err := readSourceIdentity(ctx, sess)
	if err != nil || !after.superReadOnly || after.uuid != id.uuid {
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonFenceFailed, SourceServerUUID: id.uuid}, nil
	}
	return p.postFenceProof(ctx, tm, ref, after, FenceByMemo), nil
}

// postFenceProof re-runs the probe set after the fence (§4.3): a writable
// peer of any class is FenceForeignWritable; an unreadable promotable peer
// is recorded and enforced at the pre-write gate.
func (p *runnerPlannedTopology) postFenceProof(ctx context.Context, tm *TopologyManager, ref PlannedRunRef, id sourceIdentity, by FenceAttribution) FenceProof {
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source))
	proof := FenceProof{Kind: FenceApplied, SourceServerUUID: id.uuid, GtidSet: id.gtid, Attribution: by, UnreadablePeers: v.allUnreadable()}
	if len(v.writable) > 0 {
		proof.Kind = FenceForeignWritable
		proof.Reason = v1alpha1.PlannedFailoverReasonForeignWritable
	}
	return proof
}

// ProveFence re-proves a fence whose intent is durable but whose result
// is not (§4.3 attribution on a re-run), or re-checks one after a gap.
// super_read_only=ON is the run's only if this process's memo says it sent
// the fence to this server, or (cross process) the intent is durable and
// the server's @@server_uuid is the intent's. A writable source returns
// FenceRetry with SourceWritable: the engine fences it again through Fence,
// which re-proves authority first.
func (p *runnerPlannedTopology) ProveFence(ctx context.Context, ref PlannedRunRef, fence v1alpha1.PlannedFailoverFenceRecord) (FenceProof, error) {
	if l := p.leader(); l != nil && l.Err() != nil {
		return FenceProof{}, errLeaderLost
	}
	tm, err := p.manager(ref.Group)
	if err != nil {
		return FenceProof{}, err
	}
	src := tm.getSite(ref.Source)
	if src == nil {
		return FenceProof{}, errSiteNotFound
	}
	id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	if err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	switch {
	case !id.readOnly:
		return FenceProof{Kind: FenceRetry, SourceWritable: true, SourceServerUUID: id.uuid}, nil
	case !id.superReadOnly:
		// mysqld's boot state: neither writable nor fenced by the run.
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonSourceUnfenced, SourceServerUUID: id.uuid}, nil
	}
	by := p.fenceAttribution(ref.RunID, &fence, id.uuid)
	if by == FenceUnattributed {
		return FenceProof{Kind: FenceNotAttributable, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceServerUUID: id.uuid}, nil
	}
	return p.postFenceProof(ctx, tm, ref, id, by), nil
}

// fenceAttribution is §4.3's rule: the memo for this server, or a durable
// intent naming this server's @@server_uuid.
func (p *runnerPlannedTopology) fenceAttribution(runID string, fence *v1alpha1.PlannedFailoverFenceRecord, liveUUID string) FenceAttribution {
	if m, ok := p.fenceMemoFor(runID); ok && m.sent && m.uuid == liveUUID {
		return FenceByMemo
	}
	if fence != nil && fence.SourceServerUUID != "" && fence.SourceServerUUID == liveUUID {
		return FenceByIntentAndUUID
	}
	return FenceUnattributed
}

// KillAppConnections drains the fenced source's application sessions.
func (p *runnerPlannedTopology) KillAppConnections(ctx context.Context, ref PlannedRunRef) (int32, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return 0, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return 0, err
	}
	p.hooks.yield("kill.before")
	killed, err := tm.KillSiteAppConnections(ctx, ref.Source)
	p.hooks.yield("kill.after")
	return int32(killed), err
}

// ProbeLag observes the target catching up with the fenced set F, and
// whether the source and target answer (for the loss windows).
func (p *runnerPlannedTopology) ProbeLag(ctx context.Context, ref PlannedRunRef, fenced string) (LagProbe, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return LagProbe{}, err
	}
	want, err := mysql.ParseGTIDSet(fenced)
	if err != nil {
		return LagProbe{}, fmt.Errorf("parse fenced GTID set: %w", err)
	}
	src, tgt := tm.getSite(ref.Source), tm.getSite(ref.Target)
	if src == nil || tgt == nil {
		return LagProbe{}, errSiteNotFound
	}
	var out LagProbe
	out.SourceReachable = withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		_, err := src.mysql.CheckReadOnly(c)
		return err
	}) == nil
	raw, have, err := readGTIDSetRaw(ctx, tgt.mysql)
	if err != nil {
		return out, nil
	}
	out.TargetReachable, out.TargetGtid = true, raw
	out.CaughtUp = have.Contains(want)
	out.Divergent = !have.Subtract(want).IsEmpty()
	return out, nil
}

// ReadTargetServerUUID reads the target's @@server_uuid on a pinned
// session, for the Promoting intent. Calling it marks the start of this
// process's promotion memo for the run: from here on the memo can prove
// which promotion statements this process did not send.
func (p *runnerPlannedTopology) ReadTargetServerUUID(ctx context.Context, ref PlannedRunRef) (string, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return "", err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return "", err
	}
	tgt := tm.getSite(ref.Target)
	if tgt == nil {
		return "", errSiteNotFound
	}
	sess, err := tgt.mysql.PinSession(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = sess.Close() }()
	var snap mysql.GTIDSnapshot
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var err error
		snap, err = sess.ReadGTIDSnapshot(c)
		return err
	}); err != nil {
		return "", err
	}
	if snap.ServerUUID == "" {
		return "", fmt.Errorf("target %q reported an empty server_uuid", ref.Target)
	}
	p.startPromoteMemo(ref.RunID)
	return snap.ServerUUID, nil
}

// RecordPlannedPromotion records the confirmed promotion into the
// topology manager (recordFailover) while the claim is held, once per
// run, and leaves it on the claim for a replacement manager of this
// process.
func (p *runnerPlannedTopology) RecordPlannedPromotion(ctx context.Context, ref PlannedRunRef, confirmedAt time.Time, watermark string) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return err
	}
	tm.recordPlannedPromotion(ctx, ref, confirmedAt, watermark)
	return nil
}

func (tm *TopologyManager) recordPlannedPromotion(ctx context.Context, ref PlannedRunRef, confirmedAt time.Time, watermark string) {
	tm.mu.Lock()
	done := tm.lastPlannedRunRecorded == ref.RunID
	tm.lastPlannedRunRecorded = ref.RunID
	slot := tm.plannedSlot
	tm.mu.Unlock()
	slot.update(ref.RunID, func(c *plannedClaim) { c.promotion = &recordedPromotion{confirmedAt: confirmedAt} })
	if done {
		return
	}
	tm.recordFailover(ctx, confirmedAt, ref.Target, watermark)
}

// ApplyDNS points DNS at the confirmed target while the claim is held.
func (p *runnerPlannedTopology) ApplyDNS(ctx context.Context, ref PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return "", err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return "", err
	}
	tgt := tm.getSite(ref.Target)
	if tgt == nil {
		return "", errSiteNotFound
	}
	if tm.dns == nil || tgt.lbIP == "" {
		return v1alpha1.PlannedFailoverDNSSkipped, nil
	}
	dnsCtx, cancel := context.WithTimeout(ctx, plannedDNSBudget)
	defer cancel()
	var last error
	for i := 0; i < plannedDNSAttempts && dnsCtx.Err() == nil; i++ {
		if last = tm.applyDNS(dnsCtx, ref.Target, tgt.lbIP); last == nil {
			return v1alpha1.PlannedFailoverDNSApplied, nil
		}
	}
	return v1alpha1.PlannedFailoverDNSFailed, last
}

// NameUnconfirmedTarget names a target that may have taken writes but
// could not be confirmed as lastFailoverTarget in the manager's memory only
// (OPEN-1): no lastFailover and no promotedSite, so emergency failover to
// the fenced source is not cooldown-blocked. The name reaches
// status.lastFailoverTarget through the next snapshot write.
func (p *runnerPlannedTopology) NameUnconfirmedTarget(ref PlannedRunRef) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
		return err
	}
	tm.namePlannedTargetUnconfirmed(ref.Target)
	return nil
}

func (tm *TopologyManager) namePlannedTargetUnconfirmed(target string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.lastFailoverTarget = target
	tm.statusWriteFailed = true
}

// NameFencedSource names the run's own fenced source for the topology
// manager's re-assert (§6.5) before the claim is released.
func (p *runnerPlannedTopology) NameFencedSource(ref PlannedRunRef) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
		return err
	}
	tm.nameFencedSource(ref.RunID, ref.Source)
	return nil
}

func (tm *TopologyManager) nameFencedSource(runID, source string) {
	tm.mu.Lock()
	tm.lastActiveSite, tm.lastActiveSiteNamed = source, true
	tm.mu.Unlock()
	tm.logger.Info("planned failover: named fenced source for re-assert", "fg", tm.cfg.Name, "runId", runID, "site", source)
}

// ---------- re-proof (§4.4) ----------

// ownFailoverRecord reports whether the durable record rec is the run's
// own (recordFailover flushed it before the Publishing write landed):
// the run's target, not before promotion.intendedAt, carrying the fenced
// set F as its watermark.
func ownFailoverRecord(fg *v1alpha1.MysqlFailoverGroup, rec FailoverRecordRef, pf *v1alpha1.PlannedFailoverStatus) bool {
	if pf.Promotion == nil || pf.Fence == nil || pf.Fence.GtidSet == nil || rec.LastFailoverTarget != pf.Target {
		return false
	}
	if rec.LastFailover.Before(pf.Promotion.IntendedAt.UTC().Truncate(time.Second)) {
		return false
	}
	watermark := fg.GetAnnotations()[PromotionGtidExecutedAnnotation]
	if watermark == "" {
		watermark = fg.Status.PromotionGtidExecuted
	}
	return sameGTIDText(watermark, *pf.Fence.GtidSet)
}

// sameGTIDText compares two GTID sets by value, falling back to the text.
func sameGTIDText(a, b string) bool {
	x, errA := mysql.ParseGTIDSet(a)
	y, errB := mysql.ParseGTIDSet(b)
	if errA != nil || errB != nil {
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	return x.Contains(y) && y.Contains(x)
}

func reasonPtr(r v1alpha1.PlannedFailoverReason) *v1alpha1.PlannedFailoverReason { return &r }

// phaseAfterFencing reports whether the run's fence must be in place.
func phaseAfterFencing(phase v1alpha1.PlannedFailoverPhase) bool {
	switch phase {
	case v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync, v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting:
		return true
	}
	return false
}

// Reprove is the first mutating call after hydration (§4.4). It clears
// needsReprove when the record is unchanged (or is the run's own), the
// run's fence is intact and the probe set passes, and maps a failure by
// phase. A HandOff result also clears needsReprove: the engine then calls
// Promote, whose pre-write gate and confirm apply the §7.2/§7.3 mappings,
// so a lack of evidence never holds.
func (p *runnerPlannedTopology) Reprove(ctx context.Context, ref PlannedRunRef, fg *v1alpha1.MysqlFailoverGroup) (ReproveResult, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return ReproveResult{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
		return ReproveResult{}, err
	}
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked || pf.RunID != ref.RunID {
		return ReproveResult{}, errClaimLost
	}
	clear := func() { tm.plannedSlot.update(ref.RunID, func(c *plannedClaim) { c.needsReprove = false }) }

	if pf.Phase == v1alpha1.PlannedFailoverPhaseRollingBack {
		// Each rollback step is decided live under §9's stricter
		// preconditions (no writable peer of any class, an attributable
		// fence).
		clear()
		return ReproveResult{OK: true}, nil
	}

	// Check 1: the durable record is the baseline, or the run's own.
	var failed *v1alpha1.PlannedFailoverReason
	recordChanged := false
	cur, recErr := durableFailoverRecord(fg, tm.clock.Now())
	if recErr != nil || !sameFailoverRecord(cur, failoverRecordRefFromAPI(pf.ClaimBaseline)) {
		if recErr != nil || !ownFailoverRecord(fg, cur, pf) {
			failed, recordChanged = reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged), true
		}
	}
	// Check 2: the run's fence is intact.
	lackOfEvidence := false
	var warnings []v1alpha1.PlannedFailoverWarning
	if src := tm.getSite(ref.Source); src != nil && failed == nil && phaseAfterFencing(pf.Phase) {
		id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
		switch {
		case err != nil:
			failed, lackOfEvidence = reasonPtr(v1alpha1.PlannedFailoverReasonSourceLost), true
		case !id.superReadOnly || pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID:
			failed, lackOfEvidence = reasonPtr(v1alpha1.PlannedFailoverReasonSourceUnfenced), true
		}
	}
	// Check 3: the probe set, with the source read-only.
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	foreignWritable := len(v.writable) > 0
	if failed == nil && foreignWritable {
		failed = reasonPtr(v1alpha1.PlannedFailoverReasonForeignWritable)
	}
	if len(v.allUnreadable()) > 0 {
		warnings = append(warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
		if pf.Phase == v1alpha1.PlannedFailoverPhasePromoting && failed == nil {
			lackOfEvidence = true
		}
	}

	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		if failed != nil {
			return ReproveResult{RollBack: failed}, nil
		}
		clear()
		return ReproveResult{OK: true}, nil
	}

	if failed == nil && !lackOfEvidence {
		clear()
		return ReproveResult{OK: true}, nil
	}
	// Promoting: classify the target first.
	tgt := tm.getSite(ref.Target)
	if tgt == nil {
		return ReproveResult{}, errSiteNotFound
	}
	F, ferr := plannedFenceSet(pf)
	if ferr == nil {
		if live, err := readPromotionLive(ctx, tgt.mysql); err == nil && live.readOnly {
			if extra := live.gtid.Subtract(F); extra.IsEmpty() {
				// No transaction of the run committed: put the target back
				// behind super_read_only and roll back.
				if !live.superReadOnly {
					_ = withMySQLSafetyTimeout(ctx, func(c context.Context) error { return tgt.mysql.SetSuperReadOnly(c, true) })
				}
				reason := v1alpha1.PlannedFailoverReasonPeerUnreadable
				if failed != nil {
					reason = *failed
				}
				return ReproveResult{RollBack: reasonPtr(reason)}, nil
			}
		}
	}
	switch {
	case recordChanged:
		return ReproveResult{Hold: reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged)}, nil
	case foreignWritable:
		return ReproveResult{Hold: reasonPtr(v1alpha1.PlannedFailoverReasonForeignWritable)}, nil
	}
	clear()
	return ReproveResult{HandOff: &PromoteOutcome{
		Kind: PromoteUnconfirmed, Warnings: warnings, MayHaveWritten: !p.memoProvesNoWrite(ref.RunID), TargetReachable: true,
	}}, nil
}

// plannedFenceSet parses the run's fenced set F.
func plannedFenceSet(pf *v1alpha1.PlannedFailoverStatus) (mysql.GTIDSet, error) {
	if pf.Fence == nil || pf.Fence.GtidSet == nil {
		return nil, errors.New("the run has no fenced GTID set")
	}
	return mysql.ParseGTIDSet(*pf.Fence.GtidSet)
}

// ---------- rollback (§9) ----------

// Rollback runs one idempotent pass of §9, every step decided live: the
// hard preconditions, the target's channel, then the source. Outcome ""
// means not finished: the engine retries until rollbackDeadline.
func (p *runnerPlannedTopology) Rollback(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (RollbackResult, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return RollbackResult{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return RollbackResult{}, err
	}
	return tm.rollbackRun(ctx, p, ref, pf)
}

func (tm *TopologyManager) rollbackRun(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (RollbackResult, error) {
	reason := v1alpha1.PlannedFailoverReasonInternalError
	if pf.Rollback != nil {
		reason = pf.Rollback.Reason
	}
	src, tgt := tm.getSite(ref.Source), tm.getSite(ref.Target)
	if src == nil || tgt == nil {
		return RollbackResult{}, errSiteNotFound
	}
	res := RollbackResult{Reason: reason}
	F, fErr := plannedFenceSet(pf)

	// Precondition: once a write may have been sent, the target must be
	// reachable and hold nothing outside F.
	live, liveErr := readPromotionLive(ctx, tgt.mysql)
	if pf.Promotion != nil && !p.memoProvesNoWrite(ref.RunID) {
		switch {
		case liveErr != nil:
			res.Outcome = v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed
			return res, nil
		case fErr != nil || !live.gtid.Subtract(F).IsEmpty():
			res.Outcome, res.Reason = v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonExecuteFailed
			return res, nil
		}
	}

	// The source as it is now.
	id, idErr := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	if idErr == nil && !id.readOnly {
		tm.restorePlannedTargetChannel(ctx, ref, src, tgt, live, liveErr, F, fErr, &res)
		res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
		return res, nil
	}
	peers := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source))
	p.hooks.yield("rollback.afterProbe")
	if len(peers.writable) > 0 {
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		return res, nil
	}
	if reason == v1alpha1.PlannedFailoverReasonSourceLost {
		// §9 step 3: no unfence retries for a lost source.
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		return res, nil
	}
	if idErr != nil {
		return res, nil // unreachable: retry until rollbackDeadline
	}
	if p.fenceAttribution(ref.RunID, pf.Fence, id.uuid) == FenceUnattributed {
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable)
		return res, nil
	}
	tm.restorePlannedTargetChannel(ctx, ref, src, tgt, live, liveErr, F, fErr, &res)

	// Unfence on a pinned session to the attributed server.
	sess, err := src.mysql.PinSession(ctx)
	if err != nil {
		return res, nil
	}
	defer func() { _ = sess.Close() }()
	pinned, err := readSourceIdentity(ctx, sess)
	switch {
	case err != nil:
		return res, nil
	case pinned.uuid != id.uuid:
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable)
		return res, nil
	case !pinned.readOnly:
		res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
		return res, nil
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return RollbackResult{}, err
	}
	p.hooks.yield("unfence.before")
	unfenceErr := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		return errors.Join(sess.SetSuperReadOnly(c, false), sess.SetReadOnly(c, false))
	})
	p.hooks.yield("unfence.after")
	if unfenceErr != nil {
		return res, nil
	}
	ro := true
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var err error
		ro, err = sess.CheckReadOnly(c)
		return err
	}); err != nil || ro {
		return res, nil
	}
	res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
	return res, nil
}

// restorePlannedTargetChannel is §9 step 2: give the target its channel
// back from the source when it is read-only, has no channel and holds a
// non-empty subset of F; restart a stopped channel; otherwise leave it for
// clone or reclone with the warning TargetReplicationNotRestored.
func (tm *TopologyManager) restorePlannedTargetChannel(ctx context.Context, ref PlannedRunRef, src, tgt *siteTracker,
	live promotionLive, liveErr error, F mysql.GTIDSet, fErr error, res *RollbackResult) {
	warn := func() {
		if !slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
			res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored)
		}
	}
	if liveErr != nil || fErr != nil || !live.readOnly {
		warn()
		return
	}
	if live.repl != nil {
		if live.repl.IORunning && live.repl.SQLRunning {
			return
		}
		if err := tm.withReplicaStartGate(ref.Target, func() error {
			return withMySQLSafetyTimeout(ctx, func(c context.Context) error { return tgt.mysql.StartReplica(c) })
		}); err != nil {
			warn()
		}
		return
	}
	if live.gtid.IsEmpty() || !live.gtid.Subtract(F).IsEmpty() || tm.bootstrapCfg.ReplUser == "" {
		warn()
		return
	}
	opts := mysql.ReplicationSourceOpts{Host: src.host, User: tm.bootstrapCfg.ReplUser, Password: tm.bootstrapCfg.ReplPassword, UseSSL: tm.bootstrapCfg.UseSSL}
	if err := tm.withReplicaStartGate(ref.Target, func() error {
		return withMySQLSafetyTimeout(ctx, func(c context.Context) error {
			return errors.Join(tgt.mysql.ChangeReplicationSource(c, opts), tgt.mysql.StartReplica(c))
		})
	}); err != nil {
		warn()
	}
}

// ---------- promotion (§7) ----------

// promotionLive is the target's state read for stage classification.
type promotionLive struct {
	readOnly, superReadOnly bool
	repl                    *mysql.ReplicaStatus
	uuid                    string
	gtid                    mysql.GTIDSet
	gtidRaw                 string
}

type promotionReader interface {
	CheckReadOnly(context.Context) (bool, error)
	ShowReplicaStatus(context.Context) (*mysql.ReplicaStatus, error)
}

// readPromotionLive reads the stage inputs. On a pinned Session every read
// is on that one connection; on a Checker the optional reads go through
// pooledIdentity.
func readPromotionLive(ctx context.Context, r promotionReader) (promotionLive, error) {
	var id interface {
		CheckSuperReadOnly(context.Context) (bool, error)
		ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error)
	}
	switch c := r.(type) {
	case mysql.Session:
		id = c
	case mysql.Checker:
		id = pooledIdentity{c}
	default:
		return promotionLive{}, errors.New("unsupported promotion reader")
	}
	var live promotionLive
	err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var err error
		if live.readOnly, err = r.CheckReadOnly(c); err != nil {
			return err
		}
		if live.superReadOnly, err = id.CheckSuperReadOnly(c); err != nil {
			return err
		}
		if live.repl, err = r.ShowReplicaStatus(c); err != nil {
			return err
		}
		snap, err := id.ReadGTIDSnapshot(c)
		if err != nil {
			return err
		}
		live.uuid, live.gtidRaw = snap.ServerUUID, snap.GtidExecuted
		live.gtid, err = mysql.ParseGTIDSet(snap.GtidExecuted)
		return err
	})
	return live, err
}

// ownExtras reports whether every GTID in extra was written by the server
// with @@server_uuid u.
func ownExtras(extra mysql.GTIDSet, u string) bool {
	for sid := range extra {
		if uuid, _, _ := strings.Cut(sid, ":"); !strings.EqualFold(uuid, u) {
			return false
		}
	}
	return true
}

// classifyPromotionStage is §7.1's table: from the target's live state on
// the pinned session, the fenced set F, the intent's @@server_uuid U and
// whether this process's memo proves super_read_only=OFF was never sent,
// it returns the stage to resume from, or a final outcome (verdict != nil)
// for Aborted and Inconsistent rows.
func classifyPromotionStage(live promotionLive, F mysql.GTIDSet, U string, memoNoWrite bool) (PromotionStage, *PromoteOutcome) {
	abort := func(reason v1alpha1.PlannedFailoverReason, terminal v1alpha1.PlannedFailoverOutcome) (PromotionStage, *PromoteOutcome) {
		return PromotionStageTargetReplaced, &PromoteOutcome{Kind: PromoteAborted, Stage: PromotionStageTargetReplaced, Reason: reason,
			Terminal: terminal, TargetServerUUID: live.uuid, TargetReachable: true}
	}
	inconsistent := func(reason v1alpha1.PlannedFailoverReason) (PromotionStage, *PromoteOutcome) {
		return PromotionStageInconsistent, &PromoteOutcome{Kind: PromoteInconsistent, Stage: PromotionStageInconsistent, Reason: reason,
			TargetServerUUID: live.uuid, TargetReachable: true, MayHaveWritten: true}
	}
	extra := live.gtid.Subtract(F)
	missing := !live.gtid.Contains(F)
	own := ownExtras(extra, U)
	switch {
	case !strings.EqualFold(live.uuid, U):
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonTargetChanged, "")
		}
		return inconsistent(v1alpha1.PlannedFailoverReasonExecuteFailed)
	case !live.readOnly && live.repl != nil:
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonForeignWritable, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		}
		return inconsistent(v1alpha1.PlannedFailoverReasonForeignWritable)
	case !live.readOnly && memoNoWrite:
		// Writable although this process never cleared super_read_only:
		// someone else made it writable.
		return abort(v1alpha1.PlannedFailoverReasonForeignWritable, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
	case !live.readOnly && !missing && own:
		return PromotionStageWritable, nil
	case !live.readOnly && missing:
		return inconsistent(v1alpha1.PlannedFailoverReasonExecuteFailed)
	case !live.readOnly:
		return inconsistent(v1alpha1.PlannedFailoverReasonForeignWritable)
	case live.repl != nil && (live.repl.IORunning || live.repl.IOConnecting || live.repl.SQLRunning):
		return PromotionStageNotStarted, nil
	case live.repl != nil:
		return PromotionStageReplicaStopped, nil
	case !missing && extra.IsEmpty():
		return PromotionStageReplicaReset, nil
	case !missing && own:
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonAuthorityChanged, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		}
		return PromotionStageWasWritable, nil
	case missing && own:
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonTargetChanged, "")
		}
		return inconsistent(v1alpha1.PlannedFailoverReasonExecuteFailed)
	}
	return inconsistent(v1alpha1.PlannedFailoverReasonForeignWritable)
}

// plannedGateRefusal carries the outcome of a refused pre-write gate
// through promoteVerifiedFrom.
type plannedGateRefusal struct{ out PromoteOutcome }

func (e *plannedGateRefusal) Error() string {
	return fmt.Sprintf("planned failover: pre-write gate refused (%s)", e.out.Reason)
}

// Promote is §7: classify the target's stage on a pinned session, resume
// the staged promotion from it with the pre-write gate immediately before
// super_read_only=OFF, then confirm on the same session.
func (p *runnerPlannedTopology) Promote(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return PromoteOutcome{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return PromoteOutcome{}, err
	}
	return tm.promotePlannedRun(ctx, p, ref, pf)
}

// beginPlannedPromotion claims the topology-change window for the run's
// promotion; the caller defers finishDeploymentTopologyChange.
func (tm *TopologyManager) beginPlannedPromotion() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.authorityEpoch++
	tm.deploymentChanging++
	tm.deploymentTopologyPendingLocked()
}

func (tm *TopologyManager) promotePlannedRun(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error) {
	tgt := tm.getSite(ref.Target)
	if tgt == nil {
		return PromoteOutcome{}, errSiteNotFound
	}
	if pf.Promotion == nil || pf.Promotion.TargetServerUUID == "" {
		return PromoteOutcome{}, errors.New("planned failover: the Promoting record names no target server_uuid")
	}
	U := pf.Promotion.TargetServerUUID
	F, err := plannedFenceSet(pf)
	if err != nil {
		return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonInvalidGTID}, nil
	}
	tm.beginPlannedPromotion()
	defer tm.finishDeploymentTopologyChange()

	unconfirmed := func(stage PromotionStage, warnings ...v1alpha1.PlannedFailoverWarning) PromoteOutcome {
		reachable := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
			_, err := tgt.mysql.CheckReadOnly(c)
			return err
		}) == nil
		return PromoteOutcome{Kind: PromoteUnconfirmed, Stage: stage, Reason: v1alpha1.PlannedFailoverReasonPromotionUnconfirmed,
			Warnings: warnings, TargetServerUUID: U, TargetReachable: reachable, MayHaveWritten: !p.memoProvesNoWrite(ref.RunID)}
	}

	sess, err := tgt.mysql.PinSession(ctx)
	if err != nil {
		return unconfirmed(""), nil
	}
	defer func() { _ = sess.Close() }()
	live, err := readPromotionLive(ctx, sess)
	if err != nil {
		return unconfirmed(""), nil
	}
	stage, verdict := classifyPromotionStage(live, F, U, p.memoProvesNoWrite(ref.RunID))
	if verdict != nil {
		return *verdict, nil
	}
	if stage != PromotionStageWritable {
		var gateWarnings []v1alpha1.PlannedFailoverWarning
		hooks := promoteHooks{
			before: func(s promoteMemoStage) {
				p.notePromoteSent(ref.RunID, s)
				p.hooks.yield("promote.before")
			},
			after: func() { p.hooks.yield("promote.after") },
			gate: func(gateCtx context.Context) error {
				w, err := tm.plannedPreWriteGate(gateCtx, p, ref, pf, sess, F, U, stage == PromotionStageWasWritable)
				gateWarnings = w
				return err
			},
		}
		_, sent, err := tm.failover.promoteVerifiedFrom(ctx, sess, ref.Target, F, stage, hooks)
		var refusal *plannedGateRefusal
		var divergent *DivergentCandidateError
		mismatch := errors.Is(err, errCandidateGTIDMismatch) || errors.As(err, &divergent)
		switch {
		case errors.As(err, &refusal):
			refusal.out.Stage = stage
			return refusal.out, nil
		case errors.Is(err, errLeaderLost) || errors.Is(err, errClaimLost):
			return PromoteOutcome{}, err
		case mismatch && sent < promoteStageWriteAttempted && p.memoProvesNoWrite(ref.RunID):
			return PromoteOutcome{Kind: PromoteAborted, Stage: stage, Reason: v1alpha1.PlannedFailoverReasonTargetChanged,
				TargetServerUUID: U, TargetReachable: true}, nil
		case err != nil:
			return unconfirmed(stage, gateWarnings...), nil
		}
		tm.deploymentPromotionExecuted(ref.Target)
		out := tm.confirmPlannedPromotion(ctx, ref, pf, sess, F, U)
		out.Stage = stage
		out.Warnings = mergeWarnings(gateWarnings, out.Warnings)
		if out.Kind == PromoteUnconfirmed {
			out.TargetReachable = unconfirmed(stage).TargetReachable
		}
		return out, nil
	}
	out := tm.confirmPlannedPromotion(ctx, ref, pf, sess, F, U)
	out.Stage = PromotionStageWritable
	if out.Kind == PromoteConfirmed {
		tm.deploymentPromotionExecuted(ref.Target)
	}
	return out, nil
}

func mergeWarnings(a, b []v1alpha1.PlannedFailoverWarning) []v1alpha1.PlannedFailoverWarning {
	out := slices.Clone(a)
	for _, w := range b {
		if !slices.Contains(out, w) {
			out = append(out, w)
		}
	}
	return out
}

// plannedPreWriteGate is §7.2, run on sess immediately before
// SET super_read_only=OFF. A refusal is a *plannedGateRefusal with the
// mapped outcome; nothing has been written to the target by this attempt.
func (tm *TopologyManager) plannedPreWriteGate(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef,
	pf *v1alpha1.PlannedFailoverStatus, sess mysql.Session, F mysql.GTIDSet, U string, wasWritable bool) ([]v1alpha1.PlannedFailoverWarning, error) {
	// 1. Leader and claim.
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return nil, err
	}
	refuse := func(kind PromoteOutcomeKind, reason v1alpha1.PlannedFailoverReason, terminal v1alpha1.PlannedFailoverOutcome, sites []string) error {
		tm.logger.Warn("planned failover: pre-write gate refused", "fg", tm.cfg.Name, "runId", ref.RunID, "reason", string(reason), "sites", sites)
		return &plannedGateRefusal{out: PromoteOutcome{Kind: kind, Reason: reason, Terminal: terminal, TargetServerUUID: U,
			TargetReachable: true, MayHaveWritten: wasWritable}}
	}
	// 2. The target, on the session that will be written.
	live, err := readPromotionLive(ctx, sess)
	if err != nil {
		return nil, err
	}
	extra := live.gtid.Subtract(F)
	gtidOK := live.gtid.Contains(F) && extra.IsEmpty()
	if wasWritable {
		gtidOK = live.gtid.Contains(F) && ownExtras(extra, U)
	}
	if !live.readOnly || live.repl != nil || !strings.EqualFold(live.uuid, U) || !gtidOK {
		if wasWritable {
			return nil, refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed, "", []string{ref.Target})
		}
		return nil, refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonTargetChanged, "", []string{ref.Target})
	}
	// 3. The source (pooled: it is not the server being written).
	var warnings []v1alpha1.PlannedFailoverWarning
	src := tm.getSite(ref.Source)
	id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	switch {
	case err != nil:
		return nil, refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonSourceLost, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, []string{ref.Source})
	case wasWritable && !id.readOnly:
		return nil, refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, "", []string{ref.Source})
	case wasWritable && (pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID):
		warnings = append(warnings, v1alpha1.PlannedFailoverWarningSourceReplaced)
	case !wasWritable && (!id.superReadOnly || pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID):
		return nil, refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonSourceUnfenced, "", []string{ref.Source})
	}
	// 4. Every other site.
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	p.hooks.yield("gate.afterProbe")
	switch {
	case len(v.writable) > 0 && wasWritable:
		return nil, refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, "", v.writable)
	case len(v.writable) > 0:
		return nil, refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonForeignWritable, "", v.writable)
	case len(v.unreadable) > 0 && wasWritable:
		return nil, refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonPeerUnreadable, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, v.unreadable)
	case len(v.unreadable) > 0:
		return nil, refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonPeerUnreadable, "", v.unreadable)
	}
	if len(v.unreadableReaders) > 0 {
		warnings = append(warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	return warnings, nil
}

// confirmPlannedPromotion is §7.3: every check live, on the session that
// was written and on the other sites.
func (tm *TopologyManager) confirmPlannedPromotion(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus,
	sess mysql.Session, F mysql.GTIDSet, U string) PromoteOutcome {
	out := PromoteOutcome{TargetServerUUID: U, TargetReachable: true, MayHaveWritten: true}
	live, err := readPromotionLive(ctx, sess)
	if err != nil {
		out.Kind, out.Reason, out.TargetReachable = PromoteUnconfirmed, v1alpha1.PlannedFailoverReasonPromotionUnconfirmed, false
		return out
	}
	extra := live.gtid.Subtract(F)
	switch {
	case live.readOnly:
		// Re-fenced or restarted after the clear: re-classify next pass.
		out.Kind, out.Reason = PromoteUnconfirmed, v1alpha1.PlannedFailoverReasonPromotionUnconfirmed
		return out
	case live.repl != nil || !live.gtid.Contains(F) || !ownExtras(extra, U):
		out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable
		return out
	case !strings.EqualFold(live.uuid, U):
		out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed
		return out
	}
	if src := tm.getSite(ref.Source); src != nil {
		id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
		switch {
		case err != nil:
			out.Warnings = append(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
		case !id.readOnly:
			out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable
			return out
		case pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID:
			out.Warnings = append(out.Warnings, v1alpha1.PlannedFailoverWarningSourceReplaced)
		}
	}
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	if len(v.writable) > 0 {
		out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable
		return out
	}
	if len(v.allUnreadable()) > 0 && !slices.Contains(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers) {
		out.Warnings = append(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	out.Kind, out.Reason = PromoteConfirmed, ""
	return out
}
