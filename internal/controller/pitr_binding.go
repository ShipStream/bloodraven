package controller

// The PITR archive binding engine (#203, IMPL-203 §4.1-§4.5, §4.10): per
// normalized destination the group writes or reads, the encryption identity
// it accepted. Bindings become Bound only by proof (a sidecar verified the
// descriptor with exactly that snapshot and mode, or a check Job did), are
// never demoted by sidecar observations, and are never removed except by an
// explicit release. Drift is reported on the PITRArchiveBindingReady
// condition; the renderer (pitr_render.go) keeps rendering the binding.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
)

// pitrBindingProcedure is the way out of a refused encryption or key change.
const pitrBindingProcedure = "archiving continues under the accepted identity; to change it, create a new profile with a new storage prefix and its own passphrase, switch spec.backup.pitr.profileName to it, wait until its archive binding is Bound, then take a full backup (see Backup encryption > Rotation)"

// Requeue hints of the pass.
const (
	pitrCheckPollInterval = 10 * time.Second
	pitrPendingRecheck    = 30 * time.Second
)

// pitrBindingTarget is one normalized destination the group writes or reads
// (§4.1).
type pitrBindingTarget struct {
	dest  pitrarchive.Destination
	write bool
	// profile is the identity profile: the write profile when the
	// destination is written, else the first read profile (initFromBackup,
	// then verifications by name).
	profile *v1alpha1.BackupProfile
	// uses are the profiles in archive use that resolve to dest, identity
	// profile first, each once.
	uses []*v1alpha1.BackupProfile
	// readers describes the read uses ("initFromBackup",
	// "MysqlBackupVerification <name>").
	readers []string
}

// useDescription says how a target is used, for messages.
func (t *pitrBindingTarget) useDescription() string {
	var parts []string
	if t.write {
		parts = append(parts, "the active PITR archive (profile "+t.uses[0].Name+")")
	}
	if len(t.readers) > 0 {
		parts = append(parts, "read by "+strings.Join(t.readers, ", "))
	}
	return strings.Join(parts, " and ")
}

func (t *pitrBindingTarget) addUse(p *v1alpha1.BackupProfile) {
	for _, u := range t.uses {
		if u.Name == p.Name {
			return
		}
	}
	t.uses = append(t.uses, p)
}

// pitrBindingTargets derives the group's binding targets (§4.1): the write
// target of spec.backup.pitr, the read target of a non-terminal
// initFromBackup.pointInTime, and the read target of every non-terminal
// verification replaying point in time (through its archiveProfileName or
// the active PITR profile). Targets with the same destination merge. A
// profile that is missing or does not resolve contributes nothing (the
// existing paths report it). The order is write, initFromBackup, then
// verifications by name.
func pitrBindingTargets(fg *v1alpha1.MysqlFailoverGroup, mbvs []v1alpha1.MysqlBackupVerification) []pitrBindingTarget {
	var out []pitrBindingTarget
	index := map[string]int{}
	add := func(p *v1alpha1.BackupProfile, write bool, reader string) {
		if p == nil {
			return
		}
		d, err := pitrprofile.DestinationFor(fg, p)
		if err != nil {
			return
		}
		i, ok := index[d.String()]
		if !ok {
			i = len(out)
			index[d.String()] = i
			out = append(out, pitrBindingTarget{dest: d, profile: p})
		}
		t := &out[i]
		if write {
			t.write = true
			t.profile = p
			// The identity profile is listed first.
			t.uses = append([]*v1alpha1.BackupProfile{p}, t.uses...)
			for j := 1; j < len(t.uses); j++ {
				if t.uses[j].Name == p.Name {
					t.uses = append(t.uses[:j], t.uses[j+1:]...)
					break
				}
			}
		} else {
			t.addUse(p)
			t.readers = append(t.readers, reader)
		}
	}
	if pitrEnabled(fg) {
		add(findProfile(fg, fg.Spec.Backup.PITR.ProfileName), true, "")
	}
	if ifb := fg.Spec.InitFromBackup; ifb != nil && ifb.PointInTime != nil && ifb.PointInTime.ArchiveProfileName != "" && pitrRestoreReadsArchive(fg.Status.Restore) {
		add(findProfile(fg, ifb.PointInTime.ArchiveProfileName), false, "initFromBackup")
	}
	sorted := append([]v1alpha1.MysqlBackupVerification(nil), mbvs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for i := range sorted {
		v := &sorted[i]
		if v.Spec.FailoverGroupRef.Name != fg.Name || v.DeletionTimestamp != nil || verificationTerminal(v.Status.Phase) {
			continue
		}
		pit := v.Spec.PointInTime
		if pit == nil || (pit.Mode != v1alpha1.PointInTimeModeLatest && pit.Mode != v1alpha1.PointInTimeModeTimestamp) {
			continue
		}
		name := pit.ArchiveProfileName
		if name == "" {
			if !pitrEnabled(fg) {
				continue
			}
			name = fg.Spec.Backup.PITR.ProfileName
		}
		add(findProfile(fg, name), false, "MysqlBackupVerification "+v.Name)
	}
	return out
}

// pitrRestoreReadsArchive reports whether an initFromBackup restore in
// state rs still reads its point-in-time archive: it has not succeeded, and
// it has not failed with a Job (a restore that failed before its Job was
// created, a pre-launch refusal, is rebuilt on every reconcile once the
// cause is fixed, so its read binding, and the refusal it records, stay).
func pitrRestoreReadsArchive(rs *v1alpha1.RestoreStatus) bool {
	if rs == nil {
		return true
	}
	switch rs.Phase {
	case v1alpha1.BackupPhaseSucceeded:
		return false
	case v1alpha1.BackupPhaseFailed:
		return rs.JobName == ""
	default:
		return true
	}
}

// pitrCandidate is one encryption identity a non-Bound entry may be bound
// to: the identity profile's mode and algorithm and, for Passphrase, a key
// snapshot.
type pitrCandidate struct {
	enc v1alpha1.PITRArchiveBindingEncryption
}

// pitrBindingPass is one run of reconcilePITRArchiveBindings.
type pitrBindingPass struct {
	r         *MysqlFailoverGroupReconciler
	fg        *v1alpha1.MysqlFailoverGroup
	now       time.Time
	digestKey []byte

	snaps   []corev1.Secret
	byName  map[string]*corev1.Secret
	deploys []appsv1.Deployment
	hold    pitrRenderInputs
	holdK   pitrHoldKind
	// unannotated are the site Deployments without a decodable
	// pitr-render annotation (pre-#203 or mangled): their writer may hold
	// any key, and the renderer leaves them alone until the write binding
	// is authenticated (K3).
	unannotated []string
	targets     []pitrBindingTarget
	// inUse are the destinations of the targets.
	inUse map[string]bool

	entries []v1alpha1.PITRArchiveBinding
	// causes records why an entry changed this pass (log cause).
	causes map[string]string
	// newlyBound are the ids that became Bound by proof or check this
	// pass: their snapshot gets the acceptance annotation before the
	// status write.
	newlyBound map[string]bool
	// checkedSnapshot is, per id bound by a check this pass, the UID of
	// the snapshot the check Job mounted: acceptance refuses another
	// object of the same name (U22).
	checkedSnapshot map[string]types.UID
	// recreated are the snapshots C3 re-created this pass.
	recreated []string
	// consumed are check Jobs whose result was recorded, that were
	// abandoned, or leftovers of the deterministic name: deleted (UID
	// precondition) after a successful status write.
	consumed []*batchv1.Job
	// limit are destinations that got no entry (BindingLimitReached).
	limit []string
	// overflow are recovery's binding ids that found no slot.
	overflow []string
	// notes are snapshot findings for the condition message.
	notes []pitrSnapshotNote
	// keySourceRemoved are the live Secrets missing behind Bound entries.
	keySourceRemoved []string
	requeue          time.Duration
	releasePending   string
}

func (p *pitrBindingPass) requeueAfter(d time.Duration) {
	if d > 0 && (p.requeue == 0 || d < p.requeue) {
		p.requeue = d
	}
}

func (p *pitrBindingPass) find(dest string) *v1alpha1.PITRArchiveBinding {
	for i := range p.entries {
		if p.entries[i].Destination == dest {
			return &p.entries[i]
		}
	}
	return nil
}

// setState moves entry to state/reason with message, keeping
// LastTransitionTime unless state or reason changed.
func (p *pitrBindingPass) setState(entry *v1alpha1.PITRArchiveBinding, state v1alpha1.PITRArchiveBindingState, reason v1alpha1.PITRArchiveBindingReason, message, cause string) {
	if entry.State != state || entry.Reason != reason || entry.LastTransitionTime == nil {
		entry.LastTransitionTime = &metav1.Time{Time: p.now.UTC().Truncate(time.Second)}
	}
	entry.State, entry.Reason = state, reason
	entry.Message = truncatePITRMessage(message)
	if cause != "" {
		p.causes[entry.ID] = cause
	}
}

func truncatePITRMessage(s string) string {
	const limit = 2048
	if len(s) <= limit {
		return s
	}
	return s[:limit-3] + "..."
}

// reconcilePITRArchiveBindings runs one pass of the PITR archive binding
// engine (#203, IMPL-203 §4.10): list snapshots, recover, check integrity,
// derive targets, advance each entry (consumer proof, check Jobs,
// eligibility, evaluation), drop unused non-Bound entries, process a
// release, write acceptance annotations, write status, emit Events, delete
// consumed check Jobs. It runs after reconcileEncryptionAtRest and before
// reconcileSiteConfigMaps, and leaves fg.Status.PITRArchiveBindings filled
// in memory for the rendering that follows. The duration is a requeue hint
// (0: none). A group with no PITR target, no binding, no binding condition,
// no release request and no key snapshot has nothing to process: the pass
// returns after the cached reads, without any uncached read.
func (r *MysqlFailoverGroupReconciler) reconcilePITRArchiveBindings(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	if fg.DeletionTimestamp != nil || fg.UID == "" {
		return 0, nil
	}
	p := &pitrBindingPass{r: r, fg: fg, now: time.Now(), causes: map[string]string{}, newlyBound: map[string]bool{},
		checkedSnapshot: map[string]types.UID{}}
	beforeEntries := append([]v1alpha1.PITRArchiveBinding(nil), fg.Status.PITRArchiveBindings...)
	for i := range beforeEntries {
		beforeEntries[i] = *beforeEntries[i].DeepCopy()
	}
	beforeCond := meta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionPITRArchiveBindingReady).DeepCopy()

	mbvs, err := r.listPITRVerifications(ctx, fg)
	if err != nil {
		return 0, err
	}
	p.targets = pitrBindingTargets(fg, mbvs)
	p.releasePending = pitrReleasePending(fg)
	if p.snaps, err = r.listPITRKeySnapshots(ctx, fg); err != nil {
		return 0, err
	}
	_, releaseRequested := fg.Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation]
	if len(p.targets) == 0 && len(beforeEntries) == 0 && beforeCond == nil && !releaseRequested && len(p.snaps) == 0 {
		return 0, nil
	}

	key, err := ensureDigestKey(ctx, r.Client, r.APIReader, r.Scheme, fg)
	if err != nil {
		return 0, err
	}
	p.digestKey = key
	p.byName = map[string]*corev1.Secret{}
	for i := range p.snaps {
		p.byName[p.snaps[i].Name] = &p.snaps[i]
	}
	if p.deploys, err = r.listPITRSiteDeployments(ctx, fg); err != nil {
		return 0, err
	}
	p.hold, p.holdK = groupPITRHold(fg, p.deploys)
	p.unannotated = pitrUndecodableDeployments(fg, p.deploys)
	p.entries = append([]v1alpha1.PITRArchiveBinding(nil), beforeEntries...)
	for i := range p.entries {
		p.entries[i] = *p.entries[i].DeepCopy()
	}

	// 1. Recovery (K8): accepted snapshots rebuild their Bound entries.
	p.inUse = map[string]bool{}
	for _, t := range p.targets {
		p.inUse[t.dest.String()] = true
	}
	var rebuilt []string
	p.entries, rebuilt, p.overflow, p.notes = recoverPITRBindings(fg, p.entries, p.snaps, p.inUse, map[string]bool{p.releasePending: true}, p.now)
	if rebuilt, err = p.confirmRecovered(ctx, beforeEntries, rebuilt); err != nil {
		return 0, err
	}
	for _, id := range rebuilt {
		p.causes[id] = pitrBindingCauseRecovery
	}

	// 2. Integrity of every Bound snapshot (Δ34) and C3. A Bound entry's
	// snapshot is reported by integrity, not as a recovery note.
	if err := p.checkIntegrity(ctx); err != nil {
		return 0, err
	}
	named := map[string]bool{}
	for _, e := range p.entries {
		if e.State == v1alpha1.PITRArchiveBindingBound {
			named[e.Encryption.KeySecretName] = true
		}
	}
	notes := p.notes[:0]
	for _, n := range p.notes {
		if !named[n.secret] {
			notes = append(notes, n)
		}
	}
	p.notes = notes

	// 3. Advance every target's entry.
	for i := range p.targets {
		if err := p.advance(ctx, &p.targets[i]); err != nil {
			return 0, err
		}
	}

	// 4. Drop unused entries that are not Bound (their snapshots stay, K1).
	kept := p.entries[:0]
	for _, e := range p.entries {
		if !p.retained(&e) {
			if e.Check != nil {
				p.consumeCheck(ctx, e.Check)
			}
			continue
		}
		kept = append(kept, e)
	}
	p.entries = kept

	// 5. Release (G1: serialized with the ordered updater's render; it
	// writes the entry's removal itself, so the base moves with it).
	base := beforeEntries
	release, err := r.processPITRBindingRelease(ctx, fg, base, mbvs, p.targets)
	if err != nil {
		return 0, err
	}
	if release != nil && release.removed {
		base = release.base
		kept := p.entries[:0]
		for _, e := range p.entries {
			if e.ID != release.id {
				kept = append(kept, e)
			}
		}
		p.entries = kept
	}
	if release != nil && (release.refused != "" || release.conflict) {
		p.requeueAfter(pitrReleaseRefusedRequeue)
	}

	// 6. Acceptance annotations before the status write (K8).
	p.accept(ctx, beforeEntries)

	// 7. Condition and status. An evaluation that cannot read its inputs
	// writes nothing (the stored condition stays) and is retried.
	sort.SliceStable(p.entries, func(i, j int) bool { return p.entries[i].ID < p.entries[j].ID })
	cond, err := p.condition(ctx)
	if err != nil {
		return 0, err
	}
	if err := r.writePITRArchiveBindings(ctx, fg, base, beforeCond, p.entries, cond); err != nil {
		return 0, err
	}

	// 8. After the status write: finish a release, emit, clean up.
	if release != nil {
		r.finishPITRRelease(ctx, fg, release)
	}
	p.emit(ctx, beforeEntries, beforeCond, cond)
	for _, job := range p.consumed {
		if err := r.deletePITRCheckJob(ctx, job); err != nil {
			log.FromContext(ctx).Error(err, "delete consumed pitr check job", "fg", fg.Name, "job", job.Name)
		}
	}
	p.reclaimCheckJobs(ctx)
	for _, e := range p.entries {
		if e.State == v1alpha1.PITRArchiveBindingPending {
			p.requeueAfter(pitrPendingRecheck)
		}
	}
	return p.requeue, nil
}

// retained reports whether e stays in status after the pass: Bound
// entries always (K1), the rest only while a target uses their
// destination.
func (p *pitrBindingPass) retained(e *v1alpha1.PITRArchiveBinding) bool {
	return e.State == v1alpha1.PITRArchiveBindingBound || p.inUse[e.Destination]
}

// retainedCount is the number of entries the pass keeps (retained): the
// slots the binding limit counts. Unused non-Bound entries are dropped
// before the status write, so they never hold a slot.
func (p *pitrBindingPass) retainedCount() int {
	n := 0
	for i := range p.entries {
		if p.retained(&p.entries[i]) {
			n++
		}
	}
	return n
}

// confirmRecovered re-reads (uncached) the snapshot of every entry
// recovery rebuilt from the cached listing, and undoes a rebuild whose
// snapshot is gone, terminating, or no longer verifies: a lagging cache
// must not resurrect a binding a release just removed (U20). It returns
// the rebuilds that stand.
func (p *pitrBindingPass) confirmRecovered(ctx context.Context, before []v1alpha1.PITRArchiveBinding, rebuilt []string) ([]string, error) {
	var out []string
	for _, id := range rebuilt {
		e := findBindingIn(p.entries, id)
		if e == nil {
			continue
		}
		var s corev1.Secret
		err := p.r.bindingReader().Get(ctx, types.NamespacedName{Namespace: p.fg.Namespace, Name: e.Encryption.KeySecretName}, &s)
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("confirm recovered pitr key snapshot %s: %w", e.Encryption.KeySecretName, err)
		}
		ok := err == nil && s.DeletionTimestamp == nil && s.Labels[v1alpha1.FailoverGroupUIDLabel] == string(p.fg.UID)
		if ok {
			a, verr := verifyPITRAcceptance(&s)
			ok = verr == nil && a.Destination == e.Destination && a.ArchiveID == e.ArchiveID
		}
		if ok {
			out = append(out, id)
			continue
		}
		if prev := findBindingIn(before, id); prev != nil {
			*e = *prev.DeepCopy()
			continue
		}
		kept := p.entries[:0]
		for _, x := range p.entries {
			if x.ID != id {
				kept = append(kept, x)
			}
		}
		p.entries = kept
	}
	return out, nil
}

// accept writes the acceptance annotation (K8) of every Passphrase entry
// that became Bound this pass. On failure the entry does not become Bound
// in status: a snapshot replaced after its check (another object of the
// same name) drops the verdict and is checked again; any other failure
// restores the entry as it was, keeping the check Job it records so the
// same verdict is applied on the next pass, or leaves a new entry
// Pending.
func (p *pitrBindingPass) accept(ctx context.Context, before []v1alpha1.PITRArchiveBinding) {
	for i := range p.entries {
		e := &p.entries[i]
		if !p.newlyBound[e.ID] || e.Encryption.Mode != v1alpha1.EncryptionModePassphrase {
			continue
		}
		err := p.r.acceptPITRKeySnapshot(ctx, p.fg, e, p.checkedSnapshot[e.ID], p.now)
		if err == nil {
			continue
		}
		log.FromContext(ctx).Error(err, "accept pitr key snapshot; the binding is not recorded Bound", "fg", p.fg.Name, "bindingID", e.ID)
		switch prev := findBindingIn(before, e.ID); {
		case errors.Is(err, errPITRCheckedSnapshotReplaced):
			p.setState(e, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, err.Error()+"; checking it again", pitrBindingCauseCheck)
			e.ArchiveID, e.Source, e.Check = "", nil, nil
		case prev != nil:
			*e = *prev.DeepCopy()
			delete(p.causes, e.ID)
			if e.Check != nil {
				p.unconsume(e.Check)
			}
		default:
			// A new entry whose binding could not be recorded: net, the
			// pass only added it.
			p.setState(e, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, "accepting the key snapshot failed: "+err.Error(), pitrBindingCauseInput)
			e.ArchiveID, e.Source = "", nil
		}
		delete(p.newlyBound, e.ID)
		p.requeueAfter(pitrCheckPollInterval)
	}
}

// unconsume keeps the check Job check records: its verdict is still
// needed.
func (p *pitrBindingPass) unconsume(check *v1alpha1.PITRArchiveBindingCheck) {
	kept := p.consumed[:0]
	for _, j := range p.consumed {
		if j.Name == check.JobName && (check.JobUID == "" || j.UID == check.JobUID) {
			continue
		}
		kept = append(kept, j)
	}
	p.consumed = kept
}

// reclaimCheckJobs deletes, after a successful status write, the group's
// finished check Jobs that no entry records: a check consumed by a pass
// that crashed before it deleted the Job, or displaced by recovery (U54).
// Check Jobs have no TTL; nothing else removes them before the group goes.
func (p *pitrBindingPass) reclaimCheckJobs(ctx context.Context) {
	var jobs batchv1.JobList
	if err := p.r.List(ctx, &jobs, client.InNamespace(p.fg.Namespace), client.MatchingLabels{
		v1alpha1.ResourceLabel: resourcePITRArchiveCheck, v1alpha1.FailoverGroupLabel: p.fg.Name,
	}); err != nil {
		log.FromContext(ctx).Error(err, "list pitr check jobs", "fg", p.fg.Name)
		return
	}
	recorded := map[string]bool{}
	for _, e := range p.entries {
		if e.Check != nil {
			recorded[e.Check.JobName] = true
		}
	}
	for _, j := range p.consumed {
		recorded[j.Name] = true
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if recorded[job.Name] || job.DeletionTimestamp != nil || !jobFinished(job) || !metav1.IsControlledBy(job, p.fg) {
			continue
		}
		if err := p.r.deletePITRCheckJob(ctx, job); err != nil {
			log.FromContext(ctx).Error(err, "reclaim pitr check job", "fg", p.fg.Name, "job", job.Name)
		}
	}
}

// findBindingIn returns the entry with id in entries, or nil. The result
// points into entries.
func findBindingIn(entries []v1alpha1.PITRArchiveBinding, id string) *v1alpha1.PITRArchiveBinding {
	if id == "" {
		return nil
	}
	for i := range entries {
		if entries[i].ID == id {
			return &entries[i]
		}
	}
	return nil
}

// listPITRSiteDeployments lists fg's MySQL site Deployments (uncached: the
// hold must reflect what is running).
func (r *MysqlFailoverGroupReconciler) listPITRSiteDeployments(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) ([]appsv1.Deployment, error) {
	return listPITRSiteDeployments(ctx, r.bindingReader(), fg)
}

// listPITRVerifications lists the verifications that reference fg
// (cached): the namespace's verifications, filtered in memory by
// spec.failoverGroupRef. No field index is read: a List by an unregistered
// index fails, and every reconciler built without a manager (tests, tools)
// would then fail the whole pass. A verification change reaches its group
// through the direct-reference watch (pitr_binding_watch.go).
func (r *MysqlFailoverGroupReconciler) listPITRVerifications(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) ([]v1alpha1.MysqlBackupVerification, error) {
	var list v1alpha1.MysqlBackupVerificationList
	if err := r.List(ctx, &list, client.InNamespace(fg.Namespace)); err != nil {
		return nil, fmt.Errorf("list verifications: %w", err)
	}
	out := list.Items[:0]
	for _, v := range list.Items {
		if v.Spec.FailoverGroupRef.Name == fg.Name {
			out = append(out, v)
		}
	}
	return out, nil
}

// checkIntegrity checks every Bound Passphrase entry's snapshot (Δ34) and
// handles a missing one (C3). Nothing is ever deleted. Transitions are
// suppressed for a binding a release annotation names.
func (p *pitrBindingPass) checkIntegrity(ctx context.Context) error {
	for i := range p.entries {
		e := &p.entries[i]
		if e.State != v1alpha1.PITRArchiveBindingBound || e.Encryption.Mode != v1alpha1.EncryptionModePassphrase || e.ID == p.releasePending {
			continue
		}
		res, why, err := p.r.checkPITRKeySnapshotIntegrity(ctx, p.fg, e, p.byName[e.Encryption.KeySecretName])
		if err != nil {
			return err
		}
		switch res {
		case pitrIntegrityOK:
			if e.Reason == v1alpha1.PITRArchiveBindingReasonKeyMaterialLost {
				p.setState(e, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound, "", pitrBindingCauseIntegrity)
			}
		case pitrIntegrityNotFound:
			outcome, err := p.recreate(ctx, e)
			if err != nil {
				return err
			}
			switch outcome {
			case pitrRecreated:
				p.recreated = append(p.recreated, e.Encryption.KeySecretName)
				p.causes[e.ID] = pitrBindingCauseC3
				if e.Reason == v1alpha1.PITRArchiveBindingReasonKeyMaterialLost {
					p.setState(e, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound, "", pitrBindingCauseC3)
				}
				continue
			case pitrRecreateRaced:
				// Another object of the name appeared between the read and
				// the create: nothing was created; the next pass checks it.
				p.requeueAfter(pitrCheckPollInterval)
				continue
			}
			if msg := pitrKeyMaterialLostMessage(e, res, why); e.Reason != v1alpha1.PITRArchiveBindingReasonKeyMaterialLost || e.Message != msg {
				p.setState(e, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost, msg, pitrBindingCauseC3)
			}
		case pitrIntegrityInvalid:
			if msg := pitrKeyMaterialLostMessage(e, res, why); e.Reason != v1alpha1.PITRArchiveBindingReasonKeyMaterialLost || e.Message != msg {
				p.setState(e, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost, msg, pitrBindingCauseIntegrity)
			}
		}
	}
	return nil
}

// pitrKeyMaterialLostMessage explains a lost key and the way back. A
// missing snapshot is restored from a backup of it (C3 re-creates it on
// its own when a profile's passphrase reproduces its name). A snapshot
// that exists but fails the check (labels, destination or acceptance
// annotation changed, or other bytes) may still hold the only copy of the
// accepted key: it is never to be deleted before the original key is
// safe elsewhere (U07).
func pitrKeyMaterialLostMessage(e *v1alpha1.PITRArchiveBinding, res pitrIntegrity, why string) string {
	if res == pitrIntegrityNotFound {
		return fmt.Sprintf("%s; Secret %s held the accepted key of archive %s: restore it from a backup of it, or set a profile resolving to %s to the original passphrase so the operator re-creates it (MySQL keeps running; the archiver fails closed)",
			why, e.Encryption.KeySecretName, e.ArchiveID, e.Destination)
	}
	return fmt.Sprintf("%s; Secret %s no longer passes the check of the accepted key of archive %s: do not delete it, it may hold the only copy of that key; restore its labels and annotations, or its bytes, from a backup of it; delete it (the operator then re-creates it from a profile's passphrase) only once the original passphrase is safely kept elsewhere and a profile resolving to %s holds it (MySQL keeps running; the archiver fails closed)",
		why, e.Encryption.KeySecretName, e.ArchiveID, e.Destination)
}

// recreate is C3: the first profile resolving to e's destination whose live
// bytes reproduce the snapshot name re-creates it (no roll: same name).
func (p *pitrBindingPass) recreate(ctx context.Context, e *v1alpha1.PITRArchiveBinding) (pitrRecreateOutcome, error) {
	for _, prof := range p.profilesAt(e.Destination) {
		if !prof.EncryptionEnabled() {
			continue
		}
		live, err := p.r.readPITRLiveKey(ctx, p.fg, prof)
		if err != nil {
			return pitrRecreateNotReproducible, err
		}
		if live.missing != "" {
			continue
		}
		outcome, err := p.r.recreatePITRKeySnapshot(ctx, p.fg, p.digestKey, e, live.trimmed, p.now)
		if err != nil || outcome != pitrRecreateNotReproducible {
			return outcome, err
		}
	}
	return pitrRecreateNotReproducible, nil
}

// profilesAt returns every spec profile resolving to destination, the
// active PITR profile first, then by name.
func (p *pitrBindingPass) profilesAt(destination string) []*v1alpha1.BackupProfile {
	fg := p.fg
	if fg.Spec.Backup == nil {
		return nil
	}
	var out []*v1alpha1.BackupProfile
	for i := range fg.Spec.Backup.Profiles {
		prof := &fg.Spec.Backup.Profiles[i]
		if d, err := pitrprofile.DestinationFor(fg, prof); err == nil && d.String() == destination {
			out = append(out, prof)
		}
	}
	active := ""
	if fg.Spec.Backup.PITR != nil {
		active = fg.Spec.Backup.PITR.ProfileName
	}
	sort.SliceStable(out, func(i, j int) bool {
		if (out[i].Name == active) != (out[j].Name == active) {
			return out[i].Name == active
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// pitrProfileNames joins profile names for messages.
func pitrProfileNames(ps []*v1alpha1.BackupProfile) string {
	names := make([]string, 0, len(ps))
	for _, p := range ps {
		names = append(names, p.Name)
	}
	return strings.Join(names, ", ")
}

// identityOf is the candidate encryption (mode, algorithm) of profile p.
func identityOf(p *v1alpha1.BackupProfile) v1alpha1.PITRArchiveBindingEncryption {
	id := pitrprofile.EncryptionIdentityFor(p)
	return v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.EncryptionMode(id.Mode), Algorithm: v1alpha1.EncryptionAlgorithm(id.Algorithm)}
}

// sameIdentity compares mode, algorithm and key snapshot.
func sameIdentity(a, b v1alpha1.PITRArchiveBindingEncryption) bool { return a == b }

// candidates returns t's candidates in order (§4.4): the listed snapshots
// for the destination (accepted first, then newest), then the identity
// profile's live bytes (its snapshot created on demand) when they are not
// already listed. Plaintext has one candidate. live is the live candidate
// (nil when the Secret or key is missing, missing says why).
func (p *pitrBindingPass) candidates(ctx context.Context, t *pitrBindingTarget) (cands []pitrCandidate, live *pitrCandidate, missing string, err error) {
	ident := identityOf(t.profile)
	if ident.Mode == v1alpha1.EncryptionModeNone {
		c := pitrCandidate{enc: ident}
		return []pitrCandidate{c}, &c, "", nil
	}
	dest := t.dest.String()
	lk, err := p.r.readPITRLiveKey(ctx, p.fg, t.profile)
	if err != nil {
		return nil, nil, "", err
	}
	liveName := ""
	if lk.missing == "" {
		liveName = pitrKeySnapshotName(p.fg.Name, p.digestKey, dest, lk.trimmed)
	}
	for _, s := range orderPITRSnapshots(p.fg, p.snaps, dest) {
		if s.secret.Name == liveName {
			continue // the live candidate is always last
		}
		enc := ident
		enc.KeySecretName = s.secret.Name
		cands = append(cands, pitrCandidate{enc: enc})
	}
	if lk.missing != "" {
		return cands, nil, lk.missing, nil
	}
	if _, ok := p.byName[liveName]; !ok {
		snap, err := p.r.ensurePITRKeySnapshot(ctx, p.fg, p.digestKey, dest, lk.trimmed)
		if err != nil {
			if errors.Is(err, errPITRSnapshotNameHeld) {
				return cands, nil, fmt.Sprintf("key snapshot name %s is held by another object: %v", liveName, err), nil
			}
			return nil, nil, "", err
		}
		p.snaps = append(p.snaps, *snap)
		p.byName = map[string]*corev1.Secret{}
		for i := range p.snaps {
			p.byName[p.snaps[i].Name] = &p.snaps[i]
		}
	}
	enc := ident
	enc.KeySecretName = liveName
	c := pitrCandidate{enc: enc}
	return append(cands, c), &c, "", nil
}

// observation is status.pitr.archive, the sidecars' aggregated report.
func (p *pitrBindingPass) observation() *v1alpha1.PITRArchiveObservation {
	if p.fg.Status.PITR == nil {
		return nil
	}
	return p.fg.Status.PITR.Archive
}

// proven is consumer proof (§4.4, K4): a sidecar at the destination
// reported the descriptor verified (an archive ID derived for this group
// and destination) with exactly enc's mode and key revision, and nothing
// refuses it. An empty revision (a pre-#203 pod) never proves anything.
func (p *pitrBindingPass) proven(dest pitrarchive.Destination, enc v1alpha1.PITRArchiveBindingEncryption) (string, bool) {
	obs := p.observation()
	rev := pitrKeyRevision(enc)
	if obs == nil || rev == "" || obs.Destination != dest.String() || obs.ArchiveID == "" ||
		obs.State == v1alpha1.PITRArchiveRefused || obs.EncryptionMode != enc.Mode || obs.KeyRevision != rev {
		return "", false
	}
	if !pitrarchive.IsDerivedArchiveID(obs.ArchiveID, p.fg.UID, dest) {
		return "", false
	}
	return obs.ArchiveID, true
}

// correlatedRefusal is a sidecar refusal of exactly enc at the destination
// (§4.4). ok is false when no correlated report refuses; mapped is false
// for refusals that do not refuse the identity (they leave the entry
// Pending).
func (p *pitrBindingPass) correlatedRefusal(dest string, enc v1alpha1.PITRArchiveBindingEncryption) (reason v1alpha1.PITRArchiveBindingReason, raw v1alpha1.PITRArchiveRefusalReason, mapped, ok bool) {
	obs := p.observation()
	rev := pitrKeyRevision(enc)
	if obs == nil || rev == "" || obs.Destination != dest || obs.KeyRevision != rev ||
		obs.State != v1alpha1.PITRArchiveRefused || obs.Reason == "" {
		return "", "", false, false
	}
	reason, mapped = pitrBindingReasonForRefusal(obs.Reason)
	return reason, obs.Reason, mapped, true
}

// correlatedReport reports whether the sidecars report on exactly enc at
// the destination (any state).
func (p *pitrBindingPass) correlatedReport(dest string, enc v1alpha1.PITRArchiveBindingEncryption) bool {
	obs := p.observation()
	rev := pitrKeyRevision(enc)
	return obs != nil && rev != "" && obs.Destination == dest && obs.KeyRevision == rev
}

// acceptedConflict is the K8 check: an accepted record for the destination
// names another archive ID.
func (p *pitrBindingPass) acceptedConflict(dest, archiveID string) string {
	for i := range p.snaps {
		a, err := verifyPITRAcceptance(&p.snaps[i])
		if err == nil && a.Destination == dest && a.ArchiveID != archiveID {
			return fmt.Sprintf("key snapshot %s records the acceptance of archive %s at this destination, not %s", p.snaps[i].Name, a.ArchiveID, archiveID)
		}
	}
	return ""
}

// bind makes entry Bound to archiveID created by source (proof or check).
func (p *pitrBindingPass) bind(entry *v1alpha1.PITRArchiveBinding, archiveID string, source v1alpha1.PITRArchiveSource, cause string) {
	if conflict := p.acceptedConflict(entry.Destination, archiveID); conflict != "" {
		p.setState(entry, v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveForeign, conflict, cause)
		entry.Check = nil
		return
	}
	entry.ArchiveID = archiveID
	src := source
	entry.Source = &src
	entry.Check = nil
	p.setState(entry, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound, "", cause)
	p.newlyBound[entry.ID] = true
}

func (p *pitrBindingPass) ownSource() v1alpha1.PITRArchiveSource {
	return v1alpha1.PITRArchiveSource{Namespace: p.fg.Namespace, Name: p.fg.Name, UID: p.fg.UID}
}

// consumeCheck schedules a check Job for deletion after the status write.
func (p *pitrBindingPass) consumeCheck(ctx context.Context, check *v1alpha1.PITRArchiveBindingCheck) {
	var job batchv1.Job
	if err := p.r.bindingReader().Get(ctx, client.ObjectKey{Namespace: p.fg.Namespace, Name: check.JobName}, &job); err == nil {
		if check.JobUID == "" || job.UID == check.JobUID {
			p.consumed = append(p.consumed, &job)
		}
	}
}

// writerRuns reports whether a writer runs (§4.6): a site Deployment has
// no decodable render (a pre-#203 writer, which may hold any key), or the
// hold is an enabled render.
func (p *pitrBindingPass) writerRuns() bool {
	return len(p.unannotated) > 0 || (p.holdK == pitrHeld && p.hold.Enabled)
}

// holdRenders reports whether the hold is an enabled render of exactly enc
// at dest: the running writer then proves (or refuses) enc by itself.
func (p *pitrBindingPass) holdRenders(dest pitrarchive.Destination, enc v1alpha1.PITRArchiveBindingEncryption) bool {
	return p.holdK == pitrHeld && p.hold.Enabled && p.hold.Destination == dest.String() &&
		p.hold.EncryptionMode == string(enc.Mode) && p.hold.Algorithm == string(enc.Algorithm) &&
		p.hold.KeySecretName == enc.KeySecretName
}

// advance moves t's entry one step (§4.4). Bound entries are not touched
// here (evaluation reports drift on the condition).
func (p *pitrBindingPass) advance(ctx context.Context, t *pitrBindingTarget) error {
	dest := t.dest.String()
	entry := p.find(dest)
	if entry != nil && entry.State == v1alpha1.PITRArchiveBindingBound {
		return nil
	}
	cands, live, missing, err := p.candidates(ctx, t)
	if err != nil {
		return err
	}
	ident := identityOf(t.profile)
	preferred := live
	if preferred == nil && len(cands) > 0 {
		preferred = &cands[0]
	}
	if entry == nil {
		if p.retainedCount() >= pitrArchiveBindingsMax {
			p.limit = append(p.limit, dest)
			return nil
		}
		enc := ident
		if preferred != nil {
			enc = preferred.enc
		}
		p.entries = append(p.entries, v1alpha1.PITRArchiveBinding{ID: pitrBindingID(t.dest), Destination: dest, Encryption: enc})
		entry = &p.entries[len(p.entries)-1]
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, "", pitrBindingCauseInput)
	}

	// Consumer proof (write targets) wins over everything, also over a
	// check in flight.
	if t.write {
		if id, ok := p.proven(t.dest, entry.Encryption); ok {
			if entry.Check != nil {
				p.consumeCheck(ctx, entry.Check)
			}
			p.bind(entry, id, p.ownSource(), pitrBindingCauseProof)
			return nil
		}
	}

	// A candidate under check, or one the writer is establishing, that
	// the identity profile no longer offers (its mode or algorithm
	// changed, or its snapshot is gone) is superseded (G3): its verdict
	// would describe an identity the group no longer renders, so the
	// check is dropped and the entry starts over with the current
	// candidate. The renderer holds the previous writer render meanwhile.
	if entry.State == v1alpha1.PITRArchiveBindingPending && (entry.Check != nil || entry.Reason == v1alpha1.PITRArchiveBindingReasonEstablishing) &&
		!pitrCandidateOffered(entry.Encryption, ident, cands) {
		old := pitrCandidateLabel(entry.Encryption)
		if entry.Check != nil {
			p.consumeCheck(ctx, entry.Check)
			entry.Check = nil
		}
		entry.Encryption = ident
		if preferred != nil {
			entry.Encryption = preferred.enc
		}
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating,
			fmt.Sprintf("%s was superseded by a change of profile %s's encryption; authenticating %s", old, t.profile.Name, pitrCandidateLabel(entry.Encryption)),
			pitrBindingCauseInput)
	}

	// An Establishing entry waits for its writer's proof; once the
	// destination is no longer written (the profile switched away while a
	// reader still uses it), nothing will prove it, so it is authenticated
	// as a read (U04).
	if !t.write && entry.State == v1alpha1.PITRArchiveBindingPending && entry.Reason == v1alpha1.PITRArchiveBindingReasonEstablishing {
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating,
			"the destination is no longer written; authenticating it for its readers", pitrBindingCauseInput)
	}

	// A check in flight: read its verdict.
	if entry.Check != nil {
		return p.observeCheck(ctx, t, entry, cands, live)
	}

	// A Refused entry is retried only on new input (Δ21); otherwise a
	// sidecar refusal follows the current report's reason.
	if entry.State == v1alpha1.PITRArchiveBindingRefused {
		if !p.retry(t, entry, live, ident) {
			if t.write {
				if reason, raw, mapped, ok := p.correlatedRefusal(dest, entry.Encryption); ok && mapped && reason != entry.Reason {
					p.setState(entry, v1alpha1.PITRArchiveBindingRefused, reason, p.refusalMessage(t, reason, string(raw)+pitrSidecarRefusalMarker), pitrBindingCauseProof)
				}
			}
			return nil
		}
		enc := ident
		if preferred != nil {
			enc = preferred.enc
		}
		entry.Encryption = enc
		entry.Source = nil
		entry.ArchiveID = ""
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, "retrying after an input change", pitrBindingCauseInput)
	}

	// Correlated sidecar refusal (write targets, non-Bound only).
	if t.write {
		if reason, raw, mapped, ok := p.correlatedRefusal(dest, entry.Encryption); ok {
			if mapped {
				p.setState(entry, v1alpha1.PITRArchiveBindingRefused, reason, p.refusalMessage(t, reason, string(raw)+pitrSidecarRefusalMarker), pitrBindingCauseProof)
				return nil
			}
			p.setState(entry, v1alpha1.PITRArchiveBindingPending, entry.Reason, fmt.Sprintf("the sidecars report %s; see the PITRArchiveReady condition", raw), pitrBindingCauseProof)
			return nil
		}
	}

	// Pending/Establishing: the check said Absent; the renderer renders the
	// candidate and the writer's proof binds it (row 4b).
	if entry.Reason == v1alpha1.PITRArchiveBindingReasonEstablishing {
		return nil
	}

	if preferred == nil {
		entry.Encryption = ident
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, missing, pitrBindingCauseInput)
		return nil
	}

	// Write target without a check: no writer runs (row 4a, optimistic),
	// or the running writer renders exactly the preferred candidate. The
	// sidecar verifies (or creates on an empty prefix) and proves it.
	if t.write && (!p.writerRuns() || p.holdRenders(t.dest, preferred.enc)) {
		if !sameIdentity(entry.Encryption, preferred.enc) {
			entry.Encryption = preferred.enc
			p.causes[entry.ID] = pitrBindingCauseInput
		}
		if entry.Message != "" && entry.Reason == v1alpha1.PITRArchiveBindingReasonAuthenticating && entry.State == v1alpha1.PITRArchiveBindingPending {
			entry.Message = ""
		}
		return nil
	}

	// C1 (read target) or C2 (write target while another identity or a
	// pre-#203 writer runs): authenticate with a check Job.
	if wait := pitrCheckRetryWait(p.fg, entry.ID, p.now); wait > 0 {
		p.requeueAfter(wait)
		return nil
	}
	// A round of checks walks the candidates in order; it continues with
	// entry.Encryption only when the previous pass had to wait for an old
	// Job of that name to go, else it starts over.
	cand := cands[0]
	if entry.Message == pitrCheckReplacingMessage {
		for _, c := range cands {
			if sameIdentity(c.enc, entry.Encryption) {
				cand = c
				break
			}
		}
	}
	return p.startCheck(ctx, t, entry, cand)
}

// pitrCheckReplacingMessage marks an entry whose next check waits for an
// old Job of the same name to be removed (the round continues).
const pitrCheckReplacingMessage = "waiting for the previous check Job of this candidate to be removed"

// pitrSidecarRefusalMarker ends the detail of a refusal the sidecars
// reported (as opposed to a check Job); such entries are re-evaluated
// against the current report every pass (Δ21).
const pitrSidecarRefusalMarker = " reported by the group's sidecars"

// The role markers of a check refusal's message: which role's check
// reported it (a role change re-checks, U04).
const (
	pitrCheckRoleMarkerWriter = "writer check"
	pitrCheckRoleMarkerReader = "reader check"
)

func pitrCheckRoleMarker(t *pitrBindingTarget) string {
	if t.write {
		return pitrCheckRoleMarkerWriter
	}
	return pitrCheckRoleMarkerReader
}

// pitrCandidateOffered reports whether enc is still one of the identity
// profile's candidates: the profile's mode and algorithm, and for
// Passphrase a listed or live snapshot of the destination.
func pitrCandidateOffered(enc, ident v1alpha1.PITRArchiveBindingEncryption, cands []pitrCandidate) bool {
	if enc.Mode != ident.Mode || enc.Algorithm != ident.Algorithm {
		return false
	}
	for _, c := range cands {
		if sameIdentity(c.enc, enc) {
			return true
		}
	}
	return false
}

// startCheck launches the check Job for cand. A leftover Job of the same
// name is deleted after the status write; the next pass creates the Job.
func (p *pitrBindingPass) startCheck(ctx context.Context, t *pitrBindingTarget, entry *v1alpha1.PITRArchiveBinding, cand pitrCandidate) error {
	entry.Encryption = cand.enc
	job, leftover, err := p.r.ensurePITRArchiveCheck(ctx, p.fg, entry, t, cand.enc.KeySecretName)
	if leftover != nil {
		p.consumed = append(p.consumed, leftover)
	}
	if err != nil {
		if errors.Is(err, errOperatorImageUnset) {
			p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, "check Job could not start: "+err.Error(), pitrBindingCauseCheck)
			p.requeueAfter(pitrCheckRetryAfter)
			return nil
		}
		return err
	}
	msg := pitrCheckReplacingMessage
	if job != nil {
		msg = fmt.Sprintf("check Job %s is authenticating %s", job.Name, pitrCandidateLabel(cand.enc))
		p.requeueAfter(pitrCheckPollInterval)
	} else {
		p.requeueAfter(2 * time.Second)
	}
	p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, msg, pitrBindingCauseCheck)
	return nil
}

func pitrCandidateLabel(enc v1alpha1.PITRArchiveBindingEncryption) string {
	if enc.Mode == v1alpha1.EncryptionModeNone {
		return "the plaintext identity"
	}
	return "key snapshot " + enc.KeySecretName
}

// observeCheck applies a finished check (§4.4 "Reading the result").
func (p *pitrBindingPass) observeCheck(ctx context.Context, t *pitrBindingTarget, entry *v1alpha1.PITRArchiveBinding, cands []pitrCandidate, live *pitrCandidate) error {
	out, err := p.r.observePITRArchiveCheck(ctx, p.fg, entry, p.now)
	if err != nil {
		return err
	}
	// A check runs with the role of its target (writer: own UID, an empty
	// prefix is Absent; reader: any source, an empty prefix is
	// ArchiveNotFound). A Job started for the other role answers another
	// question: it is dropped and the target checked again (U04).
	if out.job != nil {
		if role := pitrCheckJobRole(out.job); role != pitrCheckRole(t) {
			p.consumed = append(p.consumed, out.job)
			entry.Check = nil
			p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating,
				fmt.Sprintf("check Job %s ran as %s, but the destination is now checked as %s; checking again", out.job.Name, role, pitrCheckRole(t)), pitrBindingCauseInput)
			p.requeueAfter(2 * time.Second)
			return nil
		}
	}
	if !out.done {
		p.requeueAfter(pitrCheckPollInterval)
		return nil
	}
	if out.job != nil {
		p.consumed = append(p.consumed, out.job)
	}
	entry.Check = nil
	if out.failure != "" {
		pitrCheckRetryGate.Store(pitrCheckRetryKey(p.fg, entry.ID), p.now.Add(pitrCheckRetryAfter))
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, out.failure+"; retrying in "+pitrCheckRetryAfter.String(), pitrBindingCauseCheck)
		p.requeueAfter(pitrCheckRetryAfter)
		return nil
	}
	res := out.result
	// The verdict names its destination by digest (G5); the destination
	// itself comes from the Job's immutable environment, which must be
	// the entry's.
	jobDest, derr := pitrCheckJobDestination(out.job)
	if derr != nil || jobDest != entry.Destination || res.DestinationDigest != PITRArchiveCheckDestinationDigest(jobDest) {
		detail := fmt.Sprintf("check Job probed %s, not %s", jobDest, entry.Destination)
		switch {
		case derr != nil:
			detail = "check Job's destination is unreadable: " + derr.Error()
		case jobDest == entry.Destination:
			detail = "check Job's verdict names another destination than the one it was given"
		}
		pitrCheckRetryGate.Store(pitrCheckRetryKey(p.fg, entry.ID), p.now.Add(pitrCheckRetryAfter))
		p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, detail+"; retrying", pitrBindingCauseCheck)
		p.requeueAfter(pitrCheckRetryAfter)
		return nil
	}
	switch res.State {
	case PITRArchiveCheckStateVerified:
		p.bind(entry, res.ArchiveID, res.Source, pitrBindingCauseCheck)
		if uid := types.UID(out.job.Annotations[v1alpha1.PITRKeySecretUIDAnnotation]); uid != "" && entry.State == v1alpha1.PITRArchiveBindingBound {
			p.checkedSnapshot[entry.ID] = uid
		}
	case PITRArchiveCheckStateAbsent:
		if t.write {
			p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonEstablishing,
				fmt.Sprintf("the prefix is empty; the group's writer creates the archive with %s", pitrCandidateLabel(entry.Encryption)), pitrBindingCauseCheck)
			return nil
		}
		p.refuseByCheck(t, entry, live, v1alpha1.PITRArchiveBindingReasonArchiveNotFound, "the prefix holds no archive, reported by the "+pitrCheckRoleMarker(t))
	case PITRArchiveCheckStateRefused:
		reason, ok := pitrBindingReasonForToken(res.Reason)
		if !ok {
			pitrCheckRetryGate.Store(pitrCheckRetryKey(p.fg, entry.ID), p.now.Add(pitrCheckRetryAfter))
			p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating,
				fmt.Sprintf("check Job reported %s; retrying in %s", res.Reason, pitrCheckRetryAfter), pitrBindingCauseCheck)
			p.requeueAfter(pitrCheckRetryAfter)
			return nil
		}
		// A listing refused for lack of permission is fixed by an RBAC
		// or bucket-policy edit, which no input this engine watches
		// reflects: it is retried like a check that could not start,
		// never recorded as a sticky refusal (U02).
		if reason == v1alpha1.PITRArchiveBindingReasonArchiveListFailed {
			pitrCheckRetryGate.Store(pitrCheckRetryKey(p.fg, entry.ID), p.now.Add(pitrCheckRetryAfter))
			p.setState(entry, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating,
				p.refusalMessage(t, reason, fmt.Sprintf("the check of %s could not list the prefix", pitrCandidateLabel(entry.Encryption)))+"; retrying in "+pitrCheckRetryAfter.String(),
				pitrBindingCauseCheck)
			p.requeueAfter(pitrCheckRetryAfter)
			return nil
		}
		if reason == v1alpha1.PITRArchiveBindingReasonKeyMismatch {
			for i, c := range cands {
				if sameIdentity(c.enc, entry.Encryption) && i+1 < len(cands) {
					return p.startCheck(ctx, t, entry, cands[i+1])
				}
			}
		}
		if res.Source.UID != "" {
			src := res.Source
			entry.Source = &src
		}
		if res.ArchiveID != "" {
			entry.ArchiveID = res.ArchiveID
		}
		p.refuseByCheck(t, entry, live, reason, fmt.Sprintf("%s reported by the %s of %s", res.Reason, pitrCheckRoleMarker(t), pitrCandidateLabel(entry.Encryption)))
	}
	return nil
}

// refuseByCheck records a check refusal. The entry keeps the identity
// profile's live candidate (when there is one), not the last candidate
// tried: a check-refused entry is retried only when that live candidate or
// the encryption identity changes (Δ21), so recording a listed candidate
// would retry it on every pass.
func (p *pitrBindingPass) refuseByCheck(t *pitrBindingTarget, entry *v1alpha1.PITRArchiveBinding, live *pitrCandidate, reason v1alpha1.PITRArchiveBindingReason, detail string) {
	msg := p.refusalMessage(t, reason, detail)
	if live != nil {
		entry.Encryption = live.enc
	}
	p.setState(entry, v1alpha1.PITRArchiveBindingRefused, reason, msg, pitrBindingCauseCheck)
}

// retry decides whether a Refused entry is retried (Δ21, no timer): its
// identity profile's live candidate or encryption identity differs from
// the entry's, or the sidecars' correlated report no longer refuses.
func (p *pitrBindingPass) retry(t *pitrBindingTarget, entry *v1alpha1.PITRArchiveBinding, live *pitrCandidate, ident v1alpha1.PITRArchiveBindingEncryption) bool {
	if ident.Mode != entry.Encryption.Mode || ident.Algorithm != entry.Encryption.Algorithm {
		return true
	}
	// A refusal of the other role (U04): a writer's verdict (its own
	// check, or a sidecar's) says nothing about reading another group's
	// archive, and a reader's says nothing about creating one on an empty
	// prefix. Each role is checked at most once per refusal: the new
	// verdict carries the current role's marker.
	if t.write && strings.Contains(entry.Message, pitrCheckRoleMarkerReader) {
		return true
	}
	if !t.write && (strings.Contains(entry.Message, pitrCheckRoleMarkerWriter) || strings.Contains(entry.Message, pitrSidecarRefusalMarker)) {
		return true
	}
	// A stored ArchiveListFailed (it is no longer recorded as a refusal,
	// U02) is retried through the check retry gate.
	if entry.Reason == v1alpha1.PITRArchiveBindingReasonArchiveListFailed {
		return true
	}
	if live != nil && live.enc.KeySecretName != entry.Encryption.KeySecretName {
		return true
	}
	if t.write && strings.Contains(entry.Message, pitrSidecarRefusalMarker) && p.correlatedReport(entry.Destination, entry.Encryption) {
		if _, _, _, refusing := p.correlatedRefusal(entry.Destination, entry.Encryption); !refusing {
			return true
		}
	}
	return false
}

// refusalMessage explains a refused entry and its way out.
func (p *pitrBindingPass) refusalMessage(t *pitrBindingTarget, reason v1alpha1.PITRArchiveBindingReason, detail string) string {
	msg := fmt.Sprintf("%s: %s at %s", reason, detail, t.dest)
	switch reason {
	case v1alpha1.PITRArchiveBindingReasonKeyMismatch:
		if t.profile.EncryptionEnabled() {
			ref := t.profile.Encryption.PassphraseSecretKeyRef
			msg += fmt.Sprintf("; no candidate key opens the archive: recover the archive's original passphrase into Secret %s key %s (profile %s), or point the profile at a new prefix",
				ref.Name, ref.PassphraseSecretKeyOrDefault(), t.profile.Name)
		}
	case v1alpha1.PITRArchiveBindingReasonArchiveEncryptionMismatch:
		msg += fmt.Sprintf("; profile %s's encryption does not match the archive's: restore the profile's original encryption settings or use a new prefix", t.profile.Name)
	case v1alpha1.PITRArchiveBindingReasonArchiveForeign, v1alpha1.PITRArchiveBindingReasonArchiveCopy, v1alpha1.PITRArchiveBindingReasonArchiveLegacy:
		msg += "; point the profile at a new prefix and take a full backup"
	case v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing:
		msg += "; restore the descriptor, or point the profile at a new prefix and take a full backup"
	case v1alpha1.PITRArchiveBindingReasonArchiveNotFound:
		msg += "; check the archive profile, or wait for the first archived binlog"
	case v1alpha1.PITRArchiveBindingReasonArchiveListFailed:
		msg += "; grant list permission on the prefix (s3:ListBucket) or make the volume readable"
	case v1alpha1.PITRArchiveBindingReasonArchiveVersionUnsupported:
		msg += "; upgrade bloodraven, or repair archive.json"
	}
	if t.write && len(p.unannotated) > 0 {
		msg += "; " + p.deferralNote()
	}
	return msg
}

// deferralNote names the Deployments a pre-#203 render keeps deferred (K3)
// and the ways out.
func (p *pitrBindingPass) deferralNote() string {
	return fmt.Sprintf("Deployments %s are not updated until the archive accepts a key: fix the passphrase Secret, switch to a new prefix, or disable PITR for this profile", strings.Join(p.unannotated, ", "))
}

// pitrRefusal is one drift finding on a Bound entry (§4.5).
type pitrRefusal struct {
	reason string
	text   string
}

// evaluate compares every Bound target entry with the profiles in archive
// use (§4.5) and checks writer eligibility (K6).
func (p *pitrBindingPass) evaluate(ctx context.Context) (eligibility []pitrRefusal, refusals []pitrRefusal, err error) {
	for i := range p.targets {
		t := &p.targets[i]
		entry := p.find(t.dest.String())
		if entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound {
			continue
		}
		if t.write {
			if ok, reason := pitrWriterEligible(p.fg, entry); !ok {
				eligibility = append(eligibility, pitrRefusal{reason: string(reason), text: fmt.Sprintf(
					"%s: binding %s for %s is archive %s created by %s; this group never writes it, so the previous writer render is kept; point the active profile %s at a new prefix",
					reason, entry.ID, entry.Destination, entry.ArchiveID, pitrSourceLabel(entry.Source), t.profile.Name)})
			}
		}
		aliases := pitrProfileNames(p.profilesAt(entry.Destination))
		for _, prof := range t.uses {
			got := identityOf(prof)
			if got.Mode != entry.Encryption.Mode || got.Algorithm != entry.Encryption.Algorithm {
				refusals = append(refusals, pitrRefusal{reason: v1alpha1.ReasonEncryptionChangeRefused, text: fmt.Sprintf(
					"profile %s configures %s, but archive %s at %s was accepted as %s (profiles resolving there: %s)",
					prof.Name, pitrIdentityLabel(got), entry.ArchiveID, entry.Destination, pitrIdentityLabel(entry.Encryption), aliases)})
				continue
			}
			// With the key material lost the accepted bytes are unknown, so
			// a key change cannot be judged (row 18 reports the loss).
			if got.Mode != v1alpha1.EncryptionModePassphrase || entry.Reason == v1alpha1.PITRArchiveBindingReasonKeyMaterialLost {
				continue
			}
			live, err := p.r.readPITRLiveKey(ctx, p.fg, prof)
			if err != nil {
				return nil, nil, err
			}
			if live.missing != "" {
				p.keySourceRemoved = append(p.keySourceRemoved, fmt.Sprintf("%s (binding %s keeps using key snapshot %s)", live.missing, entry.ID, entry.Encryption.KeySecretName))
				continue
			}
			changed := false
			if snap := p.byName[entry.Encryption.KeySecretName]; snap != nil && len(snap.Data[pitrKeySnapshotDataKey]) > 0 {
				changed = !bytes.Equal(snap.Data[pitrKeySnapshotDataKey], live.trimmed)
			} else {
				changed = pitrKeySnapshotName(p.fg.Name, p.digestKey, entry.Destination, live.trimmed) != entry.Encryption.KeySecretName
			}
			if changed {
				refusals = append(refusals, pitrRefusal{reason: v1alpha1.ReasonKeyChangeRefused, text: fmt.Sprintf(
					"profile %s's passphrase (Secret %s key %s) is not the key accepted for archive %s at %s, kept in key snapshot %s (profiles resolving there: %s)",
					prof.Name, live.secret, live.key, entry.ArchiveID, entry.Destination, entry.Encryption.KeySecretName, aliases)})
			}
		}
	}
	return eligibility, refusals, nil
}

func pitrSourceLabel(s *v1alpha1.PITRArchiveSource) string {
	if s == nil {
		return "an unknown group"
	}
	return fmt.Sprintf("group %s/%s (uid %s)", s.Namespace, s.Name, s.UID)
}

func pitrIdentityLabel(e v1alpha1.PITRArchiveBindingEncryption) string {
	if e.Algorithm == "" {
		return string(e.Mode)
	}
	return string(e.Mode) + "/" + string(e.Algorithm)
}

// condition computes PITRArchiveBindingReady (§4.10): nil (removed) with
// no targets and no entries; otherwise the first of BindingLimitReached, a
// refused in-use entry or a writer-eligibility refusal,
// EncryptionChangeRefused, KeyChangeRefused, KeyMaterialLost,
// Authenticating/Establishing, and Bound (with the baseline note).
//
// An error (an input of the drift evaluation could not be read) yields no
// condition: the pass writes nothing, so the stored condition is never
// replaced by one computed without the evaluation (U23).
func (p *pitrBindingPass) condition(ctx context.Context) (*metav1.Condition, error) {
	if len(p.targets) == 0 && len(p.entries) == 0 && len(p.overflow) == 0 && len(p.notes) == 0 {
		return nil, nil
	}
	eligibility, refusals, err := p.evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluate pitr archive bindings: %w", err)
	}
	cond := &metav1.Condition{Type: v1alpha1.ConditionPITRArchiveBindingReady, ObservedGeneration: p.fg.Generation}
	set := func(status metav1.ConditionStatus, reason, msg string) {
		cond.Status, cond.Reason, cond.Message = status, reason, msg
	}
	var (
		refused *pitrRefusal
		pending []string
		lost    []string
		pReason string
	)
	for i := range p.entries {
		e := &p.entries[i]
		switch {
		case e.State == v1alpha1.PITRArchiveBindingRefused && refused == nil:
			refused = &pitrRefusal{reason: string(e.Reason), text: fmt.Sprintf("binding %s: %s", e.ID, e.Message)}
		case e.State == v1alpha1.PITRArchiveBindingPending:
			if pReason == "" || e.Reason == v1alpha1.PITRArchiveBindingReasonAuthenticating {
				pReason = string(e.Reason)
			}
			msg := e.Message
			if msg == "" {
				msg = "waiting for the group's sidecars to verify " + pitrCandidateLabel(e.Encryption)
			}
			pending = append(pending, fmt.Sprintf("binding %s (%s) is %s: %s", e.ID, e.Destination, e.Reason, msg))
		case e.State == v1alpha1.PITRArchiveBindingBound && e.Reason == v1alpha1.PITRArchiveBindingReasonKeyMaterialLost:
			lost = append(lost, fmt.Sprintf("binding %s: %s", e.ID, e.Message))
		}
	}
	byReason := func(reason string) []string {
		var out []string
		for _, r := range refusals {
			if r.reason == reason {
				out = append(out, r.text)
			}
		}
		return out
	}
	switch {
	case len(p.limit) > 0 || len(p.overflow) > 0:
		var parts []string
		if len(p.limit) > 0 {
			parts = append(parts, fmt.Sprintf("no binding slot for %s (at most %d bindings)", strings.Join(p.limit, ", "), pitrArchiveBindingsMax))
		}
		if len(p.overflow) > 0 {
			parts = append(parts, fmt.Sprintf("accepted bindings %s could not be rebuilt (at most %d bindings)", strings.Join(p.overflow, ", "), pitrArchiveBindingsMax))
		}
		set(metav1.ConditionFalse, v1alpha1.ReasonBindingLimitReached, strings.Join(parts, "; ")+"; release bindings that are no longer needed (bloodraven.shipstream.io/release-pitr-archive-binding=<id>:confirm=<group>)")
	case refused != nil:
		set(metav1.ConditionFalse, refused.reason, refused.text)
	case len(eligibility) > 0:
		set(metav1.ConditionFalse, eligibility[0].reason, eligibility[0].text)
	case len(byReason(v1alpha1.ReasonEncryptionChangeRefused)) > 0:
		set(metav1.ConditionFalse, v1alpha1.ReasonEncryptionChangeRefused, pitrJoinRefusals(append(byReason(v1alpha1.ReasonEncryptionChangeRefused), byReason(v1alpha1.ReasonKeyChangeRefused)...)))
	case len(byReason(v1alpha1.ReasonKeyChangeRefused)) > 0:
		set(metav1.ConditionFalse, v1alpha1.ReasonKeyChangeRefused, pitrJoinRefusals(byReason(v1alpha1.ReasonKeyChangeRefused)))
	case len(lost) > 0:
		set(metav1.ConditionFalse, string(v1alpha1.PITRArchiveBindingReasonKeyMaterialLost), strings.Join(lost, "; "))
	case len(pending) > 0:
		set(metav1.ConditionUnknown, pReason, strings.Join(pending, "; "))
	default:
		set(metav1.ConditionTrue, string(v1alpha1.PITRArchiveBindingReasonBound), p.boundMessage(ctx))
	}
	var notes []string
	for _, k := range p.keySourceRemoved {
		notes = append(notes, "key source removed: "+k)
	}
	for _, n := range p.notes {
		notes = append(notes, fmt.Sprintf("key snapshot %s is unreadable: %s (never deleted by the operator)", n.secret, n.detail))
	}
	if len(notes) > 0 {
		cond.Message += "; " + strings.Join(notes, "; ")
	}
	cond.Message = truncateConditionMessage(cond.Message)
	return cond, nil
}

func truncateConditionMessage(s string) string {
	const limit = 32 * 1024
	if len(s) <= limit {
		return s
	}
	return s[:limit-3] + "..."
}

// pitrJoinRefusals joins at most pitrBindingMessageMaxRefusals refusals and
// appends the procedure once.
func pitrJoinRefusals(texts []string) string {
	more := 0
	if len(texts) > pitrBindingMessageMaxRefusals {
		more = len(texts) - pitrBindingMessageMaxRefusals
		texts = texts[:pitrBindingMessageMaxRefusals]
	}
	msg := strings.Join(texts, "; ")
	if more > 0 {
		msg += fmt.Sprintf("; and %d more", more)
	}
	return msg + "; " + pitrBindingProcedure
}

// boundMessage is the True condition's message, with the baseline note
// (O18) while no Succeeded backup of the group is stamped with the write
// binding's archive.
func (p *pitrBindingPass) boundMessage(ctx context.Context) string {
	msg := fmt.Sprintf("%d PITR archive binding(s) Bound", len(p.entries))
	for i := range p.targets {
		t := &p.targets[i]
		if !t.write {
			continue
		}
		entry := p.find(t.dest.String())
		if entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound || entry.ArchiveID == "" {
			continue
		}
		var backups v1alpha1.MysqlBackupList
		if err := p.r.List(ctx, &backups, client.InNamespace(p.fg.Namespace)); err != nil {
			log.FromContext(ctx).Error(err, "list backups for the pitr baseline note", "fg", p.fg.Name)
			return msg
		}
		for _, b := range backups.Items {
			if b.Spec.FailoverGroupRef.Name == p.fg.Name && b.Status.Phase == v1alpha1.BackupPhaseSucceeded && b.Status.PITRArchiveID == entry.ArchiveID {
				return msg
			}
		}
		return msg + fmt.Sprintf("; no full backup is stamped with archive %s yet; take one (PITR restores need a baseline)", entry.ArchiveID)
	}
	return msg
}

// errPITRBindingStatusMoved means the stored bindings (or their condition)
// are not the ones the binding pass started from: the pass read a cache
// older than a write of a previous pass (F-A). Its decisions are stale, so
// nothing is written and nothing renders; Reconcile requeues at once
// (pitrBindingMovedRequeue) and the next pass starts from the stored state.
var errPITRBindingStatusMoved = errors.New("pitr archive binding status changed since this reconcile read it")

// pitrBindingMovedRequeue is how soon a pass whose base moved retries.
const pitrBindingMovedRequeue = time.Second

// pitrBindingCondEqual compares the fields of the binding condition a pass
// writes.
func pitrBindingCondEqual(a, b *metav1.Condition) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Status == b.Status && a.Reason == b.Reason && a.Message == b.Message && a.ObservedGeneration == b.ObservedGeneration
}

// writePITRArchiveBindings writes status.pitrArchiveBindings and the
// condition on a fresh copy (RetryOnConflict), and nothing else. The fresh
// copy must still hold base and baseCond, what the pass started from (the
// cached group, or the status a release wrote this pass); otherwise it
// returns errPITRBindingStatusMoved without writing (F-A: a pass computed
// from a cache that predates a previous pass's write must not replace a
// fresher Bound entry). It skips the write when nothing changed. fg's
// in-memory status is updated for the rendering that follows.
func (r *MysqlFailoverGroupReconciler) writePITRArchiveBindings(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, base []v1alpha1.PITRArchiveBinding, baseCond *metav1.Condition, entries []v1alpha1.PITRArchiveBinding, cond *metav1.Condition) error {
	apply := func(st *v1alpha1.MysqlFailoverGroupStatus) bool {
		changed := false
		if len(entries) == 0 && len(st.PITRArchiveBindings) != 0 || len(entries) != 0 && !equality.Semantic.DeepEqual(st.PITRArchiveBindings, entries) {
			if len(entries) == 0 {
				st.PITRArchiveBindings = nil
			} else {
				st.PITRArchiveBindings = append([]v1alpha1.PITRArchiveBinding(nil), entries...)
			}
			changed = true
		}
		if cond == nil {
			if meta.FindStatusCondition(st.Conditions, v1alpha1.ConditionPITRArchiveBindingReady) != nil {
				meta.RemoveStatusCondition(&st.Conditions, v1alpha1.ConditionPITRArchiveBindingReady)
				changed = true
			}
			return changed
		}
		if !pitrBindingCondEqual(meta.FindStatusCondition(st.Conditions, v1alpha1.ConditionPITRArchiveBindingReady), cond) {
			meta.SetStatusCondition(&st.Conditions, *cond)
			changed = true
		}
		return changed
	}
	// The base is checked even when nothing changes: rendering from a
	// stale in-memory status is as wrong as writing it.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.bindingReader().Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
			return err
		}
		if fresh.UID != fg.UID {
			return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("mysqlfailovergroups").GroupResource(), fg.Name)
		}
		if !pitrBindingsEqual(fresh.Status.PITRArchiveBindings, base) ||
			!pitrBindingCondEqual(meta.FindStatusCondition(fresh.Status.Conditions, v1alpha1.ConditionPITRArchiveBindingReady), baseCond) {
			return errPITRBindingStatusMoved
		}
		if !apply(&fresh.Status) {
			return nil
		}
		return r.Status().Update(ctx, &fresh)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		if errors.Is(err, errPITRBindingStatusMoved) {
			return err
		}
		return fmt.Errorf("write pitr archive bindings: %w", err)
	}
	apply(&fg.Status)
	return nil
}

// finishPITRRelease removes the release annotation (after the status
// write) and reports the outcome.
func (r *MysqlFailoverGroupReconciler) finishPITRRelease(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, out *pitrReleaseOutcome) {
	logger := log.FromContext(ctx)
	switch {
	case out.rejected != "":
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveBindingReleaseRejected,
			"release annotation %s rejected and removed: %s", v1alpha1.ReleasePITRArchiveBindingAnnotation, out.rejected)
		if err := r.removePITRReleaseAnnotation(ctx, fg, out.value); err != nil {
			logger.Error(err, "remove rejected pitr release annotation", "fg", fg.Name)
		}
	case out.refused != "":
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveBindingReleaseRefused,
			"release of binding %s refused, retrying: %s", out.id, out.refused)
	case out.conflict:
		// A snapshot changed between the in-use read and the delete (an
		// MBV hold landed): re-evaluated next pass.
	default:
		if err := r.removePITRReleaseAnnotation(ctx, fg, out.value); err != nil {
			logger.Error(err, "remove pitr release annotation", "fg", fg.Name)
			return
		}
		logger.Info(logMsgPITRBindingReleased, "fg", fg.Name, "bindingID", out.id, "destination", out.destination)
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventPITRArchiveBindingReleased,
			"binding %s (%s) released; its key snapshots were deleted", out.id, out.destination)
	}
}

// emit logs entry changes and emits the edge-triggered Events (§4.10).
func (p *pitrBindingPass) emit(ctx context.Context, before []v1alpha1.PITRArchiveBinding, beforeCond, cond *metav1.Condition) {
	logger := log.FromContext(ctx)
	rec := p.r.Recorder
	fg := p.fg
	for i := range p.entries {
		e := &p.entries[i]
		prev := findBindingIn(before, e.ID)
		if prev != nil && prev.State == e.State && prev.Reason == e.Reason && prev.ArchiveID == e.ArchiveID &&
			prev.Encryption == e.Encryption && reflect.DeepEqual(prev.Check, e.Check) && p.causes[e.ID] == "" {
			continue
		}
		// Every transition records its cause; an unrecorded one can
		// only be an input change (never a check that did not run).
		cause := p.causes[e.ID]
		if cause == "" {
			cause = pitrBindingCauseInput
		}
		if prev == nil || prev.State != e.State || prev.Reason != e.Reason || prev.ArchiveID != e.ArchiveID || prev.Encryption.KeySecretName != e.Encryption.KeySecretName {
			logger.Info(logMsgPITRBindingChanged, "fg", fg.Name, "bindingID", e.ID, "destination", e.Destination,
				"archiveID", e.ArchiveID, "state", string(e.State), "reason", string(e.Reason),
				"keySecretName", e.Encryption.KeySecretName, "cause", cause)
		}
		becameBound := e.State == v1alpha1.PITRArchiveBindingBound && e.Reason == v1alpha1.PITRArchiveBindingReasonBound &&
			(prev == nil || prev.State != v1alpha1.PITRArchiveBindingBound) && cause != pitrBindingCauseRecovery
		switch {
		case becameBound:
			rec.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventPITRArchiveBound, "binding %s for %s is Bound to archive %s (%s) by %s",
				e.ID, e.Destination, e.ArchiveID, pitrCandidateLabel(e.Encryption), cause)
		case e.State == v1alpha1.PITRArchiveBindingRefused && (prev == nil || prev.State != e.State || prev.Reason != e.Reason):
			rec.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveBindingRefused, "binding %s refused: %s", e.ID, e.Message)
		case e.Reason == v1alpha1.PITRArchiveBindingReasonKeyMaterialLost && (prev == nil || prev.Reason != e.Reason):
			rec.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveKeyMaterialLost, "binding %s: %s", e.ID, e.Message)
		}
	}
	for _, name := range p.recreated {
		rec.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveKeySnapshotRecreated,
			"key snapshot %s was missing and was re-created from the profile's passphrase (same name; no roll)", name)
	}
	if cond == nil {
		return
	}
	edge := beforeCond == nil || beforeCond.Reason != cond.Reason || beforeCond.Message != cond.Message
	if edge && cond.Status == metav1.ConditionFalse {
		switch cond.Reason {
		case v1alpha1.ReasonEncryptionChangeRefused:
			rec.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveEncryptionChangeRefused, cond.Message)
		case v1alpha1.ReasonKeyChangeRefused:
			rec.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveKeyChangeRefused, cond.Message)
		case v1alpha1.ReasonBindingLimitReached:
			rec.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveBindingLimitReached, cond.Message)
		case string(v1alpha1.PITRArchiveBindingReasonArchiveForeign), string(v1alpha1.PITRArchiveBindingReasonArchiveCopy):
			// Writer eligibility (K6) has no entry transition of its own.
			if !p.entryRefused(cond.Reason) {
				rec.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveBindingRefused, cond.Message)
			}
		}
	}
	old := ""
	if beforeCond != nil {
		old = beforeCond.Message
	}
	for _, k := range p.keySourceRemoved {
		if !strings.Contains(old, "key source removed: "+k) {
			rec.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveKeySourceRemoved, k)
		}
	}
	for _, n := range p.notes {
		if !strings.Contains(old, "key snapshot "+n.secret+" is unreadable") {
			rec.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventPITRArchiveKeySnapshotReadFailed,
				"key snapshot %s is unreadable: %s; it is never used for recovery and never deleted by the operator", n.secret, n.detail)
		}
	}
}

func (p *pitrBindingPass) entryRefused(reason string) bool {
	for i := range p.entries {
		if p.entries[i].State == v1alpha1.PITRArchiveBindingRefused && string(p.entries[i].Reason) == reason {
			return true
		}
	}
	return false
}

// ReconcilePITRArchiveBindingsForTest runs one binding pass on fg (for
// test/component). Not for production use.
func (r *MysqlFailoverGroupReconciler) ReconcilePITRArchiveBindingsForTest(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	return r.reconcilePITRArchiveBindings(ctx, fg)
}
