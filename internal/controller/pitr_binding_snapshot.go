package controller

// Archive key snapshots (#203, IMPL-203 §4.3): immutable, operator-made,
// ownerless copies of a PITR archive passphrase. A binding names one; every
// consumer mounts it instead of the user's Secret. Nothing here deletes a
// snapshot except an explicit release.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

// pitrKeySnapshotH10 is the keyed content address of a snapshot (Δ4):
// hex(HMAC-SHA256(digestKey, domain ‖ destination ‖ "\0" ‖ trimmed))[:10].
// It only deduplicates (one snapshot per destination and bytes); it proves
// nothing after creation.
func pitrKeySnapshotH10(digestKey []byte, destination string, trimmed []byte) string {
	mac := hmac.New(sha256.New, digestKey)
	mac.Write([]byte(pitrKeySnapshotNameDomain))
	mac.Write([]byte(destination))
	mac.Write([]byte{0})
	mac.Write(trimmed)
	return hex.EncodeToString(mac.Sum(nil))[:10]
}

// pitrKeySnapshotName is the snapshot name for destination and trimmed
// passphrase bytes: mysql-<group>-pitr-key-<h10>, truncated to a DNS-1123
// label.
func pitrKeySnapshotName(fgName string, digestKey []byte, destination string, trimmed []byte) string {
	return truncateDNS1123("mysql-" + fgName + "-pitr-key-" + pitrKeySnapshotH10(digestKey, destination, trimmed))
}

// pitrLiveKey is a profile's live passphrase as the user's Secret holds it.
type pitrLiveKey struct {
	secret, key string
	// trimmed is backupcrypto.TrimPassphrase of the Secret value; nil when
	// missing is set.
	trimmed []byte
	// missing explains why there is no key ("" when there is one).
	missing string
}

// readPITRLiveKey reads profile p's passphrase (cached). A missing Secret,
// key or an all-whitespace value is reported in missing, not as an error.
func (r *MysqlFailoverGroupReconciler) readPITRLiveKey(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, p *v1alpha1.BackupProfile) (pitrLiveKey, error) {
	if !p.EncryptionEnabled() {
		return pitrLiveKey{}, nil
	}
	ref := p.Encryption.PassphraseSecret
	out := pitrLiveKey{secret: ref.Name, key: ref.PassphraseSecretKeyOrDefault()}
	var s corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: ref.Name}, &s)
	switch {
	case apierrors.IsNotFound(err):
		out.missing = fmt.Sprintf("passphrase Secret %s (key %s) of profile %s is missing", out.secret, out.key, p.Name)
		return out, nil
	case err != nil:
		return out, fmt.Errorf("get passphrase Secret %s: %w", ref.Name, err)
	}
	trimmed := backupcrypto.TrimPassphrase(s.Data[out.key])
	if len(trimmed) == 0 {
		out.missing = fmt.Sprintf("passphrase Secret %s key %s of profile %s is missing or empty", out.secret, out.key, p.Name)
		return out, nil
	}
	out.trimmed = append([]byte(nil), trimmed...)
	return out, nil
}

// errPITRSnapshotNameHeld: the snapshot name is taken by an object that is
// not this group's snapshot of these bytes for this destination.
var errPITRSnapshotNameHeld = errors.New("snapshot name is held by another object")

// pitrKeySnapshotLabels are an archive key snapshot's labels.
func pitrKeySnapshotLabels(fg *v1alpha1.MysqlFailoverGroup, destination string) map[string]string {
	return map[string]string{
		labelAppName:                   appNamePITRKeySnapshot,
		labelInstance:                  fg.Name,
		v1alpha1.FailoverGroupLabel:    fg.Name,
		labelManagedBy:                 managerName,
		v1alpha1.FailoverGroupUIDLabel: string(fg.UID),
		v1alpha1.PITRBindingIDLabel:    pitrBindingIDFromString(destination),
	}
}

// newPITRKeySnapshot builds the snapshot object for destination and trimmed
// bytes: immutable, no owner reference (Δ31), labelled and annotated with
// its destination. acceptance, when set, is written with it (C3 re-create).
func newPITRKeySnapshot(fg *v1alpha1.MysqlFailoverGroup, name, destination string, trimmed []byte, acceptance string) *corev1.Secret {
	ann := map[string]string{v1alpha1.PITRDestinationAnnotation: destination}
	if acceptance != "" {
		ann[v1alpha1.PITRAcceptedAnnotation] = acceptance
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: fg.Namespace,
			Labels:      pitrKeySnapshotLabels(fg, destination),
			Annotations: ann,
		},
		Immutable: boolPtr(true),
		Type:      corev1.SecretTypeOpaque,
		Data:      map[string][]byte{pitrKeySnapshotDataKey: append([]byte(nil), trimmed...)},
	}
}

// ensurePITRKeySnapshot returns the snapshot of trimmed for destination,
// creating it when absent. On AlreadyExists it adopts the existing object
// only when it carries this group's UID label, the destination and the same
// bytes; anything else is errPITRSnapshotNameHeld. It never deletes or
// overwrites.
func (r *MysqlFailoverGroupReconciler) ensurePITRKeySnapshot(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, digestKey []byte, destination string, trimmed []byte) (*corev1.Secret, error) {
	if len(trimmed) == 0 {
		return nil, errors.New("pitr key snapshot: empty passphrase")
	}
	name := pitrKeySnapshotName(fg.Name, digestKey, destination, trimmed)
	snap := newPITRKeySnapshot(fg, name, destination, trimmed, "")
	err := r.Create(ctx, snap)
	if err == nil {
		return snap, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create pitr key snapshot %s: %w", name, err)
	}
	var existing corev1.Secret
	if err := r.bindingReader().Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &existing); err != nil {
		return nil, fmt.Errorf("get pitr key snapshot %s: %w", name, err)
	}
	if !pitrSnapshotOf(fg, &existing, destination) || !bytes.Equal(existing.Data[pitrKeySnapshotDataKey], trimmed) {
		return nil, fmt.Errorf("%w: Secret %s", errPITRSnapshotNameHeld, name)
	}
	return &existing, nil
}

// pitrSnapshotOf reports whether s is an archive key snapshot of fg for
// destination (labels and the destination annotation).
func pitrSnapshotOf(fg *v1alpha1.MysqlFailoverGroup, s *corev1.Secret, destination string) bool {
	return s.Labels[labelAppName] == appNamePITRKeySnapshot &&
		s.Labels[v1alpha1.FailoverGroupUIDLabel] == string(fg.UID) &&
		s.Annotations[v1alpha1.PITRDestinationAnnotation] == destination
}

// listPITRKeySnapshots lists fg's archive key snapshots (cached) by the
// app label and the group-UID label: another group's, or a deleted
// same-name predecessor's, are never seen.
func (r *MysqlFailoverGroupReconciler) listPITRKeySnapshots(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) ([]corev1.Secret, error) {
	var list corev1.SecretList
	if err := r.List(ctx, &list, client.InNamespace(fg.Namespace), client.MatchingLabels{
		labelAppName: appNamePITRKeySnapshot, v1alpha1.FailoverGroupUIDLabel: string(fg.UID),
	}); err != nil {
		return nil, fmt.Errorf("list pitr key snapshots: %w", err)
	}
	return list.Items, nil
}

// pitrSnapshotCandidate is a listed snapshot considered for a destination.
type pitrSnapshotCandidate struct {
	secret     *corev1.Secret
	accepted   bool
	acceptedAt time.Time
}

// orderPITRSnapshots returns the snapshots of destination in candidate
// order (§4.4): accepted first (newest acceptedAt first), then the rest
// newest first; ties by name.
func orderPITRSnapshots(fg *v1alpha1.MysqlFailoverGroup, snaps []corev1.Secret, destination string) []pitrSnapshotCandidate {
	var out []pitrSnapshotCandidate
	for i := range snaps {
		s := &snaps[i]
		if !pitrSnapshotOf(fg, s, destination) || len(s.Data[pitrKeySnapshotDataKey]) == 0 {
			continue
		}
		c := pitrSnapshotCandidate{secret: s}
		if a, err := verifyPITRAcceptance(s); err == nil && a.Destination == destination {
			c.accepted, c.acceptedAt = true, a.AcceptedAt
		}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.accepted != b.accepted:
			return a.accepted
		case a.accepted && !a.acceptedAt.Equal(b.acceptedAt):
			return a.acceptedAt.After(b.acceptedAt)
		case !a.accepted && !a.secret.CreationTimestamp.Equal(&b.secret.CreationTimestamp):
			return a.secret.CreationTimestamp.After(b.secret.CreationTimestamp.Time)
		default:
			return a.secret.Name < b.secret.Name
		}
	})
	return out
}

// errPITRCheckedSnapshotReplaced: the snapshot a check Job verified was
// replaced (deleted and re-created under its name) before its acceptance:
// the verdict is about bytes that are no longer there.
var errPITRCheckedSnapshotReplaced = errors.New("the key snapshot the check verified was replaced")

// acceptPITRKeySnapshot writes the acceptance annotation (K8) on entry's
// snapshot, computing the keyCheck locally from the snapshot's bytes. It
// runs before the status write that records entry Bound. checked, when
// set, is the UID of the snapshot object the check Job mounted: another
// object of the name is refused (errPITRCheckedSnapshotReplaced), so the
// keyCheck is always computed from the bytes the check verified (U22). An
// existing valid acceptance of the same archive is kept; one of another
// archive is never overwritten.
func (r *MysqlFailoverGroupReconciler) acceptPITRKeySnapshot(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding, checked types.UID, now time.Time) error {
	var snap corev1.Secret
	if err := r.bindingReader().Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: entry.Encryption.KeySecretName}, &snap); err != nil {
		return fmt.Errorf("get pitr key snapshot %s: %w", entry.Encryption.KeySecretName, err)
	}
	if checked != "" && snap.UID != checked {
		return fmt.Errorf("%w: Secret %s is object %s, the check Job mounted %s", errPITRCheckedSnapshotReplaced, snap.Name, snap.UID, checked)
	}
	if !pitrSnapshotOf(fg, &snap, entry.Destination) {
		return fmt.Errorf("%w: Secret %s is not this group's snapshot for %s", errPITRSnapshotNameHeld, snap.Name, entry.Destination)
	}
	if prev, err := verifyPITRAcceptance(&snap); err == nil {
		if prev.ArchiveID == entry.ArchiveID && prev.Source == *entry.Source &&
			prev.Mode == entry.Encryption.Mode && prev.Algorithm == entry.Encryption.Algorithm {
			return nil
		}
		return fmt.Errorf("snapshot %s is already accepted for archive %s; refusing to re-accept it for %s", snap.Name, prev.ArchiveID, entry.ArchiveID)
	} else if !errors.Is(err, errPITRAcceptanceMissing) && !errors.Is(err, errPITRAcceptanceMalformed) {
		return fmt.Errorf("snapshot %s: %w", snap.Name, err)
	}
	a, err := newPITRAcceptance(entry, snap.Data[pitrKeySnapshotDataKey], now)
	if err != nil {
		return err
	}
	raw, err := a.encode()
	if err != nil {
		return err
	}
	patch := client.MergeFromWithOptions(snap.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if snap.Annotations == nil {
		snap.Annotations = map[string]string{}
	}
	snap.Annotations[v1alpha1.PITRAcceptedAnnotation] = raw
	if err := r.Patch(ctx, &snap, patch); err != nil {
		return fmt.Errorf("annotate pitr key snapshot %s: %w", snap.Name, err)
	}
	return nil
}

// pitrSnapshotNote is a snapshot finding the condition message carries (so
// its Warning is edge-triggered on the message).
type pitrSnapshotNote struct {
	secret string
	detail string
}

// recoverPITRBindings is K8 recovery: every listed snapshot with a valid
// acceptance whose bytes verify against its own keyCheck rebuilds a Bound
// entry for its destination when that destination has no Bound entry
// (newest acceptedAt wins). At most pitrArchiveBindingsMax entries exist:
// in-use destinations are rebuilt first, the rest are returned by binding
// id (BindingLimitReached). Malformed acceptances are never used; they are
// returned as notes (Warning PITRArchiveKeySnapshotUnreadable). Destinations
// in skip (a pending release) are not rebuilt.
func recoverPITRBindings(fg *v1alpha1.MysqlFailoverGroup, entries []v1alpha1.PITRArchiveBinding, snaps []corev1.Secret, inUse map[string]bool, skip map[string]bool, now time.Time) (out []v1alpha1.PITRArchiveBinding, rebuilt []string, overflow []string, unreadable []pitrSnapshotNote) {
	type found struct {
		a    pitrAcceptance
		name string
	}
	best := map[string]found{}
	for i := range snaps {
		s := &snaps[i]
		a, err := verifyPITRAcceptance(s)
		switch {
		case errors.Is(err, errPITRAcceptanceMissing):
			continue
		case errors.Is(err, errPITRAcceptanceMalformed):
			unreadable = append(unreadable, pitrSnapshotNote{secret: s.Name, detail: "its acceptance annotation is malformed"})
			continue
		case err != nil:
			unreadable = append(unreadable, pitrSnapshotNote{secret: s.Name, detail: "it does not hold the key its acceptance records"})
			continue
		}
		if s.Labels[v1alpha1.FailoverGroupUIDLabel] != string(fg.UID) || s.Labels[v1alpha1.PITRBindingIDLabel] != pitrBindingIDFromString(a.Destination) {
			continue
		}
		if prev, ok := best[a.Destination]; !ok || a.AcceptedAt.After(prev.a.AcceptedAt) ||
			(a.AcceptedAt.Equal(prev.a.AcceptedAt) && s.Name < prev.name) {
			best[a.Destination] = found{a: a, name: s.Name}
		}
	}
	out = append([]v1alpha1.PITRArchiveBinding(nil), entries...)
	index := map[string]int{}
	for i := range out {
		index[out[i].Destination] = i
	}
	dests := make([]string, 0, len(best))
	for d := range best {
		if skip[pitrBindingIDFromString(d)] {
			continue
		}
		if i, ok := index[d]; ok && out[i].State == v1alpha1.PITRArchiveBindingBound {
			continue
		}
		dests = append(dests, d)
	}
	sort.Slice(dests, func(i, j int) bool {
		if inUse[dests[i]] != inUse[dests[j]] {
			return inUse[dests[i]]
		}
		return dests[i] < dests[j]
	})
	for _, d := range dests {
		f := best[d]
		src := f.a.Source
		e := v1alpha1.PITRArchiveBinding{
			ID: pitrBindingIDFromString(d), Destination: d, ArchiveID: f.a.ArchiveID, Source: &src,
			Encryption: v1alpha1.PITRArchiveBindingEncryption{Mode: f.a.Mode, Algorithm: f.a.Algorithm, KeySecretName: f.name},
			State:      v1alpha1.PITRArchiveBindingBound, Reason: v1alpha1.PITRArchiveBindingReasonBound,
			Message:            fmt.Sprintf("rebuilt from the acceptance of key snapshot %s", f.name),
			LastTransitionTime: &metav1.Time{Time: now},
		}
		if i, ok := index[d]; ok {
			out[i] = e
			rebuilt = append(rebuilt, e.ID)
			continue
		}
		if len(out) >= pitrArchiveBindingsMax {
			overflow = append(overflow, e.ID)
			continue
		}
		index[d] = len(out)
		out = append(out, e)
		rebuilt = append(rebuilt, e.ID)
	}
	return out, rebuilt, overflow, unreadable
}

// pitrIntegrity is the outcome of checkPITRKeySnapshotIntegrity.
type pitrIntegrity int

const (
	pitrIntegrityOK pitrIntegrity = iota
	pitrIntegrityNotFound
	pitrIntegrityInvalid
)

// checkPITRKeySnapshotIntegrity checks the snapshot a Bound Passphrase
// entry names (Δ34): it exists, carries fg's UID label and the entry's
// destination, and holds a valid acceptance of the entry's archive whose
// keyCheck verifies its bytes. cached is the listed copy (nil when the
// listing lacks it); a failing or missing cached copy is confirmed through
// the uncached reader before it is reported, so a lagging cache never
// raises KeyMaterialLost.
func (r *MysqlFailoverGroupReconciler) checkPITRKeySnapshotIntegrity(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding, cached *corev1.Secret) (pitrIntegrity, string, error) {
	if cached != nil {
		if why := pitrSnapshotIntegrityProblem(fg, entry, cached); why == "" {
			return pitrIntegrityOK, "", nil
		}
	}
	var s corev1.Secret
	err := r.bindingReader().Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: entry.Encryption.KeySecretName}, &s)
	switch {
	case apierrors.IsNotFound(err):
		return pitrIntegrityNotFound, fmt.Sprintf("key snapshot Secret %s is missing", entry.Encryption.KeySecretName), nil
	case err != nil:
		return 0, "", fmt.Errorf("get pitr key snapshot %s: %w", entry.Encryption.KeySecretName, err)
	}
	if why := pitrSnapshotIntegrityProblem(fg, entry, &s); why != "" {
		return pitrIntegrityInvalid, why, nil
	}
	return pitrIntegrityOK, "", nil
}

// pitrSnapshotIntegrityProblem explains why s does not hold entry's
// accepted key, or "".
func pitrSnapshotIntegrityProblem(fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding, s *corev1.Secret) string {
	if s.Labels[v1alpha1.FailoverGroupUIDLabel] != string(fg.UID) {
		return fmt.Sprintf("Secret %s does not carry this group's UID label", s.Name)
	}
	if s.Annotations[v1alpha1.PITRDestinationAnnotation] != entry.Destination {
		return fmt.Sprintf("Secret %s is not a snapshot for %s", s.Name, entry.Destination)
	}
	a, err := verifyPITRAcceptance(s)
	if err != nil {
		return fmt.Sprintf("Secret %s does not hold the accepted key: %v", s.Name, err)
	}
	if a.Destination != entry.Destination || a.ArchiveID != entry.ArchiveID || entry.Source == nil || a.Source != *entry.Source ||
		a.Mode != entry.Encryption.Mode || a.Algorithm != entry.Encryption.Algorithm {
		return fmt.Sprintf("Secret %s records the acceptance of another archive (%s)", s.Name, a.ArchiveID)
	}
	return ""
}

// pitrRecreateOutcome is what recreatePITRKeySnapshot did.
type pitrRecreateOutcome int

const (
	// pitrRecreateNotReproducible: the bytes do not reproduce the name;
	// nothing was created.
	pitrRecreateNotReproducible pitrRecreateOutcome = iota
	// pitrRecreated: the snapshot was created again.
	pitrRecreated
	// pitrRecreateRaced: an object of the name appeared before the
	// create; nothing was created and nothing is known about it until the
	// next pass checks it.
	pitrRecreateRaced
)

// recreatePITRKeySnapshot is C3: a Bound entry's snapshot is gone and
// trimmed (a profile's live bytes) reproduces its name, so the same Secret
// is created again with the acceptance rewritten from the entry. An
// acceptance that cannot be built for the entry is an error, never "the
// bytes do not match" (U58).
func (r *MysqlFailoverGroupReconciler) recreatePITRKeySnapshot(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, digestKey []byte, entry *v1alpha1.PITRArchiveBinding, trimmed []byte, now time.Time) (pitrRecreateOutcome, error) {
	if len(trimmed) == 0 || pitrKeySnapshotName(fg.Name, digestKey, entry.Destination, trimmed) != entry.Encryption.KeySecretName {
		return pitrRecreateNotReproducible, nil
	}
	a, err := newPITRAcceptance(entry, trimmed, now)
	if err != nil {
		return pitrRecreateNotReproducible, fmt.Errorf("re-create pitr key snapshot %s: %w", entry.Encryption.KeySecretName, err)
	}
	raw, err := a.encode()
	if err != nil {
		return pitrRecreateNotReproducible, fmt.Errorf("re-create pitr key snapshot %s: %w", entry.Encryption.KeySecretName, err)
	}
	snap := newPITRKeySnapshot(fg, entry.Encryption.KeySecretName, entry.Destination, trimmed, raw)
	if err := r.Create(ctx, snap); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return pitrRecreateRaced, nil
		}
		return pitrRecreateNotReproducible, fmt.Errorf("re-create pitr key snapshot %s: %w", snap.Name, err)
	}
	return pitrRecreated, nil
}

// pitrKeySnapshotInUse reports why the binding at destination, or any of
// its snapshots, is still used (K9), or "": a site Deployment's
// pitr-render annotation renders the destination or names a snapshot, a
// site Deployment carries an annotation that does not decode (what it
// renders cannot be told), a site Deployment's pod template references a
// snapshot (scaled to zero or not), a non-terminal pod in the namespace
// references one (a Secret or projected volume, env or envFrom: old
// ReplicaSets mid-roll, restore, verification, check, staging and
// maintenance pods), a Job labelled with one has not finished, or a
// snapshot carries a live verification hold. Reads are uncached.
func (r *MysqlFailoverGroupReconciler) pitrKeySnapshotInUse(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, destination string, snaps []corev1.Secret, deploys []appsv1.Deployment) (string, error) {
	names := map[string]bool{}
	for _, s := range snaps {
		names[s.Name] = true
	}
	for _, sr := range sitePITRRenders(fg, deploys) {
		switch {
		case sr.Err == nil && sr.Inputs.Enabled && destination != "" && sr.Inputs.Destination == destination:
			return fmt.Sprintf("Deployment %s renders %s", sr.Deployment, destination), nil
		case sr.Err == nil && names[sr.Inputs.KeySecretName]:
			return fmt.Sprintf("Deployment %s renders key snapshot %s", sr.Deployment, sr.Inputs.KeySecretName), nil
		case sr.Err != nil && !errors.Is(sr.Err, errPITRRenderAbsent):
			return fmt.Sprintf("Deployment %s carries a %s annotation that does not decode (%v), so what it renders cannot be told; repair or remove it", sr.Deployment, v1alpha1.PITRRenderAnnotation, sr.Err), nil
		}
	}
	if len(names) == 0 {
		return "", nil
	}
	for i := range deploys {
		d := &deploys[i]
		if !isPITRSiteDeployment(fg, d) {
			continue
		}
		if name := podSpecSecretRef(&d.Spec.Template.Spec, names); name != "" {
			return fmt.Sprintf("Deployment %s's pod template references key snapshot %s", d.Name, name), nil
		}
	}
	reader := r.bindingReader()
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(fg.Namespace)); err != nil {
		return "", fmt.Errorf("list pods: %w", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if name := podSpecSecretRef(&p.Spec, names); name != "" {
			return fmt.Sprintf("pod %s mounts key snapshot %s", p.Name, name), nil
		}
	}
	for name := range names {
		var jobs batchv1.JobList
		if err := reader.List(ctx, &jobs, client.InNamespace(fg.Namespace), client.MatchingLabels{v1alpha1.PITRKeySecretLabel: name}); err != nil {
			return "", fmt.Errorf("list jobs: %w", err)
		}
		for i := range jobs.Items {
			if !jobFinished(&jobs.Items[i]) {
				return fmt.Sprintf("Job %s mounts key snapshot %s", jobs.Items[i].Name, name), nil
			}
		}
	}
	for _, s := range snaps {
		for key, holder := range s.Annotations {
			uid, ok := strings.CutPrefix(key, v1alpha1.VerificationHoldAnnotationPrefix)
			if !ok {
				continue
			}
			live, err := holdLive(ctx, reader, fg.Namespace, uid, holder)
			if err != nil {
				return "", err
			}
			if live {
				return fmt.Sprintf("MysqlBackupVerification %s holds key snapshot %s", holder, s.Name), nil
			}
		}
	}
	return "", nil
}

// podSpecSecretRef returns the first Secret of names spec references (a
// Secret volume, a projected volume's Secret source, or a container's or
// init container's env valueFrom or envFrom), or "".
func podSpecSecretRef(spec *corev1.PodSpec, names map[string]bool) string {
	for _, v := range spec.Volumes {
		if v.Secret != nil && names[v.Secret.SecretName] {
			return v.Secret.SecretName
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.Secret != nil && names[src.Secret.Name] {
					return src.Secret.Name
				}
			}
		}
	}
	for _, list := range [][]corev1.Container{spec.InitContainers, spec.Containers} {
		for _, c := range list {
			for _, e := range c.Env {
				if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && names[e.ValueFrom.SecretKeyRef.Name] {
					return e.ValueFrom.SecretKeyRef.Name
				}
			}
			for _, e := range c.EnvFrom {
				if e.SecretRef != nil && names[e.SecretRef.Name] {
					return e.SecretRef.Name
				}
			}
		}
	}
	for _, c := range spec.EphemeralContainers {
		for _, e := range c.Env {
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && names[e.ValueFrom.SecretKeyRef.Name] {
				return e.ValueFrom.SecretKeyRef.Name
			}
		}
		for _, e := range c.EnvFrom {
			if e.SecretRef != nil && names[e.SecretRef.Name] {
				return e.SecretRef.Name
			}
		}
	}
	return ""
}

// pitrReleaseOutcome is what processPITRBindingRelease did.
type pitrReleaseOutcome struct {
	// id is the released binding (the annotation is removed after the
	// pass's status write).
	id string
	// value is the annotation value processed: the annotation is removed
	// only while it still holds exactly this request.
	value string
	// destination is the released binding's destination, when known.
	destination string
	// rejected is the parse error of a malformed or unconfirmed value: the
	// annotation is removed with a Warning.
	rejected string
	// refused explains why the binding is still in use: the annotation is
	// kept and the release retried.
	refused string
	// conflict is set when the group, its status or a snapshot changed
	// under the release: re-evaluated next pass.
	conflict bool
	// removed is set once the stored status no longer holds the entry:
	// the pass drops it too, and base is the stored list the pass's own
	// status write must find.
	removed bool
	base    []v1alpha1.PITRArchiveBinding
}

// processPITRBindingRelease handles the release annotation (§4.3 Release,
// K9, G1). It runs under the group's PITR render lock, the lock the
// ordered updater holds from its fresh read of the group until it has
// written a site Deployment, so no render can start mounting a snapshot
// between the in-use check and its deletion. Under the lock it re-reads
// the group (whose stored bindings must still be base, the list this pass
// started from, else errPITRBindingStatusMoved), refuses while the
// binding's destination is a target of the pass or of the stored spec, or
// any of its snapshots is in use (Deployments, pods, Jobs and holds read
// uncached), then removes the entry from the stored status (a
// resourceVersion-conditioned write) and deletes each snapshot with a
// resourceVersion precondition from the in-use read. Removing the entry
// first means a render after the lock no longer finds the binding to
// mount. The caller removes the annotation after its own status write.
func (r *MysqlFailoverGroupReconciler) processPITRBindingRelease(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, base []v1alpha1.PITRArchiveBinding, mbvs []v1alpha1.MysqlBackupVerification, targets []pitrBindingTarget) (*pitrReleaseOutcome, error) {
	value, ok := fg.Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation]
	if !ok {
		return nil, nil
	}
	id, err := parsePITRBindingRelease(value, fg.Name)
	if err != nil {
		return &pitrReleaseOutcome{rejected: err.Error(), value: value}, nil
	}
	out := &pitrReleaseOutcome{id: id, value: value}
	for i := range base {
		if base[i].ID == id {
			out.destination = base[i].Destination
		}
	}
	for i := range targets {
		if pitrBindingID(targets[i].dest) == id {
			out.refused = fmt.Sprintf("binding %s (%s) is %s", id, targets[i].dest, targets[i].useDescription())
			return out, nil
		}
	}

	unlock := r.lockPITRRender(client.ObjectKeyFromObject(fg))
	defer unlock()
	reader := r.bindingReader()
	var fresh v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
		if apierrors.IsNotFound(err) {
			out.conflict = true
			return out, nil
		}
		return nil, fmt.Errorf("get group for release: %w", err)
	}
	if fresh.UID != fg.UID || pitrReleasePending(&fresh) != id {
		out.conflict = true
		return out, nil
	}
	if !pitrBindingsEqual(fresh.Status.PITRArchiveBindings, base) {
		return nil, errPITRBindingStatusMoved
	}
	for _, t := range pitrBindingTargets(&fresh, mbvs) {
		if pitrBindingID(t.dest) == id {
			out.refused = fmt.Sprintf("binding %s (%s) is %s", id, t.dest, t.useDescription())
			return out, nil
		}
	}
	var list corev1.SecretList
	if err := reader.List(ctx, &list, client.InNamespace(fg.Namespace), client.MatchingLabels{
		labelAppName: appNamePITRKeySnapshot, v1alpha1.FailoverGroupUIDLabel: string(fg.UID), v1alpha1.PITRBindingIDLabel: id,
	}); err != nil {
		return nil, fmt.Errorf("list pitr key snapshots of %s: %w", id, err)
	}
	if out.destination == "" {
		for _, s := range list.Items {
			if d := s.Annotations[v1alpha1.PITRDestinationAnnotation]; d != "" {
				out.destination = d
				break
			}
		}
	}
	deploys, err := listPITRSiteDeployments(ctx, reader, &fresh)
	if err != nil {
		return nil, err
	}
	why, err := r.pitrKeySnapshotInUse(ctx, &fresh, out.destination, list.Items, deploys)
	if err != nil {
		return nil, err
	}
	if why != "" {
		out.refused = fmt.Sprintf("binding %s is in use: %s", id, why)
		return out, nil
	}

	// The entry leaves the stored status before any snapshot goes.
	kept := make([]v1alpha1.PITRArchiveBinding, 0, len(fresh.Status.PITRArchiveBindings))
	for _, e := range fresh.Status.PITRArchiveBindings {
		if e.ID != id {
			kept = append(kept, e)
		}
	}
	if len(kept) != len(fresh.Status.PITRArchiveBindings) {
		if len(kept) == 0 {
			kept = nil
		}
		fresh.Status.PITRArchiveBindings = kept
		if err := r.Status().Update(ctx, &fresh); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				out.conflict = true
				return out, nil
			}
			return nil, fmt.Errorf("remove released binding %s from status: %w", id, err)
		}
	}
	out.removed = true
	out.base = append([]v1alpha1.PITRArchiveBinding(nil), kept...)

	for i := range list.Items {
		s := &list.Items[i]
		uid, rv := s.UID, s.ResourceVersion
		err := r.Delete(ctx, s, client.Preconditions{UID: &uid, ResourceVersion: &rv})
		switch {
		case err == nil, apierrors.IsNotFound(err):
		case apierrors.IsConflict(err):
			// A verification hold landed after the in-use read: the
			// snapshot stays, and the release is retried (and refused
			// while the verification reads it).
			out.conflict = true
			return out, nil
		default:
			return nil, fmt.Errorf("delete pitr key snapshot %s: %w", s.Name, err)
		}
	}
	return out, nil
}

// pitrBindingsEqual compares two binding lists (nil equals empty).
func pitrBindingsEqual(a, b []v1alpha1.PITRArchiveBinding) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return equality.Semantic.DeepEqual(a, b)
}

// pitrReleasePending returns the binding id a valid release annotation
// names, or "". While it names an id, C3 and KeyMaterialLost transitions
// are suppressed for it and recovery does not rebuild it (a crash between
// the release's status write and its snapshot deletions, or a snapshot
// deleted by hand, finishes the release on the next pass).
func pitrReleasePending(fg *v1alpha1.MysqlFailoverGroup) string {
	value, ok := fg.Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation]
	if !ok {
		return ""
	}
	id, err := parsePITRBindingRelease(value, fg.Name)
	if err != nil {
		return ""
	}
	return id
}

// removePITRReleaseAnnotation removes the release annotation from fg when
// it still holds value, the request the pass processed: it re-reads the
// group (uncached) and patches with a resourceVersion precondition, so a
// newer request written meanwhile is never erased (U29); a conflict leaves
// it for the next pass.
func (r *MysqlFailoverGroupReconciler) removePITRReleaseAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, value string) error {
	var fresh v1alpha1.MysqlFailoverGroup
	if err := r.bindingReader().Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get group to remove the release annotation: %w", err)
	}
	if got, ok := fresh.Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation]; fresh.UID != fg.UID || !ok || got != value {
		return nil
	}
	patch := client.MergeFromWithOptions(fresh.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(fresh.Annotations, v1alpha1.ReleasePITRArchiveBindingAnnotation)
	if err := r.Patch(ctx, &fresh, patch); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("remove release annotation: %w", err)
	}
	delete(fg.Annotations, v1alpha1.ReleasePITRArchiveBindingAnnotation)
	return nil
}
