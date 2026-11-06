package controller

// Group-level PITR render resolution (#203, IMPL-203 §4.6): which archive
// identity and access fields every site Deployment of a group renders, read
// from the group's bindings (status.pitrArchiveBindings) and the render the
// running writers already carry (the bloodraven.shipstream.io/pitr-render
// annotation of the site Deployments). Also the read-side helpers the
// consumers share: the read binding of a restore or verification, the
// read-only profile rule (D-10), the archive ID backups stamp (K5), the
// archive IDs retention accepts (Δ12) and the Secret keys the pod and
// credential digests consume (Δ15).

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
)

// listPITRSiteDeployments lists fg's MySQL Deployments through reader (the
// APIReader where the decision must not rest on a lagging cache). The
// result still contains whatever matched the selector; sitePITRRenders and
// groupPITRHold keep only the site Deployments.
func listPITRSiteDeployments(ctx context.Context, reader client.Reader, fg *v1alpha1.MysqlFailoverGroup) ([]appsv1.Deployment, error) {
	var list appsv1.DeploymentList
	if err := reader.List(ctx, &list, client.InNamespace(fg.Namespace),
		client.MatchingLabels{labelAppName: "mysql", labelFailoverGroup: fg.Name}); err != nil {
		return nil, fmt.Errorf("list site deployments: %w", err)
	}
	return list.Items, nil
}

// activePITRProfile returns the profile spec.backup.pitr names and its
// normalized destination, or ok=false when PITR is disabled or the profile
// is missing or invalid (§4.6 row 1).
func activePITRProfile(fg *v1alpha1.MysqlFailoverGroup) (*v1alpha1.BackupProfile, pitrarchive.Destination, bool) {
	if !pitrEnabled(fg) || fg.Spec.Backup.PITR.ProfileName == "" {
		return nil, pitrarchive.Destination{}, false
	}
	profile := findProfile(fg, fg.Spec.Backup.PITR.ProfileName)
	if profile == nil {
		return nil, pitrarchive.Destination{}, false
	}
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return nil, pitrarchive.Destination{}, false
	}
	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3, v1alpha1.BackupStoragePVC:
	default:
		return nil, pitrarchive.Destination{}, false
	}
	return profile, dest, true
}

// pitrProfileRender is profile's render at dest without an encryption
// identity or archive ID: the location fields (from the normalized
// destination, plus the raw PVC subPath), and the access fields (S3
// region, raw endpointURL, credentials Secret, and the group's archive poll
// interval). It never carries a profile name (K7).
func pitrProfileRender(fg *v1alpha1.MysqlFailoverGroup, profile *v1alpha1.BackupProfile, dest pitrarchive.Destination) pitrRenderInputs {
	in := pitrRenderInputs{
		SchemaVersion: pitrRenderSchemaVersion,
		Enabled:       true,
		Destination:   dest.String(),
		StorageType:   string(profile.Storage.Type),
		KeyPrefix:     dest.KeyPrefix,
	}
	switch dest.Kind {
	case pitrarchive.DestinationS3:
		in.Bucket = dest.Bucket
		if s3 := profile.Storage.S3; s3 != nil {
			in.Region = s3.Region
			in.EndpointURL = s3.EndpointURL
			in.CredentialsSecret = s3.CredentialsSecret
		}
	case pitrarchive.DestinationPVC:
		in.Claim = dest.Claim
		if pvc := profile.Storage.PVC; pvc != nil {
			in.SubPath = pvc.SubPath
		}
	}
	if fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.ArchivePollInterval != nil {
		in.PollInterval = fg.Spec.Backup.PITR.ArchivePollInterval.Duration.String()
	}
	return in
}

// withEncryption sets in's encryption identity from a binding's.
func (p pitrRenderInputs) withEncryption(enc v1alpha1.PITRArchiveBindingEncryption) pitrRenderInputs {
	p.EncryptionMode = string(enc.Mode)
	p.Algorithm = string(enc.Algorithm)
	p.KeySecretName = enc.KeySecretName
	return p
}

// withAccess replaces p's access fields with src's.
func (p pitrRenderInputs) withAccess(src pitrRenderInputs) pitrRenderInputs {
	p.Region = src.Region
	p.EndpointURL = src.EndpointURL
	p.CredentialsSecret = src.CredentialsSecret
	p.PollInterval = src.PollInterval
	return p
}

// pitrBindingEncryptionFor converts a profile's encryption identity to the
// binding form with the given snapshot (empty for None).
func pitrBindingEncryptionFor(ident pitrarchive.EncryptionIdentity, keySecretName string) v1alpha1.PITRArchiveBindingEncryption {
	enc := v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.PITRArchiveEncryptionMode(ident.Mode)}
	if ident.Mode == pitrarchive.EncryptionPassphrase {
		enc.Algorithm = v1alpha1.EncryptionAlgorithm(ident.Algorithm)
		enc.KeySecretName = keySecretName
	}
	return enc
}

// pitrRenderCandidate is the encryption identity a non-Bound write entry
// renders optimistically (§4.6 rows 4a, 4b): the entry's candidate when it
// has a complete one matching the profile's mode and algorithm, else the
// profile's own identity when it is plaintext (no snapshot needed). An
// encrypted profile with no candidate snapshot yet has none (O13: the
// group renders {enabled:false} until one exists).
func pitrRenderCandidate(profile *v1alpha1.BackupProfile, entry *v1alpha1.PITRArchiveBinding) (v1alpha1.PITRArchiveBindingEncryption, bool) {
	ident := pitrprofile.EncryptionIdentityFor(profile)
	if entry != nil && pitrKeyRevision(entry.Encryption) != "" &&
		string(entry.Encryption.Mode) == string(ident.Mode) && string(entry.Encryption.Algorithm) == ident.Algorithm {
		return entry.Encryption, true
	}
	if ident.Mode == pitrarchive.EncryptionNone {
		return pitrBindingEncryptionFor(ident, ""), true
	}
	return v1alpha1.PITRArchiveBindingEncryption{}, false
}

// pitrEstablishingRenderable reports whether an Establishing entry's
// candidate is complete and still matches profile's mode and algorithm.
func pitrEstablishingRenderable(profile *v1alpha1.BackupProfile, entry *v1alpha1.PITRArchiveBinding) bool {
	ident := pitrprofile.EncryptionIdentityFor(profile)
	return pitrKeyRevision(entry.Encryption) != "" &&
		string(entry.Encryption.Mode) == string(ident.Mode) && string(entry.Encryption.Algorithm) == ident.Algorithm
}

// resolvePITRRender is the group's PITR render (§4.6), evaluated once per
// group pass from fg's bindings and the hold H its site Deployments carry
// (deploys: the group's Deployments, listed through the APIReader by the
// reconciler):
//
//	1   PITR disabled, active profile missing or invalid → {enabled:false}
//	2   write entry Bound and writer-eligible (K6) → entry identity + profile access
//	3   write entry Bound, not eligible → H's identity (K13 access), or disabled
//	4b  write entry Pending/Establishing → candidate + profile access
//	4a  not Bound, no writer runs (no Deployment, or H disabled) → candidate, or disabled
//	4c  not Bound, H enabled → H's identity (K13 access)
//	4d  not Bound, any site Deployment without a decodable annotation → Defer
//	L   no entry, the binding list full → as 4c/4a/4d without a candidate
//
// Under pitrRenderDefer the inputs are what a site being created renders
// (H's identity when H is enabled, else the candidate); existing
// Deployments must not be touched. Row 4d also holds when only some site
// Deployments lack a decodable annotation (one was re-created, or a site
// added, while the others were deferred, U01): a decodable annotation on
// one site never authenticates what the others run. Row L never renders a
// destination that has no binding (U14): its archive would have no
// retention authorization and no reader could use it. Every returned
// render validates.
func resolvePITRRender(fg *v1alpha1.MysqlFailoverGroup, deploys []appsv1.Deployment) (pitrRenderInputs, pitrRenderAction) {
	profile, dest, ok := activePITRProfile(fg)
	if !ok {
		return pitrRenderDisabled(), pitrRenderApply
	}
	desired := pitrProfileRender(fg, profile, dest)
	entry := findPITRBinding(fg, dest)
	hold, kind := groupPITRHold(fg, deploys)

	// heldRender is H's identity with the access fields K13 picks: the
	// active profile's when it resolves to H's destination, else H's own.
	heldRender := func() pitrRenderInputs {
		out := hold.Identity()
		if hold.Destination == desired.Destination {
			return checkedPITRRender(out.withAccess(desired))
		}
		return checkedPITRRender(out.withAccess(hold))
	}
	// candidateRender is the optimistic render of the candidate (4a/4b).
	candidateRender := func() pitrRenderInputs {
		enc, ok := pitrRenderCandidate(profile, entry)
		if !ok {
			return pitrRenderDisabled()
		}
		out := desired.withEncryption(enc)
		out.ArchiveID = pitrarchive.CreateArchiveID(fg.UID, dest)
		return checkedPITRRender(out)
	}

	if entry != nil && entry.State == v1alpha1.PITRArchiveBindingBound {
		if eligible, _ := pitrWriterEligible(fg, entry); eligible {
			out := desired.withEncryption(entry.Encryption)
			out.ArchiveID = entry.ArchiveID
			return checkedPITRRender(out), pitrRenderApply
		}
		if kind == pitrHeld && hold.Enabled {
			return heldRender(), pitrRenderApply
		}
		return pitrRenderDisabled(), pitrRenderApply
	}
	// An Establishing candidate renders only while it is still the
	// profile's identity (G3): once the profile's mode or algorithm
	// changed, the binding pass supersedes it, and until then the rows
	// below hold the previous writer render instead of disabling PITR.
	if entry != nil && entry.State == v1alpha1.PITRArchiveBindingPending && entry.Reason == pitrBindingEstablishing &&
		pitrEstablishingRenderable(profile, entry) {
		return candidateRender(), pitrRenderApply
	}
	// L: no binding slot for the destination.
	full := entry == nil && pitrBindingSlotsTaken(fg) >= pitrArchiveBindingsMax
	if full {
		candidateRender = pitrRenderDisabled
	}
	action := pitrRenderApply
	if len(pitrUndecodableDeployments(fg, deploys)) > 0 {
		action = pitrRenderDefer
	}
	switch {
	case kind == pitrHeld && hold.Enabled:
		return heldRender(), action
	default: // pitrHoldNone, a disabled hold, pitrHoldUnannotated
		return candidateRender(), action
	}
}

// pitrBindingSlotsTaken is the number of fg's bindings that hold a slot of
// the binding limit for sure: Bound entries, which only a release removes.
// A non-Bound entry no target uses is dropped by the next binding pass.
func pitrBindingSlotsTaken(fg *v1alpha1.MysqlFailoverGroup) int {
	n := 0
	for i := range fg.Status.PITRArchiveBindings {
		if fg.Status.PITRArchiveBindings[i].State == v1alpha1.PITRArchiveBindingBound {
			n++
		}
	}
	return n
}

// checkedPITRRender returns in, or the disabled render when in does not
// validate (an internal inconsistency must never reach a pod as a
// half-configured archiver; the binding condition reports the group's
// state).
func checkedPITRRender(in pitrRenderInputs) pitrRenderInputs {
	in = in.normalized()
	if err := in.validate(); err != nil {
		return pitrRenderDisabled()
	}
	return in
}

// renderedPITRInputs is the PITR render fg's writers run (Δ14): the hold H
// of its site Deployments among deploys (cached reads are fine: it only
// labels sidecar reports), else, when no site Deployment carries a
// decodable annotation, the render the group would create now.
func renderedPITRInputs(fg *v1alpha1.MysqlFailoverGroup, deploys []appsv1.Deployment) pitrRenderInputs {
	if hold, kind := groupPITRHold(fg, deploys); kind == pitrHeld {
		return hold
	}
	in, _ := resolvePITRRender(fg, nil)
	return in
}

// pitrDeploymentMayRun reports whether d may still run a pod: it wants
// replicas, or its status still counts one.
func pitrDeploymentMayRun(d *appsv1.Deployment) bool {
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 0 {
		return true
	}
	return d.Status.Replicas != 0 || d.Status.ReadyReplicas != 0 || d.Status.AvailableReplicas != 0 || d.Status.UnavailableReplicas != 0
}

// pitrRendersArchiver reports whether in renders an enabled archiver.
func pitrRendersArchiver(in pitrRenderInputs) bool {
	return in.Enabled
}

// renderedWriterArchiveID is the archive ID fg's rendered writers archive
// to (K5): every site Deployment's pitr-render annotation must decode and
// name the same archive ID ("" when all are disabled). ok is false, with a
// reason, while they disagree (a roll in progress) or one cannot be read;
// a group with no site Deployment has no writer and stamps "". The
// Deployment of a site no longer in spec.sites, which the operator never
// renders again, counts only while it may still run a writer (U08): once
// it is scaled to zero with no pod left it is ignored; until then the
// reason names it and the way out.
func renderedWriterArchiveID(fg *v1alpha1.MysqlFailoverGroup, deploys []appsv1.Deployment) (string, bool, string) {
	renders := sitePITRRenders(fg, deploys)
	ids := map[string][]string{}
	var unreadable, removed []string
	inSpec := map[string]bool{}
	for _, s := range fg.Spec.Sites {
		inSpec[s.Name] = true
	}
	byName := map[string]*appsv1.Deployment{}
	for i := range deploys {
		byName[deploys[i].Name] = &deploys[i]
	}
	for _, r := range renders {
		if !inSpec[r.Site] {
			if d := byName[r.Deployment]; d != nil && !pitrDeploymentMayRun(d) {
				continue
			}
			removed = append(removed, r.Deployment)
		}
		if r.Err != nil {
			unreadable = append(unreadable, r.Deployment)
			continue
		}
		id := ""
		if r.Inputs.Enabled {
			id = r.Inputs.ArchiveID
		}
		ids[id] = append(ids[id], r.Deployment)
	}
	if len(unreadable) > 0 {
		return "", false, fmt.Sprintf("site Deployments %s carry no readable %s annotation yet", strings.Join(unreadable, ", "), annotationPITRRender)
	}
	if len(ids) > 1 {
		parts := make([]string, 0, len(ids))
		for id, names := range ids {
			if id == "" {
				id = "none"
			}
			parts = append(parts, fmt.Sprintf("%s (%s)", id, strings.Join(names, ", ")))
		}
		sort.Strings(parts)
		msg := fmt.Sprintf("site Deployments render different PITR archives: %s", strings.Join(parts, "; "))
		if len(removed) > 0 {
			msg += fmt.Sprintf("; Deployments %s belong to sites no longer in spec.sites and are never re-rendered: scale them to zero or delete them", strings.Join(removed, ", "))
		}
		return "", false, msg
	}
	for id := range ids {
		return id, true, ""
	}
	return "", true, ""
}

// pitrBackupWaitsForRollout reports whether a backup of profileName waits
// for the site Deployments to agree on the rendered archive (F-B): only a
// backup of the active PITR profile, and only while a roll is in progress
// (at least one site Deployment carries a decodable render and the render
// is not deferred). Under the pre-#203 deferral (some site Deployment has
// no decodable render) nothing ends the disagreement without the user, so
// the backup runs unstamped instead of pending indefinitely.
func pitrBackupWaitsForRollout(fg *v1alpha1.MysqlFailoverGroup, profileName string, deploys []appsv1.Deployment) bool {
	if !pitrEnabled(fg) || fg.Spec.Backup.PITR.ProfileName != profileName {
		return false
	}
	_, kind := groupPITRHold(fg, deploys)
	_, action := resolvePITRRender(fg, deploys)
	return kind == pitrHeld && action != pitrRenderDefer
}

// pitrReadBinding returns the binding a restore or verification reading
// profileName's archive uses. The error is an *errPITRBindingNotReady unless
// the entry is Bound/Bound (§4.7, Δ22), or a plain error when the profile
// is missing or its destination is invalid. With no entry and a full list,
// the reason is BindingLimitReached (no entry can be made for it).
func pitrReadBinding(fg *v1alpha1.MysqlFailoverGroup, profileName string) (*v1alpha1.PITRArchiveBinding, error) {
	profile := findProfile(fg, profileName)
	if profile == nil {
		return nil, fmt.Errorf("pitr profile %q not found in spec.backup.profiles", profileName)
	}
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return nil, err
	}
	entry := findPITRBinding(fg, dest)
	if notReady := pitrBindingReadiness(entry); notReady != nil {
		// Only Bound entries surely hold their slot (only a release
		// removes them); while unused non-Bound entries fill the list,
		// the next binding pass drops them and makes room, so the reader
		// waits (U10).
		switch {
		case entry != nil || len(fg.Status.PITRArchiveBindings) < pitrArchiveBindingsMax:
		case pitrBindingSlotsTaken(fg) >= pitrArchiveBindingsMax:
			notReady.reason = reasonPITRBindingLimitReached
			notReady.message = fmt.Sprintf("the group already has %d Bound archive bindings; release one (annotation %s) so %s can be bound",
				pitrArchiveBindingsMax, ReleasePITRArchiveBindingAnnotation, dest.String())
		default:
			notReady.message = fmt.Sprintf("the binding list is full of entries that are not Bound; waiting for the group's binding pass to make room for %s", dest.String())
		}
		return entry, notReady
	}
	return entry, nil
}

// pitrProfileReadOnly reports whether profileName may only be read, never
// written by backups or archived to (D-10, Δ23), with the reason: it is the
// initFromBackup archive profile while that restore has not succeeded, or
// its destination's binding names another group as the archive's source,
// or was refused as foreign or a copy, or it is the initFromBackup archive
// profile of a restore that succeeded and its destination has no binding
// (U19: a plaintext read binding is not rebuilt after a status loss, and
// whose archive it is must be established again, by the writer check of a
// write target, before anything writes there).
func pitrProfileReadOnly(fg *v1alpha1.MysqlFailoverGroup, profileName string) (bool, string) {
	if ifb := fg.Spec.InitFromBackup; ifb != nil && ifb.PointInTime != nil && ifb.PointInTime.ArchiveProfileName == profileName &&
		(fg.Status.Restore == nil || fg.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded) {
		return true, fmt.Sprintf("profile %q is the initFromBackup point-in-time archive profile, which is read-only until the restore succeeds", profileName)
	}
	profile := findProfile(fg, profileName)
	if profile == nil {
		return false, ""
	}
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return false, ""
	}
	entry := findPITRBinding(fg, dest)
	if entry == nil {
		if ifb := fg.Spec.InitFromBackup; ifb != nil && ifb.PointInTime != nil && ifb.PointInTime.ArchiveProfileName == profileName {
			return true, fmt.Sprintf("profile %q is the initFromBackup point-in-time archive profile and %s has no archive binding: the restore read another group's archive there, so it stays read-only until a binding shows whose archive it is",
				profileName, dest.String())
		}
		return false, ""
	}
	if entry.Source != nil && entry.Source.UID != "" && entry.Source.UID != fg.UID {
		return true, fmt.Sprintf("profile %q resolves to %s, an archive created by group %s/%s; it is read-only for this group",
			profileName, dest.String(), entry.Source.Namespace, entry.Source.Name)
	}
	if entry.State == v1alpha1.PITRArchiveBindingRefused &&
		(entry.Reason == pitrBindingArchiveForeign || entry.Reason == pitrBindingArchiveCopy) {
		return true, fmt.Sprintf("profile %q resolves to %s, whose archive binding was refused (%s); it is read-only for this group",
			profileName, dest.String(), entry.Reason)
	}
	return false, ""
}

// PITRRetentionArchiveAllowed reports whether archive is the archive ID fg
// creates at the destination of one of its bindings (Δ12): the /pitr-cutoff
// gate. The ID is derived from fg's UID, so another group's archive, or a
// copy, never matches.
func PITRRetentionArchiveAllowed(fg *v1alpha1.MysqlFailoverGroup, archive string) bool {
	if fg == nil || fg.UID == "" || !pitrarchive.ValidArchiveID(archive) {
		return false
	}
	for i := range fg.Status.PITRArchiveBindings {
		dest, err := pitrarchive.ParseDestination(fg.Status.PITRArchiveBindings[i].Destination)
		if err != nil {
			continue
		}
		if pitrarchive.CreateArchiveID(fg.UID, dest) == archive {
			return true
		}
	}
	return false
}

// consumedSecretKeys maps a Secret name to the data keys a consumer reads
// from it.
type consumedSecretKeys map[string][]string

func (c consumedSecretKeys) add(name string, keys ...string) {
	if name == "" {
		return
	}
	for _, k := range keys {
		if !containsString(c[name], k) {
			c[name] = append(c[name], k)
		}
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// credentialRoleSecretNames is the Secret keys the MySQL pods read in
// credentials mode (Δ15, O10): username and password of the five role
// Secrets, and the operator Secret's MYSQL_ROOT_PASSWORD. Nothing else of
// those Secrets (a passphrase sharing one, say) enters the pod hash.
func credentialRoleSecretNames(fg *v1alpha1.MysqlFailoverGroup) consumedSecretKeys {
	out := consumedSecretKeys{}
	c := fg.Spec.Credentials
	if c == nil {
		return out
	}
	out.add(c.OperatorSecret, "username", "password", "MYSQL_ROOT_PASSWORD")
	for _, name := range []string{c.AppSecret, c.ReadOnlySecret, c.MonitorSecret, c.BackupSecret} {
		out.add(name, "username", "password")
	}
	return out
}

// credentialHashSecretKeys is what reconcileCredentials consumes (Δ15): the
// role keys of credentialRoleSecretNames, the operator Secret's
// replication account (a reserved principal), and in legacy mode the dsn of
// spec.secretName. No passphrase or decryption Secret enters it.
func credentialHashSecretKeys(fg *v1alpha1.MysqlFailoverGroup) consumedSecretKeys {
	out := credentialRoleSecretNames(fg)
	if c := fg.Spec.Credentials; c != nil {
		out.add(c.OperatorSecret, "MYSQL_REPLICATION_USER", "MYSQL_REPLICATION_PASSWORD")
	}
	out.add(fg.Spec.SecretName, "dsn")
	return out
}

// mysqlTLSConsumedKeys are the TLS Secret keys the pods mount (K10).
var mysqlTLSConsumedKeys = []string{"ca.crt", "tls.crt", "tls.key"}

// pickSecretKeys returns the listed keys of data that are present.
func pickSecretKeys(data map[string][]byte, keys []string) map[string][]byte {
	out := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if v, ok := data[k]; ok {
			out[k] = v
		}
	}
	return out
}

// groupPITRRender is resolvePITRRender's verdict for one group pass.
type groupPITRRender struct {
	inputs pitrRenderInputs
	action pitrRenderAction
	// existing are the names of the group's Deployments when resolved.
	existing map[string]bool
}

// defers reports whether the site Deployment deployName exists and the
// render is deferred: neither it nor the site's ConfigMap is touched
// (§4.6 row 4d, U09). A site being created renders.
func (g groupPITRRender) defers(deployName string) bool {
	return g.action == pitrRenderDefer && g.existing[deployName]
}

// resolveGroupPITRRender lists fg's Deployments through the APIReader and
// resolves the group's render once (§4.6). It logs, on change only, WARN
// "pitr render annotation ignored" for a site Deployment whose annotation
// does not decode, and INFO "pitr render deferred" while row 4d holds the
// existing Deployments.
//
// A group whose PITR is disabled (or whose active profile does not
// resolve) renders {enabled:false} whatever its Deployments carry, so no
// Deployment is listed (U63).
func (r *MysqlFailoverGroupReconciler) resolveGroupPITRRender(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (groupPITRRender, error) {
	if _, _, ok := activePITRProfile(fg); !ok {
		r.forgetPITRRenderLog(fg)
		return groupPITRRender{inputs: pitrRenderDisabled(), action: pitrRenderApply}, nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	deploys, err := listPITRSiteDeployments(ctx, reader, fg)
	if err != nil {
		return groupPITRRender{}, err
	}
	in, action := resolvePITRRender(fg, deploys)
	r.logPITRRender(fg, deploys, action)
	existing := make(map[string]bool, len(deploys))
	for i := range deploys {
		existing[deploys[i].Name] = true
	}
	return groupPITRRender{inputs: in, action: action, existing: existing}, nil
}

// pitrRenderLogState is what logPITRRender last logged for a group.
type pitrRenderLogState struct {
	ignored  map[string]string // site -> error
	deferred string            // sorted site list, "" when not deferred
}

// logPITRRender writes the render log lines when they change.
func (r *MysqlFailoverGroupReconciler) logPITRRender(fg *v1alpha1.MysqlFailoverGroup, deploys []appsv1.Deployment, action pitrRenderAction) {
	logger := r.licenseLogger()
	key := fg.Namespace + "/" + fg.Name + "/" + string(fg.UID)
	next := pitrRenderLogState{ignored: map[string]string{}}
	var deferredSites []string
	for _, sr := range sitePITRRenders(fg, deploys) {
		if sr.Err == nil {
			continue
		}
		deferredSites = append(deferredSites, sr.Site)
		if errors.Is(sr.Err, errPITRRenderAbsent) {
			continue
		}
		next.ignored[sr.Site] = sr.Err.Error()
	}
	if action == pitrRenderDefer {
		sort.Strings(deferredSites)
		next.deferred = strings.Join(deferredSites, ",")
	}

	r.pitrRenderLogMu.Lock()
	defer r.pitrRenderLogMu.Unlock()
	if r.pitrRenderLog == nil {
		r.pitrRenderLog = map[string]pitrRenderLogState{}
	}
	prev := r.pitrRenderLog[key]
	for site, msg := range next.ignored {
		if prev.ignored[site] != msg {
			logger.Warn(logMsgPITRRenderIgnored, "fg", fg.Name, "namespace", fg.Namespace, "site", site, "error", msg)
		}
	}
	if next.deferred != "" && next.deferred != prev.deferred {
		logger.Info(logMsgPITRRenderDeferred, "fg", fg.Name, "namespace", fg.Namespace, "sites", next.deferred)
	}
	// An empty state is never stored (U27): nothing to compare against.
	if len(next.ignored) == 0 && next.deferred == "" {
		delete(r.pitrRenderLog, key)
		return
	}
	r.pitrRenderLog[key] = next
}

// forgetGroupMemos drops the per-group memory of the PITR render log and
// the schedule refusals of the group nn, every incarnation (a deleted
// group, U27).
func (r *MysqlFailoverGroupReconciler) forgetGroupMemos(nn types.NamespacedName) {
	r.forgetScheduleRefusals(nn, "", nil)
	prefix := nn.Namespace + "/" + nn.Name + "/"
	r.pitrRenderLogMu.Lock()
	defer r.pitrRenderLogMu.Unlock()
	for key := range r.pitrRenderLog {
		if strings.HasPrefix(key, prefix) {
			delete(r.pitrRenderLog, key)
		}
	}
}

// forgetPITRRenderLog drops what logPITRRender remembers of fg (a group
// that renders nothing).
func (r *MysqlFailoverGroupReconciler) forgetPITRRenderLog(fg *v1alpha1.MysqlFailoverGroup) {
	r.pitrRenderLogMu.Lock()
	defer r.pitrRenderLogMu.Unlock()
	delete(r.pitrRenderLog, fg.Namespace+"/"+fg.Name+"/"+string(fg.UID))
}
