package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// encryptionRequeue is how often the reconciler re-checks a site that is
// mid-lifecycle. Sealing waits on a pod roll and on the sidecar's escrow
// push, neither of which produces a watch event on the MysqlFailoverGroup.
const encryptionRequeue = 10 * time.Second

// keyringStatusFetcher reads the live keyring view from a site's
// sidecar. Injected so tests can drive the state machine without a
// cluster.
type keyringStatusFetcher func(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) (*mysql.KeyringStatus, error)

// defaultKeyringStatusFetcher talks to the site's internal Service.
func defaultKeyringStatusFetcher(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) (*mysql.KeyringStatus, error) {
	host := internalSiteServiceHost(fg.Name, site, fg.Namespace)
	c := mysql.NewSidecarClient(fmt.Sprintf("http://%s:%d", host, sidecarPort))
	return c.GetKeyringStatus(ctx)
}

// reconcileEncryptionAtRest drives the per-site keyring lifecycle.
//
// The state machine is deliberately conservative in one direction only:
// it will happily leave a site unsealed forever if it cannot prove the
// keyring is durably escrowed, but it will never seal a site against a
// Secret whose contents it has not read back and matched against the
// live keyring digest the sidecar reports. Sealing a site against the
// wrong keyring would make that site unrecoverable on its next restart.
//
// Returns the requeue interval the caller should honour (0 = no
// encryption work pending).
func (r *MysqlFailoverGroupReconciler) reconcileEncryptionAtRest(
	ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
) (time.Duration, error) {
	logger := log.FromContext(ctx)

	if !fg.Spec.EncryptionEnabled() {
		// Turning encryption off does not decrypt anything, so this is
		// only a status cleanup. The escrow Secrets stay: deleting them
		// would strand any data still encrypted on disk.
		if fg.Status.EncryptionAtRest != nil {
			for i := range fg.Status.EncryptionAtRest.Sites {
				metrics.DeleteKeyringSiteMetrics(fg.Namespace, fg.Name, fg.Status.EncryptionAtRest.Sites[i].Name)
			}
			fg.Status.EncryptionAtRest = nil
			if err := r.patchEncryptionStatus(ctx, fg); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	// Adoption guard. Turning encryption on for a group that is already
	// serving means the existing tablespaces stay plaintext: MySQL
	// encrypts what is created after the fact, it does not rewrite what
	// is already on disk. Silently accepting the flag would hand the
	// admin a cluster that reports "encrypted" while most of the data on
	// the PVC is not, so the default is to refuse and point at the
	// supported procedure.
	if fg.Status.EncryptionAtRest == nil && fg.Status.ActiveSite != "" &&
		fg.Annotations[AdoptEncryptionAnnotation] != "confirm" {
		logger.Info("refusing to enable encryption at rest on a live failover group",
			"activeSite", fg.Status.ActiveSite)
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "EncryptionAdoptionRefused",
			"spec.encryptionAtRest.enabled was turned on for a group that is already serving from site %q. "+
				"Existing tablespaces stay plaintext — MySQL only encrypts data written after the fact. "+
				"Supported path: bring up a replica on an encrypted group and planned-failover onto it, or "+
				"acknowledge partial coverage with the annotation %s=confirm.",
			fg.Status.ActiveSite, AdoptEncryptionAnnotation)
		setEncryptionRefusedCondition(fg)
		if err := r.patchEncryptionStatus(ctx, fg); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if fg.Status.EncryptionAtRest == nil {
		fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{}
	}
	st := fg.Status.EncryptionAtRest
	st.ObservedGeneration = fg.Generation

	// Reconcile the site list so a site added to spec.sites gets an
	// entry (and a removed one stops being reported).
	activeSites := make(map[string]struct{}, len(fg.Spec.Sites))
	for _, name := range fg.Spec.SiteNames() {
		activeSites[name] = struct{}{}
	}
	for i := range st.Sites {
		if _, ok := activeSites[st.Sites[i].Name]; !ok {
			metrics.DeleteKeyringSiteMetrics(fg.Namespace, fg.Name, st.Sites[i].Name)
		}
	}
	st.Sites = alignSiteEncryptionStatus(st.Sites, fg.Spec.SiteNames())

	rotateTarget := strings.TrimSpace(fg.Annotations[RotateKeyringAnnotation])
	store := &keyringEscrowStore{client: r.Client, scheme: r.Scheme}
	fetch := r.keyringStatus
	if fetch == nil {
		fetch = defaultKeyringStatusFetcher
	}

	requeue := time.Duration(0)
	allSealed := true

	for i := range st.Sites {
		site := &st.Sites[i]
		before := *site

		wantRequeue, err := r.reconcileSiteKeyring(ctx, fg, site, store, fetch, rotateTarget)
		if err != nil {
			return 0, err
		}
		if wantRequeue {
			requeue = encryptionRequeue
		}
		if site.Phase != v1alpha1.KeyringPhaseSealed {
			allSealed = false
		}
		if before.Phase != site.Phase {
			logger.Info("keyring phase transition",
				"site", site.Name, "from", before.Phase, "to", site.Phase,
				"reason", site.UnsealReason, "version", site.KeyringVersion)
			r.Recorder.Eventf(fg, eventTypeForKeyringPhase(site.Phase), "KeyringPhase",
				"site %s keyring %s -> %s (%s)", site.Name, before.Phase, site.Phase, site.Message)
		}
		publishKeyringMetrics(fg.Namespace, fg.Name, site)
	}

	st.Sealed = allSealed

	// Clear a consumed rotation annotation once the target site is back
	// to Sealed, so the admin can trigger the next rotation without
	// having to remove the annotation by hand.
	if rotateTarget != "" {
		if s := st.SiteEncryptionStatusByName(rotateTarget); s != nil && s.Phase == v1alpha1.KeyringPhaseSealed && s.UnsealReason == "" {
			if err := r.clearRotateAnnotation(ctx, fg); err != nil {
				logger.Error(err, "failed to clear rotate-keyring annotation")
			}
		}
	}

	setEncryptionCondition(fg)
	if err := r.patchEncryptionStatus(ctx, fg); err != nil {
		return 0, err
	}
	return requeue, nil
}

// reconcileSiteKeyring advances one site. Returns true when the site is
// mid-lifecycle and the caller should requeue.
func (r *MysqlFailoverGroupReconciler) reconcileSiteKeyring(
	ctx context.Context,
	fg *v1alpha1.MysqlFailoverGroup,
	site *v1alpha1.SiteEncryptionStatus,
	store *keyringEscrowStore,
	fetch keyringStatusFetcher,
	rotateTarget string,
) (bool, error) {
	now := metav1.Now()

	if site.Phase == "" {
		site.Phase = v1alpha1.KeyringPhasePending
		site.UnsealReason = v1alpha1.UnsealReasonBootstrap
		site.UnsealedSince = &now
		site.Message = "awaiting initial keyring creation"
	}

	// A site that is not sealed needs a live escrow token; a sealed one
	// must not have one mounted, but the Secret itself is kept so a
	// later unseal does not have to wait a reconcile for minting.
	if err := r.ensureEscrowToken(ctx, fg, site.Name); err != nil {
		return false, err
	}

	// An admin-requested rotation on a site that is currently sealed is
	// the only transition that moves backwards out of Sealed.
	if site.Phase == v1alpha1.KeyringPhaseSealed && rotateTarget == site.Name {
		if ok, why := r.rotationAllowed(fg, site.Name); !ok {
			site.Message = why
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringRotationRefused",
				"refusing to rotate keyring on site %s: %s", site.Name, why)
			return false, nil
		}
		site.Phase = v1alpha1.KeyringPhaseUnsealed
		site.UnsealReason = v1alpha1.UnsealReasonRotation
		site.UnsealedSince = &now
		site.Message = "unsealing for admin-triggered master key rotation"
		return true, nil
	}

	switch site.Phase {
	case v1alpha1.KeyringPhaseFailed:
		// A site that failed while sealed keeps the sealed rendering
		// (see SiteKeyringSealed), so it cannot make progress down the
		// unsealed path — its Deployment will never roll onto a writable
		// keyring and the site would sit at "waiting for the Deployment to
		// roll" forever. Re-run the sealed drift check instead so the
		// documented recovery (restore the escrow Secret from the live
		// keyring while the pod is still up) actually converges.
		if fg.SiteKeyringSealed(site.Name) {
			return r.verifySealedSite(ctx, fg, site, store, fetch)
		}
		return r.advanceUnsealedSite(ctx, fg, site, store, fetch)

	case v1alpha1.KeyringPhasePending, v1alpha1.KeyringPhaseUnsealed:
		return r.advanceUnsealedSite(ctx, fg, site, store, fetch)

	case v1alpha1.KeyringPhaseEscrowed:
		return r.advanceEscrowedSite(ctx, fg, site, fetch)

	case v1alpha1.KeyringPhaseSealed:
		return r.verifySealedSite(ctx, fg, site, store, fetch)
	}
	return false, nil
}

// advanceUnsealedSite waits for the sidecar to escrow the live keyring
// and for the operator to independently confirm the stored bytes.
func (r *MysqlFailoverGroupReconciler) advanceUnsealedSite(
	ctx context.Context,
	fg *v1alpha1.MysqlFailoverGroup,
	site *v1alpha1.SiteEncryptionStatus,
	store *keyringEscrowStore,
	fetch keyringStatusFetcher,
) (bool, error) {
	now := metav1.Now()
	if site.Phase != v1alpha1.KeyringPhaseUnsealed && site.Phase != v1alpha1.KeyringPhaseFailed {
		site.Phase = v1alpha1.KeyringPhaseUnsealed
		if site.UnsealedSince == nil {
			site.UnsealedSince = &now
		}
	}
	ready, err := r.deploymentReadyWithUnsealedKeyring(ctx, fg, site.Name)
	if err != nil {
		return false, err
	}
	if !ready {
		site.Message = "waiting for the Deployment to roll onto the writable keyring rendering"
		return true, nil
	}

	live, err := fetch(ctx, fg, site.Name)
	if err != nil {
		site.Message = fmt.Sprintf("waiting for sidecar: %v", err)
		return true, nil
	}
	if !live.Enabled {
		site.Message = "sidecar reports encryption disabled; waiting for the pod to roll onto the encrypted rendering"
		return true, nil
	}
	if live.Digest == "" {
		site.Message = withSidecarError("waiting for MySQL to create the keyring", live.LastError)
		return checkEscrowDeadline(fg, site), nil
	}
	if site.UnsealReason == v1alpha1.UnsealReasonRotation && !live.RotateDone {
		site.Message = "waiting for MySQL to complete the requested keyring rotation"
		if live.RotateError != "" {
			site.Message = fmt.Sprintf("keyring rotation failed; waiting for retry: %s", live.RotateError)
		}
		return checkEscrowDeadline(fg, site), nil
	}
	if site.UnsealReason == v1alpha1.UnsealReasonRotation && live.Digest == site.KeyringDigest {
		site.Message = "waiting for MySQL to rotate away from the previously escrowed keyring"
		return checkEscrowDeadline(fg, site), nil
	}

	// The operator does not take the sidecar's word for it: re-read the
	// escrow Secret and hash its contents. This is what turns "the
	// sidecar said it pushed" into "the bytes MySQL is using are in a
	// Secret I just read".
	current, ok, err := store.current(ctx, fg, site.Name)
	if err != nil {
		return false, err
	}
	if !ok {
		site.Message = withSidecarError("waiting for the sidecar to escrow the keyring", live.LastError)
		return checkEscrowDeadline(fg, site), nil
	}
	if current.Digest != live.Digest {
		site.Message = withSidecarError(fmt.Sprintf(
			"escrowed keyring (v%d) does not match the live keyring; waiting for a fresh escrow",
			current.Version), live.LastError)
		return checkEscrowDeadline(fg, site), nil
	}

	site.Phase = v1alpha1.KeyringPhaseEscrowed
	site.KeyringSecret = current.Name
	site.KeyringVersion = current.Version
	site.KeyringDigest = current.Digest
	site.LastEscrowTime = &now
	site.Message = "keyring escrowed and verified; sealing"

	if err := store.prune(ctx, fg, site.Name, current.Name); err != nil {
		// Retention is housekeeping — never let it block sealing.
		log.FromContext(ctx).Error(err, "keyring retention prune failed", "site", site.Name)
	}
	return true, nil
}

// maxSidecarErrorInStatus caps how much of the sidecar's error text is
// copied into status.  Long enough for the actionable part of an x509 or
// dial error, short enough to keep the status field readable.
const maxSidecarErrorInStatus = 200

// withSidecarError appends the sidecar's own last error to a waiting
// message. Escrow HTTP failures contain only the status code: the sidecar
// deliberately strips response bodies before LastError crosses this status
// boundary because an upstream error page may contain sensitive material.
//
// A stalled escrow is otherwise indistinguishable from a slow one: the
// operator polls /keyring/status, which already carries the reason the
// push is failing (escrow listener not enabled, CA that does not trust
// its issuer, unreadable token), but the state machine used to drop it.
// The result was a group sitting at "waiting for the sidecar to escrow
// the keyring" with the actual cause only in sidecar logs.
func withSidecarError(msg, lastError string) string {
	lastError = strings.TrimSpace(lastError)
	if lastError == "" {
		return msg
	}
	if len(lastError) > maxSidecarErrorInStatus {
		lastError = lastError[:maxSidecarErrorInStatus] + "…"
	}
	return msg + "; sidecar reports: " + lastError
}

// advanceEscrowedSite waits for the Deployment to actually be running
// the sealed rendering before declaring the site sealed.
func (r *MysqlFailoverGroupReconciler) advanceEscrowedSite(
	ctx context.Context,
	fg *v1alpha1.MysqlFailoverGroup,
	site *v1alpha1.SiteEncryptionStatus,
	fetch keyringStatusFetcher,
) (bool, error) {
	var deploy appsv1.Deployment
	key := types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name)}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	if err := reader.Get(ctx, key, &deploy); err != nil {
		if apierrors.IsNotFound(err) {
			site.Message = "waiting for the site Deployment to be created"
			return true, nil
		}
		return false, fmt.Errorf("get deployment for keyring seal %s: %w", site.Name, err)
	}

	if !deploymentRendersSealedKeyring(&deploy) {
		site.Message = "waiting for the Deployment to roll onto the sealed keyring rendering"
		return true, nil
	}
	if deploy.Status.ObservedGeneration < deploy.Generation || deploy.Status.ReadyReplicas < 1 {
		site.Message = "waiting for the sealed pod to become ready"
		return true, nil
	}

	// Final confirmation from MySQL itself: the keyring component must
	// report Read_only. Without this a rendering bug (wrong plugin dir,
	// stale ConfigMap) could leave a writable keyring behind a Secret
	// mount and the operator would call it sealed.
	live, err := fetch(ctx, fg, site.Name)
	if err != nil {
		site.Message = fmt.Sprintf("waiting for sidecar to confirm the seal: %v", err)
		return true, nil
	}
	if live.Component == nil || !live.Component.ReadOnly {
		site.Message = "MySQL does not report a read-only keyring yet"
		return true, nil
	}
	if live.Digest != "" && live.Digest != site.KeyringDigest {
		site.Phase = v1alpha1.KeyringPhaseFailed
		site.Message = fmt.Sprintf(
			"sealed pod is running a keyring that does not match escrow %s; refusing to declare it sealed",
			site.KeyringSecret)
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringDigestMismatch",
			"site %s: live keyring digest does not match escrow %s", site.Name, site.KeyringSecret)
		return true, nil
	}

	site.Phase = v1alpha1.KeyringPhaseSealed
	site.UnsealReason = ""
	site.UnsealedSince = nil
	site.Message = fmt.Sprintf("sealed against %s", site.KeyringSecret)
	applyCoverage(site, live)
	return false, nil
}

// verifySealedSite is the steady-state drift check.
func (r *MysqlFailoverGroupReconciler) verifySealedSite(
	ctx context.Context,
	fg *v1alpha1.MysqlFailoverGroup,
	site *v1alpha1.SiteEncryptionStatus,
	store *keyringEscrowStore,
	fetch keyringStatusFetcher,
) (bool, error) {
	// The escrow Secret is the site's only copy of its keys. If it has
	// gone missing the site is one pod restart away from being
	// unrecoverable, so this is a loud failure rather than a quiet one.
	var sec corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: site.KeyringSecret}, &sec)
	if apierrors.IsNotFound(err) {
		site.Phase = v1alpha1.KeyringPhaseFailed
		site.Message = fmt.Sprintf("escrow Secret %s is missing", site.KeyringSecret)
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringEscrowMissing",
			"site %s: escrow Secret %s no longer exists; this site cannot restart", site.Name, site.KeyringSecret)
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get escrow secret %s: %w", site.KeyringSecret, err)
	}
	if got := keyringDigest(sec.Data[v1alpha1.KeyringDataFileName]); got != site.KeyringDigest {
		site.Phase = v1alpha1.KeyringPhaseFailed
		site.Message = fmt.Sprintf("escrow Secret %s no longer matches the recorded digest", site.KeyringSecret)
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringEscrowCorrupt",
			"site %s: escrow Secret %s digest changed", site.Name, site.KeyringSecret)
		return true, nil
	}

	// Sample coverage opportunistically. A sidecar that is briefly
	// unreachable must not knock a healthy site out of Sealed.
	//
	// confirmed records that MySQL itself vouched for the escrow this
	// reconcile: the component is read-only AND the keyring it has open
	// hashes to the version the operator recorded. Only that combination
	// is strong enough to bring a Failed site back to Sealed.
	confirmed := false
	if live, err := fetch(ctx, fg, site.Name); err == nil && live.Enabled {
		applyCoverage(site, live)
		if live.Component != nil && !live.Component.ReadOnly {
			site.Phase = v1alpha1.KeyringPhaseFailed
			site.Message = "MySQL reports a writable keyring on a site the operator considers sealed"
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringNotReadOnly",
				"site %s: keyring component is writable but the site is marked sealed", site.Name)
			return true, nil
		}
		// A sealed pod running a keyring that is not the escrowed one is
		// the stale-projection failure: the Deployment was rolled back
		// onto a superseded version, or status advanced without the pod
		// following. Tablespaces rewrapped under the newer master key
		// will not decrypt, so this has to be loud rather than silently
		// tolerated by the steady-state check.
		if live.Digest != "" && live.Digest != site.KeyringDigest {
			site.Phase = v1alpha1.KeyringPhaseFailed
			site.Message = fmt.Sprintf(
				"the running keyring does not match escrow %s; the pod may be projecting a superseded version",
				site.KeyringSecret)
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringDigestMismatch",
				"site %s: live keyring digest does not match escrow %s", site.Name, site.KeyringSecret)
			return true, nil
		}
		confirmed = live.Digest != "" && live.Digest == site.KeyringDigest
	}

	// Recovery from Failed. Reaching here means the escrow Secret exists
	// again and still hashes to the recorded digest; requiring MySQL to
	// confirm the same bytes as well is what keeps "restore the Secret"
	// from silently re-blessing a site whose keys have since diverged.
	if site.Phase == v1alpha1.KeyringPhaseFailed {
		if !confirmed {
			site.Message = fmt.Sprintf(
				"escrow %s is present again; waiting for MySQL to confirm it is running that keyring",
				site.KeyringSecret)
			return true, nil
		}
		site.Phase = v1alpha1.KeyringPhaseSealed
		site.UnsealReason = ""
		site.UnsealedSince = nil
		site.Message = fmt.Sprintf("sealed against %s", site.KeyringSecret)
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "KeyringRecovered",
			"site %s recovered to Sealed against escrow %s", site.Name, site.KeyringSecret)
	}

	// Housekeeping only — safe to ignore failures.
	_ = store.prune(ctx, fg, site.Name, site.KeyringSecret)
	return false, nil
}

// checkEscrowDeadline flips a stuck site to Failed once
// keyring.escrowTimeoutSeconds has elapsed.
//
// This is a reporting decision, not a safety one. The site stays
// unsealed either way and the operator keeps retrying — the point is to
// make a site that silently never escrows show up as a loud condition
// and a firing alert instead of sitting in Unsealed forever.
func checkEscrowDeadline(fg *v1alpha1.MysqlFailoverGroup, site *v1alpha1.SiteEncryptionStatus) bool {
	if site.UnsealedSince == nil {
		now := metav1.Now()
		site.UnsealedSince = &now
		return true
	}
	if time.Since(site.UnsealedSince.Time) > fg.Spec.EscrowTimeout() {
		site.Phase = v1alpha1.KeyringPhaseFailed
		site.Message = fmt.Sprintf(
			"keyring not escrowed within %s: %s", fg.Spec.EscrowTimeout(), site.Message)
	}
	return true
}

// rotationAllowed refuses to rotate the active primary's keyring.
//
// Rotation is the one operation that writes a new master key while real
// data already depends on it, so it is the one operation with a window
// where losing the pod before escrow completes would strand data. On a
// replica that window is harmless — the site is re-cloneable from the
// primary. On the primary it is not. The supported procedure is to
// rotate the replicas, run a planned failover, then rotate the ex-primary.
func (r *MysqlFailoverGroupReconciler) rotationAllowed(fg *v1alpha1.MysqlFailoverGroup, site string) (bool, string) {
	if fg.Status.ActiveSite == site {
		return false, "site is the active primary; rotate replicas first, then run a planned failover and rotate this site once it is a replica"
	}
	if fg.Status.ActiveSite == "" {
		return false, "no active primary is known; refusing to rotate while the topology is unsettled"
	}
	if fg.Status.UpdatePhase != "" {
		return false, "an ordered update is in progress"
	}
	if fg.Status.PlannedFailover != nil && !plannedFailoverTerminal(fg.Status.PlannedFailover) {
		return false, "a planned failover is in progress"
	}
	return true, ""
}

// RequestKeyringUnseal marks a site as needing a writable keyring before
// a CLONE INSTANCE can run against it. A clone recipient re-encrypts the
// donor's tablespace keys under its own new master key, which a
// read-only keyring cannot accept — MySQL fails the clone rather than
// silently producing an unreadable data directory.
//
// Returns true when the site is already unsealed and the clone may
// proceed. Callers that get false must retry after the pod has rolled.
func (r *MysqlFailoverGroupReconciler) RequestKeyringUnseal(
	ctx context.Context, nn types.NamespacedName, site string,
) (bool, error) {
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, nn, &fg); err != nil {
		return false, fmt.Errorf("get failover group for keyring unseal: %w", err)
	}
	if !fg.Spec.EncryptionEnabled() {
		return true, nil
	}
	if fg.Status.EncryptionAtRest == nil {
		return false, nil
	}
	s := fg.Status.EncryptionAtRest.SiteEncryptionStatusByName(site)
	if s == nil {
		return false, nil
	}
	switch s.Phase {
	case v1alpha1.KeyringPhaseUnsealed, v1alpha1.KeyringPhasePending:
		return r.deploymentReadyWithUnsealedKeyring(ctx, &fg, site)
	}

	now := metav1.Now()
	if s.KeyringSecret != "" {
		var seed corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: s.KeyringSecret}, &seed)
		switch {
		case apierrors.IsNotFound(err):
			// An explicitly confirmed reclone is the recovery path after the
			// old keyring is irretrievably lost. Do not render the missing
			// Secret as a seed; the replacement PVC starts with a fresh
			// keyring and CLONE INSTANCE rewraps the donor's tablespace keys.
			s.KeyringSecret = ""
			s.KeyringVersion = 0
			s.KeyringDigest = ""
			s.LastEscrowTime = nil
		case err != nil:
			return false, fmt.Errorf("check keyring seed before clone unseal: %w", err)
		}
	}
	s.Phase = v1alpha1.KeyringPhaseUnsealed
	s.UnsealReason = v1alpha1.UnsealReasonClone
	s.UnsealedSince = &now
	s.Message = "unsealed so CLONE INSTANCE can rewrap tablespace keys under a new master key"
	fg.Status.EncryptionAtRest.Sealed = false
	setEncryptionCondition(&fg)
	if err := r.patchEncryptionStatus(ctx, &fg); err != nil {
		return false, err
	}
	r.Recorder.Eventf(&fg, corev1.EventTypeNormal, "KeyringUnsealed",
		"site %s unsealed for clone", site)
	return false, nil
}

func (r *MysqlFailoverGroupReconciler) deploymentReadyWithUnsealedKeyring(
	ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string,
) (bool, error) {
	var deploy appsv1.Deployment
	key := types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, site)}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	if err := reader.Get(ctx, key, &deploy); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get deployment for keyring unseal %s: %w", site, err)
	}
	if !deploymentRendersUnsealedKeyring(&deploy) {
		return false, nil
	}
	return deploy.Status.ObservedGeneration >= deploy.Generation &&
		deploy.Status.UpdatedReplicas >= 1 && deploy.Status.ReadyReplicas >= 1, nil
}

// -------------------------------------------------------------------
// helpers
// -------------------------------------------------------------------

// alignSiteEncryptionStatus keeps the status list in sync with
// spec.sites, preserving existing entries and dropping removed sites.
func alignSiteEncryptionStatus(existing []v1alpha1.SiteEncryptionStatus, names []string) []v1alpha1.SiteEncryptionStatus {
	byName := make(map[string]v1alpha1.SiteEncryptionStatus, len(existing))
	for _, s := range existing {
		byName[s.Name] = s
	}
	out := make([]v1alpha1.SiteEncryptionStatus, 0, len(names))
	for _, n := range names {
		if s, ok := byName[n]; ok {
			out = append(out, s)
			continue
		}
		out = append(out, v1alpha1.SiteEncryptionStatus{Name: n})
	}
	return out
}

func applyCoverage(site *v1alpha1.SiteEncryptionStatus, live *mysql.KeyringStatus) {
	if live == nil || (live.Component == nil && live.Coverage == nil) {
		return
	}
	now := metav1.Now()
	cov := site.Coverage
	if cov == nil {
		cov = &v1alpha1.SiteEncryptionCoverage{}
	}
	if live.Component != nil {
		cov.KeyringComponent = live.Component.Name
		cov.KeyringReadOnly = live.Component.ReadOnly
	}
	if live.Coverage != nil {
		cov.SystemTablespaceEncrypted = live.Coverage.SystemTablespaceEncrypted
		cov.UnencryptedTablespaces = live.Coverage.UnencryptedTablespaces
		cov.RedoLogEncrypted = live.Coverage.RedoLogEncrypted
		cov.UndoLogEncrypted = live.Coverage.UndoLogEncrypted
		cov.BinlogEncrypted = live.Coverage.BinlogEncrypted
		cov.LastCheckTime = &now
	}
	site.Coverage = cov
}

func eventTypeForKeyringPhase(p v1alpha1.SiteKeyringPhase) string {
	if p == v1alpha1.KeyringPhaseFailed {
		return corev1.EventTypeWarning
	}
	return corev1.EventTypeNormal
}

func publishKeyringMetrics(namespace, group string, site *v1alpha1.SiteEncryptionStatus) {
	phase := strings.ToLower(string(site.Phase))
	for _, p := range metrics.AllKeyringPhases {
		v := 0.0
		if p == phase {
			v = 1
		}
		metrics.KeyringPhase.WithLabelValues(namespace, group, site.Name, p).Set(v)
	}
	metrics.KeyringEscrowVersion.WithLabelValues(namespace, group, site.Name).Set(float64(site.KeyringVersion))
	if c := site.Coverage; c != nil {
		metrics.EncryptionCoverageGaps.WithLabelValues(namespace, group, site.Name).Set(float64(c.UnencryptedTablespaces))
		for aspect, on := range map[string]bool{
			"system_tablespace": c.SystemTablespaceEncrypted,
			"redo_log":          c.RedoLogEncrypted,
			"undo_log":          c.UndoLogEncrypted,
			"binlog":            c.BinlogEncrypted,
			"keyring_read_only": c.KeyringReadOnly,
		} {
			v := 0.0
			if on {
				v = 1
			}
			metrics.EncryptionCoverageFlag.WithLabelValues(namespace, group, site.Name, aspect).Set(v)
		}
	}
}

// setEncryptionRefusedCondition reports a refused adoption. The
// condition stays False until the admin either reverts the flag or
// acknowledges partial coverage.
func setEncryptionRefusedCondition(fg *v1alpha1.MysqlFailoverGroup) {
	setCondition(&fg.Status.Conditions, metav1.Condition{
		Type:               conditionEncryptionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "AdoptionRefused",
		ObservedGeneration: fg.Generation,
		LastTransitionTime: metav1.Now(),
		Message: fmt.Sprintf(
			"encryption at rest cannot be enabled on a group that is already serving; "+
				"existing tablespaces would stay plaintext. Set %s=confirm to accept partial coverage.",
			AdoptEncryptionAnnotation),
	})
}

// setEncryptionCondition summarizes the subsystem on .status.conditions.
func setEncryptionCondition(fg *v1alpha1.MysqlFailoverGroup) {
	if !fg.Spec.EncryptionEnabled() {
		return
	}
	st := fg.Status.EncryptionAtRest
	if st == nil {
		return
	}

	var pending, failed []string
	for _, s := range st.Sites {
		switch s.Phase {
		case v1alpha1.KeyringPhaseSealed:
		case v1alpha1.KeyringPhaseFailed:
			failed = append(failed, fmt.Sprintf("%s (%s)", s.Name, s.Message))
		default:
			pending = append(pending, fmt.Sprintf("%s (%s)", s.Name, s.Message))
		}
	}

	cond := metav1.Condition{
		Type:               conditionEncryptionReady,
		ObservedGeneration: fg.Generation,
		LastTransitionTime: metav1.Now(),
	}
	switch {
	case len(failed) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "KeyringFailed"
		cond.Message = "keyring lifecycle failed on: " + strings.Join(failed, "; ")
	case len(pending) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "KeyringNotSealed"
		cond.Message = "sites not yet sealed: " + strings.Join(pending, "; ")
	default:
		cond.Status = metav1.ConditionTrue
		cond.Reason = "AllSitesSealed"
		cond.Message = "every site is sealed against an escrowed keyring"
	}
	setCondition(&fg.Status.Conditions, cond)
}

// patchEncryptionStatus writes .status.encryptionAtRest and the
// encryption condition back, retrying on conflict. It re-reads the
// object each attempt so it does not clobber concurrent status writes
// from the topology runner.
func (r *MysqlFailoverGroupReconciler) patchEncryptionStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	desired := fg.Status.EncryptionAtRest.DeepCopy()
	var desiredCond *metav1.Condition
	for i := range fg.Status.Conditions {
		if fg.Status.Conditions[i].Type == conditionEncryptionReady {
			c := fg.Status.Conditions[i]
			desiredCond = &c
			break
		}
	}

	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var latest v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{
			Namespace: fg.Namespace, Name: fg.Name,
		}, &latest); err != nil {
			return err
		}
		latest.Status.EncryptionAtRest = desired.DeepCopy()
		if desiredCond != nil {
			setCondition(&latest.Status.Conditions, *desiredCond)
		}
		return r.Status().Update(ctx, &latest)
	})
}

func (r *MysqlFailoverGroupReconciler) clearRotateAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var latest v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{
			Namespace: fg.Namespace, Name: fg.Name,
		}, &latest); err != nil {
			return err
		}
		if _, ok := latest.Annotations[RotateKeyringAnnotation]; !ok {
			return nil
		}
		delete(latest.Annotations, RotateKeyringAnnotation)
		return r.Update(ctx, &latest)
	})
}
