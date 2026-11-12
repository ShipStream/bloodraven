package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Paced rollouts (issue #244, proposals/23-paced-rollouts.md).
//
// An operator release can change the pod template of every group at once: a
// new launcher, a render-version bump, a new default sidecar image. Such a
// change is "release-driven". The operator rolls it one group at a time:
//
//   - every workload (site Deployment, Dragonfly StatefulSet) carries the
//     renderer that last wrote its pod template (RenderedByAnnotation);
//   - a workload another renderer wrote, whose template the running
//     operator would change, is written only once the group is admitted
//     (status.rollout, written by the RolloutPacer);
//   - the write path and the drift check read that same record, so the
//     ordered updater is never started for a group that is not admitted.
//
// A workload this renderer already wrote is not paced: a change to it comes
// from the group itself (its spec, its Secrets, its keyring or PITR state)
// and rolls as it always has.

// errRolloutHeld is returned by a workload write that was withheld because
// the group is not admitted to the paced rollout. The workload is untouched.
var errRolloutHeld = errors.New("release-driven pod change is held until the group is admitted to the paced rollout (status.rollout)")

var (
	rendererIdentityMu    sync.Mutex
	rendererIdentityKey   string
	rendererIdentityValue string
)

// rendererIdentity identifies the running operator release as a renderer of
// pod templates: a digest of the operator image, the default sidecar image,
// the render versions and the launcher text. The operator image is part of
// it on purpose: a release that changes what a group renders without
// touching a render version is still another renderer, so the change is
// paced. Two builds under one image reference (the playground's
// bloodraven:playground) differ only by the render versions and the
// launcher.
func rendererIdentity() string {
	key := operatorImageFromEnv + "\n" + sidecarImageFromEnv
	rendererIdentityMu.Lock()
	defer rendererIdentityMu.Unlock()
	if rendererIdentityValue != "" && rendererIdentityKey == key {
		return rendererIdentityValue
	}
	h := sha256.New()
	fmt.Fprintf(h, "operatorImage=%s\n", operatorImageFromEnv)
	fmt.Fprintf(h, "sidecarDefault=%s\n", sidecarImageFromEnv)
	fmt.Fprintf(h, "mysqlDefault=%s\n", defaultMySQLImage)
	fmt.Fprintf(h, "deploymentPodRenderVersion=%s\n", deploymentPodRenderVersion)
	fmt.Fprintf(h, "configMapRenderVersion=%s\n", configMapRenderVersion)
	fmt.Fprintf(h, "pitrPodRenderVersion=%s\n", pitrPodRenderVersion)
	fmt.Fprintf(h, "encryptionPodRenderVersion=%s\n", encryptionPodRenderVersion)
	probe := &v1alpha1.MysqlFailoverGroup{}
	for _, tls := range []bool{false, true} {
		command, args := mysqlLauncher(probe, mysqlServerArgs(0, tls))
		fmt.Fprintf(h, "launcher[tls=%t]=%q %q\n", tls, command, args)
	}
	rendererIdentityKey = key
	rendererIdentityValue = hex.EncodeToString(h.Sum(nil))[:16]
	return rendererIdentityValue
}

// renderedByOther reports whether another renderer (an earlier or later
// operator release, or one from before the annotation existed) last wrote
// obj's pod template.
func renderedByOther(obj metav1.Object) bool {
	return obj.GetAnnotations()[v1alpha1.RenderedByAnnotation] != rendererIdentity()
}

// stampRenderedBy records the running renderer on a workload it writes.
func stampRenderedBy(obj metav1.Object) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[v1alpha1.RenderedByAnnotation] = rendererIdentity()
	obj.SetAnnotations(annotations)
}

// rolloutAdmitted reports whether fg's rollout record admits it to roll the
// running renderer's pod templates. A record for another renderer does not:
// the pacer retargets an unfinished admission after an operator upgrade.
func rolloutAdmitted(fg *v1alpha1.MysqlFailoverGroup) bool {
	r := fg.Status.Rollout
	return r.InFlight() && r.Renderer == rendererIdentity()
}

// siteCloneUnsealPending reports whether site is unsealed, or being
// unsealed, to receive a CLONE INSTANCE. Such a site's data is about to be
// replaced and its pod must restart with a writable keyring before the clone
// can run, so its roll is not held for the paced rollout: holding it would
// hold the group's recovery behind every other group's upgrade.
func siteCloneUnsealPending(fg *v1alpha1.MysqlFailoverGroup, site string) bool {
	if !fg.Spec.EncryptionEnabled() {
		return false
	}
	s := fg.Status.EncryptionAtRest.SiteEncryptionStatusByName(site)
	return s != nil && s.UnsealReason == v1alpha1.UnsealReasonClone
}

// rolloutHoldsSite reports whether an existing site Deployment that differs
// from the running render must keep its template: pacing is on, another
// renderer wrote it, and the group is not admitted. The drift check and the
// write path both decide by it.
func rolloutHoldsSite(pacing bool, fg *v1alpha1.MysqlFailoverGroup, site string, deploy *appsv1.Deployment) bool {
	return pacing && renderedByOther(deploy) && !rolloutAdmitted(fg) && !siteCloneUnsealPending(fg, site)
}

// admitRolloutWrite is called before a workload another renderer wrote gets
// the running renderer's template. It reads the group uncached, refuses with
// errRolloutHeld unless the group is admitted, and latches the start of the
// roll (status.rollout.startedTime) before the first such write. The latch
// and a withdrawal of the admission are both conditional on the resource
// version, so an admission is never withdrawn from under a write.
func (r *MysqlFailoverGroupReconciler) admitRolloutWrite(ctx context.Context, nn types.NamespacedName) error {
	if !r.RolloutPacing {
		return nil
	}
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		fresh, err := r.freshFailoverGroup(ctx, nn)
		if err != nil {
			return fmt.Errorf("re-read failover group before a paced rollout write: %w", err)
		}
		if !rolloutAdmitted(fresh) {
			return errRolloutHeld
		}
		if fresh.Status.Rollout.StartedTime != nil {
			return nil
		}
		now := metav1.NewTime(r.now())
		fresh.Status.Rollout.StartedTime = &now
		fresh.Status.Rollout.Phase = v1alpha1.RolloutPhaseRolling
		fresh.Status.Rollout.Reason = rolloutReasonStarted
		fresh.Status.Rollout.Message = "the group's workloads are being rolled to the release's pod template"
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// A CRD older than the operator prunes the record: without a stored
		// latch nothing is rolled.
		if fresh.Status.Rollout == nil || fresh.Status.Rollout.StartedTime == nil {
			return errRolloutHeld
		}
		log.FromContext(ctx).Info("paced rollout: roll started", "fg", nn, "renderer", rendererIdentity())
		return nil
	})
}

// restampWorkload records the running renderer on a workload whose template
// it would not change, with a patch of that one annotation: the template is
// never written here, so a restamp cannot restart a pod.
func (r *MysqlFailoverGroupReconciler) restampWorkload(ctx context.Context, obj client.Object) error {
	base := obj.DeepCopyObject().(client.Object)
	stampRenderedBy(obj)
	return client.IgnoreNotFound(r.Patch(ctx, obj, client.MergeFrom(base)))
}

// gateDeploymentRollout runs before reconcileDeployment writes a site
// Deployment. For a Deployment another renderer wrote it decides between
// three outcomes:
//
//   - the running render would not change it: the renderer annotation alone
//     is patched and done is true (nothing else is to be written);
//   - it would, and the group is admitted: the start of the roll is latched,
//     the site's ConfigMap is written, and the caller writes the Deployment;
//   - it would, and the group is not admitted: errRolloutHeld.
//
// "Would change" is the spec hash or the PITR render for every strategy.
// Under Recreate, whose reconciler writes the whole template on every pass,
// it is also any difference a server-side dry run of the write shows, so a
// release that changes an unhashed part of the template is paced too.
// deploy and mutate are reconcileDeployment's: mutate renders into deploy.
func (r *MysqlFailoverGroupReconciler) gateDeploymentRollout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec,
	deploy *appsv1.Deployment, mutate func() error, specHash, pitrCanonical string, pitr groupPITRRender) (done bool, err error) {
	if !r.RolloutPacing {
		return false, nil
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	key := client.ObjectKeyFromObject(deploy)
	var live appsv1.Deployment
	if err := r.authorityReader().Get(ctx, key, &live); err != nil {
		if apierrors.IsNotFound(err) {
			// A new site's Deployment starts on this renderer.
			return false, nil
		}
		return false, fmt.Errorf("get deployment %s before a paced rollout write: %w", key.Name, err)
	}
	if !renderedByOther(&live) {
		r.noteRolloutTemplatePending(nn, site.Name, false)
		return false, nil
	}
	// mutate leaves these alone (a deferred PITR render, a refused selector).
	if pitr.action == pitrRenderDefer || deploymentSelectorMismatch(&live, fg, site.Name) != nil {
		return false, nil
	}
	pending := live.Annotations[v1alpha1.SpecHashAnnotation] != specHash ||
		live.Annotations[v1alpha1.PITRRenderAnnotation] != pitrCanonical
	templateOnly := false
	if !pending && fg.Spec.UpdateStrategy.EffectiveType() == v1alpha1.UpdateStrategyRecreate {
		templateOnly = r.deploymentTemplateWouldChange(ctx, &live, deploy, mutate)
		pending = templateOnly
	}
	r.noteRolloutTemplatePending(nn, site.Name, templateOnly)
	if !pending {
		return true, r.restampWorkload(ctx, &live)
	}
	if !siteCloneUnsealPending(fg, site.Name) {
		if err := r.admitRolloutWrite(ctx, nn); err != nil {
			return false, err
		}
	}
	// The site's ConfigMap moves with its pod, not before it: the group
	// reconcile skips it while the change is pending (rolloutPendingSites),
	// so a held pod that restarts for another reason still reads the
	// configuration it was rolled with.
	if !pitr.defers(resourceName(fg.Name, site.Name)) {
		if err := r.reconcileSiteConfigMap(ctx, fg, site, pitr.inputs); err != nil {
			return false, fmt.Errorf("reconcile site configmap: %w", err)
		}
	}
	return false, nil
}

// deploymentTemplateWouldChange reports whether writing the running render
// over live would change its pod template, by a server-side dry run of the
// write (so API defaulting does not read as a difference). A dry run that
// fails reads as a change: the write then waits for admission.
func (r *MysqlFailoverGroupReconciler) deploymentTemplateWouldChange(ctx context.Context, live, deploy *appsv1.Deployment, mutate func() error) bool {
	scratch := deploy.DeepCopy()
	defer scratch.DeepCopyInto(deploy)
	live.DeepCopyInto(deploy)
	if err := mutate(); err != nil {
		return true
	}
	candidate := deploy.DeepCopy()
	if err := r.Update(ctx, candidate, client.DryRunAll); err != nil {
		log.FromContext(ctx).V(1).Info("paced rollout: dry run of a deployment write failed; treating the template as changed",
			"deployment", live.Name, "error", err)
		return true
	}
	return !equality.Semantic.DeepEqual(live.Spec.Template, candidate.Spec.Template)
}

// noteRolloutTemplatePending records that a Recreate site's template would
// change although its hash does not, which only the reconciler's dry run
// can tell; the pacer reads it to count the site as pending.
func (r *MysqlFailoverGroupReconciler) noteRolloutTemplatePending(nn types.NamespacedName, site string, pending bool) {
	key := nn.String() + "/" + site
	r.rolloutTemplatePendingMu.Lock()
	defer r.rolloutTemplatePendingMu.Unlock()
	if !pending {
		delete(r.rolloutTemplatePending, key)
		return
	}
	if r.rolloutTemplatePending == nil {
		r.rolloutTemplatePending = make(map[string]bool)
	}
	r.rolloutTemplatePending[key] = true
}

func (r *MysqlFailoverGroupReconciler) rolloutTemplatePendingNoted(nn types.NamespacedName, site string) bool {
	r.rolloutTemplatePendingMu.Lock()
	defer r.rolloutTemplatePendingMu.Unlock()
	return r.rolloutTemplatePending[nn.String()+"/"+site]
}

// siteSpecHash is the spec hash reconcileDeployment would stamp on site's
// Deployment.
func (r *MysqlFailoverGroupReconciler) siteSpecHash(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, pitr pitrRenderInputs) (string, error) {
	digestKey, err := ensureDigestKey(ctx, r.Client, r.APIReader, r.Scheme, fg)
	if err != nil {
		return "", err
	}
	secretInputs, err := specSecretInputs(ctx, r.Client, fg)
	if err != nil {
		return "", err
	}
	return ComputeSpecHash(fg, site, secretContentDigest(digestKey, secretInputs), pitr), nil
}

// rolloutPendingSites reads fg's site Deployments uncached and returns, by
// site name, those another renderer wrote whose hash or PITR render the
// running render would change. The group reconcile leaves the ConfigMap of
// such a site alone (gateDeploymentRollout writes it with the pod), and
// restamps the others another renderer wrote.
func (r *MysqlFailoverGroupReconciler) rolloutPendingSites(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pitr groupPITRRender) (map[string]bool, error) {
	if !r.RolloutPacing {
		return nil, nil
	}
	pending := make(map[string]bool)
	pitrCanonical := pitr.inputs.Canonical()
	for _, site := range fg.Spec.Sites {
		var live appsv1.Deployment
		key := types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name)}
		if err := r.authorityReader().Get(ctx, key, &live); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get deployment %s: %w", key.Name, err)
		}
		if !renderedByOther(&live) || pitr.action == pitrRenderDefer || deploymentSelectorMismatch(&live, fg, site.Name) != nil {
			continue
		}
		hash, err := r.siteSpecHash(ctx, fg, site, pitr.inputs)
		if err != nil {
			return nil, err
		}
		if live.Annotations[v1alpha1.SpecHashAnnotation] != hash || live.Annotations[v1alpha1.PITRRenderAnnotation] != pitrCanonical {
			pending[site.Name] = true
			continue
		}
		// Under Recreate the Deployment write that follows restamps it, or
		// finds a template-only difference by dry run.
		if fg.Spec.UpdateStrategy.EffectiveType() != v1alpha1.UpdateStrategyRecreate {
			if err := r.restampWorkload(ctx, &live); err != nil {
				return nil, fmt.Errorf("restamp deployment %s: %w", key.Name, err)
			}
		}
	}
	return pending, nil
}

// Workload names in a rolloutGroupState.
func rolloutMySQLWorkload(site string) string     { return "mysql/" + site }
func rolloutDragonflyWorkload(site string) string { return "dragonfly/" + site }

// rolloutGroupState is what the pacer needs to know about one group's
// workloads.
type rolloutGroupState struct {
	// Pending lists the workloads another renderer wrote whose pod template
	// the running render would change: what the group needs the slot for.
	Pending []string
	// Incomplete lists the workloads whose rollout has not finished: a pod
	// of the current template is missing or not available.
	Incomplete []string
}

// RolloutGroupState reads fg's workloads through reader and classifies
// them for the pacer. It writes nothing.
func (r *MysqlFailoverGroupReconciler) RolloutGroupState(ctx context.Context, reader client.Reader, fg *v1alpha1.MysqlFailoverGroup) (rolloutGroupState, error) {
	var st rolloutGroupState
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if _, err := effectiveSidecarImage(fg); err != nil {
		return st, err
	}
	digestKey, err := readDigestKey(ctx, reader, fg)
	if err != nil {
		return st, err
	}
	secretInputs, err := specSecretInputs(ctx, reader, fg)
	if err != nil {
		return st, err
	}
	secretDigest := secretContentDigest(digestKey, secretInputs)
	deploys, err := listPITRSiteDeployments(ctx, reader, fg)
	if err != nil {
		return st, err
	}
	pitr, action := resolvePITRRender(fg, deploys)
	for _, site := range fg.Spec.Sites {
		var deploy appsv1.Deployment
		key := types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name)}
		if err := reader.Get(ctx, key, &deploy); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return st, err
		}
		if deploymentSelectorMismatch(&deploy, fg, site.Name) != nil {
			// Refused: nothing rolls it, so nothing waits for it.
			continue
		}
		if !deploymentRolloutComplete(&deploy) {
			st.Incomplete = append(st.Incomplete, rolloutMySQLWorkload(site.Name))
		}
		if !renderedByOther(&deploy) || action == pitrRenderDefer {
			continue
		}
		if deploy.Annotations[v1alpha1.SpecHashAnnotation] != ComputeSpecHash(fg, site, secretDigest, pitr) ||
			deploy.Annotations[v1alpha1.PITRRenderAnnotation] != pitr.Canonical() ||
			r.rolloutTemplatePendingNoted(nn, site.Name) {
			st.Pending = append(st.Pending, rolloutMySQLWorkload(site.Name))
		}
	}
	if !dragonflyEnabled(fg) {
		return st, nil
	}
	for _, site := range fg.Spec.Sites {
		var sts appsv1.StatefulSet
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
		if err := reader.Get(ctx, key, &sts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return st, err
		}
		if statefulSetSelectorMismatch(&sts, fg, site.Name) != nil {
			continue
		}
		if !dragonflyStatefulSetRolloutComplete(&sts) {
			st.Incomplete = append(st.Incomplete, rolloutDragonflyWorkload(site.Name))
		}
		if !renderedByOther(&sts) {
			continue
		}
		desired, err := r.desiredDragonflyStatefulSet(fg, site, &sts)
		if err != nil {
			return st, err
		}
		if !dragonflyStatefulSetTemplateEqual(&sts, desired) {
			st.Pending = append(st.Pending, rolloutDragonflyWorkload(site.Name))
		}
	}
	return st, nil
}

// gateDragonflyRollout runs before the serial rollout changes the template
// of a Dragonfly StatefulSet that differs from the running render. It
// reports held=true when another renderer wrote it and the group is not
// admitted to the paced rollout; otherwise the start of the roll is latched.
func (r *MysqlFailoverGroupReconciler) gateDragonflyRollout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, current *appsv1.StatefulSet) (held bool, err error) {
	if !r.RolloutPacing || !renderedByOther(current) {
		return false, nil
	}
	err = r.admitRolloutWrite(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
	if errors.Is(err, errRolloutHeld) {
		return true, nil
	}
	return false, err
}
