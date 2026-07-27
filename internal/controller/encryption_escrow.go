package controller

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// labelKeyringVersion carries the escrow version as a label so the
	// operator can list a site's keyring Secrets without parsing names.
	labelKeyringVersion = "shipstream.io/keyring-version"

	// annotationKeyringDigest records the digest of the escrowed bytes.
	// Purely informational — the operator always recomputes the digest
	// from the Secret's contents rather than trusting the annotation.
	annotationKeyringDigest = "shipstream.io/keyring-digest"

	// RotateKeyringAnnotation is set by an admin to trigger an InnoDB
	// master-key rotation on one site:
	//
	//	kubectl annotate mysqlfailovergroup orders \
	//	  bloodraven.shipstream.io/rotate-keyring=pdx
	//
	// The operator refuses to rotate the active primary. Rotation
	// necessarily runs with a writable keyring, and the only window in
	// which a keyring can be lost is that one; keeping it off the
	// primary means a lost keyring is always recoverable by re-cloning
	// the affected site from a healthy peer. Rotate the replicas, run a
	// planned failover, then rotate the ex-primary.
	RotateKeyringAnnotation = "bloodraven.shipstream.io/rotate-keyring"

	// AdoptEncryptionAnnotation acknowledges that encryption is being
	// turned on for a failover group that already holds data, and that
	// the pre-existing tablespaces will stay plaintext until they are
	// rebuilt:
	//
	//	kubectl annotate mysqlfailovergroup orders \
	//	  bloodraven.shipstream.io/encryption-adopt=confirm
	//
	// status.encryptionAtRest.sites[].coverage.unencryptedTablespaces
	// reports how much data is still in the clear.
	AdoptEncryptionAnnotation = "bloodraven.shipstream.io/encryption-adopt"

	// maxKeyringBytes bounds what the escrow endpoint will accept. A
	// file keyring holding master keys is well under 1 KiB; the cap
	// exists so a misbehaving sidecar cannot push a Secret-sized blob.
	maxKeyringBytes = 256 * 1024

	// conditionEncryptionReady is the condition type summarizing the
	// encryption subsystem.
	conditionEncryptionReady = "EncryptionAtRestReady"
)

// -------------------------------------------------------------------
// Escrow store
// -------------------------------------------------------------------

// escrowVersion is one immutable keyring Secret.
type escrowVersion struct {
	Name    string
	Version int32
	Digest  string
	Bytes   []byte
}

// keyringEscrowStore reads and writes the per-site immutable keyring
// Secrets. It is deliberately separate from the reconciler so the
// operator's auxiliary HTTP endpoint (which accepts sidecar pushes) and
// the reconcile loop share exactly one implementation of "what is the
// current escrow version".
type keyringEscrowStore struct {
	client client.Client
	scheme *runtime.Scheme
}

// listVersions returns every escrow Secret for a site, newest first.
func (s *keyringEscrowStore) listVersions(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) ([]escrowVersion, error) {
	var list corev1.SecretList
	if err := s.client.List(ctx, &list,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelFailoverGroup: fg.Name,
			labelSite:          site,
			labelManagedBy:     managerName,
			labelAppName:       "mysql-keyring",
		},
	); err != nil {
		return nil, fmt.Errorf("list keyring secrets for site %s: %w", site, err)
	}

	out := make([]escrowVersion, 0, len(list.Items))
	for i := range list.Items {
		sec := &list.Items[i]
		raw, ok := sec.Data[v1alpha1.KeyringDataFileName]
		if !ok || len(raw) == 0 {
			// A keyring Secret with no keyring in it is not a version
			// the operator can seal against. Skip rather than error so
			// one corrupt Secret cannot wedge the whole site.
			continue
		}
		v, err := strconv.ParseInt(sec.Labels[labelKeyringVersion], 10, 32)
		if err != nil {
			continue
		}
		out = append(out, escrowVersion{
			Name:    sec.Name,
			Version: int32(v),
			Digest:  keyringDigest(raw),
			Bytes:   raw,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version > out[j].Version })
	return out, nil
}

// current returns the newest escrow version, or ok=false when the site
// has never escrowed a keyring.
func (s *keyringEscrowStore) current(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) (escrowVersion, bool, error) {
	versions, err := s.listVersions(ctx, fg, site)
	if err != nil {
		return escrowVersion{}, false, err
	}
	if len(versions) == 0 {
		return escrowVersion{}, false, nil
	}
	return versions[0], true, nil
}

// put stores keyring bytes as a new immutable version, unless the
// newest existing version already holds exactly these bytes — in which
// case it returns that version. Idempotency matters: the sidecar retries
// its push until the operator confirms, and a retried push must not
// mint a new version on every attempt.
func (s *keyringEscrowStore) put(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string, raw []byte) (escrowVersion, error) {
	digest := keyringDigest(raw)

	versions, err := s.listVersions(ctx, fg, site)
	if err != nil {
		return escrowVersion{}, err
	}
	if len(versions) > 0 && versions[0].Digest == digest {
		return versions[0], nil
	}

	next := int32(1)
	if len(versions) > 0 {
		next = versions[0].Version + 1
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      v1alpha1.KeyringSecretName(fg.Name, site, next),
			Namespace: fg.Namespace,
			Labels: map[string]string{
				labelAppName:        "mysql-keyring",
				labelInstance:       fg.Name,
				labelFailoverGroup:  fg.Name,
				labelSite:           site,
				labelManagedBy:      managerName,
				labelKeyringVersion: strconv.FormatInt(int64(next), 10),
			},
			Annotations: map[string]string{
				annotationKeyringDigest: digest,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{v1alpha1.KeyringDataFileName: raw},
		// Immutable so a later bug — or a compromised sidecar token —
		// cannot rewrite a version that a running MySQL is already
		// sealed against. Rolling a site forward always means minting a
		// new version and re-rendering the Deployment.
		Immutable: boolPtr(true),
	}
	if err := controllerutil.SetControllerReference(fg, sec, s.scheme); err != nil {
		return escrowVersion{}, fmt.Errorf("set owner on keyring secret: %w", err)
	}
	if err := s.client.Create(ctx, sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Concurrent push won the race. Re-read and return whatever
			// landed; the caller compares digests.
			var existing corev1.Secret
			if getErr := s.client.Get(ctx, types.NamespacedName{
				Namespace: fg.Namespace, Name: sec.Name,
			}, &existing); getErr == nil {
				raw := existing.Data[v1alpha1.KeyringDataFileName]
				return escrowVersion{
					Name: existing.Name, Version: next,
					Digest: keyringDigest(raw), Bytes: raw,
				}, nil
			}
		}
		return escrowVersion{}, fmt.Errorf("create keyring secret %s: %w", sec.Name, err)
	}

	return escrowVersion{Name: sec.Name, Version: next, Digest: digest, Bytes: raw}, nil
}

// prune deletes escrow versions beyond keyring.retainVersions, oldest
// first. The version a site is currently sealed against is never
// deleted, even if retention would otherwise reach it.
func (s *keyringEscrowStore) prune(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string, inUse string) error {
	retain := int(fg.Spec.EffectiveKeyring().RetainVersions)
	versions, err := s.listVersions(ctx, fg, site)
	if err != nil {
		return err
	}
	if len(versions) <= retain {
		return nil
	}
	for _, v := range versions[retain:] {
		if v.Name == inUse {
			continue
		}
		sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: v.Name, Namespace: fg.Namespace}}
		if err := s.client.Delete(ctx, sec); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("prune keyring secret %s: %w", v.Name, err)
		}
	}
	return nil
}

// -------------------------------------------------------------------
// Escrow token
// -------------------------------------------------------------------

// ensureEscrowToken mints the per-site bearer token the sidecar presents
// when pushing a keyring, creating it if absent. The token is only
// mounted into unsealed pods, so a sealed site carries no credential
// that could be used to write an escrow Secret.
func (r *MysqlFailoverGroupReconciler) ensureEscrowToken(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) error {
	name := v1alpha1.KeyringTokenSecretName(fg.Name, site)
	var existing corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &existing)
	if err == nil {
		if len(existing.Data[v1alpha1.KeyringTokenKey]) >= 32 {
			return nil
		}
		// Malformed or truncated token: delete and re-mint rather than
		// leaving a site unable to escrow.
		if delErr := r.Delete(ctx, &existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			return fmt.Errorf("delete malformed keyring token %s: %w", name, delErr)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get keyring token %s: %w", name, err)
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Errorf("generate keyring token: %w", err)
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fg.Namespace,
			Labels: map[string]string{
				labelAppName:       "mysql-keyring-token",
				labelInstance:      fg.Name,
				labelFailoverGroup: fg.Name,
				labelSite:          site,
				labelManagedBy:     managerName,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			v1alpha1.KeyringTokenKey: []byte(hex.EncodeToString(raw)),
		},
	}
	if err := controllerutil.SetControllerReference(fg, sec, r.Scheme); err != nil {
		return fmt.Errorf("set owner on keyring token: %w", err)
	}
	if err := r.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create keyring token %s: %w", name, err)
	}
	return nil
}

// verifyEscrowToken constant-time compares a presented bearer token
// against the site's minted token.
func verifyEscrowToken(ctx context.Context, c client.Client, namespace, group, site, presented string) error {
	name := v1alpha1.KeyringTokenSecretName(group, site)
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &sec); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("no escrow token issued for %s/%s site %s", namespace, group, site)
		}
		return fmt.Errorf("read escrow token: %w", err)
	}
	want := sec.Data[v1alpha1.KeyringTokenKey]
	if len(want) == 0 {
		return fmt.Errorf("escrow token for site %s is empty", site)
	}
	if subtle.ConstantTimeCompare(want, []byte(presented)) != 1 {
		return fmt.Errorf("escrow token mismatch for site %s", site)
	}
	return nil
}

// -------------------------------------------------------------------
// Rendering inputs
// -------------------------------------------------------------------

// siteEscrowSecretName returns the escrow Secret a site's Deployment
// should reference: the Secret the site is sealed against, or the seed
// for an unsealed site. Empty means "no keyring escrowed yet", which is
// only valid on a fresh bootstrap.
func siteEscrowSecretName(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	s := fg.Status.EncryptionAtRest.SiteEncryptionStatusByName(site)
	if s == nil {
		return ""
	}
	return s.KeyringSecret
}

// siteKeyringRotating reports whether the site is unsealed specifically
// to perform a master-key rotation, which is the only case where the
// sidecar issues ALTER INSTANCE ROTATE INNODB MASTER KEY.
func siteKeyringRotating(fg *v1alpha1.MysqlFailoverGroup, site string) bool {
	s := fg.Status.EncryptionAtRest.SiteEncryptionStatusByName(site)
	return s != nil &&
		s.Phase == v1alpha1.KeyringPhaseUnsealed &&
		s.UnsealReason == v1alpha1.UnsealReasonRotation
}

// deploymentRendersSealedKeyring reports whether a live Deployment is
// already rendered with the sealed keyring — i.e. the keyring volume is
// a Secret projection rather than a memory-backed emptyDir. This is the
// observation that advances a site from Escrowed to Sealed; checking the
// rendered PodSpec directly is more honest than tracking spec hashes,
// because it is exactly the property the security claim rests on.
func deploymentRendersSealedKeyring(deploy *appsv1.Deployment) bool {
	for _, v := range deploy.Spec.Template.Spec.Volumes {
		if v.Name != keyringVolumeName {
			continue
		}
		return v.Secret != nil
	}
	return false
}
