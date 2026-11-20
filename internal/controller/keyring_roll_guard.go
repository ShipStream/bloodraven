package controller

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

// emptyKeyringDocument is the keyring document keyringInitScript writes,
// without its trailing newline, for a pod that starts with no seed.
const emptyKeyringDocument = `{"version":"1.0","elements":[]}`

// emptyKeyringDigest is the digest the sidecar reports for that file.
var emptyKeyringDigest = keyringDigest([]byte(emptyKeyringDocument + "\n"))

// KeyringRollRefusedError reports that a site's pod was not replaced
// because the new pod would start on a keyring that lacks keys the running
// pod holds. An unsealed site runs its keyring on a memory-backed emptyDir,
// and a key MySQL stores there (a clone and the restart that installs it,
// a rotation, the binary log key of a new server_uuid) exists only in that
// pod until the operator has recorded an escrow of it. A new pod takes its
// keyring from the escrow Secret the status records, so replacing the pod
// before that discards the key, and mysqld on the new pod can never open
// the data it encrypted (MY-013279). Nothing is written; the site keeps its
// pod and its drift stays queued until the template takes its keyring from
// an escrow that holds the running one.
type KeyringRollRefusedError struct {
	Site   string
	Reason string
}

func (e *KeyringRollRefusedError) Error() string {
	return fmt.Sprintf("not replacing the pod of site %s: %s", e.Site, e.Reason)
}

// SiteKeyringRollRefusal reports whether the site's pod may be replaced by
// one rendered from the group as it is now (see keyringRollRefusal). The
// ordered update asks it before it rolls a site and before it hands off the
// active one. Reads are uncached; a read that fails refuses.
func (r *MysqlFailoverGroupReconciler) SiteKeyringRollRefusal(ctx context.Context, fgName types.NamespacedName, site string) error {
	reader := r.authorityReader()
	var fg v1beta1.MysqlFailoverGroup
	if err := reader.Get(ctx, fgName, &fg); err != nil {
		return &KeyringRollRefusedError{Site: site, Reason: fmt.Sprintf("cannot read the failover group: %v", err)}
	}
	if !fg.Spec.EncryptionEnabled() {
		return nil
	}
	var live appsv1.Deployment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: fgName.Namespace, Name: resourceName(fgName.Name, site)}, &live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return &KeyringRollRefusedError{Site: site, Reason: fmt.Sprintf("cannot read its Deployment: %v", err)}
	}
	return r.keyringRollRefusal(ctx, &fg, site, &live, siteEscrowSecretName(&fg, site))
}

// keyringRollRefusal decides whether the pod of live, the site's Deployment
// as it is now, may be replaced by one that takes its keyring from next:
// the escrow Secret the new template projects (sealed) or seeds (unsealed),
// or "" for a fresh keyring. It returns a *KeyringRollRefusedError when the
// replacement would lose keys, nil when it would not.
//
// Only a pod running the unsealed rendering can lose anything. A sealed pod
// projects its keyring read-only from an escrow Secret, which outlives it,
// and a pod whose mysqld never started holds only what keyring-init copied
// in. For a pod whose mysqld did start, the sidecar reports the digest of
// the keyring it runs, and the replacement is allowed when that keyring is
// the one the pod started from (MySQL stored nothing since, so nothing is
// only in memory) or the one the new pod gets. Anything else, including a
// sidecar that cannot be read, refuses: the keys may be in that pod alone.
//
// Deleting the pod by hand is the way to accept losing them: the
// Deployment's replacement starts from the same seed, and once its mysqld
// has failed without storing a key its roll is no longer refused.
func (r *MysqlFailoverGroupReconciler) keyringRollRefusal(ctx context.Context, fg *v1beta1.MysqlFailoverGroup, site string,
	live *appsv1.Deployment, next string) error {
	if !fg.Spec.EncryptionEnabled() || live == nil || !deploymentRendersUnsealedKeyring(live) {
		return nil
	}
	refuse := func(format string, args ...any) error {
		return &KeyringRollRefusedError{Site: site, Reason: fmt.Sprintf(format, args...)}
	}
	reader := r.authorityReader()
	pod, err := keyringHoldingPod(ctx, reader, live)
	if err != nil {
		return refuse("%v", err)
	}
	if pod == nil {
		return nil
	}

	fetch := r.keyringStatus
	if fetch == nil {
		fetch = defaultKeyringStatusFetcher
	}
	ks, err := fetch(ctx, fg, site)
	if err != nil {
		return refuse("cannot read the keyring of its running pod %s from the sidecar (%v), and that memory-backed keyring may hold keys no escrow has", pod.Name, err)
	}
	if !ks.Enabled {
		return refuse("the sidecar of its running pod %s does not report a keyring, and that memory-backed keyring may hold keys no escrow has", pod.Name)
	}
	if ks.Digest == "" {
		if !ks.Present {
			return nil
		}
		return refuse("the sidecar of its running pod %s cannot read the keyring%s", pod.Name, sidecarErrorSuffix(ks.LastError))
	}
	if started, err := keyringSourceDigest(ctx, reader, fg.Namespace, podKeyringSeed(pod)); err == nil && ks.Digest == started {
		return nil
	}
	want, err := keyringSourceDigest(ctx, reader, fg.Namespace, next)
	if err != nil {
		return refuse("cannot read %s, which the new pod would take its keyring from: %v", next, err)
	}
	if ks.Digest == want {
		return nil
	}
	source := "a fresh keyring"
	if next != "" {
		source = "escrow " + next
	}
	return refuse("its running pod %s holds keyring keys (%s) that are not in %s, which the new pod would start from; "+
		"waiting for the sidecar to escrow the running keyring and the operator to record it", pod.Name, ks.Digest, source)
}

// sidecarErrorSuffix is the sidecar's last error, bounded as in status
// messages, as a parenthesized suffix, or "".
func sidecarErrorSuffix(lastError string) string {
	if s := withSidecarError("", lastError); s != "" {
		return " (" + strings.TrimPrefix(s, "; ") + ")"
	}
	return ""
}

// keyringHoldingPod returns the Deployment's pod whose memory-backed
// keyring mysqld may have written: not terminating or terminated, running
// the unsealed rendering, with a mysql container that has started. Nil
// when there is none. More than one is an error: the sidecar's answer,
// through the site's Service, could not be attributed to one of them.
func keyringHoldingPod(ctx context.Context, reader client.Reader, live *appsv1.Deployment) (*corev1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(live.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("cannot read its Deployment's selector: %w", err)
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(live.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("cannot list its pods: %w", err)
	}
	var holding []*corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if _, ok := deploymentReplicaSetOf(pod, live.Name); !ok {
			continue
		}
		if !podRendersUnsealedKeyring(&pod.Spec) || !mysqlContainerStarted(pod) {
			continue
		}
		holding = append(holding, pod)
	}
	switch len(holding) {
	case 0:
		return nil, nil
	case 1:
		return holding[0], nil
	}
	return nil, fmt.Errorf("%d running pods hold a writable keyring, and their sidecars cannot be told apart", len(holding))
}

func podRendersUnsealedKeyring(spec *corev1.PodSpec) bool {
	for _, v := range spec.Volumes {
		if v.Name == keyringVolumeName {
			return v.EmptyDir != nil
		}
	}
	return false
}

// podKeyringSeed is the escrow Secret an unsealed pod's keyring-init copied
// its keyring from, or "" when it started from an empty document.
func podKeyringSeed(pod *corev1.Pod) string {
	for _, v := range pod.Spec.Volumes {
		if v.Name == keyringSeedVolumeName && v.Secret != nil {
			return v.Secret.SecretName
		}
	}
	return ""
}

// mysqlContainerStarted reports whether the pod's mysql container has ever
// run: only a running mysqld stores keys.
func mysqlContainerStarted(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != "mysql" {
			continue
		}
		return cs.State.Running != nil || cs.State.Terminated != nil || cs.RestartCount > 0 || cs.LastTerminationState.Terminated != nil
	}
	return false
}

// keyringSourceDigest is the digest of the keyring a pod gets from secret:
// the escrow Secret's keyring bytes, or the empty document for "".
func keyringSourceDigest(ctx context.Context, reader client.Reader, namespace, secret string) (string, error) {
	if secret == "" {
		return emptyKeyringDigest, nil
	}
	var sec corev1.Secret
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secret}, &sec); err != nil {
		return "", err
	}
	raw, ok := sec.Data[v1beta1.KeyringDataFileName]
	if !ok {
		return "", fmt.Errorf("escrow Secret %s has no %q key", secret, v1beta1.KeyringDataFileName)
	}
	return keyringDigest(raw), nil
}
