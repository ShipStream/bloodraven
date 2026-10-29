package controller

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Secret contents feed two published fingerprints: the spec-hash
// annotation on each site's Deployment and pods (so a password or
// certificate change rolls the pods) and the credential-hash annotation on
// the group (so it re-runs the grants). Both are readable by principals
// that cannot read Secrets, so a plain digest of the contents would let
// them check password guesses offline. Every Secret value is therefore
// keyed with a random per-group key that lives in its own Secret: whoever
// can read that key can already read the credentials beside it.

const (
	digestKeyDataKey = "key"
	digestKeyBytes   = 32
)

// errDigestKeyMissing reports that a group's digest key Secret does not
// exist yet. Only the group reconciler creates it.
var errDigestKeyMissing = fmt.Errorf("secret digest key not created yet")

func digestKeySecretName(fgName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-digest-key", fgName))
}

// ensureDigestKey returns the group's digest key, creating it when absent
// or malformed. Replacing the key changes every published fingerprint, so
// each site rolls once through the normal update path.
func ensureDigestKey(ctx context.Context, c client.Client, reader client.Reader, scheme *runtime.Scheme, fg *v1alpha1.MysqlFailoverGroup) ([]byte, error) {
	key, err := readDigestKey(ctx, c, fg)
	if err == nil {
		return key, nil
	}
	if err != errDigestKeyMissing {
		return nil, err
	}

	name := digestKeySecretName(fg.Name)
	var existing corev1.Secret
	getErr := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &existing)
	if getErr == nil {
		// Present but malformed: replace it rather than fingerprint with
		// a short key.
		if delErr := c.Delete(ctx, &existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			return nil, fmt.Errorf("delete malformed digest key %s: %w", name, delErr)
		}
	} else if !apierrors.IsNotFound(getErr) {
		return nil, fmt.Errorf("get digest key %s: %w", name, getErr)
	}

	raw := make([]byte, digestKeyBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("generate digest key: %w", err)
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fg.Namespace,
			Labels: map[string]string{
				labelAppName:       "mysql-digest-key",
				labelInstance:      fg.Name,
				labelFailoverGroup: fg.Name,
				labelManagedBy:     managerName,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{digestKeyDataKey: raw},
	}
	if err := controllerutil.SetControllerReference(fg, sec, scheme); err != nil {
		return nil, fmt.Errorf("set owner on digest key: %w", err)
	}
	if err := c.Create(ctx, sec); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create digest key %s: %w", name, err)
		}
		// Another reconcile created it first and the cache has not seen
		// it yet: read it from the API server.
		if reader == nil {
			reader = c
		}
		return readDigestKey(ctx, reader, fg)
	}
	return raw, nil
}

// readDigestKey returns the group's digest key without creating it.
func readDigestKey(ctx context.Context, r client.Reader, fg *v1alpha1.MysqlFailoverGroup) ([]byte, error) {
	name := digestKeySecretName(fg.Name)
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &sec); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errDigestKeyMissing
		}
		return nil, fmt.Errorf("get digest key %s: %w", name, err)
	}
	key := sec.Data[digestKeyDataKey]
	if len(key) < digestKeyBytes {
		return nil, errDigestKeyMissing
	}
	return key, nil
}

// secretContentDigest is a keyed fingerprint of Secret contents. Each
// entry in secrets maps a label (e.g. "tls" or a Secret name) to that
// Secret's data. Absent secrets contribute nothing.
func secretContentDigest(key []byte, secrets map[string]map[string][]byte) string {
	if len(secrets) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, key)
	labels := make([]string, 0, len(secrets))
	for l := range secrets {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	for _, l := range labels {
		data := secrets[l]
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			// Length-prefix every field so no two inputs share an encoding.
			fmt.Fprintf(mac, "%d:%s%d:%s%d:", len(l), l, len(k), k, len(data[k]))
			mac.Write(data[k])
		}
	}
	return hex.EncodeToString(mac.Sum(nil))
}

// specSecretInputs collects the Secret contents a site's pod template
// depends on: TLS, the active PITR passphrase in either credential mode,
// and every referenced credential Secret in credentials mode. A Secret
// that does not exist yet is skipped.
func specSecretInputs(ctx context.Context, r client.Reader, fg *v1alpha1.MysqlFailoverGroup) (map[string]map[string][]byte, error) {
	secrets := make(map[string]map[string][]byte)
	if fg.Spec.TLS != nil {
		var s corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.TLS.SecretName}, &s)
		switch {
		case err == nil:
			secrets["tls"] = s.Data
		case !apierrors.IsNotFound(err):
			return nil, fmt.Errorf("get TLS secret %s: %w", fg.Spec.TLS.SecretName, err)
		}
	}
	if backup := fg.Spec.Backup; backup != nil && backup.PITR != nil && backup.PITR.Enabled {
		if profile := findProfile(fg, backup.PITR.ProfileName); profile.EncryptionEnabled() {
			ref := profile.Encryption.PassphraseSecret
			var s corev1.Secret
			err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: ref.Name}, &s)
			switch {
			case err == nil:
				key := ref.PassphraseSecretKeyOrDefault()
				secrets["pitr.passphrase"] = map[string][]byte{key: s.Data[key]}
			case !apierrors.IsNotFound(err):
				return nil, fmt.Errorf("get PITR passphrase secret %s: %w", ref.Name, err)
			}
		}
	}
	if fg.Spec.UsesCredentials() {
		for _, name := range fg.Spec.AllReferencedSecretNames() {
			var s corev1.Secret
			if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &s); err == nil {
				secrets["cred."+name] = s.Data
			}
		}
	}
	return secrets, nil
}
