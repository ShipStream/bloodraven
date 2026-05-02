package dragonfly

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// LoadPassword returns the Dragonfly AUTH password configured on the
// playground MFG. Empty string means AUTH is not configured.
func LoadPassword(ctx context.Context, k *pgkube.Client, namespace string) (string, error) {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	mfg, err := k.GetMFG(ctx, namespace)
	if err != nil {
		return "", err
	}
	if mfg.Spec.Dragonfly == nil || !mfg.Spec.Dragonfly.Enabled || mfg.Spec.Dragonfly.Auth == nil {
		return "", nil
	}
	auth := mfg.Spec.Dragonfly.Auth
	key := auth.PasswordKey
	if key == "" {
		key = "password"
	}
	secret, err := k.Kubernetes.CoreV1().Secrets(namespace).Get(ctx, auth.SecretName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get dragonfly auth secret %s: %w", auth.SecretName, err)
	}
	if v, ok := secret.Data[key]; ok {
		return string(v), nil
	}
	if v, ok := secret.StringData[key]; ok {
		return v, nil
	}
	return "", fmt.Errorf("key %s not found in dragonfly auth secret %s", key, auth.SecretName)
}
