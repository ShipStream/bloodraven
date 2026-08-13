package kube

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetSecret fetches a Secret by name. Scenarios use it to assert on
// operator-managed Secrets (for example that a superseded keyring escrow
// version still exists and can serve as a rollback target).
func (c *Client) GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	sec, err := c.Kubernetes.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get secret %s/%s: %w", namespace, name, err)
	}
	return sec, nil
}

// GetConfigMap fetches a ConfigMap by name. Encryption scenarios use it
// to check the adopt-atomicity invariant: the my.cnf a site's Deployment
// actually references must agree with the keyring wiring in that
// Deployment's pod spec, or the site CrashLoops on its next restart.
func (c *Client) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	cm, err := c.Kubernetes.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get configmap %s/%s: %w", namespace, name, err)
	}
	return cm, nil
}

// ListSecretsByLabel returns the Secrets in a namespace matching a label
// selector, e.g. the keyring escrow versions for one site.
func (c *Client) ListSecretsByLabel(ctx context.Context, namespace, selector string) (*corev1.SecretList, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	list, err := c.Kubernetes.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, fmt.Errorf("list secrets in %s (%s): %w", namespace, selector, err)
	}
	return list, nil
}
