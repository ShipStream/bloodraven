package chaos

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgrustfs "github.com/shipstream/bloodraven/internal/playground/rustfs"
)

const rustfsDeployment = "rustfs"

// ScaleRustFSToZero scales the playground RustFS Deployment to 0, returning
// the current RustFS pod name for capture, and pushes a reverter that scales
// it back to 1 and waits for an available replica. Scenario 36 uses this to
// pull object storage out from under an in-flight in-place restore Job.
func (a *Actions) ScaleRustFSToZero(ctx context.Context) (podName string, err error) {
	if pod, perr := a.K.FindPodWithLabel(ctx, a.Namespace, "app.kubernetes.io/name=rustfs"); perr == nil {
		podName = pod.Name
	}
	if err := a.K.ScaleDeployment(ctx, a.Namespace, rustfsDeployment, 0); err != nil {
		return podName, fmt.Errorf("scale rustfs to 0: %w", err)
	}
	a.push("scale rustfs back to 1 and wait available", func(ctx context.Context) error {
		return a.ScaleRustFSToOne(ctx)
	})
	return podName, nil
}

// ScaleRustFSToOne scales the RustFS Deployment back to 1 and waits (bounded)
// for an available replica so a following backup/restore step does not race a
// still-starting pod.
func (a *Actions) ScaleRustFSToOne(ctx context.Context) error {
	if err := a.K.ScaleDeployment(ctx, a.Namespace, rustfsDeployment, 1); err != nil {
		return fmt.Errorf("scale rustfs to 1: %w", err)
	}
	deadline := time.Now().Add(120 * time.Second)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		ok, err := a.K.DeploymentHasAvailableReplica(ctx, a.Namespace, rustfsDeployment)
		if err == nil && ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("rustfs deployment did not become available after scale-up (lastErr=%v)", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// RecoverRustFS is the GlobalRecover backstop for ScaleRustFSToZero: it brings
// RustFS back to 1 replica when a scenario left it at 0 and its reverter never
// ran (a killed runner, a panic, a cancelled cleanup). Object storage down is
// not self-healing — every backup, restore and PITR scenario after it would
// fail — so the safety net has to cover it, exactly as it covers scaled-down
// MySQL sites.
//
// Idempotent and cheap: a RustFS that is already up costs one GET and no wait,
// and a playground deployed without RustFS at all is a no-op.
func (a *Actions) RecoverRustFS(ctx context.Context) error {
	dep, err := a.K.GetDeployment(ctx, a.Namespace, rustfsDeployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // playground without RustFS: nothing to recover
		}
		return fmt.Errorf("read rustfs deployment: %w", err)
	}
	if dep.Spec.Replicas != nil && *dep.Spec.Replicas >= 1 {
		return nil // already up — do not pay the availability wait
	}
	return a.ScaleRustFSToOne(ctx)
}

// ReadRustFSObject fetches an object from the playground RustFS bucket through
// a port-forward, using the same dragonfly-s3-credentials Secret the operator
// and backup Jobs use. Returns found=false for a missing key. Scenario 37 uses
// this to read per-site binlog manifests straight from storage.
func (a *Actions) ReadRustFSObject(ctx context.Context, bucket, key string) (data []byte, found bool, err error) {
	creds, err := a.rustfsCredentials(ctx)
	if err != nil {
		return nil, false, err
	}
	pod, err := a.K.FindPodWithLabel(ctx, a.Namespace, "app.kubernetes.io/name=rustfs")
	if err != nil {
		return nil, false, fmt.Errorf("find RustFS pod: %w", err)
	}
	pf, err := a.K.PortForwardPod(ctx, a.Namespace, pod.Name, 9000)
	if err != nil {
		return nil, false, fmt.Errorf("port-forward RustFS: %w", err)
	}
	defer pf.Stop()
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", pf.LocalPort)
	return pgrustfs.GetObject(ctx, endpoint, bucket, key, creds)
}

func (a *Actions) rustfsCredentials(ctx context.Context) (pgrustfs.Credentials, error) {
	const secretName = "dragonfly-s3-credentials"
	secret, err := a.K.Kubernetes.CoreV1().Secrets(a.Namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return pgrustfs.Credentials{}, fmt.Errorf("read RustFS credentials secret %s: %w", secretName, err)
	}
	get := func(k string) (string, error) {
		v, ok := secret.Data[k]
		if !ok || len(v) == 0 {
			return "", fmt.Errorf("RustFS credentials secret %s missing %s", secretName, k)
		}
		return string(v), nil
	}
	ak, err := get("AWS_ACCESS_KEY_ID")
	if err != nil {
		return pgrustfs.Credentials{}, err
	}
	sk, err := get("AWS_SECRET_ACCESS_KEY")
	if err != nil {
		return pgrustfs.Credentials{}, err
	}
	region, err := get("AWS_REGION")
	if err != nil {
		return pgrustfs.Credentials{}, err
	}
	return pgrustfs.Credentials{AccessKey: ak, SecretKey: sk, Region: region}, nil
}
