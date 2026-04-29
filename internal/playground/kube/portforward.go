package kube

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// PortForward holds a single live SPDY tunnel from a local random
// port to a container port inside a pod. Close it when the test step
// is done — leaving it open across scenarios courts the SPDY drop
// failure mode noted in the design doc.
type PortForward struct {
	LocalPort uint16
	stop      chan struct{}
	ready     chan struct{}
	errCh     chan error
	fwd       *portforward.PortForwarder
}

// Stop tears down the tunnel.
func (p *PortForward) Stop() {
	select {
	case <-p.stop:
		// already closed
	default:
		close(p.stop)
	}
}

// PortForwardPod opens a SPDY tunnel to a pod's container port and
// blocks until the forwarder reports ready (or the context expires).
// LocalPort 0 picks a random available port.
func (c *Client) PortForwardPod(ctx context.Context, namespace, pod string, remotePort uint16) (*PortForward, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	roundTripper, upgrader, err := spdy.RoundTripperFor(c.Config)
	if err != nil {
		return nil, fmt.Errorf("build spdy round-tripper: %w", err)
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", namespace, pod)
	hostURL, err := url.Parse(c.Config.Host)
	if err != nil {
		return nil, fmt.Errorf("parse host: %w", err)
	}
	hostURL.Path = path
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, "POST", hostURL)

	stop := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	fwd, err := portforward.New(
		dialer,
		[]string{fmt.Sprintf("0:%d", remotePort)},
		stop,
		ready,
		io.Discard,
		io.Discard,
	)
	if err != nil {
		return nil, fmt.Errorf("build port-forwarder: %w", err)
	}

	go func() {
		errCh <- fwd.ForwardPorts()
	}()

	select {
	case <-ready:
	case err := <-errCh:
		return nil, fmt.Errorf("port-forward failed before ready: %w", err)
	case <-ctx.Done():
		close(stop)
		return nil, ctx.Err()
	}

	ports, err := fwd.GetPorts()
	if err != nil {
		close(stop)
		return nil, fmt.Errorf("get forwarded ports: %w", err)
	}
	if len(ports) == 0 {
		close(stop)
		return nil, fmt.Errorf("port-forwarder reported no ports")
	}
	return &PortForward{
		LocalPort: ports[0].Local,
		stop:      stop,
		ready:     ready,
		errCh:     errCh,
		fwd:       fwd,
	}, nil
}

// FindPodWithLabel returns the first pod matching the given label
// selector. Used to resolve the operator pod (label
// app.kubernetes.io/name=bloodraven) regardless of its random suffix.
func (c *Client) FindPodWithLabel(ctx context.Context, namespace, selector string) (*corev1.Pod, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pods, err := c.Kubernetes.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods match selector %q", selector)
	}
	// Prefer a pod that is currently Running and Ready.
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodRunning {
			return p, nil
		}
	}
	return &pods.Items[0], nil
}
