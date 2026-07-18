package kube

import (
	"context"
	"sort"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceEndpoint describes one EndpointSlice endpoint selected by a Service.
type ServiceEndpoint struct {
	PodName     string
	PodUID      string
	Addresses   []string
	Ready       bool
	Serving     bool
	Terminating bool
	PortNames   []string
}

// ServiceEndpointState is the flattened EndpointSlice state for a Service.
type ServiceEndpointState struct {
	Ports     map[string]int32
	Endpoints []ServiceEndpoint
}

// ServiceEndpointState reads every EndpointSlice owned by serviceName.
func (c *Client) ServiceEndpointState(ctx context.Context, namespace, serviceName string) (ServiceEndpointState, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	slices, err := c.Kubernetes.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: discoveryv1.LabelServiceName + "=" + serviceName,
	})
	if err != nil {
		return ServiceEndpointState{}, err
	}

	state := ServiceEndpointState{Ports: map[string]int32{}}
	for i := range slices.Items {
		slice := &slices.Items[i]
		var portNames []string
		for _, port := range slice.Ports {
			if port.Name == nil || port.Port == nil {
				continue
			}
			state.Ports[*port.Name] = *port.Port
			portNames = append(portNames, *port.Name)
		}
		sort.Strings(portNames)
		for _, endpoint := range slice.Endpoints {
			ready := endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready
			serving := ready
			if endpoint.Conditions.Serving != nil {
				serving = *endpoint.Conditions.Serving
			}
			item := ServiceEndpoint{
				Addresses: append([]string(nil), endpoint.Addresses...),
				Ready:     ready,
				Serving:   serving,
				PortNames: append([]string(nil), portNames...),
			}
			if endpoint.Conditions.Terminating != nil {
				item.Terminating = *endpoint.Conditions.Terminating
			}
			if endpoint.TargetRef != nil && endpoint.TargetRef.Kind == "Pod" {
				item.PodName = endpoint.TargetRef.Name
				item.PodUID = string(endpoint.TargetRef.UID)
			}
			state.Endpoints = append(state.Endpoints, item)
		}
	}
	return state, nil
}

// ServingPodNames returns sorted Pod targets serving the named port.
func (s ServiceEndpointState) ServingPodNames(portName string) []string {
	return s.podNames(portName, func(endpoint ServiceEndpoint) bool { return endpoint.Serving })
}

// ReadyPodNames returns sorted Pod targets ready on the named port.
func (s ServiceEndpointState) ReadyPodNames(portName string) []string {
	return s.podNames(portName, func(endpoint ServiceEndpoint) bool { return endpoint.Ready })
}

func (s ServiceEndpointState) podNames(portName string, include func(ServiceEndpoint) bool) []string {
	var names []string
	for _, endpoint := range s.Endpoints {
		if !include(endpoint) || !containsEndpointPort(endpoint.PortNames, portName) || endpoint.PodName == "" {
			continue
		}
		names = append(names, endpoint.PodName)
	}
	sort.Strings(names)
	return names
}

func containsEndpointPort(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}
