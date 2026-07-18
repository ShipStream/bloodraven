package kube

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestServiceEndpointStateFlattensSlices(t *testing.T) {
	ready, notReady, serving := true, false, true
	mysqlName, sidecarName := "mysql", "sidecar"
	mysqlPort, sidecarPort := int32(3306), int32(8080)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reader-abc",
			Namespace: "test",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "mysql-playground-reader"},
		},
		Ports: []discoveryv1.EndpointPort{
			{Name: &mysqlName, Port: &mysqlPort},
			{Name: &sidecarName, Port: &sidecarPort},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{"10.0.0.10"},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: "reader-ready", UID: types.UID("uid-ready")},
			},
			{
				Addresses:  []string{"10.0.0.11"},
				Conditions: discoveryv1.EndpointConditions{Ready: &notReady, Serving: &serving},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: "reader-serving", UID: types.UID("uid-serving")},
			},
		},
	}
	c := &Client{Kubernetes: fake.NewSimpleClientset(slice)}

	state, err := c.ServiceEndpointState(context.Background(), "test", "mysql-playground-reader")
	if err != nil {
		t.Fatalf("ServiceEndpointState() error = %v", err)
	}
	if got := state.Ports["mysql"]; got != 3306 {
		t.Errorf("mysql port = %d, want 3306", got)
	}
	if got := state.Ports["sidecar"]; got != 8080 {
		t.Errorf("sidecar port = %d, want 8080", got)
	}
	got := state.ServingPodNames("mysql")
	want := []string{"reader-ready", "reader-serving"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("ServingPodNames(mysql) = %v, want %v", got, want)
	}
	if got := state.ReadyPodNames("mysql"); len(got) != 1 || got[0] != "reader-ready" {
		t.Fatalf("ReadyPodNames(mysql) = %v, want [reader-ready]", got)
	}
	if got := state.ServingPodNames("missing"); len(got) != 0 {
		t.Fatalf("ServingPodNames(missing) = %v, want empty", got)
	}
}

func TestServiceEndpointStateDefaultsServingToReady(t *testing.T) {
	ready := false
	mysqlName := "mysql"
	mysqlPort := int32(3306)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reader-unready",
			Namespace: "test",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "mysql-playground-reader"},
		},
		Ports: []discoveryv1.EndpointPort{{Name: &mysqlName, Port: &mysqlPort}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.12"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: "reader-unready"},
		}},
	}
	c := &Client{Kubernetes: fake.NewSimpleClientset(slice)}

	state, err := c.ServiceEndpointState(context.Background(), "test", "mysql-playground-reader")
	if err != nil {
		t.Fatalf("ServiceEndpointState() error = %v", err)
	}
	if got := state.ServingPodNames("mysql"); len(got) != 0 {
		t.Fatalf("ServingPodNames(mysql) = %v, want empty", got)
	}
	if got := state.ReadyPodNames("mysql"); len(got) != 0 {
		t.Fatalf("ReadyPodNames(mysql) = %v, want empty", got)
	}
}
