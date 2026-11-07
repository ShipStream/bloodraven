package controller

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDefaultOperatorEndpoint(t *testing.T) {
	t.Run("explicit spec wins", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_DEFAULT_OPERATOR_ENDPOINT", "bloodraven.bloodraven.svc.cluster.local:8082")
		fg := &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Namespace: "orders"},
			Spec: v1alpha1.MysqlFailoverGroupSpec{
				Sidecar: v1alpha1.SidecarSpec{OperatorEndpoint: "custom.example:8082"},
			},
		}

		if got := defaultOperatorEndpoint(fg); got != "custom.example:8082" {
			t.Fatalf("defaultOperatorEndpoint() = %q, want explicit address", got)
		}
	})

	t.Run("chart-provided default wins over legacy namespace default", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_DEFAULT_OPERATOR_ENDPOINT", "bloodraven.bloodraven.svc.cluster.local:8082")
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "orders"}}

		if got := defaultOperatorEndpoint(fg); got != "bloodraven.bloodraven.svc.cluster.local:8082" {
			t.Fatalf("defaultOperatorEndpoint() = %q, want chart-provided default", got)
		}
	})

	t.Run("legacy fallback preserves namespace-local address", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_DEFAULT_OPERATOR_ENDPOINT", "")
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "orders"}}

		if got := defaultOperatorEndpoint(fg); got != "bloodraven.orders.svc.cluster.local:8082" {
			t.Fatalf("defaultOperatorEndpoint() = %q, want legacy namespace-local default", got)
		}
	})

	t.Run("whitespace-only env falls through to legacy default", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_DEFAULT_OPERATOR_ENDPOINT", " \t ")
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "orders"}}

		if got := defaultOperatorEndpoint(fg); got != "bloodraven.orders.svc.cluster.local:8082" {
			t.Fatalf("defaultOperatorEndpoint() = %q, want legacy namespace-local default", got)
		}
	})
}
