package dragonfly

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

func TestLoadPassword(t *testing.T) {
	ctx := context.Background()
	ns := pgkube.PlaygroundNamespace

	t.Run("no auth returns empty password", func(t *testing.T) {
		k := testKubeClient(t,
			&v1alpha1.MysqlFailoverGroup{
				ObjectMeta: metav1.ObjectMeta{Name: pgkube.FailoverGroupName, Namespace: ns},
				Spec: v1alpha1.MysqlFailoverGroupSpec{
					Dragonfly: &v1alpha1.DragonflySpec{Enabled: true},
				},
			},
		)
		got, err := LoadPassword(ctx, k, ns)
		if err != nil {
			t.Fatalf("LoadPassword: %v", err)
		}
		if got != "" {
			t.Fatalf("password=%q want empty", got)
		}
	})

	t.Run("auth secret password", func(t *testing.T) {
		k := testKubeClient(t,
			&v1alpha1.MysqlFailoverGroup{
				ObjectMeta: metav1.ObjectMeta{Name: pgkube.FailoverGroupName, Namespace: ns},
				Spec: v1alpha1.MysqlFailoverGroupSpec{
					Dragonfly: &v1alpha1.DragonflySpec{
						Enabled: true,
						Auth:    &v1alpha1.DragonflyAuthSpec{SecretName: "dragonfly-auth", PasswordKey: "pw"},
					},
				},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "dragonfly-auth", Namespace: ns},
				Data:       map[string][]byte{"pw": []byte("secret")},
			},
		)
		got, err := LoadPassword(ctx, k, ns)
		if err != nil {
			t.Fatalf("LoadPassword: %v", err)
		}
		if got != "secret" {
			t.Fatalf("password=%q want secret", got)
		}
	})

	t.Run("auth default password key", func(t *testing.T) {
		k := testKubeClient(t,
			&v1alpha1.MysqlFailoverGroup{
				ObjectMeta: metav1.ObjectMeta{Name: pgkube.FailoverGroupName, Namespace: ns},
				Spec: v1alpha1.MysqlFailoverGroupSpec{
					Dragonfly: &v1alpha1.DragonflySpec{
						Enabled: true,
						Auth:    &v1alpha1.DragonflyAuthSpec{SecretName: "dragonfly-auth"},
					},
				},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "dragonfly-auth", Namespace: ns},
				Data:       map[string][]byte{"password": []byte("default-secret")},
			},
		)
		got, err := LoadPassword(ctx, k, ns)
		if err != nil {
			t.Fatalf("LoadPassword: %v", err)
		}
		if got != "default-secret" {
			t.Fatalf("password=%q want default-secret", got)
		}
	})
}

func testKubeClient(t *testing.T, objects ...runtime.Object) *pgkube.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("core AddToScheme: %v", err)
	}
	var kubeObjects []runtime.Object
	for _, obj := range objects {
		if _, ok := obj.(*corev1.Secret); ok {
			kubeObjects = append(kubeObjects, obj)
		}
	}
	return &pgkube.Client{
		Kubernetes: fake.NewSimpleClientset(kubeObjects...),
		Controller: ctrlfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build(),
	}
}
