package scenarios

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDeploymentScenariosRegisteredAndProfiled(t *testing.T) {
	for _, tc := range []struct {
		id      string
		release bool
	}{
		{"53-deployment-hold-expiry", true},
		{"54-deployment-hold-owner-kill", false},
		{"55-deployment-emergency-revocation", true},
		{"56-deployment-live-renewal-fenced", false},
	} {
		t.Run(tc.id, func(t *testing.T) {
			s, ok := runner.DefaultRegistry.Get(tc.id)
			if !ok || s.Precheck == nil || s.Cleanup == nil || s.Timeout == 0 || len(s.Steps) < 4 || s.Quarantine != "" {
				t.Fatalf("missing or incomplete scenario: %+v", s)
			}
			all := runner.DefaultRegistry.List()
			if !inventoryContains(runner.SelectForProfile(all, runner.ProfileFull), tc.id) {
				t.Fatal("not in full profile")
			}
			if got := inventoryContains(runner.SelectForProfile(all, runner.ProfileRelease), tc.id); got != tc.release {
				t.Fatalf("release membership=%v want %v", got, tc.release)
			}
			if inventoryContains(runner.SelectForProfile(all, runner.ProfileSmoke), tc.id) {
				t.Fatal("unexpected smoke membership")
			}
		})
	}
}

func TestDeploymentClientPodUsesProjectedTokenAndTLS(t *testing.T) {
	pod := deploymentClientPod(metav1.ObjectMeta{Name: "caller", Namespace: "sandbox"}, "attempt", "group", "renew")
	if pod.Spec.ServiceAccountName != "caller" || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("client must use only its audience-scoped projected token")
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever || pod.Spec.ActiveDeadlineSeconds == nil {
		t.Fatal("crashed client must not respawn or renew indefinitely")
	}
	projection := pod.Spec.Volumes[0].Projected.Sources[0].ServiceAccountToken
	if projection.Audience != "bloodraven-deploy" || projection.Path != "token" || *projection.ExpirationSeconds != 3600 {
		t.Fatalf("wrong TokenRequest projection: %+v", projection)
	}
	container := pod.Spec.Containers[0]
	if container.Env[0].Value != "https://bloodraven.sandbox.svc.cluster.local:8443/deploy/v1/groups/group" {
		t.Fatalf("wrong TLS endpoint: %s", container.Env[0].Value)
	}
	if container.Command[3] != deployClientScript || pod.Spec.Volumes[1].ConfigMap.Name != "caller" {
		t.Fatal("client script or CA fixture not mounted")
	}
}

func TestDecodeDeploymentClientRecords(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		count       int
		bad         bool
	}{
		{"empty", "\n", 0, false},
		{"grant and fence", "{\"action\":\"grant\",\"kind\":\"migration\",\"status\":201,\"body\":{\"expiresAt\":\"2026-09-09T12:00:30Z\",\"topologyGeneration\":7}}\n{\"action\":\"renew\",\"status\":409,\"body\":{\"error\":\"revoked\",\"reason\":\"topology_changed\",\"topologyGeneration\":8}}\n", 2, false},
		{"traceback fails closed", "Traceback (most recent call last):", 0, true},
		{"invalid expiry", "{\"body\":{\"expiresAt\":\"tomorrow\"}}", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, err := decodeDeployClientRecords([]byte(tc.input))
			if (err != nil) != tc.bad || len(records) != tc.count {
				t.Fatalf("got %d records, err=%v", len(records), err)
			}
		})
	}
}

func TestDeploymentPollHonorsCancellationAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := deployPoll(ctx, time.Hour, func() (bool, error) { return false, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	want := errors.New("assertion failed")
	if err := deployPoll(context.Background(), time.Hour, func() (bool, error) { return false, want }); !errors.Is(err, want) {
		t.Fatalf("expected immediate assertion failure: %v", err)
	}
}

func TestDeploymentCleanupPreservesCredentialsOnDatabaseDeleteFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "delete failure"}[fail], func(t *testing.T) {
			meta := metav1.ObjectMeta{Name: "fixture", Namespace: "sandbox"}
			secret := &corev1.Secret{ObjectMeta: meta}
			db := &v1alpha1.MysqlDatabase{ObjectMeta: meta}
			denied := errors.New("database deletion denied")
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, db).WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*v1alpha1.MysqlDatabase); ok && fail {
						return denied
					}
					return c.Delete(ctx, obj, opts...)
				},
			}).Build()
			state := &deployScenarioState{objects: []client.Object{secret, db}}
			err := state.cleanup(context.Background(), &runner.Env{Kube: &pgkube.Client{Controller: c}})
			if fail && !errors.Is(err, denied) || !fail && err != nil {
				t.Fatalf("cleanup error=%v", err)
			}
			err = c.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{})
			if fail && err != nil || !fail && !apierrors.IsNotFound(err) {
				t.Fatalf("owner Secret lookup=%v; preserve=%v", err, fail)
			}
		})
	}
}
