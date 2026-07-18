package scenarios

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func TestS40ObserveOnceFailsMFGReadError(t *testing.T) {
	wantErr := errors.New("MFG API unavailable")
	env, state := s40ObserverTestEnv(t, k8sfake.NewSimpleClientset())
	underlying, ok := env.Kube.Controller.(client.WithWatch)
	if !ok {
		t.Fatal("fake controller client does not implement client.WithWatch")
	}
	env.Kube.Controller = interceptor.NewClient(underlying, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return wantErr
		},
	})

	observation := &s40ContinuousObservation{}
	s40ObserveOnce(context.Background(), env, state, observation)
	s40RequireObservationError(t, observation, "read MFG", wantErr.Error())
}

func TestS40ObserveOnceFailsClientEndpointSliceReadError(t *testing.T) {
	wantErr := apierrors.NewNotFound(schema.GroupResource{Group: discoveryv1.GroupName, Resource: "endpointslices"}, "reader-client")
	clientset := k8sfake.NewSimpleClientset()
	clientset.PrependReactor("list", "endpointslices", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, wantErr
	})
	env, state := s40ObserverTestEnv(t, clientset)

	observation := &s40ContinuousObservation{}
	s40ObserveOnce(context.Background(), env, state, observation)
	s40RequireObservationError(t, observation, "read reader client EndpointSlices", "not found")
}

func TestS40ObserveOnceFailsPodListError(t *testing.T) {
	wantErr := errors.New("pod list connection reset")
	clientset := k8sfake.NewSimpleClientset()
	clientset.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, wantErr
	})
	env, state := s40ObserverTestEnv(t, clientset)

	observation := &s40ContinuousObservation{}
	s40ObserveOnce(context.Background(), env, state, observation)
	s40RequireObservationError(t, observation, "list reader pods", wantErr.Error())
}

func TestS40ObserveOnceFailsInternalEndpointSliceReadError(t *testing.T) {
	wantErr := errors.New("internal EndpointSlice watch cache unavailable")
	clientset := k8sfake.NewSimpleClientset(s40ReplacementTestPod())
	endpointReads := 0
	clientset.PrependReactor("list", "endpointslices", func(k8stesting.Action) (bool, runtime.Object, error) {
		endpointReads++
		if endpointReads == 2 {
			return true, nil, wantErr
		}
		return false, nil, nil
	})
	env, state := s40ObserverTestEnv(t, clientset)

	observation := &s40ContinuousObservation{}
	s40ObserveOnce(context.Background(), env, state, observation)
	s40RequireObservationError(t, observation, "read reader internal EndpointSlices", wantErr.Error())
}

func TestS40ObserveOnceAllowsEmptyInternalEndpointSliceTransition(t *testing.T) {
	env, state := s40ObserverTestEnv(t, k8sfake.NewSimpleClientset(s40ReplacementTestPod()))
	observation := &s40ContinuousObservation{}

	s40ObserveOnce(context.Background(), env, state, observation)

	observation.mu.Lock()
	defer observation.mu.Unlock()
	if observation.err != nil {
		t.Fatalf("successful empty internal EndpointSlice transition recorded error: %v", observation.err)
	}
	if observation.sawInternalUnreadyPod {
		t.Fatal("empty internal EndpointSlice list unexpectedly recorded the replacement endpoint")
	}
}

func TestS40ObserveOnceAllowsExpectedEmptyListTransitions(t *testing.T) {
	env, state := s40ObserverTestEnv(t, k8sfake.NewSimpleClientset())
	observation := &s40ContinuousObservation{}

	s40ObserveOnce(context.Background(), env, state, observation)

	observation.mu.Lock()
	defer observation.mu.Unlock()
	if observation.err != nil {
		t.Fatalf("successful empty list transitions recorded error: %v", observation.err)
	}
	if !observation.sawClientEndpointEmpty {
		t.Fatal("successful empty client EndpointSlice list did not record endpoint absence")
	}
}

func TestS40ObserveOnceIgnoresTerminatingServingEndpoint(t *testing.T) {
	ready := false
	serving := true
	terminating := true
	portName := "mysql"
	port := int32(3306)
	clientset := k8sfake.NewSimpleClientset(&discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reader-client",
			Namespace: pgkube.PlaygroundNamespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: pgkube.MysqlDeploymentName(pgkube.FailoverGroupName, "reader")},
		},
		Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port}},
		Endpoints: []discoveryv1.Endpoint{{
			Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: "reader-old", UID: types.UID("original-pod")},
		}},
	})
	env, state := s40ObserverTestEnv(t, clientset)
	mfg, err := env.Kube.GetMFGNamed(context.Background(), env.Namespace, env.FG)
	if err != nil {
		t.Fatal(err)
	}
	statusSiteByName(mfg, "reader").Replicating = false
	if err := env.Kube.Controller.Update(context.Background(), mfg); err != nil {
		t.Fatal(err)
	}
	observation := &s40ContinuousObservation{}

	s40ObserveOnce(context.Background(), env, state, observation)

	observation.mu.Lock()
	defer observation.mu.Unlock()
	if observation.err != nil {
		t.Fatalf("terminating drain endpoint recorded as client-routable: %v", observation.err)
	}
	if !observation.sawClientEndpointEmpty {
		t.Fatal("ready=false terminating endpoint did not record client endpoint absence")
	}
}

func TestS40ObserveReadErrorIgnoresOnlyObserverCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observation := &s40ContinuousObservation{}
	s40ObserveReadError(ctx, observation, "read MFG", context.Canceled)
	if observation.err != nil {
		t.Fatalf("observer cancellation recorded as read failure: %v", observation.err)
	}

	liveObservation := &s40ContinuousObservation{}
	s40ObserveReadError(context.Background(), liveObservation, "read MFG", context.DeadlineExceeded)
	s40RequireObservationError(t, liveObservation, "read MFG", context.DeadlineExceeded.Error())
}

func s40ObserverTestEnv(t *testing.T, clientset *k8sfake.Clientset) (*runner.Env, *s40RunState) {
	t.Helper()
	lag := int64(0)
	mfg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pgkube.FailoverGroupName, Namespace: pgkube.PlaygroundNamespace},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{
				{Name: "iad", Role: v1alpha1.SiteRolePrimaryCandidate},
				{Name: "pdx", Role: v1alpha1.SiteRolePrimaryCandidate},
				{Name: "reader", Role: v1alpha1.SiteRoleReadOnly},
			},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: "writable"},
				{Name: "pdx", State: "read-only", Replicating: true},
				{
					Name:                   "reader",
					State:                  "read-only",
					Replicating:            true,
					SecondsBehindSource:    &lag,
					SourceHost:             playgroundInternalSiteHost(pgkube.FailoverGroupName, "iad", pgkube.PlaygroundNamespace),
					SourceConvergenceState: v1alpha1.SourceConvergenceConverged,
				},
			},
		},
	}
	env := testScenarioEnv(t, mfg)
	env.Kube.Kubernetes = clientset
	state := &s40RunState{
		reader:      "reader",
		active:      "iad",
		donorHost:   playgroundInternalSiteHost(pgkube.FailoverGroupName, "iad", pgkube.PlaygroundNamespace),
		originalPod: "original-pod",
	}
	return env, state
}

func s40ReplacementTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reader-replacement",
			Namespace: pgkube.PlaygroundNamespace,
			UID:       types.UID("replacement-pod"),
			Labels: map[string]string{
				"app.kubernetes.io/name":     "mysql",
				"app.kubernetes.io/instance": pgkube.FailoverGroupName,
				"shipstream.io/site":         "reader",
			},
		},
	}
}

func s40RequireObservationError(t *testing.T, observation *s40ContinuousObservation, fragments ...string) {
	t.Helper()
	err := observation.result()
	if err == nil {
		t.Fatal("observation.result() = nil, want read failure")
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("observation error %q does not contain %q", err, fragment)
		}
	}
}
