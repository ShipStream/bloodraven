package kube

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// runtimeScheme aliases runtime.Scheme so it can be returned from
// internal helpers without leaking the dependency through every
// caller signature.
type runtimeScheme = runtime.Scheme

func defaultScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(corev1.AddToScheme(s))
	utilruntime.Must(networkingv1.AddToScheme(s))
	return s
}
