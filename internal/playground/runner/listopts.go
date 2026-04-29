package runner

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// metav1ListOpts is a tiny alias so forensic.go does not have to
// import metav1 directly.
type metav1ListOpts = metav1.ListOptions
