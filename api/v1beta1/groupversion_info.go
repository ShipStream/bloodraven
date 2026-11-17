package v1beta1

import (
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "shipstream.io", Version: "v1beta1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme

	// DeprecatedGroupVersion is shipstream.io/v1alpha1. The CRDs serve it
	// beside GroupVersion with the same schema until it is removed
	// (hack/crd-served-versions writes that version of each CRD), and every
	// release before v1.8.0 serves nothing else.
	DeprecatedGroupVersion = schema.GroupVersion{Group: GroupVersion.Group, Version: "v1alpha1"}
)

// AddToSchemeAt registers the kinds of this package at gv instead of
// GroupVersion. The two served versions have one schema, so the same Go
// types read and write either. It is for a client that must talk to an API
// server that does not serve GroupVersion yet (a cluster before its upgrade
// to v1.8.0) and for tests of the deprecated version; the operator and the
// kubectl plugin use AddToScheme.
//
// A scheme takes the kinds at one version only: registering them at both
// leaves a typed client unable to tell which one an object is.
func AddToSchemeAt(s *runtime.Scheme, gv schema.GroupVersion) error {
	if gv == GroupVersion {
		return AddToScheme(s)
	}
	if gv.Group != GroupVersion.Group {
		return fmt.Errorf("%s is not a version of %s", gv, GroupVersion.Group)
	}
	own := runtime.NewScheme()
	if err := AddToScheme(own); err != nil {
		return err
	}
	for kind, typ := range own.KnownTypes(GroupVersion) {
		obj, ok := reflect.New(typ).Interface().(runtime.Object)
		if !ok {
			// metav1's option and event kinds, which AddToGroupVersion
			// registers below.
			continue
		}
		s.AddKnownTypeWithName(gv.WithKind(kind), obj)
	}
	metav1.AddToGroupVersion(s, gv)
	return nil
}
