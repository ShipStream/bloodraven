package controller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// stopAt parses an RFC 3339 test target into a pointInTime.stopTime value.
// A fractional second is kept, so tests can build the in-memory objects the
// controller must still refuse.
func stopAt(s string) metav1.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return metav1.NewTime(t)
}

// stopAtPtr is stopAt for the verification's optional stopTime.
func stopAtPtr(s string) *metav1.Time {
	t := stopAt(s)
	return &t
}
