package main

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
)

// TestRecloneValues: the plugin writes the operator's full form, which the
// operator's interlock accepts (F-35), for a cold and a divergent site and
// with an explicit --gtid-prefix.
func TestRecloneValues(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", UID: "6f1c2d3e-0000-4000-8000-000000000001"},
		Spec:       v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}},
		Status: v1alpha1.MysqlFailoverGroupStatus{Sites: []v1alpha1.SiteStatus{
			{Name: "iad", DivergentGtid: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n4f22ab58-71ca-11e1-9e33-c80aa9429562:7"},
			{Name: "pdx"},
		}},
	}
	uid := ":uid=6f1c2d3e-0000-4000-8000-000000000001"
	for _, tc := range []struct {
		value, want string
	}{
		{brcontroller.RecloneAnnotationValue(fg, "pdx"), "pdx:confirm=orders" + uid},
		{brcontroller.RecloneAnnotationValue(fg, "iad"), "iad:confirm=orders" + uid + ":gtidPrefix=3e11fa47-71ca-11e1-9e33-c80aa9429562"},
		{brcontroller.RecloneAnnotationValueWithPrefix(fg, "iad", "3e11fa47"), "iad:confirm=orders" + uid + ":gtidPrefix=3e11fa47"},
	} {
		if tc.value != tc.want {
			t.Errorf("value = %q, want %q", tc.value, tc.want)
		}
		if err := brcontroller.ValidateRecloneAnnotation(fg, tc.value); err != nil {
			t.Errorf("%q rejected: %v", tc.value, err)
		}
	}
	// A prefix of another source, or a short one, fails client-side.
	for _, prefix := range []string{"4f22ab58", "3e11"} {
		if err := brcontroller.ValidateRecloneAnnotation(fg, brcontroller.RecloneAnnotationValueWithPrefix(fg, "iad", prefix)); err == nil {
			t.Errorf("gtidPrefix=%s accepted", prefix)
		}
	}
	// A prefix on a site that has not diverged is refused.
	if err := brcontroller.ValidateRecloneAnnotation(fg, brcontroller.RecloneAnnotationValueWithPrefix(fg, "pdx", "3e11fa47")); err == nil || !strings.Contains(err.Error(), "no recorded divergent GTID") {
		t.Errorf("gtidPrefix on pdx: %v", err)
	}
}
