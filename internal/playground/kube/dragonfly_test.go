package kube

import "testing"

// TestDragonflyNameHelpers locks the deterministic naming used by the
// chaos scenarios against the operator-side controller's resource
// names (internal/controller/dragonfly_resources.go). A typo here
// would silently break port-forwards / scale operations against the
// wrong (or missing) resource; this assertion fires at unit-test time
// instead of mid-scenario.
func TestDragonflyNameHelpers(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"StatefulSet", DragonflyStatefulSetName("playground", "iad"), "playground-dragonfly-iad"},
		{"SiteService", DragonflySiteServiceName("playground", "pdx"), "playground-dragonfly-pdx"},
		{"ActiveService", DragonflyActiveServiceName("playground"), "playground-dragonfly"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestDragonflyPodSelector(t *testing.T) {
	got := DragonflyPodSelector("playground", "iad")
	want := "app.kubernetes.io/name=dragonfly,app.kubernetes.io/instance=playground,shipstream.io/site=iad"
	if got != want {
		t.Errorf("DragonflyPodSelector = %q, want %q", got, want)
	}
}
