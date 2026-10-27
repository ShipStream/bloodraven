package controller

import "testing"

func TestCheckPVCSubPath(t *testing.T) {
	for _, sub := range []string{"", "nightly", "nightly/site-a", "/nightly/", "a..b", "..x/y", "./x"} {
		if err := checkPVCSubPath(sub); err != nil {
			t.Errorf("checkPVCSubPath(%q) = %v, want nil", sub, err)
		}
	}
	for _, sub := range []string{"..", "../x", "x/..", "x/../../y", "/../x"} {
		if err := checkPVCSubPath(sub); err == nil {
			t.Errorf("checkPVCSubPath(%q) = nil, want error", sub)
		}
	}
}
