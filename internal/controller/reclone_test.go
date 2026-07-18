package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func recloneFG(sites []string, divergentPerSite map[string]string) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
	}
	for _, name := range sites {
		fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: name})
		st := v1alpha1.SiteStatus{Name: name}
		if g, ok := divergentPerSite[name]; ok {
			st.DivergentGtid = g
			st.RecoveryState = "RecoveryBlocked"
		}
		fg.Status.Sites = append(fg.Status.Sites, st)
	}
	return fg
}

func TestParseRecloneAnnotation(t *testing.T) {
	cases := []struct {
		in     string
		site   string
		prefix string
	}{
		{"iad", "iad", ""},
		{"iad:a1b2c3d4", "iad", "a1b2c3d4"},
		{"  iad:a1b2c3d4  ", "iad", "a1b2c3d4"},
		{"iad:", "iad", ""},
		{":a1b2c3d4", "", "a1b2c3d4"},
		{"", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := parseRecloneAnnotation(tc.in)
			if got.Site != tc.site || got.GtidPrefix != tc.prefix {
				t.Errorf("parse(%q) = %+v, want {Site: %q, GtidPrefix: %q}", tc.in, got, tc.site, tc.prefix)
			}
		})
	}
}

func TestValidateRecloneRequest_EmptySite(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, nil)
	err := validateRecloneRequest(fg, RecloneRequest{})
	if err == nil {
		t.Fatal("expected error on empty site")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error = %v, want 'empty'", err)
	}
}

func TestValidateRecloneRequest_UnknownSite(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, nil)
	err := validateRecloneRequest(fg, RecloneRequest{Site: "sfo"})
	if err == nil {
		t.Fatal("expected error on unknown site")
	}
	if !strings.Contains(err.Error(), "unknown site") {
		t.Errorf("error = %v, want 'unknown site'", err)
	}
}

func TestValidateRecloneRequest_ColdReclone_BareSite_RequiresConfirm(t *testing.T) {
	// Post-AUDIT L3: even without divergence, a cold reclone wipes
	// the datadir and requires a confirm=<group> token.
	fg := recloneFG([]string{"iad", "pdx"}, nil)
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad"})
	if err == nil {
		t.Fatal("expected cold reclone without confirm token to be rejected")
	}
	if !strings.Contains(err.Error(), "confirm=") {
		t.Errorf("error should suggest confirm= token, got %v", err)
	}
}

func TestValidateRecloneRequest_ColdReclone_WithConfirm_OK(t *testing.T) {
	tests := []struct {
		name      string
		sites     []string
		recipient string
		role      v1alpha1.SiteRole
	}{
		{name: "primary candidate", sites: []string{"iad", "pdx"}, recipient: "iad"},
		{name: "read-only reader", sites: []string{"iad", "pdx", "reader"}, recipient: "reader", role: v1alpha1.SiteRoleReadOnly},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fg := recloneFG(tt.sites, nil)
			if tt.role != "" {
				fg.Spec.SiteByName(tt.recipient).Role = tt.role
			}
			if err := validateRecloneRequest(fg, RecloneRequest{Site: tt.recipient, GtidPrefix: "confirm=orders"}); err != nil {
				t.Errorf("cold reclone with correct confirm token should succeed, got %v", err)
			}
		})
	}
}

func TestValidateRecloneRequest_ColdReclone_WrongConfirm_Rejected(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, nil)
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: "confirm=wrongfg"})
	if err == nil {
		t.Fatal("expected wrong confirm token to be rejected")
	}
}

func TestValidateRecloneRequest_DivergentSite_BareSite_Rejected(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{
		"iad": "a1b2c3d4-0000-0000-0000-000000000000:11-15",
	})
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad"})
	if err == nil {
		t.Fatal("expected error when divergent GTID present and no prefix given")
	}
	// The error must include the observed GTID and a usable example.
	if !strings.Contains(err.Error(), "a1b2c3d4") {
		t.Errorf("error = %v, should quote the divergent GTID", err)
	}
	if !strings.Contains(err.Error(), "reclone-site=iad:") {
		t.Errorf("error = %v, should offer a corrected annotation value", err)
	}
}

func TestValidateRecloneRequest_DivergentSite_ShortPrefix_Rejected(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{
		"iad": "a1b2c3d4-0000-0000-0000-000000000000:11-15",
	})
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: "a1b2"})
	if err == nil {
		t.Fatal("expected error for short prefix")
	}
	if !strings.Contains(err.Error(), "shorter than") {
		t.Errorf("error = %v, want 'shorter than'", err)
	}
}

func TestValidateRecloneRequest_DivergentSite_WrongPrefix_Rejected(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{
		"iad": "a1b2c3d4-0000-0000-0000-000000000000:11-15",
	})
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: "deadbeef"})
	if err == nil {
		t.Fatal("expected error when prefix mismatches observed GTID")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Errorf("error = %v, want 'does not match'", err)
	}
}

func TestValidateRecloneRequest_DivergentSite_MatchingPrefix_OK(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{
		"iad": "a1b2c3d4-0000-0000-0000-000000000000:11-15",
	})
	if err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: "a1b2c3d4"}); err != nil {
		t.Errorf("matching prefix should pass, got %v", err)
	}
}

func TestValidateRecloneRequest_DivergentSite_ExtendedPrefix_OK(t *testing.T) {
	// Admin paste the full GTID minus the ":11-15" range — should pass.
	gtid := "a1b2c3d4-0000-0000-0000-000000000000:11-15"
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{"iad": gtid})
	longPrefix := "a1b2c3d4-0000-0000-0000-000000000000"
	if err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: longPrefix}); err != nil {
		t.Errorf("extended prefix of observed GTID should pass, got %v", err)
	}
}

// Cold-reclone now requires an explicit confirm=<group> token even
// when the target site has no recorded divergent GTID (AUDIT L3).
// A GTID prefix from another divergent site is not a valid token.
func TestValidateRecloneRequest_ColdReclone_CrossSitePrefixRejected(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{
		"pdx": "f00dbabe-0000-0000-0000-000000000000:20-25",
	})
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: "f00dbabe"})
	if err == nil {
		t.Fatal("cold reclone should reject a cross-site GTID prefix; confirm=<group> is required")
	}
}

// Fat-finger scenario from the wishlist: admin meant pdx but typed iad,
// and pasted pdx's divergent-GTID prefix. When iad itself is divergent,
// the cross-site prefix mismatch must be rejected — under the old code
// this would have wiped iad.
func TestValidateRecloneRequest_DivergentSite_CrossSitePrefixRejected(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{
		"iad": "a1b2c3d4-0000-0000-0000-000000000000:11-15",
		"pdx": "f00dbabe-0000-0000-0000-000000000000:20-25",
	})
	err := validateRecloneRequest(fg, RecloneRequest{Site: "iad", GtidPrefix: "f00dbabe"})
	if err == nil {
		t.Fatal("cross-site prefix leak should be rejected")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Errorf("error = %v, want 'does not match'", err)
	}
}
