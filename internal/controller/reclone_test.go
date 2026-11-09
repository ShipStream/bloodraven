package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// recloneTestUID is the uid of recloneFG's group.
const recloneTestUID = "6f1c2d3e-0000-4000-8000-000000000001"

func recloneFG(sites []string, divergentPerSite map[string]string) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default", UID: recloneTestUID},
	}
	for i, name := range sites {
		fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: name, ServerID: int64(101 + i)})
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
		in                 string
		site, confirm, uid string
		prefix             string
		malformed          bool
	}{
		{in: "iad:confirm=orders:uid=u-1", site: "iad", confirm: "orders", uid: "u-1"},
		{in: "  iad:confirm=orders:uid=u-1:gtidPrefix=a1b2c3d4\n", site: "iad", confirm: "orders", uid: "u-1", prefix: "a1b2c3d4"},
		{in: "iad:gtidPrefix=a1b2c3d4:uid=u-1:confirm=orders", site: "iad", confirm: "orders", uid: "u-1", prefix: "a1b2c3d4"},
		// The positional prefix of earlier releases is not a pair.
		{in: "iad:a1b2c3d4", site: "iad", malformed: true},
		{in: "iad", site: "iad", malformed: true},
		{in: "iad:confirm=orders", site: "iad", malformed: true},
		{in: "iad:gtidPrefix=a1b2c3d4", site: "iad", malformed: true},
		{in: "iad:confirm=orders:uid=u-1:extra=1", site: "iad", malformed: true},
		{in: ":confirm=orders:uid=u-1", site: "", malformed: true},
		{in: "", site: "", malformed: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := parseRecloneAnnotation(tc.in)
			if got.Site != tc.site || (got.Err != nil) != tc.malformed {
				t.Fatalf("parse(%q) = %+v, want site %q malformed=%v", tc.in, got, tc.site, tc.malformed)
			}
			if !tc.malformed && (got.Confirm != tc.confirm || got.UID != tc.uid || got.GtidPrefix != tc.prefix) {
				t.Errorf("parse(%q) = %+v", tc.in, got)
			}
		})
	}
}

// TestValidateRecloneAnnotation runs raw values through the parser and the
// interlock (A-78, F-35): both forms carry confirm= and uid=, and the
// divergent form adds gtidPrefix=, a prefix of the source UUID.
func TestValidateRecloneAnnotation(t *testing.T) {
	const divergent = "a1b2c3d4-0000-0000-0000-000000000000:11-15"
	const otherDivergent = "f00dbabe-0000-0000-0000-000000000000:20-25"
	full := ":confirm=orders:uid=" + recloneTestUID
	cases := []struct {
		name    string
		value   string
		diverge map[string]string
		reader  bool
		wantErr string // "" accepts
	}{
		{name: "cold form", value: "iad" + full},
		{name: "cold form, pairs in any order", value: "iad:uid=" + recloneTestUID + ":confirm=orders"},
		{name: "cold form on a reader", value: "reader" + full, reader: true},
		{name: "divergent form", value: "iad" + full + ":gtidPrefix=a1b2c3d4", diverge: map[string]string{"iad": divergent}},
		{name: "divergent form, full source UUID", value: "iad" + full + ":gtidPrefix=a1b2c3d4-0000-0000-0000-000000000000", diverge: map[string]string{"iad": divergent}},

		{name: "empty", value: "", wantErr: "names no site"},
		{name: "unknown site", value: "sfo" + full, wantErr: "unknown site"},
		{name: "bare site", value: "iad", wantErr: "confirm= pair is missing"},
		{name: "cold without uid", value: "iad:confirm=orders", wantErr: "uid= pair is missing"},
		{name: "cold, wrong confirm", value: "iad:confirm=wrongfg:uid=" + recloneTestUID, wantErr: "does not name this object"},
		{name: "cold, a re-created group's uid", value: "iad:confirm=orders:uid=6f1c2d3e-0000-4000-8000-000000000002", wantErr: "earlier object of the same name"},
		{name: "positional prefix of earlier releases", value: "iad:a1b2c3d4", diverge: map[string]string{"iad": divergent}, wantErr: "must be key=value"},
		{name: "divergent form without confirm", value: "iad:uid=" + recloneTestUID + ":gtidPrefix=a1b2c3d4", diverge: map[string]string{"iad": divergent}, wantErr: "confirm= pair is missing"},
		{name: "divergent site without gtidPrefix", value: "iad" + full, diverge: map[string]string{"iad": divergent}, wantErr: "a1b2c3d4"},
		{name: "short gtidPrefix", value: "iad" + full + ":gtidPrefix=a1b2", diverge: map[string]string{"iad": divergent}, wantErr: "at least 8"},
		{name: "non-hex gtidPrefix", value: "iad" + full + ":gtidPrefix=a1b2c3dz", diverge: map[string]string{"iad": divergent}, wantErr: "at least 8"},
		{name: "wrong gtidPrefix", value: "iad" + full + ":gtidPrefix=deadbeef", diverge: map[string]string{"iad": divergent}, wantErr: "does not match"},
		{name: "another site's prefix", value: "iad" + full + ":gtidPrefix=f00dbabe", diverge: map[string]string{"iad": divergent, "pdx": otherDivergent}, wantErr: "does not match"},
		{name: "gtidPrefix on a site without divergence", value: "iad" + full + ":gtidPrefix=f00dbabe", diverge: map[string]string{"pdx": otherDivergent}, wantErr: "no recorded divergent GTID"},
		// The interval of the GTID would put a ':' in the value.
		{name: "a ':' in a value", value: "iad" + full + ":gtidPrefix=" + divergent, diverge: map[string]string{"iad": divergent}, wantErr: "must be key=value"},
		{name: "unknown key", value: "iad" + full + ":force=true", wantErr: "unknown key"},
		{name: "repeated key", value: "iad" + full + ":confirm=orders", wantErr: "more than once"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := recloneFG([]string{"iad", "pdx", "reader"}, tc.diverge)
			if tc.reader {
				fg.Spec.SiteByName("reader").Role = v1alpha1.SiteRoleReadOnly
			}
			err := validateRecloneRequest(fg, parseRecloneAnnotation(tc.value))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("%q rejected: %v", tc.value, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%q: err = %v, want %q", tc.value, err, tc.wantErr)
			}
		})
	}
}

// TestValidateRecloneAnnotation_RejectionPrintsFullForm: a rejected value
// is answered with the full form the site needs.
func TestValidateRecloneAnnotation_RejectionPrintsFullForm(t *testing.T) {
	fg := recloneFG([]string{"iad", "pdx"}, map[string]string{"iad": "a1b2c3d4-0000-0000-0000-000000000000:11-15"})
	err := validateRecloneRequest(fg, parseRecloneAnnotation("iad:confirm=orders"))
	want := v1alpha1.RecloneAnnotation + "=iad:confirm=orders:uid=" + recloneTestUID + ":gtidPrefix=a1b2c3d4"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want it to print %q", err, want)
	}
	fg = recloneFG([]string{"iad", "pdx"}, nil)
	err = validateRecloneRequest(fg, parseRecloneAnnotation("iad"))
	want = v1alpha1.RecloneAnnotation + "=iad:confirm=orders:uid=" + recloneTestUID
	if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "gtidPrefix") {
		t.Fatalf("err = %v, want it to print %q", err, want)
	}
}
