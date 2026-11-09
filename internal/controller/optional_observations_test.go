package controller

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/mysql"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A-44/F-22: a Dragonfly INFO field that was not reported leaves its status
// observation unset; a reported value is copied, a reported -1 stays -1.
func TestBuildDragonflyStatus_OptionalSyncObservations(t *testing.T) {
	fg := newTestFG()
	fg.Status.ActiveSite = "dc1"
	replica := func(info string) DragonflySiteSnapshot {
		return DragonflySiteSnapshot{Name: "dc2", Reachable: true, Info: dragonfly.ParseInfoReplication(info)}
	}
	for _, tc := range []struct {
		name     string
		info     string
		wantSync string
		wantIO   string
	}{
		{"neither reported", "role:slave\nmaster_link_status:up\n", "unset", "unset"},
		{"never received data", "role:slave\nmaster_link_status:up\nmaster_sync_in_progress:0\nmaster_last_io_seconds_ago:-1\n", "false", "-1"},
		{"syncing", "role:slave\nmaster_link_status:up\nmaster_sync_in_progress:1\nmaster_last_io_seconds_ago:0\n", "true", "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			master := DragonflySiteSnapshot{Name: "dc1", Reachable: true, Info: dragonfly.ReplicationInfo{Role: "master"}}
			st := buildDragonflyStatus(fg, []DragonflySiteSnapshot{master, replica(tc.info)})
			var site *v1alpha1.DragonflySiteStatus
			for i := range st.Sites {
				if st.Sites[i].Name == "dc2" {
					site = &st.Sites[i]
				}
			}
			if site == nil {
				t.Fatal("dc2 missing")
			}
			sync, io := "unset", "unset"
			if site.SyncInProgress != nil {
				sync = map[bool]string{true: "true", false: "false"}[*site.SyncInProgress]
			}
			if site.LastIOSecondsAgo != nil {
				io = map[int64]string{-1: "-1", 0: "0"}[*site.LastIOSecondsAgo]
			}
			if sync != tc.wantSync || io != tc.wantIO {
				t.Fatalf("syncInProgress=%s lastIOSecondsAgo=%s, want %s/%s", sync, io, tc.wantSync, tc.wantIO)
			}
		})
	}
}

// Equal pointer-valued observations are equal statuses: a re-observed
// unchanged site does not cause a status write.
func TestDragonflyStatusEqual_ComparesOptionalValues(t *testing.T) {
	a := &v1alpha1.DragonflyStatus{Sites: []v1alpha1.DragonflySiteStatus{{Name: "dc2", SyncInProgress: new(false), LastIOSecondsAgo: new(int64(3))}}}
	b := &v1alpha1.DragonflyStatus{Sites: []v1alpha1.DragonflySiteStatus{{Name: "dc2", SyncInProgress: new(false), LastIOSecondsAgo: new(int64(3))}}}
	if !dragonflyStatusEqual(a, b) {
		t.Fatal("equal observations compared unequal")
	}
	b.Sites[0].SyncInProgress = new(true)
	if dragonflyStatusEqual(a, b) {
		t.Fatal("a changed syncInProgress compared equal")
	}
	b.Sites[0].SyncInProgress = nil
	if dragonflyStatusEqual(a, b) {
		t.Fatal("an unreported syncInProgress compared equal to a reported false")
	}
}

// A-46/F-13: the operator copies each coverage flag through as the sidecar
// reported it; an omitted flag clears a previously observed value instead
// of keeping it or recording false.
func TestApplyCoverage_CopiesUnknownFlagsThrough(t *testing.T) {
	now := metav1.Now()
	site := &v1alpha1.SiteEncryptionStatus{Name: "dc1", Coverage: &v1alpha1.SiteEncryptionCoverage{
		SystemTablespaceEncrypted: new(true), RedoLogEncrypted: new(true), UndoLogEncrypted: new(true), BinlogEncrypted: new(true),
	}}
	live := &mysql.KeyringStatus{
		Component: &mysql.KeyringComponentStatus{Name: "component_keyring_file", ReadOnly: true},
		Coverage:  &mysql.KeyringCoverage{RedoLogEncrypted: new(false), UndoLogEncrypted: new(true)},
	}
	applyCoverage(site, live, now)
	c := site.Coverage
	for name, tc := range map[string]struct {
		got  *bool
		want string
	}{
		"systemTablespaceEncrypted": {c.SystemTablespaceEncrypted, "unset"},
		"binlogEncrypted":           {c.BinlogEncrypted, "unset"},
		"redoLogEncrypted":          {c.RedoLogEncrypted, "false"},
		"undoLogEncrypted":          {c.UndoLogEncrypted, "true"},
		"keyringReadOnly":           {c.KeyringReadOnly, "true"},
	} {
		got := "unset"
		if tc.got != nil {
			got = map[bool]string{true: "true", false: "false"}[*tc.got]
		}
		if got != tc.want {
			t.Errorf("%s = %s, want %s", name, got, tc.want)
		}
	}
	if live.Coverage.UndoLogEncrypted == c.UndoLogEncrypted {
		t.Error("status aliases the probe's value")
	}
}

// TestApplyCoverage_EachFlagMissingFalseTrue (F-13): each coverage flag,
// unreported, false and true, lands in status as nil, false and true,
// whatever an earlier observation recorded, without touching the others.
// keyringReadOnly is unreported when the sidecar sends no component.
func TestApplyCoverage_EachFlagMissingFalseTrue(t *testing.T) {
	type flag struct {
		name   string
		set    func(*mysql.KeyringStatus, *bool)
		status func(*v1alpha1.SiteEncryptionCoverage) *bool
	}
	flags := []flag{
		{"systemTablespaceEncrypted", func(l *mysql.KeyringStatus, b *bool) { l.Coverage.SystemTablespaceEncrypted = b },
			func(c *v1alpha1.SiteEncryptionCoverage) *bool { return c.SystemTablespaceEncrypted }},
		{"redoLogEncrypted", func(l *mysql.KeyringStatus, b *bool) { l.Coverage.RedoLogEncrypted = b },
			func(c *v1alpha1.SiteEncryptionCoverage) *bool { return c.RedoLogEncrypted }},
		{"undoLogEncrypted", func(l *mysql.KeyringStatus, b *bool) { l.Coverage.UndoLogEncrypted = b },
			func(c *v1alpha1.SiteEncryptionCoverage) *bool { return c.UndoLogEncrypted }},
		{"binlogEncrypted", func(l *mysql.KeyringStatus, b *bool) { l.Coverage.BinlogEncrypted = b },
			func(c *v1alpha1.SiteEncryptionCoverage) *bool { return c.BinlogEncrypted }},
		{"keyringReadOnly", func(l *mysql.KeyringStatus, b *bool) {
			if b == nil {
				l.Component = nil
				return
			}
			l.Component.ReadOnly = *b
		}, func(c *v1alpha1.SiteEncryptionCoverage) *bool { return c.KeyringReadOnly }},
	}
	show := func(b *bool) string {
		if b == nil {
			return "missing"
		}
		return map[bool]string{true: "true", false: "false"}[*b]
	}
	for _, f := range flags {
		for _, state := range []*bool{nil, new(false), new(true)} {
			t.Run(f.name+"/"+show(state), func(t *testing.T) {
				// An earlier observation recorded every flag, opposite to
				// what this probe reports where it reports one.
				prior := true
				if state != nil {
					prior = !*state
				}
				site := &v1alpha1.SiteEncryptionStatus{Name: "dc1", Coverage: &v1alpha1.SiteEncryptionCoverage{
					SystemTablespaceEncrypted: new(prior), RedoLogEncrypted: new(prior), UndoLogEncrypted: new(prior),
					BinlogEncrypted: new(prior), KeyringReadOnly: new(prior),
				}}
				live := &mysql.KeyringStatus{
					Component: &mysql.KeyringComponentStatus{Name: "component_keyring_file", ReadOnly: true},
					Coverage: &mysql.KeyringCoverage{SystemTablespaceEncrypted: new(true), RedoLogEncrypted: new(true),
						UndoLogEncrypted: new(true), BinlogEncrypted: new(true)},
				}
				f.set(live, state)
				applyCoverage(site, live, metav1.Now())
				for _, other := range flags {
					want := "true"
					if other.name == f.name {
						want = show(state)
						if state == nil && other.name == "keyringReadOnly" {
							// No component in the probe: the earlier
							// observation of it stands.
							want = show(&prior)
						}
					}
					if got := show(other.status(site.Coverage)); got != want {
						t.Errorf("%s = %s, want %s", other.name, got, want)
					}
				}
			})
		}
	}
}
