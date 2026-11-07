package scenarios

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestFailoverObservedSnapshots(t *testing.T) {
	for _, tt := range []struct {
		name     string
		active   string
		original v1alpha1.SiteState
		standby  v1alpha1.SiteState
		reader   v1alpha1.SiteState
		want     bool
	}{
		{name: "original primary", active: "iad", original: v1alpha1.SiteStateWritable, standby: v1alpha1.SiteStateReadOnly},
		{name: "early promotion authority", active: "pdx", original: v1alpha1.SiteStateUnreachable, standby: v1alpha1.SiteStateReadOnly},
		{name: "empty authority", original: v1alpha1.SiteStateUnreachable, standby: v1alpha1.SiteStateReadOnly},
		{name: "empty authority with writable standby", original: v1alpha1.SiteStateUnreachable, standby: v1alpha1.SiteStateWritable},
		{name: "missing active status", active: "pdx", original: v1alpha1.SiteStateUnreachable},
		{name: "unreachable target", active: "pdx", original: v1alpha1.SiteStateUnreachable, standby: v1alpha1.SiteStateUnreachable},
		{name: "wrong writable site", active: "pdx", original: v1alpha1.SiteStateWritable, standby: v1alpha1.SiteStateReadOnly},
		{name: "two writable candidates", active: "pdx", original: v1alpha1.SiteStateWritable, standby: v1alpha1.SiteStateWritable},
		{name: "writable reader anomaly", active: "pdx", original: v1alpha1.SiteStateUnreachable, standby: v1alpha1.SiteStateWritable, reader: v1alpha1.SiteStateWritable},
		{name: "confirmed promotion while old primary down", active: "pdx", original: v1alpha1.SiteStateUnreachable, standby: v1alpha1.SiteStateWritable, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mfg := &v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{
				ActiveSite:         tt.active,
				LastFailoverTarget: "pdx",
				Sites: []v1alpha1.SiteStatus{
					{Name: "iad", State: tt.original},
					{Name: "reader", State: tt.reader},
				},
			}}
			if tt.standby != "" {
				mfg.Status.Sites = append(mfg.Status.Sites, v1alpha1.SiteStatus{Name: "pdx", State: tt.standby})
			}
			if got, msg := failoverObserved(mfg, "iad"); got != tt.want {
				t.Fatalf("failoverObserved = %v, want %v (%s)", got, tt.want, msg)
			}
		})
	}
}

func TestS41ReaderRepointedStatusSequence(t *testing.T) {
	state := &s41RunState{
		topo:    readerTopology{active: "iad", standby: "pdx", reader: "reader"},
		oldHost: "old-primary.example",
		newHost: "new-primary.example",
	}
	for _, tt := range []struct {
		name        string
		active      string
		standby     v1alpha1.SiteState
		readerReady bool
		missing     bool
		want        bool
		wantErr     string
	}{
		{name: "early promotion authority", active: "pdx", standby: v1alpha1.SiteStateReadOnly},
		{name: "authority cleared before writable confirmation", standby: v1alpha1.SiteStateReadOnly},
		{name: "promotion confirmed reader still on old source", active: "pdx", standby: v1alpha1.SiteStateWritable},
		{name: "missing reader status", active: "pdx", standby: v1alpha1.SiteStateWritable, missing: true},
		{name: "empty authority while reader ready", standby: v1alpha1.SiteStateWritable, readerReady: true},
		{name: "target no longer observed writable", active: "pdx", standby: v1alpha1.SiteStateReadOnly, readerReady: true},
		{name: "reader and primary confirmed", active: "pdx", standby: v1alpha1.SiteStateWritable, readerReady: true, want: true},
		{name: "second promotion to original primary", active: "iad", standby: v1alpha1.SiteStateReadOnly, wantErr: `active site changed again during reader repoint: "iad"`},
		{name: "second promotion to another site", active: "other", standby: v1alpha1.SiteStateReadOnly, wantErr: `active site changed again during reader repoint: "other"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mfg := &v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{
				ActiveSite: tt.active,
				Sites: []v1alpha1.SiteStatus{
					{Name: "iad", State: v1alpha1.SiteStateUnreachable},
					{Name: "pdx", State: tt.standby},
				},
			}}
			if !tt.missing {
				lag := int64(0)
				host := state.oldHost
				if tt.readerReady {
					host = state.newHost
				}
				mfg.Status.Sites = append(mfg.Status.Sites, v1alpha1.SiteStatus{
					Name: "reader", State: v1alpha1.SiteStateReadOnly, Replicating: true,
					SourceHost: host, SourceConvergenceState: v1alpha1.SourceConvergenceConverged,
					SecondsBehindSource: &lag,
				})
			}
			got, msg, err := state.readerRepointed(mfg)
			if got != tt.want {
				t.Fatalf("readerRepointed = %v, want %v (%s)", got, tt.want, msg)
			}
			if tt.wantErr == "" && err != nil || tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("readerRepointed error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
