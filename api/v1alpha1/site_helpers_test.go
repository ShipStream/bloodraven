package v1alpha1

import "testing"

func TestSiteRoleHelpers(t *testing.T) {
	tests := []struct {
		name       string
		role       SiteRole
		promotable bool
		reader     bool
	}{
		{name: "default", promotable: true},
		{name: "candidate", role: SiteRolePrimaryCandidate, promotable: true},
		{name: "dr only", role: SiteRoleDROnly},
		{name: "reader", role: SiteRoleReadOnly, reader: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SiteSpec{Role: tt.role}
			if got := s.IsPromotable(); got != tt.promotable {
				t.Fatalf("IsPromotable() = %v, want %v", got, tt.promotable)
			}
			if got := s.IsReadOnlyReader(); got != tt.reader {
				t.Fatalf("IsReadOnlyReader() = %v, want %v", got, tt.reader)
			}
		})
	}
}

func TestPeerSiteNamesIncludesReaders(t *testing.T) {
	s := MysqlFailoverGroupSpec{Sites: []SiteSpec{
		{Name: "iad"}, {Name: "pdx"}, {Name: "reader", Role: SiteRoleReadOnly},
	}}
	got := s.PeerSiteNames("iad")
	if len(got) != 2 || got[0] != "pdx" || got[1] != "reader" {
		t.Fatalf("PeerSiteNames() = %v", got)
	}
}

func TestEffectiveReplicationLag(t *testing.T) {
	zero := int64(0)
	tests := []struct {
		name string
		spec MysqlFailoverGroupSpec
		max  int64
		read int64
	}{
		{name: "defaults", max: 300, read: 300},
		{name: "inherits", spec: MysqlFailoverGroupSpec{Replication: &ReplicationSpec{MaxLagSeconds: 42}}, max: 42, read: 42},
		{name: "explicit zero", spec: MysqlFailoverGroupSpec{Replication: &ReplicationSpec{MaxLagSeconds: 42, ReadOnlyMaxLagSeconds: &zero}}, max: 42, read: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.EffectiveMaxLagSeconds(); got != tt.max {
				t.Fatalf("EffectiveMaxLagSeconds() = %d, want %d", got, tt.max)
			}
			if got := tt.spec.EffectiveReadOnlyMaxLagSeconds(); got != tt.read {
				t.Fatalf("EffectiveReadOnlyMaxLagSeconds() = %d, want %d", got, tt.read)
			}
		})
	}
}
