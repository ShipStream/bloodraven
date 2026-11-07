package v1alpha1

import "fmt"

// SiteByName returns a pointer to the SiteSpec with the given name, or
// nil if no such site exists.
func (s *MysqlFailoverGroupSpec) SiteByName(name string) *SiteSpec {
	for i := range s.Sites {
		if s.Sites[i].Name == name {
			return &s.Sites[i]
		}
	}
	return nil
}

// MaxServerID is the largest MySQL server_id (an unsigned 32-bit value),
// the upper bound of spec.sites[].serverID.
const MaxServerID = 1<<32 - 1

// MySQLServerID returns the site's server_id as MySQL holds it. It is the
// one resolver every server_id consumer uses (the mysqld render and the
// backup stamp's expected server), so none of them can narrow or wrap
// it. It refuses a value outside 1..MaxServerID, which admission already
// rejects, so an object that bypassed validation fails instead of
// rendering a wrong id.
func (s SiteSpec) MySQLServerID() (uint32, error) {
	if s.ServerID < 1 || s.ServerID > MaxServerID {
		return 0, fmt.Errorf("site %q: serverID %d is outside 1..%d", s.Name, s.ServerID, uint64(MaxServerID))
	}
	return uint32(s.ServerID), nil
}

// SiteNames returns the names of every site in spec.sites, in declared
// order.
func (s *MysqlFailoverGroupSpec) SiteNames() []string {
	out := make([]string, len(s.Sites))
	for i := range s.Sites {
		out[i] = s.Sites[i].Name
	}
	return out
}

// PrimaryCandidates returns pointers to every site whose effective role
// is PrimaryCandidate, preserving declared order. These are the sites
// eligible for auto-promotion.
func (s *MysqlFailoverGroupSpec) PrimaryCandidates() []*SiteSpec {
	out := make([]*SiteSpec, 0, len(s.Sites))
	for i := range s.Sites {
		if s.Sites[i].IsPromotable() {
			out = append(out, &s.Sites[i])
		}
	}
	return out
}

// DefaultSeedSite returns the site that should be seeded with the
// initial primary during fresh-deploy bootstrap. When
// spec.splitBrainPolicy.sitePriorities is set, the first PrimaryCandidate
// that appears in the priority list wins; otherwise the first
// PrimaryCandidate in declared order wins. Returns nil when there is
// no PrimaryCandidate — callers must guard against that (it is a CRD
// validation error in a valid spec).
func (s *MysqlFailoverGroupSpec) DefaultSeedSite() *SiteSpec {
	if s.SplitBrainPolicy != nil {
		for _, name := range s.SplitBrainPolicy.SitePriorities {
			if site := s.SiteByName(name); site != nil && site.IsPromotable() {
				return site
			}
		}
	}
	for i := range s.Sites {
		if s.Sites[i].IsPromotable() {
			return &s.Sites[i]
		}
	}
	return nil
}

// PeerSiteNames returns every site name other than the given one, in
// declared order.
func (s *MysqlFailoverGroupSpec) PeerSiteNames(name string) []string {
	out := make([]string, 0, len(s.Sites))
	for i := range s.Sites {
		if s.Sites[i].Name != name {
			out = append(out, s.Sites[i].Name)
		}
	}
	return out
}

// IsReadOnlyReader reports whether the site is a non-promotable serving reader.
func (s SiteSpec) IsReadOnlyReader() bool {
	return s.EffectiveRole() == SiteRoleReadOnly
}

// EffectiveMaxLagSeconds returns the group replication threshold, including
// the API default for objects constructed outside admission.
func (s *MysqlFailoverGroupSpec) EffectiveMaxLagSeconds() int64 {
	if s.Replication == nil || s.Replication.MaxLagSeconds == 0 {
		return 300
	}
	return s.Replication.MaxLagSeconds
}

// EffectiveReadOnlyMaxLagSeconds returns the reader endpoint threshold. An
// explicit zero is meaningful; nil inherits the group threshold.
func (s *MysqlFailoverGroupSpec) EffectiveReadOnlyMaxLagSeconds() int64 {
	if s.Replication != nil && s.Replication.ReadOnlyMaxLagSeconds != nil {
		return *s.Replication.ReadOnlyMaxLagSeconds
	}
	return s.EffectiveMaxLagSeconds()
}
