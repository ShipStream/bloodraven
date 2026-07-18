package v1alpha1

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
// is primary-candidate, preserving declared order. These are the sites
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
// spec.splitBrainPolicy.sitePriorities is set, the first primary-
// candidate that appears in the priority list wins; otherwise the first
// primary-candidate in declared order wins. Returns nil when there is
// no primary-candidate — callers must guard against that (it is a CRD
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
