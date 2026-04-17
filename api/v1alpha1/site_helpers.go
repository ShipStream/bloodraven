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
