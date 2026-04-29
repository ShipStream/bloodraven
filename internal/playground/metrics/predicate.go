package metrics

// Counter returns the value of a counter metric matching the given
// label filter. Returns (0, false) when no matching series is found.
//
// Label filters need not be exhaustive: a series matches if it has
// every label in the filter set to the requested value. Extra labels
// on the series are ignored.
func (s *Snapshot) Counter(name string, labels map[string]string) (float64, bool) {
	fam, ok := s.Families[name]
	if !ok {
		return 0, false
	}
	for _, m := range fam.GetMetric() {
		if !labelsMatch(m.GetLabel(), labels) {
			continue
		}
		if m.Counter != nil && m.Counter.Value != nil {
			return *m.Counter.Value, true
		}
	}
	return 0, false
}

// Gauge returns the value of a gauge series matching the labels
// filter, or (0, false) when there is no match.
func (s *Snapshot) Gauge(name string, labels map[string]string) (float64, bool) {
	fam, ok := s.Families[name]
	if !ok {
		return 0, false
	}
	for _, m := range fam.GetMetric() {
		if !labelsMatch(m.GetLabel(), labels) {
			continue
		}
		if m.Gauge != nil && m.Gauge.Value != nil {
			return *m.Gauge.Value, true
		}
	}
	return 0, false
}

// HistogramSampleCount returns the cumulative sample count of a
// histogram series matching the labels filter.
func (s *Snapshot) HistogramSampleCount(name string, labels map[string]string) (uint64, bool) {
	fam, ok := s.Families[name]
	if !ok {
		return 0, false
	}
	for _, m := range fam.GetMetric() {
		if !labelsMatch(m.GetLabel(), labels) {
			continue
		}
		if m.Histogram != nil && m.Histogram.SampleCount != nil {
			return *m.Histogram.SampleCount, true
		}
	}
	return 0, false
}

func labelsMatch(have []*labelPair, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	got := make(map[string]string, len(have))
	for _, lp := range have {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
