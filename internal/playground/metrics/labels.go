package metrics

import dto "github.com/prometheus/client_model/go"

// labelPair is a tiny alias so predicate.go can name the dto type
// without importing it itself.
type labelPair = dto.LabelPair
