package license

import (
	"strings"
	"time"
)

// Resolve picks the token for one failover group and verifies it.
// Precedence: trimmed MFG field, else trimmed operator default, else
// community. A present-but-invalid MFG token does not fall through to
// the operator default — the field was set and failed.
func Resolve(mfgToken, operatorToken string, keys KeyLookup, now time.Time) Result {
	if token := strings.TrimSpace(mfgToken); token != "" {
		r := Verify(token, keys, now)
		r.Source = SourceMFG
		return r
	}
	if token := strings.TrimSpace(operatorToken); token != "" {
		r := Verify(token, keys, now)
		r.Source = SourceOperator
		return r
	}
	return Community()
}

// Community is the no-license observation.
func Community() Result {
	return Result{
		Valid:     true,
		Community: true,
		Edition:   EditionCommunity,
		Source:    SourceNone,
	}
}
