package license

import (
	"testing"
	"time"
)

func TestResolvePrecedence(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	priv, kid, keys := testAuthority(t)
	good := mustEncode(t, priv, kid, validClaims(now))
	c := validClaims(now)
	c["org"] = "Operator Inc"
	operatorGood := mustEncode(t, priv, kid, c)
	bad := "not-a-jwt"

	t.Run("mfg beats operator", func(t *testing.T) {
		r := Resolve(good, operatorGood, keys, now)
		if r.Source != SourceMFG || r.Organization != "Acme Corp" || !r.Valid {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("operator when mfg empty", func(t *testing.T) {
		r := Resolve("  ", operatorGood, keys, now)
		if r.Source != SourceOperator || r.Organization != "Operator Inc" || !r.Valid {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("community when both empty", func(t *testing.T) {
		r := Resolve("", "", keys, now)
		if !r.Valid || !r.Community || r.Edition != EditionCommunity || r.Source != SourceNone {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("invalid mfg does not fall through", func(t *testing.T) {
		r := Resolve(bad, operatorGood, keys, now)
		if r.Valid || r.Source != SourceMFG || r.Reason == "" {
			t.Fatalf("invalid mfg must not use operator token: %+v", r)
		}
	})
	t.Run("whitespace only is community", func(t *testing.T) {
		r := Resolve("\n\t", "  ", keys, now)
		if !r.Community || r.Source != SourceNone {
			t.Fatalf("got %+v", r)
		}
	})
}
