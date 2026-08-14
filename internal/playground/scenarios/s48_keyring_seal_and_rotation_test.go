package scenarios

import (
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestS48RefusalHoldDone(t *testing.T) {
	seen := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	hold := 30 * time.Second

	cases := []struct {
		name        string
		eventSeenAt time.Time
		now         time.Time
		want        bool
	}{
		{name: "event not yet seen", now: seen, want: false},
		{name: "just observed", eventSeenAt: seen, now: seen, want: false},
		{name: "hold not elapsed", eventSeenAt: seen, now: seen.Add(hold - time.Nanosecond), want: false},
		{name: "hold elapsed exactly", eventSeenAt: seen, now: seen.Add(hold), want: true},
		{name: "hold elapsed", eventSeenAt: seen, now: seen.Add(hold + time.Second), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := s48RefusalHoldDone(tc.eventSeenAt, tc.now, hold)
			if got != tc.want {
				t.Fatalf("s48RefusalHoldDone = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestS48RequirePrimarySealed(t *testing.T) {
	site := "pdx"
	mfg := &v1alpha1.MysqlFailoverGroup{}
	if err := s48RequirePrimarySealed(mfg, site); err == nil {
		t.Fatal("nil encryption status must fail")
	}

	mfg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{}
	if err := s48RequirePrimarySealed(mfg, site); err == nil {
		t.Fatal("missing site entry must fail")
	}

	mfg.Status.EncryptionAtRest.Sites = []v1alpha1.SiteEncryptionStatus{{
		Name:  site,
		Phase: v1alpha1.KeyringPhaseUnsealed,
	}}
	if err := s48RequirePrimarySealed(mfg, site); err == nil || !strings.Contains(err.Error(), "Unsealed") {
		t.Fatalf("unsealed primary must fail with phase: %v", err)
	}

	mfg.Status.EncryptionAtRest.Sites[0].Phase = v1alpha1.KeyringPhaseSealed
	if err := s48RequirePrimarySealed(mfg, site); err != nil {
		t.Fatalf("sealed primary: %v", err)
	}
}

func TestS48PrimaryRefusalSnippetDoesNotCrossSites(t *testing.T) {
	iad := "refusing to rotate keyring on site iad: site is the active primary; rotate replicas first"
	pdxSnippet := "refusing to rotate keyring on site pdx: site is the active primary"
	if strings.Contains(iad, pdxSnippet) {
		t.Fatal("pdx primary-refusal snippet must not match an iad event")
	}
}
