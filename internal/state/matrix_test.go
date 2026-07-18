package state

import (
	"sort"
	"strings"
	"testing"
)

func obs(name string, role SiteRole, s SiteState) SiteObservation {
	return SiteObservation{Name: name, Role: role, State: s}
}

func TestEvalCrossSite_TwoSite(t *testing.T) {
	pc := SiteRolePrimaryCandidate
	tests := []struct {
		name           string
		observations   []SiteObservation
		priorities     []string
		wantCandidates []string
		wantSplit      bool
		wantAlert      bool
		wantReason     string
	}{
		{
			name:         "normal primary/replica",
			observations: []SiteObservation{obs("iad", pc, StateWritable), obs("pdx", pc, StateReadOnly)},
			wantReason:   "Healthy",
		},
		{
			name:           "primary unreachable emits replica as candidate",
			observations:   []SiteObservation{obs("iad", pc, StateUnreachable), obs("pdx", pc, StateReadOnly)},
			wantCandidates: []string{"pdx"},
			wantReason:     "Degraded",
		},
		{
			name:         "replica unreachable alerts only",
			observations: []SiteObservation{obs("iad", pc, StateWritable), obs("pdx", pc, StateUnreachable)},
			wantAlert:    true,
			wantReason:   "Degraded",
		},
		{
			name:         "split brain flagged",
			observations: []SiteObservation{obs("iad", pc, StateWritable), obs("pdx", pc, StateWritable)},
			wantSplit:    true,
			wantAlert:    true,
			wantReason:   "SplitBrain",
		},
		{
			name:         "no primary both read only alerts only",
			observations: []SiteObservation{obs("iad", pc, StateReadOnly), obs("pdx", pc, StateReadOnly)},
			wantAlert:    true,
			wantReason:   "NoPrimary",
		},
		{
			name:         "total loss",
			observations: []SiteObservation{obs("iad", pc, StateUnreachable), obs("pdx", pc, StateUnreachable)},
			wantAlert:    true,
			wantReason:   "TotalLoss",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := EvalCrossSite(tt.observations, tt.priorities)
			if !equalStrings(a.PromotionCandidates, tt.wantCandidates) {
				t.Errorf("candidates: got %v, want %v", a.PromotionCandidates, tt.wantCandidates)
			}
			if a.SplitBrain != tt.wantSplit {
				t.Errorf("splitBrain: got %v, want %v", a.SplitBrain, tt.wantSplit)
			}
			if (a.Alert != "") != tt.wantAlert {
				t.Errorf("alert: got %q, wantAlert=%v", a.Alert, tt.wantAlert)
			}
			if a.Reason != tt.wantReason {
				t.Errorf("reason: got %q, want %q", a.Reason, tt.wantReason)
			}
		})
	}
}

func TestEvalCrossSite_ThreeSite(t *testing.T) {
	pc := SiteRolePrimaryCandidate
	dr := SiteRoleDROnly

	t.Run("healthy primary with two replicas", func(t *testing.T) {
		a := EvalCrossSite([]SiteObservation{
			obs("iad", pc, StateWritable),
			obs("pdx", pc, StateReadOnly),
			obs("lhr", dr, StateReadOnly),
		}, nil)
		if a.Reason != "Healthy" || len(a.PromotionCandidates) != 0 || a.Alert != "" {
			t.Fatalf("expected healthy, got %+v", a)
		}
	})

	t.Run("primary unreachable lists every primary-candidate replica (declared order)", func(t *testing.T) {
		a := EvalCrossSite([]SiteObservation{
			obs("iad", pc, StateUnreachable),
			obs("pdx", pc, StateReadOnly),
			obs("fra", pc, StateReadOnly),
			obs("lhr", dr, StateReadOnly),
		}, nil)
		if got := a.PromotionCandidates; !equalStrings(got, []string{"pdx", "fra"}) {
			t.Fatalf("expected candidates=[pdx fra], got %v", got)
		}
	})

	t.Run("primary unreachable with only dr-only reachable = no primary", func(t *testing.T) {
		a := EvalCrossSite([]SiteObservation{
			obs("iad", pc, StateUnreachable),
			obs("pdx", pc, StateUnreachable),
			obs("lhr", dr, StateReadOnly),
		}, nil)
		if len(a.PromotionCandidates) != 0 || a.Reason != "NoPrimary" {
			t.Fatalf("expected NoPrimary with no candidates, got %+v", a)
		}
	})

	t.Run("priority list orders candidates ahead of declared order", func(t *testing.T) {
		a := EvalCrossSite([]SiteObservation{
			obs("iad", pc, StateUnreachable),
			obs("pdx", pc, StateReadOnly),
			obs("fra", pc, StateReadOnly),
		}, []string{"fra", "pdx"})
		if !equalStrings(a.PromotionCandidates, []string{"fra", "pdx"}) {
			t.Fatalf("expected candidates=[fra pdx] per priority, got %v", a.PromotionCandidates)
		}
	})

	t.Run("three-way split brain flagged", func(t *testing.T) {
		a := EvalCrossSite([]SiteObservation{
			obs("iad", pc, StateWritable),
			obs("pdx", pc, StateWritable),
			obs("fra", pc, StateWritable),
		}, nil)
		if !a.SplitBrain || a.Reason != "SplitBrain" {
			t.Fatalf("expected SplitBrain, got %+v", a)
		}
		if !strings.Contains(a.Alert, "3") {
			t.Fatalf("alert should mention 3 writable sites: %q", a.Alert)
		}
	})

	t.Run("degraded alert names all unreachable sites", func(t *testing.T) {
		a := EvalCrossSite([]SiteObservation{
			obs("iad", pc, StateWritable),
			obs("pdx", pc, StateUnreachable),
			obs("fra", pc, StateUnreachable),
		}, nil)
		if a.Reason != "Degraded" {
			t.Fatalf("expected Degraded, got %+v", a)
		}
		if !strings.Contains(a.Alert, "pdx") || !strings.Contains(a.Alert, "fra") {
			t.Fatalf("alert should list every unreachable site: %q", a.Alert)
		}
	})
}

func TestEvalCrossSite_ReadOnlyReaderIsolation(t *testing.T) {
	pc := SiteRolePrimaryCandidate
	reader := SiteRoleReadOnly
	dr := SiteRoleDROnly

	tests := []struct {
		name       string
		obs        []SiteObservation
		wantReason string
		wantFence  []string
	}{
		{
			name: "reader outage does not degrade healthy core",
			obs: []SiteObservation{
				obs("iad", pc, StateWritable), obs("pdx", pc, StateReadOnly), obs("reader", reader, StateUnreachable),
			},
			wantReason: "Healthy",
		},
		{
			name: "writable reader is fenced but not split brain",
			obs: []SiteObservation{
				obs("iad", pc, StateWritable), obs("pdx", pc, StateReadOnly), obs("reader", reader, StateWritable),
			},
			wantReason: "Degraded",
			wantFence:  []string{"reader"},
		},
		{
			name: "writable dr-only degrades healthy candidate topology",
			obs: []SiteObservation{
				obs("iad", pc, StateWritable), obs("pdx", pc, StateReadOnly), obs("dr", dr, StateWritable),
			},
			wantReason: "Degraded",
			wantFence:  []string{"dr"},
		},
		{
			name: "sole writable reader is not a primary",
			obs: []SiteObservation{
				obs("iad", pc, StateReadOnly), obs("pdx", pc, StateReadOnly), obs("reader", reader, StateWritable),
			},
			wantReason: "NoPrimary",
			wantFence:  []string{"reader"},
		},
		{
			name: "writable dr only is fenced and not primary",
			obs: []SiteObservation{
				obs("iad", pc, StateReadOnly), obs("pdx", pc, StateReadOnly), obs("dr", dr, StateWritable),
			},
			wantReason: "NoPrimary",
			wantFence:  []string{"dr"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EvalCrossSite(tt.obs, nil)
			if got.Reason != tt.wantReason || !equalStrings(got.FenceSites, tt.wantFence) {
				t.Fatalf("EvalCrossSite() = %+v, want reason=%s fence=%v", got, tt.wantReason, tt.wantFence)
			}
			if got.SplitBrain {
				t.Fatal("non-promotable writable site must not create split brain")
			}
		})
	}
}

func TestRankPromotionCandidates(t *testing.T) {
	pc := SiteRolePrimaryCandidate
	dr := SiteRoleDROnly
	readOnly := []SiteObservation{
		obs("iad", pc, StateReadOnly),
		obs("pdx", pc, StateReadOnly),
		obs("fra", dr, StateReadOnly),
	}

	t.Run("no priority list preserves declared order", func(t *testing.T) {
		got := RankPromotionCandidates(readOnly, nil)
		if !equalStrings(got, []string{"iad", "pdx"}) {
			t.Fatalf("expected [iad pdx], got %v", got)
		}
	})

	t.Run("priority list reorders, dr-only filtered", func(t *testing.T) {
		got := RankPromotionCandidates(readOnly, []string{"pdx"})
		if !equalStrings(got, []string{"pdx", "iad"}) {
			t.Fatalf("expected [pdx iad], got %v", got)
		}
	})

	t.Run("priority list entries pointing at unknown or dr-only sites are ignored", func(t *testing.T) {
		got := RankPromotionCandidates(readOnly, []string{"ghost", "fra", "iad"})
		if !equalStrings(got, []string{"iad", "pdx"}) {
			t.Fatalf("expected [iad pdx], got %v", got)
		}
	})
}

func TestResolveSplitBrain(t *testing.T) {
	pc := SiteRolePrimaryCandidate
	dr := SiteRoleDROnly
	writable := []SiteObservation{
		obs("iad", pc, StateWritable),
		obs("pdx", pc, StateWritable),
		obs("fra", dr, StateWritable),
	}

	t.Run("priority list picks winner", func(t *testing.T) {
		winner, losers := ResolveSplitBrain(writable, []string{"pdx", "iad"})
		if winner != "pdx" {
			t.Fatalf("expected winner pdx, got %q", winner)
		}
		sort.Strings(losers)
		if strings.Join(losers, ",") != "fra,iad" {
			t.Fatalf("expected losers fra,iad, got %v", losers)
		}
	})

	t.Run("no priority list yields no winner (manual resolution)", func(t *testing.T) {
		winner, losers := ResolveSplitBrain(writable, nil)
		if winner != "" || len(losers) != 0 {
			t.Fatalf("expected no winner without priorities, got winner=%q losers=%v", winner, losers)
		}
	})

	t.Run("priority list referencing only unknown sites yields no winner", func(t *testing.T) {
		winner, losers := ResolveSplitBrain(writable, []string{"ghost"})
		if winner != "" || len(losers) != 0 {
			t.Fatalf("expected no winner when list has no match, got winner=%q losers=%v", winner, losers)
		}
	})

	t.Run("only dr-only writable yields no winner", func(t *testing.T) {
		drOnly := []SiteObservation{obs("fra", dr, StateWritable)}
		winner, losers := ResolveSplitBrain(drOnly, nil)
		if winner != "" || len(losers) != 0 {
			t.Fatalf("expected no winner, got winner=%q losers=%v", winner, losers)
		}
	})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
