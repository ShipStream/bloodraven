package state

import "testing"

func TestEvalCrossSite(t *testing.T) {
	tests := []struct {
		name                 string
		site0, site1         SiteState
		wantPromote, wantDNS string
		wantAlert            bool
		wantSplitBrain       bool
	}{
		{"normal site0 primary", StateWritable, StateReadOnly, "", "", false, false},
		{"normal site1 primary", StateReadOnly, StateWritable, "", "", false, false},
		{"site0 down promote site1", StateUnreachable, StateReadOnly, "site1", "site1", false, false},
		{"site1 down promote site0", StateReadOnly, StateUnreachable, "site0", "site0", false, false},
		{"site0 primary site1 down", StateWritable, StateUnreachable, "", "", true, false},
		{"site1 primary site0 down", StateUnreachable, StateWritable, "", "", true, false},
		{"split brain", StateWritable, StateWritable, "", "", true, true},
		{"no primary", StateReadOnly, StateReadOnly, "", "", true, false},
		{"total loss", StateUnreachable, StateUnreachable, "", "", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := EvalCrossSite(tt.site0, tt.site1, StateUnknown, StateUnknown, "site0", "site1")
			if a.PromoteSite != tt.wantPromote {
				t.Errorf("promote: got %q, want %q", a.PromoteSite, tt.wantPromote)
			}
			if a.FlipDNS != tt.wantDNS {
				t.Errorf("flipDNS: got %q, want %q", a.FlipDNS, tt.wantDNS)
			}
			if (a.Alert != "") != tt.wantAlert {
				t.Errorf("alert: got %q, wantAlert=%v", a.Alert, tt.wantAlert)
			}
			if a.SplitBrain != tt.wantSplitBrain {
				t.Errorf("splitBrain: got %v, want %v", a.SplitBrain, tt.wantSplitBrain)
			}
		})
	}
}
