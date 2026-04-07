package state

import "testing"

func TestEvalCrossDC(t *testing.T) {
	tests := []struct {
		name                 string
		dc1, dc2             DCState
		wantPromote, wantDNS string
		wantAlert            bool
	}{
		{"normal dc1 primary", StateWritable, StateReadOnly, "", "", false},
		{"normal dc2 primary", StateReadOnly, StateWritable, "", "", false},
		{"dc1 down promote dc2", StateUnreachable, StateReadOnly, "dc2", "dc2", false},
		{"dc2 down promote dc1", StateReadOnly, StateUnreachable, "dc1", "dc1", false},
		{"dc1 primary dc2 down", StateWritable, StateUnreachable, "", "", true},
		{"dc2 primary dc1 down", StateUnreachable, StateWritable, "", "", true},
		{"split brain", StateWritable, StateWritable, "", "", true},
		{"no primary", StateReadOnly, StateReadOnly, "", "", true},
		{"total loss", StateUnreachable, StateUnreachable, "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := EvalCrossDC(tt.dc1, tt.dc2, StateUnknown, StateUnknown, "dc1", "dc2")
			if a.PromoteDC != tt.wantPromote {
				t.Errorf("promote: got %q, want %q", a.PromoteDC, tt.wantPromote)
			}
			if a.FlipDNS != tt.wantDNS {
				t.Errorf("flipDNS: got %q, want %q", a.FlipDNS, tt.wantDNS)
			}
			if (a.Alert != "") != tt.wantAlert {
				t.Errorf("alert: got %q, wantAlert=%v", a.Alert, tt.wantAlert)
			}
		})
	}
}
