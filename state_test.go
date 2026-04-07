package main

import "testing"

func TestDCStateString(t *testing.T) {
	tests := []struct {
		state DCState
		want  string
	}{
		{StateWritable, "writable"},
		{StateReadOnly, "read-only"},
		{StateUnreachable, "unreachable"},
		{StateUnknown, "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("DCState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestShouldTaint(t *testing.T) {
	if ShouldTaint(StateWritable) {
		t.Error("writable should not be tainted")
	}
	if !ShouldTaint(StateReadOnly) {
		t.Error("read-only should be tainted")
	}
	if !ShouldTaint(StateUnreachable) {
		t.Error("unreachable should be tainted")
	}
}

func TestEvalPerDCTransition(t *testing.T) {
	tests := []struct {
		name      string
		prev, cur DCState
		wantTaint *bool
		wantBC    string
	}{
		{"no change writable", StateWritable, StateWritable, nil, ""},
		{"no change readonly", StateReadOnly, StateReadOnly, nil, ""},
		{"writable->readonly", StateWritable, StateReadOnly, boolPtr(true), "offline"},
		{"writable->unreachable", StateWritable, StateUnreachable, boolPtr(true), "offline"},
		{"readonly->writable", StateReadOnly, StateWritable, boolPtr(false), "online"},
		{"unreachable->writable", StateUnreachable, StateWritable, boolPtr(false), "online"},
		{"readonly->unreachable", StateReadOnly, StateUnreachable, nil, ""},
		{"unreachable->readonly", StateUnreachable, StateReadOnly, nil, ""},
		{"unknown->writable", StateUnknown, StateWritable, boolPtr(false), "online"},
		{"unknown->readonly", StateUnknown, StateReadOnly, boolPtr(true), "offline"},
		{"unknown->unreachable", StateUnknown, StateUnreachable, boolPtr(true), "offline"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := EvalPerDCTransition(tt.prev, tt.cur)
			if !boolPtrEq(a.Taint, tt.wantTaint) {
				t.Errorf("taint: got %v, want %v", fmtBoolPtr(a.Taint), fmtBoolPtr(tt.wantTaint))
			}
			if a.Broadcast != tt.wantBC {
				t.Errorf("broadcast: got %q, want %q", a.Broadcast, tt.wantBC)
			}
		})
	}
}

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

func boolPtr(b bool) *bool { return &b }

func boolPtrEq(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func fmtBoolPtr(b *bool) string {
	if b == nil {
		return "nil"
	}
	if *b {
		return "true"
	}
	return "false"
}
