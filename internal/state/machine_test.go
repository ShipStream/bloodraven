package state

import "testing"

func TestSiteStateString(t *testing.T) {
	tests := []struct {
		state SiteState
		want  string
	}{
		{StateWritable, "writable"},
		{StateReadOnly, "read-only"},
		{StateUnreachable, "unreachable"},
		{StateUnknown, "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("SiteState(%d).String() = %q, want %q", tt.state, got, tt.want)
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

func TestEvalPerSiteTransition(t *testing.T) {
	tests := []struct {
		name      string
		prev, cur SiteState
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
			a := EvalPerSiteTransition(tt.prev, tt.cur)
			if !boolPtrEq(a.Taint, tt.wantTaint) {
				t.Errorf("taint: got %v, want %v", fmtBoolPtr(a.Taint), fmtBoolPtr(tt.wantTaint))
			}
			if a.Broadcast != tt.wantBC {
				t.Errorf("broadcast: got %q, want %q", a.Broadcast, tt.wantBC)
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
