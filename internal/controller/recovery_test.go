package controller

import (
	"context"
	"testing"
)

func TestRecoverOldPrimary_FullSequence(t *testing.T) {
	oldPrimary := &trackingMock{}
	fc := NewFailoverController(testLogger())

	err := fc.RecoverOldPrimary(context.Background(), oldPrimary, "new-primary.example.com", "repl", "secret", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := oldPrimary.getCalls()
	expected := []string{
		"SetSuperReadOnly(ON)",
		"StopReplica",
		"ChangeReplicationSource",
		"StartReplica",
	}
	if len(calls) != len(expected) {
		t.Fatalf("calls: got %v, want %v", calls, expected)
	}
	for i, want := range expected {
		if calls[i] != want {
			t.Errorf("call[%d]: got %q, want %q", i, calls[i], want)
		}
	}
}

func TestRecoverOldPrimary_NoSSL(t *testing.T) {
	oldPrimary := &trackingMock{}
	fc := NewFailoverController(testLogger())

	err := fc.RecoverOldPrimary(context.Background(), oldPrimary, "new-primary.example.com", "repl", "secret", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := oldPrimary.getCalls()
	if len(calls) != 4 {
		t.Fatalf("expected 4 calls, got %v", calls)
	}
	// The sequence should be the same regardless of SSL.
	if calls[0] != "SetSuperReadOnly(ON)" {
		t.Errorf("call[0]: got %q, want SetSuperReadOnly(ON)", calls[0])
	}
	if calls[3] != "StartReplica" {
		t.Errorf("call[3]: got %q, want StartReplica", calls[3])
	}
}
