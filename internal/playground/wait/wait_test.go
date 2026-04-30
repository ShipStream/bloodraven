package wait

import (
	"errors"
	"testing"
	"time"
)

func TestTimeoutErrorIsTimeout(t *testing.T) {
	te := &TimeoutError{What: "x", LastMessage: "y", Elapsed: 100 * time.Millisecond}
	if !IsTimeout(te) {
		t.Fatalf("IsTimeout(te) = false, want true")
	}
	wrapped := &wrapErr{inner: te}
	if !IsTimeout(wrapped) {
		t.Fatalf("IsTimeout(wrapped) = false, want true")
	}
	if IsTimeout(errors.New("plain")) {
		t.Fatalf("IsTimeout(plain) = true, want false")
	}
	if got := te.Error(); got == "" {
		t.Fatalf("Error() returned empty string")
	}
}

type wrapErr struct{ inner error }

func (w *wrapErr) Error() string { return "wrap: " + w.inner.Error() }
func (w *wrapErr) Unwrap() error { return w.inner }
