//go:build !linux

package spillfile

// Arena is the file-backed arena. It is implemented on Linux only; on
// other platforms New fails and no Arena exists.
type Arena struct{}

// New returns ErrUnsupported.
func New(string, Options) (*Arena, error) { return nil, ErrUnsupported }

// Alloc is never reached: no Arena exists on this platform.
func (a *Arena) Alloc(int64) ([]byte, error) { return nil, ErrUnsupported }

// Resize is never reached.
func (a *Arena) Resize([]byte, int64) ([]byte, error) { return nil, ErrUnsupported }

// Free is never reached.
func (a *Arena) Free([]byte) {}

// WriteBack is never reached.
func (a *Arena) WriteBack([]byte, int64, int64) error { return ErrUnsupported }

// Settle is never reached.
func (a *Arena) Settle() error { return ErrUnsupported }

// Check is never reached.
func (a *Arena) Check() error { return nil }

// Close is never reached.
func (a *Arena) Close() error { return nil }

// Contains is never reached.
func (a *Arena) Contains(uintptr) bool { return false }

// Stats is never reached.
func (a *Arena) Stats() Stats { return Stats{} }

// Guard runs fn.
func (a *Arena) Guard(fn func() error) error { return fn() }
