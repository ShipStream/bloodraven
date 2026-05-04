package dragonfly

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RESP protocol minimal encoder/decoder. Only the bits Bloodraven needs:
// inline-array commands out, simple-string / bulk-string / error replies in.
//
// We deliberately do not depend on a full Redis client library — the
// command set is six commands wide and we benefit from owning the
// connection lifecycle and timeout policy directly.

// MaxBulkStringSize caps the bulk-string length we will allocate
// per reply. Bloodraven only issues INFO replication / persistence
// (≤32KB on real Dragonfly) and short OK replies, so 4MB is generous
// even with future-proofing. The cap is a defense-in-depth measure
// against a wedged or malicious server returning a huge n that would
// otherwise allocate unbounded memory before the read fails.
const MaxBulkStringSize = 4 << 20 // 4 MiB

// writeCommand writes an array-of-bulk-strings command, e.g.:
//
//	*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n
func writeCommand(w *bufio.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(a)); err != nil {
			return err
		}
		if _, err := w.WriteString(a); err != nil {
			return err
		}
		if _, err := w.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return w.Flush()
}

// readReply reads one RESP reply and returns it as a string. Errors from
// the server are returned as Go errors; nil bulk strings come back as
// empty strings with ok=false.
func readReply(r *bufio.Reader) (string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	switch prefix {
	case '+':
		return line, nil
	case '-':
		return "", &ServerError{Message: line}
	case ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(line)
		if err != nil {
			return "", fmt.Errorf("dragonfly: invalid bulk-string length %q: %w", line, err)
		}
		if n < 0 {
			return "", nil
		}
		if n > MaxBulkStringSize {
			return "", fmt.Errorf("dragonfly: bulk-string length %d exceeds cap %d", n, MaxBulkStringSize)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		// Consume the trailing CRLF.
		if _, err := r.Discard(2); err != nil {
			return "", err
		}
		return string(buf), nil
	case '*':
		// We do not currently issue commands that return arrays, but
		// callers should still get a sane error rather than a hang.
		return "", fmt.Errorf("dragonfly: unexpected array reply")
	default:
		return "", fmt.Errorf("dragonfly: unknown reply prefix %q", string(prefix))
	}
}

// readLine reads a CRLF-terminated line, returning the body without the CRLF.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("dragonfly: malformed line (missing CRLF)")
	}
	return line[:len(line)-2], nil
}

// ServerError is a typed error returned for `-ERR ...` replies, so callers
// can branch on Dragonfly-reported failures (e.g., NOAUTH, WRONGTYPE).
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return "dragonfly: " + e.Message
}

// IsServerError reports whether err is a *ServerError with a message that
// has the given uppercase prefix (e.g., "NOAUTH", "ERR").
func IsServerError(err error, prefix string) bool {
	var se *ServerError
	if !errors.As(err, &se) {
		return false
	}
	return strings.HasPrefix(strings.ToUpper(se.Message), prefix)
}
