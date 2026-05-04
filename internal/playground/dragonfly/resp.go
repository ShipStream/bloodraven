package dragonfly

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	dfclient "github.com/shipstream/bloodraven/internal/dragonfly"
)

// writeArrayOfBulkStrings encodes a RESP command. Mirrors the
// operator's writeCommand. Duplicated rather than re-exported so the
// operator client's surface stays minimal.
func writeArrayOfBulkStrings(w *bufio.Writer, args ...string) error {
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

// readReplyAllowNil reads one RESP reply. Returns (nil, nil) for the
// nil bulk-string sentinel ("$-1\r\n"); other non-error replies come
// back as a non-nil string pointer. RESP errors surface as a
// *dfclient.ServerError so callers can pattern-match on the message
// prefix (READONLY, NOAUTH, etc).
func readReplyAllowNil(r *bufio.Reader) (*string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := readLineCRLF(r)
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		s := line
		return &s, nil
	case '-':
		return nil, &dfclient.ServerError{Message: line}
	case ':':
		s := line
		return &s, nil
	case '$':
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk-string length %q: %w", line, err)
		}
		if n < 0 {
			return nil, nil
		}
		if n > dfclient.MaxBulkStringSize {
			return nil, fmt.Errorf("bulk-string length %d exceeds cap %d", n, dfclient.MaxBulkStringSize)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if _, err := r.Discard(2); err != nil {
			return nil, err
		}
		s := string(buf)
		return &s, nil
	case '*':
		return nil, fmt.Errorf("unexpected array reply")
	default:
		return nil, fmt.Errorf("unknown reply prefix %q", string(prefix))
	}
}

func readLineCRLF(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("malformed line (missing CRLF)")
	}
	return line[:len(line)-2], nil
}
