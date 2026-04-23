package backupcrypto

import (
	"bytes"
	"fmt"
	"os"
	"strings"
)

// ReadPassphraseFile reads a passphrase from the given file path,
// stripping trailing newline characters and whitespace. Empty
// passphrases are rejected to catch "secret mounted but value missing"
// mistakes at startup rather than during an encrypt call.
//
// We accept leading/trailing whitespace stripping because hand-edited
// Secrets frequently pick up a trailing newline from the shell (echo
// "pass" > file, kubectl create secret generic --from-literal=…). A
// literal leading or trailing space in a cryptographic passphrase is
// almost certainly an accident, and the cost of silently accepting it
// would be a subtle "my restore can't decrypt" incident — we'd rather
// produce a deterministic, whitespace-stripped value.
func ReadPassphraseFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("backupcrypto: read passphrase file %s: %w", path, err)
	}
	trimmed := bytes.TrimFunc(data, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ' ' || r == '\t'
	})
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("backupcrypto: passphrase file %s is empty", path)
	}
	return trimmed, nil
}

// PassphraseFromEnv pulls the passphrase file path out of a conventional
// env var name and returns its contents. Separate helper from
// ReadPassphraseFile so callers who accept passphrases via a flag can
// still reach the underlying reader.
func PassphraseFromEnv(varName string) ([]byte, error) {
	p := strings.TrimSpace(os.Getenv(varName))
	if p == "" {
		return nil, fmt.Errorf("backupcrypto: %s is required but unset", varName)
	}
	return ReadPassphraseFile(p)
}
