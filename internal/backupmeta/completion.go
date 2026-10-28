// Package backupmeta encodes completion sentinels shared by backup producers
// and the operator's log readers.
package backupmeta

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const dumpPrefix = "BLOODRAVEN_DUMP_COMPLETE"

// EscapeValue percent-encodes a UTF-8 value as a whitespace-free token.
// Its safe characters match urllib.parse.quote(value, safe="/:,@").
func EscapeValue(value string) string {
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~/:,@", rune(c)) {
			out.WriteByte(c)
		} else {
			const hex = "0123456789ABCDEF"
			out.WriteByte('%')
			out.WriteByte(hex[c>>4])
			out.WriteByte(hex[c&15])
		}
	}
	return out.String()
}

// ParseTokens accepts version-2 percent encoding and unversioned legacy
// tokens. Legacy values are literal; callers handle field-specific escapes.
// Unknown versions and invalid version-2 escapes are rejected.
func ParseTokens(line, prefix string) (map[string]string, bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != prefix {
		return nil, false
	}
	tokens := make(map[string]string)
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if ok && key != "" {
			if _, exists := tokens[key]; exists {
				return nil, false
			}
			tokens[key] = value
		}
	}
	version, versioned := tokens["v"]
	if versioned && version != "2" {
		return nil, false
	}
	if versioned {
		for key, value := range tokens {
			decoded, err := url.PathUnescape(value)
			if err != nil {
				return nil, false
			}
			tokens[key] = decoded
		}
	}
	return tokens, true
}

// DumpComplete describes the metadata emitted after a successful dump.
type DumpComplete struct {
	Location            string
	Size                string
	SizeBytes           int64
	GtidExecuted        string
	BinlogFile          string
	BinlogPos           int64
	Encrypted           bool
	EncryptionAlgorithm string
	CiphertextBytes     int64
	Files               int
}

// FormatDumpComplete produces the version-2 terminal line.
func FormatDumpComplete(meta DumpComplete) string {
	line := fmt.Sprintf("%s v=2 location=%s sizeBytes=%d size=%s gtidExecuted=%s binlogFile=%s binlogPos=%d",
		dumpPrefix, EscapeValue(meta.Location), meta.SizeBytes, EscapeValue(meta.Size),
		EscapeValue(meta.GtidExecuted), EscapeValue(meta.BinlogFile), meta.BinlogPos)
	if meta.Encrypted {
		line += fmt.Sprintf(" ciphertextBytes=%d files=%d encrypted=true algorithm=%s",
			meta.CiphertextBytes, meta.Files, EscapeValue(meta.EncryptionAlgorithm))
	}
	return line
}

// ParseDumpComplete decodes both producer generations. Legacy size used
// underscores for spaces; identity fields must preserve literal underscores,
// percent signs and plus signs. Malformed numeric observations remain zero.
func ParseDumpComplete(line string) (DumpComplete, bool) {
	tokens, ok := ParseTokens(line, dumpPrefix)
	if !ok {
		return DumpComplete{}, false
	}
	size := tokens["size"]
	if tokens["v"] == "" {
		size = strings.ReplaceAll(size, "_", " ")
	}
	parseInt := func(key string) int64 {
		n, err := strconv.ParseInt(tokens[key], 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return DumpComplete{
		Location: tokens["location"], Size: size, SizeBytes: parseInt("sizeBytes"),
		GtidExecuted: tokens["gtidExecuted"], BinlogFile: tokens["binlogFile"], BinlogPos: parseInt("binlogPos"),
		Encrypted: tokens["encrypted"] == "true" || tokens["encrypted"] == "1", EncryptionAlgorithm: tokens["algorithm"],
		CiphertextBytes: parseInt("ciphertextBytes"), Files: int(parseInt("files")),
	}, true
}
