package mysql

import (
	"strings"
	"testing"
)

func TestParseDSNNeverQuotesTheDSN(t *testing.T) {
	const secret = "s3cr3t-pw"
	for _, tc := range []struct {
		name, dsn string
		// keep is a fixed driver message that must survive verbatim.
		keep string
	}{
		{name: "missing @ makes user:password the network name", dsn: "op:" + secret + "/mysql"},
		{name: "password only", dsn: secret + "/mysql"},
		{name: "bad bool param", dsn: "op:" + secret + "@tcp(127.0.0.1:3306)/mysql?parseTime=" + secret},
		{name: "unknown tls config", dsn: "op:" + secret + "@tcp(127.0.0.1:3306)/mysql?tls=" + secret},
		{name: "bad duration", dsn: "op:" + secret + "@tcp(127.0.0.1:3306)/mysql?timeout=" + secret},
		{name: "unknown server pub key", dsn: "op:" + secret + "@tcp(127.0.0.1:3306)/mysql?serverPubKey=" + secret},
		{name: "bad dbname escape", dsn: "op:" + secret + "@tcp(127.0.0.1:3306)/" + secret + "%zz"},
		{name: "unterminated address", dsn: "op:" + secret + "@tcp(127.0.0.1:3306/mysql", keep: "missing closing brace"},
		{name: "missing slash", dsn: "op:" + secret + "@tcp(127.0.0.1:3306)mysql", keep: "missing the slash"},
		{name: "bare password", dsn: secret, keep: "missing the slash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDSN(tc.dsn)
			if err == nil {
				t.Fatalf("ParseDSN(%q) succeeded; want an error", tc.dsn)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error quotes the DSN's secret: %q", err)
			}
			if tc.keep != "" && !strings.Contains(err.Error(), tc.keep) {
				t.Fatalf("error %q lost the driver's fixed message %q", err, tc.keep)
			}
		})
	}
}

func TestParseDSNNamesAMissingAt(t *testing.T) {
	_, err := ParseDSN("op:s3cr3t-pw/mysql")
	if err == nil || !strings.Contains(err.Error(), `no "@"`) {
		t.Fatalf("want a hint about the missing @, got %v", err)
	}
}

func TestParseDSNValid(t *testing.T) {
	cfg, err := ParseDSN("op:pw@tcp(127.0.0.1:3306)/mysql?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "op" || cfg.Passwd != "pw" || cfg.Addr != "127.0.0.1:3306" || !cfg.ParseTime {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
