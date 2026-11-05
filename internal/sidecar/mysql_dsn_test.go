package sidecar

import (
	"strings"
	"testing"
)

// The sidecar logs this error at startup; the driver's own parser would
// report the user:password of a DSN without "@" as an unknown network name.
func TestNewLiveMysqlFromDSNErrorOmitsPassword(t *testing.T) {
	for _, dsn := range []string{
		"op:s3cr3t-pw/mysql",
		"op:s3cr3t-pw@tcp(127.0.0.1:3306)/mysql?parseTime=s3cr3t-pw",
	} {
		_, err := NewLiveMysqlFromDSN(dsn)
		if err == nil {
			t.Fatalf("NewLiveMysqlFromDSN(%q) succeeded; want an error", dsn)
		}
		if strings.Contains(err.Error(), "s3cr3t-pw") {
			t.Fatalf("error quotes the DSN password: %q", err)
		}
	}
}

func TestNewLiveMysqlFromDSNValid(t *testing.T) {
	m, err := NewLiveMysqlFromDSN("op:pw@tcp(127.0.0.1:1)/mysql")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.db.Close(); err != nil {
		t.Fatal(err)
	}
}
