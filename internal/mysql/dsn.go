package mysql

import (
	"errors"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// safeDSNErrors are the driver's DSN errors whose text is fixed, so they
// can be reported verbatim.
var safeDSNErrors = map[string]bool{
	"invalid DSN: did you forget to escape a param value?":                  true,
	"invalid DSN: network address not terminated (missing closing brace)":   true,
	"invalid DSN: missing the slash separating the database name":           true,
	"invalid DSN: interpolateParams can not be used with unsafe collations": true,
}

// ParseDSN parses a go-sql-driver DSN like the driver's ParseDSN, but its
// error never quotes the DSN. The driver's own errors can: a DSN without
// "@" reports its user:password as an unknown network name ("default addr
// for network 'op:secret' unknown"), and bad parameter values are echoed.
// These errors reach logs, status messages and Kubernetes Events, so only
// the driver's fixed-text errors are passed through.
func ParseDSN(dsn string) (*mysqldriver.Config, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err == nil {
		return cfg, nil
	}
	if safeDSNErrors[err.Error()] {
		return nil, err
	}
	msg := "malformed DSN (the driver's message is withheld because it can quote the password)"
	if !strings.Contains(dsn, "@") {
		msg += `; it has no "@" between the credentials and the address`
	}
	return nil, errors.New(msg)
}
