package mysql

import (
	"errors"
	"fmt"
	"regexp"
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
	if key := failingDSNParam(dsn); key != "" {
		return nil, fmt.Errorf("malformed DSN: invalid value for parameter %q (the value is withheld because it can be secret)", key)
	}
	msg := "malformed DSN (the driver's message is withheld because it can quote the password)"
	if !strings.Contains(dsn, "@") {
		msg += `; it has no "@" between the credentials and the address`
	}
	return nil, errors.New(msg)
}

// dsnParamKey matches the parameter names failingDSNParam may report. A
// key that does not look like a parameter name is never echoed.
var dsnParamKey = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

// failingDSNParam returns the name of the first query parameter the driver
// rejects on its own, or "" when none does. Like the driver, it takes the
// parameters from after the first "?" that follows the last "/".
//
// A password may itself contain "/" and "?", and then the "parameters" are
// a fragment of the password. So a name is reported only when the text
// before that "/" ends where a well-formed DSN's address ends: at ")" (a
// net(addr) address), at "@" (credentials with the default address), or at
// the start of the DSN, with no "@" after it.
func failingDSNParam(dsn string) string {
	slash := strings.LastIndexByte(dsn, '/')
	if slash < 0 {
		return ""
	}
	if slash > 0 && dsn[slash-1] != ')' && dsn[slash-1] != '@' {
		return ""
	}
	if strings.LastIndexByte(dsn, '@') > slash {
		return ""
	}
	_, params, ok := strings.Cut(dsn[slash+1:], "?")
	if !ok {
		return ""
	}
	for _, kv := range strings.Split(params, "&") {
		key, _, _ := strings.Cut(kv, "=")
		if !dsnParamKey.MatchString(key) {
			continue
		}
		if _, err := mysqldriver.ParseDSN("u@tcp(127.0.0.1:3306)/?" + kv); err != nil {
			return key
		}
	}
	return ""
}
