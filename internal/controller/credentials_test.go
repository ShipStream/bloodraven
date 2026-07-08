package controller

import (
	"strings"
	"testing"
)

func TestCredentialStatementErrorLabelRedactsPasswords(t *testing.T) {
	password := "super-secret-password"
	stmt := "ALTER USER 'app'@'%' IDENTIFIED BY '" + password + "'"

	label := credentialStatementErrorLabel("app", stmt)

	if strings.Contains(label, password) {
		t.Fatalf("label leaked password: %q", label)
	}
	if strings.Contains(strings.ToUpper(label), "IDENTIFIED BY") {
		t.Fatalf("label leaked credential SQL: %q", label)
	}
	if !strings.Contains(label, "app credential statement") {
		t.Fatalf("label = %q, want role context", label)
	}
}

func TestCredentialStatementErrorLabelKeepsNonSensitiveContext(t *testing.T) {
	stmt := "GRANT SELECT ON *.* TO 'app'@'%'"

	label := credentialStatementErrorLabel("app", stmt)

	if !strings.Contains(label, stmt) {
		t.Fatalf("label = %q, want non-sensitive statement context", label)
	}
}
