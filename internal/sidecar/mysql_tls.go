package sidecar

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// mysqlTLSConfigName is the name the sidecar registers its MySQL TLS
// config under and then references from the DSN. One sidecar talks to
// exactly one MySQL instance, so a constant is enough; re-registering on
// a config reload simply replaces the entry.
const mysqlTLSConfigName = "bloodraven-sidecar-mysql"

// registerMySQLTLS builds and registers the TLS config for the sidecar's
// MySQL connection from the operator-supplied env vars, returning the
// registered name. It returns "" when the operator did not configure TLS
// (spec.tls unset), which leaves the DSN untouched.
//
// This exists because spec.tls makes the operator set
// require_secure_transport=ON on mysqld, and go-sql-driver/mysql — unlike
// libmysqlclient's default ssl-mode=PREFERRED — never negotiates TLS
// unless the DSN names a registered config. Without it every query the
// sidecar makes fails with Error 3159.
func registerMySQLTLS() (string, error) {
	caFile := strings.TrimSpace(os.Getenv("BLOODRAVEN_MYSQL_TLS_CA_FILE"))
	if caFile == "" {
		return "", nil
	}

	// The sidecar dials 127.0.0.1, which no server certificate carries,
	// so the name to verify has to come from the operator. Refusing to
	// start beats a TLS handshake failure repeated on every query.
	serverName := strings.TrimSpace(os.Getenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME"))
	if serverName == "" {
		return "", fmt.Errorf("BLOODRAVEN_MYSQL_TLS_CA_FILE is set but " +
			"BLOODRAVEN_MYSQL_TLS_SERVER_NAME is empty; the sidecar dials loopback and " +
			"cannot verify the server certificate without the name to check it against")
	}

	if _, err := loadMySQLRootCAs(caFile); err != nil {
		return "", err
	}

	cfg := &tls.Config{
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // Verification is performed below with the current mounted CA.
		VerifyConnection: func(cs tls.ConnectionState) error {
			roots, err := loadMySQLRootCAs(caFile)
			if err != nil {
				return err
			}
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("verify MySQL server certificate: peer sent no certificates")
			}
			intermediates := x509.NewCertPool()
			for _, cert := range cs.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				DNSName:       serverName,
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			if err != nil {
				return fmt.Errorf("verify MySQL server certificate for %q: %w", serverName, err)
			}
			return nil
		},
	}

	// Client keypair, mirroring the operator's own MySQL client
	// (mysqlTLSConfig): offered only if the server asks for one, so it is
	// harmless when no account uses REQUIRE X509. Both halves must be
	// present or neither — a half-configured pair is a mistake worth
	// surfacing rather than silently ignoring.
	certFile := strings.TrimSpace(os.Getenv("BLOODRAVEN_MYSQL_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(os.Getenv("BLOODRAVEN_MYSQL_TLS_KEY_FILE"))
	switch {
	case certFile != "" && keyFile != "":
		if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
			return "", fmt.Errorf("load MySQL client keypair: %w", err)
		}
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("reload MySQL client keypair: %w", err)
			}
			return &cert, nil
		}
	case certFile != "" || keyFile != "":
		return "", fmt.Errorf("BLOODRAVEN_MYSQL_TLS_CERT_FILE and " +
			"BLOODRAVEN_MYSQL_TLS_KEY_FILE must be set together")
	}

	if err := mysqldriver.RegisterTLSConfig(mysqlTLSConfigName, cfg); err != nil {
		return "", fmt.Errorf("register MySQL TLS config: %w", err)
	}
	return mysqlTLSConfigName, nil
}

// loadMySQLRootCAs reads and parses the currently mounted CA bundle. Secret
// volumes are updated through atomic symlink swaps, so loading by path for each
// handshake picks up cert-manager rotations without restarting the sidecar.
func loadMySQLRootCAs(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read MySQL CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("BLOODRAVEN_MYSQL_TLS_CA_FILE %q contains no valid certificates", caFile)
	}
	return roots, nil
}

// withMySQLTLSConfig stamps a registered TLS config name onto a DSN. A
// DSN that already names one is returned unchanged, so an operator who
// pins their own tls= parameter in the legacy spec.secretName DSN keeps
// control.
func withMySQLTLSConfig(dsn, name string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if cfg.TLSConfig != "" {
		return dsn, nil
	}
	cfg.TLSConfig = name
	return cfg.FormatDSN(), nil
}
