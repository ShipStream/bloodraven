package sidecar

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// writeCAFile generates a throwaway CA and returns its path, standing in
// for the cert-manager ca.crt the operator mounts at /etc/mysql/tls.
func writeCAFile(t *testing.T) string {
	t.Helper()
	path, _, _ := writeCAWithKey(t)
	return path
}

// clearMySQLTLSEnv removes every TLS env var so each case starts from a
// known state regardless of the developer's shell.
func clearMySQLTLSEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"BLOODRAVEN_MYSQL_TLS_CA_FILE", "BLOODRAVEN_MYSQL_TLS_CERT_FILE",
		"BLOODRAVEN_MYSQL_TLS_KEY_FILE", "BLOODRAVEN_MYSQL_TLS_SERVER_NAME",
	} {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
}

func TestRegisterMySQLTLS_NoCAIsNoop(t *testing.T) {
	clearMySQLTLSEnv(t)
	name, err := registerMySQLTLS()
	if err != nil {
		t.Fatalf("registerMySQLTLS: %v", err)
	}
	if name != "" {
		t.Errorf("name = %q, want empty when the operator configured no TLS", name)
	}
}

func TestRegisterMySQLTLS_RequiresServerName(t *testing.T) {
	clearMySQLTLSEnv(t)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", writeCAFile(t))

	_, err := registerMySQLTLS()
	if err == nil {
		t.Fatal("expected an error: verifying a loopback dial without a server name " +
			"would fail the handshake on every query instead of at startup")
	}
	if !strings.Contains(err.Error(), "BLOODRAVEN_MYSQL_TLS_SERVER_NAME") {
		t.Errorf("error should name the missing variable: %v", err)
	}
}

func TestRegisterMySQLTLS_RejectsHalfConfiguredKeypair(t *testing.T) {
	clearMySQLTLSEnv(t)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", writeCAFile(t))
	t.Setenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME", "mysql-lion-dc1.shared-lion.svc.cluster.local")
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CERT_FILE", "/etc/mysql/tls/tls.crt")

	if _, err := registerMySQLTLS(); err == nil {
		t.Fatal("a cert without its key must be an error, not a silent downgrade")
	}
}

func TestRegisterMySQLTLS_RejectsUnusableCA(t *testing.T) {
	clearMySQLTLSEnv(t)
	bad := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(bad, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", bad)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME", "mysql-lion-dc1.shared-lion.svc.cluster.local")

	if _, err := registerMySQLTLS(); err == nil {
		t.Fatal("an unparseable CA must fail startup rather than connect unverified")
	}
}

// TestConfigFromEnv_TLSStampsDSN is the end-to-end sidecar half: given
// the env the operator renders, ConfigFromEnv must produce a DSN the
// driver accepts *and* which names a registered TLS config. ParseDSN
// rejects an unregistered tls= name, so a clean parse proves registration
// really happened rather than just string-matching "tls=".
func TestConfigFromEnv_TLSStampsDSN(t *testing.T) {
	clearMySQLTLSEnv(t)
	t.Setenv("MYSQL_DSN", "")
	if err := os.Unsetenv("MYSQL_DSN"); err != nil {
		t.Fatalf("unset MYSQL_DSN: %v", err)
	}
	t.Setenv("MYSQL_USER", "bloodraven")
	t.Setenv("MYSQL_PASSWORD", "pw")
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", writeCAFile(t))
	t.Setenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME", "mysql-lion-dc1.shared-lion.svc.cluster.local")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	parsed, err := mysqldriver.ParseDSN(cfg.MysqlDSN)
	if err != nil {
		t.Fatalf("driver rejected the DSN we built: %v", err)
	}
	if parsed.TLSConfig != mysqlTLSConfigName {
		t.Errorf("TLSConfig = %q, want %q; without it every query fails with Error 3159 "+
			"against require_secure_transport=ON", parsed.TLSConfig, mysqlTLSConfigName)
	}
	if parsed.TLS == nil || parsed.TLS.ServerName != "mysql-lion-dc1.shared-lion.svc.cluster.local" {
		t.Errorf("resolved TLS config does not verify the per-site Service name: %+v", parsed.TLS)
	}
	if parsed.TLS != nil && parsed.TLS.VerifyConnection == nil {
		t.Error("the sidecar must install explicit server-certificate verification")
	}
}

func TestConfigFromEnv_NoTLSLeavesDSNUntouched(t *testing.T) {
	clearMySQLTLSEnv(t)
	t.Setenv("MYSQL_DSN", "")
	if err := os.Unsetenv("MYSQL_DSN"); err != nil {
		t.Fatalf("unset MYSQL_DSN: %v", err)
	}
	t.Setenv("MYSQL_USER", "bloodraven")
	t.Setenv("MYSQL_PASSWORD", "pw")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	// Byte-identical to what pre-fix releases produced.
	if want := "bloodraven:pw@tcp(127.0.0.1:3306)/"; cfg.MysqlDSN != want {
		t.Errorf("DSN = %q, want %q", cfg.MysqlDSN, want)
	}
}

// TestConfigFromEnv_TLSHandshakeAgainstSiteServiceSAN drives a real TLS
// handshake with the config the sidecar registers, against a server
// certificate shaped like the one users are told to issue. This is what
// validates the design decision behind the fix — that verifying the
// per-site Service SAN (rather than the 127.0.0.1 the sidecar actually
// dials) is both possible and correctly rejects the wrong identity.
//
// Uses a loopback listener rather than net.Pipe: on a rejected
// certificate the client stops reading the server's TLS 1.3 flight and
// writes an alert, which deadlocks an unbuffered pipe until a deadline
// fires. Socket buffers make the rejection return immediately. (Like the
// existing httptest-based tests here, this needs a local listener.)
func TestConfigFromEnv_TLSHandshakeAgainstSiteServiceSAN(t *testing.T) {
	const siteSAN = "mysql-lion-dc1.shared-lion.svc.cluster.local"

	for _, tc := range []struct {
		name      string
		serverSAN string
		wantErr   bool
	}{
		{"documented per-site Service SAN", siteSAN, false},
		{"certificate for a different site", "mysql-lion-dc2.shared-lion.svc.cluster.local", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caPath, caCert, caKey := writeCAWithKey(t)

			clearMySQLTLSEnv(t)
			t.Setenv("MYSQL_USER", "bloodraven")
			t.Setenv("MYSQL_PASSWORD", "pw")
			t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", caPath)
			t.Setenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME", siteSAN)

			cfg, err := ConfigFromEnv()
			if err != nil {
				t.Fatalf("ConfigFromEnv: %v", err)
			}
			parsed, err := mysqldriver.ParseDSN(cfg.MysqlDSN)
			if err != nil {
				t.Fatalf("ParseDSN: %v", err)
			}
			if parsed.TLS == nil {
				t.Fatal("DSN resolved to no TLS config")
			}

			serverCert := issueLeaf(t, caCert, caKey, tc.serverSAN)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Skipf("loopback listener unavailable: %v", err)
			}
			defer ln.Close()

			go func() {
				conn, aerr := ln.Accept()
				if aerr != nil {
					return
				}
				s := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{serverCert}})
				_ = s.Handshake()
				_, _ = io.Copy(io.Discard, s)
				_ = s.Close()
			}()

			raw, err := net.DialTimeout("tcp", ln.Addr().String(), 10*time.Second)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer raw.Close()

			c := tls.Client(raw, parsed.TLS)
			if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatalf("set deadline: %v", err)
			}
			err = c.Handshake()
			if tc.wantErr {
				if err == nil {
					t.Fatal("handshake succeeded against a certificate for another site; " +
						"the sidecar would accept the wrong server")
				}
				return
			}
			if err != nil {
				t.Fatalf("handshake against the documented SAN failed: %v", err)
			}
		})
	}
}

// TestRegisterMySQLTLS_ReloadsRotatedCA proves that the driver config reads the
// mounted CA again for each new handshake. cert-manager updates Secret volumes
// in place, and a long-running sidecar must trust the replacement issuer without
// waiting for a pod restart.
func TestRegisterMySQLTLS_ReloadsRotatedCA(t *testing.T) {
	const siteSAN = "mysql-lion-dc1.shared-lion.svc.cluster.local"
	caPath, firstCA, firstKey := writeCAWithKey(t)
	secondCAPath, secondCA, secondKey := writeCAWithKey(t)

	clearMySQLTLSEnv(t)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", caPath)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME", siteSAN)
	name, err := registerMySQLTLS()
	if err != nil {
		t.Fatalf("registerMySQLTLS: %v", err)
	}
	parsed, err := mysqldriver.ParseDSN("u:p@tcp(127.0.0.1:3306)/?tls=" + name)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}

	firstLeaf := issueLeaf(t, firstCA, firstKey, siteSAN)
	assertTLSHandshake(t, parsed.TLS, firstLeaf, false)

	rotatedPEM, err := os.ReadFile(secondCAPath)
	if err != nil {
		t.Fatalf("read rotated CA: %v", err)
	}
	if err := os.WriteFile(caPath, rotatedPEM, 0o600); err != nil {
		t.Fatalf("rotate CA: %v", err)
	}
	secondLeaf := issueLeaf(t, secondCA, secondKey, siteSAN)
	assertTLSHandshake(t, parsed.TLS, secondLeaf, false)
	assertTLSHandshake(t, parsed.TLS, firstLeaf, true)
}

// TestRegisterMySQLTLS_ReloadsRotatedClientCertificate covers the mTLS half of
// Secret rotation. The callback returns the current keypair, not the copy that
// happened to be mounted when the sidecar started.
func TestRegisterMySQLTLS_ReloadsRotatedClientCertificate(t *testing.T) {
	const siteSAN = "mysql-lion-dc1.shared-lion.svc.cluster.local"
	caPath, ca, caKey := writeCAWithKey(t)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	first := issueLeaf(t, ca, caKey, "client-one")
	writeTLSKeyPair(t, certPath, keyPath, first)

	clearMySQLTLSEnv(t)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CA_FILE", caPath)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_SERVER_NAME", siteSAN)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_CERT_FILE", certPath)
	t.Setenv("BLOODRAVEN_MYSQL_TLS_KEY_FILE", keyPath)
	name, err := registerMySQLTLS()
	if err != nil {
		t.Fatalf("registerMySQLTLS: %v", err)
	}
	parsed, err := mysqldriver.ParseDSN("u:p@tcp(127.0.0.1:3306)/?tls=" + name)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if parsed.TLS.GetClientCertificate == nil {
		t.Fatal("TLS config has no client-certificate reload callback")
	}
	gotFirst, err := parsed.TLS.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatalf("load first client certificate: %v", err)
	}

	second := issueLeaf(t, ca, caKey, "client-two")
	writeTLSKeyPair(t, certPath, keyPath, second)
	gotSecond, err := parsed.TLS.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatalf("load rotated client certificate: %v", err)
	}
	if bytes.Equal(gotFirst.Certificate[0], gotSecond.Certificate[0]) {
		t.Fatal("client certificate callback returned stale material after rotation")
	}
}

func assertTLSHandshake(t *testing.T, clientConfig *tls.Config, serverCert tls.Certificate, wantErr bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listener unavailable: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		s := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{serverCert}})
		_ = s.Handshake()
		_ = s.Close()
	}()

	raw, err := net.DialTimeout("tcp", ln.Addr().String(), 10*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	c := tls.Client(raw, clientConfig.Clone())
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	err = c.Handshake()
	if wantErr && err == nil {
		t.Fatal("TLS handshake succeeded, want certificate verification failure")
	}
	if !wantErr && err != nil {
		t.Fatalf("TLS handshake failed: %v", err)
	}
}

// writeCAWithKey generates a CA, writes its certificate to disk, and
// returns the path plus the material needed to issue leaves from it.
func writeCAWithKey(t *testing.T) (string, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sidecar-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return path, cert, key
}

// issueLeaf mints a server certificate with the given DNS SAN, signed by
// the test CA — the shape site/content/docs/4.configuration/6.credentials-and-tls.md asks for.
func issueLeaf(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, dnsName string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writeTLSKeyPair(t *testing.T, certPath, keyPath string, cert tls.Certificate) {
	t.Helper()
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("private key type = %T, want *ecdsa.PrivateKey", cert.PrivateKey)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatalf("write client certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}
}

// TestWithMySQLTLSConfig_PreservesExplicitChoice guards the legacy
// spec.secretName escape hatch: an operator who pinned their own tls=
// parameter in the DSN keeps it.
func TestWithMySQLTLSConfig_PreservesExplicitChoice(t *testing.T) {
	in := "u:p@tcp(127.0.0.1:3306)/?tls=skip-verify"
	got, err := withMySQLTLSConfig(in, mysqlTLSConfigName)
	if err != nil {
		t.Fatalf("withMySQLTLSConfig: %v", err)
	}
	if got != in {
		t.Errorf("DSN = %q, want it unchanged at %q", got, in)
	}
}

func TestWithMySQLTLSConfig_RejectsUnparseableDSN(t *testing.T) {
	if _, err := withMySQLTLSConfig("this is not a dsn", mysqlTLSConfigName); err == nil {
		t.Fatal("expected a parse error")
	}
}
