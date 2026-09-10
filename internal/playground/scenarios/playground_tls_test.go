package scenarios

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlaygroundCertificatesPassStrictVerification(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is required")
	}
	dir := t.TempDir()
	// Exercise the real generator with kubectl fully mocked and keys confined to TempDir.
	const script = `
kubectl() {
    if [[ "$*" == "config current-context" ]]; then
        printf 'kind-bloodraven-e2e\n'
        return 0
    fi
    shift 2
    case "$1" in
    get)
        [[ "$TLS_TEST_REUSE" == 1 ]] || return 1
        case "$*" in
        'get secret mysql-playground-tls'|'get secret bloodraven-escrow-tls') return 0 ;;
        'get secret mysql-playground-tls -o jsonpath={.data.ca\.crt}') base64 "$TLS_TEST_DIR/ca.crt" ;;
        'get secret mysql-playground-tls -o jsonpath={.data.tls\.crt}') base64 "$TLS_TEST_DIR/mysql.crt" ;;
        'get secret bloodraven-escrow-tls -o jsonpath={.data.tls\.crt}') base64 "$TLS_TEST_DIR/escrow.crt" ;;
        *) printf 'unexpected kubectl call: %s\n' "$*" >> "$TLS_TEST_DIR/unexpected"; return 1 ;;
        esac ;;
    create)
        if [[ "$TLS_TEST_REUSE" == 1 ]]; then
            printf 'unexpected mutation: %s\n' "$*" >> "$TLS_TEST_DIR/unexpected"
            return 1
        fi
        for arg in "$@"; do
            case "$arg" in
            --from-file=ca.crt=*)
                cp "${arg#--from-file=ca.crt=}" "$TLS_TEST_DIR/ca.crt"
                cp "$(dirname "${arg#--from-file=ca.crt=}")/ca.key" "$TLS_TEST_DIR/ca.key" ;;
            --from-file=tls.crt=*) cp "${arg#--from-file=tls.crt=}" "$TLS_TEST_DIR/mysql.crt" ;;
            --cert=*) cp "${arg#--cert=}" "$TLS_TEST_DIR/escrow.crt" ;;
            esac
        done ;;
    *) printf 'unexpected kubectl call: %s\n' "$*" >> "$TLS_TEST_DIR/unexpected"; return 1 ;;
    esac
}
export -f kubectl
bash ../../../playground/enable-encryption.sh --prepare-tls
`
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(), "TLS_TEST_DIR="+dir, "TLS_TEST_REUSE=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate playground certificates: %v\n%s", err, out)
	}
	for _, tc := range []struct{ cert, hostname string }{
		{"mysql", "mysql-playground-iad-internal.bloodraven-playground.svc.cluster.local"},
		{"escrow", "bloodraven.bloodraven-playground.svc.cluster.local"},
	} {
		t.Run(tc.cert, func(t *testing.T) {
			cmd := exec.Command("openssl", "verify", "-x509_strict", "-purpose", "sslserver",
				"-verify_hostname", tc.hostname, "-CAfile", filepath.Join(dir, "ca.crt"), filepath.Join(dir, tc.cert+".crt"))
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("strict certificate verification: %v\n%s", err, out)
			}
		})
	}
	t.Run("valid reuse", func(t *testing.T) {
		cmd := exec.Command("bash", "-c", script)
		cmd.Env = append(os.Environ(), "TLS_TEST_DIR="+dir, "TLS_TEST_REUSE=1")
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "passed strict verification") {
			t.Fatalf("reuse valid certificates: %v\n%s", err, out)
		}
	})
	for _, cert := range []string{"mysql", "escrow"} {
		t.Run(cert+" wrong SAN", func(t *testing.T) {
			path := filepath.Join(dir, cert+".crt")
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Fatal(err)
				}
			})
			other := "mysql"
			if cert == "mysql" {
				other = "escrow"
			}
			replacement, err := os.ReadFile(filepath.Join(dir, other+".crt"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, replacement, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-c", script)
			cmd.Env = append(os.Environ(), "TLS_TEST_DIR="+dir, "TLS_TEST_REUSE=1")
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "hostname mismatch") ||
				!strings.Contains(string(out), "Existing playground TLS material failed validation; no Secrets were changed") {
				t.Fatalf("reuse certificate with wrong SAN: %v\n%s", err, out)
			}
		})
	}
	t.Run("invalid old CA", func(t *testing.T) {
		// Reissue the same CA without keyUsage, as older playground setups did.
		cmd := exec.Command("openssl", "req", "-x509", "-new", "-days", "3650",
			"-key", filepath.Join(dir, "ca.key"), "-out", filepath.Join(dir, "ca.crt"),
			"-config", "/dev/null", "-subj", "/CN=bloodraven-playground-ca",
			"-addext", "basicConstraints=critical,CA:TRUE",
			"-addext", "subjectKeyIdentifier=hash")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("generate old CA: %v\n%s", err, out)
		}
		cmd = exec.Command("bash", "-c", script)
		cmd.Env = append(os.Environ(), "TLS_TEST_DIR="+dir, "TLS_TEST_REUSE=1")
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("invalid old CA was accepted: %s", out)
		}
		for _, want := range []string{
			"CA cert does not include key usage extension",
			"Existing playground TLS material failed validation; no Secrets were changed",
			"For a disposable playground only, recreate an empty playground cluster",
			"BLOODRAVEN_SETUP_TLS=1 ./playground/setup.sh",
			"preserve its data and keyring Secrets",
			"do not delete encrypted group keys or blindly rotate its CA",
		} {
			if !strings.Contains(string(out), want) {
				t.Errorf("failure output missing %q:\n%s", want, out)
			}
		}
	})
	if out, err := os.ReadFile(filepath.Join(dir, "unexpected")); !os.IsNotExist(err) {
		t.Fatalf("unexpected kubectl calls (read error: %v):\n%s", err, out)
	}
}
