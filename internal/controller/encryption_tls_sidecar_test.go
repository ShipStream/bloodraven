package controller

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// These tests cover the spec.tls + spec.encryptionAtRest combination.
// Playground scenario 48 exercises it in a dedicated encryption CI job rather
// than a shared batch profile because it requires
// ./playground/enable-encryption.sh. Both bugs they pin were found in a 0.9.0
// lab drill:
//
//  1. The escrow token was projected mode 0400 into a pod with no
//     fsGroup, so the non-root sidecar could never read it and no site
//     ever sealed.
//  2. spec.tls makes the operator set require_secure_transport=ON, but
//     the sidecar's MySQL DSN carried no TLS parameter, so every sidecar
//     query failed with Error 3159 and the liveness probe crash-looped
//     the container — taking self-fencing and the super_read_only safety
//     net down with it.
//
// Everything here runs under `make test` — deliberately, since a fix
// that only landed in scenario 48 would protect nobody.
//
// These are cross-component tests (operator rendering -> sidecar config
// parsing) that would normally live in test/component, but they need the
// unexported render helpers, so they sit next to the code they cover and
// import internal/sidecar for the other half. internal/controller already
// depends on internal/sidecar (standbycluster_reconciler.go) and the
// dependency is one-way, so there is no cycle.

// sidecarContainer returns the rendered sidecar container for a site.
func sidecarContainer(t *testing.T, r *MysqlFailoverGroupReconciler, site string) *corev1.Container {
	t.Helper()
	d := getDeployment(t, r, site)
	c := containerByName(d.Spec.Template.Spec.Containers, "sidecar")
	if c == nil {
		t.Fatalf("site %s has no sidecar container", site)
	}
	return c
}

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// --- Bug 1: the escrow token must be readable by the sidecar ---------

// TestEncryption_EscrowTokenReadableBySidecar pins the file-permission
// contract between the token projection and the uid the sidecar runs as.
// A Secret volume is owned by root:root unless the pod sets an fsGroup,
// and spec.podSecurityContext is opt-in, so a mode without the world-read
// bit is unreadable by the sidecar's uid 999 — deterministically, on
// every retry.
func TestEncryption_EscrowTokenReadableBySidecar(t *testing.T) {
	fg := encTestFG()
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	d := getDeployment(t, r, "dc1")
	vol := findVolume(d.Spec.Template.Spec.Volumes, keyringTokenVolumeName)
	if vol == nil || vol.Secret == nil {
		t.Fatalf("unsealed site must project the escrow token Secret: %+v", vol)
	}
	if vol.Secret.DefaultMode == nil {
		t.Fatal("escrow token projection has no explicit mode")
	}
	mode := os.FileMode(*vol.Secret.DefaultMode)

	sc := sidecarContainer(t, r, "dc1")
	if sc.SecurityContext == nil || sc.SecurityContext.RunAsUser == nil {
		t.Fatal("encryption should pin the sidecar to mysqld's uid so it can read the keyring")
	}
	if *sc.SecurityContext.RunAsUser == 0 {
		t.Fatal("sanity: sidecar is expected to run non-root under encryption")
	}

	// No fsGroup -> kubelet leaves the projected file root:root, so only
	// the world-read bit can make it openable by the sidecar's uid.
	podSC := d.Spec.Template.Spec.SecurityContext
	if (podSC == nil || podSC.FSGroup == nil) && mode&0o004 == 0 {
		t.Fatalf("escrow token mode %#o is unreadable by the non-root sidecar (uid %d) "+
			"in a pod with no fsGroup: the keyring never escrows and the group is "+
			"stuck at phase=Unsealed/reason=Bootstrap forever",
			mode, *sc.SecurityContext.RunAsUser)
	}
}

// TestEncryption_EscrowTokenTightensWithFSGroup checks the other half of
// the contract: when the operator IS given an fsGroup, kubelet chgrps the
// projection and adds the gid as a supplementary group on the sidecar, so
// the token does not have to be world-readable.
func TestEncryption_EscrowTokenTightensWithFSGroup(t *testing.T) {
	fg := encTestFG()
	fsGroup := int64(999)
	fg.Spec.PodSecurityContext = &corev1.PodSecurityContext{FSGroup: &fsGroup}
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	d := getDeployment(t, r, "dc1")
	vol := findVolume(d.Spec.Template.Spec.Volumes, keyringTokenVolumeName)
	if vol == nil || vol.Secret == nil || vol.Secret.DefaultMode == nil {
		t.Fatalf("unsealed site must project the escrow token Secret: %+v", vol)
	}
	mode := os.FileMode(*vol.Secret.DefaultMode)
	if mode&0o040 == 0 {
		t.Errorf("escrow token mode %#o is not group-readable; with fsGroup set that is "+
			"how the sidecar reads it", mode)
	}
	if mode&0o004 != 0 {
		t.Errorf("escrow token mode %#o is world-readable even though fsGroup makes "+
			"group access sufficient", mode)
	}
}

// --- Bug 2: the sidecar's MySQL client must speak TLS ----------------

// TestEncryption_SidecarMySQLDSNUsesTLS is the cross-component contract
// test: it takes the env the operator actually renders onto the sidecar
// container and feeds it to the sidecar's own config parser. That is the
// seam bug 2 fell through — the operator rendered a TLS volume the
// sidecar never wired into its DSN, and neither side's unit tests could
// see the gap.
func TestEncryption_SidecarMySQLDSNUsesTLS(t *testing.T) {
	// Both credential shapes: spec.credentials (MYSQL_USER/MYSQL_PASSWORD)
	// and the legacy spec.secretName DSN. The lab report was the latter's
	// sibling, but the operator renders TLS for both, so both must dial
	// TLS or the same crash loop follows.
	for _, tc := range []struct {
		name string
		fg   func() *v1alpha1.MysqlFailoverGroup
	}{
		{"credentials", func() *v1alpha1.MysqlFailoverGroup {
			fg := encTestFG()
			fg.Spec.SecretName = ""
			fg.Spec.Credentials = &v1alpha1.CredentialsSpec{OperatorSecret: "mysql-operator-creds"}
			return fg
		}},
		{"dsnSecret", encTestFG},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := tc.fg()
			objs := []client.Object{fg}
			for _, s := range newTestCredentialSecrets() {
				objs = append(objs, s)
			}
			r, _ := encReconciler(t, scriptedKeyring(nil), objs...)
			reconcileOnce(t, r)

			// spec.tls is what turns on require_secure_transport, so assert
			// that premise here rather than assuming it.
			cm := siteConfigMap(t, r, "dc1")
			if !strings.Contains(cm.Data["bloodraven.cnf"], "require-secure-transport=ON") {
				t.Fatalf("premise broken: spec.tls should set require-secure-transport=ON:\n%s",
					cm.Data["bloodraven.cnf"])
			}

			mysqlContainer := containerByName(getDeployment(t, r, "dc1").Spec.Template.Spec.Containers, "mysql")
			if mysqlContainer == nil {
				t.Fatal("deployment has no mysql container")
			}
			mysqlArgs := strings.Join(mysqlContainer.Args, "\n")
			for _, want := range []string{
				"--ssl-ca=/etc/mysql/tls/ca.crt",
				"--ssl-cert=/etc/mysql/tls/tls.crt",
				"--ssl-key=/etc/mysql/tls/tls.key",
				"--require-secure-transport=ON",
			} {
				if !strings.Contains(mysqlArgs, want) {
					t.Fatalf("mysql args missing %q; server could fall back to its auto-generated certificate:\n%s",
						want, mysqlArgs)
				}
			}

			sc := sidecarContainer(t, r, "dc1")
			if findMount(sc.VolumeMounts, "/etc/mysql/tls") == nil {
				t.Fatal("sidecar has no TLS material mounted")
			}

			dsn := sidecarDSNFromRenderedEnv(t, sc)
			if !strings.Contains(dsn, "tls=") {
				t.Fatalf("sidecar MySQL DSN has no TLS parameter, so every query fails "+
					"with Error 3159 against require_secure_transport=ON and the "+
					"liveness probe crash-loops the container (self-fencing and the "+
					"super_read_only safety net go down with it): %s", redactDSN(dsn))
			}
		})
	}
}

// TestEncryption_SidecarTLSVerifiesAgainstDocumentedSAN pins which name
// the sidecar verifies. It dials 127.0.0.1, which is in no server
// certificate, so the operator has to hand it a ServerName — and the only
// name users are told to put in the SAN list is the per-site Service
// (docs/docs/credentials-and-tls.mdx).
func TestEncryption_SidecarTLSVerifiesAgainstDocumentedSAN(t *testing.T) {
	fg := encTestFG()
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	sc := sidecarContainer(t, r, "dc1")
	got, ok := envValue(sc.Env, "BLOODRAVEN_MYSQL_TLS_SERVER_NAME")
	if !ok {
		t.Fatal("sidecar has no BLOODRAVEN_MYSQL_TLS_SERVER_NAME; " +
			"verification against a loopback dial is impossible without one")
	}
	if want := siteServiceHost("lion", "dc1", "shared-lion"); got != want {
		t.Errorf("server name = %q, want the documented per-site Service SAN %q", got, want)
	}
	if ca, _ := envValue(sc.Env, "BLOODRAVEN_MYSQL_TLS_CA_FILE"); ca != "/etc/mysql/tls/ca.crt" {
		t.Errorf("CA file = %q, want the mounted /etc/mysql/tls/ca.crt", ca)
	}
}

// TestEncryption_SidecarDSNPlaintextWithoutTLS is the guard for the
// untouched path: with no spec.tls the sidecar DSN must stay exactly what
// pre-fix releases produced.
func TestEncryption_SidecarDSNPlaintextWithoutTLS(t *testing.T) {
	fg := newTestFGWithCredentials()
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, _ := encReconciler(t, nil, objs...)
	reconcileOnce(t, r)

	sc := sidecarContainer(t, r, "dc1")
	for _, e := range sc.Env {
		if strings.HasPrefix(e.Name, "BLOODRAVEN_MYSQL_TLS") {
			t.Errorf("non-TLS group must not carry %s", e.Name)
		}
	}
	dsn := sidecarDSNFromRenderedEnv(t, sc)
	if strings.Contains(dsn, "tls=") {
		t.Errorf("non-TLS group must keep a plaintext DSN: %s", redactDSN(dsn))
	}
}

// --- The operator's own legacy-DSN-mode client ----------------------

// TestBuildSiteDSN_StampsTLSWhenUnset covers the same class of bug on
// the operator's side of the fence. In legacy spec.secretName mode the
// site DSN is user-authored; without a tls= parameter every site probe
// fails against require_secure_transport=ON and no failover can run. An
// explicit user choice still wins.
func TestBuildSiteDSN_StampsTLSWhenUnset(t *testing.T) {
	fg := encTestFG()
	site := fg.Spec.Sites[0]

	for _, tc := range []struct {
		name    string
		in      string
		tlsName string
		want    string
	}{
		{
			name:    "plaintext DSN gets the registered config",
			in:      "u:p@tcp(old-host:3306)/",
			tlsName: "bloodraven-test-tls",
			want:    "tls=bloodraven-test-tls",
		},
		{
			name:    "explicit user choice is preserved",
			in:      "u:p@tcp(old-host:3306)/?tls=skip-verify",
			tlsName: "bloodraven-test-tls",
			want:    "tls=skip-verify",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildSiteDSN(tc.in, fg, site, tc.tlsName)
			if err != nil {
				t.Fatalf("buildSiteDSN: %v", err)
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("DSN = %q, want it to contain %q", redactDSN(got), tc.want)
			}
			// The host rewrite must survive either way.
			if !strings.Contains(got, internalSiteServiceHost("lion", "dc1", "shared-lion")) {
				t.Errorf("DSN lost the site host rewrite: %q", redactDSN(got))
			}
		})
	}

	// No TLS configured -> untouched apart from the host rewrite.
	got, err := buildSiteDSN("u:p@tcp(old-host:3306)/", fg, site, "")
	if err != nil {
		t.Fatalf("buildSiteDSN: %v", err)
	}
	if strings.Contains(got, "tls=") {
		t.Errorf("non-TLS group must not gain a tls parameter: %q", redactDSN(got))
	}
}

// --- Escrow stall diagnosability ------------------------------------

// TestWithSidecarError_SurfacesStallReason pins that a stalled escrow
// explains itself in status. The sidecar already reports why its push is
// failing; dropping that field is what turned this into a log hunt.
func TestWithSidecarError_SurfacesStallReason(t *testing.T) {
	const base = "waiting for the sidecar to escrow the keyring"

	if got := withSidecarError(base, ""); got != base {
		t.Errorf("a healthy sidecar must not change the message: %q", got)
	}

	got := withSidecarError(base, "  escrow push: x509: certificate signed by unknown authority  ")
	if !strings.Contains(got, "x509: certificate signed by unknown authority") {
		t.Errorf("status must carry the sidecar's reason: %q", got)
	}
	if !strings.HasPrefix(got, base) {
		t.Errorf("status must keep the original message: %q", got)
	}

	long := withSidecarError(base, strings.Repeat("x", maxSidecarErrorInStatus+50))
	if len(long) > len(base)+maxSidecarErrorInStatus+40 {
		t.Errorf("status message is not bounded: %d chars", len(long))
	}
}

// sidecarDSNFromRenderedEnv applies the container's rendered env to the
// process and runs the sidecar's real ConfigFromEnv over it, returning
// the DSN the sidecar would dial with. Secret-backed values are stubbed —
// only their shape matters here.
func sidecarDSNFromRenderedEnv(t *testing.T, c *corev1.Container) string {
	t.Helper()
	// Clear anything the ambient environment might contribute so the
	// rendered spec is the only input. t.Setenv restores on cleanup.
	for _, name := range []string{
		"MYSQL_DSN", "MYSQL_CREDS_DIR", "MYSQL_USER", "MYSQL_PASSWORD",
		"BLOODRAVEN_PITR_ENABLED", "BLOODRAVEN_KEYRING_ENABLED",
		"BLOODRAVEN_MYSQL_TLS_CA_FILE", "BLOODRAVEN_MYSQL_TLS_CERT_FILE",
		"BLOODRAVEN_MYSQL_TLS_KEY_FILE", "BLOODRAVEN_MYSQL_TLS_SERVER_NAME",
	} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	// The rendered CA path only exists inside a real pod. Point the
	// sidecar at a generated CA on disk so the production certificate
	// loading actually runs instead of being stubbed out.
	caPath := writeTestCA(t)
	for _, e := range c.Env {
		switch {
		case strings.HasPrefix(e.Name, "BLOODRAVEN_MYSQL_TLS_") && strings.HasSuffix(e.Name, "_FILE"):
			if e.Name == "BLOODRAVEN_MYSQL_TLS_CA_FILE" {
				t.Setenv(e.Name, caPath)
			}
			// tls.crt / tls.key are optional client-auth material and are
			// left unset so the optional path is exercised too.
		case e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil:
			if e.ValueFrom.SecretKeyRef.Key == "dsn" {
				// Shape-accurate stand-in for the legacy DSN secret.
				t.Setenv(e.Name, "bloodraven:pw@tcp(127.0.0.1:3306)/")
				continue
			}
			t.Setenv(e.Name, "stub-"+e.ValueFrom.SecretKeyRef.Key)
		case e.ValueFrom != nil:
			// FieldRef (pod name / namespace) — irrelevant to the DSN.
			t.Setenv(e.Name, "stub")
		default:
			t.Setenv(e.Name, e.Value)
		}
	}
	// The keyring block reads files that only exist in a real pod. Keep it
	// out of the way so an unrelated parse error cannot be mistaken for a
	// DSN problem.
	if err := os.Unsetenv("BLOODRAVEN_KEYRING_ENABLED"); err != nil {
		t.Fatalf("unset BLOODRAVEN_KEYRING_ENABLED: %v", err)
	}

	cfg, err := sidecar.ConfigFromEnv()
	if err != nil {
		t.Fatalf("sidecar rejected the env the operator rendered: %v", err)
	}
	return cfg.MysqlDSN
}

// writeTestCA generates a throwaway CA certificate and returns its path,
// standing in for the cert-manager ca.crt the operator mounts.
func writeTestCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bloodraven-test-ca"},
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
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return path
}

// redactDSN strips the password so a failing assertion cannot print it.
func redactDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i > 0 {
		if j := strings.Index(dsn[:i], ":"); j >= 0 {
			return dsn[:j] + ":***" + dsn[i:]
		}
	}
	return dsn
}

func siteConfigMap(t *testing.T, r *MysqlFailoverGroupReconciler, site string) *corev1.ConfigMap {
	t.Helper()
	deployment := getDeployment(t, r, site)
	var cm corev1.ConfigMap
	key := types.NamespacedName{
		Namespace: "shared-lion",
		Name:      deploymentConfigMapName(deployment),
	}
	if err := r.Get(t.Context(), key, &cm); err != nil {
		t.Fatalf("get configmap for %s: %v", site, err)
	}
	return &cm
}
