package controller

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func mysqlTLSConfig(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, serverName string) (string, error) {
	if fg.Spec.TLS == nil || fg.Spec.TLS.SecretName == "" {
		return "", nil
	}

	secretName := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.TLS.SecretName}
	var secret corev1.Secret
	if err := c.Get(ctx, secretName, &secret); err != nil {
		return "", fmt.Errorf("get TLS secret %s: %w", secretName, err)
	}

	caPEM := secret.Data["ca.crt"]
	if len(caPEM) == 0 {
		return "", fmt.Errorf("TLS secret %s missing required ca.crt", secretName)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return "", fmt.Errorf("TLS secret %s has invalid ca.crt", secretName)
	}

	cfg := &tls.Config{
		RootCAs:    rootCAs,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}
	certPEM, hasCert := secret.Data["tls.crt"]
	keyPEM, hasKey := secret.Data["tls.key"]
	if hasCert || hasKey {
		if len(certPEM) == 0 || len(keyPEM) == 0 {
			return "", fmt.Errorf("TLS secret %s must contain both tls.crt and tls.key when client cert material is provided", secretName)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return "", fmt.Errorf("TLS secret %s has invalid client certificate/key pair: %w", secretName, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	name := mysqlTLSConfigName(fg.Namespace, fg.Name, serverName, secret.Data)
	if err := mysqldriver.RegisterTLSConfig(name, cfg); err != nil {
		return "", fmt.Errorf("register MySQL TLS config %q: %w", name, err)
	}
	return name, nil
}

// mysqlTLSMountPath is where the operator projects spec.tls.secretName
// into the mysql and sidecar containers.
const mysqlTLSMountPath = "/etc/mysql/tls"

// mysqlTLSSidecarEnv is the sidecar's half of the MySQL TLS contract:
// where its client finds the CA and (optional) client keypair, and which
// name to verify the server certificate against.
//
// The sidecar connects over loopback, and 127.0.0.1 is in no server
// certificate, so it cannot verify against the address it dials. It
// verifies against the per-site Service instead — the SAN
// site/content/docs/4.configuration/6.credentials-and-tls.md tells users to include, and the same
// name the operator's own credentials-mode client verifies (see the
// mysqlTLSConfig call sites, which likewise dial the -internal Service).
//
// Returns nil when spec.tls is unset, so groups without TLS render
// exactly as before.
func mysqlTLSSidecarEnv(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) []corev1.EnvVar {
	if fg.Spec.TLS == nil || fg.Spec.TLS.SecretName == "" {
		return nil
	}
	return []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_TLS_CA_FILE", Value: mysqlTLSMountPath + "/ca.crt"},
		{Name: "BLOODRAVEN_MYSQL_TLS_CERT_FILE", Value: mysqlTLSMountPath + "/tls.crt"},
		{Name: "BLOODRAVEN_MYSQL_TLS_KEY_FILE", Value: mysqlTLSMountPath + "/tls.key"},
		{Name: "BLOODRAVEN_MYSQL_TLS_SERVER_NAME", Value: siteServiceHost(fg.Name, site.Name, fg.Namespace)},
	}
}

func mysqlTLSConfigName(namespace, group, serverName string, data map[string][]byte) string {
	h := sha256.New()
	h.Write([]byte(serverName))
	for _, key := range []string{"ca.crt", "tls.crt", "tls.key"} {
		if v := data[key]; len(v) > 0 {
			sum := sha256.Sum256(v)
			h.Write(sum[:])
		}
	}
	return fmt.Sprintf("bloodraven-%s-%s-%s", namespace, group, hex.EncodeToString(h.Sum(nil))[:16])
}

func siteServiceHost(group, site, namespace string) string {
	return fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local", group, site, namespace)
}

func internalSiteServiceName(group, site string) string {
	return fmt.Sprintf("mysql-%s-%s-internal", group, site)
}

func internalSiteServiceHost(group, site, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", internalSiteServiceName(group, site), namespace)
}

func primaryServiceHost(group, namespace string) string {
	return fmt.Sprintf("mysql-%s-primary.%s.svc.cluster.local", group, namespace)
}
