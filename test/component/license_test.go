package component

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/license"
	"github.com/shipstream/bloodraven/internal/metrics"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLicenseResolutionAndMetricLabels(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	kid := "test-only-1"
	keys := license.StaticKeys(map[string]ed25519.PublicKey{kid: pub})
	good, err := license.Encode(priv, kid, map[string]any{
		"iss": license.Issuer, "sub": "cus_test", "org": "Acme Corp",
		"edition": license.EditionOrganization, "issuedFor": "ord_test",
		"iat": now.Unix(), "updatesUntil": now.Add(30 * 24 * time.Hour).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	expired, err := license.Encode(priv, kid, map[string]any{
		"iss": license.Issuer, "sub": "cus_test", "org": "Acme Corp",
		"edition": license.EditionOrganization, "issuedFor": "ord_test",
		"iat":          now.Add(-400 * 24 * time.Hour).Unix(),
		"updatesUntil": now.Add(-24 * time.Hour).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}

	const ns, group = "warehouse", "orders-license"
	t.Cleanup(func() { metrics.DeleteLicense(ns, group) })

	r := &controller.MysqlFailoverGroupReconciler{
		LicenseKeys: keys,
		Now:         func() time.Time { return now },
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.LicenseInfo, metrics.LicenseUpdatesExpiry)

	observe := func(mfg, operator string) {
		fg := &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: group},
			Spec:       v1alpha1.MysqlFailoverGroupSpec{License: mfg},
		}
		r.OperatorLicense = operator
		r.ObserveLicense(fg)
	}

	observe("", "")
	info := gatherFamily(t, reg, "bloodraven_license_info")
	assertSeries(t, info, map[string]string{
		"namespace": ns, "group": group, "organization": "", "edition": "community", "valid": "true",
	}, 1)

	observe(good, "ignored")
	info = gatherFamily(t, reg, "bloodraven_license_info")
	assertSeries(t, info, map[string]string{
		"namespace": ns, "group": group, "organization": "Acme Corp", "edition": "organization", "valid": "true",
	}, 1)
	if _, ok := findSeries(info, map[string]string{"namespace": ns, "group": group, "edition": "community"}); ok {
		t.Fatal("stale community series survived a paid license")
	}
	expiry := gatherFamily(t, reg, "bloodraven_license_updates_expiry_timestamp_seconds")
	assertSeries(t, expiry, map[string]string{
		"namespace": ns, "group": group, "organization": "Acme Corp", "edition": "organization",
	}, float64(now.Add(30*24*time.Hour).Unix()))

	observe("not-a-token", good)
	info = gatherFamily(t, reg, "bloodraven_license_info")
	assertSeries(t, info, map[string]string{
		"namespace": ns, "group": group, "organization": "", "edition": "community", "valid": "false",
	}, 1)

	observe("", good)
	info = gatherFamily(t, reg, "bloodraven_license_info")
	assertSeries(t, info, map[string]string{
		"namespace": ns, "group": group, "organization": "Acme Corp", "edition": "organization", "valid": "true",
	}, 1)

	observe(expired, "")
	info = gatherFamily(t, reg, "bloodraven_license_info")
	assertSeries(t, info, map[string]string{
		"namespace": ns, "group": group, "organization": "Acme Corp", "edition": "organization", "valid": "true",
	}, 1)
	expiry = gatherFamily(t, reg, "bloodraven_license_updates_expiry_timestamp_seconds")
	assertSeries(t, expiry, map[string]string{
		"namespace": ns, "group": group, "organization": "Acme Corp", "edition": "organization",
	}, float64(now.Add(-24*time.Hour).Unix()))
}
