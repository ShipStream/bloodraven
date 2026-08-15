package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/license"
	"github.com/shipstream/bloodraven/internal/metrics"
)

func licenseTestAuthority(t *testing.T) (ed25519.PrivateKey, string, license.KeyLookup) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	kid := "test-only-1"
	return priv, kid, license.StaticKeys(map[string]ed25519.PublicKey{kid: pub})
}

func licenseClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss":          license.Issuer,
		"sub":          "cus_test",
		"org":          "Acme Corp",
		"edition":      license.EditionOrganization,
		"issuedFor":    "ord_test",
		"iat":          now.Unix(),
		"updatesUntil": now.Add(30 * 24 * time.Hour).Unix(),
	}
}

func mustLicenseToken(t *testing.T, priv ed25519.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	tok, err := license.Encode(priv, kid, claims)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return tok
}

func TestComputeSpecHashIgnoresLicense(t *testing.T) {
	fg := newTestFG()
	site := fg.Spec.Sites[0]
	before := ComputeSpecHash(fg, site, nil, nil)
	fg.Spec.License = "eyJhbGciOiJFZERTQSJ9.e30.sig"
	after := ComputeSpecHash(fg, site, nil, nil)
	if before != after {
		t.Fatal("spec.license must not change the deployment hash")
	}
}

func TestCRConfigToTopologyConfigIgnoresLicense(t *testing.T) {
	fg := newTestFG()
	before := CRConfigToTopologyConfig(fg)
	fg.Spec.License = "eyJhbGciOiJFZERTQSJ9.e30.sig"
	after := CRConfigToTopologyConfig(fg)
	if !before.Equal(after) {
		t.Fatal("spec.license must not restart the topology manager")
	}
}

func TestObserveLicenseNeverFailsReconcile(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	priv, kid, keys := licenseTestAuthority(t)
	fg := newTestFG()
	fg.Spec.License = "this-is-not-a-token"
	r, c := newReconciler(fg)
	r.LicenseKeys = keys
	r.Now = func() time.Time { return now }
	r.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	t.Cleanup(func() { metrics.DeleteLicense(nn.Namespace, nn.Name) })

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("invalid license must not fail reconcile: %v", err)
	}

	good := mustLicenseToken(t, priv, kid, licenseClaims(now))
	var live v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.License = good
	if err := c.Update(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	res2, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("valid license must not fail reconcile: %v", err)
	}
	if res2 != res {
		t.Fatalf("license must not change reconcile result: community=%+v licensed=%+v", res, res2)
	}
}

func TestObserveLicenseTransitions(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	priv, kid, keys := licenseTestAuthority(t)
	good := mustLicenseToken(t, priv, kid, licenseClaims(now))
	fg := newTestFG()
	ns, name := fg.Namespace, fg.Name
	t.Cleanup(func() { metrics.DeleteLicense(ns, name) })

	var logBuf bytes.Buffer
	rec := record.NewFakeRecorder(16)
	r := &MysqlFailoverGroupReconciler{
		Recorder:    rec,
		Logger:      slog.New(slog.NewJSONHandler(&logBuf, nil)),
		LicenseKeys: keys,
		Now:         func() time.Time { return now },
	}

	r.observeLicense(fg)
	if got := drainFakeEvents(rec); licenseEventCount(got, eventLicenseInvalid)+licenseEventCount(got, eventLicenseVerified) != 0 {
		t.Fatalf("community must not emit license events: %v", got)
	}

	fg.Spec.License = "bad-token"
	r.observeLicense(fg)
	r.observeLicense(fg)
	events := drainFakeEvents(rec)
	if licenseEventCount(events, eventLicenseInvalid) != 1 {
		t.Fatalf("invalid token should event once, got %v", events)
	}

	fg.Spec.License = good
	r.observeLicense(fg)
	events = drainFakeEvents(rec)
	if licenseEventCount(events, eventLicenseVerified) != 1 {
		t.Fatalf("valid token should event once, got %v", events)
	}
	if !strings.Contains(logBuf.String(), msgLicenseVerified) {
		t.Fatalf("expected %q in logs: %s", msgLicenseVerified, logBuf.String())
	}

	r.forgetLicense(types.NamespacedName{Namespace: ns, Name: name})
	fg.Spec.License = ""
	r.observeLicense(fg)
	fg.Spec.License = "bad-token"
	r.observeLicense(fg)
	events = drainFakeEvents(rec)
	if licenseEventCount(events, eventLicenseInvalid) != 1 {
		t.Fatalf("recreation should warn again, got %v", events)
	}
}

func TestObserveLicenseInvalidMFGDoesNotUseOperator(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	priv, kid, keys := licenseTestAuthority(t)
	operator := mustLicenseToken(t, priv, kid, licenseClaims(now))
	fg := newTestFG()
	fg.Spec.License = "bad"
	t.Cleanup(func() { metrics.DeleteLicense(fg.Namespace, fg.Name) })
	r := &MysqlFailoverGroupReconciler{
		OperatorLicense: operator,
		LicenseKeys:     keys,
		Now:             func() time.Time { return now },
		Logger:          slog.New(slog.DiscardHandler),
	}
	r.observeLicense(fg)
	r.licenseMu.Lock()
	obs := r.licenseObs[types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}]
	r.licenseMu.Unlock()
	if obs.valid != "false" || obs.edition != license.EditionCommunity {
		t.Fatalf("invalid MFG must not fall through: %+v", obs)
	}
}

func TestLogOperatorLicenseHasNoFG(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	priv, kid, keys := licenseTestAuthority(t)
	tok := mustLicenseToken(t, priv, kid, licenseClaims(now))
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	LogOperatorLicense(logger, tok, keys, now)
	out := buf.String()
	if !strings.Contains(out, msgOperatorVerified) {
		t.Fatalf("missing startup msg: %s", out)
	}
	if strings.Contains(out, `"fg"`) {
		t.Fatalf("startup log must not include fg: %s", out)
	}
}

func drainFakeEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func licenseEventCount(events []string, reason string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}
