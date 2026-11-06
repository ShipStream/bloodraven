package sidecar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// testRetentionArchiveID is a well-formed archive ID for tests that wire a
// retention client; the sidecar does not validate it, the operator does.
const testRetentionArchiveID = "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"

func retentionEnvConfig() *Config {
	return &Config{BloodravenAddress: "bloodraven:8091", PodNamespace: "ns", FailoverGroup: "fg"}
}

// TestRetentionClientFromEnvRequiresArchiveID pins Δ12: the retention
// client identifies itself by the archive it writes, and the profile name
// is no longer read at all.
func TestRetentionClientFromEnvRequiresArchiveID(t *testing.T) {
	t.Run("archive id set", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_PITR_ARCHIVE_ID", testRetentionArchiveID)
		t.Setenv("BLOODRAVEN_PITR_PROFILE_NAME", "")
		got, err := retentionClientFromEnv(retentionEnvConfig())
		if err != nil {
			t.Fatalf("retentionClientFromEnv: %v", err)
		}
		if got == nil || got.ArchiveID != testRetentionArchiveID || got.Namespace != "ns" || got.FailoverGroup != "fg" || got.Interval != time.Hour {
			t.Fatalf("retentionClientFromEnv = %+v, want archive %s in ns/fg at 1h", got, testRetentionArchiveID)
		}
	})
	t.Run("profile set, archive id unset", func(t *testing.T) {
		t.Setenv("BLOODRAVEN_PITR_ARCHIVE_ID", "")
		t.Setenv("BLOODRAVEN_PITR_PROFILE_NAME", "nightly")
		got, err := retentionClientFromEnv(retentionEnvConfig())
		if err != nil {
			t.Fatalf("retentionClientFromEnv: %v", err)
		}
		if got != nil {
			t.Fatalf("retentionClientFromEnv = %+v, want nil without BLOODRAVEN_PITR_ARCHIVE_ID", got)
		}
	})
}

// TestRetentionCutoffQueryUsesArchiveID pins the /pitr-cutoff query: the
// archive ID replaces the profile name, and every value is URL-escaped.
func TestRetentionCutoffQueryUsesArchiveID(t *testing.T) {
	var raw string
	var query url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		raw, query = r.URL.RawQuery, r.URL.Query()
		_, _ = rw.Write([]byte(`{}`))
	}))
	defer srv.Close()

	a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "dc1", &stubRoleChecker{}, newMemStore(), testLogger())
	a.SetRetentionClient(strings.TrimPrefix(srv.URL, "http://"), "ns", "fg&profile=x", testRetentionArchiveID, time.Hour)
	_, _ = a.fetchRetentionCutoff(context.Background())

	if query == nil {
		t.Fatal("operator was not queried")
	}
	if !strings.Contains(raw, "archive="+testRetentionArchiveID) {
		t.Fatalf("query %q does not carry archive=%s", raw, testRetentionArchiveID)
	}
	if strings.Contains(raw, "profile=") {
		t.Fatalf("query %q still sends a profile", raw)
	}
	for k, want := range map[string]string{"namespace": "ns", "group": "fg&profile=x", "archive": testRetentionArchiveID, "coverage": PITRGTIDCoverageProtocol} {
		if got := query.Get(k); got != want {
			t.Errorf("query %s = %q, want %q", k, got, want)
		}
	}
	if query.Has("profile") {
		t.Errorf("query %q has a profile parameter", raw)
	}
}
