package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCloudflareDNS_UpdateAZRecord(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody cfUpdateRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			t.Errorf("bad auth header: %s", auth)
		}

		if strings.Contains(r.URL.Path, "dns_records") && r.Method == http.MethodGet {
			// List records
			json.NewEncoder(w).Encode(cfListResponse{
				Success: true,
				Result: []struct {
					ID string `json:"id"`
				}{{ID: "record-123"}},
			})
			return
		}

		gotMethod = r.Method
		gotPath = r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	dns := &cloudflareDNS{
		apiToken: "test-token",
		zoneID:   "zone-abc",
		az:       "lion",
		client:   srv.Client(),
	}

	// Override the base URL by replacing the client's transport
	origURL := srv.URL
	dns.client = &http.Client{
		Transport: &rewriteTransport{base: origURL},
	}

	err := dns.UpdateAZRecord(context.Background(), "1.2.3.4")
	if err != nil {
		t.Fatalf("UpdateAZRecord: %v", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method: got %s, want PUT", gotMethod)
	}
	if !strings.Contains(gotPath, "record-123") {
		t.Errorf("path should contain record ID: %s", gotPath)
	}
	if gotBody.Content != "1.2.3.4" {
		t.Errorf("content: got %s, want 1.2.3.4", gotBody.Content)
	}
	if gotBody.Name != "lion.az.shipstream.app" {
		t.Errorf("name: got %s, want lion.az.shipstream.app", gotBody.Name)
	}
}

// rewriteTransport rewrites Cloudflare API URLs to the test server.
type rewriteTransport struct {
	base string
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Rewrite cloudflare URLs to test server
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.base, "http://")
	return http.DefaultTransport.RoundTrip(req)
}
