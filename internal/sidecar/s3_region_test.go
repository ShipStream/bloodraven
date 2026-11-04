package sidecar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// s3ListServer answers ListObjectsV2 with one page per request and records
// the region each request was signed for.
func s3ListServer(t *testing.T, pages [][]string) (*httptest.Server, *[]string) {
	t.Helper()
	var mu sync.Mutex
	var regions []string
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		auth := r.Header.Get("Authorization")
		if i := strings.Index(auth, "Credential="); i >= 0 {
			parts := strings.Split(auth[i+len("Credential="):], "/")
			if len(parts) > 2 {
				regions = append(regions, parts[2])
			}
		}
		page := pages[min(call, len(pages)-1)]
		call++
		var body strings.Builder
		body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name>`)
		for _, k := range page {
			body.WriteString("<Contents><Key>" + k + "</Key><Size>1</Size></Contents>")
		}
		if call < len(pages) {
			body.WriteString("<IsTruncated>true</IsTruncated><NextContinuationToken>t" + string(rune('0'+call)) + "</NextContinuationToken>")
		} else {
			body.WriteString("<IsTruncated>false</IsTruncated>")
		}
		body.WriteString("</ListBucketResult>")
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body.String()))
	}))
	t.Cleanup(srv.Close)
	return srv, &regions
}

func s3TestCreds(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, value := range map[string]string{"AWS_ACCESS_KEY_ID": "AKIA", "AWS_SECRET_ACCESS_KEY": "secret"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// isolateAWSEnv keeps the developer's AWS configuration out of the test.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	empty := filepath.Join(t.TempDir(), "none")
	for _, name := range []string{"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_CONFIG_FILE", empty)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", empty)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// TestS3StoreWithoutRegion pins that an S3-compatible endpoint configured
// without any region still works: the SDK refuses to sign without one, so
// the store defaults to us-east-1 (what S3-compatible stores expect).
func TestS3StoreWithoutRegion(t *testing.T) {
	isolateAWSEnv(t)
	srv, regions := s3ListServer(t, [][]string{{"p/a", "p/b"}})
	store, err := newS3Store(context.Background(), &PITRS3Config{Bucket: "b", EndpointURL: srv.URL, AWSCredsDir: s3TestCreds(t)})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := store.List(context.Background(), "p/")
	if err != nil {
		t.Fatalf("list against an endpoint without a region: %v", err)
	}
	if len(keys) != 2 || len(*regions) != 1 || (*regions)[0] != defaultS3CompatibleRegion {
		t.Fatalf("keys=%v regions=%v", keys, *regions)
	}
}

func TestS3StoreKeepsConfiguredRegion(t *testing.T) {
	isolateAWSEnv(t)
	srv, regions := s3ListServer(t, [][]string{{"p/a"}})
	store, err := newS3Store(context.Background(), &PITRS3Config{Bucket: "b", EndpointURL: srv.URL, Region: "eu-west-3", AWSCredsDir: s3TestCreds(t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(context.Background(), "p/"); err != nil {
		t.Fatal(err)
	}
	if (*regions)[0] != "eu-west-3" {
		t.Fatalf("signed for %v, want eu-west-3", *regions)
	}
}

func TestS3StoreListPagesVisitsEachPage(t *testing.T) {
	isolateAWSEnv(t)
	srv, _ := s3ListServer(t, [][]string{{"p/a", "p/b"}, {"p/c"}, {"p/d"}})
	store, err := newS3Store(context.Background(), &PITRS3Config{Bucket: "b", EndpointURL: srv.URL, AWSCredsDir: s3TestCreds(t)})
	if err != nil {
		t.Fatal(err)
	}
	var pages [][]string
	if err := store.ListPages(context.Background(), "p/", func(keys []string) error {
		pages = append(pages, keys)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(pages) != 3 || len(pages[0]) != 2 || pages[2][0] != "p/d" {
		t.Fatalf("pages = %v", pages)
	}
}
