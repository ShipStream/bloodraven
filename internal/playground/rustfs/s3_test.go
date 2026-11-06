package rustfs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// TestListKeysPaginates pins that ListKeys asks for small pages and follows
// continuation tokens to the end, so the playground's storage assertion
// covers multi-page listings against RustFS.
func TestListKeysPaginates(t *testing.T) {
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = "p/b1/" + strconv.Itoa(i)
	}
	var maxKeys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		maxKeys = append(maxKeys, q.Get("max-keys"))
		start := 0
		if tok := q.Get("continuation-token"); tok != "" {
			start, _ = strconv.Atoi(tok)
		}
		end := min(start+listPageSize, len(keys))
		var body strings.Builder
		body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bk</Name>`)
		for _, k := range keys[start:end] {
			body.WriteString("<Contents><Key>" + k + "</Key><Size>1</Size></Contents>")
		}
		if end < len(keys) {
			body.WriteString("<IsTruncated>true</IsTruncated><NextContinuationToken>" + strconv.Itoa(end) + "</NextContinuationToken>")
		} else {
			body.WriteString("<IsTruncated>false</IsTruncated>")
		}
		body.WriteString("</ListBucketResult>")
		_, _ = w.Write([]byte(body.String()))
	}))
	defer srv.Close()
	got, err := ListKeys(context.Background(), srv.URL, "bk", "p/b1/", Credentials{AccessKey: "a", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) || len(maxKeys) != 3 || maxKeys[0] != strconv.Itoa(listPageSize) {
		t.Fatalf("got %d keys over %d requests (max-keys %v)", len(got), len(maxKeys), maxKeys)
	}
}

// TestDeletePrefixDeletesEveryListedKey pins F191's archive cleanup: every
// key under the prefix is deleted, over a paginated listing, and a root or
// unterminated prefix is refused before any request.
func TestDeletePrefixDeletesEveryListedKey(t *testing.T) {
	keys := make([]string, 7)
	for i := range keys {
		keys[i] = "e2e/s37/run/binlogs/" + strconv.Itoa(i)
	}
	var deleted []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/bk/"))
			w.WriteHeader(http.StatusNoContent)
			return
		}
		q := r.URL.Query()
		start := 0
		if tok := q.Get("continuation-token"); tok != "" {
			start, _ = strconv.Atoi(tok)
		}
		end := min(start+listPageSize, len(keys))
		var body strings.Builder
		body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bk</Name>`)
		for _, k := range keys[start:end] {
			body.WriteString("<Contents><Key>" + k + "</Key><Size>1</Size></Contents>")
		}
		if end < len(keys) {
			body.WriteString("<IsTruncated>true</IsTruncated><NextContinuationToken>" + strconv.Itoa(end) + "</NextContinuationToken>")
		} else {
			body.WriteString("<IsTruncated>false</IsTruncated>")
		}
		body.WriteString("</ListBucketResult>")
		_, _ = w.Write([]byte(body.String()))
	}))
	defer srv.Close()
	creds := Credentials{AccessKey: "a", SecretKey: "s"}
	n, err := DeletePrefix(context.Background(), srv.URL, "bk", "e2e/s37/run/", creds)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(keys) || len(deleted) != len(keys) {
		t.Fatalf("deleted %d (%v), want %d", n, deleted, len(keys))
	}
	for i, k := range keys {
		if deleted[i] != k {
			t.Fatalf("deleted[%d]=%q, want %q", i, deleted[i], k)
		}
	}
	for _, bad := range []string{"", "/", "e2e/s37/run"} {
		if _, err := DeletePrefix(context.Background(), srv.URL, "bk", bad, creds); err == nil {
			t.Errorf("DeletePrefix(%q) must be refused", bad)
		}
	}
}
