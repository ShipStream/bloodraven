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
