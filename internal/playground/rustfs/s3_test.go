package rustfs

import (
	"strings"
	"testing"
	"time"
)

func TestSignedRequestUsesStableScopeAndHeaders(t *testing.T) {
	at := time.Date(2026, 5, 3, 7, 0, 0, 0, time.UTC)
	auth, amzDate, err := canonicalRequestForTest("PUT", "http://127.0.0.1:9000", "dragonfly", Credentials{
		AccessKey: "test-access",
		SecretKey: "test-secret",
		Region:    "us-east-1",
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if amzDate != "20260503T070000Z" {
		t.Fatalf("x-amz-date=%q", amzDate)
	}
	for _, want := range []string{
		"AWS4-HMAC-SHA256",
		"Credential=test-access/20260503/us-east-1/s3/aws4_request",
		"SignedHeaders=host;x-amz-content-sha256;x-amz-date",
		"Signature=",
	} {
		if !strings.Contains(auth, want) {
			t.Fatalf("Authorization=%q missing %q", auth, want)
		}
	}
}
