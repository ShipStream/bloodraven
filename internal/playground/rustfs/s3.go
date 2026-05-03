package rustfs

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type Credentials struct {
	AccessKey string
	SecretKey string
	Region    string
}

// EnsureBucket creates bucket when it is missing. Requests are signed with
// AWS Signature V4 so the runner does not depend on a separate aws-cli image
// inside the cluster.
func EnsureBucket(ctx context.Context, endpoint, bucket string, creds Credentials) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if creds.AccessKey == "" || creds.SecretKey == "" {
		return fmt.Errorf("access key and secret key are required")
	}
	if creds.Region == "" {
		creds.Region = "us-east-1"
	}
	endpoint = strings.TrimRight(endpoint, "/")
	if _, err := url.Parse(endpoint); err != nil {
		return fmt.Errorf("parse endpoint: %w", err)
	}

	client := s3.NewFromConfig(aws.Config{
		Region:      creds.Region,
		Credentials: credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return nil
	} else {
		var respErr *smithyhttp.ResponseError
		if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != http.StatusNotFound {
			return fmt.Errorf("head bucket %q: %w", bucket, err)
		}
	}

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return nil
	} else {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
				return nil
			}
		}
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}
}

func signS3Request(req *http.Request, creds Credentials, now *time.Time) {
	t := time.Now().UTC()
	if now != nil {
		t = now.UTC()
	}
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")
	payloadHash := sha256Hex(nil)

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	host := req.URL.Host
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := dateStamp + "/" + creds.Region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey(creds.SecretKey, dateStamp, creds.Region), []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+creds.AccessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func signingKey(secret, date, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func canonicalRequestForTest(method, endpoint, bucket string, creds Credentials, at time.Time) (string, string, error) {
	req, err := http.NewRequest(method, strings.TrimRight(endpoint, "/")+"/"+bucket, bytes.NewReader(nil))
	if err != nil {
		return "", "", err
	}
	signS3Request(req, creds, &at)
	return req.Header.Get("Authorization"), req.Header.Get("X-Amz-Date"), nil
}
