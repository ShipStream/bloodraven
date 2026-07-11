package rustfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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
	client, err := newS3Client(endpoint, creds)
	if err != nil {
		return err
	}

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

// GetObject fetches an object from the RustFS bucket. Returns found=false
// (with nil error) when the key does not exist so callers can distinguish a
// missing manifest from a read error. Signed with AWS SigV4 like EnsureBucket
// so no aws-cli sidecar image is needed. Used by the PITR archive-handoff
// scenario to read per-site binlog manifests directly from storage.
func GetObject(ctx context.Context, endpoint, bucket, key string, creds Credentials) (data []byte, found bool, err error) {
	if bucket == "" || key == "" {
		return nil, false, fmt.Errorf("bucket and key are required")
	}
	client, err := newS3Client(endpoint, creds)
	if err != nil {
		return nil, false, err
	}

	out, gerr := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if gerr != nil {
		var respErr *smithyhttp.ResponseError
		if errors.As(gerr, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
			return nil, false, nil
		}
		var apiErr smithy.APIError
		if errors.As(gerr, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get object %q/%q: %w", bucket, key, gerr)
	}
	defer out.Body.Close()
	body, rerr := io.ReadAll(out.Body)
	if rerr != nil {
		return nil, true, fmt.Errorf("read object %q/%q body: %w", bucket, key, rerr)
	}
	return body, true, nil
}

func newS3Client(endpoint string, creds Credentials) (*s3.Client, error) {
	if creds.AccessKey == "" || creds.SecretKey == "" {
		return nil, fmt.Errorf("access key and secret key are required")
	}
	if creds.Region == "" {
		creds.Region = "us-east-1"
	}
	endpoint = strings.TrimRight(endpoint, "/")
	if _, err := url.Parse(endpoint); err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	return s3.NewFromConfig(aws.Config{
		Region:      creds.Region,
		Credentials: credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}
