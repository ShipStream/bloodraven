package rustfs

import (
	"context"
	"errors"
	"fmt"
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
